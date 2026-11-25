// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Cross-version rolling-upgrade interop tests: run the shipped marketplace dist
// Writer (`plugins/claude_code_dist/bigquery-agent-analytics-mod`) and the
// current source Writer against one shared dead-letter directory and store.

import { expect, test } from 'claude-code/testing'

import { resolveConfig as resolveDistConfig } from '../../claude_code_dist/bigquery-agent-analytics-mod/hooks/lib/config.ts'
import { Writer as DistWriter } from '../../claude_code_dist/bigquery-agent-analytics-mod/hooks/lib/writer.ts'
import { deadLetterPartition, resolveConfig, tableRef } from '../hooks/lib/config.ts'
import { packFiles } from '../hooks/lib/deadletter.ts'
import { utf8Length } from '../hooks/lib/rows.ts'
import { type Io, Writer as CurrentWriter } from '../hooks/lib/writer.ts'
import { ACCESS_TOKEN } from './helpers.ts'

const OPTIONS = {
  project: 'my-gcp-project',
  dataset: 'agent_analytics',
  deadLetterDir: '/tmp/bqaa-interop-dl',
} as const
const currentCfg = { ...resolveConfig(OPTIONS, {}), maxBatchRows: 1 }
const distCfg = { ...resolveDistConfig(OPTIONS, {}), maxBatchRows: 1 }
const PART_DIR = deadLetterPartition(currentCfg)
const TABLE = tableRef(currentCfg)

const rowOf = (eventId: string | null, tag: string) => {
  const base: Record<string, unknown> = {
    timestamp: '2026-10-13T20:01:02.003Z',
    event_type: 'STATE_DELTA',
    agent: 'coding-agent',
    session_id: 'interop-s0',
    invocation_id: null,
    user_id: 'dev',
    trace_id: null,
    span_id: 'bbbbbbbbbbbbbbbb',
    parent_span_id: null,
    content: JSON.stringify({ tag }),
    content_parts: [],
    attributes: '{}',
    latency_ms: null,
    status: 'OK',
    error_message: null,
    is_truncated: false,
  }
  if (eventId !== null) base.event_id = eventId
  return base
}

const dlLine = (kind: 'spill' | 'retry', eventId: string | null, tag: string, key?: string): string =>
  JSON.stringify({
    v: 1,
    kind,
    reason: 'retry',
    attempts: 0,
    failed_at: '2026-10-13T20:01:02.003Z',
    table: TABLE,
    row: rowOf(eventId, tag),
    ...(key !== undefined ? { key } : {}),
  })

function makeInteropHarness() {
  const files = new Map<string, string>()
  const mtimes = new Map<string, number>()
  const store = new Map<string, unknown>()
  const sentTags: string[] = []
  const sentInsertIds: string[] = []
  let nowMs = 1_700_000_000_000
  let fetchHook: ((tags: string[], insertIds: string[]) => Promise<{ status: number; text: string } | null>) | null = null
  let writeHook: ((path: string, text: string) => Promise<void>) | null = null
  let storeSetHook: ((key: string, value: unknown) => Promise<void>) | null = null
  let storeDeleteHook: ((key: string) => Promise<void>) | null = null

  const io: Io = {
    now: () => nowMs,
    after: () => ({ cancel: () => undefined }),
    fetch: async (_url, init) => {
      const body = JSON.parse(init.body ?? '{"rows":[]}') as { rows: { insertId?: string; json: { content: string } }[] }
      const tags = body.rows.map(r => (JSON.parse(r.json.content) as { tag: string }).tag)
      const insertIds = body.rows.map(r => r.insertId ?? '')
      if (fetchHook !== null) {
        const res = await fetchHook(tags, insertIds)
        if (res !== null) return res
      }
      sentTags.push(...tags)
      sentInsertIds.push(...insertIds)
      return { status: 200, text: '{}' }
    },
    runCommand: async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
    envToken: async () => ACCESS_TOKEN,
    writeFile: async (path, text) => {
      if (writeHook !== null) await writeHook(path, text)
      files.set(path, text)
      mtimes.set(path, nowMs)
    },
    readFile: async path => files.get(path) ?? '',
    exists: async path => files.has(path),
    listDir: async dir => {
      const pref = `${dir}/`
      return [...files.entries()]
        .filter(([p]) => p.startsWith(pref) && !p.slice(pref.length).includes('/'))
        .map(([p, t]) => ({ name: p.slice(pref.length), kind: 'file', size: utf8Length(t), mtimeMs: mtimes.get(p) ?? nowMs }))
    },
    storeGet: async key => store.get(key),
    storeSet: async (key, value) => {
      if (storeSetHook !== null) await storeSetHook(key, value)
      store.set(key, value === undefined ? undefined : JSON.parse(JSON.stringify(value)))
    },
    storeDelete: async key => {
      if (storeDeleteHook !== null) await storeDeleteHook(key)
      store.delete(key)
    },
    status: () => undefined,
    debug: () => undefined,
  }

  const count = (tag: string): number => sentTags.filter(t => t === tag).length
  const liveDataFiles = (): string[] =>
    [...files.entries()]
      .filter(([p, t]) => p.startsWith(`${PART_DIR}/`) && !p.includes('/outcome') && t !== '')
      .map(([p]) => p)

  return {
    files,
    mtimes,
    store,
    sentTags,
    sentInsertIds,
    io,
    count,
    liveDataFiles,
    advance: (ms: number) => {
      nowMs += ms
    },
    newCurrent: (sid: string) => new CurrentWriter(io, () => currentCfg, () => sid),
    newDist: (sid: string) => new DistWriter(io, () => distCfg, () => sid),
    setFetch: (fn: typeof fetchHook) => {
      fetchHook = fn
    },
    setWrite: (fn: typeof writeHook) => {
      writeHook = fn
    },
    setStoreSet: (fn: typeof storeSetHook) => {
      storeSetHook = fn
    },
    setStoreDelete: (fn: typeof storeDeleteHook) => {
      storeDeleteHook = fn
    },
  }
}

type Order = 'parent-first' | 'child-first'
const ORDERS: readonly Order[] = ['parent-first', 'child-first']

const pairNames = (order: Order, tag: string): { pName: string; cName: string } =>
  order === 'parent-first'
    ? { pName: `retry-20261013T200102001Z-${tag}-parent.jsonl`, cName: `spill-20261013T200102002Z-${tag}-child.jsonl` }
    : { pName: `spill-20261013T200102001Z-${tag}-parent.jsonl`, cName: `retry-20261013T200102002Z-${tag}-child.jsonl` }

test('interop: crash before record() never duplicates on auto recovery (even with pre-force v2) and never loses child rows on dist force', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const idC = 'c'.repeat(32)

  for (const order of ORDERS) {
    // Arm A: crash before record() -> dist auto -> current auto (must hold without duplicating B from pre-force v2) -> current force delivers C once and drops backup.
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `pre-rec-auto-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
      const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      // Stale v1 and pre-force v2 conflict on fates while BOTH name C, so `force`
      // replays P[A,B] fresh; crashing before record() reproduces Shape B if the
      // pre-dispatch guard isn't bound to the specific replay attempt.
      h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [cName], written: [true] })
      h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true], uncertain: [true] })

      h.setStoreSet(async k => {
        if (k === v1Key || k === v2Key) throw new Error('crash before record')
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (p.includes('/outcome2-')) throw new Error('crash before record file')
      })
      await h.newCurrent(`cur-pre-auto-1-${order}`).backfill(['spill', 'retry'], 'force')
      expect(h.count('A')).toBe(1)
      expect(h.count('B')).toBe(1)
      expect(h.count('C')).toBe(0)

      // Lease expires; dist runs auto (holds C and standalone backup B_c; 0.1.3 may retire P from stale v1).
      h.advance(15 * 60 * 1000)
      h.setStoreSet(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(null)
      const beforeDist = h.sentTags.length
      await h.newDist(`dist-pre-auto-1-${order}`).backfill(['spill', 'retry'], 'auto')
      await h.newDist(`dist-pre-auto-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.sentTags.length - beforeDist).toBe(0)
      expect(h.files.get(cPath)).toBe(cText)

      // Current auto MUST NOT pick pre-force v2 (or 0.1.3's stale v1 retirement) to duplicate B!
      h.setStoreSet(null)
      h.setStoreDelete(null)
      const rAuto = await h.newCurrent(`cur-pre-auto-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A')).toBe(1)
      expect(h.count('B')).toBe(1)
      expect(h.count('C')).toBe(0)
      expect(rAuto.held ?? '').toContain('/bqaa-backfill force')

      // Explicit force delivers C (once) and drops the standalone backup cleanly.
      await h.newCurrent(`cur-pre-auto-3-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newCurrent(`cur-pre-auto-4-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('C')).toBe(1)
      expect(new Set(h.sentTags)).toEqual(new Set(['A', 'B', 'C']))
      expect(h.liveDataFiles()).toEqual([])
    }

    // Arm B: crash before record() -> dist force (with stale v1 files:[]) MUST NEVER lose row C (standalone backup delivers it, deduped by event_id).
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `pre-rec-force-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
      const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
      h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })

      h.setStoreSet(async k => {
        if (k === v1Key || k === v2Key) throw new Error('crash before record')
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (p.includes('/outcome2-')) throw new Error('crash before record file')
      })
      await h.newCurrent(`cur-pre-force-1-${order}`).backfill(['spill', 'retry'], 'force')

      h.advance(15 * 60 * 1000)
      h.setStoreSet(null)
      h.setStoreDelete(null)
      h.setWrite(null)
      await h.newDist(`dist-pre-force-1-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newDist(`dist-pre-force-2-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newCurrent(`cur-pre-force-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A') >= 1).toBe(true)
      expect(h.count('B') >= 1).toBe(true)
      expect(h.count('C') >= 1).toBe(true)
      expect(new Set(h.sentInsertIds)).toEqual(new Set([idA, idB, idC]))
      expect(h.liveDataFiles()).toEqual([])
    }
  }
})

test('interop: crash after record() before Phase 3 delivers mixed child exactly once on auto and never loses rows on dist force', async () => {
  const idA = '1'.repeat(32)
  const idB = '2'.repeat(32)
  const idC = '3'.repeat(32)

  for (const order of ORDERS) {
    // Arm A: unkeyed rows, crash after record() before Phase 3 -> dist auto (0 rows) -> current auto delivers ONLY C (1/1/1) and drops backup!
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `pre-p3-auto-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', null, 'A'), dlLine('spill', null, 'B')])[0]!
      const cText = packFiles([dlLine('retry', null, 'B', `${pName}#1`), dlLine('retry', null, 'C')], pName)[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [cName], written: [true] })
      h.store.set(v2Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 5, files: [], written: [] })

      let v2Writes = 0
      h.setStoreSet(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
        if (k === v2Key) {
          v2Writes += 1
          if (v2Writes >= 2) throw new Error('crash in Phase 3 staging')
        }
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (v2Writes >= 2 && p.includes('/outcome2-')) throw new Error('crash in Phase 3 file')
      })
      await h.newCurrent(`cur-p3-auto-1-${order}`).backfill(['spill', 'retry'], 'force')

      h.advance(15 * 60 * 1000)
      h.setStoreSet(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(null)
      const beforeDist = h.sentTags.length
      await h.newDist(`dist-p3-auto-1-${order}`).backfill(['spill', 'retry'], 'auto')
      await h.newDist(`dist-p3-auto-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.sentTags.length - beforeDist).toBe(0)

      h.setStoreSet(null)
      h.setStoreDelete(null)
      await h.newCurrent(`cur-p3-auto-2-${order}`).backfill(['spill', 'retry'], 'auto')
      await h.newCurrent(`cur-p3-auto-3-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A')).toBe(1)
      expect(h.count('B')).toBe(1)
      expect(h.count('C')).toBe(1)
      expect(h.liveDataFiles()).toEqual([])
    }

    // Arm B: keyed rows, crash after record() before Phase 3 -> dist force delivers C via standalone backup with zero loss in both name orders.
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `pre-p3-force-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
      const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
      h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })

      let v2Writes = 0
      h.setStoreSet(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
        if (k === v2Key) {
          v2Writes += 1
          if (v2Writes >= 2) throw new Error('crash before Phase 3')
        }
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (v2Writes >= 2 && p.includes('/outcome2-')) throw new Error('crash before Phase 3 file')
      })
      await h.newCurrent(`cur-p3-force-1-${order}`).backfill(['spill', 'retry'], 'force')

      h.advance(15 * 60 * 1000)
      h.setStoreSet(null)
      h.setStoreDelete(null)
      h.setWrite(null)
      await h.newDist(`dist-p3-force-1-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newDist(`dist-p3-force-2-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newCurrent(`cur-p3-force-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A') >= 1).toBe(true)
      expect(h.count('B') >= 1).toBe(true)
      expect(h.count('C') >= 1).toBe(true)
      expect(new Set(h.sentInsertIds)).toEqual(new Set([idA, idB, idC]))
      expect(h.liveDataFiles()).toEqual([])
    }
  }

  // Arm C: crash after record() before Phase 3 -> dist force retires P and drops C unsent, then fails before delivering B_c -> current auto delivers ONLY C via v2 accountedKeys (1/1/1).
  {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames('parent-first', 'pre-p3-partial-dist')
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
    h.files.set(pPath, pText)
    h.files.set(cPath, cText)
    const pSize = utf8Length(pText)
    const v1Key = `replayed:${TABLE}/${pName}`
    const v2Key = `replayed:v2:${TABLE}/${pName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })

    let v2Writes = 0
    h.setStoreSet(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
      if (k === v2Key) {
        v2Writes += 1
        if (v2Writes >= 2) throw new Error('crash before Phase 3')
      }
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    h.setWrite(async p => {
      if (v2Writes >= 2 && p.includes('/outcome2-')) throw new Error('crash before Phase 3 file')
    })
    await h.newCurrent('cur-p3-part-1').backfill(['spill', 'retry'], 'force')

    const backupPath = h.liveDataFiles().find(p => p !== pPath && p !== cPath)
    expect(backupPath).toBeDefined()
    h.advance(15 * 60 * 1000)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    h.setWrite(null)
    // Simulate dist force retiring P and dropping C unsent while B_c is leased/unreached.
    const backupName = backupPath!.slice(backupPath!.lastIndexOf('/') + 1)
    h.store.set(`lease:${TABLE}/${backupName}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    await h.newDist('dist-p3-part-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(pPath)).toBe('')

    // Lease expires; current auto recovers via v2 accountedKeys so ONLY C is sent (1/1/1).
    h.advance(10 * 60 * 1000)
    await h.newCurrent('cur-p3-part-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-p3-part-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count('A')).toBe(1)
    expect(h.count('B')).toBe(1)
    expect(h.count('C')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: restored exact copy of reconciled child is never resent by dist auto and is dropped by current auto, while repopulations are delivered', async () => {
  const idA = '4'.repeat(32)
  const idB = '5'.repeat(32)
  const idD = '6'.repeat(32)
  const idE = '7'.repeat(32)

  // Arm A: exact restored copy of reconciled child -> dist auto sends 0 rows, current auto drops it (0 resends).
  {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames('parent-first', 'reconciled-exact')
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const oldC = packFiles([dlLine('retry', idB, 'B')], pName)[0]!
    h.files.set(pPath, pText)
    h.files.set(cPath, oldC)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [cName], written: [true] })

    await h.newCurrent('cur-exact-1').backfill(['spill', 'retry'], 'force')
    expect(h.count('A')).toBe(1)
    expect(h.count('B')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])

    // Before any restore (within WRITE_PENDING_MS), shipped dist auto must NOT
    // emit a false "is empty, perhaps still being written" notice on emptied C.
    const rDistEmpty = await h.newDist('dist-exact-empty').backfill(['spill', 'retry'], 'auto')
    expect(rDistEmpty.skippedFiles).toBe(0)
    expect(rDistEmpty.held).toBeNull()

    // Late write restores the exact original bytes of reconciled child C.
    h.files.set(cPath, oldC)
    h.advance(15 * 60 * 1000)
    const beforeDist = h.sentTags.length
    await h.newDist('dist-exact-1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentTags.length - beforeDist).toBe(0)

    await h.newCurrent('cur-exact-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('B')).toBe(1)
    expect(h.files.get(cPath)).toBe('')
  }

  // Arm B: different-size repopulation at reconciled child -> dist auto delivers it directly with zero loss.
  {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames('parent-first', 'reconciled-diffsize')
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const oldC = packFiles([dlLine('retry', idB, 'B')], pName)[0]!
    const newC = packFiles([dlLine('retry', idD, 'D'), dlLine('retry', idE, 'E')], pName)[0]!
    h.files.set(pPath, pText)
    h.files.set(cPath, oldC)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [cName], written: [true] })

    await h.newCurrent('cur-diff-1').backfill(['spill', 'retry'], 'force')
    h.files.set(cPath, newC)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-diff-1').backfill(['spill', 'retry'], 'auto')
    expect(h.count('D')).toBe(1)
    expect(h.count('E')).toBe(1)
    expect(h.files.get(cPath)).toBe('')
  }

  // Arm C: same-size different-content repopulation -> dist auto never drops it (holds or delivers), and current auto delivers it once.
  {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames('parent-first', 'reconciled-samesize')
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const oldC = packFiles([dlLine('retry', idB, 'B')], pName)[0]!
    const newC = packFiles([dlLine('retry', idD, 'D')], pName)[0]!
    expect(utf8Length(newC)).toBe(utf8Length(oldC))
    h.files.set(pPath, pText)
    h.files.set(cPath, oldC)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [cName], written: [true] })

    await h.newCurrent('cur-same-1').backfill(['spill', 'retry'], 'force')
    h.files.set(cPath, newC)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-same-1').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-same-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('D')).toBe(1)
    expect(h.files.get(cPath)).toBe('')
  }
})

test('interop: equal-size repopulation after dist fallback record and current completion never unmasks superseded digest-less dist record', async () => {
  const idA = '8'.repeat(32)
  const idB = '9'.repeat(32)
  const idX = 'a'.repeat(32)
  const idY = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20261013T200102001Z-interop-unmask-p.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const p1Text = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const p2Text = packFiles([dlLine('retry', idX, 'X'), dlLine('retry', idY, 'Y')])[0]!
  expect(utf8Length(p2Text)).toBe(utf8Length(p1Text))
  h.files.set(pPath, p1Text)

  // Step 1: Shipped dist Writer runs under store-quota fallback (writes digest-less
  // `outcome-<pName>` with fates 'LL') and crashes before markDone / truncation.
  h.setStoreSet(async k => {
    if (k.startsWith('replayed:')) throw new Error('store quota exceeded')
  })
  h.setWrite(async (p, text) => {
    if ((p.includes('/outcome-done-') && text.includes('"retired":true')) || (p === pPath && text === '')) {
      throw new Error('crash before dist markDone / truncate')
    }
  })
  await h.newDist('dist-unmask-g1').backfill(['retry'], 'auto')
  expect(h.count('A')).toBe(1)
  expect(h.count('B')).toBe(1)
  expect(h.files.get(pPath)).toBe(p1Text)

  // Step 2: Current Writer cleanly retires generation 1 on force (writing a
  // `sourceFnv1a`-stamped retired done mark while dist's `outcome-` file remains).
  h.advance(15 * 60 * 1000)
  h.setStoreSet(null)
  h.setWrite(null)
  await h.newCurrent('cur-unmask-g1').backfill(['retry'], 'force')
  expect(h.files.get(pPath)).toBe('')

  // Step 3: Brand-new equal-size generation 2 [X, Y] replaces pPath; current
  // auto MUST NOT unmask dist's superseded digest-less `outcome-` record to drop it!
  // Because dist's own foreign sending mark from Step 1 still stands at the same
  // size, auto holds p2Text intact (zero silent loss) and force delivers [X, Y].
  h.files.set(pPath, p2Text)
  const rAuto = await h.newCurrent('cur-unmask-g2').backfill(['retry'], 'auto')
  expect(h.files.get(pPath)).toBe(p2Text)
  expect(rAuto.held ?? '').toContain('/bqaa-backfill force')
  await h.newCurrent('cur-unmask-g2-force').backfill(['retry'], 'force')
  expect(h.count('X')).toBe(1)
  expect(h.count('Y')).toBe(1)
  expect(h.files.get(pPath)).toBe('')

  // Step 4: Same equal-size repopulation when no foreign sending mark stands
  // (only dist's leftover `outcome-` record + current's `sourceFnv1a` done mark):
  // current auto delivers [Z1, Z2] directly without unmasking dist's record.
  const idZ1 = 'c'.repeat(32)
  const idZ2 = 'd'.repeat(32)
  const p3Text = packFiles([dlLine('retry', idZ1, 'P'), dlLine('retry', idZ2, 'Q')])[0]!
  expect(utf8Length(p3Text)).toBe(utf8Length(p1Text))
  // Clear dist's stale Step 1 sending mark while leaving dist's `outcome-` record intact.
  for (const [k, v] of h.files.entries()) {
    if (k.includes('/outcome-done-') && v.includes('"sending":true')) h.files.set(k, '')
  }
  h.files.set(pPath, p3Text)
  const rAutoClean = await h.newCurrent('cur-unmask-g3').backfill(['retry'], 'auto')
  expect(rAutoClean.held).toBeNull()
  expect(h.count('P')).toBe(1)
  expect(h.count('Q')).toBe(1)
  expect(h.files.get(pPath)).toBe('')
})

test('interop: foreign lease on guarded child fails closed before dispatch, and once cleared backup protects 0.1.3 force recovery in both file orders', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const idC = 'c'.repeat(32)

  for (const order of ORDERS) {
    // Arm A: pre-existing foreign lease on child C -> current force fails closed
    // (0 rows sent, 0 guards left, 0 backups written, P and C untouched). Once
    // lease expires, current force stages the backup; even if it crashes before
    // record(), 0.1.3 force delivers A/B/C with zero loss in both file orders.
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `foreign-lease-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
      const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
      h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
      h.store.set(`lease:${TABLE}/${cName}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })

      const rLeased = await h.newCurrent(`cur-lease-1-${order}`).backfill(['spill', 'retry'], 'force')
      expect(rLeased.error ?? '').toContain('standalone backups')
      expect(h.count('A')).toBe(0)
      expect(h.count('B')).toBe(0)
      expect(h.count('C')).toBe(0)
      expect(h.files.get(pPath)).toBe(pText)
      expect(h.files.get(cPath)).toBe(cText)
      expect(h.liveDataFiles().sort()).toEqual([cPath, pPath].sort())
      const standingChildGuards = [...h.files.entries()].filter(([p, t]) => p.includes('/outcome-done-') && p.endsWith(`-${cName}`) && t !== '')
      expect(standingChildGuards.length).toBe(0)

      // Lease expires -> current force stages guard + backup, dispatches P, and crashes before record().
      h.advance(15 * 60 * 1000)
      h.setStoreSet(async k => {
        if (k === v1Key || k === v2Key) throw new Error('crash before record')
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (p.includes('/outcome2-')) throw new Error('crash before record file')
      })
      await h.newCurrent(`cur-lease-2-${order}`).backfill(['spill', 'retry'], 'force')
      expect(h.count('A')).toBe(1)
      expect(h.count('B')).toBe(1)
      expect(h.liveDataFiles().length).toBe(3)

      // Lease expires -> dist 0.1.3 force runs and delivers all rows (deduped by event_id/insertId) with zero loss.
      h.advance(15 * 60 * 1000)
      h.setStoreSet(null)
      h.setStoreDelete(null)
      h.setWrite(null)
      await h.newDist(`dist-lease-1-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newDist(`dist-lease-2-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newCurrent(`cur-lease-3-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A') >= 1).toBe(true)
      expect(h.count('B') >= 1).toBe(true)
      expect(h.count('C') >= 1).toBe(true)
      expect(new Set(h.sentInsertIds)).toEqual(new Set([idA, idB, idC]))
      expect(h.liveDataFiles()).toEqual([])
    }

    // Arm B: ID-less rows staged in standalone backup receive deterministic fallback
    // event_id + insertId so 0.1.3 force replays carry matching insertIds across
    // parent dispatch, leftover child, and standalone backup (dedupable by BigQuery & views).
    {
      const h = makeInteropHarness()
      const { pName, cName } = pairNames(order, `idless-backup-${order}`)
      const pPath = `${PART_DIR}/${pName}`
      const cPath = `${PART_DIR}/${cName}`
      const pText = packFiles([dlLine('spill', null, 'A'), dlLine('spill', null, 'B')])[0]!
      const cText = packFiles(
        [
          dlLine('retry', `dl:${pName}#1`, 'B', `${pName}#1`),
          dlLine('retry', `dl:${cName}#1`, 'C', `${cName}#1`),
        ],
        pName,
      )[0]!
      h.files.set(pPath, pText)
      h.files.set(cPath, cText)
      const pSize = utf8Length(pText)
      const v1Key = `replayed:${TABLE}/${pName}`
      const v2Key = `replayed:v2:${TABLE}/${pName}`
      h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
      h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })

      h.setStoreSet(async k => {
        if (k === v1Key || k === v2Key) throw new Error('crash before record')
      })
      h.setStoreDelete(async k => {
        if (k === v1Key) throw new Error('v1 stuck')
      })
      h.setWrite(async p => {
        if (p.includes('/outcome2-')) throw new Error('crash before record file')
      })
      await h.newCurrent(`cur-idless-1-${order}`).backfill(['spill', 'retry'], 'force')

      h.advance(15 * 60 * 1000)
      h.setStoreSet(null)
      h.setStoreDelete(null)
      h.setWrite(null)
      await h.newDist(`dist-idless-1-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newDist(`dist-idless-2-${order}`).backfill(['spill', 'retry'], 'force')
      await h.newCurrent(`cur-idless-2-${order}`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('A') >= 1).toBe(true)
      expect(h.count('B') >= 1).toBe(true)
      expect(h.count('C') >= 1).toBe(true)
      // Every single sent row must have a non-empty deterministic fallback insertId.
      expect(h.sentInsertIds.every(id => id !== '')).toBe(true)
      expect(new Set(h.sentInsertIds)).toEqual(new Set([`dl:${pName}#0`, `dl:${pName}#1`, `dl:${cName}#1`]))
      expect(h.liveDataFiles()).toEqual([])
    }
  }

  // Arm C: pre-existing ID-less child on disk (no row.event_id, only envelope key) in
  // parent-first order -> current parent dispatch and backup copy share identical `dl:<key>`
  // insertIds so 0.1.3 force delivers C and deduplicates B cleanly.
  {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames('parent-first', 'idless-preexisting')
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const pText = packFiles([dlLine('spill', null, 'A'), dlLine('spill', null, 'B')])[0]!
    const cText = packFiles([dlLine('retry', null, 'B', `${pName}#1`), dlLine('retry', null, 'C')], pName)[0]!
    h.files.set(pPath, pText)
    h.files.set(cPath, cText)
    const pSize = utf8Length(pText)
    const v1Key = `replayed:${TABLE}/${pName}`
    const v2Key = `replayed:v2:${TABLE}/${pName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    h.setStoreSet(async k => {
      if (k === v1Key || k === v2Key) throw new Error('crash before record')
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    h.setWrite(async p => {
      if (p.includes('/outcome2-')) throw new Error('crash before record file')
    })
    await h.newCurrent('cur-idless-pre-1').backfill(['spill', 'retry'], 'force')

    h.advance(15 * 60 * 1000)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    h.setWrite(null)
    await h.newDist('dist-idless-pre-1').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-idless-pre-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('A')).toBe(1)
    expect(h.count('B') >= 1).toBe(true)
    expect(h.count('C')).toBe(1)
    expect(h.sentInsertIds.every(id => id !== '')).toBe(true)
    expect(new Set(h.sentInsertIds)).toEqual(new Set([`dl:${pName}#0`, `dl:${pName}#1`, `dl:${cName}#1`]))
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: same-length parent retirement carries live child from unretired earlier-digest journal so 0.1.3 force never empties it', async () => {
  const idA = 'e'.repeat(32)
  const idB = 'f'.repeat(32)
  const idC = '1'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'spill-20261013T200102001Z-same-len-gen-p.jsonl'
  const cName = 'retry-20261013T200102002Z-same-len-gen-c.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const cPath = `${PART_DIR}/${cName}`
  const pGen1 = packFiles([dlLine('spill', idA, 'A')])[0]!
  const pGen2 = packFiles([dlLine('spill', idB, 'B')])[0]!
  expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
  const cText = packFiles([dlLine('retry', idC, 'C')], pName)[0]!
  h.files.set(pPath, pGen2)
  h.files.set(cPath, cText)
  const pSize = utf8Length(pGen2)
  // Unretired earlier-generation v2 journal at the exact same byte size with a different sourceFnv1a.
  h.store.set(`replayed:v2:${TABLE}/${pName}`, {
    fates: 'W',
    reasons: [],
    refusals: [],
    size: pSize,
    sourceFnv1a: '11111111',
    waits: true,
    seq: 4,
    files: [cName],
    written: [true],
  })
  // Lease child C so current writer retires P_gen2 first without yet replaying C.
  h.store.set(`lease:${TABLE}/${cName}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })

  await h.newCurrent('cur-gen2-1').backfill(['spill'], 'auto')
  expect(h.count('B')).toBe(1)
  expect(h.files.get(pPath)).toBe('')
  expect(h.files.get(cPath)).toBe(cText)

  // Lease expires -> dist 0.1.3 force sees C carried in P's new retired done mark and delivers C instead of dropping it!
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-gen2-1').backfill(['spill', 'retry'], 'force')
  expect(h.count('C')).toBe(1)
  expect(h.files.get(cPath)).toBe('')
})
