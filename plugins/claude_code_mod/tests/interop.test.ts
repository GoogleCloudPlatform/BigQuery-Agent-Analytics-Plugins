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
import { fnv1a, packFiles, sealFile } from '../hooks/lib/deadletter.ts'
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
  let readHook: ((path: string) => Promise<string | undefined>) | null = null
  let storeGetHook: ((key: string) => Promise<void>) | null = null
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
    readFile: async path => {
      if (readHook !== null) {
        const override = await readHook(path)
        if (override !== undefined) return override
      }
      if (!files.has(path)) {
        const err = new Error(`ENOENT: no such file or directory, open '${path}'`) as Error & { code?: string }
        err.code = 'ENOENT'
        throw err
      }
      return files.get(path) as string
    },
    exists: async path => files.has(path),
    listDir: async dir => {
      const pref = `${dir}/`
      return [...files.entries()]
        .filter(([p]) => p.startsWith(pref) && !p.slice(pref.length).includes('/'))
        .map(([p, t]) => ({ name: p.slice(pref.length), kind: 'file', size: utf8Length(t), mtimeMs: mtimes.get(p) ?? nowMs }))
    },
    storeGet: async key => {
      if (storeGetHook !== null) await storeGetHook(key)
      return store.get(key)
    },
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
    newCurrent: (sid: string, cfgOverride?: Partial<typeof currentCfg>) =>
      new CurrentWriter(io, () => (cfgOverride ? { ...currentCfg, ...cfgOverride } : currentCfg), () => sid),
    newDist: (sid: string, cfgOverride?: Partial<typeof distCfg>) =>
      new DistWriter(io, () => (cfgOverride ? { ...distCfg, ...cfgOverride } : distCfg), () => sid),
    setFetch: (fn: typeof fetchHook) => {
      fetchHook = fn
    },
    setWrite: (fn: typeof writeHook) => {
      writeHook = fn
    },
    setRead: (fn: typeof readHook) => {
      readHook = fn
    },
    setStoreGet: (fn: typeof storeGetHook) => {
      storeGetHook = fn
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

      const sName = `spill-20261013T200102999Z-independent-${order}.jsonl`
      const sPath = `${PART_DIR}/${sName}`
      const idS = '9'.repeat(32)
      h.files.set(sPath, packFiles([dlLine('spill', idS, 'S')])[0]!)

      const rLeased = await h.newCurrent(`cur-lease-1-${order}`).backfill(['spill', 'retry'], 'force')
      expect(rLeased.error).toBeNull()
      expect(rLeased.held ?? '').toContain(cName)
      expect((rLeased.heldFiles ?? 0) >= 2).toBe(true)
      // Independent later file S must NOT be stalled by the leased family.
      expect(h.count('S')).toBe(1)
      expect(h.files.get(sPath)).toBe('')
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
      expect(new Set(h.sentInsertIds)).toEqual(new Set([idA, idB, idC, idS]))
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

  // Arm C: pre-existing legacy ID-less child on disk (no row.event_id) in BOTH
  // parent-first AND child-first orders:
  // - parent-first: 0.1.3 drops C unsent and defers linked backup B; both 0.1.4
  //   auto and 0.1.3 force Pass 2 deliver B once with deterministic `dl:` IDs.
  // - child-first: 0.1.3 delivers C once, retires C, and drops linked backup B
  //   via `parent: C`, so unique legacy ID-less row C is NEVER sent twice.
  for (const order of ORDERS) {
    const h = makeInteropHarness()
    const { pName, cName } = pairNames(order, `idless-preexisting-${order}`)
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
    await h.newCurrent(`cur-idless-pre-1-${order}`).backfill(['spill', 'retry'], 'force')

    h.advance(15 * 60 * 1000)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    h.setWrite(null)
    await h.newDist(`dist-idless-pre-1-${order}`).backfill(['spill', 'retry'], 'force')
    await h.newCurrent(`cur-idless-pre-2-${order}`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('A')).toBe(1)
    expect(h.count('B') >= 1).toBe(true)
    expect(h.count('C')).toBe(1)
    if (order === 'parent-first') {
      expect(h.sentInsertIds.every(id => id !== '')).toBe(true)
      expect(new Set(h.sentInsertIds)).toEqual(new Set([`dl:${pName}#0`, `dl:${pName}#1`, `dl:${cName}#1`]))
    }
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

test('interop: multi-part ID-less leftover checkpoint never loses rows across 0.1.3/0.1.4 resume in either direction', async () => {
  const pad = 'x'.repeat(280)
  const bigRows = Array.from({ length: 3900 }, (_, i) =>
    JSON.stringify({
      v: 1,
      kind: 'spill',
      reason: 'retry',
      attempts: 0,
      failed_at: '2026-10-13T20:01:02.003Z',
      table: TABLE,
      row: rowOf(null, `R${String(i).padStart(4, '0')}-${pad}`),
    }),
  )
  const sealedBigParts = packFiles(bigRows)
  expect(sealedBigParts.length).toBe(1)
  const sealedBig = sealedBigParts[0]!
  expect(utf8Length(sealedBig) < 3 * 1024 * 1024).toBe(true)

  // Arm 1: 0.1.4 refuses all 3,900 ID-less rows, writes a 2-part checkpoint (v2, stampedIds: true)
  // and fails on part 2 ([true, false]). 0.1.3 command holds P instead of mis-resuming with 0.1.3
  // packing, and 0.1.4 command finishes and delivers all 3,900 distinct rows with zero loss.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20261013T200102001Z-idless-2part-cur.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    h.files.set(pPath, sealedBig)
    h.setFetch(async tags => ({
      status: 200,
      text: JSON.stringify({
        insertErrors: tags.map((_, index) => ({ index, errors: [{ reason: 'invalid', message: 'bad row' }] })),
      }),
    }))
    let rejectedWrites = 0
    h.setWrite(async p => {
      if (p.includes('/rejected-') && !p.includes('/outcome')) {
        rejectedWrites += 1
        if (rejectedWrites === 2) throw new Error('disk full on part 2')
      }
    })
    await h.newCurrent('cur-2p-1', { maxBatchRows: 500 }).backfill(['spill'], 'auto')
    expect(rejectedWrites).toBe(2)
    // Must be isolated in v2 with stampedIds: true (never exposed in v1).
    expect(h.store.has(`replayed:${TABLE}/${pName}`)).toBe(false)
    const v2Journal = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { files?: string[]; written?: boolean[]; stampedIds?: boolean } | undefined
    expect(v2Journal?.stampedIds).toBe(true)
    expect(v2Journal?.written).toEqual([true, false])

    // 0.1.3 command sees standing sending mark, holds P, and loses 0 rows.
    h.setWrite(null)
    h.setFetch(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-2p-cmd', { maxBatchRows: 500 }).backfill(['spill', 'retry', 'rejected'], 'command')
    expect(h.files.get(pPath)).toBe(sealedBig)
    expect(h.sentTags.length).toBe(0)

    // 0.1.4 command resumes v2, writes part 2, retires P, and delivers all 3,900 distinct rows.
    await h.newCurrent('cur-2p-2', { maxBatchRows: 500 }).backfill(['spill', 'retry', 'rejected'], 'command')
    await h.newCurrent('cur-2p-3', { maxBatchRows: 500 }).backfill(['spill', 'retry', 'rejected'], 'command')
    expect(h.sentTags.length).toBe(3900)
    expect(new Set(h.sentTags).size).toBe(3900)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2: 0.1.3 creates a 2-part v1 partial checkpoint [true, false] on a 3,900-row ID-less
  // file whose rejected lines exceed 3 MiB even without fallback IDs, and 0.1.4 command resumes
  // that v1 checkpoint with exact 0.1.3 packing -> all 3,900 distinct rows delivered, 0 lost/dup.
  {
    const h = makeInteropHarness()
    const padWide = 'y'.repeat(280)
    const wideRows = Array.from({ length: 3900 }, (_, i) =>
      JSON.stringify({
        v: 1,
        kind: 'spill',
        reason: 'retry',
        attempts: 0,
        failed_at: '2026-10-13T20:01:02.003Z',
        table: TABLE,
        row: rowOf(null, `W${String(i).padStart(4, '0')}-${padWide}`),
      }),
    )
    const sealedWideParts = packFiles(wideRows)
    expect(sealedWideParts.length).toBe(1)
    const sealedWide = sealedWideParts[0]!
    expect(utf8Length(sealedWide) < 3 * 1024 * 1024).toBe(true)
    const pName = 'spill-20261013T200102002Z-idless-2part-dist.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    h.files.set(pPath, sealedWide)
    const longErr = 'e'.repeat(180)
    h.setFetch(async tags => ({
      status: 200,
      text: JSON.stringify({
        insertErrors: tags.map((_, index) => ({ index, errors: [{ reason: 'invalid', message: longErr }] })),
      }),
    }))
    let rejectedWrites = 0
    h.setWrite(async p => {
      if (p.includes('/rejected-') && !p.includes('/outcome')) {
        rejectedWrites += 1
        if (rejectedWrites === 2) throw new Error('disk full on dist part 2')
      }
    })
    await h.newDist('dist-v1-1', { maxBatchRows: 500 }).backfill(['spill'], 'auto')
    expect(rejectedWrites).toBe(2)
    const v1Journal = h.store.get(`replayed:${TABLE}/${pName}`) as { written?: boolean[] } | undefined
    expect(v1Journal?.written).toEqual([true, false])

    // 0.1.4 command resumes 0.1.3's v1 checkpoint using exact 0.1.3 boundaries, then delivers both parts.
    h.setWrite(null)
    h.setFetch(null)
    h.advance(15 * 60 * 1000)
    await h.newCurrent('cur-v1-resume-1', { maxBatchRows: 500 }).backfill(['spill', 'retry', 'rejected'], 'command')
    await h.newCurrent('cur-v1-resume-2', { maxBatchRows: 500 }).backfill(['spill', 'retry', 'rejected'], 'command')
    expect(h.sentTags.length).toBe(3900)
    expect(new Set(h.sentTags).size).toBe(3900)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: equal-length child replacement during lease lookup or backup write never loses new row D on 0.1.3 force', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const idC = 'c'.repeat(32)
  const idD = 'd'.repeat(32)
  const pName = 'spill-20261013T200102001Z-lease-swap-p.jsonl'
  const cName = 'retry-20261013T200102002Z-lease-swap-c.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const cPath = `${PART_DIR}/${cName}`
  const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
  const cOld = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
  const cNew = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idD, 'D')], pName)[0]!
  expect(utf8Length(cNew)).toBe(utf8Length(cOld))
  const pSize = utf8Length(pText)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`

  // Arm 1: Owner swaps C -> equal-length cNew (row D) and drops lease during storeGet('lease:...').
  {
    const h = makeInteropHarness()
    h.files.set(pPath, pText)
    h.files.set(cPath, cOld)
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    const childLeaseKey = `lease:${TABLE}/${cName}`
    h.store.set(childLeaseKey, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    let swapped = false
    h.setStoreGet(async k => {
      if (k === childLeaseKey && !swapped) {
        swapped = true
        h.files.set(cPath, cNew)
        h.store.delete(childLeaseKey)
      }
    })
    h.setStoreSet(async k => {
      if (k === v1Key || k === v2Key) throw new Error('crash before record')
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    h.setWrite(async p => {
      if (p.includes('/outcome2-')) throw new Error('crash before record file')
    })
    await h.newCurrent('cur-swap-1').backfill(['spill', 'retry'], 'force')
    expect(swapped).toBe(true)

    h.advance(15 * 60 * 1000)
    h.setStoreGet(null)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    h.setWrite(null)
    await h.newDist('dist-swap-1').backfill(['spill', 'retry'], 'force')
    await h.newDist('dist-swap-2').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-swap-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('D') >= 1).toBe(true)
    expect(h.count('C')).toBe(0)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2: C is swapped to equal-length cNew (row D) AFTER initial read while backup file is being written.
  // Post-loop digest re-check must abort dispatch, roll back stale backup & guard, and deliver D on retry.
  {
    const h = makeInteropHarness()
    h.files.set(pPath, pText)
    h.files.set(cPath, cOld)
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    let swappedOnBackupWrite = false
    h.setWrite(async p => {
      if (!swappedOnBackupWrite && p.includes('/retry-') && !p.includes('/outcome') && p !== cPath) {
        swappedOnBackupWrite = true
        h.files.set(cPath, cNew)
      }
    })
    const rAborted = await h.newCurrent('cur-midwrite-1').backfill(['spill', 'retry'], 'force')
    expect(swappedOnBackupWrite).toBe(true)
    expect(rAborted.error ?? '').toContain('standalone backups')
    expect(h.count('A')).toBe(0)
    expect(h.count('C')).toBe(0)
    expect(h.count('D')).toBe(0)
    // Stale backup & guard rolled back; next force + crash + 0.1.3 force delivers D cleanly.
    h.setWrite(async p => {
      if (p.includes('/outcome2-')) throw new Error('crash before record file')
    })
    h.setStoreSet(async k => {
      if (k === v1Key || k === v2Key) throw new Error('crash before record')
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    await h.newCurrent('cur-midwrite-2').backfill(['spill', 'retry'], 'force')
    h.advance(15 * 60 * 1000)
    h.setWrite(null)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    await h.newDist('dist-midwrite-1').backfill(['spill', 'retry'], 'force')
    await h.newDist('dist-midwrite-2').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-midwrite-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count('D') >= 1).toBe(true)
    expect(h.count('C')).toBe(0)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: same-length parent retirement preserves child named only in pendingReconcile across both v1 and v2 rewrites', async () => {
  const idA = '1'.repeat(32)
  const idB = '2'.repeat(32)
  const idC = '3'.repeat(32)
  const idU = '4'.repeat(32)

  for (const mode of ['v1-rewrite', 'v2-rewrite'] as const) {
    const h = makeInteropHarness()
    const pName = `spill-20261013T200102001Z-pend-ret-${mode}.jsonl`
    const cName = `retry-20261013T200102002Z-pend-ret-${mode}.jsonl`
    const bName = `retry-20261013T200102003Z-pend-ret-bk-${mode}.jsonl`
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const bPath = `${PART_DIR}/${bName}`
    const pGen1 = packFiles([dlLine('spill', idA, 'A')])[0]!
    const pGen2 = packFiles([dlLine('spill', mode === 'v2-rewrite' ? null : idB, 'B')])[0]!
    const pGen2Sized =
      mode === 'v2-rewrite'
        ? packFiles([dlLine('spill', null, 'B' + 'x'.repeat(utf8Length(pGen1) - utf8Length(pGen2)))])[0]!
        : pGen2
    expect(utf8Length(pGen2Sized)).toBe(utf8Length(pGen1))
    const cOld = packFiles([dlLine('retry', idC, 'C')], pName)[0]!
    const cNew = packFiles([dlLine('retry', idU, 'U')], pName)[0]!
    expect(utf8Length(cNew)).toBe(utf8Length(cOld))
    const bText = packFiles([dlLine('retry', idC, 'C')])[0]!
    const pSize = utf8Length(pGen2Sized)
    const cSize = utf8Length(cNew)
    const bSize = utf8Length(bText)

    h.files.set(pPath, pGen2Sized)
    h.files.set(cPath, cNew)
    h.files.set(bPath, bText)
    h.files.set(
      `${PART_DIR}/outcome-done-oldg-${bName}`,
      sealFile(
        `${JSON.stringify({
          size: bSize,
          sending: true,
          by: 'oldg',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: cName,
          backup: true,
          backupForSize: cSize,
          backupForFnv1a: fnv1a(cOld),
          backupFnv1a: fnv1a(bText),
          attempt: 'old-attempt',
        })}\n`,
      ),
    )
    // Prior-generation v2 checkpoint names cName ONLY in pendingReconcile (`files: []`).
    h.store.set(`replayed:v2:${TABLE}/${pName}`, {
      fates: 'L',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 6,
      files: [],
      written: [],
      pendingReconcile: [{ name: cName, size: cSize, fnv1a: fnv1a(cOld) }],
      backupFiles: [{ name: bName, size: bSize, fnv1a: fnv1a(bText), replaces: cName, replacesSize: cSize, replacesFnv1a: fnv1a(cOld) }],
    })
    h.store.set(`lease:${TABLE}/${cName}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })

    if (mode === 'v2-rewrite') {
      // Force v2 rewrite (`stampedIds: true`) with an initial refusal + crash before retirement, then resume.
      let refusedOnce = false
      h.setFetch(async tags => {
        if (!refusedOnce && tags.some(t => t.startsWith('B'))) {
          refusedOnce = true
          return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad' }] }] }) }
        }
        return null
      })
      let failDoneOnce = true
      h.setWrite(async p => {
        if (failDoneOnce && p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && p.includes('d-')) {
          failDoneOnce = false
          throw new Error('crash before markDone')
        }
      })
      await h.newCurrent(`cur-pend-v2-1`).backfill(['spill'], 'auto')
      h.setWrite(null)
      h.setFetch(null)
      await h.newCurrent(`cur-pend-v2-2`).backfill(['spill'], 'auto')
    } else {
      await h.newCurrent(`cur-pend-v1-1`).backfill(['spill'], 'auto')
    }
    expect(h.files.get(pPath)).toBe('')
    expect(h.files.get(cPath)).toBe(cNew)

    // Lease expires -> 0.1.3 force must see cName carried in P's retired done mark and deliver U!
    h.advance(15 * 60 * 1000)
    await h.newDist(`dist-pend-1-${mode}`).backfill(['spill', 'retry', 'rejected'], 'force')
    await h.newCurrent(`cur-pend-3-${mode}`).backfill(['spill', 'retry', 'rejected'], 'auto')
    expect(h.count('U')).toBe(1)
    expect(h.files.get(cPath)).toBe('')
  }
})

test('interop: missing recorded backup file in intact checkpoint is treated as uncovered and recovers cleanly', async () => {
  const idA = '5'.repeat(32)
  const idB = '6'.repeat(32)
  const idC = '7'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'spill-20261013T200102001Z-missing-bk-p.jsonl'
  const cName = 'retry-20261013T200102002Z-missing-bk-c.jsonl'
  const missingBkName = 'retry-20261013T200102003Z-missing-bk-gone.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const cPath = `${PART_DIR}/${cName}`
  const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
  const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')], pName)[0]!
  const missingBkText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', idC, 'C')])[0]!
  const pSize = utf8Length(pText)
  const cSize = utf8Length(cText)
  const bSize = utf8Length(missingBkText)

  h.files.set(pPath, pText)
  h.files.set(cPath, cText)
  // Note: missingBkName is intentionally ABSENT from h.files (readFile throws ENOENT, exists returns false).
  h.store.set(`replayed:v2:${TABLE}/${pName}`, {
    fates: 'LL',
    reasons: [],
    refusals: [],
    size: pSize,
    sourceFnv1a: fnv1a(pText),
    waits: true,
    seq: 4,
    files: [],
    written: [],
    pendingReconcile: [{ name: cName, size: cSize, fnv1a: fnv1a(cText) }],
    backupFiles: [
      {
        name: missingBkName,
        size: bSize,
        fnv1a: fnv1a(missingBkText),
        replaces: cName,
        replacesSize: cSize,
        replacesFnv1a: fnv1a(cText),
      },
    ],
    accountedKeys: [idA, idB],
    attempt: 'prev-a1',
    attempts: ['prev-a1'],
  })

  // Auto recovery must treat the absent backup as uncovered, stage a fresh backup, reconcile C, and deliver C once.
  await h.newCurrent('cur-missing-bk-1').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-missing-bk-2').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C')).toBe(1)
  expect(h.count('A')).toBe(0)
  expect(h.count('B')).toBe(0)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: v2 rewrite with preserved same-size v1 child-protecting record enables v1 shadow so 0.1.3 force never drops new leftovers', async () => {
  const idA = 'a'.repeat(32)
  const idC1 = 'c'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'spill-20261013T200102001Z-v1-shadow-p.jsonl'
  const c1Name = 'retry-20261013T200102002Z-v1-shadow-c1.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const c1Path = `${PART_DIR}/${c1Name}`

  const pGen1Text = packFiles([dlLine('spill', idA, 'G1A'), dlLine('spill', null, 'G1B')])[0]!
  const pGen2Text = packFiles([dlLine('spill', idA, 'G2A'), dlLine('spill', null, 'G2B')])[0]!
  expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
  const pSize = utf8Length(pGen2Text)
  const c1Text = packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!

  h.files.set(pPath, pGen2Text)
  h.files.set(c1Path, c1Text)
  // Prior generation left an unretired v1 store record protecting live child c1 at the exact same size.
  h.store.set(`replayed:${TABLE}/${pName}`, {
    fates: 'LW',
    reasons: ['', 'retry'],
    refusals: ['', ''],
    size: pSize,
    sourceFnv1a: fnv1a(pGen1Text),
    waits: true,
    seq: 2,
    files: [c1Name],
    written: [true],
  })
  // Keep c1 leased during 0.1.4's auto pass so clearOppositeSlot must preserve v1.
  h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })

  // Row 0 of P_gen2 lands; ID-less row 1 gets 503 -> stampedIds: true -> useV2: true.
  let fetchCalls = 0
  h.setFetch(async () => {
    fetchCalls += 1
    if (fetchCalls === 2) return { status: 503, text: 'upstream unavailable' }
    return null
  })
  // Fail markDone for P_gen2 so P_gen2 stays live and v1 stays unretired when 0.1.3 force runs.
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) {
      throw new Error('EIO: markDone failed')
    }
  })

  await h.newCurrent('cur-v1shadow-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'auto')
  expect(h.count('G2A')).toBe(1)
  expect(h.count('G2B')).toBe(0)

  // Release c1's lease and clear fault hooks; 0.1.3 force must deliver BOTH c1 and G2B (standalone leftover, never dropped by parentFate).
  h.setWrite(null)
  h.setFetch(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-v1shadow-1').backfill(['spill', 'retry'], 'force')
  await h.newCurrent('cur-v1shadow-2').backfill(['spill', 'retry'], 'auto')

  expect(h.count('C1')).toBe(1)
  expect(h.count('G2B')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: parent-linked backup does NOT auto-deliver if 0.1.3 emptied legacy child via safeWithoutDoneMark leaving v1 store record', async () => {
  const idA = '8'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'spill-20261013T200102001Z-nodone-p.jsonl'
  const cName = 'retry-20261013T200102002Z-nodone-c.jsonl'
  const bkName = 'retry-20261013T200102003Z-nodone-bk.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const cPath = `${PART_DIR}/${cName}`
  const bkPath = `${PART_DIR}/${bkName}`

  const pText = packFiles([dlLine('spill', idA, 'A')])[0]!
  const cText = packFiles([dlLine('retry', null, 'C')], pName)[0]!
  const bkText = packFiles([dlLine('retry', `dl:${cName}#0`, 'C')], cName)[0]!
  const pSize = utf8Length(pText)
  const cSize = utf8Length(cText)
  const bkSize = utf8Length(bkText)

  // Simulate P already retired without naming C, C already emptied by 0.1.3 via safeWithoutDoneMark
  // (v1 store record present, zero done marks on disk), and linked backup bk still on disk.
  h.files.set(pPath, '')
  h.files.set(cPath, '')
  h.files.set(bkPath, bkText)
  h.files.set(
    `${PART_DIR}/outcome-done-p1d-${pName}`,
    sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
  )
  h.files.set(
    `${PART_DIR}/outcome-done-p1g-${bkName}`,
    sealFile(
      `${JSON.stringify({
        size: bkSize,
        sending: true,
        by: 'p1g',
        stagedFor: pName,
        stagedForSize: pSize,
        replaces: cName,
        backup: true,
        backupForSize: cSize,
        backupForFnv1a: fnv1a(cText),
        backupFnv1a: fnv1a(bkText),
        attempt: 'a-nodone',
      })}\n`,
    ),
  )
  h.store.set(`replayed:${TABLE}/${cName}`, {
    fates: 'L',
    reasons: [],
    refusals: [],
    size: cSize,
    waits: false,
    seq: 2,
    files: [],
    written: [],
  })

  // 0.1.4 auto must hold bk in wait rather than auto-delivering and duplicating C.
  const r = await h.newCurrent('cur-nodone-1').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C')).toBe(0)
  expect(h.files.get(bkPath)).toBe(bkText)
  expect(r.held ?? '').toContain(bkName)
})
