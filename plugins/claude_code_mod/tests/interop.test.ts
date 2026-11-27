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
  let listDirHook: ((dir: string) => Promise<void>) | null = null

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
      if (listDirHook !== null) await listDirHook(dir)
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
    setListDir: (fn: typeof listDirHook) => {
      listDirHook = fn
    },
  }
}

type Order = 'parent-first' | 'child-first'
const ORDERS: readonly Order[] = ['parent-first', 'child-first']

const pairNames = (order: Order, tag: string): { pName: string; cName: string } =>
  order === 'parent-first'
    ? { pName: `retry-20200101T000000001Z-${tag}-parent.jsonl`, cName: `spill-20200101T000000002Z-${tag}-child.jsonl` }
    : { pName: `spill-20200101T000000001Z-${tag}-parent.jsonl`, cName: `retry-20200101T000000002Z-${tag}-child.jsonl` }

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
  const pName = 'retry-20200101T000000001Z-interop-unmask-p.jsonl'
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

      const sName = `spill-20200101T000000999Z-independent-${order}.jsonl`
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
  const pName = 'spill-20200101T000000001Z-same-len-gen-p.jsonl'
  const cName = 'retry-20200101T000000002Z-same-len-gen-c.jsonl'
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
    const pName = 'spill-20200101T000000001Z-idless-2part-cur.jsonl'
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
    const pName = 'spill-20200101T000000002Z-idless-2part-dist.jsonl'
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
  // Name P as `retry-` and C as `spill-` so P sorts before C and the lease-lookup
  // hook fires inside `stageChildBackups` (after `placeChildGuards`), not in C's scan loop.
  const pName = 'retry-20200101T000000001Z-lease-swap-p.jsonl'
  const cName = 'spill-20200101T000000002Z-lease-swap-c.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const cPath = `${PART_DIR}/${cName}`
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const cOld = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', idC, 'C')], pName)[0]!
  const cNew = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', idD, 'D')], pName)[0]!
  expect(utf8Length(cNew)).toBe(utf8Length(cOld))
  const pSize = utf8Length(pText)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`

  // Arm 1: Owner swaps C -> equal-length cNew (row D) and drops lease during stageChildBackups storeGet('lease:...').
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
      if (
        k === childLeaseKey &&
        !swapped &&
        [...h.files.keys()].some(f => f.includes('/outcome-done-') && f.endsWith(`-${cName}`))
      ) {
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
    const sName = 'spill-20200101T000000099Z-lease-swap-independent.jsonl'
    const sPath = `${PART_DIR}/${sName}`
    h.files.set(sPath, packFiles([dlLine('spill', '9'.repeat(32), 'S')])[0]!)
    let swappedOnBackupWrite = false
    h.setWrite(async p => {
      if (!swappedOnBackupWrite && p.includes('/spill-') && !p.includes('/outcome') && p !== cPath && p !== sPath) {
        swappedOnBackupWrite = true
        h.files.set(cPath, cNew)
      }
    })
    const rAborted = await h.newCurrent('cur-midwrite-1').backfill(['spill', 'retry'], 'force')
    expect(swappedOnBackupWrite).toBe(true)
    expect(rAborted.error).toBeNull()
    expect(rAborted.held ?? '').toContain(cName)
    expect(h.count('S')).toBe(1)
    expect(h.files.get(sPath)).toBe('')
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
    const pName = `spill-20200101T000000001Z-pend-ret-${mode}.jsonl`
    const cName = `retry-20200101T000000002Z-pend-ret-${mode}.jsonl`
    const bName = `retry-20200101T000000003Z-pend-ret-bk-${mode}.jsonl`
    const pPath = `${PART_DIR}/${pName}`
    const cPath = `${PART_DIR}/${cName}`
    const bPath = `${PART_DIR}/${bName}`
    const padBig = mode === 'v2-rewrite' ? 'p'.repeat(460_000) : ''
    const pGen1 =
      mode === 'v2-rewrite'
        ? packFiles([dlLine('spill', idA, `A0-${padBig}`), dlLine('spill', idB, `A1-${padBig}`)])[0]!
        : packFiles([dlLine('spill', idA, 'A')])[0]!
    const pGen2 =
      mode === 'v2-rewrite'
        ? packFiles([dlLine('spill', null, `B0-${padBig}`), dlLine('spill', null, `B1-${padBig}`)])[0]!
        : packFiles([dlLine('spill', idB, 'B')])[0]!
    const pGen2Sized =
      mode === 'v2-rewrite'
        ? packFiles([
            dlLine('spill', null, `B0-${padBig}`),
            dlLine('spill', null, `B1-${padBig}${'x'.repeat(utf8Length(pGen1) - utf8Length(pGen2))}`),
          ])[0]!
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
      // Force multi-part stamped v2 rewrite (`stampedIds: true`, 2 leftover parts > 1)
      // with an initial refusal + crash before retirement, then resume.
      let refusedOnce = false
      h.setFetch(async tags => {
        if (!refusedOnce && tags.some(t => t.startsWith('B0') || t.startsWith('B1'))) {
          refusedOnce = true
          return {
            status: 200,
            text: JSON.stringify({
              insertErrors: tags.map((_, index) => ({ index, errors: [{ reason: 'invalid', message: 'bad' }] })),
            }),
          }
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
  const pName = 'spill-20200101T000000001Z-missing-bk-p.jsonl'
  const cName = 'retry-20200101T000000002Z-missing-bk-c.jsonl'
  const missingBkName = 'retry-20200101T000000003Z-missing-bk-gone.jsonl'
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

test('interop: v1 shadow on stuck v1 & defer-before-dispatch on protected v1 prevent 0.1.3 from dropping new leftovers (pre-clock 2020 fixtures)', async () => {
  const idA = 'a'.repeat(32)
  const idC1 = 'c'.repeat(32)
  const padBig = 'z'.repeat(460_000)

  // Arm 1: Protected same-size v1 record + failure on first leftover write (writer.ts:7139):
  // 0.1.4 defers P_gen2 behind unleased live c1, delivers & empties c1, clears v1 cleanly,
  // so a leftover write failure never lets 0.1.3 retire P_gen2 unsent.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1-defer-p.jsonl'
    const c1Name = 'retry-20200101T000000002Z-v1-defer-c1.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    const c1Path = `${PART_DIR}/${c1Name}`

    const pGen1Text = packFiles([dlLine('spill', idA, `G1A-${padBig}`), dlLine('spill', null, `G1B-${padBig}`)])[0]!
    const pGen2Text = packFiles([dlLine('spill', idA, `G2A-${padBig}`), dlLine('spill', null, `G2B-${padBig}`)])[0]!
    expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
    const pSize = utf8Length(pGen2Text)
    const c1Text = packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!

    h.files.set(pPath, pGen2Text)
    h.files.set(c1Path, c1Text)
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
    // Row 0 of P_gen2 lands; row 1 gets 503; first leftover write fails.
    h.setFetch(async tags => {
      if (tags.some(t => t.startsWith('G2B'))) return { status: 503, text: 'upstream unavailable' }
      return null
    })
    let failedLeftoverWrite = false
    h.setWrite(async p => {
      if (p.includes('/spill-') && !p.includes('/outcome') && p !== pPath) {
        failedLeftoverWrite = true
        throw new Error('EIO: leftover write failed')
      }
    })

    await h.newCurrent('cur-v1defer-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'auto')
    expect(failedLeftoverWrite).toBe(true)
    expect(h.count('C1')).toBe(1)
    expect(h.files.get(c1Path)).toBe('')
    // v1 was cleared after c1 finished, so 0.1.3 auto MUST NOT retire P_gen2 unsent!
    h.setWrite(null)
    h.setFetch(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-v1defer-1').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-v1defer-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-v1defer-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count(`G2B-${padBig}`)).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2: Protected same-size v1 record with leased c1 replays P_gen2 immediately in v2
  // (carrying c1 into P's retired done mark) so a subsequent 0.1.3 auto scan cannot retire
  // P_gen2 unsent while c1 is still leased; after lease expiry, c1 delivers cleanly.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1-hold-p.jsonl'
    const c1Name = 'retry-20200101T000000002Z-v1-hold-c1.jsonl'
    const sName = 'spill-20200101T000000099Z-v1-hold-s.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    const c1Path = `${PART_DIR}/${c1Name}`
    const sPath = `${PART_DIR}/${sName}`
    const pGen1Text = packFiles([dlLine('spill', idA, 'G1A'), dlLine('spill', null, 'G1B')])[0]!
    const pGen2Text = packFiles([dlLine('spill', idA, 'G2A'), dlLine('spill', null, 'G2B')])[0]!
    expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
    const pSize = utf8Length(pGen2Text)
    h.files.set(pPath, pGen2Text)
    h.files.set(c1Path, packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!)
    h.files.set(sPath, packFiles([dlLine('spill', '9'.repeat(32), 'S')])[0]!)
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
    h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })

    const r = await h.newCurrent('cur-v1hold-1').backfill(['spill', 'retry'], 'auto')
    expect(r.error).toBeNull()
    expect(h.count('G2A')).toBe(1)
    expect(h.count('G2B')).toBe(1)
    expect(h.count('S')).toBe(1)
    expect(h.files.get(pPath)).toBe('')
    expect(h.files.get(sPath)).toBe('')
    // A 0.1.3 auto scan while c1 is still leased cannot lose G2B or drop c1.
    await h.newDist('dist-v1hold-legacy').backfill(['spill', 'retry'], 'auto')

    h.advance(15 * 60 * 1000)
    await h.newCurrent('cur-v1hold-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C1')).toBe(1)
    expect(h.count('G2A')).toBe(1)
    expect(h.count('G2B')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 3: Multi-part stamped v2 rewrite (where stamped vs unstamped part boundaries differ,
  // forcing v2) with stuck v1 store (delete & neutralize both fail) and pre-clock 2020 fixture
  // names so newly written 2023 leftovers sort AFTER P in 0.1.3 force: v1ShadowNeeded emits
  // leftovers standalone (`parent: null`), so 0.1.3 force delivers all rows.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1-shadow-p.jsonl'
    const c1Name = 'retry-20200101T000000002Z-v1-shadow-c1.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    const c1Path = `${PART_DIR}/${c1Name}`
    // 3,900 ID-less rows (~2.85 MiB unstamped = 1 part, ~3.35 MiB stamped = 2 parts ->
    // layoutsDiffer === true -> forces v2 rewrite!).
    const padV2 = 'x'.repeat(280)
    const pGen1Text = sealFile(
      `${[
        dlLine('spill', idA, 'G1A'),
        ...Array.from({ length: 3900 }, (_, i) => dlLine('spill', null, `G1-${String(i).padStart(4, '0')}-${padV2}`)),
      ].join('\n')}\n`,
    )
    const pGen2Text = sealFile(
      `${[
        dlLine('spill', idA, 'G2A'),
        ...Array.from({ length: 3900 }, (_, i) => dlLine('spill', null, `G2-${String(i).padStart(4, '0')}-${padV2}`)),
      ].join('\n')}\n`,
    )
    expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
    const pSize = utf8Length(pGen2Text)
    const v1Key = `replayed:${TABLE}/${pName}`
    h.files.set(pPath, pGen2Text)
    h.files.set(c1Path, '')
    // Stuck different-digest v1 store record (child c1 already emptied; storeDelete & storeSet on v1 both fail).
    h.store.set(v1Key, {
      fates: 'L' + 'W'.repeat(3900),
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1Text),
      waits: true,
      seq: 2,
      files: [c1Name],
      written: [true],
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 delete blocked')
    })
    h.setStoreSet(async k => {
      if (k === v1Key) throw new Error('v1 neutralize blocked')
    })
    // Row 0 lands; remaining 3,900 ID-less rows get 503 -> stamped=2 parts vs unstamped=1 part -> useV2 = true!
    h.setFetch(async tags => {
      if (tags.some(t => t.startsWith('G2-'))) return { status: 503, text: 'upstream unavailable' }
      return null
    })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) {
        throw new Error('EIO: markDone failed')
      }
    })

    await h.newCurrent('cur-v1shadow-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'auto')
    expect(h.count('G2A')).toBe(1)
    // Confirm multi-part v2 checkpoint was written and new leftovers are standalone (no parent header).
    const v2Saved = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { files?: string[] } | undefined
    expect(v2Saved?.files?.length).toBe(2)
    for (const lf of v2Saved?.files ?? []) {
      const text = h.files.get(`${PART_DIR}/${lf}`) ?? ''
      expect(text.length > 0).toBe(true)
      expect(text.includes(`"parent":"${pName}"`)).toBe(false)
    }

    h.setWrite(null)
    h.setFetch(null)
    h.setStoreDelete(null)
    h.setStoreSet(null)
    h.advance(15 * 60 * 1000)
    // 2020 P sorts strictly before 2023 leftovers; 0.1.3 force retires P via stuck v1 first,
    // and standalone leftovers survive parentFate and deliver all 3,900 G2-* rows!
    await h.newDist('dist-v1shadow-1', { maxBatchRows: 5000 }).backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-v1shadow-2', { maxBatchRows: 5000 }).backfill(['spill', 'retry'], 'auto')
    expect(new Set(h.sentTags.filter(t => t.startsWith('G2-'))).size).toBe(3900)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 4: Retired v1 companion file (`outcome-<parent>`, retired: true, earlier sourceFnv1a)
  // protecting live c1 + single-part stamped P_gen2:
  // - When c1 is unleased: backfillRun defers P_gen2 behind c1, clears `outcome-<parent>` before
  //   dispatching P_gen2, so even leftover-write EIO + 0.1.3 auto preserves G2B.
  // - When c1 is leased: backfillRun forces v2 so P_gen2 writes G2B standalone (`parent: null`),
  //   carries c1 in P's retired mark, and 0.1.3 auto delivers G2B without dropping c1.
  for (const leaseChild of [false, true] as const) {
    const h = makeInteropHarness()
    const pName = `spill-20200101T000000001Z-retv1-${leaseChild ? 'leased' : 'free'}-p.jsonl`
    const c1Name = `retry-20200101T000000002Z-retv1-${leaseChild ? 'leased' : 'free'}-c1.jsonl`
    const sName = `spill-20200101T000000099Z-retv1-${leaseChild ? 'leased' : 'free'}-s.jsonl`
    const pPath = `${PART_DIR}/${pName}`
    const c1Path = `${PART_DIR}/${c1Name}`
    const sPath = `${PART_DIR}/${sName}`
    const pGen1Text = packFiles([dlLine('spill', idA, 'G1A'), dlLine('spill', null, 'G1B')])[0]!
    const pGen2Text = packFiles([dlLine('spill', idA, 'G2A'), dlLine('spill', null, 'G2B')])[0]!
    expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
    const pSize = utf8Length(pGen2Text)
    h.files.set(pPath, pGen2Text)
    h.files.set(c1Path, packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!)
    h.files.set(sPath, packFiles([dlLine('spill', '9'.repeat(32), 'S')])[0]!)
    h.files.set(
      `${PART_DIR}/outcome-${pName}`,
      sealFile(
        `${JSON.stringify({
          fates: 'LW',
          reasons: ['', 'retry'],
          refusals: ['', ''],
          size: pSize,
          sourceFnv1a: fnv1a(pGen1Text),
          waits: true,
          seq: 2,
          files: [c1Name],
          written: [true],
          retired: true,
          retiredAt: h.io.now(),
        })}\n`,
      ),
    )
    if (leaseChild) {
      h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    }
    h.setFetch(async tags => {
      if (tags.includes('G2B')) return { status: 503, text: 'upstream unavailable' }
      return null
    })
    if (!leaseChild) {
      h.setWrite(async p => {
        if (p.includes('/spill-') && !p.includes('/outcome') && p !== pPath && p !== sPath) {
          throw new Error('EIO: leftover write failed')
        }
      })
    }
    await h.newCurrent(`cur-retv1-${leaseChild ? 'l' : 'f'}-1`, { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'auto')
    if (leaseChild) {
      expect(h.count('G2A')).toBe(1)
      expect(h.count('G2B')).toBe(0)
      expect(h.files.get(pPath)).toBe('')
      // Standalone leftover for G2B is live without parent header, and c1 stays intact.
      const standaloneLeftovers = [...h.files.entries()].filter(
        ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && k !== pPath && k !== sPath && v !== '',
      )
      expect(standaloneLeftovers.length).toBe(1)
      expect(standaloneLeftovers[0]![1].includes(`"parent":"${pName}"`)).toBe(false)
    } else {
      expect(h.count('C1')).toBe(1)
      expect(h.files.get(c1Path)).toBe('')
      expect(h.files.get(`${PART_DIR}/outcome-${pName}`)).toBe('')
    }

    h.setWrite(null)
    h.setFetch(null)
    // Run 0.1.3 auto BEFORE lease expiry in the leased arm (and right after EIO in the free arm).
    await h.newDist(`dist-retv1-${leaseChild ? 'l' : 'f'}-1`).backfill(['spill', 'retry'], 'auto')
    h.advance(15 * 60 * 1000)
    await h.newCurrent(`cur-retv1-${leaseChild ? 'l' : 'f'}-2`).backfill(['spill', 'retry'], 'auto')
    await h.newCurrent(`cur-retv1-${leaseChild ? 'l' : 'f'}-3`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('C1')).toBe(1)
    expect(h.count('S')).toBe(1)
    expect(h.count('G2A')).toBe(1)
    expect(h.count('G2B')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 5: Earlier-digest v1 record protecting a live `rejected-*` child during `auto`
  // replays P_gen2 immediately in v2 (carrying `rejName` into P's retired mark so 0.1.3
  // never drops `rejName`), and explicit `/bqaa-backfill` later drains `rejName`.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1-rej-p.jsonl'
    const rejName = 'rejected-20200101T000000002Z-v1-rej-c.jsonl'
    const sName = 'spill-20200101T000000099Z-v1-rej-s.jsonl'
    const pGen1Text = packFiles([dlLine('spill', idA, 'G1A'), dlLine('spill', null, 'G1B')])[0]!
    const pGen2Text = packFiles([dlLine('spill', idA, 'G2A'), dlLine('spill', null, 'G2B')])[0]!
    expect(utf8Length(pGen2Text)).toBe(utf8Length(pGen1Text))
    const pSize = utf8Length(pGen2Text)
    h.files.set(`${PART_DIR}/${pName}`, pGen2Text)
    h.files.set(
      `${PART_DIR}/${rejName}`,
      packFiles([JSON.stringify({ v: 1, kind: 'rejected', reason: 'invalid', attempts: 1, failed_at: '2020-01-01T00:00:00.002Z', table: TABLE, row: rowOf(idC1, 'REJ1') })], pName)[0]!,
    )
    h.files.set(`${PART_DIR}/${sName}`, packFiles([dlLine('spill', '9'.repeat(32), 'S')])[0]!)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'LR',
      reasons: ['invalid'],
      refusals: [0],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1Text),
      waits: false,
      seq: 2,
      files: [rejName],
      written: [true],
    })
    const rAuto = await h.newCurrent('cur-v1rej-auto').backfill(['spill', 'retry'], 'auto')
    expect(rAuto.error).toBeNull()
    expect(h.count('S')).toBe(1)
    expect(h.count('G2A')).toBe(1)
    expect(h.count('G2B')).toBe(1)
    expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
    // 0.1.3 auto/force on spill+retry must not drop the carried rejected child.
    await h.newDist('dist-v1rej').backfill(['spill', 'retry'], 'force')
    expect((h.files.get(`${PART_DIR}/${rejName}`) ?? '').length > 0).toBe(true)

    const rCmd = await h.newCurrent('cur-v1rej-cmd').backfill(['spill', 'retry', 'rejected'], 'command')
    expect(rCmd.error).toBeNull()
    expect(h.count('REJ1')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: parent-linked backup does NOT auto-deliver if 0.1.3 emptied legacy child via safeWithoutDoneMark leaving v1 store record', async () => {
  const idA = '8'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'spill-20200101T000000001Z-nodone-p.jsonl'
  const cName = 'retry-20200101T000000002Z-nodone-c.jsonl'
  const bkName = 'retry-20200101T000000003Z-nodone-bk.jsonl'
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

test('interop: ID-less child row survives stop-before-retired-mark -> 0.1.3 force -> current auto', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const runAutoBeforeDist of [false, true] as const) {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-linked-loss-parent.jsonl'
    const cName = 'spill-20200101T000000002Z-linked-loss-child.jsonl'
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', null, 'C')], pName)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before the retired mark')
    })
    await h.newCurrent('cur-linked-loss-1').backfill(['spill', 'retry'], 'force')
    expect(h.count('C')).toBe(0)
    if (runAutoBeforeDist) {
      // Even if a current auto scan visits C and B_bk (`['spill']`) while P (`retry`)
      // is still live and unretired, B_bk must NOT be dropped while R_c carries parent: P.
      await h.newCurrent('cur-linked-loss-mid').backfill(['spill'], 'auto')
      expect(h.count('C')).toBe(0)
      expect(h.liveDataFiles().some(n => n.includes('/spill-') && !n.endsWith(`/${cName}`))).toBe(true)
    }
    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-linked-loss-1').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-linked-loss-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-linked-loss-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: refreshed companion check after child lease release refuses parent-linking, and size-0 emptied sentinel never drops linked backup', async () => {
  // Arm 1: A different-size retirement mark for C is published right as C's prior lease is
  // released during stageChildBackups storeGet('lease:...'). Name P as `retry-` and C as `spill-`
  // so P sorts first and the hook fires inside stageChildBackups (after placeChildGuards);
  // stageChildBackups must acquire C's lease, refresh companions post-lease, refuse parent: C
  // linking (emit standalone backup), and deliver C via 0.1.3 force.
  {
    const idA = 'a'.repeat(32)
    const idB = 'b'.repeat(32)
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-fresh-comp-p.jsonl'
    const cName = 'spill-20200101T000000002Z-fresh-comp-c.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const cText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C')], pName)[0]!
    const pSize = utf8Length(pText)
    const cSize = utf8Length(cText)
    const v1Key = `replayed:${TABLE}/${pName}`
    const v2Key = `replayed:v2:${TABLE}/${pName}`
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    const childLeaseKey = `lease:${TABLE}/${cName}`
    h.store.set(childLeaseKey, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    let releasedWithMark = false
    h.setStoreGet(async k => {
      if (
        k === childLeaseKey &&
        !releasedWithMark &&
        [...h.files.keys()].some(f => f.includes('/outcome-done-') && f.endsWith(`-${cName}`))
      ) {
        releasedWithMark = true
        // Publish a different-size retirement mark for C right as the foreign lease releases inside stageChildBackups.
        h.files.set(
          `${PART_DIR}/outcome-done-old9-${cName}`,
          sealFile(
            `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`,
          ),
        )
        h.store.delete(childLeaseKey)
      }
    })
    h.setStoreSet(async k => {
      if (k === v1Key || k === v2Key) throw new Error('crash before record')
    })
    h.setStoreDelete(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    let firstBackupWriteText: string | null = null
    let backupDataWrites = 0
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && p !== `${PART_DIR}/${cName}` && text !== '') {
        backupDataWrites += 1
        if (firstBackupWriteText === null) firstBackupWriteText = text
      }
      if (p.includes('/outcome2-')) throw new Error('crash before record file')
    })
    await h.newCurrent('cur-fresh-comp-1').backfill(['spill', 'retry'], 'force')
    expect(releasedWithMark).toBe(true)
    expect(backupDataWrites).toBe(1)
    expect(firstBackupWriteText).not.toBeNull()
    expect(firstBackupWriteText!.includes(`"parent":"${cName}"`)).toBe(false)
    const stagedBackups = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${cName}`) && v !== '',
    )
    expect(stagedBackups.length).toBe(1)
    expect(stagedBackups[0]![1].includes(`"parent":"${cName}"`)).toBe(false)

    h.setStoreGet(null)
    h.setStoreSet(null)
    h.setStoreDelete(null)
    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-fresh-comp-1').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-fresh-comp-2').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C') >= 1).toBe(true)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2: A size-0 emptied sentinel (`outcome-done-<mark>d-C`, size: 0, fates: '') beside empty C
  // must NOT be treated by 0.1.4 parentFate or stagedGuardState as proof C's rows were sent.
  {
    const idA = 'a'.repeat(32)
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-sent-p.jsonl'
    const cName = 'retry-20200101T000000002Z-sent-c.jsonl'
    const bkName = 'retry-20200101T000000003Z-sent-bk.jsonl'
    const pText = packFiles([dlLine('spill', idA, 'A')])[0]!
    const cText = packFiles([dlLine('retry', null, 'C')], pName)[0]!
    const bkText = packFiles([dlLine('retry', `dl:${cName}#0`, 'C')], cName)[0]!
    const pSize = utf8Length(pText)
    const cSize = utf8Length(cText)
    const bkSize = utf8Length(bkText)
    h.files.set(`${PART_DIR}/${pName}`, '')
    h.files.set(`${PART_DIR}/${cName}`, '')
    h.files.set(`${PART_DIR}/${bkName}`, bkText)
    h.files.set(
      `${PART_DIR}/outcome-done-p1d-${pName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-c1d-${cName}`,
      sealFile(`${JSON.stringify({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
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
          attempt: 'a-sent',
        })}\n`,
      ),
    )

    await h.newCurrent('cur-sent-1').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2b: Size-0 emptied sentinel on empty parent C must NOT cause parentFate (with no
  // backup guard bypass) to drop a live child carrying `parent: C`.
  {
    const h = makeInteropHarness()
    const cName = 'retry-20200101T000000002Z-sent2b-c.jsonl'
    const childName = 'retry-20200101T000000003Z-sent2b-ch.jsonl'
    h.files.set(`${PART_DIR}/${cName}`, '')
    h.files.set(`${PART_DIR}/${childName}`, packFiles([dlLine('retry', null, 'C_SENT2B')], cName)[0]!)
    h.files.set(
      `${PART_DIR}/outcome-done-c1d-${cName}`,
      sealFile(`${JSON.stringify({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    await h.newCurrent('cur-sent-2b').backfill(['retry'], 'auto')
    expect(h.count('C_SENT2B')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2c: Size-0 emptied sentinel beside an empty adopted replacement must NOT satisfy
  // `unretiredV2ReplacementsAccounted` and drop the last backup of C.
  {
    const idA = 'a'.repeat(32)
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-sent2c-p.jsonl'
    const cName = 'retry-20200101T000000002Z-sent2c-c.jsonl'
    const rName = 'retry-20200101T000000003Z-sent2c-r.jsonl'
    const bkName = 'retry-20200101T000000004Z-sent2c-bk.jsonl'
    const pText = packFiles([dlLine('spill', idA, 'A')])[0]!
    const cText = packFiles([dlLine('retry', null, 'C_SENT2C')], pName)[0]!
    const bkText = packFiles([dlLine('retry', `dl:${cName}#0`, 'C_SENT2C')], cName)[0]!
    const pSize = utf8Length(pText)
    const cSize = utf8Length(cText)
    const bkSize = utf8Length(bkText)
    h.files.set(`${PART_DIR}/${pName}`, '')
    h.files.set(`${PART_DIR}/${cName}`, '')
    h.files.set(`${PART_DIR}/${rName}`, '')
    h.files.set(`${PART_DIR}/${bkName}`, bkText)
    // Unretired v2 record listing adopted replacement rName (no deliverableRefs, simulating
    // a pre-deliverableRefs checkpoint); rName has ONLY a size-0 emptied sentinel.
    h.files.set(
      `${PART_DIR}/outcome-done-p1d-${pName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE}/${pName}`, {
      fates: 'L',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [rName],
      written: [true],
      adopted: [rName],
      reconciled: [{ name: cName, size: cSize, fnv1a: fnv1a(cText) }],
      attempt: 'a-sent2c',
    })
    h.files.set(
      `${PART_DIR}/outcome-done-r1d-${rName}`,
      sealFile(`${JSON.stringify({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
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
          attempt: 'a-sent2c',
        })}\n`,
      ),
    )
    await h.newCurrent('cur-sent-2c').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C_SENT2C')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: pre-flag main v1 multi-part stamped checkpoint resumes without row loss, and mismatched written part holds safely', async () => {
  // 3,900 ID-less rows (~3.2 MiB source < 4 MiB HOST_FILE_BYTES) pack into 2 parts
  // under BOTH 0.1.3 unstamped and pre-flag main stamped layouts, with DIFFERENT
  // split boundaries (~400-row gap if resumed under the wrong layout).
  const pad = 'x'.repeat(400)
  const pName = 'spill-20200101T000000001Z-preflag-main.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const bigRows = Array.from({ length: 3900 }, (_, i) =>
    dlLine('spill', null, `R${String(i).padStart(4, '0')}-${pad}`),
  )
  const pText = sealFile(`${bigRows.join('\n')}\n`)
  const pSize = utf8Length(pText)
  // Build exact 0.1.3 unstamped and pre-flag main stamped parts (note key follows row).
  const unstampedLines = Array.from({ length: 3900 }, (_, i) =>
    dlLine('spill', null, `R${String(i).padStart(4, '0')}-${pad}`, `${pName}#${i}`),
  )
  const stampedLines = Array.from({ length: 3900 }, (_, i) =>
    dlLine('spill', `dl:${pName}#${i}`, `R${String(i).padStart(4, '0')}-${pad}`, `${pName}#${i}`),
  )
  const unstampedParts = packFiles(unstampedLines, pName)
  const stampedParts = packFiles(stampedLines, pName)
  expect(unstampedParts.length).toBe(2)
  expect(stampedParts.length).toBe(2)
  const unstampedPart0Count = unstampedParts[0]!.trim().split('\n').length - 1
  const stampedPart0Count = stampedParts[0]!.trim().split('\n').length - 1
  expect(stampedPart0Count < unstampedPart0Count).toBe(true)

  // Arm 1: Pre-flag main v1 checkpoint (`sourceFnv1a` set, NO `stampedIds` flag), part 0 live on disk,
  // AND Arm 2: part 0 already emptied on disk -> both deliver all 3,900 distinct rows without loss.
  for (const part0State of ['live', 'emptied'] as const) {
    const h = makeInteropHarness()
    const part0Name = `spill-20200101T000000002Z-preflag-p0-${part0State}.jsonl`
    const part1Name = `spill-20200101T000000003Z-preflag-p1-${part0State}.jsonl`
    h.files.set(pPath, pText)
    h.files.set(`${PART_DIR}/${part0Name}`, part0State === 'live' ? stampedParts[0]! : '')
    // Pre-flag main v1 record: has sourceFnv1a, NO stampedIds field, written: [true, false].
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'W'.repeat(3900),
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(pText),
      waits: true,
      seq: 2,
      files: [part0Name, part1Name],
      written: [true, false],
    })
    await h.newCurrent(`cur-preflag-${part0State}-1`, { maxBatchRows: 5000 }).backfill(['spill'], 'auto')
    await h.newCurrent(`cur-preflag-${part0State}-2`, { maxBatchRows: 5000 }).backfill(['spill'], 'auto')
    expect(h.files.get(pPath)).toBe('')
    if (part0State === 'live') {
      expect(new Set(h.sentTags).size).toBe(3900)
    } else {
      // Part 0 was already emptied prior to resume; part 1 must deliver the exact remaining stamped tail!
      expect(new Set(h.sentTags).size).toBe(3900 - stampedPart0Count)
    }
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 3: Already-written part 0 on disk matches NEITHER 0.1.3 unstamped NOR stamped packing ->
  // resumeRewrite fails closed ('unaccounted') and holds P safely instead of guessing and losing rows.
  {
    const h = makeInteropHarness()
    const part0Name = 'spill-20200101T000000002Z-preflag-alien-p0.jsonl'
    const part1Name = 'spill-20200101T000000003Z-preflag-alien-p1.jsonl'
    h.files.set(pPath, pText)
    h.files.set(`${PART_DIR}/${part0Name}`, packFiles([dlLine('spill', 'f'.repeat(32), 'ALIEN')], pName)[0]!)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'W'.repeat(3900),
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 2,
      files: [part0Name, part1Name],
      written: [true, false],
    })
    const r = await h.newCurrent('cur-preflag-alien').backfill(['spill'], 'auto')
    expect(h.files.get(pPath)).toBe(pText)
    expect(r.held ?? '').toContain(pName)
  }
})

test('interop: single-part stamped outcome stays in v1 so 0.1.3 force after interrupted replay never re-sends landed ID-less row', async () => {
  const h = makeInteropHarness()
  const pName = 'spill-20200101T000000001Z-single-v1-p.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const pText = packFiles([dlLine('spill', null, 'LANDED_IDLESS'), dlLine('spill', null, 'RETRY_IDLESS')])[0]!
  h.files.set(pPath, pText)

  // Row 0 lands; row 1 gets 503 (1 leftover part); crash before writing leftover file.
  let calls = 0
  h.setFetch(async () => {
    calls += 1
    if (calls === 2) return { status: 503, text: 'upstream unavailable' }
    return null
  })
  h.setWrite(async p => {
    if (p.includes('/spill-') && !p.includes('/outcome') && p !== pPath) {
      throw new Error('interrupted before leftover write')
    }
  })
  await h.newCurrent('cur-single-v1-1', { maxBatchRows: 1 }).backfill(['spill'], 'auto')
  expect(h.count('LANDED_IDLESS')).toBe(1)
  expect(h.count('RETRY_IDLESS')).toBe(0)
  // Single-part outcome must be recorded in v1 (not hidden in v2).
  expect(h.store.get(`replayed:${TABLE}/${pName}`)).toBeDefined()
  expect(h.store.get(`replayed:v2:${TABLE}/${pName}`)).toBeUndefined()

  // 0.1.3 force resumes the v1 checkpoint, writes ONLY RETRY_IDLESS, and never duplicates LANDED_IDLESS!
  h.setFetch(null)
  h.setWrite(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-single-v1-1').backfill(['spill', 'retry'], 'force')
  await h.newDist('dist-single-v1-2').backfill(['spill', 'retry'], 'force')
  expect(h.count('LANDED_IDLESS')).toBe(1)
  expect(h.count('RETRY_IDLESS')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: a legacy auto scan cannot retire the new parent while its old child is leased', async () => {
  const h = makeInteropHarness()
  const p = 'spill-20200101T000000001Z-protected-p.jsonl'
  const c = 'spill-20200101T000000002Z-protected-c.jsonl'
  const oldRow = 'a'.repeat(32)
  const landed = dlLine('spill', 'f'.repeat(32), 'already landed')
  const old = packFiles([landed, dlLine('spill', oldRow, 'old')])[0]!
  const fresh = packFiles([landed, dlLine('spill', 'b'.repeat(32), 'new')])[0]!
  expect(utf8Length(old)).toBe(utf8Length(fresh))
  h.files.set(`${PART_DIR}/${p}`, fresh)
  h.files.set(`${PART_DIR}/${c}`, packFiles([dlLine('spill', oldRow, 'old')], p)[0]!)
  h.store.set(`replayed:${TABLE}/${p}`, {
    fates: 'LW',
    reasons: [],
    refusals: [],
    size: utf8Length(old),
    sourceFnv1a: fnv1a(old),
    waits: true,
    seq: 2,
    files: [c],
    written: [true],
  })
  h.store.set(`lease:${TABLE}/${c}`, { by: 'child-owner', until: h.io.now() + 300_000 })
  const kinds = ['spill', 'retry'] as const
  await h.newCurrent('upgraded').backfill([...kinds], 'auto')
  await h.newDist('legacy').backfill([...kinds], 'auto')
  h.advance(15 * 60 * 1000)
  await h.newCurrent('after-expiry').backfill([...kinds], 'auto')
  await h.newCurrent('final').backfill([...kinds], 'force')
  expect(h.liveDataFiles()).toEqual([])
  expect(h.count('new')).toBe(1)
  expect(h.count('old')).toBe(1)
})

test('interop: repopulating a dropped replacement must preserve the backup of the preceding generation', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const deliverRepopFirst of [true, false] as const) {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-linked-loss-parent.jsonl'
    const cName = 'spill-20200101T000000002Z-linked-loss-child.jsonl'
    const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
    const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', null, 'C')], pName)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before the retired mark')
    })
    await h.newCurrent('cur-linked-loss-1').backfill(['spill', 'retry'], 'force')
    expect(h.count('C')).toBe(0)
    const journal = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { adopted: string[] }
    const replacement = `${PART_DIR}/${journal.adopted[0]}`
    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-linked-loss-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(replacement)).toBe('')
    // Repopulate the dropped replacement basename with a different file (both standalone and live/retired orders).
    h.files.set(replacement, packFiles([dlLine('retry', 'e'.repeat(32), 'new generation')])[0]!)
    if (!deliverRepopFirst) {
      // Run spill-only first so backup sees the live repopulated file at the same basename before it retires.
      await h.newCurrent('cur-linked-loss-spill').backfill(['spill'], 'auto')
    }
    await h.newCurrent('cur-linked-loss-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-linked-loss-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count('new generation')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
    expect(h.count('C')).toBe(1)
  }
})

test('interop: second resume of unstamped 0.1.3 multi-part checkpoint keeps stampedIds=false and never duplicates rows', async () => {
  const pad = 'x'.repeat(400)
  const pName = 'spill-20200101T000000001Z-double-resume-013.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const rawRows = Array.from({ length: 3900 }, (_, i) => dlLine('spill', null, `D${String(i).padStart(4, '0')}-${pad}`))
  const pText = sealFile(`${rawRows.join('\n')}\n`)
  const pSize = utf8Length(pText)
  const unstampedLines = Array.from({ length: 3900 }, (_, i) => dlLine('spill', null, `D${String(i).padStart(4, '0')}-${pad}`, `${pName}#${i}`))
  const unstampedParts = packFiles(unstampedLines, pName)
  expect(unstampedParts.length).toBe(2)

  const h = makeInteropHarness()
  const part0Name = 'spill-20200101T000000002Z-double-resume-p0.jsonl'
  const part1Name = 'spill-20200101T000000003Z-double-resume-p1.jsonl'
  h.files.set(pPath, pText)
  h.files.set(`${PART_DIR}/${part0Name}`, unstampedParts[0]!)
  // Legacy 0.1.3 v1 checkpoint: NO sourceFnv1a, NO stampedIds, written: [true, false].
  h.store.set(`replayed:${TABLE}/${pName}`, {
    fates: 'W'.repeat(3900),
    reasons: [],
    refusals: [],
    size: pSize,
    waits: true,
    seq: 2,
    files: [part0Name, part1Name],
    written: [true, false],
  })

  // Resume #1: fails when writing part 1, AFTER recording sourceFnv1a + stampedIds: false!
  h.setWrite(async p => {
    if (p.endsWith(`/${part1Name}`)) throw new Error('EIO on part 1')
  })
  await h.newCurrent('cur-dr-1', { maxBatchRows: 5000 }).backfill(['spill'], 'auto')
  const savedAfterR1 = h.store.get(`replayed:${TABLE}/${pName}`) as { sourceFnv1a?: string; stampedIds?: boolean } | undefined
  expect(savedAfterR1?.sourceFnv1a).toBe(fnv1a(pText))
  expect(savedAfterR1?.stampedIds).toBe(false)
  // Part 0 was delivered and emptied during Resume #1 (leaving only P and written: [true, false]).
  expect(h.files.get(`${PART_DIR}/${part0Name}`)).toBe('')

  // Resume #2: must honor stampedIds: false and write unstamped part 1 with ZERO duplicates!
  h.setWrite(null)
  await h.newCurrent('cur-dr-2', { maxBatchRows: 5000 }).backfill(['spill'], 'auto')
  await h.newCurrent('cur-dr-3', { maxBatchRows: 5000 }).backfill(['spill'], 'auto')
  expect(h.sentTags.length).toBe(3900)
  expect(new Set(h.sentTags).size).toBe(3900)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: earlier-digest v1 record with unwritten live child (written: [false]) never deadlocks auto or force', async () => {
  const idA = 'a'.repeat(32)
  const idC = 'c'.repeat(32)
  for (const trigger of ['auto', 'force'] as const) {
    const h = makeInteropHarness()
    const pName = `spill-20200101T000000001Z-unwritten-${trigger}-p.jsonl`
    const cName = `retry-20200101T000000002Z-unwritten-${trigger}-c.jsonl`
    const pGen1 = packFiles([dlLine('spill', idA, 'G1A'), dlLine('spill', null, 'G1B')])[0]!
    const pGen2 = packFiles([dlLine('spill', idA, 'G2A'), dlLine('spill', null, 'G2B')])[0]!
    expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
    h.files.set(`${PART_DIR}/${pName}`, pGen2)
    h.files.set(`${PART_DIR}/${cName}`, packFiles([dlLine('retry', idC, 'OLD_CHILD')], pName)[0]!)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: utf8Length(pGen1),
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [cName],
      written: [false],
    })
    await h.newCurrent(`cur-unw-${trigger}-1`).backfill(['spill', 'retry'], trigger)
    await h.newCurrent(`cur-unw-${trigger}-2`).backfill(['spill', 'retry'], trigger)
    expect(h.count('G2A')).toBe(1)
    expect(h.count('G2B')).toBe(1)
    expect(h.count('OLD_CHILD')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: live holderless same-session lease is treated as occupied during staging, and temporary staging lease blocks concurrent delivery', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  // Arm 1: Live holderless lease with OUR session id (`by: 'same-sid'`, no `holder`) must be treated as occupied.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-same-sid-p.jsonl'
    const cName = 'spill-20200101T000000002Z-same-sid-c.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const cText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C')], pName)[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    const childLeaseKey = `lease:${TABLE}/${cName}`
    const originalLease = { by: 'same-sid', until: h.io.now() + 300_000 }
    h.store.set(childLeaseKey, originalLease)

    const r = await h.newCurrent('same-sid').backfill(['retry'], 'force')
    expect(r.error).toBeNull()
    expect(r.held ?? '').toContain(cName)
    expect(h.count('A')).toBe(0)
    // Original holderless lease must remain untouched (neither overwritten nor deleted).
    expect(h.store.get(childLeaseKey)).toEqual(originalLease)
  }

  // Arm 2: Temporary child lease held by stageChildBackups blocks a concurrent session from
  // dispatching C mid-staging and is released cleanly in `finally`.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-temp-lease-p.jsonl'
    const cName = 'spill-20200101T000000002Z-temp-lease-c.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const cText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C')], pName)[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    const childLeaseKey = `lease:${TABLE}/${cName}`
    let sawHolderLease = false
    let ranConcurrent = false
    h.setWrite(async p => {
      if (!ranConcurrent && p.includes('/spill-') && !p.includes('/outcome') && !p.endsWith(`/${cName}`)) {
        ranConcurrent = true
        const curLease = h.store.get(childLeaseKey) as { by?: string; holder?: string; until?: number } | undefined
        sawHolderLease = Boolean(curLease?.by === 'cur-temp-1' && typeof curLease.holder === 'string' && curLease.holder.length > 0)
        // Both a foreign session AND a concurrent writer sharing the same session id must
        // respect the temporary staging `holder` lease and skip C without clobbering it.
        await h.newCurrent('concurrent-other').backfill(['spill'], 'auto')
        await h.newCurrent('cur-temp-1').backfill(['spill'], 'auto')
        expect(h.count('C')).toBe(0)
        expect(h.store.get(childLeaseKey)).toEqual(curLease)
      }
    })
    await h.newCurrent('cur-temp-1').backfill(['retry'], 'force')
    expect(ranConcurrent).toBe(true)
    expect(sawHolderLease).toBe(true)
    expect(h.store.get(childLeaseKey)).toBeUndefined()
  }
})

test('interop: split by kind with identical packing stays in v1 so 0.1.3 force never duplicates landed ID-less row', async () => {
  const h = makeInteropHarness()
  const pName = 'spill-20200101T000000001Z-split-kind-v1-p.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const pText = packFiles([
    dlLine('spill', null, 'LANDED_IDLESS'),
    dlLine('spill', null, 'REFUSED_IDLESS'),
    dlLine('spill', null, 'RETRY_IDLESS'),
  ])[0]!
  h.files.set(pPath, pText)

  // Row 0 lands; row 1 refused (rejected); row 2 gets 503 (retry) -> 2 parts with identical membership in both layouts.
  h.setFetch(async tags => {
    if (tags.includes('REFUSED_IDLESS')) {
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad' }] }] }) }
    }
    if (tags.includes('RETRY_IDLESS')) return { status: 503, text: 'upstream unavailable' }
    return null
  })
  h.setWrite(async p => {
    if ((p.includes('/spill-') || p.includes('/rejected-')) && !p.includes('/outcome') && p !== pPath) {
      throw new Error('interrupted before leftover write')
    }
  })
  await h.newCurrent('cur-kind-v1-1', { maxBatchRows: 1 }).backfill(['spill'], 'auto')
  expect(h.count('LANDED_IDLESS')).toBe(1)
  expect(h.store.get(`replayed:${TABLE}/${pName}`)).toBeDefined()
  expect(h.store.get(`replayed:v2:${TABLE}/${pName}`)).toBeUndefined()

  // 0.1.3 force resumes v1, writes only REFUSED_IDLESS and RETRY_IDLESS, and never duplicates LANDED_IDLESS.
  h.setFetch(null)
  h.setWrite(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-kind-v1-1').backfill(['spill', 'retry', 'rejected'], 'force')
  await h.newDist('dist-kind-v1-2').backfill(['spill', 'retry', 'rejected'], 'force')
  expect(h.count('LANDED_IDLESS')).toBe(1)
  expect(h.count('REFUSED_IDLESS')).toBe(1)
  expect(h.count('RETRY_IDLESS')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: post-staging retirement mark on one child selectively repacks only that backup standalone without rolling back sibling linked backups', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-repack-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-repack-c1.jsonl'
  const c2Name = 'spill-20200101T000000003Z-repack-c2.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_ROW')], pName)[0]!
  const c2Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C2_ROW')], pName)[0]!
  const pSize = utf8Length(pText)
  const c1Size = utf8Length(c1Text)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
  h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })

  let injectedLateMark = false
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome2-')) throw new Error('crash before record file')
    // Right when C1's linked backup data file is written, inject a late retirement mark on C1.
    if (!injectedLateMark && p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_ROW') && text.includes(`"parent":"${c1Name}"`)) {
      injectedLateMark = true
      h.files.set(
        `${PART_DIR}/outcome-done-late1-${c1Name}`,
        sealFile(
          `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`,
        ),
      )
    }
  })
  h.setStoreSet(async k => {
    if (k === v1Key || k === v2Key) throw new Error('crash before record')
  })
  h.setStoreDelete(async k => {
    if (k === v1Key) throw new Error('v1 stuck')
  })
  await h.newCurrent('cur-repack-1').backfill(['spill', 'retry'], 'force')
  expect(injectedLateMark).toBe(true)
  // C1's backup must have been repacked standalone (no parent header), while C2's linked backup stays intact!
  const liveBackups = [...h.files.entries()].filter(
    ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && !k.endsWith(`/${c2Name}`) && v !== '',
  )
  const c1LiveBk = liveBackups.filter(([, v]) => v.includes('C1_ROW'))
  const c2LiveBk = liveBackups.filter(([, v]) => v.includes('C2_ROW'))
  expect(c1LiveBk.length).toBe(1)
  expect(c1LiveBk[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)
  expect(c2LiveBk.length).toBe(1)
  expect(c2LiveBk[0]![1].includes(`"parent":"${c2Name}"`)).toBe(true)

  h.setWrite(null)
  h.setStoreSet(null)
  h.setStoreDelete(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-repack-1').backfill(['spill', 'retry'], 'force')
  await h.newCurrent('cur-repack-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-repack-3').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_ROW') >= 1).toBe(true)
  expect(h.count('C2_ROW') >= 1).toBe(true)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: clearOppositeSlot sets ok=false for store live-child, retired v1 file, and unretired v1 file so leftovers emit standalone', async () => {
  const idA = 'a'.repeat(32)
  const idC1 = 'c'.repeat(32)
  const idNew = 'b'.repeat(32)
  for (const mode of ['store-live-child', 'file-retired-v1', 'file-live-child'] as const) {
    const h = makeInteropHarness()
    const pName = `spill-20200101T000000001Z-clearopp-${mode}-p.jsonl`
    const c1Name = `retry-20200101T000000002Z-clearopp-${mode}-c1.jsonl`
    const lName = `spill-20200101T000000003Z-clearopp-${mode}-l.jsonl`
    const pPath = `${PART_DIR}/${pName}`
    const c1Path = `${PART_DIR}/${c1Name}`
    const lPath = `${PART_DIR}/${lName}`
    const pGen1 = packFiles([dlLine('spill', idA, 'OLD_ROW')])[0]!
    const pGen2 = packFiles([dlLine('spill', idNew, 'NEW_ROW')])[0]!
    expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
    const pSize = utf8Length(pGen2)
    h.files.set(pPath, pGen2)

    const v1Record =
      mode === 'file-retired-v1'
        ? {
            fates: 'L',
            reasons: [],
            refusals: [],
            size: pSize,
            sourceFnv1a: fnv1a(pGen1),
            waits: false,
            seq: 2,
            files: [],
            written: [],
            retired: true,
            retiredAt: h.io.now(),
          }
        : {
            fates: 'W',
            reasons: [],
            refusals: [],
            size: pSize,
            waits: true,
            seq: 2,
            files: [c1Name],
            written: [true],
          }

    if (mode === 'store-live-child') {
      h.files.set(c1Path, packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!)
      h.store.set(`replayed:${TABLE}/${pName}`, v1Record)
      h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    } else if (mode === 'file-retired-v1') {
      h.files.set(`${PART_DIR}/outcome-${pName}`, sealFile(`${JSON.stringify(v1Record)}\n`))
    } else {
      h.files.set(c1Path, packFiles([dlLine('retry', idC1, 'C1')], pName)[0]!)
      h.files.set(`${PART_DIR}/outcome-${pName}`, sealFile(`${JSON.stringify(v1Record)}\n`))
      h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
    }

    h.store.set(`replayed:v2:${TABLE}/${pName}`, {
      fates: 'W',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(pGen2),
      waits: true,
      seq: 5,
      files: [lName],
      written: [false],
      ...(mode !== 'file-retired-v1' ? { supersedesV1: { size: pSize, seq: 2, fates: 'W', files: [c1Name] } } : {}),
    })

    // Fail markDone so P_gen2 remains live alongside stale v1 and leftover lName.
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) {
        throw new Error('EIO: markDone failed')
      }
    })
    await h.newCurrent(`cur-opp-${mode}-1`).backfill(['spill', 'retry'], 'auto')
    const lText = h.files.get(lPath) ?? ''
    expect(lText.length > 0).toBe(true)
    // Must be written standalone (`parent: null`) because clearOppositeSlot returned ok=false.
    expect(lText.includes(`"parent":"${pName}"`)).toBe(false)

    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newDist(`dist-opp-${mode}-1`).backfill(['spill', 'retry'], 'force')
    await h.newCurrent(`cur-opp-${mode}-2`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('NEW_ROW') >= 1).toBe(true)
    if (mode !== 'file-retired-v1') expect(h.count('C1') >= 1).toBe(true)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: stale v1 checkpoint with leased written child plus unwritten part holds parent so 0.1.3 never resends new row without insertId', async () => {
  const h = makeInteropHarness()
  const pName = 'spill-20200101T000000001Z-v1-partunw-p.jsonl'
  const c1Name = 'rejected-20200101T000000002Z-v1-partunw-c1.jsonl'
  const c2Name = 'spill-20200101T000000003Z-v1-partunw-c2.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
  const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
  expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
  h.files.set(pPath, pGen2)
  h.files.set(`${PART_DIR}/${c1Name}`, packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`
  h.store.set(v1Key, {
    fates: 'RW',
    reasons: ['bad row'],
    refusals: [0],
    size: utf8Length(pGen1),
    sourceFnv1a: fnv1a(pGen1),
    waits: true,
    seq: 2,
    files: [c1Name, c2Name],
    written: [true, false],
  })
  h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'child-owner', until: h.io.now() + 5 * 60 * 1000 })

  // Fail both v2 persistence paths after dispatch (would expose P_gen2 if 0.1.4 dispatched).
  h.setStoreSet(async k => {
    if (k === v2Key) throw new Error('v2 store unavailable')
  })
  h.setWrite(async p => {
    if (p.includes('/outcome2-')) throw new Error('v2 companion file unavailable')
  })

  const r1 = await h.newCurrent('cur-partunw-1').backfill(['spill', 'retry', 'rejected'], 'auto')
  expect(r1.error).toBeNull()
  expect(r1.held ?? '').toContain(c1Name)
  expect(h.count('NEW_A')).toBe(0)
  expect(h.count('NEW_B')).toBe(0)
  const skippedBackups1 = [...h.files.entries()].filter(
    ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
  )
  expect(skippedBackups1.length).toBe(1)

  // A repeated 0.1.4 hold pass is idempotent and does not stage duplicate backups.
  const r1b = await h.newCurrent('cur-partunw-1b').backfill(['spill', 'retry', 'rejected'], 'auto')
  expect(r1b.error).toBeNull()
  expect(r1b.held ?? '').toContain(c1Name)
  const skippedBackups2 = [...h.files.entries()].filter(
    ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
  )
  expect(skippedBackups2.length).toBe(1)

  // Two legacy 0.1.3 auto passes rebuild c2 from P_gen2 once and deliver NEW_B once (never twice),
  // holding the staged v1-skipped backup for NEW_A because of its sending mark.
  h.setStoreSet(null)
  h.setWrite(null)
  await h.newDist('dist-partunw-1').backfill(['spill', 'retry'], 'auto')
  await h.newDist('dist-partunw-2').backfill(['spill', 'retry'], 'auto')
  expect(h.count('NEW_B')).toBe(1)
  expect(h.count('NEW_A')).toBe(0)

  // After lease expiry, command backfill drains BOTH the old rejected child AND NEW_A cleanly.
  h.advance(15 * 60 * 1000)
  await h.newCurrent('cur-partunw-2').backfill(['spill', 'retry', 'rejected'], 'command')
  expect(h.count('OLD_A')).toBe(1)
  expect(h.count('NEW_A')).toBe(1)
  expect(h.count('NEW_B')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])

  // Arm 2: When c1 clears without a legacy 0.1.3 auto pass preempting the held parent,
  // 0.1.4 delivers BOTH NEW_A and NEW_B exactly once and drops the v1-skipped backup unsent.
  const h2 = makeInteropHarness()
  h2.files.set(pPath, pGen2)
  h2.files.set(`${PART_DIR}/${c1Name}`, packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!)
  h2.store.set(v1Key, {
    fates: 'RW',
    reasons: ['bad row'],
    refusals: [0],
    size: utf8Length(pGen1),
    sourceFnv1a: fnv1a(pGen1),
    waits: true,
    seq: 2,
    files: [c1Name, c2Name],
    written: [true, false],
  })
  h2.store.set(`lease:${TABLE}/${c1Name}`, { by: 'child-owner', until: h2.io.now() + 5 * 60 * 1000 })
  const rHold = await h2.newCurrent('cur-partunw2-1').backfill(['spill', 'retry', 'rejected'], 'auto')
  expect(rHold.held ?? '').toContain(c1Name)
  expect(h2.count('NEW_A')).toBe(0)
  expect(h2.count('NEW_B')).toBe(0)
  const h2SkippedBks = [...h2.files.entries()].filter(
    ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
  )
  expect(h2SkippedBks.length).toBe(1)
  h2.advance(15 * 60 * 1000)
  await h2.newCurrent('cur-partunw2-2').backfill(['spill', 'retry', 'rejected'], 'command')
  await h2.newCurrent('cur-partunw2-3').backfill(['spill', 'retry', 'rejected'], 'auto')
  expect(h2.count('OLD_A')).toBe(1)
  expect(h2.count('NEW_A')).toBe(1)
  expect(h2.count('NEW_B')).toBe(1)
  expect(h2.files.get(h2SkippedBks[0]![0])).toBe('')
  expect(h2.liveDataFiles()).toEqual([])
})

test('interop: alternating 0.1.4 and 0.1.3 writers on identical-boundary v1 multi-part checkpoint resume without stranding retry row behind rejected files', async () => {
  const h = makeInteropHarness()
  const pName = 'spill-20200101T000000001Z-alt-writers-p.jsonl'
  const pPath = `${PART_DIR}/${pName}`
  // Two ~1.575 MiB rejected rows (> BODY_BYTES combined -> 2 rejected parts) plus 1 small retry row
  // (< 4 MiB HOST_FILE_BYTES total): stamped and unstamped boundaries agree (3 identical parts -> v1).
  const bigPad = 'x'.repeat(1_575_000)
  const pText = sealFile(
    `${[
      dlLine('spill', null, `REJ0-${bigPad}`),
      dlLine('spill', null, `REJ1-${bigPad}`),
      dlLine('spill', null, 'RETRY_SURVIVOR'),
    ].join('\n')}\n`,
  )
  h.files.set(pPath, pText)

  // Pass 1 (0.1.4 auto): rows 0 & 1 refused, row 2 gets 503; fail right before writing part 1 (2nd rejected file).
  h.setFetch(async tags => {
    if (tags.some(t => t.startsWith('REJ0-') || t.startsWith('REJ1-'))) {
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad' }] }] }) }
    }
    if (tags.includes('RETRY_SURVIVOR')) return { status: 503, text: 'upstream unavailable' }
    return null
  })
  let rejectedWrites = 0
  h.setWrite(async p => {
    if (p.includes('/rejected-') && !p.includes('/outcome')) {
      rejectedWrites += 1
      if (rejectedWrites === 2) throw new Error('interrupted before rejected part 1')
    }
  })
  await h.newCurrent('cur-alt-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'auto')
  const v1AfterCur = h.store.get(`replayed:${TABLE}/${pName}`) as { files?: string[]; written?: boolean[]; stampedIds?: boolean } | undefined
  expect(v1AfterCur?.files?.length).toBe(3)
  expect(v1AfterCur?.written).toEqual([true, false, false])
  expect(v1AfterCur?.stampedIds).toBe(true)

  // Pass 2 (0.1.3 auto): writes rejected part 1 unstamped, fails before retry part 2, strips stampedIds.
  const retryPartName = v1AfterCur!.files![2]!
  h.setWrite(async p => {
    if (p.endsWith(`/${retryPartName}`)) throw new Error('interrupted before retry part 2')
  })
  await h.newDist('dist-alt-1').backfill(['spill', 'retry'], 'auto')
  const v1AfterDist = h.store.get(`replayed:${TABLE}/${pName}`) as { written?: boolean[]; stampedIds?: boolean } | undefined
  expect(v1AfterDist?.written).toEqual([true, true, false])
  expect(v1AfterDist?.stampedIds).toBeUndefined()

  // Passes 3–5 (0.1.4 auto, skipping rejected): must resume despite mixed stamped/unstamped rejected parts.
  h.setFetch(null)
  h.setWrite(null)
  await h.newCurrent('cur-alt-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-alt-3').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-alt-4').backfill(['spill', 'retry'], 'auto')
  expect(h.files.get(pPath)).toBe('')
  expect(h.count('RETRY_SURVIVOR')).toBe(1)
})

test('interop: reusing a live replacement basename after 0.1.4 delivery still honors the retirement mark and never replays the backup', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const sameSize of [false, true] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-repop-live-${sameSize ? 'eq' : 'diff'}-p.jsonl`
    const cName = `spill-20200101T000000002Z-repop-live-${sameSize ? 'eq' : 'diff'}-c.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const cText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C_ORIG')], pName)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${cName}`, cText)
    const pSize = utf8Length(pText)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
    })
    await h.newCurrent(`cur-repop-live-${sameSize ? 'eq' : 'diff'}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.count('C_ORIG')).toBe(0)
    const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { adopted: string[]; backupFiles: { name: string }[] }
    const repName = v2.adopted[0]!
    const bkName = v2.backupFiles[0]!.name
    const repPath = `${PART_DIR}/${repName}`
    const origRepText = h.files.get(repPath)!

    // Hold backup with a lease, let 0.1.3 retire P (retry only), then let 0.1.4 deliver R_rep (spill).
    h.setWrite(null)
    h.store.set(`lease:${TABLE}/${bkName}`, { by: 'hold-bk', until: h.io.now() + 10 * 60 * 1000 })
    await h.newDist(`dist-repop-live-${sameSize ? 'eq' : 'diff'}-1`).backfill(['retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
    await h.newCurrent(`cur-repop-live-${sameSize ? 'eq' : 'diff'}-2`).backfill(['spill'], 'auto')
    expect(h.count('C_ORIG')).toBe(1)
    expect(h.files.get(repPath)).toBe('')

    // Repopulate the delivered replacement's basename with a live file, then release backup lease.
    const liveNewTag = sameSize ? 'NEW_EQ' : 'NEW_DIFF_LONGER_PAYLOAD'
    const liveNewId = 'e'.repeat(32)
    let repopText = packFiles([dlLine('spill', liveNewId, liveNewTag)])[0]!
    if (sameSize) {
      const diff = utf8Length(origRepText) - utf8Length(repopText)
      expect(diff >= 0).toBe(true)
      repopText = packFiles([dlLine('spill', liveNewId, `${liveNewTag}${'z'.repeat(diff)}`)])[0]!
      expect(utf8Length(repopText)).toBe(utf8Length(origRepText))
    }
    h.files.set(repPath, repopText)
    h.advance(15 * 60 * 1000)

    await h.newCurrent(`cur-repop-live-${sameSize ? 'eq' : 'diff'}-3`).backfill(['spill', 'retry'], 'auto')
    await h.newCurrent(`cur-repop-live-${sameSize ? 'eq' : 'diff'}-4`).backfill(['spill', 'retry'], 'auto')
    // Backup must be dropped via R_rep's matching retirement mark (C_ORIG sent ONCE, never twice).
    expect(h.count('C_ORIG')).toBe(1)
    expect(h.sentTags.filter(t => t.startsWith(liveNewTag)).length).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: repacking a disk-reused linked backup truncates the reused copy so recovery dispatches the child row once', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-reused-repack-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-reused-repack-c1.jsonl'
  const c2Name = 'spill-20200101T000000003Z-reused-repack-c2.jsonl'
  const bReusedName = 'spill-20200101T000000004Z-reused-repack-bk.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_REUSED')], pName)[0]!
  const c2Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C2_SIB')], pName)[0]!
  const pSize = utf8Length(pText)
  const c1Size = utf8Length(c1Text)
  const bReusedText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_REUSED', `${c1Name}#1`)], c1Name)[0]!
  const bReusedSize = utf8Length(bReusedText)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
  // Pre-seed a disk-reusable linked backup for C1 from an earlier crashed attempt.
  h.files.set(`${PART_DIR}/${bReusedName}`, bReusedText)
  h.files.set(
    `${PART_DIR}/outcome-done-old1g-${bReusedName}`,
    sealFile(
      `${JSON.stringify({
        size: bReusedSize,
        sending: true,
        by: 'old1g',
        stagedFor: pName,
        stagedForSize: pSize,
        replaces: c1Name,
        backup: true,
        backupForSize: c1Size,
        backupForFnv1a: fnv1a(c1Text),
        backupFnv1a: fnv1a(bReusedText),
        attempt: 'old-att-1',
      })}\n`,
    ),
  )
  h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })

  let injectedLateMark = false
  let c1Phase2Writes = 0
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome2-')) throw new Error('crash before record file')
    if (!injectedLateMark && p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_REUSED')) {
      c1Phase2Writes += 1
    }
    // When C2's linked backup is written in Phase 2, publish a late retirement mark on C1.
    if (!injectedLateMark && p.includes('/spill-') && !p.includes('/outcome') && text.includes('C2_SIB')) {
      injectedLateMark = true
      h.files.set(
        `${PART_DIR}/outcome-done-late1-${c1Name}`,
        sealFile(
          `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`,
        ),
      )
    }
  })
  h.setStoreSet(async k => {
    if (k === v1Key || k === v2Key) throw new Error('crash before record')
  })
  h.setStoreDelete(async k => {
    if (k === v1Key) throw new Error('v1 stuck')
  })

  await h.newCurrent('cur-reused-repack-1').backfill(['spill', 'retry'], 'force')
  expect(c1Phase2Writes).toBe(0)
  expect(injectedLateMark).toBe(true)
  // Reused linked backup must be truncated on disk, leaving exactly one standalone backup for C1.
  expect(h.files.get(`${PART_DIR}/${bReusedName}`)).toBe('')
  const liveBackups = [...h.files.entries()].filter(
    ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && !k.endsWith(`/${c2Name}`) && v !== '',
  )
  const c1LiveBk = liveBackups.filter(([, v]) => v.includes('C1_REUSED'))
  const c2LiveBk = liveBackups.filter(([, v]) => v.includes('C2_SIB'))
  expect(c1LiveBk.length).toBe(1)
  expect(c1LiveBk[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)
  expect(c2LiveBk.length).toBe(1)
  expect(c2LiveBk[0]![1].includes(`"parent":"${c2Name}"`)).toBe(true)

  h.setWrite(null)
  h.setStoreSet(null)
  h.setStoreDelete(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-reused-repack-1').backfill(['spill', 'retry'], 'force')
  await h.newCurrent('cur-reused-repack-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-reused-repack-3').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_REUSED')).toBe(1)
  expect(h.count('C2_SIB') >= 1).toBe(true)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: Phase 3a rechecks leases and bytes before Phase 3b snapshot so sibling retirement mark published during C1 read repacks C2', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-sib-recheck-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-sib-recheck-c1.jsonl'
  const c2Name = 'spill-20200101T000000003Z-sib-recheck-c2.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_SIBR')], pName)[0]!
  const c2Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C2_SIBR')], pName)[0]!
  const pSize = utf8Length(pText)
  const c2Size = utf8Length(c2Text)
  const v1Key = `replayed:${TABLE}/${pName}`
  const v2Key = `replayed:v2:${TABLE}/${pName}`
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
  h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(v2Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })

  let c2BackupStaged = false
  let injectedDuringC1Recheck = false
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome2-')) throw new Error('crash before record file')
    if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C2_SIBR') && text.includes(`"parent":"${c2Name}"`)) {
      c2BackupStaged = true
    }
  })
  h.setRead(async p => {
    // Once Phase 2 has staged C2's linked backup, the next read of C1 is Phase 3a's byte check.
    if (c2BackupStaged && !injectedDuringC1Recheck && p === `${PART_DIR}/${c1Name}`) {
      injectedDuringC1Recheck = true
      h.files.set(
        `${PART_DIR}/outcome-done-late2-${c2Name}`,
        sealFile(
          `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c2Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`,
        ),
      )
    }
    return undefined
  })
  h.setStoreSet(async k => {
    if (k === v1Key || k === v2Key) throw new Error('crash before record')
  })
  h.setStoreDelete(async k => {
    if (k === v1Key) throw new Error('v1 stuck')
  })

  await h.newCurrent('cur-sibr-1').backfill(['spill', 'retry'], 'force')
  expect(injectedDuringC1Recheck).toBe(true)
  const liveBackups = [...h.files.entries()].filter(
    ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && !k.endsWith(`/${c2Name}`) && v !== '',
  )
  const c1LiveBk = liveBackups.filter(([, v]) => v.includes('C1_SIBR'))
  const c2LiveBk = liveBackups.filter(([, v]) => v.includes('C2_SIBR'))
  expect(c1LiveBk.length).toBe(1)
  expect(c1LiveBk[0]![1].includes(`"parent":"${c1Name}"`)).toBe(true)
  expect(c2LiveBk.length).toBe(1)
  expect(c2LiveBk[0]![1].includes(`"parent":"${c2Name}"`)).toBe(false)

  h.setWrite(null)
  h.setRead(null)
  h.setStoreSet(null)
  h.setStoreDelete(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-sibr-1').backfill(['spill', 'retry'], 'force')
  await h.newCurrent('cur-sibr-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-sibr-3').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_SIBR') >= 1).toBe(true)
  expect(h.count('C2_SIBR')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: legacy 0.1.3 delivery of a same-size reused replacement basename must not discard the preceding generation backup', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-legacy-repop-parent.jsonl'
  const cName = 'spill-20200101T000000002Z-legacy-repop-child.jsonl'
  const pText = packFiles([dlLine('spill', idA, 'A'), dlLine('spill', idB, 'B')])[0]!
  const cText = packFiles([dlLine('retry', idB, 'B'), dlLine('retry', null, 'C_LEGACY_REPOP')], pName)[0]!
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${cName}`, cText)
  const pSize = utf8Length(pText)
  h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [cName], written: [true] })
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
  })
  await h.newCurrent('cur-leg-repop-1').backfill(['spill', 'retry'], 'force')
  expect(h.count('C_LEGACY_REPOP')).toBe(0)
  const journal = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { adopted: string[] }
  const replacement = `${PART_DIR}/${journal.adopted[0]}`
  const origRepSize = utf8Length(h.files.get(replacement)!)
  h.setWrite(null)
  h.advance(15 * 60 * 1000)
  // 0.1.3 force drops the adopted replacement unsent.
  await h.newDist('dist-leg-repop-1').backfill(['spill', 'retry'], 'force')
  expect(h.files.get(replacement)).toBe('')

  // Repopulate the dropped replacement basename with an EXACT SAME-SIZE different file and deliver it via 0.1.3 (digest-less done mark).
  const repopPrefix = 'LEGACY_SAME_SIZE_'
  const repopBase = packFiles([dlLine('retry', 'e'.repeat(32), repopPrefix)])[0]!
  const padLen = origRepSize - utf8Length(repopBase)
  expect(padLen >= 0).toBe(true)
  const sameSizeRepop = packFiles([dlLine('retry', 'e'.repeat(32), `${repopPrefix}${'q'.repeat(padLen)}`)])[0]!
  expect(utf8Length(sameSizeRepop)).toBe(origRepSize)
  h.files.set(replacement, sameSizeRepop)
  await h.newDist('dist-leg-repop-2').backfill(['spill', 'retry'], 'auto')
  expect(h.files.get(replacement)).toBe('')

  // 0.1.4 auto must NOT accept the digest-less 0.1.3 done mark as proof the original replacement was delivered.
  await h.newCurrent('cur-leg-repop-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-leg-repop-3').backfill(['spill', 'retry'], 'auto')
  expect(h.sentTags.filter(t => t.startsWith(repopPrefix)).length).toBe(1)
  expect(h.count('C_LEGACY_REPOP')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: shadowed partial v1 store does not hold equal-size parent when completed outcome- or done mark is visible to 0.1.3', async () => {
  for (const mode of ['done-mark', 'filed-complete'] as const) {
    const h = makeInteropHarness()
    const pName = `spill-20200101T000000001Z-v1-shadow-${mode}-p.jsonl`
    const c1Name = `rejected-20200101T000000002Z-v1-shadow-${mode}-c1.jsonl`
    const c2Name = `spill-20200101T000000003Z-v1-shadow-${mode}-c2.jsonl`
    const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
    const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
    expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
    const pSize = utf8Length(pGen1)
    h.files.set(`${PART_DIR}/${pName}`, pGen2)
    h.files.set(`${PART_DIR}/${c1Name}`, packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!)
    const v1Key = `replayed:${TABLE}/${pName}`
    // Stale partial v1 store record left behind after storeDelete failure.
    h.store.set(v1Key, {
      fates: 'RW',
      reasons: ['bad row'],
      refusals: [0],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [c1Name, c2Name],
      written: [true, false],
    })
    if (mode === 'done-mark') {
      h.files.set(
        `${PART_DIR}/outcome-done-m1-${pName}`,
        sealFile(
          `${JSON.stringify({
            fates: 'RW',
            reasons: ['bad row'],
            refusals: [0],
            size: pSize,
            sourceFnv1a: fnv1a(pGen1),
            waits: true,
            seq: 3,
            files: [c1Name, c2Name],
            written: [true, true],
            retired: true,
            retiredAt: h.io.now(),
          })}\n`,
        ),
      )
    } else {
      h.files.set(
        `${PART_DIR}/outcome-${pName}`,
        sealFile(
          `${JSON.stringify({
            fates: 'RW',
            reasons: ['bad row'],
            refusals: [0],
            size: pSize,
            sourceFnv1a: fnv1a(pGen1),
            waits: true,
            seq: 3,
            files: [c1Name, c2Name],
            written: [true, true],
          })}\n`,
        ),
      )
    }
    // 0.1.4 auto must dispatch P_gen2 with forceV2 instead of holding and letting 0.1.3 truncate it unsent.
    const r1 = await h.newCurrent(`cur-shadow-${mode}-1`).backfill(['spill', 'retry'], 'auto')
    expect(r1.error).toBeNull()
    expect(h.count('NEW_A')).toBe(1)
    expect(h.count('NEW_B')).toBe(1)
    // Subsequent 0.1.3 auto sees P_gen2 already delivered and cannot lose rows.
    await h.newDist(`dist-shadow-${mode}-1`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('NEW_A')).toBe(1)
    expect(h.count('NEW_B')).toBe(1)
    // Command backfill drains the old rejected child cleanly.
    await h.newCurrent(`cur-shadow-${mode}-2`).backfill(['spill', 'retry', 'rejected'], 'command')
    expect(h.count('OLD_A')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: orphaned childGuard on live written v1 child does not deadlock with unwritten v1 parent hold on force or command', async () => {
  for (const mode of ['force', 'command', 'force-with-parent-sending'] as const) {
    const trigger = mode === 'command' ? 'command' : 'force'
    const h = makeInteropHarness()
    const pName = `spill-20200101T000000001Z-v1-cg-${mode}-p.jsonl`
    const c1Name = `rejected-20200101T000000002Z-v1-cg-${mode}-c1.jsonl`
    const c2Name = `spill-20200101T000000003Z-v1-cg-${mode}-c2.jsonl`
    const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
    const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
    expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
    const pSize = utf8Length(pGen1)
    const c1Text = packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!
    const c1Size = utf8Length(c1Text)
    h.files.set(`${PART_DIR}/${pName}`, pGen2)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'RW',
      reasons: ['bad row'],
      refusals: [0],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [c1Name, c2Name],
      written: [true, false],
    })
    h.files.set(
      `${PART_DIR}/outcome-done-oldg-${c1Name}`,
      sealFile(
        `${JSON.stringify({
          size: c1Size,
          sending: true,
          by: 'oldg',
          stagedFor: pName,
          stagedForSize: pSize,
          childGuard: true,
          attempt: 'att-orph-1',
        })}\n`,
      ),
    )
    if (mode === 'force-with-parent-sending') {
      // Simulate the crashed force run's parent sending mark standing alongside c1Name's childGuard.
      h.files.set(
        `${PART_DIR}/outcome-done-old-${pName}`,
        sealFile(`${JSON.stringify({ size: pSize, sending: true, by: 'old' })}\n`),
      )
      // Non-force command must continue holding while the parent sending mark stands.
      const rCmd = await h.newCurrent(`cur-cg-${mode}-cmd`).backfill(['spill', 'retry', 'rejected'], 'command')
      expect(rCmd.error).toBeNull()
      expect(h.count('OLD_A')).toBe(0)
      expect(h.count('NEW_A')).toBe(0)
    }
    const r = await h.newCurrent(`cur-cg-${mode}-1`).backfill(['spill', 'retry', 'rejected'], trigger)
    expect(r.error).toBeNull()
    expect(r.held).toBeNull()
    expect(h.count('OLD_A')).toBe(1)
    expect(h.count('NEW_A')).toBe(1)
    expect(h.count('NEW_B')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: commit-point backup retirement preserves predecessor on rollback, promotes replacement on HTTP 503, and retires prior standalone on linked transition', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)

  // Arm 1: Phase 3b sibling failure leaves pre-existing linked backup intact and rolls back newly staged standalone copy.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-commit-arm1-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-commit-arm1-c1.jsonl'
    const c2Name = 'spill-20200101T000000003Z-commit-arm1-c2.jsonl'
    const bOld1Name = 'spill-20200101T000000004Z-commit-arm1-bk1.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_ARM1')], pName)[0]!
    const c2Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C2_ARM1')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOld1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_ARM1', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
    h.files.set(`${PART_DIR}/${bOld1Name}`, bOld1Text)
    h.files.set(
      `${PART_DIR}/outcome-done-old1g-${bOld1Name}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOld1Text),
          sending: true,
          by: 'old1g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOld1Text),
          attempt: 'old-arm1',
        })}\n`,
      ),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })
    let injected = false
    let poisonedC2Lease = false
    h.setWrite(async (p, text) => {
      if (!injected && p.includes('/spill-') && !p.includes('/outcome') && text.includes('C2_ARM1')) {
        injected = true
        h.files.set(
          `${PART_DIR}/outcome-done-latea1-${c1Name}`,
          sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
        )
      }
      // Once C1's standalone repack backup is written in Phase 3b, assign C2's lease to a foreign session so Phase 3b throws.
      if (injected && !poisonedC2Lease && p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_ARM1') && !text.includes(`"parent":"${c1Name}"`)) {
        poisonedC2Lease = true
        h.store.set(`lease:${TABLE}/${c2Name}`, { by: 'other-sess', holder: 'other-h', until: h.io.now() + 5 * 60 * 1000 })
      }
    })
    await h.newCurrent('cur-arm1-1').backfill(['spill', 'retry'], 'force')
    expect(poisonedC2Lease).toBe(true)
    // Pre-existing bOld1 must remain intact on disk; newly staged standalone copy must be rolled back.
    expect(h.files.get(`${PART_DIR}/${bOld1Name}`)).toBe(bOld1Text)
    const liveC1Bks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && !k.endsWith(`/${c2Name}`) && v.includes('C1_ARM1'),
    )
    expect(liveC1Bks.length).toBe(1)
    expect(liveC1Bks[0]![0]).toBe(`${PART_DIR}/${bOld1Name}`)
  }

  // Arm 2: HTTP 503 after Phase 4 replaces pre-existing bOld1 with bStand1 must NOT roll back bStand1.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-commit-arm2-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-commit-arm2-c1.jsonl'
    const bOld1Name = 'spill-20200101T000000003Z-commit-arm2-bk1.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_ARM2')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOld1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_ARM2', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOld1Name}`, bOld1Text)
    h.files.set(
      `${PART_DIR}/outcome-done-old2g-${bOld1Name}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOld1Text),
          sending: true,
          by: 'old2g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOld1Text),
          attempt: 'old-arm2',
        })}\n`,
      ),
    )
    // Pre-existing retirement mark on C1 forces standalone backup in Phase 2.
    h.files.set(
      `${PART_DIR}/outcome-done-prev-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    h.setFetch(async () => ({ status: 503, text: 'transient upstream failure' }))

    await h.newCurrent('cur-arm2-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${bOld1Name}`)).toBe('')
    const liveC1Bks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_ARM2'),
    )
    expect(liveC1Bks.length).toBe(1)
    expect(liveC1Bks[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)

    // Recovery after 0.1.3 force drops C1 unsent delivers C1_ARM2 exactly once via the promoted standalone backup.
    h.setFetch(null)
    h.advance(15 * 60 * 1000)
    await h.newDist('dist-arm2-1').backfill(['spill', 'retry'], 'force')
    await h.newCurrent('cur-arm2-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-arm2-3').backfill(['spill', 'retry'], 'auto')
    expect(h.count('C1_ARM2')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 3: Standalone -> linked transition retires pre-existing standalone backup so child-first 0.1.3 force does not duplicate.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-commit-arm3-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-commit-arm3-c1.jsonl'
    const bStandOldName = 'spill-20200101T000000003Z-commit-arm3-bk1.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_ARM3')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bStandOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_ARM3', `${c1Name}#1`)])[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bStandOldName}`, bStandOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old3g-${bStandOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bStandOldText),
          sending: true,
          by: 'old3g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bStandOldText),
          attempt: 'old-arm3',
        })}\n`,
      ),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    h.setFetch(async () => ({ status: 503, text: 'transient upstream failure' }))

    await h.newCurrent('cur-arm3-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${bStandOldName}`)).toBe('')
    const liveC1Bks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_ARM3'),
    )
    expect(liveC1Bks.length).toBe(1)
    expect(liveC1Bks[0]![1].includes(`"parent":"${c1Name}"`)).toBe(true)
  }
})

test('interop: 0.1.3 digest-less delivery of an adopted sibling does not cause a reconciled child backup whose own replacement was 0.1.4-delivered to double-send', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  // C2 (retry) sorts before P (retry) so 0.1.3 force(['retry']) delivers unmodified adopted C2
  // (digest-less done mark) and retires P via stale v1, leaving spill R_rep1 for 0.1.4 auto.
  const c2Name = 'retry-20200101T000000001Z-sib-iso-c2.jsonl'
  const pName = 'retry-20200101T000000002Z-sib-iso-p.jsonl'
  const c1Name = 'spill-20200101T000000003Z-sib-iso-c1.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_SIB_ISO')], pName)[0]!
  const c2Text = packFiles([dlLine('retry', null, 'C2_SIB_ISO')], pName)[0]!
  const pSize = utf8Length(pText)
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
  const v1Key = `replayed:${TABLE}/${pName}`
  h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [c2Name], written: [true] })
  h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })
  h.setStoreSet(async k => {
    if (k === v1Key) throw new Error('v1 stuck')
  })
  h.setStoreDelete(async k => {
    if (k === v1Key) throw new Error('v1 delete failed')
  })
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
  })
  await h.newCurrent('cur-sib-iso-1').backfill(['spill', 'retry'], 'force')
  expect(h.count('C1_SIB_ISO')).toBe(0)
  const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
    files: string[]
    adopted: string[]
    backupFiles: { name: string; replaces: string }[]
  }
  expect(v2.adopted.includes(c2Name)).toBe(true)
  expect(v2.files.includes(c2Name)).toBe(true)
  const rep1Name = v2.adopted.find(n => n !== c2Name)!
  const bk1Name = v2.backupFiles.find(b => b.replaces === c1Name)!.name

  // Hold BK1; 0.1.3 force(['retry']) delivers unmodified adopted C2 (writing a
  // digest-less 0.1.3 done mark alongside 0.1.4's childGuard) and retires P via v1;
  // then 0.1.4 auto(['spill']) delivers R_rep1 (full sourceFnv1a retirement mark).
  h.setWrite(null)
  h.setStoreSet(null)
  h.setStoreDelete(null)
  h.store.set(`lease:${TABLE}/${bk1Name}`, { by: 'hold-bk1', until: h.io.now() + 10 * 60 * 1000 })
  await h.newDist('dist-sib-iso-1').backfill(['retry'], 'force')
  expect(h.count('C2_SIB_ISO')).toBe(1)
  expect(h.files.get(`${PART_DIR}/${c2Name}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${rep1Name}`) !== '').toBe(true)
  await h.newCurrent('cur-sib-iso-2').backfill(['spill'], 'auto')
  expect(h.count('C1_SIB_ISO')).toBe(1)
  expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe('')

  // Release BK1's lease: 0.1.4 auto must drop BK1 cleanly via R_rep1's verified mark despite C2's digest-less 0.1.3 mark.
  h.advance(15 * 60 * 1000)
  await h.newCurrent('cur-sib-iso-3').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-sib-iso-4').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_SIB_ISO')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: resumed writeOut and childGuard marks on a parent-linked synthesized replacement never drop or duplicate the last backup', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const mode of ['resumed-writeout', 'preexisting-childguard-on-rep'] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-resrep-${mode}-p.jsonl`
    const c1Name = `spill-20200101T000000002Z-resrep-${mode}-c1.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_RESUMED_UNIQUE')], pName)[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    // Seed conflicting v1 + v2 so 0.1.4 force re-dispatches P and reconciles c1Name;
    // v1 clears cleanly during writeOut so v1ShadowNeeded() === false and R_rep1 gets `parent: pName`.
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
      if (p === `${PART_DIR}/${pName}` && text === '') throw new Error('stopped before truncating parent')
    })

    // Pass 1 (0.1.4 force): stages linked bk1, clears v1, synthesizes parent-linked rep1, records v2, fails at markDone.
    await h.newCurrent(`cur-resrep-${mode}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.count('C1_RESUMED_UNIQUE')).toBe(0)
    const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
      adopted: string[]
      deliverableRefs?: { name: string; replaces?: string }[]
      backupFiles: { name: string; replaces: string }[]
    }
    const rep1Name = v2.adopted[0]!
    const bk1Name = v2.backupFiles.find(b => b.replaces === c1Name)!.name
    const rep1Text = h.files.get(`${PART_DIR}/${rep1Name}`)!
    expect(rep1Text.includes(`"parent":"${pName}"`)).toBe(true)
    expect(v2.deliverableRefs?.find(d => d.name === rep1Name)?.replaces).toBe(c1Name)

    if (mode === 'resumed-writeout') {
      // Pass 2 (0.1.4 auto with markDone still failing): must NOT stage a duplicate backup for rep1Name.
      await h.newCurrent(`cur-resrep-${mode}-2`).backfill(['spill', 'retry'], 'auto')
      const v2After = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { backupFiles: { name: string; replaces: string }[] }
      expect(v2After.backupFiles.length).toBe(1)
      expect(v2After.backupFiles[0]!.name).toBe(bk1Name)
    } else {
      // Simulate a preexisting childGuard mark stamped onto parent-linked rep1Name from an earlier build/session.
      h.files.set(
        `${PART_DIR}/outcome-done-pre2g-${rep1Name}`,
        sealFile(
          `${JSON.stringify({
            size: utf8Length(rep1Text),
            sending: true,
            by: 'pre2g',
            stagedFor: pName,
            stagedForSize: pSize,
            childGuard: true,
            attempt: 'pre2-att',
          })}\n`,
        ),
      )
    }

    // Pass 3 (0.1.3 force after lease expiry): retires pName with files: [] and drops c1Name and rep1Name unsent.
    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newDist(`dist-resrep-${mode}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
    expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe('')
    expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe('')
    expect(h.files.get(`${PART_DIR}/${bk1Name}`) !== '').toBe(true)

    // Pass 4 (0.1.4 auto): must recognize rep1Name belongs to c1Name via deliverableRefs.replaces and deliver bk1Name once.
    await h.newCurrent(`cur-resrep-${mode}-3`).backfill(['spill', 'retry'], 'auto')
    await h.newCurrent(`cur-resrep-${mode}-4`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('C1_RESUMED_UNIQUE')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: resumed writeOut verifies replacement, child, and leased backup fingerprints across reuse, mid-check truncation, and backup v1 records', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const bkMode of [
    'empty-backup',
    'mismatched-backup',
    'linked-backup',
    'diff-size-retired-linked-backup',
    'v1-store-linked-backup',
    'reused-rep-same-size',
    'backup-truncated-after-read',
    'backup-v1-store-record',
    'live-orig-backup-v1-slot',
    'live-orig-late-backup-v1-slot',
    'live-orig-late-backup-v1-and-rep-swap',
    'live-orig-late-backup-v1-and-foreign-rep-swap',
    'rep-emptied-on-last-read',
    'rep-already-empty-orig-empty',
    'late-rep-swap-syncs-v2',
  ] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-reused-orig-${bkMode}-p.jsonl`
    const c1Name = `spill-20200101T000000002Z-reused-orig-${bkMode}-c1.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_ORIG_UNIQUE')], pName)[0]!
    const c1Gen2Text =
      bkMode === 'linked-backup'
        ? packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_REUSED_GEN2')])[0]!
        : bkMode === 'diff-size-retired-linked-backup'
          ? packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_REUSED_GEN2_DIFFERENT_SIZE_PAYLOAD')])[0]!
          : packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_REUSED_GEN2')], pName)[0]!
    if (bkMode === 'empty-backup' || bkMode === 'mismatched-backup') expect(utf8Length(c1Gen2Text)).toBe(utf8Length(c1Text))
    if (bkMode === 'diff-size-retired-linked-backup') expect(utf8Length(c1Gen2Text)).not.toBe(utf8Length(c1Text))
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
      if (p === `${PART_DIR}/${pName}` && text === '') throw new Error('stopped before truncating parent')
    })

    // Pass 1 (0.1.4 force): stages linked bk1, clears v1, synthesizes parent-linked rep1, records v2, fails at markDone.
    await h.newCurrent(`cur-rorig-${bkMode}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.count('C1_ORIG_UNIQUE')).toBe(0)
    const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
      adopted: string[]
      backupFiles: { name: string; replaces: string }[]
    }
    const rep1Name = v2.adopted[0]!
    const bk1Name = v2.backupFiles.find(b => b.replaces === c1Name)!.name
    const origRep1Text = h.files.get(`${PART_DIR}/${rep1Name}`)!
    const origBk1Text = h.files.get(`${PART_DIR}/${bk1Name}`)!
    expect(origRep1Text.includes(`"parent":"${pName}"`)).toBe(true)

    let truncatedBkAfterRead = false
    let foreignRepText = ''
    // Mutate c1Name / bk1Name / rep1Name according to mode before resuming writeOut.
    let attemptedParentRetiredMark = false
    if (bkMode === 'empty-backup') {
      h.files.set(`${PART_DIR}/${c1Name}`, c1Gen2Text)
      h.files.set(`${PART_DIR}/${bk1Name}`, '')
    } else if (bkMode === 'mismatched-backup') {
      h.files.set(`${PART_DIR}/${c1Name}`, c1Gen2Text)
      const bkDataLines = origBk1Text
        .trimEnd()
        .split('\n')
        .slice(0, -1)
        .map(line => line.replace('C1_ORIG_UNIQUE', 'C1_MISMATCH_BK'))
      const mismatchedBk = packFiles(bkDataLines, c1Name)[0]!
      expect(utf8Length(mismatchedBk)).toBe(utf8Length(origBk1Text))
      h.files.set(`${PART_DIR}/${bk1Name}`, mismatchedBk)
    } else if (bkMode === 'linked-backup') {
      h.files.set(`${PART_DIR}/${c1Name}`, c1Gen2Text)
      expect(origBk1Text.includes(`"parent":"${c1Name}"`)).toBe(true)
    } else if (bkMode === 'diff-size-retired-linked-backup') {
      // Different-size standalone generation reuses c1Name; 0.1.3 auto delivers and retires it,
      // emptying c1Name and leaving a different-size retired done mark that 0.1.3 parentFate(-1) matches.
      expect(origBk1Text.includes(`"parent":"${c1Name}"`)).toBe(true)
      h.files.set(`${PART_DIR}/${c1Name}`, c1Gen2Text)
      h.advance(15 * 60 * 1000)
      await h.newDist(`dist-rorig-pre-${bkMode}`).backfill(['spill'], 'auto')
      expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe('')
      expect(h.count('C1_REUSED_GEN2_DIFFERENT_SIZE_PAYLOAD')).toBe(1)
      expect(h.files.get(`${PART_DIR}/${bk1Name}`) !== '').toBe(true)
    } else if (bkMode === 'v1-store-linked-backup') {
      // c1Name is empty on disk with a v1 store tombstone `{ cleared: true, size: -1 }`
      // that shipped 0.1.3 parentFate(-1) treats as matching and drops linked bk1Name on.
      expect(origBk1Text.includes(`"parent":"${c1Name}"`)).toBe(true)
      h.files.set(`${PART_DIR}/${c1Name}`, '')
      h.store.set(`replayed:${TABLE}/${c1Name}`, { cleared: true, size: -1, files: [], written: [] })
    } else if (bkMode === 'reused-rep-same-size') {
      // Original c1Name and bk1Name remain intact, but rep1Name itself was overwritten
      // by a different same-size generation carrying REP1_GEN2_UNIQ.
      const repDataLines = origRep1Text
        .trimEnd()
        .split('\n')
        .slice(0, -1)
        .map(line => line.replace('C1_ORIG_UNIQUE', 'REP1_GEN2_UNIQ'))
      const swappedRep1 = packFiles(repDataLines, pName)[0]!
      expect(utf8Length(swappedRep1)).toBe(utf8Length(origRep1Text))
      h.files.set(`${PART_DIR}/${rep1Name}`, swappedRep1)
    } else if (bkMode === 'backup-truncated-after-read') {
      // c1Name is empty; bk1Name is intact on first read inside hasVerifiedOrigOrBackup
      // and then truncated immediately after that read returns.
      h.files.set(`${PART_DIR}/${c1Name}`, '')
      h.setRead(async p => {
        if (!truncatedBkAfterRead && p === `${PART_DIR}/${bk1Name}`) {
          truncatedBkAfterRead = true
          h.files.set(p, '')
          return origBk1Text
        }
        return undefined
      })
    } else if (bkMode === 'backup-v1-store-record') {
      // c1Name is empty; bk1Name is intact on disk, but carries an unfinished v1 store record.
      h.files.set(`${PART_DIR}/${c1Name}`, '')
      h.store.set(`replayed:${TABLE}/${bk1Name}`, {
        fates: 'LL',
        reasons: [],
        refusals: [],
        size: utf8Length(origBk1Text),
        waits: false,
        seq: 1,
        files: [],
        written: [],
      })
    } else if (bkMode === 'live-orig-backup-v1-slot') {
      // c1Name stays live on disk (skippedByOrig path), while bk1Name already carries
      // an unfinished v1 store record: stageChildBackups must NOT accept bk1Name as clean
      // coverage, and instead stage a clean replacement backup (or guard rep1Name).
      h.store.set(`replayed:${TABLE}/${bk1Name}`, {
        fates: 'LL',
        reasons: [],
        refusals: [],
        size: utf8Length(origBk1Text),
        waits: false,
        seq: 1,
        files: [],
        written: [],
      })
    } else if (bkMode === 'live-orig-late-backup-v1-slot') {
      // c1Name stays live and bk1Name starts clean through stageChildBackups, then
      // gains an unfinished v1 store record right before post-staging backup lease
      // verification: writeOut must fall back to guarding & backing up rep1Name in
      // the SAME pass before calling record().
      let injectedLateV1 = false
      h.setStoreSet(async k => {
        if (!injectedLateV1 && k === `lease:${TABLE}/${bk1Name}`) {
          injectedLateV1 = true
          h.store.set(`replayed:${TABLE}/${bk1Name}`, {
            fates: 'LL',
            reasons: [],
            refusals: [],
            size: utf8Length(origBk1Text),
            waits: false,
            seq: 1,
            files: [],
            written: [],
          })
        }
      })
    } else if (bkMode === 'live-orig-late-backup-v1-and-rep-swap' || bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap') {
      // Combined failure: bk1Name gains a late v1 store record AND rep1Name swaps
      // (to either a same-parent generation or a foreign-parent file) right before
      // checkFinalSkippedItem, with markDone ALLOWED. Pass 2 must NOT retire pName
      // or empty c1Name, preserving C1_ORIG_UNIQUE (and REP1_GEN2_UNIQ / foreignRep).
      h.setWrite(async (p, text) => {
        if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) {
          attemptedParentRetiredMark = true
        }
      })
      let injectedLateV1 = false
      h.setStoreSet(async k => {
        if (!injectedLateV1 && k === `lease:${TABLE}/${bk1Name}`) {
          injectedLateV1 = true
          h.store.set(`replayed:${TABLE}/${bk1Name}`, {
            fates: 'LL',
            reasons: [],
            refusals: [],
            size: utf8Length(origBk1Text),
            waits: false,
            seq: 1,
            files: [],
            written: [],
          })
        }
      })
      const otherParentName = `retry-20200101T000000009Z-foreign-${bkMode}-op.jsonl`
      if (bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap') {
        const otherParentText = packFiles([dlLine('retry', 'f'.repeat(32), 'OTHER_P')])[0]!
        h.files.set(`${PART_DIR}/${otherParentName}`, otherParentText)
        h.store.set(`replayed:${TABLE}/${otherParentName}`, {
          fates: 'W',
          reasons: [],
          refusals: [],
          size: utf8Length(otherParentText),
          waits: true,
          seq: 1,
          files: [rep1Name],
          written: [false],
        })
        h.store.set(`lease:${TABLE}/${otherParentName}`, { by: 'other-parent-owner', until: h.io.now() + 60 * 60 * 1000 })
      }
      const repDataLines = origRep1Text
        .trimEnd()
        .split('\n')
        .slice(0, -1)
        .map(line => line.replace('C1_ORIG_UNIQUE', bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap' ? 'REP1_FOREIGN_UNIQ' : 'REP1_GEN2_UNIQ'))
      const swappedRep =
        bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap'
          ? packFiles(repDataLines, otherParentName)[0]!
          : packFiles(repDataLines, pName)[0]!
      if (bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap') foreignRepText = swappedRep
      let repReads = 0
      h.setRead(async p => {
        if (p === `${PART_DIR}/${rep1Name}`) {
          repReads += 1
          if (repReads === 2) {
            h.files.set(p, swappedRep)
            return swappedRep
          }
        }
        return undefined
      })
    } else if (bkMode === 'rep-emptied-on-last-read') {
      // c1Name stays live; allow markDone to succeed if Pass 2 fails to abort on ''
      // so the test strictly fails without the empty-read rejection.
      h.setWrite(async (p, text) => {
        if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) {
          attemptedParentRetiredMark = true
        }
      })
      let repReads = 0
      h.setRead(async p => {
        if (p === `${PART_DIR}/${rep1Name}`) {
          repReads += 1
          if (repReads === 2) {
            h.files.set(p, '')
            return ''
          }
        }
        return undefined
      })
    } else if (bkMode === 'rep-already-empty-orig-empty') {
      // Both rep1Name and c1Name are already empty unsent before Pass 2 with ONLY bk1Name live;
      // allow markDone to succeed so writeOut must preserve bk1Name instead of dropping it on retirement.
      h.setWrite(null)
      h.files.set(`${PART_DIR}/${c1Name}`, '')
      h.files.set(`${PART_DIR}/${rep1Name}`, '')
    } else {
      // c1Name stays live and enters guardTargets; bk1Name is repacked standalone during
      // stageChildBackups Phase 4, and rep1Name is swapped to REP1_GEN2_UNIQ right before
      // the post-staging check so writeOut must guard & stage rep1Backup for the swapped
      // generation IN THE SAME PASS and persist both to v2.
      h.files.set(
        `${PART_DIR}/outcome-done-prev8-${c1Name}`,
        sealFile(
          `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: utf8Length(c1Text) + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`,
        ),
      )
      const repDataLines = origRep1Text
        .trimEnd()
        .split('\n')
        .slice(0, -1)
        .map(line => line.replace('C1_ORIG_UNIQUE', 'REP1_GEN2_UNIQ'))
      const swappedRep1 = packFiles(repDataLines, pName)[0]!
      expect(utf8Length(swappedRep1)).toBe(utf8Length(origRep1Text))
      let repReads = 0
      h.setRead(async p => {
        if (p === `${PART_DIR}/${rep1Name}`) {
          repReads += 1
          // Read 1 (initial dRef check) returns origRep1Text; read 2 (post-staging check) sees swappedRep1.
          if (repReads === 2) {
            h.files.set(p, swappedRep1)
            return swappedRep1
          }
        }
        return undefined
      })
    }
    // Make delivery fail with HTTP 503 while markDone still fails on resume so rep1/bk1 stay live on disk.
    h.setFetch(async tags =>
      tags.includes('C1_ORIG_UNIQUE') || tags.includes('REP1_GEN2_UNIQ') || (bkMode === 'rep-already-empty-orig-empty' && tags.includes('B'))
        ? { status: 503, text: 'upstream 503' }
        : null,
    )

    // Pass 2 (0.1.4 auto resume): must protect rep1Name / c1Name in a single pass before failing markDone.
    await h
      .newCurrent(`cur-rorig-${bkMode}-2`)
      .backfill(bkMode === 'rep-already-empty-orig-empty' ? ['retry'] : ['spill', 'retry'], 'auto')
    h.setRead(null)
    h.setStoreSet(null)
    if (bkMode === 'late-rep-swap-syncs-v2') {
      const v2Mid = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
        backupFiles: { name: string; replaces: string }[]
        attempts?: string[]
      }
      const repackedC1Bk = v2Mid.backupFiles.find(b => b.replaces === c1Name)
      expect(repackedC1Bk).not.toBeUndefined()
      expect(repackedC1Bk!.name).not.toBe(bk1Name)
      expect(h.files.get(`${PART_DIR}/${bk1Name}`)).toBe('')
      expect(h.files.get(`${PART_DIR}/${repackedC1Bk!.name}`)?.includes(`"parent":"${c1Name}"`)).toBe(false)
      // Swapped rep1Name MUST already be guarded & backed up in Pass 2a itself (not deferred to a later pass).
      const rep1BkSamePass = v2Mid.backupFiles.find(b => b.replaces === rep1Name)
      expect(rep1BkSamePass).not.toBeUndefined()
      expect(h.files.get(`${PART_DIR}/${rep1BkSamePass!.name}`)?.includes('REP1_GEN2_UNIQ')).toBe(true)
    }
    if (bkMode === 'backup-truncated-after-read') expect(truncatedBkAfterRead).toBe(true)
    const v2After = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
      backupFiles: { name: string; replaces: string }[]
    }
    const expectedTag = bkMode === 'reused-rep-same-size' || bkMode === 'late-rep-swap-syncs-v2' ? 'REP1_GEN2_UNIQ' : 'C1_ORIG_UNIQUE'
    if (bkMode === 'live-orig-backup-v1-slot') {
      // A fresh clean backup (not the v1-poisoned bk1Name) must now cover c1Name.
      const cleanC1Bk = v2After.backupFiles.find(b => b.replaces === c1Name && b.name !== bk1Name)
      expect(cleanC1Bk).not.toBeUndefined()
      expect(v2After.backupFiles.some(b => b.name === bk1Name)).toBe(false)
    } else if (bkMode === 'live-orig-late-backup-v1-and-rep-swap' || bkMode === 'live-orig-late-backup-v1-and-foreign-rep-swap') {
      // Aborted BEFORE markDone without emptying pName or c1Name.
      expect(attemptedParentRetiredMark).toBe(false)
      expect(h.files.get(`${PART_DIR}/${pName}`)).toBe(pText)
      expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe(c1Text)
      if (bkMode === 'live-orig-late-backup-v1-and-rep-swap') {
        const rep1BkSamePass = v2After.backupFiles.find(b => b.replaces === rep1Name)
        expect(rep1BkSamePass).not.toBeUndefined()
        expect(h.files.get(`${PART_DIR}/${rep1BkSamePass!.name}`)?.includes('REP1_GEN2_UNIQ')).toBe(true)
      } else {
        expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe(foreignRepText)
      }
      // Subsequent 0.1.4 auto passes reconcile c1Name cleanly and deliver C1_ORIG_UNIQUE without losing it.
      h.setWrite(null)
      h.setFetch(null)
      await h.newCurrent(`cur-rorig-${bkMode}-3`).backfill(['spill', 'retry'], 'auto')
      await h.newCurrent(`cur-rorig-${bkMode}-4`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('C1_ORIG_UNIQUE')).toBe(1)
      if (bkMode === 'live-orig-late-backup-v1-and-rep-swap') {
        expect(h.count('REP1_GEN2_UNIQ')).toBe(1)
        expect(h.liveDataFiles()).toEqual([])
      } else {
        // Foreign-parent rep1Name remains intact (waiting for its own parent) and was not truncated.
        expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe(foreignRepText)
      }
      continue
    } else if (bkMode === 'rep-emptied-on-last-read') {
      // Aborted BEFORE markDone without dropping pName, c1Name, or bk1Name when rep1Name vanished on the final read.
      expect(attemptedParentRetiredMark).toBe(false)
      expect(h.files.get(`${PART_DIR}/${pName}`)).toBe(pText)
      expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe(c1Text)
      expect(h.files.get(`${PART_DIR}/${bk1Name}`)).toBe(origBk1Text)
      // A subsequent 0.1.4 auto pass (without 0.1.3) must NOT drop c1Name and bk1Name together:
      // it restores c1Name out of reconciledFiles and delivers C1_ORIG_UNIQUE exactly once.
      h.setWrite(null)
      h.setFetch(null)
      await h.newCurrent(`cur-rorig-${bkMode}-3`).backfill(['spill', 'retry'], 'auto')
      await h.newCurrent(`cur-rorig-${bkMode}-4`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('C1_ORIG_UNIQUE')).toBe(1)
      expect(h.liveDataFiles()).toEqual([])
      continue
    } else if (bkMode === 'rep-already-empty-orig-empty') {
      // Pass 2 retired pName while keeping bk1Name alive (did NOT drop bk1Name in cleanupBackups).
      expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
      expect(h.files.get(`${PART_DIR}/${bk1Name}`)).toBe(origBk1Text)
      // Next 0.1.4 auto pass delivers C1_ORIG_UNIQUE exactly once from bk1Name WITHOUT re-sending already-landed B.
      h.setFetch(null)
      await h.newCurrent(`cur-rorig-${bkMode}-3`).backfill(['spill', 'retry'], 'auto')
      await h.newCurrent(`cur-rorig-${bkMode}-4`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('C1_ORIG_UNIQUE')).toBe(1)
      expect(h.count('B')).toBe(1)
      expect(h.liveDataFiles()).toEqual([])
      continue
    } else {
      const rep1Backup = v2After.backupFiles.find(b => b.replaces === rep1Name)
      expect(rep1Backup).not.toBeUndefined()
      expect(h.files.get(`${PART_DIR}/${rep1Backup!.name}`)?.includes(expectedTag)).toBe(true)
    }

    // Pass 3 (0.1.3 force after lease expiry): retires pName with files: [] and drops rep1Name unsent.
    h.setWrite(null)
    h.setFetch(null)
    h.advance(15 * 60 * 1000)
    await h.newDist(`dist-rorig-${bkMode}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
    expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe('')
    if (
      bkMode === 'linked-backup' ||
      bkMode === 'diff-size-retired-linked-backup' ||
      bkMode === 'v1-store-linked-backup' ||
      bkMode === 'backup-truncated-after-read' ||
      bkMode === 'backup-v1-store-record' ||
      bkMode === 'live-orig-backup-v1-slot' ||
      bkMode === 'live-orig-late-backup-v1-slot' ||
      bkMode === 'late-rep-swap-syncs-v2'
    ) {
      expect(h.files.get(`${PART_DIR}/${bk1Name}`)).toBe('')
      if (bkMode === 'linked-backup') expect(h.count('C1_REUSED_GEN2')).toBe(1)
    }

    // Pass 4 (0.1.4 auto): recovers expectedTag exactly once from the verified backup.
    await h.newCurrent(`cur-rorig-${bkMode}-3`).backfill(['spill', 'retry'], 'auto')
    await h.newCurrent(`cur-rorig-${bkMode}-4`).backfill(['spill', 'retry'], 'auto')
    expect(h.count(expectedTag)).toBe(1)
    if (bkMode === 'reused-rep-same-size' || bkMode === 'late-rep-swap-syncs-v2') expect(h.count('C1_ORIG_UNIQUE')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }
})

test('interop: Phase 4 promotes replacement before truncating pre-existing backup and re-checks backup lease before writing', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)

  // Arm 1: writeFile(bOld, '') truncates on disk and then throws -> kept replacement must NOT be rolled back.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-throw-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-throw-c1.jsonl'
    const bOldName = 'spill-20200101T000000003Z-p4-throw-bk.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_THROW')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_THROW', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old4g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old4g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev4-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let truncatedThenThrew = false
    h.setWrite(async (p, text) => {
      if (p === `${PART_DIR}/${bOldName}` && text === '') {
        // Simulate truncation landing on disk before the syscall/close returns an error.
        h.files.set(p, '')
        truncatedThenThrew = true
        throw new Error('EIO after truncate landed')
      }
    })
    await h.newCurrent('cur-p4-throw-1').backfill(['spill', 'retry'], 'force')
    expect(truncatedThenThrew).toBe(true)
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe('')
    const liveBks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_P4_THROW'),
    )
    expect(liveBks.length).toBe(1)
    expect(liveBks[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)
  }

  // Arm 2: Foreign lease taken on old backup strictly AFTER Phase 4 pre-flight (during post-preflight readIfPresent) blocks truncation.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-lease-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-lease-c1.jsonl'
    const bOldName = 'spill-20200101T000000003Z-p4-lease-bk.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_LEASE')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_LEASE', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old5g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old5g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4-lease',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev5-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let standaloneWritten = false
    let preflightChecked = false
    let leasedAfterPreflight = false
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_P4_LEASE') && !text.includes(`"parent":"${c1Name}"`)) {
        standaloneWritten = true
      }
    })
    h.setStoreGet(async k => {
      if (standaloneWritten && !preflightChecked && k === `lease:${TABLE}/${bOldName}`) {
        preflightChecked = true
      }
    })
    h.setRead(async p => {
      if (preflightChecked && !leasedAfterPreflight && p === `${PART_DIR}/${bOldName}`) {
        leasedAfterPreflight = true
        h.store.set(`lease:${TABLE}/${bOldName}`, { by: 'foreign-bk', holder: 'fh', until: h.io.now() + 5 * 60 * 1000 })
      }
      return undefined
    })
    await h.newCurrent('cur-p4-lease-1').backfill(['spill', 'retry'], 'force')
    expect(standaloneWritten).toBe(true)
    expect(preflightChecked).toBe(true)
    expect(leasedAfterPreflight).toBe(true)
    // Foreign-leased bOld must remain untouched on disk and newly staged standalone copy rolled back.
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe(bOldText)
    const liveBks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_P4_LEASE'),
    )
    expect(liveBks.length).toBe(1)
    expect(liveBks[0]![0]).toBe(`${PART_DIR}/${bOldName}`)
  }

  // Arm 3: Different same-size generation swapped into bOldName during Phase 4's post-read lease lookup is NOT truncated.
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-swap-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-swap-c1.jsonl'
    const bOldName = 'spill-20200101T000000003Z-p4-swap-bk.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_SWAP1')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_SWAP1', `${c1Name}#1`)], c1Name)[0]!
    const bSwappedText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_SWAP2', `${c1Name}#1`)], c1Name)[0]!
    expect(utf8Length(bSwappedText)).toBe(utf8Length(bOldText))
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old6g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old6g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4-swap',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev6-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let standaloneWritten = false
    let leaseLookups = 0
    let swappedDuringLeaseCheck = false
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_P4_SWAP1') && !text.includes(`"parent":"${c1Name}"`)) {
        standaloneWritten = true
      }
    })
    h.setStoreGet(async k => {
      if (standaloneWritten && k === `lease:${TABLE}/${bOldName}`) {
        leaseLookups += 1
        // Lookup 1 is Phase 4 pre-flight; lookup 2 is the post-read `bLeaseNow` check right before truncate.
        if (leaseLookups === 2 && !swappedDuringLeaseCheck) {
          swappedDuringLeaseCheck = true
          h.files.set(`${PART_DIR}/${bOldName}`, bSwappedText)
        }
      }
    })
    await h.newCurrent('cur-p4-swap-1').backfill(['retry'], 'force')
    expect(swappedDuringLeaseCheck).toBe(true)
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe(bSwappedText)
    expect(h.count('C1_P4_SWAP2')).toBe(0)
  }

  // Arm 4: writeFile(bOld, '') throws BEFORE modifying bytes -> newly staged replacement IS rolled back (no duplicate).
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-preerr-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-preerr-c1.jsonl'
    const bOldName = 'spill-20200101T000000003Z-p4-preerr-bk.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_PREERR')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_PREERR', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old7g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old7g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4-preerr',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev7-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let threwBeforeTruncate = false
    h.setWrite(async (p, text) => {
      if (p === `${PART_DIR}/${bOldName}` && text === '') {
        threwBeforeTruncate = true
        throw new Error('EACCES before modifying disk bytes')
      }
    })
    await h.newCurrent('cur-p4-preerr-1').backfill(['spill', 'retry'], 'force')
    expect(threwBeforeTruncate).toBe(true)
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe(bOldText)
    const liveBks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_P4_PREERR'),
    )
    // Only the untouched bOld survives; the newly staged standalone replacement was rolled back.
    expect(liveBks.length).toBe(1)
    expect(liveBks[0]![0]).toBe(`${PART_DIR}/${bOldName}`)
  }
})

test('interop: parent lease holds childGuard on auto/command and Phase 3b happy-path sibling lease aborts before dispatch', async () => {
  // Arm 1: auto/command must NOT clear childGuard via parentHeldForUnwrittenV1Child while another session holds parent lease.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-cg-please-p.jsonl'
    const c1Name = 'rejected-20200101T000000002Z-cg-please-c1.jsonl'
    const c2Name = 'spill-20200101T000000003Z-cg-please-c2.jsonl'
    const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
    const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
    const pSize = utf8Length(pGen1)
    const c1Text = packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pGen2)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'RW',
      reasons: ['bad row'],
      refusals: [0],
      size: pSize,
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [c1Name, c2Name],
      written: [true, false],
    })
    h.files.set(
      `${PART_DIR}/outcome-done-oldg-${c1Name}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(c1Text),
          sending: true,
          by: 'oldg',
          stagedFor: pName,
          stagedForSize: pSize,
          childGuard: true,
          attempt: 'att-pl-1',
        })}\n`,
      ),
    )
    h.store.set(`lease:${TABLE}/${pName}`, { by: 'other-parent-owner', holder: 'oph', until: h.io.now() + 5 * 60 * 1000 })
    const rCmd = await h.newCurrent('cur-cg-pl-1').backfill(['spill', 'retry', 'rejected'], 'command')
    expect(rCmd.error).toBeNull()
    expect(h.count('OLD_A')).toBe(0)
    expect(h.count('NEW_A')).toBe(0)
  }

  // Arm 2: Foreign sibling lease acquired during Phase 3b happy-path link check aborts staging before dispatch.
  {
    const idA = 'a'.repeat(32)
    const idB = 'b'.repeat(32)
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p3b-lease-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p3b-lease-c1.jsonl'
    const c2Name = 'spill-20200101T000000003Z-p3b-lease-c2.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P3BL')], pName)[0]!
    const c2Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C2_P3BL')], pName)[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name, c2Name], written: [true, true] })
    let c2Staged = false
    let c2Phase3aRead = false
    let injectedLease = false
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C2_P3BL')) c2Staged = true
    })
    h.setRead(async p => {
      if (c2Staged && p === `${PART_DIR}/${c2Name}`) c2Phase3aRead = true
      return undefined
    })
    h.setStoreGet(async k => {
      // Right after Phase 3a finishes reading C2, Phase 3b calls canLinkBackupToChild(C1) -> storeGet(replayed:.../C1).
      if (c2Phase3aRead && !injectedLease && k === `replayed:${TABLE}/${c1Name}`) {
        injectedLease = true
        h.store.set(`lease:${TABLE}/${c2Name}`, { by: 'late-sib-owner', holder: 'lsh', until: h.io.now() + 5 * 60 * 1000 })
      }
    })
    const r = await h.newCurrent('cur-p3b-lease-1').backfill(['spill', 'retry'], 'force')
    expect(injectedLease).toBe(true)
    // Must abort before HTTP dispatch of P (A/B not sent) and roll back newly staged backups.
    expect(h.count('A')).toBe(0)
    expect(r.held ?? '').toContain(c2Name)
  }
})

test('interop: unrelated parent leftover delivered by 0.1.3 does not release a reconciled child backup whose rows only overlapped landed parent rows', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const idX = 'c'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-unrel-lp-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-unrel-lp-c1.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B'), dlLine('retry', idX, 'X_LEFTOVER')])[0]!
  // C1 overlaps ONLY landed row B (not X_LEFTOVER), plus its unique ID-less row.
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_UNREL_LP')], pName)[0]!
  const pSize = utf8Length(pText)
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LLL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LLW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
  // Fail X_LEFTOVER with 503 during Pass 1 so P emits parent leftover L_p containing [X_LEFTOVER].
  h.setFetch(async tags => (tags.includes('X_LEFTOVER') ? { status: 503, text: 'transient 503 on X' } : null))
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
  })

  await h.newCurrent('cur-unrel-lp-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'force')
  expect(h.count('C1_UNREL_LP')).toBe(0)
  const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
    files: string[]
    adopted: string[]
    reconciled: { name: string; needsLeftovers?: boolean }[]
    backupFiles: { name: string; replaces: string }[]
  }
  const rep1Name = v2.adopted[0]!
  const lpName = v2.files.find(n => !v2.adopted.includes(n))!
  const bk1Name = v2.backupFiles.find(b => b.replaces === c1Name)!.name
  expect(lpName).not.toBeUndefined()
  expect(v2.reconciled.find(rec => rec.name === c1Name)?.needsLeftovers).toBe(false)

  // Hold BK1; 0.1.3 retires P and delivers parent leftover L_p (digest-less 0.1.3 done mark);
  // 0.1.4 auto delivers R_rep1 (explicit sourceFnv1a retirement mark).
  h.setFetch(null)
  h.setWrite(null)
  h.store.set(`lease:${TABLE}/${bk1Name}`, { by: 'hold-bk1', until: h.io.now() + 10 * 60 * 1000 })
  await h.newDist('dist-unrel-lp-1').backfill(['retry'], 'force')
  expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${lpName}`)).toBe('')
  expect(h.count('X_LEFTOVER')).toBe(1)
  await h.newCurrent('cur-unrel-lp-2').backfill(['spill'], 'auto')
  expect(h.count('C1_UNREL_LP')).toBe(1)
  expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe('')

  // Release BK1's lease: 0.1.4 auto must drop BK1 via R_rep1's verified mark despite L_p's digest-less 0.1.3 mark.
  h.advance(15 * 60 * 1000)
  await h.newCurrent('cur-unrel-lp-3').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-unrel-lp-4').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_UNREL_LP')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: per-leftover scoping keeps unrelated 0.1.3-delivered parent leftover part from releasing a reconciled child backup', async () => {
  const idA = 'a'.repeat(32)
  const idR = 'd'.repeat(32)
  const idX = 'e'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-part-scope-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-part-scope-c1.jsonl'
  // P emits TWO non-adopted leftover parts: L_rej (rejected row R_OVERLAP) and L_retry (503 row X_UNREL).
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idR, 'R_OVERLAP'), dlLine('retry', idX, 'X_UNREL')])[0]!
  // C1 overlaps ONLY R_OVERLAP (in L_rej), NOT X_UNREL (in L_retry), plus its unique ID-less row.
  const c1Text = packFiles([dlLine('spill', idR, 'R_OVERLAP'), dlLine('spill', null, 'C1_PART_SCOPED')], pName)[0]!
  const pSize = utf8Length(pText)
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LLL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LLW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
  h.setFetch(async tags => {
    if (tags.includes('R_OVERLAP')) {
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad schema' }] }] }) }
    }
    if (tags.includes('X_UNREL')) return { status: 503, text: 'transient 503 on X_UNREL' }
    return null
  })
  h.setStoreDelete(async k => {
    if (k === `replayed:${TABLE}/${pName}`) throw new Error('v1 delete unavailable')
  })
  h.setStoreSet(async k => {
    if (k === `replayed:${TABLE}/${pName}`) throw new Error('v1 neutralize unavailable')
  })
  h.setWrite(async (p, text) => {
    // Block every retired write on pName (both outcome-done- and v1 shadow outcome-) so only
    // the unretired v2 record carries `reconciled` + `leftoverParts` when 0.1.3 retires pName.
    if (p.includes('/outcome') && p.endsWith(pName) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
    // Keep the staged backup alive across the simulated crash before backup cleanup finishes
    // so the test exercises live backup release via `leftoverParts` rather than an already-empty file.
    if (p !== `${PART_DIR}/${c1Name}` && p.includes('/spill-') && text === '') throw new Error('stopped before backup cleanup')
  })

  await h.newCurrent('cur-pscope-1', { maxBatchRows: 1 }).backfill(['spill', 'retry'], 'force')
  expect(h.count('C1_PART_SCOPED')).toBe(0)
  const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
    files: string[]
    adopted: string[]
    reconciled: { name: string; needsLeftovers?: boolean; leftoverParts?: string[] }[]
    backupFiles: { name: string; replaces: string }[]
  }
  const rep1Name = v2.adopted[0]!
  const lRejName = v2.files.find(n => !v2.adopted.includes(n) && n.startsWith('rejected-'))!
  const lRetryName = v2.files.find(n => !v2.adopted.includes(n) && n.startsWith('retry-'))!
  const bk1Name = v2.backupFiles.find(b => b.replaces === c1Name)!.name
  expect(lRejName).not.toBeUndefined()
  expect(lRetryName).not.toBeUndefined()
  expect(v2.reconciled.find(rec => rec.name === c1Name)?.leftoverParts).toEqual([lRejName])
  expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${bk1Name}`) ?? '').not.toBe('')

  // Hold BK1; 0.1.3 force(['retry']) retires P via stuck v1 and genuinely delivers
  // standalone unrelated L_retry (writing a digest-less 0.1.3 done mark for L_retry);
  // 0.1.4 delivers R_rep1 and L_rej (explicit sourceFnv1a retirement marks).
  h.setFetch(null)
  h.setWrite(null)
  h.setStoreDelete(null)
  h.setStoreSet(null)
  h.store.set(`lease:${TABLE}/${bk1Name}`, { by: 'hold-bk1', until: h.io.now() + 10 * 60 * 1000 })
  await h.newDist('dist-pscope-1').backfill(['retry'], 'force')
  expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${lRetryName}`)).toBe('')
  expect(h.count('X_UNREL')).toBe(1)
  const lRetryDoneMarks = [...h.files.entries()].filter(
    ([k, v]) => k.includes('/outcome-done-') && k.endsWith(`-${lRetryName}`) && v.includes('"retired":true'),
  )
  expect(lRetryDoneMarks.length).toBeGreaterThan(0)
  expect(lRetryDoneMarks.every(([, v]) => !v.includes('"sourceFnv1a"'))).toBe(true)

  const rOverlapBefore = h.count('R_OVERLAP')
  await h.newCurrent('cur-pscope-2').backfill(['spill', 'rejected'], 'command')
  expect(h.count('C1_PART_SCOPED')).toBe(1)
  expect(h.count('R_OVERLAP')).toBe(rOverlapBefore + 1)
  expect(h.files.get(`${PART_DIR}/${rep1Name}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${lRejName}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${bk1Name}`) ?? '').not.toBe('')

  // Release BK1's lease: 0.1.4 auto must drop BK1 via R_rep1 + L_rej's verified marks despite L_retry's digest-less 0.1.3 mark.
  h.advance(15 * 60 * 1000)
  await h.newCurrent('cur-pscope-3').backfill(['spill', 'retry', 'rejected'], 'auto')
  await h.newCurrent('cur-pscope-4').backfill(['spill', 'retry', 'rejected'], 'auto')
  expect(h.count('C1_PART_SCOPED')).toBe(1)
  expect(h.count('R_OVERLAP')).toBe(rOverlapBefore + 1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: disk-candidate backup reuse rejects candidates with unfinished v1 replay slots so Phase 4 never drops good backup for poisoned copy', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  const h = makeInteropHarness()
  const pName = 'retry-20200101T000000001Z-diskslot-p.jsonl'
  const c1Name = 'spill-20200101T000000002Z-diskslot-c1.jsonl'
  const bGoodName = 'spill-20200101T000000003Z-diskslot-bgood.jsonl'
  const bPoisonName = 'spill-20200101T000000004Z-diskslot-bpoison.jsonl'
  const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
  const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_DISKSLOT_UNIQ')], pName)[0]!
  const pSize = utf8Length(pText)
  const c1Size = utf8Length(c1Text)
  const bGoodText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_DISKSLOT_UNIQ', `${c1Name}#1`)])[0]!
  const bPoisonText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_DISKSLOT_UNIQ', `${c1Name}#1`)], c1Name)[0]!
  h.files.set(`${PART_DIR}/${pName}`, pText)
  h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
  h.files.set(`${PART_DIR}/${bGoodName}`, bGoodText)
  h.files.set(`${PART_DIR}/${bPoisonName}`, bPoisonText)
  const markFor = (by: string, text: string, attempt: string) =>
    sealFile(
      `${JSON.stringify({
        size: utf8Length(text),
        sending: true,
        by,
        stagedFor: pName,
        stagedForSize: pSize,
        replaces: c1Name,
        backup: true,
        backupForSize: c1Size,
        backupForFnv1a: fnv1a(c1Text),
        backupFnv1a: fnv1a(text),
        attempt,
      })}\n`,
    )
  h.files.set(`${PART_DIR}/outcome-done-gd1g-${bGoodName}`, markFor('gd1g', bGoodText, 'att-good'))
  h.files.set(`${PART_DIR}/outcome-done-ps1g-${bPoisonName}`, markFor('ps1g', bPoisonText, 'att-poison'))
  // Poison bPoisonName with an unfinished v1 store checkpoint that 0.1.3 would resume and empty unsent.
  h.store.set(`replayed:${TABLE}/${bPoisonName}`, {
    fates: 'LL',
    reasons: [],
    refusals: [],
    size: utf8Length(bPoisonText),
    waits: false,
    seq: 1,
    files: [],
    written: [],
  })
  h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE}/${pName}`, {
    fates: 'LW',
    reasons: [],
    refusals: [],
    size: pSize,
    waits: true,
    seq: 5,
    files: [c1Name],
    written: [true],
    backupFiles: [{ name: bGoodName, size: utf8Length(bGoodText), fnv1a: fnv1a(bGoodText), replaces: c1Name, replacesSize: c1Size, replacesFnv1a: fnv1a(c1Text) }],
  })
  // Fail markDone so 0.1.3 force exercises the staged backup after v2 persists.
  h.setWrite(async (p, text) => {
    if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
    if (p === `${PART_DIR}/${pName}` && text === '') throw new Error('stopped before truncating parent')
  })

  await h.newCurrent('cur-diskslot-1').backfill(['spill', 'retry'], 'force')
  const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { backupFiles: { name: string; replaces: string }[] }
  // Must NOT reuse bPoisonName; must stage a fresh clean backup for c1Name.
  expect(v2.backupFiles.some(b => b.name === bPoisonName)).toBe(false)
  const freshBk = v2.backupFiles.find(b => b.replaces === c1Name && b.name !== bGoodName && b.name !== bPoisonName)
  expect(freshBk).not.toBeUndefined()
  expect(h.files.get(`${PART_DIR}/${freshBk!.name}`)?.includes('C1_DISKSLOT_UNIQ')).toBe(true)

  // 0.1.3 force retires P, drops c1Name, and consumes bPoisonName via its stale v1 record;
  // 0.1.4 auto still delivers C1_DISKSLOT_UNIQ exactly once from freshBk.
  h.setWrite(null)
  h.advance(15 * 60 * 1000)
  await h.newDist('dist-diskslot-1').backfill(['spill', 'retry'], 'force')
  expect(h.files.get(`${PART_DIR}/${pName}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${c1Name}`)).toBe('')
  expect(h.files.get(`${PART_DIR}/${bPoisonName}`)).toBe('')
  await h.newCurrent('cur-diskslot-2').backfill(['spill', 'retry'], 'auto')
  await h.newCurrent('cur-diskslot-3').backfill(['spill', 'retry'], 'auto')
  expect(h.count('C1_DISKSLOT_UNIQ')).toBe(1)
  expect(h.liveDataFiles()).toEqual([])
})

test('interop: Phase 4 tracks per-part old-to-new rollback coverage, promotes already-empty old part against 503, and claims backup lease before final read', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)

  // Arm 1: Pre-existing old backup is already empty when Phase 4 reads it -> replacement MUST be promoted
  // out of newlyStaged so a subsequent HTTP 503 rollback in backfillFile leaves 1 live backup (never 0).
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-alremp-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-alremp-c1.jsonl'
    const bOldName = 'spill-20200101T000000003Z-p4-alremp-bk.jsonl'
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_ALREMP')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_ALREMP', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old7g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old7g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4-alremp',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev7-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let standaloneWritten = false
    let emptiedBeforePhase4Read = false
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_P4_ALREMP') && !text.includes(`"parent":"${c1Name}"`)) {
        standaloneWritten = true
      }
    })
    h.setRead(async p => {
      if (standaloneWritten && !emptiedBeforePhase4Read && p === `${PART_DIR}/${bOldName}`) {
        emptiedBeforePhase4Read = true
        h.files.set(p, '')
        return ''
      }
      return undefined
    })
    h.setFetch(async () => ({ status: 503, text: 'upstream 503 after Phase 4' }))
    await h.newCurrent('cur-p4-alremp-1').backfill(['retry'], 'force')
    expect(emptiedBeforePhase4Read).toBe(true)
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe('')
    const liveBks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('C1_P4_ALREMP'),
    )
    expect(liveBks.length).toBe(1)
    expect(liveBks[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)
  }

  // Arm 2: Multi-part backup transition (oldPart0, oldPart1 -> newPart0, newPart1):
  // truncating oldPart0 succeeds, then truncating oldPart1 fails before modifying bytes.
  // newPart0 MUST survive (promoted), while newPart1 MUST roll back so part 1 is not duplicated!
  {
    const h = makeInteropHarness()
    const pName = 'retry-20200101T000000001Z-p4-mpart-p.jsonl'
    const c1Name = 'spill-20200101T000000002Z-p4-mpart-c1.jsonl'
    const bOld0Name = 'spill-20200101T000000003Z-p4-mpart-bk0.jsonl'
    const bOld1Name = 'spill-20200101T000000004Z-p4-mpart-bk1.jsonl'
    const bigPad = 'p'.repeat(1_575_000)
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = sealFile(`${[dlLine('spill', null, `MP0-${bigPad}`), dlLine('spill', null, `MP1-${bigPad}`)].join('\n')}\n`, pName)
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const oldLinkedParts = packFiles(
      [
        dlLine('spill', `dl:${c1Name}#0`, `MP0-${bigPad}`, `${c1Name}#0`),
        dlLine('spill', `dl:${c1Name}#1`, `MP1-${bigPad}`, `${c1Name}#1`),
      ],
      c1Name,
    )
    expect(oldLinkedParts.length).toBe(2)
    const bOld0Text = oldLinkedParts[0]!
    const bOld1Text = oldLinkedParts[1]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOld0Name}`, bOld0Text)
    h.files.set(`${PART_DIR}/${bOld1Name}`, bOld1Text)
    const seedOldMark = (name: string, text: string, by: string) =>
      h.files.set(
        `${PART_DIR}/outcome-done-${by}-${name}`,
        sealFile(
          `${JSON.stringify({
            size: utf8Length(text),
            sending: true,
            by,
            stagedFor: pName,
            stagedForSize: pSize,
            replaces: c1Name,
            backup: true,
            backupForSize: c1Size,
            backupForFnv1a: fnv1a(c1Text),
            backupFnv1a: fnv1a(text),
            attempt: 'old-mpart',
          })}\n`,
        ),
      )
    seedOldMark(bOld0Name, bOld0Text, 'mp0g')
    seedOldMark(bOld1Name, bOld1Text, 'mp1g')
    h.files.set(
      `${PART_DIR}/outcome-done-prevmp-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let blockedOld1 = false
    h.setWrite(async (p, text) => {
      if (p === `${PART_DIR}/${bOld1Name}` && text === '') {
        blockedOld1 = true
        throw new Error('EIO before truncating old part 1')
      }
    })
    await h.newCurrent('cur-p4-mpart-1').backfill(['retry'], 'force')
    expect(blockedOld1).toBe(true)
    expect(h.files.get(`${PART_DIR}/${bOld0Name}`)).toBe('')
    expect(h.files.get(`${PART_DIR}/${bOld1Name}`)).toBe(bOld1Text)
    const liveMp0Bks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('MP0-'),
    )
    const liveMp1Bks = [...h.files.entries()].filter(
      ([k, v]) => k.includes('/spill-') && !k.includes('/outcome') && !k.endsWith(`/${c1Name}`) && v.includes('MP1-'),
    )
    // Part 0 has exactly 1 live backup (newPart0 promoted); Part 1 has exactly 1 live backup (oldPart1 intact, newPart1 rolled back).
    expect(liveMp0Bks.length).toBe(1)
    expect(liveMp0Bks[0]![1].includes(`"parent":"${c1Name}"`)).toBe(false)
    expect(liveMp1Bks.length).toBe(1)
    expect(liveMp1Bks[0]![0]).toBe(`${PART_DIR}/${bOld1Name}`)
  }

  // Arm 3: Phase 4 claims a temporary backup lease BEFORE its final read (`resFinal`),
  // catches a racing lease overwrite before truncating, and releases its lease on completion.
  // Arm 3: Phase 4 claims temporary backup lease before resFinal and aborts truncation
  // if the lease is overwritten by a foreign session, deleted, or expired during resFinal.
  for (const leaseLossMode of ['foreign-overwrite', 'deleted-lease', 'expired-lease'] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-p4-claim-${leaseLossMode}-p.jsonl`
    const c1Name = `spill-20200101T000000002Z-p4-claim-${leaseLossMode}-c1.jsonl`
    const bOldName = `spill-20200101T000000003Z-p4-claim-${leaseLossMode}-bk.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_CLAIM')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_CLAIM', `${c1Name}#1`)], c1Name)[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(
      `${PART_DIR}/outcome-done-old8g-${bOldName}`,
      sealFile(
        `${JSON.stringify({
          size: utf8Length(bOldText),
          sending: true,
          by: 'old8g',
          stagedFor: pName,
          stagedForSize: pSize,
          replaces: c1Name,
          backup: true,
          backupForSize: c1Size,
          backupForFnv1a: fnv1a(c1Text),
          backupFnv1a: fnv1a(bOldText),
          attempt: 'old-p4-claim',
        })}\n`,
      ),
    )
    h.files.set(
      `${PART_DIR}/outcome-done-prev8-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })
    let sawOwnLeaseDuringFinalRead = false
    let disruptedLeaseDuringFinalRead = false
    let phase4ReadCount = 0
    let standaloneWritten = false
    h.setWrite(async (p, text) => {
      if (p.includes('/spill-') && !p.includes('/outcome') && text.includes('C1_P4_CLAIM') && !text.includes(`"parent":"${c1Name}"`)) {
        standaloneWritten = true
      }
    })
    h.setRead(async p => {
      if (standaloneWritten && p === `${PART_DIR}/${bOldName}`) {
        phase4ReadCount += 1
        // Read 1 is pre-claim read; Read 2 is `resFinal` under our claimed backup lease.
        if (phase4ReadCount === 2) {
          const leaseKey = `lease:${TABLE}/${bOldName}`
          const lease = h.store.get(leaseKey) as { by?: string; holder?: string; until?: number } | undefined
          if (lease?.by === `cur-p4-claim-${leaseLossMode}-1` && typeof lease.holder === 'string' && lease.holder !== '' && (lease.until ?? 0) > h.io.now()) {
            sawOwnLeaseDuringFinalRead = true
          }
          disruptedLeaseDuringFinalRead = true
          if (leaseLossMode === 'foreign-overwrite') {
            h.store.set(leaseKey, { by: 'racer', holder: 'rh', until: h.io.now() + 5 * 60 * 1000 })
          } else if (leaseLossMode === 'deleted-lease') {
            h.store.delete(leaseKey)
          } else {
            h.advance(15 * 60 * 1000)
          }
        }
      }
      return undefined
    })
    await h.newCurrent(`cur-p4-claim-${leaseLossMode}-1`).backfill(['retry'], 'force')
    expect(sawOwnLeaseDuringFinalRead).toBe(true)
    expect(disruptedLeaseDuringFinalRead).toBe(true)
    // Foreign overwrite, deleted lease, or expired lease during `resFinal` blocked truncation of bOldName.
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe(bOldText)
    // After any foreign lease expires, a clean pass claims, truncates bOldName, and releases its temporary lease.
    h.setRead(null)
    h.setWrite(null)
    h.advance(15 * 60 * 1000)
    await h.newCurrent(`cur-p4-claim-${leaseLossMode}-2`).backfill(['retry'], 'force')
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe('')
    expect(h.store.get(`lease:${TABLE}/${bOldName}`)).toBeUndefined()
  }
})

test('interop: resumed writeOut batches directory listing across many synthesized replacements and backups', async () => {
  const measureBatch = async (N: number): Promise<number> => {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-batchlist-${N}-p.jsonl`
    const pText = packFiles([dlLine('retry', 'a'.repeat(32), 'A')])[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    const adopted: string[] = []
    const reconciled: { name: string; size: number; fnv1a: string }[] = []
    const deliverableRefs: { name: string; size: number; fnv1a: string; replaces: string }[] = []
    const backupFiles: { name: string; size: number; fnv1a: string; replaces: string; replacesSize: number; replacesFnv1a: string }[] = []
    for (let i = 0; i < N; i += 1) {
      const idx = String(i).padStart(2, '0')
      const cName = `spill-20200101T0000001${idx}Z-batchlist-c${idx}.jsonl`
      const repName = `spill-20200101T0000002${idx}Z-batchlist-r${idx}.jsonl`
      const bkName = `spill-20200101T0000003${idx}Z-batchlist-b${idx}.jsonl`
      const cText = packFiles([dlLine('spill', null, `BATCH_ROW_${idx}`)], pName)[0]!
      const repText = packFiles([dlLine('spill', `dl:${cName}#0`, `BATCH_ROW_${idx}`, `${cName}#0`)], pName)[0]!
      const bkText = packFiles([dlLine('spill', `dl:${cName}#0`, `BATCH_ROW_${idx}`, `${cName}#0`)])[0]!
      h.files.set(`${PART_DIR}/${cName}`, '')
      h.files.set(`${PART_DIR}/${repName}`, repText)
      h.files.set(`${PART_DIR}/${bkName}`, bkText)
      h.files.set(
        `${PART_DIR}/outcome-done-bg${idx}g-${bkName}`,
        sealFile(
          `${JSON.stringify({
            size: utf8Length(bkText),
            sending: true,
            by: `bg${idx}g`,
            stagedFor: pName,
            stagedForSize: pSize,
            replaces: cName,
            backup: true,
            backupForSize: utf8Length(cText),
            backupForFnv1a: fnv1a(cText),
            backupFnv1a: fnv1a(bkText),
            attempt: 'att-batch',
          })}\n`,
        ),
      )
      adopted.push(repName)
      reconciled.push({ name: cName, size: utf8Length(cText), fnv1a: fnv1a(cText) })
      deliverableRefs.push({ name: repName, size: utf8Length(repText), fnv1a: fnv1a(repText), replaces: cName })
      backupFiles.push({
        name: bkName,
        size: utf8Length(bkText),
        fnv1a: fnv1a(bkText),
        replaces: cName,
        replacesSize: utf8Length(cText),
        replacesFnv1a: fnv1a(cText),
      })
    }
    h.store.set(`replayed:v2:${TABLE}/${pName}`, {
      fates: 'L',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(pText),
      waits: true,
      seq: 4,
      files: adopted,
      written: adopted.map(() => true),
      adopted,
      reconciled,
      deliverableRefs,
      backupFiles,
      attempts: ['att-batch'],
    })
    // Fail retired mark so only resumed writeOut's guard/backup verification runs on pName.
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
      if (p === `${PART_DIR}/${pName}` && text === '') throw new Error('stopped before truncating parent')
    })
    let listDirCalls = 0
    h.setListDir(async dir => {
      if (dir === PART_DIR) listDirCalls += 1
    })
    let backupLeaseWrites = 0
    h.setStoreSet(async k => {
      if (k.startsWith(`lease:${TABLE}/spill-`) && k.includes('-batchlist-b')) backupLeaseWrites += 1
    })
    await h.newCurrent(`cur-batchlist-${N}`).backfill(['retry'], 'auto')
    h.setListDir(null)
    h.setStoreSet(null)
    // Every one of the N predecessor backups must actually be leased & verified (not skipped via invalid adopted[]).
    expect(backupLeaseWrites).toBe(N)
    for (const bk of backupFiles) {
      expect(h.store.get(`lease:${TABLE}/${bk.name}`)).toBeUndefined()
    }
    const v2After = h.store.get(`replayed:v2:${TABLE}/${pName}`) as { backupFiles: { name: string }[] }
    expect(v2After.backupFiles.length).toBe(N)
    return listDirCalls
  }
  const callsFor4 = await measureBatch(4)
  const callsFor12 = await measureBatch(12)
  // Batched verification must be strictly O(1) in directory listings regardless of backup count.
  expect(callsFor12).toBe(callsFor4)
  expect(callsFor12).toBeLessThanOrEqual(8)
})

test('interop: v1SkippedBackup survives 0.1.3 advance stopping before retirement, reports backup write failure with held parent lease, and reuses leased backup without duplication', async () => {
  // Arm 1: 0.1.3 advances stale v1 to written: [true, true] (dropping sourceFnv1a) and
  // stops right before retirement -> 0.1.4 auto retires P_gen2 WITHOUT certifying P_gen2's
  // digest, preserving the staged v1SkippedBackup so NEW_A is delivered exactly once.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1adv-stop-p.jsonl'
    const c1Name = 'rejected-20200101T000000002Z-v1adv-stop-c1.jsonl'
    const c2Name = 'spill-20200101T000000003Z-v1adv-stop-c2.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
    const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
    expect(utf8Length(pGen2)).toBe(utf8Length(pGen1))
    h.files.set(pPath, pGen2)
    h.files.set(`${PART_DIR}/${c1Name}`, packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'RW',
      reasons: ['bad row'],
      refusals: [0],
      size: utf8Length(pGen1),
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [c1Name, c2Name],
      written: [true, false],
    })
    h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'child-owner', until: h.io.now() + 5 * 60 * 1000 })

    // Pass 1 (0.1.4 auto): stages v1SkippedBackup for NEW_A and holds P_gen2.
    await h.newCurrent('cur-v1adv-1').backfill(['spill', 'retry', 'rejected'], 'auto')
    const stagedBks = [...h.files.entries()].filter(
      ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
    )
    expect(stagedBks.length).toBe(1)

    // Arm 1b (Smaller #1): a foreign lease on the staged v1SkippedBackup must NOT make
    // the next 0.1.4 hold pass stage a second duplicate backup with the same event_id.
    const stagedBkName = stagedBks[0]![0].slice(`${PART_DIR}/`.length)
    h.store.set(`lease:${TABLE}/${stagedBkName}`, { by: 'other-scanner', until: h.io.now() + 5 * 60 * 1000 })
    await h.newCurrent('cur-v1adv-1b').backfill(['spill', 'retry', 'rejected'], 'auto')
    const stagedBksAfterLease = [...h.files.entries()].filter(
      ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
    )
    expect(stagedBksAfterLease.length).toBe(1)
    h.store.delete(`lease:${TABLE}/${stagedBkName}`)

    // Pass 2 (0.1.3 auto): writes c2Name (NEW_B) and advances v1 to written: [true, true]
    // (dropping sourceFnv1a!), then stops right before writing P's retired done mark / truncating P.
    h.setWrite(async (p, text) => {
      if ((p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) || (p === pPath && text === '')) {
        throw new Error('0.1.3 stopped before retirement')
      }
    })
    await h.newDist('dist-v1adv-1').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(pPath)).toBe(pGen2)
    expect(h.files.get(`${PART_DIR}/${c2Name}`)?.includes('NEW_B')).toBe(true)
    const v1AfterDist = h.store.get(`replayed:${TABLE}/${pName}`) as { sourceFnv1a?: string; written?: boolean[] }
    expect(v1AfterDist.sourceFnv1a).toBeUndefined()
    expect(v1AfterDist.written).toEqual([true, true])

    // Pass 3 (0.1.4 auto): resumes digest-less v1, retires P_gen2 WITHOUT certifying fnv1a(pGen2),
    // keeps v1SkippedBackup alive, and delivers NEW_A + NEW_B (and OLD_A after lease expiry).
    h.setWrite(null)
    await h.newCurrent('cur-v1adv-2').backfill(['spill', 'retry'], 'auto')
    await h.newCurrent('cur-v1adv-3').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(pPath)).toBe('')
    expect(h.count('NEW_A')).toBe(1)
    expect(h.count('NEW_B')).toBe(1)
    h.advance(15 * 60 * 1000)
    await h.newCurrent('cur-v1adv-4').backfill(['spill', 'retry', 'rejected'], 'command')
    expect(h.count('OLD_A')).toBe(1)
    expect(h.liveDataFiles()).toEqual([])
  }

  // Arm 2: Write failure inside stageV1SkippedBackups surfaces report.error and keeps the
  // parent lease so an immediate 0.1.3 auto pass cannot preempt P_gen2 and lose NEW_A.
  {
    const h = makeInteropHarness()
    const pName = 'spill-20200101T000000001Z-v1fail-p.jsonl'
    const c1Name = 'rejected-20200101T000000002Z-v1fail-c1.jsonl'
    const c2Name = 'spill-20200101T000000003Z-v1fail-c2.jsonl'
    const pPath = `${PART_DIR}/${pName}`
    const pGen1 = packFiles([dlLine('spill', null, 'OLD_A'), dlLine('spill', null, 'OLD_B')])[0]!
    const pGen2 = packFiles([dlLine('spill', null, 'NEW_A'), dlLine('spill', null, 'NEW_B')])[0]!
    h.files.set(pPath, pGen2)
    h.files.set(`${PART_DIR}/${c1Name}`, packFiles([dlLine('spill', null, 'OLD_A')], pName)[0]!)
    h.store.set(`replayed:${TABLE}/${pName}`, {
      fates: 'RW',
      reasons: ['bad row'],
      refusals: [0],
      size: utf8Length(pGen1),
      sourceFnv1a: fnv1a(pGen1),
      waits: true,
      seq: 2,
      files: [c1Name, c2Name],
      written: [true, false],
    })
    h.store.set(`lease:${TABLE}/${c1Name}`, { by: 'child-owner', until: h.io.now() + 5 * 60 * 1000 })

    // Fail the backup data write inside stageV1SkippedBackups.
    h.setWrite(async (p, text) => {
      if (p.startsWith(`${PART_DIR}/spill-`) && p !== pPath && text.includes('NEW_A')) {
        throw new Error('ENOSPC writing v1SkippedBackup')
      }
    })
    const rErr = await h.newCurrent('cur-v1fail-1').backfill(['spill', 'retry', 'rejected'], 'auto')
    expect(rErr.error ?? '').toContain('ENOSPC')
    // Parent lease must remain held so 0.1.3 auto cannot immediately grab P_gen2 and drop NEW_A!
    const keptLease = h.store.get(`lease:${TABLE}/${pName}`) as { by?: string; until?: number } | undefined
    expect(keptLease?.by).toBe('cur-v1fail-1')
    expect((keptLease?.until ?? 0) > h.io.now()).toBe(true)

    h.setWrite(null)
    await h.newDist('dist-v1fail-preempt').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(pPath)).toBe(pGen2)
    expect(h.count('NEW_B')).toBe(0)

    // Once disk error clears, the same 0.1.4 session stages the backup and releases the lease cleanly.
    const rOk = await h.newCurrent('cur-v1fail-1').backfill(['spill', 'retry', 'rejected'], 'auto')
    expect(rOk.error).toBeNull()
    expect(h.store.get(`lease:${TABLE}/${pName}`)).toBeUndefined()
    const stagedBks = [...h.files.entries()].filter(
      ([k, v]) => k.startsWith(`${PART_DIR}/spill-`) && k !== pPath && v.includes('NEW_A') && !v.includes('NEW_B'),
    )
    expect(stagedBks.length).toBe(1)
  }
})

test('interop: Phase 4 leases and rechecks reused disk candidate replay slots before truncating old backup', async () => {
  const idA = 'a'.repeat(32)
  const idB = 'b'.repeat(32)
  for (const poisonMode of ['v1-store-record', 'done-mark-file'] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-p4-reuse-${poisonMode}-p.jsonl`
    const c1Name = `spill-20200101T000000002Z-p4-reuse-${poisonMode}-c1.jsonl`
    const bOldName = `spill-20200101T000000003Z-p4-reuse-${poisonMode}-old.jsonl`
    const bReuseName = `spill-20200101T000000004Z-p4-reuse-${poisonMode}-cand.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idB, 'B')])[0]!
    const c1Text = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', null, 'C1_P4_REUSE')], pName)[0]!
    const pSize = utf8Length(pText)
    const c1Size = utf8Length(c1Text)
    const bOldText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_REUSE', `${c1Name}#1`)], c1Name)[0]!
    const bReuseText = packFiles([dlLine('spill', idB, 'B'), dlLine('spill', `dl:${c1Name}#1`, 'C1_P4_REUSE', `${c1Name}#1`)])[0]!
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    h.files.set(`${PART_DIR}/${bOldName}`, bOldText)
    h.files.set(`${PART_DIR}/${bReuseName}`, bReuseText)
    for (const [bName, bText, byMark] of [
      [bOldName, bOldText, 'old9g'],
      [bReuseName, bReuseText, 'cand9g'],
    ] as const) {
      h.files.set(
        `${PART_DIR}/outcome-done-${byMark}-${bName}`,
        sealFile(
          `${JSON.stringify({
            size: utf8Length(bText),
            sending: true,
            by: byMark,
            stagedFor: pName,
            stagedForSize: pSize,
            replaces: c1Name,
            backup: true,
            backupForSize: c1Size,
            backupForFnv1a: fnv1a(c1Text),
            backupFnv1a: fnv1a(bText),
            attempt: `att-${byMark}`,
          })}\n`,
        ),
      )
    }
    // Force standalone packing for c1Name via a different-size done mark on c1Name.
    h.files.set(
      `${PART_DIR}/outcome-done-prev9-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: c1Size + 77, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: h.io.now() })}\n`),
    )
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [c1Name], written: [true] })

    // Inject a legacy outcome (store record or retired done mark on disk) onto bReuseName
    // right after stageMissingParts leases and reads it (before Phase 4 truncates bOldName).
    let poisonedReuseCandidate = false
    let sawLeaseOnReuseCandidate = false
    h.setStoreSet(async (k, v) => {
      if (k === `lease:${TABLE}/${bReuseName}`) {
        const lease = v as { by?: string; holder?: string; until?: number } | undefined
        if (lease?.by === `cur-p4-reuse-${poisonMode}-1` && typeof lease.holder === 'string' && lease.holder !== '' && (lease.until ?? 0) > h.io.now()) {
          sawLeaseOnReuseCandidate = true
        }
      }
    })
    h.setRead(async p => {
      if (sawLeaseOnReuseCandidate && !poisonedReuseCandidate && p === `${PART_DIR}/${bReuseName}`) {
        poisonedReuseCandidate = true
        if (poisonMode === 'v1-store-record') {
          h.store.set(`replayed:${TABLE}/${bReuseName}`, {
            fates: 'LL',
            reasons: [],
            refusals: [],
            size: utf8Length(bReuseText),
            waits: false,
            seq: 1,
            files: [],
            written: [],
          })
        } else {
          h.files.set(
            `${PART_DIR}/outcome-done-leg013-${bReuseName}`,
            sealFile(
              `${JSON.stringify({
                fates: 'LL',
                reasons: [],
                refusals: [],
                size: utf8Length(bReuseText),
                waits: false,
                seq: 1,
                files: [],
                written: [],
                retired: true,
                retiredAt: h.io.now(),
              })}\n`,
            ),
          )
        }
      }
      return undefined
    })

    await h.newCurrent(`cur-p4-reuse-${poisonMode}-1`).backfill(['retry'], 'force')
    expect(sawLeaseOnReuseCandidate).toBe(true)
    expect(poisonedReuseCandidate).toBe(true)
    // Phase 4 must have rechecked bReuseName's replay slots under lease and aborted WITHOUT truncating bOldName!
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe(bOldText)
    expect(h.store.get(`lease:${TABLE}/${bReuseName}`)).toBeUndefined()

    // Next pass skips the now-unclean bReuseName, stages a clean standalone backup, truncates bOldName,
    // and delivers C1_P4_REUSE exactly once.
    h.setRead(null)
    h.setStoreSet(null)
    await h.newCurrent(`cur-p4-reuse-${poisonMode}-2`).backfill(['spill', 'retry'], 'force')
    await h.newCurrent(`cur-p4-reuse-${poisonMode}-3`).backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(`${PART_DIR}/${bOldName}`)).toBe('')
    expect(h.count('C1_P4_REUSE')).toBe(1)
  }
})

test('interop: empty synthesized replacement never re-submits parent-rejected row while rejected file stays parked', async () => {
  const idA = 'a'.repeat(32)
  const idRej = 'r'.repeat(32)
  for (const arm of ['live-orig', 'empty-orig-backup', 'empty-orig-with-deferred-pending-sibling'] as const) {
    const h = makeInteropHarness()
    const pName = `retry-20200101T000000001Z-emptrep-${arm}-p.jsonl`
    const c1Name = `spill-20200101T000000002Z-emptrep-${arm}-c1.jsonl`
    const c2Name = `spill-20200101T000000003Z-emptrep-${arm}-c2.jsonl`
    const pText = packFiles([dlLine('retry', idA, 'A'), dlLine('retry', idRej, 'R_OVERLAP')])[0]!
    const c1Text = packFiles([dlLine('spill', idRej, 'R_OVERLAP'), dlLine('spill', null, 'C1_UNIQUE')], pName)[0]!
    const c2Text = packFiles([dlLine('spill', idRej, 'R_OVERLAP'), dlLine('spill', null, 'C2_UNIQUE')], pName)[0]!
    const pSize = utf8Length(pText)
    h.files.set(`${PART_DIR}/${pName}`, pText)
    h.files.set(`${PART_DIR}/${c1Name}`, c1Text)
    const childNames = arm === 'empty-orig-with-deferred-pending-sibling' ? [c1Name, c2Name] : [c1Name]
    if (arm === 'empty-orig-with-deferred-pending-sibling') {
      h.files.set(`${PART_DIR}/${c2Name}`, c2Text)
    }
    h.store.set(`replayed:${TABLE}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE}/${pName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: childNames,
      written: childNames.map(() => true),
    })

    let rOverlapAttempts = 0
    h.setFetch(async tags => {
      if (tags.includes('R_OVERLAP')) {
        rOverlapAttempts += 1
        const idx = tags.indexOf('R_OVERLAP')
        return {
          status: 200,
          text: JSON.stringify({
            insertErrors: [{ index: idx, errors: [{ reason: 'invalid', message: 'schema refusal' }] }],
          }),
        }
      }
      return null
    })
    h.setWrite(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${pName}`) && text.includes('"retired":true')) throw new Error('stopped before retired mark')
      if (p === `${PART_DIR}/${pName}` && text === '') throw new Error('stopped before truncating parent')
    })

    // Pass 1 (0.1.4 force): refuses R_OVERLAP into rejected-* file, reconciles child(ren) into rep(s), stops before markDone.
    await h.newCurrent(`cur-emptrep-${arm}-1`).backfill(['spill', 'retry'], 'force')
    expect(rOverlapAttempts).toBe(1)
    const v2 = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
      files: string[]
      adopted: string[]
      deliverableRefs: { name: string; replaces?: string }[]
      backupFiles: { name: string; replaces: string }[]
    }
    const rep1Name = v2.deliverableRefs.find(d => d.replaces === c1Name)!.name
    const rejFileName = v2.files.find(f => f.startsWith('rejected-'))!
    expect(rejFileName).not.toBeUndefined()
    expect(h.files.get(`${PART_DIR}/${rejFileName}`)?.includes('R_OVERLAP')).toBe(true)
    expect(h.files.get(`${PART_DIR}/${rep1Name}`)?.includes('C1_UNIQUE')).toBe(true)
    expect(h.files.get(`${PART_DIR}/${rep1Name}`)?.includes('R_OVERLAP')).toBe(false)

    // Empty rep1Name unsent (and in Arm 2 / Arm 3 also empty c1Name so recovery must come from bk1).
    h.files.set(`${PART_DIR}/${rep1Name}`, '')
    if (arm !== 'live-orig') {
      h.files.set(`${PART_DIR}/${c1Name}`, '')
    }
    if (arm === 'empty-orig-with-deferred-pending-sibling') {
      const rep2Name = v2.deliverableRefs.find(d => d.replaces === c2Name)!.name
      // Empty rep2Name while c2Name stays live, and hold c2Name under a foreign lease so Phase 3
      // defers c2Name AFTER recording v2 with BOTH pendingReconcile: [c2Name] and emitRetiredAccountedKeys: true.
      h.files.set(`${PART_DIR}/${rep2Name}`, '')
      h.store.set(`lease:${TABLE}/${c2Name}`, { by: 'other-session', until: h.io.now() + 5 * 60 * 1000 })
      h.setWrite(null)
      await h.newCurrent(`cur-emptrep-${arm}-2`).backfill(['spill', 'retry'], 'auto')
      expect(h.files.get(`${PART_DIR}/${pName}`)).toBe(pText)
      const v2Deferred = h.store.get(`replayed:v2:${TABLE}/${pName}`) as {
        pendingReconcile?: { name: string }[]
        emitRetiredAccountedKeys?: boolean
      }
      expect(v2Deferred.pendingReconcile?.some(p => p.name === c2Name)).toBe(true)
      expect(v2Deferred.emitRetiredAccountedKeys).toBe(true)
      // Clear foreign lease on c2Name and finish recovery in a fresh session (resuming v2 with pendingReconcile non-empty!).
      h.store.delete(`lease:${TABLE}/${c2Name}`)
      await h.newCurrent(`cur-emptrep-${arm}-3`).backfill(['spill', 'retry'], 'auto')
      await h.newCurrent(`cur-emptrep-${arm}-4`).backfill(['spill', 'retry'], 'auto')
      expect(h.count('C1_UNIQUE')).toBe(1)
      expect(h.count('C2_UNIQUE')).toBe(1)
      expect(rOverlapAttempts).toBe(1)
      expect(h.files.get(`${PART_DIR}/${rejFileName}`)?.includes('R_OVERLAP')).toBe(true)
      expect(h.liveDataFiles()).toEqual([`${PART_DIR}/${rejFileName}`])
      continue
    }

    // Pass 2 + 3 (0.1.4 auto): recovers C1_UNIQUE exactly once WITHOUT re-submitting R_OVERLAP while rejected-* stays parked!
    h.setWrite(null)
    await h.newCurrent(`cur-emptrep-${arm}-2`).backfill(['spill', 'retry'], 'auto')
    await h.newCurrent(`cur-emptrep-${arm}-3`).backfill(['spill', 'retry'], 'auto')
    expect(h.count('C1_UNIQUE')).toBe(1)
    expect(rOverlapAttempts).toBe(1)
    expect(h.files.get(`${PART_DIR}/${rejFileName}`)?.includes('R_OVERLAP')).toBe(true)
    expect(h.liveDataFiles()).toEqual([`${PART_DIR}/${rejFileName}`])
  }
})
