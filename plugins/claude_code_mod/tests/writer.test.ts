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

// The background writer: retries, timeouts, auth refresh, table creation,
// dead-letter files, backfill and the end-of-session spill. No network: the
// insertAll answers are scripted.

import { type Engine, expect, test } from 'claude-code/testing'

import { type Config, deadLetterPartition, resolveConfig } from '../hooks/lib/config.ts'
import { type DeadLetterLine, encodeLines, fnv1a, packFiles, parseFile, sealFile } from '../hooks/lib/deadletter.ts'
import { buildRow } from '../hooks/lib/rows.ts'
import { formatStatus } from '../hooks/lib/status.ts'
import { type Io, Writer, recordOf } from '../hooks/lib/writer.ts'

import {
  ACCESS_TOKEN,
  COMMAND,
  DEAD_LETTER_DIR,
  DRY_RUN_DIR,
  GREETED,
  HOME,
  HOST_FILE_BYTES,
  OK_INSERT,
  OPTIONS,
  PARTITION_DIR,
  SESSION_ID,
  TABLE_REF,
  type World,
  byType,
  deadLetterFiles,
  deadLetterRows,
  json,
  setUp,
  startSession,
  utf8Bytes,
} from './helpers.ts'

const UNAVAILABLE = { status: 503, text: '{"error":{"code":503,"message":"The service is currently unavailable.","status":"UNAVAILABLE"}}' }
const BAD_REQUEST = {
  status: 400,
  text: '{"error":{"code":400,"message":"Invalid value at rows[0]","errors":[{"reason":"invalid"}],"status":"INVALID_ARGUMENT"}}',
}
const TABLE_MISSING = {
  status: 404,
  text: '{"error":{"code":404,"message":"Not found: Table my-gcp-project:agent_analytics.agent_events","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}',
}
const DATASET_MISSING = {
  status: 404,
  text: '{"error":{"code":404,"message":"Not found: Dataset my-gcp-project:agent_analytics","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}',
}
const DENIED = {
  status: 403,
  text: '{"error":{"code":403,"message":"Access Denied: Table my-gcp-project:agent_analytics.agent_events: Permission bigquery.tables.updateData denied","errors":[{"reason":"accessDenied"}],"status":"PERMISSION_DENIED"}}',
}
const UNAUTHENTICATED = { status: 401, text: '{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}' }

const statusText = async ($: Engine): Promise<string> => (await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND })).text ?? ''

// The mod keeps each dead-letter file to 3 MiB of UTF-8.
const FILE_CAP = 3 * 1024 * 1024

const END = { reason: 'prompt_input_exit', sessionId: SESSION_ID, resume: { id: SESSION_ID } } as const

// A row parked by an earlier session, and its dead-letter line.
const parkedRow = (eventId: string, sessionId: string) => ({
  timestamp: '2026-10-13T20:01:02.003Z',
  event_id: eventId,
  event_type: 'STATE_DELTA',
  agent: 'coding-agent',
  session_id: sessionId,
  invocation_id: null,
  user_id: 'dev',
  trace_id: null,
  span_id: 'bbbbbbbbbbbbbbbb',
  parent_span_id: null,
  content: '{"session_end":true}',
  content_parts: [],
  attributes: '{}',
  latency_ms: null,
  status: 'OK',
  error_message: null,
  is_truncated: false,
})
const parkedLine = (table: string, eventId: string, sessionId: string): string =>
  JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table, row: parkedRow(eventId, sessionId) })

// A dead-letter file as a writer that finished leaves it: its lines, then
// the completion record a replay checks for.
const parkedFile = (...lines: string[]): string => packFiles(lines)[0] as string

const OTHER_TABLE = 'other-project.private_dataset.agent_events'
const OTHER_OPTIONS = { project: 'other-project', dataset: 'private_dataset' }

// Parked rows for two tables: each table's files in its own subdirectory, a
// line for the other table inside this table's subdirectory (moved there by
// hand, say), and a file in the dead-letter directory itself.
function parkForTwoTables(world: World): { mine: string; theirs: string; mixed: string; loose: string } {
  const files = {
    mine: `${PARTITION_DIR}/spill-20261013T200102003Z-mine-1.jsonl`,
    theirs: `${DEAD_LETTER_DIR}/${OTHER_TABLE}/spill-20261013T200102003Z-theirs-1.jsonl`,
    mixed: `${PARTITION_DIR}/retry-20261013T200102003Z-mixed-1.jsonl`,
    loose: `${DEAD_LETTER_DIR}/spill-20261013T200102003Z-loose-1.jsonl`,
  }
  world.files.set(files.mine, parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'mine-session')))
  world.files.set(files.theirs, parkedFile(parkedLine(OTHER_TABLE, 'b'.repeat(32), 'their-session')))
  world.files.set(files.mixed, parkedFile(parkedLine(OTHER_TABLE, 'c'.repeat(32), 'their-session'), parkedLine(TABLE_REF, 'd'.repeat(32), 'mine-session')))
  world.files.set(files.loose, parkedFile(parkedLine(OTHER_TABLE, 'e'.repeat(32), 'their-session')))
  return files
}

// Credential-shaped failure text, put together at run time. SECRET must never
// reach a sink: dead-letter files, the status line, the debug log, the
// transcript, or a command's output.
const SECRET = ['canary', 'secret', 'x'.repeat(16)].join('-')
const LEAKY = `${['ya29', SECRET].join('.')} password=${SECRET}`

async function everySink($: Engine, world: World, extra: string[] = []): Promise<string[]> {
  return [...world.files.values(), ...world.statusLines.map(l => l ?? ''), ...world.logs, ...world.toasts, await statusText($), ...extra]
}

test('a failed request is retried with the same insertIds, then succeeds', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  expect(world.acceptedRows().length).toBe(0)
  await world.clock.advance(5000)
  const [first, second] = world.insertCalls()
  const ids = (call: typeof first) => (JSON.parse(call?.body ?? '{}') as { rows: { insertId: string }[] }).rows.map(r => r.insertId)
  expect(ids(second)).toEqual(ids(first))
  expect(world.acceptedRows().length).toBe(3)
  expect(deadLetterFiles(world).length).toBe(0)
  expect(await statusText($)).toMatch(/sent 3, dead-lettered 0/)
})

test('with automatic replay off, the status line for rows parked after retries says /bqaa-backfill sends them', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_AUTO_BACKFILL: 'false' })
  world.replies = [UNAVAILABLE]
  await startSession($)
  for (let t = 0; t < 6; t += 1) await world.clock.advance(5000)
  expect(deadLetterRows(world).length).toBe(1)
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: 1 row\(s\) parked after retries \(HTTP 503: .*\); \/bqaa-backfill sends them$/)
})

test('rows that exhaust their retries go to a dead-letter file, and are replayed once BigQuery is back', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE, UNAVAILABLE, UNAVAILABLE, UNAVAILABLE, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  for (let t = 0; t < 4; t += 1) await world.clock.advance(5000)
  // Four attempts (one plus three retries), then parked on disk.
  expect(world.insertCalls().length).toBe(4)
  const files = deadLetterFiles(world)
  expect(files.length).toBe(1)
  const [path, text] = files[0] as [string, string]
  expect(path.startsWith(`${PARTITION_DIR}/retry-`)).toBe(true)
  // Every line but the last, the file's completion record, holds a row.
  const lines = text
    .trim()
    .split('\n')
    .map(l => JSON.parse(l) as { kind: string; attempts: number; table: string; reason: string; row?: { event_type: string } })
    .filter(l => l.row !== undefined) as { kind: string; attempts: number; table: string; reason: string; row: { event_type: string } }[]
  expect(lines.length).toBe(text.trim().split('\n').length - 1)
  expect(parseFile(text).complete).toBe(true)
  expect(lines.map(l => l.row.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED'])
  expect(lines[0]).toMatchObject({ kind: 'retry', attempts: 4, table: 'my-gcp-project.agent_analytics.agent_events' })
  expect(lines[0]?.reason ?? '').toMatch(/HTTP 503/)
  expect(world.statusLines.some(l => /^BQAA: 3 row\(s\) parked after retries \(HTTP 503: .*\); they are replayed on their own$/.test(l ?? ''))).toBe(true)

  // BigQuery answers again: new rows go out, then the parked ones follow.
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  for (let t = 0; t < 4; t += 1) await world.clock.advance(5000)
  const sent = world.acceptedRows().map(r => r.event_type)
  expect(sent.filter(t => t === 'STATE_DELTA').length).toBe(1)
  expect(sent.filter(t => t === 'TOOL_COMPLETED').length).toBe(2)
  expect(deadLetterFiles(world).length).toBe(0)
  expect(world.files.get(path)).toBe('')
  expect(world.statusLines.at(-1)).toBeUndefined()
})

test('a missing table is created with the BQAA schema, then written', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [TABLE_MISSING, { status: 200, text: '{"kind":"bigquery#table"}' }, OK_INSERT]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.fetches.map(f => f.url.split('/').slice(-2).join('/'))).toEqual(['agent_events/insertAll', 'agent_analytics/tables', 'agent_events/insertAll'])
  const create = JSON.parse(world.fetches[1]?.body ?? '{}') as {
    tableReference: unknown
    schema: { fields: { name: string; type: string; mode?: string }[] }
    timePartitioning: unknown
    clustering: unknown
  }
  expect(create.tableReference).toEqual({ projectId: 'my-gcp-project', datasetId: 'agent_analytics', tableId: 'agent_events' })
  expect(create.schema.fields.map(f => f.name)).toEqual([
    'timestamp',
    'event_id',
    'event_type',
    'agent',
    'session_id',
    'invocation_id',
    'user_id',
    'trace_id',
    'span_id',
    'parent_span_id',
    'content',
    'content_parts',
    'attributes',
    'latency_ms',
    'status',
    'error_message',
    'is_truncated',
  ])
  expect(create.timePartitioning).toEqual({ type: 'DAY', field: 'timestamp' })
  expect(create.clustering).toEqual({ fields: ['event_type', 'agent', 'user_id'] })
  expect(world.acceptedRows().length).toBe(1)
})

// The notice and the replay once it is found: destination.test.ts.
test('a missing dataset is not created: the rows wait as retries, none refused', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.fetches.length).toBe(1)
  expect(deadLetterFiles(world).length).toBe(0)
  const status = await statusText($)
  expect(status).toMatch(/queue: 1 rows/)
  expect(status).toMatch(/\(0 refused\)/)
})

test('a permission error parks rows as refused; /bqaa-backfill sends them once fixed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  const [file] = deadLetterFiles(world)
  expect(file?.[0].startsWith(`${PARTITION_DIR}/rejected-`)).toBe(true)
  // In an interactive terminal session where the AbovePrompt band shows the
  // permission notice, the persistent status line is not duplicated; the toast
  // and /bqaa-status carry the fix and waiting count.
  expect(world.statusLines.at(-1)).toBeUndefined()
  expect(world.toasts[0] ?? '').toContain(
    'BQAA: rows are not being written: BigQuery denied the identity access to the dataset. To fix, ask for bigquery.tables.get and bigquery.tables.updateData on the table my-gcp-project.agent_analytics.agent_events (no role that reads rows), then run /bqaa-backfill.',
  )
  // Refused rows are not replayed automatically.
  await world.clock.advance(60_000)
  expect(world.acceptedRows().length).toBe(0)

  const started = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  expect(started.text ?? '').toMatch(/backfill started for 1 file/)
  await world.clock.settle()
  expect(world.acceptedRows().map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED'])
  expect(deadLetterFiles(world).length).toBe(0)
  expect(world.logs.some(l => l.includes('bqaa backfill: 1 file(s), 3 row(s) sent, 0 left'))).toBe(true)
  expect(await statusText($)).toMatch(/last backfill \(command, .*\): 1 file\(s\), 3 rows sent, 0 left/)
})

test('the /bqaa-backfill transcript line says why a file was left waiting', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(deadLetterFiles(world).length).toBe(1)
  // No mark can be written beside the file: none of it is sent.
  world.refuseWrite = (path, text) => path.includes('/outcome-done-') && text !== ''
  await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  await world.clock.settle()
  expect(world.acceptedRows().length).toBe(0)
  expect(world.logs.some(l => /^bqaa backfill: .*; left waiting: could not mark the replay of rejected-\S+ before sending, so none of it is sent/.test(l))).toBe(true)
})

test('a refused token is fetched again', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAUTHENTICATED, OK_INSERT]
  await startSession($)
  await world.clock.advance(5000)
  await world.clock.advance(5000)
  expect(world.commands.length).toBe(2)
  expect(world.acceptedRows().length).toBe(1)
})

test('a failing token command is reported, and rows wait for it', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.processReply = { exitCode: 1, stdout: '', stderr: 'ERROR: (gcloud.auth.application-default.print-access-token) Reauthentication failed.\n' }
  await startSession($)
  await world.clock.advance(5000)
  expect(world.fetches.length).toBe(0)
  const status = await statusText($)
  // The command is named by its program only: its arguments may hold secrets.
  expect(status).toMatch(/authentication: token command .gcloud. exited 1: ERROR: \(gcloud/)
  expect(status).toMatch(/queue: 1 rows/)
})

test('BQAA_ACCESS_TOKEN is used as is, with no token command', { options: OPTIONS }, async ($, on) => {
  // Joined at run time, like every credential-shaped test value (helpers.ts).
  const token = ['ya29', 'from-the-environment-0123456789'].join('.')
  const world = setUp(on, { BQAA_ACCESS_TOKEN: token })
  await startSession($)
  await world.clock.advance(5000)
  expect(world.commands.length).toBe(0)
  expect(world.insertCalls()[0]?.headers.authorization).toBe(`Bearer ${token}`)
})

test('rows BigQuery reports as invalid are parked; the rest land', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [
    {
      status: 200,
      text: JSON.stringify({ kind: 'bigquery#tableDataInsertAllResponse', insertErrors: [{ index: 1, errors: [{ reason: 'invalid', message: 'no such field: foo.' }] }] }),
    },
  ]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  const [file] = deadLetterFiles(world)
  // One row, then the file's completion record.
  const [first = '', record = ''] = (file?.[1] ?? '').trim().split('\n')
  const line = JSON.parse(first) as { kind: string; reason: string; row: { event_type: string } }
  expect(JSON.parse(record)).toMatchObject({ complete: true, lines: 1 })
  expect(line.kind).toBe('rejected')
  expect(line.reason).toMatch(/invalid: no such field: foo/)
  expect(line.row.event_type).toBe('TOOL_STARTING')
  expect(await statusText($)).toMatch(/sent 2, dead-lettered 1 \(1 refused\)/)
})

test('session end sends what is still queued in one request, with the token in hand: no dead-letter file is written and no process runs', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await world.clock.settle()
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  const commands = world.commands.length
  await $.session.end(END)
  expect(world.insertCalls().length).toBe(1)
  expect(world.acceptedRows().map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  expect(world.commands.length).toBe(commands)
  // Neither a spill file nor a replay record beside one.
  expect([...world.files.keys()].filter(path => path.startsWith(`${DEAD_LETTER_DIR}/`))).toEqual([])
})

test('a session end whose request fails spills its rows within the budget, and the next session sends them', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE, OK_INSERT]
  await startSession($)
  await world.clock.settle()
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await $.session.end(END)
  expect(world.insertCalls().length).toBe(1)
  const [file] = deadLetterFiles(world)
  expect(file?.[0].startsWith(`${PARTITION_DIR}/spill-`)).toBe(true)
  const spilled = deadLetterRows(world)
  expect(spilled.map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  expect(await statusText($)).toMatch(/last error \([^)]*\): session end: HTTP 503/)
  // The next session replays the file 15 seconds after it starts.
  await startSession($)
  await world.clock.advance(15_000)
  const sent = new Set(world.acceptedRows().map(r => r.event_id))
  expect(spilled.every(r => sent.has(r.event_id))).toBe(true)
  expect(world.files.get(file?.[0] ?? '')).toBe('')
})

test('a session end request that does not answer is given up 150 ms before the budget ends: its rows are spilled within the budget', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await world.clock.settle()
  world.replies = [{ hang: true }]
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  let ended = false
  const ending = $.session.end(END).then(() => {
    ended = true
  })
  await world.clock.advance(600)
  // Its request is out, and nothing is spilled yet.
  expect([ended, world.insertCalls().length, deadLetterRows(world).length]).toEqual([false, 1, 0])
  // BQAA_EXIT_FLUSH_MS defaults to 800 ms: at 650 ms the request is given up
  // and the rows go to a spill file, well before the budget ends.
  await world.clock.advance(100)
  await ending
  expect(ended).toBe(true)
  expect(world.insertCalls().length).toBe(1)
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  world.releaseHung(200, '{}')
})

test('session end waits no more than half its budget for a stuck request: no request goes behind it, and every row is spilled', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ hang: true }]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  let ended = false
  const ending = $.session.end({ ...END, reason: 'other' }).then(() => {
    ended = true
  })
  await world.clock.settle()
  // The rows queued after the request wait for its answer: should it land,
  // they go in one request of their own.
  expect(deadLetterRows(world)).toEqual([])
  // BQAA_EXIT_FLUSH_MS defaults to 800 ms: the answer is waited for up to
  // half of it (the host's bound can leave a millisecond less).
  await world.clock.advance(300)
  expect(ended).toBe(false)
  await world.clock.advance(100)
  await ending
  // No second request was started behind the stuck one.
  expect(world.insertCalls().length).toBe(1)
  // The stuck batch (it may yet land; event_id dedups), then the rows queued after it.
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  world.releaseHung(200, '{}')
})

test('session end waits, inside half its budget, for a request already out: once it lands, the rows queued after it go in one more request, and none is spilled', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ hang: true }, OK_INSERT]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  const ending = $.session.end({ ...END, reason: 'other' })
  // The answer comes 168 ms after session end began, as in short -p runs.
  await world.clock.advance(168)
  world.releaseHung(200, '{}')
  await ending
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world)).toEqual([])
  // Every row went out once.
  const ids = world.sentRows().map(r => r.event_id)
  expect(world.sentRows().map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  expect(new Set(ids).size).toBe(ids.length)
})

test('a spill write that hangs holds session end no longer than its budget', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // The token command started at session start does not answer: the exit
  // waits for it until 450 ms before its budget ends, then spills.
  world.processDelayMs = 100_000
  await startSession($)
  await world.clock.settle()
  world.hangWrites = true
  let ended = false
  const ending = $.session.end(END).then(() => {
    ended = true
  })
  await world.clock.advance(500)
  expect(ended).toBe(false)
  // BQAA_EXIT_FLUSH_MS defaults to 800 ms.
  await world.clock.advance(400)
  await ending
  expect(ended).toBe(true)
  expect(world.fetches.length).toBe(0)
  world.releaseWrites()
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA', 'STATE_DELTA'])
})

// The event types of the rows in this table's files of one kind.
const typesIn = (world: World, kind: 'spill' | 'rejected'): string[] =>
  deadLetterFiles(world)
    .filter(([path, text]) => path.startsWith(`${PARTITION_DIR}/${kind}-`) && text !== '')
    .flatMap(([, text]) => parseFile(text).lines.map(l => l.row.event_type))

// A send whose second row BigQuery refuses, while the file that row goes to
// is still being written when the session ends.
async function refusedWhileEnding($: Engine, on: Parameters<typeof setUp>[0]): Promise<{ world: World; ended: () => boolean; ending: Promise<void> }> {
  const world = setUp(on)
  world.replies = [
    {
      status: 200,
      text: JSON.stringify({ kind: 'bigquery#tableDataInsertAllResponse', insertErrors: [{ index: 1, errors: [{ reason: 'invalid', message: 'no such field: foo.' }] }] }),
    },
  ]
  world.hangWrite = path => path.includes('/rejected-')
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  let ended = false
  const ending = $.session.end(END).then(() => {
    ended = true
  })
  return { world, ended: () => ended, ending }
}

test('session end waits, inside its budget, for a file of refused rows still being written, and spills its own row apart', { options: OPTIONS }, async ($, on) => {
  const { world, ended, ending } = await refusedWhileEnding($, on)
  await world.clock.advance(500)
  // Its own row is on disk; the refused row is not yet.
  expect([ended(), typesIn(world, 'spill'), typesIn(world, 'rejected')]).toEqual([false, ['STATE_DELTA'], []])
  world.releaseWrites()
  await ending
  expect([typesIn(world, 'spill'), typesIn(world, 'rejected'), world.insertCalls().length]).toEqual([['STATE_DELTA'], ['TOOL_STARTING'], 1])
})

test('a file of refused rows whose write hangs holds session end no longer than its budget', { options: OPTIONS }, async ($, on) => {
  const { world, ended, ending } = await refusedWhileEnding($, on)
  await world.clock.advance(500)
  expect(ended()).toBe(false)
  await world.clock.advance(400)
  await ending
  expect([ended(), typesIn(world, 'spill'), typesIn(world, 'rejected')]).toEqual([true, ['STATE_DELTA'], []])
  world.releaseWrites()
})

test('with BQAA_EXIT_FLUSH_MS=0 session end does not wait for its spill write', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_EXIT_FLUSH_MS: '0' })
  await startSession($)
  await world.clock.settle()
  world.hangWrites = true
  await $.session.end(END)
  expect(world.fetches.length).toBe(0)
  // The write was started all the same.
  world.releaseWrites()
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA', 'STATE_DELTA'])
})

test('the next session replays spill files on its own', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const path = `${PARTITION_DIR}/spill-20261013T200102003Z-previoussess-1.jsonl`
  world.files.set(path, parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'previous-session'), 'not json'))
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.acceptedRows().some(r => r.event_id === 'a'.repeat(32))).toBe(true)
  // The unreadable line is kept for a person to look at, in a new, complete
  // file; the source is emptied once that file is written.
  expect(world.files.get(path)).toBe('')
  const kept = deadLetterFiles(world)
  expect(kept.map(([, text]) => text.split('\n')[0])).toEqual(['not json'])
  expect(kept.map(([, text]) => parseFile(text).complete)).toEqual([true])
})

test('replay sends only rows recorded for the configured table; other tables keep theirs', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const files = parkForTwoTables(world)
  const before = new Map(world.files)
  await startSession($)
  await world.clock.advance(15_000)
  // Every request went to this table, and none carried another table's row.
  expect(world.insertCalls().length > 0).toBe(true)
  for (const call of world.insertCalls()) expect(call.url).toBe('https://bigquery.googleapis.com/bigquery/v2/projects/my-gcp-project/datasets/agent_analytics/tables/agent_events/insertAll')
  const sent = world.acceptedRows()
  expect(sent.some(r => r.session_id === 'their-session')).toBe(false)
  expect(sent.filter(r => r.session_id === 'mine-session').map(r => r.event_id).sort()).toEqual(['a'.repeat(32), 'd'.repeat(32)])
  // The other table's files are as they were, byte for byte; in the mixed
  // file the line for the other table is all that is left, unchanged.
  expect(world.files.get(files.theirs)).toBe(before.get(files.theirs))
  expect(world.files.get(files.loose)).toBe(before.get(files.loose))
  expect(world.files.get(files.mine)).toBe('')
  // The mixed file's line for the other table moved, as written, to a new file
  // in this table's directory before the mixed file was emptied.
  expect(world.files.get(files.mixed)).toBe('')
  const moved = deadLetterFiles(world).filter(([path]) => path.startsWith(`${PARTITION_DIR}/`))
  expect(moved.map(([, text]) => parseFile(text).raw)).toEqual([[parkedLine(OTHER_TABLE, 'c'.repeat(32), 'their-session')]])
  expect(moved.map(([, text]) => parseFile(text).complete)).toEqual([true])
  const status = await statusText($)
  expect(status).toMatch(/2 rows sent, 1 left, 1 of them for another table/)
  expect(status).toMatch(/dead-letter: 2 waiting file\(s\) in .* for other tables or an older layout, not replayed under this configuration/)
})

test('configured for the other table, replay sends its rows there and leaves this table\'s alone', { options: OTHER_OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const files = parkForTwoTables(world)
  const before = new Map(world.files)
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.insertCalls().length > 0).toBe(true)
  for (const call of world.insertCalls()) expect(call.url).toBe('https://bigquery.googleapis.com/bigquery/v2/projects/other-project/datasets/private_dataset/tables/agent_events/insertAll')
  const sent = world.acceptedRows()
  expect(sent.some(r => r.session_id === 'mine-session')).toBe(false)
  expect(sent.filter(r => r.session_id === 'their-session').map(r => r.event_id)).toEqual(['b'.repeat(32)])
  for (const path of [files.mine, files.mixed, files.loose]) expect(world.files.get(path)).toBe(before.get(path))
  expect(world.files.get(files.theirs)).toBe('')
})

test('a file another session is replaying is left to it', { options: OPTIONS }, async ($, on) => {
  const name = 'spill-20261013T200102003Z-othersession-1.jsonl'
  const world = setUp(on, {}, { ...GREETED, [`lease:${TABLE_REF}/${name}`]: { by: 'other-session', until: Date.now() + 60_000 } })
  world.files.set(`${PARTITION_DIR}/${name}`, '{"v":1}\n')
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.files.get(`${PARTITION_DIR}/${name}`)).toBe('{"v":1}\n')
  expect(await statusText($)).toMatch(/1 skipped/)
})

test('multibyte rows: dead-letter files stay within the limit in bytes, and every row is replayed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // 15,000 three-byte characters per tool result: about 45 KB of UTF-8 a row,
  // 5 MB for the 120 calls, though only 1.8 million characters.
  world.toolReply = () => ({ result: { type: 'text' }, text: '界'.repeat(15_000) })
  await startSession($)
  await world.clock.settle()
  for (let i = 0; i < 120; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}.ts` })
  // BigQuery is unavailable as the session ends: every row goes to spill
  // files, the overflow of the exit's one request and that request's batch.
  world.replies = [UNAVAILABLE]
  await $.session.end(END)
  world.replies = [OK_INSERT]
  const files = deadLetterFiles(world)
  expect(files.length > 1).toBe(true)
  for (const [, text] of files) expect(utf8Bytes(text) <= FILE_CAP).toBe(true)
  expect(deadLetterRows(world).filter(r => r.event_type === 'TOOL_COMPLETED').length).toBe(120)
  // The next session reads every file back and sends every row.
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.acceptedRows().filter(r => r.event_type === 'TOOL_COMPLETED').length).toBe(120)
  for (const call of world.insertCalls()) expect(call.body.length <= HOST_FILE_BYTES).toBe(true)
  expect(deadLetterFiles(world).length).toBe(0)
})

test('a dead-letter file over the 4 MiB read limit is reported, not skipped silently', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const name = 'spill-20261013T200102003Z-oversized-1.jsonl'
  const text = '界'.repeat(1_500_000)
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.files.get(`${PARTITION_DIR}/${name}`)).toBe(text)
  expect(await statusText($)).toMatch(new RegExp(`1 file\\(s\\) over the 4 MiB read limit, not replayed: ${name.replace(/\./g, '\\.')}`))
})

test('the failing token-command fixture keeps its canaries out of the exercised files and diagnostics', { options: { ...OPTIONS, accessTokenCommand: 'fake-token-tool --profile argcanaryprofile' } }, async ($, on) => {
  const world = setUp(on)
  world.processReply = { exitCode: 1, stdout: '', stderr: `ERROR: refresh failed for ${LEAKY}\n` }
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // Four attempts, then the rows are parked; the automatic replay at 15 s
  // stops on the same failure, and so does /bqaa-backfill.
  for (let t = 0; t < 6; t += 1) await world.clock.advance(5000)
  const started = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  await world.clock.settle()
  for (const text of await everySink($, world, [started.text ?? ''])) {
    expect(text.includes(SECRET)).toBe(false)
    expect(text.includes('argcanaryprofile')).toBe(false)
  }
  // The diagnostics did reach those sinks, redacted.
  const parked = deadLetterFiles(world)
  expect(parked.length > 0).toBe(true)
  for (const [, text] of parked) expect(text).toMatch(/"reason":"authentication: token command `fake-token-tool` exited 1: ERROR: refresh failed for \[REDACTED:google-oauth-token\] password=\[REDACTED\]"/)
  // While no credentials work, the status line names the fix, the command by
  // its program only; the toast and the transcript line carry the reason.
  expect(world.statusLines.some(l => (l ?? '').startsWith('BQAA: no working Google credentials. To fix, check the token command `fake-token-tool`'))).toBe(true)
  expect(world.toasts.length).toBe(1)
  expect(world.transcript.some(l => l.includes('exited 1: ERROR: refresh failed for [REDACTED:google-oauth-token] password=[REDACTED]'))).toBe(true)
  expect(await statusText($)).toMatch(/last backfill \(command, .*stopped: authentication: token command .fake-token-tool. exited 1: .*\[REDACTED/)
  expect(world.logs.some(l => l.startsWith('bqaa backfill:') && l.includes('[REDACTED'))).toBe(true)
})

test('the scripted API error fixture keeps its canaries out of the exercised files and diagnostics', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const refusal = { status: 403, text: JSON.stringify({ error: { code: 403, message: `Access Denied: ${LEAKY}`, errors: [{ reason: 'accessDenied' }] } }) }
  const invalid = { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: `bad value ${LEAKY}` }] }] }) }
  const busy = { status: 503, text: JSON.stringify({ error: { code: 503, message: `backend busy ${LEAKY}` } }) }
  world.replies = [refusal, invalid, busy]
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // Refused: the row is parked with BigQuery's reason, and the status line says so.
  await world.clock.advance(5000)
  // A backfill: insertAll reports the row invalid, so its reason is replaced.
  const first = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  await world.clock.settle()
  // Another: BigQuery is busy, and the backfill stops on that.
  const second = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  await world.clock.settle()
  for (const text of await everySink($, world, [first.text ?? '', second.text ?? ''])) expect(text.includes(SECRET)).toBe(false)
  const [file] = deadLetterFiles(world)
  expect(file?.[1] ?? '').toMatch(/"reason":"invalid: bad value \[REDACTED:google-oauth-token\] password=\[REDACTED\]"/)
  expect(world.statusLines.some(l => (l ?? '').startsWith('BQAA: BigQuery denied the identity access to the dataset.'))).toBe(true)
  expect(world.transcript.some(l => l.includes('Access Denied: [REDACTED:google-oauth-token] password=[REDACTED]'))).toBe(true)
  expect(await statusText($)).toMatch(/stopped: HTTP 503: backend busy \[REDACTED:google-oauth-token\]/)
  expect(world.logs.some(l => l.startsWith('bqaa backfill:') && l.includes('stopped: HTTP 503'))).toBe(true)
})

test('under queue pressure while BigQuery is down, rows go to disk instead of being dropped', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '20' })
  world.replies = [UNAVAILABLE]
  await startSession($)
  await world.clock.advance(5000)
  for (let i = 0; i < 9; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  await world.clock.advance(5000)
  const status = await statusText($)
  expect(status).toMatch(/dropped 0/)
  expect(status).toMatch(/spilled [1-9]/)
  expect(deadLetterFiles(world).some(([p]) => p.startsWith(`${PARTITION_DIR}/spill-`))).toBe(true)
})

test('a dead-letter write that fails is counted as dropped, not thrown', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED]
  world.failWrites = true
  await startSession($)
  await world.clock.advance(5000)
  const status = await statusText($)
  expect(status).toMatch(/dropped 1/)
  expect(status).toMatch(/dead-letter write failed \(1 rows lost\)/)
})

test('refused rows whose dead-letter file cannot be written are said to be lost, and not counted among the rows /bqaa-backfill sends', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ status: 400, text: '{"error":{"code":400,"message":"Invalid value for the table schema","errors":[{"reason":"invalid"}],"status":"INVALID_ARGUMENT"}}' }]
  world.failWrites = true
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe('BQAA: 3 refused row(s) lost: their dead-letter file could not be written (HTTP 400: Invalid value for the table schema); see /bqaa-status')
  // Writes work again: the next refused rows are the only ones counted.
  world.failWrites = false
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  for (let t = 0; t < 4; t += 1) await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe('BQAA: 2 row(s) refused (HTTP 400: Invalid value for the table schema); fix, then /bqaa-backfill')
})

test('session.start fired again clears the status line, and the writer that takes over says the same line again', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ status: 400, text: '{"error":{"code":400,"message":"Invalid value for the table schema","errors":[{"reason":"invalid"}],"status":"INVALID_ARGUMENT"}}' }]
  await startSession($)
  await world.clock.advance(5000)
  const said = 'BQAA: 1 row(s) refused (HTTP 400: Invalid value for the table schema); fix, then /bqaa-backfill'
  expect(world.statusLines.at(-1)).toBe(said)
  // The plugin turned off and on: session.start fires again in the same
  // load, and its own row is refused the same way.
  await startSession($)
  expect(world.statusLines.at(-1)).toBeUndefined()
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe(said)
})

test('rows out of retries whose dead-letter file cannot be written are said to be lost', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE]
  world.failWrites = true
  await startSession($)
  for (let t = 0; t < 6; t += 1) await world.clock.advance(5000)
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: 1 row\(s\) out of retries lost: their dead-letter file could not be written \(HTTP 503: .*\); see \/bqaa-status$/)
})

test('dry run writes the rows locally and sends nothing', { options: { dryRun: true } }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.fetches.length).toBe(0)
  expect(world.commands.length).toBe(0)
  const written = [...world.files.entries()].filter(([p]) => p.startsWith(`${DRY_RUN_DIR}/dry-run-`))
  expect(written.length).toBe(1)
  const rows = (written[0]?.[1] ?? '')
    .trim()
    .split('\n')
    .map(l => JSON.parse(l) as { event_type: string; attributes: string })
  expect(rows.map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED'])
  expect(json(rows[0]?.attributes ?? null)).toMatchObject({ writer: { mode: 'dry_run' } })
})

test('error text is redacted before /bqaa-status shows it', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const token = ['ya29', 'a0AfH6SMBexampleexampleexample0123'].join('.')
  world.processReply = { exitCode: 1, stdout: '', stderr: `ERROR: refresh failed for ${token}\n` }
  await startSession($)
  await world.clock.advance(5000)
  const status = await statusText($)
  expect(status).toMatch(/\[REDACTED:google-oauth-token\]/)
  expect(status).not.toMatch(/a0AfH6SMBexample/)
})

test('/bqaa-flush sends right away instead of at the next interval', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  const reply = await $.command.run({ command: 'bqaa-flush', args: '', ...COMMAND })
  expect(reply.text ?? '').toMatch(/flush requested for 3 queued row/)
  await world.clock.settle()
  expect(world.acceptedRows().length).toBe(3)
})

test('/clear keeps the process sending, under the new session id', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await $.session.end({ reason: 'clear', sessionId: SESSION_ID, resume: { id: SESSION_ID } })
  world.sessionId = 'after-clear-session'
  await $.classic.SessionStart({ source: 'clear', session_id: 'after-clear-session' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  // Nothing was spilled: the process did not end.
  expect(deadLetterFiles(world).length).toBe(0)
  const rows = world.acceptedRows()
  expect(rows.map(r => [r.event_type, r.session_id])).toEqual([
    ['STATE_DELTA', SESSION_ID],
    ['STATE_DELTA', SESSION_ID],
    ['STATE_DELTA', 'after-clear-session'],
    ['TOOL_STARTING', 'after-clear-session'],
    ['TOOL_COMPLETED', 'after-clear-session'],
  ])
})

// A GitHub-style token, built at run time: 40 characters a pattern knows.
const CUT_TOKEN = ['gh', 'p_', 'x'.repeat(36)].join('')
// Its first characters, which must not survive a cut either.
const FRAGMENT = CUT_TOKEN.slice(0, 4)

test('the API-error credential fixture crossing the length cap is removed in the exercised sinks', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // The token ends just past the 1,000-character cap on error messages.
  world.replies = [{ status: 403, text: JSON.stringify({ error: { message: `${'e'.repeat(950)} ${CUT_TOKEN}`, errors: [{ reason: 'accessDenied' }] } }) }]
  await startSession($)
  await world.clock.advance(5000)
  expect(deadLetterFiles(world).length).toBe(1)
  for (const text of await everySink($, world)) expect(text.includes(FRAGMENT)).toBe(false)
  expect(deadLetterFiles(world)[0]?.[1] ?? '').toMatch(/"reason":"HTTP 403: e+ \[REDACTED/)
})

test('a credential across the cap on a non-JSON error body is removed whole', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // A proxy's error page: the token sits across the 300 characters kept.
  world.replies = [{ status: 400, text: `<html>${'p'.repeat(280)} ${CUT_TOKEN}</html>` }]
  await startSession($)
  await world.clock.advance(5000)
  expect(deadLetterFiles(world).length).toBe(1)
  for (const text of await everySink($, world)) expect(text.includes(FRAGMENT)).toBe(false)
})

test('a credential across the cap on a row\'s insert error is removed whole, sent or replayed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // Sent: the token runs past the 1,000-character cap on a row's reason.
  const sent = { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: `${'e'.repeat(955)} ${CUT_TOKEN}` }] }] }) }
  // Replayed: the same for a parked row, whose reason is rewritten in its file.
  const replayed = { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: `${'e'.repeat(970)} ${CUT_TOKEN}` }] }] }) }
  const path = `${PARTITION_DIR}/retry-20261013T200102003Z-previoussess-1.jsonl`
  world.files.set(path, parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'previous-session')))
  world.replies = [sent, replayed]
  await startSession($)
  await world.clock.advance(15_000)
  expect(deadLetterFiles(world).filter(([, text]) => text.includes('"kind":"rejected"')).length).toBe(2)
  for (const text of await everySink($, world)) expect(text.includes(FRAGMENT)).toBe(false)
})

test('a tool result sent as text keeps the redaction its keys get as structure', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const secrets = { token: 'x'.repeat(12), sessionCookie: 'y'.repeat(12), password: 'alpha beta gamma', nested: { api_key: 'z'.repeat(12), credentials: { user: 'u', pass: 'w'.repeat(12) } } }
  world.toolReply = () => ({ result: secrets, text: JSON.stringify(secrets) })
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'settings.json' })
  await world.clock.advance(5000)
  const [row] = world.sentRows().filter(r => r.event_type === 'TOOL_COMPLETED')
  const content = row?.content ?? ''
  for (const value of ['x'.repeat(12), 'y'.repeat(12), 'alpha', 'beta gamma', 'z'.repeat(12), 'w'.repeat(12)]) expect(content.includes(value)).toBe(false)
  // Still the text the tool returned, with each value under a key that is
  // not listed replaced whole.
  expect((JSON.parse(content) as { result: string }).result).toBe('{"token":"[REDACTED]","sessionCookie":"[REDACTED]","password":"[REDACTED]","nested":"[REDACTED]"}')
})

test('a 2xx answer that is not a valid insertAll response keeps the rows: retried, then parked', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // Cut off mid-object, as a body past the response limit can be.
  world.replies = [{ status: 200, text: '{"insertErrors":[{"index":0,"errors":[{"reason":"invalid"' }]
  await startSession($)
  for (let t = 0; t < 4; t += 1) await world.clock.advance(5000)
  // Four attempts with the same insertIds, none counted as sent.
  const calls = world.insertCalls()
  expect(calls.length).toBe(4)
  const ids = (call: (typeof calls)[number]) => (JSON.parse(call.body) as { rows: { insertId: string }[] }).rows.map(r => r.insertId)
  for (const call of calls) expect(ids(call)).toEqual(ids(calls[0] as (typeof calls)[number]))
  const status = await statusText($)
  expect(status).toMatch(/sent 0, dead-lettered 1/)
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA'])
  expect(deadLetterFiles(world)[0]?.[0].startsWith(`${PARTITION_DIR}/retry-`)).toBe(true)
})

test('a replayed file whose answer is malformed stays as it was', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const path = `${PARTITION_DIR}/spill-20261013T200102003Z-previoussess-1.jsonl`
  const text = parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'previous-session'))
  world.files.set(path, text)
  world.replies = [OK_INSERT, { status: 200, text: '{"insertErrors": {"index": 0}}' }]
  await startSession($)
  await world.clock.advance(15_000)
  expect(world.files.get(path)).toBe(text)
  // The replay 15 s in, or the one the paced retries run after it.
  expect(await statusText($)).toMatch(/last backfill \((auto|recovery), .*\): 1 file\(s\), 0 rows sent, 1 left; stopped: HTTP 200 with a response that is not a valid insertAll answer/)
})

// The round-three probes, end to end: each payload reaches the mod as a tool
// result (its text and its structure), BigQuery refuses the batch, so the row
// goes out once in the request and is then parked on disk. The canary, built
// at run time, must be in neither.
const CANARY = ['canary', 'value', 'q'.repeat(12)].join('-')
const SECRET_NUM = Number(['9876', '54321'].join(''))
const SLACK_HOOK = `https://${['hooks', 'slack', 'com'].join('.')}/${['services', 'T0000', 'B0000', CANARY].join('/')}`
const DISCORD_HOOK = `https://${['discord', 'com'].join('.')}/api/${['webhooks', '1234567890', CANARY].join('/')}`
const OFFICE_HOOK = `https://contoso.${['webhook', 'office', 'com'].join('.')}/webhookb2/${CANARY}`
const REFUSED = { status: 403, text: '{"error":{"message":"permission denied","errors":[{"reason":"accessDenied"}]}}' }

const PROBES: [string, string | Record<string, unknown>, string][] = [
  ['a cookie header with a redaction marker inside it', `Cookie: session=${CANARY}; csrf=[REDACTED]`, CANARY],
  ['a credentials list mixing a marker and a secret', { credentials: ['[REDACTED]', CANARY] }, CANARY],
  ['a password longer than 5,000 characters', { password: `${'x'.repeat(5000)}${CANARY}` }, CANARY],
  ['a header value longer than 5,000 characters', `Authorization: ${'x'.repeat(5000)}${CANARY}`, CANARY],
  ['a password in JSON inside JSON', { text: JSON.stringify({ password: CANARY }) }, CANARY],
  ['a password three encodings deep', { a: JSON.stringify({ b: JSON.stringify({ c: JSON.stringify({ pwd: CANARY }) }) }) }, CANARY],
  ['pass and pwd keys', { pass: CANARY, db: { root_pwd: CANARY, MY_PASS: CANARY } }, CANARY],
  ['pass and pwd assignments in text', `export MY_PWD=${CANARY}\nPASS=${CANARY}\nmode=fast`, CANARY],
  ['a numeric secret', { password: SECRET_NUM, pin: 1 }, String(SECRET_NUM)],
  ['a numeric secret in text', `password: ${SECRET_NUM}`, String(SECRET_NUM)],
  ['webhook URLs', `notify ${SLACK_HOOK} and ${DISCORD_HOOK} or ${OFFICE_HOOK}`, CANARY],
  ['a URL with user-info and a signed parameter', `fetch https://${['user', CANARY].join(':')}@db.example.test/x?sig=${CANARY}&page=2`, CANARY],
]

for (const [label, payload, secret] of PROBES) {
  test(`${label}: neither sent nor parked`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    const text = typeof payload === 'string' ? payload : JSON.stringify(payload)
    world.toolReply = () => ({ result: payload, text })
    world.replies = [REFUSED]
    await startSession($)
    await $.tool.call({ tool: 'Read', file_path: 'synthetic.txt' })
    await world.clock.advance(5000)
    const sent = world.sentRows().filter(r => r.event_type === 'TOOL_COMPLETED')
    const parked = deadLetterRows(world).filter(r => r.event_type === 'TOOL_COMPLETED')
    expect(sent.length).toBe(1)
    expect(parked.length).toBe(1)
    expect(JSON.stringify(sent).includes(secret)).toBe(false)
    expect(JSON.stringify(parked).includes(secret)).toBe(false)
    expect(sent[0]?.content ?? '').toMatch(/REDACTED/)
  })
}

test('the same probes as tool input: every argument not listed goes whole', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [REFUSED]
  await startSession($)
  await $.tool.call({ tool: 'mcp__deploy__run', api_key: SECRET_NUM, env: { DB_PWD: CANARY, cookies: ['[REDACTED]', CANARY] }, note: JSON.stringify({ token: CANARY }), target: 'prod', description: 'deploy the app' })
  await world.clock.advance(5000)
  const [sent] = world.sentRows().filter(r => r.event_type === 'TOOL_STARTING')
  const [parked] = deadLetterRows(world).filter(r => r.event_type === 'TOOL_STARTING')
  for (const row of [sent, parked]) {
    const text = JSON.stringify(row)
    expect(text.includes(CANARY) || text.includes(String(SECRET_NUM))).toBe(false)
    // A listed key keeps its value; one that is not goes, credential or not.
    expect((JSON.parse(row?.content ?? '{}') as { args: { description: string; target: string } }).args).toMatchObject({ description: 'deploy the app', target: '[REDACTED]' })
  }
})

test('a credential in BigQuery error text goes whole even next to a redaction marker', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ status: 403, text: JSON.stringify({ error: { message: `denied for Cookie: session=${CANARY}; csrf=[REDACTED]\npassword: ${'x'.repeat(5000)}${CANARY}`, errors: [{ reason: 'accessDenied' }] } }) }]
  await startSession($)
  await world.clock.advance(5000)
  expect(deadLetterFiles(world).length).toBe(1)
  for (const text of await everySink($, world)) expect(text.includes(CANARY)).toBe(false)
})

// Round four. Formatted JSON documents whose credential sits on a later line
// than its name, too long to parse whole: they must not reach delivery or disk.
const credentialDocuments = {
  object: (padding: number) => JSON.stringify({ credentials: { value: CANARY }, padding: 'x'.repeat(padding) }, null, 2),
  list: (padding: number) => JSON.stringify([{ credentials: { value: CANARY } }, 'x'.repeat(padding)], null, 2),
  'list led by a number': (padding: number) => JSON.stringify([1, { credentials: { value: CANARY } }, 'x'.repeat(padding)], null, 2),
}
const credentialDocument = credentialDocuments.object

for (const [shape, document] of Object.entries(credentialDocuments)) {
  for (const sink of ['sent', 'parked'] as const) {
    test(`a formatted JSON ${shape} past the parse budget is dropped and marked, not scrubbed line by line (${sink})`, { options: OPTIONS }, async ($, on) => {
      const world = setUp(on)
      if (sink === 'parked') world.replies = [REFUSED]
      world.toolReply = () => ({ result: { type: 'text' }, text: document(30_000) })
      await startSession($)
      await $.tool.call({ tool: 'Read', file_path: 'large-config.json' })
      await world.clock.advance(5000)
      const rows = (sink === 'sent' ? world.sentRows() : deadLetterRows(world)).filter(r => r.event_type === 'TOOL_COMPLETED')
      expect(rows.length).toBe(1)
      expect(JSON.stringify(rows).includes(CANARY)).toBe(false)
      expect(rows[0]?.is_truncated).toBe(true)
      expect((JSON.parse(rows[0]?.content ?? '{}') as { result: string }).result).toMatch(/^\[TRUNCATED: \d+ chars of JSON text that could not be parsed and checked\]$/)
    })
  }
}

test('formatted JSON within the budget is still redacted by its structure', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.toolReply = () => ({ result: { type: 'text' }, text: credentialDocument(100) })
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'small-config.json' })
  await world.clock.advance(5000)
  const [row] = world.sentRows().filter(r => r.event_type === 'TOOL_COMPLETED')
  expect(JSON.stringify(row).includes(CANARY)).toBe(false)
  expect(JSON.parse((JSON.parse(row?.content ?? '{}') as { result: string }).result)).toEqual({ credentials: '[REDACTED]', padding: '[REDACTED]' })
})

test('JSON text that does not parse is dropped, not scrubbed line by line', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // Cut off before its end, as a tool's own output limit can leave it.
  world.toolReply = () => ({ result: { type: 'text' }, text: credentialDocument(100).slice(0, -20) })
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'cut-config.json' })
  await world.clock.advance(5000)
  const [row] = world.sentRows().filter(r => r.event_type === 'TOOL_COMPLETED')
  expect(JSON.stringify(row).includes(CANARY)).toBe(false)
  expect(row?.is_truncated).toBe(true)
})

test('after /clear the next rows carry the new session id, with no classic hook to say so', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // No classic.SessionStart: a managed policy can keep classic hooks from a
  // user plugin, and session.start does not fire after /clear.
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  await $.session.end({ reason: 'clear', sessionId: SESSION_ID, resume: { id: SESSION_ID } })
  world.sessionId = 'session-after-clear'
  await $.turn.start({ turnId: 'turn-after-clear', text: 'after clear' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.sentRows().map(r => [r.event_type, r.session_id])).toEqual([
    ['STATE_DELTA', SESSION_ID],
    ['INVOCATION_STARTING', 'session-after-clear'],
    ['TOOL_STARTING', 'session-after-clear'],
    ['TOOL_COMPLETED', 'session-after-clear'],
  ])
})

test('after a session change, a refused id read leaves rows with no session id, never the old one', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  await $.session.end({ reason: 'resume', sessionId: SESSION_ID, resume: { id: SESSION_ID } })
  world.sessionIdRefused = true
  await $.turn.start({ turnId: 'turn-after-resume', text: 'after resume' })
  await world.clock.advance(5000)
  const [started] = world.sentRows().filter(r => r.event_type === 'INVOCATION_STARTING')
  expect(started?.session_id).toBe(null)
})

// A replay whose leftovers outgrow one file (every row refused with a long
// reason), run against a file system that can fail every write but the
// source's own.
async function splitReplay(failNewFiles: boolean) {
  const config = resolveConfig({ project: 'replay-project', dataset: 'events', deadLetterDir: '/replay/dead-letter' }, {})
  const source = `${deadLetterPartition(config)}/spill-20261013T200102003Z-replay-1.jsonl`
  const lines: DeadLetterLine[] = Array.from({ length: 1000 }, (_, i) => ({
    v: 1,
    kind: 'spill',
    reason: 'session ended',
    attempts: 0,
    failed_at: '2026-10-13T20:01:02.003Z',
    table: 'replay-project.events.agent_events',
    row: buildRow({ eventType: 'TOOL_COMPLETED', timestampMs: 0, eventId: String(i).padStart(32, '0'), agent: 'coding-agent', sessionId: 's', invocationId: 't', userId: 'u', traceId: null, spanId: null, parentSpanId: null, content: { result: 'r'.repeat(2200) }, attributes: {}, maxPayloadChars: 16384 }).row,
  }))
  const encoded = encodeLines(lines)
  expect(encoded.length).toBe(1)
  const files = new Map<string, string>([[source, encoded[0] as string]])
  const refusal = (count: number) => JSON.stringify({ insertErrors: Array.from({ length: count }, (_, index) => ({ index, errors: [{ reason: 'invalid', message: 'm'.repeat(900) }] })) })
  const io: Io = {
    now: () => 0,
    after: () => ({ cancel: () => undefined }),
    fetch: async (_url, init) => ({ status: 200, text: refusal((JSON.parse(init.body ?? '{}') as { rows: unknown[] }).rows.length) }),
    runCommand: async () => ({ exitCode: 0, stdout: 'a'.repeat(24), stderr: '' }),
    envToken: async () => undefined,
    readFile: async path => files.get(path) ?? '',
    writeFile: async (path, text) => {
      // The replay's mark and record files beside the source are written.
      if (failNewFiles && path !== source && !path.includes('/outcome-')) throw new Error('disk full')
      files.set(path, text)
    },
    listDir: async () => [{ name: source.split('/').pop() as string, kind: 'file', size: utf8Bytes(encoded[0] as string) }],
    storeGet: async () => undefined,
    storeSet: async () => undefined,
    storeDelete: async () => undefined,
    status: () => undefined,
    debug: () => undefined,
  }
  const writer = new Writer(io, () => config, () => 'replay-session')
  const report = await writer.backfill(['spill'], 'command')
  const ids = [...files.values()].flatMap(text => parseFile(text).lines.map(line => line.row.event_id))
  const incomplete = [...files].filter(([path, text]) => path !== source && !path.includes('/outcome-') && !parseFile(text).complete).map(([path]) => path)
  return { files, source, report, ids, incomplete }
}

test('a replay that splits what is left writes every part before emptying its source', async () => {
  const { files, source, report, ids, incomplete } = await splitReplay(false)
  expect(report.error).toBe(null)
  // Each new file ends with its completion record.
  expect(incomplete).toEqual([])
  // Lengths, not contents: a failure message holding megabytes of rows
  // would not reach the test report.
  expect(files.get(source)?.length).toBe(0)
  // At least two new files, holding every refused row exactly once.
  expect(files.size >= 3).toBe(true)
  expect(ids.length).toBe(1000)
  expect(new Set(ids).size).toBe(1000)
})

test('a write that fails while a replay splits leaves the source whole', async () => {
  const { files, source, report, ids } = await splitReplay(true)
  expect(report.held ?? report.error ?? '').toMatch(/disk full/)
  expect(parseFile(files.get(source) ?? '').lines.length).toBe(1000)
  expect(new Set(ids).size).toBe(1000)
})

test('/bqaa-status reports replayed row count, replay request batches, and retired 0-byte/outcome files after backfill', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // Seed a spill file with 2 rows from a prior session, plus an outcome-*
  // record whose own file is gone: no leftover (the cleanup script keeps it
  // too, as it pairs a record only with an emptied file beside it).
  const spilled = parkedFile(parkedLine(TABLE_REF, 'spilled-row-1', 'old-session'), parkedLine(TABLE_REF, 'spilled-row-2', 'old-session'))
  world.files.set(`${PARTITION_DIR}/spill-1700000000000-aa.jsonl`, spilled)
  world.files.set(`${PARTITION_DIR}/outcome-spill-1699999000000-bb.jsonl`, '{"fates":"L"}\n')
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  // Let the 15s auto-backfill replay the 2 spilled rows (with no live rows sent).
  await world.clock.advance(15_000)
  await world.clock.settle()
  const status = await statusText($)
  expect(status).toContain('sent 2 (2 replayed)')
  expect(status).toMatch(/requests: 1 ok, 0 retried/)
  expect(status).toMatch(/dead-letter: 0 file\(s\), 0 B in .*; 2 replayed\/retired file\(s\) \(0-byte or outcome-\*, expected leftovers; host fs cannot delete\)/)
  // The emptied spill file and the done mark the replay left beside it.
  expect(status).toContain('1 empty file(s); 1 replay record(s), kept on purpose')
})

test('when the token arrives during the exit wait, no insertAll starts after session.end began: rows are spilled once and replayed once', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // Make the initial token command resolve at t=5200 ms (200 ms after the 5000 ms flush + session.end).
  world.processDelayMs = 5200
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // Start the 5s flush: it takes the 3 rows and waits on the in-flight getToken().
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(0)
  // Now session.end begins while the flush is waiting on getToken().
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.settle()
  // The token arrives 200 ms into the exit wait: deliver must NOT start insertAll.
  await world.clock.advance(200)
  await ending
  expect(world.insertCalls().length).toBe(0)
  const spilled = deadLetterRows(world)
  expect(spilled.map(r => r.event_type).sort()).toEqual(['STATE_DELTA', 'STATE_DELTA', 'TOOL_COMPLETED', 'TOOL_STARTING'])
  expect(new Set(spilled.map(r => r.event_id)).size).toBe(4)
  // Next session replays: every row is sent once.
  world.processDelayMs = 0
  await startSession($)
  await world.clock.advance(15_000)
  await world.clock.settle()
  const replayedIds = world.acceptedRows().filter(r => spilled.some(s => s.event_id === r.event_id)).map(r => r.event_id)
  expect(replayedIds.length).toBe(4)
  expect(new Set(replayedIds).size).toBe(4)
})

test('when an in-flight post gets 401 and the refreshed token arrives during the exit wait, no retry insertAll starts after session.end began', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAUTHENTICATED, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // Prefetch already fetched the first token at t=0; now delay the 401 refresh token by 200 ms.
  world.processDelayMs = 200
  // First post gets 401; deliver drops token and waits 200 ms on the second getToken().
  await world.clock.advance(5000)
  await world.clock.settle()
  expect(world.insertCalls().length).toBe(1)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.settle()
  await world.clock.advance(200)
  await ending
  // No second insertAll was started after session.end began.
  expect(world.insertCalls().length).toBe(1)
  const spilled = deadLetterRows(world)
  expect(spilled.length).toBe(4)
  expect(new Set(spilled.map(r => r.event_id)).size).toBe(4)
})

test('a table-not-found answer during the exit wait creates the table once and sends the rows when ample budget remains', { options: { ...OPTIONS, autoCreateTable: true } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ hang: true }, { status: 200, text: '{"kind":"bigquery#table"}' }, OK_INSERT]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(TABLE_MISSING.status, TABLE_MISSING.text)
  await world.clock.settle()
  await ending
  expect(world.fetches.map(f => f.url.split('/').slice(-2).join('/'))).toEqual([
    'agent_events/insertAll',
    'agent_analytics/tables',
    'agent_events/insertAll',
  ])
  expect(world.acceptedRows().map(r => r.content).sort()).toEqual(['{"session_end":true}', '{"session_start":true}'])
  expect(deadLetterRows(world)).toEqual([])
})

test('a table-not-found answer during the exit wait creates no table under a tight budget: the batch is spilled cleanly', { options: { ...OPTIONS, autoCreateTable: true } }, async ($, on) => {
  const world = setUp(on, { BQAA_EXIT_FLUSH_MS: '500' })
  world.replies = [{ hang: true }, { status: 200, text: '{"kind":"bigquery#table"}' }, OK_INSERT]
  await startSession($)
  await world.clock.advance(5000)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(300)
  world.releaseHung(TABLE_MISSING.status, TABLE_MISSING.text)
  await ending
  expect(world.fetches.map(f => f.url.split('/').slice(-2).join('/'))).toEqual(['agent_events/insertAll'])
  expect(deadLetterRows(world).length).toBeGreaterThanOrEqual(2)
})

test('a 401 that comes during the exit wait fetches no fresh token and sends nothing again: the batch is spilled with the rest', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ hang: true }]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  // The token command ran once, for the token in hand.
  const tokenCommands = world.commands.length
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(UNAUTHENTICATED.status, UNAUTHENTICATED.text)
  await ending
  expect(world.commands.length).toBe(tokenCommands)
  expect(world.fetches.length).toBe(1)
  expect(deadLetterRows(world).map(r => r.content).sort()).toEqual(['{"session_end":true}', '{"session_start":true}'])
})

test('a replay whose request gets a 401 after session end began fetches no fresh token and sends nothing again', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const file = `${PARTITION_DIR}/spill-20261013T200102003Z-mine-1.jsonl`
  world.files.set(file, parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'earlier-session')))
  world.replies = [{ hang: true }]
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  // The replay 15 seconds after the start sends the file, and waits for its answer.
  await world.clock.advance(15_000)
  expect(world.insertCalls().length).toBe(1)
  const tokenCommands = world.commands.length
  await $.session.end(END)
  world.releaseHung(UNAUTHENTICATED.status, UNAUTHENTICATED.text)
  await world.clock.settle()
  expect(world.commands.length).toBe(tokenCommands)
  expect(world.insertCalls().length).toBe(1)
  // The file is left as it was, for the next session.
  expect(parseFile(world.files.get(file) ?? '').lines.map(l => l.row.event_id)).toEqual(['a'.repeat(32)])
})

test('a send BigQuery takes during the exit wait starts no replay: the rows the exit spilled wait for the next session', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE, { hang: true }]
  await startSession($)
  await world.clock.advance(5000)
  // The send after the backoff is out when session end begins.
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(2)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await ending
  // Past the second a replay after a recovery waits for.
  await world.clock.advance(2000)
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world).map(r => r.content)).toEqual(['{"session_end":true}'])
  expect(await statusText($)).not.toMatch(/last backfill \(recovery/)
})

test('a replay a send after failures set going does not start once the exit began: the parked rows wait for the next session', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10' })
  world.replies = [UNAVAILABLE, OK_INSERT]
  await startSession($)
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  // The send fails with the queue past 80%: half of it goes to disk.
  await world.clock.advance(5000)
  const parked = deadLetterRows(world).length
  expect(parked > 0).toBe(true)
  // BigQuery takes the rest after the backoff: the replay of the parked rows
  // is due a second later, and session end begins before that.
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(2)
  await $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(2000)
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world).length).toBe(parked + 1)
  expect(await statusText($)).not.toMatch(/last backfill \(recovery/)
})

test('a short non-interactive session with an earlier spill on disk and an in-flight batch at session.end sends the batch, session_end, and earlier spill once each', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const earlierFile = `${PARTITION_DIR}/spill-20261013T200102003Z-earlier-1.jsonl`
  world.files.set(earlierFile, parkedFile(parkedLine(TABLE_REF, 'e'.repeat(32), 'earlier-session')))
  world.replies = [{ hang: true }, OK_INSERT, OK_INSERT]
  await $.classic.SessionStart({ source: 'startup', session_id: SESSION_ID, model: 'example-model', cwd: '/work/my-repo' })
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // At 5s the session's turn batch is in flight when session.end arrives (well before 15s auto-backfill).
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await ending
  expect(world.insertCalls().length).toBe(3)
  expect(deadLetterFiles(world)).toEqual([])
  expect(world.files.get(earlierFile)).toBe('')
  const sentIds = world.sentRows().map(r => r.event_id)
  expect(sentIds.length).toBe(5)
  expect(new Set(sentIds).size).toBe(5)
  expect(sentIds.includes('e'.repeat(32))).toBe(true)
})

test('headless /bqaa-backfill and interactive /bqaa-backfill wait await completion and report skipped files that could not be replayed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const file1 = `${PARTITION_DIR}/spill-20261013T200102003Z-one-1.jsonl`
  world.files.set(file1, parkedFile(parkedLine(TABLE_REF, '1'.repeat(32), 's1'), parkedLine(TABLE_REF, '2'.repeat(32), 's1')))
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.settle()
  const res = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  expect(res.text ?? '').toBe('bqaa backfill: 1 file(s), 2 row(s) sent, 0 left')
  expect(deadLetterFiles(world)).toEqual([])

  // Now seed a waiting file leased by another session (skipped before reading): /bqaa-backfill wait reports "1 waiting file(s) not replayed" and why it was left waiting.
  await startSession($)
  await world.clock.settle()
  const leasedName = 'spill-20261013T200102003Z-leased-1.jsonl'
  world.files.set(`${PARTITION_DIR}/${leasedName}`, parkedFile(parkedLine(TABLE_REF, '3'.repeat(32), 's2')))
  world.store.set(`lease:${TABLE_REF}/${leasedName}`, { by: 'other-session', until: Date.now() + 60_000 })
  const skippedRes = await $.command.run({ command: 'bqaa-backfill', args: 'wait', ...COMMAND })
  expect(skippedRes.text ?? '').toBe(`bqaa backfill: 0 file(s), 0 row(s) sent, 0 left, 1 waiting file(s) not replayed; left waiting: another session's lease holds ${leasedName} (ten minutes at most); it waits for the next replay`)
  expect(await statusText($)).toMatch(/last backfill \(command, .*\): 0 file\(s\), 0 rows sent, 0 left, 1 skipped; left waiting: another session's lease holds spill-20261013T200102003Z-leased-1\.jsonl/)
})

test('/bqaa-status separates elsewhere waiting files from leftovers and shows clean_dead_letter.py (dry run by default) only at >= 100 retired files', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  // 1 active file and 2 retired leftovers in another table's partition, plus 97 retired files in this table's partition (99 total retired < 100).
  world.files.set(`${DEAD_LETTER_DIR}/${OTHER_TABLE}/spill-20261013T200102003Z-other-1.jsonl`, parkedFile(parkedLine(OTHER_TABLE, 'b'.repeat(32), 'other-s')))
  world.files.set(`${DEAD_LETTER_DIR}/${OTHER_TABLE}/spill-20261013T200102003Z-retired-1.jsonl`, '')
  world.files.set(`${DEAD_LETTER_DIR}/${OTHER_TABLE}/outcome-spill-20261013T200102003Z-retired-1.jsonl`, '{"fates":"L"}\n')
  // dry-run/ inside deadLetterDir is completely skipped by deadLetterSummary.
  world.files.set(`${DRY_RUN_DIR}/dry-run-20261013T200102003Z-1.jsonl`, '{"row":1}\n')
  world.files.set(`${DRY_RUN_DIR}/spill-20261013T200102003Z-ignored.jsonl`, parkedFile(parkedLine(OTHER_TABLE, 'c'.repeat(32), 'dry-s')))
  for (let i = 0; i < 97; i += 1) {
    world.files.set(`${PARTITION_DIR}/spill-20261013T200102003Z-r${i}.jsonl`, '')
  }
  await startSession($)
  const below100 = await statusText($)
  expect(below100).toContain(`1 waiting file(s) (2 leftover(s)) in ${DEAD_LETTER_DIR} for other tables or an older layout, not replayed under this configuration`)
  expect(below100).not.toContain('cleanup (run while no session is running; dry run by default, pass --execute to delete):')

  // Adding 1 more retired leftover reaches 100 total (98 here + 2 elsewhere) and surfaces the cleanup command.
  world.files.set(`${PARTITION_DIR}/spill-20261013T200102003Z-r97.jsonl`, '')
  const at100 = await statusText($)
  expect(at100).toMatch(/^cleanup \(run while no session is running; dry run by default, pass --execute to delete\): python3 '\/[^']*\/scripts\/clean_dead_letter\.py'$/m)
})

test('/bqaa-status counts outcome-* records as leftovers only beside an emptied file in the same directory: never a replay mark beside a file that still holds rows, nor a record whose file is gone', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const other = `${DEAD_LETTER_DIR}/${OTHER_TABLE}`
  const record = '{"fates":"L"}\n'
  const sendingMark = '{"size":200,"sending":true,"by":"aaaaaaaaaa"}\n'
  const doneMark = '{"size":200,"retired":true}\n'
  // This table's: a file a replay is sending (its mark beside it), a file a
  // replay finished (emptied, its done mark and its record beside it), and a
  // record whose file is gone.
  world.files.set(`${PARTITION_DIR}/spill-20261013T200102003Z-sending-1.jsonl`, parkedFile(parkedLine(TABLE_REF, 'a'.repeat(32), 'old-session')))
  world.files.set(`${PARTITION_DIR}/outcome-done-aaaaaaaaaa-spill-20261013T200102003Z-sending-1.jsonl`, sendingMark)
  world.files.set(`${PARTITION_DIR}/spill-20261013T200102003Z-done-1.jsonl`, '')
  world.files.set(`${PARTITION_DIR}/outcome-done-bbbbbbbbbb-spill-20261013T200102003Z-done-1.jsonl`, doneMark)
  world.files.set(`${PARTITION_DIR}/outcome-spill-20261013T200102003Z-done-1.jsonl`, record)
  world.files.set(`${PARTITION_DIR}/outcome-spill-20261013T200102003Z-gone-1.jsonl`, record)
  // Another table's: the same, and a record for an emptied file of this
  // table's, which is in another directory, so no leftover here.
  world.files.set(`${other}/spill-20261013T200102003Z-sending-2.jsonl`, parkedFile(parkedLine(OTHER_TABLE, 'b'.repeat(32), 'old-session')))
  world.files.set(`${other}/outcome-done-aaaaaaaaaa-spill-20261013T200102003Z-sending-2.jsonl`, sendingMark)
  world.files.set(`${other}/spill-20261013T200102003Z-done-2.jsonl`, '')
  world.files.set(`${other}/outcome-b-spill-20261013T200102003Z-done-2.jsonl`, record)
  world.files.set(`${other}/outcome-spill-20261013T200102003Z-gone-2.jsonl`, record)
  world.files.set(`${other}/outcome-spill-20261013T200102003Z-done-1.jsonl`, record)
  // The older flat layout, in the dead-letter directory itself: an emptied
  // file and its record, and a done mark whose file is gone.
  world.files.set(`${DEAD_LETTER_DIR}/spill-20261013T200102003Z-loose-3.jsonl`, '')
  world.files.set(`${DEAD_LETTER_DIR}/outcome-spill-20261013T200102003Z-loose-3.jsonl`, record)
  world.files.set(`${DEAD_LETTER_DIR}/outcome-done-dddddddddd-spill-20261013T200102003Z-gone-3.jsonl`, doneMark)
  await startSession($)
  const status = await statusText($)
  expect(status).toMatch(/^dead-letter: 1 file\(s\), [^;]* \(1 spill\) in [^;]*; 3 replayed\/retired file\(s\) \(0-byte or outcome-\*, expected leftovers; host fs cannot delete\): 1 empty file\(s\); 2 replay record\(s\), kept on purpose$/m)
  expect(status).toContain(`dead-letter: 1 waiting file(s) (4 leftover(s)) in ${DEAD_LETTER_DIR} for other tables or an older layout, not replayed under this configuration`)
  // However many replay marks beside a file that still holds rows, and
  // records whose file is gone, there are: no leftovers, no cleanup command.
  for (let i = 0; i < 60; i += 1) {
    world.files.set(`${PARTITION_DIR}/outcome-done-${String(i).padStart(10, 'c')}-spill-20261013T200102003Z-sending-1.jsonl`, sendingMark)
    world.files.set(`${other}/outcome-spill-20261013T200102003Z-gone-${i + 4}.jsonl`, record)
  }
  const crowded = await statusText($)
  expect(crowded).toContain('; 3 replayed/retired file(s) ')
  expect(crowded).toContain(' (4 leftover(s)) ')
  expect(crowded).not.toContain('cleanup (run while no session is running')
})

test('/bqaa-status passes custom deadLetterDir with shell quoting in the cleanup command', { options: { ...OPTIONS, deadLetterDir: "/tmp/custom dir/it's-bqaa" } }, async ($, on) => {
  const customDir = "/tmp/custom dir/it's-bqaa"
  const world = setUp(on)
  for (let i = 0; i < 100; i += 1) {
    world.files.set(`${customDir}/${TABLE_REF}/spill-20261013T200102003Z-r${i}.jsonl`, '')
  }
  await startSession($)
  const status = await statusText($)
  expect(status).toMatch(/^cleanup \(run while no session is running; dry run by default, pass --execute to delete\): python3 '\/[^']*\/scripts\/clean_dead_letter\.py' '\/tmp\/custom dir\/it'\\''s-bqaa'$/m)
})

test('an HTTP 400 or partial rejection at session.end awaits the rejected file write and leaves zero uncancelled backoff timers', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)

  world.replies = [BAD_REQUEST]
  await $.tool.call({ tool: 'Read', file_path: 'bad.ts' })
  await $.session.end({ ...END, reason: 'other' })
  const rejected = [...world.files.keys()].filter(k => k.includes('/rejected-'))
  expect(rejected.length).toBe(1)
  expect(deadLetterRows(world).length).toBe(3)
  const fetchesAtExit = world.fetches.length
  await world.clock.advance(60_000)
  expect(world.fetches.length).toBe(fetchesAtExit)

  // Direct Writer timer check: neither HTTP 400 nor 200 partial-rejection at spillForExit leaves an uncancelled timer.
  for (const answer of [
    BAD_REQUEST,
    { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad row' }] }] }) },
  ]) {
    const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-timers' }, {})
    const timers: { ms: number; cancelled: boolean }[] = []
    const files = new Map<string, string>()
    const io: Io = {
      now: () => 1000,
      after: ms => {
        const entry = { ms, cancelled: false }
        timers.push(entry)
        return { cancel: () => { entry.cancelled = true } }
      },
      fetch: async () => answer,
      runCommand: async () => ({ exitCode: 0, stdout: 'a'.repeat(24), stderr: '' }),
      envToken: async () => 'env-token-for-exit-check',
      readFile: async p => files.get(p) ?? '',
      writeFile: async (p, text) => { files.set(p, text) },
      listDir: async () => [],
      storeGet: async () => undefined,
      storeSet: async () => undefined,
      storeDelete: async () => undefined,
      status: () => undefined,
      debug: () => undefined,
    }
    const writer = new Writer(io, () => config, () => 'exit-timer-session')
    writer.enqueue(
      buildRow({
        eventType: 'STATE_DELTA',
        timestampMs: 1000,
        eventId: '0'.repeat(32),
        agent: 'coding-agent',
        sessionId: 'exit-timer-session',
        invocationId: null,
        userId: 'u',
        traceId: null,
        spanId: null,
        parentSpanId: null,
        content: { session_end: true },
        attributes: {},
        maxPayloadChars: 16384,
      }).row,
      0,
    )
    await writer.spillForExit(1500, { sendQueued: true })
    expect(timers.filter(t => !t.cancelled)).toEqual([])
    expect([...files.keys()].some(k => k.includes('/rejected-'))).toBe(true)
  }
})

test('a short non-interactive session where autoBackfill is already running at session.end still sends session_end and finishes replaying the spill', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const earlierFile = `${PARTITION_DIR}/spill-20261013T200102003Z-earlier-active.jsonl`
  world.files.set(earlierFile, parkedFile(parkedLine(TABLE_REF, 'f'.repeat(32), 'earlier-session')))
  world.replies = [OK_INSERT, { hang: true }, OK_INSERT]
  await $.classic.SessionStart({ source: 'startup', session_id: SESSION_ID, model: 'example-model', cwd: '/work/my-repo' })
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  // At 5s session_start lands (200 OK), which in `-p` mode immediately starts autoBackfill whose insertAll hangs in flight when session.end arrives.
  await world.clock.advance(5000)
  await world.clock.settle()
  expect(world.insertCalls().length).toBe(2)
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(50)
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await ending
  expect(world.insertCalls().length).toBe(3)
  expect(deadLetterFiles(world)).toEqual([])
  expect(world.files.get(earlierFile)).toBe('')
  const sentIds = world.sentRows().map(r => r.event_id)
  expect(sentIds.includes('f'.repeat(32))).toBe(true)
  expect(
    byType(world.sentRows(), 'STATE_DELTA')
      .filter(r => r.session_id === SESSION_ID)
      .map(r => JSON.parse(r.content ?? '{}')),
  ).toEqual([{ session_start: true }, { session_end: true }])
})

// ---- session end's own request, takeovers and the commands around them ----

const TABLE_FOUND = { status: 200, text: '{"tableReference":{"projectId":"my-gcp-project","datasetId":"agent_analytics","tableId":"agent_events"}}' }
const DENIED_NOTICE = 'rows are not being written: BigQuery denied the identity access to the dataset'
const DENIED_LINE = /^BQAA: BigQuery denied the identity access to the dataset\. To fix, /

// session.start again with no reload (the plugin turned off and on): a new
// writer takes over from the one before (`adopt`).
const restart = ($: Engine, surface: 'terminal' | 'web' = 'terminal'): Promise<unknown> => $.session.start({ surface, isInteractive: true, cwd: '/work/my-repo' })

// A writer of its own, as session.start makes one, on this test's clock,
// files, store and scripted BigQuery answers, with a configuration no option
// sets (`overrides`).
function ownWriter(world: World, overrides: Partial<Config> = {}): Writer {
  const config: Config = { ...resolveConfig(OPTIONS, { HOME, USER: 'dev' }), ...overrides }
  return new Writer(world.createIo(), () => config, () => SESSION_ID)
}

// A row of this session's, for a writer of its own.
const ownRow = (eventId: string) =>
  buildRow({
    eventType: 'STATE_DELTA',
    timestampMs: 1000,
    eventId,
    agent: 'coding-agent',
    sessionId: SESSION_ID,
    invocationId: null,
    userId: 'dev',
    traceId: null,
    spanId: null,
    parentSpanId: null,
    content: { session_end: true },
    attributes: {},
    maxPayloadChars: 16384,
  }).row

test('with BQAA_EXIT_FLUSH_MS=300, under the 450 ms its own request needs, session end sends nothing: its rows go to a spill file', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_EXIT_FLUSH_MS: '300' })
  await startSession($)
  // A token is at hand.
  await world.clock.settle()
  expect(world.commands.length).toBe(1)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await $.session.end(END)
  expect(world.insertCalls()).toEqual([])
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['STATE_DELTA', 'TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
})

// Its 800 ms budget: the request starts by 350 ms in, when 450 ms are left,
// or not at all.
for (const [answerMs, sends] of [
  [340, true],
  [360, false],
] as const) {
  test(`session end's own request starts only while 450 ms of its budget are left: a batch out answered ${answerMs} ms in ${sends ? 'lets the rows queued after it go in one request' : 'leaves them to a spill file'}`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    world.replies = [{ hang: true }, OK_INSERT]
    await startSession($)
    await world.clock.advance(5000)
    expect(world.insertCalls().length).toBe(1)
    await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
    const ending = $.session.end({ ...END, reason: 'other' })
    await world.clock.advance(answerMs)
    world.releaseHung(OK_INSERT.status, OK_INSERT.text)
    await ending
    expect(world.insertCalls().length).toBe(sends ? 2 : 1)
    expect(deadLetterRows(world).map(r => r.event_type)).toEqual(sends ? [] : ['TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
  })
}

test('rows parked on disk this session keep session end from sending, though a later send landed and no failure, backoff or replay is left', { options: OPTIONS }, async ($, on) => {
  // Replays off: nothing replays the parked rows, or clears their mark.
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10', BQAA_AUTO_BACKFILL: 'false' })
  world.replies = [UNAVAILABLE, OK_INSERT]
  await startSession($)
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  // The send fails with the queue past 80%: half of it goes to disk.
  await world.clock.advance(5000)
  const parked = deadLetterRows(world).length
  expect(parked > 0).toBe(true)
  // BigQuery takes the rest after the backoff.
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(2)
  expect(await statusText($)).not.toMatch(/backing off/)
  await $.tool.call({ tool: 'Read', file_path: 'last.ts' })
  await $.session.end(END)
  // Its own rows go behind the parked ones, to a spill file.
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world).length).toBe(parked + 3)
})

test('in a -p run, session end that spills rows past one batch replays no earlier spill file at its end', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const earlier = `${PARTITION_DIR}/spill-20261013T200102003Z-earlier-1.jsonl`
  const file = parkedFile(parkedLine(TABLE_REF, 'e'.repeat(32), 'earlier-session'))
  world.files.set(earlier, file)
  const writer = ownWriter(world, { maxBatchRows: 2 })
  writer.prefetchToken()
  await world.clock.settle()
  for (const id of ['1', '2', '3']) writer.enqueue(ownRow(id.repeat(32)), 0)
  const exiting = writer.spillForExit(800, { sendQueued: true, replaySpills: true })
  await world.clock.advance(800)
  // One batch goes; the row past it is spilled, so the replay does not run.
  expect(await exiting).toEqual({ spilled: 1 })
  expect(world.insertCalls().map(call => (JSON.parse(call.body) as { rows: { json: { event_id: string } }[] }).rows.map(r => r.json.event_id))).toEqual([['1'.repeat(32), '2'.repeat(32)]])
  expect(world.files.get(earlier)).toBe(file)
  expect(deadLetterRows(world).map(r => r.event_id).sort()).toEqual(['3'.repeat(32), 'e'.repeat(32)])
})

test('a headless /bqaa-backfill still replaying after 8 s says the files left wait for a later session, never that /bqaa-status shows progress', { options: OPTIONS }, async ($, on) => {
  // The replay's request is not given up before the command stops waiting.
  const world = setUp(on, { BQAA_REQUEST_TIMEOUT_MS: '30000' })
  world.files.set(`${PARTITION_DIR}/spill-20261013T200102003Z-slow-1.jsonl`, parkedFile(parkedLine(TABLE_REF, '9'.repeat(32), 'earlier-session')))
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.settle()
  // BigQuery does not answer the replay.
  world.replies = [{ hang: true }]
  const running = $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND })
  await world.clock.advance(8_000)
  expect((await running).text ?? '').toBe(
    `BigQuery Agent Analytics mod: backfill of 1 file(s) in ${PARTITION_DIR} did not finish within 8 s before this non-interactive run ended; remaining files stay queued for a later session (or /bqaa-backfill force after 10 minutes if a batch was still in flight).`,
  )
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
})

test('a refused /bqaa-setup, which the tracing plugin may hold, is no internal error; a refused name of the mod still is', { options: OPTIONS }, async ($, on) => {
  on('command.register', { name: 'bqaa-setup' }, () => ({ deny: 'command already registered by another plugin' }))
  on('command.register', { name: 'bqaa-flush' }, () => ({ deny: 'command refused' }))
  const world = setUp(on)
  await startSession($)
  expect(world.registered).toEqual(['bqaa-status', 'bqaa-backfill', 'bqaa-mod-setup'])
  expect(await statusText($)).toMatch(/^internal errors \(skipped bookkeeping\): 1$/m)
})

// After a takeover, the writer before holds what it last saw: a failure or a
// notice from before the new writer's own sends.
for (const [failure, replies] of [
  ['a 503', [UNAVAILABLE]],
  ['a 401, with a fresh token refused too', [UNAUTHENTICATED, UNAUTHENTICATED]],
] as const) {
  test(`after ${failure} on the writer before a takeover, session end still sends once the new writer's own send landed`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    world.replies = [...replies, OK_INSERT]
    await startSession($)
    await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
    await world.clock.advance(5000)
    expect(world.acceptedRows()).toEqual([])
    await restart($)
    await world.clock.advance(5000)
    const landed = world.acceptedRows().length
    expect(landed).toBe(3)
    await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
    await $.session.end(END)
    // Its own request: the call's two rows and session_end, none spilled.
    expect(world.acceptedRows().length).toBe(landed + 3)
    expect(deadLetterRows(world)).toEqual([])
  })
}

test('a batch the writer before a takeover still has out, answered with a failure while session end waits, keeps session end from sending: every row is spilled', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [{ hang: true }, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(1)
  await restart($)
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(UNAVAILABLE.status, UNAVAILABLE.text)
  await ending
  expect(world.insertCalls().length).toBe(1)
  expect(deadLetterRows(world).map(r => r.event_type).sort()).toEqual(['STATE_DELTA', 'STATE_DELTA', 'TOOL_COMPLETED', 'TOOL_COMPLETED', 'TOOL_STARTING', 'TOOL_STARTING'])
})

test('a batch the writer before a takeover has out after a failure keeps session end from sending, though it lands while session end waits', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [UNAVAILABLE, { hang: true }, OK_INSERT]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  // After the backoff the batch goes again, and is out when the takeover
  // comes.
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(2)
  await restart($)
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  const ending = $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(100)
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await ending
  // No request after the batch, which landed: the rows queued behind it
  // were spilled.
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world).map(r => r.event_type)).toEqual(['TOOL_STARTING', 'TOOL_COMPLETED', 'STATE_DELTA'])
})

test('a replay that a send after failures set going on the writer before a takeover keeps session end from sending while it is due', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10' })
  world.replies = [UNAVAILABLE, OK_INSERT]
  await startSession($)
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  await world.clock.advance(5000)
  const parked = deadLetterRows(world).length
  expect(parked > 0).toBe(true)
  // BigQuery takes the rest after the backoff: the replay of the parked rows
  // is due a second later, and the takeover and session end come first.
  await world.clock.advance(5000)
  expect(world.insertCalls().length).toBe(2)
  await restart($)
  // The new writer has its token at hand (nothing else keeps it from
  // sending); the replay is still due.
  const commands = world.commands.length
  await world.clock.settle()
  expect(world.commands.length).toBe(commands + 1)
  await $.tool.call({ tool: 'Read', file_path: 'last.ts' })
  await $.session.end({ ...END, reason: 'other' })
  await world.clock.advance(2000)
  expect(world.insertCalls().length).toBe(2)
  expect(deadLetterRows(world).length).toBe(parked + 3)
})

test('a takeover keeps a denial of writes BigQuery gave rows: the status line shows it again, a /bqaa-status check that finds the table keeps it, it is told once, and a write BigQuery takes clears it', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED]
  await $.classic.SessionStart({ source: 'startup', session_id: SESSION_ID, model: 'example-model', cwd: '/work/my-repo' })
  // A surface that draws no band above the prompt: the notice is the status
  // line.
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1) ?? '').toMatch(DENIED_LINE)
  expect(world.toasts.length).toBe(1)
  await restart($, 'web')
  expect(world.statusLines.at(-1) ?? '').toMatch(DENIED_LINE)
  // Refused again under the new writer: no second toast or transcript line.
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  expect(world.toasts.length).toBe(1)
  expect(world.transcript.filter(l => l.startsWith('bqaa: rows are not being written')).length).toBe(1)
  world.replies = [TABLE_FOUND]
  const checked = (await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND })).text ?? ''
  expect(checked).toContain('destination check: BigQuery found my-gcp-project.agent_analytics.agent_events')
  expect(checked).toContain(DENIED_NOTICE)
  world.replies = [OK_INSERT]
  await $.tool.call({ tool: 'Read', file_path: 'c.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBeUndefined()
  expect(world.transcript.filter(l => l.includes('/bqaa-backfill sends'))).toEqual(['bqaa: rows are being written again; /bqaa-backfill sends the 5 row(s) parked while they could not be.'])
})

test('a takeover under another dataset starts clean: a denial of writes to the table before does not carry over', { options: { project: 'my-gcp-project' } }, async ($, on) => {
  let dataset = 'agent_analytics'
  on('env.get', { name: 'BQAA_DATASET' }, () => ({ value: dataset }))
  const world = setUp(on)
  world.replies = [DENIED]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(await statusText($)).toContain(DENIED_NOTICE)
  dataset = 'other_dataset'
  await restart($, 'web')
  expect(world.statusLines.at(-1)).toBeUndefined()
  expect(await statusText($)).not.toContain('rows are not being written')
})

// The host checks option values against the JSON types plugin.json declares
// before it loads the mod (a string for a boolean or a number keeps the mod
// from loading at all), so the coercions below are the mod's own defense in
// depth, for a host or a settings path that hands it such values anyway;
// numbers out of range reach it and are clamped.
test('/bqaa-status and the debug log surface the option clamping notices', { options: { ...OPTIONS, maxPayloadChars: 9e9, flushIntervalMs: 50 } }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const status = await statusText($)
  expect(status).toContain('flush every 1000 ms')
  expect(status).toContain('config notice: option maxPayloadChars (9000000000) is outside 1024-500000; clamped to 500000')
  expect(status).toContain('config notice: option flushIntervalMs (50) is outside 1000-300000; clamped to 1000')
  const debugNotices = world.logs.filter(l => l.startsWith('bqaa: config notice:'))
  expect(debugNotices.length).toBe(2)
  expect(debugNotices.some(l => l.includes('maxPayloadChars') && l.includes('clamped to 500000'))).toBe(true)
})

test('runtime defense in depth: wrong-type boolean/number options coerce safely in resolveConfig and surface config notices in status', async ($, on) => {
  const world = setUp(on)
  const coerced = resolveConfig(
    { ...OPTIONS, maxPayloadChars: 9e9, flushIntervalMs: 50, dryRun: 'false' as never, userIdFromCredentials: 'maybe' as never },
    { HOME, USER: 'dev' },
  )
  const writer = ownWriter(world, coerced)
  const status = formatStatus({
    config: coerced,
    writer,
    now: world.clock.now(),
    sessionId: SESSION_ID,
    deadLetter: { dir: PARTITION_DIR, files: 0, bytes: 0, byKind: {}, oversized: [], elsewhere: 0 },
    backfill: null,
    isBackfilling: false,
    cleanupCommand: null,
    destinationCheck: null,
  })
  expect(coerced.dryRun).toBe(false)
  expect(coerced.userIdFromCredentials).toBe(false)
  expect(status).toContain('flush every 1000 ms')
  expect(status).toContain('config notice: option dryRun ("false") coerced from string to boolean false')
  expect(status).toContain('config notice: option maxPayloadChars (9000000000) is outside 1024-500000; clamped to 500000')
  expect(status).toContain('config notice: option flushIntervalMs (50) is outside 1000-300000; clamped to 1000')
  expect(status).toContain('config notice: option userIdFromCredentials ("maybe") is not a valid boolean; using default false')

  const disabled = resolveConfig({ ...OPTIONS, enabled: 'false' as never }, { HOME, USER: 'dev' })
  const disabledWriter = ownWriter(world, disabled)
  disabledWriter.enqueue(ownRow('a'.repeat(32)), 0)
  await world.clock.advance(5000)
  expect(disabled.enabled).toBe(false)
  expect(world.fetches).toEqual([])
  expect(world.acceptedRows()).toEqual([])
  const disabledStatus = formatStatus({
    config: disabled,
    writer: disabledWriter,
    now: world.clock.now(),
    sessionId: SESSION_ID,
    deadLetter: { dir: PARTITION_DIR, files: 0, bytes: 0, byKind: {}, oversized: [], elsewhere: 0 },
    backfill: null,
    isBackfilling: false,
    cleanupCommand: null,
    destinationCheck: null,
  })
  expect(disabledStatus).toContain('BigQuery Agent Analytics mod: disabled')
  expect(disabledStatus).toContain('config notice: option enabled ("false") coerced from string to boolean false')
})

// What the host passes a hook drawing in the band above the prompt.
const WELCOME_BAND = {
  component: 'AbovePrompt',
  requestId: 'above-prompt',
  viewport: { columns: 120, rows: 40 },
  props: { hasSurvey: false, isCollapsed: false, isWorking: false, maxRows: 10, bodyColumns: 115, scroll: { offset: 0, bodyRows: 10 }, view: {} },
} as const

test('the first session after an install shows the configuration notices with the welcome card and in its transcript line, without /bqaa-status', { options: { ...OPTIONS, flushIntervalMs: 50, maxPayloadChars: 100 } }, async ($, on) => {
  const world = setUp(on, {}, {})
  await startSession($)
  await world.clock.settle()
  const ui = await $.ui.mount({ plugin: 'bigquery-agent-analytics-mod', surface: 'terminal', ...WELCOME_BAND })
  const texts = (await ui.findAll({ type: 'Text' })).map(t => t.text)
  await ui.unmount()
  const notices = ['config notice: option maxPayloadChars (100) is outside 1024-500000; clamped to 1024', 'config notice: option flushIntervalMs (50) is outside 1000-300000; clamped to 1000']
  // Under the card's own lines, and before what the mods after it drew.
  expect(texts.slice(-3)).toEqual([...notices, 'drawn beneath'])
  expect(texts.some(t => t.startsWith('◌ Waiting for the first rows, sent every 1 s'))).toBe(true)
  const [greeting = ''] = world.transcript.filter(l => l.startsWith('bqaa: BigQuery Agent Analytics mod installed'))
  expect(greeting.endsWith(`; /bqaa-status shows what was sent. ${notices.join(' | ')}`)).toBe(true)
})

test('exit auto-create spills without wasted insertAll on 403 createTable denial', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  world.replies = [{ hang: true }, DENIED]
  const writer = ownWriter(world)
  writer.enqueue(ownRow('1'.repeat(32)), 0)
  const flushing = writer.flush()
  await world.clock.settle()
  expect(world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual(['POST insertAll'])
  const exiting = writer.spillForExit(5_000, { sendQueued: true })
  await world.clock.advance(50)
  world.releaseHung(TABLE_MISSING.status, TABLE_MISSING.text)
  await flushing
  await exiting
  expect(world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual(['POST insertAll', 'POST tables'])
  expect(deadLetterRows(world).map(r => r.event_id)).toEqual(['1'.repeat(32)])
})

test('backfill auto-creates missing table once after transient 503 createTable error', { options: OPTIONS }, async ($, on) => {
  const world2 = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const spillPath = `${PARTITION_DIR}/spill-20261013T200102003Z-retry-create-1.jsonl`
  world2.files.set(spillPath, parkedFile(parkedLine(TABLE_REF, '2'.repeat(32), 's-create')))
  world2.replies = [TABLE_MISSING, UNAVAILABLE]
  const writer2 = ownWriter(world2)
  const firstReport = await writer2.backfill(['spill', 'retry'], 'command')
  expect(firstReport.rowsSent).toBe(0)
  expect(world2.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual(['POST insertAll', 'POST tables'])
  expect(world2.files.get(spillPath)).not.toBe('')

  world2.replies = [TABLE_MISSING, { status: 200, text: '{"kind":"bigquery#table"}' }, OK_INSERT]
  const secondReport = await writer2.backfill(['spill', 'retry'], 'command')
  expect(secondReport.rowsSent).toBe(1)
  expect(world2.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual([
    'POST insertAll',
    'POST tables',
    'POST insertAll',
    'POST tables',
    'POST insertAll',
  ])
  expect(world2.files.get(spillPath)).toBe('')
})

test('foreground send retries createTable after transient 503 and latches once created without duplicating rows', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  world.replies = [TABLE_MISSING, UNAVAILABLE]
  const writer = ownWriter(world)
  writer.enqueue(ownRow('a'.repeat(32)), 0)
  await writer.flush()
  expect(world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual(['POST insertAll', 'POST tables'])
  expect(world.acceptedRows()).toEqual([])

  await world.clock.advance(5_000)
  world.replies = [TABLE_MISSING, { status: 200, text: '{"kind":"bigquery#table"}' }, OK_INSERT]
  await writer.flush()
  expect(world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual([
    'POST insertAll',
    'POST tables',
    'POST insertAll',
    'POST tables',
    'POST insertAll',
  ])
  expect(world.acceptedRows().map(r => r.event_id)).toEqual(['a'.repeat(32)])

  // Once createTable succeeds (200), a later 404 does not trigger another POST tables.
  world.replies = [TABLE_MISSING]
  writer.enqueue(ownRow('b'.repeat(32)), 0)
  await writer.flush()
  expect(world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)).toEqual([
    'POST insertAll',
    'POST tables',
    'POST insertAll',
    'POST tables',
    'POST insertAll',
    'POST insertAll',
  ])
})

test('relocated deadLetterDir preserves legacy child files via basename matching while still dropping retired competing-writer duplicates', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const oldPartition = `/old/dead-letter/${TABLE_REF}`
  const newPartition = PARTITION_DIR
  const parentName = 'spill-20261013T200102003Z-parent-1.jsonl'
  const childName = 'retry-20261013T200103003Z-child-1.jsonl'
  const orphanName = 'retry-20261013T200104003Z-orphan-2.jsonl'

  // Case A: parent is retired; legacy done mark and child header hold old absolute paths.
  world.files.set(`${newPartition}/${parentName}`, '')
  world.files.set(
    `${newPartition}/outcome-done-aaaaaaaaaa-${parentName}`,
    sealFile(`${JSON.stringify({ size: 200, fates: 'W', reasons: [], refusals: [], files: [`${oldPartition}/${childName}`], written: [true], waits: true, seq: 2, retired: true })}\n`),
  )
  world.files.set(`${newPartition}/${childName}`, packFiles([parkedLine(TABLE_REF, '3'.repeat(32), 's-old')], `${oldPartition}/${parentName}`)[0] as string)
  world.files.set(`${newPartition}/${orphanName}`, packFiles([parkedLine(TABLE_REF, '4'.repeat(32), 's-old')], `${oldPartition}/${parentName}`)[0] as string)

  const writer = ownWriter(world)
  world.replies = [OK_INSERT]
  const report = await writer.backfill(['spill', 'retry'], 'command')
  expect(report.rowsSent).toBe(1)
  expect(world.acceptedRows().map(r => r.event_id)).toEqual(['3'.repeat(32)])
  // Orphan sibling was dropped unsent; relocated child was replayed then emptied.
  expect(world.files.get(`${newPartition}/${orphanName}`)).toBe('')
  expect(world.files.get(`${newPartition}/${childName}`)).toBe('')

  // Case B: parent still holds rows (written: [false]) with legacy absolute parent path -> child waits safely.
  const parent2 = 'spill-20261013T200202003Z-parent-2.jsonl'
  const child2 = 'retry-20261013T200203003Z-child-2.jsonl'
  const parent2Text = parkedFile(parkedLine(TABLE_REF, '5'.repeat(32), 's-live'))
  world.files.set(`${newPartition}/${parent2}`, parent2Text)
  world.store.set(`replayed:${TABLE_REF}/${parent2}`, {
    size: utf8Bytes(parent2Text),
    fates: 'W',
    reasons: [],
    refusals: [],
    files: [`${oldPartition}/${child2}`],
    written: [false],
    waits: true,
    seq: 1,
  })
  world.files.set(`${newPartition}/${child2}`, packFiles([parkedLine(TABLE_REF, '5'.repeat(32), 's-live')], `${oldPartition}/${parent2}`)[0] as string)
  // Refused write in child2 is skipped as 'wait'; parent2 finishes writing child2 and writes basename into journal.
  world.replies = [{ status: 403, text: '{"error":{"code":403,"message":"invalid row","errors":[{"reason":"invalid"}]}}' }]
  const report2 = await writer.backfill(['spill', 'retry'], 'command')
  expect(report2.files).toBeGreaterThanOrEqual(1)
  const storedRecord = world.store.get(`replayed:${TABLE_REF}/${parent2}`) as { files?: string[] } | undefined
  if (storedRecord?.files && storedRecord.files.length > 0) {
    expect(storedRecord.files.every(f => !f.includes('/'))).toBe(true)
  }
})

test('held files (sending: true) avoid backoff and exit lockout, emit status notice, and reset cooldown on /bqaa-flush', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const heldName = 'retry-20261013T200102003Z-held-1.jsonl'
  const heldPath = `${PARTITION_DIR}/${heldName}`
  const heldText = parkedFile(parkedLine(TABLE_REF, '6'.repeat(32), 'other-sess'))
  world.files.set(heldPath, heldText)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-bbbbbbbbbb-${heldName}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(heldText), sending: true, by: 'bbbbbbbbbb' })}\n`),
  )

  world.replies = [UNAVAILABLE]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5_000)
  world.replies = [OK_INSERT]
  await world.clock.advance(5_000)
  // At 15 s autoBackfill marks partition due; at 20 s recovery backfill runs and enters 30 s heldCheckTimer.
  await world.clock.advance(5_000)
  await world.clock.advance(5_000)
  const statusWhileHeld = await statusText($)
  expect(statusWhileHeld).not.toMatch(/backing off:/)
  expect(statusWhileHeld).toMatch(/^notice: a replay of retry-20261013T200102003Z-held-1\.jsonl sent rows/m)

  // Track fs.list calls during the 30 s held cooldown, then clear cooldown with /bqaa-flush.
  let listCalls = 0
  on('fs.list', { path: PARTITION_DIR }, ($, e, next) => {
    listCalls += 1
    return next(e)
  })
  await world.clock.advance(10_000)
  expect(listCalls).toBe(0)

  // Mark the file as retired by the other session and empty it, then /bqaa-flush triggers immediate recovery and clears notice:.
  world.files.set(
    `${PARTITION_DIR}/outcome-done-bbbbbbbbbb-${heldName}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(heldText), fates: 'L', reasons: [], refusals: [], files: [], written: [], waits: false, seq: 1, retired: true })}\n`),
  )
  world.files.set(heldPath, '')
  await $.command.run({ command: 'bqaa-flush', args: '', ...COMMAND })
  await world.clock.settle()
  expect(listCalls).toBeGreaterThan(0)
  expect(await statusText($)).not.toMatch(/^notice:/m)

  // Subsequent turn & session.end deliver normally (prior failure did not lock out exit send).
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await $.session.end(END)
  expect(world.acceptedRows().some(r => r.content?.includes('b.ts'))).toBe(true)
})

test('error-free recovery against only held files resets prior consecutiveFailures so exit send remains enabled', { options: OPTIONS }, async ($, on) => {
  const world2 = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const heldName = 'retry-20261013T200102003Z-held-1.jsonl'
  const heldPath = `${PARTITION_DIR}/${heldName}`
  const heldText = parkedFile(parkedLine(TABLE_REF, '6'.repeat(32), 'other-sess'))
  world2.files.set(heldPath, heldText)
  world2.files.set(
    `${PARTITION_DIR}/outcome-done-bbbbbbbbbb-${heldName}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(heldText), sending: true, by: 'bbbbbbbbbb' })}\n`),
  )
  world2.replies = [UNAVAILABLE]
  const writer2 = ownWriter(world2, { maxQueueRows: 1 })
  writer2.enqueue(ownRow('7'.repeat(32)), 0)
  await writer2.flush()
  expect(writer2.snapshot().consecutiveFailures).toBe(1)
  expect(writer2.queue.length).toBe(0)
  for (const [p] of deadLetterFiles(world2)) {
    if (p !== heldPath) world2.files.set(p, '')
  }
  await world2.clock.advance(5_000)
  await writer2.flush()
  expect(writer2.snapshot().consecutiveFailures).toBe(0)
  expect(writer2.snapshot().pausedUntil).toBe(0)
  world2.replies = [OK_INSERT]
  writer2.enqueue(ownRow('9'.repeat(32)), 0)
  const exited = await writer2.spillForExit(800, { sendQueued: true })
  expect(exited.spilled).toBe(0)
  expect(world2.acceptedRows().some(r => r.event_id === '9'.repeat(32))).toBe(true)
})

// ---- Issue #53: exit replay budget and uncertain-delivery marks ----------

function exitReplayHarness(overrides: Partial<Config> = {}) {
  const config: Config = { ...resolveConfig(OPTIONS, { HOME, USER: 'dev' }), ...overrides }
  const files = new Map<string, string>()
  const mtimes = new Map<string, number>()
  const entryKinds = new Map<string, string>()
  const store = new Map<string, unknown>()
  const timers: { at: number; fn: () => void; cancelled: boolean }[] = []
  const requests: { url: string; ids: string[]; at: number; resolve: (answer: { status: number; text: string }) => void; reject: (err: unknown) => void }[] = []
  const sent: { id: string; status: number }[] = []
  let now = 0
  let fetchHandler: (
    url: string,
    ids: string[],
    controls: { resolve: (answer: { status: number; text: string }) => void; reject: (err: unknown) => void },
  ) => Promise<{ status: number; text: string }> | undefined = async () => OK_INSERT
  let listDirHook: (() => Promise<void>) | null = null
  let readFileHook: ((path: string) => Promise<void>) | null = null
  let writeFileHook: ((path: string, text: string) => Promise<void>) | null = null
  let storeGetHook: ((key: string) => Promise<void>) | null = null
  let storeSetHook: ((key: string) => Promise<void>) | null = null
  let storeDeleteHook: ((key: string) => Promise<void>) | null = null

  const ioFor = (): Io => ({
    now: () => now,
    after: (ms, fn) => {
      const entry = { at: now + ms, fn, cancelled: false }
      timers.push(entry)
      return { cancel: () => { entry.cancelled = true } }
    },
    fetch: (url, init) => {
      const ids = url.endsWith('/insertAll')
        ? (JSON.parse(init.body ?? '{"rows":[]}') as { rows: { json: { event_id: string } }[] }).rows.map(r => r.json.event_id)
        : []
      return new Promise((resolve, reject) => {
        const done = (answer: { status: number; text: string }) => {
          if (url.endsWith('/insertAll')) {
            for (const id of ids) sent.push({ id, status: answer.status })
          }
          resolve(answer)
        }
        const entry = { url, ids, at: now, resolve: done, reject }
        requests.push(entry)
        const immediate = fetchHandler(url, ids, { resolve: done, reject })
        if (immediate !== undefined) {
          void immediate.then(done, reject)
        }
      })
    },
    runCommand: async () => ({ exitCode: 0, stdout: `${'a'.repeat(24)}\n`, stderr: '' }),
    envToken: async () => 'env-token-for-exit-replay',
    writeFile: async (path, text) => {
      if (writeFileHook) await writeFileHook(path, text)
      files.set(path, text)
    },
    readFile: async path => {
      if (readFileHook) await readFileHook(path)
      return files.get(path) ?? ''
    },
    exists: async path => files.has(path),
    listDir: async dir => {
      if (listDirHook) await listDirHook()
      const prefix = `${dir.replace(/\/$/, '')}/`
      return [...files]
        .filter(([path]) => path.startsWith(prefix) && !path.slice(prefix.length).includes('/'))
        .map(([path, text]) => ({ name: path.slice(prefix.length), kind: entryKinds.get(path) ?? 'file', size: utf8Bytes(text), mtimeMs: mtimes.get(path) }))
    },
    storeGet: async key => {
      if (storeGetHook) await storeGetHook(key)
      return store.get(key)
    },
    storeSet: async (key, value) => {
      if (storeSetHook) await storeSetHook(key)
      store.set(key, JSON.parse(JSON.stringify(value)) as unknown)
    },
    storeDelete: async key => {
      if (storeDeleteHook) await storeDeleteHook(key)
      store.delete(key)
    },
    status: () => undefined,
    debug: () => undefined,
  })

  const settleMicrotasks = async (): Promise<void> => {
    for (let i = 0; i < 40; i += 1) await Promise.resolve()
  }

  const advance = async (ms: number): Promise<void> => {
    const target = now + ms
    for (;;) {
      await settleMicrotasks()
      const next = timers
        .filter(t => !t.cancelled && t.at <= target)
        .sort((a, b) => a.at - b.at)[0]
      if (!next) break
      now = Math.max(now, next.at)
      next.cancelled = true
      next.fn()
    }
    now = Math.max(now, target)
    await settleMicrotasks()
  }

  const newWriter = (session = 's-1', c = config) => new Writer(ioFor(), () => c, () => session)

  const warmWriter = async (session = 's-1', c = config): Promise<Writer> => {
    const w = newWriter(session, c)
    w.enqueue(ownRow('0'.repeat(32)), 0)
    await w.flush()
    await settleMicrotasks()
    sent.length = 0
    requests.length = 0
    return w
  }

  const seedSpill = (name: string, ...eventIds: string[]): string => {
    const path = `${PARTITION_DIR}/${name}`
    files.set(path, parkedFile(...eventIds.map(id => parkedLine(TABLE_REF, id, 'old-session'))))
    return path
  }

  const doneMarksFor = (name: string) =>
    [...files.entries()]
      .filter(([p, text]) => p.includes(`/outcome-done-`) && p.endsWith(`-${name}`) && text !== '')
      .map(([p, text]) => {
        const parsed = parseFile(text)
        let data: Record<string, unknown> | null = null
        if (parsed.complete && parsed.unreadable[0]) {
          try {
            data = JSON.parse(parsed.unreadable[0]) as Record<string, unknown>
          } catch {
            data = null
          }
        }
        return { path: p, complete: parsed.complete, data }
      })

  return {
    config,
    files,
    mtimes,
    entryKinds,
    store,
    timers,
    requests,
    sent,
    advance,
    settleMicrotasks,
    newWriter,
    warmWriter,
    seedSpill,
    doneMarksFor,
    ioFor,
    sentOf: (id: string) => sent.filter(s => s.id === id).length,
    uncancelledTimers: () => timers.filter(t => !t.cancelled),
    setFetch: (
      fn: (
        url: string,
        ids: string[],
        controls: { resolve: (answer: { status: number; text: string }) => void; reject: (err: unknown) => void },
      ) => Promise<{ status: number; text: string }> | undefined,
    ) => {
      fetchHandler = fn
    },
    setListDirHook: (fn: (() => Promise<void>) | null) => {
      listDirHook = fn
    },
    setReadFileHook: (fn: ((path: string) => Promise<void>) | null) => {
      readFileHook = fn
    },
    setWriteFileHook: (fn: ((path: string, text: string) => Promise<void>) | null) => {
      writeFileHook = fn
    },
    setStoreGetHook: (fn: ((key: string) => Promise<void>) | null) => {
      storeGetHook = fn
    },
    setStoreSetHook: (fn: ((key: string) => Promise<void>) | null) => {
      storeSetHook = fn
    },
    setStoreDeleteHook: (fn: ((key: string) => Promise<void>) | null) => {
      storeDeleteHook = fn
    },
    advanceNow: (ms: number) => {
      now += ms
    },
  }
}

test('T53-1: exit replay late 200 OK between marks.replay and marks.replayPost retires mark, empties spill, and sends 0 duplicates in next session', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-1.jsonl'
  const id = 'a'.repeat(32)
  const spillPath = h.seedSpill(name, id)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(710)
  expect(h.requests.length).toBe(1)
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(90)
  await exiting

  const marks = h.doneMarksFor(name)
  expect(marks.length).toBe(1)
  expect(marks[0]?.data?.retired).toBe(true)
  expect(h.files.get(spillPath)).toBe('')
  expect(h.uncancelledTimers()).toEqual([])

  h.setFetch(async () => OK_INSERT)
  await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(id)).toBe(1)
})

test('T53-2: exit replay skips File 2 when File 1 leaves < 400 ms total / < 300 ms before marks.replay', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name1 = 'spill-20261013T200102003Z-t53-2a.jsonl'
  const name2 = 'spill-20261013T200102004Z-t53-2b.jsonl'
  const id1 = '1'.repeat(32)
  const id2 = '2'.repeat(32)
  const path1 = h.seedSpill(name1, id1)
  const path2 = h.seedSpill(name2, id2)
  const raw2 = h.files.get(path2)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(420)
  expect(h.requests.length).toBe(1)
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(380)
  await exiting

  expect(h.files.get(path1)).toBe('')
  expect(h.doneMarksFor(name1)[0]?.data?.retired).toBe(true)
  expect(h.requests.length).toBe(1)
  expect(h.sentOf(id1)).toBe(1)
  expect(h.sentOf(id2)).toBe(0)
  expect(h.doneMarksFor(name2)).toEqual([])
  expect(h.files.get(path2)).toBe(raw2)
  expect(h.uncancelledTimers()).toEqual([])

  h.setFetch(async () => OK_INSERT)
  await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(id1)).toBe(1)
  expect(h.sentOf(id2)).toBe(1)
  expect(h.files.get(path2)).toBe('')
})

test('T53-3a: exit replay does not start when sendAtExit leaves < 400 ms', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-3a.jsonl'
  const spilledId = '3'.repeat(32)
  const queuedId = '4'.repeat(32)
  const spillPath = h.seedSpill(name, spilledId)
  const raw = h.files.get(spillPath)

  w1.enqueue(ownRow(queuedId), 0)
  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(450)
  expect(h.requests.length).toBe(1)
  expect(h.requests[0]!.ids).toEqual([queuedId])
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(350)
  await exiting

  expect(h.requests.length).toBe(1)
  expect(h.sentOf(spilledId)).toBe(0)
  expect(h.doneMarksFor(name)).toEqual([])
  expect(h.files.get(spillPath)).toBe(raw)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-3b: exit replay aborts before POST and unmarks when pre-POST I/O leaves < 300 ms before marks.replay', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-3b.jsonl'
  const id = '5'.repeat(32)
  const spillPath = h.seedSpill(name, id)
  const raw = h.files.get(spillPath)

  let slowed = false
  h.setWriteFileHook(async path => {
    if (!slowed && path.includes('/outcome-done-')) {
      slowed = true
      h.advanceNow(450)
    }
  })
  h.setFetch(async () => OK_INSERT)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(h.sentOf(id)).toBe(0)
  expect(h.doneMarksFor(name)).toEqual([])
  expect(h.files.get(spillPath)).toBe(raw)
  expect(h.uncancelledTimers()).toEqual([])

  h.setWriteFileHook(null)
  await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(id)).toBe(1)
  expect(h.files.get(spillPath)).toBe('')
})

test('T53-4a: unanswered exit-replay POST retains sending: true mark across sessions until force backfill', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-4a.jsonl'
  const id = '6'.repeat(32)
  const spillPath = h.seedSpill(name, id)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(h.sent.length).toBe(0)
  expect(h.requests.length).toBe(1)
  expect(h.uncancelledTimers()).toEqual([])
  const marks = h.doneMarksFor(name)
  expect(marks.length).toBe(1)
  expect(marks[0]?.data?.sending).toBe(true)
  expect(marks[0]?.data?.retired).not.toBe(true)
  expect(h.files.get(spillPath)).not.toBe('')

  // Next session auto-backfill holds the file (0 silent duplicates).
  h.setFetch(async () => OK_INSERT)
  const w2 = h.newWriter('s-2')
  const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
  expect(autoReport.skippedFiles).toBe(1)
  expect(autoReport.held ?? '').toMatch(/sent rows and has recorded no outcome/)
  expect(h.sentOf(id)).toBe(0)

  // Explicit force backfill replays it and empties the file.
  const forceReport = await w2.backfill(['spill', 'retry'], 'force')
  expect(forceReport.rowsSent).toBe(1)
  expect(h.sentOf(id)).toBe(1)
  expect(h.files.get(spillPath)).toBe('')
})

test('T53-4b: live-session request timeouts across 3 parked files release marks and auto-deliver all 3 once BigQuery answers again', async () => {
  const h = exitReplayHarness({ requestTimeoutMs: 1000 })
  const w = await h.warmWriter('s-1')
  const names = [
    'spill-20261013T200102003Z-t53-4b-1.jsonl',
    'spill-20261013T200102004Z-t53-4b-2.jsonl',
    'retry-20261013T200102005Z-t53-4b-3.jsonl',
  ]
  const ids = ['7'.repeat(32), 'd'.repeat(32), 'e'.repeat(32)]
  const paths = names.map((n, i) => h.seedSpill(n, ids[i]!))

  // Requests hang for 10 minutes across repeated auto/recovery attempts.
  h.setFetch(() => undefined)
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const running = w.backfill(['spill', 'retry'], attempt === 0 ? 'auto' : 'recovery')
    await h.advance(150_000)
    const timedOut = await running
    expect(timedOut.error ?? '').toMatch(/timed out/)
    for (const n of names) expect(h.doneMarksFor(n)).toEqual([])
  }
  expect(h.sent.length).toBe(0)

  // Once BigQuery answers again, automatic recovery delivers all 3 files without force.
  h.setFetch(async () => OK_INSERT)
  const delivered = await w.backfill(['spill', 'retry'], 'recovery')
  expect(delivered.rowsSent).toBe(3)
  expect(delivered.skippedFiles).toBe(0)
  for (const id of ids) expect(h.sentOf(id)).toBe(1)
  for (const p of paths) expect(h.files.get(p)).toBe('')
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-4c: unanswered exit-replay POST records own-unanswered held message on both exiting and subsequent sessions until force', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-4c.jsonl'
  const id = 'f'.repeat(32)
  const spillPath = h.seedSpill(name, id)

  h.setFetch(() => undefined)
  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting
  expect(h.doneMarksFor(name)[0]?.data?.sending).toBe(true)
  expect(h.doneMarksFor(name)[0]?.data?.uncertain).toBe(true)
  expect(w.backfillReport?.held ?? '').toContain('no answer before the session ended')
  expect(w.backfillReport?.held ?? '').not.toContain('another session')
  expect(w.heldNoticeInfo().files).toBe(1)

  h.setFetch(async () => OK_INSERT)
  const w2 = h.newWriter('s-2')
  const nextAuto = await w2.backfill(['spill', 'retry'], 'auto')
  expect(nextAuto.skippedFiles).toBe(1)
  expect(nextAuto.held ?? '').toContain('no answer before the session ended')
  expect(nextAuto.held ?? '').not.toContain('another session')
  expect(h.sentOf(id)).toBe(0)

  const nextCmd = await w2.backfill(['spill', 'retry'], 'command')
  expect(nextCmd.skippedFiles).toBe(1)
  expect(nextCmd.held ?? '').toContain('no answer before the session ended')
  expect(h.sentOf(id)).toBe(0)

  const forced = await w2.backfill(['spill', 'retry'], 'force')
  expect(forced.rowsSent).toBe(1)
  expect(h.sentOf(id)).toBe(1)
  expect(h.files.get(spillPath)).toBe('')
  expect(w2.heldNoticeInfo().files).toBe(0)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-4d: slow disk I/O during post-cutoff split writeOut still populates heldNoticeInfo before marks.end settles spillForExit', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-4d.jsonl'
  h.seedSpill(name, 'e'.repeat(32), 'd'.repeat(32))

  let unblockSlowWrite = (): void => undefined
  const slowWrite = new Promise<void>(resolve => {
    unblockSlowWrite = resolve
  })
  h.setWriteFileHook(async path => {
    if (path.startsWith(`${PARTITION_DIR}/spill-`) && !path.endsWith(`/${name}`)) {
      await slowWrite
    }
  })

  h.setFetch(() => undefined)
  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  // Even while writeOut is still blocked past marks.end, heldNoticeInfo already reports 1 held file.
  expect(w.heldNoticeInfo().files).toBe(1)
  expect(w.heldNoticeInfo().reason ?? '').toContain('no answer before the session ended')
  unblockSlowWrite()
  await h.settleMicrotasks()
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-5: multi-batch exit replay with Batch 2 skipped by budget retires Batch 1, writes unmarked leftover for Batch 2, and next session sends Batch 2 once', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-5.jsonl'
  const id1 = '8'.repeat(32)
  const id2 = '9'.repeat(32)
  const sourcePath = h.seedSpill(name, id1, id2)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(420)
  expect(h.requests.length).toBe(1)
  expect(h.requests[0]!.ids).toEqual([id1])
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(380)
  await exiting

  expect(h.requests.length).toBe(1)
  expect(h.sentOf(id1)).toBe(1)
  expect(h.sentOf(id2)).toBe(0)
  expect(h.files.get(sourcePath)).toBe('')
  expect(h.doneMarksFor(name)[0]?.data?.retired).toBe(true)
  expect(h.uncancelledTimers()).toEqual([])

  const leftovers = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
  expect(leftovers.length).toBe(1)
  const leftoverName = leftovers[0]![0].split('/').pop()!
  expect(h.doneMarksFor(leftoverName)).toEqual([])

  h.setFetch(async () => OK_INSERT)
  await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(id1)).toBe(1)
  expect(h.sentOf(id2)).toBe(1)
  expect(h.files.get(leftovers[0]![0])).toBe('')
})

test('T53-6: multi-batch exit replay with Batch 2 cut off at marks.replay retires Batch 1, marks Batch 2 leftover sending: true, and force replays only Batch 2', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-6.jsonl'
  const id1 = 'a'.repeat(32)
  const id2 = 'b'.repeat(32)
  const sourcePath = h.seedSpill(name, id1, id2)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(50)
  expect(h.requests.length).toBe(1)
  expect(h.requests[0]!.ids).toEqual([id1])
  h.requests[0]!.resolve(OK_INSERT)
  // Batch 2 goes out at t=50 ms and hangs past marks.replay (700 ms).
  await h.advance(656)
  expect(h.requests.length).toBe(2)
  expect(h.requests[1]!.ids).toEqual([id2])
  // Late 200 OK arrives at t=706 ms (after marks.replay cutoff at 700 ms).
  h.requests[1]!.resolve(OK_INSERT)
  await h.advance(94)
  await exiting

  expect(h.files.get(sourcePath)).toBe('')
  const sourceDone = h.doneMarksFor(name)
  expect(sourceDone.length).toBe(1)
  expect(sourceDone[0]?.data?.retired).toBe(true)
  expect(sourceDone[0]?.data?.fates).toBe('LW')
  expect(h.uncancelledTimers()).toEqual([])

  const leftovers = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
  expect(leftovers.length).toBe(1)
  const [leftoverPath, leftoverText] = leftovers[0]!
  const leftoverName = leftoverPath.split('/').pop()!
  expect(parseFile(leftoverText).lines.map(l => l.row.event_id)).toEqual([id2])
  const leftoverMarks = h.doneMarksFor(leftoverName)
  expect(leftoverMarks.length).toBe(1)
  expect(leftoverMarks[0]?.data?.sending).toBe(true)

  // Next session auto-backfill holds Batch 2 without silently duplicating it.
  h.sent.length = 0
  h.setFetch(async () => OK_INSERT)
  const w2 = h.newWriter('s-2')
  const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
  expect(autoReport.skippedFiles).toBe(1)
  expect(h.sentOf(id1)).toBe(0)
  expect(h.sentOf(id2)).toBe(0)

  // Force backfill replays ONLY Batch 2 (never Batch 1).
  const forceReport = await w2.backfill(['spill', 'retry'], 'force')
  expect(forceReport.rowsSent).toBe(1)
  expect(h.sentOf(id1)).toBe(0)
  expect(h.sentOf(id2)).toBe(1)
  expect(h.files.get(leftoverPath)).toBe('')
})

test('T53-7: explicit error responses and fetch rejections (503, 401, ECONNREFUSED, FIRST_AGAIN) release the mark so automatic retry proceeds', async () => {
  for (const mode of ['live', 'exit'] as const) {
    for (const kind of ['503', '401', 'reject', 'again'] as const) {
      const h = exitReplayHarness()
      const w1 = await h.warmWriter('s-1')
      const name = `spill-20261013T200102003Z-t53-7-${mode}-${kind}.jsonl`
      const id = 'c'.repeat(32)
      const spillPath = h.seedSpill(name, id)

      h.setFetch(async () => {
        if (kind === '503') return UNAVAILABLE
        if (kind === '401') return UNAUTHENTICATED
        if (kind === 'reject') throw new Error('ECONNREFUSED')
        return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'backendError', message: 'try again' }] }] }) }
      })

      if (mode === 'live') {
        const r1 = await w1.backfill(['spill', 'retry'], 'auto')
        expect(r1.rowsSent).toBe(0)
      } else {
        const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
        await h.advance(800)
        await exiting
      }
      expect(h.doneMarksFor(name)).toEqual([])
      expect(h.files.get(spillPath)).not.toBe('')

      h.setFetch(async () => OK_INSERT)
      h.sent.length = 0
      const r2 = await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
      expect(r2.rowsSent).toBe(1)
      expect(h.sentOf(id)).toBe(1)
      expect(h.files.get(spillPath)).toBe('')
      expect(h.uncancelledTimers()).toEqual([])
    }
  }
})

test('T53-6b: 1,001-row spill where Batch 1 cuts off at exit splits Batch 1 (500 held) from Batches 2-3 (501 unmarked auto-replayed)', async () => {
  const h = exitReplayHarness({ maxBatchRows: 500 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-6b.jsonl'
  const ids = Array.from({ length: 1001 }, (_, i) => i.toString(16).padStart(32, '0'))
  const sourcePath = h.seedSpill(name, ...ids)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  // Only Batch 1 (500 rows) went out on the wire; parent was split into 2 leftovers and emptied.
  expect(h.requests.length).toBe(1)
  expect(h.requests[0]!.ids.length).toBe(500)
  expect(h.files.get(sourcePath)).toBe('')
  expect(h.doneMarksFor(name)[0]?.data?.retired).toBe(true)
  expect(h.uncancelledTimers()).toEqual([])

  const leftovers = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
  expect(leftovers.length).toBe(2)

  // Next session auto-backfill delivers the 501 never-submitted rows while holding the 500 uncertain rows.
  h.sent.length = 0
  h.setFetch(async () => OK_INSERT)
  const w2 = h.newWriter('s-2')
  const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
  expect(autoReport.rowsSent).toBe(501)
  expect(autoReport.skippedFiles).toBe(1)
  for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
  for (let i = 500; i < 1001; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)

  // Force backfill then delivers only the 500 uncertain rows.
  const forceReport = await w2.backfill(['spill', 'retry'], 'force')
  expect(forceReport.rowsSent).toBe(500)
  for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
  for (let i = 500; i < 1001; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
})

test('T53-6c: 1,001-row spill where Batch 1 lands and Batch 2 cuts off retires Batch 1, holds Batch 2 (500), and auto-replays row 1,001', async () => {
  const h = exitReplayHarness({ maxBatchRows: 500 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-6c.jsonl'
  const ids = Array.from({ length: 1001 }, (_, i) => (10_000 + i).toString(16).padStart(32, '0'))
  const sourcePath = h.seedSpill(name, ...ids)

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(50)
  expect(h.requests.length).toBe(1)
  expect(h.requests[0]!.ids.length).toBe(500)
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(750)
  await exiting

  expect(h.requests.length).toBe(2)
  expect(h.requests[1]!.ids.length).toBe(500)
  expect(h.files.get(sourcePath)).toBe('')
  expect(h.doneMarksFor(name)[0]?.data?.retired).toBe(true)
  expect(h.uncancelledTimers()).toEqual([])

  const leftovers = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
  expect(leftovers.length).toBe(2)

  // Next session auto-backfill sends row 1,001 immediately while holding Batch 2 (500..999) and never resending Batch 1 (0..499).
  h.sent.length = 0
  h.setFetch(async () => OK_INSERT)
  const w2 = h.newWriter('s-2')
  const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
  expect(autoReport.rowsSent).toBe(1)
  expect(autoReport.skippedFiles).toBe(1)
  expect(h.sentOf(ids[1000]!)).toBe(1)
  for (let i = 0; i < 1000; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)

  const forceReport = await w2.backfill(['spill', 'retry'], 'force')
  expect(forceReport.rowsSent).toBe(500)
  for (let i = 500; i < 1001; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
  for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
})

test('T53-8a: unattended -p session.end emits a one-line held-file notice with count and force command on stderr and debug log when exit replay cuts off', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102003Z-t53-8a.jsonl'
  const spilledId = '9'.repeat(32)
  world.files.set(`${PARTITION_DIR}/${name}`, parkedFile(parkedLine(TABLE_REF, spilledId, 'earlier-p')))

  // -p run: its own rows land (OK_INSERT), then exit replay hangs ({ hang: true }) and leaves 1 held file.
  world.replies = [OK_INSERT, { hang: true }]
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  const ending1 = $.session.end({ reason: 'other', sessionId: 'p-run-1', resume: { id: 'p-run-1' } })
  await world.clock.advance(2000)
  await ending1

  const heldNotices1 = world.stderr.filter(l => l.includes('dead-letter file(s) held'))
  expect(heldNotices1.length).toBe(1)
  expect(heldNotices1[0]).toContain('1 dead-letter file(s) held')
  expect(heldNotices1[0]).toContain('/bqaa-backfill force')
  expect(heldNotices1[0]).toContain('--plugin-dir')
  expect(heldNotices1[0]).toContain('destination')
  expect(heldNotices1[0]).toContain('dead-letter dir')
  expect(heldNotices1[0]).toContain('no answer before the session ended')
  expect(world.logs.filter(l => l.includes('dead-letter file(s) held')).length).toBe(1)
})

test('T53-8b: subsequent -p process with standing held mark emits held-file notice, interactive stays silent on stderr, and force clears it', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const name = 'spill-20261013T200102003Z-t53-8b.jsonl'
  const spilledId = '9'.repeat(32)
  const text = parkedFile(parkedLine(TABLE_REF, spilledId, 'earlier-p'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-earlier123-${name}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, uncertain: true, by: 'earlier123' })}\n`),
  )

  // Normal -p run using accessTokenCommand: auto-backfill sees the standing mark and session.end emits the notice.
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending2 = $.session.end({ reason: 'other', sessionId: 'p-run-2', resume: { id: 'p-run-2' } })
  await world.clock.advance(2000)
  await ending2

  expect(world.stderr.filter(l => l.includes('1 dead-letter file(s) held')).length).toBe(1)
  expect(world.stderr[0]).toContain('/bqaa-backfill force')
  expect(world.stderr[0]).toContain('--plugin-dir')
  expect(world.stderr[0]).toContain('destination')
  expect(world.stderr[0]).toContain('dead-letter dir')
  expect(world.stderr[0]).toContain('no answer before the session ended')
})

test('T53-8c: /bqaa-backfill force in -p mode replays held file and emits 0 stderr notices on exit (and interactive never writes stderr)', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const name = 'spill-20261013T200102003Z-t53-8c.jsonl'
  const spilledId = '9'.repeat(32)
  const text = parkedFile(parkedLine(TABLE_REF, spilledId, 'earlier-p'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-earlier123-${name}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, uncertain: true, by: 'earlier123' })}\n`),
  )

  // Interactive session seeing a standing held mark uses UI status/bands and never writes stderr.
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'c.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  await $.session.end({ reason: 'clear', sessionId: SESSION_ID, resume: { id: SESSION_ID } })
  expect(world.stderr).toEqual([])

  // Running /bqaa-backfill force in -p mode replays the held file and emits 0 stderr notices on exit.
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  const cmdOut = (await $.command.run({ command: 'bqaa-backfill', args: 'force', ...COMMAND })).text ?? ''
  expect(cmdOut).toContain('1 row(s) sent, 0 left')
  const ending = $.session.end({ reason: 'other', sessionId: 'p-run-3', resume: { id: 'p-run-3' } })
  await world.clock.advance(2000)
  await ending
  expect(world.stderr).toEqual([])
})

test('T53-9: rolling upgrade interrupted 501-row split uses versioned journal and keeps parent sending mark so 0.1.3 holds and 0.1.4 finishes with 1 refused + 1 uncertain row', async () => {
  for (const storage of ['store', 'file'] as const) {
    const h = exitReplayHarness({ maxBatchRows: 500 })
    if (storage === 'file') {
      h.setStoreSetHook(async key => {
        if (key.startsWith('replayed:')) throw new Error('store full')
      })
    }
    const w1 = await h.warmWriter('s-1')
    const name = `spill-20261013T200102003Z-t53-9-${storage}.jsonl`
    const ids = Array.from({ length: 501 }, (_, i) => (20_000 + i).toString(16).padStart(32, '0'))
    const sourcePath = h.seedSpill(name, ...ids)

    // Abort w1's writeOut when attempting to write the rejected child so w1 never finishes the split itself.
    h.setWriteFileHook(async (path, text) => {
      if (path.startsWith(`${PARTITION_DIR}/rejected-`) && text !== '') throw new Error('disk full during exit')
    })

    h.setFetch(() => undefined)
    const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(50)
    expect(h.requests.length).toBe(1)
    expect(h.requests[0]!.ids.length).toBe(500)
    // Refuse row 1 (index 0), accept rows 2..500 (indices 1..499); Batch 2 (row 501) times out at cutoff.
    h.requests[0]!.resolve({
      status: 200,
      text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad row 1' }] }] }),
    })
    await h.advance(750)
    await exiting

    // v1 journal keys/files must stay empty so a 0.1.3 reader ignores the v2 split journal.
    expect(h.store.get(`replayed:${TABLE_REF}/${name}`)).toBeUndefined()
    expect(h.files.get(`${PARTITION_DIR}/outcome-${name}`) ?? '').toBe('')
    expect(h.files.get(`${PARTITION_DIR}/outcome-b-${name}`) ?? '').toBe('')
    if (storage === 'store') {
      expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeDefined()
    } else {
      const v2Filed = (h.files.get(`${PARTITION_DIR}/outcome2-${name}`) ?? '') || (h.files.get(`${PARTITION_DIR}/outcome2-b-${name}`) ?? '')
      expect(v2Filed).not.toBe('')
      // Simulate 25h passing before a 0.1.3 reader runs: 0.1.3 recordOf must ignore outcome2-* files.
      for (const p of h.files.keys()) {
        if (p.includes('/outcome2-')) h.mtimes.set(p, -90_000_000)
      }
    }
    // Parent still holds its sending mark while the rejected child write failed.
    expect(h.files.get(sourcePath)).not.toBe('')
    expect(h.doneMarksFor(name)[0]?.data?.sending).toBe(true)
    expect(h.doneMarksFor(name)[0]?.data?.retired).not.toBe(true)

    // Advance past the 10-minute lease and simulate a 0.1.3 reader (ignores replayed:v2:* and outcome2-*, strips uncertainLines).
    await h.advance(600_001)
    const baseIo = h.ioFor()
    const io013: Io = {
      ...baseIo,
      storeGet: async key => {
        if (key.startsWith('replayed:v2:')) return undefined
        const val = await baseIo.storeGet(key)
        if (key.startsWith('replayed:') && val && typeof val === 'object') {
          const clone = { ...(val as Record<string, unknown>) }
          delete clone.uncertainLines
          delete clone.uncertain
          return clone
        }
        return val
      },
      exists: async path => (path.includes('/outcome2-') ? false : baseIo.exists!(path)),
      readFile: async path => (path.includes('/outcome2-') ? '' : baseIo.readFile(path)),
      listDir: async dir => (await baseIo.listDir(dir)).filter(e => !e.name.startsWith('outcome2-')),
    }
    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const w013 = new Writer(io013, () => h.config, () => 's-013')
    const r013 = await w013.backfill(['spill', 'retry'], 'auto')
    expect(r013.rowsSent).toBe(0)
    expect(h.sent.length).toBe(0)
    // Parent and any v2 outcome2-* journal remain intact after 0.1.3 scan, with 0 rejected files written yet.
    expect(h.files.get(sourcePath)).not.toBe('')
    expect([...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/rejected-`) && t !== '').length).toBe(0)
    if (storage === 'file') {
      const v2After013 = (h.files.get(`${PARTITION_DIR}/outcome2-${name}`) ?? '') || (h.files.get(`${PARTITION_DIR}/outcome2-b-${name}`) ?? '')
      expect(v2After013).not.toBe('')
    }

    // Restore disk writes and let a fresh 0.1.4 reader resume via resumeRewrite -> writeOut and finish the split.
    h.setWriteFileHook(null)
    await h.advance(600_001)
    const w014 = h.newWriter('s-014')
    const r014 = await w014.backfill(['spill', 'retry'], 'auto')
    expect(r014.rowsSent).toBe(0)
    expect(h.files.get(sourcePath)).toBe('')
    expect(h.doneMarksFor(name).some(m => m.data?.retired === true)).toBe(true)
    expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeUndefined()

    const rejectedFiles = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/rejected-`) && t !== '')
    const spillChildren = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
    expect(rejectedFiles.length).toBe(1)
    expect(spillChildren.length).toBe(1)
    expect(parseFile(rejectedFiles[0]![1]).lines.map(l => l.row.event_id)).toEqual([ids[0]!])
    expect(parseFile(spillChildren[0]![1]).lines.map(l => l.row.event_id)).toEqual([ids[500]!])
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-9b: resuming a v2 split journal before its uncertain child was written records heldNoticeInfo and deletes replayed:v2:* even after retry of parent empty', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-9b.jsonl'
  const id1 = 'a'.repeat(32)
  const id2 = 'b'.repeat(32)
  const sourcePath = h.seedSpill(name, id1, id2)

  // Fail right when w1 tries to write the uncertain child's mark, after recording replayed:v2:* (written: [false]).
  h.setWriteFileHook(async (path, text) => {
    if (path.includes('/outcome-done-') && !path.endsWith(`-${name}`) && text !== '') {
      throw new Error('disk full before uncertain child')
    }
  })

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(50)
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(750)
  await exiting

  expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeDefined()
  expect(h.files.get(sourcePath)).not.toBe('')
  expect(w1.heldNoticeInfo().files).toBe(1)

  // w2 resumes via resumeRewrite, writes the uncertain child + mark + retired done mark, but fails emptying sourcePath.
  await h.advance(600_001)
  h.setWriteFileHook(async (path, text) => {
    if (path === sourcePath && text === '') throw new Error('busy emptying parent')
  })
  const w2 = h.newWriter('s-2')
  const r2 = await w2.backfill(['spill', 'retry'], 'auto')
  expect(r2.held ?? '').toContain('no answer before the session ended')
  expect(w2.heldNoticeInfo().files).toBe(1)

  // w3 finishes emptying sourcePath via retired done mark and cleans up replayed:v2:* from store.
  h.setWriteFileHook(null)
  await h.advance(600_001)
  const w3 = h.newWriter('s-3')
  await w3.backfill(['spill', 'retry'], 'auto')
  expect(h.files.get(sourcePath)).toBe('')
  expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeUndefined()
  expect(w3.heldNoticeInfo().files).toBe(1)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-9c: interrupted markDone on a 501-row v2 split (0-byte truncation or partial write) leaves sending guard intact so 0.1.3 sends 0 rows and 0.1.4 finishes cleanly', async () => {
  for (const storage of ['store', 'file'] as const) {
    for (const cut of ['truncated', 'torn'] as const) {
      const h = exitReplayHarness({ maxBatchRows: 500 })
      if (storage === 'file') {
        h.setStoreSetHook(async key => {
          if (key.startsWith('replayed:')) throw new Error('store full')
        })
      }
      const w1 = await h.warmWriter('s-1')
      const name = `spill-20261013T200102003Z-t53-9c-${storage}-${cut}.jsonl`
      const ids = Array.from({ length: 501 }, (_, i) => (40_000 + i).toString(16).padStart(32, '0'))
      const sourcePath = h.seedSpill(name, ...ids)

      // Simulate crash/failure inside markDone right after truncating or partially writing the retired mark.
      let interruptedMarkDone = false
      h.setWriteFileHook(async (path, text) => {
        if (!interruptedMarkDone && path.includes('/outcome-done-') && path.endsWith(`-${name}`) && text.includes('"retired":true')) {
          interruptedMarkDone = true
          h.files.set(path, cut === 'truncated' ? '' : '{"fates":"L')
          throw new Error(`killed during markDone (${cut})`)
        }
      })

      h.setFetch(() => undefined)
      const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
      await h.advance(50)
      expect(h.requests.length).toBe(1)
      expect(h.requests[0]!.ids.length).toBe(500)
      // Batch 1 (500 rows) is acknowledged; Batch 2 (row 501) times out at exit cutoff.
      h.requests[0]!.resolve(OK_INSERT)
      await h.advance(750)
      await exiting

      expect(interruptedMarkDone).toBe(true)
      expect(h.files.get(sourcePath)).not.toBe('')

      // Advance past the 10-minute lease and run a 0.1.3 reader (ignores replayed:v2:* and outcome2-*).
      await h.advance(600_001)
      const baseIo = h.ioFor()
      const io013: Io = {
        ...baseIo,
        storeGet: async key => {
          if (key.startsWith('replayed:v2:')) return undefined
          const val = await baseIo.storeGet(key)
          if (key.startsWith('replayed:') && val && typeof val === 'object') {
            const clone = { ...(val as Record<string, unknown>) }
            delete clone.uncertainLines
            delete clone.uncertain
            return clone
          }
          return val
        },
        exists: async path => (path.includes('/outcome2-') ? false : baseIo.exists!(path)),
        readFile: async path => (path.includes('/outcome2-') ? '' : baseIo.readFile(path)),
        listDir: async dir => (await baseIo.listDir(dir)).filter(e => !e.name.startsWith('outcome2-')),
      }
      h.sent.length = 0
      h.setFetch(async () => OK_INSERT)
      const w013 = new Writer(io013, () => h.config, () => 's-013')
      const r013Auto = await w013.backfill(['spill', 'retry'], 'auto')
      expect(r013Auto.rowsSent).toBe(0)
      const r013Cmd = await w013.backfill(['spill', 'retry', 'rejected'], 'command')
      expect(r013Cmd.rowsSent).toBe(0)
      expect(h.sent.length).toBe(0)

      // Fresh 0.1.4 reader finishes retiring the parent with 0 rows sent on auto, and force sends only row 501.
      h.setWriteFileHook(null)
      await h.advance(600_001)
      const w014 = h.newWriter('s-014')
      const r014Auto = await w014.backfill(['spill', 'retry'], 'auto')
      expect(r014Auto.rowsSent).toBe(0)
      expect(h.sent.length).toBe(0)
      expect(h.files.get(sourcePath)).toBe('')
      expect(h.doneMarksFor(name).some(m => m.data?.retired === true)).toBe(true)

      const r014Force = await w014.backfill(['spill', 'retry'], 'force')
      expect(r014Force.rowsSent).toBe(1)
      expect(h.sentOf(ids[500]!)).toBe(1)
      for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
      expect(h.uncancelledTimers()).toEqual([])
    }
  }
})

test('T53-10: single-batch exit replay writes outcome-done-* once up front with uncertain: true and performs no second overwrite at cutoff', async () => {
  const h = exitReplayHarness()
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-10.jsonl'
  h.seedSpill(name, '6'.repeat(32))

  const markWrites: { atRequestCount: number; text: string }[] = []
  h.setWriteFileHook(async (path, text) => {
    if (path.includes(`/outcome-done-`) && path.endsWith(`-${name}`)) {
      markWrites.push({ atRequestCount: h.requests.length, text })
    }
  })

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(markWrites.length).toBe(1)
  expect(markWrites[0]!.atRequestCount).toBe(0)
  expect(h.doneMarksFor(name)[0]?.data).toMatchObject({ sending: true, uncertain: true })
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-11: live replay dispatched before shutdown retains its sending mark if its client timeout settles after shutdown began', async () => {
  for (const replaySpills of [true, false] as const) {
    const h = exitReplayHarness({ requestTimeoutMs: 500 })
    const w1 = await h.warmWriter('s-1')
    const name = `spill-20261013T200102003Z-t53-11-${replaySpills}.jsonl`
    const id = 'b'.repeat(32)
    const spillPath = h.seedSpill(name, id)

    h.setFetch(() => undefined)
    // Live replay starts at t=0 ms.
    const liveBackfill = w1.backfill(['spill', 'retry'], 'auto')
    await h.advance(400)
    expect(h.requests.length).toBe(1)

    // Shutdown (800 ms budget) starts at t=400 ms; local requestTimeoutMs fires at t=500 ms while closing === true.
    const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills })
    await h.advance(200)
    // Late HTTP 200 lands at t=600 ms (after the 500 ms local timeout).
    h.requests[0]!.resolve(OK_INSERT)
    await h.advance(600)
    await exiting
    await liveBackfill

    expect(h.doneMarksFor(name).length).toBe(1)
    expect(h.doneMarksFor(name)[0]?.data?.sending).toBe(true)
    expect(w1.heldNoticeInfo().files).toBe(1)
    expect(h.files.get(spillPath)).not.toBe('')

    // Fresh writer in the next session holds the file instead of re-sending the event.
    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const w2 = h.newWriter('s-2')
    const r2 = await w2.backfill(['spill', 'retry'], 'auto')
    expect(r2.skippedFiles).toBe(1)
    expect(r2.rowsSent).toBe(0)
    expect(h.sentOf(id)).toBe(0)
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-11b: live replay dispatched before shutdown on a 10s client timeout reports 1 held file when the 800 ms (or 0 ms) exit budget runs out first', async () => {
  // Case 1: 800 ms -p exit budget with Batch 1 (500 rows) acknowledged and Batch 2 (row 501) on 10s timeout.
  {
    const h = exitReplayHarness({ requestTimeoutMs: 10_000, maxBatchRows: 500 })
    const w1 = await h.warmWriter('s-1')
    const name = 'spill-20261013T200102003Z-t53-11b-warm.jsonl'
    const ids = Array.from({ length: 501 }, (_, i) => (50_000 + i).toString(16).padStart(32, '0'))
    const sourcePath = h.seedSpill(name, ...ids)

    h.setFetch(() => undefined)
    const liveBackfill = w1.backfill(['spill', 'retry'], 'auto')
    await h.advance(50)
    expect(h.requests.length).toBe(1)
    h.requests[0]!.resolve(OK_INSERT)
    await h.advance(50)
    expect(h.requests.length).toBe(2)

    // Shutdown starts at t=100 ms with 800 ms exit budget while Batch 2 is on its 10s client timeout.
    const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    expect(w1.heldNoticeInfo().files).toBe(1)
    expect(w1.heldNoticeInfo().reason ?? '').toContain('no answer before the session ended')
    expect(h.files.get(sourcePath)).toBe('')
    expect(h.uncancelledTimers()).toEqual([])
    await liveBackfill

    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const w2 = h.newWriter('s-2')
    const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
    expect(autoReport.rowsSent).toBe(0)
    const forceReport = await w2.backfill(['spill', 'retry'], 'force')
    expect(forceReport.rowsSent).toBe(1)
    expect(h.sentOf(ids[500]!)).toBe(1)
    for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
  }

  // Case 2: cold / zero-budget exit while a 10s live replay batch is still in flight when spillForExit returns.
  {
    const h = exitReplayHarness({ requestTimeoutMs: 10_000 })
    const w1 = h.newWriter('s-cold')
    const name = 'spill-20261013T200102003Z-t53-11b-cold.jsonl'
    h.seedSpill(name, 'c'.repeat(32))

    h.setFetch(() => undefined)
    const liveBackfill = w1.backfill(['spill', 'retry'], 'auto')
    await h.advance(50)
    expect(h.requests.length).toBe(1)

    await w1.spillForExit(0, { sendQueued: true, replaySpills: true })
    // Batch is still awaiting its 10s timeout, yet heldNoticeInfo already reports 1 held file for stderr notice.
    expect(w1.heldNoticeInfo().files).toBe(1)
    expect(w1.heldNoticeInfo().reason ?? '').toContain('no answer before the session ended')

    await h.advance(10_000)
    await liveBackfill
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-11c: end-to-end -p session whose pre-shutdown replay stays on a 10s timeout emits 1 held-file notice on stderr at 800 ms exit', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_REQUEST_TIMEOUT_MS: '10000' })
  const name = 'spill-20261013T200102003Z-t53-11c.jsonl'
  world.files.set(`${PARTITION_DIR}/${name}`, parkedFile(parkedLine(TABLE_REF, '9'.repeat(32), 'earlier-p')))

  // First flush of this session's row lands (OK_INSERT); then live auto-backfill starts and hangs on its 10s timeout.
  world.replies = [OK_INSERT, { hang: true }, OK_INSERT]
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()

  const ending = $.session.end({ reason: 'other', sessionId: 'p-10s-inflight', resume: { id: 'p-10s-inflight' } })
  await world.clock.advance(900)
  await ending

  const notices = world.stderr.filter(l => l.includes('1 dead-letter file(s) held'))
  expect(notices.length).toBe(1)
  expect(notices[0]).toContain('--plugin-dir')
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await world.clock.settle()
})

test('T53-12: 1,000-row exit replay with 500 retryable errors in Batch 1 gives Batch 2 the 100 ms reserve so Batch 1 auto-replays next session', async () => {
  for (const batch2Outcome of ['cutoff', 'landed'] as const) {
    const h = exitReplayHarness({ maxBatchRows: 500 })
    const w1 = await h.warmWriter('s-1')
    const name = `spill-20261013T200102003Z-t53-12-${batch2Outcome}.jsonl`
    const ids = Array.from({ length: 1000 }, (_, i) => (30_000 + i).toString(16).padStart(32, '0'))
    const sourcePath = h.seedSpill(name, ...ids)

    h.setFetch(() => undefined)
    const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(50)
    expect(h.requests.length).toBe(1)
    // All 500 rows of Batch 1 fail with retryable per-row errors (changed stays false).
    h.requests[0]!.resolve({
      status: 200,
      text: JSON.stringify({
        insertErrors: Array.from({ length: 500 }, (_, index) => ({
          index,
          errors: [{ reason: 'backendError', message: 'try again' }],
        })),
      }),
    })
    await h.advance(50)
    expect(h.requests.length).toBe(2)

    // Slow journal reads in writeOut take 40 ms total (exceeding a 25 ms reserve if Batch 2 ran to 775 ms, but fitting inside 100 ms reserve at 700 ms).
    h.setStoreGetHook(async key => {
      if (key.startsWith('replayed:')) {
        await new Promise<void>(resolve => {
          h.timers.push({ at: h.ioFor().now() + 20, fn: resolve, cancelled: false })
        })
      }
    })

    if (batch2Outcome === 'landed') {
      await h.advance(580) // t=680 ms: Batch 2 lands within 700 ms deadline, leaving 120 ms for writeOut.
      h.requests[1]!.resolve(OK_INSERT)
      await h.advance(120)
    } else {
      // Batch 2 hangs: must cut off at 700 ms (100 ms reserve), NOT 775 ms (25 ms reserve).
      await h.advance(700)
    }
    await exiting

    expect(h.files.get(sourcePath)).toBe('')
    expect(h.doneMarksFor(name)[0]?.data?.retired).toBe(true)

    // Next session auto-replays all 500 retryable rows of Batch 1 without needing force.
    h.setStoreGetHook(null)
    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const w2 = h.newWriter('s-2')
    const r2 = await w2.backfill(['spill', 'retry'], 'auto')
    expect(r2.rowsSent).toBe(500)
    for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
    for (let i = 500; i < 1000; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-13: pause right before replayPost leaving < 300 ms before marks.replay aborts without dispatching fetch and releases mark', async () => {
  let pauseOnTimeoutRead = false
  let hRef: ReturnType<typeof exitReplayHarness> | null = null
  const baseConfig = resolveConfig(OPTIONS, { HOME, USER: 'dev' })
  const hookedConfig = new Proxy(baseConfig, {
    get(target, prop, receiver) {
      if (prop === 'requestTimeoutMs' && pauseOnTimeoutRead && hRef !== null) {
        pauseOnTimeoutRead = false
        // Pause until t=450 ms of 800 ms (350 ms total / 250 ms < 300 ms before marks.replay, while postUntil still has 325 ms).
        hRef.advanceNow(450)
      }
      return Reflect.get(target, prop, receiver)
    },
  })
  const h = exitReplayHarness()
  hRef = h
  const w1 = await h.warmWriter('s-1', hookedConfig)
  const name = 'spill-20261013T200102003Z-t53-13.jsonl'
  const id = 'd'.repeat(32)
  const spillPath = h.seedSpill(name, id)

  pauseOnTimeoutRead = true
  h.setFetch(async () => OK_INSERT)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(h.requests.length).toBe(0)
  expect(h.sentOf(id)).toBe(0)
  expect(h.doneMarksFor(name)).toEqual([])
  expect(h.files.get(spillPath)).not.toBe('')
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-14: a later scan that stops early on an earlier file preserves heldNoticeInfo count for still-standing marks on disk', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-1')
  const heldName = 'spill-20261013T200102005Z-b-held.jsonl'
  const heldPath = h.seedSpill(heldName, '1'.repeat(32))
  const heldSize = utf8Bytes(h.files.get(heldPath)!)
  h.files.set(
    `${PARTITION_DIR}/outcome-done-otherwriter-${heldName}`,
    sealFile(`${JSON.stringify({ size: heldSize, sending: true, uncertain: true, by: 'otherwriter' })}\n`),
  )

  // Scan 1 sees 1 held file.
  await w.backfill(['spill', 'retry'], 'auto')
  expect(w.heldNoticeInfo().files).toBe(1)

  // Scan 2 encounters a newer file sorting ahead of heldName that fails with 503, stopping the scan early before reaching heldName.
  const earlierName = 'spill-20261013T200102001Z-a-err.jsonl'
  h.seedSpill(earlierName, '2'.repeat(32))
  h.setFetch(async () => UNAVAILABLE)
  const earlyStopped = await w.backfill(['spill', 'retry'], 'recovery')
  expect(earlyStopped.error).not.toBeNull()
  expect(earlyStopped.skippedFiles).toBe(1)
  expect(w.heldNoticeInfo().files).toBe(1)
  expect(w.heldNoticeInfo().reason ?? '').toContain('no answer before the session ended')

  // Force backfill when BigQuery recovers clears both files and resets heldNoticeInfo to 0.
  h.setFetch(async () => OK_INSERT)
  const wAdopted = h.newWriter('s-1b')
  wAdopted.adopt(w)
  const forced = await wAdopted.backfill(['spill', 'retry'], 'force')
  expect(forced.rowsSent).toBe(2)
  expect(wAdopted.heldNoticeInfo().files).toBe(0)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-14b: a fresh -p writer whose first scan stops on an earlier file error still seeds later held files from the directory listing', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-fresh')

  // Earlier file fails with 503 so backfillRun stops before scanning any of the later files.
  h.seedSpill('spill-20261013T200102001Z-a-err.jsonl', '1'.repeat(32))

  // Later file 1: held by another writer's unanswered mark.
  const heldName = 'spill-20261013T200102005Z-b-held.jsonl'
  const heldPath = h.seedSpill(heldName, '2'.repeat(32))
  h.files.set(
    `${PARTITION_DIR}/outcome-done-otherwriter-${heldName}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(h.files.get(heldPath)!), sending: true, uncertain: true, by: 'otherwriter' })}\n`),
  )

  // Later file 2: held by a torn replay record.
  const tornName = 'spill-20261013T200102006Z-c-torn.jsonl'
  h.seedSpill(tornName, '3'.repeat(32))
  h.files.set(`${PARTITION_DIR}/outcome-done-otherwriter-${tornName}`, '{"sending":true')

  // Later file 3: unmarked ordinary spill (must NOT count as held).
  h.seedSpill('spill-20261013T200102007Z-d-plain.jsonl', '4'.repeat(32))

  // Later file 4: already-retired mark waiting only for source truncation (must NOT count as held).
  const retiredName = 'spill-20261013T200102008Z-e-retired.jsonl'
  const retiredPath = h.seedSpill(retiredName, '5'.repeat(32))
  h.files.set(
    `${PARTITION_DIR}/outcome-done-otherwriter-${retiredName}`,
    sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: utf8Bytes(h.files.get(retiredPath)!), waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`),
  )

  h.setFetch(async () => UNAVAILABLE)
  const r = await w.backfill(['spill', 'retry'], 'auto')
  expect(r.error).not.toBeNull()
  expect(w.heldNoticeInfo().files).toBe(2)
  expect(w.heldNoticeInfo().reason ?? '').toContain('b-held.jsonl')
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-14c: Windows dead-letter paths with backslash separators preserve heldNoticeInfo across early-stopped scans and prune emptied files', async () => {
  const winRoot = 'C:\\Users\\dev\\.bqaa-mod\\dead-letter'
  const winConfig = resolveConfig({ ...OPTIONS, deadLetterDir: winRoot }, { USERPROFILE: 'C:\\Users\\dev', USERNAME: 'dev' })
  const winDir = deadLetterPartition(winConfig)
  expect(winDir).toContain('\\')

  const files = new Map<string, string>()
  const store = new Map<string, unknown>()
  let status = 200
  const io: Io = {
    now: () => 1_000,
    after: () => ({ cancel: () => undefined }),
    fetch: async () => (status === 200 ? OK_INSERT : UNAVAILABLE),
    runCommand: async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
    envToken: async () => ACCESS_TOKEN,
    writeFile: async (p, t) => void files.set(p, t),
    readFile: async p => files.get(p) ?? '',
    exists: async p => files.has(p),
    listDir: async dir => {
      const prefix = `${dir}\\`
      return [...files.entries()]
        .filter(([p]) => p.startsWith(prefix) && !p.slice(prefix.length).includes('\\'))
        .map(([p, t]) => ({ name: p.slice(prefix.length), kind: 'file', size: utf8Bytes(t) }))
    },
    storeGet: async k => store.get(k),
    storeSet: async (k, v) => void store.set(k, v),
    storeDelete: async k => void store.delete(k),
    status: () => undefined,
    debug: () => undefined,
  }

  const w = new Writer(io, () => winConfig, () => 'win-s1')
  const heldName = 'spill-20261013T200102005Z-b-held.jsonl'
  const heldText = parkedFile(parkedLine(TABLE_REF, '1'.repeat(32), 'old'))
  files.set(`${winDir}\\${heldName}`, heldText)
  files.set(
    `${winDir}\\outcome-done-otherwriter-${heldName}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(heldText), sending: true, uncertain: true, by: 'otherwriter' })}\n`),
  )

  await w.backfill(['spill', 'retry'], 'auto')
  expect(w.heldNoticeInfo().files).toBe(1)

  // Add earlier failing file: Windows path in heldPaths must survive finish() prefix filter.
  files.set(`${winDir}\\spill-20261013T200102001Z-a-err.jsonl`, parkedFile(parkedLine(TABLE_REF, '2'.repeat(32), 'old')))
  status = 503
  await w.backfill(['spill', 'retry'], 'recovery')
  expect(w.heldNoticeInfo().files).toBe(1)

  // Empty held file externally: Windows path must be pruned on next scan.
  files.set(`${winDir}\\${heldName}`, '')
  files.delete(`${winDir}\\spill-20261013T200102001Z-a-err.jsonl`)
  status = 200
  await w.backfill(['spill', 'retry'], 'auto')
  expect(w.heldNoticeInfo().files).toBe(0)
})

test('T53-14d: resuming an interrupted v2 split whose uncertain child and mark already landed before record() crashed counts the child only once', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-14d.jsonl'
  h.seedSpill(name, 'a'.repeat(32), 'b'.repeat(32))

  // Let w1 record seq=1 (written: [false]), write the uncertain child's mark + file, and crash on record() seq=2!
  let v2Sets = 0
  h.setStoreSetHook(async key => {
    if (key.startsWith('replayed:v2:')) {
      v2Sets += 1
      if (v2Sets >= 2) throw new Error('store failed on child record')
    }
  })
  h.setWriteFileHook(async path => {
    if (path.includes('/outcome2-')) throw new Error('disk failed on child record')
  })

  h.setFetch(() => undefined)
  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(50)
  h.requests[0]!.resolve(OK_INSERT)
  await h.advance(750)
  await exiting

  // Both parent and uncertain child exist on disk while journal still says written: [false].
  h.setStoreSetHook(null)
  h.setWriteFileHook(null)
  await h.advance(600_001)
  const w2 = h.newWriter('s-2')
  const r2 = await w2.backfill(['spill', 'retry'], 'auto')
  expect(r2.heldFiles).toBe(1)
  expect(w2.heldNoticeInfo().files).toBe(1)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-15: exit-only headless spillForExit truncates >24h expired outcome-* and outcome2-* records while preserving fresh ones', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-1')
  h.advanceNow(100_000_000) // now = ~27.7 hours

  const oldName = 'spill-20261012T000000000Z-old.jsonl'
  const freshName = 'spill-20261013T200000000Z-fresh.jsonl'
  h.files.set(`${PARTITION_DIR}/${oldName}`, '')
  h.files.set(`${PARTITION_DIR}/${freshName}`, '')

  const expiredPaths = [
    `${PARTITION_DIR}/outcome-done-w0-${oldName}`,
    `${PARTITION_DIR}/outcome-${oldName}`,
    `${PARTITION_DIR}/outcome2-${oldName}`,
    `${PARTITION_DIR}/outcome2-b-${oldName}`,
  ]
  for (const p of expiredPaths) {
    h.files.set(p, sealFile(`${JSON.stringify({ retired: true, retiredAt: 1_000 })}\n`))
    h.mtimes.set(p, 1_000) // >24h old
  }
  const freshMark = `${PARTITION_DIR}/outcome-done-w0-${freshName}`
  h.files.set(freshMark, sealFile(`${JSON.stringify({ retired: true, retiredAt: 99_000_000 })}\n`))
  h.mtimes.set(freshMark, 99_000_000) // <24h old

  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  for (const p of expiredPaths) expect(h.files.get(p)).toBe('')
  expect(h.files.get(freshMark)).not.toBe('')
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-16: recording a fresh v1 or v2 outcome clears stale higher-seq journals in the opposite slot so waiting rows are never dropped', async () => {
  for (const activeSlot of ['v2', 'v1'] as const) {
    for (const storage of ['store', 'file'] as const) {
      const h = exitReplayHarness({ maxBatchRows: 1 })
      if (storage === 'file') {
        h.setStoreSetHook(async key => {
          if (key.startsWith('replayed:')) throw new Error('store full')
        })
      }
      const w1 = await h.warmWriter('s-1')
      const name = `spill-20261013T200102003Z-t53-16-${activeSlot}-${storage}.jsonl`
      const id1 = '1'.repeat(32)
      const id2 = '2'.repeat(32)
      const sourcePath = h.seedSpill(name, id1, id2)
      const size = utf8Bytes(h.files.get(sourcePath)!)

      // Seed a stale higher-seq journal (seq: 9, claiming all rows landed with 0 leftover files) in the opposite slot
      // from an earlier size of the same filename: ignored while source is live (size differs), but if left uncleared
      // it wins seq comparison in parentFate (size: -1) and drops the newly written leftover child unsent!
      const staleJournal = { fates: 'LL', reasons: [], refusals: [], size: size - 1, waits: false, seq: 9, files: [], written: [] }
      if (activeSlot === 'v2') {
        h.store.set(`replayed:${TABLE_REF}/${name}`, staleJournal)
        h.files.set(`${PARTITION_DIR}/outcome-${name}`, sealFile(`${JSON.stringify(staleJournal)}\n`))
      } else {
        h.store.set(`replayed:v2:${TABLE_REF}/${name}`, staleJournal)
        h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(staleJournal)}\n`))
      }

      let calls = 0
      h.setFetch((_url, _ids, controls) => {
        calls += 1
        if (calls === 1) {
          controls.resolve(OK_INSERT)
          return undefined
        }
        if (activeSlot === 'v1') {
          controls.resolve(UNAVAILABLE)
        }
        return undefined
      })

      if (activeSlot === 'v2') {
        const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
        await h.advance(800)
        await exiting
      } else {
        await w1.backfill(['spill', 'retry'], 'auto')
      }

      // Opposite slot must be cleared on the first record() (both store key and outcome* files),
      // parent emptied, and id2 preserved in a non-empty child spill file.
      expect(h.files.get(sourcePath)).toBe('')
      if (activeSlot === 'v2') {
        expect(h.store.get(`replayed:${TABLE_REF}/${name}`)).toBeUndefined()
        expect(h.files.get(`${PARTITION_DIR}/outcome-${name}`) ?? '').toBe('')
      } else {
        expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeUndefined()
        expect(h.files.get(`${PARTITION_DIR}/outcome2-${name}`) ?? '').toBe('')
      }
      const children = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
      expect(children.length).toBe(1)
      expect(parseFile(children[0]![1]).lines.map(l => l.row.event_id)).toEqual([id2])

      // Even if the done mark is absent (e.g., markDone failed on v1), parentFate (size: -1) must NOT pick the stale
      // opposite-slot seq: 9 journal and drop the waiting child!
      if (activeSlot === 'v1') {
        for (const p of [...h.files.keys()]) {
          if (p.includes('/outcome-done-') && p.endsWith(`-${name}`)) h.files.set(p, '')
        }
        h.sent.length = 0
        h.setFetch(async () => OK_INSERT)
        const r2 = await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
        expect(r2.rowsSent).toBe(1)
        expect(h.sentOf(id2)).toBe(1)
      }
      expect(h.uncancelledTimers()).toEqual([])
    }
  }
})

test('T53-16b: stale opposite-slot journals never erase unsent children (0.1.3 snapshot, rejected clear, and same-size conflict) on both backends', async () => {
  for (const storage of ['store', 'file'] as const) {
    // Subcase A: exact 0.1.3 writer snapshot — 1,001 rows, 500 acked, 503 on batch 2, failed markDone,
    // stale higher-seq v2 journal of a different size sitting in store or outcome2-*.
    {
      const h = exitReplayHarness({ maxBatchRows: 500 })
      const name = `spill-20261013T200102003Z-t53-16b-013-${storage}.jsonl`
      const ids = Array.from({ length: 1001 }, (_, i) => (60_000 + i).toString(16).padStart(32, '0'))
      const sourcePath = h.seedSpill(name, ...ids)
      const size = utf8Bytes(h.files.get(sourcePath)!)
      const childName = `spill-20261013T200102500Z-child-013-${storage}.jsonl`
      const childPath = `${PARTITION_DIR}/${childName}`
      const childText = packFiles(
        ids.slice(500).map(id => JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(id) })),
        name,
      )[0]!

      // Stale v2 journal from an earlier generation (different size, higher seq: 9, naming 0 leftover files).
      const staleV2 = { fates: 'L'.repeat(500), reasons: [], refusals: [], size: size - 1, waits: false, seq: 9, files: [], written: [] }
      // Current 0.1.3 v1 journal (seq: 2, naming childName as written, markDone failed, parent emptied).
      const v1Journal = { fates: `${'L'.repeat(500)}${'W'.repeat(501)}`, reasons: [], refusals: [], size, waits: true, seq: 2, files: [childName], written: [true] }
      h.files.set(sourcePath, '')
      h.files.set(childPath, childText)
      if (storage === 'store') {
        h.store.set(`replayed:v2:${TABLE_REF}/${name}`, staleV2)
        h.store.set(`replayed:${TABLE_REF}/${name}`, v1Journal)
      } else {
        h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(staleV2)}\n`))
        h.files.set(`${PARTITION_DIR}/outcome-${name}`, sealFile(`${JSON.stringify(v1Journal)}\n`))
      }

      h.setFetch(async () => OK_INSERT)
      const r = await h.newWriter('s-014').backfill(['spill', 'retry'], 'auto')
      expect(r.rowsSent).toBe(501)
      expect(h.files.get(childPath)).toBe('')
      for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
      for (let i = 500; i < 1001; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
    }

    // Subcase B: 0.1.4 writeOut fails closed when clearing a stale opposite-slot journal is rejected,
    // leaving parent intact with zero leftover children written until cleanup succeeds on retry.
    {
      const h = exitReplayHarness({ maxBatchRows: 1 })
      if (storage === 'file') {
        h.setStoreSetHook(async key => {
          if (key.startsWith('replayed:')) throw new Error('store full')
        })
      }
      const w1 = await h.warmWriter('s-1')
      const name = `spill-20261013T200102003Z-t53-16b-reject-${storage}.jsonl`
      const id1 = '1'.repeat(32)
      const id2 = '2'.repeat(32)
      const sourcePath = h.seedSpill(name, id1, id2)
      const size = utf8Bytes(h.files.get(sourcePath)!)
      const staleV2 = { fates: 'LL', reasons: [], refusals: [], size: size - 1, waits: false, seq: 9, files: [], written: [] }
      if (storage === 'store') {
        h.store.set(`replayed:v2:${TABLE_REF}/${name}`, staleV2)
        h.setStoreDeleteHook(async key => {
          if (key === `replayed:v2:${TABLE_REF}/${name}`) throw new Error('storeDelete rejected')
        })
      } else {
        h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(staleV2)}\n`))
        h.setWriteFileHook(async (path, text) => {
          if (path === `${PARTITION_DIR}/outcome2-${name}` && text === '') throw new Error('truncate rejected')
        })
      }

      let calls = 0
      h.setFetch(async () => {
        calls += 1
        return calls === 1 ? OK_INSERT : UNAVAILABLE
      })

      const r1 = await w1.backfill(['spill', 'retry'], 'auto')
      expect(r1.rowsSent).toBe(1)
      // Parent must NOT be emptied and no leftover child may be written while stale opposite-slot clear failed.
      expect(h.files.get(sourcePath)).not.toBe('')
      const childrenAfterFail = [...h.files.entries()].filter(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '')
      expect(childrenAfterFail.length).toBe(0)

      // Allow opposite-slot cleanup and retry: parent is now retired safely and id2 is delivered once (id1 never resent).
      h.setStoreDeleteHook(null)
      h.setWriteFileHook(null)
      h.sent.length = 0
      h.setFetch(async () => OK_INSERT)
      const r2 = await w1.backfill(['spill', 'retry'], 'auto')
      expect(h.files.get(sourcePath)).toBe('')
      expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeUndefined()
      expect(h.files.get(`${PARTITION_DIR}/outcome2-${name}`) ?? '').toBe('')
      const r3 = await h.newWriter('s-2').backfill(['spill', 'retry'], 'auto')
      expect(r2.rowsSent + r3.rowsSent).toBe(1)
      expect(h.sentOf(id1)).toBe(0)
      expect(h.sentOf(id2)).toBe(1)
    }

    // Subcase C: same-size v1/v2 disagreement on an emptied parent (no done mark) preserves the child and reports ambiguity.
    {
      const h = exitReplayHarness()
      const name = `spill-20261013T200102003Z-t53-16b-conflict-${storage}.jsonl`
      const id = '9'.repeat(32)
      const sourcePath = h.seedSpill(name, id)
      const size = utf8Bytes(h.files.get(sourcePath)!)
      const childName = `spill-20261013T200102500Z-conflict-child-${storage}.jsonl`
      const childPath = `${PARTITION_DIR}/${childName}`
      const childText = packFiles(
        [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(id) })],
        name,
      )[0]!

      const v1Journal = { fates: 'W', reasons: [], refusals: [], size, waits: true, seq: 2, files: [childName], written: [true] }
      const v2Journal = { fates: 'L', reasons: [], refusals: [], size, waits: false, seq: 9, files: [], written: [] }
      h.files.set(sourcePath, '')
      h.files.set(childPath, childText)
      if (storage === 'store') {
        h.store.set(`replayed:${TABLE_REF}/${name}`, v1Journal)
        h.store.set(`replayed:v2:${TABLE_REF}/${name}`, v2Journal)
      } else {
        h.files.set(`${PARTITION_DIR}/outcome-${name}`, sealFile(`${JSON.stringify(v1Journal)}\n`))
        h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(v2Journal)}\n`))
      }

      h.setFetch(async () => OK_INSERT)
      const wAuto = h.newWriter('s-conflict')
      const autoReport = await wAuto.backfill(['spill', 'retry'], 'auto')
      expect(autoReport.rowsSent).toBe(0)
      expect(autoReport.held ?? '').toMatch(/conflicting replay records/i)
      expect(wAuto.heldNoticeInfo().files).toBe(1)
      expect(h.files.get(childPath)).toBe(childText)

      const forceReport = await wAuto.backfill(['spill', 'retry'], 'force')
      expect(forceReport.rowsSent).toBe(1)
      expect(h.sentOf(id)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }

    // Subcase D: same-slot cross-generation stale file (outcome-b- with higher seq, different size) beside current store v1 journal.
    if (storage === 'store') {
      const h = exitReplayHarness()
      const name = 'spill-20261013T200102003Z-t53-16b-same-slot.jsonl'
      const id = '8'.repeat(32)
      const sourcePath = h.seedSpill(name, id)
      const size = utf8Bytes(h.files.get(sourcePath)!)
      const childName = 'spill-20261013T200102500Z-same-slot-child.jsonl'
      const childPath = `${PARTITION_DIR}/${childName}`
      const childText = packFiles(
        [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(id) })],
        name,
      )[0]!

      h.files.set(sourcePath, '')
      h.files.set(childPath, childText)
      h.files.set(
        `${PARTITION_DIR}/outcome-b-${name}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: size - 1, waits: false, seq: 9, files: [], written: [] })}\n`),
      )
      h.store.set(`replayed:${TABLE_REF}/${name}`, { fates: 'W', reasons: [], refusals: [], size, waits: true, seq: 2, files: [childName], written: [true] })

      h.setFetch(async () => OK_INSERT)
      const r = await h.newWriter('s-same-slot').backfill(['spill', 'retry'], 'auto')
      expect(r.rowsSent).toBe(1)
      expect(h.sentOf(id)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }
  }
})

test('T53-15b: retention cleanup validates recordOf suffix with kindOf and ignores non-regular entries', async () => {
  expect(recordOf('outcome2-operator-notes.txt')).toBe(null)
  expect(recordOf('outcome2-spill-example.jsonl.bak')).toBe(null)
  expect(recordOf('outcome-operator-notes.txt')).toBe(null)
  expect(recordOf('outcome-done-abc-notes.txt')).toBe(null)
  expect(recordOf('outcome2-spill-20261013T200102003Z-valid-1.jsonl')).toBe('spill-20261013T200102003Z-valid-1.jsonl')

  const h = exitReplayHarness()
  h.advanceNow(100_000_000)
  const w = h.newWriter('s-1')

  const preserved = new Map<string, string>([
    [`${PARTITION_DIR}/outcome2-operator-notes.txt`, 'important operator notes\n'],
    [`${PARTITION_DIR}/outcome2-spill-example.jsonl.bak`, 'backup file\n'],
    [`${PARTITION_DIR}/outcome-operator-notes.txt`, 'more notes\n'],
    [`${PARTITION_DIR}/outcome-done-w0-notes.txt`, 'done-prefixed notes\n'],
  ])
  for (const [p, content] of preserved) {
    h.files.set(p, content)
    h.mtimes.set(p, 1_000) // >24h old
  }
  const dirEntryPath = `${PARTITION_DIR}/outcome2-spill-20261013T200102003Z-dir-1.jsonl`
  h.files.set(dirEntryPath, 'dir-marker')
  h.entryKinds.set(dirEntryPath, 'dir')
  h.mtimes.set(dirEntryPath, 1_000)

  const realExpired = `${PARTITION_DIR}/outcome2-spill-20261013T200102003Z-expired-1.jsonl`
  h.files.set(realExpired, sealFile(`${JSON.stringify({ retired: true, retiredAt: 1_000 })}\n`))
  h.mtimes.set(realExpired, 1_000)

  const writtenPaths: string[] = []
  h.setWriteFileHook(async path => {
    writtenPaths.push(path)
  })

  await w.backfill(['spill', 'retry'], 'auto')

  for (const [p, content] of preserved) {
    expect(h.files.get(p)).toBe(content)
    expect(writtenPaths).not.toContain(p)
  }
  expect(h.files.get(dirEntryPath)).toBe('dir-marker')
  expect(writtenPaths).not.toContain(dirEntryPath)
  expect(h.files.get(realExpired)).toBe('')
})

test('T53-11d: failed exit upload (503) or disabled exit send still arms cutoff for already-dispatched live replays to save confirmed rows', async () => {
  for (const mode of ['503-queued', 'send-disabled'] as const) {
    const h = exitReplayHarness({ requestTimeoutMs: 10_000, maxBatchRows: 500 })
    const w1 = mode === '503-queued' ? await h.warmWriter('s-1') : h.newWriter('s-1')
    const name = `spill-20261013T200102003Z-t53-11d-${mode}.jsonl`
    const ids = Array.from({ length: 1001 }, (_, i) => (70_000 + i).toString(16).padStart(32, '0'))
    const endRowId = 'e'.repeat(32)
    const sourcePath = h.seedSpill(name, ...ids)

    h.setFetch((_url, reqIds, controls) => {
      if (reqIds.includes(endRowId)) {
        controls.resolve(UNAVAILABLE)
        return undefined
      }
      return undefined
    })

    const liveBackfill = w1.backfill(['spill', 'retry'], 'auto')
    await h.advance(50)
    expect(h.requests.length).toBe(1)
    expect(h.requests[0]!.ids.length).toBe(500)
    // Rows 1-500 confirmed by BigQuery.
    h.requests[0]!.resolve(OK_INSERT)
    await h.advance(50)
    // Rows 501-1,000 in flight on 10s client timeout.
    expect(h.requests.length).toBe(2)
    expect(h.requests[1]!.ids.length).toBe(500)

    w1.enqueue(ownRow(endRowId), 0)
    const exiting = w1.spillForExit(800, { sendQueued: mode === '503-queued', replaySpills: true })
    await h.advance(800)
    const exitResult = await exiting

    expect(exitResult.spilled).toBe(1)
    expect(w1.isBackfilling).toBe(false)
    expect(h.files.get(sourcePath)).toBe('')
    expect(w1.heldNoticeInfo().files).toBe(1)
    expect(h.uncancelledTimers()).toEqual([])
    await liveBackfill

    // Restart: auto delivers row 1,001 + spilled session_end row; force delivers ONLY 501-1,000 (0 duplicates of 1-500).
    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const w2 = h.newWriter('s-2')
    const autoReport = await w2.backfill(['spill', 'retry'], 'auto')
    expect(autoReport.rowsSent).toBe(2)
    expect(h.sentOf(ids[1000]!)).toBe(1)
    expect(h.sentOf(endRowId)).toBe(1)

    const forceReport = await w2.backfill(['spill', 'retry'], 'force')
    expect(forceReport.rowsSent).toBe(500)
    for (let i = 0; i < 500; i += 1) expect(h.sentOf(ids[i]!)).toBe(0)
    for (let i = 500; i < 1000; i += 1) expect(h.sentOf(ids[i]!)).toBe(1)
  }
})

test('T53-11e: inFlightReplay remains set after HTTP success until writeOut commits so exit deadline does not suppress heldNoticeInfo', async () => {
  const h = exitReplayHarness({ maxBatchRows: 1 })
  const w1 = await h.warmWriter('s-1')
  const name = 'spill-20261013T200102003Z-t53-11e.jsonl'
  const id1 = '1'.repeat(32)
  const id2 = '2'.repeat(32)
  h.seedSpill(name, id1, id2)

  let releaseWriteOut = (): void => undefined
  const writeOutGate = new Promise<void>(resolve => {
    releaseWriteOut = resolve
  })
  let stallArmed = false
  h.setWriteFileHook(async path => {
    if (stallArmed && path.includes('/spill-') && !path.endsWith(`/${name}`)) {
      await writeOutGate
    }
  })

  let calls = 0
  h.setFetch(async () => {
    calls += 1
    if (calls === 1) {
      stallArmed = true
      return OK_INSERT
    }
    return UNAVAILABLE
  })

  const exiting = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  // writeOut is still stalled before committing the outcome, leaving the parent sending mark on disk.
  expect(w1.heldNoticeInfo().files).toBe(1)
  expect(w1.heldNoticeInfo().reason ?? '').toContain(name)

  releaseWriteOut()
  await h.settleMicrotasks()
  expect(w1.heldNoticeInfo().files).toBe(0)
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-14e: early-stop counting deduplicates existing uncertain children, counts zero-byte journals, and scales O(N) on 256 marked files', async () => {
  // Subcase A: already-existing uncertain child is counted once (not twice), and already-emptied written child counts zero.
  {
    for (const childWritten of [true, false] as const) {
      const h = exitReplayHarness()
      const firstName = 'spill-20261013T200102001Z-000-err.jsonl'
      const parentName = 'spill-20261013T200102002Z-001-parent.jsonl'
      const childName = 'spill-20261013T200102003Z-002-child.jsonl'
      h.seedSpill(firstName, '1'.repeat(32))
      const parentPath = h.seedSpill(parentName, '2'.repeat(32), '3'.repeat(32))
      const childPath = h.seedSpill(childName, '2'.repeat(32))
      const parentSize = utf8Bytes(h.files.get(parentPath)!)
      const childSize = utf8Bytes(h.files.get(childPath)!)
      h.files.set(
        `${PARTITION_DIR}/outcome2-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'WW', reasons: [], refusals: [], uncertainLines: [0], size: parentSize, waits: true, seq: 2, files: [childName], written: [childWritten], uncertain: [true] })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-w0-${parentName}`,
        sealFile(`${JSON.stringify({ size: parentSize, sending: true, uncertain: true, by: 'w0' })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-w0-${childName}`,
        sealFile(`${JSON.stringify({ size: childSize, sending: true, uncertain: true, by: 'w0' })}\n`),
      )

      h.setFetch(async () => UNAVAILABLE)
      const w = h.newWriter('s-early-dedup')
      await w.backfill(['spill', 'retry'], 'auto')
      expect(w.heldNoticeInfo().files).toBe(1)
    }

    // Already-emptied uncertain child with written: [true] counts 0 on early stop (matches full scan).
    const hEmpty = exitReplayHarness()
    const firstName = 'spill-20261013T200102001Z-000-err.jsonl'
    const parentName = 'spill-20261013T200102002Z-001-parent.jsonl'
    const childName = 'spill-20261013T200102003Z-002-child.jsonl'
    hEmpty.seedSpill(firstName, '1'.repeat(32))
    const parentPath = hEmpty.seedSpill(parentName, '2'.repeat(32), '3'.repeat(32))
    hEmpty.files.set(`${PARTITION_DIR}/${childName}`, '')
    const parentSize = utf8Bytes(hEmpty.files.get(parentPath)!)
    hEmpty.files.set(
      `${PARTITION_DIR}/outcome2-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'WW', reasons: [], refusals: [], uncertainLines: [0], size: parentSize, waits: true, seq: 2, files: [childName], written: [true], uncertain: [true] })}\n`),
    )
    hEmpty.files.set(
      `${PARTITION_DIR}/outcome-done-w0-${parentName}`,
      sealFile(`${JSON.stringify({ size: parentSize, sending: true, uncertain: true, by: 'w0' })}\n`),
    )
    hEmpty.setFetch(async () => UNAVAILABLE)
    const wEmpty = hEmpty.newWriter('s-early-empty-child')
    await wEmpty.backfill(['spill', 'retry'], 'auto')
    expect(wEmpty.heldNoticeInfo().files).toBe(0)
  }

  // Subcase B: zero-byte outcome-* and outcome2-* journals beside live sources are counted as held on early stop.
  {
    const h = exitReplayHarness()
    const firstName = 'spill-20261013T200102001Z-000-err.jsonl'
    const v1TornName = 'spill-20261013T200102002Z-001-zero-v1.jsonl'
    const v2TornName = 'spill-20261013T200102003Z-002-zero-v2.jsonl'
    h.seedSpill(firstName, '1'.repeat(32))
    h.seedSpill(v1TornName, '2'.repeat(32))
    h.seedSpill(v2TornName, '3'.repeat(32))
    h.files.set(`${PARTITION_DIR}/outcome-${v1TornName}`, '')
    h.files.set(`${PARTITION_DIR}/outcome2-${v2TornName}`, '')

    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-early-zero')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(2)
  }

  // Subcase C: 256 marked files early-stop scan stays O(N) on directory entry `.name` accesses.
  {
    const h = exitReplayHarness()
    const firstName = 'spill-20261013T200000000Z-000-err.jsonl'
    h.seedSpill(firstName, 'a'.repeat(32))
    for (let i = 1; i <= 256; i += 1) {
      const idx = String(i).padStart(4, '0')
      const name = `spill-20261013T200102003Z-marked-${idx}.jsonl`
      const p = h.seedSpill(name, i.toString(16).padStart(32, '0'))
      const size = utf8Bytes(h.files.get(p)!)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-w0-${name}`,
        sealFile(`${JSON.stringify({ size, sending: true, uncertain: true, by: 'w0' })}\n`),
      )
    }

    let nameAccesses = 0
    const baseIo = h.ioFor()
    const countingIo: Io = {
      ...baseIo,
      listDir: async dir => {
        const raw = await baseIo.listDir(dir)
        return raw.map(entry =>
          new Proxy(entry, {
            get(target, prop, receiver) {
              if (prop === 'name') nameAccesses += 1
              return Reflect.get(target, prop, receiver)
            },
          }),
        )
      },
    }

    h.setFetch(async () => UNAVAILABLE)
    const w = new Writer(countingIo, () => h.config, () => 's-scale')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(256)
    expect(nameAccesses).toBeLessThan(15_000)
  }
})

test('T53-16c: orphaned v2 store journal left behind after legacy 0.1.3 retirement is cleaned up by the next backfill', async () => {
  const h = exitReplayHarness()
  const retiredSpill = 'spill-20261013T200102003Z-legacy-retired.jsonl'
  const retiredRejected = 'rejected-20261013T200102004Z-legacy-retired.jsonl'
  const pendingSpill = 'spill-20261013T200102005Z-still-sending.jsonl'

  h.files.set(`${PARTITION_DIR}/${retiredSpill}`, '')
  h.files.set(
    `${PARTITION_DIR}/outcome-done-w013-${retiredSpill}`,
    sealFile(`${JSON.stringify({ size: 200, fates: 'L', reasons: [], refusals: [], seq: 2, files: [], written: [], retired: true, retiredAt: 100 })}\n`),
  )
  h.store.set(`replayed:v2:${TABLE_REF}/${retiredSpill}`, { size: 199, fates: 'W', reasons: [], refusals: [], seq: 1, files: [], written: [] })

  h.files.set(`${PARTITION_DIR}/${retiredRejected}`, '')
  h.files.set(
    `${PARTITION_DIR}/outcome-done-w013-${retiredRejected}`,
    sealFile(`${JSON.stringify({ size: 220, fates: 'L', reasons: [], refusals: [], seq: 2, files: [], written: [], retired: true, retiredAt: 100 })}\n`),
  )
  h.store.set(`replayed:v2:${TABLE_REF}/${retiredRejected}`, { size: 219, fates: 'W', reasons: [], refusals: [], seq: 1, files: [], written: [] })

  // Non-retired sending mark beside a 0-byte source must NOT drop its v2 store key.
  h.files.set(`${PARTITION_DIR}/${pendingSpill}`, '')
  h.files.set(
    `${PARTITION_DIR}/outcome-done-w014-${pendingSpill}`,
    sealFile(`${JSON.stringify({ size: 240, sending: true, by: 'w014' })}\n`),
  )
  h.store.set(`replayed:v2:${TABLE_REF}/${pendingSpill}`, { size: 240, fates: 'W', reasons: [], refusals: [], seq: 1, files: [], written: [] })

  h.setFetch(async () => OK_INSERT)
  await h.newWriter('s-clean-v2').backfill(['spill', 'retry'], 'auto')

  expect(h.store.get(`replayed:v2:${TABLE_REF}/${retiredSpill}`)).toBeUndefined()
  expect(h.store.get(`replayed:v2:${TABLE_REF}/${retiredRejected}`)).toBeUndefined()
  expect(h.store.get(`replayed:v2:${TABLE_REF}/${pendingSpill}`)).toBeDefined()
})

test('T53-16d: cleanOrphanedRetiredV2 and cleanExpiredRecords never delete a store journal naming a live non-empty child before parentFate runs', async () => {
  for (const expired of [false, true] as const) {
    const h = exitReplayHarness()
    if (expired) h.advanceNow(100_000_000)
    const name = `spill-20261013T200102003Z-t53-16d-${expired}.jsonl`
    const id = 'd'.repeat(32)
    const childName = `spill-20261013T200102500Z-live-child-${expired}.jsonl`
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(id) })],
      name,
    )[0]!

    // Generation 1 retired with files: [] (left outcome-done-w1-* on disk, optionally >24h old).
    h.files.set(`${PARTITION_DIR}/${name}`, '')
    const done1 = `${PARTITION_DIR}/outcome-done-w1-${name}`
    h.files.set(done1, sealFile(`${JSON.stringify({ size: 200, fates: 'L', reasons: [], refusals: [], seq: 1, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`))
    if (expired) h.mtimes.set(done1, 1_000)

    // Generation 2 (size: 500) wrote liveChild, emptied parent, failed markDone, leaving replayed:v2:* in store.
    h.files.set(childPath, childText)
    h.store.set(`replayed:v2:${TABLE_REF}/${name}`, { size: 500, fates: 'W', reasons: [], refusals: [], seq: 2, files: [childName], written: [true] })

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-16d')
    const r1 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r1.rowsSent).toBe(1)
    expect(h.sentOf(id)).toBe(1)
    expect(h.files.get(childPath)).toBe('')

    // Once liveChild is emptied, the next backfill reclaims the orphaned v2 store key (when done1 still stands or expired).
    if (!expired) {
      await w.backfill(['spill', 'retry'], 'auto')
      expect(h.store.get(`replayed:v2:${TABLE_REF}/${name}`)).toBeUndefined()
    }
  }
})

test('T53-16e: /bqaa-backfill force over same-size conflicting journals clears stale active-slot records before opposite-slot cleanup so unsent rows are preserved', async () => {
  for (const storage of ['store', 'file'] as const) {
    const h = exitReplayHarness({ maxBatchRows: 1 })
    if (storage === 'file') {
      h.setStoreSetHook(async key => {
        if (key.startsWith('replayed:')) throw new Error('store full')
      })
    }
    const name = `spill-20261013T200102003Z-t53-16e-${storage}.jsonl`
    const id1 = '1'.repeat(32)
    const id2 = '2'.repeat(32)
    const sourcePath = h.seedSpill(name, id1, id2)
    const size = utf8Bytes(h.files.get(sourcePath)!)

    const staleV1 = { size, fates: 'LL', reasons: [], refusals: [], waits: false, seq: 9, files: [], written: [] }
    const staleV2 = { size, fates: 'WW', reasons: [], refusals: [], waits: true, seq: 9, files: ['spill-20261013T200102500Z-old.jsonl'], written: [false] }
    if (storage === 'store') {
      // File-backed conflict in both slots while storeSet succeeds for the forced v1 rewrite.
      h.files.set(`${PARTITION_DIR}/outcome-${name}`, sealFile(`${JSON.stringify(staleV1)}\n`))
      h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(staleV2)}\n`))
    } else {
      // Stale conflict in store + outcome-b- while storeSet fails and forced v1 rewrite writes outcome-.
      h.store.set(`replayed:${TABLE_REF}/${name}`, staleV1)
      h.files.set(`${PARTITION_DIR}/outcome-b-${name}`, sealFile(`${JSON.stringify(staleV1)}\n`))
      h.files.set(`${PARTITION_DIR}/outcome2-${name}`, sealFile(`${JSON.stringify(staleV2)}\n`))
    }

    const w = h.newWriter('s-force-conflict')
    const autoBefore = await w.backfill(['spill', 'retry'], 'auto')
    expect(autoBefore.rowsSent).toBe(0)
    expect(autoBefore.held ?? '').toMatch(/conflicting replay records/i)

    let calls = 0
    h.setFetch(async () => {
      calls += 1
      return calls === 1 ? OK_INSERT : UNAVAILABLE
    })
    const forceReport = await w.backfill(['spill', 'retry'], 'force')
    expect(forceReport.rowsSent).toBe(1)
    expect(h.files.get(sourcePath)).toBe('')

    // Leftover child for id2 must exist and be delivered once on the next auto backfill.
    h.sent.length = 0
    h.setFetch(async () => OK_INSERT)
    const autoAfter = await h.newWriter('s-next').backfill(['spill', 'retry'], 'auto')
    expect(autoAfter.rowsSent).toBe(1)
    expect(h.sentOf(id1)).toBe(0)
    expect(h.sentOf(id2)).toBe(1)
  }
})

test('T53-11f: spillForExit with replaySpills: false does not arm exit cutoffs on a live background replay', async () => {
  const h = exitReplayHarness({ requestTimeoutMs: 10_000 })
  const wOld = await h.warmWriter('s-old')
  const wNew = await h.warmWriter('s-new')
  const spillName = 'spill-20261013T200102003Z-t53-11f.jsonl'
  h.seedSpill(spillName, 'a'.repeat(32))

  let replayResolved = false
  h.setFetch((_url, ids) => {
    // Both the live replay on wOld and the queued exit send on wNew stay pending past 700 ms.
    if (ids.includes('a'.repeat(32))) return undefined
    return undefined
  })

  const liveBackfill = wOld.backfill(['spill', 'retry'], 'auto').then(r => {
    replayResolved = true
    return r
  })
  await h.advance(50)
  expect(h.requests.length).toBe(1)

  // Adopt wOld into wNew while wOld has a live replay in flight; wNew calls spillForExit with replaySpills: false.
  wNew.adopt(wOld)
  wNew.enqueue(ownRow('b'.repeat(32)), 0)
  const exiting = wNew.spillForExit(800, { sendQueued: true, replaySpills: false })
  await h.advance(750)
  // wOld's live replay must NOT have been cut off by wNew's non-replaySpills exit.
  expect(replayResolved).toBe(false)
  await h.advance(50)
  await exiting
  expect(replayResolved).toBe(false)

  // Clean up wOld's live replay.
  h.requests[0]!.resolve(OK_INSERT)
  await liveBackfill
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-16f: same-size intra-slot disagreement between store and outcome-b file on emptied parent preserves child as ambiguous instead of dropping it', async () => {
  const h = exitReplayHarness()
  const name = 'spill-20261013T200102003Z-t53-16f-intraslot.jsonl'
  const id = 'f'.repeat(32)
  const sourcePath = h.seedSpill(name, id)
  const size = utf8Bytes(h.files.get(sourcePath)!)
  const childName = 'spill-20261013T200102500Z-intraslot-child.jsonl'
  const childPath = `${PARTITION_DIR}/${childName}`
  const childText = packFiles(
    [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(id) })],
    name,
  )[0]!

  h.files.set(sourcePath, '')
  h.files.set(childPath, childText)
  // Legacy 0.1.3 store journal names childName; stale same-size outcome-b file claims files: [].
  h.store.set(`replayed:${TABLE_REF}/${name}`, { size, fates: 'W', reasons: [], refusals: [], waits: true, seq: 2, files: [childName], written: [true] })
  h.files.set(
    `${PARTITION_DIR}/outcome-b-${name}`,
    sealFile(`${JSON.stringify({ size, fates: 'L', reasons: [], refusals: [], waits: false, seq: 3, files: [], written: [] })}\n`),
  )

  h.setFetch(async () => OK_INSERT)
  const w = h.newWriter('s-intraslot')
  const autoReport = await w.backfill(['spill', 'retry'], 'auto')
  expect(autoReport.rowsSent).toBe(0)
  expect(autoReport.held ?? '').toMatch(/conflicting replay records/i)
  expect(w.heldNoticeInfo().files).toBe(1)
  expect(h.files.get(childPath)).toBe(childText)

  const forceReport = await w.backfill(['spill', 'retry'], 'force')
  expect(forceReport.rowsSent).toBe(1)
  expect(h.sentOf(id)).toBe(1)
  expect(h.files.get(childPath)).toBe('')
})

test('T53-17a: retention keeps expired file journals that live children depend on, and bare v2 records never authorize dropping an unlisted child', async () => {
  // Subcase A: exact 25h scenario — v2 split leaves replayed:v2:* + live childA; different-size source under
  // same parent name has 0.1.3 file-journal fallback outcome-<parent> (failed markDone) naming live childB.
  // At 25 hours, childB must be delivered (not dropped to 0 bytes).
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17a-a.jsonl'
    const childAName = 'spill-20261013T200102100Z-child-a.jsonl'
    const childBName = 'retry-20261013T200102200Z-child-b.jsonl'
    const idA = 'a'.repeat(32)
    const idB = 'b'.repeat(32)
    const childAPath = `${PARTITION_DIR}/${childAName}`
    const childBPath = `${PARTITION_DIR}/${childBName}`
    const childAText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.100Z', table: TABLE_REF, row: ownRow(idA) })],
      parent,
    )[0]!
    const childBText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idB) })],
      parent,
    )[0]!

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(childAPath, childAText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-v2writer01-${childAName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(childAText), sending: true, uncertain: true, by: 'v2writer01' })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parent}`, {
      size: 620,
      fates: 'W',
      reasons: [],
      refusals: [],
      waits: true,
      seq: 2,
      files: [childAName],
      written: [true],
      uncertain: [true],
    })

    h.files.set(childBPath, childBText)
    const outcomeFile = `${PARTITION_DIR}/outcome-${parent}`
    h.files.set(
      outcomeFile,
      sealFile(`${JSON.stringify({ size: 480, fates: 'W', reasons: [], refusals: [], waits: true, seq: 1, files: [childBName], written: [true] })}\n`),
    )
    h.mtimes.set(outcomeFile, 1_000)
    h.advanceNow(25 * 60 * 60 * 1000 + 1_000)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17a-a')
    const r = await w.backfill(['spill', 'retry'], 'auto')
    expect(r.rowsSent).toBe(1)
    expect(h.sentOf(idB)).toBe(1)
    expect(h.files.get(childBPath)).toBe('')
    expect(h.files.get(childAPath)).toBe(childAText)
  }

  // Subcase B: when childB remains live across a 25h backfill (transient 503), expired outcome-<parent>
  // must NOT be truncated while childB still depends on it, so the next backfill delivers childB.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17a-b.jsonl'
    const childAName = 'spill-20261013T200102100Z-child-a.jsonl'
    const childBName = 'retry-20261013T200102200Z-child-b.jsonl'
    const idA = 'c'.repeat(32)
    const idB = 'd'.repeat(32)
    const childAPath = `${PARTITION_DIR}/${childAName}`
    const childBPath = `${PARTITION_DIR}/${childBName}`
    const childAText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.100Z', table: TABLE_REF, row: ownRow(idA) })],
      parent,
    )[0]!
    const childBText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idB) })],
      parent,
    )[0]!

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(childAPath, childAText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-v2writer02-${childAName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(childAText), sending: true, uncertain: true, by: 'v2writer02' })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parent}`, {
      size: 620,
      fates: 'W',
      reasons: [],
      refusals: [],
      waits: true,
      seq: 2,
      files: [childAName],
      written: [true],
      uncertain: [true],
    })
    h.files.set(childBPath, childBText)
    const outcomeFile = `${PARTITION_DIR}/outcome-${parent}`
    const outcomeText = sealFile(`${JSON.stringify({ size: 480, fates: 'W', reasons: [], refusals: [], waits: true, seq: 1, files: [childBName], written: [true] })}\n`)
    h.files.set(outcomeFile, outcomeText)
    h.mtimes.set(outcomeFile, 1_000)
    h.advanceNow(25 * 60 * 60 * 1000 + 1_000)

    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-17a-b')
    const r1 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r1.rowsSent).toBe(0)
    expect(h.files.get(childBPath)).toBe(childBText)
    // Expired file journal must remain intact while live childB still depends on it.
    expect(h.files.get(outcomeFile)).toBe(outcomeText)

    h.advanceNow(60 * 60 * 1000)
    h.setFetch(async () => OK_INSERT)
    const r2 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r2.rowsSent).toBe(1)
    expect(h.sent.filter(s => s.id === idB && s.status === 200).length).toBe(1)
    expect(h.files.get(childBPath)).toBe('')
  }

  // Subcase C: even when only a bare v2 store record survives beside an emptied parent (no retired done mark),
  // parentFate must preserve an unlisted child for recovery instead of dropping it unsent.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17a-c.jsonl'
    const childAName = 'spill-20261013T200102100Z-child-a.jsonl'
    const childBName = 'retry-20261013T200102200Z-child-b.jsonl'
    const idB = 'e'.repeat(32)
    const childBPath = `${PARTITION_DIR}/${childBName}`
    const childBText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idB) })],
      parent,
    )[0]!

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(`${PARTITION_DIR}/outcome-${parent}`, '')
    h.store.set(`replayed:v2:${TABLE_REF}/${parent}`, {
      size: 620,
      fates: 'W',
      reasons: [],
      refusals: [],
      waits: true,
      seq: 2,
      files: [childAName],
      written: [true],
      uncertain: [true],
    })
    h.files.set(childBPath, childBText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17a-c')
    const rAuto = await w.backfill(['spill', 'retry'], 'auto')
    expect(rAuto.rowsSent).toBe(0)
    expect(h.files.get(childBPath)).toBe(childBText)
    expect(w.heldNoticeInfo().files).toBe(1)

    const rForce = await w.backfill(['spill', 'retry'], 'force')
    expect(rForce.rowsSent).toBe(1)
    expect(h.sentOf(idB)).toBe(1)
    expect(h.files.get(childBPath)).toBe('')
  }
})

test('T53-8d: headless notice keeps owner-stopped and lease-expired qualification for a foreign active sending mark', { options: OPTIONS }, async ($, on) => {
  // Foreign non-uncertain sending mark (e.g. pending sender after lease-write failure).
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102003Z-t53-8d-a.jsonl'
  const text = parkedFile(parkedLine(TABLE_REF, '1'.repeat(32), 'other-p'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-active0001-${name}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, by: 'active0001' })}\n`),
  )

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8d-a', resume: { id: 'p-8d-a' } })
  await world.clock.advance(2000)
  await ending

  const notices = world.stderr.filter(l => l.includes('dead-letter file(s) held'))
  expect(notices.length).toBe(1)
  const notice = notices[0]!
  expect(notice).toContain('1 dead-letter file(s) held')
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-8e: headless notice keeps owner-stopped and lease-expired qualification in mixed directories and on files with both uncertain and active marks', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const nameA = 'spill-20261013T200102003Z-t53-8d-b1.jsonl'
  const nameB = 'spill-20261013T200102004Z-t53-8d-b2.jsonl'
  const textA = parkedFile(parkedLine(TABLE_REF, '2'.repeat(32), 'ended-p'))
  const textB = parkedFile(parkedLine(TABLE_REF, '3'.repeat(32), 'active-p'))
  world.files.set(`${PARTITION_DIR}/${nameA}`, textA)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-ended00001-${nameA}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(textA), sending: true, uncertain: true, by: 'ended00001' })}\n`),
  )
  world.files.set(`${PARTITION_DIR}/${nameB}`, textB)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-ended00002-${nameB}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(textB), sending: true, uncertain: true, by: 'ended00002' })}\n`),
  )
  world.files.set(
    `${PARTITION_DIR}/outcome-done-active0002-${nameB}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(textB), sending: true, by: 'active0002' })}\n`),
  )

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8d-b', resume: { id: 'p-8d-b' } })
  await world.clock.advance(2000)
  await ending

  const notices = world.stderr.filter(l => l.includes('dead-letter file(s) held'))
  expect(notices.length).toBe(1)
  const notice = notices[0]!
  expect(notice).toContain('2 dead-letter file(s) held')
  expect(notice).toContain(nameB)
  expect(notice).not.toContain('no answer before the session ended')
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-15c: live spills run before historical cleanup and cleanup yields promptly to shutdown and bounded exit budget', async () => {
  const seedRetired = (h: ReturnType<typeof exitReplayHarness>, count: number, ageMs: number): void => {
    const retiredTime = h.ioFor().now() - ageMs
    for (let i = 0; i < count; i += 1) {
      const name = `spill-20261013T180000000Z-hist-${String(i).padStart(3, '0')}.jsonl`
      const donePath = `${PARTITION_DIR}/outcome-done-oldmark123-${name}`
      h.files.set(`${PARTITION_DIR}/${name}`, '')
      h.files.set(
        donePath,
        sealFile(`${JSON.stringify({ size: 120, fates: 'L', reasons: [], refusals: [], waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: retiredTime })}\n`),
      )
      h.mtimes.set(`${PARTITION_DIR}/${name}`, retiredTime)
      h.mtimes.set(donePath, retiredTime)
    }
  }

  // Subcase A: pre-shutdown backfill with 130 two-hour-old retirements + 1 waiting spill (8 ms per v2 lookup,
  // 400 ms dispatch, 800 ms exit budget) delivers the waiting row first instead of starving in historical lookups.
  {
    const h = exitReplayHarness()
    h.advanceNow(4 * 60 * 60 * 1000)
    const w = await h.warmWriter('s-15c-a')
    seedRetired(h, 130, 2 * 60 * 60 * 1000)
    const waitingId = '7'.repeat(32)
    const waitingPath = h.seedSpill('spill-20261013T200102003Z-waiting-a.jsonl', waitingId)

    let histLookups = 0
    let histLookupsBeforeDispatch = -1
    h.setStoreGetHook(async key => {
      if (key.startsWith('replayed:v2:') && key.includes('-hist-')) {
        histLookups += 1
        await new Promise<void>(resolve => {
          h.ioFor().after(8, resolve)
        })
      }
    })
    h.setFetch((_url, _ids, controls) => {
      histLookupsBeforeDispatch = histLookups
      h.ioFor().after(400, () => controls.resolve(OK_INSERT))
      return undefined
    })

    const liveBackfill = w.backfill(['spill', 'retry'], 'auto')
    await h.settleMicrotasks()
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting
    await liveBackfill

    expect(h.sentOf(waitingId)).toBe(1)
    expect(h.files.get(waitingPath)).toBe('')
    expect(histLookupsBeforeDispatch).toBe(0)
    expect(histLookups).toBeLessThanOrEqual(20)
    expect(h.uncancelledTimers()).toEqual([])
  }

  // Subcase B: fresh -p exit replay with 130 two-hour-old retirements + 1 waiting spill delivers the waiting row
  // first and caps cleanup to a bounded share of the remaining exit window.
  {
    const h = exitReplayHarness()
    h.advanceNow(4 * 60 * 60 * 1000)
    const w = await h.warmWriter('s-15c-b')
    seedRetired(h, 130, 2 * 60 * 60 * 1000)
    const waitingId = '8'.repeat(32)
    const waitingPath = h.seedSpill('spill-20261013T200102003Z-waiting-b.jsonl', waitingId)

    let v2Lookups = 0
    h.setStoreGetHook(async key => {
      if (key.startsWith('replayed:v2:')) {
        v2Lookups += 1
        await new Promise<void>(resolve => {
          h.ioFor().after(8, resolve)
        })
      }
    })
    h.setFetch((_url, _ids, controls) => {
      h.ioFor().after(400, () => controls.resolve(OK_INSERT))
      return undefined
    })

    const start = h.ioFor().now()
    let finishedAt = 0
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true }).then(res => {
      finishedAt = h.ioFor().now() - start
      return res
    })
    await h.advance(800)
    await exiting

    expect(h.sentOf(waitingId)).toBe(1)
    expect(h.files.get(waitingPath)).toBe('')
    // Cleanup must not consume the rest of the 800 ms exit budget.
    expect(v2Lookups).toBeLessThanOrEqual(20)
    expect(finishedAt).toBeLessThan(650)
    expect(h.uncancelledTimers()).toEqual([])
  }

  // Subcase C: pre-shutdown backfill already inside orphan cleanup aborts immediately once shutdown starts
  // so replayAtExit has time to deliver a newly arrived spill row.
  {
    const h = exitReplayHarness()
    h.advanceNow(4 * 60 * 60 * 1000)
    const w = await h.warmWriter('s-15c-c')
    seedRetired(h, 130, 2 * 60 * 60 * 1000)

    let v2Lookups = 0
    h.setStoreGetHook(async key => {
      if (key.startsWith('replayed:v2:')) {
        v2Lookups += 1
        await new Promise<void>(resolve => {
          h.ioFor().after(8, resolve)
        })
      }
    })
    h.setFetch((_url, _ids, controls) => {
      h.ioFor().after(400, () => controls.resolve(OK_INSERT))
      return undefined
    })

    const preShutdown = w.backfill(['spill', 'retry'], 'auto')
    await h.advance(20)
    expect(v2Lookups).toBeGreaterThanOrEqual(1)
    expect(v2Lookups).toBeLessThan(10)

    const waitingId = '9'.repeat(32)
    const waitingPath = h.seedSpill('spill-20261013T200102003Z-waiting-c.jsonl', waitingId)
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting
    await preShutdown

    expect(h.sentOf(waitingId)).toBe(1)
    expect(h.files.get(waitingPath)).toBe('')
    expect(v2Lookups).toBeLessThan(25)
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-11g: exit hold count tracks the durable sending mark from write through confirmed release or retirement', async () => {
  // Subcase A: pre-dispatch stall after writing the sending mark reports 1 held at exit (matching restart).
  {
    const h = exitReplayHarness()
    const w = await h.warmWriter('s-11g-a')
    const name = 'spill-20261013T200102003Z-t53-11g-a.jsonl'
    h.seedSpill(name, '1'.repeat(32))

    let markWritten = false
    let releaseRead = (): void => undefined
    const readGate = new Promise<void>(resolve => {
      releaseRead = resolve
    })
    h.setWriteFileHook(async (path, text) => {
      if (path.includes('/outcome-done-') && path.endsWith(`-${name}`) && text !== '') {
        markWritten = true
      }
    })
    h.setReadFileHook(async path => {
      if (markWritten && path.includes('/outcome-done-') && path.endsWith(`-${name}`)) {
        await readGate
      }
    })

    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    expect(markWritten).toBe(true)
    expect(h.requests.length).toBe(0)
    expect(w.heldNoticeInfo().files).toBe(1)

    // Verify a restart after lease expiry sees the same held file on disk.
    h.setReadFileHook(null)
    h.advanceNow(11 * 60 * 1000)
    const wRestart = h.newWriter('s-11g-a-restart')
    const restartReport = await wRestart.backfill(['spill', 'retry'], 'auto')
    expect(restartReport.heldFiles).toBe(1)

    releaseRead()
    await h.settleMicrotasks()
    expect(h.uncancelledTimers()).toEqual([])
  }

  // Subcase B: HTTP 200 followed by failed journal persistence keeps the sending mark tracked (1 held)
  // until a later retry retires the file.
  {
    const h = exitReplayHarness()
    const w = await h.warmWriter('s-11g-b')
    const name = 'spill-20261013T200102003Z-t53-11g-b.jsonl'
    const sourcePath = h.seedSpill(name, '2'.repeat(32))

    h.setFetch(async () => OK_INSERT)
    h.setStoreSetHook(async key => {
      if (key.startsWith('replayed:')) throw new Error('store unavailable')
    })
    h.setWriteFileHook(async (path, text) => {
      if ((path.includes('/outcome-') || path.includes('/outcome2-')) && !path.includes('/outcome-done-') && text !== '') {
        throw new Error('disk full')
      }
    })

    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(sourcePath)).not.toBe('')
    expect(w.heldNoticeInfo().files).toBe(1)

    // Confirm a restart sees the same held mark on disk.
    const wRestart = h.newWriter('s-11g-b-restart')
    const restartReport = await wRestart.backfill(['spill', 'retry'], 'auto')
    expect(restartReport.heldFiles).toBe(1)

    // Exiting while persistence still fails reports 1 held file.
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting
    expect(w.heldNoticeInfo().files).toBe(1)

    // Once persistence recovers on a live writer, resuming and retiring the file clears the hold count back to 0.
    const w2 = await h.warmWriter('s-11g-b-retry')
    const name2 = 'spill-20261013T200102004Z-t53-11g-b2.jsonl'
    const sourcePath2 = h.seedSpill(name2, '4'.repeat(32))
    h.files.set(sourcePath, '')
    await w2.backfill(['spill', 'retry'], 'auto')
    expect(w2.heldNoticeInfo().files).toBe(1)
    h.setStoreSetHook(null)
    h.setWriteFileHook(null)
    await w2.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(sourcePath2)).toBe('')
    expect(w2.heldNoticeInfo().files).toBe(0)
  }

  // Subcase C: once writeOut empties the source file, a stall during storeDelete cleanup reports 0 held at exit.
  {
    const h = exitReplayHarness()
    const w = await h.warmWriter('s-11g-c')
    const name = 'spill-20261013T200102003Z-t53-11g-c.jsonl'
    const sourcePath = h.seedSpill(name, '3'.repeat(32))

    let releaseDelete = (): void => undefined
    const deleteGate = new Promise<void>(resolve => {
      releaseDelete = resolve
    })
    let deleteStalled = false
    h.setStoreDeleteHook(async key => {
      if (key === `replayed:${TABLE_REF}/${name}`) {
        deleteStalled = true
        await deleteGate
      }
    })
    h.setFetch(async () => OK_INSERT)

    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    expect(deleteStalled).toBe(true)
    expect(h.files.get(sourcePath)).toBe('')
    expect(w.heldNoticeInfo().files).toBe(0)

    releaseDelete()
    await h.settleMicrotasks()
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-14f: noteHeld maintains O(1) per-directory held counts so full scans scale linearly across 32 and 128 held files, adopt, and Windows paths', async () => {
  const measureScanVisits = async (count: number): Promise<{ visits: number; held: number }> => {
    const h = exitReplayHarness()
    for (let i = 0; i < count; i += 1) {
      const name = `spill-20261013T200102003Z-lin-${String(i).padStart(3, '0')}.jsonl`
      const p = h.seedSpill(name, (10_000 + i).toString(16).padStart(32, '0'))
      const text = h.files.get(p)!
      h.files.set(
        `${PARTITION_DIR}/outcome-done-other00001-${name}`,
        sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, uncertain: true, by: 'other00001' })}\n`),
      )
    }
    const w = h.newWriter(`s-14f-${count}`)
    const heldMap = (w as unknown as { heldPaths: Map<string, unknown> }).heldPaths
    let visits = 0
    const origKeys = heldMap.keys.bind(heldMap)
    const origEntries = heldMap.entries.bind(heldMap)
    const origValues = heldMap.values.bind(heldMap)
    const origIter = heldMap[Symbol.iterator].bind(heldMap)
    const wrap = <T>(iter: IterableIterator<T>): IterableIterator<T> => ({
      [Symbol.iterator]() {
        return this
      },
      next() {
        const res = iter.next()
        if (!res.done) visits += 1
        return res
      },
    })
    heldMap.keys = () => wrap(origKeys())
    heldMap.entries = () => wrap(origEntries())
    heldMap.values = () => wrap(origValues())
    heldMap[Symbol.iterator] = () => wrap(origIter())

    const r = await w.backfill(['spill', 'retry'], 'auto')
    expect(r.heldFiles).toBe(count)
    expect(w.heldNoticeInfo().files).toBe(count)
    return { visits, held: r.heldFiles ?? 0 }
  }

  const small = await measureScanVisits(32)
  const large = await measureScanVisits(128)
  expect(small.held).toBe(32)
  expect(large.held).toBe(128)
  // Quadratic scanning yields 528 visits for 32 files and 8,256 visits for 128 files.
  // O(1) per-directory counters keep total visits linear (<= 2*N) instead of O(N^2).
  expect(small.visits).toBeLessThanOrEqual(64)
  expect(large.visits).toBeLessThanOrEqual(256)

  // Verify adopt() + Windows backslash paths keep per-directory counts synchronized across add and prune.
  const winRoot = 'C:\\Users\\dev\\.bqaa\\dead-letter'
  const winDir = `${winRoot}\\${TABLE_REF}`
  const winConfig = resolveConfig({ ...OPTIONS, deadLetterDir: winRoot }, { HOME, USER: 'dev' })
  const winFiles = new Map<string, string>()
  const winStore = new Map<string, unknown>()
  const winIo = (): Io => ({
    now: () => 0,
    after: () => ({ cancel: () => undefined }),
    fetch: async () => OK_INSERT,
    runCommand: async () => ({ exitCode: 0, stdout: `${'a'.repeat(24)}\n`, stderr: '' }),
    envToken: async () => ACCESS_TOKEN,
    writeFile: async (p, text) => void winFiles.set(p, text),
    readFile: async p => winFiles.get(p) ?? '',
    exists: async p => winFiles.has(p),
    listDir: async d => {
      const prefix = `${d.replace(/[\\/]+$/, '')}\\`
      return [...winFiles]
        .filter(([p]) => p.startsWith(prefix) && !p.slice(prefix.length).includes('\\'))
        .map(([p, text]) => ({ name: p.slice(prefix.length), kind: 'file', size: utf8Bytes(text) }))
    },
    storeGet: async k => winStore.get(k),
    storeSet: async (k, v) => void winStore.set(k, JSON.parse(JSON.stringify(v)) as unknown),
    storeDelete: async k => void winStore.delete(k),
    status: () => undefined,
    debug: () => undefined,
  })
  const n1 = 'spill-20261013T200102003Z-win-1.jsonl'
  const n2 = 'spill-20261013T200102004Z-win-2.jsonl'
  const t1 = parkedFile(parkedLine(TABLE_REF, '4'.repeat(32), 'win-s'))
  const t2 = parkedFile(parkedLine(TABLE_REF, '5'.repeat(32), 'win-s'))
  winFiles.set(`${winDir}\\${n1}`, t1)
  winFiles.set(`${winDir}\\outcome-done-winmark001-${n1}`, sealFile(`${JSON.stringify({ size: utf8Bytes(t1), sending: true, uncertain: true, by: 'winmark001' })}\n`))
  winFiles.set(`${winDir}\\${n2}`, t2)
  winFiles.set(`${winDir}\\outcome-done-winmark002-${n2}`, sealFile(`${JSON.stringify({ size: utf8Bytes(t2), sending: true, uncertain: true, by: 'winmark002' })}\n`))

  const w1 = new Writer(winIo(), () => winConfig, () => 'win-1')
  await w1.backfill(['spill', 'retry'], 'auto')
  expect(w1.heldNoticeInfo().files).toBe(2)

  const w2 = new Writer(winIo(), () => winConfig, () => 'win-2')
  w2.adopt(w1)
  expect(w2.heldNoticeInfo().files).toBe(2)
  // Clear one file and prune via backfill; count drops from 2 -> 1 -> 0.
  winFiles.set(`${winDir}\\${n1}`, '')
  await w2.backfill(['spill', 'retry'], 'auto')
  expect(w2.heldNoticeInfo().files).toBe(1)
  await w2.backfill(['spill', 'retry'], 'force')
  expect(w2.heldNoticeInfo().files).toBe(0)
})

test('T53-17b: cleanup preserves parent evidence whenever any live dead-letter file on disk still names that parent', async () => {
  // Subcase A: cleanOrphanedRetiredV2 must NOT delete replayed:v2:* while an unlisted live childB naming parent
  // is held as ambiguous; a second auto backfill must preserve childB instead of dropping it unsent.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17b-a.jsonl'
    const childAName = 'spill-20261013T200102100Z-child-a.jsonl'
    const childBName = 'retry-20261013T200102200Z-child-b.jsonl'
    const idB = '1'.repeat(32)
    const childBPath = `${PARTITION_DIR}/${childBName}`
    const childBText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idB) })],
      parent,
    )[0]!

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(`${PARTITION_DIR}/${childAName}`, '')
    h.files.set(
      `${PARTITION_DIR}/outcome-done-v1writer01-${parent}`,
      sealFile(`${JSON.stringify({ size: 520, fates: 'L', reasons: [], refusals: [], waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parent}`, {
      size: 620,
      fates: 'W',
      reasons: [],
      refusals: [],
      waits: true,
      seq: 2,
      files: [childAName],
      written: [true],
      uncertain: [true],
    })
    h.files.set(childBPath, childBText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17b-a')
    const r1 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r1.rowsSent).toBe(0)
    expect(r1.heldFiles).toBe(1)
    expect(h.files.get(childBPath)).toBe(childBText)
    expect(h.store.get(`replayed:v2:${TABLE_REF}/${parent}`)).toBeDefined()

    // Second auto backfill must STILL hold childB (never drop it unsent).
    const r2 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r2.rowsSent).toBe(0)
    expect(r2.heldFiles).toBe(1)
    expect(h.files.get(childBPath)).toBe(childBText)

    // Force delivers childB once.
    const rForce = await w.backfill(['spill', 'retry'], 'force')
    expect(rForce.rowsSent).toBe(1)
    expect(h.sentOf(idB)).toBe(1)
    expect(h.files.get(childBPath)).toBe('')
  }

  // Subcase B: cleanExpiredRecords at 25h must NOT truncate bare outcome2-<parent> while live unlisted childB
  // still names parent.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17b-b.jsonl'
    const childAName = 'spill-20261013T200102100Z-child-a.jsonl'
    const childBName = 'retry-20261013T200102200Z-child-b.jsonl'
    const idB = '2'.repeat(32)
    const childBPath = `${PARTITION_DIR}/${childBName}`
    const childBText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idB) })],
      parent,
    )[0]!
    const outcome2File = `${PARTITION_DIR}/outcome2-${parent}`
    const outcome2Text = sealFile(
      `${JSON.stringify({ size: 620, fates: 'W', reasons: [], refusals: [], waits: true, seq: 2, files: [childAName], written: [true], uncertain: [true] })}\n`,
    )

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(`${PARTITION_DIR}/${childAName}`, '')
    h.files.set(outcome2File, outcome2Text)
    h.mtimes.set(outcome2File, 1_000)
    h.files.set(childBPath, childBText)
    h.advanceNow(25 * 60 * 60 * 1000 + 1_000)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17b-b')
    const r1 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r1.rowsSent).toBe(0)
    expect(r1.heldFiles).toBe(1)
    expect(h.files.get(outcome2File)).toBe(outcome2Text)

    // Second auto backfill still holds childB rather than bypassing the ambiguous hold.
    const r2 = await w.backfill(['spill', 'retry'], 'auto')
    expect(r2.rowsSent).toBe(0)
    expect(r2.heldFiles).toBe(1)
    expect(h.files.get(childBPath)).toBe(childBText)
  }
})

test('T53-17c: parentFate checks all generations before dropping when parent had size > 0 and preserves children beside unexplained torn journals', async () => {
  // Subcase A: parent is re-populated (Gen 2) and emptied earlier in the same backfillRun, while Gen 1's retired
  // done mark explicitly names child; parentFate must deliver child instead of dropping via stale size > 0.
  {
    const h = exitReplayHarness()
    const parent = 'retry-20261013T190000000Z-t53-17c-parent.jsonl'
    const childName = 'spill-20261013T200000000Z-t53-17c-child.jsonl'
    const idParent = '3'.repeat(32)
    const idChild = '4'.repeat(32)
    const parentPath = h.seedSpill(parent, idParent)
    const parentGen2Size = utf8Bytes(h.files.get(parentPath)!)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:00:00.000Z', table: TABLE_REF, row: ownRow(idChild) })],
      parent,
    )[0]!

    h.files.set(
      `${PARTITION_DIR}/outcome-done-gen1mark01-${parent}`,
      sealFile(`${JSON.stringify({ size: parentGen2Size - 40, fates: 'W', reasons: [], refusals: [], waits: true, seq: 1, files: [childName], written: [true], retired: true })}\n`),
    )
    h.files.set(childPath, childText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17c-a')
    const r = await w.backfill(['spill', 'retry'], 'auto')
    expect(r.rowsSent).toBe(2)
    expect(h.sentOf(idParent)).toBe(1)
    expect(h.sentOf(idChild)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase B: live parent (size > 0) with a retired Gen 2 done mark while child sorts BEFORE parent and belongs to Gen 1.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200000000Z-t53-17c-parent-b.jsonl'
    const childName = 'retry-20261013T190000000Z-t53-17c-child-b.jsonl'
    const idParent = '5'.repeat(32)
    const idChild = '6'.repeat(32)
    const parentPath = h.seedSpill(parent, idParent)
    const parentGen2Size = utf8Bytes(h.files.get(parentPath)!)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T19:00:00.000Z', table: TABLE_REF, row: ownRow(idChild) })],
      parent,
    )[0]!

    h.files.set(
      `${PARTITION_DIR}/outcome-done-gen1mark02-${parent}`,
      sealFile(`${JSON.stringify({ size: parentGen2Size - 40, fates: 'W', reasons: [], refusals: [], waits: true, seq: 1, files: [childName], written: [true], retired: true })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-gen2mark02-${parent}`,
      sealFile(`${JSON.stringify({ size: parentGen2Size, fates: 'L', reasons: [], refusals: [], waits: false, seq: 1, files: [], written: [], retired: true })}\n`),
    )
    h.files.set(childPath, childText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17c-b')
    const r = await w.backfill(['spill', 'retry'], 'auto')
    expect(r.rowsSent).toBe(1)
    expect(h.sentOf(idChild)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase C: retired Gen 1 done mark (size: 480) + torn Gen 2 outcome-<parent> (size: 620, unknown, or
  // mid-digit EOF truncation of 4805 to 480) beside emptied parent must preserve unlisted child as ambiguous.
  for (const tornText of ['{"size":620,"fates":"W","refu', '{"fates":"W","refu', '{"fates":"W","reasons":[],"refusals":[],"size":480'] as const) {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17c-c.jsonl'
    const childName = 'retry-20261013T200102200Z-t53-17c-child-c.jsonl'
    const idChild = '7'.repeat(32)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.200Z', table: TABLE_REF, row: ownRow(idChild) })],
      parent,
    )[0]!

    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(
      `${PARTITION_DIR}/outcome-done-gen1mark03-${parent}`,
      sealFile(`${JSON.stringify({ size: 480, fates: 'L', reasons: [], refusals: [], waits: false, seq: 1, files: [], written: [], retired: true })}\n`),
    )
    h.files.set(`${PARTITION_DIR}/outcome-${parent}`, tornText)
    h.files.set(childPath, childText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter('s-17c-c')
    const rAuto = await w.backfill(['spill', 'retry'], 'auto')
    expect(rAuto.rowsSent).toBe(0)
    expect(rAuto.heldFiles).toBe(1)
    expect(h.files.get(childPath)).toBe(childText)

    const rForce = await w.backfill(['spill', 'retry'], 'force')
    expect(rForce.rowsSent).toBe(1)
    expect(h.sentOf(idChild)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-8f: foreign active sending mark coexisting with a torn or conflicting record still qualifies headlessNotice', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102003Z-t53-8f.jsonl'
  const text = parkedFile(parkedLine(TABLE_REF, '8'.repeat(32), 'other-p'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  // Active foreign sending mark + 0-byte torn journal file -> readJournal returns 'torn', must still keep mayBeActive: true.
  world.files.set(
    `${PARTITION_DIR}/outcome-done-active0003-${name}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, by: 'active0003' })}\n`),
  )
  world.files.set(`${PARTITION_DIR}/outcome-${name}`, '')

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'c.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8f', resume: { id: 'p-8f' } })
  await world.clock.advance(2000)
  await ending

  const notices = world.stderr.filter(l => l.includes('dead-letter file(s) held'))
  expect(notices.length).toBe(1)
  const notice = notices[0]!
  expect(notice).toContain('1 dead-letter file(s) held')
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-11h: writeOut early returns, pruned stale lastHeld, and multi-child uncertain stalls keep heldNoticeInfo exact', async () => {
  // Subcase A: concurrent writer finishes and retires source with a different child name before this writer's
  // writeOut runs (!catchUp) -> clears activeSendingMarks & unmarks so heldNoticeInfo reports 0 held.
  {
    const h = exitReplayHarness({ maxBatchRows: 1 })
    const w = await h.warmWriter('s-11h-a')
    const name = 'spill-20261013T200102003Z-t53-11h-a.jsonl'
    const sourcePath = h.seedSpill(name, '1'.repeat(32), '2'.repeat(32))
    const size = utf8Bytes(h.files.get(sourcePath)!)

    let calls = 0
    h.setFetch(async () => {
      calls += 1
      if (calls === 1) return OK_INSERT
      // Simulate concurrent writer retiring source with its own child filename before our writeOut checks readJournal.
      h.files.set(
        `${PARTITION_DIR}/outcome-done-otherw0001-${name}`,
        sealFile(`${JSON.stringify({ size, fates: 'LW', reasons: [], refusals: [], waits: true, seq: 2, files: ['spill-20261013T200102500Z-other-child.jsonl'], written: [true], retired: true })}\n`),
      )
      h.files.set(sourcePath, '')
      return UNAVAILABLE
    })

    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(0)
    const ownMarks = h.doneMarksFor(name).filter(m => !m.path.includes('otherw0001'))
    expect(ownMarks.length).toBe(0)
  }

  // Subcase B: Backfill #1 holds 1 active-foreign file (lastHeld.files = 1, mayBeActive = true); that file is emptied
  // and pruned at start of Backfill #2 while Backfill #2 stalls on a fresh spill -> heldNoticeInfo must reflect only
  // the live file (mayBeActive: false), never stale lastHeld.
  {
    const h = exitReplayHarness()
    const w = await h.warmWriter('s-11h-b')
    const oldName = 'spill-20261013T200102003Z-t53-11h-old.jsonl'
    const oldPath = h.seedSpill(oldName, '3'.repeat(32))
    const oldText = h.files.get(oldPath)!
    h.files.set(
      `${PARTITION_DIR}/outcome-done-foreign001-${oldName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(oldText), sending: true, by: 'foreign001' })}\n`),
    )

    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(1)
    expect(w.heldNoticeInfo().mayBeActive).toBe(true)

    // Old file is emptied; fresh file stalls on storeDelete after being emptied -> heldNoticeInfo must report 0 held.
    h.files.set(oldPath, '')
    const freshName = 'spill-20261013T200102004Z-t53-11h-fresh.jsonl'
    const freshPath = h.seedSpill(freshName, '4'.repeat(32))
    let releaseDelete = (): void => undefined
    const deleteGate = new Promise<void>(resolve => {
      releaseDelete = resolve
    })
    h.setStoreDeleteHook(async key => {
      if (key === `replayed:${TABLE_REF}/${freshName}`) await deleteGate
    })
    h.setFetch(async () => OK_INSERT)

    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    expect(h.files.get(freshPath)).toBe('')
    expect(w.heldNoticeInfo().files).toBe(0)
    expect(w.heldNoticeInfo().mayBeActive).toBe(false)

    releaseDelete()
    await h.settleMicrotasks()
    expect(h.uncancelledTimers()).toEqual([])
  }

  // Subcase C: multi-child uncertain split (e.g. mixed spill + retry lines in one source) stalling on storeSet
  // before any child file is written reports 1 held file (the parent on disk), not 2.
  {
    const h = exitReplayHarness({ maxBatchRows: 2 })
    const w = await h.warmWriter('s-11h-c')
    const name = 'spill-20261013T200102003Z-t53-11h-c.jsonl'
    const line1 = JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow('5'.repeat(32)) })
    const line2 = JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 1, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow('6'.repeat(32)) })
    const line3 = JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow('7'.repeat(32)) })
    h.files.set(`${PARTITION_DIR}/${name}`, packFiles([line1, line2, line3])[0]!)

    let releaseStore = (): void => undefined
    const storeGate = new Promise<void>(resolve => {
      releaseStore = resolve
    })
    h.setStoreSetHook(async key => {
      if (key === `replayed:v2:${TABLE_REF}/${name}`) await storeGate
    })
    // Batch 1 (spill + retry, 2 rows) hangs past replay cutoff; Batch 2 (1 row) skipped -> 2 uncertainFiles!
    h.setFetch(() => undefined)

    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    expect(w.heldNoticeInfo().files).toBe(1)

    releaseStore()
    await h.settleMicrotasks()
    expect(h.uncancelledTimers()).toEqual([])
  }

  // Subcase D: resumeRewrite superseded by a concurrent writer that already retired and emptied path
  // must NOT re-add the retired 0-byte path to heldPaths in backfillRun's finally block.
  {
    const h = exitReplayHarness({ maxBatchRows: 1 })
    const w = await h.warmWriter('s-11h-d')
    const name = 'spill-20261013T200102003Z-t53-11h-d.jsonl'
    const sourcePath = h.seedSpill(name, '8'.repeat(32), '9'.repeat(32))
    const size = utf8Bytes(h.files.get(sourcePath)!)
    const v2Key = `replayed:v2:${TABLE_REF}/${name}`

    // Seed an unwritten uncertain child in v2 store journal so backfillRun enters resumeRewrite -> writeOut.
    h.store.set(v2Key, {
      size,
      fates: 'WW',
      reasons: [],
      refusals: [],
      uncertainLines: [0],
      waits: true,
      seq: 1,
      files: ['spill-20261013T200102100Z-uncertain.jsonl', 'spill-20261013T200102101Z-waiting.jsonl'],
      written: [false, false],
      uncertain: [true, false],
    })
    // Before writeOut's readJournal check runs, a concurrent writer retires and empties source with its own done mark.
    let v2Reads = 0
    h.setStoreGetHook(async key => {
      if (key === v2Key) {
        v2Reads += 1
        if (v2Reads === 2) {
          h.files.set(
            `${PARTITION_DIR}/outcome-done-otherw0002-${name}`,
            sealFile(`${JSON.stringify({ size, fates: 'LL', reasons: [], refusals: [], waits: false, seq: 3, files: [], written: [], retired: true })}\n`),
          )
          h.files.set(sourcePath, '')
        }
      }
    })

    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(sourcePath)).toBe('')
    expect(w.heldNoticeInfo().files).toBe(0)
  }
})

test('T53-17d: cleanExpiredRecords deletes store keys before truncating companion files so store errors never strand orphaned v2 keys', async () => {
  const h = exitReplayHarness()
  const parent = 'spill-20261013T200102003Z-t53-17d.jsonl'
  const donePath = `${PARTITION_DIR}/outcome-done-oldmark01d-${parent}`
  const doneText = sealFile(`${JSON.stringify({ size: 500, fates: 'L', reasons: [], refusals: [], waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`)
  const v2Key = `replayed:v2:${TABLE_REF}/${parent}`

  h.files.set(`${PARTITION_DIR}/${parent}`, '')
  h.files.set(donePath, doneText)
  h.mtimes.set(donePath, 1_000)
  h.store.set(v2Key, { size: 500, fates: 'W', reasons: [], refusals: [], waits: true, seq: 1, files: [], written: [] })
  h.advanceNow(25 * 60 * 60 * 1000 + 1_000)

  // First backfill: storeDelete fails on v2Key -> done mark MUST remain on disk and v2Key must NOT be cached as cleaned.
  let failDelete = true
  h.setStoreDeleteHook(async key => {
    if (failDelete && key === v2Key) throw new Error('store delete transient error')
  })
  const w = h.newWriter('s-17d')
  await w.backfill(['spill', 'retry'], 'auto')
  expect(h.store.get(v2Key)).toBeDefined()
  expect(h.files.get(donePath)).toBe(doneText)

  // Second backfill once store recovers: deletes BOTH v2Key and donePath.
  failDelete = false
  await w.backfill(['spill', 'retry'], 'auto')
  expect(h.store.get(v2Key)).toBeUndefined()
  expect(h.files.get(donePath)).toBe('')
})

test('T53-18: exit split markDone advances seq past earlier generations of any size so a 0.1.3 reader picks the new mark and delivers leftover rows', async () => {
  const h = exitReplayHarness({ maxBatchRows: 500 })
  const w014 = await h.warmWriter('s-18')
  const name = 'spill-20261013T200102003Z-t53-18.jsonl'
  const ids = Array.from({ length: 501 }, (_, i) => (70_000 + i).toString(16).padStart(32, '0'))
  const sourcePath = h.seedSpill(name, ...ids)

  // Prior generation at another size left an unexpired retired done mark (seq: 9) sorting later by writer mark.
  h.files.set(
    `${PARTITION_DIR}/outcome-done-zzzzgen001-${name}`,
    sealFile(`${JSON.stringify({ size: 222, fates: 'L', reasons: [], refusals: [], waits: false, seq: 9, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`),
  )
  h.files.set(
    `${PARTITION_DIR}/outcome-${name}`,
    sealFile(`${JSON.stringify({ size: 222, fates: 'L', reasons: [], refusals: [], waits: false, seq: 6, files: [], written: [] })}\n`),
  )

  h.setFetch(() => undefined)
  const exiting = w014.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(h.files.get(sourcePath)).toBe('')
  const child501 = [...h.files.entries()].find(([p, t]) => p.startsWith(`${PARTITION_DIR}/spill-`) && p !== sourcePath && t !== '' && parseFile(t).lines.some(l => l.row.event_id === ids[500]!))
  expect(child501).toBeDefined()
  const child501Name = child501![0].split('/').pop()!

  // Simulate released 0.1.3 doneMarks cross-size seq selection on an emptied parent (size === -1).
  const marks013 = h
    .doneMarksFor(name)
    .filter(m => m.data?.retired === true)
    .sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0))
  let picked013: (typeof marks013)[number]['data'] | undefined
  for (const m of marks013) {
    const seq = typeof m.data?.seq === 'number' ? m.data.seq : 0
    const bestSeq = typeof picked013?.seq === 'number' ? picked013.seq : 0
    if (picked013 === undefined || seq >= bestSeq) picked013 = m.data
  }
  expect(typeof picked013?.seq === 'number' ? picked013.seq : 0).toBeGreaterThan(9)
  expect(Array.isArray(picked013?.files) ? picked013.files : []).toContain(child501Name)

  h.sent.length = 0
  h.setFetch(async () => OK_INSERT)
  await h.advance(600_001)
  const rFollowup = await h.newWriter('s-18-next').backfill(['spill', 'retry'], 'auto')
  expect(rFollowup.rowsSent).toBe(1)
  expect(h.sentOf(ids[500]!)).toBe(1)
})

test('T53-19: finishing a newer parent never erases an earlier generation journal protecting a live child', async () => {
  // Subcase A: already-retired newer Gen 2 (store & file variants) preserves earlier Gen 1 live child journal across opposite slot.
  for (const storage of ['store', 'file'] as const) {
    const h = exitReplayHarness()
    const parent = `retry-20261013T190000000Z-t53-19a-${storage}.jsonl`
    const oldChildName = `spill-20261013T200000000Z-t53-19a-old-${storage}.jsonl`
    const idNew = 'a'.repeat(32)
    const idOld = 'b'.repeat(32)
    const parentPath = h.seedSpill(parent, idNew)
    const gen2Size = utf8Bytes(h.files.get(parentPath)!)
    const oldChildPath = `${PARTITION_DIR}/${oldChildName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:00:00.000Z', table: TABLE_REF, row: ownRow(idOld) })],
      parent,
    )[0]!

    // Gen 2 is already retired on disk (v2 done mark); Gen 1 (size !== gen2Size) protects live oldChild in v1 store or file.
    h.files.set(
      `${PARTITION_DIR}/outcome-done-gen2mark01-${parent}`,
      sealFile(`${JSON.stringify({ size: gen2Size, fates: 'L', reasons: [], refusals: [], waits: false, seq: 3, files: [], written: [], retired: true })}\n`),
    )
    const gen1Journal = { size: gen2Size - 32, fates: 'W', reasons: [], refusals: [], waits: true, seq: 2, files: [oldChildName], written: [true] }
    if (storage === 'store') {
      h.store.set(`replayed:${TABLE_REF}/${parent}`, gen1Journal)
    } else {
      h.files.set(`${PARTITION_DIR}/outcome-${parent}`, sealFile(`${JSON.stringify(gen1Journal)}\n`))
    }
    h.files.set(oldChildPath, oldChildText)

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter(`s-19a-${storage}`)
    const r = await w.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    expect(r.rowsSent).toBe(1)
    expect(h.sentOf(idOld)).toBe(1)
    expect(h.files.get(oldChildPath)).toBe('')
  }

  // Subcase B: Gen 2 actively writing into the SAME v2 slot when Gen 1 live child is protected only by replayed:v2:* (or outcome2-*).
  for (const storage of ['store', 'file'] as const) {
    const h = exitReplayHarness({ maxBatchRows: 1 })
    const w = await h.warmWriter(`s-19b-${storage}`)
    const parent = `retry-20261013T190000000Z-t53-19b-${storage}.jsonl`
    const oldChildName = `spill-20261013T200000000Z-t53-19b-old-${storage}.jsonl`
    const idGen2a = '1'.repeat(32)
    const idGen2b = '2'.repeat(32)
    const idOld = '3'.repeat(32)
    const parentPath = h.seedSpill(parent, idGen2a, idGen2b)
    const gen2Size = utf8Bytes(h.files.get(parentPath)!)
    const oldChildPath = `${PARTITION_DIR}/${oldChildName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:00:00.000Z', table: TABLE_REF, row: ownRow(idOld) })],
      parent,
    )[0]!

    const gen1V2Journal = { size: gen2Size - 32, fates: 'W', reasons: [], refusals: [], waits: true, seq: 2, files: [oldChildName], written: [true], uncertain: [false] }
    if (storage === 'store') {
      h.store.set(`replayed:v2:${TABLE_REF}/${parent}`, gen1V2Journal)
    } else {
      h.files.set(`${PARTITION_DIR}/outcome2-${parent}`, sealFile(`${JSON.stringify(gen1V2Journal)}\n`))
    }
    h.files.set(oldChildPath, oldChildText)

    h.setFetch(async (_url, ids) => {
      if (ids.includes(idGen2b)) return UNAVAILABLE
      return OK_INSERT
    })
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exiting

    // Deliver all remaining rows with recovery/force: zero rows may be silently truncated (all 3 rows accounted for).
    h.setFetch(async () => OK_INSERT)
    await h.advance(600_001)
    const w2 = h.newWriter(`s-19b-next-${storage}`)
    await w2.backfill(['spill', 'retry'], 'auto')
    await w2.backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(idOld)).toBeGreaterThanOrEqual(1)
    expect(h.sentOf(idGen2a)).toBeGreaterThanOrEqual(1)
    expect(h.sentOf(idGen2b)).toBeGreaterThanOrEqual(1)
  }
})

test('T53-8g: foreign pre-dispatch uncertain mark qualifies headlessNotice', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102003Z-t53-8g-unc.jsonl'
  const text = parkedFile(parkedLine(TABLE_REF, '1'.repeat(32), 'other-exit'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.files.set(
    `${PARTITION_DIR}/outcome-done-uncfor001-${name}`,
    sealFile(`${JSON.stringify({ size: utf8Bytes(text), sending: true, uncertain: true, by: 'uncfor001' })}\n`),
  )
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8g-a', resume: { id: 'p-8g-a' } })
  await world.clock.advance(2000)
  await ending
  const notice = world.stderr.find(l => l.includes('dead-letter file(s) held')) ?? ''
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-8h: live foreign lease before sending mark written qualifies headlessNotice', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102004Z-t53-8h-lease.jsonl'
  const text = parkedFile(parkedLine(TABLE_REF, '2'.repeat(32), 'other-lease'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.store.set(`lease:${TABLE_REF}/${name}`, { by: 'other-session', until: Date.now() + 600_000 })
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8h-b', resume: { id: 'p-8h-b' } })
  await world.clock.advance(2000)
  await ending
  const notice = world.stderr.find(l => l.includes('dead-letter file(s) held')) ?? ''
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-8i: torn foreign sending mark qualifies headlessNotice', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const name = 'spill-20261013T200102005Z-t53-8i-torn.jsonl'
  const text = parkedFile(parkedLine(TABLE_REF, '3'.repeat(32), 'other-torn'))
  world.files.set(`${PARTITION_DIR}/${name}`, text)
  world.files.set(`${PARTITION_DIR}/outcome-done-tornfor01-${name}`, '{"sending":true')
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'c.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8i-c', resume: { id: 'p-8i-c' } })
  await world.clock.advance(2000)
  await ending
  const notice = world.stderr.find(l => l.includes('dead-letter file(s) held')) ?? ''
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-15d: hung historical cleanup storeGet aborts at shutdown so fresh exit spill still runs', async () => {
  const h = exitReplayHarness()
  h.advanceNow(4 * 60 * 60 * 1000)
  const w = await h.warmWriter('s-15d')
  const oldName = 'spill-20261013T180000000Z-hist-hung.jsonl'
  const donePath = `${PARTITION_DIR}/outcome-done-oldmark15d-${oldName}`
  const retiredTime = h.ioFor().now() - 2 * 60 * 60 * 1000
  h.files.set(`${PARTITION_DIR}/${oldName}`, '')
  h.files.set(donePath, sealFile(`${JSON.stringify({ size: 120, fates: 'L', reasons: [], refusals: [], waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: retiredTime })}\n`))
  h.mtimes.set(`${PARTITION_DIR}/${oldName}`, retiredTime)
  h.mtimes.set(donePath, retiredTime)

  let storeLookedUp = false
  let releaseHungStore = (): void => undefined
  const hungStore = new Promise<void>(resolve => {
    releaseHungStore = resolve
  })
  h.setStoreGetHook(async key => {
    if (key === `replayed:v2:${TABLE_REF}/${oldName}`) {
      storeLookedUp = true
      await hungStore
    }
  })

  const preShutdown = w.backfill(['spill', 'retry'], 'auto')
  await h.settleMicrotasks()
  expect(storeLookedUp).toBe(true)

  const freshId = '9'.repeat(32)
  const freshPath = h.seedSpill('spill-20261013T200102003Z-fresh-15d.jsonl', freshId)
  h.setFetch(async () => OK_INSERT)
  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting
  releaseHungStore()
  await preShutdown

  expect(h.sentOf(freshId)).toBe(1)
  expect(h.files.get(freshPath)).toBe('')
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-17e: zero-byte truncated done marker beside emptied parent reclaims orphaned v2 key when no live child remains and preserves it otherwise', async () => {
  // Subcase A: live unlisted child referencing parent keeps orphaned v2 key intact even when done marker is 0 bytes.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17e-a.jsonl'
    const childName = 'retry-20261013T200102100Z-t53-17e-child.jsonl'
    const v2Key = `replayed:v2:${TABLE_REF}/${parent}`
    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(`${PARTITION_DIR}/outcome-done-truncmark1-${parent}`, '')
    h.store.set(v2Key, { size: 500, fates: 'W', reasons: [], refusals: [], waits: true, seq: 2, files: [], written: [] })
    h.files.set(
      `${PARTITION_DIR}/${childName}`,
      packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.100Z', table: TABLE_REF, row: ownRow('a'.repeat(32)) })], parent)[0]!,
    )
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-17e-a').backfill(['spill', 'retry'], 'auto')
    expect(h.store.get(v2Key)).toBeDefined()
  }

  // Subcase B: once no live child remains, 0-byte done marker beside emptied parent reclaims orphaned v2 key.
  {
    const h = exitReplayHarness()
    const parent = 'spill-20261013T200102003Z-t53-17e-b.jsonl'
    const v2Key = `replayed:v2:${TABLE_REF}/${parent}`
    h.files.set(`${PARTITION_DIR}/${parent}`, '')
    h.files.set(`${PARTITION_DIR}/outcome-done-truncmark2-${parent}`, '')
    h.store.set(v2Key, { size: 500, fates: 'L', reasons: [], refusals: [], waits: false, seq: 2, files: [], written: [] })
    await h.newWriter('s-17e-b').backfill(['spill', 'retry'], 'auto')
    expect(h.store.get(v2Key)).toBeUndefined()
  }
})

test('T53-11i: durable sending mark whose writeFile ack settles after exit still populates heldNoticeInfo', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-11i')
  const name = 'spill-20261013T200102003Z-t53-11i.jsonl'
  h.seedSpill(name, '5'.repeat(32))

  let releaseAck = (): void => undefined
  const ackGate = new Promise<void>(resolve => {
    releaseAck = resolve
  })
  h.setWriteFileHook(async (path, text) => {
    if (path.includes('/outcome-done-') && path.endsWith(`-${name}`) && text !== '') {
      // Simulate bytes durable on disk while async ack finishes past exit budget.
      h.files.set(path, text)
      await ackGate
    }
  })

  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting

  expect(h.requests.length).toBe(0)
  expect(h.doneMarksFor(name).length).toBe(1)
  expect(w.heldNoticeInfo().files).toBe(1)

  releaseAck()
  await h.settleMicrotasks()
  expect(h.uncancelledTimers()).toEqual([])
})

test('T53-14g: seedSkippedHeld preserves active foreign lease and sending mark on uncertain child regardless of parent-child sort order', async () => {
  // Subcase A: retry- child sorts before spill- parent; parent must not overwrite child's active foreign lease.
  {
    const h = exitReplayHarness()
    const firstName = 'retry-20261013T200102000Z-000-err.jsonl'
    const childName = 'retry-20261013T200102001Z-001-child.jsonl'
    const parentName = 'spill-20261013T200102002Z-002-parent.jsonl'
    h.seedSpill(firstName, '1'.repeat(32))
    const childPath = h.seedSpill(childName, '2'.repeat(32))
    const parentPath = h.seedSpill(parentName, '2'.repeat(32), '3'.repeat(32))
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session-a', until: 601_000 })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'WW',
      reasons: [],
      refusals: [],
      uncertainLines: [0],
      size: parentSize,
      waits: true,
      seq: 2,
      files: [childPath],
      written: [true],
      uncertain: [true],
    })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w0-${parentName}`,
      sealFile(`${JSON.stringify({ size: parentSize, sending: true, uncertain: true, by: 'w0' })}\n`),
    )
    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-14g-a')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(1)
    expect(w.heldNoticeInfo().mayBeActive).toBe(true)
    expect(w.heldNoticeInfo().reason ?? '').toContain('lease')
  }

  // Subcase B: parent sorts before child; child with foreign active sending mark must not be bypassed.
  {
    const h = exitReplayHarness()
    const firstName = 'spill-20261013T200102001Z-000-err.jsonl'
    const parentName = 'spill-20261013T200102002Z-001-parent.jsonl'
    const childName = 'spill-20261013T200102003Z-002-child.jsonl'
    h.seedSpill(firstName, '1'.repeat(32))
    const parentPath = h.seedSpill(parentName, '2'.repeat(32), '3'.repeat(32))
    const childPath = h.seedSpill(childName, '2'.repeat(32))
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const childSize = utf8Bytes(h.files.get(childPath)!)
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'WW',
      reasons: [],
      refusals: [],
      uncertainLines: [0],
      size: parentSize,
      waits: true,
      seq: 2,
      files: [childPath],
      written: [true],
      uncertain: [true],
    })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w0-${parentName}`,
      sealFile(`${JSON.stringify({ size: parentSize, sending: true, uncertain: true, by: 'w0' })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-otherw01-${childName}`,
      sealFile(`${JSON.stringify({ size: childSize, sending: true, by: 'otherw01' })}\n`),
    )
    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-14g-b')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(1)
    expect(w.heldNoticeInfo().mayBeActive).toBe(true)
    expect(w.heldNoticeInfo().reason ?? '').toContain('if that session stopped')
  }

  // Subcase C: parent sorts before unmarked child holding only a live foreign lease while replay is halted.
  {
    const h = exitReplayHarness()
    const firstName = 'spill-20261013T200102001Z-000-err.jsonl'
    const parentName = 'spill-20261013T200102002Z-001-parent.jsonl'
    const childName = 'spill-20261013T200102003Z-002-child.jsonl'
    h.seedSpill(firstName, '1'.repeat(32))
    const parentPath = h.seedSpill(parentName, '2'.repeat(32), '3'.repeat(32))
    const childPath = h.seedSpill(childName, '2'.repeat(32))
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'WW',
      reasons: [],
      refusals: [],
      uncertainLines: [0],
      size: parentSize,
      waits: true,
      seq: 2,
      files: [childPath],
      written: [true],
      uncertain: [true],
    })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w0-${parentName}`,
      sealFile(`${JSON.stringify({ size: parentSize, sending: true, uncertain: true, by: 'w0' })}\n`),
    )
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session-c', until: 601_000 })
    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-14g-c')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(w.heldNoticeInfo().files).toBe(1)
    expect(w.heldNoticeInfo().mayBeActive).toBe(true)
    expect(w.heldNoticeInfo().reason ?? '').toContain('lease')
  }
})

test('T53-18b: markDone carries forward live children named by earlier generations so 0.1.3 readers never drop them', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-18b-parent.jsonl'
  const refusedName = 'rejected-20261013T200102100Z-t53-18b-refused.jsonl'
  const idOld = '1'.repeat(32)
  const idNew1 = '2'.repeat(32)
  const idNew2 = '3'.repeat(32)

  // Generation 1 (0.1.3): parent retired with a live refused child on disk.
  const firstParentPath = h.seedSpill(parentName, idOld)
  const sizeGen1 = utf8Bytes(h.files.get(firstParentPath)!)
  const refusedPath = `${PARTITION_DIR}/${refusedName}`
  const refusedText = packFiles(
    [JSON.stringify({ v: 1, kind: 'rejected', reason: 'invalid', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(idOld) })],
    parentName,
  )[0]!
  h.files.set(refusedPath, refusedText)
  h.files.set(
    `${PARTITION_DIR}/outcome-done-old013w-${parentName}`,
    sealFile(`${JSON.stringify({ fates: 'R', reasons: ['invalid'], refusals: [0], size: sizeGen1, waits: false, seq: 1, files: [refusedName], written: [true], retired: true, retiredAt: 1_000 })}\n`),
  )
  h.files.set(firstParentPath, '')

  // Generation 2 (0.1.4): parent repopulated with 2 different rows and replayed cleanly (no new children).
  h.seedSpill(parentName, idNew1, idNew2)
  h.setFetch(async () => OK_INSERT)
  const w014 = h.newWriter('s-18b-014')
  const r2 = await w014.backfill(['spill', 'retry'], 'auto')
  expect(r2.rowsSent).toBe(2)

  // Highest retired mark selected by a 0.1.3 size=-1 scan must still include refusedName.
  const doneMarks = h.doneMarksFor(parentName).map(m => m.data as { seq?: number; retired?: boolean; files?: string[] } | null)
  const latestRetired = doneMarks
    .filter((d): d is { seq?: number; retired?: boolean; files?: string[] } => d !== null && d.retired === true)
    .sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0))
    .pop()
  expect(latestRetired).toBeDefined()
  expect(latestRetired!.seq ?? 0).toBeGreaterThan(1)
  expect(latestRetired!.files ?? []).toContain(refusedName)
  expect(h.files.get(refusedPath)).toBe(refusedText)
})

test('T53-16g: /bqaa-backfill force over same-size v1/v2 conflict with live marked child retires parent cleanly without resending it', async () => {
  for (const order of ['parent-first', 'child-first'] as const) {
    const h = exitReplayHarness()
    const parentName =
      order === 'parent-first'
        ? 'spill-20261013T200102001Z-t53-16g-parent.jsonl'
        : 'spill-20261013T200102003Z-t53-16g-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16g-child.jsonl'
    const idA = 'a'.repeat(32)
    const idB = 'b'.repeat(32)
    const idC = 'c'.repeat(32)
    const parentPath = h.seedSpill(parentName, idA, idB)
    const childPath = h.seedSpill(childName, idC)
    // Mark child with parent reference as writeOut creates it.
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(idC) })],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const childSize = utf8Bytes(childText)

    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-otherw-${childName}`,
      sealFile(`${JSON.stringify({ size: childSize, sending: true, by: 'otherw' })}\n`),
    )

    h.setFetch(async () => OK_INSERT)
    const w = h.newWriter(`s-16g-${order}`)
    const force1 = await w.backfill(['spill', 'retry'], 'force')
    expect(force1.error).toBeNull()
    expect(h.files.get(parentPath)).toBe('')
    expect(h.sentOf(idA)).toBe(1)
    expect(h.sentOf(idB)).toBe(1)

    const force2 = await w.backfill(['spill', 'retry'], 'force')
    expect(force2.error).toBeNull()
    expect(h.sentOf(idA)).toBe(1)
    expect(h.sentOf(idB)).toBe(1)
    expect(h.sentOf(idC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    expect(w.heldNoticeInfo().files).toBe(0)
  }
})

test('T53-8j: live foreign lease observed before a later slow markSending exit cutoff still qualifies headlessNotice', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_ACCESS_TOKEN: ACCESS_TOKEN })
  const leasedName = 'spill-20261013T200102001Z-t53-8j-leased.jsonl'
  const slowName = 'spill-20261013T200102002Z-t53-8j-slow.jsonl'
  world.files.set(`${PARTITION_DIR}/${leasedName}`, parkedFile(parkedLine(TABLE_REF, '1'.repeat(32), 'leased')))
  world.files.set(`${PARTITION_DIR}/${slowName}`, parkedFile(parkedLine(TABLE_REF, '2'.repeat(32), 'slow')))
  world.store.set(`lease:${TABLE_REF}/${leasedName}`, { by: 'other-session-8j', until: Date.now() + 600_000 })

  world.hangWrite = (p, t) => p.includes('/outcome-done-') && p.endsWith(`-${slowName}`) && t !== ''

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  await world.clock.settle()
  const ending = $.session.end({ reason: 'other', sessionId: 'p-8j', resume: { id: 'p-8j' } })
  await world.clock.advance(800)
  await ending
  world.releaseWrites()

  const notice = world.stderr.find(l => l.includes('dead-letter file(s) held')) ?? ''
  expect(notice).toMatch(/confirming that session stopped and its lease expired/i)
  expect(notice.indexOf('confirming that session stopped and its lease expired')).toBeLessThan(notice.indexOf('/bqaa-backfill force'))
})

test('T53-15e: slow v2 lookup on empty retired file never stalls live spill replay past the exit budget', async () => {
  for (const hasV1Record of [true, false] as const) {
    const h = exitReplayHarness()
    const w = await h.warmWriter('s-15e')
    const emptyName = 'spill-20261013T200102001Z-t53-15e-empty.jsonl'
    const liveName = 'spill-20261013T200102002Z-t53-15e-live.jsonl'
    const emptyPath = `${PARTITION_DIR}/${emptyName}`
    h.files.set(emptyPath, '')
    if (hasV1Record) {
      h.store.set(`replayed:${TABLE_REF}/${emptyName}`, { fates: 'L', reasons: [], refusals: [], size: 100, waits: false, seq: 1, files: [], written: [] })
    }
    const livePath = h.seedSpill(liveName, '7'.repeat(32))

    let releaseV2 = (): void => undefined
    const v2Gate = new Promise<void>(resolve => {
      releaseV2 = resolve
    })
    h.setStoreGetHook(async key => {
      if (key === `replayed:v2:${TABLE_REF}/${emptyName}`) {
        await v2Gate
        return undefined
      }
      return undefined
    })

    h.setFetch(async () => OK_INSERT)
    const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(50)
    await h.settleMicrotasks()
    // Wait long enough for live spill to post and finish while empty v2 lookup is hung.
    expect(h.sentOf('7'.repeat(32))).toBe(1)
    expect(h.files.get(livePath)).toBe('')
    releaseV2()
    await h.advance(750)
    await exiting
    expect(h.uncancelledTimers()).toEqual([])
  }
})

test('T53-18c: markDone preserves carried children across equal-size repopulations, multi-step retries, and torn store slots', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-18c-parent.jsonl'
  const refusedName = 'rejected-20261013T200102100Z-t53-18c-refused.jsonl'
  const idOld = '1'.repeat(32)
  const idNewEqualSize = '2'.repeat(32)
  const idGen3EqualSize = '3'.repeat(32)

  // Generation 1: parent retired with a live refused child at size S.
  const parentPath = h.seedSpill(parentName, idOld)
  const equalSize = utf8Bytes(h.files.get(parentPath)!)
  const refusedPath = `${PARTITION_DIR}/${refusedName}`
  const refusedText = packFiles(
    [JSON.stringify({ v: 1, kind: 'rejected', reason: 'invalid', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(idOld) })],
    parentName,
  )[0]!
  h.files.set(refusedPath, refusedText)
  h.files.set(
    `${PARTITION_DIR}/outcome-done-old013w-${parentName}`,
    sealFile(`${JSON.stringify({ fates: 'R', reasons: ['invalid'], refusals: [0], size: equalSize, waits: false, seq: 1, files: [refusedName], written: [true], retired: true, retiredAt: 1_000 })}\n`),
  )

  // Generation 2: repopulated with EXACT SAME BYTE LENGTH equalSize, replayed with force.
  h.seedSpill(parentName, idNewEqualSize)
  expect(utf8Bytes(h.files.get(parentPath)!)).toBe(equalSize)
  h.setFetch(async () => OK_INSERT)
  const w1 = h.newWriter('s-18c-eq')
  await w1.backfill(['spill', 'retry'], 'force')

  const marks1 = h.doneMarksFor(parentName).map(m => m.data as { seq?: number; retired?: boolean; files?: string[] } | null)
  const latest1 = marks1.filter((d): d is { seq?: number; retired?: boolean; files?: string[] } => d !== null && d.retired === true).sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0)).pop()
  expect(latest1?.files ?? []).toContain(refusedName)

  // Delete older Gen 1 mark; leave only Gen 2 retired mark at equalSize plus a corrupt store value, then replay Gen 3 via force and Gen 4 via auto with a torn store value.
  h.files.delete(`${PARTITION_DIR}/outcome-done-old013w-${parentName}`)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, 'torn')
  h.seedSpill(parentName, idGen3EqualSize)
  await h.newWriter('s-18c-multistep').backfill(['spill', 'retry'], 'force')

  const marks2 = h.doneMarksFor(parentName).map(m => m.data as { seq?: number; retired?: boolean; files?: string[] } | null)
  const latest2 = marks2.filter((d): d is { seq?: number; retired?: boolean; files?: string[] } => d !== null && d.retired === true).sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0)).pop()
  expect(latest2?.files ?? []).toContain(refusedName)
  expect(h.files.get(refusedPath)).toBe(refusedText)

  // Non-force auto replay with torn store record beside parent must also preserve refusedName via sawTorn.
  const parentAuto = 'spill-20261013T200102099Z-t53-18c-auto.jsonl'
  const refusedAuto = 'rejected-20261013T200102199Z-t53-18c-auto-refused.jsonl'
  h.seedSpill(parentAuto, idOld)
  h.files.set(
    `${PARTITION_DIR}/${refusedAuto}`,
    packFiles(
      [JSON.stringify({ v: 1, kind: 'rejected', reason: 'invalid', attempts: 0, failed_at: '2026-10-13T20:01:02.099Z', table: TABLE_REF, row: ownRow('4'.repeat(32)) })],
      parentAuto,
    )[0]!,
  )
  h.store.set(`replayed:v2:${TABLE_REF}/${parentAuto}`, 'torn')
  await h.newWriter('s-18c-auto').backfill(['spill', 'retry'], 'auto')
  const marksAuto = h.doneMarksFor(parentAuto).map(m => m.data as { seq?: number; retired?: boolean; files?: string[] } | null)
  const latestAuto = marksAuto.filter((d): d is { seq?: number; retired?: boolean; files?: string[] } => d !== null && d.retired === true).sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0)).pop()
  expect(latestAuto?.files ?? []).toContain(refusedAuto)
})

test('T53-16h: forced writeOut adopting a live waiting child sets waits: true even when parent rows all succeed', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-16h-parent.jsonl'
  const childName = 'retry-20261013T200102500Z-t53-16h-child.jsonl'
  const parentPath = h.seedSpill(parentName, '1'.repeat(32))
  const childPath = `${PARTITION_DIR}/${childName}`
  const childText = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.500Z', table: TABLE_REF, row: ownRow('2'.repeat(32)) })],
    parentName,
  )[0]!
  h.files.set(childPath, childText)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

  let parentOnlyCalls = 0
  h.setFetch(async (_url, ids) => {
    if (ids.includes('1'.repeat(32))) parentOnlyCalls += 1
    return ids.includes('1'.repeat(32)) ? OK_INSERT : UNAVAILABLE
  })
  const r = await h.newWriter('s-16h').backfill(['spill'], 'force')
  expect(r.error).toBeNull()
  expect(parentOnlyCalls).toBe(1)
  const doneData = h.doneMarksFor(parentName).map(m => m.data as { waits?: boolean; files?: string[]; retired?: boolean } | null).find(d => d?.retired === true)
  expect(doneData?.files ?? []).toContain(childName)
  expect(doneData?.waits).toBe(true)
})

test('T53-15f: postCutoff-aborted isRetired store lookup never marks empty files as waiting or held at exit', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-15f')
  const emptyName = 'spill-20261013T200102001Z-t53-15f-empty.jsonl'
  h.files.set(`${PARTITION_DIR}/${emptyName}`, '')
  h.store.set(`replayed:${TABLE_REF}/${emptyName}`, { fates: 'L', reasons: [], refusals: [], size: 100, waits: false, seq: 1, files: [], written: [] })

  h.setStoreGetHook(async key => {
    if (key === `replayed:${TABLE_REF}/${emptyName}`) {
      await new Promise<void>(resolve => h.ioFor().after(5_000, resolve))
    }
  })
  const exiting = w.spillForExit(800, { sendQueued: true, replaySpills: true })
  await h.advance(800)
  await exiting
  await h.advance(5_000)
  expect(w.heldNoticeInfo().files).toBe(0)
  expect(w.backfillReport?.skippedFiles).toBe(0)
  expect(w.backfillReport?.held).toBeNull()
})

test('T53-16i: restarting an interrupted forced replay with an adopted child resumes checkpoint without resending confirmed row or dropping adopted child', async () => {
  // Part A: forced replay writes durable checkpoint naming [childB, adoptedC],
  // then fails writing childB. A fresh writer must resume without clearing the
  // checkpoint, without resending A, and without dropping C.
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-16i-parent.jsonl'
  const childCName = 'retry-20261013T200102500Z-t53-16i-c.jsonl'
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const parentPath = h.seedSpill(parentName, rowA, rowB)
  const childCPath = `${PARTITION_DIR}/${childCName}`
  const childCText = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.500Z', table: TABLE_REF, row: ownRow(rowC) })],
    parentName,
  )[0]!
  h.files.set(childCPath, childCText)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  // Conflicting same-size v1 + v2 journals where v2 names live child C.
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childCName], written: [true] })

  // Server accepts A and refuses B on index 1; C succeeds when later replayed.
  h.setFetch(async (_url, ids) => {
    if (ids.includes(rowA) && ids.includes(rowB)) {
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 1, errors: [{ reason: 'invalid', message: 'bad row B' }] }] }) }
    }
    return OK_INSERT
  })
  // Fail writing the newly generated rejected-* child for B after checkpoint is durable.
  h.setWriteFileHook(async (p, text) => {
    if (p.includes('/rejected-') && text !== '') throw new Error('EIO writing child B')
  })
  await h.newWriter('s-16i-1').backfill(['spill'], 'force')
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowB)).toBe(1)
  expect(h.files.get(parentPath) !== '').toBe(true)
  expect(h.store.get(`replayed:${TABLE_REF}/${parentName}`)).toBeUndefined()
  const checkpoint = h.store.get(`replayed:v2:${TABLE_REF}/${parentName}`) as { files?: string[]; written?: boolean[]; adopted?: string[] } | undefined
  expect(checkpoint?.files ?? []).toContain(childCName)

  // Now child writes succeed; a fresh writer resumes the checkpoint cleanly.
  h.setWriteFileHook(null)
  await h.newWriter('s-16i-2').backfill(['spill', 'retry'], 'auto')
  // A must NOT be resent; parent is emptied; C was delivered (not emptied unsent); B's rejected child exists.
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowC)).toBe(1)
  expect(h.files.get(parentPath)).toBe('')
  const rejectedFiles = [...h.files.entries()].filter(([p, t]) => p.includes('/rejected-') && t !== '')
  expect(rejectedFiles.length).toBe(1)
  const retiredMark = h.doneMarksFor(parentName).map(m => m.data as { retired?: boolean; files?: string[] } | null).find(d => d?.retired === true)
  expect(retiredMark?.files ?? []).toContain(childCName)
  expect(retiredMark?.files ?? []).toContain(rejectedFiles[0]![0].split('/').pop()!)

  // Part B: legacy checkpoint without `adopted` tag that recovery cannot account for must be KEPT,
  // never silently deleted, so a subsequent /bqaa-backfill force adopts live child C2 instead of dropping it.
  const h2 = exitReplayHarness()
  const parent2 = 'spill-20261013T200102003Z-t53-16i-legacy.jsonl'
  const childB2 = 'rejected-20261013T200102600Z-t53-16i-b2.jsonl'
  const childC2 = 'retry-20261013T200102700Z-t53-16i-c2.jsonl'
  const parent2Path = h2.seedSpill(parent2, rowA, rowB)
  const parent2Size = utf8Bytes(h2.files.get(parent2Path)!)
  h2.files.set(
    `${PARTITION_DIR}/${childC2}`,
    packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.700Z', table: TABLE_REF, row: ownRow(rowC) })], parent2)[0]!,
  )
  h2.store.set(`replayed:${TABLE_REF}/${parent2}`, {
    fates: 'LR',
    reasons: ['invalid'],
    refusals: [0],
    size: parent2Size,
    waits: true,
    seq: 2,
    files: [childB2, childC2],
    written: [false, true],
  })
  const autoReport = await h2.newWriter('s-16i-legacy-auto').backfill(['spill'], 'auto')
  expect(autoReport.held).not.toBeNull()
  expect(h2.store.get(`replayed:${TABLE_REF}/${parent2}`)).toBeDefined()
  expect(h2.sentOf(rowA)).toBe(0)
  // Force replays parent2 while childC2 is still live: adopts childC2 in retired mark, then auto replay delivers C2 once.
  h2.setFetch(async () => OK_INSERT)
  await h2.newWriter('s-16i-legacy-force').backfill(['spill'], 'force')
  const retiredMark2 = h2.doneMarksFor(parent2).map(m => m.data as { retired?: boolean; files?: string[] } | null).find(d => d?.retired === true)
  expect(retiredMark2?.files ?? []).toContain(childC2)
  await h2.newWriter('s-16i-legacy-child').backfill(['spill', 'retry'], 'auto')
  expect(h2.sentOf(rowC)).toBe(1)
})

test('T53-18d: v1 rewrite with children keeps parent, sending mark, and journal when markDone cannot write a completion marker', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-18d-parent.jsonl'
  const oversizedName = 'spill-20261013T200102004Z-t53-18d-huge.jsonl'
  const rowRefused = 'd'.repeat(32)
  const parentPath = h.seedSpill(parentName, rowRefused)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  // Older Gen 1 retired completion marker (does NOT list new child) + torn completion marker + unrelated >4 MiB file.
  h.files.set(
    `${PARTITION_DIR}/outcome-done-old013w-${parentName}`,
    sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 123, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1_000 })}\n`),
  )
  h.files.set(`${PARTITION_DIR}/outcome-done-tornmark1-${parentName}`, '{"fates":"L","size":123')
  h.files.set(`${PARTITION_DIR}/${oversizedName}`, 'x'.repeat(4 * 1024 * 1024 + 1))

  h.setFetch(async () => ({
    status: 200,
    text: JSON.stringify({ insertErrors: [{ index: 0, errors: [{ reason: 'invalid', message: 'bad field' }] }] }),
  }))

  await h.newWriter('s-18d').backfill(['spill', 'retry'], 'force')

  // markDone skipped writing a new retired mark due to torn mark + >4 MiB file.
  // Parent, sending mark, and v1 journal MUST stay durable so 0.1.3 never trusts the older marker and drops the child.
  expect(h.files.get(parentPath)).not.toBe('')
  const v1Record = h.store.get(`replayed:${TABLE_REF}/${parentName}`) as { size?: number; files?: string[]; written?: boolean[] } | undefined
  expect(v1Record?.size).toBe(parentSize)
  expect((v1Record?.files ?? []).length).toBe(1)
  expect(v1Record?.written).toEqual([true])
  const sendingMarks = h.doneMarksFor(parentName).filter(m => m.complete && m.data?.sending === true)
  expect(sendingMarks.length).toBe(1)
  const newChildName = v1Record!.files![0]!
  expect((h.files.get(`${PARTITION_DIR}/${newChildName}`) ?? '').length).toBeGreaterThan(0)
})

test('T53-15g: isRetired never emits unhandledRejection when marker read finishes past exit cutoff or raced storeGet rejects', async () => {
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => {
    unhandled.push(reason)
  }
  const hostGlobals = globalThis as unknown as {
    process?: { on: (event: string, fn: (reason: unknown) => void) => void; off: (event: string, fn: (reason: unknown) => void) => void }
    setTimeout?: (fn: () => void, ms: number) => void
  }
  hostGlobals.process?.on('unhandledRejection', onUnhandled)
  const flushHostQueue = () => new Promise<void>(resolve => (hostGlobals.setTimeout ? hostGlobals.setTimeout(resolve, 0) : resolve()))
  try {
    // Subcase 1: done-mark read finishes at 780 ms (> 775 ms postCutoff) and storeGet rejects.
    const h1 = exitReplayHarness()
    const w1 = await h1.warmWriter('s-15g-1')
    const empty1 = 'spill-20261013T200102001Z-t53-15g-1.jsonl'
    const done1 = `${PARTITION_DIR}/outcome-done-nonret001-${empty1}`
    h1.files.set(`${PARTITION_DIR}/${empty1}`, '')
    h1.files.set(done1, sealFile(`${JSON.stringify({ size: 100, sending: true, by: 'nonret001' })}\n`))
    let storeGetCalls1 = 0
    h1.setReadFileHook(async p => {
      if (p === done1) await new Promise<void>(resolve => h1.ioFor().after(780, resolve))
    })
    h1.setStoreGetHook(async key => {
      if (key.includes(empty1)) {
        storeGetCalls1 += 1
        throw new Error('store rejected after cutoff')
      }
    })
    const exit1 = w1.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h1.advance(800)
    await exit1
    await flushHostQueue()
    expect(storeGetCalls1).toBe(0)
    expect(unhandled).toEqual([])

    // Subcase 2: storeGet starts BEFORE postCutoff, loses Promise.race at 775 ms, and rejects at 900 ms.
    const h2 = exitReplayHarness()
    const w2 = await h2.warmWriter('s-15g-2')
    const empty2 = 'spill-20261013T200102001Z-t53-15g-2.jsonl'
    h2.files.set(`${PARTITION_DIR}/${empty2}`, '')
    h2.setStoreGetHook(async key => {
      if (key === `replayed:${TABLE_REF}/${empty2}`) {
        await new Promise<void>(resolve => h2.ioFor().after(900, resolve))
        throw new Error('late store rejection')
      }
    })
    const exit2 = w2.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h2.advance(1_000)
    await exit2
    await flushHostQueue()
    expect(unhandled).toEqual([])
  } finally {
    hostGlobals.process?.off('unhandledRejection', onUnhandled)
  }
})

test('T53-15h: stalled live-scan isRetired v2 store lookup yields on shutdown so exit replay delivers fresh spills', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-15h')
  const emptyName = 'spill-20261013T200102001Z-t53-15h-empty.jsonl'
  const freshName = 'spill-20261013T200102999Z-t53-15h-fresh.jsonl'
  const freshId = 'f'.repeat(32)
  h.files.set(`${PARTITION_DIR}/${emptyName}`, '')

  let releaseStall = (): void => undefined
  const stall = new Promise<void>(resolve => {
    releaseStall = resolve
  })
  let v2Started = (): void => undefined
  const v2Reached = new Promise<void>(resolve => {
    v2Started = resolve
  })
  h.setStoreGetHook(async key => {
    if (key === `replayed:v2:${TABLE_REF}/${emptyName}`) {
      v2Started()
      await stall
    }
  })

  try {
    // Live scan starts and stalls on the empty file's v2 lookup.
    let liveSettled = false
    const liveScan = w.backfill(['spill', 'retry'], 'auto').then(r => {
      liveSettled = true
      return r
    })
    await v2Reached
    // Fresh spill arrives AFTER the live scan's directory listing; shutdown begins immediately.
    h.seedSpill(freshName, freshId)
    const exit = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    await h.advance(800)
    await exit
    await h.settleMicrotasks()
    expect(liveSettled).toBe(true)
    expect(h.sentOf(freshId)).toBe(1)
    expect(h.files.get(`${PARTITION_DIR}/${freshName}`)).toBe('')
    await liveScan
  } finally {
    releaseStall()
  }
})

test('T53-19b: torn-marker recovery scales linearly in source reads across 8 vs 24 torn sources', async () => {
  const runTornBatch = async (count: number): Promise<number> => {
    const h = exitReplayHarness()
    let sourceReads = 0
    for (let i = 0; i < count; i += 1) {
      const name = `spill-20261013T200102${String(i).padStart(3, '0')}Z-t53-19b-${i}.jsonl`
      const id = (i + 1).toString(16).padStart(32, '0')
      h.seedSpill(name, id)
      h.files.set(`${PARTITION_DIR}/outcome-done-tornmark${String(i).padStart(2, '0')}-${name}`, '{"fates":"L","size":10')
    }
    h.setReadFileHook(async p => {
      const base = p.split('/').pop() ?? ''
      if (base.startsWith('spill-')) sourceReads += 1
    })
    const report = await h.newWriter(`s-19b-${count}`).backfill(['spill', 'retry'], 'force')
    expect(report.error).toBeNull()
    expect(report.files).toBe(count)
    for (let i = 0; i < count; i += 1) {
      const name = `spill-20261013T200102${String(i).padStart(3, '0')}Z-t53-19b-${i}.jsonl`
      expect(h.files.get(`${PARTITION_DIR}/${name}`)).toBe('')
    }
    return sourceReads
  }

  const reads8 = await runTornBatch(8)
  const reads24 = await runTornBatch(24)
  // Quadratic scans did ~80 -> ~624 source reads (7.8x for 3x files).
  // Per-run parent indexing keeps source reads linear (~4*N: <= 3.5x ratio and <= 120 for 24 files).
  expect(reads24).toBeLessThanOrEqual(120)
  expect(reads24).toBeLessThanOrEqual(Math.ceil(reads8 * 3.5))
})

test('T53-16j: forced replay adopting a live child stores its checkpoint in versioned v2 slot so 0.1.3 never deletes it on restart', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102003Z-t53-16j-parent.jsonl'
  const childCName = 'retry-20261013T200102500Z-t53-16j-c.jsonl'
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const parentPath = h.seedSpill(parentName, rowA, rowB)
  const childCText = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.500Z', table: TABLE_REF, row: ownRow(rowC) })],
    parentName,
  )[0]!
  h.files.set(`${PARTITION_DIR}/${childCName}`, childCText)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childCName], written: [true] })

  h.setFetch(async (_url, ids) => {
    if (ids.includes(rowA) && ids.includes(rowB)) {
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: 1, errors: [{ reason: 'invalid', message: 'bad row B' }] }] }) }
    }
    return OK_INSERT
  })
  h.setWriteFileHook(async (p, text) => {
    if (p.includes('/rejected-') && text !== '') throw new Error('EIO writing child B')
  })

  await h.newWriter('s-16j-1').backfill(['spill'], 'force')
  // Checkpoint with adopted child MUST live in v2 slot (invisible to 0.1.3), v1 slot cleared, sending mark visible.
  expect(h.store.get(`replayed:${TABLE_REF}/${parentName}`)).toBeUndefined()
  const v2Checkpoint = h.store.get(`replayed:v2:${TABLE_REF}/${parentName}`) as { files?: string[]; adopted?: string[]; written?: boolean[] } | undefined
  expect(v2Checkpoint?.adopted).toEqual([childCName])
  expect(v2Checkpoint?.files ?? []).toContain(childCName)
  expect(h.doneMarksFor(parentName).some(m => m.complete && m.data?.sending === true)).toBe(true)

  // Fresh writer resumes from v2 without resending A and preserves C.
  h.setWriteFileHook(null)
  await h.newWriter('s-16j-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowC)).toBe(1)
  expect(h.files.get(parentPath)).toBe('')
})

test('T53-16k: single force run reconciles overlapping rows in adopted children without duplicate submits or losing non-overlapping rows', async () => {
  // Subcase 1: parent [A, B] + conflicting v2 child [B], in both parent-first and child-first sort orders.
  for (const order of ['parent-first', 'child-first'] as const) {
    const h = exitReplayHarness()
    const parentName =
      order === 'parent-first'
        ? 'spill-20261013T200102001Z-t53-16k-parent.jsonl'
        : 'spill-20261013T200102003Z-t53-16k-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16k-child.jsonl'
    const rowA = 'a'.repeat(32)
    const rowB = 'b'.repeat(32)
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-otherw-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(childText), sending: true, by: 'otherw' })}\n`),
    )

    h.setFetch(async () => OK_INSERT)
    const report = await h.newWriter(`s-16k-${order}`).backfill(['spill', 'retry'], 'force')
    expect(report.error).toBeNull()
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(report.rowsSent).toBe(2)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase 2: mixed child [B, C] where B landed with parent [A, B] and C did not -> sends A, B, C exactly once each.
  const hMix = exitReplayHarness()
  const parentMix = 'spill-20261013T200102001Z-t53-16k-mix-parent.jsonl'
  const childMix = 'spill-20261013T200102002Z-t53-16k-mix-child.jsonl'
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const parentMixPath = hMix.seedSpill(parentMix, rowA, rowB)
  const childMixPath = `${PARTITION_DIR}/${childMix}`
  const childMixText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
    ],
    parentMix,
  )[0]!
  hMix.files.set(childMixPath, childMixText)
  const parentMixSize = utf8Bytes(hMix.files.get(parentMixPath)!)
  hMix.store.set(`replayed:${TABLE_REF}/${parentMix}`, { fates: 'LL', reasons: [], refusals: [], size: parentMixSize, waits: false, seq: 2, files: [], written: [] })
  hMix.store.set(`replayed:v2:${TABLE_REF}/${parentMix}`, { fates: 'WW', reasons: [], refusals: [], size: parentMixSize, waits: true, seq: 9, files: [childMix], written: [true] })
  hMix.setFetch(async () => OK_INSERT)

  await hMix.newWriter('s-16k-mix-1').backfill(['spill', 'retry'], 'force')
  await hMix.newWriter('s-16k-mix-2').backfill(['spill', 'retry'], 'auto')
  expect(hMix.sentOf(rowA)).toBe(1)
  expect(hMix.sentOf(rowB)).toBe(1)
  expect(hMix.sentOf(rowC)).toBe(1)
  expect(hMix.files.get(parentMixPath)).toBe('')
  expect(hMix.files.get(childMixPath)).toBe('')
})

test('T53-15i: skipped-file held discovery after request error yields on shutdown so exit replay delivers fresh spills', async () => {
  const h = exitReplayHarness()
  const w = await h.warmWriter('s-15i')
  const file1 = 'spill-20261013T200102001Z-t53-15i-1.jsonl'
  const file2 = 'spill-20261013T200102002Z-t53-15i-2.jsonl'
  const fresh = 'spill-20261013T200102999Z-t53-15i-fresh.jsonl'
  const id1 = '1'.repeat(32)
  const id2 = '2'.repeat(32)
  const idFresh = '3'.repeat(32)
  h.seedSpill(file1, id1)
  h.seedSpill(file2, id2)

  let backendOk = false
  h.setFetch(async () => (backendOk ? OK_INSERT : UNAVAILABLE))

  let leaseStallStarted = (): void => undefined
  const leaseStallReached = new Promise<void>(resolve => {
    leaseStallStarted = resolve
  })
  let releaseLeaseStall = (): void => undefined
  const leaseStall = new Promise<void>(resolve => {
    releaseLeaseStall = resolve
  })
  let leaseHit = false
  h.setStoreGetHook(async key => {
    if (key === `lease:${TABLE_REF}/${file2}` && !leaseHit) {
      leaseHit = true
      leaseStallStarted()
      await leaseStall
    }
  })

  try {
    let liveSettled = false
    const liveScan = w.backfill(['spill', 'retry'], 'auto').then(r => {
      liveSettled = true
      return r
    })
    await leaseStallReached

    // Backend recovers and a fresh spill arrives after live listing; shutdown begins.
    backendOk = true
    h.seedSpill(fresh, idFresh)
    const exit = w.spillForExit(800, { sendQueued: true, replaySpills: true })
    // Settle microtasks at t=0 so exit replay delivers all live spills without waiting for the 800 ms timer.
    for (let i = 0; i < 6; i += 1) await h.settleMicrotasks()
    await h.advance(800)
    await exit
    expect(liveSettled).toBe(true)
    expect(h.sentOf(id1)).toBe(2)
    expect(h.sentOf(id2)).toBe(1)
    expect(h.sentOf(idFresh)).toBe(1)
    expect(h.files.get(`${PARTITION_DIR}/${fresh}`)).toBe('')
    await liveScan
  } finally {
    releaseLeaseStall()
  }
})

test('T53-16l: interrupted forced replay persists reconciled children in v2 and holds them on auto replay until parent retires', async () => {
  // Subcase 1: mixed child [B, D] (retry-* sorts before spill-*) with EIO on rejected-B write.
  const hMix = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16l-parent.jsonl'
  const childName = 'retry-20261013T200102003Z-t53-16l-child.jsonl'
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowD = 'd'.repeat(32)
  const parentPath = hMix.seedSpill(parentName, rowA, rowB)
  const childPath = `${PARTITION_DIR}/${childName}`
  const childText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowD) }),
    ],
    parentName,
  )[0]!
  hMix.files.set(childPath, childText)
  const parentSize = utf8Bytes(hMix.files.get(parentPath)!)
  hMix.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  hMix.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

  hMix.setFetch(async (_url, ids) => {
    if (ids.includes(rowB)) {
      const idx = ids.indexOf(rowB)
      return { status: 200, text: JSON.stringify({ insertErrors: [{ index: idx, errors: [{ reason: 'invalid', message: 'bad row B' }] }] }) }
    }
    return OK_INSERT
  })
  hMix.setWriteFileHook(async (p, text) => {
    if (p.includes('/rejected-') && text !== '') throw new Error('EIO writing rejected B')
  })

  await hMix.newWriter('s-16l-mix-1').backfill(['spill', 'retry'], 'force')
  expect(hMix.store.get(`replayed:${TABLE_REF}/${parentName}`)).toBeUndefined()
  const v2Mix = hMix.store.get(`replayed:v2:${TABLE_REF}/${parentName}`) as
    | { reconciled?: { name: string; size: number; fnv1a: string }[]; pendingReconcile?: { name: string; size: number; fnv1a: string }[]; adopted?: string[]; files?: string[] }
    | undefined
  expect(v2Mix?.pendingReconcile?.map(r => r.name)).toEqual([childName])
  expect(hMix.files.get(parentPath)).not.toBe('')
  expect(hMix.files.get(childPath)).not.toBe('')

  // Clear EIO: auto backfill visits retry-* before spill-*; childName MUST wait,
  // parent resumes to write rejected B and replacement [D], retires parent & childName,
  // and the next auto backfill delivers replacement [D] once without resending A or B.
  hMix.setWriteFileHook(null)
  await hMix.newWriter('s-16l-mix-2').backfill(['spill', 'retry'], 'auto')
  await hMix.newWriter('s-16l-mix-3').backfill(['spill', 'retry'], 'auto')
  expect(hMix.sentOf(rowA)).toBe(1)
  expect(hMix.sentOf(rowB)).toBe(1)
  expect(hMix.sentOf(rowD)).toBe(1)
  expect(hMix.files.get(parentPath)).toBe('')
  expect(hMix.files.get(childPath)).toBe('')
  const rejectedFiles = [...hMix.files.entries()].filter(([p, txt]) => p.includes('/rejected-') && txt !== '')
  expect(rejectedFiles.length).toBe(1)

  // Subcase 2: exact overlap [B] with fates 'LL' (files: []), interrupted by markDone failure.
  const hExact = exitReplayHarness()
  const parentExact = 'spill-20261013T200102011Z-t53-16l-exact-parent.jsonl'
  const childExact = 'retry-20261013T200102013Z-t53-16l-exact-child.jsonl'
  const parentExactPath = hExact.seedSpill(parentExact, rowA, rowB)
  const childExactPath = `${PARTITION_DIR}/${childExact}`
  const childExactText = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.013Z', table: TABLE_REF, row: ownRow(rowB) })],
    parentExact,
  )[0]!
  hExact.files.set(childExactPath, childExactText)
  const parentExactSize = utf8Bytes(hExact.files.get(parentExactPath)!)
  hExact.store.set(`replayed:${TABLE_REF}/${parentExact}`, { fates: 'LL', reasons: [], refusals: [], size: parentExactSize, waits: false, seq: 2, files: [], written: [] })
  hExact.store.set(`replayed:v2:${TABLE_REF}/${parentExact}`, { fates: 'LW', reasons: [], refusals: [], size: parentExactSize, waits: true, seq: 9, files: [childExact], written: [true] })
  hExact.setFetch(async () => OK_INSERT)
  hExact.setWriteFileHook(async (p, text) => {
    if (p.includes(`/outcome-done-`) && p.endsWith(`-${parentExact}`) && text.includes('"retired":true')) {
      throw new Error('EIO writing retired done mark')
    }
  })

  await hExact.newWriter('s-16l-exact-1').backfill(['spill', 'retry'], 'force')
  // Even with adoptedFiles empty, reconciledFiles MUST be stored in v2 with size+checksum and sending mark visible.
  expect(hExact.store.get(`replayed:${TABLE_REF}/${parentExact}`)).toBeUndefined()
  const v2Exact = hExact.store.get(`replayed:v2:${TABLE_REF}/${parentExact}`) as { reconciled?: { name: string; size: number; fnv1a: string }[]; files?: string[] } | undefined
  expect(v2Exact?.reconciled?.map(r => r.name)).toEqual([childExact])
  expect(v2Exact?.reconciled?.[0]?.size).toBe(utf8Bytes(childExactText))
  expect((v2Exact?.reconciled?.[0]?.fnv1a ?? '').length).toBe(8)
  expect(hExact.doneMarksFor(parentExact).some(m => m.complete && m.data?.sending === true)).toBe(true)
  expect(hExact.files.get(parentExactPath)).not.toBe('')
  expect(hExact.files.get(childExactPath)).not.toBe('')

  // Clear EIO: auto backfill visits retry-* before spill-*; childExact MUST wait,
  // parent resumes and retires, emptying childExact without resending rowB.
  hExact.setWriteFileHook(null)
  await hExact.newWriter('s-16l-exact-2').backfill(['spill', 'retry'], 'auto')
  expect(hExact.sentOf(rowA)).toBe(1)
  expect(hExact.sentOf(rowB)).toBe(1)
  expect(hExact.files.get(parentExactPath)).toBe('')
  expect(hExact.files.get(childExactPath)).toBe('')
})

test('T53-16m: forced reconciliation preserves distinct legacy rows without event_id even when payloads are byte-identical', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16m-parent.jsonl'
  const childName = 'retry-20261013T200102002Z-t53-16m-child.jsonl'
  const parentPath = `${PARTITION_DIR}/${parentName}`
  const childPath = `${PARTITION_DIR}/${childName}`
  const legacyRow = { ...ownRow(''), event_id: '' }
  delete (legacyRow as Record<string, unknown>).event_id
  const parentText = packFiles([
    JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: legacyRow }),
  ])[0]!
  const childText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: legacyRow, key: `${parentName}#0` }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: legacyRow, key: 'other#3' }),
    ],
    parentName,
  )[0]!
  h.files.set(parentPath, parentText)
  h.files.set(childPath, childText)
  const parentSize = utf8Bytes(parentText)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

  let totalRowsSubmitted = 0
  h.setFetch(async (_url, ids) => {
    totalRowsSubmitted += ids.length
    return OK_INSERT
  })

  await h.newWriter('s-16m-1').backfill(['spill', 'retry'], 'force')
  await h.newWriter('s-16m-2').backfill(['spill', 'retry'], 'auto')
  expect(totalRowsSubmitted).toBe(2)
  expect(h.files.get(parentPath)).toBe('')
  expect(h.files.get(childPath)).toBe('')
})

test('T53-16n: replacement write failure holds mixed child without reauthorizing already-landed rows', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16n-parent.jsonl'
  const childName = 'retry-20261013T200102002Z-t53-16n-child.jsonl'
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const parentPath = h.seedSpill(parentName, rowA, rowB)
  const childPath = `${PARTITION_DIR}/${childName}`
  const childText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
    ],
    parentName,
  )[0]!
  h.files.set(childPath, childText)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

  let parentFetched = false
  h.setFetch(async () => {
    parentFetched = true
    return OK_INSERT
  })
  let failedOnce = false
  h.setWriteFileHook(async (p, text) => {
    if (parentFetched && !failedOnce && p.includes('/retry-') && p !== childPath && text !== '') {
      failedOnce = true
      throw new Error('EIO writing replacement child')
    }
  })

  await h.newWriter('s-16n-1').backfill(['spill', 'retry'], 'force')
  expect(failedOnce).toBe(true)
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowB)).toBe(1)
  expect(h.sentOf(rowC)).toBe(0)
  expect(h.files.get(parentPath)).not.toBe('')
  expect(h.files.get(childPath)).not.toBe('')

  // Clear write failure and run automatic replays: parent resumes, writes replacement [C],
  // retires parent & child, and sends C once without ever resending A or B.
  h.setWriteFileHook(null)
  await h.newWriter('s-16n-2').backfill(['spill', 'retry'], 'auto')
  await h.newWriter('s-16n-3').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowB)).toBe(1)
  expect(h.sentOf(rowC)).toBe(1)
  expect(h.files.get(parentPath)).toBe('')
  expect(h.files.get(childPath)).toBe('')
})

test('T53-16o: forced reconciliation respects child retired delivery record and unexpired foreign lease', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase 1: child already retired (failed to truncate) — do not copy child-only row C into a replacement.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16o-ret-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16o-ret-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const childSize = utf8Bytes(childText)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    // Child's own replay already succeeded and wrote its retired marker, just failed to truncate.
    const now = h.ioFor().now()
    h.files.set(
      `${PARTITION_DIR}/outcome-done-prevw-${childName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: childSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: now })}\n`),
    )
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16o-ret-1').backfill(['spill', 'retry'], 'force')
    await h.newWriter('s-16o-ret-2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase 2a: child held by an unexpired foreign lease BEFORE force starts —
  // fail closed before dispatch (0 rows sent, 0 guards left, 0 backups/replacements);
  // after lease expires, force completes reconciliation cleanly (A=1, B=1, C=1).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102011Z-t53-16o-lease-parent.jsonl'
    const childName = 'retry-20261013T200102012Z-t53-16o-lease-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.012Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.012Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    const now = h.ioFor().now()
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: now + 5 * 60_000 })
    const standaloneName = 'spill-20261013T200102099Z-t53-16o-independent.jsonl'
    const rowS = 's'.repeat(32)
    const standalonePath = h.seedSpill(standaloneName, rowS)
    h.setFetch(async () => OK_INSERT)

    const rPre = await h.newWriter('s-16o-lease-1').backfill(['spill', 'retry'], 'force')
    expect(rPre.error).toBeNull()
    expect(rPre.held ?? '').toContain(childName)
    expect((rPre.heldFiles ?? 0) >= 2).toBe(true)
    // Independent standalone file sorting after the leased family must still be delivered.
    expect(h.sentOf(rowS)).toBe(1)
    expect(h.files.get(standalonePath)).toBe('')
    expect(h.sentOf(rowA)).toBe(0)
    expect(h.sentOf(rowB)).toBe(0)
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(childPath)).toBe(childText)
    const replacementsWhileLeased = [...h.files.entries()].filter(([p, t]) => p.includes('/retry-') && p !== childPath && t !== '')
    expect(replacementsWhileLeased.length).toBe(0)

    // After lease expires, forced replay completes reconciliation without duplicating any row.
    await h.advance(15 * 60_000)
    await h.newWriter('s-16o-lease-2').backfill(['spill', 'retry'], 'force')
    await h.newWriter('s-16o-lease-3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase 2b: child acquires a foreign lease AFTER parent stages backup & dispatches
  // (before Phase 3 classifyChild) — Phase 3 defers leased child without emptying it
  // or creating a Phase 3 replacement, and automatic replay finishes once lease expires.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102021Z-t53-16o-midlease-parent.jsonl'
    const childName = 'retry-20261013T200102022Z-t53-16o-midlease-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.022Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.022Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => {
      h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: h.ioFor().now() + 5 * 60_000 })
      return OK_INSERT
    })

    await h.newWriter('s-16o-mid-1').backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(childPath)).toBe(childText)
    // Only the pre-dispatch backup exists (no Phase 3 non-backup replacement created while leased).
    const nonBackupReplacements = [...h.files.entries()].filter(
      ([p, t]) =>
        p.includes('/retry-') &&
        p !== childPath &&
        t !== '' &&
        ![...h.files.entries()].some(([mp, mt]) => mp.includes('/outcome-done-') && mp.endsWith(`-${p.slice(p.lastIndexOf('/') + 1)}`) && mt.includes('"backup":true')),
    )
    expect(nonBackupReplacements.length).toBe(0)

    h.setFetch(async () => OK_INSERT)
    await h.advance(15 * 60_000)
    await h.newWriter('s-16o-mid-2').backfill(['spill', 'retry'], 'auto')
    await h.newWriter('s-16o-mid-3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-16p: failed v1 journal clear neutralizes v1 or completes retired done mark so stale v1 never overrides v2 reconciliation', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  for (const faultMode of ['delete-only', 'delete-and-set'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16p-${faultMode}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16p-${faultMode}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    if (faultMode === 'delete-and-set') {
      h.setStoreSetHook(async key => {
        if (key === v1Key) throw new Error('v1 storeSet rejected')
      })
    }

    await h.newWriter(`s-16p-${faultMode}-1`).backfill(['spill', 'retry'], 'force')
    // A retired done mark naming the replacement child MUST exist so both 0.1.3 and 0.1.4 ignore stale v1.
    const retiredMarks = h.doneMarksFor(parentName).filter(m => m.complete && m.data?.retired === true)
    expect(retiredMarks.length).toBeGreaterThanOrEqual(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')

    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await h.newWriter(`s-16p-${faultMode}-2`).backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
  }
})

test('T53-16q: restart validates reconciled child size and checksum before truncating so a repopulated newer generation is preserved', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowNew = 'f'.repeat(32)
  for (const sameParent of [false, true]) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16q-${sameParent ? 'p' : 's'}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16q-${sameParent ? 'p' : 's'}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    h.files.set(childPath, oldChildText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && text.includes('"retired":true')) {
        throw new Error('EIO on parent completion marker')
      }
    })

    await h.newWriter(`s-16q-${sameParent}-1`).backfill(['spill', 'retry'], 'force')
    expect(h.files.get(parentPath)).not.toBe('')

    // Repopulate childName with a brand-new sealed file holding rowNew before restart.
    const newChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:05.000Z', table: TABLE_REF, row: ownRow(rowNew) })],
      sameParent ? parentName : undefined,
    )[0]!
    h.files.set(childPath, newChildText)
    h.setWriteFileHook(null)

    await h.newWriter(`s-16q-${sameParent}-2`).backfill(['spill', 'retry'], 'auto')
    await h.newWriter(`s-16q-${sameParent}-3`).backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowNew)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-16r: retention cleanup refreshes directory listing before deleting parent outcome when a late child appears during an in-flight request', async () => {
  const h = exitReplayHarness()
  const oldParent = 'spill-20261013T100102001Z-t53-16r-old-parent.jsonl'
  const otherSpill = 'spill-20261013T200102002Z-t53-16r-other.jsonl'
  const lateChild = 'retry-20261013T200102009Z-t53-16r-late-child.jsonl'
  const rowLanded = 'e'.repeat(32)
  const rowOther = 'd'.repeat(32)

  // Old parent is empty with an expired retired record and store entry still present.
  h.files.set(`${PARTITION_DIR}/${oldParent}`, '')
  const expiredAt = h.ioFor().now() - 25 * 60 * 60 * 1000
  const oldDonePath = `${PARTITION_DIR}/outcome-done-oldw-${oldParent}`
  const oldDone = { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: expiredAt }
  h.files.set(oldDonePath, sealFile(`${JSON.stringify(oldDone)}\n`))
  h.mtimes.set(oldDonePath, expiredAt)
  h.store.set(`replayed:${TABLE_REF}/${oldParent}`, { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 1, files: [], written: [] })
  h.seedSpill(otherSpill, rowOther)

  // While otherSpill's HTTP request is in flight, a late child naming oldParent appears on disk.
  const lateChildText = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.009Z', table: TABLE_REF, row: ownRow(rowLanded) })],
    oldParent,
  )[0]!
  h.setFetch(async (_url, ids) => {
    if (ids.includes(rowOther)) {
      h.files.set(`${PARTITION_DIR}/${lateChild}`, lateChildText)
    }
    return OK_INSERT
  })

  await h.newWriter('s-16r-1').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowOther)).toBe(1)
  expect(h.sentOf(rowLanded)).toBe(0)

  // Next scan sees lateChild: oldParent's retired outcome must still exist so lateChild is dropped without resending rowLanded.
  await h.newWriter('s-16r-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowLanded)).toBe(0)
  expect(h.files.get(`${PARTITION_DIR}/${lateChild}`)).toBe('')
})

test('T53-16s: partial multi-part replacement write failure stages and holds Part 1 so rows without event_id are never duplicated', async () => {
  for (const rollbackFails of [false, true]) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16s-${rollbackFails ? 'stuck' : 'clean'}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16s-${rollbackFails ? 'stuck' : 'clean'}-child.jsonl`
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const makeUnkeyed = (tag: string) => {
      const r = ownRow('0'.repeat(32)) as Record<string, unknown>
      delete r.event_id
      r.content = JSON.stringify({ tag })
      return r as ReturnType<typeof ownRow>
    }
    const parentRow = makeUnkeyed('p-row')
    const childRow1 = makeUnkeyed('c-row-1')
    const childRow2 = makeUnkeyed('c-row-2')
    const parentText = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: parentRow }),
    ])[0]!
    // Two distinct kinds ('retry' and 'spill') force Phase 3 to write two replacement files.
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: parentRow, key: `${parentName}#0` }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow1 }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow2 }),
      ],
      parentName,
    )[0]!
    h.files.set(parentPath, parentText)
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(parentText)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

    const tagCounts = new Map<string, number>()
    const countingWriter = (session: string) => {
      const baseIo = h.ioFor()
      return new Writer(
        {
          ...baseIo,
          fetch: async (url, init) => {
            if (url.endsWith('/insertAll') && init.body) {
              const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
              for (const entry of body.rows) {
                const parsed = JSON.parse(String(entry.json.content ?? '{}')) as { tag?: string }
                if (parsed.tag) tagCounts.set(parsed.tag, (tagCounts.get(parsed.tag) ?? 0) + 1)
              }
            }
            return baseIo.fetch(url, init)
          },
        },
        () => h.config,
        () => session,
      )
    }

    // Let Part 1 replacement write succeed, fail Part 2; when rollbackFails is true, also fail rollback truncation.
    let replWrites = 0
    let firstReplPath: string | null = null
    let blockAllReplacements = false
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-')) return
      if (p === parentPath || p === childPath) return
      if (rollbackFails && text === '' && p === firstReplPath) {
        throw new Error('EIO on rollback truncate')
      }
      if (text !== '' && (p.includes('/retry-') || p.includes('/spill-'))) {
        if (blockAllReplacements) throw new Error('EIO still active on replacement write')
        replWrites += 1
        if (replWrites === 1) {
          firstReplPath = p
          return
        }
        throw new Error('EIO on second replacement part')
      }
    })

    await countingWriter(`s-16s-${rollbackFails}-1`).backfill(['spill', 'retry'], 'force')
    expect(replWrites).toBe(2)
    expect(firstReplPath).not.toBeNull()
    expect(tagCounts.get('p-row')).toBe(1)
    expect(tagCounts.get('c-row-1') ?? 0).toBe(0)
    expect(tagCounts.get('c-row-2') ?? 0).toBe(0)

    // Next auto run (even while replacement writes still fail so parent stays un-retired) must NOT deliver Part 1 early.
    blockAllReplacements = true
    await countingWriter(`s-16s-${rollbackFails}-2`).backfill(['spill', 'retry'], 'auto')
    expect(tagCounts.get('c-row-1') ?? 0).toBe(0)
    expect(tagCounts.get('c-row-2') ?? 0).toBe(0)

    // Once disk writes recover, replay finishes cleanly and every row is delivered exactly once.
    h.setWriteFileHook(null)
    await countingWriter(`s-16s-${rollbackFails}-3`).backfill(['spill', 'retry'], 'force')
    await countingWriter(`s-16s-${rollbackFails}-4`).backfill(['spill', 'retry'], 'auto')
    expect(tagCounts.get('p-row')).toBe(1)
    expect(tagCounts.get('c-row-1')).toBe(1)
    expect(tagCounts.get('c-row-2')).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-16t: child with unreadable row beside torn journal or unaccounted child journal reconciles without deadlocking under /bqaa-backfill force', async () => {
  // Subcase A: unreadable line + torn child journal
  {
    const h = exitReplayHarness()
    const rowA = 'a'.repeat(32)
    const rowC1 = 'c'.repeat(32)
    const parentName = 'spill-20261013T200102001Z-t53-16t-a-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16t-a-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childBody =
      `${JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) })}\n` +
      `${JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC1) })}\n` +
      `{broken unreadable line\n`
    const childText = sealFile(childBody, parentName)
    h.files.set(childPath, childText)
    h.files.set(`${PARTITION_DIR}/outcome-${childName}`, '{"size":') // torn child journal
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16t-a-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowC1)).toBe(0)

    // Auto holds the uncertain replacement rather than auto-replaying uncertain child rows.
    const autoAfter = await h.newWriter('s-16t-a-2').backfill(['spill', 'retry'], 'auto')
    expect(autoAfter.rowsSent).toBe(0)
    expect(h.sentOf(rowC1)).toBe(0)

    // Second force delivers rowC1 once and preserves the unreadable raw line without deadlock.
    await h.newWriter('s-16t-a-3').backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowC1)).toBe(1)
    const remainingUnreadable = [...h.files.entries()].filter(([p, txt]) => !p.includes('/outcome-') && txt.includes('{broken unreadable line'))
    expect(remainingUnreadable.length).toBe(1)
  }

  // Subcase B: unaccounted child journal (fates length mismatch, no live/written grandchildren)
  {
    const h = exitReplayHarness()
    const rowA = '1'.repeat(32)
    const rowC1 = '2'.repeat(32)
    // Parent sorts before child so parent's classifyChild directly encounters the non-fitting child journal.
    const parentName = 'spill-20261013T200102001Z-t53-16t-b-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16t-b-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC1) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    h.store.set(`replayed:${TABLE_REF}/${childName}`, {
      fates: 'LLLL',
      reasons: [],
      refusals: [],
      size: utf8Bytes(childText),
      waits: true,
      seq: 3,
      files: ['retry-20261013T200102003Z-absent.jsonl'],
      written: [false],
    })
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16t-b-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowC1)).toBe(0)

    const autoAfter = await h.newWriter('s-16t-b-2').backfill(['spill', 'retry'], 'auto')
    expect(autoAfter.rowsSent).toBe(0)

    await h.newWriter('s-16t-b-3').backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowC1)).toBe(1)
  }

  // Subcase C & D: unaccounted child journal WITH a live grandchild (both written:true and written:false,
  // with and without event_id, and with rowA overlapping parent->child->grandchild) reconciles without
  // deadlocking or duplicating parent-accounted rows.
  for (const writtenFlag of [true, false]) {
    for (const withEventId of [true, false]) {
      for (const childPrefix of ['retry', 'spill'] as const) {
        const h = exitReplayHarness()
        const tag = `${writtenFlag ? 'w' : 'u'}-${withEventId ? 'id' : 'leg'}-${childPrefix}`
        const parentName = `spill-20261013T200102001Z-t53-16t-${tag}-parent.jsonl`
        const childName = `${childPrefix}-20261013T200102002Z-t53-16t-${tag}-child.jsonl`
        const grandName = `${childPrefix}-20261013T200102003Z-t53-16t-${tag}-grand.jsonl`
        const parentPath = `${PARTITION_DIR}/${parentName}`
        const childPath = `${PARTITION_DIR}/${childName}`
        const grandPath = `${PARTITION_DIR}/${grandName}`
        const makeRow = (id: string, label: string) => {
          const r = ownRow(id) as Record<string, unknown>
          if (!withEventId) delete r.event_id
          r.content = JSON.stringify({ label })
          return r as ReturnType<typeof ownRow>
        }
        const rowA = makeRow('3'.repeat(32), 'A')
        const rowC1 = makeRow('4'.repeat(32), 'C1')
        const rowGC2 = makeRow('5'.repeat(32), 'GC2')
        const parentText = packFiles([
          JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: rowA }),
        ])[0]!
        // For writtenFlag=true, C's pre-existing committed journal already wrote GC (no rowA overlap in C's own
        // committed plan); for writtenFlag=false, C and GC legitimately carry parent's rowA alongside C1/GC2.
        const childLines = writtenFlag
          ? [JSON.stringify({ v: 1, kind: childPrefix, reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: rowC1 })]
          : [
              JSON.stringify({
                v: 1,
                kind: childPrefix,
                reason: 'retry',
                attempts: 0,
                failed_at: '2026-10-13T20:01:02.002Z',
                table: TABLE_REF,
                row: rowA,
                ...(withEventId ? {} : { key: `${parentName}#0` }),
              }),
              JSON.stringify({ v: 1, kind: childPrefix, reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: rowC1 }),
            ]
        const grandLines = writtenFlag
          ? [
              JSON.stringify({
                v: 1,
                kind: childPrefix,
                reason: 'retry',
                attempts: 0,
                failed_at: '2026-10-13T20:01:02.003Z',
                table: TABLE_REF,
                row: rowC1,
                ...(withEventId ? {} : { key: `${childName}#0` }),
              }),
              JSON.stringify({ v: 1, kind: childPrefix, reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: rowGC2 }),
            ]
          : [
              JSON.stringify({
                v: 1,
                kind: childPrefix,
                reason: 'retry',
                attempts: 0,
                failed_at: '2026-10-13T20:01:02.003Z',
                table: TABLE_REF,
                row: rowA,
                ...(withEventId ? {} : { key: `${parentName}#0` }),
              }),
              JSON.stringify({
                v: 1,
                kind: childPrefix,
                reason: 'retry',
                attempts: 0,
                failed_at: '2026-10-13T20:01:02.003Z',
                table: TABLE_REF,
                row: rowC1,
                ...(withEventId ? {} : { key: `${childName}#1` }),
              }),
              JSON.stringify({ v: 1, kind: childPrefix, reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: rowGC2 }),
            ]
        const childText = packFiles(childLines, parentName)[0]!
        const grandText = packFiles(grandLines, childName)[0]!
        h.files.set(parentPath, parentText)
        h.files.set(childPath, childText)
        h.files.set(grandPath, grandText)
        h.store.set(`replayed:${TABLE_REF}/${childName}`, {
          fates: 'LLLL',
          reasons: [],
          refusals: [],
          size: utf8Bytes(childText),
          waits: true,
          seq: 3,
          files: [grandName],
          written: [writtenFlag],
        })
        const parentSize = utf8Bytes(parentText)
        h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
        h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

        const labelCounts = new Map<string, number>()
        const countingWriter = (session: string) => {
          const baseIo = h.ioFor()
          return new Writer(
            {
              ...baseIo,
              fetch: async (url, init) => {
                if (url.endsWith('/insertAll') && init.body) {
                  const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
                  for (const entry of body.rows) {
                    const parsed = JSON.parse(String(entry.json.content ?? '{}')) as { label?: string }
                    if (parsed.label) labelCounts.set(parsed.label, (labelCounts.get(parsed.label) ?? 0) + 1)
                  }
                }
                return baseIo.fetch(url, init)
              },
            },
            () => h.config,
            () => session,
          )
        }

        await countingWriter(`s-16t-${tag}-1`).backfill(['spill', 'retry'], 'force')
        await countingWriter(`s-16t-${tag}-2`).backfill(['spill', 'retry'], 'force')
        await countingWriter(`s-16t-${tag}-3`).backfill(['spill', 'retry'], 'auto')
        expect(h.files.get(parentPath)).toBe('')
        expect(h.files.get(childPath)).toBe('')
        expect(h.files.get(grandPath)).toBe('')
        expect(labelCounts.get('A')).toBe(1)
        expect(labelCounts.get('C1')).toBe(1)
        expect(labelCounts.get('GC2')).toBe(1)
      }
    }
  }
})

test('T53-16u: failed v1 clear plus markDone failure guards 0.1.3 via v1 shadow or standalone replacement so unique rows survive', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase A: v1 delete+set fail AND outcome-done-* markDone fails, while outcome-* shadow is writable (both v1=[] and v1=[child]).
  for (const v1Shape of ['empty', 'lists-child'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16u-${v1Shape}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16u-${v1Shape}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(
      v1Key,
      v1Shape === 'empty'
        ? { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] }
        : { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 2, files: [childName], written: [true] },
    )
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && text.includes('"retired":true')) {
        throw new Error('EIO on outcome-done retired mark')
      }
    })

    await h.newWriter(`s-16u-${v1Shape}-1`).backfill(['spill', 'retry'], 'force')
    // A retired v1 shadow file (`outcome-<parent>`) MUST exist naming the replacement (not stale v1).
    const shadowRaw = h.files.get(`${PARTITION_DIR}/outcome-${parentName}`) ?? ''
    expect(shadowRaw).not.toBe('')
    const shadowParsed = parseFile(shadowRaw)
    expect(shadowParsed.complete).toBe(true)
    const shadowJson = JSON.parse(shadowParsed.unreadable[0] ?? '{}') as { retired?: boolean; files?: string[]; reconciled?: { name: string }[] }
    expect(shadowJson.retired).toBe(true)
    expect(shadowJson.files?.length).toBe(2)
    expect(shadowJson.files).toContain(childName)
    expect(shadowJson.files?.some(f => f !== childName)).toBe(true)
    expect(shadowJson.reconciled?.map(r => r.name)).toEqual([childName])

    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await h.newWriter(`s-16u-${v1Shape}-2`).backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase B: v1=[], delete+set fail AND ALL outcome* writes fail after replacement R is written -> R is standalone (parent === null).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-standalone-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-standalone-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-') && p.endsWith(parentName) && text.includes('"retired":true')) {
        throw new Error('EIO on all retired outcome writes')
      }
    })

    await h.newWriter('s-16u-standalone-1').backfill(['spill', 'retry'], 'force')
    const replEntry = [...h.files.entries()].find(
      ([p, txt]) => !p.includes('/outcome-') && p !== parentPath && p !== childPath && txt !== '',
    )
    expect(replEntry).toBeDefined()
    expect(parseFile(replEntry![1]).parent).toBeNull()
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase C: multi-part standalone replacement (v1ShadowNeeded + needsTempStageMark) where Part 2 AND rollback emptyCopy fail keeps Part 1's sending mark intact so auto never delivers Part 1 early.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-mp-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-mp-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const makeUnkeyed = (tag: string) => {
      const r = ownRow('0'.repeat(32)) as Record<string, unknown>
      delete r.event_id
      r.content = JSON.stringify({ tag })
      return r as ReturnType<typeof ownRow>
    }
    const parentRow = makeUnkeyed('p-16u-mp')
    const childRow1 = makeUnkeyed('c1-16u-mp')
    const childRow2 = makeUnkeyed('c2-16u-mp')
    const parentText = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: parentRow }),
    ])[0]!
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: parentRow, key: `${parentName}#0` }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow1 }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow2 }),
      ],
      parentName,
    )[0]!
    h.files.set(parentPath, parentText)
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(parentText)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })

    const tagCounts = new Map<string, number>()
    const countingWriter = (session: string) => {
      const baseIo = h.ioFor()
      return new Writer(
        {
          ...baseIo,
          fetch: async (url, init) => {
            if (url.endsWith('/insertAll') && init.body) {
              const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
              for (const entry of body.rows) {
                const parsed = JSON.parse(String(entry.json.content ?? '{}')) as { tag?: string }
                if (parsed.tag) tagCounts.set(parsed.tag, (tagCounts.get(parsed.tag) ?? 0) + 1)
              }
            }
            return baseIo.fetch(url, init)
          },
        },
        () => h.config,
        () => session,
      )
    }

    let replWrites = 0
    let firstReplPath: string | null = null
    let blockRepls = false
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome')) return
      if (p === parentPath || p === childPath) return
      if (text === '' && p === firstReplPath) throw new Error('EIO on rollback truncate')
      if (text !== '' && (p.includes('/retry-') || p.includes('/spill-'))) {
        if (blockRepls) throw new Error('EIO on replacement')
        replWrites += 1
        if (replWrites === 1) {
          firstReplPath = p
          return
        }
        throw new Error('EIO on part 2')
      }
    })

    await countingWriter('s-16u-mp-1').backfill(['spill', 'retry'], 'force')
    expect(replWrites).toBe(2)
    expect(firstReplPath).not.toBeNull()
    expect(h.files.get(firstReplPath!)).not.toBe('')

    // Auto run must hold standalone Part 1 via its preserved sending mark.
    blockRepls = true
    await countingWriter('s-16u-mp-2').backfill(['spill', 'retry'], 'auto')
    expect(tagCounts.get('c1-16u-mp') ?? 0).toBe(0)

    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await countingWriter('s-16u-mp-3').backfill(['spill', 'retry'], 'force')
    await countingWriter('s-16u-mp-4').backfill(['spill', 'retry'], 'auto')
    expect(tagCounts.get('p-16u-mp')).toBe(1)
    expect(tagCounts.get('c1-16u-mp')).toBe(1)
    expect(tagCounts.get('c2-16u-mp')).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase D: v1 shadow carrying earlier-generation carriedChildren must not trigger pickJournalSlots 'conflict' on subsequent 0.1.4 auto replay.
  {
    const h = exitReplayHarness()
    const rowOld = 'd'.repeat(32)
    const parentName = 'spill-20261013T200102001Z-t53-16u-carried-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-carried-child.jsonl'
    const oldChildName = 'retry-20261013T200101000Z-t53-16u-old-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldChildPath = `${PARTITION_DIR}/${oldChildName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:01.000Z', table: TABLE_REF, row: ownRow(rowOld) })],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    h.files.set(oldChildPath, oldChildText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    // Prior different-size retired mark naming oldChildName forces markDone to carry oldChildName into the retired v1 shadow.
    h.files.set(
      `${PARTITION_DIR}/outcome-done-prevgen-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'W', reasons: [], refusals: [], size: parentSize + 100, waits: true, seq: 1, files: [oldChildName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && text.includes('"retired":true')) {
        throw new Error('EIO on outcome-done retired mark')
      }
    })

    await h.newWriter('s-16u-carried-1').backfill(['spill', 'retry'], 'force')

    // Subsequent auto run must retire parent cleanly without reporting 'conflict' and deliver all 4 distinct rows 1x each.
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    const autoReport = await h.newWriter('s-16u-carried-2').backfill(['spill', 'retry'], 'auto')
    expect(autoReport.held ?? '').not.toMatch(/conflicting replay records/i)
    await h.newWriter('s-16u-carried-3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.sentOf(rowOld)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(oldChildPath)).toBe('')
  }

  // Subcase E: stuck v1 (`empty` or `lists-child`) + all retired outcomes failing + parent leftover R_p (503 on rowB) writes R_p and R_c standalone, empties C, and delivers A/B/C 1x each.
  for (const v1Shape of ['empty', 'lists-child'] as const) {
    const h = exitReplayHarness({ maxBatchRows: 1 })
    const parentName = `spill-20261013T200102001Z-t53-16u-lp-${v1Shape}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16u-lp-${v1Shape}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(
      v1Key,
      v1Shape === 'empty'
        ? { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] }
        : { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 2, files: [childName], written: [true] },
    )
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async (_url, ids) => (ids.includes(rowB) ? UNAVAILABLE : OK_INSERT))
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-') && p.endsWith(parentName) && text.includes('"retired":true')) {
        throw new Error('EIO on all retired outcome writes')
      }
    })

    const w1 = h.newWriter(`s-16u-lp-${v1Shape}-1`)
    await w1.backfill(['spill', 'retry'], 'force')
    const standaloneFiles = [...h.files.entries()].filter(
      ([p, txt]) => !p.includes('/outcome-') && p !== parentPath && p !== childPath && txt !== '',
    )
    expect(standaloneFiles.length).toBe(2)
    for (const [, txt] of standaloneFiles) {
      expect(parseFile(txt).parent).toBeNull()
    }
    expect(h.files.get(childPath)).toBe('')

    // Recover disk, BQ, and v1 store: a brand-new writer instance (cross-session restart) MUST resume v2 via supersedesV1 without 'conflict', retire parent, and deliver R_p (rowB) and R_c (rowC) 1x each.
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    h.setFetch(async () => OK_INSERT)
    const restartReport = await h.newWriter(`s-16u-lp-${v1Shape}-2`).backfill(['spill', 'retry'], 'auto')
    expect(restartReport.held ?? '').not.toMatch(/conflicting|no outcome|sending/i)
    await h.newWriter(`s-16u-lp-${v1Shape}-3`).backfill(['spill', 'retry'], 'auto')
    expect(h.sent.filter(s => s.id === rowA && s.status === 200).length).toBe(1)
    expect(h.sent.filter(s => s.id === rowB && s.status === 200).length).toBe(1)
    expect(h.sent.filter(s => s.id === rowC && s.status === 200).length).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase F: failed-keep where emptyCopy(child) ALSO fails leaves non-uncertain stagedFor guard on R_c; after restart and recovery, a new writer retires P, empties C, and delivers R_c on auto without stranding it under a false uncertain error.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-stagedguard-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-stagedguard-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 2, files: [childName], written: [true] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-') && p.endsWith(parentName) && text.includes('"retired":true')) {
        throw new Error('EIO on all retired outcome writes')
      }
      if (p === childPath && text === '') {
        throw new Error('EIO emptying child')
      }
    })

    await h.newWriter('s-16u-sg-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(childPath)).toBe(childText)
    const replEntry = [...h.files.entries()].find(
      ([p, txt]) => !p.includes('/outcome-') && p !== parentPath && p !== childPath && txt !== '',
    )
    expect(replEntry).toBeDefined()
    expect(parseFile(replEntry![1]).parent).toBeNull()

    // New writer after recovery MUST retire P, empty C, and deliver R_c automatically with zero held error.
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    const autoReport = await h.newWriter('s-16u-sg-2').backfill(['spill', 'retry'], 'auto')
    expect(autoReport.held ?? '').not.toMatch(/conflicting|no outcome|sending/i)
    await h.newWriter('s-16u-sg-3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(replEntry![0])).toBe('')
  }

  // Subcase G: v2 store quota fallback during Phase 3 promotion leaves stale stagedFiles=[R_c] in $.store[v2Key] beside newer outcome2- (seq+1) and retired outcome-done; stagedGuardState checks allRetired & highest-seq v2 first so R_c delivers on auto without deadlocking.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-quota-sg-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-quota-sg-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 2, files: [childName], written: [true] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    let blockedPromotionStore = false
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
      if (key === v2Key) {
        const cur = h.store.get(v2Key) as { stagedFiles?: string[] } | undefined
        if (Array.isArray(cur?.stagedFiles) && cur.stagedFiles.length > 0) {
          blockedPromotionStore = true
          throw new Error('v2 store quota exceeded on promotion')
        }
      }
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-') && p.endsWith(parentName) && text.includes('"retired":true')) {
        throw new Error('EIO on all retired outcome writes')
      }
      if (p === childPath && text === '') throw new Error('EIO emptying child')
    })

    await h.newWriter('s-16u-qsg-1').backfill(['spill', 'retry'], 'force')
    expect(blockedPromotionStore).toBe(true)
    const staleStored = h.store.get(v2Key) as { stagedFiles?: string[] } | undefined
    expect((staleStored?.stagedFiles ?? []).length).toBe(1)

    // Recover disk & store: new session MUST retire P and deliver R_c on auto (NOT deadlock on stale v2Key.stagedFiles).
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await h.newWriter('s-16u-qsg-2').backfill(['spill', 'retry'], 'auto')
    await h.newWriter('s-16u-qsg-3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase H: uncertain multi-part standalone replacement under v1ShadowNeeded() also stamps stagedFor so force defers Part 1 behind parent cleanup and never duplicates rows.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-ump-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-ump-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const makeUnkeyed = (tag: string) => {
      const r = ownRow('0'.repeat(32)) as Record<string, unknown>
      delete r.event_id
      r.content = JSON.stringify({ tag })
      return r as ReturnType<typeof ownRow>
    }
    const parentRow = makeUnkeyed('p-16u-ump')
    const childRow1 = makeUnkeyed('c1-16u-ump')
    const childRow2 = makeUnkeyed('c2-16u-ump')
    const parentText = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: parentRow }),
    ])[0]!
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: parentRow, key: `${parentName}#0` }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow1 }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow2 }),
      ],
      parentName,
    )[0]!
    h.files.set(parentPath, parentText)
    h.files.set(childPath, childText)
    // Mark child with a foreign uncertain sending mark so Phase 3 sets decision.markUncertain === true.
    h.files.set(
      `${PARTITION_DIR}/outcome-done-foreignu-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(childText), sending: true, uncertain: true, by: 'foreignu' })}\n`),
    )
    const parentSize = utf8Bytes(parentText)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
    })
    const tagCounts = new Map<string, number>()
    const countingWriter = (session: string) => {
      const baseIo = h.ioFor()
      return new Writer(
        {
          ...baseIo,
          fetch: async (url, init) => {
            if (url.endsWith('/insertAll') && init.body) {
              const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
              for (const entry of body.rows) {
                const parsed = JSON.parse(String(entry.json.content ?? '{}')) as { tag?: string }
                if (parsed.tag) tagCounts.set(parsed.tag, (tagCounts.get(parsed.tag) ?? 0) + 1)
              }
            }
            return baseIo.fetch(url, init)
          },
        },
        () => h.config,
        () => session,
      )
    }
    let replWrites = 0
    let firstReplPath: string | null = null
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome')) return
      if (p === parentPath || p === childPath) return
      if (text === '' && p === firstReplPath) throw new Error('EIO on rollback truncate')
      if (text !== '' && (p.includes('/retry-') || p.includes('/spill-'))) {
        replWrites += 1
        if (replWrites === 1) {
          firstReplPath = p
          return
        }
        throw new Error('EIO on part 2')
      }
    })

    await countingWriter('s-16u-ump-1').backfill(['spill', 'retry'], 'force')
    expect(replWrites).toBe(2)
    expect(firstReplPath).not.toBeNull()
    expect(h.files.get(firstReplPath!)).not.toBe('')

    // Recover and run force: Part 1 MUST defer behind parent cleanup (not send early and duplicate c1-16u-ump).
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await countingWriter('s-16u-ump-2').backfill(['spill', 'retry'], 'force')
    await countingWriter('s-16u-ump-3').backfill(['spill', 'retry'], 'force')
    expect(tagCounts.get('p-16u-ump')).toBe(1)
    expect(tagCounts.get('c1-16u-ump')).toBe(1)
    expect(tagCounts.get('c2-16u-ump')).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase I: parentFate must NOT let an older retired v1 shadow (`seq: 11, files: [R_1]`) override a newer higher-seq v2 checkpoint (`seq: 15, files: [R_2]`) and drop R_2.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-newer-v2-parent.jsonl'
    const childR1 = 'retry-20261013T200102002Z-t53-16u-r1.jsonl'
    const childR2 = 'retry-20261013T200102003Z-t53-16u-r2.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childR2Path = `${PARTITION_DIR}/${childR2}`
    h.files.set(parentPath, '')
    h.files.set(
      childR2Path,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: 512, waits: true, seq: 11, files: [childR1], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: 512,
      waits: true,
      seq: 15,
      files: [childR2],
      written: [true],
    })
    h.setFetch(async () => OK_INSERT)

    const report = await h.newWriter('s-16u-newerv2-1').backfill(['spill', 'retry'], 'auto')
    expect(report.held ?? '').toMatch(/conflicting replay records/i)
    expect(h.files.get(childR2Path)).not.toBe('')
    await h.newWriter('s-16u-newerv2-2').backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childR2Path)).toBe('')
  }

  // Subcase J: uncertain + stagedFor aborted stage R1 survives failed cleanupStaged until P retires with R2 and deletes v2; subsequent force drops redundant R1 via stagedGuardState instead of duplicating rowC.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-uncstage-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-uncstage-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    // Foreign uncertain mark on child triggers decision.markUncertain === true.
    h.files.set(
      `${PARTITION_DIR}/outcome-done-fmark-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(childText), sending: true, uncertain: true, by: 'fmark' })}\n`),
    )
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 storeDelete rejected')
    })
    let failPromote = true
    let r1Path: string | null = null
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 storeSet rejected')
      if (failPromote && key === v2Key) {
        const cur = h.store.get(v2Key) as { stagedFiles?: string[] } | undefined
        if (Array.isArray(cur?.stagedFiles) && cur.stagedFiles.length > 0) {
          throw new Error('v2 storeSet failed on promote')
        }
      }
    })
    h.setWriteFileHook(async (p, text) => {
      if (failPromote && p.includes(`/outcome2-`) && p.endsWith(parentName)) {
        throw new Error('EIO on outcome2 fallback during promote')
      }
      if (!p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '' && r1Path === null) {
        r1Path = p
      }
      if (r1Path !== null && p === r1Path && text === '') {
        throw new Error('EIO emptying aborted R1')
      }
    })

    // Step 1: P writes uncertain standalone staged R1, fails promote record().
    await h.newWriter('s-16u-j-1').backfill(['spill', 'retry'], 'force')
    expect(r1Path).not.toBeNull()
    expect(h.files.get(r1Path!)).not.toBe('')

    // Step 2: Promote unblocks, but emptying R1 still fails EIO; P reconciles C into R2, retires P, empties P & C, and deletes v2Key.
    failPromote = false
    h.setStoreDeleteHook(null)
    await h.newWriter('s-16u-j-2').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(r1Path!)).not.toBe('')

    // Step 3: EIO clears; force MUST drop redundant R1 via stagedGuardState and deliver rowC only once (from R2).
    h.setWriteFileHook(null)
    h.setStoreSetHook(null)
    await h.newWriter('s-16u-j-3').backfill(['spill', 'retry'], 'force')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(r1Path!)).toBe('')
  }

  // Subcase K: parentFate resolves Phase-3 v2 files expansion ([] -> [repl]) across store/file and outcome2-/outcome2-b- alongside a retired v1 shadow without reporting conflict.
  for (const mode of ['store-file', 'two-files'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16u-v2exp-${mode}-parent.jsonl`
    const replName = `retry-20261013T200102003Z-t53-16u-v2exp-${mode}-repl.jsonl`
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const replPath = `${PARTITION_DIR}/${replName}`
    h.files.set(parentPath, '')
    h.files.set(
      replPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, waits: true, seq: 6, files: [replName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    const olderV2 = { fates: 'LL', reasons: [], refusals: [], size: 512, waits: true, seq: 2, files: [], written: [] }
    const newerV2 = { fates: 'LL', reasons: [], refusals: [], size: 512, waits: true, seq: 5, files: [replName], written: [true] }
    if (mode === 'store-file') {
      h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, olderV2)
      h.files.set(`${PARTITION_DIR}/outcome2-${parentName}`, sealFile(`${JSON.stringify(newerV2)}\n`))
    } else {
      h.files.set(`${PARTITION_DIR}/outcome2-${parentName}`, sealFile(`${JSON.stringify(olderV2)}\n`))
      h.files.set(`${PARTITION_DIR}/outcome2-b-${parentName}`, sealFile(`${JSON.stringify(newerV2)}\n`))
    }
    h.setFetch(async () => OK_INSERT)

    const report = await h.newWriter(`s-16u-k-${mode}`).backfill(['spill', 'retry'], 'auto')
    expect(report.held ?? '').not.toMatch(/conflicting replay records/i)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(replPath)).toBe('')
  }

  // Subcase L: failed-keep empties C while R_c retains stagedFor (e.g. clearTempMarks EIO); a subsequent 0.1.3 retirement naming already-empty C (no retired done mark on C) must NOT cause stagedGuardState to drop R_c.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-l-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-l-child.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16u-l-repl.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const replPath = `${PARTITION_DIR}/${replName}`
    const replText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, '')
    h.files.set(replPath, replText)
    // R_c keeps stagedFor: P; P retired with outcome-done-013 naming already-empty C (no done mark on C).
    h.files.set(
      `${PARTITION_DIR}/outcome-done-s1-${replName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 's1', stagedFor: parentName })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: 512, waits: true, seq: 3, files: [childName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: true,
      seq: 10,
      files: [replName],
      written: [true],
      adopted: [replName],
      reconciled: [{ name: childName, size: 300, fnv1a: '12345678' }],
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-l-1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(replPath)).toBe('')
  }

  // Subcase M: older parent size generation retired with `reconciled` must NOT cause stagedGuardState to drop an unwritten v2.files leftover R_p of a newer parent size generation.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-m-parent.jsonl'
    const oldChildName = 'retry-20261013T200101000Z-t53-16u-m-oldchild.jsonl'
    const rpName = 'retry-20261013T200102003Z-t53-16u-m-rp.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const rpPath = `${PARTITION_DIR}/${rpName}`
    const rpText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    h.files.set(parentPath, '')
    h.files.set(rpPath, rpText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-s1-${rpName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(rpText), sending: true, by: 's1', stagedFor: parentName })}\n`),
    )
    // Prior generation (size 999) retired with reconciled; current generation (size 1167) retired by 0.1.3 with v1=[] while v2 still had written: [false] for R_p.
    h.files.set(
      `${PARTITION_DIR}/outcome-done-oldgen-${parentName}`,
      sealFile(
        `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 999, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 500, reconciled: [{ name: oldChildName, size: 200, fnv1a: 'abcdef01' }] })}\n`,
      ),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-013cur-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 1167, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: 1167,
      waits: true,
      seq: 10,
      files: [rpName],
      written: [false],
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-m-1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.files.get(rpPath)).toBe('')
  }

  // Subcase N: stagedGuardState must NOT drop R_c on a stale-size or torn child done mark, and must NOT drop a staged replacement when v1/v2 companion slots are torn.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-n-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-n-child.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16u-n-repl.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const replPath = `${PARTITION_DIR}/${replName}`
    const replText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, '')
    h.files.set(replPath, replText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-s1-${replName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 's1', stagedFor: parentName })}\n`),
    )
    // Stale earlier-generation done mark on child (size 200 != reconciled size 300).
    h.files.set(
      `${PARTITION_DIR}/outcome-done-oldgen-${childName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 500 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: 512, waits: true, seq: 3, files: [childName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: true,
      seq: 10,
      files: [replName],
      written: [true],
      adopted: [replName],
      reconciled: [{ name: childName, size: 300, fnv1a: '12345678' }],
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-n-1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(replPath)).toBe('')

    // Part 2: torn v2 companion (`outcome2-`) beside an older retired reconciled done mark must block `'drop'` in stagedGuardState.
    const h2 = exitReplayHarness()
    const p2 = 'spill-20261013T200102001Z-t53-16u-n2-parent.jsonl'
    const c2 = 'retry-20261013T200102002Z-t53-16u-n2-oldchild.jsonl'
    const r2 = 'retry-20261013T200102003Z-t53-16u-n2-repl.jsonl'
    const r2Path = `${PARTITION_DIR}/${r2}`
    const r2Text = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    h2.files.set(`${PARTITION_DIR}/${p2}`, '')
    h2.files.set(r2Path, r2Text)
    h2.files.set(
      `${PARTITION_DIR}/outcome-done-s1-${r2}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(r2Text), sending: true, by: 's1', stagedFor: p2 })}\n`),
    )
    h2.files.set(
      `${PARTITION_DIR}/outcome-done-oldgen-${p2}`,
      sealFile(
        `${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 999, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 500, reconciled: [{ name: c2, size: 200, fnv1a: 'abcdef01' }] })}\n`,
      ),
    )
    // Torn v2 companion (no seal / truncated JSON) prevents dropping r2.
    h2.files.set(`${PARTITION_DIR}/outcome2-${p2}`, '{"fates":"LW","size":1200,"files":["' + r2)
    h2.setFetch(async () => OK_INSERT)

    await h2.newWriter('s-16u-n-2').backfill(['spill', 'retry'], 'auto')
    expect(h2.files.get(r2Path)).toBe(r2Text)
    expect(h2.sentOf(rowB)).toBe(0)
    // Operator force can still recover r2 because it was preserved rather than truncated.
    await h2.newWriter('s-16u-n-3').backfill(['spill', 'retry'], 'force')
    expect(h2.sentOf(rowB)).toBe(1)
    expect(h2.files.get(r2Path)).toBe('')
  }

  // Subcase O: forced replay places 0.1.3-honored `childGuard` sending marks on BOTH adopted and deferred/mixed children BEFORE dispatching rows, and fails closed when a childGuard write fails.
  {
    const rowD = 'd'.repeat(32)
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-o-parent.jsonl'
    const child1Name = 'retry-20261013T200102002Z-t53-16u-o-child1.jsonl'
    const child2Name = 'retry-20261013T200102003Z-t53-16u-o-child2.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const child1Path = `${PARTITION_DIR}/${child1Name}`
    const child2Path = `${PARTITION_DIR}/${child2Name}`
    // child1 has zero overlap with parent (adopted unmodified); child2 overlaps rowB and has an active foreign lease (deferred in pendingReconcile).
    const child1Text = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    const child2Text = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowD) }),
      ],
      parentName,
    )[0]!
    h.files.set(child1Path, child1Text)
    h.files.set(child2Path, child2Text)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 2, files: [child1Name], written: [true] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 4, files: [child1Name, child2Name], written: [true, true] })
    // Acquire a foreign lease on child2 once parent dispatches so pre-dispatch guards
    // and backups are staged cleanly while Phase 3 defers child2.
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 delete blocked')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 set blocked')
    })
    h.setFetch(async () => {
      h.store.set(`lease:${TABLE_REF}/${child2Name}`, { by: 'other-session', until: h.ioFor().now() + 60_000 })
      return OK_INSERT
    })

    // First verify fail-closed: if placing child2's guard mark fails, zero rows are sent and parent stays untouched.
    let failGuard = true
    h.setWriteFileHook(async p => {
      if (failGuard && p.includes('outcome-done-') && p.endsWith(`-${child2Name}`)) throw new Error('guard write EIO')
    })
    const failReport = await h.newWriter('s-16u-o-0').backfill(['spill', 'retry'], 'force')
    expect(failReport.error).not.toBeNull()
    expect(h.sentOf(rowA)).toBe(0)
    expect(h.sentOf(rowB)).toBe(0)
    // Newly placed child1 guard was rolled back on failure.
    const child1GuardsAfterFail = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${child1Name}`) && v !== '')
    expect(child1GuardsAfterFail.length).toBe(0)

    // Allow childGuard writes: BOTH child1 and child2 receive 0.1.3-honored `childGuard: true` sending marks before dispatch!
    failGuard = false
    h.setWriteFileHook(null)
    await h.newWriter('s-16u-o-1').backfill(['spill', 'retry'], 'force')
    for (const [childName, childText] of [
      [child1Name, child1Text],
      [child2Name, child2Text],
    ] as const) {
      const guards = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${childName}`) && v !== '')
      expect(guards.length).toBe(1)
      const parsed = JSON.parse(guards[0]![1].split('\n')[0]!) as {
        size?: number
        sending?: boolean
        stagedFor?: string
        stagedForSize?: number
        childGuard?: boolean
      }
      expect(parsed.sending).toBe(true)
      expect(parsed.childGuard).toBe(true)
      expect(parsed.size).toBe(utf8Bytes(childText))
      expect(parsed.stagedFor).toBe(parentName)
      expect(parsed.stagedForSize).toBe(parentSize)
    }
  }

  // Subcase P: all-adopt child with stuck v1=[] + failed-keep:
  // - P1: Phase 3 materializes a standalone replacement (`parent: null`) and empties childName so even 0.1.3 `force` delivers rowC once.
  // - P2: when Phase 3 replacement write throws EIO, childGuard stays intact on childName so stale 0.1.3 retirement (`files: []`) cannot strand it, and 0.1.4 auto delivers rowC.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-p1-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-p1-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 8, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 delete blocked')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 set blocked')
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.endsWith(parentName) && text.includes('"retired":true')) throw new Error('all retired writes fail')
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-p1-1').backfill(['spill', 'retry'], 'force')
    // Phase 3 materialized standalone replacement R_c (parent: null) and failed-keep emptied childPath.
    expect(h.files.get(childPath)).toBe('')
    const standaloneRepl = [...h.files.entries()].find(
      ([k, v]) => k.startsWith(`${PARTITION_DIR}/retry-`) && k !== childPath && v !== '',
    )
    expect(standaloneRepl).toBeDefined()
    expect(parseFile(standaloneRepl![1]).parent).toBeNull()

    // Even after 0.1.3 retires parent with stale `files: []`, standalone replacement delivers rowC once.
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(parentPath, '')
    h.mtimes.set(childPath, 1_000)
    h.advanceNow(11 * 60_000 + 2_000)
    const r2 = await h.newWriter('s-16u-p1-2').backfill(['spill', 'retry'], 'auto')
    expect(r2.held).toBeNull()
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(standaloneRepl![0])).toBe('')
  }
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-p2-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-p2-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 8, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 delete blocked')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 set blocked')
    })
    // Block Phase 3 replacement writes (after parent HTTP fetch) so childName stays live in pendingReconcile with its childGuard intact.
    let parentFetched = false
    h.setWriteFileHook(async (p, text) => {
      if (parentFetched && !p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '') throw new Error('replacement write EIO')
    })
    h.setFetch(async () => {
      parentFetched = true
      return OK_INSERT
    })

    await h.newWriter('s-16u-p2-1').backfill(['spill', 'retry'], 'force')
    const guards = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${childName}`) && v !== '')
    expect(guards.length).toBe(1)

    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(parentPath, '')
    const r2 = await h.newWriter('s-16u-p2-2').backfill(['spill', 'retry'], 'auto')
    expect(r2.held).toBeNull()
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase Q: foreign non-uncertain stagedFor mark alongside this writer's own leftover sending mark clears consistently in backfillRun, seedSkippedHeld, and markSending.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-q-parent.jsonl'
    const earlyName = 'retry-20261013T200102000Z-t53-16u-q-early.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16u-q-repl.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const earlyPath = `${PARTITION_DIR}/${earlyName}`
    const replPath = `${PARTITION_DIR}/${replName}`
    const replText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(parentPath, '')
    h.files.set(replPath, replText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-s1-${replName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 's1', stagedFor: parentName })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-p1-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, waits: true, seq: 3, files: [replName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    const w = h.newWriter('s-16u-q-1')
    // Run 1: w writes its own sending mark on replName, gets 503, and unmark fails -> own mark stays beside foreign stagedFor mark.
    let failUnmark = true
    h.setWriteFileHook(async (targetPath, text) => {
      if (failUnmark && text === '' && targetPath.includes(`-${replName}`) && targetPath.includes('outcome-done-')) {
        throw new Error('unmark EIO')
      }
    })
    h.setFetch(async () => ({ status: 503, text: 'unavailable' }))
    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(replPath)).toBe(replText)
    expect(h.sentOf(rowC)).toBe(1)
    const marksOnRepl = [...h.files.entries()].filter(([k, v]) => k.includes(`-${replName}`) && k.includes('outcome-done-') && v !== '')
    expect(marksOnRepl.length).toBe(2)

    // Run 2: seedSkippedHeld (when an earlier file hits 503 and stops the scan before replName) must NOT falsely hold replName.
    failUnmark = false
    const earlyText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.000Z', table: TABLE_REF, row: ownRow(rowA) }),
    ])[0]!
    h.files.set(earlyPath, earlyText)
    const rSkip = await w.backfill(['spill', 'retry'], 'auto')
    expect(rSkip.held).toBeNull()
    expect(rSkip.heldFiles).toBe(0)
    expect(h.sentOf(rowC)).toBe(1)

    // Run 3: remove earlyName and restore HTTP 200 -> w delivers replName cleanly without false hold or conflict.
    h.files.set(earlyPath, '')
    h.mtimes.set(earlyPath, 1_000)
    h.advanceNow(11 * 60_000 + 2_000)
    h.setFetch(async () => OK_INSERT)
    const rDeliver = await w.backfill(['spill', 'retry'], 'auto')
    expect(rDeliver.held).toBeNull()
    expect(h.sentOf(rowC)).toBe(2)
    expect(h.files.get(replPath)).toBe('')
  }

  // Subcase R: emptied parent beside stale v1 (`files: []`) and unretired v2 (`files: []`, `pendingReconcile: [C]`, `supersedesV1`) must NOT authorize parentFate `'drop'` on live child C.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-r-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-r-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, childText)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: false,
      seq: 2,
      files: [],
      written: [],
    })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: true,
      seq: 10,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: utf8Bytes(childText), fnv1a: '' }],
      supersedesV1: { size: 512, seq: 2, fates: 'LL', files: [] },
    })
    h.setFetch(async () => OK_INSERT)

    const rAuto = await h.newWriter('s-16u-r-1').backfill(['spill', 'retry'], 'auto')
    expect(rAuto.held ?? '').toContain('preserved')
    expect(h.files.get(childPath)).toBe(childText)
    expect(h.sentOf(rowC)).toBe(0)

    const rForce = await h.newWriter('s-16u-r-2').backfill(['spill', 'retry'], 'force')
    expect(rForce.held).toBeNull()
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase S: clearOppositeSlot must NOT truncate a different-size v1 companion that still protects a live child from an earlier generation.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-s-parent.jsonl'
    const oldChildName = 'retry-20261013T200101000Z-t53-16u-s-oldchild.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const oldChildPath = `${PARTITION_DIR}/${oldChildName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:01.000Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(oldChildPath, oldChildText)
    // Earlier generation (size 123 != parentSize) left a v1 companion protecting live oldChildName.
    const guardPath = `${PARTITION_DIR}/outcome-${parentName}`
    const guardText = sealFile(
      `${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: 123, waits: true, seq: 4, files: [oldChildName], written: [true] })}\n`,
    )
    h.files.set(guardPath, guardText)
    // Hold oldChildName with a foreign lease so parentName retires first and exercises clearOppositeSlot while oldChildName stays live.
    h.store.set(`lease:${TABLE_REF}/${oldChildName}`, { by: 'other-session', until: h.ioFor().now() + 60_000 })
    // Seed a same-size v2 checkpoint (uses v2 slot so otherFiles = v1 recordFiles = [outcome-, outcome-b-]) so parentName writes v2 and runs clearOppositeSlot.
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: parentSize,
      waits: false,
      seq: 5,
      files: [],
      written: [],
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-s-1').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    // Earlier-generation v1 companion must remain intact while oldChildName is still live!
    expect(h.files.get(guardPath)).toBe(guardText)
  }

  // Subcase T: pickJournalSlots preserves `reconciled` and `retiredAt` from a higher-seq retired v1 shadow when merged with a lower-seq unretired v2 checkpoint sharing `files: []`.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16u-t-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16u-t-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    // Repopulated child content has different digest/size from the retired v1 shadow's reconciled digest.
    const repopulatedText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, repopulatedText)
    // Lower-seq unretired v2 store record (stale after store quota fallback, missing `reconciled`) alongside higher-seq retired v1 shadow (`outcome-`, same `files: []`, with `reconciled` + `retiredAt`).
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: false,
      seq: 4,
      files: [],
      written: [],
    })
    h.files.set(
      `${PARTITION_DIR}/outcome-${parentName}`,
      sealFile(
        `${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, waits: false, seq: 9, files: [], written: [], retired: true, retiredAt: 1000, reconciled: [{ name: childName, size: 111, fnv1a: 'deadbeef' }] })}\n`,
      ),
    )
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16u-t-1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-16v: readJournalSlot merges store and newer file checkpoint using newest complete fields so stagedFiles survives store quota fallback', async () => {
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16v-parent.jsonl'
  const childName = 'retry-20261013T200102002Z-t53-16v-child.jsonl'
  const parentPath = `${PARTITION_DIR}/${parentName}`
  const childPath = `${PARTITION_DIR}/${childName}`
  const makeUnkeyed = (tag: string) => {
    const r = ownRow('0'.repeat(32)) as Record<string, unknown>
    delete r.event_id
    r.content = JSON.stringify({ tag })
    return r as ReturnType<typeof ownRow>
  }
  const parentRow = makeUnkeyed('p-16v')
  const childRow1 = makeUnkeyed('c1-16v')
  const childRow2 = makeUnkeyed('c2-16v')
  const parentText = packFiles([
    JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: parentRow }),
  ])[0]!
  const childText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: parentRow, key: `${parentName}#0` }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow1 }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: childRow2 }),
    ],
    parentName,
  )[0]!
  h.files.set(parentPath, parentText)
  h.files.set(childPath, childText)
  const parentSize = utf8Bytes(parentText)
  const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(v2Key, { fates: 'W', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })

  const tagCounts = new Map<string, number>()
  const countingWriter = (session: string) => {
    const baseIo = h.ioFor()
    return new Writer(
      {
        ...baseIo,
        fetch: async (url, init) => {
          if (url.endsWith('/insertAll') && init.body) {
            const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
            for (const entry of body.rows) {
              const parsed = JSON.parse(String(entry.json.content ?? '{}')) as { tag?: string }
              if (parsed.tag) tagCounts.set(parsed.tag, (tagCounts.get(parsed.tag) ?? 0) + 1)
            }
          }
          return baseIo.fetch(url, init)
        },
      },
      () => h.config,
      () => session,
    )
  }

  // Let Phase 2 storeSet succeed (seq 10 in store, no stagedFiles), then fail storeSet on Phase 3 staging (seq 11 falls back to outcome2- file!).
  let v2Sets = 0
  h.setStoreSetHook(async key => {
    if (key === v2Key) {
      v2Sets += 1
      if (v2Sets >= 2) throw new Error('store quota exceeded')
    }
  })
  // Let Part 1 write succeed, fail Part 2 AND fail rollback truncation so Part 1 stays on disk beside store(seq=10) + file(seq=11, stagedFiles).
  let replWrites = 0
  let firstReplPath: string | null = null
  let blockReplacements = false
  h.setWriteFileHook(async (p, text) => {
    if (p.includes('/outcome')) return
    if (p === parentPath || p === childPath) return
    if (text === '' && p === firstReplPath) throw new Error('EIO on rollback')
    if (text !== '' && (p.includes('/retry-') || p.includes('/spill-'))) {
      if (blockReplacements) throw new Error('EIO still active')
      replWrites += 1
      if (replWrites === 1) {
        firstReplPath = p
        return
      }
      throw new Error('EIO on part 2')
    }
  })

  await countingWriter('s-16v-1').backfill(['spill', 'retry'], 'force')
  expect(v2Sets).toBeGreaterThanOrEqual(2)
  expect(replWrites).toBe(2)
  expect(firstReplPath).not.toBeNull()
  expect(h.files.get(firstReplPath!)).not.toBe('')

  // Next auto run MUST read stagedFiles from the newer outcome2- file (not the stale store record) and hold Part 1.
  blockReplacements = true
  await countingWriter('s-16v-2').backfill(['spill', 'retry'], 'auto')
  expect(tagCounts.get('c1-16v') ?? 0).toBe(0)
  expect(tagCounts.get('c2-16v') ?? 0).toBe(0)

  // Recover writes: every row is delivered exactly once.
  h.setStoreSetHook(null)
  h.setWriteFileHook(null)
  await countingWriter('s-16v-3').backfill(['spill', 'retry'], 'force')
  await countingWriter('s-16v-4').backfill(['spill', 'retry'], 'auto')
  expect(tagCounts.get('p-16v')).toBe(1)
  expect(tagCounts.get('c1-16v')).toBe(1)
  expect(tagCounts.get('c2-16v')).toBe(1)
  expect(h.files.get(parentPath)).toBe('')
  expect(h.files.get(childPath)).toBe('')
})

test('T53-16w: retired marker records reconciled child digests so a child repopulated after markDone is preserved and delivered instead of dropped', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowNew = 'f'.repeat(32)

  // Subcase A: repopulated child after markDone is kept intact and delivered once (never dropped by parentFate).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16w-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16w-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    h.files.set(childPath, oldChildText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    // Return 503 for rowNew during force #1 so deferred childName stays on disk for the subsequent auto scan.
    h.setFetch(async (_url, ids) => (ids.includes(rowNew) ? UNAVAILABLE : OK_INSERT))

    const newChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:05.000Z', table: TABLE_REF, row: ownRow(rowNew) })],
      parentName,
    )[0]!
    let repopulated = false
    h.setWriteFileHook(async (p, text) => {
      if (p === parentPath && text === '' && !repopulated) {
        repopulated = true
        h.files.set(childPath, newChildText)
      }
    })

    await h.newWriter('s-16w-1').backfill(['spill', 'retry'], 'force')
    expect(repopulated).toBe(true)
    expect(h.files.get(parentPath)).toBe('')
    // Retired marker recorded BOTH childName in `files` (so 0.1.3 never drops a repopulated child) AND the old digest in `reconciled`.
    const retired = h.doneMarksFor(parentName).find(m => m.complete && m.data?.retired === true)
    expect(retired?.data?.files).toContain(childName)
    expect((retired?.data?.reconciled as { name: string }[] | undefined)?.map(r => r.name)).toEqual([childName])
    expect(h.files.get(childPath)).toBe(newChildText)

    // Next auto scan checks parentFate against the retired marker: MUST deliver rowNew instead of dropping childName.
    expect(h.sent.filter(s => s.id === rowNew && s.status === 200).length).toBe(0)
    h.setWriteFileHook(null)
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16w-2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sent.filter(s => s.id === rowNew && s.status === 200).length).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase B: when post-retirement truncation fails on an unchanged reconciled child, the next auto scan still drops it without resending rowB.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102011Z-t53-16w-same-parent.jsonl'
    const childName = 'retry-20261013T200102012Z-t53-16w-same-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.012Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    h.files.set(childPath, oldChildText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setWriteFileHook(async (p, text) => {
      if (p === childPath && text === '') throw new Error('EIO truncating old child')
    })

    await h.newWriter('s-16w-b-1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe(oldChildText)
    // childGuard mark stays on the un-emptied stale reconciled child so 0.1.3 never double-sends rowB.
    const staleGuards = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${childName}`) && v !== '')
    expect(staleGuards.length).toBe(1)

    h.setWriteFileHook(null)
    await h.newWriter('s-16w-b-2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }
})

test('T53-16x: retention cleanup bounds per-pass deletions for O(1) listings while rechecking confirmDirStable before each parent deletion', async () => {
  // Subcase A: 24 expired parents are cleaned across 3 passes (8 per pass), keeping per-pass listDir calls O(1).
  {
    const h = exitReplayHarness()
    const expiredAt = h.ioFor().now() - 25 * 60 * 60 * 1000
    const count = 24
    for (let i = 0; i < count; i += 1) {
      const parent = `spill-20261013T100102${String(i).padStart(3, '0')}Z-t53-16x.jsonl`
      h.files.set(`${PARTITION_DIR}/${parent}`, '')
      const donePath = `${PARTITION_DIR}/outcome-done-oldw-${parent}`
      const done = { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: expiredAt }
      h.files.set(donePath, sealFile(`${JSON.stringify(done)}\n`))
      h.mtimes.set(donePath, expiredAt)
      h.store.set(`replayed:${TABLE_REF}/${parent}`, { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 1, files: [], written: [] })
    }
    let listDirCalls = 0
    h.setListDirHook(async () => {
      listDirCalls += 1
    })

    // Pass 1 cleans the first 8 expired parents using O(1) directory listings (not O(N)).
    await h.newWriter('s-16x-1').backfill(['spill', 'retry'], 'auto')
    let cleanedAfter1 = 0
    for (let i = 0; i < count; i += 1) {
      const parent = `spill-20261013T100102${String(i).padStart(3, '0')}Z-t53-16x.jsonl`
      if (h.files.get(`${PARTITION_DIR}/outcome-done-oldw-${parent}`) === '') cleanedAfter1 += 1
    }
    expect(cleanedAfter1).toBe(8)
    expect(listDirCalls).toBeLessThanOrEqual(12)

    // Passes 2 and 3 finish the remaining 16 expired parents.
    await h.newWriter('s-16x-2').backfill(['spill', 'retry'], 'auto')
    await h.newWriter('s-16x-3').backfill(['spill', 'retry'], 'auto')
    for (let i = 0; i < count; i += 1) {
      const parent = `spill-20261013T100102${String(i).padStart(3, '0')}Z-t53-16x.jsonl`
      expect(h.files.get(`${PARTITION_DIR}/outcome-done-oldw-${parent}`)).toBe('')
      expect(h.store.get(`replayed:${TABLE_REF}/${parent}`)).toBeUndefined()
    }
  }

  // Subcase B: a late child of parent #2 arriving while parent #1's storeDelete runs aborts before deleting parent #2's evidence.
  {
    const h = exitReplayHarness()
    const expiredAt = h.ioFor().now() - 25 * 60 * 60 * 1000
    const p1 = 'spill-20261013T100102000Z-t53-16x-b1.jsonl'
    const p2 = 'spill-20261013T100102001Z-t53-16x-b2.jsonl'
    for (const parent of [p1, p2]) {
      h.files.set(`${PARTITION_DIR}/${parent}`, '')
      const donePath = `${PARTITION_DIR}/outcome-done-oldw-${parent}`
      const done = { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: expiredAt }
      h.files.set(donePath, sealFile(`${JSON.stringify(done)}\n`))
      h.mtimes.set(donePath, expiredAt)
      h.store.set(`replayed:${TABLE_REF}/${parent}`, { fates: 'L', reasons: [], refusals: [], size: 512, waits: false, seq: 1, files: [], written: [] })
    }
    const lateChild = 'retry-20261013T100109999Z-t53-16x-late.jsonl'
    let injected = false
    h.setStoreDeleteHook(async key => {
      if (key === `replayed:${TABLE_REF}/${p1}` && !injected) {
        injected = true
        h.files.set(
          `${PARTITION_DIR}/${lateChild}`,
          packFiles(
            [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:09.000Z', table: TABLE_REF, row: ownRow('e'.repeat(32)) })],
            p2,
          )[0]!,
        )
      }
    })

    await h.newWriter('s-16x-b').backfill(['spill', 'retry'], 'auto')
    expect(injected).toBe(true)
    // Parent #1 was cleaned before the late child arrived; parent #2's store record and done mark MUST remain intact!
    expect(h.store.get(`replayed:${TABLE_REF}/${p1}`)).toBeUndefined()
    expect(h.store.get(`replayed:${TABLE_REF}/${p2}`)).toBeDefined()
    expect(h.files.get(`${PARTITION_DIR}/outcome-done-oldw-${p2}`)).not.toBe('')
  }
})

test('T53-16y: classifyChild defers when a retired child is replaced between readJournal and readFile instead of reconciling the new bytes', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowNew = '9'.repeat(32)
  const h = exitReplayHarness()
  // Name parent with `retry-` and child with `spill-` so parentName runs BEFORE childName in backfillRun.
  const parentName = 'retry-20261013T200102001Z-t53-16y-parent.jsonl'
  const childName = 'spill-20261013T200102002Z-t53-16y-child.jsonl'
  const parentPath = h.seedSpill(parentName, rowA, rowB)
  const childPath = `${PARTITION_DIR}/${childName}`
  const oldChildText = packFiles(
    [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
    parentName,
  )[0]!
  // New replacement has 2 rows (different size and line count from the retired 1-row generation!).
  const newChildText = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:05.000Z', table: TABLE_REF, row: ownRow(rowNew) }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:05.001Z', table: TABLE_REF, row: ownRow('8'.repeat(32)) }),
    ],
    parentName,
  )[0]!
  h.files.set(childPath, oldChildText)
  const oldChildSize = utf8Bytes(oldChildText)
  const parentSize = utf8Bytes(h.files.get(parentPath)!)
  // Child has a retired done mark matching oldChildSize (e.g. finished earlier, failed to truncate).
  const childDonePath = `${PARTITION_DIR}/outcome-done-oldc-${childName}`
  h.files.set(
    childDonePath,
    sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: oldChildSize, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
  )
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
  h.setFetch(async () => OK_INSERT)

  // Replace childPath right as classifyChild reads childPath (AFTER readJournal(child) saw the retired mark at oldChildSize!).
  let readChildDone = false
  let swapped = false
  const baseIo = h.ioFor()
  const swapIo = {
    ...baseIo,
    readFile: async (p: string) => {
      if (p === childDonePath) readChildDone = true
      if (p === childPath && readChildDone && !swapped) {
        swapped = true
        h.files.set(childPath, newChildText)
      }
      return baseIo.readFile(p)
    },
  }
  await new Writer(swapIo, () => h.config, () => 's-16y-1').backfill(['spill', 'retry'], 'force')
  expect(swapped).toBe(true)
  // classifyChild MUST have deferred (NOT reconciled newChildText), and neither Phase 3 nor the stale-size child scan may truncate newChildText!
  expect(h.files.get(childPath)).toBe(newChildText)
  const parentRetired = h.doneMarksFor(parentName).find(m => m.complete && m.data?.retired === true)
  expect((parentRetired?.data?.reconciled as { name: string }[] | undefined) ?? []).toEqual([])
  expect(parentRetired?.data?.files).toContain(childName)

  // Next auto replay delivers BOTH new rows from childPath without dropping them.
  await h.newWriter('s-16y-2').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowA)).toBe(1)
  expect(h.sentOf(rowB)).toBe(1)
  expect(h.sentOf(rowNew)).toBe(1)
  expect(h.sentOf('8'.repeat(32))).toBe(1)
  expect(h.files.get(parentPath)).toBe('')
  expect(h.files.get(childPath)).toBe('')
})

test('T53-16z: abandoned staged part keeps its guard mark on failed truncation, and failed-keep digest mismatch retains tempMarks', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const rowD = 'd'.repeat(32)
  const rowNew = 'f'.repeat(32)

  // Subcase A: when an abandoned staged part fails truncation during parent retirement, its sending mark MUST NOT be cleared.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16z-a-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16z-a-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    let parentFetched = false
    h.setFetch(async () => {
      parentFetched = true
      return OK_INSERT
    })

    let partWrites = 0
    let abandonedPath: string | null = null
    let failPart2 = true
    let failAbandonedTruncate = true
    h.setWriteFileHook(async (p, text) => {
      if (!parentFetched || p.includes('/outcome') || p === parentPath || p === childPath) return
      if (failAbandonedTruncate && text === '' && p === abandonedPath) throw new Error('EIO truncating abandoned stage')
      if (failPart2 && text !== '' && (p.includes('/retry-') || p.includes('/spill-'))) {
        partWrites += 1
        if (partWrites === 1) {
          abandonedPath = p
          return
        }
        throw new Error('EIO on part 2')
      }
    })

    // Run 1: Part 1 written with tempMark, Part 2 fails, rollback truncation of Part 1 fails -> Part 1 stays staged.
    const w1 = h.newWriter('s-16z-a-1')
    await w1.backfill(['spill', 'retry'], 'force')
    expect(abandonedPath).not.toBeNull()
    const abandonedName = abandonedPath!.slice(abandonedPath!.lastIndexOf('/') + 1)

    // Run 2 (same writer, holds tempMarks): Part 2 now succeeds, parent retires, BUT truncating abandonedPath STILL fails!
    failPart2 = false
    await w1.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(abandonedPath!)).not.toBe('')
    // Sending mark on abandonedPath MUST still be standing (non-empty) so another reader cannot prematurely replay it!
    const abandonedMarks = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${abandonedName}`) && v !== '')
    expect(abandonedMarks.length).toBe(1)

    // Once truncation recovers, auto drops abandonedPath cleanly without duplicating rowC or rowD.
    failAbandonedTruncate = false
    h.setWriteFileHook(null)
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    await h.newWriter('s-16z-a-2').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(abandonedPath!)).toBe('')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.sentOf(rowD)).toBe(1)
  }

  // Subcase B: failed-keep with a repopulated reconciled child (digest mismatch) MUST NOT count it as emptied or clear promoted standalone tempMarks.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16z-b-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16z-b-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldChildText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const repopulatedChildText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:09.000Z', table: TABLE_REF, row: ownRow(rowNew) })],
      parentName,
    )[0]!
    h.files.set(childPath, oldChildText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    h.setFetch(async () => OK_INSERT)

    let replName: string | null = null
    let repopulated = false
    h.setWriteFileHook(async (p, text) => {
      if (!p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '') {
        replName = p.slice(p.lastIndexOf('/') + 1)
      }
      if (p.endsWith(parentName) && text.includes('"retired":true')) {
        if (!repopulated) {
          repopulated = true
          h.files.set(childPath, repopulatedChildText)
        }
        throw new Error('all retired writes fail')
      }
    })

    await h.newWriter('s-16z-b-1').backfill(['spill', 'retry'], 'force')
    expect(repopulated).toBe(true)
    expect(replName).not.toBeNull()
    // Because childPath had a digest mismatch (repopulated), failed-keep MUST keep replName's tempMark standing!
    const replMarks = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${replName!}`) && v !== '')
    expect(replMarks.length).toBe(1)
    expect(h.files.get(childPath)).toBe(repopulatedChildText)
  }
})

test('T53-16aa: forced unreferenced child discovery, live-parent v2 skipIndices, resumed replacement guards, unwritten v1 failed-keep, and pre-record crash recovery', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase A: `/bqaa-backfill force` discovers and preserves an unreferenced live child on conflict, fresh parent, and stale v1-only (`files: []`).
  for (const mode of ['conflict', 'fresh', 'v1-empty'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16aa-a-${mode}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16aa-a-${mode}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
        ],
        parentName,
      )[0]!,
    )
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    if (mode === 'conflict') {
      h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
      h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 5, files: [], written: [] })
    } else if (mode === 'v1-empty') {
      h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    }
    h.setFetch(async () => OK_INSERT)

    await h.newWriter(`s-16aa-a-${mode}-1`).backfill(['spill', 'retry'], 'force')
    await h.newWriter(`s-16aa-a-${mode}-2`).backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase B: live parent retired by 0.1.3 (without truncation) still consults same-size v2 skipIndices so unkeyed row B is not duplicated.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-b-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-b-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const makeUnkeyed = (tag: string) => {
      const r = ownRow('0'.repeat(32)) as Record<string, unknown>
      delete r.event_id
      r.content = JSON.stringify({ tag })
      return r as ReturnType<typeof ownRow>
    }
    const pText = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: makeUnkeyed('A-b') }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: makeUnkeyed('B-b') }),
    ])[0]!
    const cText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: makeUnkeyed('B-b'), key: `${parentName}#1` }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: makeUnkeyed('C-b') }),
      ],
      parentName,
    )[0]!
    h.files.set(parentPath, pText)
    h.files.set(childPath, cText)
    const pSize = utf8Bytes(pText)
    const cSize = utf8Bytes(cText)
    // 0.1.3 wrote retired done mark naming C, but failed to truncate P (so P stays live).
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 3, files: [childName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    // 0.1.4 had placed childGuard on C and persisted v2 with skipIndices: [0].
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
      sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 9,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(cText), skipIndices: [0] }],
    })
    const sentTags: string[] = []
    const baseIo = h.ioFor()
    const w = new Writer(
      {
        ...baseIo,
        writeFile: async (p, t) => {
          // Keep parentPath failing truncation so P stays live while childName is evaluated!
          if (p === parentPath && t === '') throw new Error('EIO truncating P')
          return baseIo.writeFile(p, t)
        },
        fetch: async (url, init) => {
          if (url.endsWith('/insertAll') && init.body) {
            const body = JSON.parse(init.body) as { rows: { json: { content?: string } }[] }
            for (const r of body.rows) sentTags.push((JSON.parse(String(r.json.content ?? '{}')) as { tag: string }).tag)
          }
          return baseIo.fetch(url, init)
        },
      },
      () => h.config,
      () => 's-16aa-b',
    )
    h.setFetch(async () => OK_INSERT)
    await w.backfill(['spill', 'retry'], 'auto')
    expect(sentTags).toEqual(['C-b'])
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase C: resuming a v2 checkpoint with standalone replacement R_c does NOT stamp childGuard on R_c and holds R_c while overlapping C is live.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-c-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-c-child.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16aa-c-repl.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const replPath = `${PARTITION_DIR}/${replName}`
    const cText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const rText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(childPath, cText)
    h.files.set(replPath, rText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w1-${replName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(rText), sending: true, by: 'w1', stagedFor: parentName, stagedForSize: pSize })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 9,
      files: [replName],
      written: [true],
      adopted: [replName],
      reconciled: [{ name: childName, size: utf8Bytes(cText), fnv1a: fnv1a(cText) }],
    })
    h.setWriteFileHook(async (p, text) => {
      if (p.endsWith(parentName) && text.includes('"retired":true')) throw new Error('EIO markDone')
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16aa-c-resume').backfill(['spill', 'retry'], 'auto')
    // No `childGuard` mark (`*g-${replName}`) may have been written onto standalone replacement replName!
    const replGuards = [...h.files.entries()].filter(([k, v]) => k.includes('g-') && k.endsWith(`-${replName}`) && v !== '')
    expect(replGuards.length).toBe(0)
  }

  // Subcase D: failed-keep on a v2 rewrite with stuck v1 having unwritten files (`written: [false]`) retains tempMark on parent leftover L_p.
  {
    const h = exitReplayHarness()
    h.config.maxBatchRows = 1
    const parentName = 'spill-20261013T200102001Z-t53-16aa-d-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-d-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) })],
        parentName,
      )[0]!,
    )
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: ['retry-old-unwritten.jsonl'], written: [false] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'WW', reasons: [], refusals: [], size: pSize, waits: true, seq: 5, files: [childName], written: [true] })
    h.setStoreDeleteHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    h.setStoreSetHook(async key => {
      if (key === v1Key) throw new Error('v1 stuck')
    })
    h.setFetch(async (_url, ids) => (ids.includes(rowB) ? UNAVAILABLE : OK_INSERT))
    let leftoverName: string | null = null
    h.setWriteFileHook(async (p, text) => {
      if (!p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '') leftoverName = p.slice(p.lastIndexOf('/') + 1)
      if (p.endsWith(parentName) && text.includes('"retired":true')) throw new Error('all retired writes fail')
    })

    await h.newWriter('s-16aa-d-1').backfill(['spill', 'retry'], 'force')
    expect(leftoverName).not.toBeNull()
    expect(h.files.get(childPath)).toBe('')
    // Even though reconciled childName was emptied, stuck v1 has `written: [false]`, so L_p MUST keep its tempMark!
    const lpMarks = [...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${leftoverName!}`) && v !== '')
    expect(lpMarks.length).toBe(1)

    // Once 0.1.3 finishes v1 (retiring P with retry-old-unwritten.jsonl and emptying P), 0.1.4 auto must DROP superseded L_p rather than double-sending rowB!
    h.setStoreDeleteHook(null)
    h.setStoreSetHook(null)
    h.setWriteFileHook(null)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(
        `${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 3, files: ['retry-old-unwritten.jsonl'], written: [true], retired: true })}\n`,
      ),
    )
    h.files.set(parentPath, '')
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-d-2').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(`${PARTITION_DIR}/${leftoverName!}`)).toBe('')
    expect(h.sent.filter(s => s.id === rowB && s.status === 200).length).toBe(0)
  }

  // Subcase E: crash before record() + 0.1.3 stale v1 (`files: []`) retirement holds guarded child on auto (report.held) and delivers it on 0.1.4 force (never drops).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-e-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-e-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const cText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, cText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(cText), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: 512, childGuard: true })}\n`),
    )
    h.setFetch(async () => OK_INSERT)

    const rAuto = await h.newWriter('s-16aa-e-1').backfill(['spill', 'retry'], 'auto')
    expect(rAuto.held).not.toBeNull()
    expect(h.files.get(childPath)).toBe(cText)
    expect(h.sentOf(rowC)).toBe(0)

    const rForce = await h.newWriter('s-16aa-e-2').backfill(['spill', 'retry'], 'force')
    expect(rForce.held).toBeNull()
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase F: live-but-retired parent (`0.1.3` wrote `retired: true, files: []` and failed to truncate `P`, so `parentSize > 0`):
  // - With v2 `pendingReconcile` (`skipIndices: [0]`), `0.1.4` auto bypasses `parentFate === 'drop'` and delivers unique row C.
  // - Without v2 (pre-record crash), `0.1.4` auto emits `report.held` and `0.1.4` force bypasses `parentFate === 'drop'` and delivers C.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-f1-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-f1-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const cText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, cText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const cSize = utf8Bytes(cText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
      sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 9,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(cText), skipIndices: [0] }],
    })
    h.setWriteFileHook(async (p, text) => {
      if (p === parentPath && text === '') throw new Error('EIO truncating live retired P')
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16aa-f1').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowB)).toBe(0)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')

    // Part 2: no v2 (pre-record crash) + live retired P (`files: []`).
    const h2 = exitReplayHarness()
    const p2 = 'spill-20261013T200102001Z-t53-16aa-f2-parent.jsonl'
    const c2 = 'retry-20261013T200102002Z-t53-16aa-f2-child.jsonl'
    const p2Path = h2.seedSpill(p2, rowA, rowB)
    const c2Path = `${PARTITION_DIR}/${c2}`
    const c2Text = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      p2,
    )[0]!
    h2.files.set(c2Path, c2Text)
    const p2Size = utf8Bytes(h2.files.get(p2Path)!)
    h2.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${p2}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: p2Size, waits: false, seq: 3, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h2.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${c2}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(c2Text), sending: true, by: 'w14g', stagedFor: p2, stagedForSize: p2Size, childGuard: true })}\n`),
    )
    h2.setWriteFileHook(async (p, text) => {
      if (p === p2Path && text === '') throw new Error('EIO truncating live retired P2')
    })
    h2.setFetch(async () => OK_INSERT)

    const rAuto2 = await h2.newWriter('s-16aa-f2-a').backfill(['spill', 'retry'], 'auto')
    expect(rAuto2.held ?? '').toContain('/bqaa-backfill force')
    expect(h2.files.get(c2Path)).toBe(c2Text)

    const rForce2 = await h2.newWriter('s-16aa-f2-f').backfill(['spill', 'retry'], 'force')
    expect(rForce2.held).toBeNull()
    expect(h2.sentOf(rowC)).toBe(1)
    expect(h2.files.get(c2Path)).toBe('')
  }

  // Subcase G: unmodified adopted child C2 (`childGuard`) MUST NOT be dropped by `allChildrenReplayed` when sibling reconciled child C1 already retired.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-g-parent.jsonl'
    const c1Name = 'retry-20261013T200102002Z-t53-16aa-g-c1.jsonl'
    const c2Name = 'retry-20261013T200102003Z-t53-16aa-g-c2.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const c1Path = `${PARTITION_DIR}/${c1Name}`
    const c2Path = `${PARTITION_DIR}/${c2Name}`
    const c2Text = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(c1Path, '')
    h.files.set(c2Path, c2Text)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-oldc1-${c1Name}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 300, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 500 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: 512, waits: true, seq: 3, files: [c1Name], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${c2Name}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(c2Text), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: 512, childGuard: true })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: 512,
      waits: true,
      seq: 9,
      files: [c2Name],
      written: [true],
      adopted: [c2Name],
      reconciled: [{ name: c1Name, size: 300, fnv1a: '12345678' }],
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16aa-g').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(c2Path)).toBe('')
  }

  // Subcases H & I: same-Writer `auto` failure leaving `this.rewrites` held (both `found === undefined` and `Writer.catchUp`) then `/bqaa-backfill force` MUST set `force` + `accountedRowKeys`, preserving D and deduplicating B.
  for (const failStage of ['record', 'leftover'] as const) {
    const h = exitReplayHarness()
    h.config.maxBatchRows = 1
    const parentName = `retry-20261013T200102001Z-t53-16aa-hi-${failStage}-parent.jsonl`
    const childName = `spill-20261013T200102002Z-t53-16aa-hi-${failStage}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const rowD = 'd'.repeat(32)
    h.files.set(
      childPath,
      packFiles(
        [
          JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
          JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) }),
        ],
        parentName,
      )[0]!,
    )
    let failB = true
    let injectFail = true
    h.setFetch(async (_url, ids) => (failB && ids.includes(rowB) ? UNAVAILABLE : OK_INSERT))
    h.setStoreSetHook(async key => {
      if (injectFail && failStage === 'record' && key.includes(parentName)) throw new Error('record store EIO')
    })
    h.setWriteFileHook(async (p, text) => {
      if (!injectFail) return
      if (failStage === 'record' && (p.includes(`/outcome-${parentName}`) || p.includes(`/outcome-b-${parentName}`))) throw new Error('record file EIO')
      if (failStage === 'leftover' && !p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '') throw new Error('leftover EIO')
    })

    const w = h.newWriter(`s-16aa-hi-${failStage}`)
    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)

    failB = false
    injectFail = false
    h.setStoreSetHook(null)
    h.setWriteFileHook(null)
    await w.backfill(['spill', 'retry'], 'force')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sent.filter(s => s.id === rowB && s.status === 200).length).toBe(1)
    expect(h.sentOf(rowD)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase J: stale lower-seq `stagedFiles` in `outcome2-<P>` must NOT suppress `childGuard` for a child promoted in a higher-seq checkpoint.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-j-parent.jsonl'
    const rcName = 'retry-20261013T200102002Z-t53-16aa-j-rc.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const rcPath = `${PARTITION_DIR}/${rcName}`
    h.files.set(
      rcPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [] })
    h.files.set(
      `${PARTITION_DIR}/outcome2-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [], written: [], stagedFiles: [rcName] })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 3,
      files: [rcName],
      written: [true],
      adopted: [rcName],
    })
    let rcGuardedAtFetch = false
    h.setFetch(async (_url, ids) => {
      if (ids.includes(rowA)) {
        rcGuardedAtFetch = [...h.files.entries()].some(([k, v]) => k.includes('g-') && k.endsWith(`-${rcName}`) && v !== '')
      }
      return OK_INSERT
    })
    await h.newWriter('s-16aa-j').backfill(['spill', 'retry'], 'force')
    expect(rcGuardedAtFetch).toBe(true)
  }

  // Subcase K: transient listDir failure during writeOut forced child audit must fail closed without retiring P or clearing childGuardMarks.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-k-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-k-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    let fetchedParent = false
    let postFetchListCalls = 0
    let failedListOnce = false
    h.setFetch(async (_url, ids) => {
      if (ids.includes(rowA)) fetchedParent = true
      return OK_INSERT
    })
    h.setListDirHook(async () => {
      if (fetchedParent && !failedListOnce) {
        postFetchListCalls += 1
        // Skip the 3 listDir calls inside writeOut's initial readJournal() so we hit the forced child audit dirListing().
        if (postFetchListCalls === 4) {
          failedListOnce = true
          throw new Error('EIO: transient listDir failure in forced child audit')
        }
      }
    })
    const w = h.newWriter('s-16aa-k')
    await w.backfill(['spill', 'retry'], 'force')
    expect(failedListOnce).toBe(true)
    expect(w.snapshot().lastError?.message.includes('forced child audit')).toBe(true)
    expect(h.files.get(parentPath) !== '').toBe(true)
    expect(h.files.get(childPath) !== '').toBe(true)
    expect([...h.files.entries()].some(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '')).toBe(true)
    expect([...h.files.entries()].some(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${parentName}`) && v.includes('"retired":true'))).toBe(false)

    await w.backfill(['spill', 'retry'], 'force')
    await w.backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase L: when a concurrent 0.1.3 writer retires P with files:[] while 0.1.4 force HTTP is in flight, writeOut (finished && !wroteDone via catchUp) must NOT clear childGuardMarks.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-l-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-l-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    let concurrent013Retired = false
    h.setFetch(async (_url, ids) => {
      if (ids.includes(rowA)) {
        // Concurrent 0.1.3 retires P with files:[] while 0.1.4 force HTTP is in flight.
        concurrent013Retired = true
        h.files.set(
          `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
          sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [], retired: true })}\n`),
        )
      }
      return OK_INSERT
    })
    await h.newWriter('s-16aa-l1').backfill(['spill'], 'force')
    expect(concurrent013Retired).toBe(true)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath) !== '').toBe(true)
    expect([...h.files.entries()].some(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '')).toBe(true)

    const rAuto = await h.newWriter('s-16aa-l2').backfill(['retry'], 'auto')
    expect(rAuto.held?.includes('/bqaa-backfill force')).toBe(true)
    expect(h.files.get(childPath) !== '').toBe(true)

    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-l3').backfill(['retry'], 'force')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase M: classifyChild retired-child check must skip a repopulated file belonging to a different parent even on initial force scan (fallbackRef === undefined).
  {
    const h = exitReplayHarness()
    const parentP = 'spill-20261013T200102001Z-t53-16aa-m-p.jsonl'
    const parentQ = 'spill-20261013T200102001Z-t53-16aa-m-q.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-m-c.jsonl'
    const pPath = h.seedSpill(parentP, rowA)
    const cPath = `${PARTITION_DIR}/${childName}`
    const cContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentQ,
    )[0]!
    h.files.set(cPath, cContent)
    const pSize = utf8Bytes(h.files.get(pPath)!)
    const cSize = utf8Bytes(cContent)
    h.store.set(`replayed:${TABLE_REF}/${parentP}`, { fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [childName], written: [true] })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-other-${childName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, waits: false, seq: 1, files: [], written: [], retired: true })}\n`),
    )
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-m').backfill(['spill'], 'force')
    expect(h.files.get(pPath)).toBe('')
    // Child belongs to parentQ, so parentP must neither record it in reconciled nor truncate it.
    expect(h.files.get(cPath)).toBe(cContent)
    const pDone = [...h.files.entries()].find(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${parentP}`) && v.includes('"retired":true'))
    expect(pDone !== undefined).toBe(true)
    const pDoneJson = JSON.parse(pDone![1].split('\n')[0]!) as { reconciled?: { name: string }[] }
    expect((pDoneJson.reconciled ?? []).some(r => r.name === childName)).toBe(false)
  }

  // Subcase N: when finished && !wroteDone via catchUp against a concurrent 0.1.3 files:[] retirement, cleanupReconciledAndStaged(wroteDone) must NOT clear childGuard on a repopulated reconciled child (both same-size and different-size repopulations must be delivered, never dropped).
  for (const sameSize of [false, true]) {
    const h = exitReplayHarness()
    const tag = sameSize ? 'same' : 'diff'
    const parentName = `spill-20261013T200102001Z-t53-16aa-n-${tag}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16aa-n-${tag}-child.jsonl`
    const parentPath = h.seedSpill(parentName, rowA)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) })],
        parentName,
      )[0]!,
    )
    const repopulatedChild = packFiles(
      sameSize
        ? [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })]
        : [
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) }),
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
          ],
      parentName,
    )[0]!
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [] })
    let injectedConcurrent013 = false
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.setStoreSetHook(async (key: string) => {
      if (key === v2Key && !injectedConcurrent013) {
        injectedConcurrent013 = true
        // Right as 0.1.4 checkpoints v2 (reconciled:[childName]), concurrent 0.1.3 retires P with files:[] and child is repopulated.
        h.files.set(
          `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
          sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [], retired: true })}\n`),
        )
        h.files.set(childPath, repopulatedChild)
      }
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter(`s-16aa-n1-${tag}`).backfill(['spill'], 'force')
    expect(injectedConcurrent013).toBe(true)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe(repopulatedChild)
    expect([...h.files.entries()].some(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '')).toBe(true)

    h.setStoreSetHook(null)
    // Next 0.1.4 auto must deliver repopulated child (via v2 reconciled digest mismatch -> 'go'), never drop rowC!
    await h.newWriter(`s-16aa-n2-${tag}`).backfill(['retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase O: non-retired classifyChild and discoverLiveChildrenForForce must skip a live file belonging to a different parent Q (neither guarding, reconciling, nor truncating it).
  {
    const h = exitReplayHarness()
    const parentP = 'retry-20261013T200102001Z-t53-16aa-o-p.jsonl'
    const parentQ = 'spill-20261013T200102001Z-t53-16aa-o-q.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16aa-o-c.jsonl'
    const pPath = `${PARTITION_DIR}/${parentP}`
    const pContent = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
    ])[0]!
    h.files.set(pPath, pContent)
    const cPath = `${PARTITION_DIR}/${childName}`
    const cContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'spill', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowA) })],
      parentQ,
    )[0]!
    h.files.set(cPath, cContent)
    const pSize = utf8Bytes(pContent)
    // Conflicting v1/v2 where stale v1 names childName so force runs both discoverLiveChildrenForForce and classifyChild(fallbackRef=undefined).
    h.store.set(`replayed:${TABLE_REF}/${parentP}`, { fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [childName], written: [true] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentP}`, { fates: 'L', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [] })
    let childGuardedAtFetch = false
    h.setFetch(async (_url, ids) => {
      if (ids.includes(rowA)) {
        childGuardedAtFetch = [...h.files.entries()].some(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '')
      }
      return OK_INSERT
    })
    await h.newWriter('s-16aa-o').backfill(['retry'], 'force')
    expect(childGuardedAtFetch).toBe(false)
    expect(h.files.get(pPath)).toBe('')
    expect(h.files.get(cPath)).toBe(cContent)
    const pDone = [...h.files.entries()].find(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${parentP}`) && v.includes('"retired":true'))
    expect(pDone !== undefined).toBe(true)
    const pDoneJson = JSON.parse(pDone![1].split('\n')[0]!) as { reconciled?: { name: string }[] }
    expect((pDoneJson.reconciled ?? []).some(r => r.name === childName)).toBe(false)
  }

  // Subcase P: parentFate must NOT merge v2.reconciled onto a legacy retired done mark when v2 has unwritten leftovers (`written: [false]`).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-p-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-p-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const cContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, cContent)
    const pSize = 700
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 3,
      files: ['retry-20261013T200102003Z-unwritten-lp.jsonl'],
      written: [false],
      reconciled: [{ name: childName, size: utf8Bytes(cContent), fnv1a: fnv1a(cContent) }],
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-p').backfill(['retry'], 'auto')
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase Q: classifyChild defers a live child that already has a valid fitting unretired journal so the child completes cleanly without racing to emit a duplicate replacement.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-q-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-q-child.jsonl'
    const childLeftover = 'retry-20261013T200102003Z-t53-16aa-q-lc.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const cContent = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, cContent)
    h.store.set(`replayed:${TABLE_REF}/${childName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: utf8Bytes(cContent),
      waits: true,
      seq: 1,
      files: [childLeftover],
      written: [false],
    })
    h.setFetch(async () => OK_INSERT)
    // Pass 1 (force): P defers C (active unretired journal), then C finishes its own journal writing childLeftover and emptying C.
    await h.newWriter('s-16aa-q1').backfill(['spill', 'retry'], 'force')
    expect(h.files.get(childPath)).toBe('')
    expect((h.files.get(`${PARTITION_DIR}/${childLeftover}`) ?? '') !== '').toBe(true)
    // Pass 2 (auto): P sees C is emptied and retires cleanly; childLeftover delivers rowC once with zero duplicate replacements.
    await h.newWriter('s-16aa-q2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(`${PARTITION_DIR}/${childLeftover}`)).toBe('')
  }

  // Subcase R: retiredCoversLeftover must NOT drop standalone L_p when 0.1.3's retired marker names a reconciled or pendingReconcile child C that 0.1.4 drops or skips in reliance on L_p.
  for (const mode of ['reconciled', 'pending-skip'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16aa-r-${mode}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16aa-r-${mode}-child.jsonl`
    const lpName = `retry-20261013T200102003Z-t53-16aa-r-${mode}-lp.jsonl`
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const lpPath = `${PARTITION_DIR}/${lpName}`
    const cContent = packFiles(
      mode === 'reconciled'
        ? [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })]
        : [
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
          ],
      parentName,
    )[0]!
    const lpContent = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    const pSize = 700
    h.files.set(parentPath, '')
    h.files.set(childPath, cContent)
    h.files.set(lpPath, lpContent)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(cContent), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14-${lpName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(lpContent), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [lpName],
      written: [true],
      ...(mode === 'reconciled'
        ? { reconciled: [{ name: childName, size: utf8Bytes(cContent), fnv1a: fnv1a(cContent) }] }
        : { pendingReconcile: [{ name: childName, size: utf8Bytes(cContent), fnv1a: fnv1a(cContent), skipIndices: [0] }] }),
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter(`s-16aa-r1-${mode}`).backfill(['retry'], 'auto')
    await h.newWriter(`s-16aa-r2-${mode}`).backfill(['retry'], 'auto')
    expect(h.sentOf(rowB)).toBe(1)
    if (mode === 'pending-skip') expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(lpPath)).toBe('')
  }

  // Subcase S: parentFate merging v2.reconciled onto a legacy retired done mark naming C must also preserve non-standalone written v2.files leftover L_p so rowB is delivered once.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-s-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-s-child.jsonl'
    const lpName = 'retry-20261013T200102003Z-t53-16aa-s-lp.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const lpPath = `${PARTITION_DIR}/${lpName}`
    const cContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    const lpContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    const pSize = 700
    h.files.set(parentPath, '')
    h.files.set(childPath, cContent)
    h.files.set(lpPath, lpContent)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [lpName],
      written: [true],
      reconciled: [{ name: childName, size: utf8Bytes(cContent), fnv1a: fnv1a(cContent) }],
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-s').backfill(['retry'], 'auto')
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(lpPath)).toBe('')
  }

  // Subcase T: an abandoned staged part in v2.stagedFiles drops cleanly once P retires even after Phase 3 retries cleared pendingReconcile.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16aa-t-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16aa-t-child.jsonl'
    const part1Name = 'retry-20261013T200102003Z-t53-16aa-t-part1.jsonl'
    const part1bName = 'retry-20261013T200102004Z-t53-16aa-t-part1b.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const part1Path = `${PARTITION_DIR}/${part1Name}`
    const part1bPath = `${PARTITION_DIR}/${part1bName}`
    const part1Content = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const part1bContent = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.004Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const pSize = 700
    h.files.set(parentPath, '')
    h.files.set(part1Path, part1Content)
    h.files.set(part1bPath, part1bContent)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14-${part1Name}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(part1Content), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize, replaces: childName })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14-${part1bName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(part1bContent), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize, replaces: childName })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14d-${parentName}`,
      sealFile(
        `${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, waits: true, seq: 8, files: [part1bName, childName], written: [true, true], retired: true, reconciled: [{ name: childName, size: 300, fnv1a: '12345678' }] })}\n`,
      ),
    )
    // v2 retains abandoned part1 in stagedFiles while pendingReconcile is already cleared (undefined).
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 7,
      files: [part1bName],
      written: [true],
      adopted: [part1bName],
      reconciled: [{ name: childName, size: 300, fnv1a: '12345678' }],
      stagedFiles: [part1Name],
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16aa-t').backfill(['retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(part1bPath)).toBe('')
    expect(h.files.get(part1Path)).toBe('')
  }

  // Subcase U: when partial v2 (`written: [true, false]`, `reconciled: [C]`) coexists with a legacy retired done mark naming C, neither parentFate nor stagedGuardState may duplicate C's overlapping row via lp1.
  for (const standaloneLp of [false, true]) {
    const h = exitReplayHarness()
    const tag = standaloneLp ? 'staged' : 'parentfate'
    const parentName = `spill-20261013T200102001Z-t53-16aa-u-${tag}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16aa-u-${tag}-child.jsonl`
    const lp1Name = `retry-20261013T200102003Z-t53-16aa-u-${tag}-lp1.jsonl`
    const lp2Name = `retry-20261013T200102004Z-t53-16aa-u-${tag}-lp2.jsonl`
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const lp1Path = `${PARTITION_DIR}/${lp1Name}`
    const unkeyedB = ownRow(rowB) as Record<string, unknown>
    delete unkeyedB.event_id
    unkeyedB.content = JSON.stringify({ tag: 'unkeyed-u' })
    const cContent = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: unkeyedB })],
      parentName,
    )[0]!
    const lp1Content = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: unkeyedB })],
      standaloneLp ? undefined : parentName,
    )[0]!
    const pSize = 700
    h.files.set(parentPath, '')
    h.files.set(childPath, cContent)
    h.files.set(lp1Path, lp1Content)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'WW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
    )
    if (standaloneLp) {
      h.files.set(
        `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
        sealFile(`${JSON.stringify({ size: utf8Bytes(cContent), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-w14-${lp1Name}`,
        sealFile(`${JSON.stringify({ size: utf8Bytes(lp1Content), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize })}\n`),
      )
    }
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'WW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [lp1Name, lp2Name],
      written: [true, false],
      reconciled: [{ name: childName, size: utf8Bytes(cContent), fnv1a: fnv1a(cContent) }],
    })
    let deliveredUnkeyed = 0
    h.setFetch(async (_url, ids) => {
      deliveredUnkeyed += ids.length
      return OK_INSERT
    })
    await h.newWriter(`s-16aa-u1-${tag}`).backfill(['retry'], 'auto')
    await h.newWriter(`s-16aa-u2-${tag}`).backfill(['retry'], 'auto')
    expect(deliveredUnkeyed).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(lp1Path)).toBe('')
  }
})

test('T53-16ab: pre-dispatch backup rollback, attempt-bound child guards, and multi-attempt backup cleanup', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase A: stageChildBackups EIO before parent HTTP dispatch fails closed, rolls back childGuard and parent sending mark, and sends 0 rows.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-a-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ab-a-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
        ],
        parentName,
      )[0]!,
    )
    h.setFetch(async () => OK_INSERT)
    h.setWriteFileHook(async (p, text) => {
      if (!p.includes('/outcome') && p !== parentPath && p !== childPath && text !== '') {
        throw new Error('EIO staging pre-dispatch backup')
      }
    })
    const r = await h.newWriter('s-16ab-a').backfill(['spill', 'retry'], 'force')
    expect(r.error).not.toBeNull()
    expect(h.sent.length).toBe(0)
    expect(h.files.get(parentPath)).not.toBe('')
    expect([...h.files.entries()].filter(([k, v]) => k.includes('outcome-done-') && v !== '').length).toBe(0)
  }

  // Subcase B: when all parent rows receive 503 (!changed && !needsSplit), newly staged backups and childGuards roll back cleanly.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-b-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ab-b-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA)
    const childPath = `${PARTITION_DIR}/${childName}`
    h.files.set(
      childPath,
      packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!,
    )
    h.setFetch(async () => UNAVAILABLE)
    await h.newWriter('s-16ab-b').backfill(['spill'], 'force')
    expect(h.files.get(parentPath)).not.toBe('')
    expect([...h.files.entries()].filter(([p, v]) => p.startsWith(`${PARTITION_DIR}/`) && !p.includes('/outcome') && p !== parentPath && p !== childPath && v !== '').length).toBe(0)
    expect([...h.files.entries()].filter(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '').length).toBe(0)
    expect([...h.files.entries()].filter(([k, v]) => k.includes('g-') && v.includes('"sending":true')).length).toBe(0)
    // Subsequent fresh session auto replay must NOT emit false "is empty, perhaps still being written" on the rolled-back backup.
    h.setFetch(async () => OK_INSERT)
    const rAfterRollback = await h.newWriter('s-16ab-b2').backfill(['retry'], 'auto')
    expect(rAfterRollback.skippedFiles).toBe(0)
    expect(rAfterRollback.held).toBeNull()
  }

  // Subcase C: two crashed attempts (A1 wrote v2(attempt: A1), then A2 crashed before record()): auto MUST NOT accept A1's stale v2 because A2 is uncovered; later A3 cleans up prior-attempt backup B_c1.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-c-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ab-c-child.jsonl'
    const priorBackupName = 'retry-20261013T200102003Z-t53-16ab-c-backup1.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const priorBackupPath = `${PARTITION_DIR}/${priorBackupName}`
    const cText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const b1Text = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(childPath, cText)
    h.files.set(priorBackupPath, b1Text)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const cSize = utf8Bytes(cText)
    const b1Size = utf8Bytes(b1Text)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-a1oldg-${childName}`,
      sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'a1oldg', stagedFor: parentName, stagedForSize: pSize, childGuard: true, attempt: 'attempt-1' })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-a2oldg-${childName}`,
      sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'a2oldg', stagedFor: parentName, stagedForSize: pSize, childGuard: true, attempt: 'attempt-2' })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-a1oldg-${priorBackupName}`,
      sealFile(
        `${JSON.stringify({ size: b1Size, sending: true, by: 'a1oldg', stagedFor: parentName, stagedForSize: pSize, replaces: childName, backup: true, backupForSize: cSize, backupFnv1a: fnv1a(b1Text), attempt: 'attempt-1' })}\n`,
      ),
    )
    // Stale v2 covers only attempt-1 (NOT attempt-2!), and 0.1.3 already retired P.
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LW',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [childName],
      written: [true],
      attempt: 'attempt-1',
      attempts: ['attempt-1'],
    })
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
    )
    h.files.set(parentPath, '')
    h.setFetch(async () => OK_INSERT)

    const rAuto = await h.newWriter('s-16ab-c1').backfill(['spill', 'retry'], 'auto')
    expect(rAuto.held ?? '').toContain('/bqaa-backfill force')
    expect(h.sent.length).toBe(0)
    expect(h.files.get(childPath)).toBe(cText)
    expect(h.files.get(priorBackupPath)).toBe(b1Text)

    // Now test A3 replaying live P afresh (conflicting v1 vs v2) retires P covering attempt-1 and attempt-2 and cleans up prior-attempt live backup too.
    h.files.set(parentPath, h.seedSpill(parentName, rowA, rowB) && h.files.get(parentPath)!)
    h.files.delete(`${PARTITION_DIR}/outcome-done-old013-${parentName}`)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    await h.newWriter('s-16ab-c2').backfill(['spill', 'retry'], 'force')
    await h.newWriter('s-16ab-c3').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(priorBackupPath)).toBe('')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
  }

  // Subcase D: multi-child stageChildBackups rolls back child 1's staged backup when child 2 changes size mid-staging (0 HTTP requests, no orphaned live backup or standing sending mark).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-d-parent.jsonl'
    const c1Name = 'retry-20261013T200102002Z-t53-16ab-d-c1.jsonl'
    const c2Name = 'retry-20261013T200102003Z-t53-16ab-d-c2.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const c1Path = `${PARTITION_DIR}/${c1Name}`
    const c2Path = `${PARTITION_DIR}/${c2Name}`
    const rowD = 'd'.repeat(32)
    h.files.set(
      c1Path,
      packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })], parentName)[0]!,
    )
    h.files.set(
      c2Path,
      packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowD) })], parentName)[0]!,
    )
    let mutatedC2 = false
    h.setWriteFileHook(async (p, text) => {
      // As soon as child 1's backup data file is written, mutate child 2's size so stageChildBackups aborts on child 2.
      if (!mutatedC2 && !p.includes('/outcome') && p !== parentPath && p !== c1Path && p !== c2Path && text !== '') {
        mutatedC2 = true
        h.files.set(
          c2Path,
          packFiles(
            [
              JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowD) }),
              JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.004Z', table: TABLE_REF, row: ownRow(rowB) }),
            ],
            parentName,
          )[0]!,
        )
      }
    })
    h.setFetch(async () => OK_INSERT)
    const r = await h.newWriter('s-16ab-d').backfill(['spill', 'retry'], 'force')
    expect(mutatedC2).toBe(true)
    expect(r.error).not.toBeNull()
    expect(h.sent.length).toBe(0)
    expect([...h.files.entries()].filter(([p, v]) => p.startsWith(`${PARTITION_DIR}/`) && !p.includes('/outcome') && p !== parentPath && p !== c1Path && p !== c2Path && v !== '').length).toBe(0)
    expect([...h.files.entries()].filter(([k, v]) => k.includes('g-') && v.includes('"sending":true')).length).toBe(0)
  }

  // Subcase E: standalone backup B_c MUST stay held (even under `force`) while parent P is still live and unretired (`pj === undefined`).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-e-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ab-e-child.jsonl'
    const backupName = 'retry-20261013T200102003Z-t53-16ab-e-backup.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const backupPath = `${PARTITION_DIR}/${backupName}`
    const cText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    const bText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(childPath, cText)
    h.files.set(backupPath, bText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(cText), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true, attempt: 'a1' })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14g-${backupName}`,
      sealFile(
        `${JSON.stringify({ size: utf8Bytes(bText), sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, replaces: childName, backup: true, backupForSize: utf8Bytes(cText), backupForFnv1a: fnv1a(cText), backupFnv1a: fnv1a(bText), attempt: 'a1' })}\n`,
      ),
    )
    // Parent P is live with no journal (`pj === undefined`); forcing only ['retry'] MUST NOT prematurely deliver C or B_c!
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16ab-e1').backfill(['retry'], 'force')
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(childPath)).toBe(cText)
    expect(h.files.get(backupPath)).toBe(bText)
  }

  // Subcase F: failed-keep WITHOUT stuck v1 (`v1ShadowNeeded() === false`, so replacement carries `parent: P`) MUST keep standalone backup B_c live until P retires on disk.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ab-f-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ab-f-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const cText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, cText)
    h.setFetch(async () => OK_INSERT)
    h.setWriteFileHook(async (p, text) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && text.includes('"retired":true')) {
        throw new Error('EIO on parent retired done mark')
      }
    })
    await h.newWriter('s-16ab-f1').backfill(['spill', 'retry'], 'force')
    // P did not retire (`failed-keep`, `v1ShadowNeeded() === false`): child C and standalone backup B_c must BOTH remain live.
    expect(h.files.get(parentPath)).not.toBe('')
    expect(h.files.get(childPath)).toBe(cText)
    const liveBackups = [...h.files.entries()].filter(([p, txt]) => {
      if (p.includes('/outcome') || p === parentPath || p === childPath || txt === '') return false
      const parsed = parseFile(txt)
      return parsed.complete && parsed.parent === null
    })
    expect(liveBackups.length).toBe(1)
    // Once disk recovers, auto retires P, empties C and B_c, and delivers rowC once.
    h.setWriteFileHook(null)
    await h.newWriter('s-16ab-f2').backfill(['spill', 'retry'], 'auto')
    await h.newWriter('s-16ab-f3').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.files.get(liveBackups[0]![0])).toBe('')
  }

  // Subcase G: superseded digest-less 0.1.3 done mark + higher-seq 0.1.4 done mark for C_old MUST NOT be unmasked by stagedGuardState or cleanupBackups when C_new (equal size, new digest) is emptied unsent.
  {
    const rowNew = 'n'.repeat(32)
    // Arm 1: stagedGuardState on standalone backup B_c after P retires and C_new is emptied unsent -> MUST deliver rowNew (never drop B_c via superseded 0.1.3 done mark).
    {
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ab-g1-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ab-g1-child.jsonl'
      const backupName = 'retry-20261013T200102003Z-t53-16ab-g1-backup.jsonl'
      const parentPath = h.seedSpill(parentName, rowA, rowB)
      const childPath = `${PARTITION_DIR}/${childName}`
      const backupPath = `${PARTITION_DIR}/${backupName}`
      const oldC = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!
      const newC = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) })],
        parentName,
      )[0]!
      const bText = packFiles([
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) }),
      ])[0]!
      expect(utf8Bytes(newC)).toBe(utf8Bytes(oldC))
      const pSize = utf8Bytes(h.files.get(parentPath)!)
      const cSize = utf8Bytes(newC)
      // Parent retired by 0.1.3 force with files: [], C_new emptied unsent; C has both seq:1 digest-less 0.1.3 done mark and seq:2 0.1.4 done mark for oldC.
      h.files.set(parentPath, '')
      h.files.set(childPath, '')
      h.files.set(backupPath, bText)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-dist1-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-dist1-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-cur1d-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, sourceFnv1a: fnv1a(oldC), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-cur2g-${backupName}`,
        sealFile(
          `${JSON.stringify({ size: utf8Bytes(bText), sending: true, by: 'cur2g', stagedFor: parentName, stagedForSize: pSize, replaces: childName, backup: true, backupForSize: cSize, backupForFnv1a: fnv1a(newC), backupFnv1a: fnv1a(bText), attempt: 'a1' })}\n`,
        ),
      )
      h.setFetch(async () => OK_INSERT)
      const rAuto = await h.newWriter('s-16ab-g1').backfill(['retry'], 'auto')
      expect(rAuto.held ?? '').toContain('/bqaa-backfill force')
      expect(h.files.get(backupPath)).toBe(bText)
      expect(h.sentOf(rowNew)).toBe(0)
      await h.newWriter('s-16ab-g1-force').backfill(['retry'], 'force')
      expect(h.sentOf(rowNew)).toBe(1)
      expect(h.files.get(backupPath)).toBe('')
    }

    // Arm 2: cleanupBackups(true) when P adopts C_new and C_new is concurrently emptied unsent before P finishes -> MUST keep B_c alive and deliver rowNew on auto.
    {
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ab-g2-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ab-g2-child.jsonl'
      const parentPath = h.seedSpill(parentName, rowA)
      const childPath = `${PARTITION_DIR}/${childName}`
      const oldC = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) })],
        parentName,
      )[0]!
      const newC = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) })],
        parentName,
      )[0]!
      expect(utf8Bytes(newC)).toBe(utf8Bytes(oldC))
      const cSize = utf8Bytes(newC)
      h.files.set(childPath, newC)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-dist1-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-cur1d-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, sourceFnv1a: fnv1a(oldC), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      let emptiedC = false
      h.setWriteFileHook(async (p, text) => {
        // Right after P writes its retired done mark (before cleanupBackups), simulate concurrent 0.1.3 emptying C_new unsent.
        if (!emptiedC && p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && text.includes('"retired":true')) {
          emptiedC = true
          h.files.set(childPath, '')
        }
      })
      h.setFetch(async () => OK_INSERT)
      await h.newWriter('s-16ab-g2-1').backfill(['spill'], 'force')
      expect(emptiedC).toBe(true)
      await h.newWriter('s-16ab-g2-2').backfill(['spill', 'retry'], 'auto')
      expect(h.sentOf(rowA)).toBe(1)
      expect(h.sentOf(rowNew)).toBe(1)
    }
  }
})

test('T53-16ac: emptied reconciled child retains its size-pinned childGuard without false empty-waiting notices and drops exact late copies', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowD = 'd'.repeat(32)
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16ac-parent.jsonl'
  const childName = 'retry-20261013T200102002Z-t53-16ac-child.jsonl'
  const parentPath = h.seedSpill(parentName, rowA, rowB)
  const childPath = `${PARTITION_DIR}/${childName}`
  const oldC = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
    parentName,
  )[0]!
  const newC = packFiles(
    [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) })],
    parentName,
  )[0]!
  expect(utf8Bytes(newC)).toBe(utf8Bytes(oldC))
  h.files.set(childPath, oldC)
  const pSize = utf8Bytes(h.files.get(parentPath)!)
  h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [childName], written: [true] })
  h.setFetch(async () => OK_INSERT)

  await h.newWriter('s-16ac-1').backfill(['spill', 'retry'], 'force')
  expect(h.files.get(parentPath)).toBe('')
  expect(h.files.get(childPath)).toBe('')
  // Size-pinned childGuard mark remains standing beside emptied reconciled child C.
  const standingGuards = [...h.files.entries()].filter(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '')
  expect(standingGuards.length).toBe(1)

  // Fresh writer within WRITE_PENDING_MS must NOT emit a false "is empty, perhaps still being written" held notice.
  const rEmpty = await h.newWriter('s-16ac-2').backfill(['spill', 'retry'], 'auto')
  expect(rEmpty.held).toBeNull()

  // Late exact copy of oldC is dropped cleanly while keeping the guard standing.
  h.files.set(childPath, oldC)
  await h.newWriter('s-16ac-3').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowB)).toBe(1)
  expect(h.files.get(childPath)).toBe('')
  expect([...h.files.entries()].filter(([k, v]) => k.includes('g-') && k.endsWith(`-${childName}`) && v !== '').length).toBe(1)

  // Same-size different-content repopulation newC is delivered once on auto.
  h.files.set(childPath, newC)
  await h.newWriter('s-16ac-4').backfill(['spill', 'retry'], 'auto')
  expect(h.sentOf(rowD)).toBe(1)
  expect(h.files.get(childPath)).toBe('')

  // Retention lifecycle across no-mtimes, cap-8, and mtimeMs skew: retention
  // MUST NEVER delete P's retired records while emptied C's childGuard mark
  // still stands, and MUST expire C's childGuard cleanly without deadlocking a
  // future same-size file at C.
  for (const mode of ['no-mtimes', 'cap8', 'mtime-skew'] as const) {
    const h2 = exitReplayHarness()
    const pName = `retry-20261013T200102001Z-t53-16ac-${mode}-p.jsonl`
    const cName = `spill-20261013T200102002Z-t53-16ac-${mode}-c.jsonl`
    const pPath = `${PARTITION_DIR}/${pName}`
    const cPath = `${PARTITION_DIR}/${cName}`
    const pContent = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    const cOld = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      pName,
    )[0]!
    const cNew = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) })],
      pName,
    )[0]!
    expect(utf8Bytes(cNew)).toBe(utf8Bytes(cOld))
    h2.files.set(pPath, pContent)
    h2.files.set(cPath, cOld)
    const pSz = utf8Bytes(pContent)
    h2.store.set(`replayed:${TABLE_REF}/${pName}`, { fates: 'LL', reasons: [], refusals: [], size: pSz, waits: false, seq: 2, files: [], written: [] })
    h2.store.set(`replayed:v2:${TABLE_REF}/${pName}`, { fates: 'LW', reasons: [], refusals: [], size: pSz, waits: true, seq: 9, files: [cName], written: [true] })
    h2.setFetch(async () => OK_INSERT)
    await h2.newWriter(`s-16ac-${mode}-init`).backfill(['spill', 'retry'], 'force')

    const cGuardPath = [...h2.files.entries()].find(([k, v]) => k.includes('g-') && k.endsWith(`-${cName}`) && v !== '')?.[0]
    const pDonePath = [...h2.files.entries()].find(([k, v]) => k.includes('outcome-done-') && k.endsWith(`-${pName}`) && v.includes('"retired":true'))?.[0]
    expect(cGuardPath).toBeDefined()
    expect(pDonePath).toBeDefined()
    if (mode !== 'no-mtimes') {
      for (const p of h2.files.keys()) h2.mtimes.set(p, 1000)
    }

    if (mode === 'cap8') {
      for (let i = 0; i < 8; i += 1) {
        const filler = `retry-20261013T20010100${i}Z-t53-16ac-filler-${i}.jsonl`
        const fPath = `${PARTITION_DIR}/${filler}`
        const fDone = `${PARTITION_DIR}/outcome-done-old${i}-${filler}`
        h2.files.set(fPath, '')
        h2.mtimes.set(fPath, 1000)
        h2.files.set(
          fDone,
          sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 100, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
        )
        h2.mtimes.set(fDone, 1000)
      }
    }

    h2.advanceNow(25 * 60 * 60_000)
    if (mode === 'mtime-skew') {
      h2.mtimes.set(cGuardPath!, h2.ioFor().now() - 23 * 60 * 60_000)
    }
    const baseIo = h2.ioFor()
    const runIo =
      mode === 'no-mtimes'
        ? {
            ...baseIo,
            listDir: async (d: string) => (await baseIo.listDir(d)).map(({ mtimeMs: _omit, ...rest }) => rest),
          }
        : baseIo
    await new Writer(runIo, () => h2.config, () => `s-16ac-${mode}-ret1`).backfill(['spill', 'retry'], 'auto')

    // Invariant: whenever cGuardPath still stands, pDonePath MUST still stand too!
    if ((h2.files.get(cGuardPath!) ?? '') !== '') {
      expect(h2.files.get(pDonePath!)).not.toBe('')
    }

    // Same-size newC arriving right after pass 1 MUST deliver on auto without deadlocking!
    h2.files.set(cPath, cNew)
    const rDeliver = await new Writer(runIo, () => h2.config, () => `s-16ac-${mode}-deliver`).backfill(['spill', 'retry'], 'auto')
    if (mode !== 'no-mtimes') expect(rDeliver.held).toBeNull()
    else expect(rDeliver.held ?? '').not.toContain(cName)
    expect(h2.sentOf(rowD)).toBe(1)
    expect(h2.files.get(cPath)).toBe('')

    // After another 25h (2 passes), both C's guard and P's retired marker expire cleanly!
    h2.advanceNow(25 * 60 * 60_000)
    await new Writer(runIo, () => h2.config, () => `s-16ac-${mode}-ret2`).backfill(['spill', 'retry'], 'auto')
    await new Writer(runIo, () => h2.config, () => `s-16ac-${mode}-ret3`).backfill(['spill', 'retry'], 'auto')
    expect(h2.files.get(cGuardPath!)).toBe('')
    expect(h2.files.get(pDonePath!)).toBe('')
  }
})

test('T53-16ad: equal-size replacement of a retired child is delivered instead of recorded as the old generation', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowD = 'd'.repeat(32)

  // Subcase A: 0.1.4 retired oldC (persisting sourceFnv1a), then equal-size newC (same parent & row count) replaced childPath BEFORE force runs on P.
  {
    const h = exitReplayHarness()
    const parentName = 'retry-20261013T200102001Z-t53-16ad-a-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16ad-a-child.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldC = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    const newC = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) })],
      parentName,
    )[0]!
    expect(utf8Bytes(newC)).toBe(utf8Bytes(oldC))
    h.files.set(childPath, oldC)
    h.setFetch(async () => OK_INSERT)
    // First retire oldC via 0.1.4 so its retired done mark carries sourceFnv1a = fnv1a(oldC).
    await h.newWriter('s-16ad-a0').backfill(['spill'], 'auto')
    expect(h.sentOf(rowB)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    const cDone = h.doneMarksFor(childName).find(m => m.complete && m.data?.retired === true)
    expect(cDone?.data?.sourceFnv1a).toBe(fnv1a(oldC))

    // Equal-size replacement newC now occupies childPath, and forced replay runs on P.
    h.files.set(childPath, newC)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [childName], written: [true] })
    await h.newWriter('s-16ad-a1').backfill(['spill', 'retry'], 'force')
    await h.newWriter('s-16ad-a2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowA)).toBe(1)
    expect(h.sentOf(rowD)).toBe(1)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    const pDone = h.doneMarksFor(parentName).find(m => m.complete && m.data?.retired === true)
    expect((pDone?.data?.reconciled as { name: string }[] | undefined) ?? []).toEqual([])
  }

  // Subcase B: legacy digest-less retired mark on childPath with equal-size replacement newC -> classifyChild MUST NOT fabricate fnv1a(newC) into P.reconciled or truncate newC.
  {
    const h = exitReplayHarness()
    const parentName = 'retry-20261013T200102001Z-t53-16ad-b-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16ad-b-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    h.files.set(
      parentPath,
      packFiles([
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowB) }),
      ])[0]!,
    )
    const childPath = `${PARTITION_DIR}/${childName}`
    const newC = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) })],
      parentName,
    )[0]!
    h.files.set(childPath, newC)
    const cSize = utf8Bytes(newC)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old013-${childName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ad-b1').backfill(['retry'], 'force')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe(newC)
    const pDone = h.doneMarksFor(parentName).find(m => m.complete && m.data?.retired === true)
    expect((pDone?.data?.reconciled as { name: string }[] | undefined) ?? []).toEqual([])
  }

  // Subcase C: equal-size swap between readBefore and readAfter inside classifyChild defers instead of reconciling the new bytes.
  {
    const h = exitReplayHarness()
    const parentName = 'retry-20261013T200102001Z-t53-16ad-c-parent.jsonl'
    const childName = 'spill-20261013T200102002Z-t53-16ad-c-child.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    h.files.set(
      parentPath,
      packFiles([
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowB) }),
      ])[0]!,
    )
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldC = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
      parentName,
    )[0]!
    const newC = packFiles(
      [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) })],
      parentName,
    )[0]!
    expect(utf8Bytes(newC)).toBe(utf8Bytes(oldC))
    h.files.set(childPath, oldC)
    const cDonePath = `${PARTITION_DIR}/outcome-done-oldc-${childName}`
    h.files.set(
      cDonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: utf8Bytes(oldC), sourceFnv1a: fnv1a(oldC), waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:${TABLE_REF}/${parentName}`, { fates: 'LL', reasons: [], refusals: [], size: pSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, { fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)

    let sawDone = false
    let swapped = false
    const baseIo = h.ioFor()
    const swapIo = {
      ...baseIo,
      readFile: async (p: string) => {
        if (p === cDonePath) sawDone = true
        if (p === childPath && sawDone && !swapped) {
          swapped = true
          h.files.set(childPath, newC)
        }
        return baseIo.readFile(p)
      },
    }
    await new Writer(swapIo, () => h.config, () => 's-16ad-c1').backfill(['retry'], 'force')
    expect(swapped).toBe(true)
    expect(h.files.get(childPath)).toBe(newC)
    await h.newWriter('s-16ad-c2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowD)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase D: equal-size generation 2 with stale v1(sourceFnv1a: D1) and active v2(sourceFnv1a: D2) disambiguates `'conflict'` via live digest and resumes v2 on auto.
  {
    const h = exitReplayHarness()
    const fileName = 'spill-20261013T200102001Z-t53-16ad-d-gen2.jsonl'
    const filePath = `${PARTITION_DIR}/${fileName}`
    const gen1 = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
    ])[0]!
    const gen2 = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowD) }),
    ])[0]!
    expect(utf8Bytes(gen2)).toBe(utf8Bytes(gen1))
    const sz = utf8Bytes(gen2)
    h.files.set(filePath, gen2)
    h.store.set(`replayed:${TABLE_REF}/${fileName}`, {
      fates: 'W',
      reasons: [],
      refusals: [],
      size: sz,
      sourceFnv1a: fnv1a(gen1),
      waits: true,
      seq: 2,
      files: ['retry-gen1-leftover.jsonl'],
      written: [true],
    })
    h.store.set(`replayed:v2:${TABLE_REF}/${fileName}`, {
      fates: 'L',
      reasons: [],
      refusals: [],
      size: sz,
      sourceFnv1a: fnv1a(gen2),
      waits: false,
      seq: 1,
      files: [],
      written: [],
    })
    h.setFetch(async () => OK_INSERT)
    const rAuto = await h.newWriter('s-16ad-d1').backfill(['spill'], 'auto')
    expect(rAuto.held).toBeNull()
    expect(h.sentOf(rowD)).toBe(0)
    expect(h.files.get(filePath)).toBe('')
  }

  // Subcase E: `'conflict'` involving a digest-less 0.1.3 v1 record (fates 'L') and a mismatched 0.1.4 v2 record (sourceFnv1a: D1) MUST stay `'conflict'` on equal-size gen2 (D2) instead of unmasking digest-less v1 to drop D2.
  {
    const h = exitReplayHarness()
    const fileName = 'spill-20261013T200102001Z-t53-16ad-e-conflict.jsonl'
    const filePath = `${PARTITION_DIR}/${fileName}`
    const gen1 = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
    ])[0]!
    const gen2 = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowD) }),
    ])[0]!
    expect(utf8Bytes(gen2)).toBe(utf8Bytes(gen1))
    const sz = utf8Bytes(gen2)
    h.files.set(filePath, gen2)
    h.store.set(`replayed:${TABLE_REF}/${fileName}`, { fates: 'L', reasons: [], refusals: [], size: sz, waits: false, seq: 1, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${fileName}`, {
      fates: 'W',
      reasons: [],
      refusals: [],
      size: sz,
      sourceFnv1a: fnv1a(gen1),
      waits: true,
      seq: 2,
      files: ['retry-gen1.jsonl'],
      written: [true],
    })
    h.setFetch(async () => OK_INSERT)
    const rAuto = await h.newWriter('s-16ad-e1').backfill(['spill'], 'auto')
    expect(rAuto.held ?? '').toContain('/bqaa-backfill force')
    expect(h.files.get(filePath)).toBe(gen2)
    await h.newWriter('s-16ad-e2').backfill(['spill'], 'force')
    expect(h.sentOf(rowD)).toBe(1)
    expect(h.files.get(filePath)).toBe('')
  }

  // Subcase F: genuine 0.1.3 (digest-less v1, fates 'L') vs 0.1.4 (v2 with sourceFnv1a === fnv1a(gen2), fates 'W', no supersedesV1) split-brain conflict on the SAME generation MUST stay `'conflict'` on auto instead of silently adopting v2.
  {
    const h = exitReplayHarness()
    const fileName = 'spill-20261013T200102001Z-t53-16ad-f-splitbrain.jsonl'
    const filePath = `${PARTITION_DIR}/${fileName}`
    const gen2 = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowD) }),
    ])[0]!
    const sz = utf8Bytes(gen2)
    h.files.set(filePath, gen2)
    h.store.set(`replayed:${TABLE_REF}/${fileName}`, { fates: 'L', reasons: [], refusals: [], size: sz, waits: false, seq: 2, files: [], written: [] })
    h.store.set(`replayed:v2:${TABLE_REF}/${fileName}`, {
      fates: 'W',
      reasons: [],
      refusals: [],
      size: sz,
      sourceFnv1a: fnv1a(gen2),
      waits: true,
      seq: 1,
      files: ['retry-gen2-v2.jsonl'],
      written: [false],
    })
    h.setFetch(async () => OK_INSERT)
    const rAuto = await h.newWriter('s-16ad-f1').backfill(['spill'], 'auto')
    expect(rAuto.held ?? '').toContain('/bqaa-backfill force')
    expect(h.files.get(filePath)).toBe(gen2)
    expect(h.sentOf(rowD)).toBe(0)
    await h.newWriter('s-16ad-f2').backfill(['spill'], 'force')
    expect(h.sentOf(rowD)).toBe(1)
    expect(h.files.get(filePath)).toBe('')
  }

  // Subcase G: equal-seq tie where a digest-less 0.1.3 done mark (`zdist`) sorts AFTER a 0.1.4 `sourceFnv1a` done mark (`acur`) MUST still prefer `acur` so equal-size `cNew` is delivered (both standalone `backfillRun` auto and `classifyChild` under parent `force`).
  {
    const rowNew = 'n'.repeat(32)
    // Arm 1: standalone auto replay on `cNew`.
    {
      const h = exitReplayHarness()
      const fileName = 'spill-20261013T200102001Z-t53-16ad-g1.jsonl'
      const filePath = `${PARTITION_DIR}/${fileName}`
      const cOld = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
      ])[0]!
      const cNew = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowNew) }),
      ])[0]!
      expect(utf8Bytes(cNew)).toBe(utf8Bytes(cOld))
      const sz = utf8Bytes(cNew)
      h.files.set(filePath, cNew)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-acur-${fileName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, sourceFnv1a: fnv1a(cOld), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-zdist-${fileName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.setFetch(async () => OK_INSERT)
      const r = await h.newWriter('s-16ad-g1').backfill(['spill'], 'auto')
      expect(r.held).toBeNull()
      expect(h.sentOf(rowNew)).toBe(1)
      expect(h.files.get(filePath)).toBe('')
    }

    // Arm 2: `classifyChild` under parent `force` with `zdist` sorting after `acur` at equal `seq: 2`.
    {
      const h = exitReplayHarness()
      const parentName = 'retry-20261013T200102001Z-t53-16ad-g2-parent.jsonl'
      const childName = 'spill-20261013T200102002Z-t53-16ad-g2-child.jsonl'
      const parentPath = `${PARTITION_DIR}/${parentName}`
      const childPath = `${PARTITION_DIR}/${childName}`
      h.files.set(
        parentPath,
        packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) })])[0]!,
      )
      const cOld = packFiles(
        [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) })],
        parentName,
      )[0]!
      const cNew = packFiles(
        [JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) })],
        parentName,
      )[0]!
      expect(utf8Bytes(cNew)).toBe(utf8Bytes(cOld))
      const sz = utf8Bytes(cNew)
      h.files.set(childPath, cNew)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-acur-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, sourceFnv1a: fnv1a(cOld), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-zdist-${childName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.setFetch(async () => OK_INSERT)
      await h.newWriter('s-16ad-g2-1').backfill(['retry'], 'force')
      await h.newWriter('s-16ad-g2-2').backfill(['spill', 'retry'], 'auto')
      expect(h.sentOf(rowA)).toBe(1)
      expect(h.sentOf(rowNew)).toBe(1)
      expect(h.files.get(parentPath)).toBe('')
      expect(h.files.get(childPath)).toBe('')
    }
  }

  // Subcase H: `parentFate` equal-seq tie where `zdist` (`files: []`) sorts AFTER `acur` (`sourceFnv1a`, `files: [C]`) beside emptied `P` MUST deliver `C` instead of dropping it, and refuse to drop an unlisted sibling `U`.
  {
    const rowChild = 'e'.repeat(32)
    const rowUnlisted = 'f'.repeat(32)
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ad-h-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ad-h-child.jsonl'
    const unlistedName = 'retry-20261013T200102003Z-t53-16ad-h-unlisted.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const unlistedPath = `${PARTITION_DIR}/${unlistedName}`
    const cText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowChild) })],
      parentName,
    )[0]!
    const uText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowUnlisted) })],
      parentName,
    )[0]!
    h.files.set(parentPath, '')
    h.files.set(childPath, cText)
    h.files.set(unlistedPath, uText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-acur-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'W', reasons: [], refusals: [], size: 256, sourceFnv1a: 'deadbeef', waits: true, seq: 2, files: [childName], written: [true], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-zdist-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 256, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.setFetch(async () => OK_INSERT)
    const r = await h.newWriter('s-16ad-h1').backfill(['retry'], 'auto')
    expect(h.sentOf(rowChild)).toBe(1)
    expect(h.files.get(childPath)).toBe('')
    // Unlisted child on a disagreeing equal-seq parent tie must stay held (ambiguous), never dropped unsent.
    expect(h.sentOf(rowUnlisted)).toBe(0)
    expect(h.files.get(unlistedPath)).toBe(uText)
    expect(r.held ?? '').toContain('/bqaa-backfill force')
  }

  // Subcase I: `seedSkippedHeld` MUST NOT suppress the held notice for an equal-size replacement `cNew` locked by a foreign sending mark just because `cOld` has a mismatched `sourceFnv1a` done mark (even alongside a same-seq digest-less `zdist` mark).
  {
    const rowNew = 'm'.repeat(32)
    const h = exitReplayHarness()
    const firstName = 'spill-20261013T200102001Z-t53-16ad-i-first.jsonl'
    const secondName = 'spill-20261013T200102002Z-t53-16ad-i-second.jsonl'
    const firstPath = h.seedSpill(firstName, rowA)
    const secondPath = `${PARTITION_DIR}/${secondName}`
    const cOld = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    const cNew = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) }),
    ])[0]!
    expect(utf8Bytes(cNew)).toBe(utf8Bytes(cOld))
    const sz = utf8Bytes(cNew)
    h.files.set(secondPath, cNew)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-acur-${secondName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, sourceFnv1a: fnv1a(cOld), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-zdist-${secondName}`,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: sz, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-other1-${secondName}`,
      sealFile(`${JSON.stringify({ size: sz, sending: true, uncertain: true, by: 'other1' })}\n`),
    )
    // Earlier file hits 503 UNAVAILABLE so the scan stops before `secondPath` and inspects `secondPath` via `seedSkippedHeld`.
    h.setFetch(async () => UNAVAILABLE)
    const w = h.newWriter('s-16ad-i1')
    const r = await w.backfill(['spill'], 'auto')
    expect(h.files.get(firstPath)).not.toBe('')
    expect(h.files.get(secondPath)).toBe(cNew)
    expect(r.held ?? w.heldNoticeInfo()?.reason ?? '').toContain('/bqaa-backfill force')
    expect(w.heldNoticeInfo()?.files).toBe(1)
  }

  // Subcase J: equal-size parent repopulation (`P_old` retired with `sourceFnv1a: H1, files: []` vs `P_new` `sourceFnv1a: H2` writing child `C_new`) MUST NOT let `parentFate` drop `C_new` (both while `P_new` is live and after `P_new` is emptied).
  {
    const rowX = 'x'.repeat(32)
    const rowY = 'y'.repeat(32)
    // Arm 1: `P_new` is live and leased while its v1 journal (`H2`, `written: [false]`) names `C_new`.
    {
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ad-j1-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ad-j1-child.jsonl'
      const parentPath = `${PARTITION_DIR}/${parentName}`
      const childPath = `${PARTITION_DIR}/${childName}`
      const pOld = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      ])[0]!
      const pNew = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowX) }),
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowY) }),
      ])[0]!
      const cNew = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowY) })],
        parentName,
      )[0]!
      expect(utf8Bytes(pNew)).toBe(utf8Bytes(pOld))
      const pSize = utf8Bytes(pNew)
      h.files.set(parentPath, pNew)
      h.files.set(childPath, cNew)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-old1d-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, sourceFnv1a: fnv1a(pOld), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.store.set(`replayed:${TABLE_REF}/${parentName}`, {
        fates: 'LW',
        reasons: [],
        refusals: [],
        size: pSize,
        sourceFnv1a: fnv1a(pNew),
        waits: true,
        seq: 1,
        files: [childName],
        written: [false],
      })
      h.store.set(`lease:${TABLE_REF}/${parentName}`, { by: 'other-session', until: h.ioFor().now() + 60_000 })
      h.setFetch(async () => OK_INSERT)
      await h.newWriter('s-16ad-j1-auto').backfill(['spill', 'retry'], 'auto')
      // `C_new` must NOT be dropped while `P_new` is live and `written[0] === false`.
      expect(h.files.get(childPath)).toBe(cNew)
      expect(h.sentOf(rowY)).toBe(0)
      // Once `P_new` finishes (`written: [true]` and emptied), `C_new` delivers rowY once.
      h.store.set(`replayed:${TABLE_REF}/${parentName}`, {
        fates: 'LW',
        reasons: [],
        refusals: [],
        size: pSize,
        sourceFnv1a: fnv1a(pNew),
        waits: true,
        seq: 1,
        files: [childName],
        written: [true],
      })
      h.files.set(parentPath, '')
      await h.newWriter('s-16ad-j1-finish').backfill(['spill', 'retry'], 'auto')
      expect(h.sentOf(rowY)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }

    // Arm 2: `P_new` already emptied while `P_old`'s retired done mark (`H1`, `files: []`) coexists with `P_new`'s `v1` record (`H2`, `files: [C_new]`).
    {
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ad-j2-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ad-j2-child.jsonl'
      const parentPath = `${PARTITION_DIR}/${parentName}`
      const childPath = `${PARTITION_DIR}/${childName}`
      const cNew = packFiles(
        [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowY) })],
        parentName,
      )[0]!
      h.files.set(parentPath, '')
      h.files.set(childPath, cNew)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-old1d-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, sourceFnv1a: '11111111', waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.store.set(`replayed:${TABLE_REF}/${parentName}`, {
        fates: 'LW',
        reasons: [],
        refusals: [],
        size: 512,
        sourceFnv1a: '22222222',
        waits: true,
        seq: 1,
        files: [childName],
        written: [true],
      })
      h.setFetch(async () => OK_INSERT)
      await h.newWriter('s-16ad-j2').backfill(['retry'], 'auto')
      expect(h.sentOf(rowY)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }
  }

  // Subcase K: equal-size repopulated live unretired `P_new` (`H2`) beside `P_old`'s retired done mark (`H1`) MUST keep guarded `C` and standalone backup `B_c` held on BOTH `auto` (no false parent-retired notice) and `force`.
  {
    const rowX = 'x'.repeat(32)
    const rowY = 'y'.repeat(32)
    const rowC = 'c'.repeat(32)
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ad-k-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ad-k-child.jsonl'
    const backupName = 'retry-20261013T200102003Z-t53-16ad-k-backup.jsonl'
    const parentPath = `${PARTITION_DIR}/${parentName}`
    const childPath = `${PARTITION_DIR}/${childName}`
    const backupPath = `${PARTITION_DIR}/${backupName}`
    const pOld = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowA) }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    const pNew = packFiles([
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: ownRow(rowX) }),
      JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowY) }),
    ])[0]!
    const cText = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parentName,
    )[0]!
    const bText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    expect(utf8Bytes(pNew)).toBe(utf8Bytes(pOld))
    const pSize = utf8Bytes(pNew)
    h.files.set(parentPath, pNew)
    h.files.set(childPath, cText)
    h.files.set(backupPath, bText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-old1d-${parentName}`,
      sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: pSize, sourceFnv1a: fnv1a(pOld), waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-cur2g-${childName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(cText), sending: true, by: 'cur2g', stagedFor: parentName, stagedForSize: pSize, childGuard: true, attempt: 'A2' })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-cur2g-${backupName}`,
      sealFile(
        `${JSON.stringify({ size: utf8Bytes(bText), sending: true, by: 'cur2g', stagedFor: parentName, stagedForSize: pSize, replaces: childName, backup: true, backupForSize: utf8Bytes(cText), backupForFnv1a: fnv1a(cText), backupFnv1a: fnv1a(bText), attempt: 'A2' })}\n`,
      ),
    )
    h.setFetch(async () => OK_INSERT)
    const rAuto = await h.newWriter('s-16ad-k1').backfill(['retry'], 'auto')
    expect(rAuto.held ?? '').not.toContain('retired without accounting')
    expect(rAuto.held ?? '').not.toContain('/bqaa-backfill force')
    expect(h.sentOf(rowC)).toBe(0)
    await h.newWriter('s-16ad-k2').backfill(['retry'], 'force')
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(childPath)).toBe(cText)
    expect(h.files.get(backupPath)).toBe(bText)
  }

  // Subcase L: (1) `seedSkippedHeld` validates `sourceFnv1a` on unretired `v2` slots even with 0 done marks; (2) `parentFate` 3-mark equal-seq tie preserves merged `files` when a 3rd mark matches the winning digest; (3) transient `readFile` error in `stagedGuardState` `pendingReconcile` returns `'hold'` so neither `auto` emits a false parent-retired notice nor `force` bypasses `skipIndices`.
  {
    const rowNew = 'q'.repeat(32)
    // Arm 1: 0 done marks + stale unretired v2 (`sourceFnv1a: cOld`) + foreign sending mark on equal-size `cNew` -> `seedSkippedHeld` reports held notice.
    {
      const h = exitReplayHarness()
      const firstName = 'spill-20261013T200102001Z-t53-16ad-l1-first.jsonl'
      const secondName = 'spill-20261013T200102002Z-t53-16ad-l1-second.jsonl'
      h.seedSpill(firstName, rowA)
      const secondPath = `${PARTITION_DIR}/${secondName}`
      const cOld = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      ])[0]!
      const cNew = packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowNew) }),
      ])[0]!
      expect(utf8Bytes(cNew)).toBe(utf8Bytes(cOld))
      const sz = utf8Bytes(cNew)
      h.files.set(secondPath, cNew)
      h.store.set(`replayed:v2:${TABLE_REF}/${secondName}`, {
        fates: 'L',
        reasons: [],
        refusals: [],
        size: sz,
        sourceFnv1a: fnv1a(cOld),
        waits: false,
        seq: 1,
        files: [],
        written: [],
      })
      h.files.set(
        `${PARTITION_DIR}/outcome-done-other1-${secondName}`,
        sealFile(`${JSON.stringify({ size: sz, sending: true, uncertain: true, by: 'other1' })}\n`),
      )
      h.setFetch(async () => UNAVAILABLE)
      const w = h.newWriter('s-16ad-l1')
      const r = await w.backfill(['spill'], 'auto')
      expect(r.held ?? w.heldNoticeInfo()?.reason ?? '').toContain('/bqaa-backfill force')
      expect(w.heldNoticeInfo()?.files).toBe(1)
    }

    // Arm 2: 3-mark equal-seq tie `acur1 (D1, [])` + `bdist (no digest, [C])` + `zcur1b (D1, [])` keeps `C` in merged files and delivers it 1x.
    {
      const rowChild = 'r'.repeat(32)
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ad-l2-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ad-l2-child.jsonl'
      h.files.set(`${PARTITION_DIR}/${parentName}`, '')
      const childPath = `${PARTITION_DIR}/${childName}`
      h.files.set(
        childPath,
        packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowChild) })], parentName)[0]!,
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-acur1-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 256, sourceFnv1a: '11111111', waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-bdist-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'W', reasons: [], refusals: [], size: 256, waits: true, seq: 2, files: [childName], written: [true], retired: true, retiredAt: 1000 })}\n`),
      )
      h.files.set(
        `${PARTITION_DIR}/outcome-done-zcur1b-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 256, sourceFnv1a: '11111111', waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.setFetch(async () => OK_INSERT)
      await h.newWriter('s-16ad-l2').backfill(['retry'], 'auto')
      expect(h.sentOf(rowChild)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }

    // Arm 3: transient `readFile` error in `stagedGuardState` `pendingReconcile` returns `'hold'` (no false parent-retired notice on `auto` and no un-skipped duplicate delivery on `force` when `!stagedGuardClearable`).
    {
      const rowC = 'c'.repeat(32)
      const h = exitReplayHarness()
      const parentName = 'spill-20261013T200102001Z-t53-16ad-l3-parent.jsonl'
      const childName = 'retry-20261013T200102002Z-t53-16ad-l3-child.jsonl'
      h.files.set(`${PARTITION_DIR}/${parentName}`, '')
      const childPath = `${PARTITION_DIR}/${childName}`
      const cText = packFiles(
        [
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
        ],
        parentName,
      )[0]!
      const cSize = utf8Bytes(cText)
      h.files.set(childPath, cText)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
        sealFile(`${JSON.stringify({ fates: 'LL', reasons: [], refusals: [], size: 512, waits: false, seq: 2, files: [], written: [], retired: true, retiredAt: 1000 })}\n`),
      )
      h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
        fates: 'LL',
        reasons: [],
        refusals: [],
        size: 512,
        waits: false,
        seq: 5,
        files: [],
        written: [],
        attempt: 'A1',
        attempts: ['A1'],
        pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(cText), skipIndices: [0] }],
      })
      h.files.set(
        `${PARTITION_DIR}/outcome-done-cur1g-${childName}`,
        sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'cur1g', stagedFor: parentName, stagedForSize: 512, childGuard: true, attempt: 'A1' })}\n`),
      )
      // Additional foreign non-staged sending mark makes `stagedGuardClearable === false`.
      h.files.set(
        `${PARTITION_DIR}/outcome-done-foreign1-${childName}`,
        sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'foreign1' })}\n`),
      )
      h.setFetch(async () => OK_INSERT)
      let failChildRead = true
      const baseIo = h.ioFor()
      const flakyIo = {
        ...baseIo,
        readFile: async (p: string) => {
          if (p === childPath && failChildRead) {
            failChildRead = false
            throw new Error('EIO transient')
          }
          return baseIo.readFile(p)
        },
      }
      // On auto: transient EIO holds cleanly without false parent-retired notice.
      const rAuto = await new Writer(flakyIo, () => h.config, () => 's-16ad-l3-auto').backfill(['retry'], 'auto')
      expect(rAuto.held ?? '').not.toContain('retired without accounting')
      expect(rAuto.held ?? '').not.toContain('/bqaa-backfill force')
      expect(h.sent.length).toBe(0)
      // On force with transient EIO on `checkTargetDigest`: MUST hold (`0` rows sent, never bypass `skipIndices` to duplicate rowB).
      failChildRead = true
      await new Writer(flakyIo, () => h.config, () => 's-16ad-l3-force1').backfill(['retry'], 'force')
      expect(h.sent.length).toBe(0)
      // Clean force after EIO clears skips index 0 (rowB) and delivers only rowC once.
      await h.newWriter('s-16ad-l3-force2').backfill(['retry'], 'force')
      expect(h.sentOf(rowB)).toBe(0)
      expect(h.sentOf(rowC)).toBe(1)
      expect(h.files.get(childPath)).toBe('')
    }
  }
})

test('T53-16ae: positional skipIndices are revalidated against the replayed buffer digest so an equal-size swap never skips an undelivered row', async () => {
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const rowD = 'd'.repeat(32)
  const rowE = 'e'.repeat(32)
  const h = exitReplayHarness()
  const parentName = 'spill-20261013T200102001Z-t53-16ae-parent.jsonl'
  const childName = 'retry-20261013T200102002Z-t53-16ae-child.jsonl'
  const parentPath = `${PARTITION_DIR}/${parentName}`
  const childPath = `${PARTITION_DIR}/${childName}`
  const oldC = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ],
    parentName,
  )[0]!
  const swappedC = packFiles(
    [
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowD) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowE) }),
    ],
    parentName,
  )[0]!
  expect(utf8Bytes(swappedC)).toBe(utf8Bytes(oldC))
  const pSize = 700
  const cSize = utf8Bytes(oldC)
  h.files.set(parentPath, '')
  h.files.set(childPath, oldC)
  h.files.set(
    `${PARTITION_DIR}/outcome-done-old013-${parentName}`,
    sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true })}\n`),
  )
  h.files.set(
    `${PARTITION_DIR}/outcome-done-w14g-${childName}`,
    sealFile(`${JSON.stringify({ size: cSize, sending: true, by: 'w14g', stagedFor: parentName, stagedForSize: pSize, childGuard: true, attempt: 'att-ae' })}\n`),
  )
  h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
    fates: 'LL',
    reasons: [],
    refusals: [],
    size: pSize,
    waits: false,
    seq: 5,
    files: [],
    written: [],
    attempt: 'att-ae',
    attempts: ['att-ae'],
    pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(oldC), skipIndices: [0] }],
  })
  h.setFetch(async () => OK_INSERT)

  // First read of childPath is inside stagedGuardState (sees oldC -> returns skipIndices:[0], skipFnv1a: fnv1a(oldC));
  // swap childPath to equal-size swappedC BEFORE backfillRun's second read!
  let childReads = 0
  const baseIo = h.ioFor()
  const swapIo = {
    ...baseIo,
    readFile: async (p: string) => {
      if (p === childPath) {
        childReads += 1
        if (childReads === 2) h.files.set(childPath, swappedC)
      }
      return baseIo.readFile(p)
    },
  }
  await new Writer(swapIo, () => h.config, () => 's-16ae').backfill(['retry'], 'auto')
  expect(childReads >= 2).toBe(true)
  // Digest revalidation clears positional skipIndices so BOTH rowD (index 0) and rowE (index 1) are delivered!
  expect(h.sentOf(rowD)).toBe(1)
  expect(h.sentOf(rowE)).toBe(1)
  expect(h.files.get(childPath)).toBe('')
})

test('T53-16af: abandoned staged-part cleanup checks byte digest and foreign leases on both retry and retirement paths', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)
  const rowForeign = 'f'.repeat(32)

  // Subcase A: retry path (cleanupStaged) and retirement path (cleanupReconciledAndStaged) leave another writer's live file untouched on digest mismatch (equal-size and different-size).
  for (const mode of ['retry-equal', 'retry-diff', 'retire-equal'] as const) {
    const h = exitReplayHarness()
    const parentName = `spill-20261013T200102001Z-t53-16af-${mode}-parent.jsonl`
    const childName = `retry-20261013T200102002Z-t53-16af-${mode}-child.jsonl`
    const stagedName = `retry-20261013T200102003Z-t53-16af-${mode}-staged.jsonl`
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const stagedPath = `${PARTITION_DIR}/${stagedName}`
    const oldStagedText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const foreignText =
      mode === 'retry-diff'
        ? packFiles([
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowForeign) }),
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.004Z', table: TABLE_REF, row: ownRow('e'.repeat(32)) }),
          ])[0]!
        : packFiles([
            JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowForeign) }),
          ])[0]!
    if (mode !== 'retry-diff') expect(utf8Bytes(foreignText)).toBe(utf8Bytes(oldStagedText))
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.files.set(stagedPath, foreignText)
    if (mode === 'retire-equal') {
      // No pendingReconcile, already retired-ready except for abandoned stagedFiles entry -> hits retirement cleanupReconciledAndStaged directly.
      h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
        fates: 'LL',
        reasons: [],
        refusals: [],
        size: pSize,
        waits: true,
        seq: 5,
        files: [],
        written: [],
        stagedFiles: [{ name: stagedName, size: utf8Bytes(oldStagedText), fnv1a: fnv1a(oldStagedText) }],
      })
    } else {
      const cText = packFiles(
        [
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
          JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
        ],
        parentName,
      )[0]!
      h.files.set(childPath, cText)
      h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
        fates: 'LL',
        reasons: [],
        refusals: [],
        size: pSize,
        waits: true,
        seq: 5,
        files: [],
        written: [],
        pendingReconcile: [{ name: childName, size: utf8Bytes(cText), fnv1a: fnv1a(cText), skipIndices: [0] }],
        stagedFiles: [{ name: stagedName, size: utf8Bytes(oldStagedText), fnv1a: fnv1a(oldStagedText) }],
      })
    }
    h.setFetch(async () => OK_INSERT)
    // Run spill only first so P cleans up stagedFiles and retires without replaying stagedPath yet.
    await h.newWriter(`s-16af-${mode}-1`).backfill(['spill'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    // Foreign live file at stagedPath MUST be completely untouched!
    expect(h.files.get(stagedPath)).toBe(foreignText)

    // Next auto delivers foreignText cleanly!
    await h.newWriter(`s-16af-${mode}-2`).backfill(['retry'], 'auto')
    expect(h.sentOf(rowForeign)).toBe(1)
    if (mode !== 'retire-equal') expect(h.sentOf(rowC)).toBe(1)
    expect(h.files.get(stagedPath)).toBe('')
  }

  // Subcase B: matching abandoned staged part held by an unexpired foreign lease is NOT truncated during retry or retirement until the lease expires.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16af-lease-parent.jsonl'
    const stagedName = 'retry-20261013T200102003Z-t53-16af-lease-staged.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const stagedPath = `${PARTITION_DIR}/${stagedName}`
    const stagedText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    h.files.set(stagedPath, stagedText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14-${stagedName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(stagedText), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [],
      written: [],
      stagedFiles: [{ name: stagedName, size: utf8Bytes(stagedText), fnv1a: fnv1a(stagedText) }],
    })
    h.store.set(`lease:${TABLE_REF}/${stagedName}`, { by: 'other-session', until: h.ioFor().now() + 5 * 60_000 })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16af-lease-1').backfill(['spill'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    // Still held by foreign lease: NOT truncated, and v2 key kept so auto drops it once lease expires.
    expect(h.files.get(stagedPath)).toBe(stagedText)
    expect(h.store.get(`replayed:v2:${TABLE_REF}/${parentName}`)).toBeDefined()

    await h.advance(10 * 60_000)
    await h.newWriter('s-16af-lease-2').backfill(['retry'], 'auto')
    expect(h.files.get(stagedPath)).toBe('')
    expect(h.sentOf(rowC)).toBe(0)
  }
})

test('T53-16ag: retention protects stagedFor/replaces in live marks and confirmDirStable catches same-size re-parented children', async () => {
  const rowC = 'c'.repeat(32)

  // Subcase A (Item 6): after 25 hours with standalone replacement R_c leased, retention MUST preserve BOTH parent P and replaced sibling C's done mark so duplicate row C is still suppressed once lease expires.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ag-a-parent.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ag-a-child.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16ag-a-repl.jsonl'
    const replPath = `${PARTITION_DIR}/${replName}`
    const replText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const pSize = 700
    const cSize = 350
    const oldTs = 1_000
    h.files.set(`${PARTITION_DIR}/${parentName}`, '')
    h.files.set(`${PARTITION_DIR}/${childName}`, '')
    h.files.set(replPath, replText)
    const pDonePath = `${PARTITION_DIR}/outcome-done-old013-${parentName}`
    const cDonePath = `${PARTITION_DIR}/outcome-done-oldc-${childName}`
    h.files.set(
      pDonePath,
      sealFile(`${JSON.stringify({ fates: 'LW', reasons: [], refusals: [], size: pSize, waits: true, seq: 2, files: [childName], written: [true], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      cDonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: cSize, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      `${PARTITION_DIR}/outcome-done-w14-${replName}`,
      sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 'w14', stagedFor: parentName, stagedForSize: pSize, replaces: childName })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      waits: true,
      seq: 5,
      files: [replName],
      written: [true],
      adopted: [replName],
      reconciled: [{ name: childName, size: cSize, fnv1a: '12345678' }],
    })
    h.mtimes.set(pDonePath, oldTs)
    h.mtimes.set(cDonePath, oldTs)
    h.advanceNow(25 * 60 * 60_000)
    // Replacement is currently leased by another session while retention runs after 25h.
    h.store.set(`lease:${TABLE_REF}/${replName}`, { by: 'other-session', until: h.ioFor().now() + 5 * 60_000 })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ag-a1').backfill(['spill', 'retry'], 'auto')
    // BOTH parent P's done mark/v2 AND replaced sibling C's done mark MUST survive retention!
    expect(h.files.get(pDonePath)).not.toBe('')
    expect(h.files.get(cDonePath)).not.toBe('')
    expect(h.store.get(`replayed:v2:${TABLE_REF}/${parentName}`)).toBeDefined()

    // Once lease expires, stagedGuardState sees C's preserved done mark and drops R_c (0 duplicates!).
    await h.advance(10 * 60_000)
    await h.newWriter('s-16ag-a2').backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(replPath)).toBe('')
    expect(h.sentOf(rowC)).toBe(0)
  }

  // Subcase B (Item 7): confirmDirStable catches a live child switching parent seal to parent2 at the exact same byte size and mtimeMs between freshListed and parent2's deletion.
  {
    const h = exitReplayHarness()
    const parent1 = 'spill-20261013T200102001Z-t53-16ag-p1.jsonl'
    const parent2 = 'spill-20261013T200102002Z-t53-16ag-p2.jsonl'
    const childName = 'retry-20261013T200102003Z-t53-16ag-child.jsonl'
    const childPath = `${PARTITION_DIR}/${childName}`
    const p1DonePath = `${PARTITION_DIR}/outcome-done-old1-${parent1}`
    const p2DonePath = `${PARTITION_DIR}/outcome-done-old2-${parent2}`
    const oldTs = 1_000
    const childForP1 = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parent1,
    )[0]!
    const childForP2 = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parent2,
    )[0]!
    expect(utf8Bytes(childForP2)).toBe(utf8Bytes(childForP1))
    h.files.set(`${PARTITION_DIR}/${parent1}`, '')
    h.files.set(`${PARTITION_DIR}/${parent2}`, '')
    h.files.set(childPath, childForP1)
    h.mtimes.set(childPath, 50_000)
    // Add an unrelated expired parent0 (sorts before parent2) whose deletion runs first so parent2 is deleted second.
    const parent0 = 'spill-20261013T200102000Z-t53-16ag-p0.jsonl'
    const p0DonePath = `${PARTITION_DIR}/outcome-done-old0-${parent0}`
    h.files.set(`${PARTITION_DIR}/${parent0}`, '')
    h.files.set(
      p0DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      p1DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      p2DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.store.set(`replayed:v2:${TABLE_REF}/${parent2}`, { fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true })
    h.mtimes.set(p0DonePath, oldTs)
    h.mtimes.set(p1DonePath, oldTs)
    h.mtimes.set(p2DonePath, oldTs)
    h.advanceNow(25 * 60 * 60_000)
    // Put childPath inside the racy-clean coarse-mtime window (`mtimeMs === now`),
    // and keep it leased so retention runs while childPath stays live.
    const racyNow = h.ioFor().now()
    h.mtimes.set(childPath, racyNow)
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: racyNow + 5 * 60_000 })

    // Right as parent0's done mark is truncated, overwrite childPath with same-size childForP2 at the EXACT same racy-window mtimeMs!
    let switched = false
    h.setWriteFileHook(async (p, text) => {
      if (p === p0DonePath && text === '' && !switched) {
        switched = true
        h.files.set(childPath, childForP2)
        h.mtimes.set(childPath, racyNow)
      }
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ag-b1').backfill(['spill', 'retry'], 'auto')
    expect(switched).toBe(true)
    // confirmDirStable(parent2) MUST re-read childPath's seal and preserve BOTH parent2's done mark AND store record!
    expect(h.files.get(p2DonePath)).not.toBe('')
    expect(h.store.get(`replayed:v2:${TABLE_REF}/${parent2}`)).toBeDefined()

    // Once lease expires, parent2's preserved done mark (files: []) drops childForP2 cleanly without resending rowC!
    h.setWriteFileHook(null)
    await h.advance(10 * 60_000)
    await h.newWriter('s-16ag-b2').backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(0)
    expect(h.files.get(childPath)).toBe('')
  }

  // Subcase C (Item 7 mark variant): confirmDirStable catches a live file's companion mark switching `replaces` to parent2 at the exact same size and mtimeMs.
  {
    const h = exitReplayHarness()
    const parent0 = 'spill-20261013T200102000Z-t53-16ag-c-p0.jsonl'
    const parent1 = 'spill-20261013T200102001Z-t53-16ag-c-p1.jsonl'
    const parent2 = 'spill-20261013T200102002Z-t53-16ag-c-p2.jsonl'
    const replName = 'retry-20261013T200102003Z-t53-16ag-c-repl.jsonl'
    const replPath = `${PARTITION_DIR}/${replName}`
    const replMarkPath = `${PARTITION_DIR}/outcome-done-w14-${replName}`
    const replText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    const markForP1 = sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 'w14', stagedFor: parent1, stagedForSize: 200, replaces: parent1 })}\n`)
    const markForP2 = sealFile(`${JSON.stringify({ size: utf8Bytes(replText), sending: true, by: 'w14', stagedFor: parent1, stagedForSize: 200, replaces: parent2 })}\n`)
    expect(utf8Bytes(markForP2)).toBe(utf8Bytes(markForP1))
    const oldTs = 1_000
    const p0DonePath = `${PARTITION_DIR}/outcome-done-old0-${parent0}`
    const p2DonePath = `${PARTITION_DIR}/outcome-done-old2-${parent2}`
    h.files.set(`${PARTITION_DIR}/${parent0}`, '')
    h.files.set(`${PARTITION_DIR}/${parent1}`, '')
    h.files.set(`${PARTITION_DIR}/${parent2}`, '')
    h.files.set(replPath, replText)
    h.files.set(replMarkPath, markForP1)
    h.mtimes.set(replPath, 50_000)
    h.mtimes.set(replMarkPath, 50_000)
    h.files.set(
      p0DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      p2DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.mtimes.set(p0DonePath, oldTs)
    h.mtimes.set(p2DonePath, oldTs)
    h.advanceNow(25 * 60 * 60_000)
    const racyNowC = h.ioFor().now()
    h.mtimes.set(replMarkPath, racyNowC)
    h.store.set(`lease:${TABLE_REF}/${replName}`, { by: 'other-session', until: racyNowC + 5 * 60_000 })

    let switched = false
    h.setWriteFileHook(async (p, text) => {
      if (p === p0DonePath && text === '' && !switched) {
        switched = true
        h.files.set(replMarkPath, markForP2)
        h.mtimes.set(replMarkPath, racyNowC)
      }
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ag-c1').backfill(['spill', 'retry'], 'auto')
    expect(switched).toBe(true)
    expect(h.files.get(p2DonePath)).not.toBe('')
  }

  // Subcase D (performance): confirmDirStable skips redundant re-reads of settled live files across multiple expired parents.
  {
    const h = exitReplayHarness()
    const oldTs = 1_000
    const settledTs = 10_000
    // Seed 5 expired parents and 6 settled leased live files.
    const expiredParents: string[] = []
    for (let i = 0; i < 5; i += 1) {
      const pName = `spill-20261013T20010200${i}Z-t53-16ag-d-p${i}.jsonl`
      const dPath = `${PARTITION_DIR}/outcome-done-old${i}-${pName}`
      h.files.set(`${PARTITION_DIR}/${pName}`, '')
      h.files.set(
        dPath,
        sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
      )
      h.mtimes.set(dPath, oldTs)
      expiredParents.push(dPath)
    }
    const liveNames: string[] = []
    for (let i = 0; i < 6; i += 1) {
      const lName = `retry-20261013T20010201${i}Z-t53-16ag-d-live${i}.jsonl`
      const lPath = `${PARTITION_DIR}/${lName}`
      h.files.set(
        lPath,
        packFiles([JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })])[0]!,
      )
      h.mtimes.set(lPath, settledTs)
      liveNames.push(lName)
    }
    h.advanceNow(25 * 60 * 60_000)
    const now = h.ioFor().now()
    for (const lName of liveNames) {
      h.store.set(`lease:${TABLE_REF}/${lName}`, { by: 'other-session', until: now + 5 * 60_000 })
    }
    let liveReads = 0
    const baseIo = h.ioFor()
    const countingIo: Io = {
      ...baseIo,
      readFile: async p => {
        if (liveNames.some(n => p.endsWith(`/${n}`))) liveReads += 1
        return baseIo.readFile(p)
      },
    }
    const w = new Writer(countingIo, () => h.config, () => 's-16ag-d1')
    await w.backfill(['spill', 'retry'], 'auto')
    for (const dPath of expiredParents) expect(h.files.get(dPath)).toBe('')
    // Must read each settled live file at most once (1 * 6 = 6), NOT (5 + 1) * 6 = 36!
    expect(liveReads).toBe(liveNames.length)
  }

  // Subcase E (racy-window clock advancement): if a live file or mark was inside the
  // 2s coarse-mtime racy window at `initialListedAt` / `freshListedAt`, advancing the
  // clock by >= 2s during slow HTTP/deletion I/O MUST NOT promote it to settled cache!
  {
    const h = exitReplayHarness()
    const parent0 = 'spill-20261013T200102000Z-t53-16ag-e-p0.jsonl'
    const parent1 = 'spill-20261013T200102001Z-t53-16ag-e-p1.jsonl'
    const parent2 = 'spill-20261013T200102002Z-t53-16ag-e-p2.jsonl'
    const childName = 'retry-20261013T200102003Z-t53-16ag-e-child.jsonl'
    const childPath = `${PARTITION_DIR}/${childName}`
    const oldTs = 1_000
    const childForP1 = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parent1,
    )[0]!
    const childForP2 = packFiles(
      [JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) })],
      parent2,
    )[0]!
    expect(utf8Bytes(childForP2)).toBe(utf8Bytes(childForP1))
    const p0DonePath = `${PARTITION_DIR}/outcome-done-old0-${parent0}`
    const p2DonePath = `${PARTITION_DIR}/outcome-done-old2-${parent2}`
    h.files.set(`${PARTITION_DIR}/${parent0}`, '')
    h.files.set(`${PARTITION_DIR}/${parent1}`, '')
    h.files.set(`${PARTITION_DIR}/${parent2}`, '')
    h.files.set(childPath, childForP1)
    h.files.set(
      p0DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.files.set(
      p2DonePath,
      sealFile(`${JSON.stringify({ fates: 'L', reasons: [], refusals: [], size: 200, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: oldTs })}\n`),
    )
    h.mtimes.set(p0DonePath, oldTs)
    h.mtimes.set(p2DonePath, oldTs)
    h.advanceNow(25 * 60 * 60_000)
    const racyTick = h.ioFor().now()
    h.mtimes.set(childPath, racyTick)
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: racyTick + 5 * 60_000 })

    // When parent0's done mark is cleared, overwrite childPath at the SAME coarse mtime
    // AND advance wall clock by +2500ms so `nowCheck - racyTick >= 2000`.
    let switched = false
    h.setWriteFileHook(async (p, text) => {
      if (p === p0DonePath && text === '' && !switched) {
        switched = true
        h.files.set(childPath, childForP2)
        h.mtimes.set(childPath, racyTick)
        h.advanceNow(2_500)
      }
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ag-e1').backfill(['spill', 'retry'], 'auto')
    expect(switched).toBe(true)
    expect(h.files.get(p2DonePath)).not.toBe('')
  }
})

test('T53-16ah: on-disk backup dedup, legacy-cover strictness, safeTruncateStaged TOCTOU, and ID-less fallback insertId', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase 1: Repeated crash-before-record force runs reuse the intact on-disk
  // standalone backup instead of multiplying backups (1, 2, 3...), and final
  // retirement cleans up the single reused backup cleanly.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ah-dedup-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ah-dedup-c.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const v1Key = `replayed:${TABLE_REF}/${parentName}`
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v1Key, { fates: 'LL', reasons: [], refusals: [], size: parentSize, waits: false, seq: 2, files: [], written: [] })
    h.store.set(v2Key, { fates: 'WW', reasons: [], refusals: [], size: parentSize, waits: true, seq: 9, files: [childName], written: [true] })
    h.setFetch(async () => OK_INSERT)
    h.setStoreSetHook(async k => {
      if (k === v1Key || k === v2Key) throw new Error('crash before record')
    })
    h.setStoreDeleteHook(async k => {
      if (k === v1Key) throw new Error('v1 stuck')
    })
    h.setWriteFileHook(async p => {
      if (p.includes('/outcome2-')) throw new Error('crash before record file')
    })

    const countLiveBackups = (): number =>
      [...h.files.entries()].filter(([p, t]) => p.includes('/retry-') && p !== childPath && t !== '').length

    for (let i = 1; i <= 3; i += 1) {
      await h.newWriter(`s-16ah-dedup-${i}`).backfill(['spill', 'retry'], 'force')
      expect(countLiveBackups()).toBe(1)
      await h.advance(15 * 60_000)
    }

    // Clear faults: force run #4 reuses the single on-disk backup, reconciles C, and empties it cleanly.
    h.setStoreSetHook(null)
    h.setStoreDeleteHook(null)
    h.setWriteFileHook(null)
    await h.newWriter('s-16ah-dedup-4').backfill(['spill', 'retry'], 'force')
    await h.newWriter('s-16ah-dedup-5').backfill(['spill', 'retry'], 'auto')
    expect(countLiveBackups()).toBe(0)
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(h.sentOf(rowC)).toBe(1)
  }

  // Subcase 2: Legacy backup entry lacking `replacesFnv1a` does NOT suppress staging a fresh exact-digest backup.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ah-leg-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ah-leg-c.jsonl'
    const bogusBackupName = 'retry-20261013T200102003Z-t53-16ah-leg-bogus.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    const childSize = utf8Bytes(childText)
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    // Pre-existing v2 resume checkpoint with a legacy digest-less backup entry (no replacesFnv1a).
    h.store.set(v2Key, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: parentSize,
      waits: true,
      seq: 5,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: childSize, fnv1a: fnv1a(childText) }],
      backupFiles: [{ name: bogusBackupName, size: 10, fnv1a: 'deadbeef', replaces: childName, replacesSize: childSize }],
    })
    // Crash in Phase 3 after stageChildBackups runs in writeOut so we can observe the newly staged real backup.
    let v2Sets = 0
    h.setStoreSetHook(async k => {
      if (k === v2Key) {
        v2Sets += 1
        if (v2Sets >= 1) throw new Error('stop after staging backup')
      }
    })
    h.setWriteFileHook(async p => {
      if (p.includes('/outcome2-')) throw new Error('stop after staging backup file')
    })
    await h.newWriter('s-16ah-leg-1').backfill(['spill', 'retry'], 'auto')
    const realBackups = [...h.files.entries()].filter(
      ([p, t]) => p.includes('/retry-') && p !== childPath && p !== `${PARTITION_DIR}/${bogusBackupName}` && t !== '',
    )
    expect(realBackups.length).toBe(1)
  }

  // Subcase 3: safeTruncateStaged TOCTOU — an equal-length foreign file swapped in
  // during storeGet('lease:...') must NEVER be truncated.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ah-toctou-p.jsonl'
    const stagedName = 'retry-20261013T200102002Z-t53-16ah-toctou-s.jsonl'
    const parentPath = h.seedSpill(parentName, rowA)
    const stagedPath = `${PARTITION_DIR}/${stagedName}`
    const oldStaged = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
    ])[0]!
    const foreignEqual = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    expect(utf8Bytes(foreignEqual)).toBe(utf8Bytes(oldStaged))
    h.files.set(stagedPath, oldStaged)
    const parentSize = utf8Bytes(h.files.get(parentPath)!)
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'L',
      reasons: [],
      refusals: [],
      size: parentSize,
      waits: true,
      seq: 5,
      files: [],
      written: [],
      stagedFiles: [{ name: stagedName, size: utf8Bytes(oldStaged), fnv1a: fnv1a(oldStaged) }],
    })
    let swapped = false
    h.setStoreGetHook(async k => {
      if (k === `lease:${TABLE_REF}/${stagedName}` && !swapped) {
        swapped = true
        h.files.set(stagedPath, foreignEqual)
      }
    })
    h.setFetch(async () => OK_INSERT)

    await h.newWriter('s-16ah-toctou-1').backfill(['spill'], 'auto')
    expect(swapped).toBe(true)
    // Foreign equal-length replacement swapped in during lease lookup MUST survive untouched!
    expect(h.files.get(stagedPath)).toBe(foreignEqual)
  }

  // Subcase 4: Direct replay of an ID-less row sends a deterministic fallback insertId + event_id.
  {
    const h = exitReplayHarness()
    const fileNameIdless = 'spill-20261013T200102001Z-t53-16ah-idless.jsonl'
    const filePath = `${PARTITION_DIR}/${fileNameIdless}`
    const noIdRow = { ...ownRow(''), event_id: '' }
    delete (noIdRow as Record<string, unknown>).event_id
    h.files.set(
      filePath,
      packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: noIdRow }),
      ])[0]!,
    )
    const seenInsertIds: string[] = []
    h.setFetch(async (_url, ids) => {
      seenInsertIds.push(...ids)
      return OK_INSERT
    })
    await h.newWriter('s-16ah-idless-1').backfill(['spill'], 'auto')
    expect(seenInsertIds).toEqual([`dl:${fileNameIdless}#0`])
    expect(h.files.get(filePath)).toBe('')
  }

  // Subcase 5: Multibyte non-ASCII dead-letter key (>120 UTF-8 bytes despite <=120 UTF-16 code units)
  // stays within BigQuery's 128-byte insertId limit with valid UTF-16 (no split surrogate pairs).
  {
    const h = exitReplayHarness()
    const fileNameMb = 'spill-20261013T200102001Z-t53-16ah-mb.jsonl'
    const filePath = `${PARTITION_DIR}/${fileNameMb}`
    const noIdRow = { ...ownRow(''), event_id: '' }
    delete (noIdRow as Record<string, unknown>).event_id
    const emojiKey = `${'😀'.repeat(50)}#0` // 102 UTF-16 code units, 202 UTF-8 bytes
    h.files.set(
      filePath,
      packFiles([
        JSON.stringify({ v: 1, kind: 'spill', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.001Z', table: TABLE_REF, row: noIdRow, key: emojiKey }),
      ])[0]!,
    )
    const seenInsertIds: string[] = []
    h.setFetch(async (_url, ids) => {
      seenInsertIds.push(...ids)
      return OK_INSERT
    })
    await h.newWriter('s-16ah-mb-1').backfill(['spill'], 'auto')
    expect(seenInsertIds.length).toBe(1)
    const gotId = seenInsertIds[0]!
    expect(gotId.startsWith('dl:')).toBe(true)
    expect(utf8Bytes(gotId) <= 120).toBe(true)
    // Verify pure printable ASCII (no unpaired UTF-16 surrogates).
    expect(/^[\x20-\x7e]+$/.test(gotId)).toBe(true)
    expect(h.files.get(filePath)).toBe('')
  }
})

test('T53-16ai: linear backup discovery, absent-backup swap persistence, EACCES fail-closed, child-linked backup skip on force(C), and momentary empty child rollback', async () => {
  const rowA = 'a'.repeat(32)
  const rowB = 'b'.repeat(32)
  const rowC = 'c'.repeat(32)

  // Subcase 1: Backup discovery groups companions on the first walk so name accesses scale
  // linearly rather than quadratically with unrelated g-marked files (32 vs 128).
  const measureBackupNameAccesses = async (unrelatedCount: number): Promise<number> => {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-scale-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-scale-c.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const otherParent = 'spill-20261013T200000000Z-unrelated-parent.jsonl'
    const unrelatedBody = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:00:00.001Z', table: TABLE_REF, row: ownRow('u'.repeat(32)) }),
    ])[0]!
    const uSize = utf8Bytes(unrelatedBody)
    const uDigest = fnv1a(unrelatedBody)
    for (let i = 0; i < unrelatedCount; i += 1) {
      const uName = `retry-20261013T200000${String(i).padStart(3, '0')}Z-unrelated-${i}.jsonl`
      h.files.set(`${PARTITION_DIR}/${uName}`, unrelatedBody)
      h.files.set(
        `${PARTITION_DIR}/outcome-done-unrelg-${uName}`,
        sealFile(
          `${JSON.stringify({
            size: uSize,
            sending: true,
            by: 'unrelg',
            stagedFor: otherParent,
            stagedForSize: 999,
            replaces: `retry-20261013T200000000Z-rep-${i}.jsonl`,
            backup: true,
            backupForSize: uSize,
            backupForFnv1a: uDigest,
            backupFnv1a: uDigest,
            attempt: `unrel-${i}`,
          })}\n`,
        ),
      )
    }
    const baseIo = h.ioFor()
    let nameAccesses = 0
    const trackedIo = {
      ...baseIo,
      listDir: async (dir: string) => {
        const raw = await baseIo.listDir(dir)
        return raw.map(entry => {
          const nameVal = entry.name
          const proxy = { kind: entry.kind, size: entry.size, mtimeMs: entry.mtimeMs } as {
            name: string
            kind: string
            size: number
            mtimeMs?: number
          }
          Object.defineProperty(proxy, 'name', {
            enumerable: true,
            get() {
              nameAccesses += 1
              return nameVal
            },
          })
          return proxy
        })
      },
    }
    // Fail parent dispatch after staging backups so we measure only staging + discovery for parentName.
    h.setFetch(async () => ({ status: 503, text: 'busy' }))
    const w = new Writer(trackedIo, () => h.config, () => `s-16ai-scale-${unrelatedCount}`)
    // Lease all unrelated files so only parentName runs force staging.
    const now = baseIo.now()
    for (let i = 0; i < unrelatedCount; i += 1) {
      const uName = `retry-20261013T200000${String(i).padStart(3, '0')}Z-unrelated-${i}.jsonl`
      h.store.set(`lease:${TABLE_REF}/${uName}`, { by: 'other', until: now + 5 * 60_000 })
    }
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other', until: now - 1 })
    await w.backfill(['spill'], 'force')
    expect(h.files.get(parentPath)).not.toBe('')
    return nameAccesses
  }
  const acc32 = await measureBackupNameAccesses(32)
  const acc128 = await measureBackupNameAccesses(128)
  // Linear growth (~4x for 4x files), nowhere near quadratic (~15x / 33,666).
  expect(acc128 < 6000).toBe(true)
  expect(acc128 <= acc32 * 6).toBe(true)

  // Subcase 2: Absent recorded backup swapped for an existing on-disk backup of the same array
  // length (1 -> 1) persists the new backup name in v2 (`!sameBackups`), while EACCES still fails closed.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-swap-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-swap-c.jsonl'
    const missingBkName = 'retry-20261013T200102003Z-t53-16ai-missing.jsonl'
    const diskBkName = 'retry-20261013T200102004Z-t53-16ai-ondisk.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const bkText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(childPath, childText)
    h.files.set(`${PARTITION_DIR}/${diskBkName}`, bkText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const cSize = utf8Bytes(childText)
    const bSize = utf8Bytes(bkText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-prevg-${diskBkName}`,
      sealFile(
        `${JSON.stringify({
          size: bSize,
          sending: true,
          by: 'prevg',
          stagedFor: parentName,
          stagedForSize: pSize,
          replaces: childName,
          backup: true,
          backupForSize: cSize,
          backupForFnv1a: fnv1a(childText),
          backupFnv1a: fnv1a(bkText),
          attempt: 'a-disk',
        })}\n`,
      ),
    )
    const v2Key = `replayed:v2:${TABLE_REF}/${parentName}`
    h.store.set(v2Key, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(h.files.get(parentPath)!),
      waits: true,
      seq: 3,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(childText) }],
      backupFiles: [{ name: missingBkName, size: bSize, fnv1a: fnv1a(bkText), replaces: childName, replacesSize: cSize, replacesFnv1a: fnv1a(childText) }],
      accountedKeys: [rowA, rowB],
      attempt: 'a-disk',
      attempts: ['a-disk'],
    })
    // Defer Phase 3 via foreign lease on childName only AFTER writeOut records the updated backupFiles?
    // Even simpler: fail markDone once so writeOut stops after recording updated v2 backupFiles.
    h.setWriteFileHook(async (p, txt) => {
      if (p.includes('/outcome-done-') && p.endsWith(`-${parentName}`) && txt.includes('"retired":true')) {
        throw new Error('pause after v2 record')
      }
    })
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16ai-swap-1').backfill(['spill', 'retry'], 'auto')
    const updatedV2 = h.store.get(v2Key) as { backupFiles?: { name: string }[] } | undefined
    expect(updatedV2?.backupFiles?.map(b => b.name)).toEqual([diskBkName])

    // Non-ENOENT errors reading an existing backup — including explicit non-ENOENT error codes
    // whose message says "not found" and generic "table not found" strings — must fail closed.
    const baseIo = h.ioFor()
    h.setWriteFileHook(null)
    for (const errFactory of [
      () => Object.assign(new Error('EACCES: permission denied'), { code: 'EACCES' }),
      () => Object.assign(new Error('mount not found'), { code: 'EACCES' }),
      () => new Error('table not found'),
    ]) {
      const errIo = {
        ...baseIo,
        readFile: async (p: string) => {
          if (p.endsWith(`/${diskBkName}`)) throw errFactory()
          return baseIo.readFile(p)
        },
      }
      const wErr = new Writer(errIo, () => h.config, () => 's-16ai-eacces')
      await wErr.backfill(['spill'], 'auto')
      expect(h.files.get(parentPath)).not.toBe('')
      const v2After = h.store.get(v2Key) as { backupFiles?: { name: string }[] } | undefined
      expect(v2After?.backupFiles?.map(b => b.name)).toEqual([diskBkName])
    }
  }

  // Subcase 3: `0.1.4 force` directly on C while a backup B carries `parent: C` (for legacy ID-less
  // rows) skips B in discoverLiveChildrenForForce & classifyChild (no conflicting childGuard, no B in C's retired files).
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-cforce-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-cforce-c.jsonl'
    const backupName = 'retry-20261013T200102003Z-t53-16ai-cforce-b.jsonl'
    const childPath = `${PARTITION_DIR}/${childName}`
    const backupPath = `${PARTITION_DIR}/${backupName}`
    const noIdC = { ...ownRow(''), event_id: '' }
    delete (noIdC as Record<string, unknown>).event_id
    const childText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: noIdC }),
    ])[0]!
    const backupText = packFiles(
      [
        JSON.stringify({
          v: 1,
          kind: 'retry',
          reason: 'retry',
          attempts: 0,
          failed_at: '2026-10-13T20:01:02.002Z',
          table: TABLE_REF,
          key: `${childName}#0`,
          row: { ...noIdC, event_id: `dl:${childName}#0` },
        }),
      ],
      childName,
    )[0]!
    h.files.set(childPath, childText)
    h.files.set(backupPath, backupText)
    const cSize = utf8Bytes(childText)
    const bSize = utf8Bytes(backupText)
    h.files.set(
      `${PARTITION_DIR}/outcome-done-prevg-${backupName}`,
      sealFile(
        `${JSON.stringify({
          size: bSize,
          sending: true,
          by: 'prevg',
          stagedFor: parentName,
          stagedForSize: 500,
          replaces: childName,
          backup: true,
          backupForSize: cSize,
          backupForFnv1a: fnv1a(childText),
          backupFnv1a: fnv1a(backupText),
          attempt: 'a-cforce',
        })}\n`,
      ),
    )
    h.setFetch(async () => OK_INSERT)
    await h.newWriter('s-16ai-cforce-1').backfill(['retry'], 'force')
    expect(h.files.get(childPath)).toBe('')
    // C's retired done mark must NOT include backupName in `files`, and backupName must have no conflicting childGuard for C.
    const cDoneMarks = [...h.files.entries()].filter(([p, t]) => p.includes('/outcome-done-') && p.endsWith(`-${childName}`) && t.includes('"retired":true'))
    expect(cDoneMarks.length >= 1).toBe(true)
    expect(cDoneMarks.every(([, t]) => !t.includes(backupName))).toBe(true)
    const bGuardsForC = [...h.files.entries()].filter(
      ([p, t]) => p.includes('/outcome-done-') && p.endsWith(`-${backupName}`) && t.includes(`"stagedFor":"${childName}"`),
    )
    expect(bGuardsForC.length).toBe(0)
    // Subsequent auto drops the now-superseded backup cleanly without resending.
    await h.newWriter('s-16ai-cforce-2').backfill(['retry'], 'auto')
    expect(h.files.get(backupPath)).toBe('')
  }

  // Subcase 4: Momentary empty child (`''`, non-atomic overwrite window) during post-loop digest
  // re-verify must abort dispatch and roll back newly staged backups & child guards.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-empty-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-empty-c.jsonl'
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    let childReads = 0
    let flashedEmpty = false
    const baseIo = h.ioFor()
    const flakyIo = {
      ...baseIo,
      readFile: async (p: string) => {
        if (p === childPath) {
          childReads += 1
          // Return momentary empty on the post-staging re-read after backup is staged.
          const hasStagedBackup = [...h.files.entries()].some(
            ([fp, txt]) => fp.includes('/retry-') && !fp.includes('/outcome') && fp !== childPath && txt !== '',
          )
          if (hasStagedBackup && !flashedEmpty) {
            flashedEmpty = true
            return ''
          }
        }
        return baseIo.readFile(p)
      },
    }
    h.setFetch(async () => OK_INSERT)
    const w = new Writer(flakyIo, () => h.config, () => 's-16ai-empty-1')
    const r = await w.backfill(['spill'], 'force')
    expect(flashedEmpty).toBe(true)
    expect(childReads >= 2).toBe(true)
    expect(r.error ?? '').toContain('standalone backups')
    expect(h.sentOf(rowA)).toBe(0)
    expect(h.files.get(parentPath)).not.toBe('')
    const liveBackups = [...h.files.entries()].filter(
      ([fp, txt]) => fp.includes('/retry-') && !fp.includes('/outcome') && fp !== childPath && txt !== '',
    )
    expect(liveBackups.length).toBe(0)
  }

  // Subcase 5: writeOut leased-child direct path reports clean hold without error or double-counted
  // report.files, delivers independent trailing file S, and clears heldPaths/heldNoticeInfo once the
  // SAME Writer reconciles & empties C after the lease expires.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-wo-lease-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-wo-lease-c.jsonl'
    const independentName = 'spill-20261013T200102099Z-t53-16ai-wo-lease-s.jsonl'
    const rowS = 's'.repeat(32)
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const independentPath = h.seedSpill(independentName, rowS)
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    h.files.set(childPath, childText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const cSize = utf8Bytes(childText)
    // Seed a non-conflicting incomplete v2 checkpoint so execution enters writeOut directly (no backups staged yet).
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(h.files.get(parentPath)!),
      waits: true,
      seq: 3,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(childText) }],
      accountedKeys: [rowA, rowB],
      attempt: 'a-wo-lease',
      attempts: ['a-wo-lease'],
    })
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: h.ioFor().now() + 5 * 60_000 })
    h.setFetch(async () => OK_INSERT)

    const sameWriter = h.newWriter('s-16ai-wo-same')
    const r1 = await sameWriter.backfill(['spill', 'retry'], 'force')
    expect(r1.error).toBeNull()
    expect(r1.held ?? '').toContain(childName)
    expect((r1.heldFiles ?? 0) >= 2).toBe(true)
    expect(r1.skippedFiles >= 2).toBe(true)
    // Only the independent trailing spill S counted in report.files (not lease-deferred P).
    expect(r1.files).toBe(1)
    expect(h.sentOf(rowS)).toBe(1)
    expect(h.files.get(independentPath)).toBe('')

    // Expire lease and run auto on the SAME Writer: P reconciles & empties C in this pass.
    // heldNoticeInfo() must already clear on this first auto pass (no stale hold on C).
    await h.advance(15 * 60_000)
    await sameWriter.backfill(['spill', 'retry'], 'auto')
    expect(h.files.get(parentPath)).toBe('')
    expect(h.files.get(childPath)).toBe('')
    expect(sameWriter.heldNoticeInfo().files).toBe(0)
    expect(sameWriter.heldNoticeInfo().reason).toBeNull()
    expect(sameWriter.heldNoticeInfo().mayBeActive).toBe(false)
    await sameWriter.backfill(['spill', 'retry'], 'auto')
    expect(h.sentOf(rowC)).toBe(1)
    expect(sameWriter.heldNoticeInfo().files).toBe(0)
    expect(sameWriter.heldNoticeInfo().reason).toBeNull()
    expect(sameWriter.heldNoticeInfo().mayBeActive).toBe(false)
  }

  // Subcase 6: Concurrent child mutation under an active foreign lease when `existing` already
  // references a live backup for that child returns a clean lease hold (no hard report.error)
  // and still delivers unrelated trailing files.
  {
    const h = exitReplayHarness()
    const parentName = 'spill-20261013T200102001Z-t53-16ai-lease-mut-p.jsonl'
    const childName = 'retry-20261013T200102002Z-t53-16ai-lease-mut-c.jsonl'
    const bkName = 'retry-20261013T200102003Z-t53-16ai-lease-mut-bk.jsonl'
    const trailingName = 'spill-20261013T200102099Z-t53-16ai-lease-mut-s.jsonl'
    const rowS = 's'.repeat(32)
    const parentPath = h.seedSpill(parentName, rowA, rowB)
    const childPath = `${PARTITION_DIR}/${childName}`
    const trailingPath = h.seedSpill(trailingName, rowS)
    const childText = packFiles(
      [
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
        JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
      ],
      parentName,
    )[0]!
    const bkText = packFiles([
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.002Z', table: TABLE_REF, row: ownRow(rowB) }),
      JSON.stringify({ v: 1, kind: 'retry', reason: 'retry', attempts: 0, failed_at: '2026-10-13T20:01:02.003Z', table: TABLE_REF, row: ownRow(rowC) }),
    ])[0]!
    h.files.set(childPath, childText)
    h.files.set(`${PARTITION_DIR}/${bkName}`, bkText)
    const pSize = utf8Bytes(h.files.get(parentPath)!)
    const cSize = utf8Bytes(childText)
    const bSize = utf8Bytes(bkText)
    h.store.set(`replayed:v2:${TABLE_REF}/${parentName}`, {
      fates: 'LL',
      reasons: [],
      refusals: [],
      size: pSize,
      sourceFnv1a: fnv1a(h.files.get(parentPath)!),
      waits: true,
      seq: 3,
      files: [],
      written: [],
      pendingReconcile: [{ name: childName, size: cSize, fnv1a: fnv1a(childText) }],
      backupFiles: [{ name: bkName, size: bSize, fnv1a: fnv1a(bkText), replaces: childName, replacesSize: cSize, replacesFnv1a: fnv1a(childText) }],
      accountedKeys: [rowA, rowB],
      attempt: 'a-lease-mut',
      attempts: ['a-lease-mut'],
    })
    h.store.set(`lease:${TABLE_REF}/${childName}`, { by: 'other-session', until: h.ioFor().now() + 5 * 60_000 })
    const baseIo = h.ioFor()
    let readCount = 0
    const racyIo = {
      ...baseIo,
      readFile: async (p: string) => {
        if (p === childPath) {
          readCount += 1
          // After writeOut inspects parent header on read 1, foreign lease holder empties C before stageChildBackups reads it.
          if (readCount >= 2) return ''
        }
        return baseIo.readFile(p)
      },
    }
    h.setFetch(async () => OK_INSERT)
    const w = new Writer(racyIo, () => h.config, () => 's-16ai-lease-mut')
    const r = await w.backfill(['spill', 'retry'], 'force')
    expect(r.error).toBeNull()
    expect(r.held ?? '').toContain(childName)
    expect(h.sentOf(rowS)).toBe(1)
    expect(h.files.get(trailingPath)).toBe('')
  }
})
