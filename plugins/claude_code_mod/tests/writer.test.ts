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
import { type DeadLetterLine, encodeLines, packFiles, parseFile, sealFile } from '../hooks/lib/deadletter.ts'
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
