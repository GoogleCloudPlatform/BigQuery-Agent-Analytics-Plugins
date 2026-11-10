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
import { type Io, Writer } from '../hooks/lib/writer.ts'

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
