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

// When BigQuery cannot find the dataset or table: not made yet (the mod was
// installed before setup.sh ran), or made moments ago (streaming inserts do
// not see a new dataset or table for a while). The band, one toast, one
// transcript line, the status line and /bqaa-status say so, with the
// setup.sh command; the rows wait as retries and go out on their own once
// BigQuery finds it, with no /bqaa-backfill. Also: the refused count on the
// status line counts every row refused since rows last landed.

import { type Engine, expect, test } from 'claude-code/testing'

import { type Config, PLUGIN_VERSION, resolveConfig } from '../hooks/lib/config.ts'
import { packFiles, parseFile } from '../hooks/lib/deadletter.ts'
import { buildRow } from '../hooks/lib/rows.ts'
import { type Io, Writer } from '../hooks/lib/writer.ts'

import { ACCESS_TOKEN, COMMAND, DEAD_LETTER_DIR, OK_INSERT, OPTIONS, PARTITION_DIR, SESSION_ID, TABLE_REF, type World, deadLetterFiles, setUp, startSession, utf8Bytes } from './helpers.ts'

const PLUGIN = 'bigquery-agent-analytics-mod'

// What the host passes a hook drawing in the band above the prompt.
const BAND = {
  component: 'AbovePrompt',
  requestId: 'above-prompt',
  viewport: { columns: 120, rows: 40 },
  props: { hasSurvey: false, isCollapsed: false, isWorking: false, maxRows: 10, bodyColumns: 115, scroll: { offset: 0, bodyRows: 10 }, view: {} },
} as const

const DATASET_MISSING = {
  status: 404,
  text: '{"error":{"code":404,"message":"Not found: Dataset my-gcp-project:agent_analytics","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}',
}
const TABLE_MISSING = {
  status: 404,
  text: '{"error":{"code":404,"message":"Not found: Table my-gcp-project:agent_analytics.agent_events","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}',
}
const TABLE_CREATED = { status: 200, text: '{"kind":"bigquery#table"}' }
const INVALID = { status: 400, text: '{"error":{"code":400,"message":"Invalid value for the table schema","errors":[{"reason":"invalid"}],"status":"INVALID_ARGUMENT"}}' }

const HEADLINE = 'BigQuery cannot find the dataset my-gcp-project.agent_analytics'
const FIX = "create it with the mod's setup.sh; rows wait, and are sent once BigQuery finds it (a few minutes after it is made)"
const LINE = `rows are not being written: ${HEADLINE}. To fix, ${FIX}.`
// Every name setup.sh reads, the views dataset included, so none comes from
// the shell it runs in; then bash and the script in the mod's own directory,
// quoted as one shell word.
const SETUP = /^BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '\/[^']*\/setup\.sh'$/
const PROMPT = { text: 'hi', wait: false, origin: { kind: 'composer' } } as const

const statusText = async ($: Engine): Promise<string> => (await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND })).text ?? ''

// The band as the terminal would draw it: the mod's line and its command
// line, if any, and the text the mods after it drew.
async function band($: Engine): Promise<{ line?: string; run?: string; waiting?: string; beneath: boolean }> {
  const ui = await $.ui.mount({ plugin: PLUGIN, surface: 'terminal', ...BAND })
  const line = (await ui.find({ type: 'Text', text: /^rows are not being written/ }))?.text
  const run = (await ui.find({ type: 'Text', text: /^Run: / }))?.text
  const waiting = (await ui.find({ type: 'Text', text: /row\(s\) waiting/ }))?.text
  const beneath = (await ui.find({ type: 'Text', text: 'drawn beneath' })) !== undefined
  await ui.unmount()
  return { line, run, waiting, beneath }
}

const noticeLines = (world: World): string[] => world.transcript.filter(l => l.startsWith('bqaa: rows are not being written'))
const refusedFiles = (world: World): string[] => deadLetterFiles(world).map(([path]) => path).filter(path => path.includes('/rejected-'))

test('a dataset BigQuery cannot find is a notice with the setup.sh command, not a refusal', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  // No table is made in a dataset that is not there.
  expect(world.fetches.map(f => f.url.split('/').pop())).toEqual(['insertAll'])
  // The session-start row and the call's two rows wait.
  const shown = await band($)
  expect(shown.line).toBe(LINE)
  expect((shown.run ?? '').replace(/^Run: /, '')).toMatch(SETUP)
  expect(shown.waiting).toBe(' 3 row(s) waiting.')
  expect(shown.beneath).toBe(true)
  expect(world.toasts).toEqual([`BQAA: ${LINE}`])
  expect(world.statusLines.at(-1)).toBeUndefined()
  const [notice = ''] = noticeLines(world)
  expect(notice).toContain(`${HEADLINE} (HTTP 404: Not found: Dataset my-gcp-project:agent_analytics). To fix, ${FIX}. Run: BQAA_PROJECT_ID=my-gcp-project `)
  expect(notice.endsWith("/setup.sh'; this clears once rows are written again.")).toBe(true)
  const status = await statusText($)
  expect(status).toContain(`rows are not being written: ${HEADLINE}; to fix, ${FIX}; 3 row(s) waiting; cause: HTTP 404: Not found: Dataset my-gcp-project:agent_analytics`)
  expect((status.split('\n').find(l => l.startsWith('setup: ')) ?? '').replace(/^setup: /, '')).toMatch(SETUP)
  // Nothing is refused.
  expect(status).toMatch(/dead-lettered 0 \(0 refused\)/)
  expect(refusedFiles(world)).toEqual([])
  // More failures: still one toast and one transcript line.
  for (let t = 0; t < 4; t += 1) {
    await $.tool.call({ tool: 'Read', file_path: `b${t}.ts` })
    await world.clock.advance(5000)
  }
  expect(world.toasts.length).toBe(1)
  expect(noticeLines(world).length).toBe(1)
})

test('rows wait as retries while the dataset is missing, then all go out on their own, once each', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  for (let t = 0; t < 8; t += 1) {
    await $.tool.call({ tool: 'Read', file_path: `f${t}.ts` })
    await world.clock.advance(5000)
  }
  // Rows past their retries are parked as retries, which replay on their
  // own; none as refused rows, which would wait for /bqaa-backfill.
  const parked = deadLetterFiles(world).map(([path]) => path)
  expect(parked.length > 0).toBe(true)
  expect(parked.every(path => path.startsWith(`${PARTITION_DIR}/retry-`))).toBe(true)
  // setup.sh has run, and BigQuery finds the dataset.
  world.replies = [OK_INSERT]
  await $.tool.call({ tool: 'Read', file_path: 'g.ts' })
  for (let t = 0; t < 10; t += 1) await world.clock.advance(5000)
  // The session-start row and nine calls' two rows each, every one once.
  const ids = world.acceptedRows().map(r => r.event_id)
  expect(ids.length).toBe(19)
  expect(new Set(ids).size).toBe(19)
  expect(deadLetterFiles(world).length).toBe(0)
  expect((await band($)).line).toBeUndefined()
  expect(world.statusLines.at(-1)).toBeUndefined()
  expect(await statusText($)).not.toMatch(/notice|setup:/)
})

test('with nothing new to send, the parked rows go out on their own once BigQuery finds the dataset, and the notice clears', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // Until every row is parked and none is queued: a session left idle.
  for (let t = 0; t < 30 && !/queue: 0 rows/.test(await statusText($)); t += 1) await world.clock.advance(5000)
  expect(await statusText($)).toMatch(/queue: 0 rows/)
  expect(deadLetterFiles(world).length > 0).toBe(true)
  expect((await band($)).waiting).toBe(' 3 row(s) waiting.')
  // The dataset is made; no prompt, no tool call follows.
  world.replies = [OK_INSERT]
  for (let t = 0; t < 8; t += 1) await world.clock.advance(5000)
  const ids = world.acceptedRows().map(r => r.event_id)
  expect(ids.length).toBe(3)
  expect(new Set(ids).size).toBe(3)
  expect(deadLetterFiles(world).length).toBe(0)
  expect((await band($)).line).toBeUndefined()
  expect(world.statusLines.at(-1)).toBeUndefined()
})

test('a prompt after the dataset is made sends at once, from a timer: the hook itself waits on nothing', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  for (let t = 0; t < 6; t += 1) {
    await $.tool.call({ tool: 'Read', file_path: `f${t}.ts` })
    await world.clock.advance(5000)
  }
  world.replies = [OK_INSERT]
  const fetches = world.fetches.length
  const result = await $.prompt.submit(PROMPT)
  expect(result.text).toBe('hi')
  expect(world.fetches.length).toBe(fetches)
  // No time passes: the retry runs as soon as the hook has returned.
  await world.clock.settle()
  expect(world.acceptedRows().some(r => r.event_type === 'USER_MESSAGE_RECEIVED')).toBe(true)
  expect((await band($)).line).toBeUndefined()
})

test('while BigQuery cannot find the dataset, the backoff before the next retry is capped at 30 s', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  // Fail, with rows to send at every tick, until the backoff would pass a
  // minute; read the wait right after the failure that set it.
  let failures = 0
  let waitSeconds = -1
  for (let t = 0; t < 120 && waitSeconds < 0; t += 1) {
    await $.tool.call({ tool: 'Read', file_path: `f${t}.ts` })
    await world.clock.advance(5000)
    const status = await statusText($)
    const now = Number(/backing off: (\d+) failure/.exec(status)?.[1] ?? 0)
    if (now > failures && now >= 8) waitSeconds = Number(/next try in about (\d+)s/.exec(status)?.[1] ?? -1)
    failures = now
  }
  expect(waitSeconds > 0).toBe(true)
  expect(waitSeconds <= 30).toBe(true)
})

test('session.start clears the status line an earlier load pinned: the host keeps it across a reload', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: BigQuery cannot find the dataset /)
  // The mod loads again (an edit under --plugin-dir, /reload-plugins, an
  // update) and session.start runs again; its writer has no notice yet.
  world.replies = [OK_INSERT]
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  expect(world.statusLines.at(-1)).toBeUndefined()
  expect((await band($)).line).toBeUndefined()
})

test('with table creation off, a table BigQuery cannot find is the same notice, for the table', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_AUTO_CREATE_TABLE: 'false' })
  world.replies = [TABLE_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  expect(world.fetches.map(f => f.url.split('/').pop())).toEqual(['insertAll'])
  expect((await band($)).line).toBe(
    "rows are not being written: BigQuery cannot find the table my-gcp-project.agent_analytics.agent_events. To fix, create it with the mod's setup.sh; rows wait, and are sent once BigQuery finds it (a few minutes after it is made).",
  )
  expect(refusedFiles(world)).toEqual([])
})

// A writer built by hand, for what follows the writer's own clock (Io.now,
// Date.now in a session, which the mock clock does not move): its timers
// fire as `advance` moves that clock. BigQuery gives `answers` in turn, then
// the last one again.
function handBuilt(answers: { status: number; text: string }[], pluginRoot?: string) {
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/dl' }, {})
  let now = 0
  const timers: { at: number; fn: () => void; live: boolean }[] = []
  const urls: string[] = []
  const files = new Map<string, string>()
  const io: Io = {
    now: () => now,
    after: (ms, fn) => {
      const timer = { at: now + ms, fn, live: true }
      timers.push(timer)
      return { cancel: () => (timer.live = false) }
    },
    fetch: async url => {
      urls.push(url)
      return (answers.length > 1 ? answers.shift() : answers[0]) as { status: number; text: string }
    },
    runCommand: async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
    envToken: async () => undefined,
    writeFile: async (path, text) => {
      files.set(path, text)
    },
    readFile: async path => files.get(path) ?? '',
    listDir: async dir => [...files].filter(([path]) => path.startsWith(`${dir}/`)).map(([path, text]) => ({ name: path.slice(dir.length + 1), kind: 'file', size: utf8Bytes(text) })),
    storeGet: async () => undefined,
    storeSet: async () => undefined,
    storeDelete: async () => undefined,
    status: () => undefined,
    debug: () => undefined,
    pluginRoot,
  }
  const writer = new Writer(io, () => config, () => 'hand-built-session')
  let seq = 0
  const enqueue = (): void => {
    seq += 1
    const eventId = seq.toString(16).padStart(32, '0')
    writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: now, eventId, agent: 'coding-agent', sessionId: 'hand-built-session', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  }
  const advance = (ms: number): void => {
    now += ms
    for (const timer of timers.filter(t => t.live && t.at <= now).sort((a, b) => a.at - b.at)) {
      timer.live = false
      timer.fn()
    }
  }
  const parkedKinds = (): string[] => [...files.values()].flatMap(text => parseFile(text).lines.map(line => line.kind))
  return { writer, enqueue, advance, urls, parkedKinds }
}

test('a table the mod made that BigQuery still cannot find after two minutes is the notice too, its rows kept', async () => {
  const w = handBuilt([TABLE_MISSING, TABLE_CREATED, TABLE_MISSING], '/opt/mods/bigquery-agent-analytics-mod')
  w.enqueue()
  await w.writer.flush()
  // Made, and not seen yet: retried with no notice while it is new.
  expect(w.urls.map(u => u.split('/').pop())).toEqual(['insertAll', 'tables', 'insertAll'])
  expect(w.writer.problem).toBeNull()
  // Two minutes on, BigQuery still cannot find it.
  w.advance(121_000)
  w.enqueue()
  await w.writer.flush()
  expect(w.writer.problem).toMatchObject({ kind: 'destination', missing: 'table' })
  expect(w.writer.problemText().headline).toBe('BigQuery cannot find the table my-gcp-project.agent_analytics.agent_events')
  expect(w.writer.setupCommand()).toBe("BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '/opt/mods/bigquery-agent-analytics-mod/setup.sh'")
  // Rows past their retries are parked as retries; the table is made once.
  for (let i = 0; i < 4; i += 1) {
    w.advance(31_000)
    await w.writer.flush()
  }
  expect(w.parkedKinds().length > 0).toBe(true)
  expect(w.parkedKinds().every(kind => kind === 'retry')).toBe(true)
  expect(w.urls.filter(u => u.endsWith('/tables')).length).toBe(1)
})

test('a table the mod made that a replay still cannot find after two minutes is the notice too, with nothing new queued; its rows go once it is found', async () => {
  const answers = [TABLE_MISSING, TABLE_CREATED, TABLE_MISSING]
  const w = handBuilt(answers, '/opt/mods/bigquery-agent-analytics-mod')
  w.enqueue()
  // The row's retries run out while the table is new: parked, no notice.
  for (let i = 0; i < 8; i += 1) {
    w.advance(5000)
    await w.writer.flush()
  }
  expect(w.writer.queue.length).toBe(0)
  expect(w.parkedKinds()).toEqual(['retry'])
  expect(w.writer.problem).toBeNull()
  // Two minutes on, the paced replay still gets a 404.
  for (let i = 0; i < 4; i += 1) {
    w.advance(31_000)
    await w.writer.flush()
  }
  expect(w.writer.problem).toMatchObject({ kind: 'destination', missing: 'table' })
  answers.splice(0, answers.length, OK_INSERT)
  for (let i = 0; i < 4; i += 1) {
    w.advance(31_000)
    await w.writer.flush()
  }
  expect(w.parkedKinds()).toEqual([])
  expect(w.writer.problem).toBeNull()
})

test("the setup.sh path is one shell word, a quote or a closing backslash in it included; a directory that is not a POSIX path gets no command", async () => {
  const quoted = handBuilt([DATASET_MISSING], "/home/o'brien/my mods/bigquery-agent-analytics-mod")
  quoted.enqueue()
  await quoted.writer.flush()
  expect(quoted.writer.setupCommand()).toBe("BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '/home/o'\\''brien/my mods/bigquery-agent-analytics-mod/setup.sh'")
  // On a POSIX path a backslash is part of the directory's name, not a separator.
  const backslash = handBuilt([DATASET_MISSING], '/opt/mods\\')
  backslash.enqueue()
  await backslash.writer.flush()
  expect(backslash.writer.setupCommand()).toBe("BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '/opt/mods\\/setup.sh'")
  const windows = handBuilt([DATASET_MISSING], 'C:\\mods\\bigquery-agent-analytics-mod')
  windows.enqueue()
  await windows.writer.flush()
  expect(windows.writer.setupCommand()).toBeNull()
  expect(windows.writer.problemText().fix).toBe('create it, or correct the project and dataset options; rows wait, and are sent once BigQuery finds it (a few minutes after it is made)')
})

test('a setup command too long for the band and the transcript line is pointed to there, and /bqaa-status shows it whole', { options: { project: 'my-gcp-project', dataset: 'd'.repeat(450) } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const ui = await $.ui.mount({ plugin: PLUGIN, surface: 'terminal', ...BAND })
  const run = (await ui.find({ type: 'Text', text: /^Run / }))?.text
  await ui.unmount()
  expect(run).toBe('Run the setup command /bqaa-status shows (too long for this line)')
  const notice = noticeLines(world)
  expect(notice.length).toBe(1)
  expect(notice[0] ?? '').toMatch(/\. Run the setup command \/bqaa-status shows \(too long for this line\); this clears once rows are written again\.$/)
  const d = 'd'.repeat(450)
  const setup = (await statusText($)).split('\n').find(l => l.startsWith('setup: ')) ?? ''
  expect(setup.length > 1000).toBe(true)
  expect(setup).toMatch(new RegExp(`^setup: BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=${d} BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=${d}_reporting BQAA_LOCATION=US bash '/[^']*/setup\\.sh'$`))
})

test('a long setup command that fits the band shows whole there and in the transcript line', { options: { project: 'my-gcp-project', dataset: 'd'.repeat(300) } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const d = 'd'.repeat(300)
  const command = new RegExp(`^BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=${d} BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=${d}_reporting BQAA_LOCATION=US bash '/[^']*/setup\\.sh'$`)
  const run = (await band($)).run ?? ''
  expect(run.slice('Run: '.length)).toMatch(command)
  const notice = noticeLines(world)[0] ?? ''
  expect(notice.length > 1000).toBe(true)
  expect(notice.endsWith(`. ${run}; this clears once rows are written again.`)).toBe(true)
})

// Names this long put the lines before the command past the 8,000
// characters /bqaa-status keeps, wherever the mod's directory is, unless
// those lines are cut first.
const LONG_PROJECT = `${'p'.repeat(300)}.example:my-gcp-project`

test('/bqaa-status shows the setup command whole when the names around it are very long', { options: { project: LONG_PROJECT, dataset: 'd'.repeat(1014), table: 't'.repeat(1024) } }, async ($, on) => {
  const world = setUp(on, { BQAA_AUTO_CREATE_TABLE: 'false' })
  world.replies = [TABLE_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const d = 'd'.repeat(1014)
  const t = 't'.repeat(1024)
  const setup = (await statusText($)).split('\n').find(l => l.startsWith('setup: ')) ?? ''
  expect(setup).toMatch(new RegExp(`^setup: BQAA_PROJECT_ID=${LONG_PROJECT} BQAA_DATASET=${d} BQAA_TABLE=${t} BQAA_VIEWS_DATASET=${d}_reporting BQAA_LOCATION=US bash '/[^']*/setup\\.sh'$`))
})

test('a setup command longer than /bqaa-status could show whole is not offered', { options: { project: `${'a'.repeat(4000)}.example:my-gcp-project`, dataset: 'agent_analytics' } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const shown = await band($)
  expect(shown.line ?? '').toMatch(/^rows are not being written: BigQuery cannot find the dataset /)
  expect(shown.run).toBeUndefined()
  expect(await statusText($)).not.toMatch(/^setup: /m)
})

test('a project the redaction mistakes for a credential name still gets the setup command whole in the transcript, on a line of its own, and the whole fix on the status line', { options: { project: 'auth-service-123', dataset: 'agent_analytics' } }, async ($, on) => {
  const world = setUp(on)
  // BigQuery names the dataset project:dataset, which the redaction takes,
  // with the rest of the line, for a credential.
  world.replies = [{ status: 404, text: '{"error":{"code":404,"message":"Not found: Dataset auth-service-123:agent_analytics","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}' }]
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  const command = /^bqaa: Run: BQAA_PROJECT_ID=auth-service-123 BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '\/[^']*\/setup\.sh'$/
  expect(world.transcript.filter(line => command.test(line)).length).toBe(1)
  // The status line and the band name the dataset as project.dataset, which
  // the redaction leaves whole, so the fix and the count follow it.
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: BigQuery cannot find the dataset auth-service-123\.agent_analytics\. To fix, create it with the mod's setup\.sh; .* \(\d+ row\(s\) waiting\)$/)
  // /bqaa-status gives what BigQuery said last, so its cut keeps the fix.
  expect(await statusText($)).toMatch(/^notice \(since .*\): rows are not being written: BigQuery cannot find the dataset auth-service-123\.agent_analytics; to fix, create it with the mod's setup\.sh; .*; \d+ row\(s\) waiting; cause: /m)
})

test('a mod directory with a line break in its path gets the fix with no command: a command on two lines cannot be copied whole', async () => {
  const w = handBuilt([DATASET_MISSING], '/opt/mods\nbigquery-agent-analytics-mod')
  w.enqueue()
  await w.writer.flush()
  expect(w.writer.problem).toMatchObject({ kind: 'destination', missing: 'dataset' })
  expect(w.writer.setupCommand()).toBeNull()
})

test('a mod directory whose path redaction would change gets the fix with no command, rather than a command that would not run', async () => {
  const w = handBuilt([DATASET_MISSING], '/opt/token=abcdefghijklmnopqrstuvwxyz0123456789/bigquery-agent-analytics-mod')
  w.enqueue()
  await w.writer.flush()
  expect(w.writer.problem).toMatchObject({ kind: 'destination', missing: 'dataset' })
  expect(w.writer.setupCommand()).toBeNull()
  expect(w.writer.commandLine()).toBeNull()
  expect(w.writer.problemText().fix).toBe('create it, or correct the project and dataset options; rows wait, and are sent once BigQuery finds it (a few minutes after it is made)')
})

test('names setup.sh would refuse get the fix with no command', { options: { project: 'my-gcp-project', dataset: 'agent-analytics' } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const shown = await band($)
  expect(shown.line).toBe(
    'rows are not being written: BigQuery cannot find the dataset my-gcp-project.agent-analytics. To fix, create it, or correct the project and dataset options; rows wait, and are sent once BigQuery finds it (a few minutes after it is made).',
  )
  expect(shown.run).toBeUndefined()
  expect(noticeLines(world)[0] ?? '').not.toContain('Run:')
  expect(await statusText($)).not.toMatch(/setup:/)
})

// 1015 characters is a dataset name BigQuery takes, but its views dataset,
// with `_reporting`, is longer than the 1024 setup.sh allows.
test("a dataset whose views dataset setup.sh would refuse gets no command", { options: { project: 'my-gcp-project', dataset: 'd'.repeat(1015) } }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  const shown = await band($)
  expect((shown.line ?? '').startsWith(`rows are not being written: BigQuery cannot find the dataset my-gcp-project.${'d'.repeat(100)}`)).toBe(true)
  expect(shown.run).toBeUndefined()
  expect(await statusText($)).not.toMatch(/setup:/)
})

test('a dataset that turns up with a table that refuses the rows: the notice clears, and the status line says they were refused', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await startSession($)
  await world.clock.advance(5000)
  expect((await band($)).line).toBe(LINE)
  // BigQuery answers 400 now: it found the table, and the rows are refused
  // for a reason of their own.
  world.replies = [INVALID]
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  for (let t = 0; t < 6; t += 1) await world.clock.advance(5000)
  expect((await band($)).line).toBeUndefined()
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: \d+ row\(s\) refused \(HTTP 400: Invalid value for the table schema\); fix, then \/bqaa-backfill$/)
})

test('the status line counts every row refused since rows last landed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [INVALID]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe('BQAA: 3 row(s) refused (HTTP 400: Invalid value for the table schema); fix, then /bqaa-backfill')
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe('BQAA: 5 row(s) refused (HTTP 400: Invalid value for the table schema); fix, then /bqaa-backfill')
  // Rows land: the line clears, and the next refusal counts from there.
  world.replies = [OK_INSERT]
  await $.tool.call({ tool: 'Read', file_path: 'c.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBeUndefined()
  world.replies = [INVALID]
  await $.tool.call({ tool: 'Read', file_path: 'd.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1)).toBe('BQAA: 2 row(s) refused (HTTP 400: Invalid value for the table schema); fix, then /bqaa-backfill')
})

test('/bqaa-mod-setup and /bqaa-setup print both setup.sh and the setup_gcp_prereqs.py bootstrap command when configured, and a template when unconfigured', { options: OPTIONS }, async ($, on) => {
  setUp(on)
  await startSession($)
  for (const command of ['bqaa-mod-setup', 'bqaa-setup']) {
    const res = await $.command.run({ command, args: '', ...COMMAND })
    expect(res.text ?? '').toMatch(/no repo clone needed/)
    expect(res.text ?? '').toMatch(/setup: BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics BQAA_TABLE=agent_events BQAA_VIEWS_DATASET=agent_analytics_reporting BQAA_LOCATION=US bash '\/[^']*\/setup\.sh'/)
    // The bootstrap's virtual environment is the administrator's own, in the
    // home directory: never a shared /tmp path another user could make first.
    expect(res.text ?? '').toMatch(/bootstrap \(for project administrators, dry-run by default\): python3 -m venv "\$HOME\/\.bqaa-mod\/venv" && "\$HOME\/\.bqaa-mod\/venv"\/bin\/pip install -q google-cloud-bigquery && "\$HOME\/\.bqaa-mod\/venv"\/bin\/python '\/[^']*\/scripts\/setup_gcp_prereqs\.py' --project my-gcp-project --dataset agent_analytics --table agent_events --location US/)
  }
})

test('when the host refuses /bqaa-setup registration, /bqaa-mod-setup still registers and prints both setup and bootstrap commands', { options: OPTIONS }, async ($, on) => {
  on('command.register', { name: 'bqaa-setup' }, () => ({ deny: 'command already registered by another plugin' }))
  const world = setUp(on)
  await startSession($)
  expect(world.registered).toEqual(['bqaa-status', 'bqaa-flush', 'bqaa-backfill', 'bqaa-mod-setup'])
  const res = await $.command.run({ command: 'bqaa-mod-setup', args: '', ...COMMAND })
  expect(res.text ?? '').toMatch(/setup: BQAA_PROJECT_ID=my-gcp-project /)
  expect(res.text ?? '').toMatch(/bootstrap \(for project administrators, dry-run by default\): /)
})

test('when project or dataset is unset, /bqaa-mod-setup and /bqaa-setup print template setup and bootstrap commands, /bqaa-status omits ..agent_events, and subsequent sessions show a 1-line AbovePrompt warning', { options: {} }, async ($, on) => {
  const world = setUp(on)
  // Pretend the welcome card was already shown in an earlier session so this session is a subsequent session.
  world.store.set('welcome', { version: PLUGIN_VERSION, sessionId: 'earlier-session', previous: null })
  world.store.set(`welcome:${PLUGIN_VERSION}`, { sessionId: 'earlier-session', previous: null })
  await startSession($)
  await world.clock.settle()
  for (const command of ['bqaa-mod-setup', 'bqaa-setup']) {
    const setupRes = await $.command.run({ command, args: '', ...COMMAND })
    expect(setupRes.text ?? '').toContain('target not configured')
    expect(setupRes.text ?? '').toMatch(/setup: BQAA_PROJECT_ID=my-gcp-project BQAA_DATASET=agent_analytics /)
    expect(setupRes.text ?? '').toMatch(/bootstrap \(for project administrators, dry-run by default\): .*--project my-gcp-project --dataset agent_analytics /)
  }
  const status = await statusText($)
  expect(status).toContain('target not configured')
  expect(status).not.toContain('..agent_events')
  expect(status).not.toMatch(/^dead-letter:/m)
  const shown = await band($)
  expect(shown.line).toBe('rows are not being written: target not set. Set project and dataset in /config (or BQAA_PROJECT_ID and BQAA_DATASET), or run /bqaa-mod-setup.')
})

test('dismissing the welcome card via Got it when project or dataset is unset immediately shows the target not set warning in AbovePrompt', { options: {} }, async ($, on) => {
  const world = setUp(on, {}, {})
  await startSession($)
  await world.clock.settle()
  const ui = await $.ui.mount({ plugin: PLUGIN, surface: 'terminal', ...BAND })
  const gotIt = await ui.find({ type: 'Button', key: 'bqaa-welcome-close' })
  expect(gotIt).toBeDefined()
  await ui.press({ key: 'bqaa-welcome-close' })
  const line = (await ui.find({ type: 'Text', text: /^rows are not being written/ }))?.text
  await ui.unmount()
  expect(line).toBe('rows are not being written: target not set. Set project and dataset in /config (or BQAA_PROJECT_ID and BQAA_DATASET), or run /bqaa-mod-setup.')
})

test('headless /bqaa-status and interactive /bqaa-status check probe tables.get and surface 200, 401, 403, and 404 before any batch has completed', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  // Interactive /bqaa-status without args stays zero-network.
  const plain = await statusText($)
  expect(world.fetches.length).toBe(0)
  expect(plain).not.toContain('destination check:')

  // Interactive /bqaa-status check probes tables.get -> 200 OK.
  world.replies = [{ status: 200, text: '{"tableReference":{"projectId":"my-gcp-project","datasetId":"agent_analytics","tableId":"agent_events"}}' }]
  const checkedOk = (await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND })).text ?? ''
  expect(world.fetches.at(-1)?.method).toBe('GET')
  expect(world.fetches.at(-1)?.url).toBe('https://bigquery.googleapis.com/bigquery/v2/projects/my-gcp-project/datasets/agent_analytics/tables/agent_events?fields=tableReference')
  expect(checkedOk).toContain('destination check: BigQuery found my-gcp-project.agent_analytics.agent_events')

  // Headless session probes tables.get automatically on /bqaa-status: 401, 403, 404.
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  world.replies = [{ status: 401, text: '{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}' }]
  const checked401 = await statusText($)
  expect(checked401).toContain('destination check: HTTP 401')
  expect(checked401).toContain('rows are not being written: no working Google credentials; to fix, run: gcloud auth application-default login')

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  world.replies = [{ status: 403, text: '{"error":{"code":403,"message":"Access Denied: Table my-gcp-project:agent_analytics.agent_events","errors":[{"reason":"accessDenied"}]}}' }]
  const checked403 = await statusText($)
  expect(checked403).toContain('BigQuery denied the identity access to the dataset')

  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  world.replies = [DATASET_MISSING]
  const checked404 = await statusText($)
  expect(checked404).toContain('BigQuery cannot find the dataset my-gcp-project.agent_analytics')
  expect(checked404).toMatch(/setup: BQAA_PROJECT_ID=my-gcp-project /)

  // Explicit interactive `/bqaa-status check` still probes tables.get after an earlier 404 or 200 and clears a healed 404 when the table now exists:
  await startSession($)
  world.replies = [DATASET_MISSING]
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(await statusText($)).toContain('BigQuery cannot find the dataset my-gcp-project.agent_analytics')
  world.replies = [{ status: 200, text: '{"tableReference":{"projectId":"my-gcp-project","datasetId":"agent_analytics","tableId":"agent_events"}}' }]
  const healed = (await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND })).text ?? ''
  expect(healed).toContain('destination check: BigQuery found my-gcp-project.agent_analytics.agent_events')
  expect(healed).not.toContain('BigQuery cannot find the dataset')
})

test('headless session.end writes a single redacted diagnostic line when rows could not be sent, and stays silent when healthy or interactive', { options: OPTIONS }, async ($, on) => {
  const secret = ['canary', 'stderr', 'secret', '9876543210'].join('-')
  const world = setUp(on, { BQAA_ACCESS_TOKEN_COMMAND: `my-token-helper --password=${secret}` })
  world.processReply = { exitCode: 1, stdout: '', stderr: `auth failed: password=${secret}\n` }
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.advance(5000)
  await $.session.end({ reason: 'other', sessionId: 'headless-err', resume: { id: 'headless-err' } })
  const headlessNotices = () => world.logs.filter(l => l.startsWith('BigQuery Agent Analytics: rows could not be sent'))
  expect(headlessNotices().length).toBe(1)
  expect(headlessNotices()[0]).toBe('BigQuery Agent Analytics: rows could not be sent (no working Google credentials; to fix, check the token command `my-token-helper`).')
  expect(headlessNotices()[0]?.includes(secret)).toBe(false)

  // Healthy headless session writes 0 headless diagnostic lines.
  world.logs.length = 0
  world.processReply = { exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }
  world.replies = [OK_INSERT]
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.settle()
  await $.session.end({ reason: 'other', sessionId: 'headless-ok', resume: { id: 'headless-ok' } })
  expect(headlessNotices()).toEqual([])

  // Interactive session with a problem writes 0 headless diagnostic lines (uses AbovePrompt/statusLine instead).
  world.replies = [{ status: 403, text: '{"error":{"code":403,"message":"Access Denied","errors":[{"reason":"accessDenied"}]}}' }]
  await startSession($)
  await world.clock.advance(5000)
  await $.session.end({ reason: 'prompt_input_exit', sessionId: 'interactive-err', resume: { id: 'interactive-err' } })
  expect(headlessNotices()).toEqual([])
})

// tables.get, which /bqaa-status check asks, proves the dataset, the table
// and the credentials, not that the identity may write rows
// (bigquery.tables.updateData): what it finds clears only what it proves.
const DENIED = {
  status: 403,
  text: '{"error":{"code":403,"message":"Access Denied: Table my-gcp-project:agent_analytics.agent_events: Permission bigquery.tables.updateData denied","errors":[{"reason":"accessDenied"}],"status":"PERMISSION_DENIED"}}',
}
const READ_DENIED = {
  status: 403,
  text: '{"error":{"code":403,"message":"Access Denied: Table my-gcp-project:agent_analytics.agent_events: Permission bigquery.tables.get denied","errors":[{"reason":"accessDenied"}],"status":"PERMISSION_DENIED"}}',
}
const UNAVAILABLE = { status: 503, text: '{"error":{"code":503,"message":"Service unavailable","status":"UNAVAILABLE"}}' }
const TABLE_FOUND = { status: 200, text: '{"tableReference":{"projectId":"my-gcp-project","datasetId":"agent_analytics","tableId":"agent_events"}}' }
const FOUND = 'destination check: BigQuery found my-gcp-project.agent_analytics.agent_events'
const DENIED_NOTICE = 'rows are not being written: BigQuery denied the identity access to the dataset'

const statusCheck = async ($: Engine): Promise<string> => (await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND })).text ?? ''
const writtenAgain = (world: World): string[] => world.transcript.filter(l => l.includes('rows are being written again'))

test('/bqaa-status check finding the table keeps a denial BigQuery gave rows: the notice stays, and the transcript is told rows are written again only once a write lands', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(await statusText($)).toContain(DENIED_NOTICE)
  // tables.get refused too, then answered: neither says rows may be written.
  for (const [reply, found] of [[READ_DENIED, false], [TABLE_FOUND, true]] as const) {
    world.replies = [reply]
    const checked = await statusCheck($)
    expect(world.fetches.at(-1)?.method).toBe('GET')
    expect(checked.includes(FOUND)).toBe(found)
    expect(checked).toContain(DENIED_NOTICE)
  }
  expect((await band($)).line ?? '').toMatch(/^rows are not being written: BigQuery denied the identity access to the dataset/)
  expect(writtenAgain(world)).toEqual([])
  // A write BigQuery takes clears it, and the refused rows wait for
  // /bqaa-backfill.
  world.replies = [OK_INSERT]
  await $.tool.call({ tool: 'Read', file_path: 'b.ts' })
  await world.clock.advance(5000)
  expect((await band($)).line).toBeUndefined()
  expect(writtenAgain(world)).toEqual(['bqaa: rows are being written again; /bqaa-backfill sends the 3 row(s) parked while they could not be.'])
})

test('/bqaa-status check finding the table keeps a denial BigQuery gave rows when none of them could be parked', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DENIED]
  // The refused rows' file cannot be written: nothing is counted as parked,
  // so only the denial of the write itself keeps the notice.
  world.failWrites = true
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(await statusText($)).toContain(DENIED_NOTICE)
  expect(deadLetterFiles(world)).toEqual([])
  world.replies = [READ_DENIED]
  expect(await statusCheck($)).toContain(DENIED_NOTICE)
  world.replies = [TABLE_FOUND]
  const checked = await statusCheck($)
  expect(checked).toContain(FOUND)
  expect(checked).toContain(DENIED_NOTICE)
  expect(writtenAgain(world)).toEqual([])
})

test('/bqaa-status check clears a denial of tables.get it raised once the table is found, but not while rows parked under that denial wait', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10' })
  await startSession($)
  world.replies = [READ_DENIED]
  expect(await statusCheck($)).toContain(DENIED_NOTICE)
  // Nothing parked: the table found clears the notice, with no transcript line.
  world.replies = [TABLE_FOUND]
  const healed = await statusCheck($)
  expect(healed).toContain(FOUND)
  expect(healed).not.toContain('rows are not being written')
  expect((await band($)).line).toBeUndefined()
  expect(writtenAgain(world)).toEqual([])

  // Denied again; BigQuery is unavailable while rows pile up, and past 80%
  // of the queue they are parked under the notice: it stays.
  world.replies = [READ_DENIED]
  expect(await statusCheck($)).toContain(DENIED_NOTICE)
  world.replies = [UNAVAILABLE]
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  await world.clock.advance(5000)
  expect(deadLetterFiles(world).length > 0).toBe(true)
  world.replies = [TABLE_FOUND]
  const checked = await statusCheck($)
  expect(checked).toContain(FOUND)
  expect(checked).toContain(DENIED_NOTICE)
  expect(writtenAgain(world)).toEqual([])
})

test('/bqaa-status check that finds a dataset BigQuery could not find clears the status line and the backoff, and the rows waiting go at once', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [DATASET_MISSING]
  await $.classic.SessionStart({ source: 'startup', session_id: SESSION_ID, model: 'example-model', cwd: '/work/my-repo' })
  // A surface that draws no band above the prompt: the notice is the
  // status line.
  await $.session.start({ surface: 'web', isInteractive: true, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  await world.clock.advance(5000)
  expect(world.statusLines.at(-1) ?? '').toMatch(/^BQAA: BigQuery cannot find the dataset my-gcp-project\.agent_analytics\. /)
  expect(await statusText($)).toMatch(/backing off: 1 failure\(s\) in a row/)
  expect(world.acceptedRows()).toEqual([])
  world.replies = [TABLE_FOUND, OK_INSERT]
  const healed = await statusCheck($)
  expect(healed).toContain(FOUND)
  expect(healed).not.toContain('BigQuery cannot find')
  expect(healed).not.toContain('backing off')
  expect(world.statusLines.at(-1)).toBeUndefined()
  // Well before the backoff would have ended or the next tick: the flush the
  // check asked for sends the session-start row and the call's two rows.
  await world.clock.advance(10)
  expect(world.acceptedRows().length).toBe(3)
  expect(writtenAgain(world)).toEqual([])
})

test('a headless run whose own rows all landed still writes the stderr line when the replay at its end finds the dataset gone', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const earlier = `${PARTITION_DIR}/spill-20261013T200102003Z-earlier-1.jsonl`
  const row = {
    timestamp: '2026-10-13T20:01:02.003Z', event_id: 'e'.repeat(32), event_type: 'STATE_DELTA', agent: 'coding-agent', session_id: 'earlier-session',
    invocation_id: null, user_id: 'dev', trace_id: null, span_id: 'bbbbbbbbbbbbbbbb', parent_span_id: null, content: '{"session_end":true}',
    content_parts: [], attributes: '{}', latency_ms: null, status: 'OK', error_message: null, is_truncated: false,
  }
  const file = packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: row.timestamp, table: TABLE_REF, row })])[0] as string
  world.files.set(earlier, file)
  world.replies = [{ hang: true }, OK_INSERT, DATASET_MISSING]
  await $.classic.SessionStart({ source: 'startup', session_id: SESSION_ID, model: 'example-model', cwd: '/work/my-repo' })
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.tool.call({ tool: 'Read', file_path: 'a.ts' })
  // The turn's batch is out when the session ends; it lands, then session_end
  // goes, then the replay of the earlier spill finds no dataset.
  await world.clock.advance(5000)
  const ending = $.session.end({ reason: 'other', sessionId: SESSION_ID, resume: { id: SESSION_ID } })
  await world.clock.advance(100)
  world.releaseHung(OK_INSERT.status, OK_INSERT.text)
  await ending
  expect(world.insertCalls().map(f => f.answered)).toEqual(['hang', 200, 404])
  // Nothing of this run was spilled or refused: the run had activity, and
  // that alone makes the line.
  expect(deadLetterFiles(world)).toEqual([[earlier, file]])
  // One line, ending in the setup.sh command that makes the dataset.
  const notices = world.logs.filter(l => l.startsWith('BigQuery Agent Analytics: rows could not be sent'))
  expect(notices.length).toBe(1)
  const line = notices[0] ?? ''
  const head = `BigQuery Agent Analytics: rows could not be sent (${HEADLINE}; to fix, ${FIX}). Run: `
  expect(line.startsWith(head)).toBe(true)
  expect(line.slice(head.length)).toMatch(SETUP)
})

test('/bqaa-status check that finds the table, with replays off and rows parked meanwhile, names /bqaa-backfill without saying rows are written again', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10', BQAA_AUTO_BACKFILL: 'false' })
  world.replies = [DATASET_MISSING]
  await startSession($)
  // Past 80% of the queue while the dataset is missing: rows are parked.
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  await world.clock.advance(5000)
  const parked = deadLetterFiles(world).length
  expect(parked > 0).toBe(true)
  world.replies = [TABLE_FOUND, OK_INSERT]
  const healed = await statusCheck($)
  expect(healed).toContain(FOUND)
  expect(healed).not.toContain('BigQuery cannot find')
  await world.clock.advance(10)
  // The rows still queued went; the parked ones wait for /bqaa-backfill,
  // and the transcript says so once, without claiming a write.
  expect(world.acceptedRows().length > 0).toBe(true)
  expect(deadLetterFiles(world).length).toBe(parked)
  expect(writtenAgain(world)).toEqual([])
  expect(world.transcript.filter(l => l.includes('/bqaa-backfill sends'))).toEqual([
    'bqaa: BigQuery found the table; /bqaa-backfill sends the 5 row(s) parked while rows could not be written.',
  ])
})

test('/bqaa-status check that finds the table replays the rows parked meanwhile a second later, as a send BigQuery takes after failures does', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on, { BQAA_MAX_QUEUE_ROWS: '10' })
  world.replies = [DATASET_MISSING]
  await startSession($)
  for (let i = 0; i < 4; i += 1) await $.tool.call({ tool: 'Read', file_path: `f${i}` })
  await world.clock.advance(5000)
  const parked = deadLetterFiles(world).map(([path]) => path)
  expect(parked.length > 0).toBe(true)
  world.replies = [TABLE_FOUND, OK_INSERT]
  expect(await statusCheck($)).toContain(FOUND)
  // The queued rows at once, then the parked ones a second later: long
  // before the next flush tick would replay them.
  await world.clock.advance(1100)
  expect(parked.map(path => world.files.get(path))).toEqual(parked.map(() => ''))
  const ids = world.acceptedRows().map(r => r.event_id)
  expect(ids.length).toBe(9)
  expect(new Set(ids).size).toBe(9)
  expect(writtenAgain(world)).toEqual([])
})

test('/bqaa-status check asks tables.get once more with a fresh token after a 401, and finds the table with it', { options: OPTIONS }, async () => {
  // Token pieces joined at run time (see helpers.ts).
  const stale = ['ya29', 'stale-probe-token-0001'].join('.')
  const fresh = ['ya29', 'fresh-probe-token-0002'].join('.')
  const printed = [stale, fresh]
  const asked: string[] = []
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/probe-retry' }, {})
  const io: Io = {
    now: () => 1000,
    after: () => ({ cancel: () => undefined }),
    // BigQuery refuses the first token and takes the fresh one.
    fetch: async (url, init) => {
      asked.push(`${init.method} ${url} ${init.headers.authorization ?? ''}`)
      return init.headers.authorization === `Bearer ${fresh}` ? TABLE_FOUND : { status: 401, text: '{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}' }
    },
    runCommand: async () => ({ exitCode: 0, stdout: `${printed.shift() ?? fresh}\n`, stderr: '' }),
    envToken: async () => undefined,
    readFile: async () => '',
    writeFile: async () => undefined,
    listDir: async () => [],
    storeGet: async () => undefined,
    storeSet: async () => undefined,
    storeDelete: async () => undefined,
    status: () => undefined,
    debug: () => undefined,
  }
  const writer = new Writer(io, () => config, () => 'probe-session')
  expect(await writer.probeDestination(3000, true)).toBe('BigQuery found my-gcp-project.agent_analytics.agent_events')
  const url = 'https://bigquery.googleapis.com/bigquery/v2/projects/my-gcp-project/datasets/agent_analytics/tables/agent_events?fields=tableReference'
  expect(asked).toEqual([`GET ${url} Bearer ${stale}`, `GET ${url} Bearer ${fresh}`])
  expect(writer.problem).toBeNull()
})

function probeClock(): {
  now: () => number
  after: (ms: number, fn: () => void) => { cancel: () => void }
  advance: (ms: number) => Promise<void>
  // Timers set and neither cancelled nor gone off yet.
  pending: () => number
} {
  let now = 1000
  const timers: Array<{ at: number; fn: () => void; cancelled: boolean }> = []
  const flushMicrotasks = async (): Promise<void> => {
    for (let i = 0; i < 8; i += 1) await Promise.resolve()
  }
  return {
    now: () => now,
    after: (ms, fn) => {
      const entry = { at: now + Math.max(0, ms), fn, cancelled: false }
      timers.push(entry)
      return { cancel: () => { entry.cancelled = true } }
    },
    pending: () => timers.filter(t => !t.cancelled).length,
    advance: async ms => {
      const target = now + ms
      await flushMicrotasks()
      while (true) {
        let next: { at: number; fn: () => void; cancelled: boolean } | undefined
        for (const t of timers) {
          if (!t.cancelled && t.at <= target && (!next || t.at < next.at)) next = t
        }
        if (!next) break
        next.cancelled = true
        now = Math.max(now, next.at)
        next.fn()
        await flushMicrotasks()
      }
      now = target
    },
  }
}

function makeProbeIo(
  clock: ReturnType<typeof probeClock>,
  fetchImpl: Io['fetch'],
  runCommandImpl: Io['runCommand'],
): Io {
  return {
    now: clock.now,
    after: clock.after,
    fetch: fetchImpl,
    runCommand: runCommandImpl,
    envToken: async () => undefined,
    readFile: async () => '',
    writeFile: async () => undefined,
    listDir: async () => [],
    storeGet: async () => undefined,
    storeSet: async () => undefined,
    storeDelete: async () => undefined,
    status: () => undefined,
    debug: () => undefined,
  }
}

test('probeDestination decouples token acquisition (15 s) from the 3 s HTTP timeout and refreshes deadlines on 401 retry', { options: OPTIONS }, async () => {
  const stale = ['ya29', 'slow-stale-token-0001'].join('.')
  const fresh = ['ya29', 'slow-fresh-token-0002'].join('.')
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/probe-decoupled' }, {})
  const url = 'https://bigquery.googleapis.com/bigquery/v2/projects/my-gcp-project/datasets/agent_analytics/tables/agent_events?fields=tableReference'

  // Case 1: token command takes 4000 ms (> 3000 ms HTTP budget, < 15000 ms token budget) and succeeds.
  {
    const clock = probeClock()
    const asked: string[] = []
    const writer = new Writer(
      makeProbeIo(
        clock,
        async (reqUrl, init) => {
          asked.push(`${init.method} ${reqUrl} ${init.headers.authorization ?? ''}`)
          return new Promise(resolve => { clock.after(100, () => resolve(TABLE_FOUND)) })
        },
        async () => new Promise(resolve => {
          clock.after(4000, () => resolve({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' }))
        }),
      ),
      () => config,
      () => 'probe-slow-token',
    )
    let result: string | null | undefined
    void writer.probeDestination(3000, true).then(v => { result = v })
    await clock.advance(3500)
    expect(result).toBeUndefined()
    expect(asked).toEqual([])
    await clock.advance(700)
    expect(result).toBe('BigQuery found my-gcp-project.agent_analytics.agent_events')
    expect(asked).toEqual([`GET ${url} Bearer ${fresh}`])
    expect(writer.problem).toBeNull()
  }

  // Case 2: hung/slow token command > 15000 ms reports tokenCommandTimeoutMs rather than 3000 ms.
  {
    const clock = probeClock()
    const asked: string[] = []
    const writer = new Writer(
      makeProbeIo(
        clock,
        async (reqUrl, init) => {
          asked.push(`${init.method} ${reqUrl}`)
          return TABLE_FOUND
        },
        async () => new Promise(resolve => {
          clock.after(20_000, () => resolve({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' }))
        }),
      ),
      () => config,
      () => 'probe-token-timeout',
    )
    let result: string | null | undefined
    void writer.probeDestination(3000, true).then(v => { result = v })
    await clock.advance(14_999)
    expect(result).toBeUndefined()
    await clock.advance(1)
    expect(result).toBe('no token within 15000 ms')
    expect(writer.snapshot().lastError?.message).toBe('destination check: no token within 15000 ms')
    expect(asked).toEqual([])
  }

  // Case 3: slow attempt 0 (2500 ms token + 1000 ms HTTP 401 = 3500 ms) drops token and gets fresh deadlines on retry.
  {
    const clock = probeClock()
    const printed = [stale, fresh]
    const delays = [2500, 2000]
    const asked: string[] = []
    const writer = new Writer(
      makeProbeIo(
        clock,
        async (reqUrl, init) => {
          asked.push(`${init.method} ${reqUrl} ${init.headers.authorization ?? ''}`)
          const ok = init.headers.authorization === `Bearer ${fresh}`
          return new Promise(resolve => {
            clock.after(1000, () => resolve(
              ok ? TABLE_FOUND : { status: 401, text: '{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}' },
            ))
          })
        },
        async () => {
          const val = printed.shift() ?? fresh
          const delay = delays.shift() ?? 100
          return new Promise(resolve => {
            clock.after(delay, () => resolve({ exitCode: 0, stdout: `${val}\n`, stderr: '' }))
          })
        },
      ),
      () => config,
      () => 'probe-401-refresh',
    )
    let result: string | null | undefined
    void writer.probeDestination(3000, true).then(v => { result = v })
    await clock.advance(6600)
    expect(result).toBe('BigQuery found my-gcp-project.agent_analytics.agent_events')
    expect(asked).toEqual([`GET ${url} Bearer ${stale}`, `GET ${url} Bearer ${fresh}`])
    expect(writer.problem).toBeNull()
  }

  // Case 4: HTTP tables.get still times out at 3000 ms once token is acquired.
  {
    const clock = probeClock()
    const writer = new Writer(
      makeProbeIo(
        clock,
        async () => new Promise(resolve => { clock.after(5000, () => resolve(TABLE_FOUND)) }),
        async () => ({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' }),
      ),
      () => config,
      () => 'probe-http-timeout',
    )
    let result: string | null | undefined
    void writer.probeDestination(3000, true).then(v => { result = v })
    await clock.advance(2999)
    expect(result).toBeUndefined()
    await clock.advance(1)
    expect(result).toBe('no answer (request timed out after 3000 ms)')
    expect(writer.snapshot().lastError?.message).toBe('destination check: request timed out after 3000 ms')
  }
})

const UNAUTHENTICATED = { status: 401, text: '{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}' }
const PROBE_CONFIG = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/probe-bounds' }, {})
const PROBE_FOUND = 'BigQuery found my-gcp-project.agent_analytics.agent_events'

test('the destination check ends within the token time and two requests\' time in all: after a slow token and a 401, the fresh token gets only what is left; after a quick 401, the whole token time', { options: OPTIONS }, async () => {
  const stale = ['ya29', 'bounded-stale-token-0005'].join('.')
  const fresh = ['ya29', 'bounded-fresh-token-0006'].join('.')
  // The token command takes 14 s, BigQuery refuses its token after 0.5 s,
  // and the token command run again never answers.
  {
    const clock = probeClock()
    const ran: number[] = []
    const writer = new Writer(
      makeProbeIo(
        clock,
        async () => new Promise(resolve => { clock.after(500, () => resolve(UNAUTHENTICATED)) }),
        async () => {
          ran.push(clock.now())
          if (ran.length > 1) return new Promise(() => undefined)
          return new Promise(resolve => { clock.after(14_000, () => resolve({ exitCode: 0, stdout: `${stale}\n`, stderr: '' })) })
        },
      ),
      () => PROBE_CONFIG,
      () => 'probe-slow-401',
    )
    const started = clock.now()
    let result: string | null | undefined
    let endedAt = 0
    void writer.probeDestination(3000, true).then(v => {
      result = v
      endedAt = clock.now()
    })
    await clock.advance(17_999)
    expect(result).toBeUndefined()
    await clock.advance(1)
    // 15 s for a token and 2 x 3 s for requests: 21 s in all. 14.5 s went,
    // and a request's 3 s is kept, so the fresh token gets 3.5 s.
    expect([result, endedAt - started, ran.map(at => at - started)]).toEqual(['no token within 3500 ms', 18_000, [0, 14_500]])
    expect(writer.snapshot().lastError?.message).toBe('destination check: no token within 3500 ms')
    expect(clock.pending()).toBe(0)
  }
  // A token at hand that BigQuery refuses at once: the fresh one gets the
  // whole 15 s, and comes in 14 s.
  {
    const clock = probeClock()
    const printed = [stale, fresh]
    const writer = new Writer(
      makeProbeIo(
        clock,
        async (url, init) => new Promise(resolve => { clock.after(100, () => resolve(init.headers.authorization === `Bearer ${fresh}` ? TABLE_FOUND : UNAUTHENTICATED)) }),
        async () => {
          const value = printed.shift() ?? fresh
          if (value === stale) return { exitCode: 0, stdout: `${stale}\n`, stderr: '' }
          return new Promise(resolve => { clock.after(14_000, () => resolve({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' })) })
        },
      ),
      () => PROBE_CONFIG,
      () => 'probe-quick-401',
    )
    let result: string | null | undefined
    void writer.probeDestination(3000, true).then(v => { result = v })
    await clock.advance(14_199)
    expect(result).toBeUndefined()
    await clock.advance(1)
    expect(result).toBe(PROBE_FOUND)
    expect(clock.pending()).toBe(0)
  }
})

test('a destination check answered at once, after a 401 and a fresh token too, leaves none of its timers set', { options: OPTIONS }, async () => {
  const stale = ['ya29', 'quick-stale-token-0007'].join('.')
  const fresh = ['ya29', 'quick-fresh-token-0008'].join('.')
  const clock = probeClock()
  const printed = [fresh, stale, fresh]
  const writer = new Writer(
    makeProbeIo(
      clock,
      async (url, init) => (init.headers.authorization === `Bearer ${fresh}` ? TABLE_FOUND : UNAUTHENTICATED),
      async () => ({ exitCode: 0, stdout: `${printed.shift() ?? fresh}\n`, stderr: '' }),
    ),
    () => PROBE_CONFIG,
    () => 'probe-quick',
  )
  // Found at once: neither the token's deadline (15 s) nor the request's
  // (3 s) is left set on the clock.
  expect(await writer.probeDestination(3000, true)).toBe(PROBE_FOUND)
  expect(clock.pending()).toBe(0)
  // Refused at once, then found with a fresh token at once.
  const again = new Writer(
    makeProbeIo(
      clock,
      async (url, init) => (init.headers.authorization === `Bearer ${fresh}` ? TABLE_FOUND : UNAUTHENTICATED),
      async () => ({ exitCode: 0, stdout: `${printed.shift() ?? fresh}\n`, stderr: '' }),
    ),
    () => PROBE_CONFIG,
    () => 'probe-quick-401',
  )
  expect(await again.probeDestination(3000, true)).toBe(PROBE_FOUND)
  expect([printed, clock.pending()]).toEqual([[], 0])
})

test('a destination check refused for a token another send has since replaced keeps the newer token: its retry goes with it, and no token command runs for it', { options: OPTIONS }, async () => {
  const stale = ['ya29', 'probe-stale-token-0009'].join('.')
  const fresh = ['ya29', 'send-fresh-token-0010'].join('.')
  const spare = ['ya29', 'spare-token-0011'].join('.')
  const printed = [stale, fresh, spare]
  const clock = probeClock()
  const asked: string[] = []
  let answerProbe = (): void => undefined
  const writer = new Writer(
    makeProbeIo(
      clock,
      async (url, init) => {
        const auth = init.headers.authorization ?? ''
        asked.push(`${init.method} ${url.split('/').pop()?.split('?')[0]} ${auth}`)
        // The check's request with the first token is answered last.
        if (init.method === 'GET' && auth === `Bearer ${stale}`) {
          await new Promise<void>(resolve => (answerProbe = resolve))
          return UNAUTHENTICATED
        }
        if (auth === `Bearer ${stale}`) return UNAUTHENTICATED
        return init.method === 'GET' ? TABLE_FOUND : OK_INSERT
      },
      async () => ({ exitCode: 0, stdout: `${printed.shift() ?? spare}\n`, stderr: '' }),
    ),
    () => PROBE_CONFIG,
    () => 'probe-stale-401',
  )
  const checking = writer.probeDestination(3000, true)
  for (let i = 0; i < 100 && asked.length === 0; i += 1) await Promise.resolve()
  expect(asked).toEqual([`GET agent_events Bearer ${stale}`])
  // Meanwhile a send gets a 401 for the same token, and lands with a fresh one.
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'f'.repeat(32), agent: 'coding-agent', sessionId: 'probe-stale-401', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  await writer.flush()
  answerProbe()
  expect(await checking).toBe(PROBE_FOUND)
  expect(asked).toEqual([`GET agent_events Bearer ${stale}`, `POST insertAll Bearer ${stale}`, `POST insertAll Bearer ${fresh}`, `GET agent_events Bearer ${fresh}`])
  expect(printed).toEqual([spare])
})

test('/bqaa-status registers with the argument hint [check], as /bqaa-backfill does with its words', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  expect(world.commandHints).toEqual({ 'bqaa-status': '[check]', 'bqaa-flush': undefined, 'bqaa-backfill': '[force] [wait]', 'bqaa-mod-setup': undefined, 'bqaa-setup': undefined })
})

const EXIT_TABLE_MISSING = {
  status: 404,
  text: '{"error":{"code":404,"message":"Not found: Table my-gcp-project:agent_analytics.agent_events","errors":[{"reason":"notFound"}],"status":"NOT_FOUND"}}',
}
const EXIT_TABLE_CREATED = { status: 200, text: '{"kind":"bigquery#table"}' }

const fetchOps = (world: ReturnType<typeof setUp>): string[] =>
  world.fetches.map(f => `${f.method} ${f.url.split('/').pop()?.split('?')[0]}`)

const spillRowCount = (files: Map<string, string>): number =>
  [...files.entries()]
    .filter(([p, text]) => text.length > 0 && /\/spill-/.test(p))
    .flatMap(([, text]) => text.trim().split('\n').map(l => (JSON.parse(l) as { row?: unknown }).row).filter(r => r !== undefined))
    .length

async function runShortHeadlessTurn($: Parameters<typeof startSession>[0], turnId: string): Promise<void> {
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await $.turn.start({ turnId, text: 'check' })
  await $.turn.complete({ turnId, answer: 'ok', durationMs: 25, isAborted: false, reason: 'answer' })
}

test('sendAtExit cuts off hung createTable cleanly at marks.post and spills all rows', { options: OPTIONS }, async ($, on) => {
  const token = ['ya29', 'exit-autocreate-hang-001'].join('.')
  const sessionId = '5c4b1d2e-8f7a-4c3b-9d1e-2a6f0b7c8d9e'
  const world = setUp(on, { BQAA_ACCESS_TOKEN: token })
  world.replies = [EXIT_TABLE_MISSING, { hang: true }]
  await runShortHeadlessTurn($, 't-exit-hang-create')
  const ending = $.session.end({ reason: 'other', sessionId, resume: { id: sessionId } })
  await world.clock.advance(800)
  await ending
  expect(fetchOps(world)).toEqual(['POST insertAll', 'POST tables'])
  expect(spillRowCount(world.files)).toBe(4)
})

test('sendAtExit skips createTable and spills immediately when remaining budget before marks.post is under 250 ms', { options: OPTIONS }, async () => {
  const token = ['ya29', 'exit-autocreate-tight-002'].join('.')
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-tight-budget' }, {})
  const clock = probeClock()
  const asked: string[] = []
  const files = new Map<string, string>()
  const baseIo = makeProbeIo(
    clock,
    async (reqUrl, init) => {
      asked.push(`${init.method} ${reqUrl.split('/').pop()?.split('?')[0]}`)
      if (asked.length === 1) {
        return new Promise(resolve => { clock.after(420, () => resolve(EXIT_TABLE_MISSING)) })
      }
      return EXIT_TABLE_CREATED
    },
    async () => ({ exitCode: 0, stdout: `${token}\n`, stderr: '' }),
  )
  const io: Io = {
    ...baseIo,
    envToken: async () => token,
    writeFile: async (p, text) => { files.set(p, text) },
  }
  const writer = new Writer(io, () => config, () => 'exit-tight-budget')
  const row: Parameters<Writer['enqueue']>[0] = {
    timestamp: '2026-11-08T00:00:00.000Z',
    event_id: '0123456789abcdef0123456789abcdef',
    event_type: 'SESSION_STARTED',
    agent: 'coding-agent',
    session_id: 'exit-tight-budget',
    invocation_id: null,
    user_id: 'dev',
    trace_id: null,
    span_id: null,
    parent_span_id: null,
    content: null,
    content_parts: [],
    attributes: '{}',
    latency_ms: null,
    status: 'OK',
    error_message: null,
    is_truncated: false,
  }
  writer.enqueue(row, 0)
  let result: { spilled: number } | undefined
  void writer.spillForExit(800, { sendQueued: true }).then(v => { result = v })
  await clock.advance(800)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll'])
  expect(spillRowCount(files)).toBe(1)
})

test('sendAtExit spills the rows, with no second insertAll, when the table it made leaves less than 450 ms of the exit budget', { options: OPTIONS }, async () => {
  const token = ['ya29', 'exit-autocreate-late-005'].join('.')
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-late-create' }, {})
  const clock = probeClock()
  const asked: string[] = []
  const files = new Map<string, string>()
  const io: Io = {
    ...makeProbeIo(
      clock,
      async (reqUrl, init) => {
        asked.push(`${init.method} ${reqUrl.split('/').pop()?.split('?')[0]}`)
        // The insertAll's 404 after 100 ms, the table made 300 ms later: 400 ms
        // of the 800 ms budget are left, under the 450 ms a request needs.
        if (asked.length === 1) return new Promise(resolve => { clock.after(100, () => resolve(EXIT_TABLE_MISSING)) })
        if (asked.length === 2) return new Promise(resolve => { clock.after(300, () => resolve(EXIT_TABLE_CREATED)) })
        return OK_INSERT
      },
      async () => ({ exitCode: 0, stdout: `${token}\n`, stderr: '' }),
    ),
    envToken: async () => token,
    writeFile: async (p, text) => { files.set(p, text) },
  }
  const writer = new Writer(io, () => config, () => 'exit-late-create')
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'e'.repeat(32), agent: 'coding-agent', sessionId: 'exit-late-create', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  let result: { spilled: number } | undefined
  void writer.spillForExit(800, { sendQueued: true }).then(v => { result = v })
  await clock.advance(800)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll', 'POST tables'])
  expect(spillRowCount(files)).toBe(1)
  expect(writer.snapshot().lastError?.message).toBe('session end: table created (my-gcp-project.agent_analytics.agent_events); rows spilled')
})

test('sendAtExit never calls createTable when dataset is missing', { options: OPTIONS }, async ($, on) => {
  const token = ['ya29', 'exit-autocreate-nods-003'].join('.')
  const sessionId = '5c4b1d2e-8f7a-4c3b-9d1e-2a6f0b7c8d9e'
  const world = setUp(on, { BQAA_ACCESS_TOKEN: token })
  world.replies = [DATASET_MISSING, EXIT_TABLE_CREATED, OK_INSERT]
  await runShortHeadlessTurn($, 't-exit-dataset-missing')
  await $.session.end({ reason: 'other', sessionId, resume: { id: sessionId } })
  expect(fetchOps(world)).toEqual(['POST insertAll'])
  expect(spillRowCount(world.files)).toBe(4)
})

test('sendAtExit spills without noisy missing-table stderr when retried insertAll hits streaming propagation 404 right after table creation', { options: OPTIONS }, async ($, on) => {
  const token = ['ya29', 'exit-autocreate-prop-004'].join('.')
  const sessionId = '5c4b1d2e-8f7a-4c3b-9d1e-2a6f0b7c8d9e'
  const world = setUp(on, { BQAA_ACCESS_TOKEN: token })
  world.replies = [EXIT_TABLE_MISSING, EXIT_TABLE_CREATED, EXIT_TABLE_MISSING]
  await runShortHeadlessTurn($, 't-exit-propagation')
  await $.session.end({ reason: 'other', sessionId, resume: { id: sessionId } })
  expect(fetchOps(world)).toEqual(['POST insertAll', 'POST tables', 'POST insertAll'])
  expect(spillRowCount(world.files)).toBe(4)
  expect(world.logs.filter(l => l.startsWith('BigQuery Agent Analytics: rows could not be sent'))).toEqual([])
})

test('/bqaa-status check stays within the 10 s command.run budget when the token helper hangs (15 s) and never sends a late tables.get after its deadline', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.processDelayMs = 15_000
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  const started = world.clock.now()
  let finishedAt = -1
  const checking = $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND }).then(res => {
    finishedAt = world.clock.now()
    return res
  })
  await world.clock.advance(8_000)
  const text = (await checking).text ?? ''
  expect(finishedAt - started).toBeLessThanOrEqual(8_000)
  expect(text).toContain('destination check: no token within 5000 ms')
  expect(world.fetches).toEqual([])

  // Direct Writer check: clamped token timeout reaches runCommand and an exhausted deadline skips tables.get.
  const fresh = ['ya29', 'budget-cap-token-0012'].join('.')
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/probe-cmd-budget' }, {})
  const clock = probeClock()
  const runTimeouts: number[] = []
  const asked: string[] = []
  const writer = new Writer(
    makeProbeIo(
      clock,
      async (reqUrl, init) => {
        asked.push(`${init.method} ${reqUrl}`)
        return TABLE_FOUND
      },
      async (_argv, timeoutMs) => {
        runTimeouts.push(timeoutMs)
        return new Promise(resolve => {
          clock.after(2_000, () => resolve({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' }))
        })
      },
    ),
    () => config,
    () => 'probe-cmd-budget',
  )
  let answer: string | null | undefined
  void writer.probeDestination(3000, true, 2000).then(v => { answer = v })
  await clock.advance(2000)
  expect(runTimeouts).toEqual([1000])
  expect(answer).toBe('no token within 1000 ms')
  expect(asked).toEqual([])

  // Even if token resolves right at the deadline (no time left for tables.get), tables.get is skipped.
  const clock2 = probeClock()
  const asked2: string[] = []
  const writer2 = new Writer(
    makeProbeIo(
      clock2,
      async (reqUrl, init) => {
        asked2.push(`${init.method} ${reqUrl}`)
        return TABLE_FOUND
      },
      async () => new Promise(resolve => {
        clock2.after(1500, () => resolve({ exitCode: 0, stdout: `${fresh}\n`, stderr: '' }))
      }),
    ),
    () => config,
    () => 'probe-deadline-expired',
  )
  // First seed an in-flight token fetch with long timeout; then call probeDestination with 1500 ms budget.
  writer2.prefetchToken()
  let answer2: string | null | undefined
  void writer2.probeDestination(3000, true, 1500).then(v => { answer2 = v })
  await clock2.advance(1500)
  expect(answer2).not.toBeNull()
  expect(asked2).toEqual([])
})

test('probeDestination retries once with a fresh token after 401 when 500 ms .. 3000 ms of a tight budget remains', { options: OPTIONS }, async () => {
  const stale = ['ya29', 'tight-stale-token-0013'].join('.')
  const fresh = ['ya29', 'tight-fresh-token-0014'].join('.')
  const clock = probeClock()
  const printed = [stale, fresh]
  const asked: string[] = []
  const writer = new Writer(
    makeProbeIo(
      clock,
      async (_reqUrl, init) => {
        const auth = init.headers.authorization ?? ''
        asked.push(auth)
        const delay = auth === `Bearer ${stale}` ? 2700 : 200
        return new Promise(resolve => {
          clock.after(delay, () => resolve(auth === `Bearer ${fresh}` ? TABLE_FOUND : UNAUTHENTICATED))
        })
      },
      async () => {
        const val = printed.shift() ?? fresh
        const delay = val === stale ? 2800 : 300
        return new Promise(resolve => {
          clock.after(delay, () => resolve({ exitCode: 0, stdout: `${val}\n`, stderr: '' }))
        })
      },
    ),
    () => PROBE_CONFIG,
    () => 'probe-tight-401-retry',
  )
  // attempt 0 consumes 2800 ms (token) + 2700 ms (401) = 5500 ms of an 8000 ms budget,
  // leaving 2500 ms (within 500 ms .. 3000 ms) for attempt 1's 300 ms token + 200 ms tables.get.
  let answer: string | null | undefined
  void writer.probeDestination(3000, true, 8000).then(v => { answer = v })
  await clock.advance(6100)
  expect(answer).toBe(PROBE_FOUND)
  expect(asked).toEqual([`Bearer ${stale}`, `Bearer ${fresh}`])
  expect(clock.pending()).toBe(0)
})

// The exit-wait boundaries that follow the writer's own clock, on a writer
// built by hand (the test clock of the other tests leaves Io.now alone): one
// row's batch out when the exit begins, each request answered as `script`
// says, `afterMs` after it is sent, then OK_INSERT. A token comes from the
// command.
function exitWithBatchOut(script: { afterMs: number; answer: { status: number; text: string } }[], overrides: Partial<Config> = {}) {
  const token = ['ya29', 'exit-batch-out-006'].join('.')
  const config: Config = { ...resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/dl' }, {}), ...overrides }
  const clock = probeClock()
  const asked: string[] = []
  const files = new Map<string, string>()
  const io: Io = {
    ...makeProbeIo(
      clock,
      async (reqUrl, init) => {
        asked.push(`${init.method} ${reqUrl.split('/').pop()?.split('?')[0]}`)
        const step = script[asked.length - 1] ?? { afterMs: 0, answer: OK_INSERT }
        return new Promise(resolve => { clock.after(step.afterMs, () => resolve(step.answer)) })
      },
      async () => ({ exitCode: 0, stdout: `${token}\n`, stderr: '' }),
    ),
    writeFile: async (p, text) => { files.set(p, text) },
  }
  const writer = new Writer(io, () => config, () => 'exit-batch-out')
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'f'.repeat(32), agent: 'coding-agent', sessionId: 'exit-batch-out', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  // Time moves a millisecond at a time, so what a timer sets going settles
  // before the clock reads later than the moment under test.
  const run = async (ms: number): Promise<void> => {
    for (let i = 0; i < ms; i += 1) await clock.advance(1)
  }
  // The flush sends its request, with the clock still on the exit's first moment.
  const sendBatch = async (): Promise<void> => {
    writer.requestFlush(0)
    for (let i = 0; i < 20 && asked.length === 0; i += 1) await clock.advance(0)
    expect(asked).toEqual(['POST insertAll'])
  }
  return { asked, files, writer, run, sendBatch }
}

test('a create answered too late for a retry leaves the table made: the batch out at the exit is spilled, with no notice', { options: OPTIONS }, async () => {
  // The insertAll's 404 100 ms into the exit, the table made 400 ms later:
  // 300 ms of the 800 ms budget are left, under the 450 ms a send needs.
  const { asked, files, writer, run, sendBatch } = exitWithBatchOut([
    { afterMs: 100, answer: EXIT_TABLE_MISSING },
    { afterMs: 400, answer: EXIT_TABLE_CREATED },
  ])
  await sendBatch()
  let result: { spilled: number } | undefined
  void writer.spillForExit(800, { sendQueued: true }).then(v => { result = v })
  await run(800)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll', 'POST tables'])
  expect(spillRowCount(files)).toBe(1)
  expect(writer.problem).toBe(null)
  expect(writer.snapshot().lastError?.message).toBe('session end: table created (my-gcp-project.agent_analytics.agent_events); rows spilled')
})

test('with too little of the exit budget left for a create, table-not-found during the exit wait creates no table: the batch is spilled and the notice raised', { options: OPTIONS }, async () => {
  // 240 ms into a 500 ms budget, inside its 250 ms wait: of the 260 ms left,
  // 150 ms are kept for the spill, which leaves under the 250 ms a create needs.
  const { asked, files, writer, run, sendBatch } = exitWithBatchOut([{ afterMs: 240, answer: EXIT_TABLE_MISSING }])
  await sendBatch()
  let result: { spilled: number } | undefined
  void writer.spillForExit(500, { sendQueued: true }).then(v => { result = v })
  await run(500)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll'])
  expect(spillRowCount(files)).toBe(1)
  expect(writer.problem?.missing).toBe('table')
})

test('when the answer comes too late for a send but a create still fits the budget, the table is made and the batch spilled, with no notice', { options: OPTIONS }, async () => {
  // 360 ms into the 800 ms budget: 440 ms left is under the 450 ms a send
  // needs, and past the 150 ms kept for the spill leaves 290 ms, over the
  // 250 ms a create needs.
  const { asked, files, writer, run, sendBatch } = exitWithBatchOut([
    { afterMs: 360, answer: EXIT_TABLE_MISSING },
    { afterMs: 0, answer: EXIT_TABLE_CREATED },
  ])
  await sendBatch()
  let result: { spilled: number } | undefined
  void writer.spillForExit(800, { sendQueued: true }).then(v => { result = v })
  await run(800)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll', 'POST tables'])
  expect(spillRowCount(files)).toBe(1)
  expect(writer.problem).toBe(null)
})

test('a token that runs out during the exit wait: no create without one, the batch is spilled and the notice raised', { options: OPTIONS }, async () => {
  // The token, good for 100 ms, carries the insertAll; its 404 comes 200 ms
  // into the exit, when no token is at hand and none may be fetched.
  const { asked, files, writer, run, sendBatch } = exitWithBatchOut([{ afterMs: 200, answer: EXIT_TABLE_MISSING }, { afterMs: 0, answer: EXIT_TABLE_CREATED }], { tokenTtlMs: 100 })
  await sendBatch()
  let result: { spilled: number } | undefined
  void writer.spillForExit(800, { sendQueued: true }).then(v => { result = v })
  await run(800)
  expect(result).toEqual({ spilled: 1 })
  expect(asked).toEqual(['POST insertAll'])
  expect(spillRowCount(files)).toBe(1)
  expect(writer.problem?.missing).toBe('table')
})

test('spillForExit ignores a 1 ms Io.now tick before raceUntil when waiting on in-flight batches (sendQueued: true)', { options: OPTIONS }, async () => {
  const clock = probeClock()
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-clock-tick-flights' }, {})
  const files = new Map<string, string>()
  let releaseFetch = (): void => undefined
  let fetchCalled = false
  let tickOnNextNow = false
  let nowCallsAfterArmed = 0
  const baseIo = makeProbeIo(
    clock,
    async () => {
      fetchCalled = true
      return new Promise(resolve => {
        releaseFetch = () => resolve(OK_INSERT)
      })
    },
    async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
  )
  const io: Io = {
    ...baseIo,
    now: () => {
      if (!tickOnNextNow) return clock.now()
      nowCallsAfterArmed += 1
      return nowCallsAfterArmed === 1 ? clock.now() : clock.now() + 1
    },
    writeFile: async (p, text) => {
      files.set(p, text)
    },
  }
  const writer = new Writer(io, () => config, () => 'exit-clock-tick-flights')
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: '1'.repeat(32), agent: 'coding-agent', sessionId: 'exit-clock-tick-flights', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  writer.requestFlush(0)
  for (let i = 0; i < 20 && !fetchCalled; i += 1) await clock.advance(0)
  expect(fetchCalled).toBe(true)

  tickOnNextNow = true
  let ended = false
  let result: { spilled: number } | undefined
  const ending = writer.spillForExit(800, { sendQueued: true }).then(v => {
    ended = true
    result = v
    return v
  })
  await clock.advance(399)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(ended).toBe(false)
  await clock.advance(1)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(ended).toBe(true)
  await ending
  expect(result).toEqual({ spilled: 1 })
  expect(spillRowCount(files)).toBe(1)
  releaseFetch()
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(clock.pending()).toBe(0)
})

test('spillForExit ignores a 1 ms Io.now tick before raceUntil when spilling queued rows with no in-flight batch (sendQueued: false)', { options: OPTIONS }, async () => {
  const clock = probeClock()
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-clock-tick-spill' }, {})
  let releaseWrite = (): void => undefined
  let writeStarted = false
  let tickOnNextNow = false
  let nowCallsAfterArmed = 0
  const io: Io = {
    ...makeProbeIo(
      clock,
      async () => OK_INSERT,
      async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
    ),
    now: () => {
      if (!tickOnNextNow) return clock.now()
      nowCallsAfterArmed += 1
      return nowCallsAfterArmed === 1 ? clock.now() : clock.now() + 1
    },
    writeFile: async () => {
      writeStarted = true
      await new Promise<void>(resolve => {
        releaseWrite = resolve
      })
    },
  }
  const writer = new Writer(io, () => config, () => 'exit-clock-tick-spill')
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: '2'.repeat(32), agent: 'coding-agent', sessionId: 'exit-clock-tick-spill', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)

  tickOnNextNow = true
  let ended = false
  let result: { spilled: number } | undefined
  const ending = writer.spillForExit(800, { sendQueued: false }).then(v => {
    ended = true
    result = v
    return v
  })
  await clock.advance(799)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(writeStarted).toBe(true)
  expect(ended).toBe(false)
  await clock.advance(1)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(ended).toBe(true)
  await ending
  expect(result).toEqual({ spilled: 1 })
  expect(clock.pending()).toBe(0)
  releaseWrite()
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
})

test('spillForExit subtracts elapsed flight-wait time from pending-write raceUntil budget when sendQueued is false', { options: OPTIONS }, async () => {
  const clock = probeClock()
  const config = resolveConfig({ project: 'my-gcp-project', dataset: 'agent_analytics', deadLetterDir: '/tmp/exit-flight-elapsed' }, {})
  let fetchCalled = false
  let releaseWrite = (): void => undefined
  let writeStarted = false
  const io: Io = {
    ...makeProbeIo(
      clock,
      async () => {
        fetchCalled = true
        return new Promise(resolve => {
          clock.after(100, () => resolve(OK_INSERT))
        })
      },
      async () => ({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }),
    ),
    writeFile: async () => {
      writeStarted = true
      await new Promise<void>(resolve => {
        releaseWrite = resolve
      })
    },
  }
  const writer = new Writer(io, () => config, () => 'exit-flight-elapsed')
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: '3'.repeat(32), agent: 'coding-agent', sessionId: 'exit-flight-elapsed', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)
  writer.requestFlush(0)
  for (let i = 0; i < 20 && !fetchCalled; i += 1) await clock.advance(0)
  expect(fetchCalled).toBe(true)

  // Queue a second row after the first batch is in flight so park() starts a hanging spill write.
  writer.enqueue(buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: '4'.repeat(32), agent: 'coding-agent', sessionId: 'exit-flight-elapsed', invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row, 0)

  let ended = false
  let result: { spilled: number } | undefined
  const ending = writer.spillForExit(800, { sendQueued: false }).then(v => {
    ended = true
    result = v
    return v
  })
  await clock.advance(100)
  expect(writeStarted).toBe(true)
  expect(ended).toBe(false)
  await clock.advance(699)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(ended).toBe(false)
  await clock.advance(1)
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
  expect(ended).toBe(true)
  await ending
  expect(result).toEqual({ spilled: 1 })
  expect(clock.pending()).toBe(0)
  releaseWrite()
  for (let i = 0; i < 4; i += 1) await clock.advance(0)
})

test('probeDestination ignores a 1 ms Io.now tick before computing attempt 0 token timeout', { options: OPTIONS }, async () => {
  const clock = probeClock()
  let tickOnNextNow = false
  let nowCallsAfterArmed = 0
  let releaseToken = (): void => undefined
  const io: Io = {
    ...makeProbeIo(
      clock,
      async () => TABLE_FOUND,
      async () => new Promise(resolve => {
        releaseToken = () => resolve({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' })
      }),
    ),
    now: () => {
      if (!tickOnNextNow) return clock.now()
      nowCallsAfterArmed += 1
      return nowCallsAfterArmed === 1 ? clock.now() : clock.now() + 1
    },
  }
  const writer = new Writer(io, () => PROBE_CONFIG, () => 'probe-clock-tick')
  tickOnNextNow = true
  let result: string | null | undefined
  void writer.probeDestination(3000, true, 8000).then(v => {
    result = v
  })
  await clock.advance(4999)
  expect(result).toBeUndefined()
  await clock.advance(1)
  expect(result).toBe('no token within 5000 ms')
  expect(writer.snapshot().lastError?.message).toBe('destination check: no token within 5000 ms')
  expect(clock.pending()).toBe(0)
  releaseToken()
})

test('/bqaa-status check and /bqaa-backfill stay within tight next.budget.remainingMs (0 ms, 200 ms, 1800 ms) without exceeding host command budget', { options: OPTIONS }, async ($, on) => {
  // 1. /bqaa-status check with remainingMs=200 and remainingMs=0 against a hanging credential helper:
  //    must return a structured timeout immediately without hitting the host 200ms/0ms rejection.
  const world = setUp(on)
  world.processDelayMs = 15_000
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })

  for (const remainingMs of [200, 0] as const) {
    const t0 = world.clock.now()
    const res = await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND }, { budgetMs: remainingMs })
    const elapsed = world.clock.now() - t0
    expect(elapsed).toBe(0)
    expect(res.text ?? '').toContain('destination check: timed out (insufficient command budget)')
    expect(world.fetches).toEqual([])
  }

  // 2. /bqaa-status check with remainingMs=1800 (commandBudgetMs = 300 -> tokenMs = 150) and hanging helper:
  {
    const t0 = world.clock.now()
    let finishedAt = -1
    const checking = $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND }, { budgetMs: 1_800 }).then(res => {
      finishedAt = world.clock.now()
      return res
    })
    await world.clock.advance(300)
    const text = (await checking).text ?? ''
    expect(finishedAt - t0).toBeLessThanOrEqual(300)
    expect(text).toContain('destination check: no token within 150 ms')
    expect(world.fetches).toEqual([])
  }

  // 3. /bqaa-backfill wait (interactive) and tight-budget boundaries (0, 200, 1500, 1501, 1749, 1800, 2000, NaN, Infinity):
  const tightRow1 = buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'a'.repeat(32), agent: 'coding-agent', sessionId: SESSION_ID, invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row
  world.files.set(
    `${PARTITION_DIR}/spill-20261112T100000000Z-tight-1.jsonl`,
    packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRow1 })])[0] as string,
  )
  const interactive200 = await $.command.run({ command: 'bqaa-backfill', args: 'wait', ...COMMAND }, { budgetMs: 200 })
  expect(interactive200.text ?? '').toContain('timed out waiting (insufficient command budget); backfill started in background; /bqaa-status shows progress.')
  const statusDuringTightBackfill = await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND }, { budgetMs: 200 })
  expect(statusDuringTightBackfill.text ?? '').toContain('backfill: running')

  // 4. Direct Writer.probeDestination(3000, true, 0), (3000, true, 1), and (3000, true, NaN)
  //    never inflate above budgetMs and never set lastError.
  const clock = probeClock()
  const runTimeouts: number[] = []
  let releaseTightToken = (): void => undefined
  const writer = new Writer(
    makeProbeIo(
      clock,
      async () => TABLE_FOUND,
      async (_argv, timeoutMs) => {
        runTimeouts.push(timeoutMs)
        return new Promise(resolve => {
          releaseTightToken = () => resolve({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' })
        })
      },
    ),
    () => PROBE_CONFIG,
    () => 'probe-tight-budget',
  )
  expect(await writer.probeDestination(3000, true, 0)).toBe('timed out (insufficient command budget)')
  expect(writer.snapshot().lastError).toBeNull()
  expect(await writer.probeDestination(3000, true, 1)).toBe('timed out (insufficient command budget)')
  expect(writer.snapshot().lastError).toBeNull()
  expect(await writer.probeDestination(3000, true, -5)).toBe('timed out (insufficient command budget)')
  expect(writer.snapshot().lastError).toBeNull()
  expect(await writer.probeDestination(3000, true, Number.NEGATIVE_INFINITY)).toBe('timed out (insufficient command budget)')
  expect(writer.snapshot().lastError).toBeNull()
  expect(await writer.probeDestination(3000, true, Number.NaN)).toBe('timed out (insufficient command budget)')
  expect(writer.snapshot().lastError).toBeNull()
  expect(runTimeouts).toEqual([])
  expect(clock.pending()).toBe(0)

  let out50: string | null | undefined
  void writer.probeDestination(3000, true, 50).then(v => {
    out50 = v
  })
  await clock.advance(25)
  expect(out50).toBe('no token within 25 ms')
  expect(runTimeouts).toEqual([25])
  expect(clock.pending()).toBe(0)
  releaseTightToken()

  let outInf: string | null | undefined
  let releaseInfToken = (): void => undefined
  const infTimeouts: number[] = []
  const infWriter = new Writer(
    makeProbeIo(
      clock,
      async () => TABLE_FOUND,
      async (_argv, timeoutMs) => {
        infTimeouts.push(timeoutMs)
        return new Promise(resolve => {
          releaseInfToken = () => resolve({ exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' })
        })
      },
    ),
    () => PROBE_CONFIG,
    () => 'probe-inf-budget',
  )
  const infPromise = infWriter.probeDestination(3000, true, Number.POSITIVE_INFINITY).then(v => {
    outInf = v
  })
  await Promise.resolve()
  expect(infTimeouts).toEqual([PROBE_CONFIG.tokenCommandTimeoutMs])
  releaseInfToken()
  await infPromise
  expect(outInf).toBe(`BigQuery found ${TABLE_REF}`)
  expect(clock.pending()).toBe(0)
})

test('interactive /bqaa-backfill wait respects 1800 ms command budget against hanging helper', { options: OPTIONS }, async ($, on) => {
  const worldWait = setUp(on)
  worldWait.processDelayMs = 15_000
  const tightRow1b = buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'a'.repeat(32), agent: 'coding-agent', sessionId: SESSION_ID, invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row
  worldWait.files.set(
    `${PARTITION_DIR}/spill-20261112T100000000Z-tight-1b.jsonl`,
    packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRow1b })])[0] as string,
  )
  await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
  const t0 = worldWait.clock.now()
  let finishedAt = -1
  const running1800 = $.command.run({ command: 'bqaa-backfill', args: 'wait', ...COMMAND }, { budgetMs: 1_800 }).then(res => {
    finishedAt = worldWait.clock.now()
    return res
  })
  await worldWait.clock.advance(300)
  const text = (await running1800).text ?? ''
  expect(finishedAt - t0).toBeLessThanOrEqual(300)
  const elapsedMatch = /still running after (\d+(?:\.\d+)?) s/.exec(text)
  expect(elapsedMatch).not.toBeNull()
  const elapsedSec = Number(elapsedMatch![1])
  expect(elapsedSec).toBeGreaterThan(0)
  expect(elapsedSec).toBeLessThanOrEqual(0.3)
})

const tightRowBoundary = buildRow({ eventType: 'STATE_DELTA', timestampMs: 0, eventId: 'b'.repeat(32), agent: 'coding-agent', sessionId: SESSION_ID, invocationId: null, userId: 'dev', traceId: null, spanId: null, parentSpanId: null, content: {}, attributes: {}, maxPayloadChars: 16384 }).row

// Budgets yielding waitMs < 250 ms (-50, 0, 200, 1500, 1501, 1749, NaN) must return immediately without starting a doomed backfill or writing in-flight marks.
for (const remainingMs of [-50, 0, 200, 1_500, 1_501, 1_749, Number.NaN] as const) {
  test(`headless -p /bqaa-backfill and /bqaa-status check skip doomed backfill at remainingMs=${String(remainingMs)}`, { options: OPTIONS }, async ($, on) => {
    const worldP = setUp(on)
    worldP.processDelayMs = 15_000
    worldP.files.set(
      `${PARTITION_DIR}/spill-20261112T100000000Z-tight-2.jsonl`,
      packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
    )
    await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
    const res = await $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND }, { budgetMs: remainingMs })
    expect(res.text ?? '').toContain('timed out (insufficient command budget) before this non-interactive run ended')
    expect(worldP.files.has(`${PARTITION_DIR}/spill-20261112T100000000Z-tight-2.jsonl.mark`)).toBe(false)
    expect(worldP.fetches).toEqual([])
    if (remainingMs === 1_749) {
      const checking = $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND }, { budgetMs: remainingMs })
      await worldP.clock.advance(250)
      const statusRes = await checking
      const statusText = statusRes.text ?? ''
      const noTokenMatch = /destination check: no token within (\d+) ms/.exec(statusText)
      expect(noTokenMatch).not.toBeNull()
      const noTokenMs = Number(noTokenMatch![1])
      expect(noTokenMs).toBeGreaterThanOrEqual(1)
      expect(noTokenMs).toBeLessThanOrEqual(125)
      expect(statusText).not.toContain('backfill: running')
    } else {
      const statusRes = await $.command.run({ command: 'bqaa-status', args: 'check', ...COMMAND }, { budgetMs: remainingMs })
      expect(statusRes.text ?? '').toContain('destination check: timed out (insufficient command budget)')
      expect(statusRes.text ?? '').not.toContain('backfill: running')
    }
  })
}

// remainingMs = 1800 (waitMs ~300ms >= 250ms) and 2000 (waitMs ~500ms) run bounded wait.
for (const [remainingMs, expectedWait] of [
  [1_800, 300],
  [2_000, 500],
] as const) {
  test(`headless -p /bqaa-backfill runs bounded wait at remainingMs=${remainingMs}`, { options: OPTIONS }, async ($, on) => {
    const worldP = setUp(on)
    worldP.processDelayMs = 15_000
    worldP.files.set(
      `${PARTITION_DIR}/spill-20261112T100000000Z-tight-3.jsonl`,
      packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
    )
    await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
    const t0 = worldP.clock.now()
    let finishedAt = -1
    const running = $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND }, { budgetMs: remainingMs }).then(res => {
      finishedAt = worldP.clock.now()
      return res
    })
    await worldP.clock.advance(expectedWait)
    const text = (await running).text ?? ''
    expect(finishedAt - t0).toBeLessThanOrEqual(expectedWait)
    expect(text).toContain('did not finish within')
    expect(text).toContain('before this non-interactive run ended')
  })
}

test('headless -p /bqaa-backfill caps remainingMs=Infinity at BACKFILL_WAIT_MS (8000 ms)', { options: OPTIONS }, async ($, on) => {
  const worldInf = setUp(on)
  worldInf.processDelayMs = 15_000
  worldInf.files.set(
    `${PARTITION_DIR}/spill-20261112T100000000Z-tight-inf.jsonl`,
    packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
  )
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  const t0 = worldInf.clock.now()
  let finishedAt = -1
  const runningInf = $.command.run({ command: 'bqaa-backfill', args: '', ...COMMAND }, { budgetMs: Number.POSITIVE_INFINITY }).then(res => {
    finishedAt = worldInf.clock.now()
    return res
  })
  await worldInf.clock.advance(8_000)
  const textInf = (await runningInf).text ?? ''
  expect(finishedAt - t0).toBeLessThanOrEqual(8_000)
  expect(textInf).toMatch(/did not finish within (?:7\.9\d+|8) s before this non-interactive run ended/)
})

for (const remainingMs of [Number.NaN, 1_500, 1_501] as const) {
  test(`interactive /bqaa-backfill wait starts background backfill at remainingMs=${String(remainingMs)}`, { options: OPTIONS }, async ($, on) => {
    const worldInt = setUp(on)
    worldInt.processDelayMs = 15_000
    worldInt.files.set(
      `${PARTITION_DIR}/spill-20261112T100000000Z-tight-int.jsonl`,
      packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
    )
    await $.session.start({ surface: 'terminal', isInteractive: true, cwd: '/work/my-repo' })
    if (remainingMs === 1_501) {
      const running = $.command.run({ command: 'bqaa-backfill', args: 'wait', ...COMMAND }, { budgetMs: remainingMs })
      await worldInt.clock.advance(5)
      const res = await running
      expect(res.text ?? '').toContain('/bqaa-status shows progress.')
    } else {
      const res = await $.command.run({ command: 'bqaa-backfill', args: 'wait', ...COMMAND }, { budgetMs: remainingMs })
      expect(res.text ?? '').toContain('backfill started in background; /bqaa-status shows progress.')
    }
    const status = await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND }, { budgetMs: 200 })
    expect(status.text ?? '').toContain('backfill: running')
  })
}

for (const mode of [
  { label: 'interactive /bqaa-status', isInteractive: true, args: '', expectProbe: false },
  { label: 'interactive /bqaa-status check', isInteractive: true, args: 'check', expectProbe: true },
  { label: 'headless -p /bqaa-status', isInteractive: false, args: '', expectProbe: true },
] as const) {
  test(`${mode.label} shares a single 1800 ms command deadline across slow fs.list and destination probe`, { options: OPTIONS }, async ($, on) => {
    const worldSlow = setUp(on)
    worldSlow.processDelayMs = 15_000
    worldSlow.files.set(
      `${PARTITION_DIR}/spill-20261112T100000000Z-slow-1.jsonl`,
      packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
    )
    let listDelayMs = 2_000
    const listCalls: string[] = []
    for (const dir of [PARTITION_DIR, DEAD_LETTER_DIR]) {
      on('fs.list', { path: dir }, async (_$, e, next) => {
        listCalls.push(e.path)
        if (listDelayMs > 0) await worldSlow.clock.sleep(listDelayMs)
        return next(e)
      })
    }
    await $.session.start({ surface: mode.isInteractive ? 'terminal' : null, isInteractive: mode.isInteractive, cwd: '/work/my-repo' })

    const t0 = worldSlow.clock.now()
    let finishedAt = -1
    const running = $.command.run({ command: 'bqaa-status', args: mode.args, ...COMMAND }, { budgetMs: 1_800 }).then(res => {
      finishedAt = worldSlow.clock.now()
      return res
    })
    await worldSlow.clock.advance(300)
    const res = await running
    const text = res.text ?? ''
    expect(finishedAt - t0).toBeLessThanOrEqual(300)
    expect(finishedAt - t0).toBeLessThan(1_800)
    expect(text).toContain('dead-letter: timed out listing dead-letter directory')
    if (mode.expectProbe) expect(text).toContain('destination check: no token within')
    expect(listCalls).toHaveLength(1)

    await worldSlow.clock.advance(2_000)
    expect(listCalls).toHaveLength(1)

    listDelayMs = 0
    worldSlow.processDelayMs = 0
    const recovered = await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND }, { budgetMs: 1_800 })
    expect(recovered.text ?? '').toContain(`dead-letter: 1 file(s)`)
  })
}

for (const mode of [
  { label: 'interactive /bqaa-backfill wait', isInteractive: true, args: 'wait' },
  { label: 'headless -p /bqaa-backfill', isInteractive: false, args: '' },
] as const) {
  test(`${mode.label} bounds slow fs.list under 1800 ms command deadline`, { options: OPTIONS }, async ($, on) => {
    const worldSlow = setUp(on)
    worldSlow.files.set(
      `${PARTITION_DIR}/spill-20261112T100000000Z-slow-2.jsonl`,
      packFiles([JSON.stringify({ v: 1, kind: 'spill', reason: 'session ended', attempts: 0, failed_at: '2026-11-12T10:00:00.000Z', table: TABLE_REF, row: tightRowBoundary })])[0] as string,
    )
    let listDelayMs = 2_000
    const listCalls: string[] = []
    for (const dir of [PARTITION_DIR, DEAD_LETTER_DIR]) {
      on('fs.list', { path: dir }, async (_$, e, next) => {
        listCalls.push(e.path)
        if (listDelayMs > 0) await worldSlow.clock.sleep(listDelayMs)
        return next(e)
      })
    }
    await $.session.start({ surface: mode.isInteractive ? 'terminal' : null, isInteractive: mode.isInteractive, cwd: '/work/my-repo' })

    const t0 = worldSlow.clock.now()
    let finishedAt = -1
    const running = $.command.run({ command: 'bqaa-backfill', args: mode.args, ...COMMAND }, { budgetMs: 1_800 }).then(res => {
      finishedAt = worldSlow.clock.now()
      return res
    })
    await worldSlow.clock.advance(300)
    const res = await running
    expect(finishedAt - t0).toBeLessThanOrEqual(300)
    expect(finishedAt - t0).toBeLessThan(1_800)
    expect(res.text ?? '').toContain('timed out listing dead-letter directory')
    expect(listCalls).toHaveLength(1)

    await worldSlow.clock.advance(2_000)
    expect(listCalls).toHaveLength(1)

    listDelayMs = 0
    const statusAfter = await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND }, { budgetMs: 1_800 })
    expect(statusAfter.text ?? '').toContain('dead-letter: 1 file(s)')
  })
}
