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

import { PLUGIN_VERSION, resolveConfig } from '../hooks/lib/config.ts'
import { packFiles, parseFile } from '../hooks/lib/deadletter.ts'
import { buildRow } from '../hooks/lib/rows.ts'
import { type Io, Writer } from '../hooks/lib/writer.ts'

import { ACCESS_TOKEN, COMMAND, OK_INSERT, OPTIONS, PARTITION_DIR, SESSION_ID, TABLE_REF, type World, deadLetterFiles, setUp, startSession, utf8Bytes } from './helpers.ts'

const PLUGIN = 'bigquery-agent-analytics-mod'

// What the host passes a hook drawing in the band above the prompt.
const BAND = {
  component: 'AbovePrompt',
  requestId: 'above-prompt',
  viewport: { columns: 120, rows: 40 },
  props: { hasSurvey: false, isWorking: false, maxRows: 10, bodyColumns: 115, scroll: { offset: 0, bodyRows: 10 }, view: {} },
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
  await $.session.start({ surface: 'ide', isInteractive: true, cwd: '/work/my-repo' })
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
  await $.session.start({ surface: 'ide', isInteractive: true, cwd: '/work/my-repo' })
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

test('headless session.end writes a single redacted diagnostic line to stderr when rows could not be sent, and stays silent when healthy or interactive', { options: OPTIONS }, async ($, on) => {
  const secret = ['canary', 'stderr', 'secret', '9876543210'].join('-')
  const world = setUp(on, { BQAA_ACCESS_TOKEN_COMMAND: `my-token-helper --password=${secret}` })
  world.processReply = { exitCode: 1, stdout: '', stderr: `auth failed: password=${secret}\n` }
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.advance(5000)
  await $.session.end({ reason: 'other', sessionId: 'headless-err', resume: { id: 'headless-err' } })
  expect(world.stderr.length).toBe(1)
  expect(world.stderr[0]).toBe('BigQuery Agent Analytics: rows could not be sent (no working Google credentials; to fix, check the token command `my-token-helper`).')
  expect(world.stderr[0]?.includes(secret)).toBe(false)

  // Healthy headless session writes 0 stderr lines.
  world.stderr.length = 0
  world.processReply = { exitCode: 0, stdout: `${ACCESS_TOKEN}\n`, stderr: '' }
  world.replies = [OK_INSERT]
  await $.session.start({ surface: null, isInteractive: false, cwd: '/work/my-repo' })
  await world.clock.settle()
  await $.session.end({ reason: 'other', sessionId: 'headless-ok', resume: { id: 'headless-ok' } })
  expect(world.stderr).toEqual([])

  // Interactive session with a problem writes 0 stderr lines (uses AbovePrompt/statusLine instead).
  world.replies = [{ status: 403, text: '{"error":{"code":403,"message":"Access Denied","errors":[{"reason":"accessDenied"}]}}' }]
  await startSession($)
  await world.clock.advance(5000)
  await $.session.end({ reason: 'prompt_input_exit', sessionId: 'interactive-err', resume: { id: 'interactive-err' } })
  expect(world.stderr).toEqual([])
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
  await $.session.start({ surface: 'ide', isInteractive: true, cwd: '/work/my-repo' })
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
  expect(world.stderr.length).toBe(1)
  const line = world.stderr[0] ?? ''
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

test('/bqaa-status registers with the argument hint [check], as /bqaa-backfill does with its words', { options: OPTIONS }, async ($, on) => {
  const hints = new Map<string, string | undefined>()
  on('command.register', ($, e, next) => {
    hints.set(e.name, e.argumentHint)
    return next(e)
  })
  setUp(on)
  await startSession($)
  expect(Object.fromEntries(hints)).toEqual({ 'bqaa-status': '[check]', 'bqaa-flush': undefined, 'bqaa-backfill': '[force] [wait]', 'bqaa-mod-setup': undefined, 'bqaa-setup': undefined })
})
