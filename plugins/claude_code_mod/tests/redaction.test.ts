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

// Round five's probes, end to end. Each payload reaches the mod through a
// tool, BigQuery refuses the batch, so every row goes out once in the request
// and is then parked on disk; the canary must be in neither. The canary is
// built at run time and has no known credential shape: only the key it sits
// under, or the JSON it sits in, can give it away.

import { expect, test } from 'claude-code/testing'

import { COMMAND, OPTIONS, type Row, type World, byType, deadLetterFiles, deadLetterRows, setUp, startSession } from './helpers.ts'

const CANARY = ['canary', 'value', 'r'.repeat(12)].join('-')
const REFUSED = { status: 403, text: '{"error":{"message":"permission denied","errors":[{"reason":"accessDenied"}]}}' }
const PAD = 'x'.repeat(30_000)
const DROPPED = /^\[TRUNCATED: \d+ chars of JSON text that could not be parsed and checked\]$/

// What went out in insertAll requests and what was parked, for one event type.
function rows(world: World, eventType: string): { sent: Row[]; parked: Row[] } {
  return { sent: world.sentRows().filter(r => r.event_type === eventType), parked: deadLetterRows(world).filter(r => r.event_type === eventType) }
}

// JSON past the parse budget that opens with a scalar, its credential on a
// later line than its name (the reviewer's probe).
for (const first of ['0', 'true', 'null']) {
  test(`an oversized list opening with ${first} is dropped, never sent or parked with its credential`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    const payload = `[${first}, {"password": [\n  "${CANARY}"\n], "padding": "${PAD}"}]`
    expect((JSON.parse(payload) as [unknown, { password: string[] }])[1].password[0]).toBe(CANARY)
    world.toolReply = () => ({ result: payload, text: payload })
    world.replies = [REFUSED]
    await startSession($)
    await $.tool.call({ tool: 'Read', file_path: 'synthetic.json' })
    await world.clock.advance(5000)
    const { sent, parked } = rows(world, 'TOOL_COMPLETED')
    expect([sent.length, parked.length]).toEqual([1, 1])
    for (const row of [...sent, ...parked]) {
      expect(JSON.stringify(row).includes(CANARY)).toBe(false)
      expect(row.is_truncated).toBe(true)
      expect((JSON.parse(row.content ?? '{}') as { result: string }).result).toMatch(DROPPED)
    }
  })
}

test('an oversized JSON-encoded document is dropped, never sent or parked with its credential', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const payload = JSON.stringify(JSON.stringify({ password: [CANARY], padding: PAD }, null, 2))
  world.toolReply = () => ({ result: payload, text: payload })
  world.replies = [REFUSED]
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'synthetic.json' })
  await world.clock.advance(5000)
  const { sent, parked } = rows(world, 'TOOL_COMPLETED')
  expect([sent.length, parked.length]).toEqual([1, 1])
  for (const row of [...sent, ...parked]) {
    expect(JSON.stringify(row).includes(CANARY)).toBe(false)
    expect((JSON.parse(row.content ?? '{}') as { result: string }).result).toMatch(DROPPED)
  }
})

// Plural and numbered credential names, each with its own canary, in every
// form the reviewers tried. A test fails naming the forms that leaked.
const NAMES = ['api_keys', 'access_tokens', 'signing_keys', 'decryption_keys', 'sessions', 'token2', 'secret1', 'passphrase2']
const canaryFor = (name: string): string => `${CANARY}-${name.replace(/_/g, '')}`

const leaked = (found: Row[]): string[] => NAMES.filter(name => JSON.stringify(found).includes(canaryFor(name)))

const RESULT_FORMS: [string, (name: string) => { result: unknown; text?: string }][] = [
  ['a structured tool result', name => ({ result: { [name]: canaryFor(name) } })],
  ['JSON text', name => ({ result: { type: 'text' }, text: JSON.stringify({ [name]: canaryFor(name) }) })],
  ['an assignment in text', name => ({ result: { type: 'text' }, text: `export ${name.toUpperCase()}=${canaryFor(name)}\nmode=fast` })],
  ['a URL query parameter', name => ({ result: { type: 'text' }, text: `GET https://api.example.test/v1/items?${name}=${canaryFor(name)}&page=2` })],
]

for (const [form, reply] of RESULT_FORMS) {
  test(`plural and numbered credential names: no value leaves in ${form}`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    world.replies = [REFUSED]
    world.toolReply = e => reply((e as { tool: string; file_path?: string }).file_path ?? '')
    await startSession($)
    for (const name of NAMES) await $.tool.call({ tool: 'Read', file_path: name })
    await world.clock.advance(5000)
    const { sent, parked } = rows(world, 'TOOL_COMPLETED')
    expect([sent.length, parked.length]).toEqual([NAMES.length, NAMES.length])
    expect({ sent: leaked(sent), parked: leaked(parked) }).toEqual({ sent: [], parked: [] })
  })
}

test('plural and numbered credential names: no value leaves as a tool argument', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [REFUSED]
  await startSession($)
  for (const name of NAMES) await $.tool.call({ tool: 'mcp__vault__read', [name]: canaryFor(name), description: `read ${name}` })
  await world.clock.advance(5000)
  const { sent, parked } = rows(world, 'TOOL_STARTING')
  expect([sent.length, parked.length]).toEqual([NAMES.length, NAMES.length])
  expect({ sent: leaked(sent), parked: leaked(parked) }).toEqual({ sent: [], parked: [] })
  // A listed key keeps its value.
  expect((JSON.parse(sent[0]?.content ?? '{}') as { args: { description: string } }).args.description).toBe('read api_keys')
})

// Round eight: credentials in key names. A key that is not on the list keeps
// its name unless the name itself holds a credential shape, a URL with
// user-info or a credential-named parameter, or a credential-named value:
// then the name becomes `[REDACTED_KEY:n]`. Each probe goes through capture,
// the background writer's request and the dead-letter file; the secret must
// be in no byte of any request body or parked file.
const KEY_SECRET = ['synthetic', 'key', 'secret', '0123456789'].join('-')
const TOKEN_KEY = ['ya29', KEY_SECRET].join('.')
const USERINFO_KEY = `https://${['demo', KEY_SECRET].join(':')}@example.invalid/report`
const QUERY_KEY = `https://example.invalid/report?access_token=${KEY_SECRET}&page=2`
const NAMED_KEY = `password=${KEY_SECRET}`

const nowhere = (world: World): boolean[] => [world.fetches.some(f => f.body.includes(KEY_SECRET)), deadLetterFiles(world).some(([, text]) => text.includes(KEY_SECRET))]

const KEY_FORMS: [string, () => { result: unknown; text?: string }][] = [
  ['a credential-shaped key in a structured result', () => ({ result: { [TOKEN_KEY]: 'unused' } })],
  ['a URL key with user-info', () => ({ result: { [USERINFO_KEY]: { status: 200 } } })],
  ['a URL key with a credential-named parameter', () => ({ result: { [QUERY_KEY]: null } })],
  ['a credential-named assignment as a key', () => ({ result: { [NAMED_KEY]: 1 } })],
  ['a credential key nested under listed keys', () => ({ result: { content: [{ type: 'text', text: 'ok', [TOKEN_KEY]: true }] } })],
  ['credential keys in JSON text', () => ({ result: { type: 'text' }, text: JSON.stringify({ [TOKEN_KEY]: 'unused', [USERINFO_KEY]: { status: 200 } }) })],
]

for (const [form, reply] of KEY_FORMS) {
  test(`key names: ${form} leaves neither in the request nor on disk`, { options: OPTIONS }, async ($, on) => {
    const world = setUp(on)
    world.replies = [REFUSED]
    world.toolReply = reply
    await startSession($)
    await $.tool.call({ tool: 'Read', file_path: 'report.json' })
    await world.clock.advance(5000)
    const { sent, parked } = rows(world, 'TOOL_COMPLETED')
    expect([sent.length, parked.length]).toEqual([1, 1])
    // Every request body and every parked byte, not only property values.
    expect(nowhere(world)).toEqual([false, false])
    // The entry is kept, under a placeholder name.
    for (const row of [...sent, ...parked]) expect(row.content ?? '').toMatch(/\[REDACTED_KEY:1\]/)
  })
}

test('key names: a credential-bearing argument name leaves neither in the request nor on disk', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [REFUSED]
  await startSession($)
  await $.tool.call({ tool: 'mcp__reports__fetch', [USERINFO_KEY]: 'unused', [TOKEN_KEY]: 'unused', description: 'fetch the report' })
  await world.clock.advance(5000)
  const { sent, parked } = rows(world, 'TOOL_STARTING')
  expect([sent.length, parked.length]).toEqual([1, 1])
  expect(nowhere(world)).toEqual([false, false])
  for (const row of [...sent, ...parked]) {
    expect((JSON.parse(row.content ?? '{}') as { args: Record<string, unknown> }).args).toEqual({ '[REDACTED_KEY:1]': '[REDACTED]', '[REDACTED_KEY:2]': '[REDACTED]', description: 'fetch the report' })
  }
})

test('/bqaa-status shows truncated rows when a row loses text past a credential line', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  world.replies = [REFUSED]
  await startSession($)
  await $.prompt.submit({ text: 'Use a 27" screen:\n--password=hunter2\nline three', wait: false, origin: { kind: 'composer' } })
  await world.clock.advance(5000)
  const { sent, parked } = rows(world, 'USER_MESSAGE_RECEIVED')
  expect([sent.length, parked.length]).toEqual([1, 1])
  for (const row of [...sent, ...parked]) {
    expect(row.is_truncated).toBe(true)
    expect(JSON.stringify(row)).not.toContain('hunter2')
    expect(JSON.stringify(row)).not.toContain('line three')
  }
  const status = (await $.command.run({ command: 'bqaa-status', args: '', ...COMMAND })).text ?? ''
  expect(status).toMatch(/truncated rows [1-9]/)
})

test('single-line credential tail cuts mark row.is_truncated and record attributes.redaction { values, dropped_chars }, while pure value redactions keep is_truncated false', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const tailCutCases: [string, string, number, number][] = [
    ["It's done: --password=hunter2 and then line three", "It's done: --password=[REDACTED]", 1, ' and then line three'.length],
    ['echo --password=hunter2 https://bob:s3cret@example.com && cat does-not-exist.txt', 'echo --password=[REDACTED]', 2, ' https://bob:s3cret@example.com && cat does-not-exist.txt'.length],
    ['mysql -u root --password=hunter2 mydb -e "SELECT 1"', 'mysql -u root --password=[REDACTED]', 1, ' mydb -e "SELECT 1"'.length],
    ['curl -H "Authorization: Bearer secret_token_123456" https://api.example.com/v1/health', 'curl -H "Authorization: [REDACTED]', 1, ' https://api.example.com/v1/health'.length],
  ]
  for (const [text] of tailCutCases) {
    await $.prompt.submit({ text, wait: false, origin: { kind: 'composer' } })
  }
  const pureValueCases = [
    'mysql -u root --password=hunter2',
    'curl -H "Authorization: Bearer secret_token_123456"',
    'GET https://api.example.test/v1/items?access_token=hunter2&page=2',
    'ordinary prompt with no secrets at all',
  ]
  for (const text of pureValueCases) {
    await $.prompt.submit({ text, wait: false, origin: { kind: 'composer' } })
  }
  await world.clock.advance(5000)
  const sent = world.acceptedRows().filter(r => r.event_type === 'USER_MESSAGE_RECEIVED')
  expect(sent.length).toBe(tailCutCases.length + pureValueCases.length)
  for (let i = 0; i < tailCutCases.length; i += 1) {
    const [, expectedText, expectedValues, expectedDropped] = tailCutCases[i] as [string, string, number, number]
    const row = sent[i] as Row
    expect(row.is_truncated).toBe(true)
    const content = JSON.parse(row.content ?? '{}') as { text_summary: string }
    expect(content.text_summary).toBe(expectedText)
    expect(JSON.stringify(row)).not.toContain('hunter2')
    expect(JSON.stringify(row)).not.toContain('s3cret')
    expect(JSON.stringify(row)).not.toContain('secret_token_123456')
    const attrs = JSON.parse(row.attributes ?? '{}') as { redaction?: { values: number; dropped_chars: number } }
    expect(attrs.redaction).toEqual({ values: expectedValues, dropped_chars: expectedDropped })
  }
  const eolRow = sent[tailCutCases.length] as Row
  expect(eolRow.is_truncated).toBe(false)
  expect((JSON.parse(eolRow.attributes ?? '{}') as { redaction?: unknown }).redaction).toEqual({ values: 1, dropped_chars: 0 })

  const bearerRow = sent[tailCutCases.length + 1] as Row
  expect(bearerRow.is_truncated).toBe(false)
  expect((JSON.parse(bearerRow.attributes ?? '{}') as { redaction?: unknown }).redaction).toEqual({ values: 1, dropped_chars: 0 })

  const urlRow = sent[tailCutCases.length + 2] as Row
  expect(urlRow.is_truncated).toBe(false)
  expect((JSON.parse(urlRow.content ?? '{}') as { text_summary: string }).text_summary).toBe('GET https://api.example.test/v1/items?access_token=[REDACTED]&page=2')
  expect((JSON.parse(urlRow.attributes ?? '{}') as { redaction?: unknown }).redaction).toEqual({ values: 1, dropped_chars: 0 })

  const cleanRow = sent[tailCutCases.length + 3] as Row
  expect(cleanRow.is_truncated).toBe(false)
  expect((JSON.parse(cleanRow.attributes ?? '{}') as { redaction?: unknown }).redaction).toBeUndefined()
})

// A value under a quoted key (JSON, a dict, a here-document) starts outside
// the key's quote, which closes before the separator: the value's own quote
// is not a quote around it. Such a value ends at its closing quote, so a
// pair alone loses nothing, the comma after it is one dropped character
// (never the secret's length), and an Authorization header whose token a
// pattern already redacted counts once.
test('quoted-key JSON pairs: a pair alone is no cut, a dropped comma counts one character, and a JSON Authorization header counts one value', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const cases: [string, string, boolean, { values: number; dropped_chars: number }][] = [
    ['"password": "aHVudGVyMg=="', '"password": [REDACTED]', false, { values: 1, dropped_chars: 0 }],
    ['     3\t  "password": "hunter2-hunter2",', '     3\t  "password": [REDACTED]', true, { values: 1, dropped_chars: 1 }],
    ['Sent {"Authorization": "Bearer abcdefghijklmnop"} to the API', 'Sent {"Authorization": [REDACTED]', true, { values: 1, dropped_chars: '} to the API'.length }],
    // In a here-document every quote is unknown to the scan: the key's quote
    // still closes before the value.
    ['cat <<EOF > creds.json\n  "password": "hunter2-hunter2",\nEOF', 'cat <<EOF > creds.json\n  "password": [REDACTED]\nEOF', true, { values: 1, dropped_chars: 1 }],
    ['cat <<EOF > creds.json\n  "password": "hunter2-hunter2"\nEOF', 'cat <<EOF > creds.json\n  "password": [REDACTED]\nEOF', false, { values: 1, dropped_chars: 0 }],
    // The same header in a log line's escaped JSON.
    ['log: {\\"Authorization\\": \\"Bearer abcdefghijklmnop\\"} done', 'log: {\\"Authorization\\": [REDACTED]', true, { values: 1, dropped_chars: '} done'.length }],
    // Inside `$'…'` the quote before the name is still open: the value ends
    // at the quote that closes it, and the command after it is dropped text.
    ["printf $'password: hunter2\\n' && echo 'done'", "printf $'password: [REDACTED]", true, { values: 1, dropped_chars: " && echo 'done'".length }],
  ]
  for (const [text] of cases) await $.prompt.submit({ text, wait: false, origin: { kind: 'composer' } })
  await world.clock.advance(5000)
  const sent = byType(world.acceptedRows(), 'USER_MESSAGE_RECEIVED')
  expect(sent.length).toBe(cases.length)
  cases.forEach(([text, expectedText, truncated, redaction], i) => {
    const row = sent[i] as Row
    const shown = { text, summary: (JSON.parse(row.content ?? '{}') as { text_summary: string }).text_summary, truncated: row.is_truncated, redaction: (JSON.parse(row.attributes ?? '{}') as { redaction?: unknown }).redaction }
    expect(shown).toEqual({ text, summary: expectedText, truncated, redaction })
    for (const secret of ['aHVudGVyMg', 'hunter2-hunter2', 'abcdefghijklmnop', 'hunter2']) expect(JSON.stringify(row)).not.toContain(secret)
  })
})

// `attributes.redaction` is the mod's own record, added after a row is
// redacted: a key of that name in a tool's payload is not on the allowlist,
// and its value goes like any other unlisted key's.
test('a tool payload key named redaction has its value redacted, while the row still records attributes.redaction', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  const memo = ['privileged', 'memo', 'about', 'layoffs'].join(' ')
  world.toolReply = () => ({ result: { redaction: { reason: memo }, text: 'password=hunter2' } })
  await startSession($)
  await $.tool.call({ tool: 'Read', file_path: 'memo.json' })
  await world.clock.advance(5000)
  const [row] = byType(world.acceptedRows(), 'TOOL_COMPLETED')
  expect(row).toBeDefined()
  const content = JSON.parse(row?.content ?? '{}') as { result?: { redaction?: unknown; text?: string } }
  expect(content.result?.redaction).toBe('[REDACTED]')
  expect(content.result?.text).toBe('password=[REDACTED]')
  expect(JSON.stringify(row)).not.toContain(memo)
  expect(JSON.stringify(row)).not.toContain('hunter2')
  // The key's value and the credential: two values, nothing past them.
  expect((JSON.parse(row?.attributes ?? '{}') as { redaction?: unknown }).redaction).toEqual({ values: 2, dropped_chars: 0 })
})
