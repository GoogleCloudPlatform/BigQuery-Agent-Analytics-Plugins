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

import { type Engine, expect, test } from 'claude-code/testing'

import { buildRow } from '../hooks/lib/rows.ts'
import { newStats, redactText } from '../hooks/lib/sanitize.ts'

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

// Each prompt as one row; what the row shows, cut and records, prompt by
// prompt.
async function promptRows($: Engine, world: World, texts: string[]): Promise<{ text: string; summary: string; truncated: boolean; redaction: unknown }[]> {
  for (const text of texts) await $.prompt.submit({ text, wait: false, origin: { kind: 'composer' } })
  await world.clock.advance(5000)
  const sent = byType(world.acceptedRows(), 'USER_MESSAGE_RECEIVED')
  expect(sent.length).toBe(texts.length)
  return sent.map((row, i) => ({
    text: texts[i] as string,
    summary: (JSON.parse(row.content ?? '{}') as { text_summary: string }).text_summary,
    truncated: row.is_truncated,
    redaction: (JSON.parse(row.attributes ?? '{}') as { redaction?: unknown }).redaction,
  }))
}

// A value's own quoting, as the shell, YAML or SQL reads it: the quote the
// value sits in closing right where the value starts (the value is glued on
// after it), a doubled quote (which goes on with the value), a backslash in
// single quotes (no escape there), a word glued on after a quoted string,
// and a JSON comma after one. The text is redacted as it always was; only
// what follows the value counts as dropped, and never the secret's length.
test('quoted value edges: an outer quote closing at the value, doubled quotes, a backslash in single quotes and a glued word end the value as the shell or YAML does, and only the text after it counts as dropped', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const cases: [string, string, boolean, { values: number; dropped_chars: number }][] = [
    ['curl -H "password: "s3cr3t-v4lue-XYZ', 'curl -H "password: [REDACTED]', false, { values: 1, dropped_chars: 0 }],
    ["curl -H 'Authorization: Bearer 'abcdef123456SECRET", "curl -H 'Authorization: [REDACTED]", false, { values: 1, dropped_chars: 0 }],
    ["curl -H 'Authorization: Bearer 'abcdef123456SECRET https://api.example.com", "curl -H 'Authorization: [REDACTED]", true, { values: 1, dropped_chars: ' https://api.example.com'.length }],
    ["password: 'p@ss''word'", 'password: [REDACTED]', false, { values: 1, dropped_chars: 0 }],
    ["password: 'it''s' tail", 'password: [REDACTED]', true, { values: 1, dropped_chars: ' tail'.length }],
    ['password: "abc"def', 'password: [REDACTED]', false, { values: 1, dropped_chars: 0 }],
    ['password: "abc",', 'password: [REDACTED]', true, { values: 1, dropped_chars: 1 }],
    ["password: 'abc\\' --verbose", 'password: [REDACTED]', true, { values: 1, dropped_chars: ' --verbose'.length }],
    // A quoted name and `=`, the value after the quote closes.
    ['export "DB_PASSWORD="s3cr3tV4lue --verbose', 'export "DB_PASSWORD=[REDACTED]', true, { values: 1, dropped_chars: ' --verbose'.length }],
    // An auth scheme after the quote the value sat in closes.
    ['curl -H "Authorization: "Bearer abcdef123456SECRET', 'curl -H "Authorization: [REDACTED]', false, { values: 1, dropped_chars: 0 }],
    // After a closing quote, a colon ends the value, doubled quote or not.
    ["password: 'it''s': tail", 'password: [REDACTED]', true, { values: 1, dropped_chars: ': tail'.length }],
  ]
  const shown = await promptRows($, world, cases.map(([text]) => text))
  expect(shown).toEqual(cases.map(([text, summary, truncated, redaction]) => ({ text, summary, truncated, redaction })))
  for (const secret of ['s3cr3t-v4lue-XYZ', 'abcdef123456SECRET', "p@ss''word", "it''s", 's3cr3tV4lue']) expect(JSON.stringify(world.acceptedRows())).not.toContain(secret)
})

// `NAME= value`, a blank after the `=`: the named-value rule redacts the
// value (`NAME= [REDACTED]`) and the list-name rule then drops the blank
// before the marker, as it always has. The value counts once, and the blank
// is no dropped text; a real tail still is.
test('NAME= value with a blank after the = reads NAME=[REDACTED] as before, counts one value, and drops no text but a real tail', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const cases: [string, string, boolean, { values: number; dropped_chars: number }][] = [
    ['Set DB_PASSWORD= hunter2', 'Set DB_PASSWORD=[REDACTED]', false, { values: 1, dropped_chars: 0 }],
    ["Set DB_PASSWORD= 'hunter2' --production", 'Set DB_PASSWORD=[REDACTED]', true, { values: 1, dropped_chars: ' --production'.length }],
    // A marker with more of its word after it is a value of its own.
    ['DB_PASSWORD= [REDACTED]:glued_secret', 'DB_PASSWORD=[REDACTED]', false, { values: 1, dropped_chars: 0 }],
  ]
  const shown = await promptRows($, world, cases.map(([text]) => text))
  expect(shown).toEqual(cases.map(([text, summary, truncated, redaction]) => ({ text, summary, truncated, redaction })))
  for (const secret of ['hunter2', 'glued_secret']) expect(JSON.stringify(world.acceptedRows())).not.toContain(secret)
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

test('CLI flags, positional secrets, Markdown labels/tables, code configs, armor, and slash/query URL userinfo redact cleanly while preserving safe lookalikes', () => {
  const s = 's3cr3tXyZ9'
  const mustRedact: [string, RegExp][] = [
    [`mysql -u root -p${s} app`, /mysql -u root -p\[REDACTED\] app/],
    [`mysql -u root -p ${s} app`, /mysql -u root -p \[REDACTED\] app/],
    [`psql -U postgres -W ${s} -h localhost`, /psql -U postgres -W \[REDACTED\] -h localhost/],
    [`redis-cli -h host -a ${s} ping`, /redis-cli -h host -a \[REDACTED\] ping/],
    [`mongosh -u root -p ${s}`, /mongosh -u root -p \[REDACTED\]/],
    [`sqlcmd -S host -U sa -P ${s}`, /sqlcmd -S host -U sa -P \[REDACTED\]/],
    [`twine upload -u __token__ -p ${s} dist/*`, /twine upload -u __token__ -p \[REDACTED\] dist\/\*/],
    [`zip -P ${s} out.zip file.txt`, /zip -P \[REDACTED\] out\.zip file\.txt/],
    [`curl -u admin:${s} https://api.example.com/v1/items`, /curl -u admin:\[REDACTED\] https:\/\/api\.example\.com\/v1\/items/],
    [`curl --user admin:${s} https://api.example.com/v1/items`, /curl --user admin:\[REDACTED\] https:\/\/api\.example\.com\/v1\/items/],
    [`docker login -u ci -p ${s} registry.example.com`, /docker login -u ci -p \[REDACTED\] registry\.example\.com/],
    [`sshpass -p ${s} ssh root@db.internal uptime`, /sshpass -p \[REDACTED\] ssh root@db\.internal uptime/],
    [`aws secretsmanager put-secret-value --secret-id prod/db --secret-string ${s}`, /--secret-id prod\/db --secret-string \[REDACTED\]/],
    [`aws ssm put-parameter --name /db/pw --value ${s}`, /--name \/db\/pw --value \[REDACTED\]/],
    [`gh secret set DB_PASS --body ${s}`, /gh secret set DB_PASS --body \[REDACTED\]/],
    [`vault kv put secret/db value=${s}`, /vault kv put secret\/db value=\[REDACTED\]/],
    [`aws configure set aws_secret_access_key ${s}`, /aws configure set aws_secret_access_key \[REDACTED\]/],
    [`git config --global user.password ${s}`, /git config --global user\.password \[REDACTED\]/],
    [`ENV DB_PASSWORD ${s}`, /ENV DB_PASSWORD \[REDACTED\]/],
    [`**Password**: ${s}`, /\*\*Password\*\*: \[REDACTED\]/],
    [`**Password:** ${s}`, /\*\*Password:\*\* \[REDACTED\]/],
    [`__api_key__: ${s}`, /__api_key__: \[REDACTED\]/],
    [`\`password\`: \`${s}\``, /`password`: `\[REDACTED\]`/],
    [`Password (staging): ${s}`, /Password \(staging\): \[REDACTED\]/],
    [`| \`DB_PASSWORD\` | \`${s}\` |`, /\| `DB_PASSWORD` \| `\[REDACTED\]` \|/],
    [`| Setting | Value |\n| --- | --- |\n| api_key | ${s} |\n| region | us-central1 |`, /\| api_key \| \[REDACTED\] \|\n\| region \| us-central1 \|/],
    [`os.environ["DB_PASSWORD"] = "${s}"`, /os\.environ\["DB_PASSWORD"\] = "\[REDACTED\]"/],
    [`os.environ.setdefault("API_KEY", "${s}")`, /^os\.environ\.setdefault\("API_KEY", "\[REDACTED\]"\)$/],
    [`process.env["API_KEY"] = "${s}"`, /process\.env\["API_KEY"\] = "\[REDACTED\]"/],
    [`client.setPassword("${s}")`, /client\.setPassword\("\[REDACTED\]"\)/],
    [`withApiKey("${s}")`, /withApiKey\("\[REDACTED\]"\)/],
    [`<password>${s}</password>`, /<password>\[REDACTED\]<\/password>/],
    [`CREATE USER 'app'@'%' IDENTIFIED BY '${s}';`, /CREATE USER 'app'@'%' IDENTIFIED BY '\[REDACTED\]';/],
    [`machine api.example.com\nlogin admin\npassword ${s}`, /password \[REDACTED\]/],
    [`-----BEGIN PGP PRIVATE KEY BLOCK-----\nsecretBody\n-----END PGP PRIVATE KEY BLOCK-----`, /^\[REDACTED:private-key\]$/],
    [`PuTTY-User-Key-File-2: ssh-rsa\nPrivate-Lines: 2\n${s}Line1\n${s}Line2\nPrivate-MAC: deadbeef`, /^\[REDACTED:private-key\]$/],
    ['AGE-SECRET-KEY-1QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ4J6M3W', /^\[REDACTED:private-key\]$/],
    ['foo.eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U', /^foo\.\[REDACTED:jwt\]$/],
    ['x=.eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U', /^x=\.\[REDACTED:jwt\]$/],
    ['token=.eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U', /^token=\[REDACTED\]$/],
    [`postgres://app:abc/${s}@db.internal:5432/app`, /^postgres:\/\/\[REDACTED\]@db\.internal:5432\/app$/],
    [`https://user:p?a=1#${s}@example.com/path`, /^https:\/\/\[REDACTED\]@example\.com\/path$/],
    [`http://user:${s}@[::1]:8080/api`, /^http:\/\/\[REDACTED\]@\[::1\]:8080\/api$/],
  ]
  for (const [input, pattern] of mustRedact) {
    const stats1 = newStats()
    const out1 = redactText(input, stats1)
    expect(out1).not.toContain(s)
    expect(out1).toMatch(pattern)
    expect(stats1.redactions > 0).toBe(true)
    const stats2 = newStats()
    const out2 = redactText(out1, stats2)
    expect(out2).toBe(out1)
    expect(stats2.redactions).toBe(0)
  }

  const keepUntouched = [
    'mkdir -p /tmp/foo/bar',
    'docker run -p 8080:80 nginx',
    'claude -p hello',
    'git config --global credential.helper store',
    'psql -p 5432 -U postgres',
    'https://www.npmjs.com/package/@types/node',
    'https://medium.com/@username/post',
    'https://api.example.com:8443/v1?email=alice@example.com',
  ]
  for (const input of keepUntouched) {
    const stats = newStats()
    expect(redactText(input, stats)).toBe(input)
    expect(stats.redactions).toBe(0)
  }
})

test('multi-line YAML block scalars, K8s name/value pairs, and prose-embedded multi-line JSON redact nested secrets whole while preserving siblings', { options: OPTIONS }, async ($, on) => {
  const world = setUp(on)
  await startSession($)
  const prompts = [
    'password: |\n  hunter2Line1\n\n  hunter2Line2\nuser: admin',
    '- name: DB_PASSWORD\n  value: myK8sSecret99\n- name: LOG_LEVEL\n  value: info',
    'Config:\n{\n  "api_keys": [\n    "Zq8wLmN4pT7vKc2R",\n    "Yx5nQpM9kR2wLs8T"\n  ],\n  "region": "us-central1"\n}',
  ]
  const shown = await promptRows($, world, prompts)
  expect(shown[0]!.summary).toBe('password: [REDACTED]\nuser: admin')
  expect(shown[1]!.summary).toBe('- name: DB_PASSWORD\n  value: [REDACTED]\n- name: LOG_LEVEL\n  value: info')
  expect(shown[2]!.summary).toContain('Config:\n{')
  expect(shown[2]!.summary).toContain('"region": "us-central1"')
  for (const leaked of ['hunter2Line1', 'hunter2Line2', 'myK8sSecret99', 'Zq8wLmN4pT7vKc2R', 'Yx5nQpM9kR2wLs8T']) {
    expect(JSON.stringify(world.acceptedRows())).not.toContain(leaked)
  }
})

test('ordinary English prose after "basic" stays untouched while real Basic/Bearer headers and user:pass Base64 credentials redact', () => {
  for (const prose of [
    'The API uses basic authentication over TLS.',
    'Here is a basic implementation of the parser.',
    'Add basic troubleshooting steps to the README.',
    'Follow the Basic configuration guide below.',
  ]) {
    const stats = newStats()
    expect(redactText(prose, stats)).toBe(prose)
    expect(stats.redactions).toBe(0)
  }

  for (const [cred, expected] of [
    ['Authorization: Basic dXNlcjpwYXNz', 'Authorization: [REDACTED]'],
    ['Proxy-Authorization: Basic secretwordonly', 'Proxy-Authorization: [REDACTED]'],
    ['Basic YWRtaW46c2VjcmV0', 'Basic [REDACTED]'],
    ['Bearer tok_abc123def456', 'Bearer [REDACTED]'],
  ] as const) {
    const stats = newStats()
    expect(redactText(cred, stats)).toBe(expected)
    expect(stats.redactions).toBe(1)
  }
})

test('unclosed PEM and PGP private-key headers mark row.is_truncated and count dropped_chars', () => {
  const pemText = 'A PKCS#8 key starts with -----BEGIN PRIVATE KEY----- and ends with a matching footer line. Check both before you parse it, and never log the body.'
  const pemBuilt = buildRow({
    eventType: 'AGENT_RESPONSE',
    timestampMs: 0,
    eventId: 'e1',
    agent: 'a',
    sessionId: 's',
    invocationId: 'i',
    userId: 'u',
    traceId: 't',
    spanId: 'p',
    parentSpanId: null,
    content: { response: pemText },
    attributes: {},
    maxPayloadChars: 16384,
  })
  expect(pemBuilt.row.is_truncated).toBe(true)
  expect(JSON.parse(pemBuilt.row.content ?? '{}').response).toBe('A PKCS#8 key starts with [REDACTED:private-key]')
  expect(JSON.parse(pemBuilt.row.attributes ?? '{}').redaction).toEqual({ values: 1, dropped_chars: 94 })

  const pgpText = 'Header -----BEGIN PGP PRIVATE KEY BLOCK----- trailing text dropped'
  const pgpStats = newStats()
  expect(redactText(pgpText, pgpStats)).toBe('Header [REDACTED:private-key]')
  expect(pgpStats.truncated).toBe(true)
  expect(pgpStats.redactions).toBe(1)
  expect(pgpStats.droppedChars).toBe(' trailing text dropped'.length)
})

test('redactText stays linear (O(N) <= 100 ms at n=80000) across adversarial backtracking shapes', () => {
  const generators: Record<string, (n: number) => string> = {
    'url + dots + x': n => 'see http://h/' + '.'.repeat(n) + 'x',
    'eyJ-eyJ': n => 'eyJ' + 'aaaaaaaa-eyJ'.repeat((n / 12) | 0),
    'backslash CR LF': n => '\\\r\n'.repeat((n / 3) | 0),
    'trail-parens': n => 'https://example.com/' + '('.repeat(n),
    'brace-chain': n => '{'.repeat(n),
  }
  for (const [label, gen] of Object.entries(generators)) {
    const inp10 = gen(10_000)
    const inp80 = gen(80_000)
    const t0 = performance.now()
    redactText(inp10, newStats())
    const ms10 = Math.max(0.1, performance.now() - t0)
    const t1 = performance.now()
    redactText(inp80, newStats())
    const ms80 = performance.now() - t1
    expect({
      label,
      under100ms: ms80 < 100,
      linearScaling: ms80 <= Math.max(25, ms10 * 20),
    }).toEqual({ label, under100ms: true, linearScaling: true })
  }
})

test('prose-embedded JSON fails closed beyond nesting depth 20 for both objects and arrays without leaking secrets', () => {
  const secret = ['deep', 'secret', '001'].join('-')
  const wrapObj = (depth: number, leaf: unknown): unknown => {
    let cur = leaf
    for (let i = 0; i < depth; i += 1) cur = { nest: cur }
    return cur
  }
  const wrapArr = (depth: number, leaf: unknown): unknown => {
    let cur = leaf
    for (let i = 0; i < depth; i += 1) cur = [cur]
    return { root: cur }
  }

  for (const depth of [19, 20]) {
    for (const wrapped of [wrapObj(depth, { password: secret }), wrapArr(depth - 1, { password: secret })]) {
      const input = `Result:\n${JSON.stringify(wrapped, null, 2)}`
      const stats = newStats()
      const out = redactText(input, stats)
      expect(out).not.toContain(secret)
      expect(out).toContain('"password": "[REDACTED]"')
      expect(stats.truncated).toBe(false)
      expect(stats.redactions).toBeGreaterThanOrEqual(1)

      const built = buildRow({
        eventType: 'TOOL_COMPLETED',
        timestampMs: 0,
        eventId: 'e1',
        agent: 'a',
        sessionId: 's',
        invocationId: 'i',
        userId: 'u',
        traceId: 't',
        spanId: 'p',
        parentSpanId: null,
        content: { result: input },
        attributes: {},
        maxPayloadChars: 65_536,
      })
      expect(built.row.content ?? '').not.toContain(secret)
      expect(built.row.content ?? '').toContain('\\"password\\": \\"[REDACTED]\\"')
      expect(built.row.is_truncated).toBe(false)
      expect(built.redactions).toBeGreaterThanOrEqual(1)
    }
  }

  for (const depth of [21, 22]) {
    for (const wrapped of [wrapObj(depth, { password: secret }), wrapArr(depth - 1, { password: secret })]) {
      const input = `Result:\n${JSON.stringify(wrapped, null, 2)}`
      const stats = newStats()
      const out = redactText(input, stats)
      expect(out).not.toContain(secret)
      expect(out).toContain('[TRUNCATED: nested too deep]')
      expect(stats.truncated).toBe(true)
      expect(stats.redactions).toBeGreaterThanOrEqual(1)
      expect(stats.droppedChars).toBeGreaterThan(0)

      const built = buildRow({
        eventType: 'TOOL_COMPLETED',
        timestampMs: 0,
        eventId: 'e1',
        agent: 'a',
        sessionId: 's',
        invocationId: 'i',
        userId: 'u',
        traceId: 't',
        spanId: 'p',
        parentSpanId: null,
        content: { result: input },
        attributes: {},
        maxPayloadChars: 65_536,
      })
      const serialized = built.row.content ?? ''
      expect(serialized).not.toContain(secret)
      expect(serialized).toContain('[TRUNCATED: nested too deep]')
      expect(built.row.is_truncated).toBe(true)
      expect(built.redactions).toBeGreaterThanOrEqual(1)
      const redMeta = (JSON.parse(built.row.attributes ?? '{}') as { redaction?: { values: number; dropped_chars: number } }).redaction
      expect((redMeta?.dropped_chars ?? 0) > 0).toBe(true)
    }
  }
})

test('quoted secret assignments and CLI/YAML/unquoted multi-line JSON do not expose nested secret payloads', () => {
  const secret = ['quoted', 'json', 'secret', '002'].join('-')
  const singleQuoted = `export APP_SECRET='{\n  "payload": {\n    "value": "${secret}"\n  }\n}'`
  const doubleQuoted = `export APP_SECRET="{\\n  \\"payload\\": {\\n    \\"value\\": \\"${secret}\\"\\n  }\\n}"`
  const cliFlagQuoted = `aws secretsmanager put-secret-value --secret-id app/cfg --secret-string '{\n  "payload": {\n    "value": "${secret}"\n  }\n}'`

  for (const [sample, expectedPrefix, expectTruncated] of [
    [singleQuoted, 'export APP_SECRET=[REDACTED]', true],
    [`${singleQuoted}\necho done`, 'export APP_SECRET=[REDACTED]\necho done', false],
    [doubleQuoted, 'export APP_SECRET=[REDACTED]', false],
    [`export APP_SECRET="{\n  \\"payload\\": {\n    \\"value\\": \\"${secret}\\"\n  }\n}"\necho done`, 'export APP_SECRET=[REDACTED]\necho done', false],
    [`export APP_SECRET='{"payload":{"value":"${secret}"}}'`, 'export APP_SECRET=[REDACTED]', false],
    [cliFlagQuoted, 'aws secretsmanager put-secret-value --secret-id app/cfg --secret-string [REDACTED]', true],
    [`${cliFlagQuoted} --region us`, 'aws secretsmanager put-secret-value --secret-id app/cfg --secret-string [REDACTED] --region us', false],
    [`aws secretsmanager put-secret-value --secret-id app/cfg --secret-string {\n  "payload": {\n    "value": "${secret}"\n  }\n}`, 'aws secretsmanager put-secret-value --secret-id app/cfg --secret-string [REDACTED]', false],
    [`export APP_SECRET={\n  "payload": {\n    "value": "${secret}"\n  }\n}`, 'export APP_SECRET=[REDACTED]', false],
    [`export APP_SECRET=\n{\n  "payload": {\n    "value": "${secret}"\n  }\n}\necho done`, 'export APP_SECRET=\n[REDACTED]\necho done', false],
    [`secret: |\n  {\n    "payload": {\n      "value": "${secret}"\n    }\n  }\nmode: fast`, 'secret: [REDACTED]\nmode: fast', false],
  ] as const) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).toBe(expectedPrefix)
    expect(out).not.toContain(secret)
    expect(stats.truncated).toBe(expectTruncated)
    expect(stats.redactions).toBe(1)
    expect(stats.droppedChars).toBe(0)

    const built = buildRow({
      eventType: 'TOOL_STARTING',
      timestampMs: 0,
      eventId: 'e1',
      agent: 'a',
      sessionId: 's',
      invocationId: 'i',
      userId: 'u',
      traceId: 't',
      spanId: 'p',
      parentSpanId: null,
      content: { tool: 'Bash', args: { command: sample } },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect(built.row.content ?? '').not.toContain(secret)
    expect(built.row.is_truncated).toBe(expectTruncated)
    expect(built.redactions).toBe(1)
  }

  // Quoted assignment followed by a separate benign-key prose JSON block leaves both cleanly handled.
  const secret2 = ['prose', 'json', 'secret', '003'].join('-')
  const mixed = `${singleQuoted}\nResult:\n{\n  "ok": true,\n  "token": "${secret2}"\n}`
  const mixedStats = newStats()
  const mixedOut = redactText(mixed, mixedStats)
  expect(mixedOut).not.toContain(secret)
  expect(mixedOut).not.toContain(secret2)
  expect(mixedOut).toContain('export APP_SECRET=[REDACTED]')
  expect(mixedOut).toContain('"ok": true')
  expect(mixedOut).toContain('"token": "[REDACTED]"')
  expect(mixedStats.truncated).toBe(false)
  expect(mixedStats.redactions).toBe(2)

  // Literal marker-like text in benign sibling JSON must never collide with internal placeholders.
  const collisionInput = `Result:\n{\n  "marker": "pjspan0q1z",\n  "api_key": "${secret}"\n}`
  const collisionStats = newStats()
  const collisionOut = redactText(collisionInput, collisionStats)
  expect(collisionOut).not.toContain(secret)
  expect(collisionOut).toContain('"marker": "pjspan0q1z"')
  expect(collisionOut).toContain('"api_key": "[REDACTED]"')
})

test('SQL IDENTIFIED BY / WITH PASSWORD backslash runs stay linear O(N) without ReDoS', () => {
  const shapes = [
    'IDENTIFIED BY "' + '\\'.repeat(48) + '\n',
    'IDENTIFIED BY "' + '\\'.repeat(49) + '\n',
    "WITH PASSWORD '" + '\\'.repeat(48) + '\n',
    'IDENTIFIED BY "' + '\\'.repeat(80_000) + '\n',
    "WITH ENCRYPTED PASSWORD '" + '\\'.repeat(80_000) + '\n',
  ]
  for (const inp of shapes) {
    const stats = newStats()
    const t0 = performance.now()
    const out = redactText(inp, stats)
    const elapsed = performance.now() - t0
    expect(elapsed).toBeLessThan(100)
    expect(out).toContain('[REDACTED]')
    expect(stats.redactions).toBeGreaterThanOrEqual(1)
  }

  const normalStats = newStats()
  const normalSql = `CREATE USER 'app'@'%' IDENTIFIED BY 'p''w'; ALTER USER app WITH PASSWORD "ab\\\\cd"; CREATE USER e IDENTIFIED BY "";`
  const normalOut = redactText(normalSql, normalStats)
  expect(normalOut).toBe(`CREATE USER 'app'@'%' IDENTIFIED BY '[REDACTED]'; ALTER USER app WITH PASSWORD "[REDACTED]"; CREATE USER e IDENTIFIED BY "";`)
  expect(normalStats.redactions).toBe(2)
})

test('attached -P and -a CLI flags redact secrets across sqlcmd/zip/unzip/7z and redis-cli/valkey-cli while keeping safe lookalikes', () => {
  const s = ['attached', 'flag', 'secret', '004'].join('-')
  const mustRedact: [string, string][] = [
    [`sqlcmd -S host -U sa -P${s}`, 'sqlcmd -S host -U sa -P[REDACTED]'],
    [`zip -P${s} out.zip file.txt`, 'zip -P[REDACTED] out.zip file.txt'],
    [`unzip -P${s} archive.zip`, 'unzip -P[REDACTED] archive.zip'],
    [`7z a -P${s} archive.7z file.txt`, '7z a -P[REDACTED] archive.7z file.txt'],
    [`7za a -P${s} archive.7z file.txt`, '7za a -P[REDACTED] archive.7z file.txt'],
    [`7zr a "-P${s}" archive.7z file.txt`, '7zr a "-P[REDACTED]" archive.7z file.txt'],
    [`redis-cli -h host -a${s} ping`, 'redis-cli -h host -a[REDACTED] ping'],
    [`valkey-cli -h host "-a${s}" ping`, 'valkey-cli -h host "-a[REDACTED]" ping'],
    [`sh -c "sqlcmd -S host -U sa -P${s} && redis-cli -a${s} ping"`, 'sh -c "sqlcmd -S host -U sa -P[REDACTED] && redis-cli -a[REDACTED] ping"'],
  ]
  for (const [input, expected] of mustRedact) {
    const stats1 = newStats()
    const out1 = redactText(input, stats1)
    expect(out1).toBe(expected)
    expect(out1).not.toContain(s)
    expect(stats1.redactions).toBeGreaterThanOrEqual(1)

    const stats2 = newStats()
    expect(redactText(out1, stats2)).toBe(out1)
    expect(stats2.redactions).toBe(0)
  }

  const keepUntouched = [
    '7z x -aoa archive.7z',
    '7za x -aoa archive.7z',
    'sqlcmd -S host -U sa -p1 -Q "SELECT 1"',
    'zip -P$ZIP_PASS out.zip file.txt',
    'sqlcmd -S host -P${SQL_PASS}',
    'redis-cli -p6379 -a$REDIS_AUTH ping',
  ]
  for (const input of keepUntouched) {
    const stats = newStats()
    expect(redactText(input, stats)).toBe(input)
    expect(stats.redactions).toBe(0)
  }
})

test('prose JSON scanner resets on non-NONE shell quotes and catches unclosed outer braces/brackets after sensitive assignments', () => {
  const secret = ['nested', 'brace', 'secret', '005'].join('-')

  // Non-NONE quote state inside malformed candidate resets stack so subsequent valid JSON still redacts
  // (including multi-line array under sensitive key `"api_keys"`, which only JSON span parsing catches).
  const quoteInterruptedCases = [
    `broken { "k": 1, note: 'a " {' } Result:\n{\n  "nested": {\n    "api_keys": [\n      "${secret}"\n    ]\n  }\n}`,
    `broken {\n  "k": 1,\n  note: 'a "b" {'\n}\nResult:\n{\n  "nested": {\n    "api_keys": [\n      "${secret}"\n    ]\n  }\n}`,
    `broken {\n  "k": 1, note: 'unclosed " {\n'\n}\nResult:\n{\n  "nested": {\n    "api_keys": [\n      "${secret}"\n    ]\n  }\n}`,
  ]
  for (const sample of quoteInterruptedCases) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(secret)
    expect(out).toContain('[REDACTED]')
    expect(stats.redactions).toBeGreaterThanOrEqual(1)
  }

  // Unclosed non-JSON outer `{` or `[` after sensitive assignment/flag marks nested JSON span sensitive.
  const nestedSensitiveSamples = [
    `export APP_SECRET={\n  payload: {\n    "nested": {\n      "value": "${secret}"\n    }\n  }\n}`,
    `export APP_SECRET=[\n  {\n    "nested": {\n      "value": "${secret}"\n    }\n  }\n]`,
    `aws secretsmanager put-secret-value --secret-id app/cfg --secret-string {\n  payload: {\n    "nested": {\n      "value": "${secret}"\n    }\n  }\n}`,
    `export APP_SECRET={\n  note: 'keeps } and { quoted',\n  payload: {\n    "nested": {\n      "value": "${secret}"\n    }\n  }\n}`,
  ]
  for (const sample of nestedSensitiveSamples) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(secret)
    expect(out).toContain('[REDACTED]')
    expect(stats.redactions).toBeGreaterThanOrEqual(1)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

    const built = buildRow({
      eventType: 'TOOL_STARTING',
      timestampMs: 0,
      eventId: 'e1',
      agent: 'a',
      sessionId: 's',
      invocationId: 'i',
      userId: 'u',
      traceId: 't',
      spanId: 'p',
      parentSpanId: null,
      content: { tool: 'Bash', args: { command: sample } },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect(built.row.content ?? '').not.toContain(secret)
    expect(built.redactions).toBeGreaterThanOrEqual(1)
  }

  // Benign unclosed outer `{` must not trigger false-positive redactions.
  const benignWrapper = `const config = {\n  payload: {\n    "nested": {\n      "region": "us-central1"\n    }\n  }\n}`
  const benignStats = newStats()
  const benignOut = redactText(benignWrapper, benignStats)
  expect(benignOut).toContain('"region": "us-central1"')
  expect(benignOut).not.toContain('[REDACTED]')
  expect(benignStats.redactions).toBe(0)
})
