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

test('prose JSON sanitizes credential-bearing object keys inside sensitive subtrees', () => {
  const ghpToken = ['ghp', 'Synthetic0123456789AbCdEfGhIjKlMnOpQr'].join('_')
  const urlPass = ['synthetic', 'url', 'pass', '123'].join('-')
  const urlKeyWithPass = `https://svc-user:${urlPass}@example.com/a`
  const samples = [
    `Result: ${JSON.stringify({ credentials: { [ghpToken]: 'enabled' } })}`,
    `Result: ${JSON.stringify({ secret: { [urlKeyWithPass]: [{ [ghpToken]: 'v' }] } })}`,
    `Result:\n${JSON.stringify({ credentials: { '[REDACTED_KEY:1]': 'present', [ghpToken]: 'enabled', [urlKeyWithPass]: 'enabled' } }, null, 2)}`,
  ]
  for (const sample of samples) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(ghpToken)
    expect(out).not.toContain(urlPass)
    expect(out).toContain('[REDACTED_KEY:')
    expect(stats.truncated).toBe(false)
    expect(stats.redactions).toBeGreaterThanOrEqual(2)

    // Placeholder keys are never trusted on re-scan (matching `walk` in sanitize()),
    // and secrets remain absent with exact key preservation.
    const reStats = newStats()
    const out2 = redactText(out, reStats)
    expect(out2).not.toContain(ghpToken)
    expect(out2).not.toContain(urlPass)
    expect(out2).toContain('[REDACTED_KEY:')

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(ghpToken)
    expect(serialized).not.toContain(urlPass)
    expect(serialized).toContain('[REDACTED_KEY:')
    expect(built.row.is_truncated).toBe(false)
    expect(built.redactions).toBeGreaterThanOrEqual(2)
  }

  // Collision check (matching `walk` in units.test.ts): an untrusted `[REDACTED_KEY:1]` key is renumbered
  // without colliding with existing keys (`[REDACTED_KEY:2]`, `[REDACTED_KEY:3]`, `[REDACTED_KEY:4]`).
  const collisionOut = redactText(samples[2]!, newStats())
  const parsed = JSON.parse(collisionOut.slice('Result:\n'.length)) as { credentials: Record<string, string> }
  expect(parsed.credentials).toEqual({
    '[REDACTED_KEY:2]': '[REDACTED]',
    '[REDACTED_KEY:3]': '[REDACTED]',
    '[REDACTED_KEY:4]': '[REDACTED]',
  })
})

test('prose JSON redacts unshaped keys in sensitive subtrees and multiline JSON-encoded keys', () => {
  const canary = ['canary', 'secret', 'z'.repeat(14)].join('_')
  const multiKey = '{\n  "credentials":\n  {\n    "val": "' + canary + '"\n  }\n}'
  const deepInner = JSON.stringify({ credentials: { secret_entry: canary } }, null, 2)
  const depth3Key = JSON.stringify({ [JSON.stringify({ [JSON.stringify({ [deepInner]: 'k4' })]: 'k3' })]: 'k2' }, null, 2)

  const subtreeInput = `Result: ${JSON.stringify({ credentials: { [canary]: 'enabled', text: 'secret2' } })}`
  const subtreeStats = newStats()
  const subtreeOut = redactText(subtreeInput, subtreeStats)
  expect(subtreeOut).not.toContain(canary)
  expect(subtreeOut).not.toContain('secret2')
  expect(subtreeStats.redactions).toBeGreaterThanOrEqual(3)
  expect(JSON.parse(subtreeOut.slice('Result: '.length))).toEqual({
    credentials: {
      '[REDACTED_KEY:1]': '[REDACTED]',
      text: '[REDACTED]',
    },
  })

  const keyInputs = [
    `Result:\n${JSON.stringify({ text: { [multiKey]: 'ok' } }, null, 2)}`,
    `Result:\n${JSON.stringify({ text: { [depth3Key]: 'ok' } }, null, 2)}`,
  ]
  for (const sample of [subtreeInput, ...keyInputs]) {
    const out = redactText(sample, newStats())
    expect(out).not.toContain(canary)
    expect(out).toContain('[REDACTED_KEY:1]')
    for (const field of ['result', 'text'] as const) {
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
        content: { [field]: sample },
        attributes: {},
        maxPayloadChars: 16_384,
      })
      const serialized = JSON.stringify(built.row)
      expect(serialized).not.toContain(canary)
      expect(serialized).toContain('[REDACTED_KEY:1]')
    }
  }

  const directJsonRow = buildRow({
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
    content: { result: JSON.stringify({ text: { [multiKey]: 'ok' } }) },
    attributes: {},
    maxPayloadChars: 16_384,
  })
  const directSerialized = JSON.stringify(directJsonRow.row)
  expect(directSerialized).not.toContain(canary)
  expect(directSerialized).toContain('[REDACTED_KEY:1]')

  // Nested multiline JSON string values, truncated JSON-looking string values,
  // and depth-3+ nested JSON string values inside prose JSON fail closed.
  const nestedValStats = newStats()
  const nestedValInput = `Result:\n${JSON.stringify({ text: multiKey }, null, 2)}`
  const nestedValOut = redactText(nestedValInput, nestedValStats)
  expect(nestedValOut).not.toContain(canary)
  expect(nestedValOut).toContain('[REDACTED]')
  expect(nestedValStats.truncated).toBe(false)

  const truncatedValRaw = '{\n  "credentials":\n  {\n    "val": "' + canary + '"'
  const truncatedValInput = `Result:\n${JSON.stringify({ text: truncatedValRaw }, null, 2)}`
  const truncatedValStats = newStats()
  const truncatedValOut = redactText(truncatedValInput, truncatedValStats)
  expect(truncatedValOut).not.toContain(canary)
  expect(truncatedValOut).toContain(`[TRUNCATED: ${truncatedValRaw.length} chars of JSON text that could not be parsed and checked]`)
  expect(truncatedValStats.truncated).toBe(true)

  const lvl3 = JSON.stringify({ text: multiKey }, null, 2)
  const lvl2 = JSON.stringify({ text: lvl3 }, null, 2)
  const lvl1 = JSON.stringify({ text: lvl2 }, null, 2)
  const deepValInput = `Result:\n${JSON.stringify({ text: lvl1 }, null, 2)}`
  const deepValStats = newStats()
  const deepValOut = redactText(deepValInput, deepValStats)
  expect(deepValOut).not.toContain(canary)
  expect(deepValOut).toContain('[TRUNCATED: nested too deep]')
  expect(deepValStats.truncated).toBe(true)
  expect(deepValStats.droppedChars).toBeGreaterThan(0)

  // Benign flat JSON objects and arrays inside prose JSON strings remain un-truncated.
  for (const benignVal of ['{}', '[]', '[1, 2, 3]', '{"status":"ok","count":3}']) {
    const sample = `Result:\n${JSON.stringify({ text: benignVal }, null, 2)}`
    const st = newStats()
    const out = redactText(sample, st)
    expect(st.truncated).toBe(false)
    expect(st.redactions).toBe(0)
    expect(JSON.parse(out.slice('Result:\n'.length))).toEqual({ text: benignVal })
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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect(built.row.is_truncated).toBe(false)
    expect(built.redactions).toBe(0)
  }

  // Unclosed array/object mixing valid inner span + trailing truncated secret drops whole string;
  // unclosed JSON_LOOKING object key fails closed to [REDACTED_KEY:1].
  const badVal = `[{"ok":[1]}, {"credentials":\n{\n"val":"${canary}"`
  const badKey = `{\n  "credentials":\n  {\n    "val": "${canary}"`
  const badValSample = `Result:\n${JSON.stringify({ text: badVal }, null, 2)}`
  const badKeySample = `Result:\n${JSON.stringify({ text: { [badKey]: 'ok' } }, null, 2)}`

  const badValOut = redactText(badValSample, newStats())
  expect(badValOut).not.toContain(canary)
  expect(badValOut).toContain('[TRUNCATED:')

  const badKeyOut = redactText(badKeySample, newStats())
  expect(badKeyOut).not.toContain(canary)
  expect(badKeyOut).toContain('[REDACTED_KEY:1]')

  for (const [sample, expectedMarker] of [
    [badValSample, '[TRUNCATED:'],
    [badKeySample, '[REDACTED_KEY:1]'],
  ] as const) {
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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(canary)
    expect(serialized).toContain(expectedMarker)
  }
})

test('extreme nesting (10,000+ deep arrays/objects) in prose JSON fails closed without throwing RangeError', () => {
  const secret = ['ultra', 'deep', 'secret', '006'].join('-')
  const cases = [
    `Result:\n{"root":${'['.repeat(10_000)}{"password":"${secret}"}${']'.repeat(10_000)}}`,
    `Result:\n${'{"a":'.repeat(2_500)}{"password":"${secret}"}${'}'.repeat(2_500)}`,
    `Result:\n{"root":${'[{"a":['.repeat(1_700)}{"password":"${secret}"}${']}]'.repeat(1_700)}}`,
    `Result:\n{"credentials":${'['.repeat(10_000)}{"password":"${secret}"}${']'.repeat(10_000)}}`,
    `Result:\n{"root":${'['.repeat(19)}[{"password":"${secret}"},{"token":"${secret}"}]${']'.repeat(19)}}`,
  ]
  for (const sample of cases) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(secret)
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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 100_000,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(secret)
    expect(built.row.is_truncated).toBe(true)
    expect(built.redactions).toBeGreaterThanOrEqual(1)
    const redMeta = (JSON.parse(built.row.attributes ?? '{}') as { redaction?: { values: number; dropped_chars: number } }).redaction
    expect((redMeta?.dropped_chars ?? 0) > 0).toBe(true)
  }
})

test('prose JSON placeholder suffix stays linear O(N) on adversarial q<N>z token floods', () => {
  const secret = ['linear', 'suffix', 'secret', '007'].join('-')
  redactText('Result:\n{"ok":true}\npjspan q1z', newStats())

  const makeSample = (count: number): string => {
    const tokens = Array.from({ length: count }, (_, i) => `q${i + 1}z`).join(' ')
    return `Result:\n{\n  "ok": true,\n  "api_keys": [\n    "${secret}"\n  ]\n}\npjspan ${tokens}`
  }

  const sample10 = makeSample(10_000)
  const tWarm0 = performance.now()
  redactText(sample10, newStats())
  const redact10Ms = Math.max(0.1, performance.now() - tWarm0)
  const tWarm1 = performance.now()
  buildRow({
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
    content: { result: sample10 },
    attributes: {},
    maxPayloadChars: 500_000,
  })
  const build10Ms = Math.max(0.1, performance.now() - tWarm1)

  const sample = makeSample(50_000)
  const t0 = performance.now()
  const stats = newStats()
  const out = redactText(sample, stats)
  const redactMs = performance.now() - t0

  expect(redactMs).toBeLessThan(500)
  expect(redactMs <= Math.max(80, redact10Ms * 15)).toBe(true)
  expect(out).not.toContain(secret)
  expect(out).toContain('"api_keys": [\n    "[REDACTED]"\n  ]')
  expect(out).toContain('pjspan q1z')
  expect(out).toContain('q50000z')
  expect(stats.truncated).toBe(false)
  expect(stats.redactions).toBeGreaterThanOrEqual(1)

  const t1 = performance.now()
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
    content: { result: sample },
    attributes: {},
    maxPayloadChars: 500_000,
  })
  const buildMs = performance.now() - t1

  expect(buildMs).toBeLessThan(500)
  expect(buildMs <= Math.max(80, build10Ms * 15)).toBe(true)
  expect(built.row.content ?? '').not.toContain(secret)
  expect(built.row.is_truncated).toBe(false)
  expect(built.redactions).toBeGreaterThanOrEqual(1)
})

test('prose JSON placeholder suffix avoids collisions across 12-, 13-, 15-, and 16-digit q<N>z tokens', () => {
  const secret = ['suffix', 'width', 'secret', '008'].join('-')
  const samples = [
    `pjspan0q1000000000000z and pjspan0q999999999999z and {"secret":"${secret}","nested":{"ok":1}}`,
    `pjspan0q1000000000000000z and pjspan0q999999999999999z and {"secret":"${secret}","nested":{"ok":1}}`,
  ]
  for (const [idx, sample] of samples.entries()) {
    const out = redactText(sample, newStats())
    expect(out).not.toContain(secret)
    if (idx === 0) {
      expect(out).toContain('pjspan0q1000000000000z and pjspan0q999999999999z and ')
    } else {
      expect(out).toContain('pjspan0q1000000000000000z and pjspan0q999999999999999z and ')
    }
    expect(out).toContain('{"secret":"[REDACTED]","nested":{"ok":1}}')
  }
})

test('prose JSON placeholder suffix stays linear at the 15-to-16-digit boundary with 26,000 consecutive collisions', () => {
  const secret = ['boundary', 'linear', 'secret', '010'].join('-')
  const makeSample = (count: number): string => {
    const tokens = Array.from({ length: count }, (_, i) => `q${1_000_000_000_000_000 + i}z`).join(' ')
    return `pjspan q999999999999999z ${tokens} {"secret":"${secret}","nested":{"ok":1}}`
  }
  const makeRowInput = (payload: string) => ({
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
    content: { result: payload },
    attributes: {},
    maxPayloadChars: 500_000,
  })

  const small = makeSample(2_000)
  redactText(small, newStats())
  buildRow(makeRowInput(small))

  const tWarm0 = performance.now()
  redactText(small, newStats())
  const redactSmallMs = Math.max(1, performance.now() - tWarm0)
  const tWarm1 = performance.now()
  buildRow(makeRowInput(small))
  const buildSmallMs = Math.max(1, performance.now() - tWarm1)

  const sample = makeSample(26_000)
  const t0 = performance.now()
  const stats = newStats()
  const out = redactText(sample, stats)
  const redactMs = performance.now() - t0

  expect(redactMs).toBeLessThan(500)
  expect(redactMs <= Math.max(120, redactSmallMs * 25)).toBe(true)
  expect(out).not.toContain(secret)
  expect(out).toContain('pjspan q999999999999999z')
  expect(out).toContain('q1000000000000000z')
  expect(out).toContain('q1000000000025999z')
  expect(out).toContain('{"secret":"[REDACTED]","nested":{"ok":1}}')
  expect(stats.truncated).toBe(false)
  expect(stats.redactions).toBeGreaterThanOrEqual(1)

  const t1 = performance.now()
  const built = buildRow(makeRowInput(sample))
  const buildMs = performance.now() - t1

  expect(buildMs).toBeLessThan(500)
  expect(buildMs <= Math.max(120, buildSmallMs * 25)).toBe(true)
  const serialized = JSON.stringify(built.row)
  expect(serialized).not.toContain(secret)
  expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toContain('{"secret":"[REDACTED]","nested":{"ok":1}}')
  expect(built.row.is_truncated).toBe(false)
  expect(built.redactions).toBeGreaterThanOrEqual(1)
})

test('repeated short YAML and Kubernetes block scalars scale linearly without quadratic row shifts', () => {
  const makeRowInput = (payload: string) => ({
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
    content: { result: payload },
    attributes: {},
    maxPayloadChars: 500_000,
  })

  const shapes = [
    {
      name: 'yaml',
      secret: 'yamlBlkSec9',
      unit: 'password: |\n  yamlBlkSec9\n',
      redactedUnit: 'password: [REDACTED]\n',
      expectTruncated: false,
      minBuiltRedactions: 16_000,
    },
    {
      name: 'k8s',
      secret: 'k8sBlkSec9',
      unit: '- name: DB_KEY\n  value: |\n    k8sBlkSec9\n',
      redactedUnit: '- name: DB_KEY\n  value: [REDACTED]\n',
      expectTruncated: true,
      minBuiltRedactions: 12_000,
    },
  ] as const

  for (const shape of shapes) {
    const small = shape.unit.repeat(2_000)
    redactText(small, newStats())
    buildRow(makeRowInput(small))

    const tWarm0 = performance.now()
    redactText(small, newStats())
    const redactSmallMs = Math.max(1, performance.now() - tWarm0)
    const tWarm1 = performance.now()
    buildRow(makeRowInput(small))
    const buildSmallMs = Math.max(1, performance.now() - tWarm1)

    const sample = shape.unit.repeat(16_000)
    const t0 = performance.now()
    const stats = newStats()
    const out = redactText(sample, stats)
    const redactMs = performance.now() - t0

    expect(redactMs).toBeLessThan(500)
    expect(redactMs <= Math.max(120, redactSmallMs * 20)).toBe(true)
    expect(out).not.toContain(shape.secret)
    expect(out === shape.redactedUnit.repeat(16_000)).toBe(true)
    expect(stats.redactions).toBe(16_000)

    const t1 = performance.now()
    const built = buildRow(makeRowInput(sample))
    const buildMs = performance.now() - t1

    expect(buildMs).toBeLessThan(500)
    expect(buildMs <= Math.max(120, buildSmallMs * 20)).toBe(true)
    expect(JSON.stringify(built.row)).not.toContain(shape.secret)
    expect(built.row.is_truncated).toBe(shape.expectTruncated)
    expect(built.redactions).toBeGreaterThanOrEqual(shape.minBuiltRedactions)
  }
})

test('cross-line and NBSP Bearer and Basic credentials redact while multi-line prose stays untouched', () => {
  const token = 'reviewFixtureOnly9472'
  const redactCases: readonly (readonly [string, string, string])[] = [
    [`Authorization: Bearer\n${token}`, 'Authorization: [REDACTED]', token],
    [`Authorization: Bearer\n\n${token}`, 'Authorization: [REDACTED]', token],
    [`Authorization: Bearer\r\n ${token}`, 'Authorization: [REDACTED]', token],
    [`Authorization: Bearer\r\n \r\n ${token}`, 'Authorization: [REDACTED]', token],
    [`Authorization: Bearer\n${token}: request rejected`, 'Authorization: [REDACTED]', token],
    [`Bearer\u00a0${token}`, 'Bearer [REDACTED]', token],
    ['Bearer\u00a0ReviewFixtureOnly', 'Bearer [REDACTED]', 'ReviewFixtureOnly'],
    [`Bearer\n${token}`, 'Bearer [REDACTED]', token],
    ['Bearer\nabcdefghijklmnop', 'Bearer [REDACTED]', 'abcdefghijklmnop'],
    ['Bearer\r\n abcdefghijklmnop', 'Bearer [REDACTED]', 'abcdefghijklmnop'],
    ['Bearer\nreviewfixtureonly', 'Bearer [REDACTED]', 'reviewfixtureonly'],
    ['Bearer\nreviewFixtureOnly', 'Bearer [REDACTED]', 'reviewFixtureOnly'],
    ['Bearer\r\n reviewFixtureOnly', 'Bearer [REDACTED]', 'reviewFixtureOnly'],
    ['Bearer\nReviewFixtureOnly', 'Bearer [REDACTED]', 'ReviewFixtureOnly'],
    ['Bearer\r\n ReviewFixtureOnly', 'Bearer [REDACTED]', 'ReviewFixtureOnly'],
    [`Scheme: Bearer\nBearer ${token}`, 'Scheme: Bearer\nBearer [REDACTED]', token],
    ['Authorization:\nBearer\nReviewFixtureOnly more', 'Authorization:\nBearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Proxy-Authorization:\nBearer\nReviewFixtureOnly more', 'Proxy-Authorization:\nBearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Authorization: Bearer\u00a0ConfigurationWord', 'Authorization: [REDACTED]', 'ConfigurationWord'],
    [`Basic\nBearer ${token}`, 'Basic\nBearer [REDACTED]', token],
    ['Authorization: Basic\ndXNlcjpwYXNz', 'Authorization: [REDACTED]', 'dXNlcjpwYXNz'],
    ['Authorization: Basic\ncmV2aWV3ZXI6Zml4dHVyZQ==: request rejected', 'Authorization: [REDACTED]', 'cmV2aWV3ZXI6Zml4dHVyZQ=='],
    ['Basic\ndXNlcjpwYXNz', 'Basic [REDACTED]', 'dXNlcjpwYXNz'],
    ['Basic\ndXNlcjpwYXNz: request rejected', 'Basic [REDACTED]: request rejected', 'dXNlcjpwYXNz'],
    ['Basic\nYWI6Y2Q=.', 'Basic [REDACTED]', 'YWI6Y2Q='],
    ['Basic\nYWI6Y2Q=: request rejected', 'Basic [REDACTED]: request rejected', 'YWI6Y2Q='],
    ['"Basic\nYWI6Y2Q="', '"Basic [REDACTED]"', 'YWI6Y2Q='],
    ['Basic\ncmV2aWV3ZXI6Zml4dHVyZQ==.', 'Basic [REDACTED]', 'cmV2aWV3ZXI6Zml4dHVyZQ=='],
    ['Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=.', 'Basic [REDACTED]', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['"Basic\ncmV2aWV3ZXI6Zml4dHVyZTE="', '"Basic [REDACTED]"', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['`Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=`', '`Basic [REDACTED]`', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ["'Basic\ncmV2aWV3ZXI6Zml4dHVyZTE='", "'Basic [REDACTED]'", 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['"Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=."', '"Basic [REDACTED]"', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['Authorization: "Basic\ncmV2aWV3ZXI6Zml4dHVyZTE="', 'Authorization: [REDACTED]', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['Authorization: "Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=."', 'Authorization: [REDACTED]', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['Basic\ncmV2aWV3ZXI6Zml4dHVyZQ==: request rejected', 'Basic [REDACTED]: request rejected', 'cmV2aWV3ZXI6Zml4dHVyZQ=='],
    ['Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=: request rejected', 'Basic [REDACTED]: request rejected', 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=.', 'Bearer [REDACTED]', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=-', 'Bearer [REDACTED]', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=$', 'Bearer [REDACTED]$', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['"Bearer\ncmV2aWV3Rml4dHVyZU9ubHk="', '"Bearer [REDACTED]"', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['`Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=`', '`Bearer [REDACTED]`', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk='", "'Bearer [REDACTED]'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['\'"Bearer\ncmV2aWV3Rml4dHVyZU9ubHk="\'', '\'"Bearer [REDACTED]"\'', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['"Use \'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=\' and \'other\'"', '"Use \'Bearer [REDACTED]\' and \'other\'"', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['echo "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=" "next\nline"', 'echo "Bearer [REDACTED]" "next\nline"', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk="', 'Bearer [REDACTED]"', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Authorization: "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk="', 'Authorization: [REDACTED]', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Authorization: "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=."', 'Authorization: [REDACTED]', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=: request rejected', 'Bearer [REDACTED]: request rejected', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZQ==: request rejected', 'Bearer [REDACTED]: request rejected', 'cmV2aWV3Rml4dHVyZQ=='],
    ['log: {"note": "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=", "user": "alice"}', 'log: {"note": "Bearer [REDACTED]", "user": "alice"}', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["Users' cmd: echo 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "Users' cmd: echo 'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["Users' cmd: echo 'Basic\ncmV2aWV3ZXI6Zml4dHVyZTE=' 'next\nline'", "Users' cmd: echo 'Basic [REDACTED]' 'next\nline'", 'cmV2aWV3ZXI6Zml4dHVyZTE='],
    ["fn f<'a>() { let _ = 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline' }", "fn f<'a>() { let _ = 'Bearer [REDACTED]' 'next\nline' }", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["$'it\\'s'; echo 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "$'it\\'s'; echo 'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['# "todo\necho "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=" "next\nline"', '# "todo\necho "Bearer [REDACTED]" "next\nline"', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["// 'todo\necho 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "// 'todo\necho 'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ["r'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "r'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['echo "\\"Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=\\""', 'echo "\\"Bearer [REDACTED]\\""', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['echo "\\"Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=\\" \\"next\nline\\""', 'echo "\\"Bearer [REDACTED]\\" \\"next\nline\\""', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=\\n', 'Bearer [REDACTED]\\n', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['# "todo Bearer\ncustomValue="x"\nlog="Bearer\nU2VjcmV0VG9rZW4="foo"', '# "todo Bearer\ncustomValue="x"\nlog="Bearer [REDACTED]"foo"', 'U2VjcmV0VG9rZW4='],
    ['// "todo Bearer\ncustomValue="x"\nlog="Bearer\nU2VjcmV0VG9rZW4="foo"', '// "todo Bearer\ncustomValue="x"\nlog="Bearer [REDACTED]"foo"', 'U2VjcmV0VG9rZW4='],
    ["# 'todo: check bearer\npublicvalue='hello'\necho 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "# 'todo: check bearer\npublicvalue='hello'\necho 'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['# `todo Bearer\ncustomValue=`x`\nlog=`Bearer\nU2VjcmV0VG9rZW4=`foo`', '# `todo Bearer\ncustomValue=`x`\nlog=`Bearer [REDACTED]`foo`', 'U2VjcmV0VG9rZW4='],
    ['# "todo Bearer\r\ncustomValue="x"\r\nlog="Bearer\r\nU2VjcmV0VG9rZW4="foo"', '# "todo Bearer\r\ncustomValue="x"\r\nlog="Bearer [REDACTED]"foo"', 'U2VjcmV0VG9rZW4='],
    ['# "todo Bearer\rcustomValue="x"\rlog="Bearer\rU2VjcmV0VG9rZW4="foo"', '# "todo Bearer\rcustomValue="x"\rlog="Bearer [REDACTED]"foo"', 'U2VjcmV0VG9rZW4='],
    ["# echo 'Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=' 'next\nline'", "# echo 'Bearer [REDACTED]' 'next\nline'", 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['# echo `Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=` `next\nline`', '# echo `Bearer [REDACTED]` `next\nline`', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['# echo "\\"Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=\\" \\"next\nline\\""', '# echo "\\"Bearer [REDACTED]\\" \\"next\nline\\""', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Token Bearer\nmyTokenSecret123=', 'Token Bearer [REDACTED]', 'myTokenSecret123='],
    ['log="Bearer\nmyTokenSecret123="', 'log="Bearer [REDACTED]"', 'myTokenSecret123='],
    ['Bearer\nabcJwt9Yz123456=', 'Bearer [REDACTED]', 'abcJwt9Yz123456='],
    ['Bearer\nabcJwt9Yz123456=: request rejected', 'Bearer [REDACTED]: request rejected', 'abcJwt9Yz123456='],
    ['Bearer\nabcJwt9Yz12345==', 'Bearer [REDACTED]', 'abcJwt9Yz12345=='],
    ['log="bearer myTokenSecret12="', 'log="bearer [REDACTED]"', 'myTokenSecret12='],
    ['echo "\\"Bearer mySecretTokenAb=\\""', 'echo "\\"Bearer [REDACTED]\\""', 'mySecretTokenAb='],
    ['echo "\\"Bearer mySecretTokenAb=\\" \\"next\\""', 'echo "\\"Bearer [REDACTED]\\" \\"next\\""', 'mySecretTokenAb='],
    ['echo "\\"Bearer mySecretTokenAb=\\" foo \\"unclosed"', 'echo "\\"Bearer [REDACTED]\\" foo \\"unclosed"', 'mySecretTokenAb='],
    ['echo "\\"Bearer mySecretTokenAb=\\", \\"unclosed"', 'echo "\\"Bearer [REDACTED]\\", \\"unclosed"', 'mySecretTokenAb='],
    ['bash -c "curl -H \\"X-Trace: Bearer mySecretTokenAb=\\""', 'bash -c "curl -H \\"X-Trace: Bearer [REDACTED]\\""', 'mySecretTokenAb='],
    ["# echo 'Bearer mySecretTokenAb='", "# echo 'Bearer [REDACTED]'", 'mySecretTokenAb='],
    ["# echo 'Bearer mySecretTokenAb=' 'next'", "# echo 'Bearer [REDACTED]' 'next'", 'mySecretTokenAb='],
    ["# echo 'Bearer mySecretTokenAb=' foo 'unclosed", "# echo 'Bearer [REDACTED]' foo 'unclosed", 'mySecretTokenAb='],
    ["# echo 'Bearer mySecretTokenAb=', 'unclosed", "# echo 'Bearer [REDACTED]', 'unclosed", 'mySecretTokenAb='],
    ["// echo 'Bearer mySecretTokenAb=' foo 'unclosed", "// echo 'Bearer [REDACTED]' foo 'unclosed", 'mySecretTokenAb='],
    ['# echo "\\"Bearer mySecretTokenAb=\\""', '# echo "\\"Bearer [REDACTED]\\""', 'mySecretTokenAb='],
    ['echo \'"Bearer mySecretTokenAb=" foo "unclosed\'', 'echo \'"Bearer [REDACTED]" foo "unclosed\'', 'mySecretTokenAb='],
    ['echo \'"Bearer mySecretTokenAb=", "unclosed\'', 'echo \'"Bearer [REDACTED]", "unclosed\'', 'mySecretTokenAb='],
    ['echo \'Bearer mySecretTokenAb= "next"\'', 'echo \'Bearer [REDACTED] "next"\'', 'mySecretTokenAb='],
    ["echo 'Bearer\nmySecretTokenAb='", "echo 'Bearer [REDACTED]'", 'mySecretTokenAb='],
    ['log="Bearer\nmySecretTokenAb="', 'log="Bearer [REDACTED]"', 'mySecretTokenAb='],
    ['echo "\\"Bearer\nmySecretTokenAb=\\""', 'echo "\\"Bearer [REDACTED]\\""', 'mySecretTokenAb='],
    ['# Bearer\npublic="unclosed\nBearer mySecretTokenAb="', '# Bearer\npublic="unclosed\nBearer [REDACTED]"', 'mySecretTokenAb='],
    ['Token Bearer mySecretToken123: request rejected', 'Token Bearer [REDACTED]: [REDACTED]', 'mySecretToken123'],
    ['Bearer mySecretToken123: unauthorized', 'Bearer [REDACTED]: [REDACTED]', 'mySecretToken123'],
    ['Token Bearer abcJwt9Yz123456: request rejected', 'Token Bearer [REDACTED]: [REDACTED]', 'abcJwt9Yz123456'],
    ['Bearer mySecretToken123: 401 Unauthorized', 'Bearer [REDACTED]: [REDACTED]', 'mySecretToken123'],
    ['Bearer mySecretToken123: invalid_token', 'Bearer [REDACTED]: [REDACTED]', 'mySecretToken123'],
    ['Bearer mySecretToken12==', 'Bearer [REDACTED]', 'mySecretToken12=='],
    ['Bearer\nmySecretToken12==', 'Bearer [REDACTED]', 'mySecretToken12=='],
    ['Bearer mySecretToken12==: request rejected', 'Bearer [REDACTED]: request rejected', 'mySecretToken12=='],
    ['echo "Bearer mySecretToken12=="', 'echo "Bearer [REDACTED]"', 'mySecretToken12=='],
    ['Bearer\n-mySecretToken1=', 'Bearer [REDACTED]', '-mySecretToken1='],
    ['x=`echo \\`a\\` Bearer\nmySecretTokenAb=`', 'x=`echo \\`a\\` Bearer [REDACTED]`', 'mySecretTokenAb='],
    ['x=`echo \\`a\\` Bearer mySecretTokenAb=`', 'x=`echo \\`a\\` Bearer [REDACTED]`', 'mySecretTokenAb='],
    ['# x=`echo \\`a\\` Bearer mySecretTokenAb=`', '# x=`echo \\`a\\` Bearer [REDACTED]`', 'mySecretTokenAb='],
    ['# x=`echo \\`a\\` Bearer mySecretTokenAb=` `unclosed', '# x=`echo \\`a\\` Bearer [REDACTED]` `unclosed', 'mySecretTokenAb='],
    ['sh -c \'echo "\\"Bearer\nmySecretTokenAb=\\""\'', 'sh -c \'echo "\\"Bearer [REDACTED]\\""\'', 'mySecretTokenAb='],
    ['sh -c \'echo "\\"Bearer mySecretTokenAb=\\""\'', 'sh -c \'echo "\\"Bearer [REDACTED]\\""\'', 'mySecretTokenAb='],
    ['x="echo `echo \\`a\\` Bearer\nmySecretTokenAb=`"', 'x="echo `echo \\`a\\` Bearer [REDACTED]`"', 'mySecretTokenAb='],
    ['x="echo `echo \\`a\\` Bearer mySecretToken=`"', 'x="echo `echo \\`a\\` Bearer [REDACTED]`"', 'mySecretToken='],
    ['sh -c \'echo "\\"unclosed"; echo "Bearer\nmySecretTokenAb="\'', 'sh -c \'echo "\\"unclosed"; echo "Bearer [REDACTED]"\'', 'mySecretTokenAb='],
    ['sh -c \'echo "\\"unclosed"; echo "Bearer mySecretToken=" "unclosed\'', 'sh -c \'echo "\\"unclosed"; echo "Bearer [REDACTED]" "unclosed\'', 'mySecretToken='],
    ['sh -c \'echo "foo\\\' Bearer\nmySecretTokenAb=\'', 'sh -c \'echo "foo\\\' Bearer [REDACTED]\'', 'mySecretTokenAb='],
    ['sh -c \'echo "foo\\\' && echo "Bearer\nmySecretTokenAb="\'', 'sh -c \'echo "foo\\\' && echo "Bearer [REDACTED]"\'', 'mySecretTokenAb='],
    ['sh -c \'echo "\\"Bearer mySecretToken=\\"" \\"unclosed\'', 'sh -c \'echo "\\"Bearer [REDACTED]\\"" \\"unclosed\'', 'mySecretToken='],
    ['# echo "\\"Bearer mySecretToken=\\"" \\"unclosed', '# echo "\\"Bearer [REDACTED]\\"" \\"unclosed', 'mySecretToken='],
    ['# echo "\\"Bearer mySecretTokenAb=\\"" \\"unclosed', '# echo "\\"Bearer [REDACTED]\\"" \\"unclosed', 'mySecretTokenAb='],
    ['# echo "\\"Bearer mySecretTokenAb=\\""', '# echo "\\"Bearer [REDACTED]\\""', 'mySecretTokenAb='],
    ['# echo "\\"unclosed"; echo "Bearer mySecretToken=" "unclosed', '# echo "\\"unclosed"; echo "Bearer [REDACTED]" "unclosed', 'mySecretToken='],
    ['x="echo \\` Bearer\nmySecretTokenAb="', 'x="echo \\` Bearer [REDACTED]"', 'mySecretTokenAb='],
    ['Bearer MySecretToken123=.', 'Bearer [REDACTED]', 'MySecretToken123'],
    ['Bearer MySecretToken123=...', 'Bearer [REDACTED]', 'MySecretToken123'],
    ['Bearer MySecretToken123=.\n', 'Bearer [REDACTED]\n', 'MySecretToken123'],
    ['Bearer MySecretToken123=. Next sentence.', 'Bearer [REDACTED] Next sentence.', 'MySecretToken123'],
    ['Bearer\nMySecretToken123=.', 'Bearer [REDACTED]', 'MySecretToken123'],
    ['Basic MySecretToken123=.', 'Basic [REDACTED]', 'MySecretToken123'],
    ['Bearer tokenValue12345==.', 'Bearer [REDACTED]', 'tokenValue12345'],
    ['Bearer\ntokenValue12345==.', 'Bearer [REDACTED]', 'tokenValue12345'],
    ['Bearer\nmySecretTokenAb=.', 'Bearer [REDACTED]', 'mySecretTokenAb='],
    ['Bearer\nmySecretTokenAb=\'', 'Bearer [REDACTED]\'', 'mySecretTokenAb='],
    ['Bearer\nreviewFixtureOnly9472=', 'Bearer [REDACTED]', 'reviewFixtureOnly9472='],
    [`Bearer\n${['ya29', 'a0AfH6SMBxyz123='].join('.')}`, 'Bearer [REDACTED]', ['ya29', 'a0AfH6SMBxyz123='].join('.')],
    ['Bearer\ntok-1234567890ab=', 'Bearer [REDACTED]', 'tok-1234567890ab='],
    ['Bearer\nxoxbTOKENvalue123=', 'Bearer [REDACTED]', 'xoxbTOKENvalue123='],
    ['Bearer\nAbCdEfGhIjKlMnOp=', 'Bearer [REDACTED]', 'AbCdEfGhIjKlMnOp='],
    ['Bearer\nAbcdefghijkl=', 'Bearer [REDACTED]', 'Abcdefghijkl='],
    ['Bearer\ndXNlcjpwYXNz=', 'Bearer [REDACTED]', 'dXNlcjpwYXNz='],
    ['Bearer\nENVIRONMENT=', 'Bearer [REDACTED]', 'ENVIRONMENT='],
    ['Bearer\nschemaV2Config123=', 'Bearer [REDACTED]', 'schemaV2Config123='],
    ['Bearer\nlB79GsW66LvIcKaTzjh=', 'Bearer [REDACTED]', 'lB79GsW66LvIcKaTzjh='],
    ['Bearer\np2112MrvcRo=', 'Bearer [REDACTED]', 'p2112MrvcRo='],
    ['Bearer\np2112MrvcRo= # trailing comment', 'Bearer [REDACTED] # trailing comment', 'p2112MrvcRo='],
    ['Bearer\nmySecretTokenAb=', 'Bearer [REDACTED]', 'mySecretTokenAb='],
    ['Bearer\nqWertyUiopAsdfg=', 'Bearer [REDACTED]', 'qWertyUiopAsdfg='],
    ['Bearer\nschemaV2Configx=', 'Bearer [REDACTED]', 'schemaV2Configx='],
    ['Bearer\ncustom-headerxx=', 'Bearer [REDACTED]', 'custom-headerxx='],
    ['Bearer\nWORK_DIR_XY=', 'Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['Bearer\nWORK_DIR_XYZ_AB=', 'Bearer [REDACTED]', 'WORK_DIR_XYZ_AB='],
    ['Bearer\nwork_dir_xy=', 'Bearer [REDACTED]', 'work_dir_xy='],
    ['Bearer\nwork_dir_xyz_ab=', 'Bearer [REDACTED]', 'work_dir_xyz_ab='],
    ['Bearer\n--config-fx=', 'Bearer [REDACTED]', '--config-fx='],
    ['Bearer\n--config-fix-ab=', 'Bearer [REDACTED]', '--config-fix-ab='],
    ['Bearer\n-cfg_flg_ab=', 'Bearer [REDACTED]', '-cfg_flg_ab='],
    ['foo=\nBearer\nlB79GsW66LvIcKaTzjh=', 'foo=\nBearer [REDACTED]', 'lB79GsW66LvIcKaTzjh='],
    ['x == Bearer\nlB79GsW66LvIcKaTzjh=', 'x == Bearer [REDACTED]', 'lB79GsW66LvIcKaTzjh='],
    ['MODE=bearer\nschemaV2Config= Bearer\nlB79GsW66LvIcKaTzjh=', 'MODE=bearer\nschemaV2Config= Bearer [REDACTED]', 'lB79GsW66LvIcKaTzjh='],
    ['MODE=bearer\nschemaV2Config= Bearer\nWORK_DIR_XY=', 'MODE=bearer\nschemaV2Config= Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['MODE=bearer\nschemaV2Config= Bearer\ncacheV2Mode=', 'MODE=bearer\nschemaV2Config= Bearer [REDACTED]', 'cacheV2Mode='],
    ['Bearer abc= Bearer\nWORK_DIR_XY=', 'Bearer abc= Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['MODE=bearer\nlB79GsW66LvIcKaTzjh=', 'MODE=bearer [REDACTED]', 'lB79GsW66LvIcKaTzjh='],
    ['MODE=bearer\np2112MrvcRo=', 'MODE=bearer [REDACTED]', 'p2112MrvcRo='],
    ['MODE=bearer\nabV2GsLvIcKaTzjh=', 'MODE=bearer [REDACTED]', 'abV2GsLvIcKaTzjh='],
    ['x !=' + ' '.repeat(39) + 'Bearer\nWORK_DIR_XY=', 'x !=' + ' '.repeat(39) + 'Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['x ==' + ' '.repeat(39) + 'Bearer\nWORK_DIR_XY=', 'x ==' + ' '.repeat(39) + 'Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['x <=' + ' '.repeat(39) + 'Bearer\nWORK_DIR_XY=', 'x <=' + ' '.repeat(39) + 'Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['x >=' + ' '.repeat(39) + 'Bearer\nWORK_DIR_XY=', 'x >=' + ' '.repeat(39) + 'Bearer [REDACTED]', 'WORK_DIR_XY='],
    ['Bearer A' + '_'.repeat(100) + 'a=', 'Bearer [REDACTED]', '_'.repeat(100) + 'a='],
    ['bearer a' + '_'.repeat(100) + 'A=', 'bearer [REDACTED]', '_'.repeat(100) + 'A='],
    ['Bearer -a' + '_'.repeat(100) + 'A=', 'Bearer [REDACTED]', '_'.repeat(100) + 'A='],
    ['Bearer\n' + 'A_'.repeat(55) + 'A=', 'Bearer [REDACTED]', 'A_'.repeat(55) + 'A='],
    ['Bearer\nreviewFixtureOnly9472: request rejected', 'Bearer [REDACTED]: request rejected', 'reviewFixtureOnly9472'],
    ['Authorization:\n' + ' '.repeat(27) + 'Bearer\nreviewFixtureOnly9472: request rejected', 'Authorization:\n' + ' '.repeat(27) + 'Bearer [REDACTED]: request rejected', 'reviewFixtureOnly9472'],
    ['Authorization:\n' + ' '.repeat(27) + 'Bearer\nReviewFixtureOnly more', 'Authorization:\n' + ' '.repeat(27) + 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Authorization:\n' + ' '.repeat(120) + 'Bearer\nReviewFixtureOnly more', 'Authorization:\n' + ' '.repeat(120) + 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Authorization:\n   \r\n   ' + ' '.repeat(40) + 'Bearer\nConfiguration details follow', 'Authorization:\n   \r\n   ' + ' '.repeat(40) + 'Bearer [REDACTED] details follow', 'Configuration'],
    ['Proxy-Authorization:\n\t\t' + ' '.repeat(60) + 'Bearer\nReviewFixtureOnly more', 'Proxy-Authorization:\n\t\t' + ' '.repeat(60) + 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['"Authorization":\n' + ' '.repeat(60) + '"Bearer\nReviewFixtureOnly more"', '"Authorization":\n' + ' '.repeat(60) + '"Bearer [REDACTED] more"', 'ReviewFixtureOnly'],
    ['\\"Authorization\\":\n' + ' '.repeat(60) + '\\"Bearer\nReviewFixtureOnly more\\"', '\\"Authorization\\":\n' + ' '.repeat(60) + '\\"Bearer [REDACTED] more\\"', 'ReviewFixtureOnly'],
    ['Authorization:\n' + ' '.repeat(60) + 'Basic\nReviewFixtureOnly more', 'Authorization:\n' + ' '.repeat(60) + 'Basic [REDACTED] more', 'ReviewFixtureOnly'],
    ['Authorization:\n' + ' '.repeat(60) + 'Basic\nshortTok more', 'Authorization:\n' + ' '.repeat(60) + 'Basic [REDACTED] more', 'shortTok'],
    ['Bearer\nsecretTokenValue99: expired', 'Bearer [REDACTED]: [REDACTED]', 'secretTokenValue99'],
    ['Bearer\nabcdefghijklmnop: unauthorized', 'Bearer [REDACTED]: unauthorized', 'abcdefghijklmnop'],
    ['Bearer\nreview_fixture_9472: failed', 'Bearer [REDACTED]: failed', 'review_fixture_9472'],
    ['Bearer\nreviewFixtureOnly9472: 123', 'Bearer [REDACTED]: 123', 'reviewFixtureOnly9472'],
    ['Bearer\nreview_fixture_9472: 123', 'Bearer [REDACTED]: 123', 'review_fixture_9472'],
    ['Bearer\ntokenValue1234=: request rejected', 'Bearer [REDACTED]: request rejected', 'tokenValue1234='],
    ['Bearer\nmy_token_12345=: request rejected', 'Bearer [REDACTED]: request rejected', 'my_token_12345='],
    ['Bearer\nReviewFixtureOnly: request rejected', 'Bearer [REDACTED]: request rejected', 'ReviewFixtureOnly'],
    ['Basic\ncmV2aWV3ZXI6Zml4dHVyZQ==: failed', 'Basic [REDACTED]: failed', 'cmV2aWV3ZXI6Zml4dHVyZQ=='],
    ['Bearer secret_token_value=', 'Bearer [REDACTED]', 'secret_token_value='],
    ['Bearer SECRET_TOKEN_VALUE=', 'Bearer [REDACTED]', 'SECRET_TOKEN_VALUE='],
    ['Bearer secret_token_value= # comment', 'Bearer [REDACTED] # comment', 'secret_token_value='],
    ['Bearer my_api_key_1234=', 'Bearer [REDACTED]', 'my_api_key_1234='],
    ['Bearer secret_token_value: failed', 'Bearer [REDACTED]: [REDACTED]', 'secret_token_value'],
    ['Bearer secret_token_value: rejected', 'Bearer [REDACTED]: [REDACTED]', 'secret_token_value'],
    ['Bearer\nsecret_token_value: request rejected', 'Bearer [REDACTED]: [REDACTED]', 'secret_token_value'],
    ['proxyAuthorization: Bearer\nReviewFixtureOnly more', 'proxyAuthorization: [REDACTED]', 'ReviewFixtureOnly'],
    ['XAuthorization:\n' + ' '.repeat(50) + 'Bearer\nReviewFixtureOnly more', 'XAuthorization:\n' + ' '.repeat(50) + 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['httpAuthorization: Bearer\nReviewFixtureOnly more', 'httpAuthorization: [REDACTED]', 'ReviewFixtureOnly'],
    ['preauthorization: Bearer\nReviewFixtureOnly more', 'preauthorization: [REDACTED]', 'ReviewFixtureOnly'],
    ['proxyAuthorization: Basic\nreviewFixtureOnly9472', 'proxyAuthorization: [REDACTED]', 'reviewFixtureOnly9472'],
    ['Authorization:\n\v\f\u2003\u3000Bearer\nReviewFixtureOnly more', 'Authorization:\n\v\f\u2003\u3000Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Authorization:\n\v\f\u2003\u3000' + ' '.repeat(45) + 'Bearer\nReviewFixtureOnly more', 'Authorization:\n\v\f\u2003\u3000' + ' '.repeat(45) + 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['AUTHORIZATION: Bearer\nReviewFixtureOnly more', 'AUTHORIZATION: [REDACTED]', 'ReviewFixtureOnly'],
    ['Authorization:Bearer\nReviewFixtureOnly more', 'Authorization:[REDACTED]', 'ReviewFixtureOnly'],
    ['Authorization = "Bearer\nReviewFixtureOnly"', 'Authorization = [REDACTED]', 'ReviewFixtureOnly'],
    ["'Authorization': 'Bearer\nReviewFixtureOnly'", "'Authorization': [REDACTED]", 'ReviewFixtureOnly'],
    ["('Authorization', 'Bearer\nReviewFixtureOnly more')", "('Authorization', 'Bearer [REDACTED] more')", 'ReviewFixtureOnly'],
    ['Authorization\t\u00a0:\n\u00a0Bearer\nReviewFixtureOnly more', 'Authorization\t\u00a0:\n\u00a0Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['# Bearer secret_token_value=', '# Bearer [REDACTED]', 'secret_token_value='],
    ['// Bearer secret_token_value=', '// Bearer [REDACTED]', 'secret_token_value='],
    ['# bearer CLIENT_SECRET=', '# bearer [REDACTED]', 'CLIENT_SECRET='],
    ['# bearer client_secret=', '# bearer [REDACTED]', 'client_secret='],
    ['# bearer CLIENT_SECRET= # comment', '# bearer [REDACTED] # comment', 'CLIENT_SECRET='],
    ['Bearer\nreview_fixture_only: abc123', 'Bearer [REDACTED]: abc123', 'review_fixture_only'],
    ['Bearer\nREVIEW_FIXTURE_ONLY: abc123', 'Bearer [REDACTED]: abc123', 'REVIEW_FIXTURE_ONLY'],
    ['Bearer\nreview_fixture_only: request rejected', 'Bearer [REDACTED]: request rejected', 'review_fixture_only'],
    ['x: Bearer\nreview_fixture_only: request rejected', 'x: Bearer [REDACTED]: request rejected', 'review_fixture_only'],
    ['x: Bearer\nreview_fixture_only: expired', 'x: Bearer [REDACTED]: expired', 'review_fixture_only'],
    ['Bearer\nreview_fixture_only = abc123', 'Bearer [REDACTED] = abc123', 'review_fixture_only'],
    ['Bearer\n--config-file = request rejected', 'Bearer [REDACTED] = request rejected', '--config-file'],
    ['Bearer\n--config-file: abc123', 'Bearer [REDACTED]: abc123', '--config-file'],
    ['MODE=bearer\nconfiguration: details', 'MODE=bearer [REDACTED]: details', 'configuration'],
    ['Bearer\nreview_fixture_only:', 'Bearer [REDACTED]:', 'review_fixture_only'],
    ['x :: Bearer\nschemaV2Config: foo', 'x :: Bearer [REDACTED]: foo', 'schemaV2Config'],
    ['Bearer\nabcdefghijklmg==', 'Bearer [REDACTED]', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmg=', 'Bearer [REDACTED]', 'abcdefghijklmg='],
    ['Bearer\nabcdefghijklmno=', 'Bearer [REDACTED]', 'abcdefghijklmno='],
    ['Basic\nabcdefghijklmg==', 'Basic [REDACTED]', 'abcdefghijklmg=='],
    ['Basic\nabcdefghijklmg=', 'Basic [REDACTED]', 'abcdefghijklmg='],
    ['Basic\nabcdefghijklmno=', 'Basic [REDACTED]', 'abcdefghijklmno='],
    ['Bearer\nabcdefghijklmg==\n', 'Bearer [REDACTED]\n', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmg==  # trailing', 'Bearer [REDACTED]  # trailing', 'abcdefghijklmg=='],
    ['MODE=bearer\nabcdefghijklmg==', 'MODE=bearer [REDACTED]', 'abcdefghijklmg=='],
    ['MODE=basic\nabcdefghijklmg==', 'MODE=basic [REDACTED]', 'abcdefghijklmg=='],
    ['Bearer\n  abcdefghijklmg==', 'Bearer [REDACTED]', 'abcdefghijklmg=='],
    ['Bearer\r\n  abcdefghijklmg==', 'Bearer [REDACTED]', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmg==.', 'Bearer [REDACTED]', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmno=.', 'Bearer [REDACTED]', 'abcdefghijklmno='],
    ['Bearer\nabcdefghijklmg==" and "', 'Bearer [REDACTED]" and "', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmno=" and "', 'Bearer [REDACTED]" and "', 'abcdefghijklmno='],
    ['Bearer\nabcdefghijklmg==\\" and \\"', 'Bearer [REDACTED]\\" and \\"', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmno=\\" and \\"', 'Bearer [REDACTED]\\" and \\"', 'abcdefghijklmno='],
    ['Bearer\nabcdefghijklmg==&foo=1', 'Bearer [REDACTED]&foo=1', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmg==#frag', 'Bearer [REDACTED]#frag', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmg==:8080', 'Bearer [REDACTED]:8080', 'abcdefghijklmg=='],
    ['Bearer\nabcdefghijklmno=&foo=1', 'Bearer [REDACTED]&foo=1', 'abcdefghijklmno='],
    ['scheme: bearer\nabcdefghijklmg==', 'scheme: bearer [REDACTED]', 'abcdefghijklmg=='],
    ['scheme: bearer\nabcdefghijklmg=', 'scheme: bearer [REDACTED]', 'abcdefghijklmg='],
    ['Basic\ncmV2aWV3Rml4dHVyZU9ubHk=', 'Basic [REDACTED]', 'cmV2aWV3Rml4dHVyZU9ubHk='],
    ['Basic\nmySecretToken12==', 'Basic [REDACTED]', 'mySecretToken12=='],
    ['# Bearer\nabcdefghijklmg=', '# Bearer [REDACTED]', 'abcdefghijklmg='],
    ['# Bearer\nabcdefghijklmno=', '# Bearer [REDACTED]', 'abcdefghijklmno='],
    ['# Bearer\nABCDEFGHIJKLMNO=', '# Bearer [REDACTED]', 'ABCDEFGHIJKLMNO='],
    ['# bearer\nschemaV2Configx=', '# bearer [REDACTED]', 'schemaV2Configx='],
    ['# bearer\ncustom-headerxx=', '# bearer [REDACTED]', 'custom-headerxx='],
    ['# bearer\npublicvalue=', '# bearer [REDACTED]', 'publicvalue='],
    ['Authorization:\n`Bearer\nReviewFixtureOnly more`', 'Authorization:\n`Bearer [REDACTED] more`', 'ReviewFixtureOnly'],
    ['Authorization:\n  Bearer\nAbcd1234xyz', 'Authorization:\n  Bearer [REDACTED]', 'Abcd1234xyz'],
    ['Authorization:\n  Bearer\nAbcd1234xyz more', 'Authorization:\n  Bearer [REDACTED] more', 'Abcd1234xyz'],
    ['Authorization: Bearer Abcd1234xyz', 'Authorization: [REDACTED]', 'Abcd1234xyz'],
    ['Authorization: Bearer\nAbcd1234', 'Authorization: [REDACTED]', 'Abcd1234'],
    ['Authorization:\n  Bearer\nAbcd1234', 'Authorization:\n  Bearer [REDACTED]', 'Abcd1234'],
    ['Authorization: Bearer\nshortTok', 'Authorization: [REDACTED]', 'shortTok'],
    ['Authorization:\n  Bearer\nshortTok', 'Authorization:\n  Bearer [REDACTED]', 'shortTok'],
    ['Proxy-Authorization:\n  Bearer\nShortTok', 'Proxy-Authorization:\n  Bearer [REDACTED]', 'ShortTok'],
  ]
  for (const [sample, expected, secret] of redactCases) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).toBe(expected)
    expect(out).not.toContain(secret)
    expect(stats.redactions).toBe(1)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(secret)
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(expected)
    expect(built.redactions).toBe(1)
  }

  const multiExpectCases: readonly (readonly [string, string, string | undefined])[] = [
    ['{"auth": "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=", "user": "alice"}', 'cmV2aWV3Rml4dHVyZU9ubHk=', undefined],
    ['{"note": "Bearer\ncmV2aWV3Rml4dHVyZU9ubHk=", "user": "alice"}', 'cmV2aWV3Rml4dHVyZU9ubHk=', undefined],
    [
      '"{\\"message\\":\\"Bearer mySecretTokenAb=\\",\\"step_index\\":123}"',
      'mySecretTokenAb=',
      '"{\\"message\\":\\"Bearer [REDACTED]\\",\\"step_index\\":123}"',
    ],
    [
      'log: "{\\"note\\": \\"Bearer mySecretTokenAb=\\", \\"id\\": 123}"',
      'mySecretTokenAb=',
      'log: "{\\"note\\": \\"Bearer [REDACTED]\\", \\"id\\": 123}"',
    ],
    ['"{\\"auth\\": \\"Bearer mySecretTokenAb=\\", \\"id\\": 123}"', 'mySecretTokenAb=', undefined],
    ['"Authorization": "\n' + ' '.repeat(60) + 'Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', '"Authorization": [REDACTED]'],
    ['\\"Authorization\\": \\"' + ' '.repeat(60) + 'Bearer\nReviewFixtureOnly more\\"', 'ReviewFixtureOnly', '\\"Authorization\\": [REDACTED]'],
    ['Authorization = "Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'Authorization = [REDACTED]'],
    ["'Authorization': 'Bearer\nReviewFixtureOnly more'", 'ReviewFixtureOnly', "'Authorization': [REDACTED]"],
    ["'Authorization' => 'Bearer\nReviewFixtureOnly more'", 'ReviewFixtureOnly', "'Authorization' =[REDACTED]"],
    ['authorization := "Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization :[REDACTED]'],
    ['authorization ::= "Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization :[REDACTED]'],
    ['authorization: >-\n  Bearer\n  ReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization: [REDACTED]\n'],
    ['authorization: >-\nBearer\nReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization: [REDACTED]\nBearer [REDACTED] more'],
    ['authorization: |2-\nBearer\nReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization: [REDACTED]\nBearer [REDACTED] more'],
    ['authorization: >2\nBearer\nReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization: [REDACTED]\nBearer [REDACTED] more'],
    ['x-custom-auth: >-\n  Bearer\n  ReviewFixtureOnly more', 'ReviewFixtureOnly', 'x-custom-auth: [REDACTED]\n'],
    ['authorization: f"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: r"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: $"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: @"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ["authorization: rf'Bearer\nReviewFixtureOnly more'", 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: ur"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: ru"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: u8"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization: L"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization: [REDACTED]'],
    ['authorization = $@"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = @$"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = ur"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = ru"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = u8"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = L"Bearer\nReviewFixtureOnly more"', 'ReviewFixtureOnly', 'authorization = [REDACTED]'],
    ['authorization = """\nBearer Abcd1234xyz\n"""', 'Abcd1234xyz', 'authorization = [REDACTED]\nBearer [REDACTED]\n"""'],
    ["authorization = '''\nBearer Abcd1234xyz\n'''", 'Abcd1234xyz', "authorization = [REDACTED]\nBearer [REDACTED]\n'''"],
    ["('Authorization', \"\"\"\nBearer Abcd1234xyz\n\"\"\")", 'Abcd1234xyz', "('Authorization', \"\"\"\nBearer [REDACTED]\n\"\"\")"],
    ['Authorization:\u2028Bearer\u2028ReviewFixtureOnly more', 'ReviewFixtureOnly', 'Authorization:\u2028Bearer [REDACTED] more'],
    ['Authorization:\u2028  Bearer\u2028  ReviewFixtureOnly more', 'ReviewFixtureOnly', 'Authorization:\u2028  Bearer [REDACTED] more'],
    // Comparison/scope operators (`==`, `::`) are not explicit auth headers, so
    // the KV pass redacts the `authorization` operand while `Bearer` keeps header neutrality.
    ['authorization== Bearer\nConfiguration details follow', 'Bearer', 'authorization=[REDACTED]\nConfiguration details follow'],
    ['authorization:: Bearer\nConfiguration details follow', 'Bearer', 'authorization:[REDACTED]\nConfiguration details follow'],
    ['authorization== Bearer\nReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization=[REDACTED]'],
    ['authorization:: Bearer\nReviewFixtureOnly more', 'ReviewFixtureOnly', 'authorization:[REDACTED]'],
    ['Bearer\nschemaV2Config= Bearer\nlB79GsW66LvIcKaTzjh=', 'schemaV2Config=', 'Bearer [REDACTED] Bearer [REDACTED]'],
    ['Bearer\nschemaV2Config= Bearer\nWORK_DIR_XY=', 'WORK_DIR_XY=', 'Bearer [REDACTED] Bearer [REDACTED]'],
    ['Bearer\nschemaV2Config= Bearer\ncacheV2Mode=', 'cacheV2Mode=', 'Bearer [REDACTED] Bearer [REDACTED]'],
    ['bash -c "curl -H \\"X-Auth: Bearer mySecretTokenAb=\\""', 'mySecretTokenAb=', undefined],
    ['Bearer\nlB79GsW66LvIcKaTzjh= Bearer\np2112MrvcRo=', 'p2112MrvcRo=', 'Bearer [REDACTED] Bearer [REDACTED]'],
    ['AUTH_MODE := bearer\nCLIENT_SECRET := hunter2', 'hunter2', 'AUTH_MODE :[REDACTED]'],
    ['AUTH_MODE := bearer\nGITHUB_TOKEN := hunter2', 'hunter2', 'AUTH_MODE :[REDACTED]'],
    ['AUTH_MODE ::= bearer\nCLIENT_SECRET ::= hunter2', 'hunter2', 'AUTH_MODE :[REDACTED]'],
    ['MODE := bearer\nCLIENT_SECRET := "hunter2"', 'hunter2', 'MODE := bearer\nCLIENT_SECRET :[REDACTED]'],
    ['MODE ::= bearer\nCLIENT_SECRET ::= "hunter2"', 'hunter2', 'MODE ::= bearer\nCLIENT_SECRET :[REDACTED]'],
    ['authorization: bearer\nclient_secret: "Hunter2Secret!"', 'Hunter2Secret!', 'authorization: [REDACTED]'],
    ['authorization: basic\nclient_secret: "Hunter2Secret!"', 'Hunter2Secret!', 'authorization: [REDACTED]'],
    ['AUTHORIZATION=bearer\nCLIENT_SECRET="Hunter2Secret!"', 'Hunter2Secret!', 'AUTHORIZATION=[REDACTED]'],
    ['PROXY_AUTHORIZATION=bearer\nCLIENT_SECRET="Hunter2Secret!"', 'Hunter2Secret!', 'PROXY_AUTHORIZATION=[REDACTED]'],
    ['AUTHORIZATION=bearer CLIENT_SECRET="Hunter2Secret!" ./app', 'Hunter2Secret!', 'AUTHORIZATION=[REDACTED]'],
    ['Authorization: Bearer client_secret="Hunter2Secret!"', 'Hunter2Secret!', 'Authorization: [REDACTED]'],
    ['Authorization: `Bearer\nReviewFixtureOnly more`', 'ReviewFixtureOnly', 'Authorization: [REDACTED]'],
  ]
  for (const [sample, secret, expectedText] of multiExpectCases) {
    const stats = newStats()
    const out = redactText(sample, stats)
    if (expectedText !== undefined) expect(out).toBe(expectedText)
    if (sample.endsWith('Bearer\nConfiguration details follow')) {
      expect(out).toContain('Configuration details follow')
    }
    expect(out).not.toContain(secret)
    expect(stats.redactions >= 1).toBe(true)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect(JSON.stringify(built.row)).not.toContain(secret)
    if (expectedText !== undefined) {
      expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(expectedText)
    }
    expect(built.redactions >= 1).toBe(true)
  }

  for (const prose of [
    'Basic\nconfiguration guide',
    'Bearer\u00a0ConfigurationWord',
    'Bearer\u00a0AuthenticationGuide',
    'Bearer\u00a0Configuration details follow',
    'Authentication scheme: Bearer\nConfiguration details follow',
    'Bearer\nConfiguration details follow',
    'MODE=bearer\nEMPTY_VAR=""',
    "MODE=bearer\nEMPTY_VAR=''",
    'MODE=bearer\nPUBLIC_NAME="hello"',
    'MODE=bearer\nARTIFACT_DIR=~',
    'MODE=bearer\nARTIFACT_BUCKET=~',
    'MODE=bearer\nPUBLIC_NAME=!hello',
    'MODE=bearer\npublicvalue=!hello',
    'MODE=bearer\npublic_name=~',
    'MODE=bearer\nCLIENT_SECRET=',
    'MODE=bearer\nDB_PASSWORD=',
    'MODE=bearer\npublic_name=',
    'MODE=bearer\npublicvalue=:foo',
    'MODE=bearer\npublicvalue=hello',
    'MODE=bearer\npublicvalue==hello',
    'MODE=bearer\npublicvalue="hello" # "unclosed',
    "MODE=bearer\npublicvalue='hello' // 'unclosed",
    'MODE=bearer\n--secret_pw=',
    "Users' guide to bearer\npublicvalue='hello'",
    "fn f<'a>() -> bearer\npublicvalue='hello'",
    "fn f<T: 'a + 'b>() -> bearer\npublicvalue='hello'",
    "$'it\\'s'\nMODE=bearer\npublicvalue='hello'",
    '# "todo\nMODE=bearer\npublicvalue="hello"',
    "// 'todo\nMODE=bearer\npublicvalue='hello'",
    "# 'todo\r\nMODE=bearer\r\npublicvalue='hello'",
    "# 'todo\rMODE=bearer\rpublicvalue='hello'",
    `bash -c "mode=bearer\npublicvalue=\\"${CANARY}\\""`,
    "MODE=bearer\npublicvalue='hello' # Users' note",
    "MODE=bearer\npublicvalue='hello' && Users' note",
    "MODE=bearer\npublicvalue='hello' && echo $'it\\'s'",
    "MODE=bearer\npublicvalue='hello' && fn<'a>()",
    "# 'todo: check bearer\npublicvalue='hello'",
    '// "todo: check bearer\npublicvalue="hello"',
    'MODE=bearer\npublic_value=',
    'MODE=bearer\npublicvalue=',
    'MODE=bearer\nENVIRONMENT=',
    'MODE=bearer\nclientSecret=',
    'MODE=bearer\nschemaV2Config=',
    'MODE=bearer\napiTokenKey=',
    'MODE=bearer\n-client_secret=',
    'MODE=bearer\n-client_secret:',
    'MODE=bearer\npublicvalue=" \'a\'"',
    String.raw`bash -c "mode=bearer` + '\n' + String.raw`publicvalue=\" \'a\'\""`,
    'MODE=bearer\nAPP_ENV=',
    'MODE=bearer\nENV=',
    'MODE=bearer\nserver.endpoint=',
    'MODE=bearer\ncustom-header=',
    'MODE=bearer\n--config-file=',
    'MODE=bearer\n--config-file1=',
    'MODE=bearer\n-verbose=',
    'MODE=bearer\nWORK_DIR=.',
    'MODE=bearer\npublic_dir=.',
    'MODE=bearer\nCURRENT_WORK_DIR=.',
    'MODE=bearer\npublic_work_dir=.',
    'MODE=bearer\nschemaV2Config=.',
    'MODE=bearer\nschemaV2Configx=',
    'MODE=bearer\ncacheV2Mode=',
    'MODE=bearer\ncustom-headerxx=',
    'MODE=bearer\nWORK_DIR_XY=',
    'MODE=bearer\nWORK_DIR_XYZ=',
    'MODE=bearer\nwork_dir_xy=',
    'MODE=bearer\nwork_dir_xyz=',
    'MODE=bearer\n--config-fx=',
    'MODE=bearer\n--config-fix=',
    'MODE=bearer\n-cfg_flg_ab=',
    'scheme_mode = bearer\nschemaV2Configx=',
    'type=bearer\nserver.endpointx=',
    '# bearer token=',
    '# bearer\n# schemaV2Configx=',
    '# bearer\n# custom-headerxx=',
    'MODE=bearer\nCURRENT_WORK_DIR=',
    'MODE=bearer\npublic_work_dirs=',
    'MODE=bearer\noauth2ClientKey=',
    'MODE=bearer\noauth2ClientSecret=',
    'MODE=bearer\nschemaV2Config=\nMODE=bearer\ncacheV2Mode=',
    'x :=' + ' '.repeat(39) + 'Bearer\nWORK_DIR_XY=',
    'MODE=bearer\nhost = localhost',
    'MODE=bearer\nschemaV2Config: foo',
    'MODE=bearer\nendpoint_url: https://api.example.com',
    'MODE=bearer\nSERVER_ENDPOINT = localhost',
    'MODE=bearer\nredirectUri: https://example.com',
    'MODE=bearer\nendpoint_url: "https://api.example.com"',
    'MODE=bearer\nschemaV2Config:',
    'Bearer\n--config-file = /etc/app.conf',
    'x == ' + ' '.repeat(60) + 'Bearer\nConfiguration details follow',
    'x == ' + ' '.repeat(20) + 'Bearer\nConfiguration details follow',
    'x == ' + ' '.repeat(20) + 'Bearer\nAbcd1234',
    ' '.repeat(20) + 'Bearer\nConfiguration details follow',
    ' '.repeat(20) + 'Bearer\nAbcd1234',
    'x_z12345: Bearer\nConfiguration details follow',
    'x_z12345: Bearer\nAbcd1234',
    'Authorization:\n""\nBearer\nConfiguration details follow',
    'Authentication: ' + ' '.repeat(60) + 'Bearer\nConfiguration details follow',
    'MODE=bearer\nusername=',
    'MODE=bearer\nclient_secret=',
    'MODE=bearer\ntoken=',
    'MODE=basic\nusername=',
    'MODE=basic\nCLIENT_SECRET=',
    '// bearer api_key=',
    '# bearer\n# publicvalue=',
    'MODE=' + ' '.repeat(50) + 'bearer\nusername=',
    'MODE=\u2003' + ' '.repeat(50) + 'bearer\nusername=',
    'Bearer\nusername=',
    'Bearer\ntoken=',
    'Bearer\nfoo== "bar"',
    'MODE=bearer\nfoo=="bar"',
    'MODE=bearer\u2028CLIENT_SECRET=',
    'MODE=bearer\u2029CLIENT_SECRET=',
    // Single-`=` identifiers under `MODE=bearer\n` (`precededByEq && !endsWithDoubleEq`)
    // are preserved as empty config assignments, whereas `Bearer\nabcdefghijklmno=`
    // (without `MODE=`) and `MODE=bearer\nabcdefghijklmg==` (`==`-padded) redact.
    'MODE=bearer\nabcdefghijklmno=',
    'MODE=bearer\nABCDEFGHIJKLMNO=',
    // Comparison/relational operators (`!=`, `<=`, `>=`) are not explicit auth headers
    // or assignments, so both `authorization` and genuine documentation prose / short
    // non-header tokens stay untouched.
    'authorization != Bearer\nConfiguration details follow',
    'authorization <= Bearer\nConfiguration details follow',
    'authorization >= Bearer\nConfiguration details follow',
    'authorization!= Bearer\nConfiguration details follow',
    'authorization<= Bearer\nConfiguration details follow',
    'authorization>= Bearer\nConfiguration details follow',
    'authorization != Bearer\nAbcd1234',
    'authorization <= Bearer\nshortTok',
    'authorization >= Bearer\nAbcd1234',
    'authorization!= Bearer\nshortTok',
    'authorization<= Bearer\nAbcd1234',
    'authorization>= Bearer\nshortTok',
    'Authorization:\n  Bearer\nnone',
    'Authorization:\n  Bearer\nnull',
    'Authorization:\n  Bearer\nempty',
    'Authorization:\n  Bearer\ntoken',
    'Authorization:\n  Bearer\nplaceholder',
    'Authorization:\n  Bearer\nredacted',
    'Authorization:\n  Bearer\n<token>',
  ]) {
    const stats = newStats()
    expect(redactText(prose, stats)).toBe(prose)
    expect(stats.redactions).toBe(0)

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
      content: { result: prose },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(prose)
    expect(built.redactions).toBe(0)
  }

  for (const [label, gen] of [
    ['Bearer\\n repeat', (n: number) => 'Bearer\n'.repeat((n / 7) | 0)],
    ['Basic\\n dots', (n: number) => 'Basic\n' + '.'.repeat(n) + 'x'],
    ['Basic\\n Bearer.', (n: number) => 'Basic\n' + 'Bearer.'.repeat((n / 7) | 0)],
    ['Bearer\\u00a0 Pascal', (n: number) => 'Bearer\u00a0' + 'Aa'.repeat((n / 2) | 0) + 'Z'],
    ['Bearer\\n WORK_DIR= dots', (n: number) => 'MODE=bearer\nWORK_DIR=' + '.'.repeat(n)],
    ['Bearer\\n MySecretToken123= dots', (n: number) => 'Bearer\nMySecretToken123=' + '.'.repeat(n)],
    ['MODE=bearer\\nschemaV2Config colon flood', (n: number) => 'MODE=bearer\nschemaV2Config: foo '.repeat((n / 32) | 0)],
    ['Authorization + spaces + short Bearer flood', (n: number) => ('Authorization:' + ' '.repeat(500) + 'Bearer ').repeat((n / 521) | 0)],
    ['Bearer + spaces + non-matching x', (n: number) => 'Bearer\n' + ' '.repeat(n) + 'x'],
  ] as const) {
    const small = gen(10_000)
    redactText(small, newStats())
    const t0 = performance.now()
    redactText(small, newStats())
    const ms10 = Math.max(0.1, performance.now() - t0)
    const large = gen(80_000)
    const t1 = performance.now()
    redactText(large, newStats())
    const ms80 = performance.now() - t1
    expect({
      label,
      under100ms: ms80 < 100,
      linearScaling: ms80 <= Math.max(25, ms10 * 20),
    }).toEqual({ label, under100ms: true, linearScaling: true })
  }

  for (const [label, gen, largeSize, maxMs, floorMs] of [
    ['"Bearer\\n a12=" repeat', (n: number) => '"Bearer\naaaaaaaaaaa="'.repeat((n / 22) | 0), 160_000, 150, 80],
    ['Bearer\\n a12=" repeat', (n: number) => 'Bearer\naaaaaaaaaaa="'.repeat((n / 21) | 0), 160_000, 150, 80],
    ['"Bearer\\n" + a= pairs', (n: number) => '"Bearer\n' + 'a='.repeat(((n - 8) / 2) | 0), 80_000, 250, 120],
    ['# Bearer\\r comment repeat', (n: number) => '# Bearer\raaaaaaaaaaa="'.repeat((n / 23) | 0), 160_000, 150, 80],
    ['// Bearer\\r comment repeat', (n: number) => '// Bearer\raaaaaaaaaaa="'.repeat((n / 24) | 0), 160_000, 150, 80],
    ["Users' bearer repeat", (n: number) => "Users' bearer\naaaaaaaaaaa='".repeat((n / 27) | 0), 160_000, 150, 80],
    ["fn<'a>() bearer repeat", (n: number) => "fn<'a>() bearer\naaaaaaaaaaa='".repeat((n / 29) | 0), 160_000, 150, 80],
    ["$'it\\'s' bearer repeat", (n: number) => "$'it\\'s' bearer\naaaaaaaaaaa='".repeat((n / 28) | 0), 160_000, 150, 80],
    ['# "todo + CLIENT_SECRET repeat', (n: number) => '# "todo\nMODE=bearer\nCLIENT_SECRET="x"\n'.repeat((n / 39) | 0), 160_000, 250, 120],
    ['# "todo Bearer + quote repeat', (n: number) => '# "todo Bearer\ncustomValue="x"\nlog="Bearer\nU2VjcmV0VG9rZW4="foo"\n'.repeat((n / 60) | 0), 160_000, 400, 180],
    ['// "todo Bearer + quote repeat', (n: number) => '// "todo Bearer\ncustomValue="x"\nlog="Bearer\nU2VjcmV0VG9rZW4="foo"\n'.repeat((n / 61) | 0), 160_000, 400, 180],
    ['# bearer CLIENT_SECRET single-line flood', (n: number) => '# bearer CLIENT_SECRET="MySecret1234" '.repeat((n / 38) | 0), 160_000, 250, 120],
    ['bearer CLIENT_SECRET single-line flood', (n: number) => 'bearer CLIENT_SECRET="MySecret1234" '.repeat((n / 36) | 0), 160_000, 250, 120],
    ['# bearer myTokenSecret123 single-line flood', (n: number) => '# bearer myTokenSecret123="MySecret1234" '.repeat((n / 41) | 0), 160_000, 250, 120],
    ['# bearer client_secret colon single-line flood', (n: number) => '# bearer client_secret: MySecret1234 '.repeat((n / 37) | 0), 160_000, 250, 120],
    ['echo escaped quote Bearer single-line flood', (n: number) => 'echo "\\"Bearer mySecretTokenAb=\\"" '.repeat((n / 34) | 0), 160_000, 250, 120],
    ['# echo single quote Bearer single-line flood', (n: number) => "# echo 'Bearer mySecretTokenAb=' ".repeat((n / 32) | 0), 160_000, 250, 120],
    ['# echo quote foo unclosed single-line flood', (n: number) => "# echo 'Bearer mySecretTokenAb=' foo 'unclosed ".repeat((n / 46) | 0), 160_000, 250, 120],
    ['Token Bearer colon diagnostic single-line flood', (n: number) => 'Token Bearer mySecretToken123: request rejected '.repeat((n / 48) | 0), 160_000, 250, 120],
    ['escaped backtick Bearer flood', (n: number) => 'x=`echo \\`a\\` Bearer mySecretTokenAb=` '.repeat((n / 40) | 0), 160_000, 250, 120],
    ['sh -c innerQuote escaped Bearer flood', (n: number) => 'sh -c \'echo "\\"Bearer mySecretToken=\\"" \\"unclosed\' '.repeat((n / 52) | 0), 160_000, 250, 120],
    ['comment escaped quote Bearer flood', (n: number) => '# echo "\\"Bearer mySecretToken=\\"" \\"unclosed '.repeat((n / 46) | 0), 160_000, 250, 120],
    ['Bearer trailing-dot padding flood', (n: number) => 'Bearer\nMySecretToken123=. '.repeat((n / 26) | 0), 160_000, 250, 120],
    ['Bearer double-eq trailing-dot flood', (n: number) => 'Bearer\ntokenValue12345==. '.repeat((n / 26) | 0), 160_000, 250, 120],
    ['Bearer cross-line non-assignment token flood', (n: number) => 'Bearer\nreviewFixtureOnly9472= '.repeat((n / 30) | 0), 160_000, 250, 120],
    ['Bearer cross-line base64 non-assignment flood', (n: number) => 'Bearer\ndXNlcjpwYXNz= '.repeat((n / 21) | 0), 160_000, 250, 120],
    ['Bearer cross-line base64 camel-version flood', (n: number) => 'Bearer\nlB79GsW66LvIcKaTzjh= '.repeat((n / 28) | 0), 160_000, 250, 120],
    ['Bearer cross-line diagnostic colon flood', (n: number) => 'Bearer\nreviewFixtureOnly9472: request rejected '.repeat((n / 46) | 0), 160_000, 250, 120],
    ['Bearer single-line empty env flood', (n: number) => 'Bearer secret_token_value= '.repeat((n / 27) | 0), 160_000, 250, 120],
    ['Authorization 160k whitespace Bearer prose flood', (n: number) => 'Authorization:\n' + ' '.repeat(n) + 'Bearer\nReviewFixtureOnly more', 160_000, 150, 80],
    ['Authorization 160k backslash quote Bearer flood', (n: number) => 'Authorization:\n' + '\\'.repeat(n) + '"Bearer\nReviewFixtureOnly more', 160_000, 250, 120],
    ['proxyAuthorization 160k whitespace Bearer prose flood', (n: number) => 'proxyAuthorization:\n' + ' '.repeat(n) + 'Bearer\nReviewFixtureOnly more', 160_000, 150, 80],
    ['MODE=bearer CLIENT_SECRET multi-word colon flood', (n: number) => 'MODE=bearer\nCLIENT_SECRET: correct horse battery staple\n'.repeat((n / 57) | 0), 160_000, 250, 120],
    ['Bearer bare non-sensitive colon flood', (n: number) => 'Bearer\nreview_fixture_only: abc123\n'.repeat((n / 36) | 0), 160_000, 250, 120],
    ['# Bearer single-line empty env flood', (n: number) => '# Bearer secret_token_value=\n'.repeat((n / 29) | 0), 160_000, 250, 120],
    ['Bearer CLIENT_SECRET= 160k space flood', (n: number) => 'Bearer CLIENT_SECRET=' + ' '.repeat(n) + 'MySecret1234', 160_000, 150, 80],
    ['Bearer CLIENT_SECRET= 160k empty space flood', (n: number) => 'Bearer CLIENT_SECRET=' + ' '.repeat(n), 160_000, 150, 80],
    ['Bearer cross-line CLIENT_SECRET 160k colon space flood', (n: number) => 'Bearer\nCLIENT_SECRET: ' + ' '.repeat(n) + 'invalid hunter2', 160_000, 150, 80],
    ['Bearer cross-line CLIENT_SECRET repeated colon space flood', (n: number) => ('Bearer\nCLIENT_SECRET: ' + ' '.repeat(200) + 'invalid hunter2\n').repeat((n / 238) | 0), 160_000, 250, 120],
    ['Bearer cross-line lowercase padded == flood', (n: number) => 'Bearer\nabcdefghijklmg== '.repeat((n / 24) | 0), 160_000, 250, 120],
    ['Basic cross-line lowercase padded == flood', (n: number) => 'Basic\nabcdefghijklmg== '.repeat((n / 23) | 0), 160_000, 250, 120],
    ['Bearer CLIENT_SECRET quoted colon single-line flood', (n: number) => 'Bearer CLIENT_SECRET: "x" '.repeat((n / 26) | 0), 160_000, 250, 120],
    ['Bearer CLIENT_SECRET spaced eq single-line flood', (n: number) => 'Bearer CLIENT_SECRET= x '.repeat((n / 24) | 0), 160_000, 250, 120],
  ] as const) {
    const small = gen(10_000)
    redactText(small, newStats())
    const t0 = performance.now()
    redactText(small, newStats())
    const msSmall = Math.max(1, performance.now() - t0)
    const large = gen(largeSize)
    redactText(large.slice(0, 100), newStats())
    const t1 = performance.now()
    const outLarge = redactText(large, newStats())
    const msLarge = performance.now() - t1
    expect(outLarge).not.toContain('U2VjcmV0VG9rZW4=')
    expect(outLarge).not.toContain('abcdefghijklmg==')
    expect(outLarge).not.toContain('MySecret1234')
    expect(outLarge).not.toContain('mySecretTokenAb=')
    expect(outLarge).not.toContain('mySecretToken123')
    expect(outLarge).not.toContain('mySecretToken=')
    expect(outLarge).not.toContain('MySecretToken123')
    expect(outLarge).not.toContain('tokenValue12345')
    expect(outLarge).not.toContain('reviewFixtureOnly9472=')
    expect(outLarge).not.toContain('reviewFixtureOnly9472')
    expect(outLarge).not.toContain('secret_token_value=')
    expect(outLarge).not.toContain('ReviewFixtureOnly')
    expect(outLarge).not.toContain('dXNlcjpwYXNz=')
    expect(outLarge).not.toContain('lB79GsW66LvIcKaTzjh=')
    expect(outLarge).not.toContain('correct horse battery staple')
    expect(outLarge).not.toContain('review_fixture_only')
    expect(outLarge).not.toContain('hunter2')
    expect(outLarge).not.toContain('CLIENT_SECRET: ')
    expect({
      label,
      withinBudget: msLarge < maxMs,
      linearScaling: msLarge <= Math.max(floorMs, msSmall * 25),
    }).toEqual({ label, withinBudget: true, linearScaling: true })

    if (
      label === 'Bearer CLIENT_SECRET= 160k space flood' ||
      label === 'Bearer cross-line CLIENT_SECRET 160k colon space flood'
    ) {
      const builtLarge = buildRow({
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
        content: { result: large },
        attributes: {},
        maxPayloadChars: 500_000,
      })
      const serialized = JSON.stringify(builtLarge.row)
      expect(serialized).not.toContain('MySecret1234')
      expect(serialized).not.toContain('hunter2')
      expect(builtLarge.redactions).toBeGreaterThanOrEqual(1)
    }
  }

  for (const [label, gen, expectedScheme] of [
    ['Bearer ALL_CAPS_SNAKE single long non-match', (n: number) => 'Bearer A' + '_'.repeat(n) + 'a=', 'Bearer'],
    ['bearer lower_snake single long non-match', (n: number) => 'bearer a' + '_'.repeat(n) + 'A=', 'bearer'],
    ['Bearer single-dash snake single long non-match', (n: number) => 'Bearer -a' + '_'.repeat(n) + 'A=', 'Bearer'],
    ['Bearer single-dash sensitive key single long token', (n: number) => 'Bearer -a' + '_'.repeat(n) + 'key=', 'Bearer'],
    ['Bearer cross-line camel single long non-match', (n: number) => 'Bearer\na' + 'Bc'.repeat(n >> 1) + '1=', 'Bearer'],
    ['Bearer cross-line dotted single long non-match', (n: number) => 'Bearer\n' + 'ab.'.repeat((n / 3) | 0) + '1=', 'Bearer'],
    ['Bearer cross-line kebab single long non-match', (n: number) => 'Bearer\n' + 'ab-'.repeat((n / 3) | 0) + '1=', 'Bearer'],
    ['Bearer cross-line ALL_CAPS_SNAKE over cap', (n: number) => 'Bearer\nA' + '_'.repeat(n) + 'A=', 'Bearer'],
  ] as const) {
    const small = gen(10_000)
    redactText(small, newStats())
    const t0 = performance.now()
    redactText(small, newStats())
    const msSmall = Math.max(1, performance.now() - t0)
    const large = gen(160_000)
    const statsLarge = newStats()
    const t1 = performance.now()
    const outLarge = redactText(large, statsLarge)
    const msLarge = performance.now() - t1
    expect(outLarge).toBe(`${expectedScheme} [REDACTED]`)
    expect(statsLarge.redactions).toBe(1)
    expect({
      label,
      withinBudget: msLarge < 100,
      linearScaling: msLarge <= Math.max(40, msSmall * 25),
    }).toEqual({ label, withinBudget: true, linearScaling: true })
  }

  const padTail = ' ' + 'a'.repeat(300)
  for (const [sample, isDoubleQuoted] of [
    ['# bearer myTokenSecret12="x" # "unclosed', false],
    ["# bearer myTokenSecret12='x' // 'unclosed", false],
    ["# bearer myTokenSecret12='x' $'it\\'s' 'y'", false],
    ["# bearer myTokenSecret12='Don\\'t' 'y'", false],
    ["# bearer myTokenSecret12='Users' 'y'", false],
    ["# bearer myTokenSecret12='x' fn<'a>()", false],
    ['# bearer myTokenSecret12="x"   "y"', false],
    ['bash -c "mode=bearer myTokenSecret12=\\"x\\" \\"y\\""', true],
    ['bash -c "mode=bearer myTokenSecret12=\\"x\\" \\\\ \\"y\\""', true],
    ['echo "\\"Bearer mySecretTokenAb=\\" \\"next\\""', true],
    ["# echo 'Bearer mySecretTokenAb=' 'next'", false],
    ["# echo 'Bearer mySecretTokenAb=' foo 'unclosed", false],
    ['sh -c \'echo "\\"Bearer mySecretToken=\\"" \\"unclosed\'', false],
    ['# echo "\\"Bearer mySecretToken=\\"" \\"unclosed', false],
    ['x=`echo \\`a\\` Bearer mySecretTokenAb=`', false],
    ['# x=`echo \\`a\\` Bearer mySecretTokenAb=` `unclosed', false],
    ['sh -c \'echo "\\"unclosed"; echo "Bearer mySecretToken=" "unclosed\'', false],
    ['x="echo `echo \\`a\\` Bearer mySecretToken=`"', true],
    ['Bearer MySecretToken123=.', false],
    ['Bearer tokenValue12345==.', false],
  ] as const) {
    const padded = isDoubleQuoted ? sample.slice(0, -1) + padTail + '"' : sample + padTail
    const s1 = newStats()
    const o1 = redactText(sample, s1)
    const s2 = newStats()
    const o2 = redactText(padded, s2)
    const strippedO2 = isDoubleQuoted
      ? o2.endsWith(padTail + '"')
        ? o2.slice(0, -(padTail.length + 1)) + '"'
        : o2
      : o2.endsWith(padTail)
        ? o2.slice(0, -padTail.length)
        : o2
    expect(o1).toBe(strippedO2)
    expect(s1.redactions).toBe(s2.redactions)
  }

  for (const [sample, expected, secret] of [
    [`scheme: bearer\nclient_secret: ${CANARY}`, 'scheme: bearer [REDACTED]: [REDACTED]', CANARY],
    [`scheme: bearer\npassword_hash: ${CANARY}`, 'scheme: bearer [REDACTED]: [REDACTED]', CANARY],
    [`MODE=bearer\nCLIENT_SECRET="${CANARY}"`, 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', CANARY],
    [`Don't use bearer\nCLIENT_SECRET='${CANARY}'`, "Don't use bearer\nCLIENT_SECRET=[REDACTED]", CANARY],
    [`MODE=bearer\nCLIENT_SECRET=${CANARY}`, 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', CANARY],
    ['MODE=bearer\nCLIENT_SECRET=$MY_TOKEN_VAR', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', '$MY_TOKEN_VAR'],
    ['MODE=bearer\nCLIENT_SECRET=${MY_TOKEN_VAR}', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', '${MY_TOKEN_VAR}'],
    ['MODE=bearer\nCLIENT_SECRET=!MySuperSecret123', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', '!MySuperSecret123'],
    ['MODE=bearer\nDB_PASSWORD=@pplePass123', 'MODE=bearer\nDB_PASSWORD=[REDACTED]', '@pplePass123'],
    [`bash -c "mode=bearer\nCLIENT_SECRET=\\"${CANARY}\\""`, 'bash -c "mode=bearer\nCLIENT_SECRET=[REDACTED]', CANARY],
    [`bash -c "mode=bearer\nDB_PASSWORD=\\"${CANARY}\\""`, 'bash -c "mode=bearer\nDB_PASSWORD=[REDACTED]', CANARY],
    ['MODE=bearer\nCLIENT_SECRET=~!Secret123', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', '!Secret123'],
    ['MODE=bearer\nDB_PASSWORD=~!Secret123', 'MODE=bearer\nDB_PASSWORD=[REDACTED]', '!Secret123'],
    ['MODE=bearer\nCLIENT_SECRET==super!Secret123', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', 'super!Secret123'],
    ['MODE=bearer\nCLIENT_SECRET=#hashSecret123', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', '#hashSecret123'],
    ['MODE=bearer\nCLIENT_SECRET=(parenSecret123)', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', 'parenSecret123'],
    ['MODE=bearer\nCLIENT_SECRET=:colonSecret123', 'MODE=bearer\nCLIENT_SECRET=[REDACTED]', ':colonSecret123'],
    ['MODE=bearer\n--client-secret=!MySuperSecret123', 'MODE=bearer\n--client-secret=[REDACTED]', '!MySuperSecret123'],
    ['MODE=bearer\n--secret_pw=@pplePass123', 'MODE=bearer\n--secret_pw=[REDACTED]', '@pplePass123'],
    ['MODE=basic\nCLIENT_SECRET=!MySuperSecret123', 'MODE=basic\nCLIENT_SECRET=[REDACTED]', '!MySuperSecret123'],
    [`Users' guide to bearer\nCLIENT_SECRET='${CANARY}'`, "Users' guide to bearer\nCLIENT_SECRET=[REDACTED]", CANARY],
    [`fn f<'a>() // bearer\nCLIENT_SECRET='${CANARY}'`, "fn f<'a>() // bearer\nCLIENT_SECRET=[REDACTED]", CANARY],
    [`$'it\\'s'\nMODE=bearer\nCLIENT_SECRET='${CANARY}'`, "$'it\\'s'\nMODE=bearer\nCLIENT_SECRET=[REDACTED]", CANARY],
    [`# "todo\nMODE=bearer\nCLIENT_SECRET="${CANARY}"`, '# "todo\nMODE=bearer\nCLIENT_SECRET=[REDACTED]', CANARY],
    [`// 'todo\nMODE=bearer\nCLIENT_SECRET='${CANARY}'`, "// 'todo\nMODE=bearer\nCLIENT_SECRET=[REDACTED]", CANARY],
    [`MODE=bearer\nmyTokenSecret123="${CANARY}"`, 'MODE=bearer\nmyTokenSecret123=[REDACTED]', CANARY],
    ['# bearer CLIENT_SECRET="MySecret1234"', '# bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['# bearer client_secret="MySecret1234"', '# bearer client_secret=[REDACTED]', 'MySecret1234'],
    ['# bearer clientSecret="MySecret1234"', '# bearer clientSecret=[REDACTED]', 'MySecret1234'],
    ['# bearer --secret_pw="MySecret1234"', '# bearer --secret_pw=[REDACTED]', 'MySecret1234'],
    ['# "todo bearer CLIENT_SECRET="MySecret1234"', '# "todo bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ["// 'todo bearer CLIENT_SECRET='MySecret1234'", "// 'todo bearer CLIENT_SECRET=[REDACTED]", 'MySecret1234'],
    ['# bearer CLIENT_SECRET=$MY_SECRET_VAR', '# bearer CLIENT_SECRET=[REDACTED]', '$MY_SECRET_VAR'],
    ['# bearer CLIENT_SECRET=!MySecret1234', '# bearer CLIENT_SECRET=[REDACTED]', '!MySecret1234'],
    ['# bearer CLIENT_SECRET=mySecret!123', '# bearer CLIENT_SECRET=[REDACTED]', 'mySecret!123'],
    ['# bearer CLIENT_SECRET=abc"def"', '# bearer CLIENT_SECRET=[REDACTED]', 'abc"def"'],
    ['# bearer CLIENT_SECRET=\\\nMySecret1234', '# bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret123="MySecret1234"', '# bearer myTokenSecret123=[REDACTED]', 'MySecret1234'],
    ['# bearer oauth2ClientSecret="MySecret1234"', '# bearer oauth2ClientSecret=[REDACTED]', 'MySecret1234'],
    ['# bearer ClientSecret="MySecret1234"', '# bearer ClientSecret=[REDACTED]', 'MySecret1234'],
    ['# bearer client-secret="MySecret1234"', '# bearer client-secret=[REDACTED]', 'MySecret1234'],
    ['# bearer auth.client_secret="MySecret1234"', '# bearer auth.client_secret=[REDACTED]', 'MySecret1234'],
    ['# bearer secret123456="MySecret1234"', '# bearer secret123456=[REDACTED]', 'MySecret1234'],
    ['# bearer -client_secret="MySecret1234"', '# bearer -client_secret=[REDACTED]', 'MySecret1234'],
    ['# bearer -Dssl.password="MySecret1234"', '# bearer -Dssl.password=[REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret12="MySecret1234"', '# bearer myTokenSecret12=[REDACTED]', 'MySecret1234'],
    ['# "todo bearer myTokenSecret12="MySecret1234"', '# "todo bearer myTokenSecret12=[REDACTED]', 'MySecret1234'],
    ['# "todo bearer myTokenSecret12="MySecret1234', '# "todo bearer myTokenSecret12=[REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret123=MySecret1234', '# bearer myTokenSecret123=[REDACTED]', 'MySecret1234'],
    ['# bearer -client_secret=MySecret1234', '# bearer -client_secret=[REDACTED]', 'MySecret1234'],
    ['# bearer client-secret=MySecret1234', '# bearer client-secret=[REDACTED]', 'MySecret1234'],
    ['# bearer auth.client_secret=MySecret1234', '# bearer auth.client_secret=[REDACTED]', 'MySecret1234'],
    ['# bearer client_secret: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer password_hash: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer client-secret: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer auth.client_secret: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer ClientSecret: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret123: "MySecret1234"', '# bearer myTokenSecret123: [REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret123: MySecret1234', '# bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['# bearer CLIENT_SECRET = "MySecret1234"', '# bearer CLIENT_SECRET = [REDACTED]', 'MySecret1234'],
    ['// bearer client_secret: MySecret1234', '// bearer [REDACTED]: [REDACTED]', 'MySecret1234'],
    ['// bearer myTokenSecret123="MySecret1234"', '// bearer myTokenSecret123=[REDACTED]', 'MySecret1234'],
    ['# bearer CLIENT_SECRET= MySecret1234', '# bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['# "todo bearer CLIENT_SECRET= MySecret1234', '# "todo bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['# bearer myTokenSecret123= "MySecret1234"', '# bearer myTokenSecret123=[REDACTED]', 'MySecret1234'],
    ['# bearer CLIENT_SECRET=="MySecret1234"', '# bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['bash -c "mode=bearer myTokenSecret12=\\"MySecret1234\\""', 'bash -c "mode=bearer myTokenSecret12=[REDACTED]', 'MySecret1234'],
    [`MODE=bearer\nCLIENT_SECRET: ${CANARY}`, 'MODE=bearer [REDACTED]: [REDACTED]', CANARY],
    ['MODE=bearer\nCLIENT_SECRET: foo', 'MODE=bearer [REDACTED]: [REDACTED]', 'foo'],
    ['scheme: Bearer\ndatabase_password: Winter Is Coming', 'scheme: Bearer [REDACTED]: [REDACTED]', 'Winter Is Coming'],
    ['MODE=bearer\nCLIENT_SECRET: correct horse battery staple', 'MODE=bearer [REDACTED]: [REDACTED]', 'correct horse battery staple'],
    ['type: bearer\nssh_key_passphrase: correct horse battery staple', 'type: bearer [REDACTED]: [REDACTED]', 'correct horse battery staple'],
    ['MODE=bearer\nCLIENT_SECRET: expired', 'MODE=bearer [REDACTED]: [REDACTED]', 'expired'],
    ['MODE=bearer\nmyTokenSecret12: Winter Is Coming', 'MODE=bearer [REDACTED]: [REDACTED]', 'Winter Is Coming'],
    ['Bearer\nCLIENT_SECRET: correct horse battery staple', 'Bearer [REDACTED]: [REDACTED]', 'correct horse battery staple'],
    ['Bearer\nCLIENT_SECRET: two words', 'Bearer [REDACTED]: [REDACTED]', 'two words'],
    ['Bearer\nCLIENT_SECRET: abc123', 'Bearer [REDACTED]: [REDACTED]', 'abc123'],
    ['Bearer\nCLIENT_SECRET = two words', 'Bearer [REDACTED] = [REDACTED]', 'two words'],
    ['# bearer CLIENT_SECRET: correct horse battery staple', '# bearer [REDACTED]: [REDACTED]', 'correct horse battery staple'],
    ['# bearer client_secret: Winter Is Coming', '# bearer [REDACTED]: [REDACTED]', 'Winter Is Coming'],
    ['# bearer myTokenSecret123: foo', '# bearer [REDACTED]: [REDACTED]', 'foo'],
    ['# bearer myTokenSecret123= MySecret1234', '# bearer myTokenSecret123=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET= MySecret1234', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['# bearer CLIENT_SECRET= MySecret1234', '# bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=\u00a0MySecret1234', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=\u2003MySecret1234', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=' + ' '.repeat(40) + 'MySecret1234', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=' + '\u00a0'.repeat(40) + 'MySecret1234', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=\u00a0"MySecret1234"', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET=' + ' '.repeat(40) + '"MySecret1234"', 'MODE=bearer CLIENT_SECRET=[REDACTED]', 'MySecret1234'],
    ['MODE=bearer CLIENT_SECRET: "MySecret1234"', 'MODE=bearer CLIENT_SECRET: [REDACTED]', 'MySecret1234'],
    ['# bearer CLIENT_SECRET: "MySecret1234"', '# bearer CLIENT_SECRET: [REDACTED]', 'MySecret1234'],
    ['MODE=bearer\n--client-secret: "mySecret1234"', 'MODE=bearer\n--client-secret: [REDACTED]', 'mySecret1234'],
    ['MODE=bearer\n-client_secret = mySecret1234', 'MODE=bearer [REDACTED] = [REDACTED]', 'mySecret1234'],
    [`MODE=bearer\nCLIENT_SECRET = ${CANARY}`, 'MODE=bearer [REDACTED] = [REDACTED]', CANARY],
  ] as const) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(secret)
    expect(out).toBe(expected)
    expect(stats.redactions).toBe(1)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(secret)
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(expected)
    expect(built.redactions).toBe(1)
  }

  for (const [sample, expected, secret] of [
    ['# bearer CLIENT_SECRET=https://user:s3cret@example.com', '# bearer CLIENT_SECRET=[REDACTED]', 's3cret'],
  ] as const) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).not.toContain(secret)
    expect(out).toBe(expected)
    expect(stats.redactions >= 1).toBe(true)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect(JSON.stringify(built.row)).not.toContain(secret)
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(expected)
    expect(built.redactions >= 1).toBe(true)
  }

  for (const [sample, expected, forbidden] of [
    ['Bearer secret_token_value: token is invalid', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'token is invalid']],
    ['Bearer\nsecret_token_value: request was rejected', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'request was rejected']],
    ['x: Bearer\nsecret_token_value: request rejected', 'x: Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'request rejected']],
    ['Bearer\nsecret_token_value: Token is expired', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Token is expired']],
    ['Bearer\nsecret_token_value: Signature has expired', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Signature has expired']],
    ['Bearer\nsecret_token_value: signature verification failed', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'signature verification failed']],
    ['Bearer\nsecret_token_value: not authorized', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'not authorized']],
    ['Bearer\nCLIENT_SECRET: invalid hunter2', 'Bearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'invalid hunter2']],
    ['Bearer\nCLIENT_SECRET: blocked.cat.42', 'Bearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', 'blocked.cat.42']],
    ['Bearer\nCLIENT_SECRET: 503 Seagull Ave', 'Bearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', '503 Seagull Ave']],
    ['Bearer\nCLIENT_SECRET = invalid hunter2', 'Bearer [REDACTED] = [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'invalid hunter2']],
    ['Bearer secret_token_value = invalid hunter2', 'Bearer [REDACTED] = [REDACTED]', ['secret_token_value', 'hunter2', 'invalid hunter2']],
    ['Basic\nCLIENT_SECRET: invalid hunter2', 'Basic [REDACTED]: [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'invalid hunter2']],
    ['Basic secret_token_value: token is invalid', 'Basic [REDACTED]: [REDACTED]', ['secret_token_value', 'token is invalid']],
    ['Basic\nsecret_token_value: request was rejected', 'Basic [REDACTED]: [REDACTED]', ['secret_token_value', 'request was rejected']],
    ['Authorization:\nBearer\nCLIENT_SECRET: invalid hunter2', 'Authorization:\nBearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'invalid hunter2']],
    ['Authorization:\nBearer\nCLIENT_SECRET = invalid hunter2', 'Authorization:\nBearer [REDACTED] = [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'invalid hunter2']],
    ['Bearer\nCLIENT_SECRET: ' + 'x'.repeat(120) + ' hunter2', 'Bearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', 'hunter2', 'x'.repeat(120)]],
    ['MODE=bearer\n-client_secret: mySecret1234', 'MODE=bearer [REDACTED]: [REDACTED]', ['-client_secret', 'mySecret1234']],
    ['Bearer\n--client-secret: mySecret1234', 'Bearer [REDACTED]: [REDACTED]', ['--client-secret', 'mySecret1234']],
    ['Bearer\n--CLIENT-SECRET: mySecret1234', 'Bearer [REDACTED]: [REDACTED]', ['--CLIENT-SECRET', 'mySecret1234']],
    ['Bearer -client_secret: mySecret1234', 'Bearer [REDACTED]: [REDACTED]', ['-client_secret', 'mySecret1234']],
    ['Bearer --client-secret: mySecret1234', 'Bearer [REDACTED]: [REDACTED]', ['--client-secret', 'mySecret1234']],
    [`scheme: bearer\nclient_secret: ${CANARY}`, 'scheme: bearer [REDACTED]: [REDACTED]', ['client_secret', CANARY]],
    [`MODE=bearer\nCLIENT_SECRET: ${CANARY}`, 'MODE=bearer [REDACTED]: [REDACTED]', ['CLIENT_SECRET', CANARY]],
    ['Bearer CLIENT_SECRET= MySecret1234', 'Bearer [REDACTED]= [REDACTED]', ['CLIENT_SECRET', 'MySecret1234']],
    ['Bearer CLIENT_SECRET=\u00a0MySecret1234', 'Bearer [REDACTED]=\u00a0[REDACTED]', ['CLIENT_SECRET', 'MySecret1234']],
    ['Bearer CLIENT_SECRET=\u2003MySecret1234', 'Bearer [REDACTED]=\u2003[REDACTED]', ['CLIENT_SECRET', 'MySecret1234']],
    ['Bearer CLIENT_SECRET=' + ' '.repeat(40) + 'MySecret1234', 'Bearer [REDACTED]=' + ' '.repeat(40) + '[REDACTED]', ['CLIENT_SECRET', 'MySecret1234']],
    ['Bearer CLIENT_SECRET=\u00a0"MySecret1234"', 'Bearer [REDACTED]=\u00a0[REDACTED]', ['CLIENT_SECRET', 'MySecret1234']],
    ['Bearer secret_token_12345= unauthorized', 'Bearer [REDACTED]= [REDACTED]', ['secret_token_12345', 'unauthorized']],
    ['[Bearer secret_token_12345= unauthorized]', '[Bearer [REDACTED]= [REDACTED]]', ['secret_token_12345', 'unauthorized']],
    ['[Bearer secret_token_12345= "a]b"]', '[Bearer [REDACTED]= [REDACTED]]', ['secret_token_12345', 'a]b']],
    ['[Bearer secret_token_12345= ]', '[Bearer [REDACTED] ]', ['secret_token_12345']],
    ['Rejected Bearer my_access_token_99: "invalid scope"', 'Rejected Bearer [REDACTED]: [REDACTED]', ['my_access_token_99', 'invalid scope']],
    ['log="Bearer secret_token_value: "', 'log="Bearer [REDACTED]: "', ['secret_token_value']],
    ["msg='Bearer secret_token_value:'", "msg='Bearer [REDACTED]:'", ['secret_token_value']],
    ['"Bearer secret_token_12345: $"', '"Bearer [REDACTED]: [REDACTED]"', ['secret_token_12345']],
    ['Bearer secret_token_value: $5 refund', 'Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'refund']],
    ['Bearer CLIENT_SECRET := hunter2', 'Bearer [REDACTED] := [REDACTED]', ['CLIENT_SECRET', 'hunter2']],
    ['Bearer CLIENT_SECRET ::= hunter2', 'Bearer [REDACTED] ::= [REDACTED]', ['CLIENT_SECRET', 'hunter2']],
    ['MODE=bearer\nCLIENT_SECRET\u2003: hunter2', 'MODE=bearer [REDACTED]\u2003: [REDACTED]', ['CLIENT_SECRET', 'hunter2']],
    ['Authorization:\n    Bearer\nsecret_token_value: "request rejected"', 'Authorization:\n    Bearer [REDACTED]: [REDACTED]', ['secret_token_value', 'request rejected']],
    ['Authorization:\nBearer secret_token_value = "x"', 'Authorization:\nBearer [REDACTED] = [REDACTED]', ['secret_token_value']],
    ['Authorization:\nBearer\nsecret_token_value: $REASON', 'Authorization:\nBearer [REDACTED]: [REDACTED]', ['secret_token_value', '$REASON']],
    ['Authorization:\n  Bearer\nclient_secret="Hunter2Secret!"', 'Authorization:\n  Bearer [REDACTED]', ['client_secret', 'Hunter2Secret!']],
    ['Authorization:\nBearer\napi_key="Hunter2Secret!"', 'Authorization:\nBearer [REDACTED]', ['api_key', 'Hunter2Secret!']],
    ['Authorization:\nBearer\napi_key: "Hunter2Secret!"', 'Authorization:\nBearer [REDACTED]: [REDACTED]', ['api_key', 'Hunter2Secret!']],
    ["('Authorization', 'Bearer client_secret=\"Hunter2Secret!\"')", "('Authorization', 'Bearer [REDACTED]')", ['client_secret', 'Hunter2Secret!']],
    ["('Authorization', 'Bearer\nclient_secret: \"Hunter2Secret!\"')", "('Authorization', 'Bearer [REDACTED]: [REDACTED]')", ['client_secret', 'Hunter2Secret!']],
    ['{"message":"Bearer secret_token_value: denied","status":"ok"}', '{"message":"Bearer [REDACTED]: [REDACTED]","status":"ok"}', ['secret_token_value', 'denied']],
    ['{"error":"Rejected Bearer my_access_token_99: "}', '{"error":"Rejected Bearer [REDACTED]: "}', ['my_access_token_99']],
    ['Log: {"e":"Bearer secret_token_value: denied","next":"ok"}', 'Log: {"e":"Bearer [REDACTED]: [REDACTED]","next":"ok"}', ['secret_token_value', 'denied']],
    ['Note (see docs\nBearer secret_token_value: Pa)ss-w0rd-secret', 'Note (see docs\nBearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Pa)ss-w0rd-secret', 'ss-w0rd-secret']],
    ['Note {see docs\nBearer secret_token_value: Pa}ss-w0rd-secret', 'Note {see docs\nBearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Pa}ss-w0rd-secret', 'ss-w0rd-secret']],
    ['Note [see docs\nBearer secret_token_value: Pa]ss-w0rd-secret', 'Note [see docs\nBearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Pa]ss-w0rd-secret', 'ss-w0rd-secret']],
    ['Note "see docs\nBearer secret_token_value: Pa"ss-w0rd-secret', 'Note "see docs\nBearer [REDACTED]: [REDACTED]', ['secret_token_value', 'Pa"ss-w0rd-secret', 'ss-w0rd-secret']],
    ['He said "hi\nx = "(" + "Bearer secret_token_value: Pa)ss-w0rd"', 'He said "hi\nx = "(" + "Bearer [REDACTED]: [REDACTED]"', ['secret_token_value', 'Pa)ss-w0rd', 'ss-w0rd']],
    ['He said "hi\nx = "[" + "Bearer secret_token_value: Pa]ss-w0rd"', 'He said "hi\nx = "[" + "Bearer [REDACTED]: [REDACTED]"', ['secret_token_value', 'Pa]ss-w0rd', 'ss-w0rd']],
    ['He said "hi\nx = "{" + "Bearer secret_token_value: Pa}ss-w0rd"', 'He said "hi\nx = "{" + "Bearer [REDACTED]: [REDACTED]"', ['secret_token_value', 'Pa}ss-w0rd', 'ss-w0rd']],
    ['He said "hi\nx = "(\\"Bearer secret_token_value: Pa)ss-w0rd\\""', 'He said "hi\nx = "(\\"Bearer [REDACTED]: [REDACTED]\\""', ['secret_token_value', 'Pa)ss-w0rd', 'ss-w0rd']],
    ['Authorization: Bearer client_secret= #s3cret', 'Authorization: [REDACTED]', ['client_secret', 's3cret']],
    ['Authorization:\nBearer client_secret= #s3cret', 'Authorization:\nBearer [REDACTED]', ['client_secret', 's3cret']],
    ['X-Auth: Bearer client_secret= #s3cret', 'X-Auth: [REDACTED]', ['client_secret', 's3cret']],
    ['X-Auth:\nBearer client_secret= #s3cret', 'X-Auth:\nBearer [REDACTED]= [REDACTED]', ['client_secret', 's3cret']],
  ] as const) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).toBe(expected)
    for (const part of forbidden) expect(out).not.toContain(part)
    expect(stats.redactions).toBe(1)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    for (const part of forbidden) expect(serialized).not.toContain(part)
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(expected)
    expect(built.redactions).toBe(1)
  }

  for (const [label, gen, expectedSecret] of [
    ['MODE=bearer CLIENT_SECRET quoted colon single-line flood', (n: number) => 'MODE=bearer CLIENT_SECRET: "x" '.repeat((n / 31) | 0), '"x"'],
    ['MODE=bearer CLIENT_SECRET spaced eq single-line flood', (n: number) => 'MODE=bearer CLIENT_SECRET= x '.repeat((n / 29) | 0), '= x'],
    ['MODE= 160k space + bearer\\nusername=', (n: number) => 'MODE=' + ' '.repeat(n) + 'bearer\nusername=', 'NONE_EXPECTED'],
    ['open bracket + Bearer client_secret=, flood', (n: number) => '[' + 'Bearer client_secret=, '.repeat((n / 23) | 0), 'client_secret'],
    ['open quote + Bearer client_secret=, flood', (n: number) => '"' + 'Bearer client_secret=, '.repeat((n / 23) | 0), 'client_secret'],
    ['open bracket + k= Bearer client_secret: "y" flood', (n: number) => '[' + 'k= Bearer client_secret: "y" '.repeat((n / 29) | 0), '"y"'],
    ['open bracket + Authorization: Bearer client_secret=, flood', (n: number) => '[' + 'Authorization: Bearer client_secret=, '.repeat((n / 38) | 0), 'client_secret'],
  ] as const) {
    const small = gen(10_000)
    redactText(small, newStats())
    const t0 = performance.now()
    redactText(small, newStats())
    const msSmall = Math.max(1, performance.now() - t0)
    const large = gen(160_000)
    redactText(large.slice(0, 100), newStats())
    const t1 = performance.now()
    const outLarge = redactText(large, newStats())
    const msLarge = performance.now() - t1
    if (expectedSecret === 'NONE_EXPECTED') expect(outLarge).toBe(large)
    else expect(outLarge).not.toContain(expectedSecret)
    expect({
      label,
      withinBudget: msLarge < 250,
      linearScaling: msLarge <= Math.max(120, msSmall * 25),
    }).toEqual({ label, withinBudget: true, linearScaling: true })
  }

  // Documented JSON allowlist / compact re-serialization divergence:
  // `redactText()` and `buildRow()` match 100% on allowlisted compact JSON keys above,
  // whereas `buildRow()` parses JSON strings inside tool results, enforces the key
  // allowlist (redacting unlisted keys' values whole), and re-serializes compactly.
  for (const [sample, expectedText, forbidden, rowResultExpected, expectedBuildRedactions] of [
    ['{"error": "Rejected Bearer my_access_token_99: "}', '{"error": "Rejected Bearer [REDACTED]: "}', ['my_access_token_99'], '{"error":"Rejected Bearer [REDACTED]: "}', 1],
    ['{"e":"Bearer secret_token_value: denied","next":"ok"}', '{"e":"Bearer [REDACTED]: [REDACTED]","next":"ok"}', ['secret_token_value', 'denied'], '{"e":"[REDACTED]","next":"[REDACTED]"}', 2],
    ['"{\\"note\\": \\"Bearer mySecretTokenAb=\\", \\"id\\": 123}"', '"{\\"note\\": \\"Bearer [REDACTED]\\", \\"id\\": 123}"', ['mySecretTokenAb='], '"{\\"note\\":\\"[REDACTED]\\",\\"id\\":\\"[REDACTED]\\"}"', 2],
  ] as const) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).toBe(expectedText)
    for (const part of forbidden) expect(out).not.toContain(part)
    expect(stats.redactions).toBe(1)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    for (const part of forbidden) expect(serialized).not.toContain(part)
    expect((JSON.parse(built.row.content ?? '{}') as { result: string }).result).toBe(rowResultExpected)
    expect(built.redactions).toBe(expectedBuildRedactions)
  }
})

test('lineStopFor and lineEnd scale linearly (1,600 vs 8,000 lines) with distant CR/LS and many KV pairs', () => {
  for (const [label, gen, expectedGen] of [
    [
      'lineStopFor Bearer client_secret + trailing CR',
      (n: number) => 'Bearer client_secret: x\n'.repeat(n) + '\r',
      (n: number) => 'Bearer [REDACTED]: [REDACTED]\n'.repeat(n) + '\r',
    ],
    [
      'lineStopFor Bearer client_secret + trailing LS',
      (n: number) => 'Bearer client_secret: x\n'.repeat(n) + '\u2028',
      (n: number) => 'Bearer [REDACTED]: [REDACTED]\n'.repeat(n) + '\u2028',
    ],
    [
      'lineEnd password KV pairs',
      (n: number) => 'password: x\n'.repeat(n),
      (n: number) => 'password: [REDACTED]\n'.repeat(n),
    ],
    [
      'lineEnd password KV pairs + trailing LS',
      (n: number) => 'password: x\n'.repeat(n) + '\u2028',
      (n: number) => 'password: [REDACTED]\n'.repeat(n) + '\u2028',
    ],
  ] as const) {
    const small = gen(1_600)
    redactText(small, newStats())
    const t0 = performance.now()
    redactText(small, newStats())
    const msSmall = Math.max(1, performance.now() - t0)

    const large = gen(8_000)
    redactText(large.slice(0, 400), newStats())
    const statsLarge = newStats()
    const t1 = performance.now()
    const outLarge = redactText(large, statsLarge)
    const msLarge = performance.now() - t1

    expect(outLarge === expectedGen(8_000)).toBe(true)
    expect(statsLarge.redactions).toBe(8_000)
    expect({
      label,
      withinBudget: msLarge < 250,
      linearScaling: msLarge <= Math.max(50, msSmall * 15),
    }).toEqual({ label, withinBudget: true, linearScaling: true })
  }
})

test('timeout-wrapped CLI commands redact embedded credentials across duration and signal options', () => {
  const secret = ['Fake', 'Secret', '123'].join('')
  const cases = [
    [`timeout 5 mysql -p${secret}`, 'timeout 5 mysql -p[REDACTED]'],
    [`gtimeout 5 mysql -p${secret}`, 'gtimeout 5 mysql -p[REDACTED]'],
    [`timeout 5s mysql -p${secret}`, 'timeout 5s mysql -p[REDACTED]'],
    [`timeout 5 redis-cli -a ${secret}`, 'timeout 5 redis-cli -a [REDACTED]'],
    [`env timeout 5 mysql -p${secret}`, 'env timeout 5 mysql -p[REDACTED]'],
    [`sudo -u app timeout 5 mysql -p${secret}`, 'sudo -u app timeout 5 mysql -p[REDACTED]'],
    [`env -u HOME timeout 5 redis-cli -a ${secret}`, 'env -u HOME timeout 5 redis-cli -a [REDACTED]'],
    [`time timeout 5 mysql -p${secret}`, 'time timeout 5 mysql -p[REDACTED]'],
    [`timeout -k 2s 5s mysql -p${secret}`, 'timeout -k 2s 5s mysql -p[REDACTED]'],
    [`timeout --signal KILL 0.5m mysql -p${secret}`, 'timeout --signal KILL 0.5m mysql -p[REDACTED]'],
  ] as const
  for (const [cmd, expected] of cases) {
    const stats = newStats()
    const out = redactText(cmd, stats)
    expect(out).not.toContain(secret)
    expect(out).toBe(expected)
    expect(stats.redactions).toBe(1)

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
      content: { command: cmd },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serialized = JSON.stringify(built.row)
    expect(serialized).not.toContain(secret)
    expect(serialized).toContain(expected)
    expect(built.redactions).toBe(1)
  }

  const safeStats = newStats()
  const safeCmd = 'timeout 5 psql -p 5432 -U postgres'
  expect(redactText(safeCmd, safeStats)).toBe(safeCmd)
  expect(safeStats.redactions).toBe(0)
})

test('PuTTY .ppk scanner scales linearly over repeated headers and still redacts trailing real keys', () => {
  const warmFlood = 'PuTTY-User-Key-File-3: x\n'.repeat(4_200)
  const w0 = performance.now()
  redactText(warmFlood, newStats())
  const redactSmallMs = Math.max(1, performance.now() - w0)
  const w1 = performance.now()
  buildRow({
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
    content: { result: warmFlood },
    attributes: {},
    maxPayloadChars: 500_000,
  })
  const buildSmallMs = Math.max(1, performance.now() - w1)

  const flood = 'PuTTY-User-Key-File-3: x\n'.repeat(21_000)

  const t0 = performance.now()
  const floodStats = newStats()
  const floodOut = redactText(flood, floodStats)
  const redactMs = performance.now() - t0
  expect(redactMs).toBeLessThan(250)
  expect(redactMs <= Math.max(120, redactSmallMs * 12)).toBe(true)
  expect(floodOut === 'PuTTY-User-Key-File-3: [REDACTED]\n'.repeat(21_000)).toBe(true)
  expect(floodStats.redactions).toBe(21_000)

  const t1 = performance.now()
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
    content: { result: flood },
    attributes: {},
    maxPayloadChars: 500_000,
  })
  const buildMs = performance.now() - t1
  expect(buildMs).toBeLessThan(250)
  expect(buildMs <= Math.max(120, buildSmallMs * 12)).toBe(true)
  expect(built.redactions).toBeGreaterThan(20_000)
  expect(built.row.is_truncated).toBe(true)

  const secret = ['ppk', 'trail', 'secret', '009'].join('-')
  for (const eol of ['\n', '\r\n'] as const) {
    const prefix = `PuTTY-User-Key-File-3: x${eol}`.repeat(10_000)
    const expectedPrefix = `PuTTY-User-Key-File-3: [REDACTED]${eol}`.repeat(10_000)
    const realKey = [
      'PuTTY-User-Key-File-2: ssh-rsa',
      'Private-Lines: 2',
      `${secret}Line1`,
      `${secret}Line2`,
      'Private-MAC: deadbeef',
    ].join(eol)
    const stats = newStats()
    const t2 = performance.now()
    const out = redactText(prefix + realKey, stats)
    expect(performance.now() - t2).toBeLessThan(250)
    expect(out).not.toContain(secret)
    expect(out === `${expectedPrefix}[REDACTED:private-key]`).toBe(true)
    expect(stats.redactions).toBe(10_001)
  }
})

test('cross-line Bearer/Basic prose and empty-assignment regressions redact in redactText and buildRow content and error_message', () => {
  const redactFixtures: readonly (readonly [string, string, string])[] = [
    ['Bearer\nReviewFixtureOnly more', 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Bearer\nReviewFixtureOnly expired', 'Bearer [REDACTED] expired', 'ReviewFixtureOnly'],
    ['Bearer\nReviewFixtureOnly request rejected', 'Bearer [REDACTED] request rejected', 'ReviewFixtureOnly'],
    ['Bearer\u00a0ReviewFixtureOnly more', 'Bearer [REDACTED] more', 'ReviewFixtureOnly'],
    ['Bearer\nConfiguration', 'Bearer [REDACTED]', 'Configuration'],
    ['Bearer\nsecret_token_value=', 'Bearer [REDACTED]', 'secret_token_value='],
    ['Bearer\nreview_fixture9472=', 'Bearer [REDACTED]', 'review_fixture9472='],
    ['Bearer\nreviewFixtureOnly=', 'Bearer [REDACTED]', 'reviewFixtureOnly='],
    ['Bearer\nsecret_token_value=""', 'Bearer [REDACTED]""', 'secret_token_value='],
    ["Bearer\nreviewFixtureOnly=''", "Bearer [REDACTED]''", 'reviewFixtureOnly='],
    ['Bearer\nsecret_token_value=!', 'Bearer [REDACTED]!', 'secret_token_value='],
    ['Bearer\nsecret_token_value= expired', 'Bearer [REDACTED] expired', 'secret_token_value='],
    ['Basic\nsecret_token_value=', 'Basic [REDACTED]', 'secret_token_value='],
    ['Basic\nreview_fixture9472=', 'Basic [REDACTED]', 'review_fixture9472='],
    ['Basic\nreviewFixtureOnly=', 'Basic [REDACTED]', 'reviewFixtureOnly='],
    ['# Bearer\nsecret_token_value=', '# Bearer [REDACTED]', 'secret_token_value='],
    ['error: Bearer\nsecret_token_value=', 'error: Bearer [REDACTED]', 'secret_token_value='],
    ['error: Bearer\nsecret_token_value:', 'error: Bearer [REDACTED]:', 'secret_token_value'],
    ['x: Bearer\nsecret_token_value:', 'x: Bearer [REDACTED]:', 'secret_token_value'],
    ['error: Bearer\nsecret_token_value: "request rejected"', 'error: Bearer [REDACTED]: [REDACTED]', 'secret_token_value'],
    ['error: Bearer\nreview_fixture_only:', 'error: Bearer [REDACTED]:', 'review_fixture_only'],
    ['Bearer\nreviewFixtureOnly=...', 'Bearer [REDACTED]', 'reviewFixtureOnly='],
    ['Bearer\nCURRENT_WORK_DIR=', 'Bearer [REDACTED]', 'CURRENT_WORK_DIR='],
    ['Bearer\npublic_work_dirs=', 'Bearer [REDACTED]', 'public_work_dirs='],
    ['Bearer\n--config-file=', 'Bearer [REDACTED]', '--config-file='],
  ]

  for (const [sample, expected, token] of redactFixtures) {
    const stats = newStats()
    const out = redactText(sample, stats)
    expect(out).toBe(expected)
    expect(out).not.toContain(token)
    expect(out).toContain('[REDACTED]')
    expect(stats.redactions).toBeGreaterThan(0)

    const reStats = newStats()
    expect(redactText(out, reStats)).toBe(out)
    expect(reStats.redactions).toBe(0)

    const builtContent = buildRow({
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
      content: { result: sample },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    const serializedContent = builtContent.row.content ?? ''
    expect(serializedContent).not.toContain(token)
    expect(serializedContent).toContain('[REDACTED]')
    expect((JSON.parse(serializedContent) as { result: string }).result).toBe(expected)
    expect(builtContent.redactions).toBeGreaterThan(0)

    const builtError = buildRow({
      eventType: 'TOOL_ERROR',
      timestampMs: 0,
      eventId: 'e2',
      agent: 'a',
      sessionId: 's',
      invocationId: 'i',
      userId: 'u',
      traceId: 't',
      spanId: 'p',
      parentSpanId: null,
      content: null,
      attributes: {},
      status: 'ERROR',
      errorMessage: sample,
      maxPayloadChars: 16_384,
    })
    const errOut = builtError.row.error_message ?? ''
    expect(errOut).toBe(expected)
    expect(errOut).not.toContain(token)
    expect(errOut).toContain('[REDACTED]')
    expect(builtError.redactions).toBeGreaterThan(0)
  }

  const controlFixtures: readonly string[] = [
    'MODE=bearer\nCLIENT_SECRET=',
    'MODE=bearer\npublicvalue=',
    'MODE=bearer\nENVIRONMENT=',
    'MODE=bearer\nDB_PASSWORD=',
    'MODE=bearer\npublic_name=',
    'MODE=bearer\nEMPTY_VAR=""',
    "MODE=bearer\nEMPTY_VAR=''",
    'MODE=bearer\nschemaV2Config=',
    'MODE=bearer\n--config-file=',
    'MODE=bearer\nCURRENT_WORK_DIR=',
    'MODE=bearer\npublic_work_dirs=',
    'MODE=basic\nusername=',
    'MODE=basic\nCLIENT_SECRET=',
    'Bearer\nConfiguration details follow',
    'Authentication scheme: Bearer\nConfiguration details follow',
    'Bearer\u00a0ConfigurationWord',
    'Bearer\u00a0AuthenticationGuide',
    'Bearer\u00a0Configuration details follow',
    'Basic\nconfiguration guide',
  ]

  for (const control of controlFixtures) {
    const stats = newStats()
    expect(redactText(control, stats)).toBe(control)
    expect(stats.redactions).toBe(0)

    const builtContent = buildRow({
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
      content: { result: control },
      attributes: {},
      maxPayloadChars: 16_384,
    })
    expect((JSON.parse(builtContent.row.content ?? '{}') as { result: string }).result).toBe(control)
    expect(builtContent.redactions).toBe(0)

    const builtError = buildRow({
      eventType: 'TOOL_ERROR',
      timestampMs: 0,
      eventId: 'e2',
      agent: 'a',
      sessionId: 's',
      invocationId: 'i',
      userId: 'u',
      traceId: 't',
      spanId: 'p',
      parentSpanId: null,
      content: null,
      attributes: {},
      status: 'ERROR',
      errorMessage: control,
      maxPayloadChars: 16_384,
    })
    expect(builtError.row.error_message).toBe(control)
    expect(builtError.redactions).toBe(0)
  }
})
