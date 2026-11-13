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

// Self-check for the offline `tests/shim/claude-code.mjs` test harness.
// Run with `node tests/shim/shim-self.check.mjs` from `plugins/claude_code_mod`.

import assert from 'node:assert/strict'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { createTestHarness, validateUserConfigOptions } from './claude-code.mjs'

const modRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..')

async function runChecks() {
  // 1. session.end without { sessionId } is rejected.
  {
    const { testEngine, testOn, clock } = createTestHarness(modRoot)
    try {
      testOn('session.end', async () => ({}))
      await assert.rejects(
        () => testEngine.session.end({ reason: 'other', sessionId: 's1', resume: { id: 's1' } }),
        /no implementation for session\.end: returned no \{ sessionId \}/,
      )
    } finally {
      clock.cancelAll()
    }
  }

  // 2. Duplicate on() listener detection respects exact (event, filter) pairs.
  {
    const { pluginOn, testOn, clock } = createTestHarness(modRoot)
    try {
      pluginOn('session.end', async () => ({ sessionId: 's1' }))
      assert.throws(() => pluginOn('session.end', async () => ({ sessionId: 's2' })), /on\("session\.end"\) registered twice/)

      testOn('env.get', () => ({ value: 'a' }))
      assert.throws(() => testOn('env.get', () => ({ value: 'b' })), /on\("env\.get"\) registered twice/)
      // Distinct filtered hooks do not collide with unfiltered or different filters.
      testOn('env.get', { name: 'BQAA_DATASET' }, () => ({ value: 'ds' }))
      testOn('env.get', { name: 'BQAA_PROJECT_ID' }, () => ({ value: 'proj' }))
      assert.throws(() => testOn('env.get', { name: 'BQAA_DATASET' }, () => ({ value: 'ds2' })), /on\("env\.get"\) registered twice/)
    } finally {
      clock.cancelAll()
    }
  }

  // 3. userConfig schema validation matches .claude-plugin/plugin.json types.
  validateUserConfigOptions(modRoot, { project: 'p', dataset: 'd', enabled: true, dryRun: false, maxPayloadChars: 2048, extraUnknownKey: 'ok' })
  assert.throws(() => validateUserConfigOptions(modRoot, { dryRun: 'false' }), /invalid userConfig option "dryRun": expected boolean/)
  assert.throws(() => validateUserConfigOptions(modRoot, { enabled: 'false' }), /invalid userConfig option "enabled": expected boolean/)
  assert.throws(() => validateUserConfigOptions(modRoot, { flushIntervalMs: '1000' }), /invalid userConfig option "flushIntervalMs": expected number/)
  assert.throws(() => validateUserConfigOptions(modRoot, { maxPayloadChars: Number.NaN }), /invalid userConfig option "maxPayloadChars": expected number/)
  assert.throws(() => validateUserConfigOptions(modRoot, { dataset: 123 }), /invalid userConfig option "dataset": expected string/)

  // 4. TestEngine hides plugin-only runtime APIs while plugin Engine exposes them.
  {
    const { $: pluginEngine, testEngine, pluginOn, clock } = createTestHarness(modRoot)
    try {
      assert.equal('clock' in testEngine, false)
      assert.equal('fs' in testEngine, false)
      assert.equal('store' in testEngine, false)
      assert.equal('http' in testEngine, false)
      assert.equal('process' in testEngine, false)
      assert.equal('status' in testEngine.ui, false)
      assert.equal(typeof pluginEngine.clock.after, 'function')

      // 5. command.run provides a 10 s budget and rejects runs exceeding it.
      let seenBudget = null
      pluginOn('command.run', { command: 'fast' }, async ($, _e, next) => {
        seenBudget = next?.budget?.remainingMs ?? null
        await $.clock.sleep(8_000)
        return { text: 'fast-ok' }
      })
      pluginOn('command.run', { command: 'slow' }, async ($, _e, next) => {
        seenBudget = next?.budget?.remainingMs ?? null
        await $.clock.sleep(15_000)
        return { text: 'too-late' }
      })

      const fastRun = testEngine.command.run({ command: 'fast', args: '' })
      await clock.advance(8_000)
      assert.deepEqual(await fastRun, { text: 'fast-ok' })
      assert.equal(seenBudget, 10_000)

      const slowRun = testEngine.command.run({ command: 'slow', args: '' })
      const slowOutcome = slowRun.then(
        () => null,
        err => err,
      )
      await clock.advance(15_000)
      const slowErr = await slowOutcome
      assert.match(slowErr?.message ?? '', /command\.run hook skipped: ran past its 10s budget/)
    } finally {
      clock.cancelAll()
    }
  }

  console.log('ok - shim self-checks passed')
}

await runChecks()
