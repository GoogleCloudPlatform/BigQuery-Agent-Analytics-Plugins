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

// Hermetic Node 24 runtime shim for `claude-code` and `claude-code/testing`.
// Implements `test`, `expect`, `mock` (`clock`, `env`), the host `Engine` (`$`),
// hook middleware chaining (`on`), dispatch-isolated `$.state` snapshots, and
// `$.ui.mount` so all 14 TypeScript test suites run offline in Node 24.

import assert from 'node:assert/strict'
import { AsyncLocalStorage } from 'node:async_hooks'
import fs from 'node:fs'
import path from 'node:path'
import { test as nodeTest } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'

const dispatchStorage = new AsyncLocalStorage()

function isPlainObject(v) {
  if (typeof v !== 'object' || v === null) return false
  const proto = Object.getPrototypeOf(v)
  return proto === Object.prototype || proto === null
}

function deepEqual(a, b) {
  if (b && typeof b === 'object' && typeof b.__asymmetricMatch === 'function') {
    return b.__asymmetricMatch(a)
  }
  if (a && typeof a === 'object' && typeof a.__asymmetricMatch === 'function') {
    return a.__asymmetricMatch(b)
  }
  if (Object.is(a, b)) return true
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime()
  if (a instanceof RegExp && b instanceof RegExp) return a.source === b.source && a.flags === b.flags
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i += 1) {
      if (!deepEqual(a[i], b[i])) return false
    }
    return true
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const keysA = Object.keys(a).filter(k => a[k] !== undefined)
    const keysB = Object.keys(b).filter(k => b[k] !== undefined)
    if (keysA.length !== keysB.length) return false
    for (const k of keysA) {
      if (!Object.prototype.hasOwnProperty.call(b, k) || b[k] === undefined) return false
      if (!deepEqual(a[k], b[k])) return false
    }
    return true
  }
  try {
    assert.deepStrictEqual(a, b)
    return true
  } catch {
    return false
  }
}

function matchesSubset(actual, subset) {
  if (Object.is(actual, subset)) return true
  if (typeof actual !== 'object' || actual === null || typeof subset !== 'object' || subset === null) {
    return deepEqual(actual, subset)
  }
  if (subset instanceof Date || subset instanceof RegExp) {
    return deepEqual(actual, subset)
  }
  if (Array.isArray(subset)) {
    if (!Array.isArray(actual) || actual.length !== subset.length) return false
    for (let i = 0; i < subset.length; i += 1) {
      if (!matchesSubset(actual[i], subset[i])) return false
    }
    return true
  }
  for (const key of Object.keys(subset)) {
    if (subset[key] === undefined) {
      if (actual[key] !== undefined) return false
      continue
    }
    if (!(key in actual) || !matchesSubset(actual[key], subset[key])) return false
  }
  return true
}

function createMatchers(actual, negated = false) {
  const check = (pass, message) => {
    const ok = negated ? !pass : pass
    if (!ok) {
      throw new assert.AssertionError({
        message,
        actual,
        operator: negated ? 'not' : 'expect',
      })
    }
  }
  const matchers = {
    toBe(expected) {
      check(Object.is(actual, expected), `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to be ${JSON.stringify(expected)}`)
    },
    toEqual(expected) {
      check(deepEqual(actual, expected), `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to equal ${JSON.stringify(expected)}`)
    },
    toMatchObject(expected) {
      check(matchesSubset(actual, expected), `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to match object ${JSON.stringify(expected)}`)
    },
    toBeDefined() {
      check(actual !== undefined, `Expected value ${negated ? 'not ' : ''}to be defined`)
    },
    toBeUndefined() {
      check(actual === undefined, `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to be undefined`)
    },
    toBeNull() {
      check(actual === null, `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to be null`)
    },
    toBeTruthy() {
      check(Boolean(actual), `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to be truthy`)
    },
    toBeFalsy() {
      check(!actual, `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to be falsy`)
    },
    toBeGreaterThan(expected) {
      check(actual > expected, `Expected ${actual} ${negated ? 'not ' : ''}to be > ${expected}`)
    },
    toBeGreaterThanOrEqual(expected) {
      check(actual >= expected, `Expected ${actual} ${negated ? 'not ' : ''}to be >= ${expected}`)
    },
    toBeLessThan(expected) {
      check(actual < expected, `Expected ${actual} ${negated ? 'not ' : ''}to be < ${expected}`)
    },
    toBeLessThanOrEqual(expected) {
      check(actual <= expected, `Expected ${actual} ${negated ? 'not ' : ''}to be <= ${expected}`)
    },
    toHaveLength(expected) {
      check(actual?.length === expected, `Expected length ${actual?.length} ${negated ? 'not ' : ''}to be ${expected}`)
    },
    toContain(item) {
      const pass = typeof actual === 'string' || Array.isArray(actual) ? actual.includes(item) : false
      check(pass, `Expected ${JSON.stringify(actual)} ${negated ? 'not ' : ''}to contain ${JSON.stringify(item)}`)
    },
    toMatch(pattern) {
      const str = String(actual ?? '')
      const pass = pattern instanceof RegExp ? pattern.test(str) : str.includes(String(pattern))
      check(pass, `Expected ${JSON.stringify(str)} ${negated ? 'not ' : ''}to match ${String(pattern)}`)
    },
    toThrow(expected) {
      let threw = false
      let err
      try {
        actual()
      } catch (e) {
        threw = true
        err = e
      }
      if (!threw) {
        check(false, 'Expected function to throw')
        return
      }
      if (expected !== undefined) {
        const msg = err instanceof Error ? err.message : String(err)
        const pass = expected instanceof RegExp ? expected.test(msg) : msg.includes(String(expected))
        check(pass, `Expected thrown error ${JSON.stringify(msg)} ${negated ? 'not ' : ''}to match ${String(expected)}`)
      } else {
        check(true, 'Expected function not to throw')
      }
    },
  }
  if (!negated) {
    Object.defineProperty(matchers, 'not', {
      get: () => createMatchers(actual, true),
    })
  }
  return matchers
}

export function expect(actual) {
  return createMatchers(actual, false)
}

expect.stringMatching = pattern => ({
  __asymmetricMatch(actual) {
    if (typeof actual !== 'string') return false
    return pattern instanceof RegExp ? pattern.test(actual) : actual.includes(String(pattern))
  },
})

function matchesFilter(event, filter) {
  if (!filter) return true
  if (typeof event !== 'object' || event === null) return false
  for (const [k, v] of Object.entries(filter)) {
    if (event[k] !== v) return false
  }
  return true
}

function flattenText(children) {
  if (children === null || children === undefined || typeof children === 'boolean') return ''
  if (typeof children === 'string' || typeof children === 'number') return String(children)
  if (Array.isArray(children)) return children.map(flattenText).join('')
  if (typeof children === 'object' && children.props) return flattenText(children.props.children)
  return ''
}

function collectNodes(node, out = []) {
  if (!node || typeof node !== 'object') return out
  if (Array.isArray(node)) {
    for (const child of node) collectNodes(child, out)
    return out
  }
  if (typeof node.type === 'string') {
    const props = node.props ?? {}
    const rawChildren = props.children !== undefined ? props.children : node.children
    const entry = {
      type: node.type,
      props,
      key: props.key ?? node.key,
      text: node.type === 'Text' ? flattenText(rawChildren) : undefined,
    }
    out.push(entry)
    if (Array.isArray(rawChildren)) {
      for (const c of rawChildren) collectNodes(c, out)
    } else if (rawChildren && typeof rawChildren === 'object') {
      collectNodes(rawChildren, out)
    }
  }
  return out
}

function findCallerPluginRoot() {
  const stack = new Error().stack ?? ''
  for (const line of stack.split('\n')) {
    const m = line.match(/file:\/\/([^\s):]+)/)
    if (!m) continue
    const filePath = fileURLToPath(`file://${m[1]}`)
    if (filePath.includes('/tests/shim/')) continue
    let dir = path.dirname(filePath)
    while (dir !== path.dirname(dir)) {
      const candidate = path.join(dir, 'hooks', 'register.ts')
      if (fs.existsSync(candidate)) {
        return { root: dir, registerUrl: pathToFileURL(candidate).href }
      }
      dir = path.dirname(dir)
    }
  }
  const fallbackRoot = fileURLToPath(new URL('../..', import.meta.url)).replace(/\/$/, '')
  return {
    root: fallbackRoot,
    registerUrl: new URL('../../hooks/register.ts', import.meta.url).href,
  }
}

function createVirtualClock(initialNow = 0) {
  let currentNow = initialNow
  let nextTimerId = 1
  const timers = []

  const drainMicrotasks = async (steps = 6) => {
    for (let i = 0; i < steps; i += 1) {
      await new Promise(resolve => setImmediate(resolve))
    }
  }

  const addTimer = (ms, fn, interval = null) => {
    const delay = Math.max(0, Number.isFinite(ms) ? ms : 0)
    const entry = {
      id: nextTimerId++,
      at: currentNow + (interval !== null ? Math.max(1, delay) : delay),
      interval: interval !== null ? Math.max(1, delay) : null,
      fn,
      cancelled: false,
    }
    timers.push(entry)
    return {
      cancel() {
        entry.cancelled = true
      },
    }
  }

  const nextDueTimer = maxTime => {
    let best = null
    let bestIdx = -1
    for (let i = 0; i < timers.length; i += 1) {
      const t = timers[i]
      if (t.cancelled) continue
      if (t.at <= maxTime) {
        if (best === null || t.at < best.at || (t.at === best.at && t.id < best.id)) {
          best = t
          bestIdx = i
        }
      }
    }
    if (bestIdx !== -1 && best.interval === null) {
      timers.splice(bestIdx, 1)
    }
    return best
  }

  const settle = async () => {
    await drainMicrotasks(6)
    let safety = 0
    while (safety++ < 500) {
      const due = nextDueTimer(currentNow)
      if (!due) break
      if (due.cancelled) continue
      if (due.interval !== null) {
        due.at = currentNow + due.interval
      }
      dispatchStorage.run({ moment: null }, () => due.fn())
      await drainMicrotasks(6)
    }
  }

  const advance = async ms => {
    const delta = Math.max(0, Number.isFinite(ms) ? ms : 0)
    const target = currentNow + delta
    await settle()
    let safety = 0
    while (safety++ < 2000) {
      const due = nextDueTimer(target)
      if (!due) break
      if (due.cancelled) continue
      if (due.at > currentNow) currentNow = due.at
      if (due.interval !== null) {
        due.at = currentNow + due.interval
      }
      dispatchStorage.run({ moment: null }, () => due.fn())
      await settle()
    }
    currentNow = target
    await settle()
  }

  const sleep = ms =>
    new Promise(resolve => {
      const store = dispatchStorage.getStore()
      addTimer(
        ms,
        () => {
          if (store) store.moment = null
          resolve()
        },
        null,
      )
    })

  const cancelAll = () => {
    for (const t of timers) t.cancelled = true
    timers.length = 0
  }

  return {
    now: () => currentNow,
    setNow: v => {
      currentNow = v
    },
    after: (ms, fn) => addTimer(ms, fn, null),
    every: (ms, fn) => addTimer(ms, fn, ms),
    sleep,
    settle,
    advance,
    cancelAll,
  }
}

export const mock = {
  // Controls the host virtual timer clock (`$.clock.after`, `$.clock.every`,
  // `$.clock.sleep`) while intentionally leaving `Date.now()` on real wall-clock
  // time, as required by upstream tests:
  //   - `tests/decisions.test.ts:69-73` spins `while (Date.now() < spinUntil)`
  //     after `await world.clock.advance(1000)` to wait out 15 ms of real time;
  //   - `tests/destination.test.ts:226-227` notes `Io.now, Date.now in a
  //     session, which the mock clock does not move`;
  //   - `tests/idle-replay.test.ts:490-491` notes `the test clock leaves
  //     Date.now alone`.
  clock(_on, opts = {}) {
    const active = currentTestClock
    if (!active) {
      return createVirtualClock(opts.now ?? 0)
    }
    if (typeof opts.now === 'number') {
      active.setNow(opts.now)
    }
    return {
      now: () => active.now(),
      advance: ms => active.advance(ms),
      settle: () => active.settle(),
      sleep: ms => active.sleep(ms),
    }
  },
  env(on, vars = {}) {
    on('env.get', (_$, e) => ({
      value: Object.prototype.hasOwnProperty.call(vars, e.name) ? vars[e.name] : undefined,
    }))
  },
}

let currentTestClock = null
let testCounter = 0

const userConfigCache = new Map()

function loadUserConfig(pluginRoot) {
  if (!pluginRoot) return null
  if (userConfigCache.has(pluginRoot)) return userConfigCache.get(pluginRoot)
  const manifestPath = path.join(pluginRoot, '.claude-plugin', 'plugin.json')
  let schema = null
  if (fs.existsSync(manifestPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
      if (parsed && typeof parsed.userConfig === 'object' && parsed.userConfig !== null) {
        schema = parsed.userConfig
      }
    } catch {
      schema = null
    }
  }
  userConfigCache.set(pluginRoot, schema)
  return schema
}

export function validateUserConfigOptions(pluginRoot, opts) {
  const schema = loadUserConfig(pluginRoot)
  if (!schema || !opts) return
  for (const [key, value] of Object.entries(opts)) {
    if (value === undefined || !Object.prototype.hasOwnProperty.call(schema, key)) continue
    const expected = schema[key]?.type
    if (expected === 'boolean' && typeof value !== 'boolean') {
      throw new Error(`invalid userConfig option "${key}": expected boolean, got ${typeof value}`)
    }
    if (expected === 'number' && (typeof value !== 'number' || !Number.isFinite(value))) {
      throw new Error(`invalid userConfig option "${key}": expected number, got ${typeof value}`)
    }
    if (expected === 'string' && typeof value !== 'string') {
      throw new Error(`invalid userConfig option "${key}": expected string, got ${typeof value}`)
    }
  }
}

function canonicalFilter(filter) {
  if (filter === null || filter === undefined) return 'null'
  const keys = Object.keys(filter)
    .filter(k => filter[k] !== undefined)
    .sort()
  return JSON.stringify(keys.map(k => [k, filter[k]]))
}

export function createTestHarness(pluginRoot) {
  const clock = createVirtualClock(0)
  const pluginHooks = []
  const testMiddlewareHooks = []
  const testFilteredHooks = []
  const testUnfilteredHooks = []
  const stateStore = new Map()
  const mountedUis = new Set()

  const registerHook = (bucket, conflictBuckets, eventName, arg2, arg3) => {
    const filter = typeof arg2 === 'function' ? null : arg2
    const handler = typeof arg2 === 'function' ? arg2 : arg3
    const filterKey = canonicalFilter(filter)
    for (const existingBucket of conflictBuckets) {
      if (existingBucket.some(h => h.eventName === eventName && h.filterKey === filterKey)) {
        throw new Error(`on("${eventName}") registered twice`)
      }
    }
    const entry = { eventName, filter, filterKey, handler, catchHandler: null }
    bucket.push(entry)
    return {
      catch(fn) {
        entry.catchHandler = fn
      },
    }
  }

  const pluginOn = (eventName, arg2, arg3) => registerHook(pluginHooks, [pluginHooks], eventName, arg2, arg3)
  const testOn = (eventName, arg2, arg3) => {
    const hasFilter = typeof arg2 !== 'function' && arg2 !== null && arg2 !== undefined
    const handler = typeof arg2 === 'function' ? arg2 : arg3
    const conflictBuckets = [testMiddlewareHooks, testFilteredHooks, testUnfilteredHooks]
    if (typeof handler === 'function' && handler.length >= 3) {
      return registerHook(testMiddlewareHooks, conflictBuckets, eventName, arg2, arg3)
    }
    if (hasFilter) {
      return registerHook(testFilteredHooks, conflictBuckets, eventName, arg2, arg3)
    }
    return registerHook(testUnfilteredHooks, conflictBuckets, eventName, arg2, arg3)
  }

  const defaultTerminal = async (eventName, e) => {
    switch (eventName) {
      case 'env.get':
      case 'store.get':
      case 'store.set':
      case 'store.delete':
      case 'fs.write':
      case 'telemetry.log':
      case 'command.register':
        return { value: undefined }
      case 'store.keys':
      case 'fs.list':
        return { value: [] }
      case 'fs.read':
        return { value: '' }
      case 'fs.exists':
        return { value: false }
      case 'session.id':
        return { value: 'default-session-id' }
      case 'session.version':
        return { value: { version: '1.0.0' } }
      case 'process.run':
        return { value: { exitCode: 0, stdout: '', stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
      case 'http.fetch':
        return { value: { status: 200, statusText: 'OK', headers: {}, text: '{}' } }
      case 'prompt.submit':
        return { text: e.text }
      case 'tool.call':
        return { result: { type: 'text' }, text: 'ok' }
      case 'ui.render':
        return null
      case 'command.run':
        return { text: '' }
      case 'session.end':
        return {}
      case 'state.set': {
        const k = `${e.plugin}:${e.key}`
        const current = stateStore.get(k) ?? { value: undefined, version: 0 }
        const expected = e.options?.ifVersion
        if (expected !== undefined && expected !== current.version) {
          const at = dispatchStorage.getStore()
          if (at) at.moment = structuredClone(stateStore)
          return { value: { version: current.version, isSet: false } }
        }
        const nextVersion = current.version + 1
        stateStore.set(k, { value: structuredClone(e.value), version: nextVersion })
        const at = dispatchStorage.getStore()
        if (at) at.moment = structuredClone(stateStore)
        return { value: { version: nextVersion, isSet: true } }
      }
      default:
        return e
    }
  }

  const orderedChain = (eventName, currentEvent) => {
    const all = [
      ...pluginHooks,
      ...testMiddlewareHooks,
      ...testFilteredHooks,
      ...testUnfilteredHooks,
    ]
    return all.filter(h => h.eventName === eventName && matchesFilter(currentEvent, h.filter))
  }

  const runEvent = async (eventName, initialEvent, extraNextProps = null) => {
    const chain = orderedChain(eventName, initialEvent)
    const invoke = async (pos, currentEvent) => {
      for (let i = pos; i < chain.length; i += 1) {
        const entry = chain[i]
        if (!matchesFilter(currentEvent, entry.filter)) continue
        const next = async nextEvent => {
          await Promise.resolve()
          return invoke(i + 1, nextEvent !== undefined ? nextEvent : currentEvent)
        }
        if (extraNextProps) Object.assign(next, extraNextProps)
        try {
          return await entry.handler($, currentEvent, next)
        } catch (err) {
          if (entry.catchHandler) {
            const recoveryNext = nextEvent => invoke(i + 1, nextEvent !== undefined ? nextEvent : currentEvent)
            if (extraNextProps) Object.assign(recoveryNext, extraNextProps)
            return await entry.catchHandler($, currentEvent, recoveryNext, err)
          }
          throw err
        }
      }
      return defaultTerminal(eventName, currentEvent)
    }
    return invoke(0, initialEvent)
  }

  const runTopDispatch = (eventName, initialEvent, extraNextProps = null) =>
    dispatchStorage.run({ moment: null }, () => runEvent(eventName, initialEvent, extraNextProps))

  const unwrapEnvelope = async (eventName, payload) => {
    const res = await runEvent(eventName, payload)
    if (res && typeof res === 'object') {
      if ('deny' in res && res.deny !== undefined) {
        throw new Error(String(res.deny))
      }
      if ('value' in res) {
        return res.value
      }
    }
    return res
  }

  const resetSessionState = () => {
    stateStore.clear()
    const at = dispatchStorage.getStore()
    if (at) at.moment = null
  }

  const $ = {
    plugin: {
      root: pluginRoot,
    },
    env: {
      get: name => unwrapEnvelope('env.get', { name }),
    },
    store: {
      get: key => unwrapEnvelope('store.get', { key }),
      set: (key, value) => unwrapEnvelope('store.set', { key, value }),
      delete: key => unwrapEnvelope('store.delete', { key }),
      keys: () => unwrapEnvelope('store.keys', {}),
    },
    state: {
      get: async ref => {
        const k = `${ref.plugin}:${ref.key}`
        const at = dispatchStorage.getStore()
        if (at && !at.moment) {
          at.moment = structuredClone(stateStore)
        }
        const source = at?.moment ?? stateStore
        const hit = source.get(k) ?? { value: undefined, version: 0 }
        return { value: structuredClone(hit.value), version: hit.version }
      },
      set: (ref, value, options) =>
        unwrapEnvelope('state.set', { plugin: ref.plugin, key: ref.key, value, options }),
    },
    session: {
      id: () => unwrapEnvelope('session.id', {}),
      version: () => unwrapEnvelope('session.version', {}),
      start: e => runTopDispatch('session.start', e),
      end: async e => {
        if (e && (e.reason === 'clear' || e.reason === 'resume' || e.reason === 'fork')) {
          resetSessionState()
        }
        const result = await runTopDispatch('session.end', e, { budget: { remainingMs: 5000 } })
        if (typeof result?.sessionId !== 'string') {
          throw new Error('no implementation for session.end: returned no { sessionId }')
        }
        return result
      },
    },
    classic: {
      SessionStart: e => {
        if (e && (e.source === 'clear' || e.source === 'resume' || e.source === 'fork')) {
          resetSessionState()
        }
        return runTopDispatch('classic.SessionStart', e)
      },
      SubagentStart: e => runTopDispatch('classic.SubagentStart', e),
      PermissionRequest: e => runTopDispatch('classic.PermissionRequest', e),
    },
    prompt: {
      submit: e => runTopDispatch('prompt.submit', e),
    },
    turn: {
      start: e => runTopDispatch('turn.start', e),
      complete: e => runTopDispatch('turn.complete', e),
      step: e => {
        const chain = orderedChain('turn.step', e)
        const invokeStep = (pos, currentEvent) => {
          for (let i = pos; i < chain.length; i += 1) {
            const entry = chain[i]
            if (!matchesFilter(currentEvent, entry.filter)) continue
            const next = nextEvent => invokeStep(i + 1, nextEvent !== undefined ? nextEvent : currentEvent)
            return entry.handler($, currentEvent, next)
          }
          return (async function* () {
            return { answer: '', stopReason: 'end_turn' }
          })()
        }
        return invokeStep(0, e)
      },
    },
    tool: {
      call: e => runTopDispatch('tool.call', e),
    },
    telemetry: {
      log: e => runTopDispatch('telemetry.log', e),
    },
    command: {
      register: spec => unwrapEnvelope('command.register', spec),
      run: async (e, opts) => {
        const budgetMs = opts?.budgetMs ?? 10_000
        const startNow = clock.now()
        const result = await runTopDispatch('command.run', e, { budget: { remainingMs: budgetMs } })
        const elapsed = clock.now() - startNow
        if (elapsed > budgetMs) {
          const label = budgetMs === 10_000 ? '10s' : `${budgetMs}ms`
          throw new Error(`command.run hook skipped: ran past its ${label} budget (elapsed ${elapsed} ms)`)
        }
        return result
      },
    },
    clock: {
      after: (ms, fn) => clock.after(ms, fn),
      every: (ms, fn) => clock.every(ms, fn),
      sleep: ms => clock.sleep(ms),
    },
    http: {
      fetch: (url, init) => unwrapEnvelope('http.fetch', { url, init }),
    },
    process: {
      run: (argv, opts = {}) => unwrapEnvelope('process.run', { argv, ...opts }),
    },
    fs: {
      write: (filePath, text) => unwrapEnvelope('fs.write', { path: filePath, text }),
      read: filePath => unwrapEnvelope('fs.read', { path: filePath }),
      exists: filePath => unwrapEnvelope('fs.exists', { path: filePath }),
      list: filePath => unwrapEnvelope('fs.list', { path: filePath }),
    },
    ui: {
      status: text => {
        void runEvent('ui.status', { text })
      },
      log: (text, opts = {}) => {
        void runEvent('ui.log', { text, to: opts.to ?? 'transcript' })
      },
      toast: (text, opts = {}) => {
        void runEvent('ui.toast', { text, ...opts })
      },
      invalidate: target => {
        void runEvent('ui.invalidate', { target })
        for (const mounted of mountedUis) {
          mounted.dirty = true
        }
      },
      resolve: () => ({
        Box: props => ({ type: 'Box', props: props ?? {} }),
        Text: props => ({ type: 'Text', props: props ?? {} }),
        Button: props => ({ type: 'Button', props: props ?? {} }),
      }),
      mount: async opts => {
        const instance = {
          opts,
          tree: null,
          dirty: true,
          unmounted: false,
          async renderIfNeeded() {
            if (this.unmounted) return
            this.tree = await runTopDispatch('ui.render', opts)
            this.dirty = false
          },
          async find(query = {}) {
            await this.renderIfNeeded()
            const all = await this.findAll(query)
            return all[0]
          },
          async findAll(query = {}) {
            await this.renderIfNeeded()
            const nodes = collectNodes(this.tree)
            return nodes.filter(n => {
              if (query.type !== undefined && n.type !== query.type) return false
              if (query.key !== undefined && n.key !== query.key) return false
              if (query.text !== undefined) {
                const t = n.text ?? ''
                if (query.text instanceof RegExp) {
                  if (!query.text.test(t)) return false
                } else if (t !== query.text) {
                  return false
                }
              }
              return true
            })
          },
          async press(query = {}) {
            await this.renderIfNeeded()
            const nodes = collectNodes(this.tree)
            const button = nodes.find(
              n => n.type === 'Button' && (query.key === undefined || n.key === query.key),
            )
            if (button && typeof button.props?.onPress === 'function') {
              await dispatchStorage.run({ moment: null }, () => button.props.onPress())
            }
          },
          async unmount() {
            this.unmounted = true
            mountedUis.delete(this)
          },
        }
        mountedUis.add(instance)
        await instance.renderIfNeeded()
        return instance
      },
    },
  }

  const testEngine = {
    plugin: $.plugin,
    session: $.session,
    classic: $.classic,
    prompt: $.prompt,
    turn: $.turn,
    tool: $.tool,
    telemetry: $.telemetry,
    command: { run: $.command.run },
    ui: { mount: $.ui.mount },
  }

  return { $, testEngine, pluginOn, testOn, clock }
}

export function test(name, arg2, arg3) {
  const options = typeof arg2 === 'function' ? {} : (arg2 ?? {})
  const fn = typeof arg2 === 'function' ? arg2 : arg3
  const needsHarness = fn.length > 0
  const { root, registerUrl } = needsHarness ? findCallerPluginRoot() : { root: '', registerUrl: '' }

  nodeTest(name, async () => {
    if (!needsHarness) {
      await fn()
      return
    }
    const { testEngine, pluginOn, testOn, clock } = createTestHarness(root)
    const prevClock = currentTestClock
    currentTestClock = clock
    try {
      const modOptions = options.options ?? {}
      validateUserConfigOptions(root, modOptions)
      const id = ++testCounter
      const mod = await import(`${registerUrl}?test=${id}`)
      if (typeof mod.register !== 'function') {
        throw new Error(`Expected ${registerUrl} to export a register(on, options) function`)
      }
      mod.register(pluginOn, modOptions)
      await fn(testEngine, testOn)
    } finally {
      clock.cancelAll()
      currentTestClock = prevClock
    }
  })
}

export const on = () => ({ catch: () => undefined })
