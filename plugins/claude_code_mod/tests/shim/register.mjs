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

// Node 24 module loader hook for running the mod's TypeScript test suites in CI
// without external npm runtime dependencies. Maps `claude-code` and
// `claude-code/testing` to `./claude-code.mjs` and lets each `test()` load a
// fresh instance of `hooks/register.ts` via a query parameter (`?test=<id>`).

import { registerHooks } from 'node:module'

const shimUrl = new URL('./claude-code.mjs', import.meta.url).href

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'claude-code' || specifier === 'claude-code/testing') {
      return { url: shimUrl, shortCircuit: true }
    }
    return nextResolve(specifier, context)
  },
  load(url, context, nextLoad) {
    if (url.startsWith('file:')) {
      const parsed = new URL(url)
      if (parsed.pathname.endsWith('.ts') && parsed.search !== '') {
        return nextLoad(url, { ...context, format: 'module-typescript' })
      }
    }
    return nextLoad(url, context)
  },
})
