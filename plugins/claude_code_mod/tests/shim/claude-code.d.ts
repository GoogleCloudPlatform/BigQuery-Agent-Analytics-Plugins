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

// Ambient TypeScript declarations for `claude-code`, `claude-code/testing`,
// `claude-code-tools`, and minimal Web/Node globals (`crypto`, `TextEncoder`,
// `performance`) so `tsc -p tsconfig.ci.json --noEmit` type-checks the plugin
// and its 14 test suites offline.

declare const crypto: {
  getRandomValues<T extends ArrayBufferView>(array: T): T
}

declare class TextEncoder {
  encode(input?: string): Uint8Array
}

declare const performance: {
  now(): number
}

declare module 'claude-code-tools' {}

declare module 'claude-code' {
  type Options = import('../../hooks/lib/config.ts').Options
  type StepResult = import('../../hooks/lib/tracer.ts').StepResult
  type ToolOutcome = import('../../hooks/lib/tracer.ts').ToolOutcome
  type Usage = import('../../hooks/lib/tracer.ts').Usage

  export interface PluginState {}

  export interface StepChunk {
    kind: string
    index?: number
    text?: string
    [key: string]: unknown
  }

  export type TurnStepChunk = StepChunk

  export interface TurnStepResult extends StepResult {
    turnId: string
    index: number
    answer: string
    toolUses: readonly { name: string; [key: string]: unknown }[]
    stopReason: string | null
    usage: Usage | null
  }

  export type RenderSurface = 'terminal' | 'web'

  export type ToolCallName =
    | 'Bash'
    | 'Read'
    | 'Write'
    | 'Edit'
    | 'MultiEdit'
    | 'Glob'
    | 'LS'
    | 'WebFetch'
    | 'WebSearch'
    | 'TodoWrite'
    | 'NotebookEdit'
    | 'Agent'
    | 'Skill'
    | 'SlashCommand'
    | 'ExitPlanMode'
    | 'AskUserQuestion'
    | `mcp__${string}`

  export interface SessionStartInput {
    surface: RenderSurface | null
    isInteractive: boolean
    cwd?: string
  }

  export interface SessionEndInput {
    reason: 'clear' | 'resume' | 'fork' | 'prompt_input_exit' | 'exit' | 'other' | string
    sessionId: string
    resume: { id: string }
  }

  export interface ClassicSessionStartInput {
    source: 'startup' | 'clear' | 'resume' | 'fork' | string
    session_id?: string
    model?: string
    permission_mode?: string
    cwd?: string
  }

  export interface ClassicSubagentStartInput {
    agent_id: string
    agent_type: string
  }

  export interface ClassicPermissionRequestInput {
    tool_name?: string
    tool_input?: unknown
    agent_id?: string
  }

  export interface PromptAttachment {
    type?: string
    kind?: string
    path?: string
    text?: string
    bytes?: number
    [key: string]: unknown
  }

  export interface PromptSubmitInput {
    text: string
    wait?: boolean
    origin: { kind: 'composer' | 'task-notification' | 'command' | string }
    turnId?: string
    attachments?: readonly ({ type: string } & Record<string, unknown>)[]
  }

  export interface PromptSubmitOutput {
    text: string
    drop?: boolean
  }

  export interface TurnStartInput {
    turnId: string
    text: string
    agentId?: string
    attachments?: readonly ({ type: string } & Record<string, unknown>)[]
  }

  export interface TurnStepInput {
    turnId: string
    index: number
    model: string
    effort?: string | number
    messageCount: number
    agentId?: string
  }

  export interface TurnCompleteInput {
    turnId: string
    agentId?: string
    answer: string
    durationMs: number
    isAborted: boolean
    reason: 'answer' | 'error' | 'refusal' | 'aborted'
    refusal?: { category: string | null; explanation: string | null }
    usage?: Usage
  }

  export interface AbovePromptProps {
    readonly isCollapsed: boolean
    hasSurvey?: boolean
    isWorking?: boolean
    maxRows: number
    bodyColumns?: number
    scroll?: { offset: number; bodyRows: number }
    view?: Record<string, unknown>
    [key: string]: unknown
  }

  export interface UiRenderInput {
    component: string
    requestId?: string
    viewport?: { columns: number; rows: number }
    props: AbovePromptProps
  }

  export interface RenderElement {
    readonly type?: string
    readonly props?: Record<string, unknown>
    readonly children?: unknown
    readonly key?: string
    readonly text?: string
    readonly image: unknown
  }

  export interface MountUiOptions {
    plugin: string
    surface: RenderSurface | null
    component: string
    requestId?: string
    viewport?: { columns: number; rows: number }
    props: AbovePromptProps
  }

  export interface UiNode {
    type: string
    props: Record<string, unknown>
    children?: unknown
    key?: string
    text: string
  }

  export interface MountedUi {
    find(query?: { type?: string; key?: string; text?: string | RegExp }): Promise<UiNode | undefined>
    findAll(query?: { type?: string; key?: string; text?: string | RegExp }): Promise<UiNode[]>
    press(query: { key: string }): Promise<void>
    unmount(): Promise<void>
  }

  export interface TelemetryLogInput {
    to: 'collector' | string
    event: string
    loggedAt?: string | number
    attributes: Record<string, unknown>
  }

  export interface CommandRunInput {
    command: string
    args?: string
    origin?: { kind: string }
    presentation?: { isFullscreen: boolean; columns: number }
  }

  export interface EventPayloadMap {
    'session.start': {
      event: SessionStartInput
      result: unknown
      next: (e: SessionStartInput) => Promise<unknown>
    }
    'session.end': {
      event: SessionEndInput
      result: unknown
      next: ((e: SessionEndInput) => Promise<unknown>) & { budget: { remainingMs: number } }
    }
    'classic.SessionStart': {
      event: ClassicSessionStartInput
      result: unknown
      next: (e: ClassicSessionStartInput) => Promise<unknown>
    }
    'classic.SubagentStart': {
      event: ClassicSubagentStartInput
      result: unknown
      next: (e: ClassicSubagentStartInput) => Promise<unknown>
    }
    'classic.PermissionRequest': {
      event: ClassicPermissionRequestInput
      result: unknown
      next: (e: ClassicPermissionRequestInput) => Promise<unknown>
    }
    'prompt.submit': {
      event: PromptSubmitInput
      result: PromptSubmitOutput
      next: (e: PromptSubmitInput) => Promise<PromptSubmitOutput>
    }
    'ui.render': {
      event: UiRenderInput
      result: RenderElement
      next: (e: UiRenderInput) => Promise<RenderElement>
    }
    'telemetry.log': {
      event: TelemetryLogInput
      result: { value?: undefined; deny?: string }
      next: (e: TelemetryLogInput) => Promise<{ value?: undefined; deny?: string }>
    }
    'turn.start': {
      event: TurnStartInput
      result: unknown
      next: (e: TurnStartInput) => Promise<unknown>
    }
    'turn.step': {
      event: TurnStepInput
      result: AsyncGenerator<StepChunk, StepResult, unknown>
      next: (e: TurnStepInput) => AsyncGenerator<StepChunk, StepResult, unknown>
    }
    'tool.call': {
      event: Record<string, unknown> & { tool: ToolCallName }
      result: ToolOutcome
      next: (e: Record<string, unknown> & { tool: ToolCallName }) => Promise<ToolOutcome>
    }
    'turn.complete': {
      event: TurnCompleteInput
      result: unknown
      next: (e: TurnCompleteInput) => Promise<unknown>
    }
    'command.run': {
      event: CommandRunInput
      result: { text?: string }
      next: ((e: CommandRunInput) => Promise<{ text?: string }>) & { budget: { remainingMs: number } }
    }
    'command.register': {
      event: { name: string; description: string; argumentHint?: string; immediate?: boolean }
      result: { value?: unknown; deny?: string }
      next: (e: { name: string; description: string; argumentHint?: string; immediate?: boolean }) => Promise<{ value?: unknown; deny?: string }>
    }
    'env.get': {
      event: { name: string }
      result: { value?: string | undefined; deny?: string }
      next: (e: { name: string }) => Promise<{ value?: string | undefined; deny?: string }>
    }
    'store.get': {
      event: { key: string }
      result: { value?: unknown; deny?: string }
      next: (e: { key: string }) => Promise<{ value?: unknown; deny?: string }>
    }
    'store.set': {
      event: { key: string; value: unknown }
      result: { value?: undefined; deny?: string }
      next: (e: { key: string; value: unknown }) => Promise<{ value?: undefined; deny?: string }>
    }
    'store.delete': {
      event: { key: string }
      result: { value?: undefined; deny?: string }
      next: (e: { key: string }) => Promise<{ value?: undefined; deny?: string }>
    }
    'store.keys': {
      event: Record<string, never>
      result: { value?: string[]; deny?: string }
      next: (e: Record<string, never>) => Promise<{ value?: string[]; deny?: string }>
    }
    'state.set': {
      event: { plugin: string; key: string; value: unknown; options?: { ifVersion?: number } }
      result: { value?: { version: number; isSet: boolean }; version?: number; isSet?: boolean; deny?: string }
      next: (e: { plugin: string; key: string; value: unknown; options?: { ifVersion?: number } }) => Promise<{ value?: { version: number; isSet: boolean }; version?: number; isSet?: boolean; deny?: string }>
    }
    'session.id': {
      event: Record<string, never>
      result: { value?: string; deny?: string }
      next: (e: Record<string, never>) => Promise<{ value?: string; deny?: string }>
    }
    'session.version': {
      event: Record<string, never>
      result: { value?: { version: string; base?: string }; deny?: string }
      next: (e: Record<string, never>) => Promise<{ value?: { version: string; base?: string }; deny?: string }>
    }
    'http.fetch': {
      event: { url: string; init?: { method?: string; headers?: Record<string, string>; body?: string } }
      result: { value?: { status: number; ok?: boolean; statusText?: string; headers?: Record<string, string>; text: string }; deny?: string }
      next: (e: { url: string; init?: { method?: string; headers?: Record<string, string>; body?: string } }) => Promise<{ value?: { status: number; ok?: boolean; statusText?: string; headers?: Record<string, string>; text: string }; deny?: string }>
    }
    'process.run': {
      event: { argv: string[]; timeoutMs?: number }
      result: { value?: { exitCode: number; stdout: string; stderr: string; isStdoutTruncated?: boolean; isStderrTruncated?: boolean }; deny?: string }
      next: (e: { argv: string[]; timeoutMs?: number }) => Promise<{ value?: { exitCode: number; stdout: string; stderr: string; isStdoutTruncated?: boolean; isStderrTruncated?: boolean }; deny?: string }>
    }
    'fs.write': {
      event: { path: string; text: string }
      result: { value?: undefined; deny?: string }
      next: (e: { path: string; text: string }) => Promise<{ value?: undefined; deny?: string }>
    }
    'fs.read': {
      event: { path: string }
      result: { value?: string; deny?: string }
      next: (e: { path: string }) => Promise<{ value?: string; deny?: string }>
    }
    'fs.exists': {
      event: { path: string }
      result: { value?: boolean; deny?: string }
      next: (e: { path: string }) => Promise<{ value?: boolean; deny?: string }>
    }
    'fs.list': {
      event: { path: string }
      result: { value?: { name: string; kind: 'file' | 'dir' | string; size: number; mtimeMs?: number; isLink?: boolean }[]; deny?: string }
      next: (e: { path: string }) => Promise<{ value?: { name: string; kind: 'file' | 'dir' | string; size: number; mtimeMs?: number; isLink?: boolean }[]; deny?: string }>
    }
    'ui.status': {
      event: { text: string | undefined }
      result: unknown
      next: (e: { text: string | undefined }) => Promise<unknown>
    }
    'ui.log': {
      event: { text: string; to: 'transcript' | 'debug' | string }
      result: unknown
      next: (e: { text: string; to: 'transcript' | 'debug' | string }) => Promise<unknown>
    }
    'ui.toast': {
      event: { text: string; timeoutMs?: number; kind?: string }
      result: unknown
      next: (e: { text: string; timeoutMs?: number; kind?: string }) => Promise<unknown>
    }
  }

  export interface HookHandle<K extends keyof EventPayloadMap> {
    catch(
      handler: (
        $: EngineInterface,
        e: EventPayloadMap[K]['event'],
        next: EventPayloadMap[K]['next'],
        err?: unknown,
      ) => EventPayloadMap[K]['result'] | Promise<EventPayloadMap[K]['result']>,
    ): void
  }

  export interface OnFunction {
    <K extends keyof EventPayloadMap>(
      event: K,
      handler: (
        $: EngineInterface,
        e: EventPayloadMap[K]['event'],
        next: EventPayloadMap[K]['next'],
      ) => EventPayloadMap[K]['result'] | Promise<EventPayloadMap[K]['result']>,
    ): HookHandle<K>
    <K extends keyof EventPayloadMap>(
      event: K,
      filter: Record<string, unknown>,
      handler: (
        $: EngineInterface,
        e: EventPayloadMap[K]['event'],
        next: EventPayloadMap[K]['next'],
      ) => EventPayloadMap[K]['result'] | Promise<EventPayloadMap[K]['result']>,
    ): HookHandle<K>
  }

  export type On = OnFunction

  export interface EngineInterface {
    readonly plugin: {
      readonly root: string
    }
    readonly env: {
      get(name: string): Promise<string | undefined>
    }
    readonly store: {
      get(key: string): Promise<unknown>
      set(key: string, value: unknown): Promise<void>
      delete(key: string): Promise<void>
      keys(): Promise<string[]>
    }
    readonly state: {
      get<P extends keyof PluginState, K extends keyof PluginState[P]>(
        ref: { readonly plugin: P; readonly key: K },
      ): Promise<{ value: PluginState[P][K] | undefined; version: number }>
      set<P extends keyof PluginState, K extends keyof PluginState[P]>(
        ref: { readonly plugin: P; readonly key: K },
        value: PluginState[P][K],
        options?: { ifVersion?: number },
      ): Promise<{ version: number; isSet: boolean }>
    }
    readonly session: {
      id(): Promise<string>
      version(): Promise<{ version: string; base?: string }>
      start(e: SessionStartInput): Promise<unknown>
      end(e: SessionEndInput): Promise<unknown>
    }
    readonly classic: {
      SessionStart(e: ClassicSessionStartInput): Promise<unknown>
      SubagentStart(e: ClassicSubagentStartInput): Promise<unknown>
      PermissionRequest(e: ClassicPermissionRequestInput): Promise<unknown>
    }
    readonly prompt: {
      submit(e: PromptSubmitInput): Promise<PromptSubmitOutput>
    }
    readonly turn: {
      start(e: TurnStartInput): Promise<unknown>
      step(e: { turnId: string; index: number; model?: string; effort?: string | number; messageCount?: number; agentId?: string }): AsyncGenerator<StepChunk, TurnStepResult, unknown>
      complete(e: TurnCompleteInput): Promise<unknown>
    }
    readonly tool: {
      call(
        e: Record<string, unknown> & { tool: ToolCallName },
      ): Promise<{ isError?: boolean; text?: string; result?: unknown; deny?: string }>
    }
    readonly telemetry: {
      log(e: TelemetryLogInput): Promise<{ value?: undefined; deny?: string }>
    }
    readonly command: {
      register(spec: { name: string; description: string; argumentHint?: string; immediate?: boolean }): Promise<void>
      run(e: CommandRunInput): Promise<{ text?: string }>
    }
    readonly clock: {
      after(ms: number, fn: () => void): { cancel: () => void }
      every(ms: number, fn: () => void): { cancel: () => void }
      sleep(ms: number): Promise<void>
    }
    readonly http: {
      fetch(
        url: string,
        init?: { method?: string; headers?: Record<string, string>; body?: string },
      ): Promise<{ status: number; statusText?: string; headers?: Record<string, string>; text: string }>
    }
    readonly process: {
      run(
        argv: readonly string[],
        options?: { timeoutMs?: number },
      ): Promise<{ exitCode: number; stdout: string; stderr: string; isStdoutTruncated?: boolean; isStderrTruncated?: boolean }>
    }
    readonly fs: {
      write(path: string, text: string): Promise<void>
      read(path: string): Promise<string>
      exists(path: string): Promise<boolean>
      list(path: string): Promise<readonly { name: string; kind: 'file' | 'dir' | string; size: number; mtimeMs?: number; isLink?: boolean }[]>
    }
    readonly ui: {
      status(text: string | undefined): void
      log(text: string, options?: { to?: 'transcript' | 'debug' | string }): void
      toast(text: string, options?: { timeoutMs?: number; kind?: string }): void
      invalidate(target: string): void
      resolve(e: unknown): {
        Box(props: Record<string, unknown>): UiNode
        Text(props: Record<string, unknown>): UiNode
        Button(props: Record<string, unknown>): UiNode
      }
      mount(options: MountUiOptions): Promise<MountedUi>
    }
  }

  export type Register = (on: OnFunction, options: Options) => void
  export const on: OnFunction
}

declare module 'claude-code/testing' {
  import type { EngineInterface, OnFunction } from 'claude-code'

  export type Engine = Pick<EngineInterface, 'plugin' | 'session' | 'classic' | 'prompt' | 'turn' | 'tool' | 'telemetry'> & {
    readonly command: {
      run(e: import('claude-code').CommandRunInput, opts?: { budgetMs?: number }): Promise<{ text?: string }>
    }
    readonly ui: Pick<EngineInterface['ui'], 'mount'>
  }

  export interface Matchers<T> {
    toBe(expected: unknown): void
    toEqual(expected: unknown): void
    toMatchObject(expected: Record<string, unknown> | readonly unknown[]): void
    toBeDefined(): void
    toBeUndefined(): void
    toBeNull(): void
    toBeTruthy(): void
    toBeFalsy(): void
    toBeGreaterThan(expected: number): void
    toBeGreaterThanOrEqual(expected: number): void
    toBeLessThan(expected: number): void
    toBeLessThanOrEqual(expected: number): void
    toHaveLength(expected: number): void
    toContain(expected: unknown): void
    toMatch(expected: string | RegExp): void
    toThrow(expected?: string | RegExp): void
    readonly not: Matchers<T>
  }

  export function expect<T>(actual: T): Matchers<T>
  export namespace expect {
    function stringMatching(pattern: string | RegExp): unknown
  }

  export interface MockClock {
    now(): number
    advance(ms: number): Promise<void>
    settle(): Promise<void>
    sleep(ms: number): Promise<void>
  }

  export const mock: {
    clock(on: OnFunction, options?: { now?: number }): MockClock
    env(on: OnFunction, vars: Record<string, string | undefined>): void
  }

  export function test(
    name: string,
    fn: ($: Engine, on: OnFunction) => void | Promise<void>,
  ): void
  export function test(
    name: string,
    options: { options?: Record<string, unknown> },
    fn: ($: Engine, on: OnFunction) => void | Promise<void>,
  ): void
}
