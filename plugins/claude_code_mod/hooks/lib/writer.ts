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

// The background writer. Hooks only call `enqueue` (synchronous, in memory);
// everything that touches the network, a process or the disk runs here, from
// timers, never inside a hook's dispatch (session end's bounded work aside:
// its spill write and, while BigQuery takes this session's rows, one request
// with a token already at hand, and in a `-p` run a replay of parked files,
// all within its budget, none running a process).
//
// Delivery: batched tabledata.insertAll with a client-side timeout, retries
// with exponential backoff, then a local dead-letter file. Every failure path
// keeps the rows (on disk) or counts them as dropped; none throws into a hook.
// Every diagnostic is redacted before it is cut, kept, written or shown.
//
// While rows cannot be written for lack of working credentials or of
// permission, or because BigQuery cannot find the dataset or table, the
// writer holds that problem and shows it: the band above the prompt (drawn in
// register.ts), a toast and a transcript line once, and the status line, each
// with the fix and how many rows wait. The next send or replay BigQuery
// takes clears it.

import { type Config, canDeliver, deadLetterPartition, destinationDir, joinPath, tableRef } from './config.ts'
import { type HttpAnswer, TOKEN_INFO_URL, classifyInsert, createTableBody, insertAllBody, insertAllUrl, isDatasetMissing, tablesUrl } from './bigquery.ts'
import { type DeadLetterKind, type DeadLetterLine, HOST_FILE_BYTES, MAX_FILE_BYTES, type ParsedFile, fileName, isDeadLetterKind, kindOf, lineText, packBodies, packFiles, packLines, parseFile, sealFile } from './deadletter.ts'
import { randomHex } from './ids.ts'
import { BoundedQueue, type Queued } from './queue.ts'
import { type BqaaRow, fitRow, rowBytes, utf8Length } from './rows.ts'
import { redactDiagnostic } from './sanitize.ts'
import { errorText } from './tracer.ts'

export type Timer = { cancel: () => void }

export type HttpInit = { method: string; headers: Record<string, string>; body?: string }

// Everything the writer reaches outside itself, wired to `$` in register.ts.
export type Io = {
  now: () => number
  after: (ms: number, fn: () => void) => Timer
  fetch: (url: string, init: HttpInit) => Promise<HttpAnswer>
  runCommand: (argv: string[], timeoutMs: number) => Promise<{ exitCode: number; stdout: string; stderr: string }>
  envToken: () => Promise<string | undefined>
  writeFile: (path: string, text: string) => Promise<void>
  readFile: (path: string) => Promise<string>
  // Whether a path exists. A writer without it reads the file, an empty text
  // being no file.
  exists?: (path: string) => Promise<boolean>
  listDir: (path: string) => Promise<readonly { name: string; kind: string; size: number; mtimeMs?: number }[]>
  storeGet: (key: string) => Promise<unknown>
  storeSet: (key: string, value: unknown) => Promise<void>
  storeDelete: (key: string) => Promise<void>
  status: (text: string | undefined) => void
  debug: (text: string) => void
  // The rest of the credential notice: a toast, a transcript line (dim,
  // never sent to the model), and a redraw of the band above the prompt.
  // A writer without them shows the status line alone.
  toast?: (text: string) => void
  notice?: (text: string) => void
  redraw?: () => void
  // True when the AbovePrompt band renders in this session, so problem
  // notices already visible in the band are not duplicated onto the persistent
  // status line.
  canShowBand?: () => boolean
  // The mod's own directory, where setup.sh is: the fix for a dataset
  // BigQuery cannot find names the script there. Absent, the fix is told
  // without a command.
  pluginRoot?: string
  // Rows just landed: BigQuery took them (a send or a replay), or a dry run
  // wrote them (at session end too), under `to`, the configuration they
  // were sent with. The welcome card says when the first ones land where
  // it says rows go.
  sent?: (rows: number, to: Config) => void
}

// Why rows cannot be written: no working credentials (the token source
// fails, or BigQuery refuses even a fresh token), the identity lacks
// permission on the dataset, or BigQuery cannot find the dataset or table
// (not made yet, or made so recently that streaming inserts do not see it).
export type ProblemKind = 'credentials' | 'permission' | 'destination'
// `missing` says which one BigQuery could not find, for a destination problem;
// for a permission problem, `write` when BigQuery refused rows themselves
// (insertAll: no bigquery.tables.updateData), which finding the table
// (tables.get) does not clear.
export type Problem = { kind: ProblemKind; reason: string; since: number; missing?: 'dataset' | 'table' | 'write' }

export type Counters = {
  // Rows built and queued.
  captured: number
  // Rows BigQuery accepted (or, in a dry run, written to the dry-run files).
  sent: number
  // Of `sent`, rows replayed from dead-letter / spill files via backfill.
  replayed: number
  // Rows moved to dead-letter files after a failure (retry + rejected).
  failed: number
  // Of those, rows BigQuery or the configuration refused.
  rejected: number
  // Rows written to spill files: at session end, or under queue pressure.
  spilled: number
  // Rows lost: dropped by the full queue, or a dead-letter write failed.
  dropped: number
  // Rows not queued because there was nowhere to send them.
  skipped: number
  retries: number
  batches: number
  truncatedRows: number
  redactions: number
  deadLetterFiles: number
}

export type BackfillReport = {
  // `force`: /bqaa-backfill force, which replays files another session's
  // replay holds (its mark, or a record cut off part-way) afresh.
  trigger: 'auto' | 'command' | 'force' | 'recovery'
  files: number
  rowsSent: number
  rowsLeft: number
  // Of rowsLeft, rows BigQuery refused: on this replay, or on the one whose
  // outcome this replay finished; moved to `rejected` files, which only
  // /bqaa-backfill sends.
  rowsRefused: number
  // Of rowsLeft, lines recorded for another table: kept as they were.
  otherTableRows: number
  // Files with rows that wait, left unread (another session's lease, a
  // record that cannot be read, the replay stopped first): their rows are in
  // none of the counts above.
  skippedFiles: number
  // Files over the 4 MiB `$.fs.read` limit, which cannot be read back.
  oversizedFiles: string[]
  // Files with no completion record that matches them: still being written,
  // or left by a writer that stopped part-way. Kept as they are, not read.
  incompleteFiles: string[]
  // Why the first file this replay left waiting for itself alone was left
  // (its record cannot be read, or is cut off, or another replay's mark
  // stands over it, or this replay could not write its own): the replay
  // goes on with the files after it.
  held: string | null
  // What stopped the replay (the network, the credentials, the directory):
  // the files after it wait for the next one.
  error: string | null
  finishedAt: number | null
}

// What session end may do besides spilling (spillForExit).
export type ExitFlushOptions = {
  // Send the rows still queued in one request, while BigQuery takes this
  // session's requests and a token is at hand.
  sendQueued?: boolean
  // Then replay this table's spill and retry files in what is left of the
  // budget: for a run with no later moment to do it (`-p`).
  replaySpills?: boolean
}

// A moment on the host's clock: settled when it comes, and whether it has.
type Mark = { at: Promise<void>; passed: () => boolean }

// The moments an exit keeps to, from its start: the last for a token to come
// (EXIT_SEND_MIN_MS before the end), for its request to answer
// (EXIT_SPILL_RESERVE_MS before), for its replay to send (EXIT_REPLAY_RESERVE_MS
// before), and the end of its budget.
type ExitMarks = { token: Mark; post: Mark; replay: Mark; end: Mark }

// What a replay learns as it goes: how many of its rows still wait for a
// replay that runs by itself (lines, and files it did not read), and how
// many requests it sent.
type ReplayTally = { waiting: number; requests: number }

// What a replay made of a file's lines, one letter a line in the file's
// order (L landed, W waits as it was, R refused, K kept as written: another
// table's, or a kind this replay does not send), with each refused line's
// reason (`refusals`, in the same order, indexes `reasons`, which holds
// MAX_REASONS at most).
type Outcome = { fates: string; reasons: string[]; refusals: number[] }

// A replayed file (`name`), and where its record is: in `$.store` under
// `recordKey`, or, when the store does not take it, in one of the two files
// `recordFiles` beside it (`outcome-` and the file's name, then `outcome-b-`
// and it: no replay takes either for rows), which take the record's
// updates in turn, so one cut off part-way leaves the one before it whole.
// A file beside it, in `dir`, which no record is written to, each writer's
// own (`doneFileOf`: `outcome-done-`, the writer's name, then the file's):
// written before it sends any of the file (`sending: true`), and overwritten
// with the retired outcome (`retired: true`) once done. No writer writes over
// or empties another's, so no late write of one takes another's away. A file
// is written once, under a name of its own, so its name and size tell it;
// `live` while it still holds its rows (a record of an emptied file is read
// for the files it left).
type Source = { dir: string; name: string; size: number; recordKey: string; recordFiles: readonly string[]; live: boolean }

// A file a replay has an outcome for: the outcome, the files that hold what
// is left of it under the names given them (`text` null once written),
// whether any of those holds rows that wait for a replay, whether its record
// may be in `recordFiles`, the number of its last record (`seq`), and whether
// this write converted the writer's sending mark into a retired done mark.
type Rewrite = Outcome & Source & { files: { path: string; text: string | null }[]; waits: boolean; filed: boolean; seq: number; wroteDone?: boolean }

// The record of a Rewrite, which every writer reads: written before what is
// left is, and updated as each file of it is written; once every one is,
// written as the mark that the file was done (`retired`, in the writer's
// `doneFileOf`),
// which is kept a day. A writer that finds it (a later session.start's,
// another session's, or this one after another writer moved it on) writes
// what is still missing, from the source and the outcome, and sends none of
// the file's rows again.
type Journal = Outcome & { size: number; files: string[]; written: boolean[]; waits: boolean; seq: number; retired?: boolean; retiredAt?: number }

const TIMED_OUT = Symbol('timed out')
const MAX_BATCHES_PER_TICK = 10
const LEASE_MS = 10 * 60 * 1000
// How long an empty dead-letter file, with nothing beside it to say a
// replay emptied it, may be one whose write is still under way (a write is
// not atomic, and can show the file empty first): a lease's time.
const WRITE_PENDING_MS = LEASE_MS
// A table this writer made gets this long before a 404 for it is the notice:
// streaming inserts may not see a new table for a while.
const NEW_TABLE_MS = 120_000
// While credentials or a missing dataset are the problem, a retry comes at
// least this often, so rows flow again soon after the person signs in or
// makes the dataset.
const QUICK_RETRY_CAP_MS = 30_000
// How long a failed lookup of the credential's account is kept before it is
// tried again.
const IDENTITY_RETRY_MS = 10 * 60 * 1000
const EMAIL = /^[^\s@]{1,64}@[^\s@]{1,255}$/
// Longest diagnostic kept: the last error, a report's error, a line's reason.
const DIAGNOSTIC_CHARS = 1000
// Most row ids kept to count a refused or parked row once; past that, rows
// are counted without the check.
const COUNTED_IDS = 10_000
// Most refusal reasons a replay keeps for one file: the first ones as
// BigQuery gave them, then OTHER_REASON for every row refused for yet another.
// So a file's replay record stays small: a letter and a small number a line,
// these reasons, and the names of what is left.
const MAX_REASONS = 16
const OTHER_REASON = `refused for another reason (a replay keeps ${MAX_REASONS} reasons a file)`
// How long the mark of a file done stays beside it: no late write of another
// session's copy comes that long after.
const RETIRED_MS = 24 * 60 * 60 * 1000
// Largest replay record kept in `$.store`, which holds 4 MiB of JSON in all
// for every key of the mod's: a larger one goes to the file beside its source.
const STORE_RECORD_BYTES = 64 * 1024
// Longest `Run: <command>` line the band (register.ts) and the transcript
// show whole; a longer command is shown whole by /bqaa-status alone.
export const COMMAND_LINE_CHARS = 1000
// Longest setup command offered at all.
const SETUP_COMMAND_CHARS = 4000
// Session end's own request starts only with at least EXIT_SEND_MIN_MS of
// its budget left, and is given up EXIT_SPILL_RESERVE_MS before the budget
// ends, so the rows it did not land are spilled within it. The request so
// has at least 300 ms to be answered: one given up sooner may still land,
// and its rows, spilled as well, would go twice (event_id dedups them).
const EXIT_SEND_MIN_MS = 450
const EXIT_SPILL_RESERVE_MS = 150
// Session end creates a missing table only if at least this much time remains
// before the post mark.
const EXIT_CREATE_TABLE_MIN_MS = 250
// Session end's replay starts only with at least EXIT_REPLAY_MIN_MS left,
// and sends nothing in the last EXIT_REPLAY_RESERVE_MS, which the writes of
// its outcome take.
const EXIT_REPLAY_MIN_MS = 250
const EXIT_REPLAY_RESERVE_MS = 100

const diagnostic = (text: string, maxChars = DIAGNOSTIC_CHARS): string => redactDiagnostic(text, maxChars)
// An error's own message, without the `Error: ` errorText puts first.
const messageOf = (err: unknown): string => (err instanceof Error ? err.message : errorText(err))
// One line of a command's error output: its `ERROR:` line when it has one
// (gcloud prints `WARNING:` lines first), else its first line.
const errorLine = (text: string): string => {
  let first = ''
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (line.startsWith('ERROR:')) return line
    if (first === '') first = line
  }
  return first
}
// The token command, named by its program only: its arguments can hold a
// credential (a key file, a header, a password).
const programName = (argv: readonly string[]): string => (argv[0] ?? '').split(/[\\/]/).pop() || 'command'

const HEADLINE: Record<Exclude<ProblemKind, 'destination'>, string> = {
  credentials: 'no working Google credentials',
  permission: 'BigQuery denied the identity access to the dataset',
}

// The names setup.sh takes (it checks them the same way before any call): a
// command is offered only for these, so nothing in it is read by the shell
// as anything but the name.
const PLAIN_PROJECT = /^([a-z0-9.-]+:)?[a-z][-a-z0-9]{4,28}[a-z0-9]$/
const PLAIN_NAME = /^[A-Za-z0-9_]{1,1024}$/
const PLAIN_LOCATION = /^[A-Za-z0-9-]+$/
// A path as one shell word: in single quotes, any single quote in it closed,
// escaped and reopened.
const shellQuote = (text: string): string => `'${text.replace(/'/g, `'\\''`)}'`

// Builds the shell command to run the mod's setup.sh for `c`, or null when
// `root` is unknown or not a POSIX path without control characters, any name
// is one setup.sh would refuse, or the command would be cut or redacted.
export function buildSetupCommand(c: Config, root: string | undefined): string | null {
  if (root === undefined || !root.startsWith('/') || /[\u0000-\u001f\u007f]/.test(root)) return null
  const views = `${c.dataset}_reporting`
  if (!PLAIN_PROJECT.test(c.project) || ![c.dataset, views, c.table].every(name => PLAIN_NAME.test(name)) || !PLAIN_LOCATION.test(c.location)) return null
  const script = root.endsWith('/') ? `${root}setup.sh` : `${root}/setup.sh`
  const command = `BQAA_PROJECT_ID=${c.project} BQAA_DATASET=${c.dataset} BQAA_TABLE=${c.table} BQAA_VIEWS_DATASET=${views} BQAA_LOCATION=${c.location} bash ${shellQuote(script)}`
  return diagnostic(`Run: ${command}`, SETUP_COMMAND_CHARS + 5) === `Run: ${command}` ? command : null
}

// A POSIX path without control characters, as a command may name it.
const plainPath = (path: string | undefined): path is string => path !== undefined && path.startsWith('/') && !/[\u0000-\u001f\u007f]/.test(path)

// `command`, or null where redaction would change it as shown.
const shownWhole = (command: string): string | null => (diagnostic(`Run: ${command}`, SETUP_COMMAND_CHARS + 5) === `Run: ${command}` ? command : null)

// The command a project administrator runs once to set up who writes and who
// reads the table: the mod's scripts/setup_gcp_prereqs.py for `c`'s names,
// in a virtual environment of its own in the administrator's home directory
// (the script calls the BigQuery Python client; nothing is installed in the
// mod's directory, and no shared directory such as /tmp, where another user
// could make the environment first, is used). A dry run unless --execute is
// added. Null as for buildSetupCommand.
export function buildPrereqsCommand(c: Config, root: string | undefined): string | null {
  if (!plainPath(root)) return null
  if (!PLAIN_PROJECT.test(c.project) || ![c.dataset, c.table].every(name => PLAIN_NAME.test(name)) || !PLAIN_LOCATION.test(c.location)) return null
  const script = root.endsWith('/') ? `${root}scripts/setup_gcp_prereqs.py` : `${root}/scripts/setup_gcp_prereqs.py`
  const venv = '"$HOME/.bqaa-mod/venv"'
  return shownWhole(`python3 -m venv ${venv} && ${venv}/bin/pip install -q google-cloud-bigquery && ${venv}/bin/python ${shellQuote(script)} --project ${c.project} --dataset ${c.dataset} --table ${c.table} --location ${c.location}`)
}

// The command that deletes the dead-letter files no replay needs any more
// (the mod's scripts/clean_dead_letter.py, run while no session is running):
// for the default directory, which the script cleans when given none, the
// script alone; else with `c`'s directory named. Null when `root` or the
// directory is not a POSIX path without control characters, or the command
// would be redacted.
export function buildCleanupCommand(c: Config, root: string | undefined): string | null {
  if (!plainPath(root) || (!c.deadLetterDirIsDefault && !plainPath(c.deadLetterDir))) return null
  const script = root.endsWith('/') ? `${root}scripts/clean_dead_letter.py` : `${root}/scripts/clean_dead_letter.py`
  return shownWhole(`python3 ${shellQuote(script)}${c.deadLetterDirIsDefault ? '' : ` ${shellQuote(c.deadLetterDir)}`}`)
}

// How the refused and parked counts know a line's row: its event_id; for one
// with none (in a file another writer made), the key a replay gave the line
// as it rewrote it, else the file (`name`) and the line's place there, which
// that key keeps. So such a row counts once however often it is refused,
// and rows alike in two files count as two.
const keyOf = (line: DeadLetterLine, name: string, index: number): string => {
  const id = line.row.event_id
  if (typeof id === 'string' && id !== '') return id
  return typeof line.key === 'string' && line.key !== '' ? line.key : `${name}#${index}`
}

// Whether an outcome is one for these lines: a letter for each, and a reason
// it holds for each refused one.
const fitsLines = (file: ParsedFile, outcome: Outcome): boolean =>
  outcome.fates.length === file.lines.length &&
  /^[LWRK]*$/.test(outcome.fates) &&
  outcome.refusals.length === outcome.fates.split('R').length - 1 &&
  outcome.refusals.every(k => outcome.reasons[k] !== undefined)

const sameNames = (a: readonly string[], b: readonly string[]): boolean => a.length === b.length && a.every((name, i) => name === b[i])

// A record as a writer finds it, and whether one may be in a file beside the
// source (to empty along with it).
type Found = { journal: Partial<Journal>; filed: boolean }
// No outcome recorded, and replays' marks (`pending`, the writers that left
// them): each sent rows, or is about to, and may yet record one.
type Pending = { pending: string[] }

// Where the record of the file `name`, `size` long, in `dir` is kept.
const sourceFor = (dir: string, name: string, size: number, c: Config): Source => ({
  dir,
  name,
  size,
  recordKey: `replayed:${destinationDir(tableRef(c))}/${name}`,
  recordFiles: [joinPath(dir, `outcome-${name}`), joinPath(dir, `outcome-b-${name}`)],
  live: true,
})
// The mark the writer `by` leaves before sending (`sending: true`) and once
// finished (`retired: true`), in `doneFileOf`. `markerOf` also recognizes
// legacy `outcome-sending-` markers from prior versions.
const doneFileOf = (dir: string, name: string, by: string): string => joinPath(dir, `outcome-done-${by}-${name}`)
const markerOf = (file: string, name: string): string | null => {
  const marked = /^outcome-(?:sending|done)-([0-9a-z]+)-(.+)$/.exec(file)
  return marked !== null && marked[2] === name ? (marked[1] as string) : null
}
const doneMarkerOf = (file: string, name: string): string | null => {
  const marked = /^outcome-done-([0-9a-z]+)-(.+)$/.exec(file)
  return marked !== null && marked[2] === name ? (marked[1] as string) : null
}
// The source file of a record file's name, if it is one. /bqaa-status
// (register.ts) pairs record files with their sources by it too.
export const recordOf = (name: string): string | null => {
  const marked = /^outcome-(?:sending|done)-[0-9a-z]+-(.+)$/.exec(name)
  if (marked !== null) return marked[1] as string
  for (const prefix of ['outcome-b-', 'outcome-']) if (name.startsWith(prefix)) return name.slice(prefix.length)
  return null
}

// Which files of what is left a record has as written: all of them for a
// record that does not say.
const writtenOf = (journal: Partial<Journal>): boolean[] => {
  const files = Array.isArray(journal.files) ? journal.files : []
  return Array.isArray(journal.written) ? files.map((_, i) => journal.written?.[i] === true) : files.map(() => true)
}

// The number of a record (0 for one that does not say).
const seqOf = (journal: Partial<Journal>): number => (typeof journal.seq === 'number' ? journal.seq : 0)

// A record kept in a file: its JSON on one line, then a completion record as
// a dead-letter file ends with, so one cut off part-way is told apart
// (`torn`) from none. A file that is empty, or read as anything but text
// (none there, to a writer without `exists`), holds none.
const journalText = (journal: Journal): string => sealFile(`${JSON.stringify(journal)}\n`)
const journalOf = (text: unknown): Partial<Journal> | 'torn' | undefined => {
  if (typeof text !== 'string' || text === '') return undefined
  const parsed = parseFile(text)
  const only = parsed.lines.length === 0 && parsed.unreadable.length === 1 ? parsed.unreadable[0] : undefined
  if (!parsed.complete || only === undefined) return 'torn'
  try {
    return JSON.parse(only) as Partial<Journal>
  } catch {
    return 'torn'
  }
}

// Whether a writer's mark (its text) stands for the file `size` long (-1:
// any size): whole, or cut off part-way, which is taken as standing (its
// writer may have sent). An empty one, which its writer let go, or one of
// another size, does not.
const markStands = (text: unknown, size: number): boolean => {
  const mark = journalOf(text) as { size?: unknown; sending?: unknown; retired?: unknown } | 'torn' | undefined
  if (mark === 'torn') return true
  return mark !== undefined && mark.sending === true && mark.retired !== true && (size < 0 || mark.size === size)
}

// What a replay leaves of the file `name`, from its outcome, as the files to
// write: the lines that wait or were refused (a refused line as a `rejected`
// line, with its reason and one attempt more; either, for a row with no
// event_id, with its key), then the lines kept as written, then the lines
// that do not parse; each kind to files of its own, in the order it first
// comes, each naming `name` as the file it came from, so no replay takes it
// before that file's record has it as written. The same file and outcome
// always give the same files, so a writer that resumes another's outcome
// writes the same ones.
function leftoverFiles(file: ParsedFile, fileKind: DeadLetterKind, outcome: Outcome, name: string): { kind: DeadLetterKind; text: string }[] {
  const rest = new Map<DeadLetterKind, string[]>()
  const keep = (kind: DeadLetterKind, text: string): void => {
    const texts = rest.get(kind)
    if (texts) texts.push(text)
    else rest.set(kind, [text])
  }
  const kindOfLine = (line: DeadLetterLine): DeadLetterKind => (isDeadLetterKind(line.kind) ? line.kind : fileKind)
  const keyed = (line: DeadLetterLine, i: number): DeadLetterLine => (keyOf(line, name, i) === line.row.event_id ? line : { ...line, key: keyOf(line, name, i) })
  let refused = 0
  file.lines.forEach((line, i) => {
    const fate = outcome.fates[i]
    if (fate === 'W') keep(kindOfLine(line), lineText({ ...keyed(line, i), kind: kindOfLine(line) }))
    else if (fate === 'R') {
      const reason = outcome.reasons[outcome.refusals[refused] ?? -1] ?? ''
      refused += 1
      keep('rejected', lineText({ ...keyed(line, i), kind: 'rejected', reason, attempts: line.attempts + 1 }))
    }
  })
  file.lines.forEach((line, i) => {
    if (outcome.fates[i] === 'K') keep(kindOfLine(line), file.raw[i] as string)
  })
  for (const text of file.unreadable) keep(fileKind, text)
  const files: { kind: DeadLetterKind; text: string }[] = []
  for (const [kind, texts] of rest) for (const text of packFiles(texts, name)) files.push({ kind, text })
  return files
}

export class Writer {
  queue: BoundedQueue
  readonly counters: Counters = {
    captured: 0,
    sent: 0,
    replayed: 0,
    failed: 0,
    rejected: 0,
    spilled: 0,
    dropped: 0,
    skipped: 0,
    retries: 0,
    batches: 0,
    truncatedRows: 0,
    redactions: 0,
    deadLetterFiles: 0,
  }
  lastError: { message: string; at: number } | null = null
  lastSuccessAt: number | null = null
  backfillReport: BackfillReport | null = null

  private io: Io
  private config: () => Config
  private sessionId: () => string | null
  private inFlight: Promise<void> | null = null
  // The batch out whose answer has not come: once one does, its rows are
  // handed on at once (landed, back to the queue, or to a file being
  // written), and none is held here.
  private inFlightRows: Queued[] = []
  // The writers this one took over from (`adopt`), oldest first: a batch one
  // still has out goes to the exit spill with this writer's rows.
  private before: Writer[] = []
  // The dead-letter writes not ended yet, of every writer that takes over in
  // turn (`adopt`): their rows are on no queue and in no batch out, so the
  // exit waits for them as for its own spill.
  private persisting = new Set<Promise<void>>()
  // True from when a batch is taken until its answer hands its rows on (to
  // BigQuery, back to the queue, or to disk): only then are they counted as
  // in flight among the rows waiting.
  private batchOpen = false
  private consecutiveFailures = 0
  // Backoff is a timer, not a clock comparison, so it follows the host's
  // clock (and a test's mock clock) exactly.
  private pauseTimer: Timer | null = null
  private pausedUntil = 0
  private token: { value: string; expiresAt: number } | null = null
  private tokenPending: Promise<string> | null = null
  private createAttempted = false
  private tableCreatedAt: number | null = null
  private seq = 0
  private soon: Timer | null = null
  private backfilling: Promise<BackfillReport> | null = null
  // The dead-letter directories (one per table) whose rows wait for a replay
  // that runs by itself: this writer parked them as retries or spills, or a
  // replay left them unsent. While nothing is queued, the paced retries
  // replay the configured table's until one sends every such row.
  private replayDue = new Set<string>()
  // Those parkings: how many (a replay that began before the latest one may
  // not have read its rows), and whether rows were parked since the last
  // replay that rows landing set going (`succeeded`).
  private parked = { seq: 0, recoverable: false }
  // The host's status line as the writers of this load last set it (null:
  // not known, a clear that did not land): one line for the plugin, so every
  // writer that takes over shares it (`adopt`).
  private status: { text: string | undefined | null } = { text: undefined }
  // Set once the session is ending: no new batch starts after that.
  private closing = false
  // The replay of parked rows that a send after failures set going
  // (`succeeded`), due a second later: the exit cancels it, and sends
  // nothing of its own while it is due.
  private recoveryTimer: Timer | null = null
  // Set once a batch this writer had taken went back to the queue because
  // the exit began (it was waiting for its token or its account): the exit
  // then sends nothing of its own, and spills that batch with the rest.
  private heldAtExit = false
  // While session end's own replay runs (`replayAtExit`): it goes on past
  // `closing` until `until` or `cutoff`, whichever comes first, and takes no
  // token but one already at hand.
  private exitReplay: { until: number; cutoff: Mark } | null = null
  // Why rows cannot be written now, if credentials or permission stop them.
  problem: Problem | null = null
  // Rows parked on disk while the problem stood (sent once it clears), and
  // their ids, so a row counts once however often it is parked or refused.
  private parkedDuringProblem = 0
  private parkedIds = new Set<string>()
  // Rows refused since rows last landed: the count the status line gives,
  // and their ids, likewise.
  private refusedSinceSuccess = 0
  private refusedIds = new Set<string>()
  // Whether a count went past the COUNTED_IDS it keeps, so a row may count
  // again: the count then reads "about N".
  private parkedRough = false
  private refusedRough = false
  // Replays whose outcome is not all written out yet, by the file's path: a
  // later replay finishes writing it and sends none of the file again.
  private rewrites = new Map<string, Rewrite>()
  // The name this writer leaves in its marks (`doneFileOf`), ten hex
  // characters from the same source as the row ids: a mark of its own is one
  // whose outcome it would hold, if it had one.
  private readonly mark = randomHex(5)
  // When this writer first saw each dead-letter file empty or not whole, by
  // path: how long a write may still be under way where the listing gives no
  // time (`writtenSince`).
  private seenSince = new Map<string, number>()
  // Empty dead-letter files known to be done, by path: this writer emptied
  // them after their replay, or a record beside them or in the store said
  // so. A replay looks no further for these (`backfillRun`); a path is let
  // go once it is listed with rows again, or not at all.
  private retiredEmpty = new Set<string>()
  // Sessions for which at least one row has already been sent, written in a
  // dry run, or parked to a dead-letter file (shared across writer takeovers).
  private sentSessions = new Set<string>()
  // Problem kinds already announced with a toast and a transcript line.
  private announced = new Set<ProblemKind>()
  // Where the last token came from: the fix differs.
  private tokenSource: 'environment' | 'command' = 'command'
  // With userIdFromCredentials: the account of one access token, the token it
  // describes, and the order its lookup started in (a lookup that answers
  // late never replaces a newer one). `email` is null while a failed lookup
  // waits to be tried again. The token stays in memory, like `token`.
  private identity: { token: string; email: string | null; seq: number } | null = null
  private identityPending: { token: string; answer: Promise<string | null> } | null = null
  private identitySeq = 0
  // The dead-letter partition (the table) of the configuration this writer
  // began with. The configuration is read again through the same function
  // after a reload, which may have changed it: a takeover compares this
  // (`adopt`).
  private readonly partition: string

  constructor(io: Io, config: () => Config, sessionId: () => string | null) {
    this.io = io
    this.config = config
    this.sessionId = sessionId
    const c = config()
    this.queue = new BoundedQueue(c.maxQueueRows, c.maxQueueBytes)
    this.partition = deadLetterPartition(c)
  }

  // ---- hot path ----------------------------------------------------------

  // Called from hooks: synchronous, bounded, never throws past its caller's
  // try, never waits on anything.
  enqueue(row: BqaaRow, redactions: number): void {
    if (!canDeliver(this.config())) {
      this.counters.skipped += 1
      return
    }
    this.queue.push(row)
    this.counters.captured += 1
    this.counters.redactions += redactions
    if (row.is_truncated) this.counters.truncatedRows += 1
  }

  // ---- background --------------------------------------------------------

  // Takes over from the writer before it in the same load of the code (a
  // session.start that fires again): its queue itself, under this writer's
  // bounds, so a batch it, or any writer before it, still has out comes
  // back, asked for again, to the queue this writer sends; and what waits
  // for a replay, so rows it parks late, a batch out of retries, set this
  // writer's paced replays going. Every writer that takes over shares them.
  adopt(previous: Writer): void {
    const c = this.config()
    const inherited = previous.queue.length
    // Any rows this writer queued already are newer: they go behind, under
    // this writer's bounds.
    previous.queue.limit(c.maxQueueRows, c.maxQueueBytes)
    previous.queue.append(this.queue.takeAll())
    this.queue = previous.queue
    this.counters.captured += inherited
    // The sessions with a row sent or parked: the same set too, so a batch
    // any writer before still has out that lands counts here, after any
    // number of takeovers.
    for (const s of this.sentSessions) previous.sentSessions.add(s)
    this.sentSessions = previous.sentSessions
    // The status line too (session.start clears it after the takeover,
    // `clearStatus`): a line the writer before sets late is then one this
    // writer's next landing clears.
    this.status = previous.status
    for (const dir of this.replayDue) previous.replayDue.add(dir)
    this.replayDue = previous.replayDue
    previous.parked.seq += this.parked.seq
    previous.parked.recoverable ||= this.parked.recoverable
    this.parked = previous.parked
    this.before = [...previous.before, previous]
    // The dead-letter writes under way too, so the exit waits for those any
    // of them began, before or after the takeover.
    for (const write of this.persisting) previous.persisting.add(write)
    this.persisting = previous.persisting
    // And the outcomes it holds, one whose record no store or file took
    // included: the same map, so an outcome a replay still under way there
    // comes to is held here too. Its marks stay its own: one with no
    // outcome held keeps this writer from sending the file, as another's.
    this.rewrites = previous.rewrites
    // A denial of writes BigQuery gave rows (insertAll, not tables.get)
    // stands across the takeover, with the rows parked under it, while the
    // table and dead-letter directory stay the same: a /bqaa-status check
    // that finds the table does not clear it (foundTable), only a request
    // BigQuery takes does. Another table starts clean.
    const denied = previous.problem
    if (this.problem === null && denied?.kind === 'permission' && denied.missing === 'write' && previous.partition === deadLetterPartition(c)) {
      this.problem = { ...denied }
      this.parkedDuringProblem = previous.parkedDuringProblem
      for (const id of previous.parkedIds) this.parkedIds.add(id)
      this.parkedRough = previous.parkedRough
      if (previous.announced.has('permission')) this.announced.add('permission')
    }
  }

  // Asks for a flush shortly, outside the caller's dispatch.
  requestFlush(delayMs = 0): void {
    if (this.soon) return
    this.soon = this.io.after(delayMs, () => {
      this.soon = null
      void this.flush()
    })
  }

  // One flush at a time; a call while one runs joins it.
  flush(): Promise<void> {
    if (this.inFlight) return this.inFlight
    const run = this.flushLoop().catch(err => this.recordError(`flush failed: ${errorText(err)}`))
    this.inFlight = run.finally(() => {
      this.inFlight = null
    })
    return this.inFlight
  }

  // Off a hook's dispatch: gets a token, so one is at hand for the first
  // send, and with it the account's identity.
  prefetchToken(): void {
    const c = this.config()
    if (!canDeliver(c) || c.dryRun) return
    void this.getToken()
      // Once the exit began, no lookup starts.
      .then(token => (this.closing ? null : this.identityFor(token, c)))
      .catch(err => this.recordError(`authentication: ${messageOf(err)}`))
  }

  // A prompt while a problem stands: the person may have just fixed it, so
  // the paused writer tries again now rather than at the end of its backoff.
  retrySoon(): void {
    if (this.problem === null || this.closing) return
    this.resume()
    this.requestFlush(0)
  }

  // Asks BigQuery for the table (tables.get, which the row writer's role
  // grants too) while nothing has told this writer yet whether rows can be
  // written: no request answered, no error, no notice; or whenever asked
  // (`explicit`, /bqaa-status check). A token gets up to
  // `Math.max(timeoutMs, c.tokenCommandTimeoutMs)` and each tables.get
  // request up to `timeoutMs`; after a 401 it tries once more with a fresh
  // token, in what is left of the whole check's time: that token time and two
  // requests' (21 s by default), however slowly the first token came. A
  // refused token, denied access, or a missing dataset or table raises the
  // notice a send would; any other answer is the last error, and so is no
  // answer in time. A missing table that the first send makes
  // (autoCreateTable) is no notice. The table found clears only what finding
  // it proves fixed (see foundTable). What it found, for /bqaa-status, or
  // null where it did not ask. Never throws.
  async probeDestination(timeoutMs = 3000, explicit = false): Promise<string | null> {
    const c = this.config()
    if (!canDeliver(c) || c.dryRun || this.closing || (!explicit && (this.problem !== null || this.lastSuccessAt !== null || this.lastError !== null))) return null
    const tokenTimeoutMs = Math.max(timeoutMs, c.tokenCommandTimeoutMs)
    // When the whole check is over, the fresh token after a 401 included.
    const deadline = this.io.now() + tokenTimeoutMs + timeoutMs * 2
    const url = `${tablesUrl(c)}/${encodeURIComponent(c.table)}?fields=tableReference`
    for (let attempt = 0; attempt < 2; attempt += 1) {
      // A token's time, out of what is left: a request's time is kept for
      // the request it is for.
      const tokenMs = Math.min(tokenTimeoutMs, Math.max(0, deadline - this.io.now() - timeoutMs))
      if (tokenMs <= 0) {
        this.recordError(`destination check: no token within ${tokenTimeoutMs} ms`)
        return `no token within ${tokenTimeoutMs} ms`
      }
      const tokenTimers: Timer[] = []
      let token: string | symbol | null
      try {
        const tokenDeadline = this.moment(tokenMs, tokenTimers)
        token = await Promise.race([this.getToken().catch(() => null), tokenDeadline.at.then(() => TIMED_OUT)])
      } finally {
        for (const timer of tokenTimers) timer.cancel()
      }
      // Null: getToken raised the credentials notice.
      if (token === null) return 'no working credentials'
      if (typeof token !== 'string') {
        this.recordError(`destination check: no token within ${tokenMs} ms`)
        return `no token within ${tokenMs} ms`
      }
      const requestMs = Math.min(timeoutMs, Math.max(1, deadline - this.io.now()))
      const httpTimers: Timer[] = []
      let answer: HttpAnswer | { error: string }
      try {
        const httpDeadline = this.moment(requestMs, httpTimers)
        answer = await Promise.race([
          this.request(url, { method: 'GET', headers: { authorization: `Bearer ${token}` } }, requestMs),
          httpDeadline.at.then(() => ({ error: `request timed out after ${requestMs} ms` })),
        ])
      } finally {
        for (const timer of httpTimers) timer.cancel()
      }
      if ('error' in answer) {
        this.recordError(`destination check: ${answer.error}`)
        return `no answer (${diagnostic(answer.error, 200)})`
      }
      if (answer.status >= 200 && answer.status < 300) {
        this.foundTable(c)
        return `BigQuery found ${tableRef(c)}`
      }
      const outcome = classifyInsert(answer, 0)
      if (outcome.kind === 'auth' && attempt === 0) {
        // The token may have run out early: once more with a fresh one.
        this.dropToken(token)
        continue
      }
      if (outcome.kind === 'auth') this.noteProblem('credentials', `BigQuery refused a fresh token too: ${outcome.message}`)
      else if (outcome.kind === 'not-found') {
        if (!isDatasetMissing(outcome.message) && c.autoCreateTable && !this.createAttempted) return `${tableRef(c)} is not there yet; the first send makes it`
        this.missingDestination(outcome.message)
      } else if (outcome.kind === 'rejected' && outcome.permission) {
        // Rows BigQuery refused already stay the reason: reading the table
        // denied too does not make writing allowed.
        this.noteProblem('permission', outcome.message, this.problem?.kind === 'permission' && this.problem.missing === 'write' ? 'write' : undefined)
      } else this.recordError(`destination check: ${'message' in outcome ? outcome.message : `HTTP ${answer.status}`}`)
      return diagnostic(`HTTP ${answer.status}`, 200)
    }
    return null
  }

  private async flushLoop(): Promise<void> {
    // The band's count of waiting rows moves with every tick.
    if (this.problem !== null) this.showProblem()
    // Nothing queued, and rows wait for a replay (`replayDue`): no send would
    // tell when what stopped them is fixed, so a replay does, at the paced
    // retries, in a session left idle too, until one sends them all. A
    // replay BigQuery takes clears the notice (`tookRequest`).
    const current = this.config()
    if (this.queue.length === 0 && this.replayDue.has(deadLetterPartition(current))) {
      if (this.pauseTimer !== null || this.closing || !current.autoBackfill || !canDeliver(current) || current.dryRun) return
      await this.backfill(['spill', 'retry'], 'recovery')
      // Rows still wait: the next replay comes after the backoff. Rows
      // queued meanwhile (the replay stops for them) go first, at the next
      // flush, and their send sets its own backoff.
      if (this.replayDue.has(deadLetterPartition(this.config())) && this.queue.length === 0) {
        this.consecutiveFailures += 1
        this.pause()
      }
      return
    }
    for (let i = 0; i < MAX_BATCHES_PER_TICK; i += 1) {
      const c = this.config()
      if (this.closing || this.queue.length === 0 || !canDeliver(c)) return
      if (this.pauseTimer !== null) {
        await this.relievePressure()
        return
      }
      const batch = this.queue.take(c.maxBatchRows, c.maxBatchBytes)
      this.inFlightRows = batch
      this.batchOpen = true
      let delivered = false
      try {
        delivered = await this.deliver(batch, c)
      } finally {
        this.inFlightRows = []
        this.batchOpen = false
      }
      if (!delivered) {
        if (this.closing) return
        // Backing off now: keep the queue from filling while we wait.
        await this.relievePressure()
        return
      }
    }
  }

  private async deliver(batch: Queued[], c: Config): Promise<boolean> {
    if (c.dryRun) return this.writeDryRun(batch, c)
    let token: string
    try {
      token = await this.getToken()
    } catch (err) {
      await this.retryLater(batch, `authentication: ${messageOf(err)}`)
      return false
    }
    // This process's rows carry the account of the token they go out with.
    // Once the exit began, no lookup starts: the batch goes back to the
    // queue the exit spills.
    if (!this.closing) await this.stampFor(batch, token, c)
    if (this.closing) {
      this.heldAtExit = true
      this.queue.unshift(batch)
      return false
    }
    let body = insertAllBody(batch.map(q => q.row))
    let answer = await this.post(insertAllUrl(c), body, token, c.requestTimeoutMs)
    if ('error' in answer) {
      await this.retryLater(batch, answer.error)
      return false
    }
    let outcome = classifyInsert(answer, batch.length)
    if (outcome.kind === 'auth' && !this.closing) {
      // The token may have run out early: one more try at once, with a fresh
      // one. A second 401 means BigQuery refuses the credentials themselves.
      // Once the exit began, no fresh token is fetched: the batch waits.
      this.dropToken()
      try {
        token = await this.getToken()
      } catch (err) {
        await this.retryLater(batch, `authentication: ${messageOf(err)}`)
        return false
      }
      // The fresh token can be another account's: the rows go out under it.
      if (!this.closing) await this.stampFor(batch, token, c)
      if (this.closing) {
        this.heldAtExit = true
        this.queue.unshift(batch)
        return false
      }
      body = insertAllBody(batch.map(q => q.row))
      answer = await this.post(insertAllUrl(c), body, token, c.requestTimeoutMs)
      if ('error' in answer) {
        await this.retryLater(batch, answer.error)
        return false
      }
      outcome = classifyInsert(answer, batch.length)
      if (outcome.kind === 'auth') this.noteProblem('credentials', `BigQuery refused a fresh token too: ${outcome.message}`)
    }
    switch (outcome.kind) {
      case 'ok':
        this.noteSessions(batch)
        this.succeeded(batch.length, c)
        return true
      case 'partial': {
        const rejected: Queued[] = []
        const retry: Queued[] = []
        const landed: Queued[] = []
        const reasons: string[] = []
        batch.forEach((q, i) => {
          const reason = outcome.rejected.get(i)
          if (reason !== undefined) {
            rejected.push(q)
            reasons.push(reason)
          } else if (outcome.retry.has(i)) retry.push(q)
          else landed.push(q)
        })
        this.noteSessions(landed)
        this.succeeded(batch.length - rejected.length - retry.length, c)
        // Every row handed on before either write is waited for: the refused
        // to their file, those asked for again back to the queue.
        const writes: Promise<void>[] = []
        if (rejected.length > 0) writes.push(this.reject(rejected, `BigQuery rejected ${rejected.length} row(s): ${reasons[0]}`))
        if (retry.length > 0) writes.push(this.retryLater(retry, 'BigQuery asked to retry some rows'))
        await Promise.all(writes)
        return retry.length === 0
      }
      case 'auth':
        this.dropToken()
        if (this.closing) this.noteProblem('credentials', `authentication: ${outcome.message}`)
        await this.retryLater(batch, `authentication: ${outcome.message}`)
        return false
      case 'not-found': {
        // Once the exit began, no table is created: the batch waits, and the
        // exit spills it.
        if (!this.closing && c.autoCreateTable && !this.createAttempted && !isDatasetMissing(outcome.message)) {
          this.createAttempted = true
          if (await this.createTable(c, token)) {
            this.queue.unshift(batch)
            return true
          }
        }
        // Not made yet, or made so recently that streaming inserts do not
        // see it (minutes, after setup.sh): the person is told how to make
        // it, and the rows wait as retries, sent on their own once it is
        // found. A table this writer created moments ago gets the retries
        // alone.
        this.missingDestination(outcome.message)
        await this.retryLater(batch, outcome.message)
        return false
      }
      case 'rejected':
        if (outcome.permission) this.noteProblem('permission', outcome.message, 'write')
        else this.tableAnswered()
        await this.reject(batch, outcome.message)
        return false
      case 'retry':
        await this.retryLater(batch, outcome.message)
        return false
    }
  }

  private succeeded(rows: number, c: Config): void {
    this.batchOpen = false
    const hadFailures = this.consecutiveFailures > 0
    this.counters.sent += rows
    this.counters.batches += 1
    this.tookRequest(rows)
    if (rows > 0) this.call(() => this.io.sent?.(rows, c))
    // BigQuery is reachable again: replay what this session parked on disk.
    // Not once the exit began: the next session replays it.
    if (!this.closing && hadFailures && this.parked.recoverable && this.config().autoBackfill) this.recoverSoon()
  }

  // A second from now, replays the spill and retry files this session
  // parked, once BigQuery is reachable again.
  private recoverSoon(): void {
    this.parked.recoverable = false
    this.recoveryTimer?.cancel()
    this.recoveryTimer = this.io.after(1000, () => {
      this.recoveryTimer = null
      if (!this.closing) void this.backfill(['spill', 'retry'], 'recovery')
    })
  }

  // BigQuery took a request, sent or replayed: the table is there and the
  // credentials work, so the backoff ends and so does a notice. Rows that
  // landed also end the status line and its count of refused rows; a request
  // whose rows were all refused or asked for again leaves both.
  private tookRequest(landed: number): void {
    this.consecutiveFailures = 0
    this.resume()
    this.lastSuccessAt = this.io.now()
    const stood = this.problem !== null
    this.clearProblem()
    if (landed > 0) {
      this.refusedSinceSuccess = 0
      this.refusedIds.clear()
      this.refusedRough = false
    }
    if (landed > 0 || stood) this.setStatus(undefined)
  }

  private backoffMs(): number {
    const base = Math.min(60_000, 1000 * 2 ** Math.max(0, this.consecutiveFailures - 1))
    return Math.round(base * (1 + Math.random() * 0.25))
  }

  private pause(): void {
    if (this.closing) return
    const quick = this.problem?.kind === 'credentials' || this.problem?.kind === 'destination'
    const ms = quick ? Math.min(QUICK_RETRY_CAP_MS, this.backoffMs()) : this.backoffMs()
    this.pauseTimer?.cancel()
    this.pausedUntil = this.io.now() + ms
    this.pauseTimer = this.io.after(ms, () => {
      this.pauseTimer = null
      this.pausedUntil = 0
    })
  }

  private resume(): void {
    this.pauseTimer?.cancel()
    this.pauseTimer = null
    this.pausedUntil = 0
  }

  private async retryLater(batch: Queued[], message: string): Promise<void> {
    this.batchOpen = false
    this.inFlightRows = []
    const c = this.config()
    const reason = diagnostic(message)
    this.consecutiveFailures += 1
    this.counters.retries += 1
    this.pause()
    this.recordError(reason)
    const exhausted: Queued[] = []
    const keep: Queued[] = []
    for (const q of batch) {
      q.attempts += 1
      if (q.attempts > c.maxRetries) exhausted.push(q)
      else keep.push(q)
    }
    this.queue.unshift(keep)
    if (exhausted.length > 0) {
      this.counters.failed += exhausted.length
      const lost = await this.deadLetter('retry', exhausted, reason)
      if (this.problem !== null) {
        // The notice says what stops them.
      } else if (lost > 0) this.setStatus(`BQAA: ${lost} row(s) out of retries lost: their dead-letter file could not be written (${reason.slice(0, 80)}); see /bqaa-status`)
      else this.setStatus(`BQAA: ${exhausted.length} row(s) parked after retries (${reason.slice(0, 80)}); ${c.autoBackfill ? 'they are replayed on their own' : '/bqaa-backfill sends them'}`)
    }
    if (this.problem !== null) this.showProblem()
  }

  private async reject(batch: Queued[], message: string): Promise<void> {
    this.batchOpen = false
    this.inFlightRows = []
    const reason = diagnostic(message)
    this.consecutiveFailures += 1
    this.pause()
    this.counters.failed += batch.length
    this.counters.rejected += batch.length
    this.recordError(reason)
    // Counted once in a file (`writeDeadLetter`): /bqaa-backfill can send
    // only those.
    const lost = await this.deadLetter('rejected', batch, reason)
    if (this.problem !== null) this.showProblem()
    else if (lost > 0) this.setStatus(`BQAA: ${lost} refused row(s) lost: their dead-letter file could not be written (${reason.slice(0, 80)}); see /bqaa-status`)
    else this.setStatus(`BQAA: ${this.refusedText()} row(s) refused (${reason.slice(0, 80)}); fix, then /bqaa-backfill`)
  }

  // While BigQuery is unreachable, rows past 80% of the queue go to disk
  // rather than wait to be dropped.
  private async relievePressure(): Promise<void> {
    if (!this.queue.isUnderPressure()) return
    const spill = this.queue.take(Math.ceil(this.queue.length / 2), Number.MAX_SAFE_INTEGER)
    this.counters.spilled += spill.length
    await this.deadLetter('spill', spill, 'queue pressure while BigQuery was unreachable')
  }

  // Error text reaches /bqaa-status (which the model reads) and the debug
  // log: redacted like any payload, in case a command echoed a credential.
  private recordError(message: string): void {
    const clean = diagnostic(message)
    this.lastError = { message: clean, at: this.io.now() }
    this.call(() => this.io.debug(`bqaa: ${clean}`))
  }

  // A line the surface does not take leaves the one shown as it was, and the
  // next status is written again.
  private setStatus(text: string | undefined, opts?: { inBand?: boolean }): void {
    const raw = opts?.inBand && this.io.canShowBand?.() ? undefined : text
    const clean = raw === undefined ? undefined : diagnostic(raw, 300)
    if (clean === this.status.text) return
    this.call(() => {
      this.io.status(clean)
      this.status.text = clean
    })
  }

  // At session.start, after any takeover: the host keeps the plugin's status
  // line until it is replaced, a reload included, and this writer holds no
  // notice yet, unless it took over a denial of writes (`adopt`), which it
  // shows again. A clear that does not land leaves the line not known, so
  // the next status the writers set is written whatever it is.
  clearStatus(): void {
    this.status.text = null
    if (this.problem !== null) {
      this.showProblem()
      return
    }
    this.setStatus(undefined)
  }

  // ---- credentials and permission ----------------------------------------

  // Records why rows cannot be written and shows it: the status line and the
  // band now, and the first time each kind comes up, a toast and a
  // transcript line with the reason. Never throws.
  private noteProblem(kind: ProblemKind, reason: string, missing?: Problem['missing']): void {
    const clean = diagnostic(reason, 300)
    if (this.problem === null || this.problem.kind !== kind) {
      this.problem = { kind, reason: clean, since: this.io.now(), missing }
      this.parkedDuringProblem = 0
      this.parkedIds.clear()
      this.parkedRough = false
    } else {
      this.problem.reason = clean
      this.problem.missing = missing
    }
    this.showProblem()
    if (this.announced.has(kind)) return
    this.announced.add(kind)
    const { headline, fix } = this.problemText()
    const run = this.commandLine()
    this.call(() => this.io.toast?.(diagnostic(`BQAA: rows are not being written: ${headline}. To fix, ${fix}.`, 500)))
    // The command line goes whole, after the rest is cut; on a line of its
    // own where redaction would take it with the rest (a project named like
    // a credential, `auth-…:` in the reason BigQuery gave).
    const told = diagnostic(`bqaa: rows are not being written: ${headline} (${clean}). To fix, ${fix}`, 900)
    const notice = `${told}${run ? `. ${run}` : ''}; this clears once rows are written again.`
    if (run === null || diagnostic(notice, 2000).includes(run)) this.call(() => this.io.notice?.(notice))
    else {
      this.call(() => this.io.notice?.(`${told}; this clears once rows are written again.`))
      this.call(() => this.io.notice?.(`bqaa: ${run}`))
    }
  }

  // BigQuery refused rows for a reason of their own (a 400, say): it found
  // the table, took the credentials and let the identity write, so a notice
  // about any of them is stale.
  private tableAnswered(): void {
    if (this.problem !== null) this.clearProblem()
  }

  // tables.get found the table (probeDestination): the dataset and the table
  // are there and the credentials work, so a notice about either clears, and
  // with it the backoff and the status line it set; the rows queued go now,
  // and rows parked meanwhile are replayed, as after a send BigQuery takes.
  // Finding the table says nothing of writing to it
  // (bigquery.tables.updateData): a denial BigQuery gave rows, or one under
  // which rows were parked, stands until BigQuery takes a request. No row
  // was written: the transcript is told what was found, never that rows are
  // written again (clearProblem).
  private foundTable(c: Config): void {
    const was = this.problem
    if (was === null || (was.kind === 'permission' && (was.missing === 'write' || this.parkedDuringProblem > 0))) return
    const hadFailures = this.consecutiveFailures > 0
    this.consecutiveFailures = 0
    this.resume()
    this.clearProblem({ fromProbe: true })
    this.setStatus(undefined)
    if (this.closing) return
    if (this.queue.length > 0 && !this.hasOnlyUnsentSessionStart(this.sessionId())) this.requestFlush(0)
    if (hadFailures && this.parked.recoverable && c.autoBackfill) this.recoverSoon()
  }

  // A 404 from insertAll, sent or replayed: the notice for the dataset or
  // table, unless this writer made the table in the last two minutes.
  private missingDestination(message: string): void {
    if (this.tableCreatedAt !== null && this.io.now() - this.tableCreatedAt < NEW_TABLE_MS) return
    this.noteProblem('destination', message, isDatasetMissing(message) ? 'dataset' : 'table')
  }

  // The next successful send: the problem is gone. Rows refused while
  // access was denied stay parked until /bqaa-backfill, and the transcript
  // says so; rows parked for want of credentials follow on their own. A
  // table found (`fromProbe`) wrote no row: the transcript is told what was
  // found, never that rows are written again.
  private clearProblem(opts: { fromProbe?: boolean } = {}): void {
    const was = this.problem
    if (was === null) return
    const parked = this.parkedDuringProblem
    const about = this.parkedRough ? 'about ' : ''
    this.problem = null
    this.parkedDuringProblem = 0
    this.parkedIds.clear()
    this.parkedRough = false
    this.call(() => this.io.redraw?.())
    if (parked > 0 && (was.kind === 'permission' || !this.config().autoBackfill)) {
      const notice =
        opts.fromProbe === true
          ? `bqaa: BigQuery found the table; /bqaa-backfill sends the ${about}${parked} row(s) parked while rows could not be written.`
          : `bqaa: rows are being written again; /bqaa-backfill sends the ${about}${parked} row(s) parked while they could not be.`
      this.call(() => this.io.notice?.(notice))
    }
  }

  // The status line and the band, with the count of rows waiting now.
  private showProblem(): void {
    if (this.problem === null) return
    const { headline, fix } = this.problemText()
    this.setStatus(`BQAA: ${headline}. To fix, ${fix} (${this.waitingText()} row(s) waiting)`, { inBand: true })
    this.call(() => this.io.redraw?.())
  }

  // What is wrong, and what the person runs or asks for to fix it, for the
  // token source this writer uses (the command named by its program only).
  problemText(): { headline: string; fix: string } {
    const c = this.config()
    const kind = this.problem?.kind ?? 'credentials'
    if (kind === 'permission') return { headline: HEADLINE.permission, fix: `ask for bigquery.tables.get and bigquery.tables.updateData on the table ${c.project}.${c.dataset}.${c.table} (no role that reads rows), then run /bqaa-backfill` }
    if (kind === 'destination') {
      const what = this.problem?.missing === 'table' ? `the table ${c.project}.${c.dataset}.${c.table}` : `the dataset ${c.project}.${c.dataset}`
      const make = this.setupCommand() === null ? 'create it, or correct the project and dataset options' : "create it with the mod's setup.sh"
      return { headline: `BigQuery cannot find ${what}`, fix: `${make}; rows wait, and are sent once BigQuery finds it (a few minutes after it is made)` }
    }
    if (this.tokenSource === 'environment') return { headline: HEADLINE.credentials, fix: 'set a fresh BQAA_ACCESS_TOKEN, or unset it to use the token command' }
    const argv = c.accessTokenCommand
    const program = programName(argv)
    if (argv.length === 0) return { headline: HEADLINE.credentials, fix: 'set BQAA_ACCESS_TOKEN or accessTokenCommand' }
    if (/^gcloud(\.cmd|\.exe)?$/i.test(program) && argv.includes('print-access-token')) {
      return { headline: HEADLINE.credentials, fix: argv.includes('application-default') ? 'run: gcloud auth application-default login' : 'run: gcloud auth login' }
    }
    return { headline: HEADLINE.credentials, fix: `check the token command \`${program}\`` }
  }

  // For a dataset or table BigQuery cannot find: the command that makes it,
  // the mod's setup.sh run by bash with every name it reads set on the line,
  // the views dataset included (so none comes from a variable set in the
  // person's shell), or null when the mod's directory is unknown or not a
  // POSIX path, a name is one setup.sh would refuse, or the command is too
  // long, or holds text redaction would change, to be shown as it is.
  setupCommand(): string | null {
    if (this.problem?.kind !== 'destination') return null
    return buildSetupCommand(this.config(), this.io.pluginRoot)
  }

  // The band's and the transcript's line for the setup command: the command
  // itself when the line holds it whole, else where to find it (a command
  // cut short would not run). Null when no command is offered.
  commandLine(): string | null {
    const command = this.setupCommand()
    if (command === null) return null
    const line = `Run: ${command}`
    return line.length <= COMMAND_LINE_CHARS ? line : 'Run the setup command /bqaa-status shows (too long for this line)'
  }

  // Rows that wait for the problem to clear: queued, in flight, and parked
  // on disk since it began.
  waitingRows(): number {
    return this.queue.length + (this.batchOpen ? this.inFlightRows.length : 0) + this.parkedDuringProblem
  }

  // The count as shown: "about N" once it went past the rows it keeps.
  waitingText(): string {
    return `${this.parkedRough ? 'about ' : ''}${this.waitingRows()}`
  }

  private refusedText(): string {
    return `${this.refusedRough ? 'about ' : ''}${this.refusedSinceSuccess}`
  }

  // A notice the surface may not take (no terminal, a `-p` run) is skipped.
  private call(show: () => void): void {
    try {
      show()
    } catch {
      // The status line and /bqaa-status still say it.
    }
  }

  // ---- identity ----------------------------------------------------------

  // With userIdFromCredentials: the verified email of the account `token`
  // was issued to. Each token is looked up once: a new token (a refresh, the
  // fresh one after a 401, another BQAA_ACCESS_TOKEN) is a new credential and
  // is looked up again; a failed lookup is tried again after ten minutes, by
  // a timer, so it follows the host's clock. Null when off, unknown or
  // failed. Never throws.
  private identityFor(token: string, c: Config): Promise<string | null> {
    if (!c.userIdFromCredentials) return Promise.resolve(null)
    if (this.identity !== null && this.identity.token === token) return Promise.resolve(this.identity.email)
    if (this.identityPending === null || this.identityPending.token !== token) {
      const answer = this.lookUpIdentity(token, c, ++this.identitySeq).catch(() => null)
      const pending = { token, answer }
      this.identityPending = pending
      void answer.then(() => {
        if (this.identityPending === pending) this.identityPending = null
      })
    }
    return this.identityPending.answer
  }

  // The account of the token in use now, if its lookup has answered. An
  // expired token is no longer in use, whether or not a fresh one came.
  private currentIdentity(): string | null {
    const token = this.token
    if (token === null || this.io.now() >= token.expiresAt) return null
    return this.identity !== null && this.identity.token === token.value ? this.identity.email : null
  }

  // A token that expired or that BigQuery refused is let go, and so is the
  // account looked up for it: while no fresh token comes, rows parked meanwhile
  // carry the user_id they were built with, never that account.
  private dropToken(stale?: string): void {
    const dropped = this.token
    if (stale !== undefined && dropped !== null && dropped.value !== stale) return
    this.token = null
    if (dropped !== null && this.identity !== null && this.identity.token === dropped.value) this.identity = null
  }

  // Gives each of this process's rows the account of `token`, or, while that
  // account is unknown, the user_id it was built with: never an account
  // looked up for another token.
  private async stampFor(rows: Queued[], token: string, c: Config): Promise<void> {
    if (!c.userIdFromCredentials) return
    const email = await this.identityFor(token, c)
    for (const q of rows) this.stamp(q, email)
  }

  // Google's token information endpoint names the account; the token goes
  // in the POST body, never in the URL or a header. The address is never
  // logged.
  private async lookUpIdentity(token: string, c: Config, seq: number): Promise<string | null> {
    const answer = await this.request(TOKEN_INFO_URL, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: `access_token=${encodeURIComponent(token)}` }, c.requestTimeoutMs)
    let email: string | null = null
    let why = ''
    if ('error' in answer) why = answer.error
    else if (answer.status !== 200) why = `HTTP ${answer.status}`
    else {
      let info: { email?: unknown; email_verified?: unknown } | null = null
      try {
        info = JSON.parse(answer.text) as { email?: unknown; email_verified?: unknown }
      } catch {
        info = null
      }
      if (typeof info?.email !== 'string' || !EMAIL.test(info.email)) why = 'it names no account email (the token lacks the email scope)'
      else if (info.email_verified !== true && info.email_verified !== 'true') why = "the account's email is not verified"
      else email = info.email
    }
    // A lookup that started earlier, for an older token, never replaces the
    // answer of a later one.
    if (this.identity === null || this.identity.seq < seq) {
      const known = { token, email, seq }
      this.identity = known
      if (email === null) {
        this.io.after(IDENTITY_RETRY_MS, () => {
          if (this.identity === known) this.identity = null
        })
      }
    }
    if (email === null) this.io.debug(diagnostic(`bqaa: looking up the token's account for user_id failed (${why}); rows keep the configured user_id`))
    return email
  }

  // This process's row with `email` as its user_id, or with the user_id it
  // was built with when `email` is null.
  private stamp(q: Queued, email: string | null): void {
    if (email !== null && q.configuredUserId === undefined) q.configuredUserId = q.row.user_id
    const userId = email ?? (q.configuredUserId === undefined ? q.row.user_id : q.configuredUserId)
    if (q.row.user_id === userId) return
    q.row = { ...q.row, user_id: userId }
    q.bytes = rowBytes(q.row)
  }

  private post(url: string, body: string, token: string, timeoutMs: number): Promise<HttpAnswer | { error: string }> {
    return this.request(url, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body }, timeoutMs)
  }

  private async request(url: string, init: HttpInit, timeoutMs: number): Promise<HttpAnswer | { error: string }> {
    let timer: Timer | null = null
    const timeout = new Promise<typeof TIMED_OUT>(resolve => {
      timer = this.io.after(timeoutMs, () => resolve(TIMED_OUT))
    })
    const request = this.io.fetch(url, init).then(
      answer => answer,
      err => ({ error: `request failed: ${errorText(err)}` }),
    )
    try {
      const result = await Promise.race([request, timeout])
      if (result === TIMED_OUT) return { error: `request timed out after ${timeoutMs} ms` }
      return result
    } finally {
      ;(timer as Timer | null)?.cancel()
    }
  }

  private getToken(): Promise<string> {
    const c = this.config()
    if (this.token && this.io.now() < this.token.expiresAt) return Promise.resolve(this.token.value)
    // Expired: let it go now, so a refresh that fails leaves no token behind.
    if (this.token) this.dropToken()
    if (this.tokenPending) return this.tokenPending
    const pending = (async (): Promise<string> => {
      const fromEnv = (await this.io.envToken())?.trim()
      if (fromEnv) {
        // Re-read every five minutes, so a token the person rotates is picked up.
        this.tokenSource = 'environment'
        this.token = { value: fromEnv, expiresAt: this.io.now() + 5 * 60 * 1000 }
        return fromEnv
      }
      this.tokenSource = 'command'
      if (c.accessTokenCommand.length === 0) throw new Error('no access token source: set BQAA_ACCESS_TOKEN or accessTokenCommand')
      const command = `token command \`${programName(c.accessTokenCommand)}\``
      const result = await this.io.runCommand(c.accessTokenCommand, c.tokenCommandTimeoutMs)
      if (result.exitCode !== 0) throw new Error(`${command} exited ${result.exitCode}: ${diagnostic(errorLine(result.stderr), 300)}`)
      const value = result.stdout.trim().split(/\s+/)[0] ?? ''
      if (!/^[A-Za-z0-9._~+/=-]{16,}$/.test(value)) throw new Error(`${command} printed no access token`)
      this.token = { value, expiresAt: this.io.now() + c.tokenTtlMs }
      return value
    })()
    // No token is no working credentials, whoever asked for it.
    pending.catch(err => this.noteProblem('credentials', `authentication: ${messageOf(err)}`))
    this.tokenPending = pending
    const clear = (): void => {
      if (this.tokenPending === pending) this.tokenPending = null
    }
    pending.then(clear, clear)
    return pending
  }

  private async createTable(c: Config, token: string, timeoutMs = c.requestTimeoutMs): Promise<boolean> {
    const answer = await this.post(tablesUrl(c), createTableBody(c), token, timeoutMs)
    if ('error' in answer) {
      this.recordError(`create table: ${answer.error}`)
      return false
    }
    if ((answer.status >= 200 && answer.status < 300) || answer.status === 409) {
      this.tableCreatedAt = this.io.now()
      this.io.debug(`bqaa: created ${tableRef(c)} (day-partitioned on timestamp, clustered on event_type, agent, user_id)`)
      return true
    }
    this.recordError(`create table: HTTP ${answer.status}: ${answer.text}`)
    return false
  }

  private noteSessions(rows: readonly Queued[]): void {
    for (const q of rows) {
      if (typeof q.row.session_id === 'string' && q.row.session_id !== '') this.sentSessions.add(q.row.session_id)
    }
  }

  // Dry run: the rows as JSONL files inside the dead-letter directory's
  // dry-run subdirectory, each file within the file limit in bytes.
  private async writeDryRunFiles(rows: readonly Queued[], c: Config): Promise<void> {
    for (const text of packLines(rows.map(q => JSON.stringify(fitRow(q.row, MAX_FILE_BYTES - 1))))) {
      const name = fileName('spill', this.io.now(), this.sessionId(), ++this.seq, this.mark).replace(/^spill-/, 'dry-run-')
      await this.io.writeFile(joinPath(c.dryRunDir, name), text)
    }
    this.noteSessions(rows)
  }

  private async writeDryRun(batch: Queued[], c: Config): Promise<boolean> {
    try {
      await this.writeDryRunFiles(batch, c)
      this.succeeded(batch.length, c)
      return true
    } catch (err) {
      await this.retryLater(batch, `dry run write failed: ${errorText(err)}`)
      return false
    }
  }

  // A dead-letter write, held among those under way until it ends: how many
  // of the rows no file took.
  private deadLetter(kind: DeadLetterKind, rows: Queued[], reason: string): Promise<number> {
    const write = this.writeDeadLetter(kind, rows, reason)
    const ended = write.then(
      () => undefined,
      () => undefined,
    )
    this.persisting.add(ended)
    void ended.then(() => this.persisting.delete(ended))
    return write
  }

  // Into this table's own subdirectory, each line naming the table: how many
  // of the rows no file took.
  private async writeDeadLetter(kind: DeadLetterKind, rows: Queued[], reason: string): Promise<number> {
    if (rows.length === 0) return 0
    const c = this.config()
    // The rows are this process's: they are parked with the account of the
    // token in use now, and keep it when they are replayed.
    if (c.userIdFromCredentials) for (const q of rows) this.stamp(q, this.currentIdentity())
    const at = this.io.now()
    const failedAt = new Date(at).toISOString()
    const dir = deadLetterPartition(c)
    const lines: DeadLetterLine[] = rows.map(q => ({ v: 1, kind, reason, attempts: q.attempts, failed_at: failedAt, table: tableRef(c), row: q.row }))
    let first = 0
    let lost = 0
    for (const body of packBodies(lines.map(lineText))) {
      const count = body.split('\n').length - 1
      const slice = rows.slice(first, first + count)
      const ids = slice.map(q => q.row.event_id)
      first += count
      const path = joinPath(dir, fileName(kind, at, this.sessionId(), ++this.seq, this.mark))
      try {
        if (c.deadLetterDir === '') throw new Error('no dead-letter directory')
        // The completion record goes last: a replay reads the file only once
        // the record is there and matches every row above it.
        await this.io.writeFile(path, sealFile(body))
        this.noteSessions(slice)
        this.counters.deadLetterFiles += 1
        if (kind !== 'rejected') {
          this.parked.recoverable = true
          this.replayDue.add(dir)
          this.parked.seq += 1
        }
        if (kind === 'rejected') this.countRefused(ids)
        if (this.problem !== null) this.countParked(ids)
      } catch (err) {
        lost += count
        this.counters.dropped += count
        this.recordError(`dead-letter write failed (${count} rows lost): ${errorText(err)}`)
      }
    }
    return lost
  }

  // ---- session end -------------------------------------------------------

  // True when `sessionId` has not sent, dry-run-written or parked any row yet,
  // and the queue holds only its `session_start` row(s).
  hasOnlyUnsentSessionStart(sessionId: string | null): boolean {
    if (sessionId === null || this.sentSessions.has(sessionId) || this.queue.length === 0) return false
    const items = this.queue.takeAll()
    this.queue.append(items)
    return items.every(q => q.row.session_id === sessionId && q.row.event_type === 'STATE_DELTA' && q.row.content === '{"session_start":true}')
  }

  // Drops the unsent `session_start` row(s) for `sessionId` when the session
  // ran only mod slash commands and has not sent or in-flight-flushed any row
  // for this session yet. `startCaptured` says whether the session wrote a
  // `session_start` row at all: a managed CLI that keeps classic hooks from
  // a user plugin raises no classic.SessionStart, and with none to drop such
  // a session ends silently too. Returns true when the session can end
  // silently.
  dropUnsentSessionStart(sessionId: string | null, startCaptured = true): boolean {
    if (sessionId === null || this.sentSessions.has(sessionId)) return false
    const writers = [...this.before, this]
    if (writers.some(w => w.inFlightRows.some(q => q.row.session_id === sessionId))) return false
    const items = this.queue.takeAll()
    if (
      items.some(
        item =>
          item.row.session_id === sessionId &&
          !(item.row.event_type === 'STATE_DELTA' && item.row.content === '{"session_start":true}'),
      )
    ) {
      this.queue.append(items)
      return false
    }
    const kept: Queued[] = []
    let removed = 0
    for (const item of items) {
      if (item.row.session_id === sessionId && item.row.event_type === 'STATE_DELTA' && item.row.content === '{"session_start":true}') {
        removed += 1
      } else {
        kept.push(item)
      }
    }
    this.queue.append(kept)
    this.counters.captured = Math.max(0, this.counters.captured - removed)
    return startCaptured ? removed > 0 : true
  }

  // Called from session.end when the process is ending, inside the engine's
  // short end-of-session bound. A batch whose request is out may yet be
  // answered: the exit waits for that answer up to half its budget, so rows
  // BigQuery took are not sent again from a spill file. Waits at most
  // `budgetMs` in all, for those writes and for the dead-letter writes under
  // way. Never throws.
  //
  // With `sendQueued`, while BigQuery takes this session's requests with
  // nothing in the way (no notice, backoff, failure, replay, dead-letter
  // write or recovery due), the rows still queued wait for that answer, then
  // go in one request, with a token already at hand: no token command runs.
  // What it does not land in time is spilled before the budget ends. With
  // `replaySpills` too, once every row has landed, this table's spill and
  // retry files are replayed with what is left of the budget (see
  // replayAtExit).
  //
  // Otherwise it starts no request: the rows still queued go at once to a
  // spill file the next session sends, or in a dry run to the dry-run files,
  // and so do the rows no answer took (they may yet land; event_id dedups).
  async spillForExit(budgetMs: number, opts: ExitFlushOptions = {}): Promise<{ spilled: number }> {
    const c = this.config()
    const start = this.io.now()
    const until = start + budgetMs
    // The writers this one took over from start nothing more either, and a
    // batch one still has out goes with the rest once its wait is over.
    const writers = [...this.before, this]
    // Read before the exit changes anything.
    const sending = opts.sendQueued === true && budgetMs >= EXIT_SEND_MIN_MS && this.canSendAtExit(c, opts.replaySpills === true)
    this.closing = true
    this.soon?.cancel()
    this.soon = null
    for (const writer of writers) {
      writer.closing = true
      writer.recoveryTimer?.cancel()
      writer.recoveryTimer = null
      writer.pauseTimer?.cancel()
      writer.pauseTimer = null
      writer.pausedUntil = 0
    }
    const toDryRun = c.dryRun && c.dryRunDir !== ''
    const writes: Promise<void>[] = []
    let spilled = 0
    const park = (rows: Queued[]): void => {
      if (rows.length === 0) return
      if (!toDryRun) spilled += rows.length
      writes.push(this.exitWrite(rows, c, toDryRun))
    }
    const timers: Timer[] = []
    const marks: ExitMarks | null = sending
      ? {
          token: this.moment(budgetMs - EXIT_SEND_MIN_MS, timers),
          post: this.moment(budgetMs - EXIT_SPILL_RESERVE_MS, timers),
          replay: this.moment(budgetMs - EXIT_REPLAY_RESERVE_MS, timers),
          end: this.moment(budgetMs, timers),
        }
      : null
    if (marks === null) park(this.queue.takeAll())
    // The writers with a batch out now: an answer that comes for one during
    // the wait below can leave BigQuery's state in doubt.
    const flightWriters = writers.filter(writer => writer.inFlightRows.length > 0 && writer.inFlight !== null)
    const flights = flightWriters.flatMap(writer => (writer.inFlight !== null ? [writer.inFlight] : []))
    if (flights.length > 0 && budgetMs > 0) await this.raceUntil(Promise.all(flights), start + Math.floor(budgetMs / 2))
    // What no answer took: the rows still out, and those an answer asked
    // for again. Rows an answer refused, or parks after their retries, are
    // in a file being written already: a copy here would send them again,
    // so the exit waits for that write instead, within the same bound.
    const out: Queued[] = []
    for (const writer of writers) out.push(...writer.inFlightRows)
    // Every batch out answered and landed whole, and this writer has no
    // notice or failure: the exit sends. Any other answer (or none) leaves
    // BigQuery's state in doubt, and the rows wait. A writer taken over from
    // (`adopt`) with no batch out counts only for a batch it held at the
    // exit: a notice or failures it saw came before this writer's sends.
    const settled = (w: Writer): boolean => w.problem === null && w.consecutiveFailures === 0 && !w.heldAtExit
    if (marks !== null && out.length === 0 && this.persisting.size === 0 && settled(this) && this.before.every(w => !w.heldAtExit) && flightWriters.every(settled)) {
      if ((await this.sendAtExit(c, marks, park, writes, until)) && opts.replaySpills === true && spilled === 0) await this.replayAtExit(c, marks, until)
    } else park([...out, ...this.queue.takeAll()])
    const pending = [...writes, ...this.persisting]
    if (pending.length > 0) {
      if (marks === null) await this.raceUntil(Promise.all(pending), until)
      else await Promise.race([Promise.all(pending).catch(() => undefined), marks.end.at])
    }
    for (const timer of timers) timer.cancel()
    return { spilled }
  }

  // Set once the session is ending (spillForExit began).
  get isClosing(): boolean {
    return this.closing
  }

  // Whether session end may send (spillForExit): BigQuery is to be written
  // to, and nothing leaves its answer in doubt: on this writer, no notice,
  // backoff, failure, replay or recovery due, dead-letter write or rows
  // parked to replay first. A writer this one took over from (`adopt`)
  // stops it only with a recovery or replay of its own due, a batch held at
  // the exit, or a batch out after a failure or under a notice: what it saw
  // before the takeover, with no batch out, is stale once this writer sends.
  private canSendAtExit(c: Config, replaySpills = false): boolean {
    if (!canDeliver(c) || c.dryRun || this.persisting.size > 0 || this.parked.recoverable) return false
    const quiet = (w: Writer): boolean =>
      !w.closing && !w.heldAtExit && w.recoveryTimer === null && (w.backfilling === null || (replaySpills && this.lastSuccessAt !== null))
    return (
      quiet(this) &&
      this.problem === null &&
      this.pauseTimer === null &&
      this.consecutiveFailures === 0 &&
      this.before.every(w => quiet(w) && (!w.batchOpen || (w.problem === null && w.consecutiveFailures === 0)))
    )
  }

  // A moment `ms` from now on the host's clock, its timer added to `timers`.
  private moment(ms: number, timers: Timer[]): Mark {
    let passed = false
    const at = new Promise<void>(resolve => {
      timers.push(
        this.io.after(Math.max(0, ms), () => {
          passed = true
          resolve()
        }),
      )
    })
    return { at, passed: () => passed }
  }

  // Session end's one request: the rows still queued, as many as a batch
  // takes (any more are spilled at once), with a token at hand
  // (`exitToken`), answered by the `post` mark at the latest. What it does
  // not land is parked by `park` (refused rows to a `rejected` file), in the
  // time left after that mark. True when every row landed (or none waited).
  private async sendAtExit(c: Config, marks: ExitMarks, park: (rows: Queued[]) => void, writes: Promise<void>[], until: number): Promise<boolean> {
    if (this.queue.length === 0) return true
    const token = await this.exitToken(marks.token)
    if (token === null || marks.token.passed() || until - this.io.now() < EXIT_SEND_MIN_MS) {
      park(this.queue.takeAll())
      return false
    }
    const batch = this.queue.take(c.maxBatchRows, c.maxBatchBytes)
    park(this.queue.takeAll())
    // The rows carry the account of the token in use, where its lookup has
    // answered, else the user_id they were built with: no lookup starts now.
    if (c.userIdFromCredentials) for (const q of batch) this.stamp(q, this.currentIdentity())
    const body = insertAllBody(batch.map(q => q.row))
    const postBatch = (): Promise<HttpAnswer | { error: string }> =>
      Promise.race([
        this.post(insertAllUrl(c), body, token, Math.max(50, until - this.io.now() - EXIT_SPILL_RESERVE_MS)),
        marks.post.at.then(() => ({ error: 'no answer before the session ended' })),
      ])

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const answer = await postBatch()
      if ('error' in answer) {
        this.recordError(`session end: ${answer.error}`)
        park(batch)
        return false
      }
      const outcome = classifyInsert(answer, batch.length)
      switch (outcome.kind) {
        case 'ok':
          this.noteSessions(batch)
          this.succeeded(batch.length, c)
          return true
        case 'partial': {
          const landed: Queued[] = []
          const again: Queued[] = []
          const refused: Queued[] = []
          let reason = ''
          batch.forEach((q, i) => {
            const why = outcome.rejected.get(i)
            if (why !== undefined) {
              refused.push(q)
              reason ||= why
            } else if (outcome.retry.has(i)) again.push(q)
            else landed.push(q)
          })
          this.noteSessions(landed)
          this.succeeded(landed.length, c)
          // Its write is one of those under way, which the exit waits for.
          if (refused.length > 0) writes.push(this.reject(refused, `BigQuery rejected ${refused.length} row(s): ${reason}`))
          park(again)
          return refused.length === 0 && again.length === 0
        }
        case 'rejected':
          if (outcome.permission) this.noteProblem('permission', outcome.message, 'write')
          else this.tableAnswered()
          writes.push(this.reject(batch, outcome.message))
          return false
        case 'not-found': {
          const createBudget = until - this.io.now() - EXIT_SPILL_RESERVE_MS
          if (
            attempt === 0 &&
            c.autoCreateTable &&
            !this.createAttempted &&
            !isDatasetMissing(outcome.message) &&
            !marks.post.passed() &&
            createBudget >= EXIT_CREATE_TABLE_MIN_MS
          ) {
            this.createAttempted = true
            const created = await Promise.race([
              this.createTable(c, token, Math.max(50, createBudget)),
              marks.post.at.then(() => false),
            ])
            if (created) {
              if (!marks.post.passed() && until - this.io.now() >= EXIT_SEND_MIN_MS) continue
              this.recordError(`session end: table created (${tableRef(c)}); rows spilled`)
              park(batch)
              return false
            }
            if (marks.post.passed()) {
              this.missingDestination(outcome.message)
              this.recordError('session end: no answer before the session ended')
              park(batch)
              return false
            }
          }
          this.missingDestination(outcome.message)
          this.recordError(`session end: ${outcome.message}`)
          park(batch)
          return false
        }
        case 'auth':
          // No fresh token now: the next session's send gets one.
          this.dropToken()
          this.noteProblem('credentials', `authentication: ${outcome.message}`)
          this.recordError(`session end: ${outcome.message}`)
          park(batch)
          return false
        case 'retry':
          this.recordError(`session end: ${outcome.message}`)
          park(batch)
          return false
      }
    }
    return false
  }

  // A token without running the token command: the one in hand, else the
  // one a token command already running brings, else BQAA_ACCESS_TOKEN; by
  // the mark `by` at the latest. Null when none comes.
  private async exitToken(by: Mark): Promise<string | null> {
    if (this.token && this.io.now() < this.token.expiresAt) return this.token.value
    if (this.tokenPending !== null) return Promise.race([this.tokenPending.catch(() => null), by.at.then(() => null)])
    let fromEnv: string | undefined
    try {
      fromEnv = (await Promise.race([this.io.envToken(), by.at.then(() => undefined)]))?.trim()
    } catch {
      return null
    }
    if (!fromEnv || by.passed()) return null
    this.tokenSource = 'environment'
    this.token = { value: fromEnv, expiresAt: this.io.now() + 5 * 60 * 1000 }
    return fromEnv
  }

  // Session end's replay of this table's spill and retry files, in a run
  // with no later moment for it (`-p`), once BigQuery took this session's
  // rows: it sends nothing after the `replay` mark (or EXIT_REPLAY_RESERVE_MS
  // before `until`), takes no token but one at hand, and is waited for until
  // the budget ends. Files it does not reach wait for the next session, as do
  // the rows of a request not answered in time (they may yet land; event_id
  // dedups).
  private async replayAtExit(c: Config, marks: ExitMarks, until: number): Promise<void> {
    if (!c.autoBackfill || this.lastSuccessAt === null || marks.replay.passed() || until - this.io.now() < EXIT_REPLAY_MIN_MS) return
    this.exitReplay = { until: until - EXIT_REPLAY_RESERVE_MS, cutoff: marks.replay }
    try {
      if (this.backfilling !== null) await Promise.race([this.backfilling, marks.end.at])
      if (!marks.replay.passed() && until - this.io.now() >= EXIT_REPLAY_MIN_MS && this.backfilling === null) {
        await Promise.race([this.backfill(['spill', 'retry'], 'auto'), marks.end.at])
      }
    } finally {
      this.exitReplay = null
    }
  }

  // The rows of an exit: to a spill file, or in a dry run to the dry-run
  // files.
  private async exitWrite(rows: Queued[], c: Config, toDryRun: boolean): Promise<void> {
    if (!toDryRun) {
      this.counters.spilled += rows.length
      await this.deadLetter('spill', rows, 'session ended before these rows were confirmed sent')
      return
    }
    try {
      await this.writeDryRunFiles(rows, c)
      this.counters.sent += rows.length
      this.call(() => this.io.sent?.(rows.length, c))
    } catch (err) {
      this.counters.dropped += rows.length
      this.recordError(`dry run write at exit failed (${rows.length} rows lost): ${errorText(err)}`)
    }
  }

  private raceUntil(work: Promise<unknown>, until: number): Promise<unknown> {
    const wait = until - this.io.now()
    if (wait <= 0) return Promise.resolve()
    let timer: Timer | null = null
    const timeout = new Promise<void>(resolve => {
      timer = this.io.after(wait, () => resolve())
    })
    return Promise.race([work.catch(() => undefined), timeout]).finally(() => (timer as Timer | null)?.cancel())
  }

  // ---- backfill ----------------------------------------------------------

  // Sends the rows parked for this table. Only a file whose completion record
  // matches it is read; any other file, still being written or left by a
  // writer that stopped, is kept as it is and reported. A file whose rows all
  // land is truncated to zero bytes (`$.fs` has no delete); rows that fail
  // again stay, and so does every line recorded for another table, as
  // written.
  backfill(kinds: DeadLetterKind[], trigger: BackfillReport['trigger']): Promise<BackfillReport> {
    if (this.backfilling) return this.backfilling
    const parked = this.parked.seq
    const dir = deadLetterPartition(this.config())
    const run = this.backfillRun(kinds, trigger, dir, parked).catch(err => {
      // A file that could not be read or written: its rows are still in it,
      // so they stay due and the paced retries replay it. Once a replay has
      // its outcome, none of its rows goes again (`writeOut`).
      this.replayDue.add(dir)
      const report: BackfillReport = {
        trigger,
        files: 0,
        rowsSent: 0,
        rowsLeft: 0,
        rowsRefused: 0,
        otherTableRows: 0,
        skippedFiles: 0,
        oversizedFiles: [],
        incompleteFiles: [],
        held: null,
        error: diagnostic(errorText(err)),
        finishedAt: this.io.now(),
      }
      this.backfillReport = report
      return report
    })
    this.backfilling = run.finally(() => {
      this.backfilling = null
    })
    return this.backfilling
  }

  get isBackfilling(): boolean {
    return this.backfilling !== null
  }

  private async backfillRun(kinds: DeadLetterKind[], trigger: BackfillReport['trigger'], dir: string, parked: number): Promise<BackfillReport> {
    const c = this.config()
    const report: BackfillReport = { trigger, files: 0, rowsSent: 0, rowsLeft: 0, rowsRefused: 0, otherTableRows: 0, skippedFiles: 0, oversizedFiles: [], incompleteFiles: [], held: null, error: null, finishedAt: null }
    const tally: ReplayTally = { waiting: 0, requests: 0 }
    const finish = (): BackfillReport => {
      report.finishedAt = this.io.now()
      const emptyAuto =
        trigger === 'auto' &&
        report.files === 0 &&
        report.rowsSent === 0 &&
        report.rowsLeft === 0 &&
        report.rowsRefused === 0 &&
        report.skippedFiles === 0 &&
        report.incompleteFiles.length === 0 &&
        report.oversizedFiles.length === 0 &&
        report.held === null &&
        report.error === null
      const hasPrevious =
        this.backfillReport !== null &&
        (this.backfillReport.files > 0 || this.backfillReport.rowsSent > 0 || this.backfillReport.error !== null)
      if (!emptyAuto || !hasPrevious) this.backfillReport = report
      return report
    }
    if (!canDeliver(c) || c.dryRun) {
      report.error = c.dryRun ? 'dry run: nothing is sent to BigQuery' : 'not configured: set project and dataset'
      return finish()
    }
    // Only this table's subdirectory (`dir`): rows parked for another table
    // wait for a session configured with that table.
    let entries: readonly { name: string; kind: string; size: number; mtimeMs?: number }[] = []
    try {
      entries = await this.io.listDir(dir)
    } catch (err) {
      // No such directory: no row waits in it. Any other failure leaves the
      // rows due, and the next replay lists it again.
      const message = errorText(err)
      if (!/ENOENT|no such file|not found/i.test(message)) {
        this.replayDue.add(dir)
        report.error = diagnostic(`cannot list ${dir}: ${message}`)
      } else if (this.parked.seq === parked) this.replayDue.delete(dir)
      return finish()
    }
    const files: string[] = []
    const sizes = new Map<string, number>()
    const stamps = new Map<string, number | undefined>()
    // The records beside each file, from the listing alone: a record file
    // (`outcome-`, `outcome-b-`), and done marks, which say the file was done
    // only once read. Read only for an empty file a write may still fill
    // (`isRetired`, below), never for one left empty long ago.
    const besides = new Map<string, { record: boolean; done: string[] }>()
    for (const x of entries) {
      if (x.kind !== 'file' || x.size === 0 || x.name.startsWith('outcome-sending-')) continue
      const of = recordOf(x.name)
      if (of === null) continue
      const beside = besides.get(of) ?? { record: false, done: [] }
      besides.set(of, beside)
      if (x.name.startsWith('outcome-done-')) beside.done.push(x.name)
      else beside.record = true
    }
    // Whether the empty file `name` is one a replay emptied: this writer
    // emptied it, a record file or a retired done mark is beside it, or the
    // store still holds its record (the done mark of a replay that emptied
    // it could not be written, and its sending mark was let go).
    const isRetired = async (name: string): Promise<boolean> => {
      const path = joinPath(dir, name)
      if (this.retiredEmpty.has(path)) return true
      const beside = besides.get(name)
      let retired = beside?.record === true
      for (const done of beside?.done ?? []) {
        if (retired) break
        const j = journalOf(await this.io.readFile(joinPath(dir, done)).catch(() => ''))
        retired = j !== undefined && j !== 'torn' && j.retired === true
      }
      if (!retired) retired = (await this.io.storeGet(sourceFor(dir, name, 0, c).recordKey).catch(() => undefined)) !== undefined
      if (retired) this.retiredEmpty.add(path)
      return retired
    }
    // First sights of files no longer there go, and so do empty files known
    // to be done that are no longer there empty.
    const listedPaths = new Set(entries.map(x => joinPath(dir, x.name)))
    for (const seen of this.seenSince.keys()) if (seen.startsWith(`${dir}/`) && !listedPaths.has(seen)) this.seenSince.delete(seen)
    const listedEmpty = new Set(entries.filter(x => x.kind === 'file' && x.size === 0).map(x => joinPath(dir, x.name)))
    for (const path of this.retiredEmpty) if (path.startsWith(`${dir}/`) && !listedEmpty.has(path)) this.retiredEmpty.delete(path)
    // Empty files a write still under way may fill, files another session's
    // lease holds, and files of what is left whose parent's record does not
    // have them yet: why each waits, said last (below), after any reason of
    // another file's.
    const emptyWaiting: string[] = []
    const leased: string[] = []
    const parentWaits: string[] = []
    for (const e of entries) {
      const kind = kindOf(e.name)
      if (e.kind !== 'file' || kind === null || !kinds.includes(kind)) continue
      const path = joinPath(dir, e.name)
      if (e.size === 0) {
        // Left empty long ago (`writtenSince`), or emptied after its replay
        // (see isRetired): nothing to do. Else a write still under way may
        // yet fill it: its table stays due, so the paced replays look again.
        if (kind !== 'rejected' && !this.replayHalted() && this.io.now() - this.writtenSince(path, e.mtimeMs) < WRITE_PENDING_MS && !(await isRetired(e.name))) {
          emptyWaiting.push(e.name)
          report.skippedFiles += 1
          tally.waiting += 1
        }
        continue
      }
      stamps.set(e.name, e.mtimeMs)
      // `$.fs.read` refuses a file over 4 MiB: say so rather than pass it by.
      if (e.size > HOST_FILE_BYTES) report.oversizedFiles.push(e.name)
      else {
        files.push(e.name)
        sizes.set(e.name, e.size)
      }
    }
    files.sort()
    // Every file's size, empty ones included: a file of what a replay left
    // looks its parent up here.
    const listed = new Map(entries.map(e => [e.name, e.size]))
    // Outcomes this writer holds for files since emptied (by another writer,
    // say) are let go.
    const there = new Set(entries.filter(e => e.size > 0).map(e => joinPath(dir, e.name)))
    for (const path of this.rewrites.keys()) if (path.startsWith(`${dir}/`) && !there.has(path)) this.rewrites.delete(path)
    // The records of files emptied more than a day ago go: no late copy of
    // another session's comes that long after. How long ago: by the record
    // file's time in the listing, or, where the listing gives none, by when
    // the file's done mark says it was done (none: kept).
    const doneAt = new Map<string, number | null>()
    const retiredAt = async (of: string): Promise<number | null> => {
      const known = doneAt.get(of)
      if (known !== undefined) return known
      let at: number | null = null
      for (const x of entries) {
        if (doneMarkerOf(x.name, of) === null || x.size === 0) continue
        try {
          const kept = journalOf(await this.io.readFile(joinPath(dir, x.name)))
          if (kept !== undefined && kept !== 'torn' && typeof kept.retiredAt === 'number') at = Math.max(at ?? kept.retiredAt, kept.retiredAt)
        } catch {
          // Not read: no time from it.
        }
      }
      doneAt.set(of, at)
      return at
    }
    for (const e of entries) {
      const of = recordOf(e.name)
      if (of === null || e.size === 0 || (listed.get(of) ?? 0) > 0) continue
      const at = e.mtimeMs ? e.mtimeMs : await retiredAt(of)
      if (at === null || this.io.now() - at < RETIRED_MS) continue
      try {
        await this.io.writeFile(joinPath(dir, e.name), '')
      } catch {
        // The next replay tries again.
      }
    }
    const me = this.sessionId() ?? 'unknown'
    for (const name of files) {
      if (report.error || this.replayStops(trigger, tally)) {
        report.skippedFiles += 1
        // Not read: its rows wait for the next replay.
        if (kindOf(name) !== 'rejected') tally.waiting += 1
        continue
      }
      const leaseKey = `lease:${destinationDir(tableRef(c))}/${name}`
      try {
        const lease = (await this.io.storeGet(leaseKey)) as { by?: string; until?: number } | undefined
        if (lease && typeof lease.until === 'number' && lease.until > this.io.now() && lease.by !== me) {
          // Another session replays it. Should that one stop part-way, the
          // rows still wait, so a replay here tries the file again (force
          // too, once the lease has run out).
          leased.push(name)
          report.skippedFiles += 1
          if (kindOf(name) !== 'rejected') tally.waiting += 1
          continue
        }
        await this.io.storeSet(leaseKey, { by: me, until: this.io.now() + LEASE_MS })
      } catch {
        // No store: go on without the lease; event_id still dedups in SQL.
      }
      try {
        const path = joinPath(dir, name)
        const source = sourceFor(dir, name, sizes.get(name) ?? -1, c)
        // A replay already has this file's outcome: what is left of it is
        // written out (the rest of it, now), the file is emptied, and none
        // of it goes again. Until it is emptied, its table stays due. The
        // record is read even when this writer holds the outcome: another
        // writer may have written more of it since.
        let held = this.rewrites.get(path)
        if (held !== undefined && held.size !== source.size) {
          this.rewrites.delete(path)
          held = undefined
        }
        let found: Found | Pending | 'torn' | undefined
        try {
          found = await this.readJournal(source)
        } catch (err) {
          // It may have an outcome that cannot be read now: none of it goes
          // until its record can be, and its rows wait.
          report.held ??= diagnostic(`cannot read the replay record of ${name}: ${messageOf(err)}`)
          report.skippedFiles += 1
          if (kindOf(name) !== 'rejected') tally.waiting += 1
          continue
        }
        if (found === 'torn') {
          // Its record was cut off part-way, with none whole: nothing of what
          // it left was written (that waits for a whole record), but its rows
          // may have gone, and its writer may be at it still. The writer that
          // holds the outcome writes it again; any other replay leaves the
          // file waiting, and /bqaa-backfill force replays it afresh.
          if (held === undefined && trigger !== 'force') {
            report.held ??= diagnostic(`the replay record of ${name} is cut off part-way; /bqaa-backfill force replays the file afresh`)
            report.skippedFiles += 1
            if (kindOf(name) !== 'rejected') tally.waiting += 1
            continue
          }
          found = undefined
        }
        if (found !== undefined && 'pending' in found) {
          // A replay sent rows of it, or was about to, and recorded no
          // outcome: its writer may yet (still at it, or unable to write its
          // record now), or it stopped. Its rows may have gone, and been
          // refused: none goes from any other replay, and /bqaa-backfill
          // force, for a writer that stopped, replays it afresh. A mark of
          // this writer's own is no other replay's: it would hold the outcome.
          if (held === undefined && trigger !== 'force' && found.pending.some(by => by !== this.mark)) {
            report.held ??= diagnostic(`a replay of ${name} sent rows and has recorded no outcome; if that session stopped, /bqaa-backfill force replays the file afresh`)
            report.skippedFiles += 1
            if (kindOf(name) !== 'rejected') tally.waiting += 1
            continue
          }
          found = undefined
        }
        const known = await this.resumeRewrite(path, source, kindOf(name) ?? 'retry', held, found)
        let emptied = true
        if (known !== undefined) {
          emptied = await this.writeOut(path, known)
          // What it wrote out waits for the next replay.
          if (known.waits) tally.waiting += 1
          report.files += 1
          report.rowsLeft += [...known.fates].filter(fate => fate !== 'L').length
          report.rowsRefused += [...known.fates].filter(fate => fate === 'R').length
        } else {
          // Emptied meanwhile (another writer finished it): nothing to do.
          const text = await this.io.readFile(path)
          const parsed = parseFile(text)
          if (text !== '' && !parsed.complete) {
            // Still being written, or left part-written: none of it is read,
            // and, for a lease's time from its last write (`writtenSince`),
            // its table stays due, so the paced replays read it again and
            // send it once its completion record is there.
            report.incompleteFiles.push(name)
            if (kindOf(name) !== 'rejected' && this.io.now() - this.writtenSince(path, stamps.get(name)) < WRITE_PENDING_MS) tally.waiting += 1
          } else if (text !== '' && utf8Length(text) !== source.size) {
            // Written further since it was listed (a write that had not
            // finished then): its record, and every mark, go by the size the
            // listing gave, so it waits for the next replay to list it whole.
            report.held ??= diagnostic(`${name} changed while it was read; it waits for the next replay`)
            report.skippedFiles += 1
            if (kindOf(name) !== 'rejected') tally.waiting += 1
          } else if (text === '') {
            // Emptied since it was listed: by another writer, which writes
            // its done mark first, or by a write that starts it over, still
            // under way. With no done mark beside it, it waits.
            if (kindOf(name) !== 'rejected' && !(await this.io.listDir(dir)).some(x => x.size > 0 && doneMarkerOf(x.name, name) !== null)) tally.waiting += 1
          } else {
            this.seenSince.delete(path)
            const fate = parsed.parent === null ? 'go' : await this.parentFate(dir, parsed.parent, path, listed.get(parsed.parent) ?? 0, c)
            if (fate === 'wait') {
              // Written by a replay of another file whose record does not have
              // it as written yet: it waits until that replay is finished.
              parentWaits.push(name)
              report.skippedFiles += 1
              if (kindOf(name) !== 'rejected') tally.waiting += 1
            } else if (fate === 'drop') {
              // A late copy of a file another session's outcome left, which
              // the outcome its parent was done by does not name: those rows
              // that outcome has. It is emptied, and none of it goes.
              emptied = await this.emptyCopy(path)
            } else {
              emptied = await this.backfillFile(dir, name, parsed, source, c, kinds, trigger, report, tally)
              report.files += 1
            }
          }
        }
        if (!emptied) tally.waiting += 1
      } catch (err) {
        // A file whose record could not be read or written partway (the
        // store or the disk refusing it): its rows stay, it waits for the
        // next replay, and the replay goes on with the files after it.
        report.held ??= diagnostic(`${name}: ${messageOf(err)}`)
        report.skippedFiles += 1
        if (kindOf(name) !== 'rejected') tally.waiting += 1
      } finally {
        try {
          await this.io.storeDelete(leaseKey)
        } catch {
          // The lease expires on its own.
        }
      }
    }
    if (emptyWaiting.length > 0) report.held ??= diagnostic(`${emptyWaiting[0]} is empty, perhaps still being written; it waits for the next replay`)
    if (parentWaits.length > 0) report.held ??= diagnostic(`${parentWaits[0]} waits until the replay of the file it came from has it as written`)
    if (leased.length > 0) report.held ??= diagnostic(`another session's lease holds ${leased[0]} (ten minutes at most); it waits for the next replay`)
    // Another replay of this table follows by itself while rows wait: rows
    // this one left, files it did not read (another session's lease on them
    // included), files not whole or empty that a write still under way may
    // finish, or rows parked once it had begun. Rows refused here, and files
    // too large to read, never keep it going.
    if (tally.waiting > 0 || this.parked.seq !== parked) this.replayDue.add(dir)
    else this.replayDue.delete(dir)
    return finish()
  }

  // When the file `path` was last written, as far as a replay can tell: its
  // time in the listing (0, or none, gives no time), else when this writer
  // first saw it empty or not whole.
  private writtenSince(path: string, listedAt: number | undefined): number {
    if (listedAt) return listedAt
    const seen = this.seenSince.get(path)
    if (seen !== undefined) return seen
    const now = this.io.now()
    this.seenSince.set(path, now)
    return now
  }

  // A replay stops once the session is ending (unless it is session end's
  // own, until its time is up), and a recovery replay stops between requests
  // once it has sent one when rows are queued meanwhile (the flush it holds
  // sends them first). What it did not send stays due.
  private replayStops(trigger: BackfillReport['trigger'], tally: ReplayTally): boolean {
    return this.replayHalted() || (trigger === 'recovery' && tally.requests > 0 && this.queue.length > 0)
  }

  // Whether a replay sends no more: once the session is ending, or, for
  // session end's own replay, once its time is up.
  private replayHalted(): boolean {
    const exit = this.exitReplay
    return exit === null ? this.closing : exit.cutoff.passed() || this.io.now() >= exit.until
  }

  // A replay's token: during session end's own replay, only one at hand
  // (no token command runs), else as for a send.
  private async replayToken(): Promise<string> {
    const exit = this.exitReplay
    if (exit === null) return this.getToken()
    const token = await this.exitToken(exit.cutoff)
    if (token === null) throw new Error('no token at hand as the session ended')
    return token
  }

  // A replay's request: during session end's own replay, answered by its
  // cutoff at the latest.
  private replayPost(url: string, body: string, token: string, timeoutMs: number): Promise<HttpAnswer | { error: string }> {
    const exit = this.exitReplay
    if (exit === null) return this.post(url, body, token, timeoutMs)
    return Promise.race([this.post(url, body, token, Math.max(50, Math.min(timeoutMs, exit.until - this.io.now()))), exit.cutoff.at.then(() => ({ error: 'no answer before the session ended' }))])
  }

  // Rows one replay request had refused: on the status line, and among the
  // rows the access-denied notice counts, as a send's refused rows are, each
  // row once (one /bqaa-backfill sends again, and BigQuery refuses again,
  // is not counted twice; one an earlier session refused counts once here).
  private refusedOnReplay(ids: readonly string[], reason: string): void {
    if (ids.length === 0) return
    this.countRefused(ids)
    if (this.problem?.kind === 'permission') this.countParked(ids)
    if (this.problem !== null) this.showProblem()
    else if (this.refusedSinceSuccess > 0) this.setStatus(`BQAA: ${this.refusedText()} row(s) refused (${diagnostic(reason).slice(0, 80)}); fix, then /bqaa-backfill`)
  }

  private countRefused(ids: readonly string[]): void {
    const { added, rough } = Writer.countNew(this.refusedIds, ids)
    this.refusedSinceSuccess += added
    if (rough) this.refusedRough = true
  }

  private countParked(ids: readonly string[]): void {
    const { added, rough } = Writer.countNew(this.parkedIds, ids)
    this.parkedDuringProblem += added
    if (rough) this.parkedRough = true
  }

  // How many of `ids` are not in `seen`, which then holds them, up to
  // COUNTED_IDS; past that a row counts without the check (`rough`: it may
  // count again).
  private static countNew(seen: Set<string>, ids: readonly string[]): { added: number; rough: boolean } {
    let added = 0
    let rough = false
    for (const id of ids) {
      // A row with no id counts on its own.
      if (typeof id !== 'string' || id === '') {
        added += 1
        continue
      }
      if (seen.has(id)) continue
      added += 1
      if (seen.size < COUNTED_IDS) seen.add(id)
      else rough = true
    }
    return { added, rough }
  }

  // A file's replay record, wherever a writer left it: the mark that it was
  // done, which stands over any other; else in `$.store`, and in the files
  // beside it when the store did not take it (or it was too large for the
  // store), the later of the two whole ones. When the store and the file
  // hold the same outcome, a file of what is left counts as written if either
  // says so. With none of the file's size there, `torn` when one cut off
  // part-way is, else `pending` where a replay's mark is, else none. A record
  // that cannot be read throws, and is never taken for none.
  private async readJournal(source: Source): Promise<Found | Pending | 'torn' | undefined> {
    // A record of another size is not this file's (`size` -1: any size).
    const fits = (journal: Partial<Journal> | undefined): journal is Partial<Journal> => journal !== undefined && (source.size < 0 || journal.size === source.size)
    // In files of their own, each writer's, which no write of a record goes
    // to: any whole one stands, whatever another writer's late write does to
    // its own. One cut off part-way, with no other whole record, is cut off
    // too: the file's outcome may have been done, and none of it goes but by
    // /bqaa-backfill force.
    const doneIn = (entries: readonly { name: string; size: number }[]): Promise<{ done: Partial<Journal> | undefined; torn: boolean }> => this.doneMarks(source, entries)
    let entries = await this.io.listDir(source.dir)
    const first = await doneIn(entries)
    if (first.done !== undefined) return { journal: first.done, filed: true }
    let doneTorn = first.torn
    const read = ((await this.io.storeGet(source.recordKey)) ?? undefined) as Partial<Journal> | undefined
    const stored = fits(read) ? read : undefined
    let filed: Partial<Journal> | undefined
    let torn = false
    for (const file of source.recordFiles) {
      if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
      const text = await this.io.readFile(file)
      // There but empty beside a file that still holds its rows: a write cut
      // off before its first byte (a record let go is emptied only with its
      // file, or before the file is replayed afresh).
      const kept = text === '' && this.io.exists !== undefined && source.live ? 'torn' : journalOf(text)
      if (kept === 'torn') torn = true
      else if (fits(kept) && (filed === undefined || seqOf(kept) > seqOf(filed))) filed = kept
    }
    if (stored === undefined) {
      if (filed !== undefined) return { journal: filed, filed: true }
      // No record whole in the store or beside the file: a writer may have
      // finished the file while those were read, and it writes its done mark
      // before it lets the store's record go, and before it lets its mark
      // go. So the marks are read, and only then the done marks listed
      // again, before none is taken for an answer.
      entries = await this.io.listDir(source.dir)
      const by = await this.marksOn(source, entries)
      const again = await doneIn(await this.io.listDir(source.dir))
      if (again.done !== undefined) return { journal: again.done, filed: true }
      if (torn || doneTorn || again.torn) return 'torn'
      return by.length === 0 ? undefined : { pending: by }
    }
    if (filed === undefined) return { journal: stored, filed: torn }
    // Two outcomes: the file's was written when the store did not take it.
    if (!Array.isArray(stored.files) || !Array.isArray(filed.files) || !sameNames(stored.files, filed.files)) return { journal: filed, filed: true }
    const a = writtenOf(stored)
    const b = writtenOf(filed)
    return { journal: { ...stored, written: a.map((w, i) => w || b[i] === true), seq: Math.max(seqOf(stored), seqOf(filed)), retired: stored.retired === true || filed.retired === true }, filed: true }
  }

  // The marks that `source` was done, each writer's, from `entries` (its
  // directory listed): the one that stands, and whether one is cut off. Of
  // two whole ones (two outcomes recorded at the same moment), the later by
  // number, then by name: every writer reads the same one. This writer's own
  // mark cut off part-way beside a file that still holds its rows, while it
  // holds no outcome for the file, is none of these: its sending mark, whose
  // write was cut off before any of the file went (marksOn reads it as this
  // writer's, which a replay goes past). Another writer's, or one beside an
  // emptied file, is cut off as before.
  private async doneMarks(source: Source, entries: readonly { name: string; size: number }[]): Promise<{ done: Partial<Journal> | undefined; torn: boolean }> {
    const fits = (journal: Partial<Journal> | undefined): journal is Partial<Journal> => journal !== undefined && (source.size < 0 || journal.size === source.size)
    let torn = false
    let done: Partial<Journal> | undefined
    const marks = entries.filter(entry => entry.size > 0 && doneMarkerOf(entry.name, source.name) !== null)
    if (marks.length === 0) return { done, torn }
    const ownSending = source.size > 0 && !this.rewrites.has(joinPath(source.dir, source.name))
    for (const entry of marks.sort((x, y) => (x.name < y.name ? -1 : x.name > y.name ? 1 : 0))) {
      const kept = journalOf(await this.io.readFile(joinPath(source.dir, entry.name)))
      if (kept === 'torn') {
        if (!(ownSending && doneMarkerOf(entry.name, source.name) === this.mark)) torn = true
      } else if (fits(kept) && kept.retired === true && (done === undefined || seqOf(kept) >= seqOf(done))) done = kept
    }
    return { done, torn }
  }

  // What becomes of the file `path`, which a replay of `parent` wrote (the
  // parent `size` long, 0 once emptied or gone): `go` once the parent's
  // record has it as written, or names other files (another replay's
  // outcome), or is gone; `wait` while the parent still holds its rows and
  // its record may yet write the file again, or while that record cannot be
  // read, is cut off, or is yet to be written (a replay's mark). Once the
  // parent is emptied, its record says nothing against a file it names (all
  // were written before it was emptied), and `drop` is a file that the
  // record that did the parent does not name: a late copy of another
  // session's outcome, whose rows that outcome has.
  private async parentFate(dir: string, parent: string, path: string, size: number, c: Config): Promise<'go' | 'wait' | 'drop'> {
    let found: Found | Pending | 'torn' | undefined
    try {
      found = await this.readJournal(size > 0 ? sourceFor(dir, parent, size, c) : { ...sourceFor(dir, parent, -1, c), live: false })
    } catch {
      return 'wait'
    }
    if (found === 'torn' || (found !== undefined && 'pending' in found)) return size > 0 ? 'wait' : 'go'
    if (found === undefined) return 'go'
    const at = (Array.isArray(found.journal.files) ? found.journal.files : []).indexOf(path)
    // Not named: a late copy of another session's outcome, where the record
    // is the mark the parent was done, or a whole one of an emptied parent
    // with every file it names written (its done mark lost, or never written:
    // its outcome has every row of the parent).
    if (at === -1) return found.journal.retired === true || (size === 0 && writtenOf(found.journal).every(Boolean)) ? 'drop' : 'go'
    return size === 0 || writtenOf(found.journal)[at] === true ? 'go' : 'wait'
  }

  // Empties a file whose rows are already had elsewhere; false when it could
  // not be emptied (the next replay tries again).
  private async emptyCopy(path: string): Promise<boolean> {
    try {
      await this.io.writeFile(path, '')
      return true
    } catch (err) {
      this.recordError(`could not empty ${path}, whose rows are had elsewhere: ${errorText(err)}`)
      return false
    }
  }

  // Empties the files beside `source` that held its record: none is read
  // once the source is empty, or once the record is let go.
  private async emptyRecordFiles(source: Source): Promise<void> {
    for (const file of source.recordFiles) {
      try {
        if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
        await this.io.writeFile(file, '')
      } catch {
        // Left as it is.
      }
    }
  }

  // What `rewrite` holds, brought up to its record: a file of what is left
  // that the record has as written counts as written, and never goes back to
  // unwritten (another writer may since have replayed and emptied it). False
  // when the record is of another outcome for the file.
  private static catchUp(rewrite: Rewrite, found: Found): boolean {
    if (!sameNames(rewrite.files.map(f => f.path), Array.isArray(found.journal.files) ? found.journal.files : [])) return false
    const written = writtenOf(found.journal)
    rewrite.files.forEach((file, i) => {
      if (written[i] === true) file.text = null
    })
    rewrite.filed ||= found.filed
    rewrite.seq = Math.max(rewrite.seq, seqOf(found.journal))
    return true
  }

  // The Rewrite for the file `source.size` long: what this writer holds of
  // it, brought up to its record, or else the record's, with what it has
  // still to write rebuilt from the source. With no record that fits the
  // file, what this writer holds, if anything; a record that no longer fits
  // the file is let go.
  private async resumeRewrite(path: string, source: Source, fileKind: DeadLetterKind, held: Rewrite | undefined, found: Found | undefined): Promise<Rewrite | undefined> {
    if (found === undefined) return held
    if (held !== undefined && Writer.catchUp(held, found)) return held
    const { journal } = found
    const paths = Array.isArray(journal.files) ? journal.files : []
    const written = writtenOf(journal)
    const outcome: Outcome = { fates: typeof journal.fates === 'string' ? journal.fates : '', reasons: Array.isArray(journal.reasons) ? journal.reasons : [], refusals: Array.isArray(journal.refusals) ? journal.refusals : [] }
    const rewrite: Rewrite = { ...outcome, ...source, waits: journal.waits === true, filed: found.filed, seq: seqOf(journal), files: paths.map(p => ({ path: p, text: null })) }
    if (written.every(Boolean)) return rewrite
    const text = await this.io.readFile(path)
    // Emptied meanwhile (another writer finished it): nothing to resume, and
    // its record is left as that writer left it.
    if (text === '') return undefined
    const lines = parseFile(text)
    // Not whole, or not as long as it was listed (a write still under way, or
    // one since): the record is kept, and the file waits, as one with no
    // record does, until it reads whole.
    if (!lines.complete || (source.size >= 0 && utf8Length(text) !== source.size)) return undefined
    const texts = fitsLines(lines, outcome) ? leftoverFiles(lines, fileKind, outcome, source.name) : null
    if (texts === null || texts.length !== paths.length) {
      // Not this file's outcome after all: replay it afresh.
      try {
        await this.io.storeDelete(source.recordKey)
      } catch {
        // It no longer fits the file; it is passed by again.
      }
      if (found.filed) await this.emptyRecordFiles(source)
      return held
    }
    rewrite.files = paths.map((p, i) => ({ path: p, text: written[i] === true ? null : (texts[i]?.text ?? '') }))
    return rewrite
  }

  // What a replay left of a file, written out: first the record of its
  // outcome, where every writer reads it, then each file of what is left,
  // once, each followed by the record again (a write that fails stops the
  // replay, and the next one, of this or any writer, writes the rest under
  // the same names), then the source emptied. Nothing is written until the
  // record is: one that neither the store nor a file beside the source takes
  // stops the replay, and this writer tries again at the next. A file of what
  // is left that the record does not yet have as written is never replayed
  // (`parentFate`), so one written again over it was never taken. False when
  // the source could not be emptied: it stays on disk, its rows landed or in
  // the newer files, and the record keeps every session from sending it
  // again; the next replay tries to empty it again.
  //
  // First, before anything is recorded: another writer may have finished
  // the file, or recorded more of this outcome or one of its own, while this
  // one read or sent. Another outcome: that one goes on, at the next replay;
  // this one done (the mark has every file written): nothing more is
  // written. No whole record (a replay's mark is none), and the file
  // emptied: nothing is written either. A record that cannot be read keeps
  // this outcome here until it can be. The same check comes before each
  // file of what is left, and before the mark that the file was done.
  private async writeOut(path: string, rewrite: Rewrite): Promise<boolean> {
    this.rewrites.set(path, rewrite)
    const found = Writer.recorded(await this.readJournal(rewrite))
    // Finished already (the file left again, by a late copy, say), here or
    // at any check below: the mark that it was done stands as it is, never
    // written again, and only the file is emptied.
    let finished = found !== undefined && found !== 'torn' && found.journal.retired === true
    let recorded = false
    if (found !== undefined && found !== 'torn') {
      if (!Writer.catchUp(rewrite, found)) {
        this.rewrites.delete(path)
        return false
      }
      // The record has this outcome as this writer has it: no need to write it again.
      const written = writtenOf(found.journal)
      recorded = rewrite.files.every((file, i) => (file.text === null) === written[i])
    } else if ((await this.io.readFile(path)) === '') {
      this.rewrites.delete(path)
      return true
    }
    const record = async (): Promise<void> => {
      rewrite.seq += 1
      const { fates, reasons, refusals, size, waits, seq } = rewrite
      const journal: Journal = { fates, reasons, refusals, size, waits, seq, files: rewrite.files.map(f => f.path), written: rewrite.files.map(f => f.text === null) }
      if (utf8Length(JSON.stringify(journal)) <= STORE_RECORD_BYTES) {
        try {
          await this.io.storeSet(rewrite.recordKey, journal)
          return
        } catch {
          // Not taken (the store is full, say): a file beside the source.
        }
      }
      try {
        // The two files take the updates in turn: one cut off part-way
        // leaves the record before it whole in the other (or in the store).
        await this.io.writeFile(rewrite.recordFiles[(seq + 1) % 2] as string, journalText(journal))
        rewrite.filed = true
      } catch (err) {
        const message = `could not record the replay of ${path}, so nothing more of what it left is written: ${errorText(err)}`
        this.recordError(message)
        throw new Error(message)
      }
    }
    if (!recorded) await record()
    // Another writer may have written a file of it since, replayed the file
    // at the same time with an outcome of its own, or finished it: the record
    // says. That outcome goes on, at the next replay, and no more of this one.
    const stillOurs = async (): Promise<boolean> => {
      const found = Writer.recorded(await this.readJournal(rewrite))
      if (found === 'torn' || (found !== undefined && found.journal.size === rewrite.size && !Writer.catchUp(rewrite, found))) {
        this.rewrites.delete(path)
        // Another outcome finished the file: a record of this one that the
        // store took meanwhile is let go, as that writer let its own go.
        if (found !== 'torn' && found.journal.retired === true) {
          try {
            await this.io.storeDelete(rewrite.recordKey)
          } catch {
            // Left; the done mark stands over it.
          }
        }
        return false
      }
      if (found !== undefined && found.journal.retired === true) finished = true
      return true
    }
    for (const file of rewrite.files) {
      if (file.text === null) continue
      if (!(await stillOurs())) return false
      if (file.text === null) continue
      await this.io.writeFile(file.path, file.text)
      file.text = null
      await record()
    }
    if (!(await stillOurs())) return false
    const wroteDone = !finished && (await this.markDone(rewrite))
    if (wroteDone) rewrite.wroteDone = true
    const done = finished || wroteDone
    try {
      await this.io.writeFile(path, '')
    } catch (err) {
      this.recordError(`could not empty ${path} after its replay (its rows landed or are in newer files): ${errorText(err)}`)
      return false
    }
    // Known done from here on, whether or not its done mark was written: the
    // next replay does not wait on it as on a file still being written.
    this.retiredEmpty.add(path)
    this.rewrites.delete(path)
    if (done) {
      try {
        await this.io.storeDelete(rewrite.recordKey)
      } catch {
        // Left; the done mark stands over it.
      }
    }
    return true
  }

  // A record as writeOut weighs it: a replay's mark is no outcome.
  private static recorded(found: Found | Pending | 'torn' | undefined): Found | 'torn' | undefined {
    return found !== undefined && found !== 'torn' && 'pending' in found ? undefined : found
  }

  // Every file of what is left written: the mark that the file is done, its
  // record marked `retired`, in this writer's own file beside it
  // (`doneFileOf`), written before the file is emptied. No other writer
  // writes that file, so no late write of another session's replay takes it
  // away: a late copy of the file, or of one it left, is known by it, and so
  // is another session's late outcome for it. Kept a day; the store's copy of
  // the record goes once the file is emptied. False when it could not be
  // written.
  private async markDone(rewrite: Rewrite): Promise<boolean> {
    rewrite.seq += 1
    const { size, waits, seq } = rewrite
    const { fates, reasons, refusals } = rewrite
    const journal: Journal = { fates, reasons, refusals, size, waits, seq, files: rewrite.files.map(f => f.path), written: rewrite.files.map(() => true), retired: true, retiredAt: this.io.now() }
    try {
      await this.io.writeFile(doneFileOf(rewrite.dir, rewrite.name, this.mark), journalText(journal))
      return true
    } catch {
      return false
    }
  }

  // Before any of a file's rows go: the mark that this writer sends them, in
  // its own file beside it (`doneFileOf`), which tells a writer that finds no
  // outcome recorded that one may yet be (`backfillRun`), and which no other
  // writer writes over or empties. Then the record and the marks read again:
  // another writer's outcome or done mark, the file emptied, or (unless
  // /bqaa-backfill force asked for it afresh) another writer's mark, and none
  // of it goes from here, this writer's own mark let go. False when the mark
  // cannot be written (no later writer could tell then: none goes), or another
  // writer has the file. Where the mark's write fails, or reading the record
  // or the marks back does (which throws), the mark, perhaps written in part,
  // is let go too (best effort): none of the file went under it.
  private async markSending(path: string, source: Source, trigger: BackfillReport['trigger'], report: BackfillReport): Promise<boolean> {
    try {
      await this.io.writeFile(doneFileOf(source.dir, source.name, this.mark), sealFile(`${JSON.stringify({ size: source.size, sending: true, by: this.mark })}\n`))
    } catch (err) {
      await this.unmark(source)
      report.held ??= diagnostic(`could not mark the replay of ${source.name} before sending, so none of it is sent: ${errorText(err)}`)
      return false
    }
    try {
      // The marks as readJournal read them, before its last look at the done
      // marks, so no mark let go meanwhile (by a writer that finished the
      // file) is taken for no writer. With a record cut off part-way, which
      // comes with no marks, they are read now, and the done marks once more
      // after them.
      const read = await this.readJournal(source)
      let found = Writer.recorded(read)
      let others: string[] = []
      if (read === 'torn') {
        others = (await this.marksOn(source)).filter(by => by !== this.mark)
        const last = await this.doneMarks(source, await this.io.listDir(source.dir))
        if (last.done !== undefined) found = { journal: last.done, filed: true }
      } else if (read !== undefined && 'pending' in read) {
        others = read.pending.filter(by => by !== this.mark)
      }
      const another = trigger !== 'force' && others.length > 0
      if ((found !== undefined && found !== 'torn') || another || (await this.io.readFile(path)) === '') {
        if (another && (found === undefined || found === 'torn')) report.held ??= diagnostic(`another session is replaying ${source.name}; if it stopped, /bqaa-backfill force replays the file afresh`)
        await this.unmark(source)
        return false
      }
      return true
    } catch (err) {
      await this.unmark(source)
      throw err
    }
  }

  // The writers whose marks stand beside `source`, read from its directory
  // (`entries`, when it was just listed). An emptied mark, which its writer
  // let go, is not read. A mark that cannot be read, or a directory that
  // cannot be listed, throws: it is never taken for none.
  private async marksOn(source: Source, entries?: readonly { name: string; size: number }[]): Promise<string[]> {
    const by: string[] = []
    for (const entry of entries ?? (await this.io.listDir(source.dir))) {
      const writer = markerOf(entry.name, source.name)
      if (writer === null || entry.size === 0) continue
      if (markStands(await this.io.readFile(joinPath(source.dir, entry.name)), source.size)) by.push(writer)
    }
    return by
  }

  // This writer's own mark beside `source` let go. Best effort: one left is
  // this writer's own, which it replays past.
  private async unmark(source: Source): Promise<void> {
    try {
      await this.io.writeFile(doneFileOf(source.dir, source.name, this.mark), '')
    } catch {
      // Left.
    }
  }

  // Replays one file; false when what it left is written out but the file
  // itself could not be emptied.
  private async backfillFile(dir: string, name: string, file: ParsedFile, source: Source, c: Config, kinds: readonly DeadLetterKind[], trigger: BackfillReport['trigger'], report: BackfillReport, tally: ReplayTally): Promise<boolean> {
    const path = joinPath(dir, name)
    const { lines } = file
    const fileKind = kindOf(name) ?? 'retry'
    // A line is sent only to the table it records, and only by a replay that
    // sends its kind: one that runs by itself never sends a refused row,
    // whatever file holds it. The others stay as written.
    const table = tableRef(c)
    const mine: DeadLetterLine[] = []
    // Where each of `mine` is in the file, and what became of each line.
    const at: number[] = []
    const fates: string[] = []
    const reasons: string[] = []
    const refusals: number[] = []
    let kept = 0
    lines.forEach((line, i) => {
      const kind = isDeadLetterKind(line.kind) ? line.kind : fileKind
      if (line.table === table && kinds.includes(kind)) {
        mine.push({ ...line, kind })
        at.push(i)
        fates.push('W')
      } else {
        kept += 1
        fates.push('K')
        if (line.table !== table) report.otherTableRows += 1
      }
    })
    const keys = mine.map((line, n) => keyOf(line, name, at[n] as number))
    // A row BigQuery refused: kept, with the reason, for /bqaa-backfill.
    const refuse = (n: number, reason: string): void => {
      report.rowsRefused += 1
      let why = diagnostic(reason)
      // Past the reasons a replay keeps, one stands for every other.
      if (!reasons.includes(why) && reasons.length >= MAX_REASONS - 1) why = OTHER_REASON
      let k = reasons.indexOf(why)
      if (k === -1) k = reasons.push(why) - 1
      fates[at[n] as number] = 'R'
      refusals.push(k)
    }
    let changed = false
    let marked = false
    let index = 0
    while (index < mine.length) {
      if (report.error || this.replayStops(trigger, tally)) break
      let bytes = 0
      let end = index
      while (end < mine.length && end - index < c.maxBatchRows) {
        const size = rowBytes((mine[end] as DeadLetterLine).row)
        if (end > index && bytes + size > c.maxBatchBytes) break
        bytes += size
        end += 1
      }
      const first = index
      const chunk = mine.slice(index, end)
      index = end
      let token: string
      try {
        token = await this.replayToken()
      } catch (err) {
        report.error = diagnostic(`authentication: ${messageOf(err)}`)
        continue
      }
      if (this.replayHalted()) break
      if (!marked) {
        if (!(await this.markSending(path, source, trigger, report))) break
        marked = true
      }
      if (this.replayHalted()) break
      const body = insertAllBody(chunk.map(l => l.row))
      tally.requests += 1
      let answer = await this.replayPost(insertAllUrl(c), body, token, c.requestTimeoutMs)
      if ('error' in answer) {
        report.error = diagnostic(answer.error)
        continue
      }
      let outcome = classifyInsert(answer, chunk.length)
      if (outcome.kind === 'auth' && !this.closing) {
        // As for a send: one more try at once with a fresh token, and a
        // second 401 is the credentials notice. The rows keep the user_id
        // they were parked with. Once the exit began, no fresh token is
        // fetched.
        this.dropToken()
        try {
          token = await this.getToken()
        } catch (err) {
          report.error = diagnostic(`authentication: ${messageOf(err)}`)
          continue
        }
        if (this.closing) break
        tally.requests += 1
        answer = await this.replayPost(insertAllUrl(c), body, token, c.requestTimeoutMs)
        if ('error' in answer) {
          report.error = diagnostic(answer.error)
          continue
        }
        outcome = classifyInsert(answer, chunk.length)
        if (outcome.kind === 'auth') this.noteProblem('credentials', `BigQuery refused a fresh token too: ${outcome.message}`)
      }
      if (outcome.kind === 'ok' || outcome.kind === 'partial') {
        // BigQuery took the request: the table is there and the credentials
        // work, whatever it made of each row. The command's replay sends
        // the refused rows too, so its count of rows parked meanwhile starts
        // over.
        if (trigger === 'command' || trigger === 'force') {
          this.parkedDuringProblem = 0
          this.parkedIds.clear()
          this.parkedRough = false
        }
        let landed = 0
        const refused: string[] = []
        let refusal = ''
        const { rejected, retry } = outcome.kind === 'partial' ? outcome : { rejected: new Map<number, string>(), retry: new Set<number>() }
        chunk.forEach((_, i) => {
          const reason = rejected.get(i)
          if (reason !== undefined) {
            refused.push(keys[first + i] as string)
            refusal = reason
            refuse(first + i, reason)
          } else if (!retry.has(i)) {
            landed += 1
            fates[at[first + i] as number] = 'L'
          }
        })
        report.rowsSent += landed
        this.counters.batches += 1
        this.counters.sent += landed
        this.counters.replayed += landed
        // In the order they came, as for a send: rows landing end the count,
        // then this request's refusals start it again.
        this.tookRequest(landed)
        this.refusedOnReplay(refused, refusal)
        // Said at once, before the file is emptied (which may fail).
        if (landed > 0) this.call(() => this.io.sent?.(landed, c))
        // Rows asked for again, and nothing else, leave the file as it was.
        if (outcome.kind === 'ok' || outcome.retry.size < chunk.length) changed = true
        continue
      }
      if (outcome.kind === 'rejected') {
        // Refused whole, as a send would be: parked for /bqaa-backfill, and
        // access denied is the notice. The rest waits for the next replay.
        if (outcome.permission) this.noteProblem('permission', outcome.message, 'write')
        else this.tableAnswered()
        chunk.forEach((line, i) => refuse(first + i, outcome.message))
        this.refusedOnReplay(keys.slice(first, first + chunk.length), outcome.message)
        changed = true
        report.error = diagnostic(outcome.message)
        continue
      }
      if (outcome.kind === 'auth') this.dropToken()
      if (outcome.kind === 'not-found') this.missingDestination(outcome.message)
      report.error = diagnostic(outcome.message)
    }
    tally.waiting += mine.filter((line, n) => fates[at[n] as number] === 'W' && line.kind !== 'rejected').length
    report.rowsLeft += at.filter(i => fates[i] !== 'L').length + kept
    // Nothing sent and nothing new to record: the file stays as it was, and
    // its mark goes while it is still this writer's (best effort: one left is
    // this writer's own).
    if (!changed) {
      if (marked) await this.unmark(source)
      return true
    }
    // What is left goes to new files first, each kind to files of its own (a
    // row refused here to a `rejected` file, which no replay that runs by
    // itself reads), each file ending with its completion record; only once
    // every one is written is the source emptied: `$.fs` has no rename or
    // delete. A write that fails leaves the source as it was, so an unsent
    // row is never held only in memory, and this outcome is kept: the next
    // replay finishes writing it out (`writeOut`) and sends none of the
    // file again.
    const outcome: Outcome = { fates: fates.join(''), reasons, refusals }
    const rest = leftoverFiles(file, fileKind, outcome, name)
    const files = rest.map(({ kind, text }) => ({ path: joinPath(dir, fileName(kind, this.io.now(), this.sessionId(), ++this.seq, this.mark)), text }))
    // Another writer that replayed the file at the same time may have
    // recorded its outcome meanwhile, or finished it: writeOut finds out
    // before it records this one.
    const rewrite: Rewrite = { ...source, ...outcome, files, waits: rest.some(f => f.kind !== 'rejected'), filed: false, seq: 0 }
    const done = await this.writeOut(path, rewrite)
    // The file done, clear our sending mark only when this write did not
    // replace it with a retired done mark (`wroteDone`).
    if (done && !rewrite.wroteDone) await this.unmark(source)
    return done
  }

  // ---- status ------------------------------------------------------------

  snapshot(): {
    queued: number
    queuedBytes: number
    inFlight: number
    dropped: number
    counters: Counters
    lastError: { message: string; at: number } | null
    lastSuccessAt: number | null
    pausedUntil: number
    consecutiveFailures: number
    hasToken: boolean
    problem: Problem | null
    waiting: number
    waitingText: string
    identity: 'off' | 'pending' | 'found' | 'failed'
  } {
    const c = this.config()
    return {
      queued: this.queue.length,
      queuedBytes: this.queue.bytes,
      inFlight: this.inFlightRows.length,
      dropped: this.counters.dropped + this.queue.dropped,
      counters: { ...this.counters },
      lastError: this.lastError,
      lastSuccessAt: this.lastSuccessAt,
      pausedUntil: this.pausedUntil,
      consecutiveFailures: this.consecutiveFailures,
      hasToken: this.token !== null && this.io.now() < this.token.expiresAt,
      problem: this.problem,
      waiting: this.waitingRows(),
      waitingText: this.waitingText(),
      identity: !c.userIdFromCredentials ? 'off' : this.token === null || this.identity?.token !== this.token.value ? 'pending' : this.identity.email === null ? 'failed' : 'found',
    }
  }
}
