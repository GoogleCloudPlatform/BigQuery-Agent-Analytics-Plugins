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
// with a token already at hand (for a table it cannot find, the table's
// creation and one more send), and in a `-p` run a replay of parked files,
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
import { type HttpAnswer, TOKEN_INFO_URL, apiErrorMessage, classifyInsert, createTableBody, insertAllBody, insertAllUrl, isDatasetMissing, tablesUrl } from './bigquery.ts'
import { type DeadLetterKind, type DeadLetterLine, HOST_FILE_BYTES, MAX_FILE_BYTES, type ParsedFile, fileName, fnv1a, isDeadLetterKind, kindOf, lineText, packBodies, packFiles, packLines, parseFile, sealFile } from './deadletter.ts'
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
  // Files held by a standing `sending: true` mark or cut-off record that
  // require `/bqaa-backfill force` to replay.
  heldFiles?: number
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
// (EXIT_SPILL_RESERVE_MS before), for its replay to start a multi-batch write
// (EXIT_REPLAY_RESERVE_MS before), for a single-batch replay request to commit
// (EXIT_REPLAY_COMMIT_MS before), and the end of its budget.
type ExitMarks = { token: Mark; post: Mark; replay: Mark; replayPost: Mark; end: Mark }
type ReplayExitMarks = Pick<ExitMarks, 'replay' | 'replayPost' | 'end'>

// What a replay learns as it goes: how many of its rows still wait for a
// replay that runs by itself (lines, and files it did not read), and how
// many requests it sent.
type ReplayTally = { waiting: number; requests: number }

// Per-backfill-run cache of parsed complete dead-letter file parent headers
// indexed by basename, keeping torn-marker scans linear across a directory.
type RunParentIndex = Map<string, { size: number; parent: string | null }>

// What a replay made of a file's lines, one letter a line in the file's
// order (L landed, W waits as it was, R refused, K kept as written: another
// table's, or a kind this replay does not send), with each refused line's
// reason (`refusals`, in the same order, indexes `reasons`, which holds
// MAX_REASONS at most) and any `W` line indices whose exit-replay request was
// cut off or timed out after dispatch (`uncertainLines`).
type Outcome = { fates: string; reasons: string[]; refusals: number[]; uncertainLines?: number[] }

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
type Source = {
  dir: string
  name: string
  size: number
  fnv1a?: string
  strictFnv1a?: boolean
  recordKey: string
  recordFiles: readonly string[]
  v2RecordKey: string
  v2RecordFiles: readonly string[]
  live: boolean
}

// A file a replay has an outcome for: the outcome, the files that hold what
// is left of it under the names given them (`text` null once written),
// whether any of those holds rows that wait for a replay, whether its record
// may be in `recordFiles`, the number of its last record (`seq`), and whether
// this write converted the writer's sending mark into a retired done mark.
// Exact content identity of a reconciled or pending-reconciliation child file,
// validated by byte length and FNV-1a checksum before any truncation so a newer
// generation repopulating the same basename is never emptied. `needsLeftovers:
// false` records that none of the child's skipped rows overlapped any non-`'L'`
// parent leftover (`r.files \ r.adopted`).
type ReconciledFile = { name: string; size: number; fnv1a: string; skipIndices?: number[]; needsLeftovers?: boolean }

// Exact content identity of a staged Phase 3 replacement part or adopted/leftover
// deliverable recorded in v2 before writing to disk (`replaces` records the
// owning child basename for adopted children and synthesized replacements).
type StagedFile = { name: string; size: number; fnv1a: string; replaces?: string }

// Exact content identity of a pre-dispatch standalone backup (`parent: null`)
// staged before forced parent HTTP dispatch so a `0.1.3` `/bqaa-backfill force`
// after a crash never loses child rows while `0.1.4` drops it once superseded.
type BackupFile = { name: string; size: number; fnv1a: string; replaces: string; replacesSize: number; replacesFnv1a?: string }

// Snapshot fingerprint of a stale unretired v1 record that could neither be
// deleted nor neutralized in place when a v2 checkpoint took over, allowing
// subsequent 0.1.4 sessions to resume v2 instead of stalling in `'conflict'`.
type SupersededV1 = { size: number; seq: number; fates: string; files: string[] }

type Rewrite = Outcome &
  Source & {
    sourceFnv1a?: string
    files: { path: string; text: string | null; uncertain?: boolean }[]
    // Live children adopted from a superseded same-size conflicting journal
    // during `/bqaa-backfill force`, kept separate from the positional plan
    // in `files` so interrupted-write recovery still matches `leftoverFiles()`.
    adoptedFiles?: string[]
    // Earlier same-size live children whose rows are already fully accounted
    // for by this forced rewrite (or moved into replacement adopted files),
    // kept in retired `files` (so 0.1.3 preserves repopulated children) with
    // exact `{ size, fnv1a }` digests in `reconciled` (so 0.1.4 drops unchanged
    // stale copies and empties them only while content still matches).
    reconciledFiles?: ReconciledFile[]
    // Earlier same-size live children that must be reconciled before parent
    // retirement (mixed children awaiting replacement writes, or children held
    // by a foreign lease / transient read error), persisted in v2 so parent's
    // confirmed outcome is durable first and automatic replay holds them.
    pendingReconcile?: ReconciledFile[]
    // Planned replacement parts pre-recorded with `{ name, size, fnv1a }` in v2
    // before writing any part to disk in Phase 3 so a partial multi-part write
    // failure or crash never exposes an unpromoted part or truncates a foreign file.
    stagedFiles?: StagedFile[]
    // Standalone (`parent: null`) backup copies of guarded `parent: P` children
    // staged before forced parent HTTP dispatch so `0.1.3 force` after a crash
    // delivers them (deduped by `event_id`) and `0.1.4` cleans them up safely.
    backupFiles?: BackupFile[]
    // Non-uncertain standalone files (`parent: null`) currently guarded by a
    // temporary `doneFileOf(..., this.mark)` sending mark until parent
    // retirement or reconciled-child cleanup settles.
    // Exact `{ name, size, fnv1a }` fingerprints of deliverable leftover and
    // adopted replacement files so `unretiredV2ReplacementsAccounted` never
    // mistakes a reused basename for the original replacement generation.
    deliverableRefs?: StagedFile[]
    tempMarks?: Set<string>
    // Live children guarded by `0.1.3`-honored `childGuard` sending marks
    // (`childPath -> guardedSize`) placed before forced parent replay or v2
    // persistence so 0.1.3 neither drops nor resends them across restarts.
    childGuardMarks?: Map<string, number>
    // Unique replay attempt token bound to pre-dispatch child guards/backups,
    // plus all prior standing guard attempts observed on those children so
    // `stagedGuardState` never authorizes recovery from a pre-force v2 record.
    guardAttempt?: string
    guardAttempts?: string[]
    supersedesV1?: SupersededV1
    accountedRowKeys?: ReadonlySet<string>
    // Subset of `accountedRowKeys` whose rows were NOT `'L'` on the parent
    // (i.e. carried by parent leftovers `r.files \ r.adopted`). Used during
    // child reconciliation to stamp `needsLeftovers: false` when a child only
    // overlapped already-landed parent rows.
    leftoverRowKeys?: ReadonlySet<string>
    waits: boolean
    filed: boolean
    seq: number
    wroteDone?: boolean
    useV2?: boolean
    stampedIds?: boolean
    layoutsDiffer?: boolean
    deferredByLease?: boolean
    force?: boolean
    supersededUnaccounted?: Partial<Journal>
  }

// The record of a Rewrite, which every writer reads: written before what is
// left is, and updated as each file of it is written; once every one is,
// written as the mark that the file was done (`retired`, in the writer's
// `doneFileOf`),
// which is kept a day. A writer that finds it (a later session.start's,
// another session's, or this one after another writer moved it on) writes
// what is still missing, from the source and the outcome, and sends none of
// the file's rows again.
type Journal = Outcome & {
  size: number
  sourceFnv1a?: string
  files: string[]
  written: boolean[]
  adopted?: string[]
  deliverableRefs?: StagedFile[]
  reconciled?: ReconciledFile[]
  pendingReconcile?: ReconciledFile[]
  stagedFiles?: (string | StagedFile)[]
  backupFiles?: BackupFile[]
  attempt?: string
  attempts?: string[]
  accountedKeys?: string[]
  supersedesV1?: SupersededV1
  uncertain?: boolean[]
  stampedIds?: boolean
  waits: boolean
  seq: number
  retired?: boolean
  retiredAt?: number
}

const TIMED_OUT = Symbol('timed out')
const CUTOFF = Symbol('cutoff')
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
// Session end's replay starts only with at least EXIT_REPLAY_MIN_MS left
// (EXIT_REPLAY_POST_MIN_MS HTTP window plus EXIT_REPLAY_RESERVE_MS for
// multi-batch outcome writes), and gives first requests up to
// EXIT_REPLAY_COMMIT_MS before the budget ends to land and commit.
const EXIT_REPLAY_POST_MIN_MS = 300
const EXIT_REPLAY_RESERVE_MS = 100
const EXIT_REPLAY_COMMIT_MS = 25
const EXIT_REPLAY_MIN_MS = EXIT_REPLAY_POST_MIN_MS + EXIT_REPLAY_RESERVE_MS
// Historical cleanup runs only after live spills, and during session-end
// replay takes at most this cap and share of the remaining exit budget.
const EXIT_CLEANUP_CAP_MS = 100
const EXIT_CLEANUP_SHARE = 0.25

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

// Prefix for deterministic fallback `event_id` values synthesized for legacy
// dead-letter rows that lacked `row.event_id`. Giving such rows a stable
// `event_id` ensures BigQuery `insertAll` carries a consistent `insertId` and
// the SQL daily views deduplicate them across both 0.1.3 and 0.1.4 replays.
const FALLBACK_EVENT_ID_PREFIX = 'dl:'
const fallbackEventId = (itemKey: string): string => {
  const full = `${FALLBACK_EVENT_ID_PREFIX}${itemKey}`
  if (utf8Length(full) <= 120) return full
  const asciiPrefix = itemKey.replace(/[^\x20-\x7e]/g, '').slice(0, 64)
  const h1 = fnv1a(itemKey)
  const h2 = fnv1a(`${itemKey}\0${itemKey.length}`)
  return asciiPrefix ? `${FALLBACK_EVENT_ID_PREFIX}${h1}${h2}:${asciiPrefix}` : `${FALLBACK_EVENT_ID_PREFIX}${h1}${h2}`
}

// How the refused and parked counts know a line's row: its event_id; for one
// with none (in a file another writer made), the key a replay gave the line
// as it rewrote it, else the file (`name`) and the line's place there, which
// that key keeps. So such a row counts once however often it is refused,
// and rows alike in two files count as two.
const keyOf = (line: DeadLetterLine, name: string, index: number): string => {
  const id = line.row.event_id
  if (typeof id === 'string' && id !== '') {
    if (id.startsWith(FALLBACK_EVENT_ID_PREFIX) && id.length > FALLBACK_EVENT_ID_PREFIX.length) {
      if (typeof line.key === 'string' && line.key !== '') return line.key
      return id.slice(FALLBACK_EVENT_ID_PREFIX.length)
    }
    return id
  }
  return typeof line.key === 'string' && line.key !== '' ? line.key : `${name}#${index}`
}

// Preserves existing `row.event_id` unchanged; for ID-less dead-letter rows,
// stamps both envelope `key` and deterministic `row.event_id` so BigQuery
// `insertAll` (`insertId`) and daily views deduplicate across versions.
const ensureRowEventId = (line: DeadLetterLine, name: string, index: number): DeadLetterLine => {
  const id = line.row.event_id
  if (typeof id === 'string' && id !== '') return line
  const itemKey = keyOf(line, name, index)
  return { ...line, key: itemKey, row: { ...line.row, event_id: fallbackEventId(itemKey) } }
}

// Whether an outcome is one for these lines: a letter for each, and a reason
// it holds for each refused one.
const fitsLines = (file: ParsedFile, outcome: Outcome): boolean =>
  outcome.fates.length === file.lines.length &&
  /^[LWRK]*$/.test(outcome.fates) &&
  outcome.refusals.length === outcome.fates.split('R').length - 1 &&
  outcome.refusals.every(k => outcome.reasons[k] !== undefined) &&
  (outcome.uncertainLines === undefined ||
    (Array.isArray(outcome.uncertainLines) &&
      outcome.uncertainLines.every(n => typeof n === 'number' && Number.isInteger(n) && n >= 0 && n < file.lines.length && outcome.fates[n] === 'W')))

const baseOf = (p: string): string => p.split(/[\\/]/).pop() ?? p

const dirOf = (p: string): string => {
  const cut = Math.max(p.lastIndexOf('/'), p.lastIndexOf('\\'))
  return cut <= 0 ? (cut === 0 ? p[0] as string : '') : p.slice(0, cut)
}

const inDir = (path: string, dir: string): boolean => {
  const trimmed = dir.replace(/[\\/]+$/, '')
  if (trimmed === '') return (path.startsWith('/') || path.startsWith('\\')) && !path.slice(1).includes('/') && !path.slice(1).includes('\\')
  if (!path.startsWith(trimmed)) return false
  const sep = path[trimmed.length]
  if (sep !== '/' && sep !== '\\') return false
  const rest = path.slice(trimmed.length + 1)
  return rest !== '' && !rest.includes('/') && !rest.includes('\\')
}

const dirKey = (dir: string): string => {
  const trimmed = dir.replace(/[\\/]+$/, '')
  return trimmed === '' && (dir.startsWith('/') || dir.startsWith('\\')) ? '/' : trimmed
}

const sameNames = (a: readonly string[], b: readonly string[]): boolean =>
  a.length === b.length && a.every((name, i) => baseOf(name) === baseOf(b[i] ?? ''))

type HeldEntry = { reason: string; mayBeActive: boolean; activeCertain?: boolean }
type ActiveSendingMark = { path: string; name: string; dir: string; trigger: BackfillReport['trigger']; size: number }

// A record as a writer finds it, and whether one may be in a file beside the
// source (to empty along with it). `v2` marks versioned split journals carrying
// `uncertainLines`, stored under keys/filenames older 0.1.3 readers ignore.
type Found = { journal: Partial<Journal>; filed: boolean; v2?: boolean }
// No outcome recorded, and replays' marks (`pending`, the writers that left
// them; `activeBy`, standing marks from non-self sessions; `nonUncertainBy`,
// complete non-uncertain marks whose owner may still be live): each sent rows,
// or is about to, and may yet record one.
type Pending = {
  pending: string[]
  activeBy?: string[]
  nonUncertainBy?: string[]
  nonStagedBy?: string[]
  uncertain?: boolean
  stagedFor?: string
  stagedForSize?: number
  childGuard?: boolean
  replaces?: string
  attempts?: string[]
  backup?: boolean
  backupForSize?: number
  backupForFnv1a?: string
  backupFnv1a?: string
}

// Where the record of the file `name`, `size` long, in `dir` is kept.
const sourceFor = (dir: string, name: string, size: number, c: Config): Source => {
  const dest = destinationDir(tableRef(c))
  return {
    dir,
    name,
    size,
    recordKey: `replayed:${dest}/${name}`,
    recordFiles: [joinPath(dir, `outcome-${name}`), joinPath(dir, `outcome-b-${name}`)],
    v2RecordKey: `replayed:v2:${dest}/${name}`,
    v2RecordFiles: [joinPath(dir, `outcome2-${name}`), joinPath(dir, `outcome2-b-${name}`)],
    live: true,
  }
}
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
  let candidate: string | null = marked !== null ? (marked[1] as string) : null
  if (candidate === null) {
    for (const prefix of ['outcome2-b-', 'outcome2-', 'outcome-b-', 'outcome-']) {
      if (name.startsWith(prefix)) {
        candidate = name.slice(prefix.length)
        break
      }
    }
  }
  return candidate !== null && kindOf(candidate) !== null ? candidate : null
}

const isNotFoundError = (err: unknown): boolean => {
  if (typeof err === 'object' && err !== null && 'code' in err) {
    const code = (err as { code?: unknown }).code
    if (typeof code === 'string' && code !== '') return code === 'ENOENT'
  }
  return /\bENOENT\b|no such file|file not found/i.test(errorText(err))
}

const journalChildNames = (journal: Partial<Journal>): string[] => {
  const names: string[] = []
  if (Array.isArray(journal.files)) {
    for (const f of journal.files) {
      if (typeof f === 'string' && f !== '') names.push(baseOf(f))
    }
  }
  for (const ref of normalizeReconciled(journal.pendingReconcile, true)) names.push(ref.name)
  for (const ref of normalizeReconciled(journal.reconciled)) names.push(ref.name)
  for (const staged of normalizeStaged(journal.stagedFiles)) names.push(staged)
  for (const deliv of normalizeStagedRefs(journal.deliverableRefs)) names.push(deliv.name)
  for (const b of normalizeBackups(journal.backupFiles)) names.push(b.name)
  return names
}

const sameLeftoverPartitioning = (
  a: readonly { kind: DeadLetterKind; text: string; uncertain?: boolean }[],
  b: readonly { kind: DeadLetterKind; text: string; uncertain?: boolean }[],
): boolean => {
  if (a.length !== b.length) return false
  const bodyLines = (text: string): number => text.split('\n').length - 2
  return a.every(
    (part, i) =>
      part.kind === b[i]?.kind &&
      Boolean(part.uncertain) === Boolean(b[i]?.uncertain) &&
      bodyLines(part.text) === bodyLines(b[i]!.text),
  )
}

const isEmptiedSentinel = (journal: Partial<Journal>): boolean =>
  journal.size === 0 &&
  (journal.fates === undefined || journal.fates === '') &&
  (!Array.isArray(journal.files) || journal.files.length === 0)

const groupCompanions = <T extends { name: string; kind?: string; size: number }>(entries: readonly T[]): Map<string, T[]> => {
  const bySource = new Map<string, T[]>()
  for (const entry of entries) {
    if ((entry.kind !== undefined && entry.kind !== 'file') || entry.size <= 0) continue
    const of = recordOf(entry.name)
    if (of === null) continue
    const list = bySource.get(of)
    if (list) list.push(entry)
    else bySource.set(of, [entry])
  }
  return bySource
}

// Which files of what is left a record has as written: all of them for a
// record that does not say.
const writtenOf = (journal: Partial<Journal>): boolean[] => {
  const files = Array.isArray(journal.files) ? journal.files : []
  return Array.isArray(journal.written) ? files.map((_, i) => journal.written?.[i] === true) : files.map(() => true)
}

const isClearedSlot = (val: unknown): boolean =>
  val === undefined || val === null || (typeof val === 'object' && !Array.isArray(val) && (val as { cleared?: unknown }).cleared === true)

const stripParentSeal = (sealed: string): string => {
  if (!sealed.endsWith('\n')) return sealed
  const start = sealed.lastIndexOf('\n', sealed.length - 2) + 1
  const parsed = parseFile(sealed)
  if (!parsed.complete || parsed.parent === null) return sealed
  return sealFile(sealed.slice(0, start))
}

const normalizeSupersededV1 = (value: unknown): SupersededV1 | undefined => {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined
  const rec = value as { size?: unknown; seq?: unknown; fates?: unknown; files?: unknown }
  if (
    typeof rec.size !== 'number' ||
    !Number.isFinite(rec.size) ||
    typeof rec.seq !== 'number' ||
    !Number.isFinite(rec.seq) ||
    typeof rec.fates !== 'string' ||
    !Array.isArray(rec.files)
  ) {
    return undefined
  }
  const files: string[] = []
  for (const item of rec.files) {
    if (typeof item !== 'string' || item === '') return undefined
    files.push(baseOf(item))
  }
  return { size: rec.size, seq: rec.seq, fates: rec.fates, files }
}

const matchesSupersededV1 = (v1: Partial<Journal>, ref: SupersededV1 | undefined): boolean =>
  ref !== undefined &&
  v1.retired !== true &&
  v1.size === ref.size &&
  seqOf(v1) === ref.seq &&
  v1.fates === ref.fates &&
  sameNames(Array.isArray(v1.files) ? v1.files : [], ref.files)

const normalizeReconciled = (value: unknown, allowEmptyChecksum = false): ReconciledFile[] => {
  if (!Array.isArray(value)) return []
  const seen = new Set<string>()
  const out: ReconciledFile[] = []
  for (const item of value) {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) continue
    const rec = item as { name?: unknown; size?: unknown; fnv1a?: unknown; skipIndices?: unknown; needsLeftovers?: unknown }
    if (typeof rec.name !== 'string' || rec.name === '') continue
    const name = baseOf(rec.name)
    if (seen.has(name)) continue
    const size = typeof rec.size === 'number' && Number.isFinite(rec.size) && rec.size >= 0 ? rec.size : -1
    const digest = typeof rec.fnv1a === 'string' ? rec.fnv1a : ''
    if (size < 0 || (!allowEmptyChecksum && (size === 0 || digest === ''))) continue
    const skipIndices = Array.isArray(rec.skipIndices)
      ? [...new Set(rec.skipIndices.filter((n): n is number => typeof n === 'number' && Number.isInteger(n) && n >= 0))].sort((x, y) => x - y)
      : []
    seen.add(name)
    out.push({
      name,
      size,
      fnv1a: digest,
      ...(skipIndices.length > 0 ? { skipIndices } : {}),
      ...(rec.needsLeftovers === false ? { needsLeftovers: false } : {}),
    })
  }
  return out
}

const sameSkipIndices = (a: readonly number[] | undefined, b: readonly number[] | undefined): boolean => {
  const la = a ?? []
  const lb = b ?? []
  return la.length === lb.length && la.every((v, i) => v === lb[i])
}

const sameReconciled = (a: readonly ReconciledFile[], b: readonly ReconciledFile[]): boolean =>
  a.length === b.length &&
  a.every(
    (x, i) =>
      x.name === b[i]?.name &&
      x.size === b[i]?.size &&
      x.fnv1a === b[i]?.fnv1a &&
      sameSkipIndices(x.skipIndices, b[i]?.skipIndices) &&
      x.needsLeftovers === b[i]?.needsLeftovers,
  )

const normalizeStagedRefs = (value: unknown): StagedFile[] => {
  if (!Array.isArray(value)) return []
  const byName = new Map<string, StagedFile>()
  for (const item of value) {
    if (typeof item === 'string') {
      if (item === '') continue
      const name = baseOf(item)
      if (!byName.has(name)) byName.set(name, { name, size: -1, fnv1a: '' })
      continue
    }
    if (item === null || typeof item !== 'object' || Array.isArray(item)) continue
    const rec = item as { name?: unknown; size?: unknown; fnv1a?: unknown; replaces?: unknown }
    if (typeof rec.name !== 'string' || rec.name === '') continue
    const name = baseOf(rec.name)
    const size = typeof rec.size === 'number' && Number.isFinite(rec.size) && rec.size >= 0 ? rec.size : -1
    const digest = typeof rec.fnv1a === 'string' ? rec.fnv1a : ''
    const replaces = typeof rec.replaces === 'string' && rec.replaces !== '' ? baseOf(rec.replaces) : undefined
    const existing = byName.get(name)
    if (existing === undefined) {
      byName.set(name, { name, size, fnv1a: digest, ...(replaces !== undefined ? { replaces } : {}) })
    } else if ((existing.fnv1a === '' && digest !== '') || (existing.replaces === undefined && replaces !== undefined)) {
      const mergedReplaces = existing.replaces ?? replaces
      byName.set(name, {
        name,
        size: existing.size >= 0 ? existing.size : size,
        fnv1a: existing.fnv1a !== '' ? existing.fnv1a : digest,
        ...(mergedReplaces !== undefined ? { replaces: mergedReplaces } : {}),
      })
    }
  }
  return [...byName.values()]
}

const normalizeStaged = (value: unknown): string[] => normalizeStagedRefs(value).map(s => s.name)

const sameStaged = (a: readonly StagedFile[], b: readonly StagedFile[]): boolean =>
  a.length === b.length && a.every((x, i) => x.name === b[i]?.name && x.size === b[i]?.size && x.fnv1a === b[i]?.fnv1a && x.replaces === b[i]?.replaces)

const normalizeBackups = (value: unknown): BackupFile[] => {
  if (!Array.isArray(value)) return []
  const seen = new Set<string>()
  const out: BackupFile[] = []
  for (const item of value) {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) continue
    const rec = item as { name?: unknown; size?: unknown; fnv1a?: unknown; replaces?: unknown; replacesSize?: unknown; replacesFnv1a?: unknown }
    if (typeof rec.name !== 'string' || rec.name === '' || typeof rec.replaces !== 'string' || rec.replaces === '') continue
    const name = baseOf(rec.name)
    if (seen.has(name)) continue
    const size = typeof rec.size === 'number' && Number.isFinite(rec.size) && rec.size > 0 ? rec.size : -1
    const digest = typeof rec.fnv1a === 'string' ? rec.fnv1a : ''
    const replacesSize = typeof rec.replacesSize === 'number' && Number.isFinite(rec.replacesSize) && rec.replacesSize > 0 ? rec.replacesSize : -1
    const replacesFnv1a = typeof rec.replacesFnv1a === 'string' && rec.replacesFnv1a !== '' ? rec.replacesFnv1a : undefined
    if (size <= 0 || digest === '' || replacesSize <= 0) continue
    seen.add(name)
    out.push({ name, size, fnv1a: digest, replaces: baseOf(rec.replaces), replacesSize, ...(replacesFnv1a !== undefined ? { replacesFnv1a } : {}) })
  }
  return out
}

const sameBackups = (a: readonly BackupFile[], b: readonly BackupFile[]): boolean =>
  a.length === b.length &&
  a.every(
    (x, i) =>
      x.name === b[i]?.name &&
      x.size === b[i]?.size &&
      x.fnv1a === b[i]?.fnv1a &&
      x.replaces === b[i]?.replaces &&
      x.replacesSize === b[i]?.replacesSize &&
      (x.replacesFnv1a ?? '') === (b[i]?.replacesFnv1a ?? ''),
  )

const normalizeAttempts = (single: unknown, list?: unknown): string[] => {
  const out: string[] = []
  const seen = new Set<string>()
  const add = (val: unknown): void => {
    if (typeof val !== 'string' || val === '' || seen.has(val)) return
    seen.add(val)
    out.push(val)
  }
  if (Array.isArray(list)) for (const item of list) add(item)
  add(single)
  return out
}

const coversAllAttempts = (journal: Partial<Journal>, required: readonly string[] | undefined): boolean => {
  if (required === undefined || required.length === 0) return true
  const covered = new Set(normalizeAttempts(journal.attempt, journal.attempts))
  return required.every(a => covered.has(a))
}

// Row identity for forced-replay parent/child reconciliation. Uses `event_id`
// when present; for legacy rows without `event_id`, uses the persisted `key`
// (or reconstructs `<parentName>#<index>` for unkeyed parent rows). Child rows
// lacking both `event_id` and `key` return `null` so distinct events with
// identical payloads are never dropped.
const parentRowIdentity = (line: DeadLetterLine, parentName: string, index: number): string => {
  const id = line.row.event_id
  if (typeof id === 'string' && id !== '') {
    if (id.startsWith(FALLBACK_EVENT_ID_PREFIX) && id.length > FALLBACK_EVENT_ID_PREFIX.length) {
      const k = typeof line.key === 'string' && line.key !== '' ? line.key : id.slice(FALLBACK_EVENT_ID_PREFIX.length)
      return `key:${line.table}\0${k}`
    }
    return `id:${line.table}\0${id}`
  }
  const key = typeof line.key === 'string' && line.key !== '' ? line.key : `${parentName}#${index}`
  return `key:${line.table}\0${key}`
}

const childRowIdentity = (line: DeadLetterLine): string | null => {
  const id = line.row.event_id
  if (typeof id === 'string' && id !== '') {
    if (id.startsWith(FALLBACK_EVENT_ID_PREFIX) && id.length > FALLBACK_EVENT_ID_PREFIX.length) {
      const k = typeof line.key === 'string' && line.key !== '' ? line.key : id.slice(FALLBACK_EVENT_ID_PREFIX.length)
      return `key:${line.table}\0${k}`
    }
    return `id:${line.table}\0${id}`
  }
  if (typeof line.key === 'string' && line.key !== '') return `key:${line.table}\0${line.key}`
  return null
}

// The number of a record (0 for one that does not say).
const seqOf = (journal: Partial<Journal>): number => (typeof journal.seq === 'number' ? journal.seq : 0)

// Resolves the effective `0.1.3`-visible v1 journal for `size` (matching
// `0.1.3`'s `readJournal` precedence: any whole same-size non-sentinel retired
// done mark wins; otherwise combine `stored` and highest-seq `filed` by
// bitwise-ORing `written` when `files` match, or taking `filed` when they
// differ). Returns that effective journal only when it belongs to an earlier
// equal-size generation (`sourceFnv1a !== liveDigest`), is unretired, and still
// has an unwritten part (`written[i] === false`).
const effectiveUnretiredEarlierV1 = (
  size: number,
  liveDigest: string,
  v1Slot: { stored: Partial<Journal> | undefined; filed: readonly Partial<Journal>[] },
  v1DoneRecords: readonly Partial<Journal>[],
): Partial<Journal> | undefined => {
  if (v1DoneRecords.some(rec => !isEmptiedSentinel(rec) && rec.size === size && rec.retired === true)) {
    return undefined
  }
  const stored = v1Slot.stored !== undefined && v1Slot.stored.size === size ? v1Slot.stored : undefined
  let filed: Partial<Journal> | undefined
  for (const f of v1Slot.filed) {
    if (f.size === size && (filed === undefined || seqOf(f) > seqOf(filed))) filed = f
  }
  let eff: Partial<Journal> | undefined
  if (stored === undefined) {
    eff = filed
  } else if (filed === undefined) {
    eff = stored
  } else if (!Array.isArray(stored.files) || !Array.isArray(filed.files) || !sameNames(stored.files, filed.files)) {
    eff = filed
  } else {
    const a = writtenOf(stored)
    const b = writtenOf(filed)
    const mergedDigest =
      typeof stored.sourceFnv1a === 'string' && stored.sourceFnv1a !== ''
        ? stored.sourceFnv1a
        : typeof filed.sourceFnv1a === 'string' && filed.sourceFnv1a !== ''
          ? filed.sourceFnv1a
          : undefined
    eff = {
      ...stored,
      written: a.map((w, i) => w || b[i] === true),
      seq: Math.max(seqOf(stored), seqOf(filed)),
      retired: stored.retired === true || filed.retired === true,
      ...(mergedDigest !== undefined ? { sourceFnv1a: mergedDigest } : {}),
    }
  }
  if (
    eff === undefined ||
    eff.retired === true ||
    typeof eff.sourceFnv1a !== 'string' ||
    eff.sourceFnv1a === '' ||
    eff.sourceFnv1a === liveDigest ||
    !writtenOf(eff).some(w => !w)
  ) {
    return undefined
  }
  return eff
}

// Collects all child basenames recorded as written (not pending-reconcile or
// `written[idx] === false`) on a v1 journal record in O(|files| + |children|).
const writtenChildrenOfV1 = (r: Partial<Journal>, parentName: string): Set<string> => {
  const out = new Set<string>()
  const written = writtenOf(r)
  const fileBasenames = new Set<string>()
  if (Array.isArray(r.files)) {
    r.files.forEach((f, idx) => {
      if (typeof f !== 'string' || f === '') return
      const childName = baseOf(f)
      if (childName === '') return
      fileBasenames.add(childName)
      if (childName !== parentName && written[idx] === true) out.add(childName)
    })
  }
  const pendingNames = new Set(normalizeReconciled(r.pendingReconcile, true).map(p => p.name))
  for (const childName of journalChildNames(r)) {
    if (childName === '' || childName === parentName) continue
    if (!pendingNames.has(childName) && !fileBasenames.has(childName)) out.add(childName)
  }
  return out
}

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
function leftoverFiles(
  file: ParsedFile,
  fileKind: DeadLetterKind,
  outcome: Outcome,
  name: string,
  stampFallbackIds = true,
): { files: { kind: DeadLetterKind; text: string; uncertain?: boolean }[]; stampedIds: boolean } {
  const uncertainSet = outcome.uncertainLines && outcome.uncertainLines.length > 0 ? new Set(outcome.uncertainLines) : null
  const uncertainRest = new Map<DeadLetterKind, string[]>()
  const rest = new Map<DeadLetterKind, string[]>()
  const keepInto = (target: Map<DeadLetterKind, string[]>, kind: DeadLetterKind, text: string): void => {
    const texts = target.get(kind)
    if (texts) texts.push(text)
    else target.set(kind, [text])
  }
  const kindOfLine = (line: DeadLetterLine): DeadLetterKind => (isDeadLetterKind(line.kind) ? line.kind : fileKind)
  let stampedIds = false
  const keyed = (line: DeadLetterLine, i: number): DeadLetterLine => {
    const id = line.row.event_id
    if (typeof id === 'string' && id !== '') return line
    if (stampFallbackIds) {
      stampedIds = true
      return ensureRowEventId(line, name, i)
    }
    // Exact 0.1.3 v1 layout: keep ID-less row.event_id untouched and attach
    // envelope `key` only so resuming a v1 checkpoint preserves 0.1.3 part
    // boundaries byte-for-byte.
    const legacyKey = typeof line.key === 'string' && line.key !== '' ? line.key : `${name}#${i}`
    return { ...line, key: legacyKey }
  }
  let refused = 0
  file.lines.forEach((line, i) => {
    const fate = outcome.fates[i]
    if (fate === 'W') {
      const k = kindOfLine(line)
      const formatted = lineText({ ...keyed(line, i), kind: k })
      if (uncertainSet?.has(i)) keepInto(uncertainRest, k, formatted)
      else keepInto(rest, k, formatted)
    } else if (fate === 'R') {
      const reason = outcome.reasons[outcome.refusals[refused] ?? -1] ?? ''
      refused += 1
      keepInto(rest, 'rejected', lineText({ ...keyed(line, i), kind: 'rejected', reason, attempts: line.attempts + 1 }))
    }
  })
  file.lines.forEach((line, i) => {
    if (outcome.fates[i] === 'K') keepInto(rest, kindOfLine(line), file.raw[i] as string)
  })
  for (const text of file.unreadable) keepInto(rest, fileKind, text)
  const files: { kind: DeadLetterKind; text: string; uncertain?: boolean }[] = []
  for (const [kind, texts] of uncertainRest) for (const text of packFiles(texts, name)) files.push({ kind, text, uncertain: true })
  for (const [kind, texts] of rest) for (const text of packFiles(texts, name)) files.push({ kind, text })
  return { files, stampedIds }
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
  // After a recovery backfill finds files held by another session (or still
  // incomplete / waiting on a parent), defer the next idle recovery check for
  // 30 s via host timer without triggering error backoff.
  private heldCheckTimer: Timer | null = null
  // The replay of parked rows that a send after failures set going
  // (`succeeded`), due a second later: the exit cancels it, and sends
  // nothing of its own while it is due.
  private recoveryTimer: Timer | null = null
  // Set once a batch this writer had taken went back to the queue because
  // the exit began (it was waiting for its token or its account): the exit
  // then sends nothing of its own, and spills that batch with the rest.
  private heldAtExit = false
  // While an exit that may send waits for this writer's batch out
  // (spillForExit): a table-not-found answer to it is left here
  // (`notFound`) with the batch back on the queue, so the exit makes the
  // table and sends the batch once more before anything is parked.
  private exitWait: { notFound: HttpAnswer | null } | null = null
  // While session end's own replay runs (`replayAtExit`): it goes on past
  // `closing` until `until`/`cutoff` (or `postUntil`/`postCutoff` on first
  // un-progressed requests), whichever comes first, and takes no token but
  // one already at hand.
  private exitReplay: { until: number; postUntil: number; cutoff: Mark; postCutoff: Mark } | null = null
  // Source paths whose exit-replay request timed out or hit an exit cutoff in
  // this process: held until /bqaa-backfill force even when the standing mark
  // belongs to `this.mark`.
  private uncertainSources = new Map<string, number>()
  // Latest count and summary of dead-letter files held for `/bqaa-backfill force`
  // (surfaced by `-p` `headlessNotice` when `problem === null`), plus known
  // held file paths so a scan that stops early preserves standing held marks.
  private lastHeld: { files: number; reason: string | null; trigger: BackfillReport['trigger'] | null; mayBeActive: boolean } = { files: 0, reason: null, trigger: null, mayBeActive: false }
  private inProgressHeld: { files: number; reason: string | null; trigger: BackfillReport['trigger'] | null; mayBeActive: boolean } = { files: 0, reason: null, trigger: null, mayBeActive: false }
  private currentScanHeld = new Map<string, HeldEntry>()
  private heldPaths = new Map<string, HeldEntry>()
  private heldCountByDir = new Map<string, number>()
  private heldActiveCountByDir = new Map<string, number>()
  // Sources for which this writer has written a standing sending mark on disk
  // (`markSending`) and has not yet confirmed its release (`unmark`) or
  // retirement (`writeOut`), so exit notices stay aligned with durable marks.
  private activeSendingMarks = new Map<string, ActiveSendingMark>()
  private activeExitMarks: ReplayExitMarks | null = null
  private pendingReplayWakeups = new Set<(marks: ReplayExitMarks) => void>()
  private pendingCleanupWakeups = new Set<() => void>()
  // Orphaned v2 store keys already verified/deleted for retired sources in this session.
  private cleanedRetiredV2 = new Set<string>()
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
    for (const [p, entry] of this.heldPaths) previous.setHeldPath(p, entry)
    this.heldPaths = previous.heldPaths
    this.heldCountByDir = previous.heldCountByDir
    this.heldActiveCountByDir = previous.heldActiveCountByDir
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

  // Prompts, manual flushes, and explicit commands clear the held-file poll
  // cooldown so recovery can look again immediately.
  resetHeldCooldown(): void {
    this.heldCheckTimer?.cancel()
    this.heldCheckTimer = null
  }

  // A prompt while a problem stands: the person may have just fixed it, so
  // the paused writer tries again now rather than at the end of its backoff.
  retrySoon(): void {
    this.resetHeldCooldown()
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
  async probeDestination(timeoutMs = 3000, explicit = false, budgetMs?: number): Promise<string | null> {
    const c = this.config()
    if (!canDeliver(c) || c.dryRun || this.closing || (!explicit && (this.problem !== null || this.lastSuccessAt !== null || this.lastError !== null))) return null
    if (explicit) this.resetHeldCooldown()
    const effectiveBudgetMs = budgetMs === Number.POSITIVE_INFINITY ? undefined : budgetMs
    if (effectiveBudgetMs !== undefined && (!Number.isFinite(effectiveBudgetMs) || effectiveBudgetMs <= 0)) {
      return 'timed out (insufficient command budget)'
    }
    const startedAt = this.io.now()
    const totalBudgetMs = effectiveBudgetMs !== undefined ? effectiveBudgetMs : Math.max(timeoutMs, c.tokenCommandTimeoutMs) + timeoutMs * 2
    // When the whole check is over, the fresh token after a 401 included.
    const deadline = startedAt + totalBudgetMs
    const reqBudgetMs = effectiveBudgetMs !== undefined ? Math.min(timeoutMs, Math.max(1, Math.floor(totalBudgetMs / 2))) : timeoutMs
    const tokenTimeoutMs = Math.min(Math.max(timeoutMs, c.tokenCommandTimeoutMs), Math.max(1, totalBudgetMs - reqBudgetMs))
    const url = `${tablesUrl(c)}/${encodeURIComponent(c.table)}?fields=tableReference`
    for (let attempt = 0; attempt < 2; attempt += 1) {
      // A token's time, out of what is left: keep a full request budget on
      // attempt 0 (and on unbounded checks when > reqBudgetMs remains), and at
      // least 500 ms on bounded post-401 retries so a fresh-token retry still
      // proceeds when 500 ms .. reqBudgetMs of the deadline remains.
      const remainingBeforeToken = attempt === 0 ? totalBudgetMs : deadline - this.io.now()
      const reserveMs = attempt === 0 || (effectiveBudgetMs === undefined && remainingBeforeToken > reqBudgetMs)
        ? reqBudgetMs
        : Math.min(reqBudgetMs, 500)
      const tokenMs = Math.min(tokenTimeoutMs, Math.max(0, remainingBeforeToken - reserveMs))
      if (tokenMs <= 0) {
        if (attempt === 0) return 'timed out (insufficient command budget)'
        this.recordError(`destination check: did not finish within ${totalBudgetMs} ms`)
        return `did not finish within ${totalBudgetMs} ms`
      }
      const tokenTimers: Timer[] = []
      let token: string | symbol | null
      try {
        const tokenDeadline = this.moment(tokenMs, tokenTimers)
        token = await Promise.race([this.getToken(tokenMs).catch(() => null), tokenDeadline.at.then(() => TIMED_OUT)])
      } finally {
        for (const timer of tokenTimers) timer.cancel()
      }
      // Null: getToken raised the credentials notice.
      if (token === null) return 'no working credentials'
      if (typeof token !== 'string') {
        this.recordError(`destination check: no token within ${tokenMs} ms`)
        return `no token within ${tokenMs} ms`
      }
      const remaining = deadline - this.io.now()
      if (remaining <= 0) {
        this.recordError(`destination check: did not finish within ${totalBudgetMs} ms`)
        return `did not finish within ${totalBudgetMs} ms`
      }
      const requestMs = Math.min(timeoutMs, remaining)
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
    const partition = deadLetterPartition(current)
    if (this.queue.length === 0 && this.replayDue.has(partition)) {
      if (this.pauseTimer !== null || this.heldCheckTimer !== null || this.closing || !current.autoBackfill || !canDeliver(current) || current.dryRun) return
      const report = await this.backfill(['spill', 'retry'], 'recovery')
      // Only actual errors trigger exponential backoff; held files from other
      // sessions clear prior live failures so exit flushing stays enabled and
      // idle checks pause via host timer without setting backoff.
      if (report.error !== null) {
        this.consecutiveFailures += 1
        this.pause()
      } else {
        this.consecutiveFailures = 0
        if (report.rowsLeft - report.rowsRefused === 0 && !this.closing && this.queue.length === 0) this.parked.recoverable = false
        if (
          (report.held !== null || report.incompleteFiles.length > 0) &&
          this.replayDue.has(partition) &&
          this.queue.length === 0 &&
          !this.closing
        ) {
          this.resetHeldCooldown()
          const cooldownMs = report.held !== null ? 30_000 : 45_000
          this.heldCheckTimer = this.io.after(cooldownMs, () => {
            this.heldCheckTimer = null
          })
        }
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
        if (!this.closing && this.mayCreateTable(c, outcome.message)) {
          if (await this.createTable(c, token)) {
            this.queue.unshift(batch)
            return true
          }
        } else if (this.exitWait !== null && this.mayCreateTable(c, outcome.message)) {
          // Wait-for-in-flight handoff: pass this 404 to sendAtExit with the
          // batch back on the queue so it creates the table once and retries
          // without duplicating rows or emitting a premature missing notice.
          this.exitWait.notFound = answer
          this.batchOpen = false
          this.inFlightRows = []
          this.queue.unshift(batch)
          return false
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

  private post(
    url: string,
    body: string,
    token: string,
    timeoutMs: number,
    cutoff?: { mark: Mark; message: string; uncertainOnTimeout?: boolean },
  ): Promise<HttpAnswer | { error: string; timedOut?: boolean; uncertain?: boolean }> {
    return this.request(url, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body }, timeoutMs, cutoff)
  }

  private async request(
    url: string,
    init: HttpInit,
    timeoutMs: number,
    cutoff?: { mark: Mark; message: string; uncertainOnTimeout?: boolean },
  ): Promise<HttpAnswer | { error: string; timedOut?: boolean; uncertain?: boolean }> {
    if (cutoff?.mark.passed()) return { error: cutoff.message }
    let timer: Timer | null = null
    const timeout = new Promise<typeof TIMED_OUT>(resolve => {
      timer = this.io.after(timeoutMs, () => resolve(TIMED_OUT))
    })
    const request = this.io.fetch(url, init).then(
      answer => answer,
      err => ({ error: `request failed: ${errorText(err)}` }),
    )
    const races: Promise<HttpAnswer | { error: string } | typeof TIMED_OUT | typeof CUTOFF>[] = [request, timeout]
    if (cutoff) races.push(cutoff.mark.at.then(() => CUTOFF))
    try {
      const result = await Promise.race(races)
      if (result === TIMED_OUT) {
        return { error: `request timed out after ${timeoutMs} ms`, timedOut: true, ...(cutoff !== undefined && cutoff.uncertainOnTimeout !== false ? { uncertain: true } : {}) }
      }
      if (result === CUTOFF) return { error: cutoff!.message, uncertain: true }
      return result
    } finally {
      ;(timer as Timer | null)?.cancel()
    }
  }

  private getToken(maxTimeoutMs?: number): Promise<string> {
    const c = this.config()
    if (this.token && this.io.now() < this.token.expiresAt) return Promise.resolve(this.token.value)
    // Expired: let it go now, so a refresh that fails leaves no token behind.
    if (this.token) this.dropToken()
    if (this.tokenPending) return this.tokenPending
    const timeoutMs = maxTimeoutMs !== undefined ? Math.min(c.tokenCommandTimeoutMs, Math.max(1, maxTimeoutMs)) : c.tokenCommandTimeoutMs
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
      const result = await this.io.runCommand(c.accessTokenCommand, timeoutMs)
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

  // Whether a table-not-found answer (`message`, not a dataset's) may be met
  // by making the table: `createTable` latches `createAttempted` only on
  // terminal statuses (2xx/409/400/403/404), leaving transient 5xx/429/network
  // failures retriable on a later attempt.
  private mayCreateTable(c: Config, message: string): boolean {
    return c.autoCreateTable && !this.createAttempted && !isDatasetMissing(message)
  }

  private async createTable(c: Config, token: string, timeoutMs = c.requestTimeoutMs, cutoff?: { mark: Mark; message: string }): Promise<boolean> {
    const answer = await this.post(tablesUrl(c), createTableBody(c), token, timeoutMs, cutoff)
    if ('error' in answer) {
      this.recordError(`create table: ${answer.error}`)
      return false
    }
    if ((answer.status >= 200 && answer.status < 300) || answer.status === 409) {
      this.createAttempted = true
      this.tableCreatedAt = this.io.now()
      this.io.debug(`bqaa: created ${tableRef(c)} (day-partitioned on timestamp, clustered on event_type, agent, user_id)`)
      return true
    }
    if (answer.status === 400 || answer.status === 403 || answer.status === 404) {
      this.createAttempted = true
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
    this.resetHeldCooldown()
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
  // What it does not land in time is spilled before the budget ends. A
  // table-not-found answer to that batch of this writer's, during that wait,
  // brings its rows back to that one request, which then makes the table first
  // and is sent once (sendAtExit). With `replaySpills` too, once every row
  // has landed, this table's spill and retry files are replayed with what is
  // left of the budget (see replayAtExit).
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
      for (const wake of [...writer.pendingCleanupWakeups]) wake()
      writer.recoveryTimer?.cancel()
      writer.recoveryTimer = null
      writer.pauseTimer?.cancel()
      writer.pauseTimer = null
      writer.pausedUntil = 0
      writer.resetHeldCooldown()
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
          replayPost: this.moment(budgetMs - EXIT_REPLAY_COMMIT_MS, timers),
          end: this.moment(budgetMs, timers),
        }
      : null
    const activeBackfills = writers.filter(writer => writer.backfilling !== null)
    const wakeLiveReplays = budgetMs > 0 && opts.replaySpills === true && activeBackfills.length > 0
    const replayMarks: ReplayExitMarks | null =
      marks ??
      (wakeLiveReplays
        ? {
            replay: this.moment(budgetMs - EXIT_REPLAY_RESERVE_MS, timers),
            replayPost: this.moment(budgetMs - EXIT_REPLAY_COMMIT_MS, timers),
            end: this.moment(budgetMs, timers),
          }
        : null)
    try {
      if (wakeLiveReplays && replayMarks !== null) {
        for (const writer of writers) {
          writer.activeExitMarks = replayMarks
          for (const wake of [...writer.pendingReplayWakeups]) wake(replayMarks)
        }
      }
      if (marks === null) park(this.queue.takeAll())
      // The writers with a batch out now: an answer that comes for one during
      // the wait below can leave BigQuery's state in doubt.
      const flightWriters = writers.filter(writer => writer.inFlightRows.length > 0 && writer.inFlight !== null)
      const flights = flightWriters.flatMap(writer => (writer.inFlight !== null ? [writer.inFlight] : []))
      const wait: { notFound: HttpAnswer | null } = { notFound: null }
      if (marks !== null) this.exitWait = wait
      const waitedFlights = flights.length > 0 && budgetMs > 0
      if (waitedFlights) await this.raceUntil(Promise.all(flights), start + Math.floor(budgetMs / 2), start)
      this.exitWait = null
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
        if ((await this.sendAtExit(c, marks, park, writes, until, wait.notFound)) && opts.replaySpills === true && spilled === 0) await this.replayAtExit(c, marks, until)
      } else {
        // No send follows, so no table is made: the answer kept is told now.
        if (wait.notFound !== null) this.missingDestination(apiErrorMessage(wait.notFound))
        park([...out, ...this.queue.takeAll()])
      }
      const pending: Promise<unknown>[] = [...writes, ...this.persisting]
      if (wakeLiveReplays) {
        for (const writer of activeBackfills) if (writer.backfilling !== null) pending.push(writer.backfilling)
      }
      if (pending.length > 0) {
        if (replayMarks === null) await this.raceUntil(Promise.all(pending), until, waitedFlights ? this.io.now() : start)
        else await Promise.race([Promise.all(pending).catch(() => undefined), replayMarks.end.at])
      }
    } finally {
      for (const writer of writers) writer.activeExitMarks = null
      for (const timer of timers) timer.cancel()
    }
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
  // `notFound`, a table-not-found answer the exit wait kept (spillForExit),
  // stands for the first request's answer, so the table is made first, in
  // the same bounds, and the batch is sent once.
  private async sendAtExit(c: Config, marks: ExitMarks, park: (rows: Queued[]) => void, writes: Promise<void>[], until: number, notFound: HttpAnswer | null): Promise<boolean> {
    if (this.queue.length === 0) return true
    const token = await this.exitToken(marks.token)
    // A request of its own needs a send's time. With `notFound` the first
    // request is answered already, and the table's creation has its own bound.
    const tooLate = marks.token.passed() || until - this.io.now() < EXIT_SEND_MIN_MS
    if (token === null || (tooLate && notFound === null)) {
      if (notFound !== null) this.missingDestination(apiErrorMessage(notFound))
      park(this.queue.takeAll())
      return false
    }
    const batch = this.queue.take(c.maxBatchRows, c.maxBatchBytes)
    park(this.queue.takeAll())
    // The rows carry the account of the token in use, where its lookup has
    // answered, else the user_id they were built with: no lookup starts now.
    if (c.userIdFromCredentials) for (const q of batch) this.stamp(q, this.currentIdentity())
    const body = insertAllBody(batch.map(q => q.row))
    const postBatch = (): Promise<HttpAnswer | { error: string; uncertain?: boolean }> =>
      this.post(insertAllUrl(c), body, token, Math.max(50, until - this.io.now() - EXIT_SPILL_RESERVE_MS), { mark: marks.post, message: 'no answer before the session ended' })

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const answer = attempt === 0 && notFound !== null ? notFound : await postBatch()
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
            this.mayCreateTable(c, outcome.message) &&
            !marks.post.passed() &&
            createBudget >= EXIT_CREATE_TABLE_MIN_MS
          ) {
            const created = await this.createTable(c, token, Math.max(50, createBudget), { mark: marks.post, message: 'no answer before the session ended' })
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
    const canStart =
      c.autoBackfill &&
      this.lastSuccessAt !== null &&
      !marks.replay.passed() &&
      until - this.io.now() >= EXIT_REPLAY_MIN_MS
    const prevExitMarks = this.activeExitMarks
    this.activeExitMarks = marks
    for (const wake of [...this.pendingReplayWakeups]) wake(marks)
    try {
      if (!canStart) {
        // Wait up to marks.end for an already-in-flight backfill to finish
        // writing out its outcome; with exitReplay === null, replayHalted()
        // returns this.closing (true) so it starts no new requests.
        if (this.backfilling !== null) await Promise.race([this.backfilling, marks.end.at])
        return
      }
      this.exitReplay = {
        until: until - EXIT_REPLAY_RESERVE_MS,
        postUntil: until - EXIT_REPLAY_COMMIT_MS,
        cutoff: marks.replay,
        postCutoff: marks.replayPost,
      }
      try {
        if (this.backfilling !== null) await Promise.race([this.backfilling, marks.end.at])
        if (!marks.replay.passed() && until - this.io.now() >= EXIT_REPLAY_MIN_MS && this.backfilling === null) {
          await Promise.race([this.backfill(['spill', 'retry'], 'auto'), marks.end.at])
        }
      } finally {
        this.exitReplay = null
      }
    } finally {
      this.activeExitMarks = prevExitMarks
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

  private raceUntil(work: Promise<unknown>, until: number, now = this.io.now()): Promise<unknown> {
    const wait = until - now
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
    if (trigger === 'command' || trigger === 'force') this.resetHeldCooldown()
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

  private setHeldPath(path: string, entry: HeldEntry): void {
    const prev = this.heldPaths.get(path)
    this.heldPaths.set(path, entry)
    const key = dirKey(dirOf(path))
    if (prev === undefined) {
      this.heldCountByDir.set(key, (this.heldCountByDir.get(key) ?? 0) + 1)
      if (entry.mayBeActive) this.heldActiveCountByDir.set(key, (this.heldActiveCountByDir.get(key) ?? 0) + 1)
    } else if (prev.mayBeActive !== entry.mayBeActive) {
      const next = (this.heldActiveCountByDir.get(key) ?? 0) + (entry.mayBeActive ? 1 : -1)
      if (next <= 0) this.heldActiveCountByDir.delete(key)
      else this.heldActiveCountByDir.set(key, next)
    }
  }

  private deleteHeldPath(path: string): void {
    const prev = this.heldPaths.get(path)
    if (prev === undefined) return
    this.heldPaths.delete(path)
    const key = dirKey(dirOf(path))
    const next = (this.heldCountByDir.get(key) ?? 0) - 1
    if (next <= 0) this.heldCountByDir.delete(key)
    else this.heldCountByDir.set(key, next)
    if (prev.mayBeActive) {
      const nextActive = (this.heldActiveCountByDir.get(key) ?? 0) - 1
      if (nextActive <= 0) this.heldActiveCountByDir.delete(key)
      else this.heldActiveCountByDir.set(key, nextActive)
    }
  }

  private heldCountIn(dir: string): number {
    return this.heldCountByDir.get(dirKey(dir)) ?? 0
  }

  private hasActiveHeldIn(dir: string): boolean {
    return (this.heldActiveCountByDir.get(dirKey(dir)) ?? 0) > 0
  }

  private noteHeld(report: BackfillReport, trigger: BackfillReport['trigger'], message: string, heldPath: string, mayBeActive = false, activeCertain = false): void {
    const cleanMsg = diagnostic(message)
    const entry: HeldEntry = { reason: cleanMsg, mayBeActive, ...(activeCertain ? { activeCertain: true } : {}) }
    this.currentScanHeld.set(heldPath, entry)
    this.setHeldPath(heldPath, entry)
    report.heldFiles = this.currentScanHeld.size
    report.held ??= cleanMsg
    const dir = dirOf(heldPath)
    const heldCount = Math.max(report.heldFiles, this.heldCountIn(dir))
    const active = this.hasActiveHeldIn(dir)
    this.inProgressHeld = { files: heldCount, reason: this.inProgressHeld.reason ?? report.held, trigger, mayBeActive: active }
  }

  heldNoticeInfo(): { files: number; reason: string | null; trigger: BackfillReport['trigger'] | null; mayBeActive: boolean } {
    const dir = deadLetterPartition(this.config())
    const paths = new Map<string, HeldEntry>()
    let activeMark: ActiveSendingMark | null = null
    const seenMaps = new Set<Map<string, HeldEntry>>()
    for (const w of [...this.before, this]) {
      if (!seenMaps.has(w.heldPaths)) {
        seenMaps.add(w.heldPaths)
        for (const [p, entry] of w.heldPaths) {
          if (inDir(p, dir)) paths.set(p, entry)
        }
      }
      if (this.closing || w.backfilling === null) {
        for (const [p, mark] of w.activeSendingMarks) {
          if (!inDir(p, dir)) continue
          activeMark ??= mark
          if (!paths.has(p)) {
            paths.set(p, {
              reason: diagnostic(
                `a replay of ${mark.name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
              ),
              mayBeActive: false,
            })
          }
        }
      }
    }
    const trigger = this.inProgressHeld.trigger ?? (this.lastHeld.files > 0 ? this.lastHeld.trigger : null) ?? activeMark?.trigger ?? this.lastHeld.trigger ?? null
    const files = paths.size
    if (files <= 0) return { files: 0, reason: null, trigger, mayBeActive: false }
    let mayBeActive = false
    let certainActiveReason: string | null = null
    let activeReason: string | null = null
    let firstReason: string | null = null
    for (const entry of paths.values()) {
      firstReason ??= entry.reason
      if (entry.mayBeActive) {
        mayBeActive = true
        activeReason ??= entry.reason
        if (entry.activeCertain) certainActiveReason ??= entry.reason
      }
    }
    return { files, reason: certainActiveReason ?? activeReason ?? firstReason, trigger, mayBeActive }
  }

  private async backfillRun(kinds: DeadLetterKind[], trigger: BackfillReport['trigger'], dir: string, parked: number): Promise<BackfillReport> {
    const c = this.config()
    const startedAtExit = this.exitReplay !== null
    const report: BackfillReport = { trigger, files: 0, rowsSent: 0, rowsLeft: 0, rowsRefused: 0, otherTableRows: 0, skippedFiles: 0, oversizedFiles: [], incompleteFiles: [], held: null, heldFiles: 0, error: null, finishedAt: null }
    this.currentScanHeld.clear()
    this.inProgressHeld = { files: 0, reason: null, trigger, mayBeActive: false }
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
      else if (this.backfillReport !== null && this.backfillReport.held !== null && !this.replayDue.has(dir)) {
        this.backfillReport = { ...this.backfillReport, held: null, heldFiles: 0 }
      }
      const scannedHeld = this.currentScanHeld.size
      const knownHeld = Math.max(scannedHeld, this.heldCountIn(dir))
      const mayBeActive = this.hasActiveHeldIn(dir)
      let reason: string | null = null
      if (knownHeld > 0) {
        if (mayBeActive) {
          for (const entry of this.currentScanHeld.values()) {
            if (entry.activeCertain) {
              reason = entry.reason
              break
            }
          }
          if (reason === null) {
            for (const entry of this.currentScanHeld.values()) {
              if (entry.mayBeActive) {
                reason = entry.reason
                break
              }
            }
          }
          if (reason === null) {
            for (const [p, entry] of this.heldPaths) {
              if (entry.activeCertain && inDir(p, dir)) {
                reason = entry.reason
                break
              }
            }
          }
          if (reason === null) {
            for (const [p, entry] of this.heldPaths) {
              if (entry.mayBeActive && inDir(p, dir)) {
                reason = entry.reason
                break
              }
            }
          }
        }
        if (reason === null) {
          if (scannedHeld > 0) reason = report.held
          else {
            for (const [p, entry] of this.heldPaths) {
              if (inDir(p, dir)) {
                reason = entry.reason
                break
              }
            }
          }
        }
      }
      this.lastHeld = { files: knownHeld, reason, trigger, mayBeActive }
      this.inProgressHeld = { files: 0, reason: null, trigger: null, mayBeActive: false }
      this.currentScanHeld.clear()
      return report
    }
    if (!canDeliver(c) || c.dryRun) {
      report.error = c.dryRun ? 'dry run: nothing is sent to BigQuery' : 'not configured: set project and dataset'
      return finish()
    }
    // Only this table's subdirectory (`dir`): rows parked for another table
    // wait for a session configured with that table.
    let entries: readonly { name: string; kind: string; size: number; mtimeMs?: number }[] = []
    const initialListedAt = this.io.now()
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
    // Snapshot each listing entry once so companion indexing, cleanup, and
    // early-stop held seeding stay O(N) over the directory.
    const items: { name: string; kind: string; size: number; mtimeMs?: number }[] = entries.map(x => ({
      name: x.name,
      kind: x.kind,
      size: x.size,
      mtimeMs: x.mtimeMs,
    }))
    const dest = destinationDir(tableRef(c))
    const files: string[] = []
    const sizes = new Map<string, number>()
    const stamps = new Map<string, number | undefined>()
    // The records beside each file, from the listing alone: a record file
    // (`outcome-`, `outcome-b-`, `outcome2-`, `outcome2-b-`), and done marks,
    // which say the file was done only once read. `companions` indexes every
    // relevant companion per source in O(N), including zero-byte journal files
    // beside a live source (which `readJournalSlot` treats as cut off).
    const besides = new Map<string, { record: boolean; done: string[]; zeroDone: boolean }>()
    const companions = new Map<string, { name: string; kind: string; size: number; mtimeMs?: number }[]>()
    const listedPaths = new Set<string>()
    const listedEmpty = new Set<string>()
    const listed = new Map<string, number>()
    const there = new Set<string>()
    for (const x of items) {
      const fullPath = joinPath(dir, x.name)
      listedPaths.add(fullPath)
      if (x.kind !== 'file') continue
      listed.set(x.name, x.size)
      if (x.size === 0) {
        listedEmpty.add(fullPath)
      } else {
        there.add(fullPath)
        this.cleanedRetiredV2.delete(`replayed:v2:${dest}/${x.name}`)
      }
      const of = recordOf(x.name)
      if (of === null) continue
      if (x.size > 0 || markerOf(x.name, of) === null) {
        const list = companions.get(of)
        if (list) list.push(x)
        else companions.set(of, [x])
      }
      if (x.size === 0) {
        if (doneMarkerOf(x.name, of) !== null) {
          const beside = besides.get(of) ?? { record: false, done: [], zeroDone: false }
          beside.zeroDone = true
          besides.set(of, beside)
        }
        continue
      }
      if (x.name.startsWith('outcome-sending-')) continue
      const beside = besides.get(of) ?? { record: false, done: [], zeroDone: false }
      besides.set(of, beside)
      if (x.name.startsWith('outcome-done-')) beside.done.push(x.name)
      else beside.record = true
    }
    // Whether the empty file `name` is one a replay emptied: this writer
    // emptied it, a record file or a retired done mark is beside it, or the
    // store still holds its record (the done mark of a replay that emptied
    // it could not be written, and its sending mark was let go).
    const isRetired = async (name: string): Promise<'retired' | 'unretired' | 'aborted'> => {
      const path = joinPath(dir, name)
      if (this.retiredEmpty.has(path)) return 'retired'
      const beside = besides.get(name)
      let retired = beside?.record === true
      const OP_ABORTED = Symbol('retirement lookup aborted')
      const abortedNow = (): boolean => (startedAtExit ? this.exitReplay !== null && this.exitReplay.postCutoff.passed() : this.closing)
      const boundedLookup = async <T>(start: () => Promise<T>): Promise<T | undefined | typeof OP_ABORTED> => {
        if (abortedNow()) return OP_ABORTED
        const op = Promise.resolve()
          .then(start)
          .catch(() => undefined)
        if (startedAtExit && this.exitReplay !== null) {
          return Promise.race([op, this.exitReplay.postCutoff.at.then((): typeof OP_ABORTED => OP_ABORTED)])
        }
        if (this.closing) return OP_ABORTED
        let wakeAbort = (): void => undefined
        const abortPromise = new Promise<typeof OP_ABORTED>(resolve => {
          wakeAbort = () => resolve(OP_ABORTED)
        })
        this.pendingCleanupWakeups.add(wakeAbort)
        try {
          if (this.closing) return OP_ABORTED
          return await Promise.race([op, abortPromise])
        } finally {
          this.pendingCleanupWakeups.delete(wakeAbort)
        }
      }
      for (const done of beside?.done ?? []) {
        if (retired) break
        const rawDone = await boundedLookup(() => this.io.readFile(joinPath(dir, done)))
        if (rawDone === OP_ABORTED) return 'aborted'
        const j = journalOf(rawDone ?? '') as (Partial<Journal> & { childGuard?: unknown; stagedFor?: unknown }) | 'torn' | undefined
        if (j !== undefined && j !== 'torn') {
          if (j.retired === true) {
            retired = true
          } else if (j.childGuard === true && typeof j.stagedFor === 'string' && j.stagedFor !== '' && typeof j.size === 'number' && j.size > 0) {
            const parentBeside = besides.get(baseOf(j.stagedFor))
            for (const parentDone of parentBeside?.done ?? []) {
              const rawParentDone = await boundedLookup(() => this.io.readFile(joinPath(dir, parentDone)))
              if (rawParentDone === OP_ABORTED) return 'aborted'
              const pj = journalOf(rawParentDone ?? '')
              if (
                pj !== undefined &&
                pj !== 'torn' &&
                pj.retired === true &&
                normalizeReconciled(pj.reconciled).some(r => r.name === name && r.size === j.size)
              ) {
                retired = true
                break
              }
            }
          }
        }
      }
      if (!retired) {
        const emptySource = sourceFor(dir, name, 0, c)
        const s1 = await boundedLookup(() => this.io.storeGet(emptySource.recordKey))
        if (s1 === OP_ABORTED) return 'aborted'
        if (!isClearedSlot(s1)) retired = true
        else {
          const s2 = await boundedLookup(() => this.io.storeGet(emptySource.v2RecordKey))
          if (s2 === OP_ABORTED) return 'aborted'
          retired = !isClearedSlot(s2)
        }
      }
      if (retired) this.retiredEmpty.add(path)
      return retired ? 'retired' : 'unretired'
    }
    // First sights of files no longer there go, and so do empty files known
    // to be done that are no longer there empty.
    for (const seen of this.seenSince.keys()) if (inDir(seen, dir) && !listedPaths.has(seen)) this.seenSince.delete(seen)
    for (const path of this.retiredEmpty) if (inDir(path, dir) && !listedEmpty.has(path)) this.retiredEmpty.delete(path)
    // Empty files a write still under way may fill, and files of what is
    // left whose parent's record does not have them yet: why each waits,
    // said last (below), after any reason of another file's. Recent empty
    // files are inspected AFTER live files so a slow store lookup on a
    // retired empty file never stalls live spills at exit.
    const emptyWaiting: string[] = []
    const recentEmpties: { name: string; mtimeMs: number | undefined }[] = []
    const leased: string[] = []
    const parentWaits: string[] = []
    for (const e of items) {
      const kind = kindOf(e.name)
      if (e.kind !== 'file' || kind === null || !kinds.includes(kind)) continue
      const path = joinPath(dir, e.name)
      if (e.size === 0) {
        if (kind !== 'rejected' && !this.replayHalted() && this.io.now() - this.writtenSince(path, e.mtimeMs) < WRITE_PENDING_MS) {
          recentEmpties.push({ name: e.name, mtimeMs: e.mtimeMs })
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
    // Outcomes this writer holds for files since emptied (by another writer,
    // say) are let go.
    for (const path of this.rewrites.keys()) if (inDir(path, dir) && !there.has(path)) this.rewrites.delete(path)
    for (const path of this.uncertainSources.keys()) if (inDir(path, dir) && !there.has(path)) this.uncertainSources.delete(path)
    for (const path of [...this.heldPaths.keys()]) if (inDir(path, dir) && !there.has(path)) this.deleteHeldPath(path)
    for (const w of [...this.before, this]) {
      for (const path of [...w.activeSendingMarks.keys()]) if (inDir(path, dir) && !there.has(path)) w.activeSendingMarks.delete(path)
    }
    const emptiedInRun = new Set<string>()
    const inspectedSkipped = new Map<string, 'covered' | 'handled'>()
    const fileParent = new Map<string, string>()
    const parentIndex: RunParentIndex = new Map()
    const pendingLiveFiles = new Set(files)
    const deferredChildren = new Set<string>()
    const deferredForReplaces = new Set<string>()
    const deferredForV1Child = new Set<string>()
    const heldParents = new Set<string>()
    const isSourceLive = (n: string): boolean => (listed.get(n) ?? 0) > 0 && !emptiedInRun.has(n)
    const stagedGuardState = async (
      parentName: string,
      parentSize: number | undefined,
      targetName: string,
      targetPath: string,
      isChildGuard: boolean,
      replaces?: string,
      guardAttempts?: readonly string[],
      backup?: { forSize?: number; forFnv1a?: string; fnv1a?: string },
    ): Promise<
      | { action: 'go'; skipIndices?: readonly number[]; skipFnv1a?: string; accountedKeys?: ReadonlySet<string> }
      | { action: 'wait'; parentRetired?: boolean }
      | { action: 'hold' }
      | { action: 'drop'; expectedFnv1a?: string }
    > => {
      const checkTargetDigest = async (ref: ReconciledFile): Promise<'empty' | 'match' | 'mismatch' | 'error'> => {
        try {
          const curRaw = await this.io.readFile(targetPath)
          if (curRaw === '') return 'empty'
          if (ref.fnv1a !== '' && utf8Length(curRaw) === ref.size && fnv1a(curRaw) === ref.fnv1a) return 'match'
          return 'mismatch'
        } catch {
          return 'error'
        }
      }
      const checkBackupSelf = async (): Promise<'empty' | 'match' | 'mismatch' | 'error'> => {
        if (backup?.fnv1a === undefined || backup.fnv1a === '') return 'match'
        try {
          const curRaw = await this.io.readFile(targetPath)
          if (curRaw === '') return 'empty'
          return fnv1a(curRaw) === backup.fnv1a ? 'match' : 'mismatch'
        } catch {
          return 'error'
        }
      }
      let liveParentRetired = false
      let parentHeldForUnwrittenV1Child = false
      if (isSourceLive(parentName)) {
        const pSize = listed.get(parentName) ?? 0
        if (pSize <= 0) return { action: 'hold' }
        let pj: Found | Pending | 'torn' | 'conflict' | undefined
        let pLiveDigest: string | undefined
        try {
          const liveRes = await this.readLiveJournal(joinPath(dir, parentName), sourceFor(dir, parentName, pSize, c))
          pj = liveRes.found
          pLiveDigest = liveRes.liveDigest
        } catch {
          return { action: 'hold' }
        }
        const canCheckUnwrittenV1Hold =
          isChildGuard &&
          (pj === undefined ||
            (trigger === 'force' && (pj === 'torn' || pj === 'conflict' || 'pending' in pj)))
        if (pj === undefined || pj === 'torn' || pj === 'conflict' || 'pending' in pj) {
          if (canCheckUnwrittenV1Hold) {
            try {
              const pLease = (await this.io.storeGet(`lease:${destinationDir(tableRef(c))}/${parentName}`)) as
                | { by?: string; holder?: string; until?: number }
                | undefined
              const curMe = this.sessionId() ?? 'unknown'
              const foreignParentLease = Boolean(
                pLease &&
                  typeof pLease.until === 'number' &&
                  pLease.until > this.io.now() &&
                  (pLease.by !== curMe || typeof pLease.holder !== 'string' || pLease.holder === '' || pLease.holder !== this.mark),
              )
              if (!foreignParentLease) {
                let effectiveDigest = pLiveDigest
                if ((effectiveDigest === undefined || effectiveDigest === '') && pSize <= HOST_FILE_BYTES) {
                  const pRaw = await this.io.readFile(joinPath(dir, parentName))
                  if (pRaw !== '' && utf8Length(pRaw) === pSize) effectiveDigest = fnv1a(pRaw)
                }
                if (effectiveDigest !== undefined && effectiveDigest !== '') {
                  const pSource = sourceFor(dir, parentName, pSize, c)
                  const pEntries = await this.io.listDir(dir)
                  const pCompanions = groupCompanions(pEntries)
                  const [pV1Slot, pV1Done] = await Promise.all([
                    this.readJournalSlotAll(pSource.recordKey, pSource.recordFiles),
                    this.doneMarksAll({ ...pSource, fnv1a: undefined, strictFnv1a: undefined }, pCompanions.get(parentName) ?? []),
                  ])
                  const effV1 = effectiveUnretiredEarlierV1(pSize, effectiveDigest, pV1Slot, pV1Done.records)
                  if (effV1 !== undefined && writtenChildrenOfV1(effV1, parentName).has(targetName)) {
                    parentHeldForUnwrittenV1Child = true
                  }
                }
              }
            } catch {
              return { action: 'hold' }
            }
          }
          if (!parentHeldForUnwrittenV1Child) return { action: 'hold' }
        } else if (pj.journal.retired === true) {
          if (backup !== undefined) {
            if (
              replaces !== undefined &&
              normalizeReconciled(pj.journal.reconciled).some(
                entry =>
                  entry.name === replaces &&
                  (backup.forSize === undefined || entry.size === backup.forSize) &&
                  (backup.forFnv1a === undefined || entry.fnv1a === '' || entry.fnv1a === backup.forFnv1a),
              )
            ) {
              const bd = await checkBackupSelf()
              if (bd === 'error') return { action: 'hold' }
              return bd === 'mismatch' ? { action: 'go' } : { action: 'drop', ...(backup.fnv1a ? { expectedFnv1a: backup.fnv1a } : {}) }
            }
            liveParentRetired = true
          } else {
            const rec = normalizeReconciled(pj.journal.reconciled).find(entry => entry.name === targetName)
            if (rec !== undefined) {
              const d = await checkTargetDigest(rec)
              if (d === 'error') return { action: 'hold' }
              if (d === 'empty' || d === 'match') return { action: 'drop', ...(d === 'match' ? { expectedFnv1a: rec.fnv1a } : {}) }
              if (!isChildGuard || coversAllAttempts(pj.journal, guardAttempts)) return { action: 'go' }
            }
            if (!isChildGuard) {
              const jFiles = Array.isArray(pj.journal.files) ? pj.journal.files : []
              return jFiles.some(f => typeof f === 'string' && baseOf(f) === targetName) ? { action: 'go' } : { action: 'hold' }
            }
            // When `isChildGuard` is true and the live parent's retired record has
            // no `reconciled` digest (e.g. 0.1.3 retired `P` from stale `v1` and
            // failed to truncate `P`), fall through to consult same-size `v2`
            // (`reconciled`, `pendingReconcile` `skipIndices`/`accountedKeys`,
            // and `overlapRefs`) instead of blindly returning `'go'`.
            liveParentRetired = true
          }
        } else {
          // Parent is still live and unretired in this pass (even after deferral):
          // hold guarded files just like normal `parent: P` children so
          // `/bqaa-backfill force` cannot replay them while P or C is still live.
          return { action: 'hold' }
        }
      }
      try {
        const anySource = { ...sourceFor(dir, parentName, -1, c), live: false }
        const entries = await this.io.listDir(dir)
        const liveNow = new Map<string, number>()
        for (const e of entries) {
          if ((e.kind === undefined || e.kind === 'file') && e.size > 0) liveNow.set(e.name, e.size)
        }
        let entryCompanions = groupCompanions(entries)
        const doneAll = await this.doneMarksAll(anySource, entryCompanions.get(parentName) ?? [])
        const [v1All, v2All] = await Promise.all([
          this.readJournalSlotAll(anySource.recordKey, anySource.recordFiles),
          this.readJournalSlotAll(anySource.v2RecordKey, anySource.v2RecordFiles),
        ])
        const allRetired = [
          ...doneAll.records,
          ...[v1All.stored, ...v1All.filed, v2All.stored, ...v2All.filed].filter((r): r is Partial<Journal> => r !== undefined && r.retired === true),
        ]
        const anyTorn = doneAll.torn || v1All.tornSizes.length > 0 || v2All.tornSizes.length > 0
        const matchesParentSize = (r: Partial<Journal>): boolean => parentSize === undefined || r.size === undefined || r.size === parentSize
        // Select the highest-seq v2 checkpoint per parent size (preferring filed
        // on equal seq) before evaluating childGuard / stagedFiles / overlap.
        const latestV2BySize = new Map<number, { journal: Partial<Journal>; filed: boolean }>()
        const considerV2 = (r: Partial<Journal> | undefined, filed: boolean): void => {
          if (r === undefined) return
          const sz = typeof r.size === 'number' ? r.size : -1
          const cur = latestV2BySize.get(sz)
          if (cur === undefined || seqOf(r) > seqOf(cur.journal) || (seqOf(r) === seqOf(cur.journal) && filed && !cur.filed)) {
            latestV2BySize.set(sz, { journal: r, filed })
          }
        }
        considerV2(v2All.stored, false)
        for (const f of v2All.filed) considerV2(f, true)
        const latestV2 = [...latestV2BySize.values()].map(v => v.journal)
        // Verify that every deliverable/adopted replacement named by an unretired
        // v2 checkpoint is still safely accounted for (either live and not at
        // risk of being dropped by a conflicting 0.1.3 parent retirement, or
        // already retired with its own non-empty size > 0 completion mark)
        // before letting an unretired checkpoint's `reconciled` entry drop a
        // backup or guarded child.
        const sameSizeParentRetired = allRetired.filter(ret => matchesParentSize(ret))
        const unretiredV2ReplacementsAccounted = async (
          r: Partial<Journal>,
          reconciledChild?: string,
        ): Promise<'accounted' | 'live-unprotected' | 'missing'> => {
          const adoptedSet = new Set<string>()
          if (Array.isArray(r.adopted)) {
            for (const a of r.adopted) {
              if (typeof a === 'string' && a !== '') adoptedSet.add(baseOf(a))
            }
          }
          const reconciledEntry =
            reconciledChild !== undefined
              ? normalizeReconciled(r.reconciled).find(rec => rec.name === reconciledChild)
              : undefined
          const checkParentLeftovers = reconciledEntry?.needsLeftovers !== false
          const deliverableSet = new Set<string>()
          if (checkParentLeftovers && Array.isArray(r.files)) {
            for (const f of r.files) {
              if (typeof f === 'string' && f !== '') {
                const bn = baseOf(f)
                if (!adoptedSet.has(bn)) deliverableSet.add(bn)
              }
            }
          }
          const expectedByName = new Map(normalizeStagedRefs(r.deliverableRefs).map(ref => [ref.name, ref]))
          for (const bn of adoptedSet) {
            const expectedRef = expectedByName.get(bn)
            if (
              reconciledChild !== undefined &&
              bn !== reconciledChild &&
              expectedRef?.replaces !== undefined &&
              expectedRef.replaces !== '' &&
              expectedRef.replaces !== reconciledChild
            ) {
              continue
            }
            deliverableSet.add(bn)
          }
          let sawLiveUnprotected = false
          for (const dName of deliverableSet) {
            const expected = expectedByName.get(dName)
            const hasExpected = expected !== undefined && expected.size > 0
            let liveSize = liveNow.get(dName) ?? 0
            let readObservedChange = false
            if (liveSize > 0) {
              let liveText: string | undefined
              let liveMatchesExpected = !hasExpected
              if (hasExpected) {
                if (liveSize === expected.size) {
                  try {
                    liveText = await this.io.readFile(joinPath(dir, dName))
                  } catch (err) {
                    if (isNotFoundError(err)) {
                      liveSize = 0
                      readObservedChange = true
                    } else {
                      sawLiveUnprotected = true
                      continue
                    }
                  }
                  if (liveSize > 0) {
                    if (liveText === undefined || liveText === '') {
                      liveSize = 0
                      readObservedChange = true
                    } else if (utf8Length(liveText) === expected.size && (expected.fnv1a === '' || fnv1a(liveText) === expected.fnv1a)) {
                      liveMatchesExpected = true
                    } else {
                      readObservedChange = true
                    }
                  }
                }
              }
              if (liveSize > 0 && liveMatchesExpected) {
                const protectedByParentRetired = sameSizeParentRetired.some(
                  ret => Array.isArray(ret.files) && ret.files.some(f => typeof f === 'string' && baseOf(f) === dName),
                )
                if (!protectedByParentRetired) {
                  if (liveText !== undefined) {
                    const parsedLive = parseFile(liveText)
                    const liveParent = parsedLive.parent === null ? null : baseOf(parsedLive.parent)
                    if (!parsedLive.complete || liveParent === parentName) sawLiveUnprotected = true
                  } else {
                    const dInfo = liveSize <= HOST_FILE_BYTES ? await this.readParentInfo(dir, dName, liveSize, parentIndex) : null
                    if (dInfo === null || dInfo.state !== 'ok' || !dInfo.complete || dInfo.parent === parentName) {
                      sawLiveUnprotected = true
                    }
                  }
                }
                continue
              }
            }
            // Refresh companion listing if `readFile(dName)` observed a
            // concurrent truncation or content change since `entries` was
            // captured, so a retirement mark published right before truncation
            // or repopulation is never missed.
            if (readObservedChange) {
              entryCompanions = groupCompanions(await this.io.listDir(dir))
            }
            const dDone = await this.doneMarksAll(
              { ...sourceFor(dir, dName, hasExpected ? expected.size : -1, c), live: false },
              entryCompanions.get(dName) ?? [],
            )
            if (dDone.torn) {
              sawLiveUnprotected = true
              continue
            }
            const nonSentinels = dDone.records.filter(rec => !isEmptiedSentinel(rec) && typeof rec.size === 'number' && rec.size > 0)
            // When an expected digest is recorded, require a non-sentinel retired mark
            // whose explicit `sourceFnv1a` matches so a digest-less 0.1.3 mark from a
            // same-size basename reuse cannot drop the previous generation's last backup.
            const delivered =
              hasExpected && expected.fnv1a !== ''
                ? nonSentinels.some(rec => typeof rec.sourceFnv1a === 'string' && rec.sourceFnv1a === expected.fnv1a)
                : nonSentinels.length > 0
            if (!delivered) return 'missing'
          }
          return sawLiveUnprotected ? 'live-unprotected' : 'accounted'
        }
        // Standalone pre-dispatch backup (`backup: true`, `replaces: C`): drop
        // once `P` reconciled `C` at `backup.forSize` (or `C` completed its own
        // delivery), hold while `C` is still live so `C` runs first, and deliver
        // (with `accountedKeys` from matching-attempt v2 when available) only if
        // `C` was emptied unsent (e.g. dropped by `0.1.3` `parentFate`).
        if (backup !== undefined) {
          if (replaces !== undefined) {
            let reconciledCoversBackup = allRetired.some(
              r =>
                matchesParentSize(r) &&
                normalizeReconciled(r.reconciled).some(
                  rec =>
                    rec.name === replaces &&
                    (backup.forSize === undefined || rec.size === backup.forSize) &&
                    (backup.forFnv1a === undefined || rec.fnv1a === '' || rec.fnv1a === backup.forFnv1a),
                ),
            )
            let holdForVulnerableReplacement = false
            if (!reconciledCoversBackup) {
              for (const r of latestV2) {
                if (
                  !matchesParentSize(r) ||
                  !coversAllAttempts(r, guardAttempts) ||
                  !writtenOf(r).every(Boolean) ||
                  !normalizeReconciled(r.reconciled).some(
                    rec =>
                      rec.name === replaces &&
                      (backup.forSize === undefined || rec.size === backup.forSize) &&
                      (backup.forFnv1a === undefined || rec.fnv1a === '' || rec.fnv1a === backup.forFnv1a),
                  )
                ) {
                  continue
                }
                const repStatus = await unretiredV2ReplacementsAccounted(r, replaces)
                if (repStatus === 'accounted') {
                  reconciledCoversBackup = true
                  break
                }
                if (repStatus === 'live-unprotected') holdForVulnerableReplacement = true
              }
            }
            if (reconciledCoversBackup) {
              const bd = await checkBackupSelf()
              if (bd === 'error') return { action: 'hold' }
              return bd === 'mismatch' ? { action: 'go' } : { action: 'drop', ...(backup.fnv1a ? { expectedFnv1a: backup.fnv1a } : {}) }
            }
            if (holdForVulnerableReplacement) return { action: 'hold' }
            if (liveNow.has(replaces)) return { action: 'hold' }
            const childDone = await this.hasMatchingDoneMark(
              { ...sourceFor(dir, replaces, backup.forSize ?? -1, c), live: false },
              entryCompanions.get(replaces) ?? [],
              backup.forFnv1a,
            )
            if (childDone.torn) return { action: 'hold' }
            if (childDone.matched) {
              const bd = await checkBackupSelf()
              if (bd === 'error') return { action: 'hold' }
              return bd === 'mismatch' ? { action: 'go' } : { action: 'drop', ...(backup.fnv1a ? { expectedFnv1a: backup.fnv1a } : {}) }
            }
            for (const r of latestV2) {
              if (!matchesParentSize(r) || !coversAllAttempts(r, guardAttempts)) continue
              const allV2LeftoversWritten = writtenOf(r).every(Boolean)
              const pend = normalizeReconciled(r.pendingReconcile, true).find(entry => entry.name === replaces)
              if (pend !== undefined) {
                if (allV2LeftoversWritten && Array.isArray(r.accountedKeys) && r.accountedKeys.length > 0) {
                  return { action: 'go', accountedKeys: new Set(r.accountedKeys.filter((k): k is string => typeof k === 'string')) }
                }
                return { action: 'go' }
              }
              const jFiles = Array.isArray(r.files) ? r.files : []
              const jAdopted = Array.isArray(r.adopted) ? r.adopted : []
              if (
                jFiles.some(f => typeof f === 'string' && baseOf(f) === replaces) ||
                jAdopted.some(f => typeof f === 'string' && baseOf(f) === replaces)
              ) {
                return { action: 'go' }
              }
            }
            if (
              allRetired.some(
                r =>
                  matchesParentSize(r) &&
                  coversAllAttempts(r, guardAttempts) &&
                  Array.isArray(r.files) &&
                  r.files.some(f => typeof f === 'string' && baseOf(f) === replaces),
              )
            ) {
              return { action: 'go' }
            }
            // If `P` already retired & emptied without naming or reconciling
            // `replaces`, `replaces` is empty with zero non-sentinel retired
            // marks of any size and clean v1/v2 store/record files, and this
            // backup is linked to `replaces` (`parent: replaces`, staged for a
            // clean legacy ID-less child), `0.1.3 force` dropped `replaces`
            // unsent and deferred this backup in Pass 1: deliver it
            // automatically so rows are never stranded in `'wait'`.
            if (
              !anyTorn &&
              !isSourceLive(parentName) &&
              allRetired.some(
                r =>
                  matchesParentSize(r) &&
                  !(Array.isArray(r.files) && r.files.some(f => typeof f === 'string' && baseOf(f) === replaces)) &&
                  !normalizeReconciled(r.reconciled).some(rec => rec.name === replaces),
              )
            ) {
              const targetSize = liveNow.get(targetName) ?? listed.get(targetName) ?? 0
              const targetInfo = targetSize > 0 && targetSize <= HOST_FILE_BYTES ? await this.readParentInfo(dir, targetName, targetSize, parentIndex) : null
              if (targetInfo !== null && targetInfo.state === 'ok' && targetInfo.complete && targetInfo.parent === replaces) {
                const childAnySource = { ...sourceFor(dir, replaces, -1, c), live: false }
                const childDoneAny = await this.doneMarksAll(childAnySource, entryCompanions.get(replaces) ?? [])
                const nonSentinelChildDone = childDoneAny.records.filter(rec => !isEmptiedSentinel(rec))
                if (!childDoneAny.torn && nonSentinelChildDone.length === 0) {
                  const rawV1 = await this.io.storeGet(childAnySource.recordKey)
                  const rawV2 = await this.io.storeGet(childAnySource.v2RecordKey)
                  if ((rawV1 === undefined || rawV1 === null) && isClearedSlot(rawV2)) {
                    let hasChildRecordFile = false
                    for (const rf of [...childAnySource.recordFiles, ...childAnySource.v2RecordFiles]) {
                      if (this.io.exists !== undefined && !(await this.io.exists(rf))) continue
                      try {
                        if ((await this.io.readFile(rf)) !== '') {
                          hasChildRecordFile = true
                          break
                        }
                      } catch (err) {
                        if (!isNotFoundError(err)) {
                          hasChildRecordFile = true
                          break
                        }
                      }
                    }
                    if (!hasChildRecordFile) {
                      const matchingV2Keys = latestV2.find(
                        r =>
                          matchesParentSize(r) &&
                          coversAllAttempts(r, guardAttempts) &&
                          writtenOf(r).every(Boolean) &&
                          Array.isArray(r.accountedKeys) &&
                          r.accountedKeys.length > 0,
                      )
                      if (matchingV2Keys !== undefined) {
                        return {
                          action: 'go',
                          accountedKeys: new Set(matchingV2Keys.accountedKeys!.filter((k): k is string => typeof k === 'string')),
                        }
                      }
                      return { action: 'go' }
                    }
                  }
                }
              }
            }
          }
          return { action: 'wait', parentRetired: true }
        }
        // Step 1: Check explicit reconciled digests on retired records FIRST so
        // an unchanged reconciled child (listed in `files` for 0.1.3 safety) is
        // dropped while a repopulated child is delivered. Scan all retired
        // records for a match before returning `'go'` on an earlier mismatch.
        let sawCoveredReconciledMismatch = false
        for (const r of allRetired) {
          if (!matchesParentSize(r)) continue
          const rec = normalizeReconciled(r.reconciled).find(entry => entry.name === targetName)
          if (rec !== undefined) {
            const d = await checkTargetDigest(rec)
            if (d === 'error') return { action: 'hold' }
            if (d === 'empty' || d === 'match') return { action: 'drop', ...(d === 'match' ? { expectedFnv1a: rec.fnv1a } : {}) }
            if (!isChildGuard || coversAllAttempts(r, guardAttempts)) sawCoveredReconciledMismatch = true
          }
        }
        if (sawCoveredReconciledMismatch) return { action: 'go' }
        // Step 2: For a child guarded by `childGuard: true` (placed before forced
        // parent replay), consult same-size v2 matching ALL standing guard
        // attempts BEFORE checking `allRetired.files` so neither a pre-force v2
        // checkpoint nor a stale 0.1.3 retirement can bypass v2 reconciliation,
        // `skipIndices`, or `accountedKeys`, or duplicate rows on automatic replay.
        if (isChildGuard) {
          for (const r of latestV2) {
            if (!matchesParentSize(r) || !coversAllAttempts(r, guardAttempts)) continue
            const allV2LeftoversWritten = writtenOf(r).every(Boolean)
            const rec = normalizeReconciled(r.reconciled).find(entry => entry.name === targetName)
            if (rec !== undefined) {
              const d = await checkTargetDigest(rec)
              if (d === 'error') return { action: 'hold' }
              if (d === 'empty') return { action: 'drop' }
              if (d === 'match' && allV2LeftoversWritten) {
                const repStatus = await unretiredV2ReplacementsAccounted(r, targetName)
                if (repStatus === 'accounted') return { action: 'drop', expectedFnv1a: rec.fnv1a }
                if (repStatus === 'live-unprotected') return { action: 'hold' }
                if (sameSizeParentRetired.length > 0 && Array.isArray(r.accountedKeys) && r.accountedKeys.length > 0) {
                  return { action: 'go', accountedKeys: new Set(r.accountedKeys.filter((k): k is string => typeof k === 'string')) }
                }
              }
              return { action: 'go' }
            }
            const pend = normalizeReconciled(r.pendingReconcile, true).find(entry => entry.name === targetName)
            if (pend !== undefined) {
              if (pend.fnv1a !== '') {
                const d = await checkTargetDigest(pend)
                if (d === 'error') return { action: 'hold' }
                if (d === 'empty') return { action: 'drop' }
                if (d === 'mismatch') return { action: 'go' }
              }
              if (allV2LeftoversWritten) {
                if (pend.fnv1a !== '' && Array.isArray(pend.skipIndices) && pend.skipIndices.length > 0) {
                  return { action: 'go', skipIndices: pend.skipIndices, skipFnv1a: pend.fnv1a }
                }
                if (Array.isArray(r.accountedKeys) && r.accountedKeys.length > 0) {
                  return { action: 'go', accountedKeys: new Set(r.accountedKeys.filter((k): k is string => typeof k === 'string')) }
                }
              }
              return { action: 'go' }
            }
          }
        }
        // Step 3: Check retired records naming `targetName` FIRST (after
        // reconciled/childGuard checks above) so a stale lower-seq v2 store entry
        // from a quota fallback cannot block a promoted deliverable named in a
        // retired mark. For attempt-bound `childGuard` marks, require the retired
        // record to cover all standing guard attempts so a stale 0.1.3 retirement
        // cannot bypass an interrupted forced replay on `auto`.
        if (
          allRetired.some(
            r =>
              matchesParentSize(r) &&
              (!isChildGuard || coversAllAttempts(r, guardAttempts)) &&
              Array.isArray(r.files) &&
              r.files.some(f => typeof f === 'string' && baseOf(f) === targetName),
          )
        ) {
          return { action: 'go' }
        }
        const stagedRecord = latestV2.find(r => matchesParentSize(r) && normalizeStaged(r.stagedFiles).includes(targetName))
        if (stagedRecord !== undefined) {
          // If the emptied parent was already retired at this size by an outcome
          // covering EVERY pending child (either named in `files`, e.g. 0.1.3
          // preserved/replayed them, recorded in `reconciled`, e.g. 0.1.4
          // completed reconciliation without this aborted stage, or the pending
          // child itself already retired at its recorded byte size and was
          // emptied), drop this aborted stage cleanly.
          const pendingRefs = normalizeReconciled(stagedRecord.pendingReconcile, true)
          const sameSizeRetired = allRetired.filter(ret => stagedRecord.size === undefined || ret.size === stagedRecord.size)
          if (!anyTorn && sameSizeRetired.length > 0) {
            let allPendingCovered = true
            for (const ref of pendingRefs) {
              const coveredByParent = sameSizeRetired.some(
                ret =>
                  (Array.isArray(ret.files) && ret.files.some(f => typeof f === 'string' && baseOf(f) === ref.name)) ||
                  normalizeReconciled(ret.reconciled).some(rec => rec.name === ref.name),
              )
              if (coveredByParent) continue
              if (!liveNow.has(ref.name) && ref.size > 0) {
                const childDone = await this.hasMatchingDoneMark(
                  { ...sourceFor(dir, ref.name, ref.size, c), live: false },
                  entries,
                  ref.fnv1a !== '' ? ref.fnv1a : undefined,
                )
                if (!childDone.torn && childDone.matched) continue
              }
              allPendingCovered = false
              break
            }
            if (allPendingCovered) {
              const stagedEntry = normalizeStagedRefs(stagedRecord.stagedFiles).find(s => s.name === targetName)
              return { action: 'drop', ...(stagedEntry?.fnv1a ? { expectedFnv1a: stagedEntry.fnv1a } : {}) }
            }
          }
          if (pendingRefs.some(ref => liveNow.has(ref.name))) return { action: 'hold' }
          return { action: 'hold' }
        }
        for (const r of latestV2) {
          if (!matchesParentSize(r)) continue
          if (isChildGuard && !coversAllAttempts(r, guardAttempts)) continue
          const jFiles = Array.isArray(r.files) ? r.files : []
          const idx = jFiles.findIndex(f => typeof f === 'string' && baseOf(f) === targetName)
          if (idx === -1) continue
          const allOverlapRefs = [
            ...normalizeReconciled(r.reconciled),
            ...normalizeReconciled(r.pendingReconcile, true),
          ]
          // When `replaces` identifies the exact child that this synthesized
          // standalone replacement replaced, scope live-hold and dedup-drop to
          // that child alone; never hold or drop an unmodified adopted child
          // (`isChildGuard` or `replaces === undefined`) just because a sibling
          // reconciled child completed its own replay.
          const scopedOverlapRefs = replaces !== undefined ? allOverlapRefs.filter(ref => ref.name === replaces) : allOverlapRefs
          if (!isChildGuard && scopedOverlapRefs.some(ref => liveNow.has(ref.name))) return { action: 'hold' }
          const adoptedNames = Array.isArray(r.adopted)
            ? r.adopted.filter((x): x is string => typeof x === 'string' && x !== '').map(baseOf)
            : []
          // A standalone parent leftover (`!isChildGuard && !adoptedNames.includes(targetName)`)
          // that was not named in any same-size retired record (Step 3 already
          // returned `'go'` if any same-size retired record named it) is
          // superseded once a same-size retired outcome exists that already
          // emitted its own fresh leftover files (not overlapping `reconciled`,
          // `pendingReconcile`, or `adopted`) covering every non-'L' row fate in
          // `r.fates` (e.g. 0.1.3 finished an unwritten stale v1 journal and
          // retired P). Do NOT drop when `ret.files` names a reconciled/pending
          // child `C` that 0.1.4 drops or skips in reliance on `L_p`, or when a
          // stale `files: []` / `fates: 'LL'` retirement contradicts `r.fates`.
          const allV2LeftoversWritten = writtenOf(r).every(Boolean)
          const nonFreshRetNames = new Set([...(allV2LeftoversWritten ? allOverlapRefs.map(ref => ref.name) : []), ...adoptedNames])
          const retiredCoversLeftover = (ret: Partial<Journal>): boolean => {
            if (r.size !== undefined && ret.size !== r.size) return false
            if (!Array.isArray(ret.files) || ret.files.length === 0) return false
            if (ret.files.some(f => typeof f !== 'string' || f === '' || nonFreshRetNames.has(baseOf(f)))) return false
            if (typeof r.fates !== 'string' || typeof ret.fates !== 'string' || r.fates === '' || ret.fates.length !== r.fates.length) {
              return false
            }
            let hasNonLanded = false
            for (let i = 0; i < r.fates.length; i += 1) {
              if (r.fates[i] !== 'L') {
                hasNonLanded = true
                if (ret.fates[i] !== r.fates[i]) return false
              }
            }
            return hasNonLanded
          }
          if (
            !isChildGuard &&
            !adoptedNames.includes(targetName) &&
            !anyTorn &&
            allRetired.some(retiredCoversLeftover)
          ) {
            return { action: 'drop' }
          }
          if (!isChildGuard && replaces !== undefined && adoptedNames.includes(targetName) && scopedOverlapRefs.length > 0 && !anyTorn) {
            const sameSizeRetired = allRetired.filter(ret => r.size === undefined || ret.size === r.size)
            if (
              sameSizeRetired.length > 0 &&
              scopedOverlapRefs.every(ref =>
                sameSizeRetired.some(ret => Array.isArray(ret.files) && ret.files.some(f => typeof f === 'string' && baseOf(f) === ref.name)),
              )
            ) {
              let allChildrenReplayed = true
              for (const ref of scopedOverlapRefs) {
                if (ref.size <= 0) {
                  allChildrenReplayed = false
                  break
                }
                const childDone = await this.hasMatchingDoneMark(
                  { ...sourceFor(dir, ref.name, ref.size, c), live: false },
                  entries,
                  ref.fnv1a !== '' ? ref.fnv1a : undefined,
                )
                if (childDone.torn || !childDone.matched) {
                  allChildrenReplayed = false
                  break
                }
              }
              if (allChildrenReplayed) return { action: 'drop' }
            }
          }
          return { action: 'go' }
        }
        // If 0.1.4 already retired P at this size with reconciled digests and
        // cleaned up all unretired v2 checkpoints without naming standalone
        // `targetName`, `targetName` is a superseded aborted stage. Never drop
        // a `childGuard` original child here or if any slot is torn or any
        // matching-size unretired v2 checkpoint still exists.
        if (
          !isChildGuard &&
          !anyTorn &&
          !latestV2.some(r => matchesParentSize(r) && r.retired !== true) &&
          allRetired.some(ret => matchesParentSize(ret) && normalizeReconciled(ret.reconciled).length > 0)
        ) {
          return { action: 'drop' }
        }
        if (parentHeldForUnwrittenV1Child && (!anyTorn || trigger === 'force')) return { action: 'go' }
        return liveParentRetired || !isSourceLive(parentName) ? { action: 'wait', parentRetired: true } : { action: 'hold' }
      } catch {
        return { action: 'hold' }
      }
    }
    const me = this.sessionId() ?? 'unknown'
    for (let fileIdx = 0; fileIdx < files.length; fileIdx += 1) {
      const name = files[fileIdx] as string
      pendingLiveFiles.delete(name)
      if (emptiedInRun.has(name)) {
        const emptiedPath = joinPath(dir, name)
        this.deleteHeldPath(emptiedPath)
        this.currentScanHeld.delete(emptiedPath)
        continue
      }
      if (report.error || this.replayStops(trigger, tally)) {
        const companionEntries = companions.get(name)
        const yieldLiveOnShutdown = !startedAtExit && this.closing
        if (!yieldLiveOnShutdown && trigger !== 'force' && (companionEntries !== undefined || !this.replayHalted())) {
          try {
            await this.seedSkippedHeld(dir, name, sizes.get(name) ?? -1, c, companions, listed, trigger, inspectedSkipped, startedAtExit)
          } catch {
            // Best-effort held-file seeding on early stop.
          }
        }
        report.skippedFiles += 1
        // Not read: its rows wait for the next replay.
        if (kindOf(name) !== 'rejected') tally.waiting += 1
        continue
      }
      const leaseKey = `lease:${destinationDir(tableRef(c))}/${name}`
      try {
        const lease = (await this.io.storeGet(leaseKey)) as { by?: string; holder?: string; until?: number } | undefined
        if (
          lease &&
          typeof lease.until === 'number' &&
          lease.until > this.io.now() &&
          (lease.by !== me || (typeof lease.holder === 'string' && lease.holder !== '' && lease.holder !== this.mark))
        ) {
          // Another session replays it. Record its active hold immediately so an
          // early cutoff on a later file qualifies heldNoticeInfo(), while
          // deferring report.held assignment until after parentWaits below.
          const leasedPath = joinPath(dir, name)
          const leaseMsg = diagnostic(`another session's lease holds ${name} (ten minutes at most); it waits for the next replay`)
          const leaseEntry: HeldEntry = { reason: leaseMsg, mayBeActive: true, activeCertain: true }
          this.currentScanHeld.set(leasedPath, leaseEntry)
          this.setHeldPath(leasedPath, leaseEntry)
          report.heldFiles = this.currentScanHeld.size
          const heldCount = Math.max(report.heldFiles, this.heldCountIn(dir))
          this.inProgressHeld = { files: heldCount, reason: leaseMsg, trigger, mayBeActive: true }
          leased.push(name)
          report.skippedFiles += 1
          if (kindOf(name) !== 'rejected') tally.waiting += 1
          continue
        }
        await this.io.storeSet(leaseKey, { by: me, until: this.io.now() + LEASE_MS })
      } catch {
        // No store: go on without the lease; event_id still dedups in SQL.
      }
      const path = joinPath(dir, name)
      const priorHeld = this.heldPaths.get(path)
      try {
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
        if (this.uncertainSources.has(path) && this.uncertainSources.get(path) !== source.size) {
          this.uncertainSources.delete(path)
        }
        let found: Found | Pending | 'torn' | 'conflict' | undefined
        let hadInitialRecord = false
        try {
          const liveRead = await this.readLiveJournal(path, source, held)
          found = liveRead.found
          hadInitialRecord = liveRead.found !== undefined || liveRead.strict === true
          if (liveRead.strict && liveRead.liveDigest !== undefined) {
            source.fnv1a = liveRead.liveDigest
            source.strictFnv1a = true
            if (liveRead.staleHeld) {
              this.rewrites.delete(path)
              held = undefined
            }
          }
        } catch (err) {
          // It may have an outcome that cannot be read now: none of it goes
          // until its record can be, and its rows wait.
          report.held ??= diagnostic(`cannot read the replay record of ${name}: ${messageOf(err)}`)
          report.skippedFiles += 1
          if (kindOf(name) !== 'rejected') tally.waiting += 1
          continue
        }
        this.deleteHeldPath(path)
        if (found === 'torn' || found === 'conflict') {
          // Its record was cut off part-way (with none whole), or conflicting
          // v1/v2 journals disagree for this source size: nothing of what it
          // left is guessed by seq; the writer holding an outcome finishes it,
          // while any other replay holds the file until /bqaa-backfill force.
          if (held === undefined && trigger !== 'force') {
            const msg =
              found === 'conflict'
                ? `conflicting replay records exist for ${name}; /bqaa-backfill force replays the file afresh`
                : `the replay record of ${name} is cut off part-way; /bqaa-backfill force replays the file afresh`
            const foreign = await this.hasActiveForeignMark(source)
            this.noteHeld(report, trigger, msg, path, foreign.mayBeActive, foreign.activeCertain)
            report.skippedFiles += 1
            if (kindOf(name) !== 'rejected') tally.waiting += 1
            continue
          }
          found = undefined
        }
        let stagedForParent: string | undefined
        let allowStagedFor: string | undefined
        let guardApprovedParent: string | undefined
        let guardSkipIndices: ReadonlySet<number> | undefined
        let guardSkipFnv1a: string | undefined
        let guardAccountedKeys: ReadonlySet<string> | undefined
        if (found !== undefined && 'pending' in found) {
          // A replay sent rows of it, or was about to, and recorded no
          // outcome: its writer may yet (still at it, or unable to write its
          // record now), or it stopped. Its rows may have gone, and been
          // refused: none goes from any other replay, and /bqaa-backfill
          // force, for a writer that stopped, replays it afresh. A mark of
          // this writer's own is no other replay's: it would hold the outcome.
          stagedForParent = found.stagedFor
          const ownUncertain = this.uncertainSources.get(path) === source.size
          const stagedNonUncertain = found.stagedFor !== undefined && found.uncertain !== true
          const stagedGuardClearable = stagedNonUncertain && !ownUncertain && (found.nonStagedBy ?? []).every(by => by === this.mark)
          if (held === undefined && found.stagedFor !== undefined) {
            if ((stagedGuardClearable || trigger === 'force') && isSourceLive(found.stagedFor) && pendingLiveFiles.has(found.stagedFor) && !deferredChildren.has(name)) {
              deferredChildren.add(name)
              files.push(name)
              pendingLiveFiles.add(name)
              continue
            }
            if (
              (stagedGuardClearable || trigger === 'force') &&
              found.backup === true &&
              found.replaces !== undefined &&
              isSourceLive(found.replaces) &&
              pendingLiveFiles.has(found.replaces) &&
              !deferredForReplaces.has(name)
            ) {
              deferredForReplaces.add(name)
              files.push(name)
              pendingLiveFiles.add(name)
              continue
            }
            const guard = await stagedGuardState(
              found.stagedFor,
              found.stagedForSize,
              name,
              path,
              found.childGuard === true,
              found.replaces,
              found.attempts,
              found.backup === true
                ? { forSize: found.backupForSize, forFnv1a: found.backupForFnv1a, fnv1a: found.backupFnv1a }
                : undefined,
            )
            if (guard.action === 'drop') {
              let dropSafe = true
              if (guard.expectedFnv1a !== undefined && guard.expectedFnv1a !== '') {
                try {
                  const curDrop = await this.io.readFile(path)
                  if (curDrop !== '' && (utf8Length(curDrop) !== source.size || fnv1a(curDrop) !== guard.expectedFnv1a)) {
                    dropSafe = false
                  }
                } catch {
                  dropSafe = false
                }
              }
              if (!dropSafe) {
                tally.waiting += 1
                continue
              }
              if (await this.emptyCopy(path)) {
                this.retiredEmpty.add(path)
                emptiedInRun.add(name)
                parentIndex.delete(name)
                this.uncertainSources.delete(path)
                this.deleteHeldPath(path)
                this.currentScanHeld.delete(path)
                // Keep `childGuard` (`${this.mark}g`) on an emptied reconciled
                // child so a late exact copy is held by 0.1.3 and dropped by
                // 0.1.4; on dropped standalone backups write a size-0 retired
                // marker so subsequent sessions do not treat the emptied backup
                // as an in-progress write.
                const marksToClear =
                  found.childGuard === true || found.backup === true
                    ? [doneFileOf(dir, name, this.mark)]
                    : [doneFileOf(dir, name, this.mark), doneFileOf(dir, name, `${this.mark}g`)]
                for (const ownMark of marksToClear) {
                  try {
                    if (this.io.exists === undefined || (await this.io.exists(ownMark))) {
                      await this.io.writeFile(ownMark, '')
                    }
                  } catch {
                    // Best-effort clear of our own guard mark after truncation.
                  }
                }
                if (found.backup === true || found.childGuard === true) {
                  try {
                    await this.io.writeFile(
                      doneFileOf(dir, name, found.backup === true ? `${this.mark}g` : `${this.mark}d`),
                      journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
                    )
                  } catch {
                    // Best-effort size-0 retired marker for emptied backup/reconciled child.
                  }
                }
              } else {
                tally.waiting += 1
              }
              continue
            }
            if (guard.action === 'hold' && pendingLiveFiles.size > 0) {
              if (!deferredChildren.has(name)) {
                deferredChildren.add(name)
                files.push(name)
                pendingLiveFiles.add(name)
                continue
              }
              if (found.backup === true && found.replaces !== undefined && pendingLiveFiles.has(found.replaces) && !deferredForReplaces.has(name)) {
                deferredForReplaces.add(name)
                files.push(name)
                pendingLiveFiles.add(name)
                continue
              }
            }
            const parentEmptyOrRetired = !isSourceLive(found.stagedFor) || (guard.action === 'wait' && guard.parentRetired === true)
            const forceCanBreakWait = trigger === 'force' && guard.action === 'wait' && guard.parentRetired === true
            if (guard.action === 'hold' || (stagedGuardClearable && guard.action === 'wait' && !forceCanBreakWait)) {
              if ((found.childGuard === true || found.backup === true) && guard.action === 'wait' && parentEmptyOrRetired) {
                this.noteHeld(
                  report,
                  trigger,
                  `parent ${found.stagedFor} retired without accounting for guarded child ${name}; /bqaa-backfill force replays the file afresh`,
                  path,
                )
              } else {
                parentWaits.push(name)
              }
              report.skippedFiles += 1
              if (kindOf(name) !== 'rejected') tally.waiting += 1
              continue
            }
            if (guard.action === 'go') {
              if (found.childGuard === true) guardApprovedParent = found.stagedFor
              else if (found.backup === true && found.replaces !== undefined) guardApprovedParent = found.replaces
              if (guard.skipIndices !== undefined && guard.skipIndices.length > 0 && guard.skipFnv1a !== undefined && guard.skipFnv1a !== '') {
                guardSkipIndices = new Set(guard.skipIndices)
                guardSkipFnv1a = guard.skipFnv1a
              }
              if (guard.accountedKeys !== undefined && guard.accountedKeys.size > 0) guardAccountedKeys = guard.accountedKeys
            } else if (forceCanBreakWait) {
              if (found.childGuard === true) guardApprovedParent = found.stagedFor
              else if (found.backup === true && found.replaces !== undefined) guardApprovedParent = found.replaces
            }
            if (stagedGuardClearable) {
              allowStagedFor = found.stagedFor
              found = undefined
            }
          }
          if (found !== undefined && 'pending' in found) {
            const effectivePending = stagedNonUncertain ? (found.nonStagedBy ?? []) : found.pending
            const hasForeignActive = (stagedNonUncertain ? (found.nonStagedBy ?? []) : (found.activeBy ?? [])).some(by => by !== this.mark)
            const hasCertainActive = (stagedNonUncertain ? (found.nonStagedBy ?? []) : (found.nonUncertainBy ?? [])).some(by => by !== this.mark)
            if (held === undefined && trigger !== 'force' && (ownUncertain || effectivePending.some(by => by !== this.mark))) {
              const msg =
                !hasCertainActive && (ownUncertain || found.uncertain === true)
                  ? `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`
                  : `a replay of ${name} sent rows and has recorded no outcome; if that session stopped, /bqaa-backfill force replays the file afresh`
              this.noteHeld(report, trigger, msg, path, hasForeignActive, hasCertainActive)
              report.skippedFiles += 1
              if (kindOf(name) !== 'rejected') tally.waiting += 1
              continue
            }
            found = undefined
          }
        }
        const known = await this.resumeRewrite(path, source, kindOf(name) ?? 'retry', held, found, trigger)
        let emptied = true
        if (known === 'unaccounted' && trigger !== 'force') {
          const foreign = await this.hasActiveForeignMark(source)
          this.noteHeld(
            report,
            trigger,
            `the replay record of ${name} does not match its current children; /bqaa-backfill force replays the file afresh`,
            path,
            foreign.mayBeActive,
            foreign.activeCertain,
          )
          report.skippedFiles += 1
          if (kindOf(name) !== 'rejected') tally.waiting += 1
          continue
        }
        const supersededUnaccounted =
          known === 'unaccounted' && trigger === 'force' && found !== undefined && !('pending' in found) ? found.journal : undefined
        const resumed = known === 'unaccounted' ? undefined : known
        if (resumed !== undefined) {
          const unwrittenUncertain = resumed.files.filter(f => f.uncertain && f.text !== null)
          let done = false
          try {
            done = await this.writeOut(path, resumed, c, parentIndex, emptiedInRun)
          } finally {
            for (const f of unwrittenUncertain) {
              if (f.text === null) {
                this.noteHeld(
                  report,
                  trigger,
                  `a replay of ${baseOf(f.path)} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
                  f.path,
                )
              }
            }
            if (!done && this.rewrites.has(path) && resumed.files.some(f => f.uncertain && f.text !== null)) {
              this.noteHeld(
                report,
                trigger,
                `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
                path,
              )
            }
          }
          if (done) {
            emptiedInRun.add(name)
            parentIndex.delete(name)
          } else {
            const heldNow = this.currentScanHeld.get(path)
            if (heldNow !== undefined) {
              report.held ??= heldNow.reason
              report.heldFiles = this.currentScanHeld.size
            }
          }
          if (!done && resumed.deferredByLease === true) {
            heldParents.add(name)
            report.skippedFiles += 1
            if (kindOf(name) !== 'rejected') tally.waiting += 1
            emptied = true
          } else {
            emptied = done
            // What it wrote out waits for the next replay.
            if (resumed.waits) tally.waiting += 1
            report.files += 1
            report.rowsLeft += [...resumed.fates].filter(fate => fate !== 'L').length
            report.rowsRefused += [...resumed.fates].filter(fate => fate === 'R').length
          }
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
            this.uncertainSources.delete(path)
            for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
            emptiedInRun.add(name)
            // Emptied since it was listed: by another writer, which writes
            // its done mark first, or by a write that starts it over, still
            // under way. With no done mark beside it, it waits.
            if (kindOf(name) !== 'rejected' && !(await this.io.listDir(dir)).some(x => x.size > 0 && doneMarkerOf(x.name, name) !== null)) tally.waiting += 1
          } else {
            this.seenSince.delete(path)
            const liveDigest = fnv1a(text)
            if (guardSkipIndices !== undefined && (guardSkipFnv1a === undefined || liveDigest !== guardSkipFnv1a)) {
              guardSkipIndices = undefined
              guardSkipFnv1a = undefined
            }
            const parentName = parsed.parent === null ? null : baseOf(parsed.parent)
            const effectiveParent = parentName ?? stagedForParent ?? null
            fileParent.set(name, parentName ?? '')
            parentIndex.set(name, { size: source.size, parent: parentName })
            if (trigger === 'force' && effectiveParent !== null && isSourceLive(effectiveParent) && pendingLiveFiles.has(effectiveParent) && !deferredChildren.has(name)) {
              // On forced replay, process a live parent before its live child
              // (or standalone staged replacement) so overlapping rows
              // reconcile or staged writes clean up against the parent first.
              deferredChildren.add(name)
              files.push(name)
              pendingLiveFiles.add(name)
              continue
            }
            if (trigger === 'force' && effectiveParent !== null && heldParents.has(effectiveParent)) {
              // When the parent was held in this scan (e.g. a sibling child
              // held a foreign lease or mutated during backup staging), do not
              // let its other children race ahead and skip parent reconciliation.
              parentWaits.push(name)
              report.skippedFiles += 1
              if (kindOf(name) !== 'rejected') tally.waiting += 1
              continue
            }
            // If a same-size v1 record (unretired or retired companion file) from
            // an earlier equal-size generation (`sourceFnv1a !== liveDigest`) still
            // protects a live child on disk, defer once behind runnable written
            // children so `v1` can clear before dispatch; if `0.1.3` itself would
            // still see an unretired earlier-digest v1 checkpoint with an
            // unwritten part AND a live written child, hold cleanly until that
            // child clears; otherwise proceed with `useV2 = true` forced so
            // `v1ShadowNeeded` keeps new leftovers standalone and `markDone`
            // carries the live child without exposing `name` to a `0.1.3 auto`
            // scan or deadlocking.
            let forceV2ForProtectedV1 = false
            if (source.size > 0 && hadInitialRecord) {
              let v1WrittenLiveChildren: string[] = []
              let unwrittenEarlierV1Children: string[] = []
              let v1InspectFailed = false
              try {
                const [v1SlotNow, v1DoneNow] = await Promise.all([
                  this.readJournalSlotAll(source.recordKey, source.recordFiles),
                  this.doneMarksAll({ ...source, fnv1a: undefined, strictFnv1a: undefined }, companions.get(name) ?? []),
                ])
                const v1Records = [v1SlotNow.stored, ...v1SlotNow.filed].filter((r): r is Partial<Journal> => r !== undefined)
                if (v1SlotNow.tornSizes.some(sz => sz === null || sz === source.size)) {
                  forceV2ForProtectedV1 = true
                }
                const seenWrittenV1Children = new Set<string>()
                for (const r of v1Records) {
                  const isEarlierDigestV1 =
                    r.size === source.size &&
                    typeof r.sourceFnv1a === 'string' &&
                    r.sourceFnv1a !== '' &&
                    r.sourceFnv1a !== liveDigest
                  const isSupersededSameSizeV1 = supersededUnaccounted !== undefined && r.size === source.size
                  if (isEarlierDigestV1 || isSupersededSameSizeV1) {
                    const written = writtenOf(r)
                    const rFileBasenames = new Set<string>()
                    if (Array.isArray(r.files)) {
                      r.files.forEach((f, idx) => {
                        if (typeof f !== 'string' || f === '') return
                        const childName = baseOf(f)
                        if (childName === '') return
                        rFileBasenames.add(childName)
                        if (childName === name || !isSourceLive(childName)) return
                        forceV2ForProtectedV1 = true
                        if (isEarlierDigestV1 && written[idx] === true) {
                          seenWrittenV1Children.add(childName)
                        }
                      })
                    }
                    const pendingNames = new Set(normalizeReconciled(r.pendingReconcile, true).map(p => p.name))
                    for (const childName of journalChildNames(r)) {
                      if (childName === '' || childName === name || !isSourceLive(childName)) continue
                      forceV2ForProtectedV1 = true
                      if (isEarlierDigestV1 && !pendingNames.has(childName) && !rFileBasenames.has(childName)) {
                        seenWrittenV1Children.add(childName)
                      }
                    }
                  }
                }
                const effUnwrittenV1 = effectiveUnretiredEarlierV1(source.size, liveDigest, v1SlotNow, v1DoneNow.records)
                if (effUnwrittenV1 !== undefined) {
                  for (const cn of writtenChildrenOfV1(effUnwrittenV1, name)) {
                    if (isSourceLive(cn)) {
                      unwrittenEarlierV1Children.push(cn)
                      seenWrittenV1Children.add(cn)
                    }
                  }
                }
                v1WrittenLiveChildren = [...seenWrittenV1Children]
              } catch {
                v1InspectFailed = true
              }
              if (v1InspectFailed) {
                this.noteHeld(
                  report,
                  trigger,
                  `cannot inspect prior replay record of ${name}; it waits for the next replay`,
                  path,
                )
                report.skippedFiles += 1
                if (kindOf(name) !== 'rejected') tally.waiting += 1
                continue
              }
              if (v1WrittenLiveChildren.length > 0) {
                const hasPendingV1Child = v1WrittenLiveChildren.some(cn => pendingLiveFiles.has(cn))
                if (hasPendingV1Child && !deferredForV1Child.has(name)) {
                  deferredForV1Child.add(name)
                  files.push(name)
                  pendingLiveFiles.add(name)
                  continue
                }
                // When the effective `0.1.3`-visible earlier-digest same-size v1
                // record is unretired and has BOTH a live written child AND an
                // unwritten part (`written[i] === false`), hold without
                // dispatching so a post-dispatch v2 persistence failure cannot
                // leave `0.1.3` to rebuild the unwritten part from the new
                // parent's bytes and duplicate rows without insert IDs.
                if (unwrittenEarlierV1Children.length > 0) {
                  const firstChild =
                    (!kinds.includes('rejected') ? unwrittenEarlierV1Children.find(cn => kindOf(cn) === 'rejected') : undefined) ??
                    (unwrittenEarlierV1Children[0] as string)
                  const childHeld = this.currentScanHeld.get(joinPath(dir, firstChild)) ?? this.heldPaths.get(joinPath(dir, firstChild))
                  const mayBeActive = childHeld?.mayBeActive === true
                  const activeCertain = childHeld?.activeCertain === true
                  const needsExplicitRejected = kindOf(firstChild) === 'rejected' && !kinds.includes('rejected')
                  const v1HoldMsg = needsExplicitRejected
                    ? `prior replay record of ${name} still protects live rejected child ${firstChild}; run /bqaa-backfill to clear ${firstChild} first`
                    : `prior replay record of ${name} still protects live child ${firstChild}; waiting until ${firstChild} clears`
                  this.noteHeld(report, trigger, v1HoldMsg, path, mayBeActive, activeCertain)
                  report.skippedFiles += 1
                  if (kindOf(name) !== 'rejected') tally.waiting += 1
                  continue
                }
              }
            }
            if (parentName === null && stagedForParent !== undefined && isSourceLive(stagedForParent)) {
              const stagedParentSize = listed.get(stagedForParent) ?? 0
              if (stagedParentSize > 0) {
                try {
                  const stagedParentJournal = (
                    await this.readLiveJournal(joinPath(dir, stagedForParent), sourceFor(dir, stagedForParent, stagedParentSize, c))
                  ).found
                  const blockedByLiveParent =
                    stagedParentJournal === undefined ||
                    stagedParentJournal === 'torn' ||
                    stagedParentJournal === 'conflict' ||
                    'pending' in stagedParentJournal ||
                    stagedParentJournal.journal.retired !== true
                  if (blockedByLiveParent) {
                    parentWaits.push(name)
                    report.skippedFiles += 1
                    if (kindOf(name) !== 'rejected') tally.waiting += 1
                    continue
                  }
                } catch {
                  parentWaits.push(name)
                  report.skippedFiles += 1
                  if (kindOf(name) !== 'rejected') tally.waiting += 1
                  continue
                }
              }
            }
            const parentSize = parentName !== null && isSourceLive(parentName) ? (listed.get(parentName) ?? 0) : 0
            const rawFate =
              parentName === null || guardApprovedParent === parentName
                ? 'go'
                : await this.parentFate(dir, parentName, path, parentSize, c)
            const hasLiveOrWrittenGrandchild =
              supersededUnaccounted !== undefined &&
              Array.isArray(supersededUnaccounted.files) &&
              supersededUnaccounted.files.length > 0 &&
              (writtenOf(supersededUnaccounted).some(Boolean) ||
                supersededUnaccounted.files.some(f => typeof f === 'string' && (listed.get(baseOf(f)) ?? 0) > 0))
            let parentAccountedKeys: ReadonlySet<string> | undefined
            let breakWrittenChildDeadlock = rawFate === 'wait-pending' && trigger === 'force' && hasLiveOrWrittenGrandchild
            if (breakWrittenChildDeadlock && parentName !== null) {
              const parentPath = joinPath(dir, parentName)
              const liveRewrite = this.rewrites.get(parentPath)
              if (liveRewrite?.accountedRowKeys !== undefined) {
                parentAccountedKeys = liveRewrite.accountedRowKeys
              } else if (parentSize > 0 && parentSize <= HOST_FILE_BYTES) {
                try {
                  const parentRaw = await this.io.readFile(parentPath)
                  const parsedParent = parseFile(parentRaw)
                  if (parsedParent.complete && utf8Length(parentRaw) === parentSize) {
                    parentAccountedKeys = new Set(parsedParent.lines.map((line, i) => parentRowIdentity(line, parentName, i)))
                  } else {
                    breakWrittenChildDeadlock = false
                  }
                } catch {
                  breakWrittenChildDeadlock = false
                }
              } else {
                breakWrittenChildDeadlock = false
              }
            }
            const fate = breakWrittenChildDeadlock ? 'go' : rawFate === 'wait-pending' ? 'wait' : rawFate
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
              if (emptied) {
                emptiedInRun.add(name)
                parentIndex.delete(name)
                this.deleteHeldPath(path)
                this.currentScanHeld.delete(path)
              }
            } else if (fate === 'ambiguous' && trigger !== 'force') {
              // Conflicting parent records disagree without naming this child:
              // preserve the child untouched and report the ambiguity rather
              // than dropping potential unsent rows.
              this.noteHeld(
                report,
                trigger,
                `conflicting replay records exist for ${parentName}, so ${name} is preserved; /bqaa-backfill force replays the file afresh`,
                path,
              )
              report.skippedFiles += 1
              if (kindOf(name) !== 'rejected') tally.waiting += 1
            } else {
              if (trigger === 'force') this.uncertainSources.delete(path)
              const mergedAccountedKeys =
                parentAccountedKeys !== undefined && guardAccountedKeys !== undefined
                  ? new Set([...parentAccountedKeys, ...guardAccountedKeys])
                  : (parentAccountedKeys ?? guardAccountedKeys)
              const fileResult = await this.backfillFile(
                dir,
                name,
                parsed,
                source,
                c,
                kinds,
                trigger,
                report,
                tally,
                parentIndex,
                supersededUnaccounted,
                emptiedInRun,
                mergedAccountedKeys,
                allowStagedFor,
                guardSkipIndices,
                liveDigest,
                forceV2ForProtectedV1,
              )
              if (fileResult === 'skipped') {
                heldParents.add(name)
                if (kindOf(name) !== 'rejected') tally.waiting += 1
                emptied = true
              } else {
                emptied = fileResult
                if (this.retiredEmpty.has(path)) {
                  emptiedInRun.add(name)
                  parentIndex.delete(name)
                }
                report.files += 1
              }
            }
          }
        }
        if (!emptied) tally.waiting += 1
      } catch (err) {
        // A file whose record could not be read or written partway (the
        // store or the disk refusing it): its rows stay, it waits for the
        // next replay, and the replay goes on with the files after it.
        if (priorHeld !== undefined && !this.heldPaths.has(path)) this.setHeldPath(path, priorHeld)
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
    for (const e of recentEmpties) {
      if (this.replayHalted() || (!startedAtExit && this.closing)) break
      const emptyPath = joinPath(dir, e.name)
      if (this.io.now() - this.writtenSince(emptyPath, e.mtimeMs) < WRITE_PENDING_MS) {
        const emptyState = await isRetired(e.name)
        if (emptyState === 'aborted') break
        if (emptyState === 'unretired') {
          emptyWaiting.push(e.name)
          report.skippedFiles += 1
          tally.waiting += 1
        }
      }
    }
    // Historical record retention and orphaned v2 store reclamation always run
    // AFTER live spills so cleanup never starves waiting rows or wipes a file
    // journal that a live child still depends on.
    if (companions.size > 0 || besides.size > 0) {
      let cleanupUntil = Number.POSITIVE_INFINITY
      if (startedAtExit && this.exitReplay !== null) {
        const now = this.io.now()
        const remaining = Math.max(0, this.exitReplay.postUntil - now)
        const budget = Math.min(EXIT_CLEANUP_CAP_MS, Math.floor(remaining * EXIT_CLEANUP_SHARE))
        cleanupUntil = Math.min(this.exitReplay.postUntil, now + budget)
      }
      const cleanupStopped = (): boolean => {
        if (this.exitReplay === null) return this.closing
        if (!startedAtExit) return true
        return this.exitReplay.postCutoff.passed() || this.io.now() >= cleanupUntil
      }
      if (!cleanupStopped()) {
        const CLEANUP_ABORT = Symbol('cleanup abort')
        let cleanupAborted = false
        let fireCleanupAbort = (): void => undefined
        const cleanupAbortPromise = new Promise<typeof CLEANUP_ABORT>(resolve => {
          fireCleanupAbort = () => {
            if (cleanupAborted) return
            cleanupAborted = true
            resolve(CLEANUP_ABORT)
          }
        })
        let cleanupTimer: Timer | null = null
        if (!startedAtExit) {
          this.pendingCleanupWakeups.add(fireCleanupAbort)
        } else if (this.exitReplay !== null) {
          void this.exitReplay.postCutoff.at.then(fireCleanupAbort)
          const wait = Math.max(0, cleanupUntil - this.io.now())
          if (wait === 0) fireCleanupAbort()
          else cleanupTimer = this.io.after(wait, fireCleanupAbort)
        }
        const cleanupOp = async <T>(work: Promise<T>): Promise<T | typeof CLEANUP_ABORT> => {
          work.catch(() => undefined)
          if (cleanupAborted || cleanupStopped()) {
            fireCleanupAbort()
            return CLEANUP_ABORT
          }
          const result = await Promise.race([work, cleanupAbortPromise])
          if (cleanupAborted || cleanupStopped()) {
            fireCleanupAbort()
            return CLEANUP_ABORT
          }
          return result
        }
        try {
          const doneAt = new Map<string, number | null>()
          const retiredAt = async (of: string): Promise<number | null> => {
            const known = doneAt.get(of)
            if (known !== undefined) return known
            let at: number | null = null
            const guardParents: { parent: string; size: number }[] = []
            for (const x of companions.get(of) ?? []) {
              if (cleanupStopped()) return null
              if (x.size === 0) continue
              try {
                const raw = await cleanupOp(this.io.readFile(joinPath(dir, x.name)))
                if (raw === CLEANUP_ABORT) return null
                const kept = journalOf(raw) as (Partial<Journal> & { childGuard?: unknown; stagedFor?: unknown }) | 'torn' | undefined
                if (kept !== undefined && kept !== 'torn') {
                  if (typeof kept.retiredAt === 'number') at = Math.max(at ?? kept.retiredAt, kept.retiredAt)
                  else if (kept.childGuard === true && typeof kept.stagedFor === 'string' && kept.stagedFor !== '' && typeof kept.size === 'number' && kept.size > 0) {
                    guardParents.push({ parent: baseOf(kept.stagedFor), size: kept.size })
                  }
                }
              } catch {
                // Not read: no time from it.
              }
            }
            if (at === null && (listed.get(of) ?? 0) === 0 && guardParents.length > 0) {
              for (const gp of guardParents) {
                for (const px of companions.get(gp.parent) ?? []) {
                  if (cleanupStopped()) return null
                  if (px.size === 0) continue
                  try {
                    const pRaw = await cleanupOp(this.io.readFile(joinPath(dir, px.name)))
                    if (pRaw === CLEANUP_ABORT) return null
                    const pj = journalOf(pRaw)
                    if (
                      pj !== undefined &&
                      pj !== 'torn' &&
                      pj.retired === true &&
                      typeof pj.retiredAt === 'number' &&
                      normalizeReconciled(pj.reconciled).some(r => r.name === of && r.size === gp.size)
                    ) {
                      at = Math.max(at ?? pj.retiredAt, pj.retiredAt)
                    }
                  } catch {
                    // Best-effort fallback to parent retiredAt for legacy childGuard marks.
                  }
                }
              }
            }
            doneAt.set(of, at)
            return at
          }
          // Lazily refresh the directory listing before deleting any parent
          // evidence so a late child written while an unrelated request was in
          // flight (or while cleanup reads are running) is never overlooked.
          let freshListed: Map<string, number> | null = null
          let freshMtimes: Map<string, number | undefined> = new Map()
          let freshMarksBySource: Map<string, string[]> = new Map()
          let freshListFailed = false
          let liveSourceRemains = false
          let liveParentsChecked = false
          let liveParentsIncomplete = false
          const liveParents = new Set<string>()
          const verifiedLiveFiles = new Set<string>()
          const verifiedMarkFiles = new Set<string>()
          let freshListedAt = 0
          const RACY_MTIME_MS = 2_000
          const isMtimeSettledAt = (mtimeMs: number | undefined, anchorMs: number): boolean =>
            mtimeMs !== undefined && mtimeMs > 0 && mtimeMs <= anchorMs - RACY_MTIME_MS
          const ensureFreshListing = async (): Promise<boolean> => {
            if (freshListed !== null) return true
            if (freshListFailed) return false
            try {
              const listedAt = this.io.now()
              const latest = await cleanupOp(this.io.listDir(dir))
              if (latest === CLEANUP_ABORT) {
                freshListFailed = true
                return false
              }
              const map = new Map<string, number>()
              const mtimes = new Map<string, number | undefined>()
              const marks = new Map<string, string[]>()
              for (const entry of latest) {
                if (entry.kind === undefined || entry.kind === 'file') {
                  map.set(entry.name, entry.size)
                  mtimes.set(entry.name, entry.mtimeMs)
                  if (entry.size > 0 && kindOf(entry.name) !== null) {
                    liveSourceRemains = true
                  } else if (entry.size > 0) {
                    const of = recordOf(entry.name)
                    if (of !== null && markerOf(entry.name, of) !== null) {
                      const list = marks.get(of)
                      if (list) list.push(entry.name)
                      else marks.set(of, [entry.name])
                    }
                  }
                }
              }
              freshListedAt = listedAt
              freshListed = map
              freshMtimes = mtimes
              freshMarksBySource = marks
              return true
            } catch {
              freshListFailed = true
              return false
            }
          }
          const isFreshLive = (n: string): boolean => (freshListed !== null ? (freshListed.get(n) ?? 0) > 0 : isSourceLive(n))
          const hasLiveChild = (stored: unknown): boolean => {
            if (stored === null || typeof stored !== 'object') return false
            const j = stored as Partial<Journal>
            if (Array.isArray(j.files) && j.files.some(f => typeof f === 'string' && isFreshLive(baseOf(f)))) return true
            if (normalizeStaged(j.stagedFiles).some(f => isFreshLive(f))) return true
            if (normalizeBackups(j.backupFiles).some(b => isFreshLive(b.name))) return true
            if (normalizeReconciled(j.reconciled).some(r => isFreshLive(r.name))) return true
            if (normalizeReconciled(j.pendingReconcile, true).some(r => isFreshLive(r.name))) return true
            return false
          }
          const ensureLiveParents = async (): Promise<void> => {
            if (liveParentsChecked) return
            liveParentsChecked = true
            if (!(await ensureFreshListing()) || freshListed === null) {
              liveParentsIncomplete = true
              return
            }
            if (!liveSourceRemains && freshMarksBySource.size === 0) return
            const initialAnchor = Math.min(initialListedAt, freshListedAt)
            for (const [n, sz] of freshListed) {
              if (sz <= 0 || kindOf(n) === null) continue
              let sealResolved = false
              const preSize = listed.get(n)
              const freshMtime = freshMtimes.get(n)
              const stampUnchanged =
                stamps.get(n) === freshMtime && isMtimeSettledAt(freshMtime, initialAnchor)
              if (preSize === sz && !emptiedInRun.has(n) && stampUnchanged) {
                const known = fileParent.get(n)
                if (known !== undefined) {
                  if (known !== '') liveParents.add(known)
                  sealResolved = true
                }
              }
              if (!sealResolved && stampUnchanged) {
                const indexed = parentIndex.get(n)
                if (indexed !== undefined && indexed.size === sz) {
                  if (indexed.parent !== null) liveParents.add(indexed.parent)
                  sealResolved = true
                }
              }
              if (!sealResolved) {
                if (sz > HOST_FILE_BYTES || cleanupStopped()) {
                  liveParentsIncomplete = true
                  return
                }
                try {
                  const raw = await cleanupOp(this.io.readFile(joinPath(dir, n)))
                  if (raw === CLEANUP_ABORT) {
                    liveParentsIncomplete = true
                    return
                  }
                  if (raw !== '') {
                    const parsed = parseFile(raw)
                    if (!parsed.complete) {
                      liveParentsIncomplete = true
                      return
                    }
                    if (parsed.parent !== null) liveParents.add(baseOf(parsed.parent))
                  }
                } catch {
                  liveParentsIncomplete = true
                  return
                }
              }
              if (isMtimeSettledAt(freshMtime, freshListedAt)) verifiedLiveFiles.add(n)
              for (const markName of freshMarksBySource.get(n) ?? []) {
                if (cleanupStopped()) {
                  liveParentsIncomplete = true
                  return
                }
                try {
                  const rawMark = await cleanupOp(this.io.readFile(joinPath(dir, markName)))
                  if (rawMark === CLEANUP_ABORT) {
                    liveParentsIncomplete = true
                    return
                  }
                  if (rawMark === '') continue
                  const parsedMark = journalOf(rawMark) as { stagedFor?: unknown; replaces?: unknown } | 'torn' | undefined
                  if (parsedMark === 'torn') {
                    liveParentsIncomplete = true
                    return
                  }
                  if (parsedMark !== undefined) {
                    if (typeof parsedMark.stagedFor === 'string' && parsedMark.stagedFor !== '') liveParents.add(baseOf(parsedMark.stagedFor))
                    if (typeof parsedMark.replaces === 'string' && parsedMark.replaces !== '') liveParents.add(baseOf(parsedMark.replaces))
                  }
                  if (isMtimeSettledAt(freshMtimes.get(markName), freshListedAt)) verifiedMarkFiles.add(markName)
                } catch {
                  liveParentsIncomplete = true
                  return
                }
              }
            }
            // Protect retired parents referenced by any standing `childGuard`
            // mark beside an emptied child so retention always clears the
            // child's guard before deleting the parent's retired records.
            for (const [of, markNames] of freshMarksBySource) {
              if ((freshListed.get(of) ?? 0) > 0) continue
              for (const markName of markNames) {
                const by = markerOf(markName, of)
                if (by === null || !by.endsWith('g')) continue
                if (cleanupStopped()) {
                  liveParentsIncomplete = true
                  return
                }
                try {
                  const rawMark = await cleanupOp(this.io.readFile(joinPath(dir, markName)))
                  if (rawMark === CLEANUP_ABORT) {
                    liveParentsIncomplete = true
                    return
                  }
                  if (rawMark === '') continue
                  const parsedMark = journalOf(rawMark) as { childGuard?: unknown; stagedFor?: unknown } | 'torn' | undefined
                  if (parsedMark === 'torn') {
                    liveParentsIncomplete = true
                    return
                  }
                  if (parsedMark !== undefined && parsedMark.childGuard === true && typeof parsedMark.stagedFor === 'string' && parsedMark.stagedFor !== '') {
                    liveParents.add(baseOf(parsedMark.stagedFor))
                  }
                  if (isMtimeSettledAt(freshMtimes.get(markName), freshListedAt)) verifiedMarkFiles.add(markName)
                } catch {
                  liveParentsIncomplete = true
                  return
                }
              }
            }
          }
          // Re-verify per parent right before deleting its store/companion
          // records that no new, resized, mtime-changed, or racy-window
          // same-size re-parented live dead-letter file or standing mark now
          // points to `parentName`. Settled live files/marks already verified in
          // `ensureLiveParents()` with unchanged size and `mtimeMs <= freshListedAt - 2s`
          // skip redundant full-file re-reads so a pass stays O(live files).
          const MAX_CLEANUP_PARENTS_PER_PASS = 8
          const confirmDirStable = async (parentName: string): Promise<boolean> => {
            if (freshListed === null) return false
            try {
              const nowEntries = await cleanupOp(this.io.listDir(dir))
              if (nowEntries === CLEANUP_ABORT) return false
              const settleAnchor = Math.min(freshListedAt, this.io.now())
              const isSettled = (name: string, size: number, mtimeMs: number | undefined): boolean =>
                freshListed!.get(name) === size &&
                freshMtimes.get(name) === mtimeMs &&
                isMtimeSettledAt(mtimeMs, settleAnchor)
              const liveNow: { name: string; size: number; mtimeMs?: number }[] = []
              const liveNowSet = new Set<string>()
              const marksNow = new Map<string, { name: string; size: number; mtimeMs?: number }[]>()
              for (const entry of nowEntries) {
                if (entry.kind !== undefined && entry.kind !== 'file') continue
                if (entry.name === parentName && entry.size > 0) return false
                if (kindOf(entry.name) !== null) {
                  if (!freshListed.has(entry.name) || (entry.size > 0 && freshListed.get(entry.name) !== entry.size)) {
                    return false
                  }
                  if (entry.size > 0) {
                    const prevMtime = freshMtimes.get(entry.name)
                    if (prevMtime !== undefined && entry.mtimeMs !== undefined && prevMtime !== entry.mtimeMs) {
                      return false
                    }
                    liveNow.push({ name: entry.name, size: entry.size, mtimeMs: entry.mtimeMs })
                    liveNowSet.add(entry.name)
                  }
                } else if (entry.size > 0) {
                  const of = recordOf(entry.name)
                  if (of !== null && markerOf(entry.name, of) !== null) {
                    const list = marksNow.get(of)
                    const item = { name: entry.name, size: entry.size, mtimeMs: entry.mtimeMs }
                    if (list) list.push(item)
                    else marksNow.set(of, [item])
                  }
                }
              }
              for (const live of liveNow) {
                if (live.size > HOST_FILE_BYTES || cleanupStopped()) return false
                if (!(verifiedLiveFiles.has(live.name) && isSettled(live.name, live.size, live.mtimeMs))) {
                  const raw = await cleanupOp(this.io.readFile(joinPath(dir, live.name)))
                  if (raw === CLEANUP_ABORT) return false
                  if (raw !== '') {
                    const parsed = parseFile(raw)
                    if (!parsed.complete) return false
                    if (parsed.parent !== null && baseOf(parsed.parent) === parentName) return false
                  }
                }
                for (const mark of marksNow.get(live.name) ?? []) {
                  if (cleanupStopped()) return false
                  if (!freshListed.has(mark.name) || freshListed.get(mark.name) !== mark.size) return false
                  const prevMarkMtime = freshMtimes.get(mark.name)
                  if (prevMarkMtime !== undefined && mark.mtimeMs !== undefined && prevMarkMtime !== mark.mtimeMs) return false
                  if (verifiedMarkFiles.has(mark.name) && isSettled(mark.name, mark.size, mark.mtimeMs)) continue
                  const rawMark = await cleanupOp(this.io.readFile(joinPath(dir, mark.name)))
                  if (rawMark === CLEANUP_ABORT) return false
                  if (rawMark === '') continue
                  const parsedMark = journalOf(rawMark) as { stagedFor?: unknown; replaces?: unknown } | 'torn' | undefined
                  if (parsedMark === 'torn') return false
                  if (parsedMark !== undefined) {
                    if (typeof parsedMark.stagedFor === 'string' && parsedMark.stagedFor !== '' && baseOf(parsedMark.stagedFor) === parentName) return false
                    if (typeof parsedMark.replaces === 'string' && parsedMark.replaces !== '' && baseOf(parsedMark.replaces) === parentName) return false
                  }
                }
              }
              for (const [of, markList] of marksNow) {
                if (liveNowSet.has(of)) continue
                for (const mark of markList) {
                  const by = markerOf(mark.name, of)
                  if (by === null || !by.endsWith('g')) continue
                  if (cleanupStopped()) return false
                  if (!freshListed.has(mark.name) || freshListed.get(mark.name) !== mark.size) return false
                  const prevMarkMtime = freshMtimes.get(mark.name)
                  if (prevMarkMtime !== undefined && mark.mtimeMs !== undefined && prevMarkMtime !== mark.mtimeMs) return false
                  if (verifiedMarkFiles.has(mark.name) && isSettled(mark.name, mark.size, mark.mtimeMs)) continue
                  const rawMark = await cleanupOp(this.io.readFile(joinPath(dir, mark.name)))
                  if (rawMark === CLEANUP_ABORT) return false
                  if (rawMark === '') continue
                  const parsedMark = journalOf(rawMark) as { childGuard?: unknown; stagedFor?: unknown } | 'torn' | undefined
                  if (parsedMark === 'torn') return false
                  if (parsedMark !== undefined && parsedMark.childGuard === true && typeof parsedMark.stagedFor === 'string' && baseOf(parsedMark.stagedFor) === parentName) {
                    return false
                  }
                }
              }
              return true
            } catch {
              return false
            }
          }
          const cleanedParents = new Set<string>()
          const readyExpired: { of: string; nonEmpty: { name: string; size: number; mtimeMs?: number }[]; storedKeysPresent: string[] }[] = []
          for (const [of, list] of companions) {
            if (cleanupStopped() || readyExpired.length >= MAX_CLEANUP_PARENTS_PER_PASS) break
            if ((listed.get(of) ?? 0) > 0) continue
            const nonEmpty = list.filter(e => e.size > 0)
            if (nonEmpty.length === 0) continue
            let allExpired = true
            for (const e of nonEmpty) {
              if (cleanupStopped()) {
                allExpired = false
                break
              }
              const at = e.mtimeMs ? e.mtimeMs : await retiredAt(of)
              if (at === null || this.io.now() - at < RETIRED_MS) {
                allExpired = false
                break
              }
            }
            if (!allExpired || cleanupStopped()) continue
            await ensureLiveParents()
            if (liveParentsIncomplete || liveParents.has(of) || isFreshLive(of) || cleanupStopped()) continue
            // Inspect ALL companion files and both store slots for this parent
            // before deleting or truncating any record: journals and retired
            // marks that any live child still depends on must stay intact together.
            let keepParent = false
            const storeKeys = [`replayed:${dest}/${of}`, `replayed:v2:${dest}/${of}`]
            const storedKeysPresent: string[] = []
            const inspectBatch = await cleanupOp(
              Promise.all([
                Promise.all(
                  nonEmpty.map(async e => {
                    try {
                      return { ok: true as const, raw: await this.io.readFile(joinPath(dir, e.name)) }
                    } catch {
                      return { ok: false as const, raw: '' }
                    }
                  }),
                ),
                Promise.all(
                  storeKeys.map(async key => {
                    try {
                      return { ok: true as const, key, stored: await this.io.storeGet(key) }
                    } catch {
                      return { ok: false as const, key, stored: undefined }
                    }
                  }),
                ),
              ]),
            )
            if (inspectBatch === CLEANUP_ABORT) break
            const [companionReads, storeReads] = inspectBatch
            for (const read of companionReads) {
              if (!read.ok) {
                keepParent = true
                break
              }
              const parsed = journalOf(read.raw)
              if (parsed === 'torn') {
                if (liveSourceRemains) {
                  keepParent = true
                  break
                }
              } else if (parsed !== undefined && hasLiveChild(parsed)) {
                keepParent = true
                break
              }
            }
            if (keepParent) continue
            for (const read of storeReads) {
              if (!read.ok) {
                keepParent = true
                break
              }
              if (read.stored !== undefined) {
                if (!isClearedSlot(read.stored) && hasLiveChild(read.stored)) {
                  keepParent = true
                  break
                }
                storedKeysPresent.push(read.key)
              }
            }
            if (keepParent || cleanupStopped()) continue
            readyExpired.push({ of, nonEmpty, storedKeysPresent })
          }
          if (readyExpired.length > 0 && !cleanupStopped()) {
            for (const { of, nonEmpty, storedKeysPresent } of readyExpired) {
              if (cleanupStopped()) break
              if (!(await confirmDirStable(of))) break
              // Delete store entries BEFORE truncating on-disk companion files so a
              // store failure or exit-cleanup cutoff never strands an orphaned v2
              // store key with no remaining done mark on disk to reclaim it.
              const v2Key = `replayed:v2:${dest}/${of}`
              let allStoreDeleted = true
              let v2Clean = !storedKeysPresent.includes(v2Key)
              if (storedKeysPresent.length > 0) {
                const deleteResults = await cleanupOp(
                  Promise.all(
                    storedKeysPresent.map(async key => {
                      try {
                        await this.io.storeDelete(key)
                        return { ok: true as const, key }
                      } catch {
                        return { ok: false as const, key }
                      }
                    }),
                  ),
                )
                if (deleteResults === CLEANUP_ABORT) {
                  allStoreDeleted = false
                } else {
                  for (const res of deleteResults) {
                    if (!res.ok) allStoreDeleted = false
                    else if (res.key === v2Key) v2Clean = true
                  }
                }
              }
              if (v2Clean) this.cleanedRetiredV2.add(v2Key)
              if (!allStoreDeleted || cleanupStopped()) continue
              const truncateResults = await cleanupOp(
                Promise.all(
                  nonEmpty.map(async e => {
                    try {
                      await this.io.writeFile(joinPath(dir, e.name), '')
                      return true
                    } catch {
                      return false
                    }
                  }),
                ),
              )
              if (truncateResults !== CLEANUP_ABORT && truncateResults.every(Boolean)) cleanedParents.add(of)
            }
          }
          // A legacy writer that retires a source only deletes `replayed:<dest>/<name>`
          // and leaves `replayed:v2:<dest>/<name>` orphaned in `$.store`. Reclaim any
          // such orphaned v2 key once a retired done mark (or a zero-byte truncated
          // done marker with no companion journal files) stands beside the emptied or
          // removed source, while never deleting a store journal that still names a
          // live child or whose parent is still referenced by a live file on disk.
          if (!cleanupStopped() && besides.size > 0) {
            const readyOrphanedV2: { of: string; v2Key: string }[] = []
            for (const [of, beside] of besides) {
              if (cleanupStopped() || readyOrphanedV2.length >= MAX_CLEANUP_PARENTS_PER_PASS) break
              if (cleanedParents.has(of) || (beside.done.length === 0 && !beside.zeroDone) || (listed.get(of) ?? 0) > 0) continue
              if (beside.done.length === 0 && beside.record) continue
              const v2Key = `replayed:v2:${dest}/${of}`
              if (this.cleanedRetiredV2.has(v2Key)) continue
              try {
                const stored = await cleanupOp(this.io.storeGet(v2Key))
                if (stored === CLEANUP_ABORT) break
                if (stored === undefined) {
                  this.cleanedRetiredV2.add(v2Key)
                  continue
                }
                await ensureLiveParents()
                if (liveParentsIncomplete || liveParents.has(of) || isFreshLive(of) || cleanupStopped()) continue
                if (!isClearedSlot(stored) && hasLiveChild(stored)) continue
                let retired = beside.done.length === 0 && beside.zeroDone && !beside.record
                for (const doneName of beside.done) {
                  if (cleanupStopped()) break
                  const raw = await cleanupOp(this.io.readFile(joinPath(dir, doneName)).catch(() => ''))
                  if (raw === CLEANUP_ABORT) {
                    retired = false
                    break
                  }
                  const kept = journalOf(raw)
                  if (kept !== undefined && kept !== 'torn' && kept.retired === true) {
                    retired = true
                    break
                  }
                }
                if (!retired || cleanupStopped()) continue
                readyOrphanedV2.push({ of, v2Key })
              } catch {
                // Best-effort cleanup; next replay retries.
              }
            }
            if (readyOrphanedV2.length > 0 && !cleanupStopped()) {
              for (const { of, v2Key } of readyOrphanedV2) {
                if (cleanupStopped()) break
                if (!(await confirmDirStable(of))) break
                try {
                  const deleted = await cleanupOp(this.io.storeDelete(v2Key))
                  if (deleted === CLEANUP_ABORT) break
                  this.cleanedRetiredV2.add(v2Key)
                } catch {
                  // Best-effort cleanup; next replay retries.
                }
              }
            }
          }
        } finally {
          this.pendingCleanupWakeups.delete(fireCleanupAbort)
          cleanupTimer?.cancel()
        }
      }
    }
    if (emptyWaiting.length > 0) report.held ??= diagnostic(`${emptyWaiting[0]} is empty, perhaps still being written; it waits for the next replay`)
    if (parentWaits.length > 0) report.held ??= diagnostic(`${parentWaits[0]} waits until the replay of the file it came from has it as written`)
    for (const lname of leased) {
      report.held ??= diagnostic(`another session's lease holds ${lname} (ten minutes at most); it waits for the next replay`)
    }
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
  // session end's own replay, once its cutoff has passed or fewer than
  // EXIT_REPLAY_POST_MIN_MS remain before its outcome reserve.
  private replayHalted(): boolean {
    const exit = this.exitReplay
    return exit === null ? this.closing : exit.cutoff.passed() || exit.until - this.io.now() < EXIT_REPLAY_POST_MIN_MS
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
  // cutoff at the latest (postCutoff on a single-batch whole file so a late
  // 200 can still commit its mark; cutoff once a rewrite/split reserve is
  // needed). Outside session end (`exit === null`), client timeouts release
  // the mark while the session stays live so recovery delivers parked files
  // once BigQuery answers again, and hold the mark if shutdown began while the
  // request was in flight (and wake at replayAtExit's cutoff so multi-batch
  // splits can commit confirmed rows within the exit reserve).
  private async replayPost(url: string, body: string, token: string, timeoutMs: number, needsReserve = false): Promise<HttpAnswer | { error: string; uncertain?: boolean }> {
    const exit = this.exitReplay
    if (exit === null) {
      let cut = false
      let fireCut = (): void => undefined
      const cutAt = new Promise<void>(resolve => {
        fireCut = () => {
          if (cut) return
          cut = true
          resolve()
        }
      })
      const onExitMarks = (marks: ReplayExitMarks): void => {
        const mark = needsReserve ? marks.replay : marks.replayPost
        if (mark.passed()) fireCut()
        else void mark.at.then(fireCut)
      }
      if (this.activeExitMarks !== null) onExitMarks(this.activeExitMarks)
      else this.pendingReplayWakeups.add(onExitMarks)
      try {
        const answer = await this.post(url, body, token, timeoutMs, {
          mark: { at: cutAt, passed: () => cut },
          message: 'no answer before the session ended',
          uncertainOnTimeout: false,
        })
        if ('error' in answer) {
          if (answer.uncertain === true) return { error: answer.error, uncertain: true }
          return answer.timedOut && (this.closing || this.exitReplay !== null) ? { error: answer.error, uncertain: true } : { error: answer.error }
        }
        return answer
      } finally {
        this.pendingReplayWakeups.delete(onExitMarks)
      }
    }
    const deadline = needsReserve ? exit.until : exit.postUntil
    const mark = needsReserve ? exit.cutoff : exit.postCutoff
    if (this.replayHalted() || mark.passed() || deadline - this.io.now() <= 0) return { error: 'no answer before the session ended' }
    return this.post(url, body, token, Math.max(1, Math.min(timeoutMs, deadline - this.io.now())), { mark, message: 'no answer before the session ended' })
  }

  // Inspects a file skipped on early scan stop (only when `companions` already
  // lists an `outcome-*` / `outcome2-*` companion for it) so a fresh `-p`
  // process still counts standing held marks in `heldNoticeInfo()` with O(1)
  // work per skipped file and parity with full-scan deduplication.
  private async seedSkippedHeld(
    dir: string,
    name: string,
    size: number,
    c: Config,
    companions: ReadonlyMap<string, readonly { name: string; kind: string; size: number }[]>,
    listed: ReadonlyMap<string, number>,
    trigger: BackfillReport['trigger'],
    inspectedSkipped: Map<string, 'covered' | 'handled'>,
    startedAtExit = this.exitReplay !== null,
  ): Promise<'covered' | 'handled'> {
    const skippedPath = joinPath(dir, name)
    const cached = inspectedSkipped.get(skippedPath)
    if (cached !== undefined) return cached
    if (this.rewrites.has(skippedPath) || this.heldPaths.get(skippedPath)?.activeCertain === true) {
      inspectedSkipped.set(skippedPath, 'handled')
      return 'handled'
    }
    const SKIP_ABORTED = Symbol('skipped discovery aborted')
    const abortedNow = (): boolean => (startedAtExit ? this.exitReplay !== null && this.exitReplay.postCutoff.passed() : this.closing)
    if (abortedNow()) return 'handled'
    inspectedSkipped.set(skippedPath, 'handled')
    let wakeAbort = (): void => undefined
    let abortPromise: Promise<typeof SKIP_ABORTED> | null = null
    if (!startedAtExit) {
      abortPromise = new Promise<typeof SKIP_ABORTED>(resolve => {
        wakeAbort = () => resolve(SKIP_ABORTED)
      })
      this.pendingCleanupWakeups.add(wakeAbort)
    } else if (this.exitReplay !== null) {
      abortPromise = this.exitReplay.postCutoff.at.then((): typeof SKIP_ABORTED => SKIP_ABORTED)
    }
    const bounded = async <T>(start: () => Promise<T>, onError?: () => T): Promise<T | typeof SKIP_ABORTED> => {
      if (abortedNow()) return SKIP_ABORTED
      const op = Promise.resolve()
        .then(start)
        .catch(() => (onError ? onError() : (SKIP_ABORTED as typeof SKIP_ABORTED)))
      if (abortPromise === null) return op
      return Promise.race([op, abortPromise])
    }
    try {
      const recordHeld = (targetPath: string, message: string, mayBeActive = false, activeCertain = false): void => {
        const cleanMsg = diagnostic(message)
        const prev = this.heldPaths.get(targetPath)
        const prevActive = prev?.mayBeActive === true
        const prevCertain = prev?.activeCertain === true
        if (prev !== undefined && ((prevCertain && !activeCertain) || (prevActive && !mayBeActive))) {
          return
        }
        const nextActive = prevActive || mayBeActive
        const nextCertain = prevCertain || activeCertain
        const nextMsg = prev !== undefined && !nextActive && !nextCertain ? prev.reason : cleanMsg
        this.setHeldPath(targetPath, { reason: nextMsg, mayBeActive: nextActive, ...(nextCertain ? { activeCertain: true } : {}) })
        const active = this.hasActiveHeldIn(dir)
        this.inProgressHeld = {
          files: Math.max(this.inProgressHeld.files, this.heldCountIn(dir)),
          reason: this.inProgressHeld.reason ?? nextMsg,
          trigger: this.inProgressHeld.trigger ?? trigger,
          mayBeActive: active,
        }
      }
      const me = this.sessionId() ?? 'unknown'
      const leaseRes = await bounded(
        () => this.io.storeGet(`lease:${destinationDir(tableRef(c))}/${name}`) as Promise<{ by?: string; until?: number } | undefined>,
        () => undefined,
      )
      if (leaseRes === SKIP_ABORTED) return 'handled'
      if (leaseRes && typeof leaseRes.until === 'number' && leaseRes.until > this.io.now() && leaseRes.by !== me) {
        recordHeld(skippedPath, `another session's lease holds ${name} (ten minutes at most); it waits for the next replay`, true, true)
        return 'handled'
      }
      const companionEntries = companions.get(name) ?? []
      if (companionEntries.length === 0) return 'handled'
      const source = sourceFor(dir, name, size, c)
      let liveDigest: string | undefined
      let rawReadState: 'unread' | 'ok' | 'empty' | 'mismatch-or-error' = 'unread'
      const ensureLiveDigest = async (): Promise<'ok' | 'empty' | 'mismatch-or-error' | typeof SKIP_ABORTED> => {
        if (rawReadState !== 'unread') return rawReadState
        if (size <= 0 || size > HOST_FILE_BYTES) {
          rawReadState = 'mismatch-or-error'
          return rawReadState
        }
        const rawSkipped = await bounded<string | null>(
          () => this.io.readFile(skippedPath),
          () => null,
        )
        if (rawSkipped === SKIP_ABORTED) return SKIP_ABORTED
        if (rawSkipped === null) {
          rawReadState = 'mismatch-or-error'
        } else if (rawSkipped === '') {
          rawReadState = 'empty'
        } else if (utf8Length(rawSkipped) === size) {
          liveDigest = fnv1a(rawSkipped)
          rawReadState = 'ok'
        } else {
          rawReadState = 'mismatch-or-error'
        }
        return rawReadState
      }
      const first = await bounded(() => this.doneMarksAll(source, companionEntries))
      if (first === SKIP_ABORTED) return 'handled'
      if (!first.torn && first.records.length > 0) {
        let doneMatches = true
        const hasExplicitDigest = first.records.some(r => typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '')
        if (hasExplicitDigest && size > 0 && size <= HOST_FILE_BYTES) {
          const state = await ensureLiveDigest()
          if (state === SKIP_ABORTED) return 'handled'
          if (state === 'mismatch-or-error') {
            doneMatches = false
          } else if (state === 'ok' && liveDigest !== undefined) {
            doneMatches = Writer.matchesDoneMarks(first.records, liveDigest)
          }
        }
        if (doneMatches) {
          inspectedSkipped.set(skippedPath, 'covered')
          return 'covered'
        }
        if (liveDigest !== undefined) {
          source.fnv1a = liveDigest
          source.strictFnv1a = true
        }
      }
      const readSlots = (s: Source) =>
        bounded(() =>
          Promise.all([
            this.readJournalSlot(s, s.recordKey, s.recordFiles, false),
            this.readJournalSlot(s, s.v2RecordKey, s.v2RecordFiles, true),
          ]),
        )
      let slots = await readSlots(source)
      if (slots === SKIP_ABORTED) return 'handled'
      let [v1, v2] = slots
      let picked = Writer.pickJournalSlots(v1, v2)
      const slotHasExplicitDigest =
        picked === 'conflict' ||
        (picked !== undefined && typeof picked.journal.sourceFnv1a === 'string' && picked.journal.sourceFnv1a !== '')
      if (slotHasExplicitDigest && !source.strictFnv1a && size > 0 && size <= HOST_FILE_BYTES) {
        const state = await ensureLiveDigest()
        if (state === SKIP_ABORTED) return 'handled'
        if (state === 'mismatch-or-error') {
          // Do not suppress held notices using an unverified explicit-digest slot on EIO/size change.
          picked = undefined
        } else if (state === 'ok' && liveDigest !== undefined) {
          if (picked === 'conflict') {
            const nonStrictSlots = await readSlots({ ...source, fnv1a: liveDigest, strictFnv1a: false })
            if (nonStrictSlots === SKIP_ABORTED) return 'handled'
            const nonStrictPicked = Writer.pickJournalSlots(nonStrictSlots[0], nonStrictSlots[1])
            if (
              nonStrictPicked === undefined ||
              (nonStrictPicked !== 'conflict' && nonStrictPicked.journal.sourceFnv1a === liveDigest)
            ) {
              source.fnv1a = liveDigest
              source.strictFnv1a = true
              ;[v1, v2] = nonStrictSlots
              picked = nonStrictPicked
            }
          } else if (picked !== undefined && picked.journal.sourceFnv1a !== liveDigest) {
            source.fnv1a = liveDigest
            source.strictFnv1a = true
            const strictSlots = await readSlots(source)
            if (strictSlots === SKIP_ABORTED) return 'handled'
            ;[v1, v2] = strictSlots
            picked = Writer.pickJournalSlots(v1, v2)
          }
        }
      }
      if (picked === 'conflict') {
        const foreign = await bounded(() => this.hasActiveForeignMark(source, companionEntries))
        if (foreign === SKIP_ABORTED) return 'handled'
        recordHeld(skippedPath, `conflicting replay records exist for ${name}; /bqaa-backfill force replays the file afresh`, foreign.mayBeActive, foreign.activeCertain)
        return 'handled'
      }
      if (picked !== undefined) {
        if (Array.isArray(picked.journal.uncertain) && picked.journal.uncertain.some(Boolean)) {
          const journalFiles = Array.isArray(picked.journal.files) ? picked.journal.files : []
          const journalWritten = writtenOf(picked.journal)
          let needsParent = false
          for (let i = 0; i < picked.journal.uncertain.length; i += 1) {
            if (abortedNow()) return 'handled'
            if (picked.journal.uncertain[i] !== true) continue
            const childName = journalFiles[i] !== undefined ? baseOf(journalFiles[i] as string) : ''
            const childSize = childName !== '' ? (listed.get(childName) ?? 0) : 0
            if (childName !== '' && childSize > 0) {
              const childPath = joinPath(dir, childName)
              let childCovered = false
              try {
                childCovered = (await this.seedSkippedHeld(dir, childName, childSize, c, companions, listed, trigger, inspectedSkipped, startedAtExit)) === 'covered'
              } catch {
                // Best-effort child inspection before fallback held seeding.
              }
              if (abortedNow()) return 'handled'
              if (!childCovered && !this.heldPaths.has(childPath) && !this.rewrites.has(childPath)) {
                recordHeld(
                  childPath,
                  `a replay of ${childName} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
                )
              }
            } else if (journalWritten[i] !== true) {
              needsParent = true
            }
          }
          if (needsParent) {
            recordHeld(
              skippedPath,
              `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
            )
          }
        }
        inspectedSkipped.set(skippedPath, 'covered')
        return 'covered'
      }
      if (v1.torn || v2.torn || first.torn) {
        const foreign = await bounded(() => this.hasActiveForeignMark(source, companionEntries))
        if (foreign === SKIP_ABORTED) return 'handled'
        recordHeld(skippedPath, `the replay record of ${name} is cut off part-way; /bqaa-backfill force replays the file afresh`, foreign.mayBeActive, foreign.activeCertain)
        return 'handled'
      }
      const detailed = await bounded(() => this.marksDetailed(source, companionEntries))
      if (detailed === SKIP_ABORTED) return 'handled'
      const { by, activeBy, nonUncertainBy, nonStagedBy, uncertain, stagedFor } = detailed
      const ownUncertain = this.uncertainSources.get(skippedPath) === source.size
      const stagedNonUncertain = stagedFor !== undefined && !uncertain
      const stagedGuardClearable = stagedNonUncertain && !ownUncertain && nonStagedBy.every(w => w === this.mark)
      const effectiveBy = stagedNonUncertain ? nonStagedBy : by
      const hasForeignActive = (stagedNonUncertain ? nonStagedBy : activeBy).some(w => w !== this.mark)
      const hasCertainActive = (stagedNonUncertain ? nonStagedBy : nonUncertainBy).some(w => w !== this.mark)
      if (!stagedGuardClearable && (ownUncertain || effectiveBy.some(w => w !== this.mark))) {
        const msg =
          !hasCertainActive && (ownUncertain || uncertain)
            ? `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`
            : `a replay of ${name} sent rows and has recorded no outcome; if that session stopped, /bqaa-backfill force replays the file afresh`
        recordHeld(skippedPath, msg, hasForeignActive, hasCertainActive)
      }
      return 'handled'
    } finally {
      if (!startedAtExit) this.pendingCleanupWakeups.delete(wakeAbort)
    }
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
  private async readJournalSlot(
    source: Source,
    recordKey: string,
    recordFiles: readonly string[],
    v2: boolean,
  ): Promise<{ found: Found | undefined; torn: boolean; rawStored?: Partial<Journal> }> {
    // A record of another size (or another verified content digest) is not
    // this file's (`size` -1: any size).
    const fits = (journal: Partial<Journal> | undefined): journal is Partial<Journal> =>
      journal !== undefined &&
      (source.size < 0 || journal.size === source.size) &&
      (source.fnv1a === undefined ||
        (source.strictFnv1a
          ? typeof journal.sourceFnv1a === 'string' && journal.sourceFnv1a !== '' && journal.sourceFnv1a === source.fnv1a
          : typeof journal.sourceFnv1a !== 'string' || journal.sourceFnv1a === '' || journal.sourceFnv1a === source.fnv1a))
    const rawRead = await this.io.storeGet(recordKey)
    const read = isClearedSlot(rawRead) ? undefined : (rawRead as Partial<Journal>)
    const stored = fits(read) ? read : undefined
    let filed: Partial<Journal> | undefined
    let slotTorn = false
    for (const file of recordFiles) {
      if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
      const text = await this.io.readFile(file)
      // There but empty beside a file that still holds its rows: a write cut
      // off before its first byte (a record let go is emptied only with its
      // file, or before the file is replayed afresh).
      const kept = text === '' && this.io.exists !== undefined && source.live ? 'torn' : journalOf(text)
      if (kept === 'torn') slotTorn = true
      else if (fits(kept) && (filed === undefined || seqOf(kept) > seqOf(filed))) filed = kept
    }
    if (stored === undefined) {
      return { found: filed !== undefined ? { journal: filed, filed: true, ...(v2 ? { v2: true } : {}) } : undefined, torn: slotTorn, ...(read !== undefined ? { rawStored: read } : {}) }
    }
    if (filed === undefined) return { found: { journal: stored, filed: slotTorn, ...(v2 ? { v2: true } : {}) }, torn: slotTorn, rawStored: read }
    // Two outcomes: the file's was written when the store did not take it.
    // On versioned v2 slots, a single rewrite can expand `files` across Phase 3
    // adoption while recovering from a store quota fallback, so prefer the
    // higher-seq checkpoint when both records share `fates`.
    if (!Array.isArray(stored.files) || !Array.isArray(filed.files) || !sameNames(stored.files, filed.files)) {
      const winner = v2 && stored.fates === filed.fates && seqOf(stored) > seqOf(filed) ? stored : filed
      return { found: { journal: winner, filed: true, ...(v2 ? { v2: true } : {}) }, torn: slotTorn, rawStored: read }
    }
    const a = writtenOf(stored)
    const b = writtenOf(filed)
    const newest = seqOf(filed) >= seqOf(stored) ? filed : stored
    return {
      found: {
        journal: { ...newest, written: a.map((w, i) => w || b[i] === true), seq: Math.max(seqOf(stored), seqOf(filed)), retired: stored.retired === true || filed.retired === true },
        filed: true,
        ...(v2 ? { v2: true } : {}),
      },
      torn: slotTorn,
      rawStored: read,
    }
  }

  // Combines v1 and v2 journal slots without comparing independent `seq`
  // counters across slots. Because a single Rewrite never writes both slots,
  // two fitting records that disagree on size, files, or fates always come
  // from distinct rewrites and are reported as `'conflict'`.
  private static pickJournalSlots(
    v1: { found: Found | undefined; torn: boolean },
    v2: { found: Found | undefined; torn: boolean },
  ): Found | 'conflict' | undefined {
    if (v1.found === undefined) return v2.found
    if (v2.found === undefined) return v1.found
    const a = v1.found.journal
    const b = v2.found.journal
    const aFiles = Array.isArray(a.files) ? a.files : []
    const bFiles = Array.isArray(b.files) ? b.files : []
    if (a.size === b.size && a.fates === b.fates) {
      if (sameNames(aFiles, bFiles)) {
        const aw = writtenOf(a)
        const bw = writtenOf(b)
        const base = a.retired === true && b.retired !== true && seqOf(a) > seqOf(b) ? a : b
        return {
          journal: {
            ...base,
            written: aw.map((w, i) => w || bw[i] === true),
            seq: Math.max(seqOf(a), seqOf(b)),
            retired: a.retired === true || b.retired === true,
          },
          filed: v1.found.filed || v2.found.filed,
          v2: true,
        }
      }
      // `markDone`'s retired v1 companion shadow (`outcome-`/`outcome-b-`) can
      // carry earlier-generation children beyond v2's `files` with a strictly
      // higher `seq`. Merge cleanly instead of reporting `'conflict'` on restart.
      if (
        a.retired === true &&
        v1.found.filed &&
        seqOf(a) > seqOf(b) &&
        bFiles.every((bf, i) => typeof bf === 'string' && typeof aFiles[i] === 'string' && baseOf(aFiles[i] as string) === baseOf(bf))
      ) {
        return {
          journal: {
            ...a,
            files: aFiles,
            written: aFiles.map(() => true),
            seq: Math.max(seqOf(a), seqOf(b)),
            retired: true,
          },
          filed: true,
          v2: true,
        }
      }
    }
    // If this v2 checkpoint explicitly recorded that it superseded this exact
    // stale unretired v1 snapshot (which could neither be deleted nor
    // neutralized in place), resume v2 cleanly across session restarts instead
    // of reporting `'conflict'`, while leaving genuine concurrent conflicts
    // untouched.
    if (matchesSupersededV1(a, normalizeSupersededV1(b.supersedesV1))) {
      return v2.found
    }
    return 'conflict'
  }

  private async readJournal(
    source: Source,
    slots?: { slotsRead: boolean; v1Stored?: Partial<Journal>; v2Stored?: Partial<Journal> },
  ): Promise<Found | Pending | 'torn' | 'conflict' | undefined> {
    // In files of their own, each writer's, which no write of a record goes
    // to: any whole one stands, whatever another writer's late write does to
    // its own. One cut off part-way, with no other whole record, is cut off
    // too: the file's outcome may have been done, and none of it goes but by
    // /bqaa-backfill force.
    const doneIn = (entries: readonly { name: string; kind?: string; size: number }[]): Promise<{ done: Partial<Journal> | undefined; torn: boolean }> => this.doneMarks(source, entries)
    let entries = await this.io.listDir(source.dir)
    const first = await doneIn(entries)
    if (first.done !== undefined) return { journal: first.done, filed: true }
    const doneTorn = first.torn
    const [v1, v2] = await Promise.all([
      this.readJournalSlot(source, source.recordKey, source.recordFiles, false),
      this.readJournalSlot(source, source.v2RecordKey, source.v2RecordFiles, true),
    ])
    if (slots !== undefined) {
      slots.slotsRead = true
      slots.v1Stored = v1.rawStored
      slots.v2Stored = v2.rawStored
    }
    const picked = Writer.pickJournalSlots(v1, v2)
    if (picked !== undefined) return picked
    // No record whole in the store or beside the file: a writer may have
    // finished the file while those were read, and it writes its done mark
    // before it lets the store's record go, and before it lets its mark
    // go. So the marks are read, and only then the done marks listed
    // again, before none is taken for an answer.
    entries = await this.io.listDir(source.dir)
    const {
      by,
      activeBy,
      nonUncertainBy,
      nonStagedBy,
      uncertain,
      stagedFor,
      stagedForSize,
      childGuard,
      replaces,
      attempts,
      backup,
      backupForSize,
      backupForFnv1a,
      backupFnv1a,
    } = await this.marksDetailed(source, entries)
    const again = await doneIn(await this.io.listDir(source.dir))
    if (again.done !== undefined) return { journal: again.done, filed: true }
    if (v1.torn || v2.torn || doneTorn || again.torn) return 'torn'
    return by.length === 0
      ? undefined
      : {
          pending: by,
          ...(activeBy.length > 0 ? { activeBy } : {}),
          ...(nonUncertainBy.length > 0 ? { nonUncertainBy } : {}),
          ...(nonStagedBy.length > 0 ? { nonStagedBy } : {}),
          ...(uncertain ? { uncertain: true } : {}),
          ...(stagedFor !== undefined ? { stagedFor } : {}),
          ...(stagedForSize !== undefined ? { stagedForSize } : {}),
          ...(childGuard ? { childGuard: true } : {}),
          ...(replaces !== undefined ? { replaces } : {}),
          ...(attempts !== undefined && attempts.length > 0 ? { attempts } : {}),
          ...(backup ? { backup: true } : {}),
          ...(backupForSize !== undefined ? { backupForSize } : {}),
          ...(backupForFnv1a !== undefined ? { backupForFnv1a } : {}),
          ...(backupFnv1a !== undefined ? { backupFnv1a } : {}),
        }
  }

  // All whole retired done marks for `source` from `entries` (sorted by mark
  // filename so tie-breakers stay deterministic across writers), plus whether
  // any mark was cut off part-way.
  private async doneMarksAll(source: Source, entries: readonly { name: string; kind?: string; size: number }[]): Promise<{ records: Partial<Journal>[]; torn: boolean }> {
    const fits = (journal: Partial<Journal> | undefined): journal is Partial<Journal> =>
      journal !== undefined &&
      (source.size < 0 || journal.size === source.size) &&
      (source.fnv1a === undefined ||
        (source.strictFnv1a
          ? typeof journal.sourceFnv1a === 'string' && journal.sourceFnv1a !== '' && journal.sourceFnv1a === source.fnv1a
          : typeof journal.sourceFnv1a !== 'string' || journal.sourceFnv1a === '' || journal.sourceFnv1a === source.fnv1a))
    let torn = false
    const records: Partial<Journal>[] = []
    const marks = entries.filter(entry => (entry.kind === undefined || entry.kind === 'file') && entry.size > 0 && doneMarkerOf(entry.name, source.name) !== null)
    if (marks.length === 0) return { records, torn }
    const ownSending = source.live && source.size > 0 && !this.rewrites.has(joinPath(source.dir, source.name))
    for (const entry of marks.sort((x, y) => (x.name < y.name ? -1 : x.name > y.name ? 1 : 0))) {
      const kept = journalOf(await this.io.readFile(joinPath(source.dir, entry.name)))
      if (kept === 'torn') {
        if (!(ownSending && doneMarkerOf(entry.name, source.name) === this.mark)) torn = true
      } else if (fits(kept) && kept.retired === true) records.push(kept)
    }
    return { records, torn }
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
  private doneMarks(source: Source, entries: readonly { name: string; kind?: string; size: number }[]): Promise<{ done: Partial<Journal> | undefined; torn: boolean }> {
    const marks = entries.filter(entry => (entry.kind === undefined || entry.kind === 'file') && entry.size > 0 && doneMarkerOf(entry.name, source.name) !== null)
    if (marks.length === 0) return Promise.resolve({ done: undefined, torn: false })
    const hasDigest = (j: Partial<Journal>): boolean => typeof j.sourceFnv1a === 'string' && j.sourceFnv1a !== ''
    return this.doneMarksAll(source, marks).then(({ records, torn }) => {
      let done: Partial<Journal> | undefined
      for (const kept of records) {
        if (done === undefined) {
          done = kept
          continue
        }
        const ks = seqOf(kept)
        const ds = seqOf(done)
        // Prefer a same-seq `sourceFnv1a`-tagged mark over a digest-less 0.1.3
        // mark regardless of filename sort order so equal-size digest checks
        // never skip `strictFnv1a` validation.
        if (ks > ds || (ks === ds && (hasDigest(kept) || !hasDigest(done)))) done = kept
      }
      return { done, torn }
    })
  }

  // Matches retired done marks against an expected content digest without
  // unmasking an older or same-seq digest-less 0.1.3 mark superseded by a
  // newer 0.1.4 `sourceFnv1a` mark.
  private static matchesDoneMarks(records: readonly Partial<Journal>[], digest?: string): boolean {
    if (records.length === 0) return false
    if (digest === undefined || digest === '') return true
    let maxExplicitMismatchSeq = -1
    let maxDigestLessSeq = -1
    for (const r of records) {
      const d = typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '' ? r.sourceFnv1a : undefined
      if (d === digest) return true
      const s = seqOf(r)
      if (d !== undefined) {
        if (s > maxExplicitMismatchSeq) maxExplicitMismatchSeq = s
      } else if (s > maxDigestLessSeq) {
        maxDigestLessSeq = s
      }
    }
    return maxDigestLessSeq > maxExplicitMismatchSeq
  }

  private async readLiveJournal(
    path: string,
    source: Source,
    held?: Rewrite,
    prefetched?: Found | Pending | 'torn' | 'conflict',
  ): Promise<{
    found: Found | Pending | 'torn' | 'conflict' | undefined
    liveDigest?: string
    strict?: boolean
    staleHeld?: boolean
  }> {
    const found = prefetched !== undefined ? prefetched : await this.readJournal(source)
    const needsDigestCheck =
      found === 'conflict' ||
      (found !== undefined &&
        found !== 'torn' &&
        !('pending' in found) &&
        typeof found.journal.sourceFnv1a === 'string' &&
        found.journal.sourceFnv1a !== '')
    if (!needsDigestCheck || source.size <= 0 || source.size > HOST_FILE_BYTES) {
      return { found }
    }
    const rawCheck = await this.io.readFile(path)
    if (rawCheck === '' || utf8Length(rawCheck) !== source.size) {
      return { found }
    }
    const liveDigest = fnv1a(rawCheck)
    if (found === 'conflict') {
      // Query non-strict first so a genuine 0.1.3 (digest-less) vs 0.1.4
      // (`sourceFnv1a === liveDigest`) split-brain conflict without
      // `supersedesV1` stays `'conflict'` instead of being silently overridden.
      const nonStrictFound = await this.readJournal({ ...source, fnv1a: liveDigest, strictFnv1a: false })
      const allMismatched =
        nonStrictFound === undefined || (typeof nonStrictFound === 'object' && 'pending' in nonStrictFound)
      const soleExactMatch =
        typeof nonStrictFound === 'object' &&
        !('pending' in nonStrictFound) &&
        nonStrictFound.journal.sourceFnv1a === liveDigest
      if (allMismatched || soleExactMatch) {
        return {
          found: nonStrictFound,
          liveDigest,
          strict: true,
          staleHeld: held !== undefined && held.sourceFnv1a !== undefined && held.sourceFnv1a !== liveDigest,
        }
      }
      return { found, liveDigest }
    }
    if (liveDigest !== (found as Found).journal.sourceFnv1a) {
      // Re-query strictly on `liveDigest` so filtering out a newer generation's
      // `sourceFnv1a` record never unmasks an older superseded digest-less 0.1.3 record.
      const strictFound = await this.readJournal({ ...source, fnv1a: liveDigest, strictFnv1a: true })
      return {
        found: strictFound,
        liveDigest,
        strict: true,
        staleHeld: held !== undefined && held.sourceFnv1a !== undefined && held.sourceFnv1a !== liveDigest,
      }
    }
    return { found, liveDigest }
  }

  private async hasMatchingDoneMark(
    source: Source,
    entries: readonly { name: string; kind?: string; size: number }[],
    digest?: string,
  ): Promise<{ matched: boolean; torn: boolean }> {
    const { records, torn } = await this.doneMarksAll({ ...source, fnv1a: undefined, strictFnv1a: undefined }, entries)
    if (torn) return { matched: false, torn: true }
    const nonSentinels = records.filter(r => !isEmptiedSentinel(r))
    return { matched: Writer.matchesDoneMarks(nonSentinels, digest), torn: false }
  }

  private async readJournalSlotAll(
    recordKey: string,
    recordFiles: readonly string[],
  ): Promise<{ stored: Partial<Journal> | undefined; filed: Partial<Journal>[]; tornSizes: (number | null)[] }> {
    const rawRead = await this.io.storeGet(recordKey)
    const read = isClearedSlot(rawRead) ? undefined : (rawRead as Partial<Journal>)
    const filed: Partial<Journal>[] = []
    const tornSizes: (number | null)[] = []
    for (const file of recordFiles) {
      if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
      const text = await this.io.readFile(file)
      const kept = journalOf(text)
      if (kept === 'torn') {
        const matched = /(?<!\\)"size"\s*:\s*(\d+)\s*[,}]/.exec(text)
        tornSizes.push(matched ? Number(matched[1]) : null)
      } else if (kept !== undefined) {
        filed.push(kept)
      }
    }
    return { stored: read, filed, tornSizes }
  }

  // What becomes of the file `path`, which a replay of `parent` wrote (the
  // parent `size` long, 0 once emptied or gone): `go` once the parent's
  // record has it as written, or names other files (another replay's
  // outcome), or is gone; `wait` while the parent still holds its rows and
  // its record may yet write the file again, or while that record cannot be
  // read, is cut off, conflicts, or is yet to be written (a replay's mark).
  // Before authorizing `drop`, all parent-size generations are inspected so
  // records from different parent sizes are never compared by `seq`: any
  // distinct-size generation that names the child yields `go`, a single
  // unambiguous generation with no unexplained torn evidence that does not
  // name it yields `drop`, and conflicting or incomplete evidence yields
  // `ambiguous` so the child is preserved rather than truncated.
  private async parentFate(dir: string, parent: string, path: string, size: number, c: Config): Promise<'go' | 'wait' | 'wait-pending' | 'drop' | 'ambiguous'> {
    const parentName = baseOf(parent)
    const targetName = baseOf(path)
    const namesTarget = (journal: Partial<Journal>): boolean =>
      (Array.isArray(journal.files) ? journal.files : []).some(entry => entry === path || baseOf(entry) === targetName)
    if (size > 0) {
      let found: Found | Pending | 'torn' | 'conflict' | undefined
      const liveParentSource = sourceFor(dir, parentName, size, c)
      try {
        found = await this.readJournal(liveParentSource)
        const alreadyNamesTarget =
          found !== undefined &&
          found !== 'torn' &&
          found !== 'conflict' &&
          !('pending' in found) &&
          (Array.isArray(found.journal.files) ? found.journal.files : []).some(entry => entry === path || baseOf(entry) === targetName) &&
          normalizeReconciled(found.journal.reconciled).find(entry => entry.name === targetName) === undefined
        const needsParentDigestCheck =
          found === 'conflict' ||
          (!alreadyNamesTarget &&
            found !== undefined &&
            found !== 'torn' &&
            !('pending' in found) &&
            typeof found.journal.sourceFnv1a === 'string' &&
            found.journal.sourceFnv1a !== '')
        if (needsParentDigestCheck) {
          found = (await this.readLiveJournal(joinPath(dir, parentName), liveParentSource, undefined, found)).found
        }
      } catch {
        return 'wait'
      }
      if (found === 'torn' || found === 'conflict' || (found !== undefined && 'pending' in found)) return 'wait'
      if (found === undefined) return 'go'
      const reconciledEntry = normalizeReconciled(found.journal.reconciled).find(entry => entry.name === targetName)
      const at = (Array.isArray(found.journal.files) ? found.journal.files : []).findIndex(entry => entry === path || baseOf(entry) === targetName)
      if (found.journal.retired === true) {
        // Retired markers keep reconciled basenames in `files` so 0.1.3 never
        // drops a repopulated child; fall through to the multi-generation
        // digest check whenever `targetName` is either reconciled or unnamed.
        if (at !== -1 && reconciledEntry === undefined) return 'go'
      } else {
        if (at !== -1) return writtenOf(found.journal)[at] === true ? 'go' : 'wait'
        if (normalizeStaged(found.journal.stagedFiles).includes(targetName)) return 'wait'
        if (reconciledEntry !== undefined) {
          if (reconciledEntry.fnv1a === '') return 'wait'
          try {
            const curRaw = await this.io.readFile(path)
            if (curRaw === '' || (utf8Length(curRaw) === reconciledEntry.size && fnv1a(curRaw) === reconciledEntry.fnv1a)) return 'wait'
          } catch {
            return 'wait'
          }
        }
        const pendingEntry = normalizeReconciled(found.journal.pendingReconcile, true).find(entry => entry.name === targetName)
        if (pendingEntry !== undefined) {
          if (pendingEntry.fnv1a === '') return 'wait-pending'
          try {
            const curRaw = await this.io.readFile(path)
            if (curRaw === '' || (utf8Length(curRaw) === pendingEntry.size && fnv1a(curRaw) === pendingEntry.fnv1a)) return 'wait-pending'
          } catch {
            return 'wait'
          }
        }
        return 'go'
      }
      // Never authorize `drop` from a single-size lookup alone: fall through to
      // the multi-generation inspection below so another parent-size generation
      // that owns `path` is not overlooked.
    }
    const source = { ...sourceFor(dir, parentName, -1, c), live: false }
    let doneAll: { records: Partial<Journal>[]; torn: boolean }
    let v1: { stored: Partial<Journal> | undefined; filed: Partial<Journal>[]; tornSizes: (number | null)[] }
    let v2: { stored: Partial<Journal> | undefined; filed: Partial<Journal>[]; tornSizes: (number | null)[] }
    try {
      const entries = await this.io.listDir(dir)
      doneAll = await this.doneMarksAll(source, entries)
      ;[v1, v2] = await Promise.all([
        this.readJournalSlotAll(source.recordKey, source.recordFiles),
        this.readJournalSlotAll(source.v2RecordKey, source.v2RecordFiles),
      ])
    } catch {
      return 'wait'
    }
    type RawSizeGroup = {
      doneRecords: Partial<Journal>[]
      v1Stored?: Partial<Journal>
      v1Filed: Partial<Journal>[]
      v2Stored?: Partial<Journal>
      v2Filed: Partial<Journal>[]
    }
    const bySize = new Map<number, RawSizeGroup>()
    const rawBucketFor = (journal: Partial<Journal>): RawSizeGroup => {
      const sz = typeof journal.size === 'number' ? journal.size : -1
      let bucket = bySize.get(sz)
      if (!bucket) {
        bucket = { doneRecords: [], v1Filed: [], v2Filed: [] }
        bySize.set(sz, bucket)
      }
      return bucket
    }
    for (const d of doneAll.records) {
      if (!isEmptiedSentinel(d)) rawBucketFor(d).doneRecords.push(d)
    }
    if (v1.stored !== undefined) rawBucketFor(v1.stored).v1Stored = v1.stored
    for (const f of v1.filed) rawBucketFor(f).v1Filed.push(f)
    if (v2.stored !== undefined) rawBucketFor(v2.stored).v2Stored = v2.stored
    for (const f of v2.filed) rawBucketFor(f).v2Filed.push(f)
    if (bySize.size === 0) return 'go'
    const digestOf = (j: Partial<Journal> | undefined): string | undefined =>
      j !== undefined && typeof j.sourceFnv1a === 'string' && j.sourceFnv1a !== '' ? j.sourceFnv1a : undefined
    const mergeSameSlot = (a: Partial<Journal>, b: Partial<Journal>, allowV2Expand = false): Partial<Journal> | 'conflict' => {
      const aFiles = Array.isArray(a.files) ? a.files : []
      const bFiles = Array.isArray(b.files) ? b.files : []
      if (a.fates !== b.fates) return 'conflict'
      if (!sameNames(aFiles, bFiles)) {
        if (!allowV2Expand) return 'conflict'
        if (seqOf(b) > seqOf(a)) return b
        if (seqOf(a) > seqOf(b)) return a
        return 'conflict'
      }
      const aw = writtenOf(a)
      const bw = writtenOf(b)
      const base = seqOf(b) >= seqOf(a) ? b : a
      return {
        ...base,
        written: aw.map((w, i) => w || bw[i] === true),
        seq: Math.max(seqOf(a), seqOf(b)),
        retired: a.retired === true || b.retired === true,
      }
    }
    const mergeSlot = (
      stored: Partial<Journal> | undefined,
      filed: Partial<Journal> | undefined,
      conflict: boolean | undefined,
      allowV2Expand = false,
    ): Partial<Journal> | 'conflict' | undefined => {
      if (conflict) return 'conflict'
      if (stored === undefined) return filed
      if (filed === undefined) return stored
      return mergeSameSlot(stored, filed, allowV2Expand)
    }
    const mergeDoneRecords = (winner: Partial<Journal>, other: Partial<Journal>): Partial<Journal> => {
      const winnerFiles = Array.isArray(winner.files) ? winner.files : []
      const otherFiles = Array.isArray(other.files) ? other.files : []
      const seenFiles = new Set(winnerFiles.filter((f): f is string => typeof f === 'string' && f !== '').map(baseOf))
      const mergedFiles = [...winnerFiles]
      for (const f of otherFiles) {
        if (typeof f === 'string' && f !== '' && !seenFiles.has(baseOf(f))) {
          seenFiles.add(baseOf(f))
          mergedFiles.push(f)
        }
      }
      const winnerRec = normalizeReconciled(winner.reconciled)
      const otherRec = normalizeReconciled(other.reconciled)
      const seenRec = new Set(winnerRec.map(r => r.name))
      const mergedRec = [...winnerRec]
      for (const r of otherRec) {
        if (!seenRec.has(r.name)) {
          seenRec.add(r.name)
          mergedRec.push(r)
        }
      }
      return {
        ...winner,
        files: mergedFiles,
        ...(mergedRec.length > 0 ? { reconciled: mergedRec } : {}),
      }
    }
    const resolved: { journal: Partial<Journal>; canAuthorizeDrop: boolean }[] = []
    let hasConflict = false
    const resolveSubGroup = (sub: RawSizeGroup, forceNoDrop = false): void => {
      let done: Partial<Journal> | undefined
      let doneConflict = false
      for (const d of sub.doneRecords) {
        if (done === undefined) {
          done = d
          continue
        }
        const ds = seqOf(d)
        const bs = seqOf(done)
        if (ds > bs) {
          done = d
          doneConflict = false
        } else if (ds === bs) {
          const dHas = digestOf(d) !== undefined
          const bHas = digestOf(done) !== undefined
          const crossVersionOrGenerationTie = dHas !== bHas || (dHas && bHas && d.sourceFnv1a !== done.sourceFnv1a)
          const sameFiles = sameNames(
            Array.isArray(d.files) ? d.files : [],
            Array.isArray(done.files) ? done.files : [],
          )
          if (doneConflict || (crossVersionOrGenerationTie && (!sameFiles || d.fates !== done.fates))) {
            // Equal-seq done records from different versions or equal-size
            // generations disagree (or a 3rd equal-seq mark arrives after a
            // conflict): prefer the `sourceFnv1a`-tagged record, union both
            // `files` and `reconciled`, and never authorize `'drop'` on the tie.
            const winner = dHas || !bHas ? d : done
            const other = winner === d ? done : d
            done = mergeDoneRecords(winner, other)
            doneConflict = true
          } else if (dHas || !bHas) {
            done = d
          }
        }
      }
      let v1Filed: Partial<Journal> | undefined
      let v1Conflict = false
      for (const f of sub.v1Filed) {
        if (v1Filed === undefined) v1Filed = f
        else {
          const merged = mergeSameSlot(v1Filed, f)
          if (merged !== 'conflict') {
            v1Filed = merged
          } else if (f.retired === true && v1Filed.retired !== true && seqOf(f) > seqOf(v1Filed)) {
            v1Filed = f
          } else if (v1Filed.retired === true && f.retired !== true && seqOf(v1Filed) > seqOf(f)) {
            // Keep the higher-seq retired v1 shadow over an unretired stale companion.
          } else {
            v1Conflict = true
          }
        }
      }
      let v2Filed: Partial<Journal> | undefined
      let v2Conflict = false
      for (const f of sub.v2Filed) {
        if (v2Filed === undefined) v2Filed = f
        else {
          const merged = mergeSameSlot(v2Filed, f, true)
          if (merged === 'conflict') v2Conflict = true
          else v2Filed = merged
        }
      }
      if (done !== undefined) {
        const v2Merged = mergeSlot(sub.v2Stored, v2Filed, v2Conflict, true)
        let doneJournal = done
        if (v2Merged !== undefined && v2Merged !== 'conflict' && normalizeReconciled(done.reconciled).length === 0) {
          const v2AllWritten = writtenOf(v2Merged).every(Boolean)
          const v2Reconciled = v2AllWritten ? normalizeReconciled(v2Merged.reconciled) : []
          const v2Pending = normalizeReconciled(v2Merged.pendingReconcile, true)
          const v2Adopted = Array.isArray(v2Merged.adopted)
            ? v2Merged.adopted.filter((f): f is string => typeof f === 'string' && f !== '').map(baseOf)
            : []
          const v2NonFresh = new Set([...v2Reconciled.map(r => r.name), ...v2Pending.map(p => p.name), ...v2Adopted])
          const doneCoversV2Leftovers = (): boolean => {
            if (!Array.isArray(doneJournal.files) || doneJournal.files.length === 0) return false
            if (doneJournal.files.some(f => typeof f !== 'string' || f === '' || v2NonFresh.has(baseOf(f)))) return false
            if (
              typeof v2Merged.fates !== 'string' ||
              typeof doneJournal.fates !== 'string' ||
              v2Merged.fates === '' ||
              doneJournal.fates.length !== v2Merged.fates.length
            ) {
              return false
            }
            let hasNonLanded = false
            for (let i = 0; i < v2Merged.fates.length; i += 1) {
              if (v2Merged.fates[i] !== 'L') {
                hasNonLanded = true
                if (doneJournal.fates[i] !== v2Merged.fates[i]) return false
              }
            }
            return hasNonLanded
          }
          const v2Written = writtenOf(v2Merged)
          const v2WrittenLeftovers =
            v2AllWritten && !doneCoversV2Leftovers() && Array.isArray(v2Merged.files)
              ? v2Merged.files
                  .map((f, i) => (v2Written[i] === true && typeof f === 'string' && f !== '' ? baseOf(f) : ''))
                  .filter(n => n !== '' && !v2Adopted.includes(n))
              : []
          const extraFiles = [...v2Adopted, ...v2Pending.map(p => p.name), ...v2WrittenLeftovers].filter(
            n => !(Array.isArray(doneJournal.files) && doneJournal.files.some(f => typeof f === 'string' && baseOf(f) === n)),
          )
          if (v2Reconciled.length > 0 || extraFiles.length > 0) {
            doneJournal = {
              ...doneJournal,
              ...(v2Reconciled.length > 0 ? { reconciled: v2Reconciled } : {}),
              ...(extraFiles.length > 0
                ? { files: [...(Array.isArray(doneJournal.files) ? doneJournal.files : []), ...extraFiles] }
                : {}),
            }
          }
        }
        resolved.push({ journal: doneJournal, canAuthorizeDrop: !forceNoDrop && !doneConflict })
        return
      }
      const v1Merged =
        !v1Conflict &&
        v1Filed !== undefined &&
        v1Filed.retired === true &&
        (sub.v1Stored === undefined || (sub.v1Stored.retired !== true && seqOf(v1Filed) > seqOf(sub.v1Stored)))
          ? v1Filed
          : mergeSlot(sub.v1Stored, v1Filed, v1Conflict)
      const v2Merged = mergeSlot(sub.v2Stored, v2Filed, v2Conflict, true)
      if (v1Merged === 'conflict' || v2Merged === 'conflict') {
        hasConflict = true
      } else if (v1Merged !== undefined && v2Merged !== undefined) {
        const picked = Writer.pickJournalSlots(
          { found: { journal: v1Merged, filed: v1Filed !== undefined }, torn: false },
          { found: { journal: v2Merged, filed: v2Filed !== undefined, v2: true }, torn: false },
        )
        if (picked === 'conflict') hasConflict = true
        else if (picked !== undefined) {
          const v2PendingOrSuperseded =
            normalizeReconciled(v2Merged.pendingReconcile, true).length > 0 ||
            normalizeStaged(v2Merged.stagedFiles).length > 0 ||
            normalizeSupersededV1(v2Merged.supersedesV1) !== undefined
          const canAuthorizeDrop =
            !forceNoDrop &&
            (picked.journal.retired === true ||
              (!v2PendingOrSuperseded &&
                v1Merged.fates === v2Merged.fates &&
                sameNames(Array.isArray(v1Merged.files) ? v1Merged.files : [], Array.isArray(v2Merged.files) ? v2Merged.files : [])))
          resolved.push({ journal: picked.journal, canAuthorizeDrop })
        }
      } else if (v1Merged !== undefined) {
        resolved.push({ journal: v1Merged, canAuthorizeDrop: !forceNoDrop })
      } else if (v2Merged !== undefined) {
        // A v2 split never empties its parent without writing a retired done
        // mark, so a bare v2 journal beside an emptied parent cannot prove
        // ownership over unlisted children from other generations.
        resolved.push({ journal: v2Merged, canAuthorizeDrop: false })
      }
    }
    for (const rawGroup of bySize.values()) {
      const explicitDigests = new Set<string>()
      for (const r of [...rawGroup.doneRecords, rawGroup.v1Stored, ...rawGroup.v1Filed, rawGroup.v2Stored, ...rawGroup.v2Filed]) {
        const dg = digestOf(r)
        if (dg !== undefined) explicitDigests.add(dg)
      }
      if (explicitDigests.size <= 1) {
        resolveSubGroup(rawGroup)
        continue
      }
      // Multiple distinct equal-size 0.1.4 generations (`sourceFnv1a`) coexist
      // at the same byte size: resolve each digest generation separately so an
      // older retired generation's done mark never shadows a newer equal-size
      // generation's `v1`/`v2` or `done` record and drops its live child.
      let maxExplicitDoneSeq = -1
      for (const d of rawGroup.doneRecords) {
        if (digestOf(d) !== undefined && seqOf(d) > maxExplicitDoneSeq) maxExplicitDoneSeq = seqOf(d)
      }
      for (const dg of explicitDigests) {
        resolveSubGroup({
          doneRecords: rawGroup.doneRecords.filter(r => digestOf(r) === dg),
          v1Stored: digestOf(rawGroup.v1Stored) === dg ? rawGroup.v1Stored : undefined,
          v1Filed: rawGroup.v1Filed.filter(r => digestOf(r) === dg),
          v2Stored: digestOf(rawGroup.v2Stored) === dg ? rawGroup.v2Stored : undefined,
          v2Filed: rawGroup.v2Filed.filter(r => digestOf(r) === dg),
        })
      }
      const digestLessDone = rawGroup.doneRecords.filter(r => digestOf(r) === undefined && seqOf(r) >= maxExplicitDoneSeq)
      if (digestLessDone.length > 0) {
        resolveSubGroup({ doneRecords: digestLessDone, v1Filed: [], v2Filed: [] }, true)
      }
    }
    if (bySize.size === 1 && resolved.length <= 1) {
      if (hasConflict || resolved.length === 0) return 'ambiguous'
      const only = resolved[0]!
      const unexplainedTorn = [...v1.tornSizes, ...v2.tornSizes].some(
        sz => sz === null || sz !== only.journal.size || only.journal.retired !== true,
      )
      // Check reconciled digests BEFORE `namesTarget` because retired markers
      // also keep reconciled basenames in `files` so 0.1.3 never drops a child
      // repopulated after retirement. An unchanged reconciled child drops; a
      // repopulated child (size or FNV-1a mismatch) proceeds (`go`).
      const reconciledEntry = normalizeReconciled(only.journal.reconciled).find(entry => entry.name === targetName)
      if (reconciledEntry !== undefined) {
        try {
          const curRaw = await this.io.readFile(path)
          if (curRaw !== '' && (utf8Length(curRaw) !== reconciledEntry.size || fnv1a(curRaw) !== reconciledEntry.fnv1a)) {
            return 'go'
          }
        } catch {
          return 'wait'
        }
        if (doneAll.torn || unexplainedTorn || !only.canAuthorizeDrop) return 'ambiguous'
        return only.journal.retired === true || writtenOf(only.journal).every(Boolean) ? 'drop' : 'go'
      }
      if (namesTarget(only.journal)) return 'go'
      if (doneAll.torn || unexplainedTorn || !only.canAuthorizeDrop) return 'ambiguous'
      // Not named: a late copy of another session's outcome, where the record
      // is the mark the parent was done, or a whole one of an emptied parent
      // with every file it names written (its done mark lost, or never written:
      // its outcome has every row of the parent).
      return only.journal.retired === true || writtenOf(only.journal).every(Boolean) ? 'drop' : 'go'
    }
    // Multiple distinct parent-size generations coexist (e.g. a legacy 0.1.3
    // rewrite alongside a stale v2 slot): never drop a child genuinely named by
    // any resolved generation (ignoring reconciled-only basename entries whose
    // digest still matches the live file). If an unambiguous retired generation
    // explicitly recorded `targetName` in `reconciled` with a `{ size, fnv1a }`
    // digest that still matches the live file, authorize `'drop'` so reconciled
    // rows are not re-sent; preserve the child as `'ambiguous'` otherwise.
    let cachedRaw: string | undefined
    const readCur = async (): Promise<string | null> => {
      if (cachedRaw !== undefined) return cachedRaw
      try {
        cachedRaw = await this.io.readFile(path)
        return cachedRaw
      } catch {
        return null
      }
    }
    const matchesReconciledIn = async (journal: Partial<Journal>): Promise<'match' | 'mismatch' | 'none' | 'error'> => {
      const entry = normalizeReconciled(journal.reconciled).find(e => e.name === targetName)
      if (entry === undefined) return 'none'
      const curRaw = await readCur()
      if (curRaw === null) return 'error'
      return curRaw === '' || (utf8Length(curRaw) === entry.size && fnv1a(curRaw) === entry.fnv1a) ? 'match' : 'mismatch'
    }
    let hasMatchingRetiredReconciled = false
    for (const r of resolved) {
      const recState = await matchesReconciledIn(r.journal)
      if (recState === 'error') return 'wait'
      if (recState === 'match') {
        if (r.canAuthorizeDrop && r.journal.retired === true) hasMatchingRetiredReconciled = true
        continue
      }
      if (namesTarget(r.journal)) return 'go'
    }
    if (!hasConflict && !doneAll.torn && hasMatchingRetiredReconciled) {
      return 'drop'
    }
    return 'ambiguous'
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

  private async safeStoreDeleteSameSize(key: string, expectedSize: number, dir: string): Promise<void> {
    try {
      const rawStored = await this.io.storeGet(key)
      if (rawStored === undefined) return
      if (isClearedSlot(rawStored)) {
        await this.io.storeDelete(key)
        return
      }
      const stored = rawStored as Partial<Journal>
      if (expectedSize >= 0 && stored.size !== expectedSize) {
        const childNames = journalChildNames(stored)
        if (childNames.length > 0) {
          const entries = await this.io.listDir(dir)
          const liveNames = new Set(entries.filter(e => (e.kind === undefined || e.kind === 'file') && e.size > 0).map(e => e.name))
          if (childNames.some(n => liveNames.has(n))) return
        }
      }
      await this.io.storeDelete(key)
    } catch {
      // Best-effort safe delete.
    }
  }

  // Empties the files beside `source` that held its record: none is read
  // once the source is empty, or once the record is let go. Only truncates
  // entries that match `source.size` or have no remaining live child on disk.
  private async emptyRecordFiles(source: Source): Promise<void> {
    let liveNames: Set<string> | null = null
    const getLiveNames = async (): Promise<Set<string> | null> => {
      if (liveNames !== null) return liveNames
      try {
        liveNames = new Set((await this.io.listDir(source.dir)).filter(e => (e.kind === undefined || e.kind === 'file') && e.size > 0).map(e => e.name))
        return liveNames
      } catch {
        return null
      }
    }
    for (const file of [...source.recordFiles, ...source.v2RecordFiles]) {
      try {
        if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
        const text = await this.io.readFile(file)
        if (text === '') continue
        const kept = journalOf(text)
        if (kept === 'torn') continue
        if (kept !== undefined && source.size >= 0 && kept.size !== source.size) {
          const childNames = journalChildNames(kept)
          if (childNames.length > 0) {
            const live = await getLiveNames()
            if (live === null || childNames.some(n => live.has(n))) continue
          }
        }
        await this.io.writeFile(file, '')
      } catch {
        // Left as it is.
      }
    }
  }

  private async readParentInfo(
    dir: string,
    name: string,
    size: number,
    parentIndex?: RunParentIndex,
  ): Promise<{ state: 'ok'; complete: boolean; parent: string | null } | { state: 'empty' } | { state: 'oversized' } | { state: 'error' }> {
    if (size <= 0) return { state: 'empty' }
    if (size > HOST_FILE_BYTES) return { state: 'oversized' }
    const cached = parentIndex?.get(name)
    if (cached !== undefined && cached.size === size) {
      return { state: 'ok', complete: true, parent: cached.parent }
    }
    try {
      const raw = await this.io.readFile(joinPath(dir, name))
      if (raw === '') {
        parentIndex?.delete(name)
        return { state: 'empty' }
      }
      const parsed = parseFile(raw)
      const parent = parsed.parent === null ? null : baseOf(parsed.parent)
      if (parsed.complete && utf8Length(raw) === size) {
        parentIndex?.set(name, { size, parent })
      }
      return { state: 'ok', complete: parsed.complete, parent }
    } catch {
      return { state: 'error' }
    }
  }

  private static sameUnaccounted(journal: Partial<Journal>, superseded: Partial<Journal> | undefined): boolean {
    if (superseded === undefined || journal.retired === true) return false
    return (
      journal.size === superseded.size &&
      seqOf(journal) === seqOf(superseded) &&
      journal.fates === superseded.fates &&
      sameNames(Array.isArray(journal.files) ? journal.files : [], Array.isArray(superseded.files) ? superseded.files : [])
    )
  }

  // What `rewrite` holds, brought up to its record: a file of what is left
  // that the record has as written counts as written, and never goes back to
  // unwritten (another writer may since have replayed and emptied it). False
  // when the record is of another outcome for the file.
  private static catchUp(rewrite: Rewrite, found: Found): boolean {
    const foundDigest = typeof found.journal.sourceFnv1a === 'string' && found.journal.sourceFnv1a !== '' ? found.journal.sourceFnv1a : undefined
    if (rewrite.sourceFnv1a !== undefined && foundDigest !== undefined && rewrite.sourceFnv1a !== foundDigest) return false
    if (
      rewrite.files.some(f => f.text !== null) &&
      rewrite.files.length > 1 &&
      ((found.journal.stampedIds === true && rewrite.stampedIds !== true) ||
        (found.journal.stampedIds === false && rewrite.stampedIds === true))
    ) {
      return false
    }
    const allPaths = [...rewrite.files.map(f => f.path), ...(rewrite.adoptedFiles ?? [])]
    const foundFiles = Array.isArray(found.journal.files) ? found.journal.files : []
    const retiredPrefix =
      found.journal.retired === true &&
      found.journal.size === rewrite.size &&
      found.journal.fates === rewrite.fates &&
      allPaths.every((p, i) => typeof foundFiles[i] === 'string' && baseOf(foundFiles[i] as string) === baseOf(p))
    if (!sameNames(allPaths, foundFiles) && !retiredPrefix) return false
    if (rewrite.sourceFnv1a === undefined && foundDigest !== undefined) rewrite.sourceFnv1a = foundDigest
    const written = writtenOf(found.journal)
    rewrite.files.forEach((file, i) => {
      if (written[i] === true) file.text = null
    })
    const foundReconciled = normalizeReconciled(found.journal.reconciled)
    if (foundReconciled.length > 0) {
      const merged = new Map<string, ReconciledFile>((rewrite.reconciledFiles ?? []).map(r => [r.name, r]))
      for (const entry of foundReconciled) merged.set(entry.name, entry)
      rewrite.reconciledFiles = [...merged.values()]
    }
    const foundPending = normalizeReconciled(found.journal.pendingReconcile, true)
    if (foundPending.length > 0 || rewrite.pendingReconcile !== undefined) {
      const reconciledNames = new Set((rewrite.reconciledFiles ?? []).map(r => r.name))
      const mergedPending = new Map<string, ReconciledFile>((rewrite.pendingReconcile ?? []).filter(r => !reconciledNames.has(r.name)).map(r => [r.name, r]))
      for (const entry of foundPending) {
        if (!reconciledNames.has(entry.name)) mergedPending.set(entry.name, entry)
      }
      rewrite.pendingReconcile = mergedPending.size > 0 ? [...mergedPending.values()] : undefined
    }
    const foundStaged = normalizeStagedRefs(found.journal.stagedFiles)
    if (foundStaged.length > 0 || rewrite.stagedFiles !== undefined) {
      const adoptedNames = new Set(rewrite.adoptedFiles ?? [])
      const mergedStaged = new Map<string, StagedFile>()
      for (const s of rewrite.stagedFiles ?? []) {
        if (!adoptedNames.has(s.name)) mergedStaged.set(s.name, s)
      }
      for (const s of foundStaged) {
        if (adoptedNames.has(s.name)) continue
        const cur = mergedStaged.get(s.name)
        if (cur === undefined || (cur.fnv1a === '' && s.fnv1a !== '')) mergedStaged.set(s.name, s)
      }
      rewrite.stagedFiles = mergedStaged.size > 0 ? [...mergedStaged.values()] : undefined
    }
    const foundBackups = normalizeBackups(found.journal.backupFiles)
    if (foundBackups.length > 0 || rewrite.backupFiles !== undefined) {
      const mergedBackups = new Map<string, BackupFile>((rewrite.backupFiles ?? []).map(b => [b.name, b]))
      for (const b of foundBackups) mergedBackups.set(b.name, b)
      rewrite.backupFiles = mergedBackups.size > 0 ? [...mergedBackups.values()] : undefined
    }
    const foundAttempts = normalizeAttempts(found.journal.attempt, found.journal.attempts)
    if (foundAttempts.length > 0) {
      const mergedAttempts = normalizeAttempts(rewrite.guardAttempt, [...(rewrite.guardAttempts ?? []), ...foundAttempts])
      if (rewrite.guardAttempt === undefined && typeof found.journal.attempt === 'string' && found.journal.attempt !== '') {
        rewrite.guardAttempt = found.journal.attempt
      }
      rewrite.guardAttempts = mergedAttempts
    }
    const foundSupersededV1 = normalizeSupersededV1(found.journal.supersedesV1)
    if (foundSupersededV1 !== undefined) rewrite.supersedesV1 = foundSupersededV1
    if (rewrite.accountedRowKeys === undefined && Array.isArray(found.journal.accountedKeys) && found.journal.accountedKeys.length > 0) {
      rewrite.accountedRowKeys = new Set(found.journal.accountedKeys.filter((k): k is string => typeof k === 'string'))
    }
    const foundDeliverables = normalizeStagedRefs(found.journal.deliverableRefs)
    if (foundDeliverables.length > 0 || rewrite.deliverableRefs !== undefined) {
      const mergedDeliverables = new Map<string, StagedFile>()
      for (const d of rewrite.deliverableRefs ?? []) mergedDeliverables.set(d.name, d)
      for (const d of foundDeliverables) {
        const cur = mergedDeliverables.get(d.name)
        if (cur === undefined) {
          mergedDeliverables.set(d.name, d)
        } else if ((cur.fnv1a === '' && d.fnv1a !== '') || (cur.replaces === undefined && d.replaces !== undefined)) {
          const mergedReplaces = cur.replaces ?? d.replaces
          mergedDeliverables.set(d.name, {
            name: cur.name,
            size: cur.size > 0 ? cur.size : d.size,
            fnv1a: cur.fnv1a !== '' ? cur.fnv1a : d.fnv1a,
            ...(mergedReplaces !== undefined ? { replaces: mergedReplaces } : {}),
          })
        }
      }
      rewrite.deliverableRefs = mergedDeliverables.size > 0 ? [...mergedDeliverables.values()] : undefined
    }
    if (found.journal.stampedIds === true) rewrite.stampedIds = true
    else if (found.journal.stampedIds === false && rewrite.stampedIds === undefined) rewrite.stampedIds = false
    rewrite.filed ||= found.filed
    rewrite.seq = Math.max(rewrite.seq, seqOf(found.journal))
    return true
  }

  // The Rewrite for the file `source.size` long: what this writer holds of
  // it, brought up to its record, or else the record's, with what it has
  // still to write rebuilt from the source. With no record that fits the
  // file, what this writer holds, if anything; a checkpoint whose children
  // recovery cannot account for is kept intact and returns `'unaccounted'`.
  private async resumeRewrite(
    path: string,
    source: Source,
    fileKind: DeadLetterKind,
    held: Rewrite | undefined,
    found: Found | undefined,
    trigger?: BackfillReport['trigger'],
  ): Promise<Rewrite | 'unaccounted' | undefined> {
    const prepareHeldForForce = async (h: Rewrite): Promise<Rewrite | undefined> => {
      if (trigger !== 'force' || found?.journal.retired === true) return h
      h.force = true
      if (h.accountedRowKeys === undefined) {
        const text = await this.io.readFile(path)
        if (text === '') return undefined
        const lines = parseFile(text)
        if (!lines.complete || (source.size >= 0 && utf8Length(text) !== source.size)) return undefined
        if (h.sourceFnv1a !== undefined && fnv1a(text) !== h.sourceFnv1a) return undefined
        h.sourceFnv1a ??= fnv1a(text)
        h.accountedRowKeys = new Set(lines.lines.map((line, i) => parentRowIdentity(line, source.name, i)))
        if (h.fates.length === lines.lines.length) {
          h.leftoverRowKeys = new Set(
            lines.lines.flatMap((line, i) => (h.fates[i] !== 'L' ? [parentRowIdentity(line, source.name, i)] : [])),
          )
        }
      }
      return h
    }
    if (found === undefined) return held !== undefined ? prepareHeldForForce(held) : undefined
    if (held !== undefined && Writer.catchUp(held, found)) return prepareHeldForForce(held)
    const { journal } = found
    const savedSourceFnv1a = typeof journal.sourceFnv1a === 'string' && journal.sourceFnv1a !== '' ? journal.sourceFnv1a : undefined
    const paths = Array.isArray(journal.files) ? journal.files : []
    const written = writtenOf(journal)
    const adoptedList = Array.isArray(journal.adopted)
      ? journal.adopted.filter((x): x is string => typeof x === 'string' && x !== '').map(baseOf)
      : []
    const adoptedCount = adoptedList.length
    const adoptedValid =
      adoptedCount === 0 ||
      (adoptedCount <= paths.length &&
        adoptedList.every((name, j) => {
          const idx = paths.length - adoptedCount + j
          return baseOf(paths[idx] ?? '') === name && written[idx] === true
        }))
    const generatedPaths = adoptedValid ? paths.slice(0, paths.length - adoptedCount) : paths
    const adoptedFiles = adoptedValid && adoptedCount > 0 ? adoptedList : undefined
    const reconciledList = normalizeReconciled(journal.reconciled)
    const reconciledFiles = reconciledList.length > 0 ? reconciledList : undefined
    const pendingList = normalizeReconciled(journal.pendingReconcile, true)
    const pendingReconcile = pendingList.length > 0 ? pendingList : undefined
    const stagedList = normalizeStagedRefs(journal.stagedFiles)
    const stagedFiles = stagedList.length > 0 ? stagedList : undefined
    const backupList = normalizeBackups(journal.backupFiles)
    const backupFiles = backupList.length > 0 ? backupList : undefined
    const deliverableList = normalizeStagedRefs(journal.deliverableRefs)
    const deliverableRefs = deliverableList.length > 0 ? deliverableList : undefined
    const guardAttempt = typeof journal.attempt === 'string' && journal.attempt !== '' ? journal.attempt : undefined
    const guardAttemptsList = normalizeAttempts(journal.attempt, journal.attempts)
    const guardAttempts = guardAttemptsList.length > 0 ? guardAttemptsList : undefined
    const supersedesV1 = normalizeSupersededV1(journal.supersedesV1)
    const savedAccountedKeys =
      Array.isArray(journal.accountedKeys) && journal.accountedKeys.length > 0
        ? new Set(journal.accountedKeys.filter((k): k is string => typeof k === 'string'))
        : undefined
    const outcome: Outcome = {
      fates: typeof journal.fates === 'string' ? journal.fates : '',
      reasons: Array.isArray(journal.reasons) ? journal.reasons : [],
      refusals: Array.isArray(journal.refusals) ? journal.refusals : [],
      ...(Array.isArray(journal.uncertainLines) ? { uncertainLines: journal.uncertainLines } : {}),
    }
    const stampedIds: boolean | undefined = journal.stampedIds === true ? true : journal.stampedIds === false ? false : undefined
    const useV2 =
      found.v2 === true ||
      (Array.isArray(journal.uncertainLines) && journal.uncertainLines.length > 0) ||
      (adoptedFiles !== undefined && adoptedFiles.length > 0) ||
      (reconciledFiles !== undefined && reconciledFiles.length > 0) ||
      (pendingReconcile !== undefined && pendingReconcile.length > 0) ||
      (stagedFiles !== undefined && stagedFiles.length > 0) ||
      (backupFiles !== undefined && backupFiles.length > 0) ||
      supersedesV1 !== undefined
    const resolvedFnv1a = savedSourceFnv1a ?? source.fnv1a
    const rewrite: Rewrite = {
      ...outcome,
      ...source,
      ...(resolvedFnv1a !== undefined ? { fnv1a: resolvedFnv1a, sourceFnv1a: resolvedFnv1a } : {}),
      waits: journal.waits === true || (pendingReconcile !== undefined && pendingReconcile.length > 0) || (stagedFiles !== undefined && stagedFiles.length > 0),
      filed: found.filed,
      seq: seqOf(journal),
      files: generatedPaths.map((p, i) => ({ path: joinPath(source.dir, baseOf(p)), text: null, ...(journal.uncertain?.[i] === true ? { uncertain: true } : {}) })),
      ...(adoptedFiles ? { adoptedFiles } : {}),
      ...(deliverableRefs ? { deliverableRefs } : {}),
      ...(reconciledFiles ? { reconciledFiles } : {}),
      ...(pendingReconcile ? { pendingReconcile } : {}),
      ...(stagedFiles ? { stagedFiles } : {}),
      ...(backupFiles ? { backupFiles } : {}),
      ...(guardAttempt ? { guardAttempt } : {}),
      ...(guardAttempts ? { guardAttempts } : {}),
      ...(supersedesV1 ? { supersedesV1 } : {}),
      ...(savedAccountedKeys ? { accountedRowKeys: savedAccountedKeys } : {}),
      ...(trigger === 'force' && journal.retired !== true ? { force: true } : {}),
      ...(useV2 ? { useV2: true } : {}),
      ...(stampedIds !== undefined ? { stampedIds } : {}),
    }
    const needsPendingReconcile = pendingReconcile !== undefined && pendingReconcile.length > 0
    const needsForceChildAudit = trigger === 'force' && journal.retired !== true
    if (written.every(Boolean) && !needsPendingReconcile && !needsForceChildAudit) {
      if (source.size >= 0) {
        const current = await this.io.readFile(path)
        if (current === '' || utf8Length(current) !== source.size) return undefined
        const curDigest = fnv1a(current)
        if (savedSourceFnv1a !== undefined && curDigest !== savedSourceFnv1a) return undefined
        rewrite.sourceFnv1a ??= curDigest
        rewrite.fnv1a ??= curDigest
      }
      return rewrite
    }
    const text = await this.io.readFile(path)
    // Emptied meanwhile (another writer finished it): nothing to resume, and
    // its record is left as that writer left it.
    if (text === '') return undefined
    const lines = parseFile(text)
    // Not whole, or not as long as it was listed (a write still under way, or
    // one since): the record is kept, and the file waits, as one with no
    // record does, until it reads whole.
    if (!lines.complete || (source.size >= 0 && utf8Length(text) !== source.size)) return undefined
    const liveDigest = fnv1a(text)
    if (savedSourceFnv1a !== undefined && liveDigest !== savedSourceFnv1a) return undefined
    rewrite.sourceFnv1a ??= liveDigest
    rewrite.fnv1a ??= liveDigest
    if (needsPendingReconcile || needsForceChildAudit) {
      rewrite.accountedRowKeys = new Set(lines.lines.map((line, i) => parentRowIdentity(line, source.name, i)))
      if (outcome.fates.length === lines.lines.length) {
        rewrite.leftoverRowKeys = new Set(
          lines.lines.flatMap((line, i) => (outcome.fates[i] !== 'L' ? [parentRowIdentity(line, source.name, i)] : [])),
        )
      }
    }
    if (written.every(Boolean)) return rewrite
    let texts: { kind: DeadLetterKind; text: string; uncertain?: boolean }[] | null = null
    let layoutMismatchOnDisk = false
    if (fitsLines(lines, outcome)) {
      if (stampedIds === true) {
        const stampedRes = leftoverFiles(lines, fileKind, outcome, source.name, true)
        texts = stampedRes.files
        if (stampedRes.stampedIds && stampedRes.files.length > 1) {
          const unstampedRes = leftoverFiles(lines, fileKind, outcome, source.name, false)
          if (!sameLeftoverPartitioning(stampedRes.files, unstampedRes.files)) {
            rewrite.layoutsDiffer = true
            rewrite.useV2 = true
          }
        }
      } else if (stampedIds === false) {
        texts = leftoverFiles(lines, fileKind, outcome, source.name, false).files
      } else {
        const unstampedRes = leftoverFiles(lines, fileKind, outcome, source.name, false)
        const stampedRes = leftoverFiles(lines, fileKind, outcome, source.name, true)
        if (!stampedRes.stampedIds) {
          texts = unstampedRes.files
        } else {
          const differs = !sameLeftoverPartitioning(stampedRes.files, unstampedRes.files)
          let unstampedFits = unstampedRes.files.length === generatedPaths.length
          let stampedFits = stampedRes.files.length === generatedPaths.length
          let eitherFits = !differs && stampedRes.files.length === generatedPaths.length
          // Inspect any already-written leftover parts still on disk to
          // distinguish 0.1.3 unstamped v1 checkpoints from pre-flag 0.1.4
          // stamped v1 checkpoints (and fail closed if a written part matches
          // neither layout). When stamped and unstamped boundaries agree
          // (`!differs`), alternating 0.1.4/0.1.3 writers can write individual
          // parts in either encoding with identical per-part row membership.
          for (let i = 0; i < generatedPaths.length; i += 1) {
            if (written[i] !== true) continue
            const partPath = joinPath(source.dir, baseOf(generatedPaths[i] as string))
            if (this.io.exists !== undefined && !(await this.io.exists(partPath))) continue
            let curText = ''
            try {
              curText = await this.io.readFile(partPath)
            } catch (err) {
              if (isNotFoundError(err)) continue
              throw err
            }
            if (curText === '') continue
            const matchesPart = (expected: string | undefined): boolean =>
              expected !== undefined && (curText === expected || curText === stripParentSeal(expected))
            const matchUnstamped = matchesPart(unstampedRes.files[i]?.text)
            const matchStamped = matchesPart(stampedRes.files[i]?.text)
            if (unstampedFits && !matchUnstamped) unstampedFits = false
            if (stampedFits && !matchStamped) stampedFits = false
            if (eitherFits && !matchUnstamped && !matchStamped) eitherFits = false
          }
          if (!unstampedFits && !stampedFits) {
            if (eitherFits) {
              texts = stampedRes.files
              rewrite.stampedIds = true
            } else {
              layoutMismatchOnDisk = true
              texts = null
            }
          } else if (stampedFits && !unstampedFits) {
            texts = stampedRes.files
            rewrite.stampedIds = true
            if (differs) {
              rewrite.layoutsDiffer = true
              rewrite.useV2 = true
            }
          } else if (unstampedFits && !stampedFits) {
            texts = unstampedRes.files
            rewrite.stampedIds = false
            if (differs) rewrite.layoutsDiffer = true
          } else if (found.v2 === true || savedSourceFnv1a !== undefined) {
            // 0.1.3 never emits `sourceFnv1a` or v2 checkpoints; when both
            // layouts fit the part count and no live written part contradicts
            // stamped packing, preserve pre-flag 0.1.4 stamped boundaries.
            texts = stampedRes.files
            rewrite.stampedIds = true
            if (differs) {
              rewrite.layoutsDiffer = true
              rewrite.useV2 = true
            }
          } else {
            texts = unstampedRes.files
            rewrite.stampedIds = false
            if (differs) rewrite.layoutsDiffer = true
          }
        }
      }
    }
    if (texts === null || !adoptedValid || texts.length !== generatedPaths.length) {
      // Keep the checkpoint whenever its fates still fit the file (so recovery
      // cannot account for extra/adopted children), any named child was already
      // written or is live on disk, or listing fails. Only clear a same-size
      // record whose fates do not fit the file AND that protects no live child.
      let protectsChild = layoutMismatchOnDisk || texts !== null || written.some(Boolean)
      const trackedChildren = journalChildNames(journal)
      if (!protectsChild && trackedChildren.length > 0) {
        try {
          const entries = await this.io.listDir(source.dir)
          const liveNames = new Set(entries.filter(e => (e.kind === undefined || e.kind === 'file') && e.size > 0).map(e => e.name))
          protectsChild = trackedChildren.some(n => liveNames.has(n))
        } catch {
          protectsChild = true
        }
      }
      if (protectsChild) return held ?? 'unaccounted'
      await Promise.all([
        this.safeStoreDeleteSameSize(source.recordKey, source.size, source.dir),
        this.safeStoreDeleteSameSize(source.v2RecordKey, source.size, source.dir),
      ])
      await this.emptyRecordFiles(source)
      return held
    }
    rewrite.files = generatedPaths.map((p, i) => ({
      path: joinPath(source.dir, baseOf(p)),
      text: written[i] === true ? null : (texts[i]?.text ?? ''),
      ...(journal.uncertain?.[i] === true || texts[i]?.uncertain === true ? { uncertain: true } : {}),
    }))
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
  private async writeOut(path: string, rewrite: Rewrite, c: Config, parentIndex?: RunParentIndex, emptiedInRun?: Set<string>): Promise<boolean> {
    rewrite.deferredByLease = undefined
    if (rewrite.fnv1a === undefined && rewrite.sourceFnv1a !== undefined) rewrite.fnv1a = rewrite.sourceFnv1a
    this.rewrites.set(path, rewrite)
    const initialSlots: { slotsRead: boolean; v1Stored?: Partial<Journal>; v2Stored?: Partial<Journal> } = { slotsRead: false }
    const found = Writer.recorded(await this.readJournal(rewrite, initialSlots))
    const normalizeSlot = (val: unknown): Partial<Journal> | 'torn' | undefined => {
      if (isClearedSlot(val)) return undefined
      return typeof val === 'object' && val !== null && !Array.isArray(val) ? (val as Partial<Journal>) : 'torn'
    }
    const cachedStore = new Map<string, Partial<Journal> | 'torn' | undefined>()
    const neutralizedKeys = new Set<string>()
    if (initialSlots.slotsRead) {
      cachedStore.set(rewrite.recordKey, normalizeSlot(initialSlots.v1Stored))
      cachedStore.set(rewrite.v2RecordKey, normalizeSlot(initialSlots.v2Stored))
    }
    const readStoreSlot = async (key: string): Promise<Partial<Journal> | 'torn' | undefined> => {
      if (cachedStore.has(key)) return cachedStore.get(key)
      const val = normalizeSlot(await this.io.storeGet(key))
      cachedStore.set(key, val)
      return val
    }
    const dest = destinationDir(tableRef(c))
    const me = this.sessionId() ?? 'unknown'
    // Finished already (the file left again, by a late copy, say), here or
    // at any check below: the mark that it was done stands as it is, never
    // written again, and only the file is emptied.
    let finished = found !== undefined && found !== 'torn' && found !== 'conflict' && found.journal.retired === true
    let finishedJournal: Partial<Journal> | undefined =
      finished && found !== undefined && found !== 'torn' && found !== 'conflict' ? found.journal : undefined
    let recorded = false
    if (found !== undefined && found !== 'torn' && found !== 'conflict') {
      if (rewrite.force && !finished && Writer.sameUnaccounted(found.journal, rewrite.supersededUnaccounted)) {
        rewrite.seq = Math.max(rewrite.seq, seqOf(found.journal))
      } else if (!Writer.catchUp(rewrite, found)) {
        this.rewrites.delete(path)
        for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
        if (found.journal.retired === true) {
          this.deleteHeldPath(path)
          this.currentScanHeld.delete(path)
        }
        await this.unmark(rewrite)
        return false
      } else {
        // The record has this outcome as this writer has it: no need to write it again.
        const written = writtenOf(found.journal)
        const allWritten = [...rewrite.files.map(file => file.text === null), ...(rewrite.adoptedFiles ?? []).map(() => true)]
        const foundReconciled = normalizeReconciled(found.journal.reconciled)
        const foundPending = normalizeReconciled(found.journal.pendingReconcile, true)
        const foundStaged = normalizeStagedRefs(found.journal.stagedFiles)
        const foundBackups = normalizeBackups(found.journal.backupFiles)
        const foundDeliverables = normalizeStagedRefs(found.journal.deliverableRefs)
        recorded =
          finished ||
          ((!rewrite.useV2 || found.v2 === true) &&
            (rewrite.files.length <= 1 || found.journal.stampedIds === rewrite.stampedIds) &&
            allWritten.length === written.length &&
            allWritten.every((w, i) => w === written[i]) &&
            sameReconciled(foundReconciled, rewrite.reconciledFiles ?? []) &&
            sameReconciled(foundPending, rewrite.pendingReconcile ?? []) &&
            sameStaged(foundStaged, rewrite.stagedFiles ?? []) &&
            sameStaged(foundDeliverables, rewrite.useV2 ? (rewrite.deliverableRefs ?? []) : []) &&
            sameBackups(foundBackups, rewrite.backupFiles ?? []) &&
            coversAllAttempts(found.journal, rewrite.guardAttempts))
      }
    } else if ((await this.io.readFile(path)) === '') {
      this.rewrites.delete(path)
      this.deleteHeldPath(path)
      this.currentScanHeld.delete(path)
      for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
      await this.unmark(rewrite)
      return true
    }
    const dirListing = async (): Promise<ReadonlyMap<string, number> | null> => {
      try {
        const listed = new Map<string, number>()
        for (const entry of await this.io.listDir(rewrite.dir)) {
          if (entry.kind === undefined || entry.kind === 'file') listed.set(entry.name, entry.size)
        }
        return listed
      } catch {
        return null
      }
    }
    const upsertDeliverableRef = (ref: StagedFile | undefined): void => {
      if (ref === undefined || ref.size <= 0) return
      const existing = rewrite.deliverableRefs ?? []
      const idx = existing.findIndex(r => r.name === ref.name)
      const entry: StagedFile = {
        name: ref.name,
        size: ref.size,
        fnv1a: ref.fnv1a,
        ...(ref.replaces !== undefined && ref.replaces !== '' ? { replaces: baseOf(ref.replaces) } : {}),
      }
      if (idx === -1) {
        rewrite.deliverableRefs = [...existing, entry]
      } else {
        const cur = existing[idx]!
        if ((cur.fnv1a === '' && entry.fnv1a !== '') || (cur.replaces === undefined && entry.replaces !== undefined)) {
          const mergedReplaces = cur.replaces ?? entry.replaces
          const next = [...existing]
          next[idx] = {
            name: cur.name,
            size: cur.size > 0 ? cur.size : entry.size,
            fnv1a: cur.fnv1a !== '' ? cur.fnv1a : entry.fnv1a,
            ...(mergedReplaces !== undefined ? { replaces: mergedReplaces } : {}),
          }
          rewrite.deliverableRefs = next
        }
      }
    }
    type ChildClassification =
      | { action: 'skip' }
      | { action: 'adopt'; ref?: StagedFile }
      | { action: 'reconciled'; ref: ReconciledFile }
      | {
          action: 'replace'
          ref: ReconciledFile
          childKind: DeadLetterKind
          markUncertain: boolean
          entries: { line: DeadLetterLine; index: number; refusedReason?: string }[]
          unreadable: string[]
        }
      | { action: 'defer'; ref: ReconciledFile; reason: string }
    const classifyChild = async (
      base: string,
      sz: number,
      fallbackRef?: ReconciledFile,
      currentListing?: ReadonlyMap<string, number> | null,
      forceStandalone = false,
    ): Promise<ChildClassification> => {
      const childPath = joinPath(rewrite.dir, base)
      const fallback: ReconciledFile = fallbackRef ?? { name: base, size: Math.max(0, sz), fnv1a: '' }
      // Check the child's own foreign lease before reading, copying, or reconciling it.
      try {
        const lease = (await this.io.storeGet(`lease:${dest}/${base}`)) as { by?: string; holder?: string; until?: number } | undefined
        if (
          lease &&
          typeof lease.until === 'number' &&
          lease.until > this.io.now() &&
          (lease.by !== me || typeof lease.holder !== 'string' || lease.holder === '' || lease.holder !== this.mark)
        ) {
          return { action: 'defer', ref: fallback, reason: `another session's lease holds ${base}; deferring reconciliation` }
        }
      } catch {
        return { action: 'defer', ref: fallback, reason: `cannot inspect lease for ${base}; deferring reconciliation` }
      }
      // Capture the child's raw bytes BEFORE reading its journal so an
      // equal-size replacement swapped in during `readJournal` is detected
      // even when the retired mark predates `sourceFnv1a`, and pass its digest
      // so stale journals/done marks from an earlier equal-size generation are
      // skipped immediately.
      let rawBefore: string | undefined
      let rawBeforeDigest: string | undefined
      if (sz > 0 && sz <= HOST_FILE_BYTES) {
        try {
          rawBefore = await this.io.readFile(childPath)
          if (rawBefore === '') return { action: 'skip' }
          if (utf8Length(rawBefore) !== sz) {
            return { action: 'defer', ref: fallback, reason: `${base} changed size before checking its replay record; deferring reconciliation` }
          }
          rawBeforeDigest = fnv1a(rawBefore)
        } catch {
          return { action: 'defer', ref: fallback, reason: `cannot read child ${base}; deferring reconciliation` }
        }
      }
      // Inspect the child's own delivery record before creating any replacement.
      // Query unfiltered first, and if a newer generation's explicit
      // `sourceFnv1a` mismatches (or conflicts), re-query strictly on
      // `rawBeforeDigest` so an older superseded digest-less 0.1.3 record is
      // never unmasked as the winner for an equal-size replacement.
      let childFound: Found | Pending | 'torn' | 'conflict' | undefined
      try {
        const baseChildSource = sourceFor(rewrite.dir, base, sz, c)
        childFound = await this.readJournal(baseChildSource)
        if (rawBeforeDigest !== undefined) {
          if (childFound === 'conflict') {
            const nonStrictChild = await this.readJournal({ ...baseChildSource, fnv1a: rawBeforeDigest, strictFnv1a: false })
            const allMismatched =
              nonStrictChild === undefined || (typeof nonStrictChild === 'object' && 'pending' in nonStrictChild)
            const soleExactMatch =
              typeof nonStrictChild === 'object' &&
              !('pending' in nonStrictChild) &&
              nonStrictChild.journal.sourceFnv1a === rawBeforeDigest
            if (allMismatched || soleExactMatch) childFound = nonStrictChild
          } else if (
            childFound !== undefined &&
            childFound !== 'torn' &&
            !('pending' in childFound) &&
            typeof childFound.journal.sourceFnv1a === 'string' &&
            childFound.journal.sourceFnv1a !== '' &&
            childFound.journal.sourceFnv1a !== rawBeforeDigest
          ) {
            childFound = await this.readJournal({ ...baseChildSource, fnv1a: rawBeforeDigest, strictFnv1a: true })
          }
        }
      } catch {
        return { action: 'defer', ref: fallback, reason: `cannot read replay record for ${base}; deferring reconciliation` }
      }
      if (
        childFound !== undefined &&
        childFound !== 'torn' &&
        childFound !== 'conflict' &&
        'pending' in childFound &&
        childFound.backup === true &&
        childFound.replaces === rewrite.name
      ) {
        return { action: 'skip' }
      }
      if (childFound !== undefined && childFound !== 'torn' && childFound !== 'conflict' && !('pending' in childFound) && childFound.journal.retired === true) {
        // Child already completed its own replay and wrote its retired marker
        // (e.g. failed to truncate): verify the bytes just read match the exact
        // retired generation (`sourceFnv1a` when present, and unchanged across
        // `readJournal`) before recording them as reconciled.
        try {
          const raw = await this.io.readFile(childPath)
          if (raw === '') return { action: 'skip' }
          if (rawBefore !== undefined && raw !== rawBefore) {
            return { action: 'defer', ref: fallback, reason: `${base} changed content while checking its retired record; deferring reconciliation` }
          }
          const actualSize = utf8Length(raw)
          const actualDigest = fnv1a(raw)
          if (actualSize !== sz || childFound.journal.size !== actualSize) {
            return { action: 'defer', ref: fallback, reason: `${base} changed size while checking its retired record; deferring reconciliation` }
          }
          const parsedRetired = parseFile(raw)
          if (!parsedRetired.complete) {
            return { action: 'defer', ref: fallback, reason: `${base} is incomplete while checking its retired record; deferring reconciliation` }
          }
          const currentParent = parsedRetired.parent === null ? null : baseOf(parsedRetired.parent)
          if (currentParent !== rewrite.name) return { action: 'skip' }
          const retiredDigest =
            typeof childFound.journal.sourceFnv1a === 'string' && childFound.journal.sourceFnv1a !== '' ? childFound.journal.sourceFnv1a : undefined
          if (fallbackRef !== undefined && fallbackRef.fnv1a !== '' && (actualSize !== fallbackRef.size || actualDigest !== fallbackRef.fnv1a)) {
            return { action: 'defer', ref: fallback, reason: `${base} changed content while checking its retired record; deferring reconciliation` }
          }
          const retiredOutcome: Outcome = {
            fates: typeof childFound.journal.fates === 'string' ? childFound.journal.fates : '',
            reasons: Array.isArray(childFound.journal.reasons) ? childFound.journal.reasons : [],
            refusals: Array.isArray(childFound.journal.refusals) ? childFound.journal.refusals : [],
          }
          if (!fitsLines(parsedRetired, retiredOutcome)) {
            return { action: 'defer', ref: fallback, reason: `${base} no longer matches its retired record; deferring reconciliation` }
          }
          if (retiredDigest !== undefined && retiredDigest === actualDigest) {
            return { action: 'reconciled', ref: { name: base, size: actualSize, fnv1a: actualDigest, needsLeftovers: false } }
          }
          // Legacy retired mark without `sourceFnv1a`: never fabricate proof
          // of the old delivery into `P.reconciled` unless every row is
          // independently accounted for in `P`'s own `accountedRowKeys`.
          const allRowsInParent =
            parsedRetired.unreadable.length === 0 &&
            rewrite.accountedRowKeys !== undefined &&
            parsedRetired.lines.every(line => {
              const identity = childRowIdentity(line)
              return identity !== null && rewrite.accountedRowKeys!.has(identity)
            })
          if (allRowsInParent) {
            const overlapsLeftover =
              rewrite.files.length > 0 &&
              (rewrite.leftoverRowKeys === undefined ||
                parsedRetired.lines.some(line => {
                  const identity = childRowIdentity(line)
                  return identity !== null && rewrite.leftoverRowKeys!.has(identity)
                }))
            return {
              action: 'reconciled',
              ref: { name: base, size: actualSize, fnv1a: actualDigest, ...(!overlapsLeftover ? { needsLeftovers: false } : {}) },
            }
          }
          // Adopt unmodified instead of skipping or reconciling: P preserves
          // `base` in `files` (no deadlock against a prior v2 listing it),
          // never fabricates an unverified digest in `reconciled`, never
          // truncates `base`, and never copies its rows into a replacement.
          return { action: 'adopt', ref: { name: base, size: actualSize, fnv1a: actualDigest, replaces: base } }
        } catch {
          return { action: 'defer', ref: fallback, reason: `cannot read retired child ${base}; deferring reconciliation` }
        }
      }
      if ((!forceStandalone && rewrite.accountedRowKeys === undefined) || sz > HOST_FILE_BYTES) {
        return { action: 'adopt', ref: rawBeforeDigest !== undefined ? { name: base, size: sz, fnv1a: rawBeforeDigest, replaces: base } : undefined }
      }
      let raw: string
      try {
        raw = await this.io.readFile(childPath)
      } catch {
        return { action: 'defer', ref: fallback, reason: `cannot read child ${base}; deferring reconciliation` }
      }
      if (raw === '') return { action: 'skip' }
      if (rawBefore !== undefined && raw !== rawBefore) {
        return { action: 'defer', ref: fallback, reason: `${base} changed content while reading; deferring reconciliation` }
      }
      const actualSize = utf8Length(raw)
      const actualDigest = fnv1a(raw)
      const ref: ReconciledFile = { name: base, size: actualSize, fnv1a: actualDigest }
      const parsed = parseFile(raw)
      if (!parsed.complete) {
        return { action: 'defer', ref, reason: `${base} is incomplete; deferring reconciliation` }
      }
      if (actualSize !== sz) {
        return { action: 'defer', ref: fallback, reason: `${base} changed size while reading; deferring reconciliation` }
      }
      const currentParent = parsed.parent === null ? null : baseOf(parsed.parent)
      if (currentParent !== null && currentParent !== rewrite.name) return { action: 'skip' }
      if (fallbackRef !== undefined && fallbackRef.fnv1a !== '' && (actualSize !== fallbackRef.size || actualDigest !== fallbackRef.fnv1a)) {
        if (currentParent !== rewrite.name) return { action: 'skip' }
      }
      const isOnlyOwnParentGuard =
        childFound !== undefined &&
        childFound !== 'torn' &&
        childFound !== 'conflict' &&
        'pending' in childFound &&
        childFound.stagedFor === rewrite.name &&
        childFound.uncertain !== true &&
        (childFound.nonStagedBy ?? []).length === 0
      let isChildPendingOrBad =
        childFound === 'torn' ||
        childFound === 'conflict' ||
        (childFound !== undefined && 'pending' in childFound && !isOnlyOwnParentGuard)
      if (childFound !== undefined && childFound !== 'torn' && childFound !== 'conflict' && !('pending' in childFound)) {
        const cj = childFound.journal
        const hasLiveOrWrittenGrandchild =
          Array.isArray(cj.files) &&
          cj.files.length > 0 &&
          (writtenOf(cj).some(Boolean) ||
            currentListing === null ||
            (currentListing !== undefined &&
              cj.files.some(f => typeof f === 'string' && (currentListing.get(baseOf(f)) ?? 0) > 0)))
        if (hasLiveOrWrittenGrandchild) {
          return { action: 'defer', ref, reason: `${base} has its own written children; deferring reconciliation` }
        }
        const candidate: Outcome = {
          fates: typeof cj.fates === 'string' ? cj.fates : '',
          reasons: Array.isArray(cj.reasons) ? cj.reasons : [],
          refusals: Array.isArray(cj.refusals) ? cj.refusals : [],
          ...(Array.isArray(cj.uncertainLines) ? { uncertainLines: cj.uncertainLines } : {}),
        }
        if (!fitsLines(parsed, candidate)) {
          // Unaccounted child journal with no written grandchildren: treat its
          // delivery status as uncertain rather than deadlocking parent and child.
          isChildPendingOrBad = true
        } else {
          // Shipped 0.1.3 resumes a child with a valid unretired journal ahead
          // of `marksOn` (`childGuard`), so defer reconciling `base` until its
          // own unretired replay completes rather than racing to emit a replacement.
          return { action: 'defer', ref, reason: `${base} has an active unretired replay record; deferring reconciliation` }
        }
      }
      const remainingEntries: { line: DeadLetterLine; index: number; refusedReason?: string }[] = []
      const skipIndices: number[] = []
      let overlapsParentLeftover = rewrite.files.length > 0 && rewrite.leftoverRowKeys === undefined
      for (let i = 0; i < parsed.lines.length; i += 1) {
        const line = parsed.lines[i] as DeadLetterLine
        const identity = childRowIdentity(line)
        if (identity !== null && rewrite.accountedRowKeys?.has(identity) === true) {
          skipIndices.push(i)
          if (!overlapsParentLeftover && rewrite.files.length > 0 && rewrite.leftoverRowKeys?.has(identity) === true) {
            overlapsParentLeftover = true
          }
          continue
        }
        remainingEntries.push({ line, index: i })
      }
      if (remainingEntries.length === 0 && parsed.unreadable.length === 0) {
        return {
          action: 'reconciled',
          ref: !overlapsParentLeftover ? { ...ref, needsLeftovers: false } : ref,
        }
      }
      if (!forceStandalone && remainingEntries.length === parsed.lines.length) {
        return { action: 'adopt', ref: { ...ref, replaces: base } }
      }
      const pendingRef: ReconciledFile = {
        ...ref,
        ...(skipIndices.length > 0 ? { skipIndices } : {}),
        ...(!overlapsParentLeftover ? { needsLeftovers: false } : {}),
      }
      return {
        action: 'replace',
        ref: pendingRef,
        childKind: kindOf(base) ?? 'retry',
        markUncertain: isChildPendingOrBad,
        entries: remainingEntries,
        unreadable: parsed.unreadable,
      }
    }
    if (rewrite.force && !finished) {
      const prevChildCount = (rewrite.adoptedFiles?.length ?? 0) + (rewrite.reconciledFiles?.length ?? 0) + (rewrite.pendingReconcile?.length ?? 0)
      const listed = await dirListing()
      if (listed === null) {
        this.recordError(`could not list ${rewrite.dir} during forced child audit for ${path}; retrying later`)
        rewrite.waits = true
        return false
      }
      {
        const adopted = new Set([...rewrite.files.map(f => baseOf(f.path)), ...(rewrite.adoptedFiles ?? [])])
        const reconciled = new Set((rewrite.reconciledFiles ?? []).map(r => r.name))
        const pendingSet = new Set((rewrite.pendingReconcile ?? []).map(r => r.name))
        const stagedSet = new Set((rewrite.stagedFiles ?? []).map(s => s.name))
        const backupSet = new Set((rewrite.backupFiles ?? []).map(b => b.name))
        const entries = [...listed.entries()].map(([n, sz]) => ({ name: n, size: sz }))
        const candidateJournals: (Partial<Journal> | 'torn')[] = []
        for (const key of [rewrite.recordKey, rewrite.v2RecordKey]) {
          try {
            const j = await readStoreSlot(key)
            if (j !== undefined) candidateJournals.push(j)
          } catch {
            // Best-effort inspection before forced rewrite.
          }
        }
        for (const file of [...rewrite.recordFiles, ...rewrite.v2RecordFiles]) {
          try {
            if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
            const txt = await this.io.readFile(file)
            if (txt !== '') {
              const kept = journalOf(txt)
              if (kept !== undefined) candidateJournals.push(kept)
            }
          } catch {
            // Best-effort inspection before forced rewrite.
          }
        }
        try {
          const doneAll = await this.doneMarksAll({ ...rewrite, live: false }, entries)
          for (const d of doneAll.records) candidateJournals.push(d)
          if (doneAll.torn) candidateJournals.push('torn')
        } catch {
          // Best-effort inspection before forced rewrite.
        }
        // Phase 1: Classify candidate live children WITHOUT writing any
        // replacement files yet. Mixed or leased/unreadable children go into
        // `pendingReconcile` so the parent's confirmed outcome is checkpointed
        // in v2 FIRST and a replacement write error never adopts landed rows.
        const inspectCandidate = async (base: string, sz: number): Promise<void> => {
          if (base === '' || base === rewrite.name || adopted.has(base) || reconciled.has(base) || pendingSet.has(base) || stagedSet.has(base) || backupSet.has(base) || sz <= 0) return
          const decision = await classifyChild(base, sz, undefined, listed)
          if (decision.action === 'skip') return
          if (decision.action === 'reconciled') {
            reconciled.add(base)
            ;(rewrite.reconciledFiles ??= []).push(decision.ref)
            return
          }
          if (decision.action === 'replace' || decision.action === 'defer') {
            pendingSet.add(base)
            ;(rewrite.pendingReconcile ??= []).push(decision.ref)
            rewrite.waits = true
            return
          }
          adopted.add(base)
          ;(rewrite.adoptedFiles ??= []).push(base)
          upsertDeliverableRef(decision.ref)
          if (kindOf(base) !== 'rejected') rewrite.waits = true
        }
        const maxAdoptedSeq = new Map<string, number>()
        const maxStagedSeq = new Map<string, number>()
        for (const n of adopted) maxAdoptedSeq.set(n, rewrite.seq)
        for (const n of stagedSet) maxStagedSeq.set(n, rewrite.seq)
        for (const j of candidateJournals) {
          if (j === 'torn' || j.size !== rewrite.size) continue
          const jSeq = seqOf(j)
          for (const staged of normalizeStaged(j.stagedFiles)) {
            maxStagedSeq.set(staged, Math.max(maxStagedSeq.get(staged) ?? -1, jSeq))
          }
          for (const b of normalizeBackups(j.backupFiles)) {
            backupSet.add(b.name)
          }
          if (Array.isArray(j.files)) {
            for (const f of j.files) if (typeof f === 'string' && f !== '') maxAdoptedSeq.set(baseOf(f), Math.max(maxAdoptedSeq.get(baseOf(f)) ?? -1, jSeq))
          }
          if (Array.isArray(j.adopted)) {
            for (const f of j.adopted) if (typeof f === 'string' && f !== '') maxAdoptedSeq.set(baseOf(f), Math.max(maxAdoptedSeq.get(baseOf(f)) ?? -1, jSeq))
          }
        }
        stagedSet.clear()
        for (const [staged, sSeq] of maxStagedSeq) {
          if (sSeq > (maxAdoptedSeq.get(staged) ?? -1)) stagedSet.add(staged)
        }
        for (const j of candidateJournals) {
          if (j === 'torn') continue
          if (j.size !== rewrite.size) continue
          const names = new Set<string>()
          if (Array.isArray(j.files)) {
            for (const entry of j.files) {
              if (typeof entry === 'string' && entry !== '') names.add(baseOf(entry))
            }
          }
          if (Array.isArray(j.adopted)) {
            for (const entry of j.adopted) {
              if (typeof entry === 'string' && entry !== '') names.add(baseOf(entry))
            }
          }
          for (const ref of normalizeReconciled(j.pendingReconcile, true)) names.add(ref.name)
          for (const ref of normalizeReconciled(j.reconciled)) names.add(ref.name)
          for (const base of names) {
            await inspectCandidate(base, listed.get(base) ?? 0)
          }
        }
        // Always scan live dead-letter files under `force` so children pointing
        // to this parent are never dropped even after a conflict, fresh parent
        // replay, or stale v1 journal with `files: []`.
        for (const [n, sz] of listed) {
          if (
            sz <= 0 ||
            n === rewrite.name ||
            adopted.has(n) ||
            reconciled.has(n) ||
            pendingSet.has(n) ||
            stagedSet.has(n) ||
            backupSet.has(n) ||
            kindOf(n) === null ||
            sz > HOST_FILE_BYTES
          ) {
            continue
          }
          const info = await this.readParentInfo(rewrite.dir, n, sz, parentIndex)
          if (info.state === 'ok' && info.complete && info.parent === rewrite.name) {
            await inspectCandidate(n, sz)
          }
        }
      }
      const nextChildCount = (rewrite.adoptedFiles?.length ?? 0) + (rewrite.reconciledFiles?.length ?? 0) + (rewrite.pendingReconcile?.length ?? 0)
      if (nextChildCount > prevChildCount) recorded = false
    }
    const getAdoptedNames = (): string[] => rewrite.adoptedFiles ?? []
    const getReconciledRefs = (): ReconciledFile[] => rewrite.reconciledFiles ?? []
    const getPendingRefs = (): ReconciledFile[] => rewrite.pendingReconcile ?? []
    const getStagedRefs = (): StagedFile[] => rewrite.stagedFiles ?? []
    const getStagedNames = (): string[] => getStagedRefs().map(s => s.name)
    const getBackupRefs = (): BackupFile[] => rewrite.backupFiles ?? []
    const getBackupNames = (): string[] => getBackupRefs().map(b => b.name)
    const getRewriteNames = (): string[] => [...rewrite.files.map(f => baseOf(f.path)), ...getAdoptedNames()]
    // Store checkpoints with adopted, reconciled, pending-reconcile, staged, or
    // backup replacement children in the versioned v2 slot so 0.1.3 readers never
    // delete the checkpoint or resend a reconciled/pending/staged child while
    // the standing sending mark holds the parent.
    if (
      (rewrite.stampedIds === true && rewrite.layoutsDiffer === true) ||
      getAdoptedNames().length > 0 ||
      getReconciledRefs().length > 0 ||
      getPendingRefs().length > 0 ||
      getStagedRefs().length > 0 ||
      getBackupRefs().length > 0 ||
      (rewrite.guardAttempts?.length ?? 0) > 0
    ) {
      rewrite.useV2 = true
    }
    const activeKey = rewrite.useV2 ? rewrite.v2RecordKey : rewrite.recordKey
    const activeFiles = rewrite.useV2 ? rewrite.v2RecordFiles : rewrite.recordFiles
    const otherKey = rewrite.useV2 ? rewrite.recordKey : rewrite.v2RecordKey
    const otherFiles = rewrite.useV2 ? rewrite.recordFiles : rewrite.v2RecordFiles
    const isDifferentSourceDigest = (journal: Partial<Journal>): boolean =>
      rewrite.sourceFnv1a !== undefined &&
      typeof journal.sourceFnv1a === 'string' &&
      journal.sourceFnv1a !== '' &&
      journal.sourceFnv1a !== rewrite.sourceFnv1a
    const matchesRewrite = (journal: Partial<Journal> | 'torn' | undefined): boolean =>
      journal !== undefined &&
      journal !== 'torn' &&
      journal.size === rewrite.size &&
      !isDifferentSourceDigest(journal) &&
      (rewrite.files.length <= 1 || journal.stampedIds === rewrite.stampedIds) &&
      journal.fates === rewrite.fates &&
      Array.isArray(journal.files) &&
      sameNames(journal.files, getRewriteNames()) &&
      sameReconciled(normalizeReconciled(journal.reconciled), getReconciledRefs()) &&
      sameReconciled(normalizeReconciled(journal.pendingReconcile, true), getPendingRefs()) &&
      sameStaged(normalizeStagedRefs(journal.stagedFiles), getStagedRefs()) &&
      sameStaged(normalizeStagedRefs(journal.deliverableRefs), rewrite.useV2 ? (rewrite.deliverableRefs ?? []) : []) &&
      sameBackups(normalizeBackups(journal.backupFiles), getBackupRefs()) &&
      coversAllAttempts(journal, rewrite.guardAttempts) &&
      seqOf(journal) <= rewrite.seq
    const isOwnedOrTracked = (name: string): boolean => {
      if (getRewriteNames().includes(name)) return true
      if (getReconciledRefs().some(r => r.name === name)) return true
      if (getPendingRefs().some(r => r.name === name)) return true
      if (getStagedNames().includes(name)) return true
      if (getBackupNames().includes(name)) return true
      return false
    }
    const hasUnownedLiveChild = async (listed: ReadonlyMap<string, number>): Promise<boolean> => {
      const companionsBySource = groupCompanions([...listed.entries()].map(([n, sz]) => ({ name: n, size: sz })))
      for (const [n, sz] of listed) {
        if (sz <= 0 || n === rewrite.name || isOwnedOrTracked(n) || kindOf(n) === null) continue
        const info = await this.readParentInfo(rewrite.dir, n, sz, parentIndex)
        if (info.state === 'oversized' || info.state === 'error') return true
        if (info.state === 'ok' && (!info.complete || info.parent === rewrite.name)) {
          if (info.complete && info.parent === rewrite.name) {
            try {
              const m = await this.marksDetailed(sourceFor(rewrite.dir, n, sz, c), companionsBySource.get(n) ?? [])
              if (m.backup === true && m.replaces === rewrite.name) continue
            } catch {
              return true
            }
          }
          return true
        }
      }
      return false
    }
    const protectsLiveChild = async (journal: Partial<Journal> | 'torn' | undefined): Promise<boolean> => {
      if (journal === undefined) return false
      if (journal !== 'torn' && matchesRewrite(journal)) return false
      const listed = await dirListing()
      if (listed === null) return true
      if (journal === 'torn') return hasUnownedLiveChild(listed)
      const differentDigest = isDifferentSourceDigest(journal)
      return journalChildNames(journal).some(
        base =>
          base !== '' &&
          (listed.get(base) ?? 0) > 0 &&
          (journal.size !== rewrite.size || differentDigest || !isOwnedOrTracked(base)),
      )
    }
    class StaleRecordClearError extends Error {}
    const failStaleClear = (): never => {
      const message = `could not clear stale replay record for ${path}, so nothing more of what it left is written`
      this.recordError(message)
      throw new StaleRecordClearError(message)
    }
    let clearedActiveSlot = false
    const clearStaleActiveFiles = async (skipFile?: string, clearOlderSameRewrite = false): Promise<void> => {
      for (const file of activeFiles) {
        if (file === skipFile) continue
        try {
          if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
          const text = await this.io.readFile(file)
          if (text === '') continue
          const kept = journalOf(text)
          const supersededOwnFile =
            clearOlderSameRewrite &&
            kept !== undefined &&
            kept !== 'torn' &&
            kept.size === rewrite.size &&
            kept.fates === rewrite.fates &&
            seqOf(kept) < rewrite.seq
          if ((!supersededOwnFile && matchesRewrite(kept)) || (await protectsLiveChild(kept))) continue
          await this.io.writeFile(file, '')
        } catch {
          failStaleClear()
        }
      }
    }
    const clearStaleActiveStore = async (): Promise<void> => {
      try {
        const existing = await readStoreSlot(activeKey)
        if (existing !== undefined && !matchesRewrite(existing) && !(await protectsLiveChild(existing))) {
          await this.io.storeDelete(activeKey)
          cachedStore.set(activeKey, undefined)
        }
      } catch {
        failStaleClear()
      }
    }
    let clearedOppositeSlot = false
    const clearOppositeSlot = async (): Promise<boolean> => {
      let ok = true
      try {
        const existing = await readStoreSlot(otherKey)
        if (existing !== undefined) {
          if (await protectsLiveChild(existing)) {
            if (rewrite.useV2 && (existing === 'torn' || existing.size === rewrite.size)) ok = false
          } else {
            try {
              await this.io.storeDelete(otherKey)
              cachedStore.set(otherKey, undefined)
              neutralizedKeys.delete(otherKey)
            } catch {
              // If deleting the superseded v1 store key fails, neutralize it in place
              // so 0.1.3 readers ignore it and honor the standing sending mark instead.
              let neutralized = false
              if (otherKey === rewrite.recordKey) {
                try {
                  await this.io.storeSet(otherKey, null)
                  neutralized = true
                } catch {
                  try {
                    await this.io.storeSet(otherKey, { cleared: true, size: -1, files: [], written: [] })
                    neutralized = true
                  } catch {
                    neutralized = false
                  }
                }
              }
              if (neutralized) {
                cachedStore.set(otherKey, undefined)
                neutralizedKeys.add(otherKey)
              } else {
                ok = false
              }
            }
          }
        }
      } catch {
        ok = false
      }
      for (const file of otherFiles) {
        try {
          if (this.io.exists !== undefined && !(await this.io.exists(file))) continue
          const text = await this.io.readFile(file)
          if (text === '') continue
          const kept = journalOf(text)
          if (kept !== undefined && kept !== 'torn' && kept.retired === true && !rewrite.wroteDone) {
            if (rewrite.useV2 && kept.size === rewrite.size) ok = false
            continue
          }
          if (await protectsLiveChild(kept)) {
            if (rewrite.useV2 && (kept === 'torn' || kept?.size === rewrite.size)) ok = false
            continue
          }
          await this.io.writeFile(file, '')
        } catch {
          ok = false
        }
      }
      return ok
    }
    const ensureOppositeSlotCleared = async (): Promise<void> => {
      if (clearedOppositeSlot) return
      if (await clearOppositeSlot()) {
        clearedOppositeSlot = true
        return
      }
      // On versioned v2 writes, if v1 cannot be deleted or neutralized in place,
      // proceed so markDone can write a retired completion mark (`outcome-done-<mark>d-`),
      // which both 0.1.3 and 0.1.4 readers honor ahead of any stale v1 record.
      if (rewrite.useV2) return
      failStaleClear()
    }
    const record = async (): Promise<void> => {
      rewrite.seq += 1
      const { fates, reasons, refusals, uncertainLines, size, waits, seq } = rewrite
      const adoptedNow = getAdoptedNames()
      const reconciledNow = getReconciledRefs()
      const pendingNow = getPendingRefs()
      const stagedNow = getStagedRefs()
      const backupsNow = getBackupRefs()
      const namesNow = getRewriteNames()
      const needsAccountedKeys =
        pendingNow.length > 0 &&
        rewrite.accountedRowKeys !== undefined &&
        rewrite.accountedRowKeys.size > 0
      const journal: Journal = {
        fates,
        reasons,
        refusals,
        ...(uncertainLines && uncertainLines.length > 0 ? { uncertainLines } : {}),
        size,
        ...(rewrite.sourceFnv1a ? { sourceFnv1a: rewrite.sourceFnv1a } : {}),
        waits,
        seq,
        files: namesNow,
        written: [...rewrite.files.map(f => f.text === null), ...adoptedNow.map(() => true)],
        ...(adoptedNow.length > 0 ? { adopted: adoptedNow } : {}),
        ...(rewrite.useV2 && rewrite.deliverableRefs !== undefined && rewrite.deliverableRefs.length > 0
          ? { deliverableRefs: rewrite.deliverableRefs }
          : {}),
        ...(reconciledNow.length > 0 ? { reconciled: reconciledNow } : {}),
        ...(pendingNow.length > 0 ? { pendingReconcile: pendingNow } : {}),
        ...(stagedNow.length > 0 ? { stagedFiles: stagedNow } : {}),
        ...(backupsNow.length > 0 ? { backupFiles: backupsNow } : {}),
        ...(rewrite.guardAttempt ? { attempt: rewrite.guardAttempt } : {}),
        ...(rewrite.guardAttempts && rewrite.guardAttempts.length > 0 ? { attempts: rewrite.guardAttempts } : {}),
        ...(needsAccountedKeys ? { accountedKeys: [...rewrite.accountedRowKeys!] } : {}),
        ...(rewrite.supersedesV1 !== undefined ? { supersedesV1: rewrite.supersedesV1 } : {}),
        ...(rewrite.stampedIds !== undefined ? { stampedIds: rewrite.stampedIds } : {}),
        ...(rewrite.files.some(f => f.uncertain === true)
          ? { uncertain: [...rewrite.files.map(f => f.uncertain === true), ...adoptedNow.map(() => false)] }
          : {}),
      }
      if (utf8Length(JSON.stringify(journal)) <= STORE_RECORD_BYTES) {
        let storeSlotBlocked = false
        try {
          const existingStore = await readStoreSlot(activeKey)
          if (existingStore !== undefined && !matchesRewrite(existingStore) && (await protectsLiveChild(existingStore))) {
            storeSlotBlocked = true
          }
        } catch {
          storeSlotBlocked = true
        }
        if (!storeSlotBlocked) {
          try {
            await this.io.storeSet(activeKey, journal)
            cachedStore.set(activeKey, journal)
            if (!clearedActiveSlot || rewrite.filed) {
              await clearStaleActiveFiles(undefined, rewrite.filed)
              clearedActiveSlot = true
              rewrite.filed = false
            }
            await ensureOppositeSlotCleared()
            return
          } catch (err) {
            if (err instanceof StaleRecordClearError) throw err
            // Not taken (the store is full, say): a file beside the source.
          }
        }
      }
      const candidates = [activeFiles[(seq + 1) % 2] as string, activeFiles[seq % 2] as string]
      let activeFile: string | null = null
      for (const candidate of candidates) {
        try {
          const text = this.io.exists !== undefined && !(await this.io.exists(candidate)) ? '' : await this.io.readFile(candidate)
          if (text === '' || !(await protectsLiveChild(journalOf(text)))) {
            activeFile = candidate
            break
          }
        } catch {
          // Do not overwrite an unreadable companion file that may protect a live child.
        }
      }
      if (activeFile === null) {
        const message = `could not record the replay of ${path}, so nothing more of what it left is written`
        this.recordError(message)
        throw new Error(message)
      }
      try {
        // The two files take the updates in turn: one cut off part-way
        // leaves the record before it whole in the other (or in the store).
        await this.io.writeFile(activeFile, journalText(journal))
        rewrite.filed = true
      } catch (err) {
        const message = `could not record the replay of ${path}, so nothing more of what it left is written: ${errorText(err)}`
        this.recordError(message)
        throw new Error(message)
      }
      if (!clearedActiveSlot) {
        await clearStaleActiveStore()
        await clearStaleActiveFiles(activeFile)
        clearedActiveSlot = true
      }
      await ensureOppositeSlotCleared()
    }
    const clearChildGuardMarks = async (onlyNamed?: ReadonlySet<string>, exceptNamed?: ReadonlySet<string>): Promise<void> => {
      if (rewrite.childGuardMarks === undefined || rewrite.childGuardMarks.size === 0) return
      for (const guardedPath of [...rewrite.childGuardMarks.keys()]) {
        const guardedName = baseOf(guardedPath)
        if (onlyNamed !== undefined && !onlyNamed.has(guardedName)) continue
        if (exceptNamed !== undefined && exceptNamed.has(guardedName)) continue
        try {
          await this.io.writeFile(doneFileOf(rewrite.dir, guardedName, `${this.mark}g`), '')
          rewrite.childGuardMarks.delete(guardedPath)
        } catch {
          // Best-effort clear of child guard mark.
        }
      }
    }
    if (!finished && (getPendingRefs().length > 0 || getAdoptedNames().length > 0 || getReconciledRefs().length > 0)) {
      const listedBeforeRecord = await dirListing()
      if (listedBeforeRecord === null) {
        this.recordError(`could not list ${rewrite.dir} before guarding children of ${path}; retrying later`)
        return false
      }
      const guardTargets = new Map<string, number>()
      const expectedParentChildren = new Set<string>()
      const entriesBeforeRecord = [...listedBeforeRecord.entries()].map(([n, sz]) => ({ name: n, size: sz }))
      const companionsBeforeRecord = groupCompanions(entriesBeforeRecord)
      const candidateNames = new Set([
        ...getPendingRefs().map(r => r.name),
        ...getAdoptedNames(),
        ...getReconciledRefs().map(r => r.name),
      ])
      const deliverableByName = new Map((rewrite.deliverableRefs ?? []).map(d => [d.name, d]))
      const origChildRefByName = new Map<string, ReconciledFile>()
      for (const r of getReconciledRefs()) origChildRefByName.set(r.name, r)
      for (const r of getPendingRefs()) if (!origChildRefByName.has(r.name)) origChildRefByName.set(r.name, r)
      const origBackupsByReplaces = new Map<string, BackupFile[]>()
      for (const b of rewrite.backupFiles ?? []) {
        const list = origBackupsByReplaces.get(b.replaces)
        if (list) list.push(b)
        else origBackupsByReplaces.set(b.replaces, [b])
      }
      const hasVerifiedOrigOrBackup = async (origName: string): Promise<boolean> => {
        const origRef = origChildRefByName.get(origName)
        if (
          origRef !== undefined &&
          origRef.size > 0 &&
          origRef.fnv1a !== '' &&
          origRef.size <= HOST_FILE_BYTES &&
          (listedBeforeRecord.get(origName) ?? 0) === origRef.size
        ) {
          try {
            const origPath = joinPath(rewrite.dir, origName)
            const origRaw = await this.io.readFile(origPath)
            if (origRaw !== '' && utf8Length(origRaw) === origRef.size && fnv1a(origRaw) === origRef.fnv1a) {
              const origParsed = parseFile(origRaw)
              if (origParsed.complete && origParsed.parent !== null && baseOf(origParsed.parent) === rewrite.name) {
                const origDone = await this.doneMarksAll(
                  sourceFor(rewrite.dir, origName, origRef.size, c),
                  companionsBeforeRecord.get(origName) ?? [],
                )
                const origAlreadyRetired =
                  !origDone.torn &&
                  origDone.records.length > 0 &&
                  (!origDone.records.some(r => typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '') ||
                    Writer.matchesDoneMarks(origDone.records, origRef.fnv1a))
                if (!origDone.torn && !origAlreadyRetired) return true
              }
            }
          } catch {
            // Fail closed: fall through to backup verification or guard the replacement.
          }
        }
        const origBackups = (origBackupsByReplaces.get(origName) ?? []).filter(
          b =>
            origRef === undefined ||
            origRef.size <= 0 ||
            (b.replacesSize === origRef.size &&
              (b.replacesFnv1a === undefined || b.replacesFnv1a === '' || origRef.fnv1a === '' || b.replacesFnv1a === origRef.fnv1a)),
        )
        if (origBackups.length === 0) return false
        const destDir = destinationDir(tableRef(c))
        const curMe = this.sessionId() ?? 'unknown'
        for (const b of origBackups) {
          if (b.size <= 0 || b.fnv1a === '' || b.size > HOST_FILE_BYTES) return false
          if ((listedBeforeRecord.get(b.name) ?? 0) !== b.size) return false
          try {
            const bLease = (await this.io.storeGet(`lease:${destDir}/${b.name}`)) as { by?: string; holder?: string; until?: number } | undefined
            if (
              bLease &&
              typeof bLease.until === 'number' &&
              bLease.until > this.io.now() &&
              (bLease.by !== curMe || typeof bLease.holder !== 'string' || bLease.holder === '' || bLease.holder !== this.mark)
            ) {
              return false
            }
            const bRaw = await this.io.readFile(joinPath(rewrite.dir, b.name))
            if (bRaw === '' || utf8Length(bRaw) !== b.size || fnv1a(bRaw) !== b.fnv1a) return false
            const bParsed = parseFile(bRaw)
            if (!bParsed.complete) return false
            if (bParsed.parent !== null) {
              if (baseOf(bParsed.parent) !== origName) return false
              // A parent-linked backup is vulnerable to 0.1.3 retiring `origName`
              // whenever `origName` has been reused by another live generation or
              // already carries a retirement/torn done mark.
              if ((listedBeforeRecord.get(origName) ?? 0) > 0) return false
              const origCompanions = companionsBeforeRecord.get(origName) ?? []
              if (origCompanions.length > 0) {
                const origDoneAny = await this.doneMarksAll(
                  { ...sourceFor(rewrite.dir, origName, origRef?.size ?? 0, c), live: false },
                  origCompanions,
                )
                if (origDoneAny.torn || origDoneAny.records.length > 0) return false
              }
            }
            const bDone = await this.doneMarksAll(
              sourceFor(rewrite.dir, b.name, b.size, c),
              companionsBeforeRecord.get(b.name) ?? [],
            )
            if (bDone.torn || bDone.records.length > 0) return false
          } catch {
            return false
          }
        }
        return true
      }
      for (const childName of candidateNames) {
        const childSize = listedBeforeRecord.get(childName) ?? 0
        if (childSize <= 0 || childName === rewrite.name) continue
        const childPath = joinPath(rewrite.dir, childName)
        if (rewrite.tempMarks?.has(childPath)) continue
        const dRef = deliverableByName.get(childName)
        if (dRef?.replaces !== undefined && dRef.replaces !== '' && dRef.replaces !== childName) {
          if (await hasVerifiedOrigOrBackup(dRef.replaces)) continue
        }
        let hasParentHeader = false
        if (childSize <= HOST_FILE_BYTES) {
          const info = await this.readParentInfo(rewrite.dir, childName, childSize, parentIndex)
          if (info.state === 'error') {
            this.recordError(`could not inspect parent header for child ${childName} of ${path}; retrying later`)
            return false
          }
          if (info.state === 'ok' && info.complete && info.parent !== null && info.parent !== rewrite.name) continue
          if (info.state === 'ok' && info.complete && info.parent === rewrite.name) hasParentHeader = true
        }
        try {
          const childSource = sourceFor(rewrite.dir, childName, childSize, c)
          const childCompanions = companionsBeforeRecord.get(childName) ?? []
          const childDone = await this.doneMarksAll(childSource, childCompanions)
          if (!childDone.torn && childDone.records.length > 0) {
            const hasExplicitDigest = childDone.records.some(r => typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '')
            if (!hasExplicitDigest) continue
            const curRaw = await this.io.readFile(childPath)
            if (curRaw === '' || Writer.matchesDoneMarks(childDone.records, fnv1a(curRaw))) continue
          }
          // Standalone replacements created in an earlier session already carry
          // a non-`childGuard` `stagedFor` sending mark for this parent, and
          // backups replacing this file belong to this file's parent: never
          // stamp `childGuard: true` onto either on resume.
          const existingMarks = await this.marksDetailed(childSource, childCompanions)
          if (
            (existingMarks.stagedFor === rewrite.name && existingMarks.childGuard !== true) ||
            (existingMarks.backup === true && existingMarks.replaces === rewrite.name)
          ) {
            continue
          }
        } catch {
          this.recordError(`could not inspect replay record for child ${childName} of ${path}; retrying later`)
          return false
        }
        guardTargets.set(childPath, childSize)
        if (hasParentHeader) expectedParentChildren.add(childName)
      }
      if (guardTargets.size > 0) {
        const attemptToken = rewrite.guardAttempt ?? `${this.mark}-${++this.seq}-${randomHex(3)}`
        const placedResult = await this.placeChildGuards(rewrite.dir, rewrite.name, rewrite.size, guardTargets, attemptToken, rewrite.childGuardMarks, entriesBeforeRecord)
        if (placedResult === null) {
          this.recordError(`could not guard live children of ${path} before persisting v2 outcome; retrying later`)
          return false
        }
        const backupResult = await this.stageChildBackups(
          rewrite.dir,
          rewrite.name,
          rewrite.size,
          c,
          guardTargets,
          attemptToken,
          rewrite.backupFiles,
          entriesBeforeRecord,
          expectedParentChildren,
        )
        if (!backupResult.ok) {
          const guardMark = `${this.mark}g`
          for (const guardedPath of placedResult.newlyPlaced) {
            try {
              await this.io.writeFile(doneFileOf(rewrite.dir, baseOf(guardedPath), guardMark), placedResult.prevMarks.get(guardedPath) ?? '')
            } catch {
              // Best-effort rollback of newly placed child guards.
            }
          }
          if (backupResult.leasedChild !== undefined) {
            const childLeasePath = joinPath(rewrite.dir, backupResult.leasedChild)
            const childLeaseMsg = diagnostic(`another session's lease holds ${backupResult.leasedChild} (ten minutes at most); it waits for the next replay`)
            const parentLeaseMsg = diagnostic(`another session's lease holds child ${backupResult.leasedChild} of ${rewrite.name} (ten minutes at most); it waits for the next replay`)
            const childEntry: HeldEntry = { reason: childLeaseMsg, mayBeActive: true, activeCertain: true }
            const parentEntry: HeldEntry = { reason: parentLeaseMsg, mayBeActive: true, activeCertain: true }
            this.setHeldPath(childLeasePath, childEntry)
            this.currentScanHeld.set(childLeasePath, childEntry)
            this.setHeldPath(path, parentEntry)
            this.currentScanHeld.set(path, parentEntry)
            this.inProgressHeld = {
              files: Math.max(this.currentScanHeld.size, this.heldCountIn(rewrite.dir)),
              reason: parentLeaseMsg,
              trigger: this.inProgressHeld.trigger,
              mayBeActive: true,
            }
            rewrite.waits = true
            rewrite.deferredByLease = true
          } else if (backupResult.mutatedChild !== undefined) {
            const parentMutMsg = diagnostic(`child ${backupResult.mutatedChild} of ${rewrite.name} changed while staging backups; it waits for the next replay`)
            const parentEntry: HeldEntry = { reason: parentMutMsg, mayBeActive: false }
            this.setHeldPath(path, parentEntry)
            this.currentScanHeld.set(path, parentEntry)
            this.inProgressHeld = {
              files: Math.max(this.currentScanHeld.size, this.heldCountIn(rewrite.dir)),
              reason: parentMutMsg,
              trigger: this.inProgressHeld.trigger,
              mayBeActive: this.inProgressHeld.mayBeActive,
            }
            rewrite.waits = true
            rewrite.deferredByLease = true
          } else {
            this.recordError(`could not stage standalone backups for live children of ${path} before persisting v2 outcome; retrying later`)
          }
          return false
        }
        const prevAttempts = rewrite.guardAttempts ?? []
        const nextAttempts = normalizeAttempts(attemptToken, [...prevAttempts, ...placedResult.priorAttempts, ...backupResult.priorAttempts])
        if (nextAttempts.length !== prevAttempts.length || backupResult.newlyStaged.length > 0 || !sameBackups(backupResult.backups, rewrite.backupFiles ?? [])) {
          recorded = false
        }
        rewrite.guardAttempt = attemptToken
        rewrite.guardAttempts = nextAttempts
        rewrite.childGuardMarks = placedResult.placed
        rewrite.backupFiles = backupResult.backups.length > 0 ? backupResult.backups : undefined
      }
    }
    if (!recorded) await record()
    if (!finished) {
      if (!clearedActiveSlot) {
        await clearStaleActiveStore()
        await clearStaleActiveFiles()
        clearedActiveSlot = true
      }
      await ensureOppositeSlotCleared()
    }
    let stuckV1Journal: Partial<Journal> | 'torn' | undefined
    let stuckV1Captured = false
    const sameJournalSnapshot = (a: Partial<Journal> | 'torn' | undefined, b: Partial<Journal> | 'torn' | undefined): boolean => {
      if (a === b) return true
      if (a === undefined || b === undefined || a === 'torn' || b === 'torn') return false
      return (
        a.size === b.size &&
        seqOf(a) === seqOf(b) &&
        a.fates === b.fates &&
        sameNames(Array.isArray(a.files) ? a.files : [], Array.isArray(b.files) ? b.files : [])
      )
    }
    const v1ProbeSource: Source = { ...rewrite, fnv1a: undefined, strictFnv1a: undefined }
    if (!finished && rewrite.useV2 && !clearedOppositeSlot) {
      const v1Now = await this.readJournalSlot(v1ProbeSource, rewrite.recordKey, rewrite.recordFiles, false)
      stuckV1Journal = v1Now.found?.journal ?? (v1Now.torn ? 'torn' : undefined)
      stuckV1Captured = true
      if (stuckV1Journal !== undefined && stuckV1Journal !== 'torn' && stuckV1Journal.retired !== true) {
        rewrite.seq = Math.max(rewrite.seq, seqOf(stuckV1Journal))
        const nextSuperseded: SupersededV1 = {
          size: typeof stuckV1Journal.size === 'number' ? stuckV1Journal.size : rewrite.size,
          seq: seqOf(stuckV1Journal),
          fates: typeof stuckV1Journal.fates === 'string' ? stuckV1Journal.fates : '',
          files: Array.isArray(stuckV1Journal.files)
            ? stuckV1Journal.files.filter((f): f is string => typeof f === 'string').map(baseOf)
            : [],
        }
        const prev = rewrite.supersedesV1
        const changed =
          prev === undefined ||
          prev.size !== nextSuperseded.size ||
          prev.seq !== nextSuperseded.seq ||
          prev.fates !== nextSuperseded.fates ||
          !sameNames(prev.files, nextSuperseded.files)
        if (changed) {
          rewrite.supersedesV1 = nextSuperseded
          await record()
        }
      }
    }
    const v1ShadowNeeded = (): boolean => rewrite.useV2 === true && !clearedOppositeSlot && stuckV1Captured && stuckV1Journal !== undefined
    const clearTempMarks = async (onlyNamed?: ReadonlySet<string>): Promise<void> => {
      if (rewrite.tempMarks === undefined || rewrite.tempMarks.size === 0) return
      for (const markedPath of [...rewrite.tempMarks]) {
        if (onlyNamed !== undefined && !onlyNamed.has(baseOf(markedPath))) continue
        try {
          await this.io.writeFile(doneFileOf(rewrite.dir, baseOf(markedPath), this.mark), '')
          this.uncertainSources.delete(markedPath)
          rewrite.tempMarks.delete(markedPath)
        } catch {
          // Best-effort clear of temporary standalone mark.
        }
      }
    }
    // Another writer may have written a file of it since, replayed the file
    // at the same time with an outcome of its own, or finished it: the record
    // says. That outcome goes on, at the next replay, and no more of this one.
    const stillOurs = async (): Promise<boolean> => {
      const latestSlots: { slotsRead: boolean; v1Stored?: Partial<Journal>; v2Stored?: Partial<Journal> } = { slotsRead: false }
      let found = Writer.recorded(await this.readJournal(rewrite, latestSlots))
      if (latestSlots.slotsRead) {
        cachedStore.set(rewrite.recordKey, normalizeSlot(latestSlots.v1Stored))
        cachedStore.set(rewrite.v2RecordKey, normalizeSlot(latestSlots.v2Stored))
      }
      // When moving to v2 while a pre-existing stale v1 record could neither be
      // deleted nor neutralized, do not let that unchanged stale v1 cause
      // `readJournal` to report `'conflict'` against our own v2 checkpoint and
      // block `markDone` (whose retired done mark strictly overrides v1 in both
      // 0.1.3 and 0.1.4).
      if (found === 'conflict' && rewrite.useV2 && !clearedOppositeSlot && stuckV1Captured) {
        const [v1Slot, v2Slot] = await Promise.all([
          this.readJournalSlot(v1ProbeSource, rewrite.recordKey, rewrite.recordFiles, false),
          this.readJournalSlot(rewrite, rewrite.v2RecordKey, rewrite.v2RecordFiles, true),
        ])
        const latestV1 = v1Slot.found?.journal ?? (v1Slot.torn ? 'torn' : undefined)
        if (sameJournalSnapshot(latestV1, stuckV1Journal) && v2Slot.found !== undefined && Writer.catchUp(rewrite, v2Slot.found)) {
          found = v2Slot.found
        }
      }
      if (found === 'torn' || found === 'conflict' || (found !== undefined && found.journal.size === rewrite.size && !Writer.catchUp(rewrite, found))) {
        this.rewrites.delete(path)
        // Another outcome superseded or finished the file: release our standing
        // mark and, if retired, drop any store copy of this generation we recorded meanwhile.
        if (found !== 'torn' && found !== 'conflict' && found !== undefined) {
          for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
          await this.unmark(rewrite)
          if (found.journal.retired === true) {
            if (Array.isArray(found.journal.files)) {
              const stagedSet = new Set(getStagedNames())
              const reconciledSet = new Set(normalizeReconciled(found.journal.reconciled).map(r => r.name))
              const ownPendingSet = new Set(getPendingRefs().map(r => r.name))
              const ownReconciledSet = new Set(getReconciledRefs().map(r => r.name))
              const safeToRelease = new Set(
                found.journal.files
                  .filter((f): f is string => typeof f === 'string')
                  .map(baseOf)
                  .filter(n => !stagedSet.has(n) && !reconciledSet.has(n) && !ownPendingSet.has(n) && !ownReconciledSet.has(n)),
              )
              await clearTempMarks(safeToRelease)
              if (!v1ShadowNeeded()) await clearChildGuardMarks(safeToRelease)
            }
            this.deleteHeldPath(path)
            this.currentScanHeld.delete(path)
            for (const key of [rewrite.recordKey, rewrite.v2RecordKey]) {
              try {
                const stored = await readStoreSlot(key)
                if ((stored !== undefined || neutralizedKeys.has(key)) && (stored === undefined || !(await protectsLiveChild(stored)))) {
                  await this.io.storeDelete(key)
                  cachedStore.set(key, undefined)
                  neutralizedKeys.delete(key)
                }
              } catch {
                // Leave store key on error.
              }
            }
          }
        }
        return false
      }
      if (found !== undefined && found.journal.retired === true) {
        finished = true
        finishedJournal = found.journal
      }
      return true
    }
    for (const file of rewrite.files) {
      if (file.text === null) continue
      if (!(await stillOurs())) return false
      if (file.text === null) continue
      // When a stale v1 record cannot be cleared, emit parent leftovers
      // standalone (`parent: null`) guarded by a temporary sending mark until
      // retirement or reconciled-child cleanup settles so a 0.1.3 reader
      // retiring the parent with stale v1 never truncates unique leftover rows.
      const standalone = v1ShadowNeeded()
      const outText = standalone ? stripParentSeal(file.text) : file.text
      const writtenSize = utf8Length(outText)
      const needsTempLeftoverMark = standalone && !file.uncertain
      if (file.uncertain) {
        await this.io.writeFile(
          doneFileOf(rewrite.dir, baseOf(file.path), this.mark),
          sealFile(
            `${JSON.stringify({ size: writtenSize, sending: true, uncertain: true, by: this.mark, ...(standalone ? { stagedFor: rewrite.name, stagedForSize: rewrite.size } : {}) })}\n`,
          ),
        )
        this.uncertainSources.set(file.path, writtenSize)
      } else if (needsTempLeftoverMark) {
        await this.io.writeFile(
          doneFileOf(rewrite.dir, baseOf(file.path), this.mark),
          sealFile(`${JSON.stringify({ size: writtenSize, sending: true, by: this.mark, stagedFor: rewrite.name, stagedForSize: rewrite.size })}\n`),
        )
        ;(rewrite.tempMarks ??= new Set()).add(file.path)
      }
      await this.io.writeFile(file.path, outText)
      parentIndex?.set(baseOf(file.path), { size: writtenSize, parent: standalone ? null : rewrite.name })
      upsertDeliverableRef({ name: baseOf(file.path), size: writtenSize, fnv1a: fnv1a(outText) })
      file.text = null
      if (file.uncertain) {
        const childEntry: HeldEntry = {
          reason: diagnostic(
            `a replay of ${baseOf(file.path)} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
          ),
          mayBeActive: false,
        }
        this.setHeldPath(file.path, childEntry)
        this.currentScanHeld.set(file.path, childEntry)
        if (rewrite.files.every(f => !f.uncertain || f.text === null)) {
          this.deleteHeldPath(path)
          this.currentScanHeld.delete(path)
        }
      }
      await record()
    }
    // When a stale v1 record could not be cleared and omits an unmodified
    // adopted child still carrying `parent: rewrite.name`, queue that child for
    // Phase 3 standalone materialization (`parent: null`) so even a subsequent
    // `0.1.3` `/bqaa-backfill force` after `failed-keep` cannot truncate it.
    const stuckV1Files =
      stuckV1Journal !== undefined && stuckV1Journal !== 'torn' && Array.isArray(stuckV1Journal.files)
        ? new Set(stuckV1Journal.files.filter((f): f is string => typeof f === 'string').map(baseOf))
        : null
    if (!finished && v1ShadowNeeded() && getAdoptedNames().length > 0) {
      const hasOmittedAdopted = getAdoptedNames().some(n => stuckV1Files === null || !stuckV1Files.has(n))
      const listedForAdopt = await dirListing()
      if (listedForAdopt === null) {
        if (hasOmittedAdopted) {
          this.recordError(`could not list ${rewrite.dir} to materialize adopted children for ${path}; retrying later`)
          rewrite.waits = true
          return false
        }
      } else {
        const toStandalone: ReconciledFile[] = []
        const keepAdopted: string[] = []
        const adoptCompanions = groupCompanions([...listedForAdopt.entries()].map(([n, s]) => ({ name: n, size: s })))
        for (const adoptedName of getAdoptedNames()) {
          if (stuckV1Files !== null && stuckV1Files.has(adoptedName)) {
            keepAdopted.push(adoptedName)
            continue
          }
          const sz = listedForAdopt.get(adoptedName) ?? 0
          if (sz <= 0 || sz > HOST_FILE_BYTES) {
            keepAdopted.push(adoptedName)
            continue
          }
          const info = await this.readParentInfo(rewrite.dir, adoptedName, sz, parentIndex)
          if (info.state === 'error') {
            this.recordError(`could not inspect parent header for adopted child ${adoptedName} of ${path}; retrying later`)
            rewrite.waits = true
            return false
          }
          if (info.state === 'ok' && info.complete && info.parent === rewrite.name) {
            let childAlreadyRetired = false
            try {
              const adoptedSource = sourceFor(rewrite.dir, adoptedName, sz, c)
              const cd = await this.doneMarksAll(adoptedSource, adoptCompanions.get(adoptedName) ?? [])
              if (!cd.torn && cd.records.length > 0) {
                const hasExplicitDigest = cd.records.some(r => typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '')
                if (!hasExplicitDigest) {
                  childAlreadyRetired = true
                } else {
                  const rawAdopted = await this.io.readFile(joinPath(rewrite.dir, adoptedName))
                  if (rawAdopted === '' || Writer.matchesDoneMarks(cd.records, fnv1a(rawAdopted))) {
                    childAlreadyRetired = true
                  }
                }
              }
            } catch {
              childAlreadyRetired = false
            }
            if (!childAlreadyRetired && !getPendingRefs().some(r => r.name === adoptedName) && !getReconciledRefs().some(r => r.name === adoptedName)) {
              toStandalone.push({ name: adoptedName, size: sz, fnv1a: '' })
            } else {
              keepAdopted.push(adoptedName)
            }
          } else {
            keepAdopted.push(adoptedName)
          }
        }
        if (toStandalone.length > 0) {
          const prevAdopted = rewrite.adoptedFiles ? [...rewrite.adoptedFiles] : undefined
          const prevDeliverables = rewrite.deliverableRefs ? [...rewrite.deliverableRefs] : undefined
          const prevPending = rewrite.pendingReconcile ? [...rewrite.pendingReconcile] : undefined
          const toStandaloneNames = new Set(toStandalone.map(t => t.name))
          rewrite.adoptedFiles = keepAdopted.length > 0 ? keepAdopted : undefined
          if (rewrite.deliverableRefs !== undefined) {
            const nextDeliverables = rewrite.deliverableRefs.filter(d => !toStandaloneNames.has(d.name))
            rewrite.deliverableRefs = nextDeliverables.length > 0 ? nextDeliverables : undefined
          }
          rewrite.pendingReconcile = [...getPendingRefs(), ...toStandalone]
          rewrite.waits = true
          try {
            await record()
          } catch (err) {
            rewrite.adoptedFiles = prevAdopted
            rewrite.deliverableRefs = prevDeliverables
            rewrite.pendingReconcile = prevPending
            this.recordError(`could not queue adopted children for standalone materialization on ${path}: ${errorText(err)}`)
            return false
          }
        }
      }
    }
    // Safe staged/backup truncation: verify no unexpired foreign lease holds the
    // basename BEFORE reading the file, then verify the bytes still match the
    // recorded `{ size, fnv1a }` (or, for legacy digest-less string entries,
    // still carry this parent's header or non-foreign `stagedFor` mark, with a
    // re-read after any async mark lookup) before emptying anything.
    const safeTruncateStaged = async (
      ref: StagedFile,
      markSuffix: '' | 'g' = '',
      markToClear = `${this.mark}${markSuffix}`,
    ): Promise<'emptied' | 'foreign-live' | 'retry'> => {
      const targetPath = joinPath(rewrite.dir, ref.name)
      try {
        const lease = (await this.io.storeGet(`lease:${dest}/${ref.name}`)) as { by?: string; holder?: string; until?: number } | undefined
        if (
          lease &&
          typeof lease.until === 'number' &&
          lease.until > this.io.now() &&
          (lease.by !== me || typeof lease.holder !== 'string' || lease.holder === '' || lease.holder !== this.mark)
        ) {
          return 'retry'
        }
      } catch {
        return 'retry'
      }
      let targetExists = true
      let cur = ''
      try {
        if (this.io.exists !== undefined && !(await this.io.exists(targetPath))) {
          targetExists = false
          cur = ''
        } else {
          cur = await this.io.readFile(targetPath)
        }
      } catch (err) {
        if (isNotFoundError(err)) {
          targetExists = false
          cur = ''
        } else {
          return 'retry'
        }
      }
      let emptiedNonEmpty = false
      if (cur !== '') {
        const curSize = utf8Length(cur)
        let matchesPart = false
        let awaitedSinceRead = false
        if (ref.fnv1a !== '' && ref.size >= 0) {
          matchesPart = curSize === ref.size && fnv1a(cur) === ref.fnv1a
        } else {
          try {
            const parsedCur = parseFile(cur)
            const curParent = parsedCur.complete && parsedCur.parent !== null ? baseOf(parsedCur.parent) : null
            if (curParent === rewrite.name) {
              matchesPart = true
            } else if (parsedCur.complete && curParent === null) {
              awaitedSinceRead = true
              const m = await this.marksDetailed(sourceFor(rewrite.dir, ref.name, curSize, c))
              matchesPart =
                m.stagedFor === rewrite.name &&
                (m.stagedForSize === undefined || m.stagedForSize === rewrite.size) &&
                m.childGuard !== true &&
                m.nonStagedBy.length === 0
            }
          } catch {
            return 'retry'
          }
        }
        if (!matchesPart) {
          // Bytes belong to another writer's live file: leave the file untouched
          // and clear our own stale `stagedFor` mark only if it belongs to this parent.
          const ownMarkPath = doneFileOf(rewrite.dir, ref.name, markToClear)
          try {
            if (this.io.exists === undefined || (await this.io.exists(ownMarkPath))) {
              const rawMark = await this.io.readFile(ownMarkPath)
              const parsedMark = journalOf(rawMark) as { stagedFor?: unknown } | 'torn' | undefined
              if (parsedMark !== undefined && parsedMark !== 'torn' && typeof parsedMark.stagedFor === 'string' && baseOf(parsedMark.stagedFor) === rewrite.name) {
                await this.io.writeFile(ownMarkPath, '')
              }
            }
          } catch {
            // Best-effort clear of our own stale sending mark.
          }
          rewrite.tempMarks?.delete(targetPath)
          return 'foreign-live'
        }
        if (awaitedSinceRead) {
          try {
            const curAgain = await this.io.readFile(targetPath)
            if (curAgain === '') {
              cur = ''
            } else if (curAgain !== cur) {
              return 'retry'
            }
          } catch {
            return 'retry'
          }
        }
        if (cur !== '') {
          if (!(await this.emptyCopy(targetPath))) return 'retry'
          emptiedNonEmpty = true
          this.retiredEmpty.add(targetPath)
          emptiedInRun?.add(ref.name)
          this.deleteHeldPath(targetPath)
          this.currentScanHeld.delete(targetPath)
        }
      }
      parentIndex?.delete(ref.name)
      this.uncertainSources.delete(targetPath)
      rewrite.tempMarks?.delete(targetPath)
      const markPath = doneFileOf(rewrite.dir, ref.name, markToClear)
      try {
        if (markSuffix === 'g') {
          if (emptiedNonEmpty || targetExists || this.io.exists === undefined || (await this.io.exists(markPath))) {
            await this.io.writeFile(
              markPath,
              journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
            )
          }
        } else if (this.io.exists === undefined || (await this.io.exists(markPath))) {
          await this.io.writeFile(markPath, '')
        }
      } catch {
        // Best-effort update/clear of our own sending mark after file truncation.
      }
      return 'emptied'
    }
    // Phase 3: Materialize pending child reconciliations AFTER the parent's
    // confirmed outcome is checkpointed in v2 and parent leftovers are written.
    // Planned replacement parts are pre-recorded with `{ name, size, fnv1a }` in
    // `stagedFiles` BEFORE any part is written to disk so a partial multi-part
    // failure or crash never exposes an unpromoted part or truncates a foreign file.
    if (!finished && (getPendingRefs().length > 0 || getStagedRefs().length > 0)) {
      const removePending = (name: string): void => {
        const next = (rewrite.pendingReconcile ?? []).filter(r => r.name !== name)
        rewrite.pendingReconcile = next.length > 0 ? next : undefined
      }
      const addReconciled = (ref: ReconciledFile): void => {
        const existing = rewrite.reconciledFiles ?? []
        if (!existing.some(r => r.name === ref.name)) {
          rewrite.reconciledFiles = [
            ...existing,
            {
              name: ref.name,
              size: ref.size,
              fnv1a: ref.fnv1a,
              ...(ref.needsLeftovers === false ? { needsLeftovers: false } : {}),
            },
          ]
        }
      }
      const addAdopted = (name: string, ref?: StagedFile): void => {
        const existing = rewrite.adoptedFiles ?? []
        if (!existing.includes(name)) {
          rewrite.adoptedFiles = [...existing, name]
          if (kindOf(name) !== 'rejected') rewrite.waits = true
        }
        upsertDeliverableRef(ref)
      }
      const cleanupStaged = async (refs: readonly StagedFile[]): Promise<StagedFile[]> => {
        const remaining: StagedFile[] = []
        for (const stagedRef of refs) {
          const res = await safeTruncateStaged(stagedRef, '')
          if (res === 'retry') remaining.push(stagedRef)
        }
        return remaining
      }
      if (getStagedRefs().length > 0) {
        if (!(await stillOurs())) return false
        if (!finished) {
          const prevStaged = [...getStagedRefs()]
          const remainingStaged = await cleanupStaged(prevStaged)
          if (remainingStaged.length !== prevStaged.length) {
            rewrite.stagedFiles = remainingStaged.length > 0 ? remainingStaged : undefined
            try {
              await record()
            } catch {
              rewrite.stagedFiles = prevStaged
              rewrite.waits = true
              return false
            }
          }
        }
      }
      let reconcileBlocked = false
      const pendingSnapshot = [...getPendingRefs()]
      const latestListing = pendingSnapshot.length > 0 ? await dirListing() : null
      for (const pending of pendingSnapshot) {
        if (!(await stillOurs())) return false
        if (finished) break
        const sz = latestListing !== null ? (latestListing.get(pending.name) ?? 0) : pending.size
        if (latestListing !== null && sz <= 0) {
          removePending(pending.name)
          await record()
          continue
        }
        const needsStandalone = v1ShadowNeeded() && (stuckV1Files === null || !stuckV1Files.has(pending.name))
        const decision = await classifyChild(pending.name, sz, pending, latestListing, needsStandalone)
        if (decision.action === 'skip') {
          removePending(pending.name)
          await record()
          continue
        }
        if (decision.action === 'defer') {
          this.recordError(decision.reason)
          reconcileBlocked = true
          continue
        }
        if (decision.action === 'reconciled') {
          removePending(pending.name)
          addReconciled(decision.ref)
          await record()
          continue
        }
        if (decision.action === 'adopt') {
          removePending(pending.name)
          addAdopted(pending.name, decision.ref)
          await record()
          continue
        }
        // Mixed child: group remaining non-overlapping rows by kind (plus any
        // unreadable raw lines in the child's kind), stamp legacy keys if
        // missing, pre-record all planned replacement `{ name, size, fnv1a }`
        // in `stagedFiles` BEFORE writing any part, then atomically promote them to `adopted`.
        const byKind = new Map<DeadLetterKind, string[]>()
        for (const item of decision.entries) {
          const keyedLine = ensureRowEventId(item.line, pending.name, item.index)
          const targetKind: DeadLetterKind = item.refusedReason !== undefined ? 'rejected' : isDeadLetterKind(item.line.kind) ? item.line.kind : decision.childKind
          const formatted = lineText(
            item.refusedReason !== undefined
              ? { ...keyedLine, kind: 'rejected', reason: item.refusedReason, attempts: keyedLine.attempts + 1 }
              : { ...keyedLine, kind: targetKind },
          )
          const bucket = byKind.get(targetKind)
          if (bucket) bucket.push(formatted)
          else byKind.set(targetKind, [formatted])
        }
        for (const rawBad of decision.unreadable) {
          const bucket = byKind.get(decision.childKind)
          if (bucket) bucket.push(rawBad)
          else byKind.set(decision.childKind, [rawBad])
        }
        // When a stale v1 record could not be removed or neutralized, omit the
        // `parent` header on replacements and guard them with a temporary
        // sending mark until retirement or reconciled-child cleanup settles so
        // even if all retired outcome writes fail afterward, a 0.1.3 reader
        // retiring the parent with stale v1 never truncates unique rows and
        // never sends an unpromoted or overlapping replacement early.
        const omitParentHeader = v1ShadowNeeded()
        const planned: { name: string; path: string; part: string; size: number; digest: string }[] = []
        for (const [targetKind, texts] of byKind) {
          const packed = omitParentHeader ? packFiles(texts) : packFiles(texts, rewrite.name)
          for (const part of packed) {
            const replacementName = fileName(targetKind, this.io.now(), this.sessionId(), ++this.seq, this.mark)
            planned.push({ name: replacementName, path: joinPath(rewrite.dir, replacementName), part, size: utf8Length(part), digest: fnv1a(part) })
          }
        }
        const plannedNames = planned.map(p => p.name)
        const plannedRefs: StagedFile[] = planned.map(p => ({ name: p.name, size: p.size, fnv1a: p.digest, replaces: pending.name }))
        const needsTempStageMark = omitParentHeader && !decision.markUncertain
        const prevStaged = [...getStagedRefs()]
        rewrite.stagedFiles = [...prevStaged, ...plannedRefs]
        try {
          await record()
        } catch (err) {
          rewrite.stagedFiles = prevStaged.length > 0 ? prevStaged : undefined
          this.recordError(`could not stage reconciled replacement for ${pending.name}: ${errorText(err)}`)
          reconcileBlocked = true
          break
        }
        const touchedReplacements: StagedFile[] = []
        try {
          for (const item of planned) {
            touchedReplacements.push({ name: item.name, size: item.size, fnv1a: item.digest })
            if (decision.markUncertain) {
              await this.io.writeFile(
                doneFileOf(rewrite.dir, item.name, this.mark),
                sealFile(
                  `${JSON.stringify({ size: item.size, sending: true, uncertain: true, by: this.mark, ...(omitParentHeader ? { stagedFor: rewrite.name, stagedForSize: rewrite.size, replaces: pending.name } : {}) })}\n`,
                ),
              )
              this.uncertainSources.set(item.path, item.size)
            } else if (needsTempStageMark) {
              await this.io.writeFile(
                doneFileOf(rewrite.dir, item.name, this.mark),
                sealFile(`${JSON.stringify({ size: item.size, sending: true, by: this.mark, stagedFor: rewrite.name, stagedForSize: rewrite.size, replaces: pending.name })}\n`),
              )
              ;(rewrite.tempMarks ??= new Set()).add(item.path)
            }
            await this.io.writeFile(item.path, item.part)
            parentIndex?.set(item.name, { size: item.size, parent: omitParentHeader ? null : rewrite.name })
          }
        } catch (err) {
          this.recordError(`could not write reconciled replacement for ${pending.name}: ${errorText(err)}`)
          const stillPresent = await cleanupStaged(touchedReplacements)
          const nextStaged = [...prevStaged, ...stillPresent]
          rewrite.stagedFiles = nextStaged.length > 0 ? nextStaged : undefined
          if (stillPresent.length < plannedRefs.length) {
            try {
              await record()
            } catch {
              rewrite.stagedFiles = [...prevStaged, ...plannedRefs]
            }
          }
          reconcileBlocked = true
          break
        }
        const prevAdopted = rewrite.adoptedFiles ? [...rewrite.adoptedFiles] : undefined
        const prevDeliverables = rewrite.deliverableRefs ? [...rewrite.deliverableRefs] : undefined
        const prevPending = rewrite.pendingReconcile ? [...rewrite.pendingReconcile] : undefined
        const prevReconciled = rewrite.reconciledFiles ? [...rewrite.reconciledFiles] : undefined
        const prevWaits = rewrite.waits
        const stagedAfterWrite = [...prevStaged, ...plannedRefs]
        for (const pRef of plannedRefs) addAdopted(pRef.name, pRef)
        rewrite.stagedFiles = prevStaged.length > 0 ? prevStaged : undefined
        removePending(pending.name)
        addReconciled(decision.ref)
        try {
          await record()
        } catch (err) {
          rewrite.adoptedFiles = prevAdopted
          rewrite.deliverableRefs = prevDeliverables
          rewrite.pendingReconcile = prevPending
          rewrite.reconciledFiles = prevReconciled
          rewrite.stagedFiles = stagedAfterWrite
          rewrite.waits = prevWaits
          this.recordError(`could not record reconciled replacement for ${pending.name}: ${errorText(err)}`)
          reconcileBlocked = true
          break
        }
      }
      if (!finished && (reconcileBlocked || getPendingRefs().length > 0)) {
        rewrite.waits = true
        return false
      }
    }
    if (!(await stillOurs())) return false
    let doneResult: 'wrote' | 'shadow-wrote' | 'failed-keep' | 'failed-empty-ok' = 'failed-empty-ok'
    if (!finished) {
      const hasCachedStore = cachedStore.has(rewrite.recordKey) && cachedStore.has(rewrite.v2RecordKey)
      const cachedRecords = hasCachedStore
        ? [cachedStore.get(rewrite.recordKey), cachedStore.get(rewrite.v2RecordKey)].filter((v): v is Partial<Journal> | 'torn' => v !== undefined)
        : undefined
      doneResult = await this.markDone(rewrite, cachedRecords, parentIndex, v1ShadowNeeded())
    }
    const wroteDone = doneResult === 'wrote'
    if (wroteDone) {
      rewrite.wroteDone = true
      if (!rewrite.useV2) {
        for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
      }
    }
    const done = finished || wroteDone
    const cleanupBackups = async (dropAll: boolean): Promise<Set<string>> => {
      const remainingBackups = new Set<string>()
      const allLeftoversWritten = rewrite.files.every(f => f.text === null)
      const reconciledMatchesBackup = (replacesName: string, replacesSize?: number, replacesFnv1a?: string): boolean =>
        getReconciledRefs().some(
          r =>
            r.name === replacesName &&
            (replacesSize === undefined || r.size === replacesSize) &&
            (replacesFnv1a === undefined || replacesFnv1a === '' || r.fnv1a === '' || r.fnv1a === replacesFnv1a),
        )
      let cachedEntries: readonly { name: string; kind?: string; size: number }[] | null | undefined
      let cachedEntriesCompanions: Map<string, { name: string; kind?: string; size: number }[]> | null = null
      let cachedLiveFiles: Map<string, number> | null = null
      let cachedGMarked: Set<string> | null = null
      const indexEntries = (list: readonly { name: string; kind?: string; size: number }[]): {
        companions: Map<string, { name: string; kind?: string; size: number }[]>
        liveFiles: Map<string, number>
        gMarked: Set<string>
      } => {
        const companions = groupCompanions(list)
        const liveFiles = new Map<string, number>()
        const gMarked = new Set<string>()
        for (const entry of list) {
          if (entry.kind !== undefined && entry.kind !== 'file') continue
          if (entry.size <= 0) continue
          if (kindOf(entry.name) !== null) {
            liveFiles.set(entry.name, entry.size)
          } else {
            const of = recordOf(entry.name)
            const by = of !== null ? doneMarkerOf(entry.name, of) : null
            if (of !== null && by !== null && by.endsWith('g')) gMarked.add(of)
          }
        }
        return { companions, liveFiles, gMarked }
      }
      const getEntriesSnapshot = async (): Promise<readonly { name: string; kind?: string; size: number }[] | null> => {
        if (cachedEntries !== undefined) return cachedEntries
        try {
          cachedEntries = await this.io.listDir(rewrite.dir)
          const indexed = indexEntries(cachedEntries)
          cachedEntriesCompanions = indexed.companions
          cachedLiveFiles = indexed.liveFiles
          cachedGMarked = indexed.gMarked
        } catch {
          cachedEntries = null
          cachedEntriesCompanions = null
          cachedLiveFiles = null
          cachedGMarked = null
        }
        return cachedEntries
      }
      const isBackupReplacesAccounted = async (
        replacesName: string | undefined,
        replacesSize: number | undefined,
        replacesFnv1a: string | undefined,
        entriesSnapshot?: readonly { name: string; kind?: string; size: number }[],
        companionsSnapshot?: ReadonlyMap<string, readonly { name: string; kind?: string; size: number }[]>,
        liveFilesSnapshot?: ReadonlyMap<string, number>,
      ): Promise<boolean> => {
        if (replacesName === undefined || replacesName === '') return false
        if (allLeftoversWritten && (done || v1ShadowNeeded()) && reconciledMatchesBackup(replacesName, replacesSize, replacesFnv1a)) return true
        if (!dropAll) return false
        // On full retirement (`dropAll`), verify `replacesName` is still live
        // and listed in our deliverables (`files`/`adopted`) OR has its own
        // matching retired completion marker before dropping its backup, so a
        // child concurrently emptied unsent by a legacy writer is never lost.
        const dirEntries = entriesSnapshot ?? (await getEntriesSnapshot())
        if (dirEntries === null) return false
        const companionsMap = companionsSnapshot ?? cachedEntriesCompanions
        const liveFilesMap = liveFilesSnapshot ?? cachedLiveFiles
        try {
          const liveSize = liveFilesMap !== null ? (liveFilesMap.get(replacesName) ?? 0) : 0
          if (
            liveSize > 0 &&
            (replacesSize === undefined || liveSize === replacesSize) &&
            getRewriteNames().includes(replacesName)
          ) {
            if (replacesFnv1a === undefined || replacesFnv1a === '') return true
            const rawLive = await this.io.readFile(joinPath(rewrite.dir, replacesName))
            if (rawLive !== '' && utf8Length(rawLive) === liveSize && fnv1a(rawLive) === replacesFnv1a) {
              return true
            }
          }
          const childDone = await this.hasMatchingDoneMark(
            { ...sourceFor(rewrite.dir, replacesName, replacesSize ?? -1, c), live: false },
            companionsMap !== null ? (companionsMap.get(replacesName) ?? []) : dirEntries,
            replacesFnv1a !== undefined && replacesFnv1a !== '' ? replacesFnv1a : undefined,
          )
          if (!childDone.torn && childDone.matched) return true
        } catch {
          return false
        }
        return false
      }
      for (const b of getBackupRefs()) {
        const eligible = await isBackupReplacesAccounted(b.replaces, b.replacesSize, b.replacesFnv1a)
        if (!eligible) {
          remainingBackups.add(b.name)
          continue
        }
        const res = await safeTruncateStaged({ name: b.name, size: b.size, fnv1a: b.fnv1a }, 'g')
        if (res === 'retry') remainingBackups.add(b.name)
      }
      // On full v2/forced retirement, also best-effort empty any live
      // standalone backup files left behind by prior crashed attempts for this
      // exact parent/size (inspecting only `g`-suffixed guard marks, verifying
      // the replaced child is accounted for, and never modifying another
      // writer's mark file) so `0.1.3 force` cannot later resend superseded backups.
      if (dropAll && (rewrite.useV2 || rewrite.force || getBackupRefs().length > 0)) {
        const ownBackupNames = new Set(getBackupNames())
        try {
          const entries = (await getEntriesSnapshot()) ?? (await this.io.listDir(rewrite.dir))
          const indexed =
            cachedEntriesCompanions !== null && cachedLiveFiles !== null && cachedGMarked !== null
              ? { companions: cachedEntriesCompanions, liveFiles: cachedLiveFiles, gMarked: cachedGMarked }
              : indexEntries(entries)
          const companionsMap = indexed.companions
          const liveFiles = indexed.liveFiles
          const gMarked = indexed.gMarked
          for (const liveName of gMarked) {
            const liveSize = liveFiles.get(liveName) ?? 0
            if (liveSize <= 0 || liveName === rewrite.name || ownBackupNames.has(liveName) || liveSize > HOST_FILE_BYTES) continue
            try {
              const m = await this.marksDetailed(sourceFor(rewrite.dir, liveName, liveSize, c), companionsMap.get(liveName) ?? [])
              if (
                m.backup === true &&
                m.stagedFor === rewrite.name &&
                m.stagedForSize === rewrite.size &&
                m.nonStagedBy.length === 0 &&
                m.backupFnv1a !== undefined &&
                m.backupFnv1a !== '' &&
                (await isBackupReplacesAccounted(m.replaces, m.backupForSize, m.backupForFnv1a, entries, companionsMap, liveFiles))
              ) {
                const livePath = joinPath(rewrite.dir, liveName)
                const lease = (await this.io.storeGet(`lease:${dest}/${liveName}`)) as { by?: string; holder?: string; until?: number } | undefined
                if (
                  lease &&
                  typeof lease.until === 'number' &&
                  lease.until > this.io.now() &&
                  (lease.by !== me || typeof lease.holder !== 'string' || lease.holder === '' || lease.holder !== this.mark)
                ) {
                  continue
                }
                const cur = await this.io.readFile(livePath)
                if (cur !== '' && utf8Length(cur) === liveSize && fnv1a(cur) === m.backupFnv1a) {
                  if (await this.emptyCopy(livePath)) {
                    this.retiredEmpty.add(livePath)
                    parentIndex?.delete(liveName)
                    emptiedInRun?.add(liveName)
                    this.deleteHeldPath(livePath)
                    this.currentScanHeld.delete(livePath)
                    try {
                      await this.io.writeFile(
                        doneFileOf(rewrite.dir, liveName, `${this.mark}g`),
                        journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
                      )
                    } catch {
                      // Best-effort retired marker for emptied prior-attempt backup.
                    }
                  }
                }
              }
            } catch {
              // Best-effort cleanup of prior-attempt backup.
            }
          }
        } catch {
          // Best-effort scan.
        }
      }
      return remainingBackups
    }
    const cleanupReconciledAndStaged = async (
      releaseRepopulatedChildGuards: boolean,
    ): Promise<{
      allReconciledEmptied: boolean
      remainingStaged: Set<string>
      unemptiedReconciled: Set<string>
      keepReconciledGuards: Set<string>
    }> => {
      let allReconciledEmptied = true
      const unemptiedReconciled = new Set<string>()
      const keepReconciledGuards = new Set<string>()
      const writeReconciledEmptyRetired = async (name: string): Promise<void> => {
        try {
          await this.io.writeFile(
            doneFileOf(rewrite.dir, name, `${this.mark}d`),
            journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
          )
        } catch {
          // Best-effort size-0 retired marker beside emptied reconciled child.
        }
      }
      for (const ref of getReconciledRefs()) {
        const targetPath = joinPath(rewrite.dir, ref.name)
        try {
          const exists = this.io.exists === undefined ? true : await this.io.exists(targetPath)
          const current = exists ? await this.io.readFile(targetPath) : ''
          if (current === '') {
            this.retiredEmpty.add(targetPath)
            this.deleteHeldPath(targetPath)
            this.currentScanHeld.delete(targetPath)
            // Keep the ref.size-pinned `childGuard` mark on an emptied
            // reconciled child so a late write restoring its exact bytes is
            // held by 0.1.3 and dropped by 0.1.4, and write a size-0 retired
            // marker on `${this.mark}d` so both 0.1.3 and 0.1.4 suppress
            // empty-waiting notices and expire the child guard cleanly at 24h.
            if (exists) await writeReconciledEmptyRetired(ref.name)
            keepReconciledGuards.add(ref.name)
            continue
          }
          if (utf8Length(current) !== ref.size || fnv1a(current) !== ref.fnv1a) {
            allReconciledEmptied = false
            const repopulated = parseFile(current)
            const repopulatedOtherParent =
              repopulated.complete && (repopulated.parent === null || baseOf(repopulated.parent) !== rewrite.name)
            if (releaseRepopulatedChildGuards || repopulatedOtherParent) await clearChildGuardMarks(new Set([ref.name]))
            else unemptiedReconciled.add(ref.name)
            continue
          }
        } catch {
          allReconciledEmptied = false
          unemptiedReconciled.add(ref.name)
          continue
        }
        if (await this.emptyCopy(targetPath)) {
          this.retiredEmpty.add(targetPath)
          parentIndex?.delete(ref.name)
          emptiedInRun?.add(ref.name)
          this.deleteHeldPath(targetPath)
          this.currentScanHeld.delete(targetPath)
          await writeReconciledEmptyRetired(ref.name)
          keepReconciledGuards.add(ref.name)
        } else {
          allReconciledEmptied = false
          unemptiedReconciled.add(ref.name)
        }
      }
      const remainingStaged = new Set<string>()
      for (const stagedRef of getStagedRefs()) {
        const res = await safeTruncateStaged(stagedRef, '')
        if (res === 'retry') remainingStaged.add(stagedRef.name)
      }
      return { allReconciledEmptied, remainingStaged, unemptiedReconciled, keepReconciledGuards }
    }
    if (!done && (rewrite.useV2 || doneResult === 'failed-keep')) {
      if (doneResult === 'shadow-wrote') {
        const { allReconciledEmptied, remainingStaged } = await cleanupReconciledAndStaged(false)
        if (allReconciledEmptied && remainingStaged.size === 0) {
          // Only drop backups for reconciled children (whose replacements were
          // materialized standalone with `parent: null` under `v1ShadowNeeded()`);
          // keep backups for any unmodified `parent: P` adopted children until
          // `P` actually retires on disk.
          await cleanupBackups(false)
          const promotedNames = new Set(getRewriteNames())
          await clearTempMarks(promotedNames)
        }
      } else if (doneResult === 'failed-keep' && v1ShadowNeeded() && rewrite.files.every(f => f.text === null)) {
        // Empty unchanged reconciled children and abandoned staged parts first;
        // drop reconciled-child backups and release promoted standalone
        // `tempMarks` ONLY once every reconciled child is confirmed empty (never
        // on digest mismatch / repopulation) and no abandoned staged part
        // remains un-emptied. Because `v1ShadowNeeded()` forced standalone
        // (`parent: null`) materialization of parent leftovers and reconciled
        // replacements, `cleanupBackups(false)` empties only reconciled-child
        // backups while keeping backups for any unmodified `parent: P` adopted
        // children until `P` retires on disk. When stale v1 still has unwritten
        // files (or is torn), keep `tempMarks` on `rewrite.files` so 0.1.3
        // resuming stale v1 cannot duplicate parent leftovers. Keep `childGuard`
        // marks on unmodified adopted children so 0.1.3 resuming stale v1 never
        // truncates them.
        const { allReconciledEmptied, remainingStaged } = await cleanupReconciledAndStaged(false)
        if (allReconciledEmptied && remainingStaged.size === 0) {
          await cleanupBackups(false)
          const v1AllWritten =
            stuckV1Journal !== 'torn' && (stuckV1Journal === undefined || writtenOf(stuckV1Journal).every(Boolean))
          const releasableNames = new Set(v1AllWritten ? getRewriteNames() : getAdoptedNames())
          await clearTempMarks(releasableNames)
        }
      }
      this.recordError(`could not record completion mark for ${path}; keeping parent sending mark until retried`)
      return false
    }
    try {
      if (rewrite.size >= 0) {
        const currentRaw = await this.io.readFile(path)
        if (
          currentRaw !== '' &&
          (utf8Length(currentRaw) !== rewrite.size || (rewrite.sourceFnv1a !== undefined && fnv1a(currentRaw) !== rewrite.sourceFnv1a))
        ) {
          this.rewrites.delete(path)
          this.recordError(`${path} changed content before it could be emptied; keeping newer content for the next replay`)
          return false
        }
      }
      await this.io.writeFile(path, '')
      parentIndex?.delete(rewrite.name)
    } catch (err) {
      this.recordError(`could not empty ${path} after its replay (its rows landed or are in newer files): ${errorText(err)}`)
      return false
    }
    // Known done from here on, whether or not its done mark was written: the
    // next replay does not wait on it as on a file still being written. Clear
    // standing sending-mark tracking before store cleanup so a stall during
    // storeDelete or unmark never reports a retired source as held.
    this.retiredEmpty.add(path)
    this.rewrites.delete(path)
    this.uncertainSources.delete(path)
    this.deleteHeldPath(path)
    this.currentScanHeld.delete(path)
    for (const w of [...this.before, this]) w.activeSendingMarks.delete(path)
    if (done) {
      if (rewrite.useV2) await this.unmark(rewrite)
      if (!clearedOppositeSlot) clearedOppositeSlot = await clearOppositeSlot()
      const { remainingStaged, unemptiedReconciled, keepReconciledGuards } = await cleanupReconciledAndStaged(wroteDone)
      const remainingBackups = await cleanupBackups(wroteDone)
      const keepChildGuards = new Set([...remainingStaged, ...unemptiedReconciled, ...keepReconciledGuards])
      if (wroteDone) {
        const promotedNames = new Set(getRewriteNames().filter(n => !remainingStaged.has(n)))
        await clearTempMarks(promotedNames)
        await clearChildGuardMarks(undefined, keepChildGuards)
      } else if (finishedJournal !== undefined && Array.isArray(finishedJournal.files)) {
        const stagedSet = new Set(getStagedNames())
        const reconciledSet = new Set(normalizeReconciled(finishedJournal.reconciled).map(r => r.name))
        const ownPendingSet = new Set(getPendingRefs().map(r => r.name))
        const ownReconciledSet = new Set(getReconciledRefs().map(r => r.name))
        const safeToRelease = new Set(
          finishedJournal.files
            .filter((f): f is string => typeof f === 'string')
            .map(baseOf)
            .filter(
              n =>
                !stagedSet.has(n) &&
                !remainingStaged.has(n) &&
                !reconciledSet.has(n) &&
                !ownPendingSet.has(n) &&
                !ownReconciledSet.has(n),
            ),
        )
        await clearTempMarks(safeToRelease)
        if (!v1ShadowNeeded()) await clearChildGuardMarks(safeToRelease, keepChildGuards)
      }
      for (const key of [rewrite.recordKey, rewrite.v2RecordKey]) {
        try {
          const stored = await readStoreSlot(key)
          if (stored === undefined && !neutralizedKeys.has(key)) continue
          if (
            key === rewrite.v2RecordKey &&
            (remainingStaged.size > 0 || remainingBackups.size > 0 || (!wroteDone && (unemptiedReconciled.size > 0 || getPendingRefs().length > 0)))
          ) {
            continue
          }
          if (stored !== undefined && (await protectsLiveChild(stored))) continue
          if (stored !== undefined && stored !== 'torn' && stored.size === rewrite.size && key === activeKey && !wroteDone) {
            const listed = await dirListing()
            if (listed === null) continue
            const hasDone = [...listed.entries()].some(([n, sz]) => sz > 0 && doneMarkerOf(n, rewrite.name) !== null)
            const hasLiveChild = Array.isArray(stored.files) && stored.files.some(f => typeof f === 'string' && (listed.get(baseOf(f)) ?? 0) > 0)
            if (!hasDone && hasLiveChild) continue
          }
          await this.io.storeDelete(key)
          cachedStore.set(key, undefined)
          neutralizedKeys.delete(key)
        } catch {
          // Leave store key when inspection fails.
        }
      }
    }
    return true
  }

  // A record as writeOut weighs it: a replay's mark is no outcome.
  private static recorded(found: Found | Pending | 'torn' | 'conflict' | undefined): Found | 'torn' | 'conflict' | undefined {
    return found !== undefined && found !== 'torn' && found !== 'conflict' && 'pending' in found ? undefined : found
  }

  // Every file of what is left written: the mark that the file is done, its
  // record marked `retired`, in this writer's own file beside it
  // (`doneFileOf`), written before the file is emptied. On versioned (`v2`)
  // splits, write under `${this.mark}d` so an interrupted or truncated
  // `markDone` never zeros the standing `outcome-done-${this.mark}-` sending
  // guard that 0.1.3 readers rely on; `writeOut` releases the sending mark
  // only after `markDone` and source truncation succeed. Sequence numbers
  // advance strictly past all prior retired done marks and file records of
  // any size beside `rewrite.name` so 0.1.3 readers never pick an older
  // generation's mark across sizes. No other writer writes that file, so no
  // late write of another session's replay takes it away.
  private async markDone(
    rewrite: Rewrite,
    storeRecords?: readonly (Partial<Journal> | 'torn')[],
    parentIndex?: RunParentIndex,
    fallbackV1Shadow = false,
  ): Promise<'wrote' | 'shadow-wrote' | 'failed-keep' | 'failed-empty-ok'> {
    const anySizeSource: Source = { ...rewrite, size: -1, fnv1a: undefined, strictFnv1a: undefined, live: false }
    const currentFiles = [...rewrite.files.map(f => baseOf(f.path)), ...(rewrite.adoptedFiles ?? [])]
    const seenFiles = new Set(currentFiles)
    const carriedChildren: string[] = []
    const entryNames = new Set<string>()
    const liveSizes = new Map<string, number>()
    let safeWithoutDoneMark = false
    try {
      const entries = await this.io.listDir(rewrite.dir)
      for (const e of entries) {
        entryNames.add(e.name)
        if ((e.kind === undefined || e.kind === 'file') && e.size > 0) liveSizes.set(e.name, e.size)
      }
      // Never carry aborted staged replacement files or pre-dispatch backups into the retired mark.
      for (const staged of rewrite.stagedFiles ?? []) {
        if (staged.name !== '') seenFiles.add(baseOf(staged.name))
      }
      for (const backup of rewrite.backupFiles ?? []) {
        if (backup.name !== '') seenFiles.add(baseOf(backup.name))
      }
      // Only suppress a reconciled filename from `carriedChildren` when the
      // file is empty or still matches the recorded `{ size, fnv1a }` digest.
      // If a newer generation repopulated the same name and still names this
      // parent, preserve it in `carriedChildren` so `parentFate` never drops it.
      for (const ref of rewrite.reconciledFiles ?? []) {
        if (seenFiles.has(ref.name)) continue
        const curSize = liveSizes.get(ref.name)
        if (curSize === undefined) {
          seenFiles.add(ref.name)
          continue
        }
        const curRaw = await this.io.readFile(joinPath(rewrite.dir, ref.name))
        if (curRaw === '' || (utf8Length(curRaw) === ref.size && fnv1a(curRaw) === ref.fnv1a)) {
          seenFiles.add(ref.name)
        } else {
          const parsedNew = parseFile(curRaw)
          if (parsedNew.complete && parsedNew.parent !== null && baseOf(parsedNew.parent) === rewrite.name) {
            seenFiles.add(ref.name)
            carriedChildren.push(ref.name)
          }
        }
      }
      const doneAll = await this.doneMarksAll(anySizeSource, entries)
      let maxSeq = rewrite.seq
      let sawTorn = doneAll.torn
      const priorJournals: Partial<Journal>[] = [...doneAll.records]
      const addStoreEntry = (val: unknown): void => {
        if (isClearedSlot(val)) return
        if (val === 'torn' || typeof val !== 'object' || val === null || Array.isArray(val)) {
          sawTorn = true
          return
        }
        priorJournals.push(val as Partial<Journal>)
      }
      if (storeRecords !== undefined) {
        for (const val of storeRecords) addStoreEntry(val)
      } else {
        for (const key of [rewrite.recordKey, rewrite.v2RecordKey]) {
          addStoreEntry(await this.io.storeGet(key))
        }
      }
      for (const file of [...rewrite.recordFiles, ...rewrite.v2RecordFiles]) {
        if (!entryNames.has(baseOf(file))) continue
        const text = await this.io.readFile(file)
        if (text === '') continue
        const kept = journalOf(text)
        if (kept === 'torn') sawTorn = true
        else if (kept !== undefined) priorJournals.push(kept)
      }
      for (const r of priorJournals) {
        maxSeq = Math.max(maxSeq, seqOf(r))
        const differentDigest =
          rewrite.sourceFnv1a !== undefined &&
          typeof r.sourceFnv1a === 'string' &&
          r.sourceFnv1a !== '' &&
          r.sourceFnv1a !== rewrite.sourceFnv1a
        const supersededDifferentV1 =
          (rewrite.supersedesV1 !== undefined && matchesSupersededV1(r, rewrite.supersedesV1)) ||
          (rewrite.sourceFnv1a !== undefined &&
            (typeof r.sourceFnv1a !== 'string' || r.sourceFnv1a === '') &&
            (r.fates !== rewrite.fates || !sameNames(Array.isArray(r.files) ? r.files : [], currentFiles)))
        if (r.size !== rewrite.size || r.retired === true || differentDigest || supersededDifferentV1) {
          const priorReconciled = new Map(normalizeReconciled(r.reconciled).map(rec => [rec.name, rec]))
          const candidates: string[] = []
          if (Array.isArray(r.files)) {
            for (const entry of r.files) {
              if (typeof entry === 'string') candidates.push(baseOf(entry))
            }
          }
          for (const pend of normalizeReconciled(r.pendingReconcile, true)) {
            if (pend.name !== '') candidates.push(baseOf(pend.name))
          }
          for (const base of candidates) {
            if (base === '' || base === rewrite.name || seenFiles.has(base) || !liveSizes.has(base)) continue
            const rec = priorReconciled.get(base)
            if (rec !== undefined) {
              const curRaw = await this.io.readFile(joinPath(rewrite.dir, base))
              if (curRaw === '' || (utf8Length(curRaw) === rec.size && fnv1a(curRaw) === rec.fnv1a)) {
                seenFiles.add(base)
                continue
              }
            }
            seenFiles.add(base)
            carriedChildren.push(base)
          }
        }
      }
      if (sawTorn) {
        const tornCompanions = groupCompanions(entries)
        for (const [n, sz] of liveSizes) {
          if (n === rewrite.name || seenFiles.has(n) || kindOf(n) === null) continue
          const info = await this.readParentInfo(rewrite.dir, n, sz, parentIndex)
          if (info.state === 'oversized' || info.state === 'error') return 'failed-keep'
          if (info.state === 'ok' && info.complete && info.parent === rewrite.name) {
            const m = await this.marksDetailed({ ...rewrite, name: n, size: sz, live: true }, tornCompanions.get(n) ?? [])
            if (m.backup === true && m.replaces === rewrite.name) continue
            seenFiles.add(n)
            carriedChildren.push(n)
          }
        }
      }
      // Without a durable retired done mark, emptying a v1 parent is only safe
      // when no prior/torn done mark shadows the v1 journal and every existing
      // store/file record already lists all current children (so 0.1.3 readers
      // inspecting the emptied parent never drop a newly written child).
      const allPriorListCurrent = priorJournals.every(
        r => Array.isArray(r.files) && currentFiles.every(f => r.files!.some(e => typeof e === 'string' && baseOf(e) === f)),
      )
      safeWithoutDoneMark =
        !rewrite.useV2 && doneAll.records.length === 0 && !sawTorn && carriedChildren.length === 0 && allPriorListCurrent
      rewrite.seq = maxSeq + 1
    } catch {
      return 'failed-keep'
    }
    const deliverableFiles = [...currentFiles, ...carriedChildren]
    const reconciledMarks = (rewrite.reconciledFiles ?? []).filter(r => !currentFiles.includes(r.name) && !carriedChildren.includes(r.name))
    // Keep reconciled basenames in `files` alongside exact `{ size, fnv1a }`
    // digests in `reconciled`: 0.1.3 readers then never drop a child repopulated
    // after retirement, while 0.1.4 readers revalidate the digest to drop an
    // unchanged stale copy and deliver a repopulated one.
    const allFiles = [...deliverableFiles, ...reconciledMarks.map(r => r.name)]
    if (deliverableFiles.some(n => kindOf(n) !== 'rejected')) rewrite.waits = true
    const { size, waits, seq } = rewrite
    const { fates, reasons, refusals } = rewrite
    const journal: Journal = {
      fates,
      reasons,
      refusals,
      size,
      ...(rewrite.sourceFnv1a ? { sourceFnv1a: rewrite.sourceFnv1a } : {}),
      waits,
      seq,
      files: allFiles,
      written: allFiles.map(() => true),
      retired: true,
      retiredAt: this.io.now(),
      ...(reconciledMarks.length > 0 ? { reconciled: reconciledMarks } : {}),
      ...(rewrite.guardAttempt ? { attempt: rewrite.guardAttempt } : {}),
      ...(rewrite.guardAttempts && rewrite.guardAttempts.length > 0 ? { attempts: rewrite.guardAttempts } : {}),
      ...(rewrite.stampedIds !== undefined ? { stampedIds: rewrite.stampedIds } : {}),
    }
    try {
      await this.io.writeFile(doneFileOf(rewrite.dir, rewrite.name, rewrite.useV2 ? `${this.mark}d` : this.mark), journalText(journal))
      return 'wrote'
    } catch {
      // If a stale v1 store entry could neither be deleted nor neutralized and
      // writing `outcome-done-<mark>d-` failed, shadow stale v1 in an available
      // v1 companion file (`outcome-` / `outcome-b-`) with `retired: true` and
      // `allFiles` so 0.1.3 readers prefer this retired record over stale v1.
      if (rewrite.useV2 && fallbackV1Shadow) {
        for (const candidate of rewrite.recordFiles) {
          try {
            const existingText = !entryNames.has(baseOf(candidate)) ? '' : await this.io.readFile(candidate)
            if (existingText !== '') {
              const existing = journalOf(existingText)
              if (existing === 'torn') continue
              if (existing !== undefined && existing.retired === true) {
                if (
                  existing.size === rewrite.size &&
                  existing.fates === rewrite.fates &&
                  sameNames(Array.isArray(existing.files) ? existing.files : [], allFiles)
                ) {
                  return 'shadow-wrote'
                }
                continue
              }
              if (
                existing !== undefined &&
                existing.size !== rewrite.size &&
                Array.isArray(existing.files) &&
                existing.files.some(f => typeof f === 'string' && liveSizes.has(baseOf(f)))
              ) {
                continue
              }
            }
            await this.io.writeFile(candidate, journalText(journal))
            return 'shadow-wrote'
          } catch {
            // Try next v1 companion slot.
          }
        }
      }
      return safeWithoutDoneMark ? 'failed-empty-ok' : 'failed-keep'
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
  private async markSending(
    path: string,
    source: Source,
    trigger: BackfillReport['trigger'],
    report: BackfillReport,
    supersededUnaccounted?: Partial<Journal>,
    allowStagedFor?: string,
  ): Promise<boolean> {
    const ownUncertain = this.uncertainSources.get(path) === source.size
    if (trigger !== 'force' && ownUncertain) {
      this.noteHeld(
        report,
        trigger,
        `a replay of ${source.name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
        path,
      )
      return false
    }
    const donePath = doneFileOf(source.dir, source.name, this.mark)
    this.activeSendingMarks.set(path, { path, name: source.name, dir: source.dir, trigger, size: source.size })
    try {
      const markPayload = { size: source.size, sending: true, ...(this.exitReplay !== null ? { uncertain: true } : {}), by: this.mark }
      await this.io.writeFile(donePath, sealFile(`${JSON.stringify(markPayload)}\n`))
    } catch (err) {
      const released = await this.unmark(source)
      if (!released) {
        try {
          const remaining = await this.io.readFile(donePath)
          if (remaining === '') this.activeSendingMarks.delete(path)
        } catch {
          // Keep activeSendingMarks on unreadable/failed release so exit notice fails closed.
        }
      }
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
      if (
        trigger === 'force' &&
        found !== undefined &&
        found !== 'torn' &&
        found !== 'conflict' &&
        Writer.sameUnaccounted(found.journal, supersededUnaccounted)
      ) {
        found = undefined
      }
      const isOther = (by: string): boolean => by !== this.mark || ownUncertain
      let others: string[] = []
      let othersActive: string[] = []
      let othersCertain: string[] = []
      let otherUncertain = ownUncertain
      if (read === 'torn' || read === 'conflict') {
        const detailed = await this.marksDetailed(source)
        others = detailed.by.filter(isOther)
        othersActive = detailed.activeBy.filter(by => by !== this.mark)
        othersCertain = detailed.nonUncertainBy.filter(by => by !== this.mark)
        otherUncertain ||= detailed.uncertain
        const last = await this.doneMarks(source, await this.io.listDir(source.dir))
        if (last.done !== undefined) found = { journal: last.done, filed: true }
      } else if (read !== undefined && 'pending' in read) {
        const ignoreStaged =
          allowStagedFor !== undefined &&
          read.stagedFor === allowStagedFor &&
          read.uncertain !== true &&
          (read.nonStagedBy ?? []).every(by => by === this.mark)
        others = (ignoreStaged ? (read.nonStagedBy ?? []) : read.pending).filter(isOther)
        othersActive = (ignoreStaged ? (read.nonStagedBy ?? []) : (read.activeBy ?? [])).filter(by => by !== this.mark)
        othersCertain = (ignoreStaged ? (read.nonStagedBy ?? []) : (read.nonUncertainBy ?? [])).filter(by => by !== this.mark)
        otherUncertain ||= read.uncertain === true
      }
      const another = trigger !== 'force' && others.length > 0
      const conflicted = trigger !== 'force' && found === 'conflict'
      if ((found !== undefined && found !== 'torn' && found !== 'conflict') || another || conflicted || (await this.io.readFile(path)) === '') {
        if (conflicted) {
          this.noteHeld(
            report,
            trigger,
            `conflicting replay records exist for ${source.name}; /bqaa-backfill force replays the file afresh`,
            path,
            othersActive.length > 0,
            othersCertain.length > 0,
          )
        } else if (another && (found === undefined || found === 'torn' || found === 'conflict')) {
          const hasForeignActive = othersActive.length > 0
          const hasCertainActive = othersCertain.length > 0
          const msg =
            !hasCertainActive && otherUncertain
              ? `a replay of ${source.name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`
              : `another session is replaying ${source.name}; if it stopped, /bqaa-backfill force replays the file afresh`
          this.noteHeld(report, trigger, msg, path, hasForeignActive, hasCertainActive)
        }
        await this.unmark(source)
        return false
      }
      return true
    } catch (err) {
      await this.unmark(source)
      throw err
    }
  }

  private async hasActiveForeignMark(
    source: Source,
    entries?: readonly { name: string; kind?: string; size: number }[],
  ): Promise<{ mayBeActive: boolean; activeCertain: boolean }> {
    try {
      const { activeBy, nonUncertainBy } = await this.marksDetailed(source, entries)
      return {
        mayBeActive: activeBy.some(by => by !== this.mark),
        activeCertain: nonUncertainBy.some(by => by !== this.mark),
      }
    } catch {
      // Cannot inspect standing marks while holding on torn/conflict: keep
      // the owner-stopped + lease-expired qualification on headless notices.
      return { mayBeActive: true, activeCertain: true }
    }
  }

  // The writers whose marks stand beside `source`, read from its directory
  // (`entries`, when it was just listed), plus `activeBy` (standing marks from
  // non-self sessions including pre-dispatch uncertain and torn marks) and
  // `nonUncertainBy` (complete non-uncertain marks whose owner may still be
  // live). An emptied mark, which its writer let go, is not read. A mark that
  // cannot be read, or a directory that cannot be listed, throws: it is never
  // taken for none.
  private async marksDetailed(
    source: Source,
    entries?: readonly { name: string; kind?: string; size: number }[],
  ): Promise<{
    by: string[]
    activeBy: string[]
    nonUncertainBy: string[]
    nonStagedBy: string[]
    uncertain: boolean
    stagedFor?: string
    stagedForSize?: number
    childGuard?: boolean
    replaces?: string
    attempts?: string[]
    backup?: boolean
    backupForSize?: number
    backupForFnv1a?: string
    backupFnv1a?: string
  }> {
    const by: string[] = []
    const activeBy: string[] = []
    const nonUncertainBy: string[] = []
    const nonStagedBy: string[] = []
    let uncertain = false
    let stagedFor: string | undefined
    let stagedForSize: number | undefined
    let childGuard = false
    let replaces: string | undefined
    let replacesConflict = false
    let stagedConflict = false
    const attemptsSet = new Set<string>()
    let backup = false
    let backupForSize: number | undefined
    let backupForSizeConflict = false
    let backupForFnv1a: string | undefined
    let backupForFnv1aConflict = false
    let backupFnv1a: string | undefined
    let backupFnv1aConflict = false
    for (const entry of entries ?? (await this.io.listDir(source.dir))) {
      if (entry.kind !== undefined && entry.kind !== 'file') continue
      const writer = markerOf(entry.name, source.name)
      if (writer === null || entry.size === 0) continue
      const raw = await this.io.readFile(joinPath(source.dir, entry.name))
      if (markStands(raw, source.size)) {
        by.push(writer)
        activeBy.push(writer)
        const parsed = journalOf(raw) as
          | {
              uncertain?: unknown
              stagedFor?: unknown
              stagedForSize?: unknown
              childGuard?: unknown
              replaces?: unknown
              attempt?: unknown
              backup?: unknown
              backupForSize?: unknown
              backupForFnv1a?: unknown
              backupFnv1a?: unknown
            }
          | 'torn'
          | undefined
        if (parsed !== undefined && parsed !== 'torn') {
          const isStagedMark = typeof parsed.stagedFor === 'string' && parsed.stagedFor !== ''
          if (parsed.uncertain === true) uncertain = true
          else if (!isStagedMark) nonUncertainBy.push(writer)
          if (isStagedMark) {
            const p = baseOf(parsed.stagedFor as string)
            if (stagedFor === undefined) stagedFor = p
            else if (stagedFor !== p) stagedConflict = true
            if (typeof parsed.stagedForSize === 'number' && Number.isFinite(parsed.stagedForSize) && parsed.stagedForSize > 0) {
              if (stagedForSize === undefined) stagedForSize = parsed.stagedForSize
              else if (stagedForSize !== parsed.stagedForSize) stagedConflict = true
            }
            if (parsed.childGuard === true) childGuard = true
            if (typeof parsed.replaces === 'string' && parsed.replaces !== '') {
              const rep = baseOf(parsed.replaces)
              if (replaces === undefined) replaces = rep
              else if (replaces !== rep) replacesConflict = true
            }
            if (typeof parsed.attempt === 'string' && parsed.attempt !== '') attemptsSet.add(parsed.attempt)
            if (parsed.backup === true) backup = true
            if (typeof parsed.backupForSize === 'number' && Number.isFinite(parsed.backupForSize) && parsed.backupForSize > 0) {
              if (backupForSize === undefined) backupForSize = parsed.backupForSize
              else if (backupForSize !== parsed.backupForSize) backupForSizeConflict = true
            }
            if (typeof parsed.backupForFnv1a === 'string' && parsed.backupForFnv1a !== '') {
              if (backupForFnv1a === undefined) backupForFnv1a = parsed.backupForFnv1a
              else if (backupForFnv1a !== parsed.backupForFnv1a) backupForFnv1aConflict = true
            }
            if (typeof parsed.backupFnv1a === 'string' && parsed.backupFnv1a !== '') {
              if (backupFnv1a === undefined) backupFnv1a = parsed.backupFnv1a
              else if (backupFnv1a !== parsed.backupFnv1a) backupFnv1aConflict = true
            }
          } else {
            nonStagedBy.push(writer)
          }
        } else {
          nonStagedBy.push(writer)
        }
      }
    }
    const cleanStagedFor = stagedConflict ? undefined : stagedFor
    const cleanStagedForSize = stagedConflict ? undefined : stagedForSize
    const cleanReplaces = stagedConflict || replacesConflict ? undefined : replaces
    const cleanAttempts = stagedConflict || attemptsSet.size === 0 ? undefined : [...attemptsSet]
    const cleanBackupForSize = stagedConflict || backupForSizeConflict ? undefined : backupForSize
    const cleanBackupForFnv1a = stagedConflict || backupForFnv1aConflict ? undefined : backupForFnv1a
    const cleanBackupFnv1a = stagedConflict || backupFnv1aConflict ? undefined : backupFnv1a
    return {
      by,
      activeBy,
      nonUncertainBy,
      nonStagedBy,
      uncertain,
      ...(cleanStagedFor !== undefined ? { stagedFor: cleanStagedFor } : {}),
      ...(cleanStagedFor !== undefined && cleanStagedForSize !== undefined ? { stagedForSize: cleanStagedForSize } : {}),
      ...(cleanStagedFor !== undefined && childGuard ? { childGuard: true } : {}),
      ...(cleanStagedFor !== undefined && cleanReplaces !== undefined ? { replaces: cleanReplaces } : {}),
      ...(cleanStagedFor !== undefined && cleanAttempts !== undefined ? { attempts: cleanAttempts } : {}),
      ...(cleanStagedFor !== undefined && backup ? { backup: true } : {}),
      ...(cleanStagedFor !== undefined && backup && cleanBackupForSize !== undefined ? { backupForSize: cleanBackupForSize } : {}),
      ...(cleanStagedFor !== undefined && backup && cleanBackupForFnv1a !== undefined ? { backupForFnv1a: cleanBackupForFnv1a } : {}),
      ...(cleanStagedFor !== undefined && backup && cleanBackupFnv1a !== undefined ? { backupFnv1a: cleanBackupFnv1a } : {}),
    }
  }

  // Discovers every live child that a forced replay of `source` may reconcile,
  // adopt, or defer, so `placeChildGuards` can protect them with 0.1.3-honored
  // sending marks BEFORE `backfillFile` dispatches any HTTP request. Returns
  // `null` on any listing or read error so the caller fails closed.
  private async discoverLiveChildrenForForce(
    source: Source,
    c: Config,
    parentIndex?: RunParentIndex,
    includeUnreferencedChildren = false,
  ): Promise<{
    targets: Map<string, number>
    expectedParents: Set<string>
    entries: readonly { name: string; kind?: string; size: number }[]
  } | null> {
    let entries: readonly { name: string; kind?: string; size: number }[]
    try {
      entries = await this.io.listDir(source.dir)
    } catch {
      return null
    }
    const companionsBySource = groupCompanions(entries)
    const listed = new Map<string, number>()
    for (const e of entries) {
      if (e.kind === undefined || e.kind === 'file') listed.set(e.name, e.size)
    }
    const candidateJournals: (Partial<Journal> | 'torn')[] = []
    for (const key of [source.recordKey, source.v2RecordKey]) {
      try {
        const val = await this.io.storeGet(key)
        if (isClearedSlot(val)) continue
        if (typeof val === 'object' && val !== null && !Array.isArray(val)) candidateJournals.push(val as Partial<Journal>)
        else candidateJournals.push('torn')
      } catch {
        return null
      }
    }
    for (const file of [...source.recordFiles, ...source.v2RecordFiles]) {
      const fileName = baseOf(file)
      if ((listed.get(fileName) ?? 0) === 0) continue
      try {
        const txt = await this.io.readFile(file)
        if (txt !== '') {
          const kept = journalOf(txt)
          if (kept !== undefined) candidateJournals.push(kept)
        }
      } catch {
        return null
      }
    }
    try {
      const doneAll = await this.doneMarksAll({ ...source, live: false }, companionsBySource.get(source.name) ?? [])
      for (const d of doneAll.records) candidateJournals.push(d)
      if (doneAll.torn) candidateJournals.push('torn')
    } catch {
      return null
    }
    const candidateNames = new Set<string>()
    const backupNames = new Set<string>()
    const maxAdoptedSeq = new Map<string, number>()
    const maxStagedSeq = new Map<string, number>()
    let sawTorn = false
    for (const j of candidateJournals) {
      if (j === 'torn') {
        sawTorn = true
        continue
      }
      if (j.size !== source.size) continue
      const jSeq = seqOf(j)
      for (const staged of normalizeStaged(j.stagedFiles)) {
        maxStagedSeq.set(staged, Math.max(maxStagedSeq.get(staged) ?? -1, jSeq))
      }
      for (const b of normalizeBackups(j.backupFiles)) {
        backupNames.add(b.name)
      }
      if (Array.isArray(j.files)) {
        for (const f of j.files) {
          if (typeof f === 'string' && f !== '') {
            const base = baseOf(f)
            candidateNames.add(base)
            maxAdoptedSeq.set(base, Math.max(maxAdoptedSeq.get(base) ?? -1, jSeq))
          }
        }
      }
      if (Array.isArray(j.adopted)) {
        for (const f of j.adopted) {
          if (typeof f === 'string' && f !== '') {
            const base = baseOf(f)
            candidateNames.add(base)
            maxAdoptedSeq.set(base, Math.max(maxAdoptedSeq.get(base) ?? -1, jSeq))
          }
        }
      }
      for (const ref of normalizeReconciled(j.pendingReconcile, true)) candidateNames.add(ref.name)
      for (const ref of normalizeReconciled(j.reconciled)) candidateNames.add(ref.name)
    }
    const stagedNames = new Set<string>()
    for (const [staged, sSeq] of maxStagedSeq) {
      if (sSeq > (maxAdoptedSeq.get(staged) ?? -1)) stagedNames.add(staged)
    }
    if (sawTorn || includeUnreferencedChildren) {
      for (const [n, sz] of listed) {
        if (sz <= 0 || n === source.name || candidateNames.has(n) || stagedNames.has(n) || backupNames.has(n) || kindOf(n) === null || sz > HOST_FILE_BYTES) continue
        const info = await this.readParentInfo(source.dir, n, sz, parentIndex)
        if (info.state === 'error') return null
        if (info.state === 'ok' && info.complete && info.parent === source.name) candidateNames.add(n)
      }
    }
    const targets = new Map<string, number>()
    const expectedParents = new Set<string>()
    for (const childName of candidateNames) {
      if (childName === source.name || stagedNames.has(childName) || backupNames.has(childName)) continue
      const childSize = listed.get(childName) ?? 0
      if (childSize <= 0) continue
      let hasParentHeader = false
      if (childSize <= HOST_FILE_BYTES) {
        const info = await this.readParentInfo(source.dir, childName, childSize, parentIndex)
        if (info.state === 'error') return null
        if (info.state === 'ok' && info.complete && info.parent !== null && info.parent !== source.name) continue
        if (info.state === 'ok' && info.complete && info.parent === source.name) hasParentHeader = true
      }
      const childPath = joinPath(source.dir, childName)
      try {
        const childSource = sourceFor(source.dir, childName, childSize, c)
        const childCompanions = companionsBySource.get(childName) ?? []
        const childDone = await this.doneMarksAll(childSource, childCompanions)
        if (!childDone.torn && childDone.records.length > 0) {
          const hasExplicitDigest = childDone.records.some(r => typeof r.sourceFnv1a === 'string' && r.sourceFnv1a !== '')
          if (!hasExplicitDigest) continue
          const curRaw = await this.io.readFile(childPath)
          if (curRaw === '' || Writer.matchesDoneMarks(childDone.records, fnv1a(curRaw))) continue
        }
        const existingMarks = await this.marksDetailed(childSource, childCompanions)
        if (
          (existingMarks.stagedFor === source.name && existingMarks.childGuard !== true) ||
          (existingMarks.backup === true && existingMarks.replaces === source.name)
        ) {
          continue
        }
      } catch {
        return null
      }
      targets.set(childPath, childSize)
      if (hasParentHeader) expectedParents.add(childName)
    }
    return { targets, expectedParents, entries }
  }

  // Places 0.1.3-honored `outcome-done-${this.mark}g-<child>` sending guards on
  // each live child in `targets` before a forced parent replay sends rows or
  // checkpoints a v2 journal, binding each guard to `attempt` and collecting
  // any prior standing `childGuard` attempt tokens observed on those children.
  // Using a dedicated `g` suffix ensures guards never overwrite the writer's
  // own retired or uncertain marks on the child while still matching 0.1.3's
  // `markerOf` (`[0-9a-z]+`). On any write failure, rolls back newly placed
  // guards and returns `null` so the caller fails closed.
  private async placeChildGuards(
    dir: string,
    parentName: string,
    parentSize: number,
    targets: ReadonlyMap<string, number>,
    attempt: string,
    existing?: Map<string, number>,
    entries?: readonly { name: string; kind?: string; size: number }[],
  ): Promise<{ placed: Map<string, number>; newlyPlaced: string[]; prevMarks: Map<string, string>; priorAttempts: string[] } | null> {
    const placed = new Map<string, number>(existing ?? [])
    const newlyPlaced: string[] = []
    const prevMarks = new Map<string, string>()
    const priorAttemptsSet = new Set<string>()
    const guardMark = `${this.mark}g`
    try {
      const dirEntries = entries ?? (await this.io.listDir(dir))
      const companionsBySource = groupCompanions(dirEntries)
      for (const [childPath, childSize] of targets) {
        if (childSize <= 0) continue
        const childName = baseOf(childPath)
        for (const entry of companionsBySource.get(childName) ?? []) {
          if (entry.kind !== undefined && entry.kind !== 'file') continue
          if (entry.size === 0 || markerOf(entry.name, childName) === null) continue
          const rawExisting = await this.io.readFile(joinPath(dir, entry.name))
          if (!markStands(rawExisting, childSize)) continue
          const parsedExisting = journalOf(rawExisting) as
            | { stagedFor?: unknown; stagedForSize?: unknown; childGuard?: unknown; attempt?: unknown }
            | 'torn'
            | undefined
          if (
            parsedExisting !== undefined &&
            parsedExisting !== 'torn' &&
            parsedExisting.childGuard === true &&
            typeof parsedExisting.stagedFor === 'string' &&
            baseOf(parsedExisting.stagedFor) === parentName &&
            parsedExisting.stagedForSize === parentSize &&
            typeof parsedExisting.attempt === 'string' &&
            parsedExisting.attempt !== ''
          ) {
            priorAttemptsSet.add(parsedExisting.attempt)
          }
        }
        let prevMarkText = ''
        const markPath = doneFileOf(dir, childName, guardMark)
        if (this.io.exists === undefined || (await this.io.exists(markPath))) {
          const curRaw = await this.io.readFile(markPath)
          if (curRaw !== '' && markStands(curRaw, childSize)) {
            const parsed = journalOf(curRaw) as
              | { stagedFor?: unknown; stagedForSize?: unknown; childGuard?: unknown; attempt?: unknown }
              | 'torn'
              | undefined
            if (
              parsed !== undefined &&
              parsed !== 'torn' &&
              parsed.childGuard === true &&
              typeof parsed.stagedFor === 'string' &&
              baseOf(parsed.stagedFor) === parentName &&
              parsed.stagedForSize === parentSize
            ) {
              if (parsed.attempt === attempt) {
                placed.set(childPath, childSize)
                continue
              }
              prevMarkText = curRaw
            }
          }
        }
        prevMarks.set(childPath, prevMarkText)
        await this.io.writeFile(
          markPath,
          sealFile(
            `${JSON.stringify({ size: childSize, sending: true, by: guardMark, stagedFor: parentName, stagedForSize: parentSize, childGuard: true, attempt })}\n`,
          ),
        )
        placed.set(childPath, childSize)
        newlyPlaced.push(childPath)
      }
      return { placed, newlyPlaced, prevMarks, priorAttempts: [...priorAttemptsSet] }
    } catch {
      for (const childPath of newlyPlaced) {
        try {
          await this.io.writeFile(doneFileOf(dir, baseOf(childPath), guardMark), prevMarks.get(childPath) ?? '')
        } catch {
          // Best-effort rollback of newly placed child guards.
        }
      }
      return null
    }
  }

  // Durably stages a backup copy of each guarded `parent: parentName` child
  // BEFORE forced parent HTTP dispatch (or v2 persistence), guarded by a
  // `0.1.3`-honored sending mark carrying `stagedFor`, `replaces`,
  // `backup: true`, `backupForSize`, `backupForFnv1a`, `backupFnv1a`, and
  // `attempt`. Resolves child foreign-lease ownership BEFORE reading the child
  // and re-verifies every guarded child's live bytes (and lease) after staging
  // so an equal-length concurrent replacement is never backed up with stale
  // bytes. Treats confirmed absence of a previously recorded backup as an
  // uncovered part so recovery stages a fresh copy from the live child. For
  // clean legacy ID-less children (no prior done marks or v1/v2 records), sets
  // `parent: childName` on the backup so `0.1.3 force` in child-first order
  // drops the backup after retiring the original child instead of duplicating
  // null-ID rows. Groups companion entries by source basename on the first walk
  // so discovery stays O(directory size). On any failure, rolls back newly
  // staged backups and returns `{ ok: false, leasedChild?, mutatedChild? }`.
  private async stageChildBackups(
    dir: string,
    parentName: string,
    parentSize: number,
    c: Config,
    targets: ReadonlyMap<string, number>,
    attempt: string,
    existing?: readonly BackupFile[],
    entries?: readonly { name: string; kind?: string; size: number }[],
    expectedParents?: ReadonlySet<string>,
  ): Promise<
    | { ok: true; backups: BackupFile[]; newlyStaged: BackupFile[]; priorAttempts: string[] }
    | { ok: false; leasedChild?: string; mutatedChild?: string }
  > {
    const dest = destinationDir(tableRef(c))
    const me = this.sessionId() ?? 'unknown'
    const guardMark = `${this.mark}g`
    const backups: BackupFile[] = [...(existing ?? [])]
    const absentBackups = new Set<string>()
    const newlyStaged: BackupFile[] = []
    const priorAttemptsSet = new Set<string>()
    let dirEntriesCache: readonly { name: string; kind?: string; size: number }[] | null = entries ?? null
    let companionsBySource: Map<string, { name: string; kind?: string; size: number }[]> | null =
      entries !== undefined ? groupCompanions(entries) : null
    const ensureCompanions = async (): Promise<{
      dirEntries: readonly { name: string; kind?: string; size: number }[]
      companions: Map<string, { name: string; kind?: string; size: number }[]>
    }> => {
      if (dirEntriesCache === null) dirEntriesCache = await this.io.listDir(dir)
      if (companionsBySource === null) companionsBySource = groupCompanions(dirEntriesCache)
      return { dirEntries: dirEntriesCache, companions: companionsBySource }
    }
    let diskScanLoaded = false
    const diskCandidatesByKey = new Map<string, { name: string; size: number; fnv1a: string; attempts: string[] }[]>()
    const diskCandidatesByChild = new Map<string, BackupFile[]>()
    const usedDiskBackups = new Set<string>(backups.map(b => b.name))
    const loadDiskBackups = async (): Promise<void> => {
      if (diskScanLoaded) return
      diskScanLoaded = true
      const { dirEntries, companions } = await ensureCompanions()
      const liveFiles = new Map<string, number>()
      const gMarked = new Set<string>()
      for (const entry of dirEntries) {
        if (entry.kind !== undefined && entry.kind !== 'file') continue
        if (entry.size > 0 && kindOf(entry.name) !== null) {
          liveFiles.set(entry.name, entry.size)
        } else if (entry.size > 0) {
          const of = recordOf(entry.name)
          const by = of !== null ? doneMarkerOf(entry.name, of) : null
          if (of !== null && by !== null && by.endsWith('g')) gMarked.add(of)
        }
      }
      for (const liveName of gMarked) {
        if (liveName === parentName || usedDiskBackups.has(liveName)) continue
        const liveSize = liveFiles.get(liveName) ?? 0
        if (liveSize <= 0 || liveSize > HOST_FILE_BYTES) continue
        const liveSource = sourceFor(dir, liveName, liveSize, c)
        const liveCompanions = companions.get(liveName) ?? []
        const m = await this.marksDetailed(liveSource, liveCompanions)
        if (
          m.backup !== true ||
          m.stagedFor !== parentName ||
          m.stagedForSize !== parentSize ||
          m.replaces === undefined ||
          m.replaces === '' ||
          m.backupForSize === undefined ||
          m.backupForFnv1a === undefined ||
          m.backupForFnv1a === '' ||
          m.backupFnv1a === undefined ||
          m.backupFnv1a === '' ||
          m.nonStagedBy.length > 0
        ) {
          continue
        }
        const liveDone = await this.doneMarksAll(liveSource, liveCompanions)
        if (liveDone.torn || liveDone.records.length > 0) continue
        const key = `${m.replaces}\0${m.backupForSize}\0${m.backupForFnv1a}\0${liveSize}\0${m.backupFnv1a}`
        const list = diskCandidatesByKey.get(key)
        const item = { name: liveName, size: liveSize, fnv1a: m.backupFnv1a, attempts: m.attempts ?? [] }
        if (list) list.push(item)
        else diskCandidatesByKey.set(key, [item])
        const byChildList = diskCandidatesByChild.get(m.replaces)
        const childRef: BackupFile = {
          name: liveName,
          size: liveSize,
          fnv1a: m.backupFnv1a,
          replaces: m.replaces,
          replacesSize: m.backupForSize,
          replacesFnv1a: m.backupForFnv1a,
        }
        if (byChildList) byChildList.push(childRef)
        else diskCandidatesByChild.set(m.replaces, [childRef])
      }
    }
    const readIfPresent = async (filePath: string): Promise<{ present: boolean; text: string }> => {
      if (this.io.exists !== undefined && !(await this.io.exists(filePath))) return { present: false, text: '' }
      try {
        return { present: true, text: await this.io.readFile(filePath) }
      } catch (err) {
        if (isNotFoundError(err)) return { present: false, text: '' }
        throw err
      }
    }
    let postLeaseLoaded = false
    const ensurePostLeaseCompanions = async (): Promise<{
      dirEntries: readonly { name: string; kind?: string; size: number }[]
      companions: Map<string, { name: string; kind?: string; size: number }[]>
    }> => {
      if (!postLeaseLoaded) {
        postLeaseLoaded = true
        dirEntriesCache = await this.io.listDir(dir)
        companionsBySource = groupCompanions(dirEntriesCache)
        diskScanLoaded = false
        diskCandidatesByKey.clear()
        diskCandidatesByChild.clear()
      }
      return { dirEntries: dirEntriesCache!, companions: companionsBySource! }
    }
    const canLinkBackupToChild = async (
      childName: string,
      ownedChildLease: boolean,
      companions: ReadonlyMap<string, { name: string; kind?: string; size: number }[]>,
    ): Promise<boolean> => {
      if (!ownedChildLease) return false
      const anyChildSource = { ...sourceFor(dir, childName, -1, c), live: false }
      const childCompanions = companions.get(childName) ?? []
      const doneAll = await this.doneMarksAll(anyChildSource, childCompanions)
      if (doneAll.torn || doneAll.records.length > 0) return false
      // Strictly require undefined/null on v1 store (NOT isClearedSlot): 0.1.3
      // treats `{ cleared: true, size: -1 }` as matching `size: -1` and drops.
      const rawV1 = await this.io.storeGet(anyChildSource.recordKey)
      if (rawV1 !== undefined && rawV1 !== null) return false
      const rawV2 = await this.io.storeGet(anyChildSource.v2RecordKey)
      if (!isClearedSlot(rawV2)) return false
      for (const rf of [...anyChildSource.recordFiles, ...anyChildSource.v2RecordFiles]) {
        const res = await readIfPresent(rf)
        if (res.present && res.text !== '') return false
      }
      return true
    }
    let leasedChild: string | undefined
    let mutatedChild: string | undefined
    const acquiredChildLeases: string[] = []
    const isForeignChildLease = (lease: { by?: string; holder?: string; until?: number } | undefined): boolean =>
      Boolean(
        lease &&
          typeof lease.until === 'number' &&
          lease.until > this.io.now() &&
          (lease.by !== me || typeof lease.holder !== 'string' || lease.holder === '' || lease.holder !== this.mark),
      )
    const failChildMutated = async (cName: string, hadLease: boolean, msg: string): Promise<never> => {
      if (hadLease) {
        leasedChild = cName
      } else {
        try {
          const leaseNow = (await this.io.storeGet(`lease:${dest}/${cName}`)) as { by?: string; holder?: string; until?: number } | undefined
          if (isForeignChildLease(leaseNow)) {
            leasedChild = cName
          }
        } catch {
          // Best-effort lease attribution on concurrent child mutation.
        }
      }
      if (leasedChild === undefined) mutatedChild = cName
      throw new Error(msg)
    }
    const checkCoveredParts = async (
      candidatePacked: readonly string[],
      childName: string,
      childSize: number,
      childDigest: string,
    ): Promise<{ part: string; size: number; digest: string }[]> => {
      const missing: { part: string; size: number; digest: string }[] = []
      for (const part of candidatePacked) {
        const bSize = utf8Length(part)
        const bDigest = fnv1a(part)
        let covered = false
        for (const b of backups) {
          if (
            absentBackups.has(b.name) ||
            b.replaces !== childName ||
            b.replacesSize !== childSize ||
            b.replacesFnv1a !== childDigest ||
            b.size !== bSize ||
            b.fnv1a !== bDigest
          ) {
            continue
          }
          const existingRead = await readIfPresent(joinPath(dir, b.name))
          if (!existingRead.present || existingRead.text === '') {
            absentBackups.add(b.name)
            continue
          }
          if (utf8Length(existingRead.text) !== bSize || fnv1a(existingRead.text) !== bDigest) continue
          const { companions } = await ensureCompanions()
          const bDone = await this.doneMarksAll(sourceFor(dir, b.name, bSize, c), companions.get(b.name) ?? [])
          if (bDone.torn || bDone.records.length > 0) continue
          covered = true
          break
        }
        if (!covered) missing.push({ part, size: bSize, digest: bDigest })
      }
      return missing
    }
    const stageMissingParts = async (
      missingParts: readonly { part: string; size: number; digest: string }[],
      childName: string,
      childKind: DeadLetterKind,
      childSize: number,
      childDigest: string,
    ): Promise<BackupFile[]> => {
      const stagedHere: BackupFile[] = []
      await loadDiskBackups()
      for (const { part, size: bSize, digest: bDigest } of missingParts) {
        const diskKey = `${childName}\0${childSize}\0${childDigest}\0${bSize}\0${bDigest}`
        let reused: BackupFile | null = null
        for (const cand of diskCandidatesByKey.get(diskKey) ?? []) {
          if (usedDiskBackups.has(cand.name)) continue
          const bLease = (await this.io.storeGet(`lease:${dest}/${cand.name}`)) as { by?: string; holder?: string; until?: number } | undefined
          if (isForeignChildLease(bLease)) continue
          const candRead = await readIfPresent(joinPath(dir, cand.name))
          if (candRead.present && candRead.text !== '' && utf8Length(candRead.text) === bSize && fnv1a(candRead.text) === bDigest) {
            usedDiskBackups.add(cand.name)
            for (const a of cand.attempts) priorAttemptsSet.add(a)
            reused = { name: cand.name, size: bSize, fnv1a: bDigest, replaces: childName, replacesSize: childSize, replacesFnv1a: childDigest }
            break
          }
        }
        if (reused !== null) {
          backups.push(reused)
          continue
        }
        const bName = fileName(childKind, this.io.now(), this.sessionId(), ++this.seq, this.mark)
        const bPath = joinPath(dir, bName)
        const ref: BackupFile = { name: bName, size: bSize, fnv1a: bDigest, replaces: childName, replacesSize: childSize, replacesFnv1a: childDigest }
        newlyStaged.push(ref)
        stagedHere.push(ref)
        usedDiskBackups.add(bName)
        await this.io.writeFile(
          doneFileOf(dir, bName, guardMark),
          sealFile(
            `${JSON.stringify({
              size: bSize,
              sending: true,
              by: guardMark,
              stagedFor: parentName,
              stagedForSize: parentSize,
              replaces: childName,
              backup: true,
              backupForSize: childSize,
              backupForFnv1a: childDigest,
              backupFnv1a: bDigest,
              attempt,
            })}\n`,
          ),
        )
        await this.io.writeFile(bPath, part)
        backups.push(ref)
      }
      return stagedHere
    }
    const buildBackupTexts = (
      childName: string,
      childKind: DeadLetterKind,
      parsed: ParsedFile,
    ): { texts: string[]; hasIdlessRow: boolean } => {
      const texts: string[] = []
      let hasIdlessRow = false
      parsed.lines.forEach((line, i) => {
        if (typeof line.row.event_id !== 'string' || line.row.event_id === '') hasIdlessRow = true
        const keyedLine = ensureRowEventId(line, childName, i)
        const lineKind: DeadLetterKind = isDeadLetterKind(line.kind) ? line.kind : childKind
        texts.push(lineText({ ...keyedLine, kind: lineKind }))
      })
      for (const rawBad of parsed.unreadable) texts.push(rawBad)
      return { texts, hasIdlessRow }
    }
    const targetStates: {
      childPath: string
      childName: string
      childSize: number
      hasForeignLease: boolean
      ownedChildLease: boolean
    }[] = []
    const recheckTargets: {
      childPath: string
      childName: string
      childSize: number
      childDigest: string
      hadMissing: boolean
      hadForeignLease: boolean
      linkedBackup: boolean
      stagedForChild: BackupFile[]
      finalKeepKeys: Set<string>
    }[] = []
    try {
      // Phase 1: Establish temporary ownership over every unleased target child
      // before taking a single post-lease directory snapshot so companion
      // checks stay O(directory size) regardless of target count.
      for (const [childPath, childSize] of targets) {
        if (childSize <= 0 || childSize > HOST_FILE_BYTES) continue
        const childName = baseOf(childPath)
        const childLeaseKey = `lease:${dest}/${childName}`
        const lease = (await this.io.storeGet(childLeaseKey)) as { by?: string; holder?: string; until?: number } | undefined
        const hasForeignLease = isForeignChildLease(lease)
        let ownedChildLease = false
        if (hasForeignLease) {
          let hasLiveBackup = false
          for (const b of backups) {
            if (absentBackups.has(b.name) || b.replaces !== childName) continue
            const br = await readIfPresent(joinPath(dir, b.name))
            if (!br.present || br.text === '') {
              absentBackups.add(b.name)
              continue
            }
            if (utf8Length(br.text) === b.size && fnv1a(br.text) === b.fnv1a) {
              hasLiveBackup = true
              break
            }
          }
          if (!hasLiveBackup) {
            leasedChild = childName
            throw new Error(`another session's lease holds ${childName}`)
          }
        } else {
          try {
            await this.io.storeSet(childLeaseKey, { by: me, holder: this.mark, until: this.io.now() + LEASE_MS })
            ownedChildLease = true
            acquiredChildLeases.push(childName)
          } catch {
            ownedChildLease = false
          }
        }
        targetStates.push({ childPath, childName, childSize, hasForeignLease, ownedChildLease })
      }
      // Phase 2: Read each child, evaluate linking using ONE shared post-lease
      // directory listing, and stage any missing backups.
      for (const { childPath, childName, childSize, hasForeignLease, ownedChildLease } of targetStates) {
        const raw = await this.io.readFile(childPath)
        if (raw === '') await failChildMutated(childName, hasForeignLease, `child ${childName} became empty while staging backups`)
        if (utf8Length(raw) !== childSize) await failChildMutated(childName, hasForeignLease, `child ${childName} changed size while staging backups`)
        const parsed = parseFile(raw)
        if (!parsed.complete) await failChildMutated(childName, hasForeignLease, `child ${childName} became incomplete while staging backups`)
        if (parsed.parent === null) {
          if (expectedParents?.has(childName)) await failChildMutated(childName, hasForeignLease, `child ${childName} lost parent header while staging backups`)
          continue
        }
        if (baseOf(parsed.parent) !== parentName) await failChildMutated(childName, hasForeignLease, `child ${childName} changed parent while staging backups`)
        const childDigest = fnv1a(raw)
        const childKind = kindOf(childName) ?? 'retry'
        const { texts, hasIdlessRow: childHasIdlessRow } = buildBackupTexts(childName, childKind, parsed)
        if (texts.length === 0) continue
        let backupParent: string | undefined
        let missingParts: { part: string; size: number; digest: string }[]
        let chosenPacked: string[]
        if (hasForeignLease) {
          // Under an active foreign lease we never stage new parts; accept
          // existing verified backups matching either standalone or child-linked packing.
          chosenPacked = packFiles(texts)
          missingParts = await checkCoveredParts(chosenPacked, childName, childSize, childDigest)
          if (missingParts.length > 0 && childHasIdlessRow) {
            const linkedPacked = packFiles(texts, childName)
            const linkedMissing = await checkCoveredParts(linkedPacked, childName, childSize, childDigest)
            if (linkedMissing.length === 0) {
              chosenPacked = linkedPacked
              missingParts = linkedMissing
              backupParent = childName
            }
          }
        } else {
          if (childHasIdlessRow && ownedChildLease) {
            const { companions: postLeaseCompanions } = await ensurePostLeaseCompanions()
            if (await canLinkBackupToChild(childName, ownedChildLease, postLeaseCompanions)) {
              backupParent = childName
            }
          }
          chosenPacked = backupParent !== undefined ? packFiles(texts, backupParent) : packFiles(texts)
          missingParts = await checkCoveredParts(chosenPacked, childName, childSize, childDigest)
        }
        if (missingParts.length > 0 && hasForeignLease) {
          leasedChild = childName
          throw new Error(`another session's lease holds ${childName}`)
        }
        const stagedForChild =
          missingParts.length > 0 ? await stageMissingParts(missingParts, childName, childKind, childSize, childDigest) : []
        recheckTargets.push({
          childPath,
          childName,
          childSize,
          childDigest,
          hadMissing: missingParts.length > 0,
          hadForeignLease: hasForeignLease,
          linkedBackup: backupParent !== undefined && !hasForeignLease,
          stagedForChild,
          finalKeepKeys: new Set(chosenPacked.map(part => `${utf8Length(part)}\0${fnv1a(part)}`)),
        })
      }
      // Phase 3a: Re-verify every child's lease and live bytes BEFORE taking
      // the final directory snapshot so a sibling retirement mark published
      // while reading earlier children is never missed.
      const verifyChildLeaseAndBytes = async (entry: (typeof recheckTargets)[number], checkLease: boolean, errMsg: string): Promise<string> => {
        if (checkLease) {
          const leaseNow = (await this.io.storeGet(`lease:${dest}/${entry.childName}`)) as { by?: string; holder?: string; until?: number } | undefined
          if (isForeignChildLease(leaseNow)) {
            leasedChild = entry.childName
            throw new Error(`another session's lease holds ${entry.childName}`)
          }
        }
        const curRaw = await this.io.readFile(entry.childPath)
        if (curRaw === '' || utf8Length(curRaw) !== entry.childSize || fnv1a(curRaw) !== entry.childDigest) {
          await failChildMutated(entry.childName, entry.hadForeignLease, errMsg)
        }
        return curRaw
      }
      for (const entry of recheckTargets) {
        await verifyChildLeaseAndBytes(
          entry,
          entry.hadMissing || entry.linkedBackup,
          `child ${entry.childName} changed while staging backups`,
        )
      }
      // Phase 3b: Take ONE shared post-3a directory snapshot for linked backups
      // and selectively re-pack any child that gained retirement evidence
      // standalone (rebuilding its rows on demand so happy-path staging retains
      // no per-child payload buffers). If a child repacks, re-verify any
      // remaining still-linked siblings against a fresh listing.
      const stillLinked = recheckTargets.filter(entry => entry.linkedBackup)
      const hadPhase3bLinkChecks = stillLinked.length > 0
      let repackedAny = false
      while (stillLinked.length > 0) {
        dirEntriesCache = await this.io.listDir(dir)
        const roundCompanions = groupCompanions(dirEntriesCache)
        companionsBySource = roundCompanions
        diskScanLoaded = false
        diskCandidatesByKey.clear()
        diskCandidatesByChild.clear()

        let repackedInRound = false
        for (let i = 0; i < stillLinked.length; ) {
          const entry = stillLinked[i]!
          if (!(await canLinkBackupToChild(entry.childName, true, roundCompanions))) {
            const repRaw = await verifyChildLeaseAndBytes(
              entry,
              true,
              `child ${entry.childName} changed while repacking standalone backup`,
            )
            const repParsed = parseFile(repRaw)
            if (!repParsed.complete || repParsed.parent === null || baseOf(repParsed.parent) !== parentName) {
              await failChildMutated(entry.childName, false, `child ${entry.childName} changed while repacking standalone backup`)
            }
            const repKind = kindOf(entry.childName) ?? 'retry'
            const { texts: repTexts } = buildBackupTexts(entry.childName, repKind, repParsed)
            const standalonePacked = packFiles(repTexts)
            const standaloneKeys = new Set(standalonePacked.map(part => `${utf8Length(part)}\0${fnv1a(part)}`))
            const standaloneMissing = await checkCoveredParts(standalonePacked, entry.childName, entry.childSize, entry.childDigest)
            if (standaloneMissing.length > 0) {
              await stageMissingParts(standaloneMissing, entry.childName, repKind, entry.childSize, entry.childDigest)
            }
            await verifyChildLeaseAndBytes(
              entry,
              true,
              `child ${entry.childName} changed while repacking standalone backup`,
            )
            entry.finalKeepKeys = standaloneKeys
            stillLinked.splice(i, 1)
            repackedInRound = true
            repackedAny = true
          } else {
            i += 1
          }
        }
        if (!repackedInRound) break
        for (const sibling of stillLinked) {
          await verifyChildLeaseAndBytes(sibling, true, `child ${sibling.childName} changed while staging backups`)
        }
      }
      if (hadPhase3bLinkChecks || (repackedAny && recheckTargets.length > 1)) {
        for (const entry of recheckTargets) {
          await verifyChildLeaseAndBytes(
            entry,
            entry.hadMissing || entry.linkedBackup,
            `child ${entry.childName} changed while staging backups`,
          )
        }
      }
      // Phase 4: Commit-point retirement of superseded backups for every
      // unleased child (both linked->standalone and standalone->linked
      // transitions). Pre-flight check foreign leases across all superseded
      // backups BEFORE truncating any pre-existing copy, re-check each backup's
      // lease right before writing, and promote a child's kept replacements out
      // of `newlyStaged` BEFORE truncating a pre-existing backup so even a
      // truncate-then-throw I/O error or a later caller HTTP 503 cleanup never
      // deletes both copies.
      if (recheckTargets.some(entry => !entry.hadForeignLease)) {
        await loadDiskBackups()
        const backupsByChild = new Map<string, BackupFile[]>()
        for (const b of backups) {
          const list = backupsByChild.get(b.replaces)
          if (list) list.push(b)
          else backupsByChild.set(b.replaces, [b])
        }
        const toRetireEntries: { entry: (typeof recheckTargets)[number]; ref: BackupFile }[] = []
        const seenRetireNames = new Set<string>()
        for (const entry of recheckTargets) {
          if (entry.hadForeignLease) continue
          const addCandidate = (ref: BackupFile): void => {
            if (absentBackups.has(ref.name) || seenRetireNames.has(ref.name)) return
            if (ref.replaces !== entry.childName || ref.replacesSize !== entry.childSize || ref.replacesFnv1a !== entry.childDigest) return
            if (!entry.finalKeepKeys.has(`${ref.size}\0${ref.fnv1a}`)) {
              seenRetireNames.add(ref.name)
              toRetireEntries.push({ entry, ref })
            }
          }
          for (const b of backupsByChild.get(entry.childName) ?? []) addCandidate(b)
          for (const ref of entry.stagedForChild) addCandidate(ref)
          for (const cand of diskCandidatesByChild.get(entry.childName) ?? []) addCandidate(cand)
        }
        if (toRetireEntries.length > 0) {
          for (const { ref } of toRetireEntries) {
            const bLease = (await this.io.storeGet(`lease:${dest}/${ref.name}`)) as { by?: string; holder?: string; until?: number } | undefined
            if (isForeignChildLease(bLease)) {
              leasedChild = ref.name
              throw new Error(`another session's lease holds superseded backup ${ref.name}`)
            }
          }
          const newlyStagedNames = new Set(newlyStaged.map(r => r.name))
          const removeFromNewlyStaged = (pred: (r: BackupFile) => boolean): void => {
            for (let i = newlyStaged.length - 1; i >= 0; i -= 1) {
              if (pred(newlyStaged[i]!)) newlyStaged.splice(i, 1)
            }
          }
          for (const { entry, ref } of toRetireEntries) {
            const bPath = joinPath(dir, ref.name)
            const res = await readIfPresent(bPath)
            if (!res.present || res.text === '' || utf8Length(res.text) !== ref.size || fnv1a(res.text) !== ref.fnv1a) {
              if (res.present && res.text === '') {
                this.retiredEmpty.add(bPath)
                try {
                  await this.io.writeFile(
                    doneFileOf(dir, ref.name, guardMark),
                    journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
                  )
                } catch {
                  // Best-effort sentinel write for already-empty superseded backup.
                }
              }
              absentBackups.add(ref.name)
              removeFromNewlyStaged(ns => ns.name === ref.name)
              continue
            }
            const bLeaseNow = (await this.io.storeGet(`lease:${dest}/${ref.name}`)) as { by?: string; holder?: string; until?: number } | undefined
            if (isForeignChildLease(bLeaseNow)) {
              leasedChild = ref.name
              throw new Error(`another session's lease holds superseded backup ${ref.name}`)
            }
            // Promote the child's kept replacements out of `newlyStaged` BEFORE
            // truncating a pre-existing backup so that even if `writeFile`
            // truncates on disk and then throws an error, the outer `catch`
            // never empties the kept replacement too.
            if (!newlyStagedNames.has(ref.name)) {
              removeFromNewlyStaged(
                ns =>
                  ns.replaces === entry.childName &&
                  ns.replacesSize === entry.childSize &&
                  ns.replacesFnv1a === entry.childDigest &&
                  entry.finalKeepKeys.has(`${ns.size}\0${ns.fnv1a}`),
              )
            }
            await this.io.writeFile(bPath, '')
            this.retiredEmpty.add(bPath)
            absentBackups.add(ref.name)
            if (newlyStagedNames.has(ref.name)) {
              removeFromNewlyStaged(ns => ns.name === ref.name)
            }
            try {
              await this.io.writeFile(
                doneFileOf(dir, ref.name, guardMark),
                journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() }),
              )
            } catch {
              // Best-effort sentinel write after superseded backup truncation.
            }
          }
        }
      }
      return {
        ok: true,
        backups: backups.filter(b => !absentBackups.has(b.name)),
        newlyStaged,
        priorAttempts: [...priorAttemptsSet],
      }
    } catch {
      for (const ref of newlyStaged) {
        const bPath = joinPath(dir, ref.name)
        try {
          const lease = (await this.io.storeGet(`lease:${dest}/${ref.name}`)) as { by?: string; holder?: string; until?: number } | undefined
          if (isForeignChildLease(lease)) continue
          const res = await readIfPresent(bPath)
          if (res.text === '' || (utf8Length(res.text) === ref.size && fnv1a(res.text) === ref.fnv1a)) {
            if (res.text !== '') await this.io.writeFile(bPath, '')
            if (res.present) this.retiredEmpty.add(bPath)
            await this.io.writeFile(
              doneFileOf(dir, ref.name, guardMark),
              res.present
                ? journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() })
                : '',
            )
          }
        } catch {
          // Best-effort rollback of newly staged standalone backups.
        }
      }
      return leasedChild !== undefined
        ? { ok: false, leasedChild }
        : mutatedChild !== undefined
          ? { ok: false, mutatedChild }
          : { ok: false }
    } finally {
      for (const cName of acquiredChildLeases) {
        try {
          const curLease = (await this.io.storeGet(`lease:${dest}/${cName}`)) as { by?: string; holder?: string } | undefined
          if (curLease?.by === me && curLease?.holder === this.mark) await this.io.storeDelete(`lease:${dest}/${cName}`)
        } catch {
          // Best-effort release of temporary child backup-staging lease.
        }
      }
    }
  }

  // This writer's own mark beside `source` let go. Best effort: returns true
  // only once truncation succeeds (and removes the tracked sending mark).
  private async unmark(source: Source): Promise<boolean> {
    try {
      await this.io.writeFile(doneFileOf(source.dir, source.name, this.mark), '')
      this.activeSendingMarks.delete(joinPath(source.dir, source.name))
      return true
    } catch {
      // Left on disk.
      return false
    }
  }

  // Replays one file; false when what it left is written out but the file
  // itself could not be emptied.
  private async backfillFile(
    dir: string,
    name: string,
    file: ParsedFile,
    source: Source,
    c: Config,
    kinds: readonly DeadLetterKind[],
    trigger: BackfillReport['trigger'],
    report: BackfillReport,
    tally: ReplayTally,
    parentIndex?: RunParentIndex,
    supersededUnaccounted?: Partial<Journal>,
    emptiedInRun?: Set<string>,
    parentAccountedKeys?: ReadonlySet<string>,
    allowStagedFor?: string,
    parentSkipIndices?: ReadonlySet<number>,
    sourceFnv1a?: string,
    forceV2 = false,
  ): Promise<boolean | 'skipped'> {
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
    let skippedByParent = false
    lines.forEach((line, i) => {
      if (parentSkipIndices?.has(i)) {
        fates.push('L')
        skippedByParent = true
        return
      }
      if (parentAccountedKeys !== undefined) {
        const identity = childRowIdentity(line)
        if (identity !== null && parentAccountedKeys.has(identity)) {
          fates.push('L')
          skippedByParent = true
          return
        }
      }
      const kind = isDeadLetterKind(line.kind) ? line.kind : fileKind
      if (line.table === table && kinds.includes(kind)) {
        const keyedLine = ensureRowEventId(line, name, i)
        mine.push({ ...keyedLine, kind })
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
    let changed = skippedByParent
    let marked = false
    let prePlacedChildGuards: Map<string, number> | undefined
    let newlyPlacedChildGuards: string[] = []
    let prevChildGuardMarks = new Map<string, string>()
    let preStagedBackups: BackupFile[] | undefined
    let newlyStagedBackups: BackupFile[] = []
    let guardAttempt: string | undefined
    let guardAttempts: string[] | undefined
    let preGuardChecked = false
    const uncertainLines: number[] = []
    const recordUncertain = (first: number, count: number): void => {
      for (let i = 0; i < count; i += 1) uncertainLines.push(at[first + i] as number)
    }
    const dispatchReplayPost = async (body: string, token: string, needsReserve: boolean): Promise<HttpAnswer | { error: string; uncertain?: boolean }> =>
      this.replayPost(insertAllUrl(c), body, token, c.requestTimeoutMs, needsReserve)
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
      const needsReserve = first > 0 || end < mine.length || kept > 0 || file.unreadable.length > 0
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
        if (!(await this.markSending(path, source, trigger, report, supersededUnaccounted, allowStagedFor))) break
        marked = true
      }
      if (trigger === 'force' && !preGuardChecked) {
        preGuardChecked = true
        const guardInfo = await this.discoverLiveChildrenForForce(source, c, parentIndex, true)
        if (guardInfo === null) {
          await this.unmark(source)
          report.error = diagnostic(`could not inspect live children of ${name} before forced replay; retrying later`)
          return false
        }
        const { targets: guardTargets, expectedParents, entries: guardEntries } = guardInfo
        if (guardTargets.size > 0) {
          const attemptToken = `${this.mark}-${++this.seq}-${randomHex(3)}`
          const placedResult = await this.placeChildGuards(dir, name, source.size, guardTargets, attemptToken, undefined, guardEntries)
          if (placedResult === null) {
            await this.unmark(source)
            report.error = diagnostic(`could not guard live children of ${name} before forced replay; retrying later`)
            return false
          }
          const backupResult = await this.stageChildBackups(dir, name, source.size, c, guardTargets, attemptToken, undefined, guardEntries, expectedParents)
          if (!backupResult.ok) {
            const guardMark = `${this.mark}g`
            for (const guardedPath of placedResult.newlyPlaced) {
              try {
                await this.io.writeFile(doneFileOf(dir, baseOf(guardedPath), guardMark), placedResult.prevMarks.get(guardedPath) ?? '')
              } catch {
                // Best-effort rollback of newly placed child guards.
              }
            }
            await this.unmark(source)
            if (backupResult.leasedChild !== undefined) {
              const childLeasePath = joinPath(dir, backupResult.leasedChild)
              const childLeaseMsg = diagnostic(`another session's lease holds ${backupResult.leasedChild} (ten minutes at most); it waits for the next replay`)
              const parentLeaseMsg = diagnostic(`another session's lease holds child ${backupResult.leasedChild} of ${name} (ten minutes at most); it waits for the next replay`)
              const childEntry: HeldEntry = { reason: childLeaseMsg, mayBeActive: true, activeCertain: true }
              this.currentScanHeld.set(childLeasePath, childEntry)
              this.setHeldPath(childLeasePath, childEntry)
              this.noteHeld(report, trigger, parentLeaseMsg, path, true, true)
              report.skippedFiles += 1
              return 'skipped'
            }
            if (backupResult.mutatedChild !== undefined) {
              const parentMutMsg = diagnostic(`child ${backupResult.mutatedChild} of ${name} changed while staging backups; it waits for the next replay`)
              this.noteHeld(report, trigger, parentMutMsg, path, false, false)
              report.skippedFiles += 1
              return 'skipped'
            }
            report.error = diagnostic(`could not stage standalone backups for live children of ${name} before forced replay; retrying later`)
            return false
          }
          guardAttempt = attemptToken
          guardAttempts = normalizeAttempts(attemptToken, [...placedResult.priorAttempts, ...backupResult.priorAttempts])
          prePlacedChildGuards = placedResult.placed
          newlyPlacedChildGuards = placedResult.newlyPlaced
          prevChildGuardMarks = placedResult.prevMarks
          if (backupResult.backups.length > 0) preStagedBackups = backupResult.backups
          newlyStagedBackups = backupResult.newlyStaged
        }
      }
      if (this.replayHalted()) break
      const body = insertAllBody(chunk.map(l => l.row))
      if (this.replayHalted()) break
      tally.requests += 1
      let answer = await dispatchReplayPost(body, token, needsReserve)
      if ('error' in answer) {
        if (answer.uncertain) recordUncertain(first, chunk.length)
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
        answer = await dispatchReplayPost(body, token, needsReserve)
        if ('error' in answer) {
          if (answer.uncertain) recordUncertain(first, chunk.length)
          report.error = diagnostic(answer.error)
          continue
        }
        outcome = classifyInsert(answer, chunk.length)
        if (outcome.kind === 'auth') this.noteProblem('credentials', `BigQuery refused a fresh token too: ${outcome.message}`)
      }
      if (outcome.kind === 'not-found' && !this.closing && c.autoCreateTable && !this.createAttempted && !isDatasetMissing(outcome.message)) {
        if (await this.createTable(c, token)) {
          if (this.replayHalted()) break
          tally.requests += 1
          answer = await dispatchReplayPost(body, token, needsReserve)
          if ('error' in answer) {
            if (answer.uncertain) recordUncertain(first, chunk.length)
            report.error = diagnostic(answer.error)
            continue
          }
          outcome = classifyInsert(answer, chunk.length)
        }
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
    const needsSplit = uncertainLines.length > 0 && (uncertainLines.length < lines.length || file.unreadable.length > 0)
    // Nothing sent and nothing new to record: the file stays as it was, and
    // its mark goes while it is still this writer's (best effort: one left is
    // this writer's own). When the whole single-batch file had an unanswered
    // exit-replay request, keep its mark standing instead of zeroing it.
    if (!changed && !needsSplit) {
      if (marked) {
        if (uncertainLines.length > 0) {
          this.uncertainSources.set(path, source.size)
          this.noteHeld(
            report,
            trigger,
            `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
            path,
          )
          this.activeSendingMarks.delete(path)
        } else {
          await this.unmark(source)
          const guardMark = `${this.mark}g`
          if (newlyStagedBackups.length > 0) {
            const dest = destinationDir(tableRef(c))
            const me = this.sessionId() ?? 'unknown'
            for (const ref of newlyStagedBackups) {
              const bPath = joinPath(dir, ref.name)
              try {
                const lease = (await this.io.storeGet(`lease:${dest}/${ref.name}`)) as { by?: string; holder?: string; until?: number } | undefined
                if (
                  lease &&
                  typeof lease.until === 'number' &&
                  lease.until > this.io.now() &&
                  (lease.by !== me || typeof lease.holder !== 'string' || lease.holder === '' || lease.holder !== this.mark)
                ) {
                  continue
                }
                const exists = this.io.exists === undefined ? true : await this.io.exists(bPath)
                const cur = exists ? await this.io.readFile(bPath) : ''
                if (cur === '' || (utf8Length(cur) === ref.size && fnv1a(cur) === ref.fnv1a)) {
                  if (cur !== '') await this.io.writeFile(bPath, '')
                  if (exists) this.retiredEmpty.add(bPath)
                  await this.io.writeFile(
                    doneFileOf(dir, ref.name, guardMark),
                    exists
                      ? journalText({ fates: '', reasons: [], refusals: [], size: 0, waits: false, seq: 1, files: [], written: [], retired: true, retiredAt: this.io.now() })
                      : '',
                  )
                }
              } catch {
                // Best-effort clear of newly staged backup when nothing was sent.
              }
            }
          }
          if (newlyPlacedChildGuards.length > 0) {
            for (const guardedPath of newlyPlacedChildGuards) {
              try {
                await this.io.writeFile(doneFileOf(dir, baseOf(guardedPath), guardMark), prevChildGuardMarks.get(guardedPath) ?? '')
              } catch {
                // Best-effort clear of newly placed child guard mark when nothing was sent.
              }
            }
          }
        }
      }
      return true
    }
    if (!marked) {
      if (this.replayHalted() || !(await this.markSending(path, source, trigger, report, supersededUnaccounted, allowStagedFor))) return false
      marked = true
    }
    // What is left goes to new files first, each kind to files of its own (a
    // row refused here to a `rejected` file, which no replay that runs by
    // itself reads; rows of an unanswered exit-replay batch to their own
    // marked file so unsubmitted batches stay eligible for automatic replay),
    // each file ending with its completion record; only once
    // every one is written is the source emptied: `$.fs` has no rename or
    // delete. A write that fails leaves the source as it was, so an unsent
    // row is never held only in memory, and this outcome is kept: the next
    // replay finishes writing it out (`writeOut`) and sends none of the
    // file again.
    const outcome: Outcome = {
      fates: fates.join(''),
      reasons,
      refusals,
      ...(uncertainLines.length > 0 ? { uncertainLines } : {}),
    }
    const { files: rest, stampedIds } = leftoverFiles(file, fileKind, outcome, name, true)
    let layoutsDiffer = false
    if (stampedIds && rest.length > 1) {
      const unstampedRes = leftoverFiles(file, fileKind, outcome, name, false)
      layoutsDiffer = !sameLeftoverPartitioning(rest, unstampedRes.files)
    }
    const files = rest.map(({ kind, text, uncertain }) => ({
      path: joinPath(dir, fileName(kind, this.io.now(), this.sessionId(), ++this.seq, this.mark)),
      text,
      ...(uncertain ? { uncertain: true } : {}),
    }))
    // Another writer that replayed the file at the same time may have
    // recorded its outcome meanwhile, or finished it: writeOut finds out
    // before it records this one.
    const useV2 = uncertainLines.length > 0 || (stampedIds && layoutsDiffer) || forceV2
    const accountedRowKeys =
      trigger === 'force'
        ? new Set([
            ...lines.map((line, i) => parentRowIdentity(line, name, i)),
            ...(parentAccountedKeys ?? []),
          ])
        : undefined
    const leftoverRowKeys =
      trigger === 'force' && (parentAccountedKeys === undefined || parentAccountedKeys.size === 0)
        ? new Set(lines.flatMap((line, i) => (fates[i] !== 'L' ? [parentRowIdentity(line, name, i)] : [])))
        : undefined
    const resolvedFnv1a = sourceFnv1a ?? source.fnv1a
    const rewrite: Rewrite = {
      ...source,
      ...outcome,
      ...(resolvedFnv1a !== undefined ? { fnv1a: resolvedFnv1a, sourceFnv1a: resolvedFnv1a } : {}),
      files,
      waits: rest.some(f => f.kind !== 'rejected'),
      filed: false,
      seq: 0,
      ...(useV2 ? { useV2: true } : {}),
      ...(stampedIds ? { stampedIds: true } : {}),
      ...(layoutsDiffer ? { layoutsDiffer: true } : {}),
      ...(trigger === 'force' ? { force: true } : {}),
      ...(accountedRowKeys !== undefined ? { accountedRowKeys } : {}),
      ...(leftoverRowKeys !== undefined ? { leftoverRowKeys } : {}),
      ...(supersededUnaccounted !== undefined ? { supersededUnaccounted } : {}),
      ...(prePlacedChildGuards !== undefined && prePlacedChildGuards.size > 0 ? { childGuardMarks: prePlacedChildGuards } : {}),
      ...(preStagedBackups !== undefined && preStagedBackups.length > 0 ? { backupFiles: preStagedBackups } : {}),
      ...(guardAttempt !== undefined ? { guardAttempt } : {}),
      ...(guardAttempts !== undefined && guardAttempts.length > 0 ? { guardAttempts } : {}),
    }
    const uncertainFiles = rewrite.files.filter(f => f.uncertain)
    const hadHeldReason = report.held !== null
    if (uncertainFiles.length > 0) {
      this.noteHeld(
        report,
        trigger,
        `a replay of ${name} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
        path,
      )
      this.activeSendingMarks.delete(path)
    }
    let done = false
    try {
      done = await this.writeOut(path, rewrite, c, parentIndex, emptiedInRun)
    } finally {
      if (uncertainFiles.length > 0) {
        if (!done) {
          for (const f of uncertainFiles) {
            if (f.text !== null) {
              this.deleteHeldPath(f.path)
              this.currentScanHeld.delete(f.path)
            }
          }
        }
        const firstWrittenUncertain = uncertainFiles.find(f => f.text === null)
        if (!hadHeldReason && firstWrittenUncertain !== undefined) {
          const updated = diagnostic(
            `a replay of ${baseOf(firstWrittenUncertain.path)} sent rows and has recorded no outcome (no answer before the session ended); /bqaa-backfill force replays the file afresh`,
          )
          const updatedEntry: HeldEntry = { reason: updated, mayBeActive: false }
          report.held = updated
          this.setHeldPath(firstWrittenUncertain.path, updatedEntry)
          this.currentScanHeld.set(firstWrittenUncertain.path, updatedEntry)
        } else if (!done && !this.rewrites.has(path) && firstWrittenUncertain === undefined && !hadHeldReason) {
          report.held = null
        }
        report.heldFiles = this.currentScanHeld.size
        const heldCount = Math.max(report.heldFiles, this.heldCountIn(dir))
        const active = this.hasActiveHeldIn(dir)
        this.inProgressHeld = {
          files: heldCount,
          reason: report.held ?? this.inProgressHeld.reason,
          trigger,
          mayBeActive: active,
        }
      }
    }
    if (!done && rewrite.deferredByLease === true) {
      const heldNow = this.currentScanHeld.get(path)
      if (heldNow !== undefined) {
        report.held ??= heldNow.reason
        report.heldFiles = this.currentScanHeld.size
      }
      report.skippedFiles += 1
      return 'skipped'
    }
    // The file done, clear our sending mark only when this write did not
    // replace it with a retired done mark (`wroteDone`). On `v2` splits,
    // `writeOut` already released the sending mark after writing the
    // separate retired mark and emptying `path`.
    if (done && !rewrite.wroteDone) await this.unmark(source)
    if (done && allowStagedFor !== undefined) {
      const ownGuard = doneFileOf(dir, name, `${this.mark}g`)
      try {
        if (this.io.exists === undefined || (await this.io.exists(ownGuard))) {
          await this.io.writeFile(ownGuard, '')
        }
      } catch {
        // Best-effort clear of our own child guard mark once the child is done.
      }
    }
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
