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

// The text `/bqaa-status` prints. Plain lines: the model reads a command's
// output too, so it carries counts and states, never a payload or a token,
// and the whole text is redacted once more on its way out.

import { type Config, tableRef } from './config.ts'
import { redactDiagnostic } from './sanitize.ts'
import type { BackfillReport, Writer } from './writer.ts'

// This table's dead-letter files (`dir`), the files elsewhere in the
// dead-letter directory (other tables' subdirectories, an older flat
// layout), and files too large to read back. `retiredFiles` and
// `elsewhereRetired` count the leftovers of replays, here and elsewhere:
// emptied files and `outcome-*` records, which no replay needs once no
// session is running (scripts/clean_dead_letter.py deletes them).
export type DeadLetterSummary =
  | {
      dir: string
      files: number
      bytes: number
      byKind: Record<string, number>
      oversized: string[]
      elsewhere: number
      elsewhereRetired?: number
      emptyFiles?: number
      recordFiles?: number
      retiredFiles?: number
    }
  | { error: string }

// Leftovers past which /bqaa-status offers the command that deletes them.
export const CLEANUP_HINT_FILES = 100

const ago = (now: number, at: number | null): string => {
  if (at === null) return 'never'
  const s = Math.max(0, Math.round((now - at) / 1000))
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.round(s / 60)}m ago`
  return `${Math.round(s / 3600)}h ago`
}

const kb = (bytes: number): string => (bytes < 1024 ? `${bytes} B` : `${(bytes / 1024).toFixed(1)} KB`)

const names = (list: readonly string[]): string => `${list.slice(0, 5).join(', ')}${list.length > 5 ? ', ...' : ''}`

// A line that names the table, cut for very long names: the setup command
// after it (at most 4,000 characters) then always fits whole in the 8,000
// the command's output keeps.
const short = (line: string): string => redactDiagnostic(line, 1000)

// The line /bqaa-backfill gives for a replay it ran: never `0 left` without
// the waiting files it did not read.
export function formatBackfillReport(r: BackfillReport): string {
  const refused = r.rowsRefused ? `, ${r.rowsRefused} of them refused` : ''
  const other = r.otherTableRows ? ` (${r.otherTableRows} for another table)` : ''
  const skipped = r.skippedFiles ? `, ${r.skippedFiles} waiting file(s) not replayed` : ''
  const oversized = r.oversizedFiles.length ? `, ${r.oversizedFiles.length} file(s) too large to read` : ''
  const incomplete = r.incompleteFiles.length ? `, ${r.incompleteFiles.length} file(s) without a completion record kept` : ''
  return `bqaa backfill: ${r.files} file(s), ${r.rowsSent} row(s) sent, ${r.rowsLeft} left${refused}${other}${skipped}${oversized}${incomplete}${r.held ? `; left waiting: ${r.held}` : ''}${r.error ? `; stopped: ${r.error}` : ''}`
}

// `cleanupCommand`: the command that deletes the replays' leftovers
// (buildCleanupCommand), shown once there are CLEANUP_HINT_FILES of them.
// `destinationCheck`: what asking BigQuery for the table found, when this
// command asked (Writer.probeDestination).
export function formatStatus(args: {
  config: Config
  writer: Writer
  now: number
  sessionId: string | null
  deadLetter: DeadLetterSummary
  backfill: BackfillReport | null
  isBackfilling: boolean
  cleanupCommand?: string | null
  destinationCheck?: string | null
  maskTransientSessionStart?: boolean
}): string {
  const { config: c, writer, now } = args
  const s = writer.snapshot()
  const k = s.counters
  const transient = args.maskTransientSessionStart === true
  const queued = transient ? 0 : s.queued
  const queuedBytes = transient ? 0 : s.queuedBytes
  const captured = transient ? Math.max(0, k.captured - s.queued) : k.captured
  const target = c.dryRun ? `dry run -> ${c.dryRunDir}` : c.project && c.dataset ? `${tableRef(c)} (${c.location})` : 'not configured'
  const lines = [
    short(`BigQuery Agent Analytics mod: ${c.enabled ? 'enabled' : 'disabled'}, target ${target}`),
    `queue: ${queued} rows (${kb(queuedBytes)}), ${s.inFlight} in flight; flush every ${c.flushIntervalMs} ms`,
    `rows: captured ${captured}, sent ${k.sent}${k.replayed > 0 ? ` (${k.replayed} replayed)` : ''}, dead-lettered ${k.failed} (${k.rejected} refused), spilled ${k.spilled}, dropped ${s.dropped}, not captured ${k.skipped}`,
    `requests: ${k.batches} ok, ${k.retries} retried; last success ${ago(now, s.lastSuccessAt)}`,
  ]
  if (args.destinationCheck) lines.push(short(`destination check: ${args.destinationCheck}`))
  if (s.consecutiveFailures > 0) lines.push(`backing off: ${s.consecutiveFailures} failure(s) in a row, next try in about ${Math.max(0, Math.round((s.pausedUntil - now) / 1000))}s`)
  if (s.problem) {
    const { headline, fix } = writer.problemText()
    // The cause last: where redaction cuts it (a project named like a
    // credential, `auth-…:` in what BigQuery said), the fix and the count stay.
    lines.push(short(`notice (since ${ago(now, s.problem.since)}): rows are not being written: ${headline}; to fix, ${fix}; ${s.waitingText} row(s) waiting; cause: ${s.problem.reason.slice(0, 200)}`))
    const command = writer.setupCommand()
    if (command !== null) lines.push(`setup: ${command}`)
  }
  // No credential word before a colon: the free-text rules would cut the
  // rest of the line.
  const identity = [c.userIdFromCredentials ? `user_id from the token's account (${s.identity})` : '', c.recordHostname ? 'host name recorded' : ''].filter(Boolean).join('; ')
  if (identity) lines.push(`identity: ${identity}`)
  if (s.lastError) lines.push(`last error (${ago(now, s.lastError.at)}): ${s.lastError.message.slice(0, 300)}`)
  if ('error' in args.deadLetter) lines.push(`dead-letter: ${args.deadLetter.error}`)
  else {
    const d = args.deadLetter
    const emptyFiles = d.emptyFiles ?? 0
    const recordFiles = d.recordFiles ?? 0
    const retired = d.retiredFiles ?? emptyFiles + recordFiles
    const configuredTarget = Boolean(c.project && c.dataset)
    if (configuredTarget || d.files > 0 || retired > 0) {
      const kinds = Object.entries(d.byKind)
        .map(([kind, n]) => `${n} ${kind}`)
        .join(', ')
      const counts =
        d.emptyFiles !== undefined || d.recordFiles !== undefined ? `: ${emptyFiles} empty file(s); ${recordFiles} replay record(s), kept on purpose` : ''
      const retiredNote = retired > 0 ? `; ${retired} replayed/retired file(s) (0-byte or outcome-*, expected leftovers; host fs cannot delete)${counts}` : ''
      lines.push(`dead-letter: ${d.files} file(s), ${kb(d.bytes)}${kinds ? ` (${kinds})` : ''} in ${d.dir}${retiredNote}`)
    }
    if (d.oversized.length > 0) lines.push(`dead-letter: ${d.oversized.length} file(s) over the 4 MiB read limit, not replayed: ${names(d.oversized)}`)
    const elsewhereRetired = d.elsewhereRetired ?? 0
    if (d.elsewhere > 0 || elsewhereRetired > 0) {
      lines.push(`dead-letter: ${d.elsewhere} waiting file(s)${elsewhereRetired > 0 ? ` (${elsewhereRetired} leftover(s))` : ''} in ${c.deadLetterDir} for other tables or an older layout, not replayed under this configuration`)
    }
    // No credential word before a colon here either.
    if (retired + elsewhereRetired >= CLEANUP_HINT_FILES && args.cleanupCommand) lines.push(`cleanup (run while no session is running; dry run by default, pass --execute to delete): ${args.cleanupCommand}`)
  }
  if (args.isBackfilling) lines.push('backfill: running')
  else if (args.backfill) {
    const b = args.backfill
    lines.push(
      `last backfill (${b.trigger}, ${ago(now, b.finishedAt)}): ${b.files} file(s), ${b.rowsSent} rows sent, ${b.rowsLeft} left${b.rowsRefused ? `, ${b.rowsRefused} of them refused` : ''}${b.otherTableRows ? `, ${b.otherTableRows} of them for another table` : ''}${b.skippedFiles ? `, ${b.skippedFiles} skipped` : ''}${b.oversizedFiles.length ? `, ${b.oversizedFiles.length} too large to read` : ''}${b.incompleteFiles.length ? `, ${b.incompleteFiles.length} incomplete` : ''}${b.held ? `; left waiting: ${b.held.slice(0, 200)}` : ''}${b.error ? `; stopped: ${b.error.slice(0, 200)}` : ''}`,
    )
    if (b.held) lines.push(`notice: ${b.held.slice(0, 200)}`)
    if (b.incompleteFiles.length > 0) {
      lines.push(`dead-letter: ${b.incompleteFiles.length} file(s) without a completion record (still being written, or left by a writer that stopped), kept, not replayed yet: ${names(b.incompleteFiles)}`)
    }
  }
  lines.push(`redactions ${k.redactions}, truncated rows ${k.truncatedRows}; session ${args.sessionId ?? 'unknown'}`)
  for (const problem of c.problems) lines.push(`problem: ${problem}`)
  for (const warning of c.warnings) lines.push(`config notice: ${warning}`)
  return redactDiagnostic(lines.join('\n'), 8000)
}
