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

// Bounded, redacted copies of what the agent, the model and its tools
// produced. Structure is kept under allowlisted keys: under a key that is
// not on the allowlist (allowlist.ts) the value goes whole, whatever it holds.
// Text that looks like JSON is parsed and held to the same list, or dropped
// whole when it cannot be parsed within the budget; it is never scrubbed line
// by line. Other text (prompts, answers, tool arguments and results, error
// text) is scanned within the character window for recognized credential
// shapes, named values and URL credentials. These are best-effort heuristics;
// the tests and README describe finite coverage and known gaps.
//
// Runs on the hot path. Character, depth, key and item budgets limit the copy
// and the text window scanned; timing tests exercise selected large inputs.
// Text past the retained budget is dropped and marked.

import { isListedKey, nameParts, MAX_LISTED_NAME_CHARS } from './allowlist.ts'

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue }

export type SanitizeStats = {
  // Something was cut (BQAA `is_truncated`).
  truncated: boolean
  // How many replacements were made; redaction alone does not set `truncated`,
  // as in the BQAA plugin, unless it removes text past the credential itself
  // (see noteCut).
  redactions: number
  // Characters such redactions removed past the credentials themselves.
  droppedChars: number
}

export type Limits = {
  // Characters of serialized JSON (quotes, escapes, keys and punctuation
  // counted) the whole copy may take.
  budgetChars: number
  maxStringChars: number
  maxDepth: number
  maxKeys: number
  maxItems: number
}

export const newStats = (): SanitizeStats => ({ truncated: false, redactions: 0, droppedChars: 0 })

// Room left under a column's cap for the truncation markers the walk adds
// (at most one per nesting level once the budget is spent), so a cut copy
// still serializes within `maxPayloadChars` and keeps its shape.
const MARKER_HEADROOM = 512

export const limitsFor = (maxPayloadChars: number): Limits => {
  const budget = Math.max(256, maxPayloadChars - MARKER_HEADROOM)
  return { budgetChars: budget, maxStringChars: budget, maxDepth: 12, maxKeys: 200, maxItems: 200 }
}

const MARKER = '[REDACTED]'

// Extra scan context past a cut for matches that fit within this window.
// It does not cover arbitrarily long credentials; the margin is dropped.
const REDACT_MARGIN = 8192

// ---- credential names in text -------------------------------------------------

// In free text (a header, an assignment, a URL parameter) a name is a
// credential's when one of its parts, split on punctuation and camelCase
// (`db_password`, `X-Api-Key`, `sessionCookie`, `MY_PWD`), is one of these
// words...
const SENSITIVE_WORDS = new Set([
  'pass',
  'passwd',
  'password',
  'passwords',
  'passphrase',
  'pwd',
  'secret',
  'secrets',
  'token',
  'auth',
  'authorization',
  'cookie',
  'cookies',
  'session',
  'key',
  'apikey',
  'credential',
  'credentials',
  'private',
  'bearer',
  'jwt',
  'sig',
  'signature',
])
// ...ends with one of these (`rootpwd`, `accesstoken`, `usersession`)...
const SENSITIVE_ENDINGS = [
  'passwd',
  'password',
  'passphrase',
  'pwd',
  'secret',
  'token',
  'authorization',
  'cookie',
  'session',
  'credential',
  'credentials',
  'bearer',
  'jwt',
  'signature',
  'apikey',
  'accesskey',
  'privatekey',
  'secretkey',
  'signingkey',
]
// ...or contains one of these (`passwordhash`, `privatekeypem`). Short words
// (`pass`, `key`, `auth`, `sig`) match only as a whole part: `bypass`,
// `monkey`, `oauth` and `signal` are not credential names.
const SENSITIVE_STEMS = ['password', 'passwd', 'apikey', 'accesskey', 'privatekey', 'secretkey', 'signingkey', 'credential']

// A part also counts without trailing digits and a plural `s`: `token2`,
// `api_keys`, `access_tokens` and `sessions` are credential names too.
const forms = (part: string): string[] => {
  let end = part.length
  while (end > 0 && part.charCodeAt(end - 1) >= 48 && part.charCodeAt(end - 1) <= 57) end -= 1
  const bare = part.slice(0, end)
  return bare.endsWith('s') ? [part, bare, bare.slice(0, -1)] : [part, bare]
}

const isCredentialWord = (word: string): boolean =>
  word !== '' && (SENSITIVE_WORDS.has(word) || SENSITIVE_ENDINGS.some(end => word.endsWith(end)) || SENSITIVE_STEMS.some(stem => word.includes(stem)))

// Each word, ending and stem above holds one of these, and a name's parts
// are pieces of its lower-case form: a name that holds none is no
// credential's, which is quick to tell.
const CREDENTIAL_PIECE = /pass|pwd|secret|token|auth|cookie|session|key|credential|private|bearer|jwt|sig/
// The same, in either case, for a name a word's reading gives (name
// characters alone).
const PIECE_IN_NAME = new RegExp(CREDENTIAL_PIECE.source, 'i')

// Whether each name that holds such a piece is a credential's, by the name as
// it came: a text holds the same few names again and again (one for each
// option word or list entry), and their parts are slow to tell. Up to 4,096
// names of up to 200 characters are kept.
const SENSITIVE_NAMES = new Map<string, boolean>()

export const isSensitiveKey = (name: string): boolean => {
  const lower = name.toLowerCase()
  // ADK's per-invocation state.
  if (lower.startsWith('temp:')) return true
  if (!CREDENTIAL_PIECE.test(lower)) return false
  const known = SENSITIVE_NAMES.get(name)
  if (known !== undefined) return known
  // Listed names, such as the token counts, are not credentials.
  const sensitive = !isListedKey(name) && nameParts(name).some(part => forms(part).some(isCredentialWord))
  if (name.length <= 200) {
    if (SENSITIVE_NAMES.size >= 4096) SENSITIVE_NAMES.clear()
    SENSITIVE_NAMES.set(name, sensitive)
  }
  return sensitive
}

// ---- free text ---------------------------------------------------------------

// Recognized credential shapes. Timing tests exercise selected matching and
// nonmatching inputs; those workloads do not establish a bound for all text.
// Unclosed private keys, JWTs and Bearer/Basic tokens run in linear helpers
// below so adversarial inputs stay O(N) and truncated headers set stats.truncated.
const PATTERNS: readonly (readonly [RegExp, string])[] = [
  [/\bAGE-SECRET-KEY-1[QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L]{58,}\b/gi, '[REDACTED:private-key]'],
  [/\bsk-ant-[A-Za-z0-9_-]{16,}/g, '[REDACTED:api-key]'],
  [/\bsk-(?=[A-Za-z0-9_-]{0,200}\d)[A-Za-z0-9_-]{32,}/g, '[REDACTED:api-key]'],
  [/\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g, '[REDACTED:aws-access-key-id]'],
  [/\bAIza[0-9A-Za-z_-]{35}/g, '[REDACTED:google-api-key]'],
  [/\bya29\.[0-9A-Za-z._-]{20,}/g, '[REDACTED:google-oauth-token]'],
  [/\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})/g, '[REDACTED:github-token]'],
  [/\bglpat-[A-Za-z0-9_-]{20,}/g, '[REDACTED:gitlab-token]'],
  [/\bnpm_[A-Za-z0-9]{36,}/g, '[REDACTED:npm-token]'],
  [/\bxox[abposr]-[A-Za-z0-9-]{10,}/g, '[REDACTED:slack-token]'],
  [/\b[rs]k_(?:live|test)_[A-Za-z0-9]{16,}/g, '[REDACTED:stripe-key]'],
]

const PEM_PRIVATE_BEGIN = /-----BEGIN ([A-Z0-9 ]{0,40})PRIVATE KEY-----/g
const PEM_PRIVATE_END = /-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----/g
const PGP_PRIVATE_BEGIN = '-----BEGIN PGP PRIVATE KEY BLOCK-----'
const PGP_PRIVATE_END = '-----END PGP PRIVATE KEY BLOCK-----'
const PUTTY_HEADER = /PuTTY-User-Key-File-[23]:[ \t]*[^\r\n]*/g
const PUTTY_NEXT_HDR = /\r?\nPuTTY-User-Key-File-\d+:/
const PUTTY_PRIVATE_LINES_NL = /\r?\nPrivate-Lines:[ \t]*(\d+)[ \t]*\r?\n/
const PUTTY_PRIVATE_LINES_EOF = /\r?\nPrivate-Lines:[ \t]*(\d+)[ \t]*(?:\r?\n|$)/
const PUTTY_TRUNC_SUBHDR = /^\r?\n(?:Encryption|Comment|Public-Lines):/

// Redacts PEM private keys, PGP private key blocks and PuTTY .ppk private
// material before other passes. Unclosed PEM/PGP private blocks cut to EOF and
// record truncation stats (`is_truncated` / `dropped_chars`).
function redactPrivateKeys(text: string, stats: SanitizeStats): string {
  let current = text
  if (current.includes('-----BEGIN ')) {
    // 1. PEM PRIVATE KEY blocks (closed and unclosed).
    PEM_PRIVATE_BEGIN.lastIndex = 0
    let out = ''
    let copied = 0
    for (let m = PEM_PRIVATE_BEGIN.exec(current); m !== null; m = PEM_PRIVATE_BEGIN.exec(current)) {
      const start = m.index
      const afterHdr = start + m[0].length
      PEM_PRIVATE_END.lastIndex = afterHdr
      const endMatch = PEM_PRIVATE_END.exec(current)
      out += current.slice(copied, start) + '[REDACTED:private-key]'
      stats.redactions += 1
      if (endMatch !== null) {
        copied = endMatch.index + endMatch[0].length
        PEM_PRIVATE_BEGIN.lastIndex = copied
      } else {
        noteCut(current, start, afterHdr, current.length, stats)
        copied = current.length
        break
      }
    }
    if (copied > 0) current = out + current.slice(copied)

    // 2. PGP PRIVATE KEY BLOCK (closed and unclosed).
    if (current.includes(PGP_PRIVATE_BEGIN)) {
      let pgpOut = ''
      let pgpCopied = 0
      for (let pos = current.indexOf(PGP_PRIVATE_BEGIN); pos !== -1; pos = current.indexOf(PGP_PRIVATE_BEGIN, pgpCopied)) {
        const afterHdr = pos + PGP_PRIVATE_BEGIN.length
        const footer = current.indexOf(PGP_PRIVATE_END, afterHdr)
        pgpOut += current.slice(pgpCopied, pos) + '[REDACTED:private-key]'
        stats.redactions += 1
        if (footer !== -1) {
          pgpCopied = footer + PGP_PRIVATE_END.length
        } else {
          noteCut(current, pos, afterHdr, current.length, stats)
          pgpCopied = current.length
          break
        }
      }
      if (pgpCopied > 0) current = pgpOut + current.slice(pgpCopied)
    }
  }

  // 3. PuTTY .ppk files (`PuTTY-User-Key-File-2:` / `PuTTY-User-Key-File-3:`).
  if (current.includes('PuTTY-User-Key-File-')) {
    const hasPrivateLines = current.includes('Private-Lines:')
    PUTTY_HEADER.lastIndex = 0
    let ppkOut = ''
    let ppkCopied = 0
    for (let hm = PUTTY_HEADER.exec(current); hm !== null; hm = PUTTY_HEADER.exec(current)) {
      if (hm.index < ppkCopied) continue
      const headerEnd = hm.index + hm[0].length
      let pm: RegExpExecArray | null = null
      if (hasPrivateLines) {
        const rawWindow = current.slice(headerEnd, headerEnd + 8256)
        const nextHdrMatch = PUTTY_NEXT_HDR.exec(rawWindow)
        const searchWindow =
          nextHdrMatch === null
            ? rawWindow
            : rawWindow.slice(0, nextHdrMatch.index + (rawWindow[nextHdrMatch.index] === '\r' ? 2 : 1))
        const atEof = nextHdrMatch === null && headerEnd + rawWindow.length >= current.length
        pm = (atEof ? PUTTY_PRIVATE_LINES_EOF : PUTTY_PRIVATE_LINES_NL).exec(searchWindow)
      }
      if (pm !== null && pm.index <= 8192) {
        const lineCount = Number(pm[1])
        const bodyStart = headerEnd + pm.index + pm[0].length
        if (lineCount > 0 && current.slice(bodyStart, bodyStart + 22) === '[REDACTED:private-key]') continue
        if (lineCount <= 0) continue
        let cursor = bodyStart
        let remaining = lineCount
        while (remaining > 0 && cursor < current.length) {
          const nl = current.indexOf('\n', cursor)
          if (nl === -1) {
            cursor = current.length
            remaining -= 1
            break
          }
          cursor = nl + 1
          remaining -= 1
        }
        if (remaining > 0) {
          ppkOut += current.slice(ppkCopied, hm.index) + '[REDACTED:private-key]'
          noteCut(current, hm.index, hm.index + hm[0].length, current.length, stats)
          stats.redactions += 1
          ppkCopied = current.length
          break
        }
        // Consume optional trailing Private-MAC / Private-Hash line.
        const macSlice = current.slice(cursor, cursor + 256)
        const macMatch = /^Private-(?:MAC|Hash):[ \t]*[^\r\n]*/.exec(macSlice)
        if (macMatch !== null) cursor += macMatch[0].length
        ppkOut += current.slice(ppkCopied, hm.index) + '[REDACTED:private-key]'
        stats.redactions += 1
        ppkCopied = cursor
        PUTTY_HEADER.lastIndex = cursor
      } else {
        // Multi-line .ppk header cut short before Private-Lines.
        const nextLine = current.slice(headerEnd, headerEnd + 256)
        if (PUTTY_TRUNC_SUBHDR.test(nextLine)) {
          ppkOut += current.slice(ppkCopied, hm.index) + '[REDACTED:private-key]'
          noteCut(current, hm.index, hm.index + hm[0].length, current.length, stats)
          stats.redactions += 1
          ppkCopied = current.length
          break
        }
      }
    }
    if (ppkCopied > 0) current = ppkOut + current.slice(ppkCopied)
  }
  return current
}

const isB64UrlCharCode = (code: number): boolean =>
  (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || (code >= 48 && code <= 57) || code === 95 || code === 45
const isWordCharCode = (code: number): boolean =>
  (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || (code >= 48 && code <= 57) || code === 95

// O(N) JWT scanner anchored on `.eyJ` with a monotonic backward floor.
function redactJwtsLinear(text: string, stats: SanitizeStats): string {
  let dot1 = text.indexOf('.eyJ')
  if (dot1 === -1) return text
  let out = ''
  let copied = 0
  let floor = 0
  let pos = dot1
  while (pos !== -1) {
    let pEnd = pos + 4
    while (pEnd < text.length && isB64UrlCharCode(text.charCodeAt(pEnd))) pEnd += 1
    if (pEnd - (pos + 1) < 11 || text.charCodeAt(pEnd) !== 46) {
      floor = Math.max(floor, pEnd)
      pos = text.indexOf('.eyJ', Math.max(pos + 4, floor))
      continue
    }
    let sigEnd = pEnd + 1
    while (sigEnd < text.length && isB64UrlCharCode(text.charCodeAt(sigEnd))) sigEnd += 1
    if (sigEnd - (pEnd + 1) < 8) {
      floor = Math.max(floor, pEnd + 1)
      pos = text.indexOf('.eyJ', floor)
      continue
    }
    // Valid payload and signature found; scan left up to `floor` for earliest valid `eyJ` header start.
    let runStart = pos
    while (runStart > floor && isB64UrlCharCode(text.charCodeAt(runStart - 1))) runStart -= 1
    let hdrStart = -1
    const maxStart = pos - 11
    for (let k = runStart; k <= maxStart; k += 1) {
      if (text.charCodeAt(k) !== 101 || text.charCodeAt(k + 1) !== 121 || text.charCodeAt(k + 2) !== 74) continue
      const prevBoundary = k === 0 || !isWordCharCode(text.charCodeAt(k - 1))
      if (prevBoundary) {
        hdrStart = k
        break
      }
    }
    if (hdrStart === -1) {
      floor = Math.max(floor, pos + 1)
      pos = text.indexOf('.eyJ', pEnd)
      continue
    }
    out += text.slice(copied, hdrStart) + '[REDACTED:jwt]'
    stats.redactions += 1
    copied = sigEnd
    floor = sigEnd
    pos = text.indexOf('.eyJ', floor)
  }
  return copied === 0 ? text : out + text.slice(copied)
}

const B64_LOOKUP = (() => {
  const table = new Int8Array(128).fill(-1)
  for (let i = 0; i < 26; i += 1) {
    table[65 + i] = i
    table[97 + i] = 26 + i
  }
  for (let i = 0; i < 10; i += 1) table[48 + i] = 52 + i
  table[43] = 62 // '+'
  table[45] = 62 // '-'
  table[47] = 63 // '/'
  table[95] = 63 // '_'
  return table
})()

// Pure ES2023 Base64 validator (no Buffer/atob) returning true iff `token`
// decodes to valid printable UTF-8 or RFC 7617 section 2 ISO-8859-1 (Latin-1)
// (no C0/C1/DEL control chars) containing `:`.
function isPrintableUtf8WithColon(bytes: number[]): boolean {
  let hasColon = false
  let hasNonColon = false
  for (let idx = 0; idx < bytes.length; ) {
    const b0 = bytes[idx] as number
    if (b0 < 0x80) {
      if (b0 < 0x20 || b0 === 0x7f) return false
      if (b0 === 0x3a) hasColon = true
      else hasNonColon = true
      idx += 1
      continue
    }
    let cp = 0
    let seqLen = 0
    if (b0 >= 0xc2 && b0 <= 0xdf) {
      seqLen = 2
      if (idx + 1 >= bytes.length) return false
      const b1 = bytes[idx + 1] as number
      if ((b1 & 0xc0) !== 0x80) return false
      cp = ((b0 & 0x1f) << 6) | (b1 & 0x3f)
    } else if (b0 >= 0xe0 && b0 <= 0xef) {
      seqLen = 3
      if (idx + 2 >= bytes.length) return false
      const b1 = bytes[idx + 1] as number
      const b2 = bytes[idx + 2] as number
      if ((b1 & 0xc0) !== 0x80 || (b2 & 0xc0) !== 0x80) return false
      if (b0 === 0xe0 && b1 < 0xa0) return false
      if (b0 === 0xed && b1 >= 0xa0) return false
      cp = ((b0 & 0x0f) << 12) | ((b1 & 0x3f) << 6) | (b2 & 0x3f)
    } else if (b0 >= 0xf0 && b0 <= 0xf4) {
      seqLen = 4
      if (idx + 3 >= bytes.length) return false
      const b1 = bytes[idx + 1] as number
      const b2 = bytes[idx + 2] as number
      const b3 = bytes[idx + 3] as number
      if ((b1 & 0xc0) !== 0x80 || (b2 & 0xc0) !== 0x80 || (b3 & 0xc0) !== 0x80) return false
      if (b0 === 0xf0 && b1 < 0x90) return false
      if (b0 === 0xf4 && b1 >= 0x90) return false
      cp = ((b0 & 0x07) << 18) | ((b1 & 0x3f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f)
    } else {
      return false
    }
    if ((cp >= 0x80 && cp <= 0x9f) || cp > 0x10ffff) return false
    hasNonColon = true
    idx += seqLen
  }
  return hasColon && hasNonColon
}

function isPrintableLatin1WithColon(bytes: number[]): boolean {
  let hasColon = false
  let hasNonColon = false
  for (let idx = 0; idx < bytes.length; idx += 1) {
    const b = bytes[idx] as number
    if (b < 0x20 || (b >= 0x7f && b <= 0x9f)) return false
    if (b === 0x3a) hasColon = true
    else hasNonColon = true
  }
  return hasColon && hasNonColon
}

function decodeBase64PrintableColon(token: string): boolean {
  let cleanLen = token.length
  while (cleanLen > 0 && token.charCodeAt(cleanLen - 1) === 61) cleanLen -= 1
  const pad = token.length - cleanLen
  if (pad > 2 || cleanLen < 4 || (pad > 0 && token.length % 4 !== 0) || cleanLen % 4 === 1) return false
  const bytes: number[] = []
  let i = 0
  while (i + 4 <= cleanLen) {
    const c0 = token.charCodeAt(i)
    const c1 = token.charCodeAt(i + 1)
    const c2 = token.charCodeAt(i + 2)
    const c3 = token.charCodeAt(i + 3)
    if (c0 > 127 || c1 > 127 || c2 > 127 || c3 > 127) return false
    const v0 = B64_LOOKUP[c0] as number
    const v1 = B64_LOOKUP[c1] as number
    const v2 = B64_LOOKUP[c2] as number
    const v3 = B64_LOOKUP[c3] as number
    if (v0 < 0 || v1 < 0 || v2 < 0 || v3 < 0) return false
    const n = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3
    bytes.push((n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff)
    i += 4
  }
  const rem = cleanLen - i
  if (rem === 2) {
    const c0 = token.charCodeAt(i)
    const c1 = token.charCodeAt(i + 1)
    if (c0 > 127 || c1 > 127) return false
    const v0 = B64_LOOKUP[c0] as number
    const v1 = B64_LOOKUP[c1] as number
    if (v0 < 0 || v1 < 0) return false
    bytes.push(((v0 << 18) | (v1 << 12)) >>> 16)
  } else if (rem === 3) {
    const c0 = token.charCodeAt(i)
    const c1 = token.charCodeAt(i + 1)
    const c2 = token.charCodeAt(i + 2)
    if (c0 > 127 || c1 > 127 || c2 > 127) return false
    const v0 = B64_LOOKUP[c0] as number
    const v1 = B64_LOOKUP[c1] as number
    const v2 = B64_LOOKUP[c2] as number
    if (v0 < 0 || v1 < 0 || v2 < 0) return false
    const n = (v0 << 18) | (v1 << 12) | (v2 << 6)
    bytes.push((n >>> 16) & 0xff, (n >>> 8) & 0xff)
  }
  if (bytes.length < 3) return false
  if (isPrintableUtf8WithColon(bytes)) return true
  if (!isPrintableLatin1WithColon(bytes)) return false
  return !PASCAL_CASE_WORD_RE.test(token) && !LOWER_ALPHA_WORD_RE.test(token)
}

const PASCAL_CASE_WORD_RE = /^[A-Z][a-z]+(?:[A-Z][a-z]+)*$/
const LOWER_ALPHA_WORD_RE = /^[a-z]+$/
const PASCAL_COMPONENT_RE = /[A-Z][a-z]+/g
const PROSE_PASCAL_WORDS = new Set([
  'configuration', 'word', 'words', 'authentication', 'authorization',
  'documentation', 'description', 'specification', 'implementation',
  'troubleshooting', 'information', 'overview', 'example', 'examples',
  'details', 'section', 'format', 'scheme', 'header', 'headers',
  'guide', 'reference', 'summary', 'introduction', 'quickstart',
  'migration', 'options', 'settings', 'usage', 'notes',
])

const isDocumentationPascalPhrase = (token: string): boolean => {
  PASCAL_COMPONENT_RE.lastIndex = 0
  let count = 0
  for (let m = PASCAL_COMPONENT_RE.exec(token); m !== null; m = PASCAL_COMPONENT_RE.exec(token)) {
    if (!PROSE_PASCAL_WORDS.has(m[0].toLowerCase())) return false
    count += 1
  }
  return count > 0
}

const isBase64Credential = (token: string): boolean => {
  if (decodeBase64PrintableColon(token)) return true
  let cleanEnd = token.length
  while (cleanEnd > 0) {
    const ch = token.charCodeAt(cleanEnd - 1)
    if (ch === 46 || ch === 95 || ch === 126 || ch === 45) cleanEnd -= 1
    else break
  }
  return cleanEnd >= 4 && cleanEnd < token.length && decodeBase64PrintableColon(token.slice(0, cleanEnd))
}

const isLikelyBasicCredentialToken = (token: string): boolean => {
  if (isBase64Credential(token)) return true
  if (token.length < 12) return false
  if (/[0-9+/=_.-]/.test(token)) return true
  if (/[a-z]/.test(token) && /[A-Z]/.test(token)) {
    return !PASCAL_CASE_WORD_RE.test(token)
  }
  return false
}

const ALL_CAPS_SNAKE_KEY_RE = /^[A-Z][A-Z0-9]*(?:_[A-Z0-9]*)+$/
const LOWER_SNAKE_KEY_RE = /^[a-z][a-z0-9]*(?:_[a-z0-9]*)+$/
const PYTHON_SINGLE_PREFIX_RE = /(?:^|[^A-Za-z0-9_])(?:[rbfu]|rf|fr|rb|br)$/i
const RUST_LIFETIME_PREFIX_CHARS = new Set(['<', '&', ',', '+', ':'])
const RUST_LIFETIME_TAIL_RE = /^'(?:[a-z_][a-z0-9_]*|static)(?=$|[\s,>:;)\]+])/
const RUST_BOUND_LIFETIME_TAIL_RE = /^'(?:[a-z_][a-z0-9_]*|static)(?=$|[ \t]*[,>+;{)=])/
const RUST_COMMA_PLUS_LIFETIME_TAIL_RE = /^'(?:[a-z_][a-z0-9_]*|static)(?=$|[ \t]*[,>+;{)=]|\r?\n)/

const isBase64PaddedToken = (token: string): boolean => {
  let cleanEnd = token.length
  while (cleanEnd > 0) {
    const ch = token.charCodeAt(cleanEnd - 1)
    if (ch === 46 || ch === 95 || ch === 126 || ch === 45) cleanEnd -= 1
    else break
  }
  if (cleanEnd < 12 || cleanEnd % 4 !== 0) return false
  let pad = 0
  while (pad < 2 && cleanEnd - pad > 0 && token.charCodeAt(cleanEnd - 1 - pad) === 61) pad += 1
  if (pad === 0) return false
  for (let i = 0; i < cleanEnd - pad; i += 1) {
    const c = token.charCodeAt(i)
    const ok =
      (c >= 65 && c <= 90) ||
      (c >= 97 && c <= 122) ||
      (c >= 48 && c <= 57) ||
      c === 43 ||
      c === 47 ||
      c === 95 ||
      c === 45
    if (!ok) return false
  }
  return true
}

const isLineBreakCode = (code: number): boolean =>
  code === 10 || code === 13 || code === 0x2028 || code === 0x2029

const lineEndIndex = (text: string, from: number): number => {
  for (let i = from; i < text.length; i += 1) {
    if (isLineBreakCode(text.charCodeAt(i))) return i
  }
  return text.length
}

const isHashCommentStart = (text: string, i: number): boolean =>
  text[i] === '#' && (i === 0 || /[\s;|&(]/.test(text[i - 1] as string))

const isSlashCommentStart = (text: string, i: number): boolean =>
  text[i] === '/' &&
  text[i + 1] === '/' &&
  (i === 0 || /[\s;{}()[\],]/.test(text[i - 1] as string))

const isNonQuoteSingleQuote = (text: string, i: number): boolean => {
  const prev = i > 0 ? (text[i - 1] as string) : ''
  const next = text[i + 1] ?? ''
  if (/[A-Za-z0-9]/.test(prev) && /[A-Za-z]/.test(next)) {
    return !PYTHON_SINGLE_PREFIX_RE.test(text.slice(Math.max(0, i - 3), i))
  }
  if (
    (prev === 's' || prev === 'S') &&
    i >= 2 &&
    /[A-Za-z0-9]/.test(text[i - 2] as string) &&
    (next === '' || /^[\s.,;:!?)\]}]/.test(next))
  ) {
    return true
  }
  const prevNonSpace = (prev === ' ' || prev === '\t') && i >= 2 ? (text[i - 2] as string) : prev
  if (RUST_LIFETIME_PREFIX_CHARS.has(prevNonSpace)) {
    const tail = text.slice(i, i + 32)
    if (prevNonSpace === '<' || prevNonSpace === '&') return RUST_LIFETIME_TAIL_RE.test(tail)
    if (prevNonSpace === ':') return RUST_BOUND_LIFETIME_TAIL_RE.test(tail)
    return RUST_COMMA_PLUS_LIFETIME_TAIL_RE.test(tail)
  }
  return false
}

const addQuoteParity = (rem: number): number => (rem === 0 ? 1 : rem === 1 ? 2 : rem === 2 ? 3 : 2)

const isDelimOrSpaceAfterQuote = (ch: string | undefined): boolean =>
  ch === undefined || /^[\s.,;:!?)\]}'"`]/.test(ch)

const hasSpacedSameQuoteOpen = (
  text: string,
  afterStart: number,
  lineStop: number,
  q: string,
  escaped: boolean,
): boolean => {
  let p = afterStart
  while (p < lineStop && (text[p] === ' ' || text[p] === '\t')) p += 1
  if (p === afterStart) return false
  if (escaped) {
    return p + 2 < lineStop && text[p] === '\\' && text[p + 1] === q && !/[\s"'`,;)}\]]/.test(text[p + 2] as string)
  }
  return p + 1 < lineStop && text[p] === q && !/[\s"'`,;)}\]]/.test(text[p + 1] as string)
}

const evalQuoteRemainder = (
  text: string,
  afterStart: number,
  lineStop: number,
  q: string,
  escaped: boolean,
  rem: number,
): boolean => {
  if (rem === 0) return afterStart < lineStop && !isDelimOrSpaceAfterQuote(text[afterStart])
  if (rem === 2) return false
  if (rem === 3) return true
  return !hasSpacedSameQuoteOpen(text, afterStart, lineStop, q, escaped)
}

const opensQuotedAssignmentValue = (
  text: string,
  quoteIdx: number,
  q: string,
  lineStop = lineEndIndex(text, quoteIdx + 1),
): boolean => {
  if (q === '`' && text[quoteIdx + 1] === '`') return false
  let count = 1
  let ansiSingle = false
  let i = quoteIdx + 1
  while (i < lineStop) {
    const ch = text[i] as string
    if (count % 2 === 1) {
      if ((q !== "'" || ansiSingle) && ch === '\\') {
        const next = text[i + 1]
        if (next === '\\' || next === q) {
          i += 2
          continue
        }
      }
      if (ch === q) {
        count += 1
        ansiSingle = false
      }
      i += 1
      continue
    }
    if (isHashCommentStart(text, i) || isSlashCommentStart(text, i)) break
    if (ch === '\\') {
      const next = text[i + 1]
      if (next === '\\' || next === '"' || next === "'" || next === '`' || next === '$') {
        i += 2
        continue
      }
      i += 1
      continue
    }
    if (q === "'" && ch === '$' && text[i + 1] === "'") {
      count += 1
      ansiSingle = true
      i += 2
      continue
    }
    if (ch === q) {
      if (q === "'" && isNonQuoteSingleQuote(text, i)) {
        i += 1
        continue
      }
      count += 1
    }
    i += 1
  }
  const rem = count === 1 ? 0 : count === 2 ? 1 : count % 2 === 1 ? 2 : 3
  return evalQuoteRemainder(text, quoteIdx + 1, lineStop, q, false, rem)
}

const opensEscapedQuotedAssignmentValue = (
  text: string,
  quoteIdx: number,
  q: string,
  lineStop = lineEndIndex(text, quoteIdx + 2),
): boolean => {
  let count = 1
  let i = quoteIdx + 2
  while (i < lineStop) {
    const ch = text[i] as string
    if (ch === '\\') {
      const next = text[i + 1]
      if (next === q) {
        count += 1
        i += 2
        continue
      }
      if (next === '\\') {
        i += 2
        continue
      }
    }
    i += 1
  }
  const rem = count === 1 ? 0 : count === 2 ? 1 : count % 2 === 1 ? 2 : 3
  return evalQuoteRemainder(text, quoteIdx + 2, lineStop, q, true, rem)
}

type LineQuoteTable = { lineStart: number; lineStop: number; s1: Uint8Array }

function buildUnescapedLineTable(text: string, lineStart: number, lineStop: number, q: string): LineQuoteTable {
  const len = lineStop - lineStart
  const s0 = new Uint8Array(len + 1)
  const s1 = new Uint8Array(len + 1)
  const s2 = q === "'" ? new Uint8Array(len + 1) : undefined
  for (let i = lineStop - 1; i >= lineStart; i -= 1) {
    const idx = i - lineStart
    const ch = text[i] as string
    const next = i + 1 < lineStop ? (text[i + 1] as string) : ''
    if (q !== "'" && ch === '\\' && (next === '\\' || next === q)) {
      s1[idx] = s1[idx + 2] as number
    } else if (ch === q) {
      s1[idx] = addQuoteParity(s0[idx + 1] as number)
    } else {
      s1[idx] = s1[idx + 1] as number
    }
    if (s2 !== undefined) {
      if (ch === '\\' && (next === '\\' || next === "'")) {
        s2[idx] = s2[idx + 2] as number
      } else if (ch === "'") {
        s2[idx] = addQuoteParity(s0[idx + 1] as number)
      } else {
        s2[idx] = s2[idx + 1] as number
      }
    }
    if (isHashCommentStart(text, i) || isSlashCommentStart(text, i)) {
      s0[idx] = 0
    } else if (ch === '\\' && (next === '\\' || next === '"' || next === "'" || next === '`' || next === '$')) {
      s0[idx] = s0[idx + 2] as number
    } else if (q === "'" && ch === '$' && next === "'") {
      s0[idx] = addQuoteParity(s2![idx + 2] as number)
    } else if (ch === q && !(q === "'" && isNonQuoteSingleQuote(text, i))) {
      s0[idx] = addQuoteParity(s1[idx + 1] as number)
    } else {
      s0[idx] = s0[idx + 1] as number
    }
  }
  return { lineStart, lineStop, s1 }
}

function buildEscapedLineTable(text: string, lineStart: number, lineStop: number, q: string): LineQuoteTable {
  const len = lineStop - lineStart
  const s1 = new Uint8Array(len + 1)
  for (let i = lineStop - 1; i >= lineStart; i -= 1) {
    const idx = i - lineStart
    if (text[i] === '\\' && i + 1 < lineStop) {
      const next = text[i + 1] as string
      if (next === q) s1[idx] = addQuoteParity(s1[idx + 2] as number)
      else if (next === '\\') s1[idx] = s1[idx + 2] as number
      else s1[idx] = s1[idx + 1] as number
    } else {
      s1[idx] = s1[idx + 1] as number
    }
  }
  return { lineStart, lineStop, s1 }
}

const AUTH_SCHEME_RE = /\b(Bearer|Basic)(\s+)([A-Za-z0-9._~+/=-]{4,})/gi
const CAMEL_CASE_KEY_RE = /^[a-z]+(?:[A-Z][a-z]+)+$/
const CAMEL_CASE_VERSION_KEY_RE = /^(?:[a-z]{2,}[0-9]{0,3}|v[0-9]{1,2})(?:V[0-9]{1,2}|[A-Z][a-z]{2,}[0-9]{0,2})*[A-Z][a-z]{2,}$/
const UPPER_WORD_KEY_RE = /^[A-Z]{2,}$/
const LOWER_DOTTED_KEY_RE = /^[a-z][a-z0-9_]*(?:\.[a-z][a-z_]*)+$/
const LOWER_KEBAB_KEY_RE = /^[a-z]+(?:-[a-z]+)+$/
const DOUBLE_DASH_LOWER_FLAG_RE = /^--[a-z][a-z0-9_-]*$/
const SINGLE_DASH_SNAKE_OR_LOWER_FLAG_RE = /^-(?:[a-z][a-z0-9]*(?:_[a-z0-9]*)+|[a-z]+)$/
const LOWER_WORD_KEY_RE = /^[a-z]+$/
const STRUCTURED_CONFIG_KEY_RE = /^(?:[A-Z][A-Z0-9]*(?:_[A-Z0-9]*)+|[a-z]+(?:[_.-][a-z]+)+|[A-Z][a-z]+(?:[A-Z][a-z]+)+)$/
const DIAGNOSTIC_SINGLE_WORD_RE =
  /^(?:unauthorized|forbidden|expired|rejected|denied|failed|invalid|missing|revoked|malformed|unauthenticated|unavailable|blocked|disabled|error|invalid_token|invalid_request|invalid_grant|access_denied|unauthorized_client|insufficient_scope|server_error)(?=$|[\s.,;:!?)\]}'"])/i
const DIAGNOSTIC_PROSE_RE = /^(?:[A-Za-z]{2,}(?:[ \t]+[A-Za-z]{2,})+|(?:HTTP(?:\/[\d.]+)?[ \t]+)?(?:4\d\d|5\d\d)(?:[ \t]+[A-Za-z]+)*)(?=$|[\s.,;:!?)\]}'"])/
const ASSIGN_RHS_VAR_RE = /^\$(?:[A-Za-z_({$])/
const ASSIGN_RHS_REDACTED_RE = /^\[REDACTED(?::[a-z-]+)?\](?:$|[\s,;:|)&'"`\].!?])/
const ASSIGN_RHS_PUNCT_RE = /^(?:[@#%^*({[]|[!?:&](?!$|[\s'")\]},;!?])|\\(?:\\|\$|[^nrt"'\\]))/
const UNICODE_WS_RE = /\s/
const PLAIN_KEY_TOKEN_RE = /^[A-Za-z_](?:[A-Za-z0-9_.-]*[A-Za-z0-9_])?$/
// Only placeholders of length >= 8 can reach the `shortHeaderToken` gate;
// shorter placeholders (such as `none`, `null`, `token`, `n/a`) are already
// excluded by the `coreToken.length >= 8` requirement.
const HEADER_PLACEHOLDER_TOKENS = new Set([
  'placeholder',
  'redacted',
  'undefined',
])

const isSameLineWsAt = (text: string, idx: number): boolean => {
  const code = text.charCodeAt(idx)
  return (
    code === 32 ||
    code === 9 ||
    code === 160 ||
    code === 11 ||
    code === 12 ||
    (code >= 0x1680 && code !== 0x2028 && code !== 0x2029 && UNICODE_WS_RE.test(text[idx] as string))
  )
}

const isHeaderWsAt = (text: string, idx: number): boolean => {
  const code = text.charCodeAt(idx)
  return code === 10 || code === 13 || code === 0x2028 || code === 0x2029 || isSameLineWsAt(text, idx)
}

// Walks backward linearly from `idx` across arbitrary header/JSON whitespace,
// optional YAML block indicators (`|`, `|-`, `|+`, `>`, `>-`, `>+`, `|2-`, `>2`),
// and an optional opening quote (1 or 3 consecutive `"`/`'`, or single `` ` ``,
// with optional string-literal prefixes) to detect `*authorization` headers
// (`:`, `=`, `:=`, `::=`, `=>`, or quoted tuple `,`) without fixed-window
// cutoff or backtracking. Total work across all calls is O(L) total across
// disjoint inter-match gaps because each matched scheme starts with
// non-whitespace letters. Returns 0 (not header), 1 (same-line), or 2
// (cross-line after separator).
function explicitAuthHeaderKind(text: string, idx: number): 0 | 1 | 2 {
  if (idx < 14) return 0
  let p = idx
  let sawNewline = false
  while (p > 0 && isHeaderWsAt(text, p - 1)) {
    if (isLineBreakCode(text.charCodeAt(p - 1))) sawNewline = true
    p -= 1
  }
  const openQ = p > 0 ? text.charCodeAt(p - 1) : 0
  if (openQ === 34 || openQ === 39 || openQ === 96) {
    let qCount = 1
    while (p - qCount > 0 && text.charCodeAt(p - qCount - 1) === openQ && qCount < 4) {
      qCount += 1
    }
    if (qCount !== 1 && !((openQ === 34 || openQ === 39) && qCount === 3)) return 0
    p -= qCount
    while (p > 0 && text.charCodeAt(p - 1) === 92) p -= 1
    if (openQ === 34 || openQ === 39) {
      if (p >= 2) {
        const raw1 = text.charCodeAt(p - 1)
        const raw2 = text.charCodeAt(p - 2)
        const c1 = raw1 | 32
        const c2 = raw2 | 32
        const twoCharPrefix =
          (raw2 === 36 && raw1 === 64) ||
          (raw2 === 64 && raw1 === 36) ||
          (c2 === 117 && raw1 === 56) ||
          (c2 === 114 && (c1 === 102 || c1 === 98 || c1 === 117)) ||
          (c1 === 114 && (c2 === 102 || c2 === 98 || c2 === 117))
        if (twoCharPrefix) {
          p -= 2
        } else if (raw1 === 36 || raw1 === 64 || raw1 === 76 || c1 === 102 || c1 === 114 || c1 === 98 || c1 === 117) {
          p -= 1
        }
      } else if (p >= 1) {
        const raw1 = text.charCodeAt(p - 1)
        const c1 = raw1 | 32
        if (raw1 === 36 || raw1 === 64 || raw1 === 76 || c1 === 102 || c1 === 114 || c1 === 98 || c1 === 117) {
          p -= 1
        }
      }
    }
    while (p > 0 && isHeaderWsAt(text, p - 1)) {
      if (isLineBreakCode(text.charCodeAt(p - 1))) sawNewline = true
      p -= 1
    }
  }
  if (sawNewline && p > 0) {
    let yp = p
    while (yp > 0 && text.charCodeAt(yp - 1) >= 48 && text.charCodeAt(yp - 1) <= 57) yp -= 1
    if (yp > 0 && (text.charCodeAt(yp - 1) === 43 || text.charCodeAt(yp - 1) === 45)) yp -= 1
    while (yp > 0 && text.charCodeAt(yp - 1) >= 48 && text.charCodeAt(yp - 1) <= 57) yp -= 1
    if (yp > 0 && (text.charCodeAt(yp - 1) === 124 || text.charCodeAt(yp - 1) === 62)) {
      yp -= 1
      while (yp > 0 && isSameLineWsAt(text, yp - 1)) yp -= 1
      if (yp > 0 && text.charCodeAt(yp - 1) === 58 && (yp < 2 || text.charCodeAt(yp - 2) !== 58)) {
        p = yp
      }
    }
  }
  if (p === 0) return 0
  const sep = text.charCodeAt(p - 1)
  if (sep === 58) {
    p -= 1
  } else if (sep === 61) {
    p -= 1
    if (p > 0 && text.charCodeAt(p - 1) === 58) {
      p -= 1
      if (p > 0 && text.charCodeAt(p - 1) === 58) p -= 1
    }
  } else if (sep === 62 && p >= 2 && text.charCodeAt(p - 2) === 61) {
    p -= 2
  } else if (sep === 44) {
    p -= 1
  } else {
    return 0
  }
  while (p > 0 && isSameLineWsAt(text, p - 1)) p -= 1
  const hasKeyQuote = p > 0 && (text.charCodeAt(p - 1) === 34 || text.charCodeAt(p - 1) === 39 || text.charCodeAt(p - 1) === 96)
  if (sep === 44 && !hasKeyQuote) return 0
  if (hasKeyQuote) {
    p -= 1
    while (p > 0 && text.charCodeAt(p - 1) === 92) p -= 1
  }
  if (p < 13) return 0
  const z = text.charCodeAt(p - 6)
  if (z !== 122 && z !== 90) return 0
  if (text.slice(p - 13, p).toLowerCase() !== 'authorization') return 0
  return sawNewline ? 2 : 1
}

type BearerKeyInfo = {
  keyCand: string
  isPlainKey: boolean
  isDoubleFlag: boolean
  sensitive: boolean
}

function classifyBearerKeyToken(token: string): BearerKeyInfo | null {
  const isPlainKey = token.length <= 100 && PLAIN_KEY_TOKEN_RE.test(token)
  const isDoubleFlag = !isPlainKey && token.length <= 102 && DOUBLE_DASH_LOWER_FLAG_RE.test(token)
  const isSensDoubleFlag =
    !isPlainKey &&
    !isDoubleFlag &&
    token.startsWith('--') &&
    token.length <= 102 &&
    PLAIN_KEY_TOKEN_RE.test(token.slice(2)) &&
    isSensitiveKey(token.slice(2))
  const isSensSingleFlag =
    !isPlainKey &&
    !isDoubleFlag &&
    !isSensDoubleFlag &&
    token.startsWith('-') &&
    !token.startsWith('--') &&
    token.length <= 101 &&
    PLAIN_KEY_TOKEN_RE.test(token.slice(1)) &&
    isSensitiveKey(token.slice(1))
  const keyCand = isPlainKey
    ? token
    : isDoubleFlag || isSensDoubleFlag
      ? token.slice(2)
      : isSensSingleFlag
        ? token.slice(1)
        : ''
  if (keyCand === '') return null
  return {
    keyCand,
    isPlainKey,
    isDoubleFlag: isDoubleFlag || isSensDoubleFlag,
    sensitive: isSensitiveKey(keyCand),
  }
}

type SameLineSep = {
  sepKind: ':' | '='
  rhsStart: number
  lineStop: number
}

function scanSameLineColonOrSpacedEq(text: string, from: number, lineStop: number): SameLineSep | null {
  let p = from
  while (p < lineStop && isSameLineWsAt(text, p)) p += 1
  if (p >= lineStop) return null
  const sepCode = text.charCodeAt(p)
  let sepKind: ':' | '='
  let rhsStart: number
  if (sepCode === 58) {
    const next = text.charCodeAt(p + 1)
    if (next === 61) {
      if (text.charCodeAt(p + 2) === 61) return null
      sepKind = '='
      rhsStart = p + 2
    } else if (next === 58) {
      if (text.charCodeAt(p + 2) !== 61 || text.charCodeAt(p + 3) === 61) return null
      sepKind = '='
      rhsStart = p + 3
    } else {
      sepKind = ':'
      rhsStart = p + 1
    }
  } else if (p > from && sepCode === 61) {
    if (text.charCodeAt(p + 1) === 61) return null
    sepKind = '='
    rhsStart = p + 1
  } else {
    return null
  }
  while (rhsStart < lineStop && isSameLineWsAt(text, rhsStart)) rhsStart += 1
  return { sepKind, rhsStart, lineStop }
}

function isCrossLineNonSensitiveConfigColonOrSpacedEq(
  keyCand: string,
  isPlainKey: boolean,
  isDoubleFlag: boolean,
  sepKind: ':' | '=',
  rhsHead: string,
  precededByAssign: boolean,
  validQuotedOrVarOrRedactedRhs: boolean,
): boolean {
  const isConfigKey =
    isDoubleFlag ||
    (precededByAssign &&
      (STRUCTURED_CONFIG_KEY_RE.test(keyCand) ||
        CAMEL_CASE_KEY_RE.test(keyCand) ||
        CAMEL_CASE_VERSION_KEY_RE.test(keyCand) ||
        UPPER_WORD_KEY_RE.test(keyCand) ||
        LOWER_WORD_KEY_RE.test(keyCand)))
  if (!isConfigKey) return false
  if (sepKind === ':') {
    if (!isPlainKey) return false
    if (rhsHead === '') return true
    if (validQuotedOrVarOrRedactedRhs) return true
    if (DIAGNOSTIC_PROSE_RE.test(rhsHead) || DIAGNOSTIC_SINGLE_WORD_RE.test(rhsHead)) return false
    if (
      !STRUCTURED_CONFIG_KEY_RE.test(keyCand) &&
      !(CAMEL_CASE_KEY_RE.test(keyCand) || CAMEL_CASE_VERSION_KEY_RE.test(keyCand)) &&
      /^[a-z]{3,}(?=$|[\s.,;:!?)\]}'"])/.test(rhsHead)
    ) {
      return false
    }
    return true
  }
  if (
    !precededByAssign &&
    (validQuotedOrVarOrRedactedRhs ||
      (rhsHead !== '' && (DIAGNOSTIC_PROSE_RE.test(rhsHead) || DIAGNOSTIC_SINGLE_WORD_RE.test(rhsHead))))
  ) {
    return false
  }
  return true
}

type QuoteContext = {
  activeQuote: string
  ansiSingle: boolean
  innerQuote: string
  activeEscapedQuote: string
  inComment: boolean
  commentQuote: string
  commentEscapedQuote: string
  lineActiveQuote: string
  lineAnsiSingle: boolean
  lineInnerQuote: string
  lineActiveEscapedQuote: string
  lineInComment: boolean
  lineCommentQuote: string
  lineCommentEscapedQuote: string
  parenDepth: number
  bracketDepth: number
  braceDepth: number
}

type QuoteScanCursor = {
  pos: number
  activeQuote: string
  ansiSingle: boolean
  activeEscapedQuote: string
  innerQuote: string
  innerAnsiSingle: boolean
  inComment: boolean
  commentLineBreak: number
  commentQuote: string
  commentAnsiSingle: boolean
  commentEscapedQuote: string
  parenDepth: number
  bracketDepth: number
  braceDepth: number
  cachedNl: number
  cachedCr: number
  cachedLs: number
  cachedPs: number
  crossedQuotedBreak: boolean
}

const toggleEscapedQuote = (cur: string, q: string): string => (cur === q ? '' : cur === '' ? q : cur)

const effectiveQuoteState = (qState: QuoteContext, hasNewline: boolean): QuoteContext => {
  if (!hasNewline) return qState
  if (
    !qState.inComment &&
    !qState.lineInComment &&
    qState.activeEscapedQuote === '' &&
    qState.lineActiveEscapedQuote === '' &&
    qState.parenDepth === 0 &&
    qState.bracketDepth === 0 &&
    qState.braceDepth === 0
  ) {
    return qState
  }
  return {
    ...(qState.inComment
      ? {
          activeQuote: '',
          ansiSingle: false,
          innerQuote: '',
          activeEscapedQuote: '',
          inComment: false,
          commentQuote: '',
          commentEscapedQuote: '',
        }
      : {
          activeQuote: qState.activeQuote,
          ansiSingle: qState.ansiSingle,
          innerQuote: qState.innerQuote,
          activeEscapedQuote: '',
          inComment: false,
          commentQuote: '',
          commentEscapedQuote: '',
        }),
    ...(qState.lineInComment
      ? {
          lineActiveQuote: '',
          lineAnsiSingle: false,
          lineInnerQuote: '',
          lineActiveEscapedQuote: '',
          lineInComment: false,
          lineCommentQuote: '',
          lineCommentEscapedQuote: '',
        }
      : {
          lineActiveQuote: qState.lineActiveQuote,
          lineAnsiSingle: qState.lineAnsiSingle,
          lineInnerQuote: qState.lineInnerQuote,
          lineActiveEscapedQuote: '',
          lineInComment: false,
          lineCommentQuote: '',
          lineCommentEscapedQuote: '',
        }),
    parenDepth: 0,
    bracketDepth: 0,
    braceDepth: 0,
  }
}

function redactBasicBearerAuth(text: string, stats: SanitizeStats): string {
  if (!/bearer|basic/i.test(text)) return text
  AUTH_SCHEME_RE.lastIndex = 0
  let out = ''
  let copied = 0
  let lastKeptMatchEnd = 0
  let prevKeptMatchEnd = 0
  const hasNl = text.includes('\n')
  const hasCr = text.includes('\r')
  const hasLs = text.includes('\u2028')
  const hasPs = text.includes('\u2029')
  const minPos = (a: number, b: number): number => (a === -1 ? b : b === -1 ? a : Math.min(a, b))
  const makeQuoteCursor = (): QuoteScanCursor => ({
    pos: 0,
    activeQuote: '',
    ansiSingle: false,
    activeEscapedQuote: '',
    innerQuote: '',
    innerAnsiSingle: false,
    inComment: false,
    commentLineBreak: -1,
    commentQuote: '',
    commentAnsiSingle: false,
    commentEscapedQuote: '',
    parenDepth: 0,
    bracketDepth: 0,
    braceDepth: 0,
    cachedNl: hasNl ? -2 : -1,
    cachedCr: hasCr ? -2 : -1,
    cachedLs: hasLs ? -2 : -1,
    cachedPs: hasPs ? -2 : -1,
    crossedQuotedBreak: false,
  })
  const copyCursorState = (src: QuoteScanCursor, dst: QuoteScanCursor): void => {
    dst.pos = src.pos
    dst.activeQuote = src.activeQuote
    dst.ansiSingle = src.ansiSingle
    dst.activeEscapedQuote = src.activeEscapedQuote
    dst.innerQuote = src.innerQuote
    dst.innerAnsiSingle = src.innerAnsiSingle
    dst.inComment = src.inComment
    dst.commentLineBreak = src.commentLineBreak
    dst.commentQuote = src.commentQuote
    dst.commentAnsiSingle = src.commentAnsiSingle
    dst.commentEscapedQuote = src.commentEscapedQuote
    dst.parenDepth = src.parenDepth
    dst.bracketDepth = src.bracketDepth
    dst.braceDepth = src.braceDepth
    dst.cachedNl = src.cachedNl
    dst.cachedCr = src.cachedCr
    dst.cachedLs = src.cachedLs
    dst.cachedPs = src.cachedPs
  }
  const cursorsSynced = (a: QuoteScanCursor, b: QuoteScanCursor): boolean =>
    a.pos === b.pos &&
    a.activeQuote === b.activeQuote &&
    a.ansiSingle === b.ansiSingle &&
    a.activeEscapedQuote === b.activeEscapedQuote &&
    a.innerQuote === b.innerQuote &&
    a.innerAnsiSingle === b.innerAnsiSingle &&
    a.inComment === b.inComment &&
    (!a.inComment ||
      (a.commentLineBreak === b.commentLineBreak &&
        a.commentQuote === b.commentQuote &&
        a.commentAnsiSingle === b.commentAnsiSingle &&
        a.commentEscapedQuote === b.commentEscapedQuote)) &&
    a.parenDepth === b.parenDepth &&
    a.bracketDepth === b.bracketDepth &&
    a.braceDepth === b.braceDepth
  // Strictly monotonic per-cursor helper (cur.pos never rewinds).
  const cursorNextLineBreakAfter = (cur: QuoteScanCursor, pos: number): number => {
    if (cur.cachedNl !== -1 && cur.cachedNl < pos) cur.cachedNl = text.indexOf('\n', pos)
    if (cur.cachedCr !== -1 && cur.cachedCr < pos) cur.cachedCr = text.indexOf('\r', pos)
    if (cur.cachedLs !== -1 && cur.cachedLs < pos) cur.cachedLs = text.indexOf('\u2028', pos)
    if (cur.cachedPs !== -1 && cur.cachedPs < pos) cur.cachedPs = text.indexOf('\u2029', pos)
    return minPos(minPos(cur.cachedNl, cur.cachedCr), minPos(cur.cachedLs, cur.cachedPs))
  }
  const lineTables = new Map<string, LineQuoteTable>()
  let cachedLineStart = -1
  let cachedLineQueryStop = -1
  let cachedLineNl = hasNl ? -2 : -1
  let cachedLineCr = hasCr ? -2 : -1
  let cachedLineLs = hasLs ? -2 : -1
  let cachedLinePs = hasPs ? -2 : -1
  let lastLineStopQuery = -1
  let cachedBoundsLineStop = -1
  let cachedBoundsLineStart = -1
  const lineStartFor = (pos: number): number => {
    let i = pos
    while (i > 0) {
      if (isLineBreakCode(text.charCodeAt(i - 1))) break
      i -= 1
    }
    return i
  }
  const lineStopFor = (pos: number): number => {
    if (cachedLineQueryStop !== -1 && pos >= cachedLineStart && pos <= cachedLineQueryStop) {
      return cachedLineQueryStop
    }
    if (pos < lastLineStopQuery) {
      cachedLineNl = hasNl ? -2 : -1
      cachedLineCr = hasCr ? -2 : -1
      cachedLineLs = hasLs ? -2 : -1
      cachedLinePs = hasPs ? -2 : -1
    }
    lastLineStopQuery = pos
    if (cachedLineNl !== -1 && cachedLineNl < pos) {
      cachedLineNl = text.indexOf('\n', pos)
      if (cachedLineNl === -1) cachedLineNl = text.length
    }
    if (cachedLineCr !== -1 && cachedLineCr < pos) {
      cachedLineCr = text.indexOf('\r', pos)
      if (cachedLineCr === -1) cachedLineCr = text.length
    }
    if (cachedLineLs !== -1 && cachedLineLs < pos) {
      cachedLineLs = text.indexOf('\u2028', pos)
      if (cachedLineLs === -1) cachedLineLs = text.length
    }
    if (cachedLinePs !== -1 && cachedLinePs < pos) {
      cachedLinePs = text.indexOf('\u2029', pos)
      if (cachedLinePs === -1) cachedLinePs = text.length
    }
    let lb = text.length
    if (cachedLineNl !== -1 && cachedLineNl < lb) lb = cachedLineNl
    if (cachedLineCr !== -1 && cachedLineCr < lb) lb = cachedLineCr
    if (cachedLineLs !== -1 && cachedLineLs < lb) lb = cachedLineLs
    if (cachedLinePs !== -1 && cachedLinePs < lb) lb = cachedLinePs
    cachedLineStart = pos
    cachedLineQueryStop = lb
    return lb
  }
  const opensUnescapedFast = (quoteIdx: number, q: string): boolean => {
    const afterStart = quoteIdx + 1
    if (q === '`' && text[afterStart] === '`') return false
    const lineStop = lineStopFor(afterStart)
    if (lineStop - quoteIdx <= 256) return opensQuotedAssignmentValue(text, quoteIdx, q, lineStop)
    const cacheKey = `u:${q}`
    let tbl = lineTables.get(cacheKey)
    if (tbl === undefined || tbl.lineStop !== lineStop) {
      if (cachedBoundsLineStop !== lineStop) {
        cachedBoundsLineStop = lineStop
        cachedBoundsLineStart = lineStartFor(quoteIdx)
      }
      tbl = buildUnescapedLineTable(text, cachedBoundsLineStart, lineStop, q)
      lineTables.set(cacheKey, tbl)
    }
    const rem = afterStart >= lineStop ? 0 : (tbl.s1[afterStart - tbl.lineStart] as number)
    return evalQuoteRemainder(text, afterStart, lineStop, q, false, rem)
  }
  const opensEscapedFast = (quoteIdx: number, q: string): boolean => {
    const afterStart = quoteIdx + 2
    const lineStop = lineStopFor(afterStart)
    if (lineStop - quoteIdx <= 256) return opensEscapedQuotedAssignmentValue(text, quoteIdx, q, lineStop)
    const cacheKey = `e:${q}`
    let tbl = lineTables.get(cacheKey)
    if (tbl === undefined || tbl.lineStop !== lineStop) {
      if (cachedBoundsLineStop !== lineStop) {
        cachedBoundsLineStop = lineStop
        cachedBoundsLineStart = lineStartFor(quoteIdx)
      }
      tbl = buildEscapedLineTable(text, cachedBoundsLineStart, lineStop, q)
      lineTables.set(cacheKey, tbl)
    }
    const rem = afterStart >= lineStop ? 0 : (tbl.s1[afterStart - tbl.lineStart] as number)
    return evalQuoteRemainder(text, afterStart, lineStop, q, true, rem)
  }
  const resetCursorLineState = (cur: QuoteScanCursor, resetQuotes: boolean): void => {
    if (resetQuotes) {
      cur.activeQuote = ''
      cur.ansiSingle = false
      cur.innerQuote = ''
      cur.innerAnsiSingle = false
    }
    cur.activeEscapedQuote = ''
    cur.parenDepth = 0
    cur.bracketDepth = 0
    cur.braceDepth = 0
  }
  const advanceQuoteCursor = (cur: QuoteScanCursor, target: number, resetQuotesOnLineBreak: boolean): void => {
    while (cur.pos < target) {
      const ch = text[cur.pos] as string
      if (cur.inComment) {
        if (cur.commentLineBreak !== -1 && cur.commentLineBreak < target) {
          cur.pos =
            text[cur.commentLineBreak] === '\r' && text[cur.commentLineBreak + 1] === '\n'
              ? cur.commentLineBreak + 2
              : cur.commentLineBreak + 1
          cur.inComment = false
          cur.commentLineBreak = -1
          cur.commentQuote = ''
          cur.commentAnsiSingle = false
          cur.commentEscapedQuote = ''
          resetCursorLineState(cur, resetQuotesOnLineBreak)
          continue
        }
        const next = text[cur.pos + 1]
        if (cur.commentQuote !== '') {
          if ((cur.commentQuote !== "'" || cur.commentAnsiSingle) && ch === '\\') {
            if (next === '"' || next === "'") {
              cur.commentEscapedQuote = toggleEscapedQuote(cur.commentEscapedQuote, next)
              cur.pos += 2
              continue
            }
            if (next === '\\' || next === cur.commentQuote || next === '`' || next === '$') {
              cur.pos += 2
              continue
            }
          } else if (cur.commentQuote === "'" && !cur.commentAnsiSingle && ch === '\\' && next === '"') {
            cur.commentEscapedQuote = toggleEscapedQuote(cur.commentEscapedQuote, '"')
            cur.pos += 2
            continue
          }
          if (ch === cur.commentQuote) {
            cur.commentQuote = ''
            cur.commentAnsiSingle = false
            cur.commentEscapedQuote = ''
          }
          cur.pos += 1
          continue
        }
        if (ch === '\\') {
          if (next === '"' || next === "'") {
            cur.commentEscapedQuote = toggleEscapedQuote(cur.commentEscapedQuote, next)
            cur.pos += 2
            continue
          }
          if (cur.pos + 1 < target && (next === '\\' || next === '`' || next === '$')) {
            cur.pos += 2
            continue
          }
          cur.pos += 1
          continue
        }
        if (ch === '$' && next === "'") {
          cur.commentQuote = "'"
          cur.commentAnsiSingle = true
          cur.commentEscapedQuote = ''
          cur.pos += 2
          continue
        }
        if (ch === '"') {
          cur.commentQuote = '"'
          cur.commentEscapedQuote = ''
          cur.pos += 1
          continue
        }
        if (ch === "'") {
          if (!isNonQuoteSingleQuote(text, cur.pos)) {
            cur.commentQuote = "'"
            cur.commentEscapedQuote = ''
          }
          cur.pos += 1
          continue
        }
        if (ch === '`') {
          let runLen = 1
          while (cur.pos + runLen < target && text[cur.pos + runLen] === '`') runLen += 1
          if (runLen === 1) {
            cur.commentQuote = '`'
            cur.commentEscapedQuote = ''
          }
          cur.pos += runLen
          continue
        }
        cur.pos += 1
        continue
      }
      if (cur.activeQuote !== '') {
        if (isLineBreakCode(text.charCodeAt(cur.pos))) {
          if (!resetQuotesOnLineBreak) cur.crossedQuotedBreak = true
          resetCursorLineState(cur, resetQuotesOnLineBreak)
          cur.pos += 1
          continue
        }
        const next = text[cur.pos + 1]
        if (cur.innerQuote !== '') {
          if ((cur.innerQuote !== "'" || cur.innerAnsiSingle) && ch === '\\') {
            if ((next === '"' || next === "'") && !(cur.activeQuote === "'" && !cur.ansiSingle && next === "'")) {
              cur.activeEscapedQuote = toggleEscapedQuote(cur.activeEscapedQuote, next)
              cur.pos += 2
              continue
            }
            if ((next === '\\' || next === cur.innerQuote || next === '`' || next === '$') && !(cur.activeQuote === "'" && !cur.ansiSingle && next === "'")) {
              cur.pos += 2
              continue
            }
          } else if (cur.innerQuote === "'" && !cur.innerAnsiSingle && ch === '\\' && next === '"') {
            cur.activeEscapedQuote = toggleEscapedQuote(cur.activeEscapedQuote, '"')
            cur.pos += 2
            continue
          }
          if (ch === cur.activeQuote) {
            cur.activeQuote = ''
            cur.ansiSingle = false
            cur.activeEscapedQuote = ''
            cur.innerQuote = ''
            cur.innerAnsiSingle = false
            cur.pos += 1
            continue
          }
          if (ch === cur.innerQuote) {
            cur.innerQuote = ''
            cur.innerAnsiSingle = false
            cur.activeEscapedQuote = ''
            cur.pos += 1
            continue
          }
          cur.pos += 1
          continue
        }
        if ((cur.activeQuote !== "'" || cur.ansiSingle) && ch === '\\') {
          if (next === '"' || next === "'") {
            cur.activeEscapedQuote = toggleEscapedQuote(cur.activeEscapedQuote, next)
            cur.pos += 2
            continue
          }
          if (next === '\\' || next === cur.activeQuote || next === '`' || next === '$') {
            cur.pos += 2
            continue
          }
        } else if (cur.activeQuote === "'" && !cur.ansiSingle && ch === '\\' && next === '"') {
          cur.activeEscapedQuote = toggleEscapedQuote(cur.activeEscapedQuote, '"')
          cur.pos += 2
          continue
        }
        if (ch === cur.activeQuote) {
          cur.activeQuote = ''
          cur.ansiSingle = false
          cur.activeEscapedQuote = ''
          cur.innerQuote = ''
          cur.innerAnsiSingle = false
          cur.pos += 1
          continue
        }
        if (cur.activeEscapedQuote === '') {
          if (ch === '$' && next === "'" && cur.activeQuote !== "'") {
            cur.innerQuote = "'"
            cur.innerAnsiSingle = true
            cur.pos += 2
            continue
          }
          if (ch === '"' && cur.activeQuote !== '"') {
            cur.innerQuote = '"'
            cur.pos += 1
            continue
          }
          if (ch === "'" && cur.activeQuote !== "'") {
            if (!isNonQuoteSingleQuote(text, cur.pos)) {
              cur.innerQuote = "'"
            }
            cur.pos += 1
            continue
          }
          if (ch === '`' && cur.activeQuote !== '`') {
            let runLen = 1
            while (cur.pos + runLen < target && text[cur.pos + runLen] === '`') runLen += 1
            if (runLen === 1) {
              cur.innerQuote = '`'
            }
            cur.pos += runLen
            continue
          }
        }
        cur.pos += 1
        continue
      }
      if (isLineBreakCode(text.charCodeAt(cur.pos))) {
        resetCursorLineState(cur, resetQuotesOnLineBreak)
        cur.pos += 1
        continue
      }
      if (isHashCommentStart(text, cur.pos)) {
        const lb = cursorNextLineBreakAfter(cur, cur.pos + 1)
        if (lb !== -1 && lb < target) {
          cur.pos = text[lb] === '\r' && text[lb + 1] === '\n' ? lb + 2 : lb + 1
          resetCursorLineState(cur, resetQuotesOnLineBreak)
          continue
        }
        cur.inComment = true
        cur.commentLineBreak = lb
        cur.commentQuote = ''
        cur.commentAnsiSingle = false
        cur.commentEscapedQuote = ''
        cur.pos += 1
        continue
      }
      if (isSlashCommentStart(text, cur.pos)) {
        const lb = cursorNextLineBreakAfter(cur, cur.pos + 2)
        if (lb !== -1 && lb < target) {
          cur.pos = text[lb] === '\r' && text[lb + 1] === '\n' ? lb + 2 : lb + 1
          resetCursorLineState(cur, resetQuotesOnLineBreak)
          continue
        }
        cur.inComment = true
        cur.commentLineBreak = lb
        cur.commentQuote = ''
        cur.commentAnsiSingle = false
        cur.commentEscapedQuote = ''
        cur.pos += 2
        continue
      }
      if (ch === '\\') {
        const next = text[cur.pos + 1]
        if (cur.pos + 1 < target && (next === '"' || next === "'")) {
          cur.activeEscapedQuote = toggleEscapedQuote(cur.activeEscapedQuote, next)
          cur.pos += 2
          continue
        }
        if (
          cur.pos + 1 < target &&
          (next === '\\' ||
            next === '`' ||
            next === '$' ||
            next === '#' ||
            next === '/' ||
            next === '(' ||
            next === ')' ||
            next === '[' ||
            next === ']' ||
            next === '{' ||
            next === '}')
        ) {
          cur.pos += 2
          continue
        }
        cur.pos += 1
        continue
      }
      if (ch === '$' && text[cur.pos + 1] === "'") {
        cur.activeQuote = "'"
        cur.ansiSingle = true
        cur.activeEscapedQuote = ''
        cur.innerQuote = ''
        cur.innerAnsiSingle = false
        cur.pos += 2
        continue
      }
      if (ch === '"') {
        cur.activeQuote = '"'
        cur.activeEscapedQuote = ''
        cur.innerQuote = ''
        cur.innerAnsiSingle = false
        cur.pos += 1
        continue
      }
      if (ch === "'") {
        if (!isNonQuoteSingleQuote(text, cur.pos)) {
          cur.activeQuote = "'"
          cur.activeEscapedQuote = ''
          cur.innerQuote = ''
          cur.innerAnsiSingle = false
        }
        cur.pos += 1
        continue
      }
      if (ch === '`') {
        let runLen = 1
        while (cur.pos + runLen < target && text[cur.pos + runLen] === '`') runLen += 1
        if (runLen === 1) {
          cur.activeQuote = '`'
          cur.activeEscapedQuote = ''
          cur.innerQuote = ''
          cur.innerAnsiSingle = false
        }
        cur.pos += runLen
        continue
      }
      if (ch === '(') cur.parenDepth += 1
      else if (ch === ')' && cur.parenDepth > 0) cur.parenDepth -= 1
      else if (ch === '[') cur.bracketDepth += 1
      else if (ch === ']' && cur.bracketDepth > 0) cur.bracketDepth -= 1
      else if (ch === '{') cur.braceDepth += 1
      else if (ch === '}' && cur.braceDepth > 0) cur.braceDepth -= 1
      cur.pos += 1
    }
  }
  const crossCursor = makeQuoteCursor()
  const lineCursor = makeQuoteCursor()
  let cursorsAreSynced = true
  const baseQuoteCtx: QuoteContext = {
    activeQuote: '',
    ansiSingle: false,
    innerQuote: '',
    activeEscapedQuote: '',
    inComment: false,
    commentQuote: '',
    commentEscapedQuote: '',
    lineActiveQuote: '',
    lineAnsiSingle: false,
    lineInnerQuote: '',
    lineActiveEscapedQuote: '',
    lineInComment: false,
    lineCommentQuote: '',
    lineCommentEscapedQuote: '',
    parenDepth: 0,
    bracketDepth: 0,
    braceDepth: 0,
  }
  const quoteAt = (target: number): QuoteContext => {
    if (cursorsAreSynced) {
      copyCursorState(crossCursor, lineCursor)
      crossCursor.crossedQuotedBreak = false
      advanceQuoteCursor(crossCursor, target, false)
      if (crossCursor.crossedQuotedBreak) {
        advanceQuoteCursor(lineCursor, target, true)
        cursorsAreSynced = cursorsSynced(crossCursor, lineCursor)
      }
    } else {
      advanceQuoteCursor(crossCursor, target, false)
      advanceQuoteCursor(lineCursor, target, true)
      if (cursorsSynced(crossCursor, lineCursor)) cursorsAreSynced = true
    }
    const effectiveLine = cursorsAreSynced ? crossCursor : lineCursor
    baseQuoteCtx.activeQuote = crossCursor.activeQuote
    baseQuoteCtx.ansiSingle = crossCursor.ansiSingle
    baseQuoteCtx.innerQuote = crossCursor.innerQuote
    baseQuoteCtx.activeEscapedQuote = crossCursor.activeEscapedQuote
    baseQuoteCtx.inComment = crossCursor.inComment
    baseQuoteCtx.commentQuote = crossCursor.commentQuote
    baseQuoteCtx.commentEscapedQuote = crossCursor.commentEscapedQuote
    baseQuoteCtx.lineActiveQuote = effectiveLine.activeQuote
    baseQuoteCtx.lineAnsiSingle = effectiveLine.ansiSingle
    baseQuoteCtx.lineInnerQuote = effectiveLine.innerQuote
    baseQuoteCtx.lineActiveEscapedQuote = effectiveLine.activeEscapedQuote
    baseQuoteCtx.lineInComment = effectiveLine.inComment
    baseQuoteCtx.lineCommentQuote = effectiveLine.commentQuote
    baseQuoteCtx.lineCommentEscapedQuote = effectiveLine.commentEscapedQuote
    baseQuoteCtx.parenDepth = effectiveLine.parenDepth
    baseQuoteCtx.bracketDepth = effectiveLine.bracketDepth
    baseQuoteCtx.braceDepth = effectiveLine.braceDepth
    return baseQuoteCtx
  }
  const isOpeningUnescapedQuote = (
    quoteIdx: number,
    q: string,
    qState: QuoteContext,
    token: string,
    allowPaddedLowerQuoteRhs: boolean,
  ): boolean => {
    const outerQuote = qState.inComment ? '' : qState.activeQuote
    if (outerQuote === q) return false
    const nextAfter = text[quoteIdx + 1]
    if (isBase64PaddedToken(token) && !allowPaddedLowerQuoteRhs && isDelimOrSpaceAfterQuote(nextAfter)) return false
    const openSameOrInner = qState.inComment ? qState.commentQuote : qState.innerQuote
    if (openSameOrInner === q && isDelimOrSpaceAfterQuote(nextAfter)) return false
    return opensUnescapedFast(quoteIdx, q)
  }
  const isOpeningEscapedQuote = (
    quoteIdx: number,
    q: string,
    qState: QuoteContext,
    token: string,
    allowPaddedLowerQuoteRhs: boolean,
  ): boolean => {
    const nextAfter = text[quoteIdx + 2]
    if (isBase64PaddedToken(token) && !allowPaddedLowerQuoteRhs && isDelimOrSpaceAfterQuote(nextAfter)) return false
    const openEsc = qState.inComment ? qState.commentEscapedQuote : qState.activeEscapedQuote
    if (openEsc === q) {
      const outer = qState.inComment ? '' : qState.activeQuote
      if (isDelimOrSpaceAfterQuote(nextAfter) || (outer === '' && nextAfter === '\\')) return false
    }
    return opensEscapedFast(quoteIdx, q)
  }
  const isValidVarRhsAt = (
    rhsIdx: number,
    rhsText: string,
    qState: QuoteContext,
    coreToken: string,
    allowPaddedLowerQuoteRhs: boolean,
  ): boolean => {
    if (rhsText.length < 2) return false
    const c1 = rhsText[1] as string
    if (c1 === '"' || c1 === "'") {
      return (
        !qState.inComment &&
        qState.activeQuote === '' &&
        isOpeningUnescapedQuote(rhsIdx + 1, c1, qState, coreToken, allowPaddedLowerQuoteRhs)
      )
    }
    return ASSIGN_RHS_VAR_RE.test(rhsText)
  }
  const hasAssignRhsAt = (
    rhsIdx: number,
    rhsText: string,
    qState: QuoteContext,
    coreToken: string,
    allowPaddedLowerQuoteRhs: boolean,
    allowPunctRhs: boolean,
  ): boolean => {
    if (rhsText === '') return false
    const c0 = rhsText.charCodeAt(0)
    if (c0 === 36) return isValidVarRhsAt(rhsIdx, rhsText, qState, coreToken, allowPaddedLowerQuoteRhs)
    if (c0 === 91) return ASSIGN_RHS_REDACTED_RE.test(rhsText) || (allowPunctRhs && ASSIGN_RHS_PUNCT_RE.test(rhsText))
    if (c0 === 34 || c0 === 39 || c0 === 96) {
      return isOpeningUnescapedQuote(rhsIdx, rhsText[0] as string, qState, coreToken, allowPaddedLowerQuoteRhs)
    }
    if (c0 === 92) {
      const c1 = rhsText.charCodeAt(1)
      if (c1 === 34 || c1 === 39) return isOpeningEscapedQuote(rhsIdx, rhsText[1] as string, qState, coreToken, allowPaddedLowerQuoteRhs)
    }
    return allowPunctRhs && ASSIGN_RHS_PUNCT_RE.test(rhsText)
  }
  const assignPrefixContext = (matchIndex: number): { precededByEq: boolean; precededByColon: boolean } => {
    const keptFloor = lastKeptMatchEnd <= matchIndex ? lastKeptMatchEnd : prevKeptMatchEnd
    const floor = Math.max(copied, keptFloor)
    let p = matchIndex
    while (p > floor && isSameLineWsAt(text, p - 1)) p -= 1
    if (p <= floor) return { precededByEq: false, precededByColon: false }
    const sep = text.charCodeAt(p - 1)
    if (sep === 61) {
      return {
        precededByEq: p - 1 === 0 || !/[=!<>]/.test(text[p - 2] as string),
        precededByColon: false,
      }
    }
    if (sep === 58) {
      return {
        precededByEq: false,
        precededByColon: p - 1 === 0 || text.charCodeAt(p - 2) !== 58,
      }
    }
    return { precededByEq: false, precededByColon: false }
  }
  const computeDualStop = (
    rhsStart: number,
    lineStop: number,
    tokQState: QuoteContext,
    requireDelimAfterQuote = false,
  ): number => {
    const outerQ = tokQState.lineInComment ? tokQState.lineCommentQuote : tokQState.lineActiveQuote
    const innerQ = tokQState.lineInComment ? '' : tokQState.lineInnerQuote
    const escQ = tokQState.lineInComment ? tokQState.lineCommentEscapedQuote : tokQState.lineActiveEscapedQuote
    const checkDelims =
      !tokQState.lineInComment &&
      tokQState.lineActiveQuote === '' &&
      (tokQState.parenDepth > 0 || tokQState.bracketDepth > 0 || tokQState.braceDepth > 0)
    if (outerQ === '' && innerQ === '' && escQ === '' && !checkDelims) return lineStop
    let localParen = 0
    let localBracket = 0
    let localBrace = 0
    let rhsQuote = ''
    let i = rhsStart
    while (i < lineStop) {
      const ch = text[i] as string
      if (escQ !== '' && ch === '\\' && i + 1 < lineStop && text[i + 1] === escQ) {
        return i
      }
      if (ch === '\\') {
        if (outerQ === "'" && !tokQState.lineAnsiSingle) {
          if (innerQ === '"' && i + 1 < lineStop && (text[i + 1] === '"' || text[i + 1] === '\\')) {
            i += 2
            continue
          }
        } else if (rhsQuote !== "'" && i + 1 < lineStop) {
          i += 2
          continue
        }
      }
      if (outerQ !== '' && ch === outerQ && (!requireDelimAfterQuote || isDelimOrSpaceAfterQuote(text[i + 1]))) {
        return i
      }
      if (innerQ !== '' && ch === innerQ && (!requireDelimAfterQuote || isDelimOrSpaceAfterQuote(text[i + 1]))) {
        return i
      }
      if (checkDelims) {
        if (rhsQuote !== '') {
          if (ch === rhsQuote) rhsQuote = ''
        } else if (ch === '"' || (ch === "'" && !isNonQuoteSingleQuote(text, i))) {
          rhsQuote = ch
        } else if (ch === '(') {
          localParen += 1
        } else if (ch === ')') {
          if (localParen > 0) localParen -= 1
          else if (tokQState.parenDepth > 0) return i
        } else if (ch === '[') {
          localBracket += 1
        } else if (ch === ']') {
          if (localBracket > 0) localBracket -= 1
          else if (tokQState.bracketDepth > 0) return i
        } else if (ch === '{') {
          localBrace += 1
        } else if (ch === '}') {
          if (localBrace > 0) localBrace -= 1
          else if (tokQState.braceDepth > 0) return i
        }
      }
      i += 1
    }
    return lineStop
  }
  let curMatchIndex = 0
  let curScheme = ''
  let curMatchEnd = 0
  let curEffectiveMatchEnd = 0
  let curHasNewline = false
  let cachedRawTokQState: QuoteContext | undefined
  let cachedTokQState: QuoteContext | undefined
  let cachedPrefix: { precededByEq: boolean; precededByColon: boolean } | undefined
  const rewind = (): void => {
    AUTH_SCHEME_RE.lastIndex = Math.max(curMatchIndex + curScheme.length, curMatchEnd - 6)
  }
  const keepAndRewind = (): void => {
    if (curEffectiveMatchEnd > lastKeptMatchEnd) {
      prevKeptMatchEnd = lastKeptMatchEnd
      lastKeptMatchEnd = curEffectiveMatchEnd
    }
    rewind()
  }
  const getRawTokQState = (): QuoteContext => {
    if (cachedRawTokQState === undefined) cachedRawTokQState = quoteAt(curMatchIndex)
    return cachedRawTokQState
  }
  const getTokQState = (): QuoteContext => {
    if (cachedTokQState === undefined) cachedTokQState = effectiveQuoteState(getRawTokQState(), curHasNewline)
    return cachedTokQState
  }
  const getPrefix = (): { precededByEq: boolean; precededByColon: boolean } => {
    if (cachedPrefix === undefined) cachedPrefix = assignPrefixContext(curMatchIndex)
    return cachedPrefix
  }
  const emitSpanRedaction = (replacementTail: string, stopIdx: number): void => {
    out += text.slice(copied, curMatchIndex) + curScheme + ' ' + MARKER + replacementTail
    stats.redactions += 1
    copied = stopIdx
    AUTH_SCHEME_RE.lastIndex = stopIdx
  }
  const trySpacedSensitiveEqRhs = (
    hasCtx: boolean,
    coreToken: string,
    eqIdx: number,
    isLowerWordKey: boolean,
    endsWithDoubleEq: boolean,
    tokQState: QuoteContext,
  ): boolean => {
    const eqLineStop = lineStopFor(curEffectiveMatchEnd)
    let spEnd = curEffectiveMatchEnd
    while (spEnd < eqLineStop && isSameLineWsAt(text, spEnd)) spEnd += 1
    if (spEnd <= curEffectiveMatchEnd || spEnd >= eqLineStop) return false
    const boundedStop = Math.min(eqLineStop, spEnd + 64)
    const dualHeadStop = computeDualStop(spEnd, boundedStop, tokQState, curHasNewline)
    if (spEnd >= dualHeadStop) return false
    const spaceBeforeQuoteRhs =
      !isBase64PaddedToken(coreToken) &&
      (() => {
        const ch = text[spEnd]
        if (ch === '"' || ch === "'" || ch === '`') {
          return isOpeningUnescapedQuote(spEnd, ch, tokQState, coreToken, isLowerWordKey && !endsWithDoubleEq)
        }
        if (ch === '\\' && (text[spEnd + 1] === '"' || text[spEnd + 1] === "'")) {
          return isOpeningEscapedQuote(spEnd, text[spEnd + 1] as string, tokQState, coreToken, isLowerWordKey && !endsWithDoubleEq)
        }
        if (ch === '$') {
          return isValidVarRhsAt(
            spEnd,
            text.slice(spEnd, dualHeadStop),
            tokQState,
            coreToken,
            isLowerWordKey && !endsWithDoubleEq,
          )
        }
        return false
      })()
    const spaceBeforeValueRhs =
      !endsWithDoubleEq &&
      !isBase64PaddedToken(coreToken) &&
      /^(?!(?:#(?:$|[\s])|[;)|&"'`$\\\]},]))(?!(?:bearer|basic)(?:[\s]|$))\S/i.test(
        text.slice(spEnd, Math.min(dualHeadStop, spEnd + 8)),
      )
    if (!spaceBeforeQuoteRhs && !spaceBeforeValueRhs) return false
    if (hasCtx) {
      keepAndRewind()
      return true
    }
    const dualStop =
      dualHeadStop < boundedStop ? dualHeadStop : computeDualStop(spEnd, eqLineStop, tokQState, curHasNewline)
    emitSpanRedaction(coreToken.slice(eqIdx) + text.slice(curEffectiveMatchEnd, spEnd) + MARKER, dualStop)
    return true
  }
  for (let m = AUTH_SCHEME_RE.exec(text); m !== null; m = AUTH_SCHEME_RE.exec(text)) {
    const scheme = m[1] as string
    const space = m[2] as string
    const token = m[3] as string
    const matchEnd = m.index + m[0].length
    const headerKind = explicitAuthHeaderKind(text, m.index)
    const inHeader = headerKind !== 0
    const hasNewline =
      space.length === 1
        ? isLineBreakCode(space.charCodeAt(0))
        : space.includes('\n') ||
          space.includes('\r') ||
          (hasLs && space.includes('\u2028')) ||
          (hasPs && space.includes('\u2029'))
    const after = text.slice(matchEnd, matchEnd + 64)
    const isBasic = scheme.length === 5 && (scheme === 'Basic' || scheme === 'basic' || scheme.toLowerCase() === 'basic')
    const basicB64 = isBasic && isBase64Credential(token)
    const eqIdx = token.indexOf('=')
    let coreToken = token
    let trailingDots = ''
    if (eqIdx > 0 && token.charCodeAt(token.length - 1) === 46) {
      const eqCount = token[eqIdx + 1] === '=' ? 2 : 1
      if (eqIdx + eqCount < token.length) {
        let allDots = true
        for (let d = eqIdx + eqCount; d < token.length; d += 1) {
          if (token.charCodeAt(d) !== 46) {
            allDots = false
            break
          }
        }
        if (allDots) {
          coreToken = token.slice(0, eqIdx + eqCount)
          trailingDots = token.slice(eqIdx + eqCount)
        }
      }
    }
    const effectiveAfter =
      trailingDots === ''
        ? after
        : trailingDots.length >= 64
          ? trailingDots.slice(0, 64)
          : trailingDots + after.slice(0, 64 - trailingDots.length)
    const effectiveMatchEnd = trailingDots === '' ? matchEnd : m.index + scheme.length + space.length + coreToken.length
    curMatchIndex = m.index
    curScheme = scheme
    curMatchEnd = matchEnd
    curEffectiveMatchEnd = effectiveMatchEnd
    curHasNewline = hasNewline
    cachedRawTokQState = undefined
    cachedTokQState = undefined
    cachedPrefix = undefined
    let forceHeaderTokenRedact = false
    if (eqIdx === -1 && !basicB64) {
      const keyInfo = classifyBearerKeyToken(token)
      if (keyInfo !== null && (keyInfo.sensitive || (hasNewline && !isBasic && !inHeader && coreToken.length >= 12))) {
        const ac0 = after.charCodeAt(0)
        if (ac0 === 58 || ac0 === 61 || isSameLineWsAt(text, matchEnd)) {
          const sep = scanSameLineColonOrSpacedEq(text, matchEnd, lineStopFor(matchEnd))
          if (sep !== null) {
            if (keyInfo.sensitive) {
              const tokQState = getTokQState()
              const boundedStop = Math.min(sep.lineStop, sep.rhsStart + 64)
              const dualHeadStop = computeDualStop(sep.rhsStart, boundedStop, tokQState, hasNewline)
              if (sep.rhsStart >= dualHeadStop) {
                if (!inHeader && hasNewline) {
                  const { precededByEq } = getPrefix()
                  if (precededByEq || tokQState.inComment) {
                    keepAndRewind()
                    continue
                  }
                }
                if (inHeader) forceHeaderTokenRedact = true
              } else {
                if (!inHeader) {
                  const { precededByEq, precededByColon } = getPrefix()
                  const hasAssignOrCommentCtx = precededByEq || tokQState.inComment || (!hasNewline && precededByColon)
                  if (hasAssignOrCommentCtx) {
                    const rhsHead = text.slice(sep.rhsStart, dualHeadStop)
                    if (hasAssignRhsAt(sep.rhsStart, rhsHead, tokQState, coreToken, true, false)) {
                      keepAndRewind()
                      continue
                    }
                  }
                }
                // Ambiguous or header-scoped sensitive key-shaped token with non-empty RHS:
                // fail closed on both sides up to enclosing quote/delimiter or EOL.
                const dualStop =
                  dualHeadStop < boundedStop
                    ? dualHeadStop
                    : computeDualStop(sep.rhsStart, sep.lineStop, tokQState, hasNewline)
                emitSpanRedaction(text.slice(matchEnd, sep.rhsStart) + MARKER, dualStop)
                continue
              }
            } else {
              const tokQState = getTokQState()
              const { precededByEq } = getPrefix()
              const rhsHead = text.slice(sep.rhsStart, Math.min(sep.lineStop, sep.rhsStart + 256))
              const validQuotedVarOrRedacted = hasAssignRhsAt(
                sep.rhsStart,
                rhsHead.slice(0, 64),
                tokQState,
                coreToken,
                true,
                false,
              )
              if (
                isCrossLineNonSensitiveConfigColonOrSpacedEq(
                  keyInfo.keyCand,
                  keyInfo.isPlainKey,
                  keyInfo.isDoubleFlag,
                  sep.sepKind,
                  rhsHead,
                  precededByEq || tokQState.inComment,
                  validQuotedVarOrRedacted,
                )
              ) {
                keepAndRewind()
                continue
              }
            }
          }
        }
      }
    }
    let isPaddedBasicCandidate = false
    if (!basicB64 && eqIdx > 0) {
      const rawKeyPart = coreToken.slice(0, eqIdx)
      const keyWithinCap = rawKeyPart.length <= 100
      const isPlainAssignKey = /^[A-Za-z_][A-Za-z0-9_.-]*$/.test(rawKeyPart)
      const isDashPrefixed = !isPlainAssignKey && rawKeyPart.charCodeAt(0) === 45
      const flagStripped = isDashPrefixed ? (rawKeyPart.charCodeAt(1) === 45 ? rawKeyPart.slice(2) : rawKeyPart.slice(1)) : ''
      const isDoubleFlag = isDashPrefixed && rawKeyPart.charCodeAt(1) === 45
      const isConfigFlagShape =
        isDashPrefixed &&
        keyWithinCap &&
        (isDoubleFlag ? DOUBLE_DASH_LOWER_FLAG_RE.test(rawKeyPart) : SINGLE_DASH_SNAKE_OR_LOWER_FLAG_RE.test(rawKeyPart))
      const isFlagAssignKey =
        isDashPrefixed &&
        flagStripped !== '' &&
        /^[A-Za-z_][A-Za-z0-9_.-]*$/.test(flagStripped) &&
        (isConfigFlagShape || (flagStripped.length <= 100 && isSensitiveKey(flagStripped)))
      const isAssignKey = isPlainAssignKey || isFlagAssignKey
      const assignKey = isFlagAssignKey ? flagStripped : rawKeyPart
      const isLowerWordKey = isPlainAssignKey && keyWithinCap && LOWER_WORD_KEY_RE.test(rawKeyPart)
      const isEmptyAssignIdentifier =
        isAssignKey &&
        keyWithinCap &&
        !isLowerWordKey &&
        ((isFlagAssignKey && isDoubleFlag) ||
          ALL_CAPS_SNAKE_KEY_RE.test(assignKey) ||
          LOWER_SNAKE_KEY_RE.test(assignKey) ||
          CAMEL_CASE_KEY_RE.test(assignKey))
      const isRecognizedConfigKey =
        isEmptyAssignIdentifier ||
        isConfigFlagShape ||
        (keyWithinCap &&
          (CAMEL_CASE_VERSION_KEY_RE.test(rawKeyPart) ||
            UPPER_WORD_KEY_RE.test(rawKeyPart) ||
            isLowerWordKey ||
            LOWER_DOTTED_KEY_RE.test(rawKeyPart) ||
            LOWER_KEBAB_KEY_RE.test(rawKeyPart)))
      const endsWithEq = isAssignKey && eqIdx === coreToken.length - 1
      const endsWithDoubleEq = isAssignKey && eqIdx === coreToken.length - 2 && coreToken[eqIdx + 1] === '='
      const isEqPaddedEnd = endsWithEq || endsWithDoubleEq
      isPaddedBasicCandidate = !inHeader && coreToken.length >= 12

      if (inHeader) {
        if (isAssignKey && assignKey.length <= 100 && isSensitiveKey(assignKey)) {
          const tokQState = getTokQState()
          const eqLineStop = lineStopFor(effectiveMatchEnd)
          if (!isEqPaddedEnd) {
            const rhsAt = m.index + scheme.length + space.length + eqIdx + 1
            const dualStop = computeDualStop(rhsAt, eqLineStop, tokQState, hasNewline)
            emitSpanRedaction('', dualStop)
            continue
          }
          let spEnd = effectiveMatchEnd
          while (spEnd < eqLineStop && isSameLineWsAt(text, spEnd)) spEnd += 1
          const immediateStop =
            spEnd >= eqLineStop ||
            computeDualStop(spEnd, Math.min(eqLineStop, spEnd + 2), tokQState, hasNewline) <= spEnd
          const hasNonEmptyHeaderRhs =
            !immediateStop &&
            (spEnd > effectiveMatchEnd
              ? !/^[;)|&,\]}]/.test(text[spEnd] as string)
              : hasAssignRhsAt(effectiveMatchEnd, effectiveAfter, tokQState, coreToken, false, true) ||
                !/^[;)|&,\]}]/.test(text[spEnd] as string))
          if (hasNonEmptyHeaderRhs) {
            const dualStop = computeDualStop(effectiveMatchEnd, eqLineStop, tokQState, hasNewline)
            emitSpanRedaction('', dualStop)
            continue
          }
          forceHeaderTokenRedact = true
        }
      } else if (hasNewline) {
        if (isAssignKey) {
          const tokQState = getTokQState()
          const { precededByEq } = getPrefix()
          const precededByEqOrInComment = precededByEq || tokQState.inComment
          if (!isEqPaddedEnd) {
            const safeAfter =
              effectiveAfter === '' ||
              /^(?:[\s),;}\]"'`]|[:.!?](?:$|[\s'")\]},;])|\$(?:$|[\s),;}\]"'`]))/.test(effectiveAfter) ||
              ((effectiveAfter.startsWith('\\"') || effectiveAfter.startsWith("\\'"))
                ? !opensEscapedFast(effectiveMatchEnd, effectiveAfter[1] as string)
                : /^\\[nrt]/.test(effectiveAfter))
            if (
              precededByEqOrInComment &&
              isRecognizedConfigKey &&
              (!isBase64PaddedToken(coreToken) || isEmptyAssignIdentifier || coreToken[eqIdx + 1] === '~' || !safeAfter)
            ) {
              keepAndRewind()
              continue
            }
          } else {
            const hasAssignOrLineCommentCtx = precededByEqOrInComment
            const allowPaddedLowerQuoteRhs =
              isLowerWordKey && !endsWithDoubleEq && hasAssignOrLineCommentCtx
            const allowPunctRhs = !endsWithDoubleEq && hasAssignOrLineCommentCtx
            const isEmptyQuotedRhs =
              ((effectiveAfter[0] === '"' || effectiveAfter[0] === "'" || effectiveAfter[0] === '`') &&
                effectiveAfter[1] === effectiveAfter[0] &&
                isDelimOrSpaceAfterQuote(effectiveAfter[2])) ||
              (effectiveAfter[0] === '\\' &&
                (effectiveAfter[1] === '"' || effectiveAfter[1] === "'") &&
                effectiveAfter[2] === '\\' &&
                effectiveAfter[3] === effectiveAfter[1] &&
                isDelimOrSpaceAfterQuote(effectiveAfter[4]))
            const canKeepAssignRhs =
              hasAssignOrLineCommentCtx &&
              (isRecognizedConfigKey || (assignKey.length <= 100 && isSensitiveKey(assignKey)))
            const assignRhs =
              canKeepAssignRhs &&
              !isEmptyQuotedRhs &&
              hasAssignRhsAt(
                effectiveMatchEnd,
                effectiveAfter,
                tokQState,
                coreToken,
                allowPaddedLowerQuoteRhs,
                allowPunctRhs,
              )
            if (assignRhs) {
              keepAndRewind()
              continue
            }
            if (!hasAssignOrLineCommentCtx && assignKey.length <= 100 && isSensitiveKey(assignKey)) {
              if (
                !isEmptyQuotedRhs &&
                hasAssignRhsAt(
                  effectiveMatchEnd,
                  effectiveAfter,
                  tokQState,
                  coreToken,
                  isLowerWordKey && !endsWithDoubleEq,
                  !endsWithDoubleEq,
                )
              ) {
                const eqLineStop = lineStopFor(effectiveMatchEnd)
                const dualStop = computeDualStop(effectiveMatchEnd, eqLineStop, tokQState, hasNewline)
                emitSpanRedaction(coreToken.slice(eqIdx) + MARKER, dualStop)
                continue
              }
              if (trySpacedSensitiveEqRhs(false, coreToken, eqIdx, isLowerWordKey, endsWithDoubleEq, tokQState)) {
                continue
              }
              if (isEmptyQuotedRhs) {
                emitSpanRedaction('', effectiveMatchEnd)
                continue
              }
            }
            if (
              hasAssignOrLineCommentCtx &&
              assignKey.length <= 100 &&
              isSensitiveKey(assignKey) &&
              trySpacedSensitiveEqRhs(true, coreToken, eqIdx, isLowerWordKey, endsWithDoubleEq, tokQState)
            ) {
              continue
            }
            if (!endsWithDoubleEq) {
              const closedByEnclosingQuote =
                isBase64PaddedToken(coreToken) &&
                !precededByEq &&
                ((!tokQState.inComment && tokQState.activeQuote !== '' && effectiveAfter[0] === tokQState.activeQuote) ||
                  (!tokQState.inComment &&
                    tokQState.innerQuote !== '' &&
                    effectiveAfter[0] === tokQState.innerQuote &&
                    isDelimOrSpaceAfterQuote(effectiveAfter[1])) ||
                  ((tokQState.inComment ? tokQState.commentEscapedQuote : tokQState.activeEscapedQuote) !== '' &&
                    effectiveAfter.startsWith('\\' + (tokQState.inComment ? tokQState.commentEscapedQuote : tokQState.activeEscapedQuote)) &&
                    (isDelimOrSpaceAfterQuote(effectiveAfter[2]) ||
                      (!tokQState.inComment && tokQState.activeQuote === '' && effectiveAfter[2] === '\\'))))
              if (!closedByEnclosingQuote) {
                const isEmptyTailForAssign =
                  effectiveAfter === '' ||
                  /^[\s#;)|&]/.test(effectiveAfter) ||
                  ((effectiveAfter[0] === '"' || effectiveAfter[0] === "'" || effectiveAfter[0] === '`') &&
                    effectiveAfter[1] === effectiveAfter[0] &&
                    isDelimOrSpaceAfterQuote(effectiveAfter[2])) ||
                  trailingDots !== '' ||
                  ((effectiveAfter[0] === '"' || effectiveAfter[0] === "'") &&
                    isDelimOrSpaceAfterQuote(effectiveAfter[1]))
                if (isEmptyTailForAssign && precededByEqOrInComment && isRecognizedConfigKey) {
                  keepAndRewind()
                  continue
                }
              }
            }
          }
        }
      } else if (isAssignKey && assignKey.length <= 100 && isSensitiveKey(assignKey)) {
        const tokQState = getTokQState()
        const { precededByEq, precededByColon } = getPrefix()
        const hasAssignOrCommentCtx = precededByEq || precededByColon || tokQState.inComment
        if (isEqPaddedEnd) {
          const singleAssignRhs = hasAssignRhsAt(
            effectiveMatchEnd,
            effectiveAfter,
            tokQState,
            coreToken,
            isLowerWordKey && !endsWithDoubleEq,
            !endsWithDoubleEq || (isEmptyAssignIdentifier && isSensitiveKey(assignKey)),
          )
          if (singleAssignRhs) {
            if (hasAssignOrCommentCtx) {
              keepAndRewind()
              continue
            }
            const eqLineStop = lineStopFor(effectiveMatchEnd)
            const dualStop = computeDualStop(effectiveMatchEnd, eqLineStop, tokQState, hasNewline)
            emitSpanRedaction(coreToken.slice(eqIdx) + MARKER, dualStop)
            continue
          }
          if (trySpacedSensitiveEqRhs(hasAssignOrCommentCtx, coreToken, eqIdx, isLowerWordKey, endsWithDoubleEq, tokQState)) {
            continue
          }
        } else if (!isBase64PaddedToken(coreToken)) {
          if (hasAssignOrCommentCtx) {
            keepAndRewind()
            continue
          }
          const eqLineStop = lineStopFor(effectiveMatchEnd)
          const rhsAt = m.index + scheme.length + space.length + eqIdx + 1
          const dualStop = computeDualStop(rhsAt, eqLineStop, tokQState, hasNewline)
          emitSpanRedaction('=' + MARKER, dualStop)
          continue
        }
      }
    }
    if (!isBasic) {
      const shortHeaderToken =
        inHeader &&
        coreToken.length >= 8 &&
        !HEADER_PLACEHOLDER_TOKENS.has(coreToken.toLowerCase()) &&
        /[A-Za-z]/.test(coreToken) &&
        /[A-Z0-9._~+/=-]/.test(coreToken)
      if (!forceHeaderTokenRedact && coreToken.length < 12 && !shortHeaderToken) {
        keepAndRewind()
        continue
      }
      if (!inHeader && PASCAL_CASE_WORD_RE.test(coreToken) && isDocumentationPascalPhrase(coreToken)) {
        const hasProseFollow = (hasNewline || space.includes('\u00a0')) && /^[ \t\u00a0]+[a-z]{2,}\b/.test(effectiveAfter)
        if (hasProseFollow || space.includes('\u00a0')) {
          keepAndRewind()
          continue
        }
      }
    } else {
      if (
        hasNewline
          ? !inHeader && !basicB64 && !isBase64PaddedToken(coreToken) && !isPaddedBasicCandidate
          : !inHeader && !isLikelyBasicCredentialToken(coreToken)
      ) {
        keepAndRewind()
        continue
      }
      if (inHeader && coreToken.length < 4) {
        keepAndRewind()
        continue
      }
    }
    out += text.slice(copied, m.index) + scheme + ' ' + MARKER
    stats.redactions += 1
    copied = matchEnd
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// Incoming-webhook URLs, whose path is the credential: removed whole.
const WEBHOOK_URLS: readonly RegExp[] = [
  /^https?:\/\/hooks\.slack\.com\/(?:services|workflows|triggers)\//i,
  /^https?:\/\/(?:[a-z0-9-]+\.)?discord(?:app)?\.com\/api\/(?:v\d+\/)?webhooks\//i,
  /^https?:\/\/[a-z0-9-]+\.webhook\.office\.com\//i,
  /^https?:\/\/outlook\.office(?:365)?\.com\/webhook\//i,
]
const WEBHOOK_QUICK = /hooks\.slack\.com|discord(?:app)?\.com\/api\/|webhook\.office\.com|outlook\.office(?:365)?\.com\/webhook\//i

const decodeName = (name: string): string => {
  try {
    return decodeURIComponent(name.replace(/\+/g, ' '))
  } catch {
    return name
  }
}

const isSchemeChar = (c: number): boolean =>
  (c >= 97 && c <= 122) || (c >= 65 && c <= 90) || (c >= 48 && c <= 57) || c === 43 || c === 45 || c === 46
const isAsciiLetter = (c: number): boolean => (c >= 97 && c <= 122) || (c >= 65 && c <= 90)
const isUrlStop = (c: number): boolean =>
  c <= 32 || c === 34 || c === 39 || c === 60 || c === 62 || c === 92 || c === 96 || c === 127 || /\s/.test(String.fromCharCode(c))
const isUrlTrailer = (c: number): boolean =>
  c === 46 || c === 44 || c === 59 || c === 58 || c === 33 || c === 63 || c === 41 || c === 93 || c === 125

function trimUrlTrailerLinear(text: string, start: number, end: number): number {
  while (end > start && isUrlTrailer(text.charCodeAt(end - 1))) {
    // Preserve closing `]` of an IPv6 bracketed literal host (`[::1]`).
    if (text.charCodeAt(end - 1) === 93) {
      const slice = text.slice(Math.max(start, end - 64), end - 1)
      const openIdx = slice.lastIndexOf('[')
      if (openIdx !== -1 && (openIdx === 0 || slice[openIdx - 1] === '/' || slice[openIdx - 1] === '@') && /^[0-9a-fA-F:.]+$/.test(slice.slice(openIdx + 1))) break
    }
    end -= 1
  }
  return end
}

const VALID_HOST_AFTER_AT = /^(?:\[[0-9a-fA-F:.]+\]|localhost(?::\d+)?|[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+(?::\d+)?)(?:[/?#]|$)/

function redactUrl(url: string, stats: SanitizeStats): string {
  if (WEBHOOK_QUICK.test(url) && WEBHOOK_URLS.some(pattern => pattern.test(url))) {
    stats.redactions += 1
    return '[REDACTED:webhook-url]'
  }
  let out = url
  const atIdx = out.indexOf('@')
  if (atIdx !== -1) {
    const schemeSep = out.indexOf('://')
    const authStart = schemeSep + 3
    let firstDelim = out.length
    for (let k = authStart; k < out.length; k += 1) {
      const c = out.charCodeAt(k)
      if (c === 47 || c === 63 || c === 35) {
        firstDelim = k
        break
      }
    }
    const firstAt = out.indexOf('@', authStart)
    if (firstAt !== -1 && firstAt < firstDelim) {
      const userInfo = out.slice(authStart, firstAt)
      if (userInfo !== '') {
        if (userInfo !== MARKER) stats.redactions += 1
        out = `${out.slice(0, authStart)}${MARKER}${out.slice(firstAt)}`
      }
    } else if (firstDelim < out.length && atIdx > firstDelim) {
      // Check for `/`, `?` or `#` inside URL password (`https://user:p/a/s/s@host`
      // or `https://user:p?a=1#b@host`). Do NOT touch benign URLs whose host
      // has no non-port `:` (`npmjs.com/package/@types/node`, `example.com:8443?email=a@b`).
      const preDelim = out.slice(authStart, firstDelim)
      const colonIdx = preDelim.indexOf(':')
      if (colonIdx > 0 && !preDelim.startsWith('[')) {
        const afterColon = preDelim.slice(colonIdx + 1)
        if (!/^\d+$/.test(afterColon)) {
          let candidate = firstAt
          for (let tries = 0; candidate !== -1 && tries < 8; tries += 1) {
            if (VALID_HOST_AFTER_AT.test(out.slice(candidate + 1))) {
              out = `${out.slice(0, authStart)}${MARKER}${out.slice(candidate)}`
              stats.redactions += 1
              break
            }
            candidate = out.indexOf('@', candidate + 1)
          }
        }
      }
    }
  }
  if (!/[?&#;]/.test(out)) return out
  // Query and fragment parameters with a credential's name.
  return out.replace(/([?&#;])([^=&#;?/]+)=([^&#;]*)/g, (whole: string, separator: string, name: string, value: string) => {
    if (value === '' || !isSensitiveKey(decodeName(name))) return whole
    if (value !== MARKER) stats.redactions += 1
    return `${separator}${name}=${MARKER}`
  })
}

function nextUrlAt(text: string, from: number): { start: number; rawEnd: number } | null {
  let pos = text.indexOf('://', from)
  while (pos !== -1) {
    let s = pos
    while (s > 0 && pos - s <= 22 && isSchemeChar(text.charCodeAt(s - 1))) s -= 1
    const len = pos - s
    if (len >= 2 && len <= 21 && isAsciiLetter(text.charCodeAt(s)) && (s === 0 || !isWordCharCode(text.charCodeAt(s - 1)))) {
      let e = pos + 3
      while (e < text.length && !isUrlStop(text.charCodeAt(e))) e += 1
      if (e > pos + 3) return { start: s, rawEnd: e }
    }
    pos = text.indexOf('://', pos + 3)
  }
  return null
}

function redactUrls(text: string, stats: SanitizeStats): string {
  if (!text.includes('://')) return text
  let out = ''
  let copied = 0
  for (let u = nextUrlAt(text, 0); u !== null; u = nextUrlAt(text, u.rawEnd)) {
    const end = trimUrlTrailerLinear(text, u.start, u.rawEnd)
    const rawUrl = text.slice(u.start, end)
    const redacted = redactUrl(rawUrl, stats)
    if (redacted !== rawUrl) {
      out += text.slice(copied, u.start) + redacted
      copied = end
    }
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// Where the URLs in a text are: each one's start and end, in order.
function urlSpans(text: string): number[] {
  const spans: number[] = []
  if (!text.includes('://')) return spans
  for (let u = nextUrlAt(text, 0); u !== null; u = nextUrlAt(text, u.rawEnd)) spans.push(u.start, u.rawEnd)
  return spans
}

// Whether `i` is in one of the URLs at `spans` (see urlSpans).
function inUrl(spans: number[], i: number): boolean {
  let lo = 0
  let hi = spans.length / 2
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if ((spans[mid * 2 + 1] as number) <= i) lo = mid + 1
    else hi = mid
  }
  return lo < spans.length / 2 && (spans[lo * 2] as number) <= i
}

// A line continuation in a name, as text holds it: a backslash and a line
// break, or that pair as a JSON string encodes it (`\\\n`, `\\\r\n`), as in a
// log line that quotes a command.
const JOIN = String.raw`(?:\\(?:\r?\n|\\\\(?:r\\)?n))`

// The length of the line continuation (JOIN) that starts at `i`; 0 where none
// does.
function joinLength(text: string, i: number): number {
  if (text[i] !== '\\') return 0
  if (text[i + 1] === '\n') return 2
  if (text[i + 1] === '\r') return text[i + 2] === '\n' ? 3 : 0
  if (text[i + 1] !== '\\' || text[i + 2] !== '\\') return 0
  if (text[i + 3] === 'n') return 4
  return text[i + 3] === 'r' && text[i + 4] === '\\' && text[i + 5] === 'n' ? 6 : 0
}

// A character's place in a name: 2 for one a name may start with (a letter
// or `_`), 1 for one it may only hold (a digit, `.` or `-`), else 0.
const NAME_PLACES = Uint8Array.from({ length: 128 }, (_, c) => (/[A-Za-z_]/.test(String.fromCharCode(c)) ? 2 : /[0-9.-]/.test(String.fromCharCode(c)) ? 1 : 0))
const namePlace = (text: string, i: number): number => NAME_PLACES[text.charCodeAt(i)] ?? 0

// A name and a separator: `name: value` and `name=value` (a header, a YAML or
// .env line, a JSON key, an assignment, a command-line flag such as
// `--password=value` or `-Dssl.password=value`), the name quoted or not, its
// quote escaped or not, line continuations (JOIN) in and after it read
// through. That is, after the text's start or a character no name has
// (`boundary`), where this pattern matches, the name taken whole, so a long
// continued name that no separator follows is not tried again shorter:
//   (?:-JOIN*){0,2}([A-Za-z_](?:JOIN*[A-Za-z0-9_.-]){0,99})(?:\\*["'])?(?:[ \t]|JOIN)*([:=])[ \t]*
// As a regular expression it reads a long run of continuations again from
// each place in it where a name may start (the `n` of a JSON-encoded one),
// which is quadratic. Here each run (continuations, a name, backslashes, the
// blanks before a separator) is measured once, from the text's end back (see
// nameRuns), and each place is tried in a few steps.
type NamedValue = { index: number; boundary: string; name: string; separator: number; end: number }

// For each position in a text: where the run of line continuations (JOIN)
// that starts there ends (`joins`); the run of spaces, tabs and continuations
// (`blanks`); the run of backslashes (`slashes`); and, at a name's character,
// the name from there, continuations read through (`names`), and how many
// characters it has, counted up to 101 (`lengths`).
type NameRuns = { joins: Int32Array; blanks: Int32Array; slashes: Int32Array; names: Int32Array; lengths: Uint8Array }

function nameRuns(text: string): NameRuns {
  const n = text.length
  const runs: NameRuns = { joins: new Int32Array(n + 1), blanks: new Int32Array(n + 1), slashes: new Int32Array(n + 1), names: new Int32Array(n + 1), lengths: new Uint8Array(n + 1) }
  const { joins, blanks, slashes, names, lengths } = runs
  joins[n] = blanks[n] = slashes[n] = names[n] = n
  for (let i = n - 1; i >= 0; i -= 1) {
    const c = text[i]
    const join = joinLength(text, i)
    joins[i] = join === 0 ? i : (joins[i + join] as number)
    blanks[i] = c === ' ' || c === '\t' ? (blanks[i + 1] as number) : join === 0 ? i : (blanks[i + join] as number)
    slashes[i] = c === '\\' ? (slashes[i + 1] as number) : i
    if (namePlace(text, i) === 0) continue
    const next = joins[i + 1] as number
    const more = namePlace(text, next) !== 0
    names[i] = more ? (names[next] as number) : i + 1
    lengths[i] = more ? Math.min(101, (lengths[next] as number) + 1) : 1
  }
  return runs
}

// Without backslashes, a bounded native search needs none of the continuation
// tables. The boundary still prevents matching the tail of a longer name.
const PLAIN_NAMED_VALUE = /(^|[^A-Za-z0-9_.-])-{0,2}([A-Za-z_][A-Za-z0-9_.-]{0,99})["']?[ \t]*([:=])[ \t]*/g

// The first name and separator (see NamedValue) at or after `from`.
function namedValue(text: string, from: number, runs: NameRuns | undefined): NamedValue | null {
  if (runs === undefined) {
    PLAIN_NAMED_VALUE.lastIndex = from
    const m = PLAIN_NAMED_VALUE.exec(text)
    if (m === null) return null
    return { index: m.index, boundary: m[1] as string, name: m[2] as string,
      separator: m.index + m[0].lastIndexOf(m[3] as string), end: m.index + m[0].length }
  }
  for (let i = from; i < text.length; i += 1) {
    const found = (i === 0 ? namedAt(text, 0, '', runs) : null) ?? (namePlace(text, i) === 0 ? namedAt(text, i + 1, text[i] as string, runs) : null)
    if (found !== null) return found
  }
  return null
}

// A name and a separator (see NamedValue) right after `boundary`, which ends
// at `p`.
function namedAt(text: string, p: number, boundary: string, runs: NameRuns): NamedValue | null {
  // Up to two dashes, each with the continuations after it.
  let q = p
  for (let k = 0; k < 2 && text[q] === '-'; k += 1) q = runs.joins[q + 1] as number
  if (namePlace(text, q) !== 2 || (runs.lengths[q] as number) > 100) return null
  const end = runs.names[q] as number
  // A quote after the name, escaped or not, then spaces, tabs and
  // continuations.
  const slash = runs.slashes[end] as number
  const separator = runs.blanks[text[slash] === '"' || text[slash] === "'" ? slash + 1 : end] as number
  if (text[separator] !== ':' && text[separator] !== '=') return null
  let after = separator + 1
  while (text[after] === ' ' || text[after] === '\t') after += 1
  return { index: p - boundary.length, boundary, name: text.slice(q, end), separator, end: after }
}

// After a credential name, the value goes: to the end of its line, whatever is
// in it (quotes, brackets and redaction markers are not trusted to end it),
// or, for a query-style `?name=` / `&name=`, to the next `&`, `#`, `;` or
// space that no backslash escapes (through its shell word if that is longer:
// after `;` or `&`, as a shell assignment; after `?` or `#` where gcloud's
// delimiter holding it sets a list's entries apart (`^#^`); and where a
// quote, escaped or not, `$` or a backtick opens it). A line that
// ends in a continuation (`\`) runs on to the next. After `=` with no space
// around it (a flag or an assignment) the value runs on through the whole
// shell word it starts (see readValue), to the end of the line that word
// ends on; a flag's, to a control operator that ends its word
// (`--password=x;echo ok` keeps `;echo ok`); and one that starts inside a
// backtick span (a command substitution, or inline code in prose), to the
// backtick that closes it. Otherwise it runs through a quote that opens the
// value, to the end of the line that quote closes on. Anything left open runs
// to the end of the text, and the row is marked cut (see noteCut).

// The end of the line at or after `from`, run on through each line that ends
// in a continuation; `start` is where the value starts.
function lineEnd(text: string, start: number, from: number): number {
  for (let i = from; i < text.length; ) {
    const code = text.charCodeAt(i)
    if (code === 10 || code === 13 || code === 0x2028 || code === 0x2029) {
      if (i <= start || text.charCodeAt(i - 1) !== 92) return i
      i += code === 13 && text.charCodeAt(i + 1) === 10 ? 2 : 1
      continue
    }
    i += 1
  }
  return text.length
}

// Where a `name:` or `name = ` value ends.
function namedValueEnd(text: string, start: number): number {
  const quote = text[start]
  if (quote !== '"' && quote !== "'") return lineEnd(text, start, start)
  let q = start + 1
  while (q < text.length && text[q] !== quote) q += quote === '"' && text[q] === '\\' ? 2 : 1
  return q >= text.length ? text.length : lineEnd(text, start, q)
}

// Where a `name=` value whose shell word ends at `end` stops: there for a
// flag's whose word a control operator ends; for one in a code span
// (`spanEnd`, see readValue), at the backtick that closes the span, so the
// span stays closed and the text after it stays; else at the end of that
// line.
const valueStop = (text: string, start: number, end: number, flag: boolean, spanEnd = 0): number =>
  flag && /[;&|<>)]/.test(text[end] ?? '') ? end : spanEnd > 0 ? spanEnd : lineEnd(text, start, end)

// Whether a value read by the rules below is over at `at`: at `limit`, or at
// a blank, an operator, or a `,`, `}` or `]`, which end a value in JSON,
// YAML or a dict.
const valueOverAt = (text: string, at: number, limit: number): boolean =>
  at >= limit || /\s/.test(text[at] as string) || /[;&|<>(),}\]]/.test(text[at] as string)

// Where a credential's value itself, starting at `start`, ends within
// `[start, limit)`, as best these rules can tell: a quoted string at the
// quote that closes it (a doubled quote, `''` or `""`, does not), `$'…'`,
// `$(…)` and `${…}` whole, else at the first blank, operator or line break,
// or just past the quote the value sits in (`outerQuote`, as the `"` that
// closes `-H "Authorization: Bearer …"`). A `,`, `}` or `]` right after a
// quoted string ends the value, as in JSON, YAML or a dict.
// Used only to tell whether a redaction removed more than the value (see
// noteCut): it never narrows what is removed.
function fallbackValueEnd(text: string, start: number, limit: number, outerQuote = ''): number {
  let i = start
  if (i >= limit) return limit
  // A value that starts with the quote it sits in closes that quote
  // (`"PASSWORD="s3cret`): the value is what is glued on after it, read
  // outside quotes; none, where a blank or a delimiter follows.
  if (outerQuote !== '' && text[i] === outerQuote) {
    i += 1
    if (valueOverAt(text, i, limit)) return i
    outerQuote = ''
  }
  // A quote a command string's quoting escapes (`\"value\"`).
  if (text[i] === '\\' && (text[i + 1] === '"' || text[i + 1] === "'")) {
    const q = text[i + 1]
    i += 2
    while (i < limit) {
      if (text[i] === '\\' && text[i + 1] === q) return i + 2
      if (text[i] === '\n') break
      i += 1
    }
    return limit
  }
  const q = text[i]
  if ((q === '"' || q === "'") && q !== outerQuote) {
    i += 1
    while (i < limit) {
      if (q === '"' && text[i] === '\\' && i + 1 < limit) {
        i += 2
        continue
      }
      if (text[i] === q) {
        if (i + 1 < limit && text[i + 1] === q) {
          i += 2
          continue
        }
        i += 1
        // The quote the value sits in, closed right after its own.
        if (outerQuote !== '' && i < limit && text[i] === outerQuote) {
          i += 1
          outerQuote = ''
        }
        // More of the word glued after the quote is read on below, unless a
        // delimiter (`,`, `}`, `]`, an operator) ends the value.
        if (!valueOverAt(text, i, limit)) break
        return i
      }
      i += 1
    }
    if (i >= limit) return limit
  } else if (q === '$' && text[i + 1] === "'") {
    i += 2
    while (i < limit) {
      if (text[i] === '\\' && i + 1 < limit) {
        i += 2
        continue
      }
      if (text[i] === "'") return i + 1
      i += 1
    }
    return limit
  }
  while (i < limit) {
    const c = text[i] as string
    if (outerQuote !== '' && c === outerQuote) return i + 1
    if (c === '\\') {
      if (text[i + 1] === '\n') {
        i += 2
        continue
      }
      if (text[i + 1] === '\r' && text[i + 2] === '\n') {
        i += 3
        continue
      }
      if (i + 1 < limit) {
        i += 2
        continue
      }
    }
    if (c === '$' && (text[i + 1] === '(' || text[i + 1] === '{')) {
      const open = text[i + 1] as string
      const close = open === '(' ? ')' : '}'
      let depth = 1
      i += 2
      while (i < limit && depth > 0) {
        if (text[i] === '\\' && i + 1 < limit) {
          i += 2
          continue
        }
        if (text[i] === open) depth += 1
        else if (text[i] === close) depth -= 1
        i += 1
      }
      continue
    }
    if (c === '"' || c === "'") {
      // A quoted piece of the word only where a quote like it closes it
      // before `limit`; otherwise it closes a quote around the value.
      let k = i + 1
      let found = -1
      while (k < limit) {
        if (c === '"' && text[k] === '\\' && k + 1 < limit) {
          k += 2
          continue
        }
        if (text[k] === c) {
          if (k + 1 < limit && text[k + 1] === c) {
            k += 2
            continue
          }
          found = k
          break
        }
        k += 1
      }
      if (found === -1) return i + 1
      i = found + 1
      continue
    }
    if (c === '`') {
      const closeTick = text.indexOf('`', i + 1)
      if (closeTick !== -1 && closeTick < limit) {
        i = closeTick + 1
        continue
      }
      break
    }
    if (/\s/.test(c) || /[;&|<>()]/.test(c)) break
    i += 1
  }
  return i
}

// Just past the quote that closes the quoted value that starts at `start`,
// else `limit`. A backslash escapes the character after it inside double
// quotes, as in JSON, a dict or a log line, but not inside single quotes, as
// in the shell and YAML (`'abc\'` closes at its last quote); a doubled quote
// (`''`, `""`) goes on with the value. The quote the value sits in
// (`outerQuote`), closed right after, is the value's too, and so is a word
// glued on after the quotes (`"abc"def`), up to a blank, an operator or a
// `,`, `}`, `]` or `:` (fallbackValueEnd).
function closingQuoteEnd(text: string, start: number, limit: number, outerQuote = ''): number {
  const quote = text[start]
  for (let i = start + 1; i < limit; i += 1) {
    if (quote === '"' && text[i] === '\\') {
      i += 1
      continue
    }
    if (text[i] === quote) {
      if (i + 1 < limit && text[i + 1] === quote) {
        i += 1
        continue
      }
      let end = i + 1
      if (outerQuote !== '' && end < limit && text[end] === outerQuote) {
        end += 1
        outerQuote = ''
      }
      return valueOverAt(text, end, limit) || text[end] === ':' ? end : fallbackValueEnd(text, end, limit, outerQuote)
    }
  }
  return limit
}

// Where one value after a `name:` or `name = ` ends (see namedTokenEnd): a
// quoted value as closingQuoteEnd reads it, else its word (fallbackValueEnd).
// Where the quote the value sits in closes right at `start`
// (`'Authorization: Bearer 'token`), the value is what is glued on after it.
function namedSingleValueEnd(text: string, start: number, limit: number, outerQuote = ''): number {
  if (start >= limit) return limit
  if (outerQuote !== '' && text[start] === outerQuote) return valueOverAt(text, start + 1, limit) ? start + 1 : namedSingleValueEnd(text, start + 1, limit, '')
  if (text[start] === '"' || text[start] === "'") return closingQuoteEnd(text, start, limit, outerQuote)
  return fallbackValueEnd(text, start, limit, outerQuote)
}

// The same for a `name:` or `name = ` value (namedValueEnd), which may be an
// auth scheme and its token (`Bearer …`, `Basic …`). A quoted value ends at
// the quote that closes it: what follows (the `,` or `}` after a JSON or
// dict value) is not the credential's, unless it is glued on (`"abc"def`).
// A scheme can follow the quote the value sits in, where that quote closes
// right at `start` (`-H "Authorization: "Bearer …`).
function namedTokenEnd(text: string, start: number, limit: number, outerQuote = ''): number {
  if (start >= limit) return limit
  if (outerQuote !== '' && text[start] === outerQuote) return valueOverAt(text, start + 1, limit) ? start + 1 : namedTokenEnd(text, start + 1, limit, '')
  // A quoted value, or `$'…'`, is no scheme: it starts with a quote or `$`.
  const first = namedSingleValueEnd(text, start, limit, outerQuote)
  if (/^(?:Bearer|Basic|Token|Digest|Negotiate|AWS4-HMAC-SHA256|Bot)$/i.test(text.slice(start, first))) {
    let j = first
    while (j < limit && (text[j] === ' ' || text[j] === '\t')) j += 1
    if (j < limit && text[j] !== '\n' && text[j] !== '\r') return namedSingleValueEnd(text, j, limit, outerQuote)
  }
  return first
}

// Text a redaction removes past the credential's value (`valueEnd`) up to
// where it stops (`end`) is lost, not hidden: when any of it is more than
// blanks, the row is marked cut (`is_truncated`), as a cut for size is, and
// its length is counted (`droppedChars`). A cut that runs on past the line
// the value starts on (`start`) to the end of the text (an end these rules
// cannot tell) marks the row cut too.
function noteCut(text: string, start: number, valueEnd: number, end: number, stats: SanitizeStats): void {
  const dropped = text.slice(Math.min(Math.max(start, valueEnd), end), end)
  if (dropped.trim() !== '') {
    stats.truncated = true
    stats.droppedChars += dropped.length
  } else if (end >= text.length && !stats.truncated && lineEnd(text, start, start) < text.length) stats.truncated = true
}

// ---- credential values in shell text ----------------------------------------
//
// A credential's value is its shell word, read by a few simple rules
// (simpleEnd) from the quote it starts in, where that quote is known
// (readValue). When these heuristics report an uncertain end, the detected
// value and the text after it are removed. This can over-redact; it does not
// establish that every credential name or value is detected.

// The quotes a position can be in: none, double, single, ANSI-C (`$'…'`),
// or unknown.
const NONE = 0
const DOUBLE = 1
const SINGLE = 2
const ANSI = 3
const UNKNOWN = 4

// The quote a credential value sits in, for fallbackValueEnd: the one its
// first character is in (`state`, as quoteStates reads it). Where that scan
// cannot tell (`$'…'`, past a here-document's `<<`), a quote just before
// its name (`boundary`), while that quote is still open where the value
// starts (`open`): the key's own quote in `"password": "…"` closes before
// the value, so it is no quote around it. Outside quotes (NONE), none.
const outerQuoteOf = (state: number | undefined, boundary = '', open = true): string =>
  state === DOUBLE ? '"' : state === SINGLE ? "'" : state !== NONE && open && (boundary === '"' || boundary === "'") ? boundary : ''

// The quote open at `to`, reading `text` from `from` by the shell's quoting
// rules alone: single quotes, double quotes, `$'…'` and backslashes (a line
// continuation is an escaped line break). Two readings use it. The
// whole-text scan (`states` given, from the text's start) records each
// position's quote and reads all other code as plain text, but a comment
// (from a `#` that starts a word) is read on its own up to its line break;
// it cannot tell past a comment that ends in a backslash (bash 3.2 continues
// it, other shells do not), a `#` right after an operator (bash 3.2 reads no
// comment there), a `<<` that is not `<<<` (a here-document, whose body is
// not code) or a backtick that does not close: UNKNOWN from there on. A word
// read alone (no `states`) cannot be told past a backtick, `$(` or `${`,
// which open code, an escaped quote, backslash, `$` or backtick outside
// quotes (which a command string's quoting can leave in), or where an escape
// runs past `to`. The whole-text scan also reads an apostrophe in a word of
// prose as text (see proseApostrophe), unless `prose` is false (the shell's
// own reading, see SHELL_READINGS), and records in `ticks` where a
// backtick it reads outside quotes is open.
const QUOTE_TEXT = /[^"'\\$`<#\n]+/y
function isFenceStart(text: string, from: number, to: number, i: number, L: number): boolean {
  if (L < 3) return false
  let p = i
  while (p > from && text[p - 1] === ' ' && i - p < 3) p -= 1
  if (p !== from && text[p - 1] !== '\n') return false
  const nl = text.indexOf('\n', i + L)
  const lineStop = nl === -1 ? to : nl
  return !text.slice(i + L, lineStop).includes('`')
}

function pairLineRuns(text: string, from: number, to: number, lineRuns: { start: number; len: number }[], spanEnds?: Int32Array): number {
  let unclosed = -1
  let r = 0
  while (r < lineRuns.length) {
    const openRun = lineRuns[r]!
    const closeRun = lineRuns[r + 1]
    if (closeRun !== undefined && openRun.len === closeRun.len) {
      const inner = text.slice(openRun.start + openRun.len, closeRun.start)
      if (!inner.includes('`')) {
        const beforeChar = openRun.start > from ? (text[openRun.start - 1] ?? '') : ''
        const beforeOk = openRun.start === from || beforeChar === '\n' || /[\s([{"',;:]/.test(beforeChar)
        const afterEnd = closeRun.start + closeRun.len
        const afterChar = afterEnd < to ? (text[afterEnd] ?? '') : ''
        const afterOk = afterEnd >= to || afterChar === '\r' || afterChar === '\n' || /[\s.,;:!?)}\]]/.test(afterChar)
        // Two runs pair as a code span, or, one backtick each, as a command
        // substitution right after `=` (VERSION=`git rev-parse HEAD`, is no
        // span). Where two runs pair as neither, the runs of the line may pair
        // otherwise (the value in X=pre` SECRET` and `ok may run past SECRET),
        // so the line is left unclosed.
        if (openRun.start + openRun.len < closeRun.start && !/^\s|\s$/.test(inner) && afterOk && (beforeOk || (openRun.len === 1 && beforeChar === '='))) {
          if (beforeOk) spanEnds?.fill(closeRun.start, openRun.start + openRun.len, closeRun.start)
          r += 2
          continue
        }
      }
    }
    if (unclosed === -1) unclosed = openRun.start
    r += 1
  }
  lineRuns.length = 0
  return unclosed
}

function readQuotes(text: string, from: number, to: number, states?: Uint8Array, spanEnds?: Int32Array, prose = true): number {
  // These are the only characters that can alter the initial quote state.
  // A supplied state table is newly zeroed, so plain spans need no writes.
  if (!/["'\\$`<#]/.test(text.slice(from, to))) return NONE
  let q = NONE
  let comment = false
  let unclosedTick = -1
  let lineRuns: { start: number; len: number }[] | undefined
  let i = from
  while (i < to) {
    QUOTE_TEXT.lastIndex = i
    if (QUOTE_TEXT.test(text)) {
      const end = Math.min(to, QUOTE_TEXT.lastIndex)
      states?.fill(q, i, end)
      i = end
      continue
    }
    const c = text[i] as string
    if (c === '\n' && lineRuns !== undefined && lineRuns.length > 0) {
      const lineUnclosed = pairLineRuns(text, from, to, lineRuns, spanEnds)
      if (unclosedTick === -1 && lineUnclosed !== -1) unclosedTick = lineUnclosed
    }
    let next = i + 1
    let after = q
    if (comment && c === '\n') {
      after = NONE
      comment = false
    } else if (comment && c === '\\' && afterContinuations(text, i) > i) {
      after = UNKNOWN
    } else if (q === SINGLE) {
      if (c === "'") after = NONE
    } else if (c === '\\') {
      if (states === undefined && q === NONE && /["'\\$`]/.test(text[i + 1] ?? '')) after = UNKNOWN
      next = i + 2
    } else if (q === DOUBLE || q === ANSI) {
      if (c === (q === DOUBLE ? '"' : "'")) after = NONE
    } else if (states !== undefined && prose && c === "'" && proseApostrophe(text, i)) {
      // Text, not a quote (see proseApostrophe).
    } else if (c === "'" || c === '"') {
      after = c === "'" ? SINGLE : DOUBLE
    } else if (c === '$') {
      const n = afterContinuations(text, i + 1)
      const d = text[n] ?? ''
      if (d === "'") {
        after = ANSI
        next = n + 1
      } else if (states === undefined && (d === '(' || d === '{')) {
        after = UNKNOWN
      }
    } else if (states === undefined) {
      if (c === '`') after = UNKNOWN
    } else if (comment) {
      // A comment's text holds no code.
    } else if (c === '`') {
      let runLen = 0
      while (i + runLen < to && text[i + runLen] === '`') runLen += 1
      if (unclosedTick === -1 && (lineRuns === undefined || lineRuns.length === 0) && isFenceStart(text, from, to, i, runLen)) {
        const nl = text.indexOf('\n', i + runLen)
        next = nl === -1 ? to : nl + 1
      } else {
        ;(lineRuns ??= []).push({ start: i, len: runLen })
        next = i + runLen
      }
    } else if (c === '#' && (i === 0 || /[\s;&|()<>]/.test(text[i - 1] as string))) {
      if (i === 0 || /\s/.test(text[i - 1] as string)) comment = true
      else after = UNKNOWN
    } else if (c === '<') {
      const n = afterContinuations(text, i + 1)
      if (text[n] === '<') {
        const e = afterContinuations(text, n + 1)
        if (text[e] === '<') next = e + 1
        else after = UNKNOWN
      }
    }
    if (after === UNKNOWN) {
      if (lineRuns !== undefined && lineRuns.length > 0) {
        const lineUnclosed = pairLineRuns(text, from, to, lineRuns, spanEnds)
        if (unclosedTick === -1 && lineUnclosed !== -1) unclosedTick = lineUnclosed
      }
      if (unclosedTick !== -1) spanEnds?.fill(0)
      states?.fill(UNKNOWN, unclosedTick !== -1 && unclosedTick < i ? unclosedTick : i)
      return UNKNOWN
    }
    states?.fill(q, i, Math.min(next, to))
    q = after
    i = next
  }
  if (states !== undefined) {
    if (lineRuns !== undefined && lineRuns.length > 0) {
      const lineUnclosed = pairLineRuns(text, from, to, lineRuns, spanEnds)
      if (unclosedTick === -1 && lineUnclosed !== -1) unclosedTick = lineUnclosed
    }
    states[to] = q
    if (unclosedTick !== -1) {
      spanEnds?.fill(0)
      states.fill(UNKNOWN, unclosedTick)
    }
  }
  return i > to ? UNKNOWN : q
}

// An apostrophe in a word of prose: a letter before it and a common English
// ending after it (`It's`, `don't`, `we're`, `I'll`, `you've`, `I'd`,
// `I'm`). Prompts and answers are prose: read as a quote that never closes,
// one such apostrophe leaves the end of every credential value after it
// uncertain, and all the text from the first such value on is removed. In
// shell code the same word opens a quote, which a later `'` can close: a
// credential value after one is also read that way (see shellAgrees), and
// where the two readings of its end disagree, more is removed.
const WORD_BEFORE_APOSTROPHE = /[\p{L}\p{M}]/u
const CONTRACTION_END = /(?:s|t|re|ve|ll|d|m)(?![\p{L}\p{M}\p{N}_'])/iyu
function proseApostrophe(text: string, i: number): boolean {
  if (i === 0 || !WORD_BEFORE_APOSTROPHE.test(text[i - 1] ?? '')) return false
  CONTRACTION_END.lastIndex = i + 1
  return CONTRACTION_END.test(text)
}

// Where a same-line Markdown code span closes, by position, for the states
// quoteStates read: `closeStart > 0` at each index inside an unambiguous
// same-line span (open and close on one line, matched run lengths, no
// interior backtick, valid Markdown boundaries, non-empty interior); `0`
// otherwise. Absent for a text with no backtick.
const SPAN_ENDS = new WeakMap<Uint8Array, Int32Array>()

// The shell's own reading, for the states quoteStates read where the
// whole-text scan read an apostrophe in prose as text (absent elsewhere):
// each position's quote with every apostrophe a quote (see readQuotes), where
// the first apostrophe read as text is, where the last `'` that is no
// apostrophe in prose is (-1 where none is), and the last position outside
// quotes (-1 where none is).
type ShellReading = { states: Uint8Array; firstProse: number; lastPlainQuote: number; lastNone: number }
const SHELL_READINGS = new WeakMap<Uint8Array, ShellReading>()

function lastPlainQuoteOf(text: string): number {
  for (let k = text.length - 1; k >= 0; k -= 1) {
    if (text[k] === "'" && !proseApostrophe(text, k)) return k
  }
  return -1
}

// Each position's quote as the whole-text scan reads it (see readQuotes).
function quoteStates(text: string): Uint8Array {
  const states = new Uint8Array(text.length + 1)
  const spanEnds = text.includes('`') ? new Int32Array(text.length + 1) : undefined
  readQuotes(text, 0, text.length, states, spanEnds)
  if (spanEnds !== undefined) SPAN_ENDS.set(states, spanEnds)
  // An apostrophe in prose outside quotes is one the scan read as text.
  for (let k = text.indexOf("'"); k !== -1; k = text.indexOf("'", k + 1)) {
    if (states[k] === NONE && proseApostrophe(text, k)) {
      const shell = new Uint8Array(text.length + 1)
      readQuotes(text, 0, text.length, shell, undefined, false)
      let lastNone = text.length
      while (lastNone >= 0 && shell[lastNone] !== NONE) lastNone -= 1
      SHELL_READINGS.set(states, { states: shell, firstProse: k, lastPlainQuote: lastPlainQuoteOf(text), lastNone })
      break
    }
  }
  return states
}

// Where the word that holds `p` starts, as it reads alone: just past the last
// space, tab, line break or operator before `p`; -1 when that is more than
// 4,096 characters back.
function wordStart(text: string, p: number): number {
  let k = p
  while (k > 0 && !/[\s;&|<>()]/.test(text[k - 1] as string)) {
    k -= 1
    if (p - k > 4096) return -1
  }
  return k
}

// Where a credential value that starts at `start` in quote `q` ends, read by
// these rules alone: plain and backslash-escaped characters, single-quoted
// parts and double-quoted parts with no expansion, joined, up to the first
// space, tab, line break, `;`, `|`, `&`, `<`, `>` or `)` outside them. -1
// where they cannot tell: where the value holds `$(`, `${`, `$[`, `$'`, `$"`,
// a backtick, `(`, `<(`, `>(`, a line continuation, a quote that does not
// close, a single quote written `"'"` (as a single-quoted command string
// writes its own, `'"'"'`: the value may be in such a string, its quotes
// misread), or, outside quotes, an escaped quote, backslash, `$` or backtick
// (which a command string's quoting can leave in); where the quote it starts
// in closes with more of its word joined after it (`"PASSWORD="Secret`, where
// that quote may as well be an outer string's, such as JSON's); and where a
// redirection (`<`, `>` or `&>`, after nothing or a file descriptor's
// number) ends it, as the real value can come after a redirection. In a
// value that starts inside a backtick span (`span`), a backtick outside
// quotes after its first character is the one that closes the span, and it
// ends the value.
function simpleEnd(text: string, start: number, q: number, span = false, spanClose = 0): number {
  // Whether the value is still in the quote it starts in.
  let first = q !== NONE
  let i = start
  for (; i < text.length; i += 1) {
    const c = text[i] as string
    const d = text[i + 1] ?? ''
    if (c === '"' && d === "'" && text[i + 2] === '"') {
      return -1
    } else if (q === SINGLE ? c === "'" : q === DOUBLE && c === '"') {
      q = NONE
      if (first && !/^(?:[ \t\n;&|<>)]|\r\n|$)/.test(text.slice(i + 1, i + 3))) return -1
      first = false
    } else if (q === SINGLE) {
      // Single-quoted text is literal.
    } else if (c === '\\') {
      if (d === '' || d === '\n' || d === '\r' || (q === NONE && /["'\\$`]/.test(d))) return -1
      i += 1
    } else if (c === '`' && spanClose > 0 && q === NONE && i === spanClose) {
      break
    } else if (c === '`' && span && q === NONE && i > start) {
      if (d !== '' && !/[\s.,;:!?)}\]]/.test(d)) return -1
      break
    } else if (c === '`' || (c === '$' && /[({['"]/.test(d))) {
      return -1
    } else if (q === DOUBLE) {
      // Double-quoted text with no expansion is literal.
    } else if (c === "'" || c === '"') {
      q = c === "'" ? SINGLE : DOUBLE
    } else if (c === ' ' || c === '\t' || c === '\n' || (c === '\r' && d === '\n') || c === ';' || c === '|' || c === ')' || (c === '&' && d !== '>')) {
      break
    } else if (c === '<' || c === '>' || c === '&') {
      return text[afterContinuations(text, i + 1)] === '(' || /^\d*$/.test(text.slice(start, i)) ? -1 : i
    } else if (c === '(') {
      return -1
    }
  }
  return q === NONE ? i : -1
}

// Where the quote that opens at `open` closes; -1 where it does not.
function quoteCloses(text: string, open: number): number {
  const c = text[open]
  for (let i = open + 1; i < text.length; i += 1) {
    if (text[i] === c) return i
    if (c === '"' && text[i] === '\\') i += 1
  }
  return -1
}

// Whether a credential value that starts at `start` in quote `q`, its word
// ending at `end`, ends there by the shell's own reading too (see
// SHELL_READINGS), in which an apostrophe in prose before it opens a quote
// that a later `'` can close. It does where both readings start the value in
// the same quote. A value that is one quoted string (`'--password Secret'`,
// `--password 'Secret'`) keeps the whole-text scan's reading where the
// shell's reading leaves a quote open from `end` through the end of the text
// without closing around following text. An unquoted value keeps it where the
// shell's reading puts it in a single quote that only a later apostrophe in
// prose can close (`It's … --password=x … don't`), no other `'` coming at or
// after it.
function shellAgrees(text: string, start: number, end: number, q: number, states: Uint8Array): boolean {
  const shell = SHELL_READINGS.get(states)
  if (shell === undefined || shell.firstProse >= start || shell.states[start] === q) return true
  if (q !== NONE || ((text[start] === "'" || text[start] === '"') && end - 1 > start && quoteCloses(text, start) === end - 1)) {
    const sq = shell.states[end]
    return (sq === SINGLE || sq === DOUBLE) && shell.states[text.length] === sq && shell.lastNone < end
  }
  return shell.states[start] === SINGLE && shell.lastPlainQuote < start
}

// A credential value that starts at `start`, in the word that starts at
// `from`: where its shell word ends (see simpleEnd), and the quote it starts
// in, put back after the marker. Null where that end cannot be told with
// certainty: where the quote it starts in is not known, because the
// whole-text scan (`states`) and the word read alone from `from` disagree on
// it or either cannot tell (as after an apostrophe in prose, in a command
// string such as `sh -c "… --password …"`, past a here-document); where it
// starts in `$'…'`; where simpleEnd cannot tell; or where, after an
// apostrophe in prose, the shell's own reading may end it elsewhere (see
// shellAgrees). `alone`, where given, is the quote the word read alone is in
// at `start`, already read. `spanEnd`, where the value starts in a code span
// (see SPAN_ENDS) and its word ends within it, is where the backtick that
// closes that span starts; else 0. A word that runs past that backtick
// (`it's` and a later `don't`, which the whole-text scan reads as prose and
// this word as a quote) gets 0.
function readValue(text: string, start: number, from: number, states: Uint8Array, alone?: number): { end: number; close: string; spanEnd: number } | null {
  const q = states[start] ?? UNKNOWN
  if (from === -1 || q === UNKNOWN || q === ANSI || (alone ?? readQuotes(text, from, start)) !== q) return null
  const spanClose = q === NONE ? (SPAN_ENDS.get(states)?.[start] ?? 0) : 0
  const end = simpleEnd(text, start, q, false, spanClose)
  if (end === -1 || !shellAgrees(text, start, end, q, states)) return null
  return { end, close: q === DOUBLE ? '"' : q === SINGLE ? "'" : '', spanEnd: spanClose > 0 && end <= spanClose ? spanClose : 0 }
}

const JOINS = new RegExp(JOIN, 'g')
const JOIN_AT = new RegExp(JOIN, 'y')

// A name as the shell reads it, its line continuations (JOIN) removed.
const joined = (name: string): string => (name.includes('\\') ? name.replace(JOINS, '') : name)

// The first index at or after `i` that no line continuation (JOIN) covers.
function afterJoins(text: string, i: number): number {
  for (JOIN_AT.lastIndex = i; JOIN_AT.test(text); ) i = JOIN_AT.lastIndex
  return i
}

function redactNamedValues(text: string, stats: SanitizeStats): string {
  if (!/[:=]/.test(text)) return text
  const runs = text.includes('\\') ? nameRuns(text) : undefined
  let states: Uint8Array | undefined
  // gcloud's lists (see delimitedLists), found once: null where none is.
  let lists: DelimitedLists | null | undefined
  let out = ''
  let copied = 0
  let next = 0
  for (let m = namedValue(text, 0, runs); m !== null; m = namedValue(text, next, runs)) {
    next = m.end
    if (!isSensitiveKey(joined(m.name))) {
      // Resume at the separator: it can be the boundary before the next name
      // (`--from-literal=password=…`, `env: NAME=…`).
      next = m.separator
      continue
    }
    const start = m.end
    const from = wordStart(text, m.index + m.boundary.length)
    const equals = text[m.separator] === '='
    let end: number
    // Where the value itself ends (see noteCut).
    let valueEnd: number
    // A quote open where the value starts and closed in it is put back after
    // the marker.
    let close = ''
    if (equals && /^[?&#;]$/.test(m.boundary)) {
      // An escaped `&`, `#`, `;` or blank does not end it.
      end = text.length
      const stops = /[&#;\s]/g
      stops.lastIndex = start
      for (let stop = stops.exec(text); stop !== null; stop = stops.exec(text)) {
        let slash = stop.index
        while (slash > start && text[slash - 1] === '\\') slash -= 1
        // Outer quotes may encode another shell's backslashes. Unless the
        // value is unquoted, keep that uncertain suffix with the credential.
        if ((stop.index - slash) % 2 === 0 && (slash === stop.index || (states ??= quoteStates(text))[start] === NONE)) {
          end = stop.index
          break
        }
      }
      valueEnd = end
      // After `;` or `&` it may be a shell assignment, after `?` or `#` in a
      // list gcloud's delimiter holding it sets apart (`^#^`), a list's entry,
      // whose value a quoted or escaped `&` does not end, and a quote
      // (escaped too, as in a log line's JSON string), `$` or a backtick may
      // open it: through its shell word, if that is longer (escaped spaces,
      // quotes, expansions).
      if (lists === undefined) lists = delimitedLists(text)
      if (m.boundary === ';' || m.boundary === '&' || /^\\*["'`]|^\$/.test(text.slice(start, start + 8)) || delimiterAt(lists, m.index).includes(m.boundary)) {
        const read = readValue(text, start, from, (states ??= quoteStates(text)))
        if (read === null) {
          end = text.length
          valueEnd = fallbackValueEnd(text, start, end, outerQuoteOf((states ??= quoteStates(text))[start]))
        } else if (read.end >= end) {
          // Also where both end at once: the quote the value starts in is
          // put back.
          end = read.end
          valueEnd = read.end
          close = read.close
        }
      }
    } else if (equals && start === m.separator + 1 && !/[ \t]/.test(text[m.separator - 1] as string)) {
      const read = readValue(text, start, from, (states ??= quoteStates(text)))
      end = read === null ? text.length : valueStop(text, start, read.end, /^\$?["']*-/.test(text.slice(from, start)), read.spanEnd)
      valueEnd = read === null ? fallbackValueEnd(text, start, end, outerQuoteOf((states ??= quoteStates(text))[start])) : read.end
      close = read?.close ?? ''
    } else {
      end = namedValueEnd(text, start)
      const open = !text.slice(m.index + 1, start).includes(m.boundary)
      valueEnd = namedTokenEnd(text, start, end, outerQuoteOf((states ??= quoteStates(text))[start], m.boundary, open))
    }
    const value = text.slice(start, end)
    const trimmed = value.trim()
    if (trimmed === '') continue
    // Preserve values already replaced with a single redaction marker (e.g.,
    // `[REDACTED:jwt]`, `[REDACTED:private-key]`, `` `[REDACTED]` ``, or a
    // `-u token:[REDACTED] ...` credential whose username happens to match a
    // sensitive word), while keeping glued tails like ` [REDACTED]:glued`.
    if (
      /^(?:(?:\*\*|__|[*_])[ \t]+)?`?\[REDACTED(?::[a-z-]+)?\]`?$/.test(trimmed) ||
      (/(?:^|\s)(?:-u|--(?:proxy-)?user)[ \t=]+$/.test(text.slice(Math.max(0, m.index - 24), m.index + m.boundary.length)) && /^\[REDACTED(?::[a-z-]+)?\](?:[ \t'"]|$)/.test(trimmed))
    ) {
      continue
    }
    // An auth scheme whose token a pattern already redacted (and counted),
    // quoted or not (`"Bearer [REDACTED]"` in JSON, escaped in a log line),
    // including any trailing sentence/diagnostic punctuation.
    const alreadyRedactedScheme =
      /^\\?['"]?(?:Bearer|Basic|Token|Digest|Negotiate|AWS4-HMAC-SHA256|Bot)\s+\[REDACTED(?::[a-z-]+)?\](?:\\?['"]|[:,.!?])?$/i.test(text.slice(start, valueEnd).trim())
    noteCut(text, start, valueEnd, end, stats)
    out += text.slice(copied, start) + MARKER + close
    copied = end
    next = end
    if (value !== MARKER + close && !alreadyRedactedScheme) stats.redactions += 1
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// A long command-line option and its value after a space (`--password VALUE`,
// `--api-key "VALUE"`, `--pass -VALUE`, `"--password" VALUE`), or after line
// continuations (`--password \` and the value on the next line): the whole
// shell argument (see readValue). The option's name is a credential's and
// its last part a credential word (or a credential value noun such as
// `--secret-string`), so `--password-stdin host`, `--secret-id prod/db`,
// `--token-file path` and `--max-tokens 1024` keep theirs. A value that
// starts with `--` is the next option, not a value. Line continuations (JOIN)
// in the name are read through (`--pass\` and `word VALUE`). A continuation
// with no space beside it right after a credential name glues what follows
// to it (`--password\` and `VALUE`, `--pass\` and `"word" VALUE`): everything
// from there to the end of the text goes, unless what follows carries the
// name on into another option (`--password\` and `-stdin`, `"--password"\`
// and `"-stdin"`).
const FLAG_VALUE = new RegExp(String.raw`(^|[^A-Za-z0-9_.-])-${JOIN}*-${JOIN}*([A-Za-z](?:${JOIN}*[A-Za-z0-9_.-]){0,99})((?:\\*["'])?)((?:[ \t]|${JOIN})+)(?![ \t]|${JOIN})(?!--)(?=\S)`, 'g')
// A value that is one redaction marker and nothing else.
const WHOLE_MARKER = /^\[REDACTED(?::[a-z-]+)?\]$/

let _contText = ''
let _contFrom = -1
let _contTo = -1

// The first index at or after `i` that a line continuation (a backslash and
// a newline, which the shell removes) does not cover.
function afterContinuations(text: string, i: number): number {
  if (text[i] !== '\\') return i
  if (text === _contText && i >= _contFrom && i < _contTo) return _contTo
  const start = i
  for (;;) {
    if (text[i] === '\\' && text[i + 1] === '\n') i += 2
    else if (text[i] === '\\' && text[i + 1] === '\r' && text[i + 2] === '\n') i += 3
    else {
      if (i > start) {
        _contText = text
        _contFrom = start
        _contTo = i
      }
      return i
    }
  }
}

const CREDENTIAL_VALUE_NOUNS = new Set(['string', 'value', 'values', 'data', 'text', 'raw', 'hex', 'b64', 'base64', 'pem', 'json', 'payload', 'blob'])

// Whether a long option's name is a credential's: one with a credential word
// for its last part, or a compound value noun (`--secret-string`, `--token-value`).
const credentialOption = (name: string): boolean => {
  if (!isSensitiveKey(name)) return false
  const parts = nameParts(name)
  const last = parts.pop() ?? ''
  if (forms(last).some(isCredentialWord)) return true
  return CREDENTIAL_VALUE_NOUNS.has(last) && parts.length > 0 && forms(parts[parts.length - 1] as string).some(isCredentialWord)
}

function redactFlagValues(text: string, stats: SanitizeStats): string {
  FLAG_VALUE.lastIndex = 0
  let states: Uint8Array | undefined
  let out = ''
  let copied = 0
  for (let m = FLAG_VALUE.exec(text); m !== null; m = FLAG_VALUE.exec(text)) {
    const raw = m[2] as string
    const own = joined(raw)
    const dashes = m.index + (m[1] as string).length
    let start = m.index + m[0].length
    let end = text.length
    // Where the value itself ends (see noteCut); -1 until it is read.
    let valueEnd = -1
    let close = ''
    if (!/[ \t]/.test(m[4] as string)) {
      // Line continuations alone after the name glue what follows to its
      // word, as the shell passes it.
      const at = optionStart(text, dashes)
      const word = at === -1 ? null : optionWord(text, at)
      const name = word !== null && /^--[A-Za-z]/.test(word.name) ? word.name.slice(2) : own
      // Carried on into another option: its value, if it is a credential's,
      // is redactOptionWords'.
      if (name.length > own.length && name.startsWith(own) && /^[-_.]/.test(name.slice(own.length))) continue
      if (!credentialOption(own) && !credentialOption(name)) continue
    } else if (credentialOption(own)) {
      const read = readValue(text, start, wordStart(text, dashes), (states ??= quoteStates(text)))
      if (read !== null) {
        // No value before an operator (`--password ;`).
        if (read.end === start) continue
        end = read.end
        valueEnd = read.end
        close = read.close
      }
    } else {
      // With a continuation in the name taken for a glued value (`--password\`
      // and `VALUE`), the part before it may be a credential option's: the
      // longest such part, before a letter or digit. Everything after its
      // continuation goes.
      let cut = -1
      let part = ''
      for (let k = 0; k < raw.length; ) {
        const after = afterJoins(raw, k)
        if (after === k) {
          part += raw[k]
          k += 1
          continue
        }
        if (/[A-Za-z0-9]/.test(raw[after] ?? '') && credentialOption(part)) cut = after
        k = after
      }
      if (cut === -1) continue
      start = m.index + m[0].indexOf(raw, (m[1] as string).length) + cut
    }
    if (wholeMarker(text.slice(start, end), close)) continue
    if (valueEnd === -1) valueEnd = fallbackValueEnd(text, start, end, outerQuoteOf((states ??= quoteStates(text))[start]))
    noteCut(text, start, valueEnd, end, stats)
    out += text.slice(copied, start) + MARKER + close
    copied = end
    stats.redactions += 1
    FLAG_VALUE.lastIndex = end
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// Whether a value is one redaction marker and the quote put back after it.
const wholeMarker = (value: string, close: string): boolean =>
  WHOLE_MARKER.test(close !== '' && value.endsWith(close) ? value.slice(0, -close.length) : value)

// The single-dash password options of the JDK's keytool and jarsigner
// (`-storepass VALUE`, `-keypass`, `-srcstorepass`, `-deststorepass`,
// `-srckeypass`, `-destkeypass`, `-truststorepass`), which the rule above
// (two dashes) does not read: a list of their names, so `-keystore`,
// `-storetype`, `-storepassword` and every other single-dash option keep
// their values. After the name, blanks or line continuations (JOIN), then
// the value: the whole shell argument (see readValue), never one that starts
// with a dash and a letter (the next option, as in `-keypass -v`).
// `-storepass:env NAME` and `-storepass:file PATH` name a variable or a file
// and keep them, as does a value that is one shell variable alone (`$NAME`,
// `${NAME}`, double-quoted or not): where the password comes from, not the
// password. Names spelled in pieces (`"-storepass"`, `-store'pass'`) are
// redactOptionWords'.
const SINGLE_DASH_SECRET_FLAGS: ReadonlySet<string> = new Set(['storepass', 'keypass', 'srcstorepass', 'deststorepass', 'srckeypass', 'destkeypass', 'truststorepass'])
const SINGLE_DASH_SECRET = new RegExp(String.raw`(^|[\s;|&()\x60"'])-(?:${[...SINGLE_DASH_SECRET_FLAGS].join('|')})(?![:=])(?:[ \t]|${JOIN})+(?![ \t]|${JOIN})(?!-[A-Za-z])(?=\S)`, 'gi')
// A value that is one shell variable alone, as above, from its first
// character to the end of its word.
const VARIABLE_ALONE = /(?:\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[A-Za-z_][A-Za-z0-9_]*\})|"\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[A-Za-z_][A-Za-z0-9_]*\})")(?![^\s;&|<>)])/y
const variableAlone = (text: string, start: number): boolean => {
  VARIABLE_ALONE.lastIndex = start
  return VARIABLE_ALONE.test(text)
}

function redactSingleDashSecrets(text: string, stats: SanitizeStats): string {
  SINGLE_DASH_SECRET.lastIndex = 0
  let states: Uint8Array | undefined
  let out = ''
  let copied = 0
  for (let m = SINGLE_DASH_SECRET.exec(text); m !== null; m = SINGLE_DASH_SECRET.exec(text)) {
    const dashes = m.index + (m[1] as string).length
    const start = m.index + m[0].length
    if (variableAlone(text, start)) continue
    const read = readValue(text, start, wordStart(text, dashes), (states ??= quoteStates(text)))
    // No value before an operator (`-storepass ;`).
    if (read !== null && read.end === start) continue
    const end = read !== null ? read.end : text.length
    const close = read !== null ? read.close : ''
    if (wholeMarker(text.slice(start, end), close)) continue
    const valueEnd = read !== null ? read.end : fallbackValueEnd(text, start, end, outerQuoteOf(states[start]))
    noteCut(text, start, valueEnd, end, stats)
    out += text.slice(copied, start) + MARKER + close
    copied = end
    stats.redactions += 1
    SINGLE_DASH_SECRET.lastIndex = end
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// The start of the word whose first dash is at `p`, past the quotes before
// it; -1 when other text comes first (the word is no option).
function optionStart(text: string, p: number): number {
  let k = p
  while (k > 0 && (text[k - 1] === '"' || text[k - 1] === "'" || (text[k - 1] === '$' && text[k] === "'"))) k -= 1
  return k === 0 || /[\s;&|(]/.test(text[k - 1] as string) ? k : -1
}

// An ANSI-C escape at `i` (a backslash inside `$'…'`): the character it
// stands for and its length. A control character or an escape it does not
// decode gives a character no option name has.
function ansiEscape(text: string, i: number): { char: string; length: number } {
  const ahead = text.slice(i + 1, i + 10)
  const hex = /^x([0-9A-Fa-f]{1,2})/.exec(ahead)
  if (hex !== null) return { char: String.fromCharCode(parseInt(hex[1] as string, 16)), length: 1 + hex[0].length }
  const octal = /^[0-7]{1,3}/.exec(ahead)
  if (octal !== null) return { char: String.fromCharCode(parseInt(octal[0], 8) & 0xff), length: 1 + octal[0].length }
  const unicode = /^(?:u[0-9A-Fa-f]{1,4}|U[0-9A-Fa-f]{1,8})/.exec(ahead)
  if (unicode !== null) {
    const code = parseInt(unicode[0].slice(1), 16)
    return { char: code <= 0x10ffff ? String.fromCodePoint(code) : '\0', length: 1 + unicode[0].length }
  }
  const e = ahead[0] ?? ''
  return { char: /^[\\'"?]$/.test(e) ? e : '\0', length: 2 }
}

// A word's name, where it starts and ends, where the value after an `=`
// starts (-1: none), and the state its reader is in there or at the word's
// end (see optionWord and looseWord), from which a list reading goes on.
type OptionWord = { name: string; start: number; end: number; value: number; state: number }

// What a list reading (see optionWord and looseWord) comes to from each place
// in a text where a name starts again, by the state it is in there: each
// list is read once, whichever word's reading comes to it.
type ListReadings = Map<number, OptionWord | null>

// optionWord's quotes, as readQuotes numbers them, and back.
const QUOTE_OF: Readonly<Record<string, number>> = { '': NONE, '"': DOUBLE, "'": SINGLE, a: ANSI }
const QUOTES = ['', '"', "'", 'a']

// What a list reading came to, kept for each place `passed` where it started
// a name again.
function settle(readings: ListReadings | undefined, passed: number[] | undefined, word: OptionWord | null): OptionWord | null {
  if (readings !== undefined && passed !== undefined) for (const key of passed) readings.set(key, word)
  return word
}

// Whether a character is one a name may hold (see NAME_PLACES), white space
// (as `\s` reads it) or a control operator's (`;&|<>()`), told by a table
// for ASCII, as the readers ask of each character.
const isNameChar = (ch: string): boolean => ch.length === 1 && (NAME_PLACES[ch.charCodeAt(0)] ?? 0) !== 0
const ASCII_KINDS = Uint8Array.from({ length: 128 }, (_, c) => (/\s/.test(String.fromCharCode(c)) ? 1 : /[;&|<>()]/.test(String.fromCharCode(c)) ? 2 : 0))
const isSpace = (c: string): boolean => (c.charCodeAt(0) < 128 ? ASCII_KINDS[c.charCodeAt(0)] === 1 : /\s/.test(c))
const isOperator = (c: string): boolean => ASCII_KINDS[c.charCodeAt(0)] === 2

// A bounded heuristic for a long option's word from `i`: quotes and
// backslashes removed, supported ANSI-C escapes decoded and line continuations
// dropped, up to its first `=` or its end. Returns that text, where it starts
// and ends and where the value after an `=` starts (-1: no `=`), and the
// state this reading is in there (see wordState); null when, before then,
// the word holds an expansion, a character no option name has, or a name
// longer than 200 characters. With `accept`, a name before an `=` that it
// does not take is passed by, and the rest of the word is read as a list of
// names and values (`--set-env-vars=ORDINARY=x,DB_PASS"WORD"=…`,
// `-o user=x:pass"word"=…`): a name starts again after each `=`, each
// expansion (`$VALUE`, `${VALUE}`, whatever it holds) and each other
// character no name has but a quote or a backslash the word leaves in
// (those are dropped), up to a name before an `=` that `accept` takes. A name
// of more than 200 characters is none, but does not end the list: it may be
// a long value. Null where code whose end is not read (a backtick, `$(`)
// comes first, or the word ends, unless nothing but name characters came
// after its last `=` (that name is returned, as without `accept`). Given
// `after`, the state a reading was in after a word's `=` whose value starts
// at `i`, it reads on from there as such a list. What a list comes to is kept
// in `readings` (see ListReadings).
function optionWord(text: string, i: number, accept?: (word: OptionWord) => boolean, readings?: ListReadings, after?: number): OptionWord | null {
  let name = ''
  let start = i
  let quote = after === undefined ? '' : (QUOTES[after >> 1] as string)
  // Whether a name before an `=` was passed by, and since the last `=` a
  // character no name has came.
  let listed = after !== undefined
  let apart = false
  // Whether the word read alone from where this reading started (see
  // readQuotes) still tells the quote it is in: not past `${`, `$'` across a
  // line continuation, or, outside quotes, an escaped quote, backslash, `$`
  // or backtick.
  let known = after === undefined || (after & 1) === 1
  // Where this reading started a name again in a list, with its state there.
  let passed: number[] | undefined
  while (listed || name.length <= 200) {
    if (readings !== undefined && listed && start === i) {
      const key = ((i * 4 + (QUOTE_OF[quote] as number)) * 2 + (known ? 1 : 0)) * 2 + (apart ? 1 : 0)
      const read = readings.get(key)
      if (read !== undefined) return settle(readings, passed, read)
      ;(passed ??= []).push(key)
    }
    const c = text[i]
    if (c === undefined) return settle(readings, passed, quote === '' && !apart && name.length <= 200 ? { name, start, end: i, value: -1, state: known ? 1 : 0 } : null)
    let ch = c
    let step = 1
    if (quote === "'" || quote === 'a') {
      if (c === "'") {
        quote = ''
        i += 1
        continue
      }
      if (quote === 'a' && c === '\\') {
        const e = ansiEscape(text, i)
        ch = e.char
        step = e.length
      }
    } else if (c === '\\') {
      const next = afterContinuations(text, i)
      if (next > i) {
        i = next
        continue
      }
      if (quote === '"' && !/[\\"$`]/.test(text[i + 1] ?? '')) {
        ch = '\\'
      } else {
        if (quote === '' && /["'\\$`]/.test(text[i + 1] ?? '')) known = false
        ch = text[i + 1] ?? ''
        step = 2
      }
    } else if (quote === '"' && c === '"') {
      quote = ''
      i += 1
      continue
    } else if (quote === '' && c === '$' && text[i + 1] === "'") {
      quote = 'a'
      i += 2
      continue
    } else if (c === '$' || c === '`') {
      // An expansion, outside quotes or in double quotes: one more character
      // no name has, in a list.
      const d = text[afterContinuations(text, i + 1)]
      if (!listed || c === '`' || d === '(') return settle(readings, passed, null)
      if (quote === '' && (d === '{' || d === "'")) known = false
    } else if (quote === '' && (c === "'" || c === '"')) {
      quote = c
      i += 1
      continue
    } else if (quote === '' && /[\s;&|<>()]/.test(c)) {
      return settle(readings, passed, apart || name.length > 200 ? null : { name, start, end: i, value: -1, state: known ? 1 : 0 })
    }
    if (ch === '=') {
      const word = { name, start, end: i, value: i + step, state: (QUOTE_OF[quote] as number) * 2 + (known ? 1 : 0) }
      if (accept === undefined || (name.length <= 200 && accept(word))) return settle(readings, passed, word)
      listed = true
      apart = false
      name = ''
      start = i + step
    } else if (isNameChar(ch)) {
      if (name.length <= 200) name += ch
    } else if (listed && /^["'\\]$/.test(ch)) {
      // A quote or backslash the word leaves in is dropped, as the shell of a
      // command string the word may be in reads it (`DB_PASS'"'"'WORD'"'"'=`).
    } else if (listed) {
      apart = true
      name = ''
      start = i + step
    } else {
      return settle(readings, passed, null)
    }
    i += step
  }
  return settle(readings, passed, null)
}

// The quote the word read alone is in where optionWord's reading from the
// word's start left it in `state` (see readQuotes): UNKNOWN where it cannot
// tell.
const wordState = (state: number): number => ((state & 1) === 1 ? state >> 1 : UNKNOWN)

// A line break as a JSON string encodes it, after the backslashes that end
// before it: `n`, or `r`, backslashes and `n`.
const JSON_BREAK = /(?:r\\+)?n/y

// Apostrophes in an outer command string, with extra backslashes from
// JSON or another shell. Quote analysis keeps each transition separate.
// The loose name reader can skip the entire run of encoding fragments.
const OUTER_APOSTROPHE = /'(?:\\+''|\\*"'\\*"')/y
function outerApostrophe(text: string, at: number, fragments = false): number {
  OUTER_APOSTROPHE.lastIndex = at
  if (!OUTER_APOSTROPHE.test(text)) return 0
  let end = OUTER_APOSTROPHE.lastIndex
  if (!fragments) return end - at
  while (end < text.length && /["'\\]/.test(text[end] as string)) end += 1
  while (text[end - 1] === '\\') end -= 1
  return end - at
}

// The word at `i` read loosely for selected command-string spellings:
// quotes, backslashes and recognized line continuations (raw or JSON-encoded)
// are dropped. This is not a parser for arbitrary quoting depth. Escapes in
// `$'…'` decoded, up to its first `=` or its end (a space, tab, line break
// or operator). A continuation right after a credential's name glues what
// follows to it as its value (as after an `=`), unless that carries the name
// on (`-`, `_`, `.`). Null where it holds a character no name has. With
// `accept`, read on as a list past a name before an `=` that it does not
// take, as optionWord reads one, from `after` and with `readings` as it does
// too; its state is whether `$'` came before.
function looseWord(text: string, i: number, accept?: (word: OptionWord) => boolean, readings?: ListReadings, after?: number, stickyAnsi = false): OptionWord | null {
  let name = ''
  let start = i
  let ansi = after === 1
  // As in optionWord.
  let listed = after !== undefined
  let apart = false
  let passed: number[] | undefined
  while (listed || name.length <= 200) {
    if (readings !== undefined && listed && start === i) {
      const key = (i * 2 + (ansi ? 1 : 0)) * 2 + (apart ? 1 : 0)
      const read = readings.get(key)
      if (read !== undefined) return settle(readings, passed, read)
      ;(passed ??= []).push(key)
    }
    const c = text[i]
    // In a list, an operator, which the string's own quotes may hold, is one
    // more character no name has.
    if (c === undefined || isSpace(c) || (!listed && isOperator(c))) return settle(readings, passed, apart || name.length > 200 ? null : { name, start, end: i, value: -1, state: ansi ? 1 : 0 })
    if (c === '\\') {
      let j = i
      while (text[j] === '\\') j += 1
      let next = text[j] === '\n' ? j + 1 : text[j] === '\r' && text[j + 1] === '\n' ? j + 2 : -1
      JSON_BREAK.lastIndex = j
      if (next === -1 && j - i >= 3 && (j - i) % 2 === 1 && JSON_BREAK.test(text)) next = JSON_BREAK.lastIndex
      if (next !== -1 && name !== '' && name.length <= 200 && !/[-_.]/.test(text[next] ?? '') && isSensitiveKey(name.replace(/^-+/, ''))) {
        const word = { name, start, end: next, value: next, state: ansi ? 1 : 0 }
        if (accept === undefined || accept(word)) return settle(readings, passed, word)
      }
      if (next !== -1) {
        i = next
        continue
      }
      if (ansi && /[0-7xuU]/.test(text[j] ?? '')) {
        const e = ansiEscape(text, j - 1)
        i = j - 1 + e.length
        if (isNameChar(e.char)) {
          if (name.length <= 200) name += e.char
        } else if (listed) {
          // After a character no name has, a name starts again in a list.
          apart = true
          name = ''
          start = i
        } else {
          return settle(readings, passed, null)
        }
      } else {
        i = j
      }
      continue
    }
    const ansiQuote = c === '$' && text[i + 1] === "'"
    if (ansiQuote) {
      ansi = true
      // A single-quoted command string spells its own apostrophe this way.
      const apostrophe = outerApostrophe(text, i + 1, true)
      if (apostrophe > 0) {
        i += 1 + apostrophe
        continue
      }
    } else if (c === '=') {
      const word = { name, start, end: i, value: i + 1, state: ansi ? 1 : 0 }
      if (accept === undefined || (name.length <= 200 && accept(word))) return settle(readings, passed, word)
      listed = true
      apart = false
      name = ''
      start = i + 1
    } else if (isNameChar(c)) {
      if (name.length <= 200) name += c
    } else if (c === '"' || c === "'") {
      if (c === "'") {
        if (!stickyAnsi) ansi = false
        const apostrophe = outerApostrophe(text, i, true)
        if (apostrophe > 0) {
          i += apostrophe
          continue
        }
      }
    } else if (listed && c !== '`' && !(c === '$' && text[i + 1] === '(')) {
      // After a character no name has, an expansion among them, a name
      // starts again in a list.
      apart = true
      name = ''
      start = i + 1
    } else {
      // Else the word is no name's; code whose end is not read (a backtick,
      // `$(`) ends a list too.
      return settle(readings, passed, null)
    }
    i += ansiQuote ? 2 : 1
  }
  return settle(readings, passed, null)
}

// The option or name a word is: `--name`, or before an `=` also `-name`,
// `-Dname` or a shell variable's name; in a list (`listed`), any name as the
// rules above read one (`db.password`, `x-api-key`).
function nameOf(word: OptionWord, listed = false): string | undefined {
  const m = (word.value === -1 ? /^--([A-Za-z][A-Za-z0-9_.-]*)$/ : listed ? /^(?:--?([A-Za-z][A-Za-z0-9_.-]*)|([A-Za-z_][A-Za-z0-9_.-]*))$/ : /^(?:--?([A-Za-z][A-Za-z0-9_.-]*)|([A-Za-z_][A-Za-z0-9_]*))$/).exec(word.name)
  return m?.[1] ?? m?.[2]
}

// A command that runs its arguments, joined, as shell code (`ssh host cmd
// …`, `eval cmd …`, `watch cmd …`): that shell reads their quotes again.
const JOINER = /(?:^|[\s(`/])(?:ssh|eval|watch)(?=[ \t])/g

// For each position in `text`, 1 where such a command is named before it in
// its command (after the last line break, `;`, `&` or `|`).
function joinedArguments(text: string): Uint8Array {
  const marks = new Uint8Array(text.length + 1)
  JOINER.lastIndex = 0
  const ends = /[\n;&|]/g
  let through = -1
  for (let m = JOINER.exec(text); m !== null; m = JOINER.exec(text)) {
    const start = m.index + m[0].length
    if (start <= through) continue
    ends.lastIndex = start
    through = ends.exec(text)?.index ?? text.length
    // The separator itself is still in the preceding command's interval.
    marks.fill(1, start, through + 1)
  }
  return marks
}

// The whole-text readings a word's rules use, made once per text: each
// position's quote (see quoteStates) and the first position after it in
// another quote (see quoteChanges), whether a shell reads it again as a
// joined argument (see joinedArguments), where commandWord's marks are (see
// commandMarks), and what the lists of names and values read as the shell
// passes them and loosely come to (see ListReadings).
type Scan = { states: () => Uint8Array; changes: () => Int32Array; rerun: () => Uint8Array; marks: () => CommandMarks; lists: () => ListReadings; ansi: boolean; looseLists: (sticky?: boolean) => ListReadings }

// For each position, the first position after it whose quote (`states`)
// differs.
function quoteChanges(states: Uint8Array): Int32Array {
  const changes = new Int32Array(states.length)
  changes[states.length - 1] = states.length
  for (let i = states.length - 2; i >= 0; i -= 1) changes[i] = states[i + 1] !== states[i] ? i + 1 : (changes[i + 1] as number)
  return changes
}

// For each position in a text: the first backslash or `"'"` at or after it
// (`marks`) and the first space, tab, line break or other white space
// (`spaces`), so that commandWord, run at each of a long word's dashes,
// quotes or backslashes, does not read the word again each time.
type CommandMarks = { marks: Int32Array; spaces: Int32Array }

function commandMarks(text: string): CommandMarks {
  const n = text.length
  const marks = new Int32Array(n + 1)
  const spaces = new Int32Array(n + 1)
  marks[n] = spaces[n] = n
  for (let i = n - 1; i >= 0; i -= 1) {
    marks[i] = text[i] === '\\' || text.startsWith(`"'"`, i) ? i : (marks[i + 1] as number)
    spaces[i] = isSpace(text[i] as string) ? i : (spaces[i + 1] as number)
  }
  return { marks, spaces }
}

// Whether the word at `at` may be a command string's, quoted for the shell
// that runs it: where it holds a backslash that the whole-text scan does not
// read inside a quote the word opens itself (in `cli '--pa\ssword' x` the
// backslash is literal), or does but the word is a joined argument (`ssh
// host cli '--pa\ssword' x`); or a single quote written `"'"`, as a
// single-quoted command string writes its own (`'"'"'`). Only the first 400
// characters of the word are read.
function commandWord(text: string, at: number, scan: Scan): boolean {
  const { marks, spaces } = scan.marks()
  const end = Math.min(spaces[at] as number, at + 400)
  for (let k = marks[at] as number; k < end; k = marks[k + 1] as number) {
    if (text.startsWith(`"'"`, k)) return true
    const states = scan.states()
    if (states[k] !== SINGLE && states[k] !== DOUBLE) return true
    // In a quote: one the word opens itself, unless the quote open at `at`
    // lasts to it.
    if ((scan.changes()[at] as number) > k || scan.rerun()[at] === 1) return true
  }
  return false
}

// Whether a word's name, or its name and `=`, holds a quote or a backslash
// that is not a line continuation's (`"DB_PASS"WORD`, `--password\=`): one
// spelled plainly is the rules above's.
const inPieces = (text: string, word: OptionWord): boolean =>
  /["'\\]/.test(joined(text.slice(word.start, word.value === -1 ? word.end : word.value)))

// The credential name the word at `at` holds, as the shell passes it (see
// optionWord), else, where the word may be a command string's (see
// commandWord), read loosely (see looseWord): the word's own, or, after the
// `=` of a name that is not a credential's, the next one's
// (`--from-literal=pass"word"=…`, each part after an `=` read afresh); and
// where it starts. Where neither reading finds one so, the word is read on
// as a list of names and values (`--set-env-vars=ORDINARY=x,DB_PASS"WORD"=…`),
// both ways, each list once for the whole text (see ListReadings), and of the
// names spelled in pieces (see inPieces) the one whose value starts first is
// taken; read as the shell passes it, with the quote the word read alone is
// in there. Undefined where none is.
function credentialWord(text: string, at: number, scan: Scan): { word: OptionWord; name: string; nested: boolean; alone?: number | undefined } | undefined {
  // The words the readings that pass an `=` read first (only those can go on
  // as a list), and what they read next.
  const heads: [OptionWord, boolean, OptionWord | null, boolean][] = []
  // Ambiguous nesting can hide an ANSI-C closing quote. Keep a second,
  // conservative reading in which later escapes can still belong to it.
  for (const mode of commandWord(text, at, scan) ? (scan.ansi ? [0, 1, 2] : [0, 1]) : [0]) {
    const nested = mode !== 0
    const sticky = mode === 2
    let head: OptionWord | undefined
    for (let from = at, k = 0; k < 8; k += 1) {
      const word = nested ? looseWord(text, from, undefined, undefined, undefined, sticky) : optionWord(text, from)
      const name = word === null || !PIECE_IN_NAME.test(word.name) ? undefined : nameOf(word)
      if (word !== null && name !== undefined && isSensitiveKey(name)) return { word, name, nested }
      if (k === 1 && head !== undefined) heads.push([head, nested, word, sticky])
      if (word === null || word.value === -1) break
      if (k === 0) head = word
      from = word.value
    }
  }
  // A list's credential name, spelled in pieces.
  const listed = (word: OptionWord): boolean => {
    const name = PIECE_IN_NAME.test(word.name) ? nameOf(word, true) : undefined
    return name !== undefined && isSensitiveKey(name) && inPieces(text, word)
  }
  let found: { word: OptionWord; name: string; nested: boolean; alone?: number | undefined } | undefined
  const starts = (word: OptionWord): number => (word.value === -1 ? word.end : word.value)
  for (const [head, nested, next, sticky] of heads) {
    // The list goes on after the first word's `=`, in the state its reading
    // was in there; after a loose word glued at a continuation, it reads that
    // name on. Where the next reading, from that `=` outside quotes, ended the
    // word (loosely, at a space or the text's end) with a name that is no
    // credential's, the list ends there too: it reads what that reading did.
    const glued = head.value === head.end
    const ended = !glued && next !== null && next.value === -1 && (nested ? head.state === 0 && (next.end === text.length || /\s/.test(text[next.end] as string)) : head.state >> 1 === NONE)
    const word = listed(head) ? head : ended ? null : glued ? looseWord(text, at, listed, scan.looseLists(sticky), undefined, sticky) : nested ? looseWord(text, head.value, listed, scan.looseLists(sticky), head.state, sticky) : optionWord(text, head.value, listed, scan.lists(), head.state)
    // A word with a value is one `listed` took.
    if (word === null || (word.value === -1 && !listed(word))) continue
    if (found === undefined || starts(word) < starts(found.word)) found = { word, name: nameOf(word, true) as string, nested, alone: nested ? undefined : wordState(word.state) }
  }
  return found
}

// Where a word starts that the rules above may see only in pieces: at a
// quote, a backslash, `$'` or a dash, or a name that a quote, a backslash or
// `$'` comes in (`DB_PASS"WORD"`), or an `=` follows (a list of names and
// values, `ORDINARY=x,DB_PASS"WORD"=…`). Also where a list goes on after a
// command substitution, whose end its reading does not read: after a `)` or
// a backtick and a list's separator (`VERSION=$(git rev-parse HEAD),…`).
const OPTION_WORD = /(?:^|[\s;&|(]|[)`][,:@#/+%!~^])(?=["'\\$-]|[A-Za-z_][A-Za-z0-9_.-]*["'\\$=])/g

// A word the shell passes as one of the listed single-dash password options
// (see SINGLE_DASH_SECRET) once its quotes and backslashes are read
// (`"-storepass"`, `-store'pass'`, `-store\pass`), with no `=` or `:` in it:
// its value is the next shell argument, as a long option's is.
function singleDashWord(text: string, at: number): { word: OptionWord; name: string; nested: boolean; single: boolean } | undefined {
  const word = optionWord(text, at)
  if (word === null || word.value !== -1 || !word.name.startsWith('-') || word.name.startsWith('--')) return undefined
  const name = word.name.slice(1)
  return SINGLE_DASH_SECRET_FLAGS.has(name.toLowerCase()) ? { word, name, nested: false, single: true } : undefined
}

// A word the shell passes as a long option once its quotes, backslashes,
// ANSI-C escapes and line continuations are read (`"--pass"word`,
// `--pass''word`, `--pa\ssword`, `-\-password`, `$'--pa\x73sword'`,
// `"--pass"word"=…"`), or as a name before an `=` (`"DB_PASS"WORD=…`,
// `-Dssl.pass"word"=…`, also after another option's `=`), which the rules
// above see only in pieces; also such a word of a command string another
// shell runs (`\"--pass\"word`, `--pa\\ssword`, `'"'"'--password'"'"'`).
// With `=`, its value as a flag's or an assignment's; else, if its name's
// last part is a credential word, or it is one of the listed single-dash
// password options (see singleDashWord), the next shell argument. A command
// string's value runs to the end of the text.
function redactOptionWords(text: string, stats: SanitizeStats): string {
  OPTION_WORD.lastIndex = 0
  let states: Uint8Array | undefined
  let changes: Int32Array | undefined
  let rerun: Uint8Array | undefined
  let marks: CommandMarks | undefined
  let lists: ListReadings | undefined
  let looseLists: ListReadings | undefined
  let stickyLists: ListReadings | undefined
  const scan: Scan = {
    ansi: text.includes("$'"),
    states: () => (states ??= quoteStates(text)),
    changes: () => (changes ??= quoteChanges(scan.states())),
    rerun: () => (rerun ??= joinedArguments(text)),
    marks: () => (marks ??= commandMarks(text)),
    lists: () => (lists ??= new Map()),
    looseLists: sticky => sticky ? (stickyLists ??= new Map()) : (looseLists ??= new Map()),
  }
  // Each listed single-dash option's name ends in `pass`: a text with none
  // holds none of them.
  const singleDash = /pass/i.test(text)
  let backslashes: ReturnType<typeof backslashReader> | undefined
  let out = ''
  let copied = 0
  for (let m = OPTION_WORD.exec(text); m !== null; m = OPTION_WORD.exec(text)) {
    const at = m.index + m[0].length
    OPTION_WORD.lastIndex = at + 1
    // A line continuation's line break starts no word.
    if (m[0] === '\n' && (text[m.index - 1] === '\\' || (text[m.index - 1] === '\r' && text[m.index - 2] === '\\'))) continue
    const option: { word: OptionWord; name: string; nested: boolean; alone?: number | undefined; single?: boolean } | undefined = credentialWord(text, at, scan) ?? (singleDash ? singleDashWord(text, at) : undefined)
    if (option === undefined || !inPieces(text, option.word)) continue
    const { word, name, nested } = option
    // Undoing an outer command string must still respect a word's own
    // literal backslashes. ANSI-C names have a separate escape reader.
    const quoted = /["']/.test(text[word.start] ?? '') || (text[word.start] === '\\' && /["']/.test(text[word.start + 1] ?? ''))
    if (nested && quoted && scan.rerun()[word.start] !== 1 && !text.slice(word.start, word.end).includes("$'")) {
      let literal = false
      for (let k = word.start; k < word.end && !literal; k += 1) {
        if (text[k] === '\\' && isNameChar(text[k + 1] ?? '')) literal = (backslashes ??= backslashReader(text, scan.states, scan.rerun)).literal(k)
      }
      if (literal) continue
    }
    const from = word.start
    // A name the rules above see whole (a quote at most before and after
    // it) is theirs.
    if (!nested && word.value !== -1 && !word.name.startsWith('--') && /^\$?["']?-?[A-Za-z_][A-Za-z0-9_.-]*["']?$/.test(text.slice(from, word.value - 1))) continue
    let start: number
    let end = text.length
    // Where the value itself ends (see noteCut).
    let valueEnd: number
    let close = ''
    if (word.value !== -1) {
      start = word.value
      const read = nested ? null : readValue(text, start, at, scan.states(), option.alone)
      if (read !== null) {
        end = valueStop(text, start, read.end, /^\$?["']*-/.test(text.slice(at, start)), read.spanEnd)
        valueEnd = read.end
        close = read.close
      } else valueEnd = fallbackValueEnd(text, start, end, outerQuoteOf(scan.states()[start]))
    } else {
      if (option.single !== true && !credentialOption(name)) continue
      start = word.end
      for (;;) {
        if (text[start] === ' ' || text[start] === '\t') start += 1
        else if (afterContinuations(text, start) > start) start = afterContinuations(text, start)
        else break
      }
      if (start === word.end || start >= text.length || /\s/.test(text[start] as string) || text.startsWith('--', start)) continue
      // A listed single-dash option's value is never the next option, nor a
      // shell variable alone (see SINGLE_DASH_SECRET).
      if (option.single === true && (/^-[A-Za-z]/.test(text.slice(start, start + 2)) || variableAlone(text, start))) continue
      const read = nested ? null : readValue(text, start, at, scan.states(), option.alone)
      if (read !== null) {
        if (read.end === start) continue
        end = read.end
        valueEnd = read.end
        close = read.close
      } else valueEnd = fallbackValueEnd(text, start, end, outerQuoteOf(scan.states()[start]))
    }
    const value = text.slice(start, end)
    if (value.trim() === '' || wholeMarker(value, close)) continue
    noteCut(text, start, valueEnd, end, stats)
    out += text.slice(copied, start) + MARKER + close
    copied = end
    OPTION_WORD.lastIndex = end
    stats.redactions += 1
  }
  return copied === 0 ? text : out + text.slice(copied)
}

// ---- heuristic fallback for names in lists ----------------------------------
//
// Finite regression cases include `^::^`, `^#^`, `^'^`, `^\^`, `^.^`,
// `^1^`, `^ab^` and `"^ q^"`, with selected earlier expansions, substitutions
// and literal values. The fallback scans backward from `=` through name
// characters and recognized quote/escape fragments, then classifies candidate
// suffixes using the detected delimiter. A detected credential value is read
// as a flag's or assignment's (see readValue).
// This does not make earlier text irrelevant: an earlier quoted space can
// cause a backslash-spelled name to be missed in a double-quoted command
// string or INFO JSON log. Non-`/` substitution patterns after an assignment
// can also be over-redacted. Both are pinned as known gaps in the tests.

// Detected declared lists: where each starts (just past the delimiter) and
// ends, and the delimiter produced by delimiterAfter's limited decoding. As
// a substitution in the list may hold spaces, whose end is not read, a list
// runs to the end of its line (on through continued lines).
type DelimitedLists = { starts: number[]; ends: number[]; delimiters: string[] }

// Whether the `^` at `a` may open gcloud's delimiter: at the start of a word
// or of an option's value (`--set-env-vars=^DELIM^…`, `--labels "^DELIM^…"`),
// past the quotes and backslashes before it. One after a list entry's `=`
// (`VERSION=^x^…`) is that entry's value.
function opensList(text: string, a: number): boolean {
  let b = a
  while (b > 0 && (/["'\\]/.test(text[b - 1] as string) || (text[b - 1] === '$' && text[b] === "'"))) b -= 1
  if (b === 0 || isSpace(text[b - 1] as string)) return true
  if (text[b - 1] !== '=') return false
  let s = b - 1
  while (s > 0 && isNameChar(text[s - 1] as string)) s -= 1
  if (text[s] !== '-') return false
  while (s > 0 && /["'\\]/.test(text[s - 1] as string)) s -= 1
  return s === 0 || isSpace(text[s - 1] as string)
}

// A delimiter candidate after `^`: a closing `^` at most 256 positions on
// (at most 255 intervening characters), with recognized continuations,
// quotes and backslashes dropped and spaces kept. ANSI-C and other shell
// expansions are not decoded here. Null if empty or no closing `^` is found.
function delimiterAfter(text: string, a: number): { end: number; delimiter: string } | null {
  let delimiter = ''
  for (let i = a + 1; i < text.length && i - a <= 256; ) {
    const c = text[i] as string
    if (c === '^') return delimiter === '' ? null : { end: i + 1, delimiter }
    const join = joinLength(text, i)
    if (join > 0) {
      i += join
      continue
    }
    if (c === '\n' || c === '\r') return null
    if (c !== '"' && c !== "'" && c !== '\\') delimiter += c
    i += 1
  }
  return null
}

function delimitedLists(text: string): DelimitedLists | null {
  if (!text.includes('^')) return null
  const lists: DelimitedLists = { starts: [], ends: [], delimiters: [] }
  // The end of the line the last list started on, found once for each line.
  let line = -1
  for (let a = text.indexOf('^'); a !== -1; a = text.indexOf('^', a + 1)) {
    if (!opensList(text, a)) continue
    const found = delimiterAfter(text, a)
    if (found === null) continue
    const start = found.end
    if (start >= line) line = lineEnd(text, start, start)
    lists.starts.push(start)
    lists.ends.push(line)
    lists.delimiters.push(found.delimiter)
    // The closing `^` opens nothing.
    a = start - 1
  }
  return lists.starts.length === 0 ? null : lists
}

// The detected delimiter at `i` (see delimitedLists): the latest list's
// that started before it and has not ended; '' where none is.
function delimiterAt(lists: DelimitedLists | null, i: number): string {
  if (lists === null) return ''
  let lo = 0
  let hi = lists.starts.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if ((lists.starts[mid] as number) <= i) lo = mid + 1
    else hi = mid
  }
  return lo > 0 && (lists.ends[lo - 1] as number) > i ? (lists.delimiters[lo - 1] as string) : ''
}

// A double quote can carry escape layers from command strings and logs.
function localeQuoteAt(text: string, i: number): boolean {
  while (text[i] === '\\') i += 1
  return text[i] === '"'
}

// Where the pieces of the name before the `=` at `e` start (see above), and
// whether a name they hold is a credential's, using the declared delimiter
// to distinguish entry boundaries from pieces of the same name. Candidate
// segments before successive `=` are disjoint; name-length tests are finite.
function listName(text: string, e: number, commandEquals: (end: number) => number, delimiter: string, names: Map<string, boolean>, reader: () => ReturnType<typeof backslashReader>): { from: number; credential: boolean } {
  let r = e
  while (r > 0) {
    const c = text[r - 1] as string
    if (isNameChar(c) || c === '"' || c === "'" || c === '\\' || (c === '$' && (text[r] === "'" || localeQuoteAt(text, r)))) r -= 1
    else if (c === '\n' && text[r - 2] === '\\') r -= 2
    else if (c === '\n' && text[r - 2] === '\r' && text[r - 3] === '\\') r -= 3
    else break
  }
  const raw = text.slice(r, e)
  if (!/[A-Za-z]/.test(raw) && !raw.includes("$'")) return { from: r, credential: false }
  const plain = !/["'\\$]/.test(raw)
  // Plain names with no letter/digit delimiter suffix have one candidate.
  // Reuse the normal classifier and its cache instead of rebuilding parts.
  if (plain && !/[A-Za-z0-9]$/.test(delimiter)) {
    const name = raw.replace(/^[0-9.-]+/, '')
    // A long alphanumeric name cannot be listed. A credential stem then
    // settles it without constructing thousands of camel-case parts.
    const stem = name.length > MAX_LISTED_NAME_CHARS && /^[A-Za-z0-9]+$/.test(name) && SENSITIVE_STEMS.some(part => name.toLowerCase().includes(part))
    return { from: r, credential: stem || isSensitiveKey(name) }
  }
  if (plain) {
    const name = raw.replace(/^[0-9.-]+/, '')
    if (isSensitiveKey(name)) return { from: r, credential: true }
    if (!isListedKey(name)) {
      // Removing a delimiter prefix cannot create a stem or an ending that
      // the whole name lacks. Only a whole credential word at the end of a
      // part can become sensitive when its prefix is removed.
      const lower = raw.toLowerCase()
      let from = 0
      for (const part of nameParts(raw)) {
        const start = lower.indexOf(part, from)
        for (const form of forms(part)) for (const word of SENSITIVE_WORDS) {
          if (!form.endsWith(word)) continue
          const at = start + form.length - word.length
          // Leading underscores do not change the remaining name's parts.
          for (let candidate = at; candidate >= 0; candidate -= 1) {
            if (r + candidate >= delimiter.length && text.startsWith(delimiter, r + candidate - delimiter.length) && isSensitiveKey(raw.slice(candidate))) return { from: r, credential: true }
            if (raw[candidate - 1] !== '_') break
          }
        }
        from = start + part.length
      }
      return { from: r, credential: false }
    }
  }
  // Heuristic: scope the earlier-assignment check to this command and treat
  // a preceding `/` as standalone. This preserves the tested `/` patterns.
  // Known gap: `#`, `:` and `%` patterns after an assignment in this command
  // can bypass the literal-quote guard and be over-redacted.
  const standalone = text[r - 1] === '/' || commandEquals(e) >= r
  // Without escapes, repeated spellings in the same quote context share
  // their final classification, including rejected literal quotes.
  const cacheable = raw.length <= 200 && !raw.includes('\\')
  const context = cacheable && standalone && delimiter === '' ? reader().quote(r) : NONE
  const cacheKey = cacheable ? `${standalone ? 1 : 0}:${context}:${delimiter.length}:${delimiter}${raw}` : undefined
  const cached = cacheKey === undefined ? undefined : names.get(cacheKey)
  if (cached !== undefined) return { from: r, credential: cached }
  // An outer single-quoted command string represents its apostrophes with
  // joined quote/escape sequences. Undo those before reading ANSI-C parts.
  const hasAnsi = raw.includes("$'")
  const source = hasAnsi ? raw.replace(/'\\''/g, "'").replace(/'"'"'/g, "'") : raw
  let chars = ''
  const starts: number[] = []
  const quotes: number[] = []
  const slashes: number[] = []
  let open = true
  let ansi = false
  const suffix = delimiter
  const prefix = text.slice(Math.max(0, r - suffix.length), r)
  // Prefix lengths let overlapping delimiter matches advance once through
  // the source, instead of re-comparing the whole suffix at every letter.
  const prefixes = new Uint16Array(suffix.length)
  for (let i = 1, matched = 0; i < suffix.length; i += 1) {
    while (matched > 0 && suffix[i] !== suffix[matched]) matched = prefixes[matched - 1] as number
    if (suffix[i] === suffix[matched]) matched += 1
    prefixes[i] = matched
  }
  let scanned = -prefix.length
  let matched = 0
  const followsSuffix = (at: number): boolean => {
    if (suffix === '') return false
    while (scanned < at) {
      const c = (scanned < 0 ? prefix[scanned + prefix.length] : source[scanned]) as string
      scanned += 1
      if (c === '"' || c === "'" || c === '\\') continue
      while (matched > 0 && (matched === suffix.length || c !== suffix[matched])) matched = prefixes[matched - 1] as number
      if (c === suffix[matched]) matched += 1
    }
    return matched === suffix.length
  }
  if (plain) {
    for (let i = 0; i < source.length; i += 1) {
      if (namePlace(source, i) !== 2) continue
      if (open || followsSuffix(i)) starts.push(i)
      open = false
    }
    return { from: r, credential: sensitiveListSuffix(source, starts) }
  }
  const add = (ch: string, at: number): void => {
    if (namePlace(ch, 0) === 2) {
      if (open || followsSuffix(at)) starts.push(chars.length)
      open = false
    }
    chars += ch
  }
  for (let i = 0; i < source.length; ) {
    const join = joinLength(source, i)
    const c = source[i] as string
    if (join > 0) {
      i += join
    } else if (c === '$' && source[i + 1] === "'") {
      ansi = true
      i += 2
    } else if (c === '$' && localeQuoteAt(source, i + 1)) {
      // Locale quoting joins the same name pieces as ordinary double quotes.
      i += 1
    } else if (c === "'" || c === '"') {
      // In the standalone, undeclared-list cases below, check for a literal
      // opposite quote. Other contexts are still read loosely.
      if (source === raw && !hasAnsi && delimiter === '' && standalone) {
        quotes.push(r + i)
      }
      if (c === "'") ansi = false
      if (delimiter.includes(c)) open = true
      i += 1
    } else if (c === '\\' && ansi) {
      let j = i + 1
      while (source[j] === '\\') j += 1
      // Outer double quotes and JSON strings add backslashes. The final
      // backslash introduces the ANSI-C escape after that quoting is undone.
      const escape = ansiEscape(source, j - 1)
      i = j - 1 + escape.length
      if (isNameChar(escape.char)) add(escape.char, j - 1)
      else if (!/["'\\]/.test(escape.char)) {
        chars = ''
        starts.length = 0
        open = true
      }
    } else if (c === '\\') {
      // A word's own single/double quotes can keep this backslash literal.
      // Such a name is not the name obtained by simply deleting the slash.
      if (source === raw && !hasAnsi && isNameChar(source[i + 1] ?? '')) slashes.push(r + i)
      if (delimiter.includes(c)) open = true
      i += 1
    } else {
      add(c, i)
      i += 1
    }
  }
  // There is no name-length cap here. Each segment before an `=` is read
  // once, including long names that the bounded forward readers skip.
  const credential = sensitiveListSuffix(chars, starts)
  const finish = (credential: boolean): { from: number; credential: boolean } => {
    if (cacheKey !== undefined && names.size < 4096) names.set(cacheKey, credential)
    return { from: r, credential }
  }
  if (!credential) return finish(false)
  // Check the remaining quote transitions only for a possible credential;
  // repeated spellings already reused their final result above.
  for (const at of quotes) {
    const q = reader().quote(at)
    if ((text[at] === '"' && q === SINGLE) || (text[at] === "'" && q === DOUBLE)) return finish(false)
  }
  for (const at of slashes) if (reader().literal(at)) return finish(false)
  return finish(true)
}

// Classify the supplied candidate suffixes without slicing and normalizing overlapping
// names. Camel-case parts stay the same when a prefix is removed; only the
// first remaining part can change. Each part's credential stems and endings
// are scanned once, then each candidate uses those summaries.
function sensitiveListSuffix(chars: string, starts: number[]): boolean {
  if (starts.length === 0 || !PIECE_IN_NAME.test(chars)) return false
  if (starts.length === 1) return isSensitiveKey(chars.slice(starts[0]))
  const lower = chars.toLowerCase()
  const parts = nameParts(chars)
  const ranges: { start: number; end: number; until: number; whole: Set<number>; tail: boolean }[] = []
  let from = 0
  for (const part of parts) {
    const start = lower.indexOf(part, from)
    const end = start + part.length
    let until = -1
    for (const stem of SENSITIVE_STEMS) {
      const at = part.lastIndexOf(stem)
      if (at !== -1) until = Math.max(until, start + at)
    }
    const whole = new Set<number>()
    for (const form of forms(part)) {
      const stop = start + form.length
      for (const ending of SENSITIVE_ENDINGS) if (form.endsWith(ending)) until = Math.max(until, stop - ending.length)
      for (const word of SENSITIVE_WORDS) if (form.endsWith(word)) whole.add(stop - word.length)
    }
    ranges.push({ start, end, until, whole, tail: false })
    from = end
  }
  let tail = false
  for (let i = ranges.length - 1; i >= 0; i -= 1) {
    const part = ranges[i]!
    part.tail = tail
    tail ||= part.start <= part.until || part.whole.has(part.start)
  }
  // Only a bounded number of suffixes can be listed: each distinct normalized candidate starts
  // at a letter, and listed names contain at most this many name
  // characters. Punctuation does not count toward that bound.
  let listedFrom = chars.length
  let count = 0
  while (listedFrom > 0 && count <= MAX_LISTED_NAME_CHARS) {
    listedFrom -= 1
    if (/[A-Za-z0-9]/.test(chars[listedFrom] as string)) count += 1
  }
  if (count > MAX_LISTED_NAME_CHARS) listedFrom += 1
  let partAt = 0
  let checkedAt = -1
  for (const at of starts) {
    while (partAt < ranges.length && ranges[partAt]!.end <= at) partAt += 1
    const part = ranges[partAt]
    if (part === undefined) break
    const candidate = Math.max(at, part.start)
    // Leading underscores all normalize to the same remaining parts.
    if (candidate === checkedAt) continue
    checkedAt = candidate
    if (candidate >= listedFrom && isListedKey(chars.slice(candidate))) break
    if (part.tail || candidate <= part.until || part.whole.has(candidate)) return true
  }
  return false
}

// Heuristic quote/backslash context for the tested direct, command-string
// and logged spellings, including `sed 's/password\=.*/x/'` and selected
// joined arguments (see joinedArguments). `word` is an apparent word start
// (-1: unknown). Raw spaces reset it even inside earlier quoted values:
// known gap, this can misclassify an active backslash as literal and suppress
// redaction of a later list entry. This is not general shell quote analysis.
function backslashReader(text: string, getStates: () => Uint8Array, rerun: () => Uint8Array): { word: (at: number) => number; literal: (at: number) => boolean; ownLiteral: (at: number) => boolean; quote: (at: number) => number } {
  const words = new Int32Array(text.length + 1)
  let word = 0
  let uncertain = false
  for (let i = 0; i <= text.length; i += 1) {
    if (i > 0 && (isSpace(text[i - 1] as string) || isOperator(text[i - 1] as string))) {
      word = i
      uncertain = false
    }
    // An apparent word can span encoded whitespace, or a continued name.
    // Neither reading can prove that its quotes keep a backslash literal.
    if (text[i] === '\\' && /[tnrf]/.test(text[i + 1] ?? '')) uncertain = true
    words[i] = uncertain ? -1 : word
  }
  // Cache every traversed position, including those before the most recent
  // query: checking an escaped equals can precede checking its name.
  const cache = new Uint8Array(text.length + 1)
  let lastWord = -1
  let pos = 0
  let inner = NONE
  let escaped = false
  const start = (at: number): number => (words[at] as number) < 0 || at - (words[at] as number) > 4096 ? -1 : (words[at] as number)
  const quote = (k: number): number => {
    const from = start(k)
    if (from === -1 || rerun()[k] === 1) return NONE
    const states = getStates()
    const q = states[from]
    if (q !== SINGLE && q !== DOUBLE) return states[k] as number
    if (cache[k] !== 0) return (cache[k] as number) - 1
    if (lastWord !== from) {
      lastWord = from
      pos = from
      inner = NONE
      escaped = false
    }
    for (; pos < k; ) {
      let c = text[pos] as string
      const before = pos
      const apostrophe = outerApostrophe(text, pos)
      if (apostrophe > 0) {
        c = "'"
        pos += apostrophe
      } else if (q === SINGLE && c === '\\' && /["'\\]/.test(text[pos + 1] ?? '')) {
        // Escapes between encoded apostrophes require another shell layer.
        // One-layer quote analysis cannot prove a backslash is literal there.
        inner = UNKNOWN
        pos += 1
      } else if (q === DOUBLE && c === '\\' && pos + 1 < text.length && /["\\$`]/.test(text[pos + 1] as string)) {
        c = text[pos + 1] as string
        pos += 2
        if (c === '\\' && /["'\\]/.test(text[pos] ?? '')) inner = UNKNOWN
      } else {
        pos += 1
      }
      cache.fill(inner + 1, before, pos)
      if (inner === UNKNOWN) continue
      if (escaped) escaped = false
      else if (inner === SINGLE) inner = c === "'" ? NONE : SINGLE
      else if (c === '\\') escaped = true
      else if (inner === DOUBLE) inner = c === '"' ? NONE : DOUBLE
      else if (c === "'" || c === '"') inner = c === "'" ? SINGLE : DOUBLE
    }
    cache[pos] = inner + 1
    return (cache[k] as number) - 1
  }
  return { word: start, quote, ownLiteral: k => {
    const from = start(k)
    if (from === -1 || (text[from] !== "'" && text[from] !== '"')) return false
    const states = getStates()
    return states[from] === NONE && (states[k] === SINGLE || states[k] === DOUBLE) && rerun()[k] !== 1
  }, literal: k => {
    const q = quote(k)
    return q === SINGLE || q === DOUBLE
  } }
}

// A redaction marker that is a whole shell word, with a quote put back after
// it: what the rules above left of a value.
const MARKER_WORD = /\[REDACTED(?::[a-z-]+)?\]["']?(?![^\s;&|<>)])/y

function redactListNames(text: string, stats: SanitizeStats): string {
  const firstEquals = text.indexOf('=')
  if (firstEquals === -1) return text
  // Without escapes or ANSI-C decoding, removing quotes exposes every
  // possible credential piece. Text with none needs no list-name fallback.
  if (!text.includes('\\') && !text.includes("$'") && !PIECE_IN_NAME.test(text.replace(/\$?["']/g, ''))) return text
  let states: Uint8Array | undefined
  let urls: number[] | undefined
  const names = new Map<string, boolean>()
  let rerun: Uint8Array | undefined
  // As in redactNamedValues.
  let lists: DelimitedLists | null | undefined
  const scanned = (): Uint8Array => (states ??= quoteStates(text))
  const joinedAt = (): Uint8Array => (rerun ??= joinedArguments(text))
  let backslashes: ReturnType<typeof backslashReader> | undefined
  const reader = (): ReturnType<typeof backslashReader> => (backslashes ??= backslashReader(text, scanned, joinedAt))
  let out = ''
  let copied = 0
  // Read command boundaries lazily, once. Plain list names need no quote
  // context, and native searches skip long ordinary spans between matches.
  const boundaries = /[=;\n|&]|\\[nr]/g
  let boundary: RegExpExecArray | null | undefined
  let localEquals = text.length
  const commandEquals = (end: number): number => {
    if (boundary === undefined) boundary = boundaries.exec(text)
    while (boundary !== null && boundary.index < end) {
      const at = boundary.index
      const c = boundary[0]
      if (c === '=') localEquals = Math.min(localEquals, at)
      else if (!(c === '\n' && (text[at - 1] === '\\' || (text[at - 1] === '\r' && text[at - 2] === '\\')))) localEquals = text.length
      boundary = boundaries.exec(text)
    }
    return localEquals
  }
  for (let e = firstEquals; e !== -1; e = text.indexOf('=', e + 1)) {
    const start = e + 1
    if (start >= text.length) continue
    // A marker at the very end leaves no unscanned suffix. Avoid decoding
    // a long name again after an earlier pass already removed its value.
    if (text[start] === '[' && /^\[REDACTED(?::[a-z-]+)?\][\"']?$/.test(text.slice(start))) continue
    // A word's own quote can make every escaped equals literal. Reject it
    // before repeatedly decoding the names in a substitution pattern.
    if (text[e - 1] === '\\' && reader().ownLiteral(e - 1)) continue
    if (lists === undefined) lists = delimitedLists(text)
    const delimiter = delimiterAt(lists, e)
    const name = listName(text, e, commandEquals, delimiter, names, reader)
    if (!name.credential) continue
    // A literal backslash before `=` cannot introduce an assignment.
    if (text[e - 1] === '\\' && reader().literal(e - 1)) continue
    const before = text[name.from - 1] ?? ''
    // A name written whole after `?`, `&`, `#` or `;` is redactNamedValues'
    // (a query string's value ends at its `&` there), and a URL's query or
    // fragment parameter redactUrl's.
    if (/[?&#;]/.test(before) && ((delimiter === '' && /^-{0,2}[A-Za-z_][A-Za-z0-9_.-]{0,99}$/.test(text.slice(name.from, e))) || inUrl((urls ??= urlSpans(text)), e))) continue
    // Build the whole-text word cache only when escape analysis needs it.
    // A single word-start lookup is bounded and does not allocate that cache.
    const from = backslashes === undefined ? wordStart(text, name.from) : backslashes.word(name.from)
    // An escaped `=` is the shell's `=` (`…::DB_PASSWORD\=…`); literal
    // backslashes were excluded before reading the name.
    const read = readValue(text, start, from, scanned())
    // A value that is one redaction marker is done; one with more of its
    // word after the marker (`[REDACTED:github-token]:x`) is not.
    if (read === null ? ((MARKER_WORD.lastIndex = start), MARKER_WORD.test(text)) : wholeMarker(text.slice(start, read.end), read.close)) continue
    const end = read === null ? text.length : valueStop(text, start, read.end, /^\$?["']*-/.test(text.slice(from, start)), read.spanEnd)
    const close = read?.close ?? ''
    const value = text.slice(start, end)
    if (value.trim() === '' || wholeMarker(value, close)) continue
    // A marker an earlier rule left after a blank (`NAME= [REDACTED]`) is a
    // value redacted and counted already: the blank goes, as it always has,
    // with nothing counted again.
    if (!wholeMarker(value.trimStart(), close)) {
      noteCut(text, start, read === null ? fallbackValueEnd(text, start, end, outerQuoteOf(scanned()[start])) : read.end, end, stats)
      stats.redactions += 1
    }
    out += text.slice(copied, start) + MARKER + close
    copied = end
    if (end >= text.length) break
    e = end - 1
  }
  return copied === 0 ? text : out + text.slice(copied)
}

const WHOLE_OR_TICKED_MARKER = /^`?\[REDACTED(?::[a-z-]+)?\]`?$/

function isMarkerLike(raw: string): boolean {
  const trimmed = raw.trim()
  return trimmed === '' || WHOLE_OR_TICKED_MARKER.test(trimmed)
}

type LineRow = { line: string; eol: string }

function splitLinesPreserveEol(text: string): LineRow[] {
  const rows: LineRow[] = []
  let start = 0
  for (let i = 0; i < text.length; i += 1) {
    if (text.charCodeAt(i) === 10) {
      const cr = i > start && text.charCodeAt(i - 1) === 13
      rows.push({ line: text.slice(start, cr ? i - 1 : i), eol: cr ? '\r\n' : '\n' })
      start = i + 1
    }
  }
  rows.push({ line: text.slice(start), eol: '' })
  return rows
}

function indentCols(line: string): number {
  let c = 0
  while (c < line.length && (line[c] === ' ' || line[c] === '\t')) c += 1
  return c
}

const isLabelTrimChar = (raw: string, i: number): boolean => {
  const c = raw.charCodeAt(i)
  return (
    c === 42 || // *
    c === 34 || // "
    c === 39 || // '
    c === 96 || // `
    c === 95 || // _
    c === 32 || // space
    (c >= 9 && c <= 13) ||
    (c >= 127 && /\s/.test(raw[i] as string))
  )
}

function cleanLabelKey(raw: string): string {
  let start = 0
  let end = raw.length
  while (start < end && isLabelTrimChar(raw, start)) start += 1
  while (end > start && isLabelTrimChar(raw, end - 1)) end -= 1
  return raw.slice(start, end)
}

function stripTrailingParenQualifier(s: string, withSep: boolean): string {
  let end = s.length
  if (withSep) {
    while (end > 0 && (s.charCodeAt(end - 1) === 32 || s.charCodeAt(end - 1) === 9)) end -= 1
    if (end === 0) return s
    const sep = s.charCodeAt(end - 1)
    if (sep !== 58 && sep !== 61) return s
    end -= 1
    while (end > 0 && (s.charCodeAt(end - 1) === 32 || s.charCodeAt(end - 1) === 9)) end -= 1
  }
  if (end === 0 || s.charCodeAt(end - 1) !== 41) return s
  const closeIdx = end - 1
  const minOpen = Math.max(0, closeIdx - 41)
  let openIdx = -1
  for (let i = closeIdx - 1; i >= minOpen; i -= 1) {
    const c = s.charCodeAt(i)
    if (c === 10 || c === 41) break
    if (c === 40) openIdx = i
  }
  if (openIdx === -1) return s
  const innerLen = closeIdx - openIdx - 1
  if (innerLen < 1 || innerLen > 40) return s
  let labelEnd = openIdx
  while (labelEnd > 0 && (s.charCodeAt(labelEnd - 1) === 32 || s.charCodeAt(labelEnd - 1) === 9)) labelEnd -= 1
  return s.slice(0, labelEnd)
}

const YAML_BLOCK_HEADER = /^([ \t]*(?:-[ \t]+)?)([A-Za-z0-9_.*"`'-]+(?:[ \t]*\([^)\n]{1,40}\))?)[ \t]*:[ \t]*(?:[|>][+-]?\d*|\d*[|>][+-]?)[ \t]*(?:#[^\n]*)?$/
const K8S_NAME_LINE = /^([ \t]*-[ \t]+)name[ \t]*:[ \t]*(?:"([^"\n]+)"|'([^'\n]+)'|([^\s#]+))[ \t]*(?:#.*)?$/
const K8S_VALUE_LINE = /^([ \t]+)value[ \t]*:[ \t]*(.*)$/

// Redacts K8s/Actions `- name: SENSITIVE\n  value: ...` pairs and multi-line
// YAML block scalars (`|`, `>`, `|-`, `>-`, `|+`, `>+`) under sensitive keys.
function redactK8sAndYamlBlocks(text: string, stats: SanitizeStats): string {
  if (!text.includes(':') || (!text.includes('|') && !text.includes('>') && !text.includes('name:'))) return text
  const rows = splitLinesPreserveEol(text)
  const out: LineRow[] = []
  let changed = false
  let i = 0
  while (i < rows.length) {
    const row = rows[i]!
    const line = row.line
    // 1. Kubernetes / GitHub Actions env `- name: <SENSITIVE>\n  value: <VAL>`
    const nm = K8S_NAME_LINE.exec(line)
    if (nm !== null && i + 1 < rows.length) {
      const keyName = (nm[2] ?? nm[3] ?? nm[4] ?? '').trim()
      if (keyName !== '' && isSensitiveKey(keyName)) {
        const prefixLen = (nm[1] as string).length
        const nextRow = rows[i + 1]!
        const vm = K8S_VALUE_LINE.exec(nextRow.line)
        if (vm !== null && (vm[1] as string).length >= prefixLen) {
          const valRaw = (vm[2] as string).trim()
          if (/^(?:[|>][+-]?\d*|\d*[|>][+-]?)(?:[ \t]*(?:#.*)?)?$/.test(valRaw)) {
            const keyCol = (vm[1] as string).length
            let lastBody = -1
            for (let j = i + 2; j < rows.length; j += 1) {
              const cand = rows[j]!.line
              if (cand.trim() === '') continue
              if (indentCols(cand) > keyCol) lastBody = j
              else break
            }
            if (lastBody !== -1) {
              out.push(row, { line: `${vm[1]}value: ${MARKER}`, eol: nextRow.eol })
              stats.redactions += 1
              changed = true
              i = lastBody + 1
              continue
            }
          } else if (valRaw !== '' && !isMarkerLike(valRaw) && !valRaw.startsWith('#')) {
            out.push(row, { line: `${vm[1]}value: ${MARKER}`, eol: nextRow.eol })
            stats.redactions += 1
            changed = true
            i += 2
            continue
          }
        }
      }
    }

    // 2. General YAML block scalar (`password: |\n  secret...`)
    const ym = YAML_BLOCK_HEADER.exec(line)
    if (ym !== null) {
      const lead = ym[1] as string
      const rawKey = stripTrailingParenQualifier(ym[2] as string, false)
      const cleaned = cleanLabelKey(rawKey)
      if (cleaned !== '' && (isSensitiveKey(cleaned) || isSensitiveKey(cleaned.replace(/\s+/g, '_')))) {
        const keyCol = lead.length
        let lastBody = -1
        for (let j = i + 1; j < rows.length; j += 1) {
          const cand = rows[j]!.line
          if (cand.trim() === '') continue
          if (indentCols(cand) > keyCol) lastBody = j
          else break
        }
        if (lastBody !== -1) {
          out.push({ line: `${lead}${ym[2]}: ${MARKER}`, eol: row.eol })
          stats.redactions += 1
          changed = true
          i = lastBody + 1
          continue
        }
      }
    }
    out.push(row)
    i += 1
  }
  if (!changed) return text
  return out.map(r => r.line + r.eol).join('')
}

const MD_WRAP_CLOSED = /^([ \t]*(?:(?:[-*+]|\d+[.)])[ \t]+)?)(\*\*|__|[*_`])([A-Za-z_][A-Za-z0-9_.\t -]{0,80})\2([ \t]*(?:\([^)\n]{1,40}\)[ \t]*)?[:=][ \t]*)(.+)$/
const MD_WRAP_AROUND_SEP = /^([ \t]*(?:(?:[-*+]|\d+[.)])[ \t]+)?)(\*\*|__|[*_])([A-Za-z_][A-Za-z0-9_.\t -]{0,80})([:=])\2([ \t]*)(.+)$/
const MD_PAREN_LABEL = /^([ \t]*(?:(?:[-*+]|\d+[.)])[ \t]+)?)([A-Za-z_](?:[A-Za-z0-9_.\t -]{0,59}[A-Za-z0-9_.-])?[ \t]*\([^)\n]{1,40}\)[ \t]*[:=][ \t]*)(.+)$/
const MD_SEPARATOR_ROW = /^\|(?:\s*:?-{2,}:?\s*\|)+\s*$/
const GENERIC_KEY_COL_HEADERS = new Set([
  'key', 'keys', 'name', 'names', 'field', 'fields', 'setting', 'settings',
  'property', 'properties', 'variable', 'variables', 'var', 'vars', 'env',
  'param', 'params', 'parameter', 'parameters', 'option', 'options',
  'flag', 'flags', 'header', 'headers', 'item', 'items', 'attribute', 'attributes',
  'secret name', 'key name', 'token name', 'credential name', 'config key',
])

const isSensitiveLabel = (raw: string): boolean => {
  const cleaned = cleanLabelKey(raw)
  if (cleaned === '' || cleaned.length > 80) return false
  return isSensitiveKey(cleaned) || (cleaned.includes(' ') && isSensitiveKey(cleaned.replace(/[ \t]+/g, '_')))
}

const isMdWrapperChar = (c: number): boolean =>
  c === 42 || c === 34 || c === 39 || c === 95 || c === 96

function stripMarkdownWrappers(s: string): string {
  let start = 0
  let end = s.length
  while (start < end && isMdWrapperChar(s.charCodeAt(start))) start += 1
  while (end > start && isMdWrapperChar(s.charCodeAt(end - 1))) end -= 1
  return s.slice(start, end)
}

const isSkippedCellOrVal = (raw: string): boolean => {
  const t = raw.trim()
  if (t === '' || /^:?-{2,}:?$/.test(t) || /^(?:n\/a|none|null|unset|empty|tbd)$/i.test(t)) return true
  return isMarkerLike(t) || isMarkerLike(stripMarkdownWrappers(t))
}

function redactCellText(rawCell: string, stats: SanitizeStats): string {
  let leadEnd = 0
  while (leadEnd < rawCell.length) {
    const c = rawCell.charCodeAt(leadEnd)
    if (c !== 32 && c !== 9) break
    leadEnd += 1
  }
  let trailStart = rawCell.length
  while (trailStart > leadEnd) {
    const c = rawCell.charCodeAt(trailStart - 1)
    if (c !== 32 && c !== 9) break
    trailStart -= 1
  }
  const lead = rawCell.slice(0, leadEnd)
  const trail = rawCell.slice(trailStart)
  const inner = rawCell.slice(leadEnd, trailStart)
  if (isSkippedCellOrVal(inner)) return rawCell
  stats.redactions += 1
  if (inner.startsWith('`') && inner.endsWith('`') && inner.length >= 2) return `${lead}\`${MARKER}\`${trail}`
  return `${lead}${MARKER}${trail}`
}

function splitPipeRow(line: string): { prefix: string; cells: string[]; suffix: string } | null {
  const firstPipe = line.indexOf('|')
  const lastPipe = line.lastIndexOf('|')
  if (firstPipe === -1 || lastPipe <= firstPipe) return null
  if (line.slice(0, firstPipe).trim() !== '' || line.slice(lastPipe + 1).trim() !== '') return null
  const cells: string[] = []
  let cur = ''
  for (let i = firstPipe + 1; i < lastPipe; i += 1) {
    if (line[i] === '\\' && i + 1 < lastPipe && line[i + 1] === '|') {
      cur += '\\|'
      i += 1
      continue
    }
    if (line[i] === '|') {
      cells.push(cur)
      cur = ''
    } else {
      cur += line[i]
    }
  }
  cells.push(cur)
  return { prefix: line.slice(0, firstPipe + 1), cells, suffix: line.slice(lastPipe) }
}

// Redacts formatted Markdown labels (`**Password**: v`, `` `password`: `v` ``,
// `Password (staging): v`) and Markdown pipe tables (`| `DB_PASSWORD` | `v` |`).
function redactMarkdownLabelsAndTables(text: string, stats: SanitizeStats): string {
  const hasColonEq = text.includes(':') || text.includes('=')
  const hasPipe = text.includes('|')
  if (!hasColonEq && !hasPipe) return text
  const rows = splitLinesPreserveEol(text)
  let changed = false

  // 1. Formatted labels & parenthetical qualifiers.
  if (hasColonEq) {
    for (let i = 0; i < rows.length; i += 1) {
      const line = rows[i]!.line
      let matched = false
      let prefix = ''
      let val = ''
      const m1 = MD_WRAP_CLOSED.exec(line)
      if (m1 !== null && isSensitiveLabel(m1[3] as string)) {
        prefix = `${m1[1]}${m1[2]}${m1[3]}${m1[2]}${m1[4]}`
        val = m1[5] as string
        matched = true
      } else {
        const m2 = MD_WRAP_AROUND_SEP.exec(line)
        if (m2 !== null && isSensitiveLabel(m2[3] as string)) {
          prefix = `${m2[1]}${m2[2]}${m2[3]}${m2[4]}${m2[2]}${m2[5]}`
          val = m2[6] as string
          matched = true
        } else {
          const m3 = MD_PAREN_LABEL.exec(line)
          if (m3 !== null) {
            const labelNoParen = stripTrailingParenQualifier(m3[2] as string, true)
            if (isSensitiveLabel(labelNoParen)) {
              prefix = `${m3[1]}${m3[2]}`
              val = m3[3] as string
              matched = true
            }
          }
        }
      }
      if (!matched) continue
      const trimmedVal = val.trim()
      if (isSkippedCellOrVal(trimmedVal)) continue
      const tickWrapped = trimmedVal.startsWith('`') && trimmedVal.endsWith('`') && trimmedVal.length >= 2 && !trimmedVal.slice(1, -1).includes('`')
      rows[i]!.line = prefix + (tickWrapped ? `\`${MARKER}\`` : MARKER)
      stats.redactions += 1
      changed = true
    }
  }

  // 2. Markdown pipe tables (`| ... |`).
  if (hasPipe) {
    let i = 0
    while (i < rows.length) {
      const parsed0 = splitPipeRow(rows[i]!.line)
      if (parsed0 === null || parsed0.cells.length < 2) {
        i += 1
        continue
      }
      let end = i + 1
      while (end < rows.length) {
        const p = splitPipeRow(rows[end]!.line)
        if (p === null || p.cells.length < 2) break
        end += 1
      }
      const hasSep = i + 1 < end && MD_SEPARATOR_ROW.test(rows[i + 1]!.line.trim())
      if (hasSep) {
        const hdrCells = parsed0.cells
        const sensitiveCols = new Set<number>()
        const genericCols = new Set<number>()
        for (let c = 0; c < hdrCells.length; c += 1) {
          const clean = cleanLabelKey(hdrCells[c] as string)
          const lower = clean.toLowerCase()
          if (GENERIC_KEY_COL_HEADERS.has(lower)) genericCols.add(c)
          else if (isSensitiveLabel(clean)) sensitiveCols.add(c)
        }
        for (let r = i + 2; r < end; r += 1) {
          if (MD_SEPARATOR_ROW.test(rows[r]!.line.trim())) continue
          const row = splitPipeRow(rows[r]!.line)
          if (row === null) continue
          let rowChanged = false
          for (let c = 0; c < row.cells.length; c += 1) {
            if (sensitiveCols.has(c)) {
              const nextCell = redactCellText(row.cells[c] as string, stats)
              if (nextCell !== row.cells[c]) {
                row.cells[c] = nextCell
                rowChanged = true
              }
            } else if (c + 1 < row.cells.length && (genericCols.has(c) || c === 0 || row.cells.length === 2)) {
              const keyClean = cleanLabelKey(row.cells[c] as string)
              if (keyClean !== '' && isSensitiveLabel(keyClean)) {
                const nextCell = redactCellText(row.cells[c + 1] as string, stats)
                if (nextCell !== row.cells[c + 1]) {
                  row.cells[c + 1] = nextCell
                  rowChanged = true
                }
              }
            }
          }
          if (rowChanged) {
            rows[r]!.line = `${row.prefix}${row.cells.join('|')}${row.suffix}`
            changed = true
          }
        }
      } else {
        for (let r = i; r < end; r += 1) {
          if (MD_SEPARATOR_ROW.test(rows[r]!.line.trim())) continue
          const row = splitPipeRow(rows[r]!.line)
          if (row === null || row.cells.length < 2) continue
          let rowChanged = false
          for (let c = 0; c < row.cells.length - 1; c += 1) {
            const keyClean = cleanLabelKey(row.cells[c] as string)
            if (keyClean !== '' && isSensitiveLabel(keyClean)) {
              const nextCell = redactCellText(row.cells[c + 1] as string, stats)
              if (nextCell !== row.cells[c + 1]) {
                row.cells[c + 1] = nextCell
                rowChanged = true
              }
            }
          }
          if (rowChanged) {
            rows[r]!.line = `${row.prefix}${row.cells.join('|')}${row.suffix}`
            changed = true
          }
        }
      }
      i = end
    }
  }

  if (!changed) return text
  return rows.map(r => r.line + r.eol).join('')
}

const ENV_INDEX_RE = /\b((?:os\.environ(?:\.setdefault)?|process\.env|ENV|System\.getenv|setenv))([ \t]*)(?:\[[ \t]*(['"])([^'"\n]{1,100})\3[ \t]*\][ \t]*=[ \t]*|\([ \t]*(['"])([^'"\n]{1,100})\5[ \t]*,[ \t]*)("(?:\\.|[^"\n\\])*"|'(?:\\.|[^'\n\\])*'|`[^`\n]*`|[^;\n,)]+)/g
const SETTER_ONE_ARG_RE = /\b((?:set|with)_?([A-Za-z][A-Za-z0-9_]{1,80}))([ \t]*\([ \t]*)("(?:\\.|[^"\n\\])*"|'(?:\\.|[^'\n\\])*')/g
const SETTER_TWO_ARG_RE = /\b((?:set|with|put|add|setenv)_?[A-Za-z0-9_]{0,40})([ \t]*\([ \t]*)(['"])([^'"\n]{1,80})\3([ \t]*,[ \t]*)("(?:\\.|[^"\n\\])*"|'(?:\\.|[^'\n\\])*')/g
const XML_TAG_RE = /<([A-Za-z_][A-Za-z0-9_.:-]{0,80})>([^<\r\n]{1,4096})<\/\1>/g
const SQL_IDENT_RE = /\b(IDENTIFIED(?:\s+WITH\s+[A-Za-z0-9_]+)?\s+BY|WITH(?:\s+(?:ENCRYPTED|UNENCRYPTED))?\s+PASSWORD)([ \t]+)("(?:""|\\[^\r\n]|[^"\\\r\n])*(?:"|\\?(?=\r|\n|$))|'(?:''|\\[^\r\n]|[^'\\\r\n])*(?:'|\\?(?=\r|\n|$))|[^\s;]+)/gi
const DOCKER_ENV_RE = /^([ \t]*(?:ENV|ARG)[ \t]+)([A-Za-z_][A-Za-z0-9_]{0,80})([ \t]+)(?![=\s])([^\r\n]+)$/gm
const NETRC_LINE_HDR = /\b(?:machine[ \t]+\S+|default)\b/
const NETRC_PW_RE = /\b(password)([ \t]+)(\S+)/g

function redactQuotedOrBare(raw: string, stats: SanitizeStats): string {
  const trimmed = raw.trim()
  if (trimmed === '' || trimmed === '""' || trimmed === "''" || isMarkerLike(trimmed)) return raw
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith('`') && trimmed.endsWith('`'))) {
    const q = trimmed[0] as string
    const inner = trimmed.slice(1, -1)
    if (isMarkerLike(inner)) return raw
    stats.redactions += 1
    return `${q}${MARKER}${q}`
  }
  stats.redactions += 1
  return MARKER
}

// Redacts common code & config secret forms: env dictionary lookups, setter
// methods, XML config tags, SQL `IDENTIFIED BY`, `.netrc`, and Dockerfile `ENV`.
function redactCodeAndConfigForms(text: string, stats: SanitizeStats): string {
  let out = text

  if (out.includes('os.environ') || out.includes('process.env') || out.includes('ENV[') || out.includes('setenv')) {
    ENV_INDEX_RE.lastIndex = 0
    out = out.replace(ENV_INDEX_RE, (whole: string, obj: string, sp: string, q1: string, k1: string, q2: string, k2: string, val: string) => {
      const key = k1 ?? k2
      if (!isSensitiveKey(key)) return whole
      const rep = redactQuotedOrBare(val, stats)
      if (rep === val) return whole
      return q1 !== undefined ? `${obj}${sp}[${q1}${k1}${q1}] = ${rep}` : `${obj}${sp}(${q2}${k2}${q2}, ${rep}`
    })
  }

  if (out.includes('(') && (/set|with|put|add/i.test(out))) {
    SETTER_ONE_ARG_RE.lastIndex = 0
    out = out.replace(SETTER_ONE_ARG_RE, (whole: string, fn: string, suffix: string, open: string, arg: string) => {
      const cleanSuffix = suffix.replace(/^_+/, '')
      if (cleanSuffix === '' || !isSensitiveKey(cleanSuffix)) return whole
      const rep = redactQuotedOrBare(arg, stats)
      return rep === arg ? whole : `${fn}${open}${rep}`
    })
    SETTER_TWO_ARG_RE.lastIndex = 0
    out = out.replace(SETTER_TWO_ARG_RE, (whole: string, fn: string, open: string, q: string, key: string, comma: string, val: string) => {
      if (!isSensitiveKey(key)) return whole
      const rep = redactQuotedOrBare(val, stats)
      return rep === val ? whole : `${fn}${open}${q}${key}${q}${comma}${rep}`
    })
  }

  if (out.includes('<') && out.includes('</')) {
    XML_TAG_RE.lastIndex = 0
    out = out.replace(XML_TAG_RE, (whole: string, tag: string, inner: string) => {
      const colon = tag.indexOf(':')
      const local = colon === -1 ? tag : tag.slice(colon + 1)
      if (!isSensitiveKey(local)) return whole
      const trimmed = inner.trim()
      if (trimmed === '' || isMarkerLike(trimmed) || /^\$\{[^}]+\}$/.test(trimmed)) return whole
      stats.redactions += 1
      return `<${tag}>${MARKER}</${tag}>`
    })
  }

  if (/identified|password/i.test(out) && /identified\s+|with\s+(?:encrypted\s+|unencrypted\s+)?password\s+/i.test(out)) {
    SQL_IDENT_RE.lastIndex = 0
    out = out.replace(SQL_IDENT_RE, (whole: string, clause: string, sp: string, val: string) => {
      if (/^null$/i.test(val)) return whole
      const rep = redactQuotedOrBare(val, stats)
      return rep === val ? whole : `${clause}${sp}${rep}`
    })
  }

  if (out.includes('password') && (out.includes('machine') || out.includes('default'))) {
    const rows = splitLinesPreserveEol(out)
    let stanzaLinesLeft = 0
    let changed = false
    for (let i = 0; i < rows.length; i += 1) {
      const line = rows[i]!.line
      const hasHdr = NETRC_LINE_HDR.test(line)
      if (hasHdr) stanzaLinesLeft = 6
      else if (line.trim() === '' || /^#/.test(line.trim())) stanzaLinesLeft = 0
      if (stanzaLinesLeft > 0) {
        const isValidStanzaLine = hasHdr || /^[ \t]*(?:login|account|macdef|password)[ \t]+/.test(line)
        if (isValidStanzaLine) {
          NETRC_PW_RE.lastIndex = 0
          const updated = line.replace(NETRC_PW_RE, (whole: string, kw: string, sp: string, secret: string) => {
            if (isMarkerLike(secret)) return whole
            stats.redactions += 1
            return `${kw}${sp}${MARKER}`
          })
          if (updated !== line) {
            rows[i]!.line = updated
            changed = true
          }
          stanzaLinesLeft -= 1
        } else {
          stanzaLinesLeft = 0
        }
      }
    }
    if (changed) out = rows.map(r => r.line + r.eol).join('')
  }

  if (/^[ \t]*(?:ENV|ARG)[ \t]+/m.test(out)) {
    DOCKER_ENV_RE.lastIndex = 0
    out = out.replace(DOCKER_ENV_RE, (whole: string, kw: string, key: string, sp: string, val: string) => {
      if (!isSensitiveKey(key)) return whole
      const trimmed = val.trim()
      if (trimmed === '' || isMarkerLike(trimmed) || /^\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[^}]+\})$/.test(trimmed)) return whole
      stats.redactions += 1
      return `${kw}${key}${sp}${MARKER}`
    })
  }

  return out
}

const SHELL_WRAPPERS = new Set(['sudo', 'env', 'command', 'nohup', 'time', 'timeout', 'gtimeout', 'builtin', 'exec'])
const SUDO_ARG_OPTS = new Set(['-u', '--user', '-g', '--group', '-h', '--host', '-p', '--prompt', '-r', '--role', '-t', '--type', '-U', '--other-user', '-C', '--close-from', '-D', '--chdir', '-R', '--chroot', '-T', '--command-timeout'])
const ENV_ARG_OPTS = new Set(['-u', '--unset', '-C', '--chdir', '-S', '--split-string'])
const ATTACHED_P_CMDS = new Set(['mysql', 'mysqldump', 'mariadb', 'mariadb-dump', 'mongosh', 'mongo', 'sshpass', 'twine', '7z', '7za', '7zr', 'psql', 'redis-cli', 'valkey-cli'])
const ATTACHED_P_PORT_CMDS = new Set(['psql', 'redis-cli', 'valkey-cli'])
const SPACED_P_PASS_CMDS = new Set(['mysql', 'mysqldump', 'mariadb', 'mariadb-dump', 'mongosh', 'mongo', 'sshpass', 'twine', 'htpasswd', 'htdigest', 'smbclient', 'rpcclient', 'net', 'mosquitto_passwd', 'kadmin'])
const LOGIN_SUBCMD_CMDS = new Set(['docker', 'podman', 'nerdctl', 'skopeo', 'oras', 'crane', 'helm', 'npm', 'yarn', 'pnpm', 'poetry', 'pipenv'])
const SPACED_P_EXCLUDED = new Set(['claude', 'mkdir', 'cp', 'mv', 'rm', 'ln', 'install', 'tar', 'git', 'ssh', 'scp', 'rsync', 'psql', 'redis-cli', 'valkey-cli', 'grep', 'rg', 'sed', 'awk', 'python', 'python3', 'node', 'sh', 'bash', 'zsh'])
const POSTGRES_W_CMDS = new Set(['psql', 'pg_dump', 'pg_dumpall', 'pg_restore', 'createdb', 'dropdb', 'createuser', 'dropuser', 'clusterdb', 'reindexdb', 'vacuumdb'])
const REDIS_A_CMDS = new Set(['redis-cli', 'valkey-cli'])
const P_UPPER_PASS_CMDS = new Set(['sqlcmd', 'zip', 'unzip', '7z', '7za', '7zr'])
const SECRET_VALUE_CMDS = new Set(['aws', 'gcloud', 'vault', 'az', 'gh', 'doppler', 'infisical'])
const GIT_NONSECRET_KEYS = new Set(['credential.helper', 'credential.username', 'credential.usehttppath', 'credential.modalprompt', 'user.signingkey'])
const ENV_ASSIGN_TOK = /^[A-Za-z_][A-Za-z0-9_]*=/
const SHELL_VAR_LITERAL = /^\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[A-Za-z_][A-Za-z0-9_]*\})$/

type CliToken = { start: number; end: number; raw: string; unquoted: string; quote: string }
type CliEdit = { start: number; end: number; replacement: string }

function unquoteCliToken(raw: string): { unquoted: string; quote: string } {
  if (raw.length >= 2 && ((raw[0] === '"' && raw[raw.length - 1] === '"') || (raw[0] === "'" && raw[raw.length - 1] === "'"))) {
    return { unquoted: raw.slice(1, -1), quote: raw[0] as string }
  }
  return { unquoted: raw, quote: '' }
}

const isCliPathOrPort = (tok: CliToken): boolean => {
  const s = tok.unquoted
  if (s === '' || s.startsWith('-')) return true
  if (isMarkerLike(s) || SHELL_VAR_LITERAL.test(s)) return true
  if (/^\d+(?::\d+)?$/.test(s)) return true
  if (tok.quote === '' && (/^[./~]/.test(s) || s.includes('/'))) return true
  return false
}

const cmdBaseName = (raw: string): string => {
  const slash = raw.lastIndexOf('/')
  return (slash === -1 ? raw : raw.slice(slash + 1)).toLowerCase()
}

function scanCliSegment(text: string, tokens: CliToken[], edits: CliEdit[], stats: SanitizeStats, depth: number): void {
  if (tokens.length === 0) return
  let idx = 0
  while (idx < tokens.length) {
    const t = tokens[idx]!.unquoted
    if (ENV_ASSIGN_TOK.test(t)) {
      idx += 1
      continue
    }
    const base = cmdBaseName(t)
    if (SHELL_WRAPPERS.has(base)) {
      idx += 1
      if (base === 'timeout' || base === 'gtimeout') {
        while (idx < tokens.length && tokens[idx]!.unquoted.startsWith('-')) {
          const opt = tokens[idx]!.unquoted
          idx += 1
          if (opt === '--') break
          if ((opt === '-k' || opt === '--kill-after' || opt === '-s' || opt === '--signal') && idx < tokens.length) {
            idx += 1
          }
        }
        if (idx < tokens.length && /^(?:\d+(?:\.\d+)?|\.\d+)[smhd]?$/i.test(tokens[idx]!.unquoted)) idx += 1
      } else if (base === 'env' || base === 'sudo' || base === 'command' || base === 'time') {
        while (idx < tokens.length && (tokens[idx]!.unquoted.startsWith('-') || ENV_ASSIGN_TOK.test(tokens[idx]!.unquoted))) {
          const opt = tokens[idx]!.unquoted
          idx += 1
          if (opt === '--') break
          if (base === 'sudo' && SUDO_ARG_OPTS.has(opt) && idx < tokens.length) idx += 1
          else if (base === 'env' && ENV_ARG_OPTS.has(opt) && idx < tokens.length) idx += 1
        }
      }
      continue
    }
    break
  }
  if (idx >= tokens.length) return

  const cmd = cmdBaseName(tokens[idx]!.unquoted)
  if ((cmd === 'sh' || cmd === 'bash' || cmd === 'zsh' || cmd === 'dash' || cmd === 'ksh') && depth < 3) {
    for (let j = idx + 1; j < tokens.length - 1; j += 1) {
      const f = tokens[j]!.unquoted
      if (f === '-c' || f === '-lc') {
        const scriptTok = tokens[j + 1]!
        if (scriptTok.quote !== '' && !scriptTok.unquoted.includes('\\')) {
          const innerOut = redactCliFlagsAndPositionalImpl(scriptTok.unquoted, stats, depth + 1)
          if (innerOut !== scriptTok.unquoted) {
            edits.push({ start: scriptTok.start + 1, end: scriptTok.end - 1, replacement: innerOut })
          }
        }
        return
      }
    }
  }

  let hasLoginSubcmd = false
  let sawUserFlag = false
  let hasSecretSubcmd = false
  for (let j = idx + 1; j < tokens.length; j += 1) {
    const u = tokens[j]!.unquoted
    const low = u.toLowerCase()
    if (low === 'login' && j === idx + 1) hasLoginSubcmd = true
    if (u === '-u' || u === '--user' || u === '--proxy-user' || u.startsWith('--user=') || u.startsWith('--proxy-user=')) sawUserFlag = true
    if (j <= idx + 2 && (low === 'secretsmanager' || low === 'ssm' || low === 'secrets' || low === 'secret' || low === 'kv' || low === 'keyvault')) {
      hasSecretSubcmd = true
    }
  }

  // `aws configure set [--profile P] <sensitive_key> <val>`
  if (cmd === 'aws' && tokens[idx + 1]?.unquoted === 'configure' && tokens[idx + 2]?.unquoted === 'set') {
    const positional: CliToken[] = []
    for (let j = idx + 3; j < tokens.length; j += 1) {
      const u = tokens[j]!.unquoted
      if (u === '--profile' || u === '-p') {
        j += 1
        continue
      }
      if (u.startsWith('-')) continue
      positional.push(tokens[j]!)
    }
    if (positional.length >= 2) {
      const keyTok = positional[0]!
      const valTok = positional[1]!
      if (isSensitiveKey(keyTok.unquoted) && !isMarkerLike(valTok.unquoted) && !SHELL_VAR_LITERAL.test(valTok.unquoted)) {
        edits.push({ start: valTok.start, end: valTok.end, replacement: MARKER })
        stats.redactions += 1
        return
      }
    }
  }

  // `git config ... <sensitive_key> <val>`
  if (cmd === 'git' && tokens[idx + 1]?.unquoted === 'config') {
    let readOnly = false
    const positional: CliToken[] = []
    for (let j = idx + 2; j < tokens.length; j += 1) {
      const u = tokens[j]!.unquoted
      if (u === '--get' || u === '--get-all' || u === '--get-regexp' || u === '--unset' || u === '--unset-all' || u === '--list' || u === '-l' || u === '-e' || u === '--edit') {
        readOnly = true
        break
      }
      if (u === '--file' || u === '-f' || u === '--blob' || u === '--type' || u === '-t' || u === '--default' || u === '--value') {
        j += 1
        continue
      }
      if (u.startsWith('-')) continue
      positional.push(tokens[j]!)
    }
    if (!readOnly && positional.length >= 2) {
      const keyTok = positional[0]!
      const valTok = positional[1]!
      const lowKey = keyTok.unquoted.toLowerCase()
      if (!GIT_NONSECRET_KEYS.has(lowKey) && isSensitiveKey(keyTok.unquoted) && !isMarkerLike(valTok.unquoted) && !SHELL_VAR_LITERAL.test(valTok.unquoted)) {
        edits.push({ start: valTok.start, end: valTok.end, replacement: MARKER })
        stats.redactions += 1
        return
      }
    }
  }

  for (let j = idx + 1; j < tokens.length; j += 1) {
    const tok = tokens[j]!
    const u = tok.unquoted
    // `-u user:pass` / `--user user:pass` / `--proxy-user=user:pass`
    if (u === '-u' || u === '--user' || u === '--proxy-user') {
      const nextTok = tokens[j + 1]
      if (nextTok !== undefined) {
        const inner = nextTok.unquoted
        const colon = inner.indexOf(':')
        if (colon > 0 && !inner.includes('://')) {
          const passPart = inner.slice(colon + 1)
          if (passPart !== '' && !isMarkerLike(passPart) && !SHELL_VAR_LITERAL.test(passPart)) {
            const offsetInRaw = nextTok.quote !== '' ? 1 + colon + 1 : colon + 1
            const endOffsetInRaw = nextTok.quote !== '' ? nextTok.raw.length - 1 : nextTok.raw.length
            edits.push({ start: nextTok.start + offsetInRaw, end: nextTok.start + endOffsetInRaw, replacement: MARKER })
            stats.redactions += 1
            j += 1
            continue
          }
        }
      }
      continue
    }
    if (u.startsWith('-u') && u.length > 2 && u[2] !== '=') {
      const val = u.slice(2)
      const colon = val.indexOf(':')
      if (colon > 0 && !val.includes('://')) {
        const passPart = val.slice(colon + 1)
        if (passPart !== '' && !isMarkerLike(passPart) && !SHELL_VAR_LITERAL.test(passPart)) {
          edits.push({ start: tok.start + 2 + colon + 1, end: tok.end, replacement: MARKER })
          stats.redactions += 1
          continue
        }
      }
    }
    const eqUser = /^(?:--(?:proxy-)?user|-u)=(.+)$/.exec(u)
    if (eqUser !== null) {
      const eqPos = u.indexOf('=')
      const val = eqUser[1] as string
      const colon = val.indexOf(':')
      if (colon > 0 && !val.includes('://')) {
        const passPart = val.slice(colon + 1)
        if (passPart !== '' && !isMarkerLike(passPart) && !SHELL_VAR_LITERAL.test(passPart)) {
          const offsetInRaw = (tok.quote !== '' ? 1 : 0) + eqPos + 1 + colon + 1
          const endOffset = tok.quote !== '' ? tok.end - 1 : tok.end
          edits.push({ start: tok.start + offsetInRaw, end: endOffset, replacement: MARKER })
          stats.redactions += 1
          continue
        }
      }
    }

    // Attached `-p<pass>` on DB / password CLIs
    if (ATTACHED_P_CMDS.has(cmd) && u.startsWith('-p') && u.length > 2) {
      const pass = u.slice(2)
      const skipPort = ATTACHED_P_PORT_CMDS.has(cmd) && /^\d{1,5}$/.test(pass)
      if (!skipPort && !isMarkerLike(pass) && !SHELL_VAR_LITERAL.test(pass)) {
        const qOff = tok.quote !== '' ? 1 : 0
        edits.push({ start: tok.start + qOff + 2, end: tok.end - qOff, replacement: MARKER })
        stats.redactions += 1
        continue
      }
    }

    // Attached `-P<pass>` on DB / archive CLIs (`sqlcmd`, `zip`, `unzip`, `7z*`)
    if (P_UPPER_PASS_CMDS.has(cmd) && u.startsWith('-P') && u.length > 2) {
      const pass = u.slice(2)
      if (!isMarkerLike(pass) && !SHELL_VAR_LITERAL.test(pass)) {
        const qOff = tok.quote !== '' ? 1 : 0
        edits.push({ start: tok.start + qOff + 2, end: tok.end - qOff, replacement: MARKER })
        stats.redactions += 1
        continue
      }
    }

    // Attached `-a<pass>` on Redis / Valkey CLIs
    if (REDIS_A_CMDS.has(cmd) && u.startsWith('-a') && u.length > 2) {
      const pass = u.slice(2)
      if (!isMarkerLike(pass) && !SHELL_VAR_LITERAL.test(pass)) {
        const qOff = tok.quote !== '' ? 1 : 0
        edits.push({ start: tok.start + qOff + 2, end: tok.end - qOff, replacement: MARKER })
        stats.redactions += 1
        continue
      }
    }

    // Spaced `-p <pass>`
    if (u === '-p' && !SPACED_P_EXCLUDED.has(cmd)) {
      const allow = SPACED_P_PASS_CMDS.has(cmd) || (LOGIN_SUBCMD_CMDS.has(cmd) && hasLoginSubcmd) || sawUserFlag
      const nextTok = tokens[j + 1]
      if (allow && nextTok !== undefined && !isCliPathOrPort(nextTok)) {
        edits.push({ start: nextTok.start, end: nextTok.end, replacement: MARKER })
        stats.redactions += 1
        j += 1
        continue
      }
    }

    // Scoped `-W <pass>`, `-a <pass>`, `-P <pass>`
    if ((u === '-W' && POSTGRES_W_CMDS.has(cmd)) || (u === '-a' && REDIS_A_CMDS.has(cmd)) || (u === '-P' && P_UPPER_PASS_CMDS.has(cmd))) {
      const nextTok = tokens[j + 1]
      if (nextTok !== undefined && !nextTok.unquoted.startsWith('-') && !isMarkerLike(nextTok.unquoted) && !SHELL_VAR_LITERAL.test(nextTok.unquoted)) {
        edits.push({ start: nextTok.start, end: nextTok.end, replacement: MARKER })
        stats.redactions += 1
        j += 1
        continue
      }
    }

    // `--value` / `--body` in secret management CLIs
    if (SECRET_VALUE_CMDS.has(cmd) && (hasSecretSubcmd || cmd === 'vault' || cmd === 'doppler' || cmd === 'infisical')) {
      if (u === '--value' || u === '--body') {
        const nextTok = tokens[j + 1]
        if (nextTok !== undefined && !nextTok.unquoted.startsWith('-') && !isMarkerLike(nextTok.unquoted) && !SHELL_VAR_LITERAL.test(nextTok.unquoted)) {
          edits.push({ start: nextTok.start, end: nextTok.end, replacement: MARKER })
          stats.redactions += 1
          j += 1
          continue
        }
      } else if (u.startsWith('--value=') || u.startsWith('--body=')) {
        const eqPos = u.indexOf('=')
        const val = u.slice(eqPos + 1)
        if (val !== '' && !isMarkerLike(val) && !SHELL_VAR_LITERAL.test(val)) {
          const qOff = tok.quote !== '' ? 1 : 0
          edits.push({ start: tok.start + qOff + eqPos + 1, end: tok.end - qOff, replacement: MARKER })
          stats.redactions += 1
          continue
        }
      } else if ((cmd === 'vault' || cmd === 'doppler' || cmd === 'infisical') && (u.startsWith('value=') || u.startsWith('data=') || u.startsWith('VALUE='))) {
        const eqPos = u.indexOf('=')
        const val = u.slice(eqPos + 1)
        if (val !== '' && !val.startsWith('@') && !isMarkerLike(val) && !SHELL_VAR_LITERAL.test(val)) {
          const qOff = tok.quote !== '' ? 1 : 0
          edits.push({ start: tok.start + qOff + eqPos + 1, end: tok.end - qOff, replacement: MARKER })
          stats.redactions += 1
          continue
        }
      }
    }
  }
}

function redactCliFlagsAndPositionalImpl(text: string, stats: SanitizeStats, depth: number): string {
  const edits: CliEdit[] = []
  let tokens: CliToken[] = []
  const n = text.length
  let i = 0
  const flush = (): void => {
    if (tokens.length > 0) {
      scanCliSegment(text, tokens, edits, stats, depth)
      tokens = []
    }
  }
  while (i < n) {
    const c = text[i] as string
    if (c === ' ' || c === '\t') {
      i += 1
      continue
    }
    if (c === '\n' || c === '\r' || c === ';' || c === '|' || c === '&' || c === '(' || c === ')' || c === '`') {
      flush()
      if (c === '|' && text[i + 1] === '|') i += 2
      else if (c === '&' && text[i + 1] === '&') i += 2
      else i += 1
      continue
    }
    if (c === '$' && text[i + 1] === '(') {
      flush()
      i += 2
      continue
    }
    if (c === '\\' && (text[i + 1] === '\n' || (text[i + 1] === '\r' && text[i + 2] === '\n'))) {
      i += text[i + 1] === '\r' ? 3 : 2
      continue
    }
    const start = i
    let inQuote = ''
    while (i < n) {
      const ch = text[i] as string
      if (inQuote !== '') {
        if (inQuote === '"' && ch === '\\' && i + 1 < n) {
          i += 2
          continue
        }
        if (ch === inQuote) inQuote = ''
        i += 1
        continue
      }
      if (ch === '"' || ch === "'") {
        inQuote = ch
        i += 1
        continue
      }
      if (ch === '\\' && i + 1 < n) {
        if (text[i + 1] === '\n' || (text[i + 1] === '\r' && text[i + 2] === '\n')) break
        i += 2
        continue
      }
      if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === ';' || ch === '|' || ch === '&' || ch === '(' || ch === ')' || ch === '`') break
      i += 1
    }
    const raw = text.slice(start, i)
    const { unquoted, quote } = unquoteCliToken(raw)
    tokens.push({ start, end: i, raw, unquoted, quote })
  }
  flush()
  if (edits.length === 0) return text
  edits.sort((a, b) => a.start - b.start)
  let out = ''
  let copied = 0
  for (const e of edits) {
    if (e.start < copied) continue
    out += text.slice(copied, e.start) + e.replacement
    copied = e.end
  }
  return out + text.slice(copied)
}

function redactCliFlagsAndPositional(text: string, stats: SanitizeStats): string {
  return redactCliFlagsAndPositionalImpl(text, stats, 0)
}

function redactTextPasses(text: string, stats: SanitizeStats): string {
  let out = redactPrivateKeys(text, stats)
  for (const [pattern, replacement] of PATTERNS) {
    pattern.lastIndex = 0
    if (!pattern.test(out)) continue
    pattern.lastIndex = 0
    out = out.replace(pattern, (...args: unknown[]) => {
      stats.redactions += 1
      const groups = args.slice(1, -2) as (string | undefined)[]
      return replacement.replace(/\$(\d)/g, (_, n: string) => groups[Number(n) - 1] ?? '')
    })
  }
  out = redactJwtsLinear(out, stats)
  out = redactBasicBearerAuth(out, stats)
  out = redactUrls(out, stats)
  out = redactK8sAndYamlBlocks(out, stats)
  out = redactMarkdownLabelsAndTables(out, stats)
  out = redactCodeAndConfigForms(out, stats)
  out = redactCliFlagsAndPositional(out, stats)
  return redactListNames(redactOptionWords(redactSingleDashSecrets(redactFlagValues(redactNamedValues(out, stats), stats), stats), stats), stats)
}

const PROSE_SENSITIVE_ASSIGN_BEFORE = /(?:^|[^A-Za-z0-9_.-])([A-Za-z_][A-Za-z0-9_.-]{0,99})["']?[ \t]*[:=](?:[ \t]*(?:[|>][+-]?\d*)?)?[ \t]*(?:\r?\n[ \t]*)?$/
const PROSE_SENSITIVE_FLAG_BEFORE = /(?:^|[\s;|&(])--([A-Za-z][A-Za-z0-9_.-]{0,99})[ \t]*(?:\r?\n[ \t]*)?$/
const PROSE_JSON_MAX_SPAN = 65_536
const PROSE_JSON_MAX_PARSE_TRIES = 64

function hasNestedContainer(val: unknown): boolean {
  if (val === null || typeof val !== 'object') return false
  if (Array.isArray(val)) return true
  for (const k of Object.keys(val as Record<string, unknown>)) {
    const v = (val as Record<string, unknown>)[k]
    if (v !== null && typeof v === 'object') return true
  }
  return false
}

function estimateDroppedJsonChars(root: unknown, maxNodes = 512): number {
  const stack: unknown[] = [root]
  let chars = 0
  let visited = 0
  while (stack.length > 0 && visited < maxNodes) {
    const cur = stack.pop()
    visited += 1
    if (cur === null || cur === undefined) {
      chars += 4
    } else if (typeof cur === 'string') {
      chars += cur.length + 2
    } else if (typeof cur === 'number' || typeof cur === 'boolean') {
      chars += String(cur).length
    } else if (Array.isArray(cur)) {
      chars += 2 + Math.max(0, cur.length - 1)
      const limit = Math.min(cur.length, maxNodes - visited)
      for (let idx = limit - 1; idx >= 0; idx -= 1) stack.push(cur[idx])
    } else if (typeof cur === 'object') {
      const obj = cur as Record<string, unknown>
      const keys = Object.keys(obj)
      chars += 2 + Math.max(0, keys.length - 1)
      const limit = Math.min(keys.length, maxNodes - visited)
      for (let idx = limit - 1; idx >= 0; idx -= 1) {
        const k = keys[idx] as string
        chars += k.length + 3
        stack.push(obj[k])
      }
    } else {
      chars += 4
    }
  }
  if (stack.length > 0) chars += stack.length * 4
  return Math.max(2, chars)
}

function droppedCharsAtDepthLimit(val: unknown): number {
  if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
    const obj = val as Record<string, unknown>
    const keys = Object.keys(obj)
    const cut = obj.__bqaa_depth_cut__
    if (keys.length === 1 && keys[0] === '__bqaa_depth_cut__' && typeof cut === 'number' && Number.isFinite(cut) && cut >= 0) {
      return cut
    }
  }
  return estimateDroppedJsonChars(val)
}

function sanitizeProseJsonString(val: string, stats: SanitizeStats, keyDepth: number): string {
  if (JSON_LOOKING.test(val)) {
    if (keyDepth >= 3) {
      stats.redactions += 1
      stats.truncated = true
      stats.droppedChars += val.length
      return '[TRUNCATED: nested too deep]'
    }
    if (val.length > PROSE_JSON_MAX_SPAN) {
      stats.redactions += 1
      stats.truncated = true
      stats.droppedChars += val.length
      return `[TRUNCATED: ${val.length} chars of JSON text that could not be parsed and checked]`
    }
    let parsed: unknown
    try {
      parsed = JSON.parse(clipDeepJsonSlice(val.trim()))
    } catch {
      stats.redactions += 1
      stats.truncated = true
      stats.droppedChars += val.length
      return `[TRUNCATED: ${val.length} chars of JSON text that could not be parsed and checked]`
    }
    const subStats = newStats()
    const walked = typeof parsed === 'string'
      ? sanitizeProseJsonString(parsed, subStats, keyDepth + 1)
      : walkProseJsonValue(parsed, subStats, 0, keyDepth + 1)
    stats.redactions += subStats.redactions
    stats.droppedChars += subStats.droppedChars
    if (subStats.truncated) stats.truncated = true
    if (subStats.redactions === 0 && !subStats.truncated) return val
    return JSON.stringify(walked, null, typeof parsed !== 'string' && val.includes('\n') ? 2 : undefined)
  }
  if (keyDepth >= 3) {
    if (findBalancedProseJsonSpans(val).length > 0) {
      stats.redactions += 1
      stats.truncated = true
      stats.droppedChars += val.length
      return '[TRUNCATED: nested too deep]'
    }
    return redactTextPasses(val, stats)
  }
  return splitAndRedactProseJson(val, stats, keyDepth + 1)
}

function sanitizeJsonObjectKey(
  key: string,
  own: Set<string>,
  state: { placeholders: number },
  stats: SanitizeStats,
  inSensitiveSubtree = false,
  keyDepth = 0,
): string {
  if (isListedKey(key)) return key
  if (!inSensitiveSubtree && keyDepth < 3 && key.length <= PROSE_JSON_MAX_SPAN) {
    const kStats = newStats()
    if (sanitizeProseJsonString(key, kStats, keyDepth) === key && !kStats.truncated && kStats.redactions === 0) return key
  }
  let outKey: string
  do {
    state.placeholders += 1
    outKey = `[REDACTED_KEY:${state.placeholders}]`
  } while (own.has(outKey))
  own.add(outKey)
  stats.redactions += 1
  return outKey
}

function redactSensitiveSubtree(val: unknown, stats: SanitizeStats, depth = 0, keyDepth = 0): unknown {
  if (val === null) return null
  if (typeof val === 'string') {
    if (isMarkerLike(val)) return val
    stats.redactions += 1
    return MARKER
  }
  if (typeof val === 'number' || typeof val === 'boolean') {
    stats.redactions += 1
    return MARKER
  }
  if (depth > 20) {
    stats.truncated = true
    stats.redactions += 1
    stats.droppedChars += droppedCharsAtDepthLimit(val)
    return MARKER
  }
  if (Array.isArray(val)) return val.map(item => redactSensitiveSubtree(item, stats, depth + 1, keyDepth))
  if (typeof val === 'object') {
    const obj = val as Record<string, unknown>
    const out: Record<string, unknown> = {}
    const keys = Object.keys(obj)
    const own = new Set(keys)
    const state = { placeholders: 0 }
    for (const key of keys) {
      const outKey = sanitizeJsonObjectKey(key, own, state, stats, true, keyDepth)
      out[outKey] = redactSensitiveSubtree(obj[key], stats, depth + 1, keyDepth)
    }
    return out
  }
  return MARKER
}

function walkProseJsonValue(val: unknown, stats: SanitizeStats, depth = 0, keyDepth = 0): unknown {
  if (val === null) return null
  if (typeof val === 'string') return sanitizeProseJsonString(val, stats, keyDepth)
  if (typeof val !== 'object') return val
  if (depth > 20) {
    stats.truncated = true
    stats.redactions += 1
    stats.droppedChars += droppedCharsAtDepthLimit(val)
    return '[TRUNCATED: nested too deep]'
  }
  if (Array.isArray(val)) return val.map(item => walkProseJsonValue(item, stats, depth + 1, keyDepth))
  const obj = val as Record<string, unknown>
  const out: Record<string, unknown> = {}
  const keys = Object.keys(obj)
  const own = new Set(keys)
  const state = { placeholders: 0 }
  for (const key of keys) {
    const outKey = sanitizeJsonObjectKey(key, own, state, stats, false, keyDepth)
    const raw = obj[key]
    if (isSensitiveKey(key)) {
      out[outKey] = redactSensitiveSubtree(raw, stats, depth + 1, keyDepth)
    } else {
      out[outKey] = walkProseJsonValue(raw, stats, depth + 1, keyDepth)
    }
  }
  return out
}

function hasSensitiveAssignmentOrFlag(before: string): boolean {
  const am = PROSE_SENSITIVE_ASSIGN_BEFORE.exec(before)
  if (am !== null && isSensitiveKey(am[1] as string)) return true
  const fm = PROSE_SENSITIVE_FLAG_BEFORE.exec(before)
  return fm !== null && credentialOption(fm[1] as string)
}

function precededBySensitiveAssignment(text: string, braceIdx: number, qStates: Uint8Array): boolean {
  const start = Math.max(0, braceIdx - 512)
  const prefix = text.slice(start, braceIdx)
  if (hasSensitiveAssignmentOrFlag(prefix)) return true
  // Also catch nested JSON inside an unclosed non-JSON outer `{` or `[`
  // opened immediately after a sensitive assignment or CLI flag, e.g.
  // `export APP_SECRET={\n  payload: {\n    "nested": ...\n  }\n}`.
  const unclosed: { rel: number; ch: number }[] = []
  for (let k = 0; k < prefix.length; k += 1) {
    if (qStates[start + k] !== NONE) continue
    const ch = prefix.charCodeAt(k)
    if (ch === 123 || ch === 91) {
      unclosed.push({ rel: k, ch })
    } else if (ch === 125 || ch === 93) {
      const want = ch === 125 ? 123 : 91
      for (let s = unclosed.length - 1; s >= 0; s -= 1) {
        if (unclosed[s]!.ch === want) {
          unclosed.length = s
          break
        }
      }
    }
  }
  for (let s = unclosed.length - 1; s >= 0; s -= 1) {
    const absOpen = start + unclosed[s]!.rel
    const beforeOpen = text.slice(Math.max(0, absOpen - 256), absOpen)
    if (hasSensitiveAssignmentOrFlag(beforeOpen)) return true
  }
  return false
}

function clipDeepJsonSlice(slice: string, maxDepth = 21): string {
  const containers: number[] = []
  const parts: string[] = []
  let inStr = false
  let esc = false
  let cutStart = -1
  let copied = 0
  for (let i = 0; i < slice.length; i += 1) {
    const ch = slice.charCodeAt(i)
    if (inStr) {
      if (esc) esc = false
      else if (ch === 92) esc = true
      else if (ch === 34) inStr = false
      continue
    }
    if (ch === 34) {
      inStr = true
      continue
    }
    if (ch === 123 || ch === 91) {
      containers.push(ch)
      if (containers.length === maxDepth + 1) cutStart = i
    } else if (ch === 125 || ch === 93) {
      const want = ch === 125 ? 123 : 91
      if (containers.length === 0 || containers[containers.length - 1] !== want) return slice
      containers.pop()
      if (cutStart !== -1 && containers.length === maxDepth) {
        parts.push(slice.slice(copied, cutStart), `{"__bqaa_depth_cut__":${i + 1 - cutStart}}`)
        copied = i + 1
        cutStart = -1
      }
    }
  }
  if (inStr || containers.length !== 0 || cutStart !== -1 || copied === 0) return slice
  parts.push(slice.slice(copied))
  return parts.join('')
}

type JsonCandidateSpan = { start: number; end: number; parsed: unknown; sensitiveContext: boolean; multiline: boolean }

function findBalancedProseJsonSpans(text: string): JsonCandidateSpan[] {
  if (!text.includes('{') || !text.includes('}')) return []
  const qStates = quoteStates(text)
  const spans: JsonCandidateSpan[] = []
  const stack: number[] = []
  let inStr = false
  let esc = false
  let wsCursor = -1
  let parseTries = 0
  for (let i = 0; i < text.length; i += 1) {
    const ch = text.charCodeAt(i)
    if (inStr) {
      if (esc) {
        esc = false
      } else if (ch === 92) {
        esc = true
      } else if (ch === 34) {
        inStr = false
      } else if (ch === 10) {
        // Broken string across raw newline: invalidates open frames cleanly.
        inStr = false
        stack.length = 0
      }
      continue
    }
    // Leave text inside shell quotes, ANSI-C quotes, or unknown quote states
    // intact so enclosing assignment/flag redactors consume the whole string,
    // and invalidate partially open candidate frames if non-NONE quotes intervene.
    if (qStates[i] !== NONE) {
      stack.length = 0
      continue
    }
    if (stack.length > 0 && ch === 34) {
      inStr = true
      continue
    }
    if (ch === 123) {
      // Only push `{` if next non-whitespace character is `"` or `}` (checked via monotonic wsCursor).
      let k = wsCursor > i + 1 ? wsCursor : i + 1
      if (k === i + 1) {
        while (k < text.length) {
          const w = text.charCodeAt(k)
          if (w === 32 || w === 9 || w === 10 || w === 13) k += 1
          else break
        }
        wsCursor = k
      }
      const nextCh = k < text.length ? text.charCodeAt(k) : -1
      if (nextCh === 34 || nextCh === 125) {
        stack.push(i)
      } else if (stack.length > 0 && (i === 0 || text.charCodeAt(i - 1) === 123)) {
        // Unquoted `{` directly inside malformed `{` resets candidate stack.
        stack.length = 0
      }
      continue
    }
    if (ch === 125 && stack.length > 0) {
      const open = stack.pop() as number
      if (stack.length === 0) {
        const len = i + 1 - open
        if (len <= PROSE_JSON_MAX_SPAN && parseTries < PROSE_JSON_MAX_PARSE_TRIES) {
          const slice = text.slice(open, i + 1)
          const multiline = slice.includes('\n')
          const maybeNested = multiline || slice.includes('[') || slice.indexOf('{', 1) !== -1
          if (maybeNested) {
            parseTries += 1
            try {
              const parsed = JSON.parse(clipDeepJsonSlice(slice)) as unknown
              if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed) && (multiline || hasNestedContainer(parsed))) {
                spans.push({ start: open, end: i + 1, parsed, sensitiveContext: precededBySensitiveAssignment(text, open, qStates), multiline })
              }
            } catch {
              // Not valid JSON; leave for free-text passes.
            }
          }
        }
      }
    }
  }
  return spans
}

const PROSE_JSON_SUFFIX_RE = /q(\d+)z/g

function proseJsonTokenSuffix(text: string): number {
  if (!text.includes('pjspan')) return 1
  const used = new Set<string>()
  PROSE_JSON_SUFFIX_RE.lastIndex = 0
  let m: RegExpExecArray | null
  while ((m = PROSE_JSON_SUFFIX_RE.exec(text)) !== null) {
    used.add(m[1] as string)
  }
  let suffix = 1
  while (used.has(String(suffix))) suffix += 1
  return suffix
}

function splitAndRedactProseJson(text: string, stats: SanitizeStats, keyDepth = 0): string {
  const spans = findBalancedProseJsonSpans(text)
  if (spans.length === 0) return redactTextPasses(text, stats)
  const suffix = proseJsonTokenSuffix(text)
  let masked = ''
  let copied = 0
  for (let idx = 0; idx < spans.length; idx += 1) {
    const span = spans[idx]!
    masked += text.slice(copied, span.start) + `pjspan${idx}q${suffix}z`
    copied = span.end
  }
  masked += text.slice(copied)
  const redactedMasked = redactTextPasses(masked, stats)
  const tokenRe = new RegExp(`pjspan(\\d+)q${suffix}z`, 'g')
  return redactedMasked.replace(tokenRe, (whole: string, rawIdx: string) => {
    const span = spans[Number(rawIdx)]
    if (span === undefined) return whole
    if (span.sensitiveContext) {
      stats.redactions += 1
      return MARKER
    }
    const sanitized = walkProseJsonValue(span.parsed, stats, 0, keyDepth)
    return JSON.stringify(sanitized, null, span.multiline ? 2 : undefined)
  })
}

// The free-text rules: prose-embedded multi-line/nested JSON objects first,
// then credential shapes, URLs, YAML/K8s blocks, Markdown labels/tables, code
// and CLI credential forms, credential-named values, command-line options,
// option words the shell reads in pieces, and the heuristic list-name fallback.
export function redactText(input: string, stats: SanitizeStats): string {
  return splitAndRedactProseJson(input, stats)
}

// Error text, dead-letter reasons and status lines: redacted first, then held
// to `maxChars`, to avoid cutting recognized matches before redaction. This
// does not cover missed patterns or matches beyond the scan window. Of a
// window cut from a longer text,
// the last REDACT_MARGIN characters are never kept: they were scanned only
// for what comes before them, and may end in a credential the window cut
// short (redactions before it can shrink the window under `maxChars`).
export function redactDiagnostic(text: string, maxChars: number): string {
  const clipped = text.length > maxChars + REDACT_MARGIN
  const redacted = redactText(clipped ? text.slice(0, maxChars + REDACT_MARGIN) : text, newStats())
  return cutAt(redacted, clipped ? Math.min(maxChars, Math.max(0, redacted.length - REDACT_MARGIN)) : maxChars)
}

// ---- bounded copies ------------------------------------------------------------

type WalkState = { remaining: number; seen: WeakSet<object> }

// Length of the marker a cut string ends with.
const MARKER_RESERVE = 40

const jsonLength = (text: string): number => JSON.stringify(text).length

// Never end a cut between the two halves of a surrogate pair.
export const cutAt = (text: string, end: number): string => {
  let cut = text.slice(0, Math.max(0, end))
  const last = cut.charCodeAt(cut.length - 1)
  if (last >= 0xd800 && last <= 0xdbff) cut = cut.slice(0, -1)
  return cut
}

// Already-redacted `text`, cut to `maxJson` characters of serialized JSON and
// marked.
function cutToFit(text: string, originalLength: number, maxJson: number, stats: SanitizeStats): { text: string; cost: number } {
  stats.truncated = true
  const room = Math.max(0, maxJson - MARKER_RESERVE)
  let keep = Math.min(text.length, room)
  let cut = cutAt(text, keep)
  let cost = jsonLength(cut)
  // Escapes (quotes, newlines, control characters) make JSON longer than
  // the text: shrink until the serialized form fits.
  for (let i = 0; i < 8 && cost > room && keep > 0; i += 1) {
    keep = Math.max(0, Math.floor((keep * room) / cost) - 1)
    cut = cutAt(text, keep)
    cost = jsonLength(cut)
  }
  const out = `${cut}...[TRUNCATED ${Math.max(0, originalLength - cut.length)} chars]`
  return { text: out, cost: jsonLength(out) }
}

// `input` redacted by the text rules and, past `maxJson` characters of
// serialized JSON, cut and marked. Only a window reaching REDACT_MARGIN past
// the cut is scanned; the rest of a longer input is dropped, not passed on.
// Of that window, the last REDACT_MARGIN characters are never kept: they were
// scanned only for what comes before them, and may end in a credential the
// window cut short (redactions before it can shrink the window under the cut).
function clipString(input: string, maxJson: number, stats: SanitizeStats): { text: string; cost: number } {
  if (input.length > maxJson + REDACT_MARGIN) {
    const redacted = redactText(input.slice(0, maxJson + REDACT_MARGIN), stats)
    return cutToFit(cutAt(redacted, Math.max(0, redacted.length - REDACT_MARGIN)), input.length, maxJson, stats)
  }
  const redacted = redactText(input, stats)
  const cost = jsonLength(redacted)
  if (cost <= maxJson) return { text: redacted, cost }
  return cutToFit(redacted, input.length, maxJson, stats)
}

// Text that looks like JSON: an object; a list that opens with a value (an
// object, a list, a string, a number, true, false or null) or is empty; or a
// JSON-encoded string whose text opens an object or a list. `[1/3] Building`,
// `[INFO] ready` and `"Hi," she said.` do not.
const JSON_LOOKING = /^\s*(?:\{|\[\s*(?:[{["\]]|(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)\s*[,\]])|"(?:\s|\\[nrt"\\/])*[{[])/

// JSON text short enough to be scanned whole, parsed.
function parseJsonText(text: string, limits: Limits): { value: unknown } | null {
  if (text.length > limits.budgetChars + REDACT_MARGIN) return null
  try {
    return { value: JSON.parse(text) as unknown }
  } catch {
    return null
  }
}

// JSON-looking text that cannot be parsed whole (longer than the scan budget,
// or not valid JSON, such as a document cut short) cannot be checked key by
// key, and scrubbing it line by line would pass a credential nested below
// its name's line: it is dropped and marked instead.
function dropJsonText(text: string, stats: SanitizeStats, state: WalkState): string {
  stats.truncated = true
  stats.redactions += 1
  const marker = `[TRUNCATED: ${text.length} chars of JSON text that could not be parsed and checked]`
  state.remaining -= jsonLength(marker)
  return marker
}

// A key name that is not on the list is copied only when the free-text
// rules find nothing in it. One holding a credential shape, a URL with
// user-info, a credential-named parameter or a credential-named value, or
// one too long to scan within the budget, gives way to `[REDACTED_KEY:n]`:
// numbered in order within its object, never one of that object's own
// names, so no entry overwrites another.
function placeholderNeeded(key: string, limits: Limits): boolean {
  return key.length > limits.budgetChars + REDACT_MARGIN || redactText(key, newStats()) !== key
}

// JSON found in a string: walked under the same allowlist, then encoded
// again. Past the depth budget it is dropped, not passed on unscanned.
function walkJsonText(parsed: unknown, depth: number, allowed: number, limits: Limits, stats: SanitizeStats, state: WalkState): string {
  if (depth >= limits.maxDepth) {
    stats.truncated = true
    const text = '[TRUNCATED: nested JSON too deep]'
    state.remaining -= jsonLength(text)
    return text
  }
  // Encoding the result again escapes it: give the walk half the room.
  const inner = walk(parsed, depth + 1, limits, stats, { remaining: Math.floor(allowed / 2), seen: state.seen })
  const encoded = JSON.stringify(inner === undefined ? null : inner)
  const fitted = jsonLength(encoded) <= allowed ? { text: encoded, cost: jsonLength(encoded) } : cutToFit(encoded, encoded.length, allowed, stats)
  state.remaining -= fitted.cost
  return fitted.text
}

function walk(value: unknown, depth: number, limits: Limits, stats: SanitizeStats, state: WalkState): JsonValue | undefined {
  if (value === null) {
    state.remaining -= 4
    return null
  }
  switch (typeof value) {
    case 'string': {
      const allowed = Math.max(0, Math.min(limits.maxStringChars, state.remaining))
      if (JSON_LOOKING.test(value)) {
        const parsed = parseJsonText(value, limits)
        return parsed === null ? dropJsonText(value, stats, state) : walkJsonText(parsed.value, depth, allowed, limits, stats, state)
      }
      const { text, cost } = clipString(value, allowed, stats)
      state.remaining -= cost
      return text
    }
    case 'number': {
      const out = Number.isFinite(value) ? value : null
      state.remaining -= String(out).length
      return out
    }
    case 'boolean':
      state.remaining -= 5
      return value
    case 'bigint': {
      const out = value.toString()
      state.remaining -= out.length + 2
      return out
    }
    case 'undefined':
    case 'function':
    case 'symbol':
      return undefined
  }
  const obj = value as object
  if (state.seen.has(obj)) return '[CIRCULAR]'
  if (depth >= limits.maxDepth) {
    stats.truncated = true
    return '[TRUNCATED: nested too deep]'
  }
  if (ArrayBuffer.isView(obj)) return `[binary ${obj.byteLength} bytes]`
  if (obj instanceof ArrayBuffer) return `[binary ${obj.byteLength} bytes]`
  if (obj instanceof Date) return Number.isNaN(obj.getTime()) ? null : obj.toISOString()
  state.seen.add(obj)
  state.remaining -= 2
  try {
    if (Array.isArray(obj)) {
      const out: JsonValue[] = []
      for (let i = 0; i < obj.length; i += 1) {
        if (i >= limits.maxItems || state.remaining <= 0) {
          stats.truncated = true
          out.push(`[TRUNCATED ${obj.length - i} items]`)
          break
        }
        state.remaining -= 1
        const item = walk(obj[i], depth + 1, limits, stats, state)
        out.push(item === undefined ? null : item)
      }
      return out
    }
    const out: { [key: string]: JsonValue } = {}
    const keys = Object.keys(obj)
    const own = new Set(keys)
    let placeholders = 0
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i] as string
      if (i >= limits.maxKeys || state.remaining <= 0) {
        stats.truncated = true
        out['_truncated_keys'] = keys.length - i
        break
      }
      const raw = (obj as Record<string, unknown>)[key]
      if (raw === undefined || typeof raw === 'function' || typeof raw === 'symbol') continue
      // Decided by the name as it came; a listed name is made of listed
      // words alone and is copied as it is.
      const listed = isListedKey(key)
      let name = key
      if (!listed && placeholderNeeded(key, limits)) {
        do {
          placeholders += 1
          name = `[REDACTED_KEY:${placeholders}]`
        } while (own.has(name))
        stats.redactions += 1
      }
      state.remaining -= jsonLength(name) + 2
      // Under a key that is not listed the whole value goes, whatever its
      // type; null has nothing to hide.
      if (!listed) {
        if (raw === null) {
          state.remaining -= 4
          out[name] = null
        } else {
          stats.redactions += 1
          state.remaining -= 12
          out[name] = MARKER
        }
        continue
      }
      const item = walk(raw, depth + 1, limits, stats, state)
      if (item !== undefined) out[name] = item
    }
    return out
  } finally {
    state.seen.delete(obj)
  }
}

// A JSON-safe, bounded, redacted copy of a row's `content`, its
// `attributes` or its error text: every key is held to the allowlist, every
// string to the text rules.
export function sanitize(value: unknown, limits: Limits, stats: SanitizeStats): JsonValue {
  const out = walk(value, 0, limits, stats, { remaining: limits.budgetChars, seen: new WeakSet() })
  return out === undefined ? null : out
}

// Serializes a sanitized value as the text of a BigQuery JSON column, held to
// `maxChars`. Past it the column becomes a valid JSON object holding a prefix
// of the serialized text, and `truncated` is set.
export function toJsonColumn(value: JsonValue, maxChars: number, stats: SanitizeStats): string {
  const json = JSON.stringify(value)
  if (json.length <= maxChars) return json
  stats.truncated = true
  let keep = Math.max(0, Math.floor((maxChars - 120) / 2))
  for (;;) {
    const wrapped = JSON.stringify({ truncated: true, original_chars: json.length, preview: json.slice(0, keep) })
    if (wrapped.length <= maxChars || keep === 0) return wrapped
    keep = Math.floor(keep / 2)
  }
}
