# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Claude Code hook producer for BQAA.

Maps Claude Code's native hooks (``SessionStart``, ``UserPromptSubmit``,
``PreToolUse``, ``PostToolUse``, ``PostToolUseFailure``, ``Stop``,
``StopFailure``, ``SubagentStop``, ``Notification``, ``PermissionRequest``,
``SessionEnd``) onto BQAA ``agent_events`` rows. Each hook receives a JSON
payload on stdin and this module's ``main()`` is the entry that hook shell
wrappers invoke.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time
import traceback
from typing import Any, Iterator

from ._utils import deterministic_span
from ._utils import ensure_private_dir
from ._utils import hex_id
from ._utils import inspect_spool_status
from ._utils import log_to_file
from ._utils import PrivateDir
from ._utils import safe_json_loads
from ._utils import timestamp_ms
from ._utils import to_jsonable
from .config import _env_bool
from .config import BQAAConfig
from .config import DEFAULT_LOG_FILE
from .config import DEFAULT_STATE_DIR
from .config import InvalidConfigError
from .logger import BigQueryAgentAnalyticsLogger

CLAUDE_CODE_DEFAULT_AGENT = "claude-code"
_MAX_SEEN_USAGE = 256
_USAGE_FIELDS = (
    "input_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
)


# ----------------------------------------------------------------------------
# Transcript reader
# ----------------------------------------------------------------------------


def _normalize_seen_usage(raw: Any) -> dict[str, dict[str, int]]:
  out: dict[str, dict[str, int]] = {}
  if not isinstance(raw, dict):
    return out
  for key, val in raw.items():
    if isinstance(val, dict):
      out[str(key)] = {f: int(val.get(f) or 0) for f in _USAGE_FIELDS}
    elif isinstance(val, (int, float)):
      out[str(key)] = {
          "input_tokens": 0,
          "cache_read_input_tokens": 0,
          "cache_creation_input_tokens": 0,
          "output_tokens": int(val),
      }
  while len(out) > _MAX_SEEN_USAGE:
    del out[next(iter(out))]
  return out


def _read_transcript_delta(
    path: str,
    start_line: int,
    max_bytes: int,
    seen_usage: dict[str, Any] | None = None,
) -> tuple[str, str, dict[str, int], bool, int, dict[str, dict[str, int]]]:
  """Read complete transcript JSONL lines after ``start_line``.

  Returns ``(output_text, model, usage, was_truncated, last_line, seen_usage)``.

  - Incomplete trailing lines at EOF without a trailing newline that raise
    ``json.JSONDecodeError`` pause cursor advancement before that line so a
    subsequent flush picks up the completed record.
  - Token usage is deduplicated per ``message.id`` (taking per-field ``max``
    within the pass across content-block lines, then subtracting tokens
    already emitted in prior hooks via ``seen_usage``). Anonymous lines lacking
    ``message.id`` are keyed by ``f"line-{line_no}"``.
  - ``seen_usage`` is FIFO-capped at ``_MAX_SEEN_USAGE`` (256) entries.
  """
  output_parts: list[str] = []
  bytes_collected = 0
  was_truncated = False
  model = ""
  usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
  next_seen = _normalize_seen_usage(seen_usage)
  last_consumed_line = max(0, int(start_line or 0))
  if not path:
    return "", model, usage, False, last_consumed_line, next_seen
  transcript = Path(path).expanduser()
  if not transcript.exists():
    return "", model, usage, False, last_consumed_line, next_seen

  pass_max: dict[str, dict[str, int]] = {}
  with transcript.open("r", encoding="utf-8", errors="replace") as fh:
    for line_no, raw_line in enumerate(fh, start=1):
      if line_no <= last_consumed_line:
        continue
      if not raw_line.strip():
        last_consumed_line = line_no
        continue
      try:
        item = json.loads(raw_line)
      except json.JSONDecodeError:
        if not raw_line.endswith(("\n", "\r")):
          # Incomplete line at EOF still being written by the host CLI.
          break
        last_consumed_line = line_no
        continue
      last_consumed_line = line_no
      if not isinstance(item, dict) or item.get("type") != "assistant":
        continue
      message = item.get("message") or {}
      if not isinstance(message, dict):
        continue
      model = message.get("model") or model
      text = _text_from_content(message.get("content"))
      if text:
        if max_bytes > 0:
          encoded = text.encode("utf-8", "replace")
          remaining = max_bytes - bytes_collected
          if remaining <= 0:
            was_truncated = True
          elif len(encoded) > remaining:
            output_parts.append(
                encoded[:remaining].decode("utf-8", "replace") + "...[TRUNCATED]"
            )
            bytes_collected = max_bytes
            was_truncated = True
          else:
            output_parts.append(text)
            bytes_collected += len(encoded)
        else:
          output_parts.append(text)

      raw_usage = message.get("usage")
      if isinstance(raw_usage, dict) and any(
          raw_usage.get(f) is not None for f in _USAGE_FIELDS
      ):
        msg_id = message.get("id")
        dedup_key = (
            str(msg_id)
            if isinstance(msg_id, str) and msg_id.strip()
            else f"line-{line_no}"
        )
        cur = pass_max.get(dedup_key, {f: 0 for f in _USAGE_FIELDS})
        for field in _USAGE_FIELDS:
          try:
            val = int(raw_usage.get(field) or 0)
          except (TypeError, ValueError):
            val = 0
          if val > cur[field]:
            cur[field] = val
        pass_max[dedup_key] = cur

  for key, cur_fields in pass_max.items():
    prev = next_seen.get(key, {f: 0 for f in _USAGE_FIELDS})
    delta_in = max(0, cur_fields["input_tokens"] - prev["input_tokens"])
    delta_cr = max(
        0,
        cur_fields["cache_read_input_tokens"]
        - prev["cache_read_input_tokens"],
    )
    delta_cc = max(
        0,
        cur_fields["cache_creation_input_tokens"]
        - prev["cache_creation_input_tokens"],
    )
    delta_out = max(0, cur_fields["output_tokens"] - prev["output_tokens"])
    usage["prompt_tokens"] += delta_in + delta_cr + delta_cc
    usage["completion_tokens"] += delta_out
    merged = {f: max(prev[f], cur_fields[f]) for f in _USAGE_FIELDS}
    next_seen.pop(key, None)
    next_seen[key] = merged
  while len(next_seen) > _MAX_SEEN_USAGE:
    del next_seen[next(iter(next_seen))]

  usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
  return (
      "\n".join(output_parts),
      model,
      usage,
      was_truncated,
      last_consumed_line,
      next_seen,
  )


def _read_transcript_since(
    path: str, start_line: int, max_bytes: int
) -> tuple[str, str, dict[str, int], bool]:
  """Backward-compatible wrapper returning ``(text, model, usage, truncated)``."""
  text, model, usage, was_truncated, _, _ = _read_transcript_delta(
      path, start_line, max_bytes, None
  )
  return text, model, usage, was_truncated


def _text_from_content(content: Any) -> str:
  if isinstance(content, str):
    return content
  if isinstance(content, list):
    pieces = []
    for item in content:
      if isinstance(item, dict) and item.get("type") == "text":
        pieces.append(str(item.get("text", "")))
      elif isinstance(item, str):
        pieces.append(item)
    return "\n".join(p for p in pieces if p)
  return ""


def _line_count(path: str) -> int:
  """Count complete trailing-newline or valid-JSON lines in ``path``."""
  if not path:
    return 0
  transcript = Path(path).expanduser()
  if not transcript.exists():
    return 0
  count = 0
  with transcript.open("r", encoding="utf-8", errors="replace") as fh:
    for raw_line in fh:
      if not raw_line.endswith(("\n", "\r")) and raw_line.strip():
        try:
          json.loads(raw_line)
        except json.JSONDecodeError:
          break
      count += 1
  return count


# ----------------------------------------------------------------------------
# State store with file locking
# ----------------------------------------------------------------------------


class _LockedJSONStore:
  """Atomic read-modify-write JSON store guarded by ``fcntl.flock``."""

  def __init__(self, directory: PrivateDir, name: str):
    self.directory = directory
    self.name = name

  @contextlib.contextmanager
  def transaction(self) -> Iterator[dict[str, Any]]:
    fd = self.directory.open(self.name, os.O_CREAT | os.O_RDWR)
    try:
      fcntl.flock(fd, fcntl.LOCK_EX)
      try:
        with os.fdopen(fd, "r+", encoding="utf-8", closefd=False) as handle:
          handle.seek(0)
          raw = handle.read()
          state = safe_json_loads(raw, {}) if raw.strip() else {}
          yield state
          handle.seek(0)
          handle.truncate()
          handle.write(json.dumps(state, sort_keys=True))
          handle.flush()
          os.fsync(fd)
      finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
      try:
        os.close(fd)
      except OSError:
        pass

  def read(self) -> dict[str, Any]:
    if not self.directory.exists(self.name):
      return {}
    with self.transaction() as state:
      return dict(state)

  def remove(self) -> None:
    try:
      self.directory.unlink(self.name)
    except FileNotFoundError:
      pass


class StateStore:
  """Per-session state with locked RMW + per-tool sub-files."""

  def __init__(self, key: str, root: str = DEFAULT_STATE_DIR):
    safe_key = (
        "".join(ch for ch in key if ch.isalnum() or ch in "._-") or "default"
    )
    self.key = safe_key
    self._dir = ensure_private_dir(root)
    self.root = self._dir.path
    self._session_store = _LockedJSONStore(self._dir, f"state_{safe_key}.json")
    self._cache: dict[str, Any] = self._session_store.read()

  def get(self, key: str, default: Any = None) -> Any:
    return self._cache.get(key, default)

  def set(self, key: str, value: Any) -> None:
    self.update({key: value})

  def update(self, values: dict[str, Any]) -> None:
    with self._session_store.transaction() as state:
      state.update(values)
      self._cache = dict(state)

  def delete(self, *keys: str) -> None:
    with self._session_store.transaction() as state:
      for key in keys:
        state.pop(key, None)
      self._cache = dict(state)

  def remove(self) -> None:
    self._session_store.remove()
    prefix = f"tool_{self.key}_"
    for name in self._dir.names():
      if name.startswith(prefix) and name.endswith(".json"):
        _LockedJSONStore(self._dir, name).remove()

  # -- Per-tool sub-state ---------------------------------------------------

  def _tool_store(self, tool_use_id: str) -> _LockedJSONStore:
    safe = (
        "".join(ch for ch in tool_use_id if ch.isalnum() or ch in "._-")
        or "unknown"
    )
    return _LockedJSONStore(self._dir, f"tool_{self.key}_{safe}.json")

  def set_tool(self, tool_use_id: str, value: dict[str, Any]) -> None:
    with self._tool_store(tool_use_id).transaction() as state:
      state.clear()
      state.update(value)

  def pop_tool(self, tool_use_id: str) -> dict[str, Any]:
    store = self._tool_store(tool_use_id)
    with store.transaction() as state:
      value = dict(state)
    store.remove()
    return value


def cleanup_stale_state(root: str | Path, ttl_hours: float) -> int:
  """Remove state, per-tool, and marker files older than ``ttl_hours``."""
  if ttl_hours <= 0:
    return 0
  base = Path(root).expanduser()
  if not base.exists():
    return 0
  cutoff = time.time() - ttl_hours * 3600
  removed = 0
  with ensure_private_dir(base) as directory:
    for name in directory.names():
      is_state_json = (
          name.startswith("state_") or name.startswith("tool_")
      ) and name.endswith(".json")
      is_marker = name.startswith("missing_env_")
      if not (is_state_json or is_marker):
        continue
      try:
        info = directory.lstat(name)
      except FileNotFoundError:
        continue
      if stat.S_ISREG(info.st_mode) and info.st_mtime < cutoff:
        try:
          directory.unlink(name)
          removed += 1
        except FileNotFoundError:
          pass
  return removed


# ----------------------------------------------------------------------------
# Tool & payload helpers
# ----------------------------------------------------------------------------


def _tool_origin(tool_name: Any) -> str:
  name = str(tool_name or "")
  if name.lower().startswith("mcp__"):
    return "MCP"
  if name in {"Task", "Subagent"}:
    return "SUB_AGENT"
  return "LOCAL"


def _tool_status(result: Any) -> tuple[str, str | None]:
  if isinstance(result, dict):
    if result.get("is_error") or result.get("error"):
      return "ERROR", str(
          result.get("error") or result.get("message") or "tool error"
      )
  text = str(result or "")
  if text.lower().startswith("error:"):
    return "ERROR", text[:1000]
  return "OK", None


def _non_empty_str(value: Any) -> str:
  if isinstance(value, str) and value.strip():
    return value
  return ""


def _pick_response(
    payload: dict[str, Any],
    transcript_text: str,
    *,
    fallback_keys: tuple[str, ...],
    default: str,
) -> tuple[str, bool]:
  """Select best assistant response; returns ``(text, was_captured)``."""
  direct = _non_empty_str(payload.get("last_assistant_message"))
  if direct:
    return direct, True
  if _non_empty_str(transcript_text):
    return transcript_text, True
  for key in fallback_keys:
    val = _non_empty_str(payload.get(key))
    if val:
      return val, True
  return default, False


def _extract_error_message(payload: dict[str, Any], default: str) -> str:
  for key in ("error", "error_message", "reason", "message"):
    raw = payload.get(key)
    if isinstance(raw, dict):
      nested = (
          raw.get("message")
          or raw.get("error")
          or json.dumps(raw, sort_keys=True)
      )
      if str(nested).strip():
        return str(nested)[:1000]
    elif raw is not None and str(raw).strip():
      return str(raw)[:1000]
  return default


def _warn_missing_destination_once(
    config: BQAAConfig, hook_name: str, session_key: str
) -> None:
  """Log missing BQAA_PROJECT_ID / BQAA_DATASET once per session."""
  digest = hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:16]
  marker = f"missing_env_{digest}"
  try:
    with ensure_private_dir(config.state_dir) as directory:
      fd = directory.open(
          marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
      )
      os.close(fd)
  except FileExistsError:
    return
  except OSError:
    pass
  log_to_file(
      config,
      f"WARN hook={hook_name} session={session_key}: BQAA_PROJECT_ID and"
      " BQAA_DATASET are required unless BQAA_DRY_RUN=true; skipping trace"
      " emission for this session. Run /bqaa-setup to configure.",
  )


# ----------------------------------------------------------------------------
# Hook adapter
# ----------------------------------------------------------------------------


class ClaudeHookBQAAAdapter:
  """Dispatches Claude Code hook payloads to ``BigQueryAgentAnalyticsLogger``."""

  def __init__(self, logger: BigQueryAgentAnalyticsLogger | None = None):
    self.logger = logger or BigQueryAgentAnalyticsLogger()
    self.config = self.logger.config

  def process(self, hook_name: str, payload: dict[str, Any]) -> None:
    session_key = self._state_key(payload)
    if not self.config.dry_run and (
        not self.config.project_id or not self.config.dataset
    ):
      _warn_missing_destination_once(self.config, hook_name, session_key)
      return

    state = StateStore(session_key, root=self.config.state_dir)
    if hook_name == "SessionStart":
      self._session_start(payload, state)
    elif hook_name == "UserPromptSubmit":
      self._user_prompt_submit(payload, state)
    elif hook_name == "PreToolUse":
      self._pre_tool_use(payload, state)
    elif hook_name == "PostToolUse":
      self._post_tool_use(payload, state)
    elif hook_name == "PostToolUseFailure":
      self._post_tool_use_failure(payload, state)
    elif hook_name == "Stop":
      self._stop(payload, state)
    elif hook_name == "StopFailure":
      self._stop_failure(payload, state)
    elif hook_name == "SubagentStop":
      self._subagent_stop(payload, state)
    elif hook_name == "Notification":
      self._notification(payload, state)
    elif hook_name == "PermissionRequest":
      self._permission_request(payload, state)
    elif hook_name == "SessionEnd":
      self._session_end(payload, state)

  def _state_key(self, payload: dict[str, Any]) -> str:
    return (
        str(payload.get("session_id") or "")
        or os.environ.get("CLAUDE_SESSION_KEY", "")
        or str(os.getppid())
    )

  def _ensure_session(self, payload: dict[str, Any], state: StateStore) -> None:
    if state.get("session_id"):
      return
    self._session_start(payload, state)

  def _flush_late_transcript(
      self, payload: dict[str, Any], state: StateStore
  ) -> None:
    """Flush any transcript lines written after the last Stop event."""
    transcript = str(
        payload.get("transcript_path")
        or state.get("last_transcript_path")
        or state.get("transcript_path")
        or ""
    )
    if not transcript:
      return
    cursor = int(
        state.get("transcript_line")
        if state.get("transcript_line") is not None
        else (state.get("transcript_start_line") or 0)
    )
    seen_usage = state.get("seen_usage")
    text, model, usage, was_truncated, next_line, next_seen = (
        _read_transcript_delta(
            transcript,
            cursor,
            self.config.transcript_max_bytes,
            seen_usage,
        )
    )
    state.update(
        {
            "transcript_line": next_line,
            "seen_usage": next_seen,
        }
    )
    target_trace = state.get("current_trace_id") or state.get("last_trace_id")
    if not target_trace:
      return
    captured_for_trace = (
        bool(state.get("last_response_captured"))
        and state.get("last_response_captured_trace_id") == target_trace
    )
    need_text = not captured_for_trace and bool(text.strip())
    if usage["total_tokens"] <= 0 and not need_text:
      return
    inv_id = (
        state.get("current_invocation_id")
        or state.get("last_invocation_id")
        or hex_id(32)
    )
    span_id = (
        state.get("current_span_id") or state.get("last_span_id") or hex_id(16)
    )
    self.logger.log_llm_response(
        response=text if need_text else "",
        session_id=state.get("session_id"),
        invocation_id=inv_id,
        trace_id=target_trace,
        span_id=span_id,
        agent=state.get("agent"),
        user_id=state.get("user_id"),
        model=model or str(state.get("last_stop_model") or ""),
        usage_metadata=usage,
        attributes={
            "session_metadata": {
                "project_name": state.get("project_name"),
                "source": "claude_code",
                "trace_number": state.get("trace_count"),
            },
            "custom_tags": {"assistant": "claude_code"},
            "late_transcript_flush": True,
        },
        is_truncated=was_truncated if need_text else False,
    )
    if need_text:
      state.update(
          {
              "last_response_captured": True,
              "last_response_captured_trace_id": target_trace,
          }
      )

  def _session_start(self, payload: dict[str, Any], state: StateStore) -> None:
    cwd = payload.get("cwd") or os.getcwd()
    session_id = payload.get("session_id") or hex_id(32)
    state.update(
        {
            "session_id": session_id,
            "project_name": (
                os.environ.get("BQAA_PROJECT_NAME") or Path(cwd).name
            ),
            "agent": (
                os.environ.get("BQAA_AGENT_NAME") or CLAUDE_CODE_DEFAULT_AGENT
            ),
            "user_id": os.environ.get("BQAA_USER_ID") or os.environ.get("USER"),
            "trace_count": int(state.get("trace_count", 0)),
            "session_start_ms": timestamp_ms(),
        }
    )
    try:
      cleanup_stale_state(self.config.state_dir, self.config.state_ttl_hours)
    except OSError:
      pass

    spool_status = inspect_spool_status(self.config.spool_dir)
    if spool_status.get("error"):
      msg = (
          f"BQAA tracing spool warning ({self.config.spool_dir}):"
          f" {spool_status['error']}. Run /bqaa-setup for details."
      )
      sys.stdout.write(json.dumps({"systemMessage": msg}) + "\n")
      sys.stdout.flush()
    elif (
        spool_status["pending_count"] > 0
        or spool_status["deferred_count"] > 0
        or spool_status["dead_letter_count"] > 0
    ):
      msg = (
          f"BQAA tracing backlog ({self.config.spool_dir}):"
          f" pending={spool_status['pending_count']},"
          f" deferred={spool_status['deferred_count']},"
          f" dead-letter={spool_status['dead_letter_count']}"
      )
      if spool_status["latest_error"]:
        msg += f" [last error: {str(spool_status['latest_error'])[:160]}]"
      msg += ". Run /bqaa-setup for details."
      sys.stdout.write(json.dumps({"systemMessage": msg}) + "\n")
      sys.stdout.flush()

  def _user_prompt_submit(
      self, payload: dict[str, Any], state: StateStore
  ) -> None:
    self._ensure_session(payload, state)
    # Flush any trailing lines from the previous turn before starting the next.
    if state.get("last_trace_id") and not state.get("current_trace_id"):
      self._flush_late_transcript(payload, state)

    trace_count = int(state.get("trace_count", 0)) + 1
    trace_id = hex_id(32)
    span_id = hex_id(16)
    invocation_id = payload.get("invocation_id") or hex_id(32)
    prompt = str(payload.get("prompt") or "")
    transcript = str(payload.get("transcript_path") or "")
    prev_path = state.get("last_transcript_path") or state.get("transcript_path")
    if transcript and prev_path == transcript and state.get("transcript_line") is not None:
      start_line = max(int(state.get("transcript_line") or 0), _line_count(transcript))
    else:
      start_line = _line_count(transcript)
    state.update(
        {
            "trace_count": trace_count,
            "current_trace_id": trace_id,
            "current_span_id": span_id,
            "current_invocation_id": invocation_id,
            "current_prompt": prompt,
            "current_start_ms": timestamp_ms(),
            "transcript_path": transcript,
            "last_transcript_path": transcript,
            "transcript_start_line": start_line,
            "transcript_line": start_line,
            "last_response_captured": False,
            "last_response_captured_trace_id": None,
            "last_stop_text": None,
            "last_stop_usage": None,
            "last_stop_model": None,
        }
    )
    self.logger.log_llm_request(
        prompt=prompt,
        session_id=state.get("session_id"),
        invocation_id=invocation_id,
        trace_id=trace_id,
        span_id=span_id,
        agent=state.get("agent"),
        user_id=state.get("user_id"),
        attributes={
            "session_metadata": {
                "project_name": state.get("project_name"),
                "cwd": payload.get("cwd"),
                "source": "claude_code",
                "trace_number": trace_count,
            },
            "custom_tags": {"assistant": "claude_code"},
        },
    )

  def _pre_tool_use(self, payload: dict[str, Any], state: StateStore) -> None:
    self._ensure_session(payload, state)
    tool_use_id = str(payload.get("tool_use_id") or hex_id(16))
    span_id = deterministic_span(tool_use_id)
    tool_name = str(payload.get("tool_name") or "unknown")
    state.set_tool(
        tool_use_id,
        {
            "start_ms": timestamp_ms(),
            "span_id": span_id,
            "tool_name": tool_name,
        },
    )
    self.logger.log_tool_starting(
        tool=tool_name,
        args=to_jsonable(payload.get("tool_input") or {}),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id") or hex_id(32),
        trace_id=state.get("current_trace_id") or hex_id(32),
        span_id=span_id,
        parent_span_id=state.get("current_span_id"),
        agent=state.get("agent"),
        tool_origin=_tool_origin(tool_name),
        attributes={"source": "claude_code"},
    )

  def _post_tool_use(self, payload: dict[str, Any], state: StateStore) -> None:
    self._ensure_session(payload, state)
    tool_use_id = str(payload.get("tool_use_id") or "")
    tool_state = state.pop_tool(tool_use_id) if tool_use_id else {}
    start_ms = int(tool_state.get("start_ms") or timestamp_ms())
    tool_name = str(
        payload.get("tool_name") or tool_state.get("tool_name") or "unknown"
    )
    span_id = tool_state.get("span_id") or deterministic_span(tool_use_id)
    result = payload.get("tool_response")
    status, error_message = _tool_status(result)
    self.logger.log_tool_completed(
        tool=tool_name,
        result=to_jsonable(result),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id") or hex_id(32),
        trace_id=state.get("current_trace_id") or hex_id(32),
        span_id=span_id,
        parent_span_id=state.get("current_span_id"),
        agent=state.get("agent"),
        tool_origin=_tool_origin(tool_name),
        total_ms=max(0, timestamp_ms() - start_ms),
        status=status,
        error_message=error_message,
        attributes={"source": "claude_code"},
    )

  def _post_tool_use_failure(
      self, payload: dict[str, Any], state: StateStore
  ) -> None:
    self._ensure_session(payload, state)
    tool_use_id = str(payload.get("tool_use_id") or "")
    tool_state = state.pop_tool(tool_use_id) if tool_use_id else {}
    start_ms = int(tool_state.get("start_ms") or timestamp_ms())
    tool_name = str(
        payload.get("tool_name") or tool_state.get("tool_name") or "unknown"
    )
    span_id = tool_state.get("span_id") or deterministic_span(tool_use_id)
    error_message = _extract_error_message(payload, "tool error")
    result = payload.get("tool_response")
    if result is None:
      result = {
          "is_error": True,
          "error": error_message,
          "is_interrupt": bool(payload.get("is_interrupt", False)),
      }
    self.logger.log_tool_completed(
        tool=tool_name,
        result=to_jsonable(result),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id") or hex_id(32),
        trace_id=state.get("current_trace_id") or hex_id(32),
        span_id=span_id,
        parent_span_id=state.get("current_span_id"),
        agent=state.get("agent"),
        tool_origin=_tool_origin(tool_name),
        total_ms=max(0, timestamp_ms() - start_ms),
        status="ERROR",
        error_message=error_message,
        attributes={
            "source": "claude_code",
            "hook_event_name": "PostToolUseFailure",
        },
    )

  def _stop(self, payload: dict[str, Any], state: StateStore) -> None:
    trace_id = state.get("current_trace_id")
    if not trace_id:
      return
    transcript = str(
        payload.get("transcript_path") or state.get("transcript_path") or ""
    )
    cursor = int(
        state.get("transcript_line")
        if state.get("transcript_line") is not None
        else (state.get("transcript_start_line") or 0)
    )
    text, model, usage, was_truncated, next_line, next_seen = (
        _read_transcript_delta(
            transcript,
            cursor,
            self.config.transcript_max_bytes,
            state.get("seen_usage"),
        )
    )
    output, was_captured = _pick_response(
        payload,
        text,
        fallback_keys=("response", "output"),
        default="(No response captured)",
    )
    start_ms = int(state.get("current_start_ms") or timestamp_ms())
    effective_model = model or str(payload.get("model") or "")
    inv_id = state.get("current_invocation_id")
    span_id = state.get("current_span_id")
    self.logger.log_llm_response(
        response=output,
        session_id=state.get("session_id"),
        invocation_id=inv_id,
        trace_id=trace_id,
        span_id=span_id,
        agent=state.get("agent"),
        user_id=state.get("user_id"),
        model=effective_model,
        usage_metadata=usage,
        total_ms=max(0, timestamp_ms() - start_ms),
        attributes={
            "session_metadata": {
                "project_name": state.get("project_name"),
                "source": "claude_code",
                "trace_number": state.get("trace_count"),
            },
            "custom_tags": {"assistant": "claude_code"},
        },
        is_truncated=was_truncated,
    )
    state.update(
        {
            "last_trace_id": trace_id,
            "last_span_id": span_id,
            "last_invocation_id": inv_id,
            "last_transcript_path": transcript,
            "transcript_line": next_line,
            "seen_usage": next_seen,
            "last_response_captured": was_captured,
            "last_response_captured_trace_id": trace_id if was_captured else None,
            "last_stop_model": effective_model,
        }
    )
    state.delete(
        "current_trace_id",
        "current_span_id",
        "current_invocation_id",
        "current_prompt",
        "current_start_ms",
        "transcript_path",
        "transcript_start_line",
    )

  def _stop_failure(self, payload: dict[str, Any], state: StateStore) -> None:
    self._ensure_session(payload, state)
    trace_id = (
        state.get("current_trace_id")
        or state.get("last_trace_id")
        or hex_id(32)
    )
    inv_id = (
        state.get("current_invocation_id")
        or state.get("last_invocation_id")
        or hex_id(32)
    )
    span_id = (
        state.get("current_span_id") or state.get("last_span_id") or hex_id(16)
    )
    transcript = str(
        payload.get("transcript_path")
        or state.get("transcript_path")
        or state.get("last_transcript_path")
        or ""
    )
    cursor = int(
        state.get("transcript_line")
        if state.get("transcript_line") is not None
        else (state.get("transcript_start_line") or 0)
    )
    text, model, usage, was_truncated, next_line, next_seen = (
        _read_transcript_delta(
            transcript,
            cursor,
            self.config.transcript_max_bytes,
            state.get("seen_usage"),
        )
    )
    output, was_captured = _pick_response(
        payload,
        text,
        fallback_keys=("response", "output"),
        default="",
    )
    error_message = _extract_error_message(payload, "stop failure")
    start_ms = int(state.get("current_start_ms") or timestamp_ms())
    effective_model = model or str(payload.get("model") or "")
    attrs: dict[str, Any] = {
        "session_metadata": {
            "project_name": state.get("project_name"),
            "source": "claude_code",
            "trace_number": state.get("trace_count"),
        },
        "custom_tags": {"assistant": "claude_code"},
        "stop_failure": True,
        "hook_event_name": "StopFailure",
    }
    if payload.get("error_type"):
      attrs["error_type"] = payload.get("error_type")
    if payload.get("reason"):
      attrs["reason"] = payload.get("reason")
    self.logger.log_llm_response(
        response=output,
        session_id=state.get("session_id"),
        invocation_id=inv_id,
        trace_id=trace_id,
        span_id=span_id,
        agent=state.get("agent"),
        user_id=state.get("user_id"),
        model=effective_model,
        usage_metadata=usage,
        total_ms=max(0, timestamp_ms() - start_ms),
        status="ERROR",
        error_message=error_message,
        attributes=attrs,
        is_truncated=was_truncated,
    )
    state.update(
        {
            "last_trace_id": trace_id,
            "last_span_id": span_id,
            "last_invocation_id": inv_id,
            "last_transcript_path": transcript,
            "transcript_line": next_line,
            "seen_usage": next_seen,
            "last_response_captured": was_captured,
            "last_response_captured_trace_id": trace_id if was_captured else None,
            "last_stop_model": effective_model,
        }
    )
    state.delete(
        "current_trace_id",
        "current_span_id",
        "current_invocation_id",
        "current_prompt",
        "current_start_ms",
        "transcript_path",
        "transcript_start_line",
    )

  def _subagent_stop(self, payload: dict[str, Any], state: StateStore) -> None:
    if not state.get("current_trace_id"):
      return
    transcript = str(payload.get("agent_transcript_path") or "")
    text, model, usage, was_truncated = _read_transcript_since(
        transcript, 0, self.config.transcript_max_bytes
    )
    output, _ = _pick_response(
        payload,
        text,
        fallback_keys=("output", "response"),
        default="(No response captured)",
    )
    agent_name = str(
        payload.get("agent_type") or payload.get("agent_id") or "subagent"
    )
    self.logger.log_llm_response(
        response=output,
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id"),
        trace_id=state.get("current_trace_id"),
        span_id=hex_id(16),
        parent_span_id=state.get("current_span_id"),
        agent=agent_name,
        user_id=state.get("user_id"),
        model=model or str(payload.get("model") or ""),
        usage_metadata=usage,
        attributes={
            "session_metadata": {
                "project_name": state.get("project_name"),
                "source": "claude_code_subagent",
            },
            "custom_tags": {
                "assistant": "claude_code",
                "subagent_id": payload.get("agent_id"),
            },
        },
        is_truncated=was_truncated,
    )

  def _notification(self, payload: dict[str, Any], state: StateStore) -> None:
    self._ensure_session(payload, state)
    self.logger.log_event(
        event_type="STATE_DELTA",
        agent=state.get("agent"),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id"),
        trace_id=state.get("current_trace_id"),
        span_id=hex_id(16),
        parent_span_id=state.get("current_span_id"),
        content={
            "notification": {
                "title": payload.get("title"),
                "message": payload.get("message"),
                "type": payload.get("notification_type") or "info",
            }
        },
        attributes={
            "state_delta": {
                "source": "claude_code_notification",
                "notification_type": (
                    payload.get("notification_type") or "info"
                ),
            }
        },
    )

  def _permission_request(
      self, payload: dict[str, Any], state: StateStore
  ) -> None:
    self._ensure_session(payload, state)
    self.logger.log_event(
        event_type="HITL_CONFIRMATION_REQUEST",
        agent=state.get("agent"),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id"),
        trace_id=state.get("current_trace_id"),
        span_id=hex_id(16),
        parent_span_id=state.get("current_span_id"),
        content={
            "permission": payload.get("permission"),
            "tool": payload.get("tool_name"),
            "args": to_jsonable(payload.get("tool_input") or {}),
        },
        attributes={
            "session_metadata": {"source": "claude_code_permission_request"},
            "custom_tags": {"assistant": "claude_code"},
        },
    )

  def _session_end(self, payload: dict[str, Any], state: StateStore) -> None:
    self._ensure_session(payload, state)
    self._flush_late_transcript(payload, state)
    self.logger.log_event(
        event_type="STATE_DELTA",
        agent=state.get("agent"),
        session_id=state.get("session_id"),
        invocation_id=state.get("current_invocation_id")
        or state.get("last_invocation_id"),
        trace_id=state.get("current_trace_id") or state.get("last_trace_id"),
        span_id=hex_id(16),
        parent_span_id=state.get("current_span_id")
        or state.get("last_span_id"),
        content={"session_end": True},
        attributes={
            "state_delta": {
                "source": "claude_code_session_end",
                "trace_count": state.get("trace_count", 0),
                "duration_ms": max(
                    0,
                    timestamp_ms()
                    - int(state.get("session_start_ms") or timestamp_ms()),
                ),
            }
        },
    )
    state.remove()


# ----------------------------------------------------------------------------
# Entry points
# ----------------------------------------------------------------------------


def run_claude_hook(
    hook_name: str, payload: dict[str, Any] | None = None
) -> None:
  """Convenience: dispatch a single hook payload through a fresh adapter."""
  adapter = ClaudeHookBQAAAdapter()
  adapter.process(hook_name, payload or {})


def _report_invalid_config(hook_name: str, exc: Exception) -> int:
  """Handle BQAA_* settings that ``BQAAConfig.from_env()`` cannot parse."""
  if not _env_bool("BQAA_TRACE_ENABLED", True):
    return 0
  message = f"ERROR hook={hook_name}: invalid BQAA configuration: {exc}"
  print(f"bqaa-claude-hook: {message}", file=sys.stderr)
  log_to_file(
      BQAAConfig(
          project_id="",
          dataset="",
          log_file=os.environ.get("BQAA_LOG_FILE", DEFAULT_LOG_FILE),
          state_dir=os.environ.get("BQAA_STATE_DIR", DEFAULT_STATE_DIR),
      ),
      message,
  )
  return 0


def main(argv: list[str] | None = None) -> int:
  """Hook entry: read JSON payload from stdin, dispatch by hook name."""
  argv = argv if argv is not None else sys.argv[1:]
  if not argv:
    print("usage: bqaa-claude-hook <ClaudeHookName>", file=sys.stderr)
    return 2
  hook_name = argv[0]
  raw = sys.stdin.read()
  payload = safe_json_loads(raw, {}) if raw.strip() else {}
  try:
    config = BQAAConfig.from_env()
  except (InvalidConfigError, ValueError, Exception) as exc:
    return _report_invalid_config(hook_name, exc)
  if not config.enabled:
    return 0
  try:
    ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(config)).process(
        hook_name, payload
    )
  except Exception as exc:
    log_to_file(
        config, f"ERROR hook={hook_name}: {exc}\n{traceback.format_exc()}"
    )
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
