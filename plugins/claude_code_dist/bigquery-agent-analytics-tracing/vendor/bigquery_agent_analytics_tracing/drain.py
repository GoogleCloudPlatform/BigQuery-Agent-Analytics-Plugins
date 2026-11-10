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

"""Background drainer for the BQAA spool."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import fcntl
import fnmatch
import json
import os
from pathlib import Path
import random
import sys
import time
import traceback
from typing import Any, Iterator

# Storage Write API channels share gRPC state across forks; matches ADK
# plugin behavior and keeps the channel safe if the host agent forks us.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "1")

from ._utils import ensure_private_dir  # noqa: E402  (after env tweak)
from ._utils import log_to_file  # noqa: E402
from ._utils import PrivateDir  # noqa: E402
from ._utils import safe_json_loads  # noqa: E402
from ._writer_identity import DEFAULT_WRITER_LABEL  # noqa: E402
from .config import BQAAConfig  # noqa: E402
from .config import DEFAULT_LOG_FILE  # noqa: E402
from .config import DEFAULT_STATE_DIR  # noqa: E402
from .schema import bq_schema  # noqa: E402

_RETRY_MAX_BACKOFF_SECONDS = 3600.0
_RETRY_BASE_SECONDS = 5.0


@dataclass
class _RetryConfig:
  max_retries: int = 3
  initial_delay: float = 1.0
  multiplier: float = 2.0
  max_delay: float = 10.0


@dataclass
class _WriteOutcome:
  status: str  # "ok", "retryable", "permanent", or "partial"
  error: str = ""
  row_outcomes: dict[int, tuple[str, str]] | None = None


@dataclass
class _Envelope:
  path: Path
  config_dict: dict[str, Any]
  row: dict[str, Any]


class _SkippedEnvelope(Exception):
  """A spooled entry that is not a regular file we own."""


# Entries already reported as skipped: each is logged once per drainer.
_reported_skips: set[str] = set()


def _retry_sidecar_name(spool_name: str) -> str:
  return f"{spool_name}.retry"


def _unlink_retry(spool: PrivateDir, spool_name: str) -> None:
  try:
    spool.unlink(_retry_sidecar_name(spool_name))
  except (FileNotFoundError, OSError):
    pass


def _read_retry_meta(spool: PrivateDir, spool_name: str) -> dict[str, Any] | None:
  """Read ``<spool_name>.retry`` if present; None if absent or corrupt."""
  rname = _retry_sidecar_name(spool_name)
  try:
    fd = spool.open(rname, os.O_RDONLY)
    with os.fdopen(fd, "r", encoding="utf-8", errors="replace") as handle:
      raw = handle.read()
  except FileNotFoundError:
    return None
  except OSError:
    return None
  parsed = safe_json_loads(raw, None)
  if not isinstance(parsed, dict):
    return None
  return parsed


def _write_retry_meta(
    spool: PrivateDir, spool_name: str, meta: dict[str, Any]
) -> None:
  rname = _retry_sidecar_name(spool_name)
  tmp_name = f".{rname}.{os.getpid()}.tmp"
  fd = spool.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
  with os.fdopen(fd, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(meta, sort_keys=True))
  spool.replace(tmp_name, rname)


def _compute_backoff_seconds(attempts: int, config: BQAAConfig) -> float:
  override = getattr(config, "drain_retry_backoff_seconds", None)
  if override is not None:
    return max(0.0, float(override))
  exp = min(max(0, int(attempts) - 1), 16)
  return min(
      _RETRY_MAX_BACKOFF_SECONDS,
      _RETRY_BASE_SECONDS * (2 ** exp),
  )


def _extract_envelope_config(data: dict[str, Any]) -> dict[str, Any]:
  """Return destination config dict from a spool envelope."""
  cfg = data.get("config")
  if isinstance(cfg, dict) and any(
      cfg.get(k) for k in ("project_id", "dataset", "table")
  ):
    return cfg
  bqaa = data.get("_bqaa")
  if isinstance(bqaa, dict) and isinstance(bqaa.get("config"), dict):
    return bqaa["config"]
  return cfg if isinstance(cfg, dict) else {}


def is_destination_mismatch(
    envelope_cfg: dict[str, Any], config: BQAAConfig
) -> bool:
  """Return True if an envelope embeds a different non-empty destination."""
  for key, expected in (
      ("project_id", config.project_id),
      ("dataset", config.dataset),
      ("table", config.table),
  ):
    raw_val = envelope_cfg.get(key)
    val = (
        raw_val.strip()
        if isinstance(raw_val, str)
        else (str(raw_val).strip() if raw_val is not None else "")
    )
    if not val:
      continue
    if val != expected:
      return True
  return False


def _quarantine(
    spool: PrivateDir,
    name: str,
    reason: str,
    *,
    subdir: str | None = None,
) -> None:
  try:
    with spool.subdir("dead-letter") as dead:
      if subdir:
        with dead.subdir(subdir) as sub:
          try:
            spool.replace(name, f"{Path(name).stem}.{reason}.json", sub)
          except (FileNotFoundError, OSError):
            pass
      else:
        try:
          spool.replace(name, f"{Path(name).stem}.{reason}.json", dead)
        except (FileNotFoundError, OSError):
          pass
  finally:
    _unlink_retry(spool, name)


@contextlib.contextmanager
def _try_acquire_pidfile(pidfile: Path) -> Iterator[int | None]:
  """Acquire the drainer pidfile non-blockingly. Yields None if held."""
  with ensure_private_dir(pidfile.parent) as directory:
    fd = directory.open(pidfile.name, os.O_CREAT | os.O_RDWR)
  try:
    try:
      fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
      yield None
      return
    try:
      os.ftruncate(fd, 0)
      os.write(fd, str(os.getpid()).encode())
      os.fsync(fd)
    except OSError:
      pass
    try:
      yield fd
    finally:
      try:
        fcntl.flock(fd, fcntl.LOCK_UN)
      except OSError:
        pass
  finally:
    try:
      os.close(fd)
    except OSError:
      pass


def _list_pending(spool: PrivateDir) -> list[str]:
  """Envelope names, oldest first. ``_read_envelope`` vets each one."""
  names = spool.names()
  return sorted(n for n in names if fnmatch.fnmatchcase(n, "event-*.json"))


def _read_envelope(
    spool: PrivateDir, name: str, config: BQAAConfig
) -> _Envelope | None:
  """Parse one envelope; quarantine if corrupt/mismatched/missing-config."""
  try:
    fd = spool.open(name, os.O_RDONLY)
    with os.fdopen(fd, encoding="utf-8") as handle:
      raw = handle.read()
  except FileNotFoundError:
    _unlink_retry(spool, name)
    return None
  except OSError as exc:  # A symlink, or not a regular file of ours.
    raise _SkippedEnvelope(f"{spool.path / name}: {exc}") from exc
  try:
    data = json.loads(raw)
  except json.JSONDecodeError:
    _quarantine(spool, name, reason="corrupt-json")
    return None
  if not isinstance(data, dict):
    _quarantine(spool, name, reason="corrupt-json")
    return None

  env_cfg = _extract_envelope_config(data)
  if is_destination_mismatch(env_cfg, config):
    dest_desc = (
        f"{env_cfg.get('project_id')}.{env_cfg.get('dataset')}."
        f"{env_cfg.get('table')}"
    )
    expected_desc = f"{config.project_id}.{config.dataset}.{config.table}"
    log_to_file(
        config,
        f"DRAINER destination_mismatch file={name} envelope_dest={dest_desc}"
        f" expected_dest={expected_desc}",
    )
    _quarantine(
        spool, name, reason="destination-mismatch", subdir="destination-mismatch"
    )
    return None

  row = data.get("row")
  if not isinstance(row, dict):
    row = {}
  return _Envelope(
      path=spool.path / name,
      config_dict=env_cfg,
      row=row,
  )


def _group_envelopes(
    envelopes: list[_Envelope],
    config: BQAAConfig,
) -> dict[tuple, list[_Envelope]]:
  """Group by configured destination + writer label + effective auto-create.

  Legacy envelopes with empty config fall back to ``config`` destination.
  Envelope flags may opt down from ``config`` (e.g. ``auto_create_table=False``)
  but never escalate creation privileges above ``config``.
  """
  grouped: dict[tuple, list[_Envelope]] = {}
  for env in envelopes:
    cfg = env.config_dict
    auto_create_table = cfg.get("auto_create_table")
    auto_create_dataset = cfg.get("auto_create_dataset")
    eff_table = bool(config.auto_create_table) and (
        True if auto_create_table is None else bool(auto_create_table)
    )
    eff_dataset = bool(config.auto_create_dataset) and bool(
        auto_create_dataset
    )
    key = (
        config.project_id,
        config.dataset,
        config.table,
        cfg.get("location") or config.location,
        cfg.get("writer_label") or config.writer_label or DEFAULT_WRITER_LABEL,
        eff_table,
        eff_dataset,
    )
    grouped.setdefault(key, []).append(env)
  return grouped


def _is_permanent_write_exception(exc: Exception) -> bool:
  """True for 400 BadRequest / schema rejection; False for 404/429/5xx/net."""
  code = getattr(exc, "code", None)
  if code == 400:
    return True
  name = type(exc).__name__
  if name == "BadRequest":
    return True
  msg = str(exc)
  return msg.startswith("400 ") or "Bad Request" in msg


def _is_not_found_error(exc: Exception) -> bool:
  """True for HTTP 404 / NotFound / gRPC NOT_FOUND exceptions."""
  code = getattr(exc, "code", None)
  if callable(code):
    try:
      code = code()
    except Exception:
      code = None
  if code == 404 or getattr(code, "name", None) == "NOT_FOUND":
    return True
  grpc_code = getattr(exc, "grpc_status_code", None)
  if getattr(grpc_code, "name", None) == "NOT_FOUND":
    return True
  if type(exc).__name__ == "NotFound":
    return True
  msg = str(exc)
  return (
      msg.startswith("404 ")
      or "404 Not Found" in msg
      or "404 Not found" in msg
      or "NOT_FOUND" in msg
  )


def _is_permanent_row_errors(errors: list[Any]) -> bool:
  """Classify ``insert_rows_json`` row error lists as permanent vs transient."""
  transient_reasons = frozenset(
      {"timeout", "backendError", "internalError", "rateLimitExceeded", "quotaExceeded", "unavailable"}
  )
  saw_any_reason = False
  for entry in errors:
    if not isinstance(entry, dict):
      continue
    for item in entry.get("errors") or []:
      if isinstance(item, dict):
        reason = str(item.get("reason") or "")
        if reason:
          saw_any_reason = True
          if reason not in transient_reasons and reason != "stopped":
            return True
  if saw_any_reason:
    return False
  msg = str(errors)
  return "invalid" in msg.lower()


# ---------------------------------------------------------------------------
# Storage Write API path
# ---------------------------------------------------------------------------


def _storage_write_available() -> bool:
  try:
    from google.cloud.bigquery_storage_v1 import types as _types  # noqa: F401
    from google.cloud.bigquery_storage_v1.services.big_query_write.async_client import BigQueryWriteAsyncClient  # noqa: F401
    import pyarrow  # noqa: F401
  except Exception:
    return False
  return True


def _arrow_schema():
  import pyarrow as pa

  object_ref_type = pa.struct(
      [
          pa.field("uri", pa.string()),
          pa.field("version", pa.string()),
          pa.field("authorizer", pa.string()),
          pa.field("details", pa.string()),
      ]
  )
  content_part_type = pa.struct(
      [
          pa.field("mime_type", pa.string()),
          pa.field("uri", pa.string()),
          pa.field("object_ref", object_ref_type),
          pa.field("text", pa.string()),
          pa.field("part_index", pa.int64()),
          pa.field("part_attributes", pa.string()),
          pa.field("storage_mode", pa.string()),
      ]
  )
  return pa.schema(
      [
          pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
          pa.field("event_type", pa.string()),
          pa.field("agent", pa.string()),
          pa.field("session_id", pa.string()),
          pa.field("invocation_id", pa.string()),
          pa.field("user_id", pa.string()),
          pa.field("trace_id", pa.string()),
          pa.field("span_id", pa.string()),
          pa.field("parent_span_id", pa.string()),
          pa.field("content", pa.string()),
          pa.field("content_parts", pa.list_(content_part_type)),
          pa.field("attributes", pa.string()),
          pa.field("latency_ms", pa.string()),
          pa.field("status", pa.string()),
          pa.field("error_message", pa.string()),
          pa.field("is_truncated", pa.bool_()),
      ]
  )


def _row_to_arrow_dict(row: dict[str, Any]) -> dict[str, Any]:
  """Adapt a spooled row to the Arrow column shape."""
  from datetime import datetime
  from datetime import timezone

  ts_raw = row.get("timestamp")
  if isinstance(ts_raw, str):
    try:
      ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except ValueError:
      ts = datetime.now(timezone.utc)
  elif isinstance(ts_raw, datetime):
    ts = ts_raw
  else:
    ts = datetime.now(timezone.utc)
  if ts.tzinfo is None:
    ts = ts.replace(tzinfo=timezone.utc)

  parts = []
  for part in row.get("content_parts") or []:
    ref = part.get("object_ref")
    if isinstance(ref, dict):
      details = ref.get("details")
      if details is not None and not isinstance(details, str):
        ref = dict(ref)
        ref["details"] = json.dumps(details, sort_keys=True)
    part_attrs = part.get("part_attributes")
    if part_attrs is not None and not isinstance(part_attrs, str):
      part_attrs = json.dumps(part_attrs, sort_keys=True)
    parts.append(
        {
            "mime_type": part.get("mime_type"),
            "uri": part.get("uri"),
            "object_ref": ref,
            "text": part.get("text"),
            "part_index": part.get("part_index"),
            "part_attributes": part_attrs,
            "storage_mode": part.get("storage_mode"),
        }
    )

  def _as_json(value: Any) -> str | None:
    if value is None:
      return None
    if isinstance(value, str):
      return value
    return json.dumps(value, sort_keys=True, default=str)

  return {
      "timestamp": ts,
      "event_type": row.get("event_type"),
      "agent": row.get("agent"),
      "session_id": row.get("session_id"),
      "invocation_id": row.get("invocation_id"),
      "user_id": row.get("user_id"),
      "trace_id": row.get("trace_id"),
      "span_id": row.get("span_id"),
      "parent_span_id": row.get("parent_span_id"),
      "content": _as_json(row.get("content")),
      "content_parts": parts,
      "attributes": _as_json(row.get("attributes")),
      "latency_ms": _as_json(row.get("latency_ms")),
      "status": row.get("status"),
      "error_message": row.get("error_message"),
      "is_truncated": bool(row.get("is_truncated")),
  }


async def _write_batch_storage_api(
    *,
    project: str,
    dataset: str,
    table: str,
    location: str | None,
    rows: list[dict[str, Any]],
    retry: _RetryConfig,
    config: BQAAConfig,
    writer_label: str,
) -> _WriteOutcome:
  """Write a batch via the Storage Write API."""
  from google.api_core.exceptions import InternalServerError
  from google.api_core.exceptions import ServiceUnavailable
  from google.api_core.exceptions import TooManyRequests
  from google.cloud.bigquery_storage_v1 import types as bq_storage_types
  from google.cloud.bigquery_storage_v1.services.big_query_write.async_client import BigQueryWriteAsyncClient
  import pyarrow as pa

  del location

  schema = _arrow_schema()
  arrow_rows = [_row_to_arrow_dict(r) for r in rows]
  try:
    record_batch = pa.RecordBatch.from_pylist(arrow_rows, schema=schema)
  except Exception as exc:
    log_to_file(config, f"DRAINER arrow_serialize_failed: {exc}")
    return _WriteOutcome("retryable", str(exc))

  serialized_schema = schema.serialize().to_pybytes()
  serialized_batch = record_batch.serialize().to_pybytes()
  write_stream = (
      f"projects/{project}/datasets/{dataset}/tables/{table}/_default"
  )

  client = BigQueryWriteAsyncClient()
  try:
    req = bq_storage_types.AppendRowsRequest(
        write_stream=write_stream,
        trace_id=writer_label,
    )
    req.arrow_rows.writer_schema.serialized_schema = serialized_schema
    req.arrow_rows.rows.serialized_record_batch = serialized_batch

    attempt = 0
    delay = retry.initial_delay
    while True:
      try:

        async def _requests_iter():
          yield req

        async def _perform():
          responses = await client.append_rows(_requests_iter())
          async for response in responses:
            error = getattr(response, "error", None)
            error_code = getattr(error, "code", None)
            if error_code and error_code != 0:
              error_message = getattr(error, "message", "Unknown error")
              # gRPC: 4=DEADLINE_EXCEEDED, 5=NOT_FOUND, 13=INTERNAL, 14=UNAVAILABLE
              if error_code in (4, 13, 14):
                raise ServiceUnavailable(error_message)
              if error_code == 5:
                raise RuntimeError(f"404 NOT_FOUND: {error_message}")
              if error_code == 3:  # INVALID_ARGUMENT
                raise ValueError(
                    f"400 Bad Request (INVALID_ARGUMENT): {error_message}"
                )
              raise RuntimeError(
                  f"storage_write_error code={error_code}: {error_message}"
              )

        await asyncio.wait_for(_perform(), timeout=30.0)
        return _WriteOutcome("ok")

      except (
          ServiceUnavailable,
          TooManyRequests,
          InternalServerError,
          asyncio.TimeoutError,
      ) as exc:
        attempt += 1
        if attempt > retry.max_retries:
          log_to_file(
              config,
              f"DRAINER batch_deferred_after_{retry.max_retries + 1}: {exc}",
          )
          return _WriteOutcome("retryable", str(exc))
        sleep_for = min(delay * (1 + random.random()), retry.max_delay)
        log_to_file(
            config,
            f"DRAINER retry attempt={attempt} sleep={sleep_for:.2f} err={exc}",
        )
        await asyncio.sleep(sleep_for)
        delay *= retry.multiplier
      except Exception as exc:
        if _is_not_found_error(exc):
          return _WriteOutcome("retryable", str(exc))
        log_to_file(
            config,
            f"DRAINER unexpected_error: {exc}\n{traceback.format_exc()}",
        )
        if _is_permanent_write_exception(exc) or isinstance(exc, ValueError):
          return _WriteOutcome("permanent", str(exc))
        return _WriteOutcome("retryable", str(exc))
  finally:
    try:
      transport = getattr(client, "transport", None)
      close = getattr(transport, "close", None) if transport else None
      if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
          await result
    except Exception:
      pass


# ---------------------------------------------------------------------------
# insert_rows_json fallback
# ---------------------------------------------------------------------------


def _ensure_table_exists(
    *,
    project: str,
    dataset: str,
    table: str,
    location: str | None,
    auto_create_table: bool,
    auto_create_dataset: bool,
) -> None:
  from google.cloud import bigquery
  from google.cloud.exceptions import NotFound

  client = bigquery.Client(project=project, location=location)
  dataset_id = f"{project}.{dataset}"
  if auto_create_dataset:
    try:
      client.get_dataset(dataset_id)
    except NotFound:
      ds = bigquery.Dataset(dataset_id)
      if location:
        ds.location = location
      client.create_dataset(ds)
  table_id = f"{project}.{dataset}.{table}"
  try:
    client.get_table(table_id)
    return
  except NotFound:
    if not auto_create_table:
      raise
  bq_table = bigquery.Table(table_id, schema=bq_schema(bigquery))
  bq_table.time_partitioning = bigquery.TimePartitioning(
      type_=bigquery.TimePartitioningType.DAY,
      field="timestamp",
  )
  bq_table.clustering_fields = ["event_type", "agent", "user_id"]
  bq_table.labels = {"adk_schema_version": "1"}
  client.create_table(bq_table)


def _write_batch_insert_rows_json(
    *,
    project: str,
    dataset: str,
    table: str,
    location: str | None,
    rows: list[dict[str, Any]],
    retry: _RetryConfig,
    config: BQAAConfig,
    auto_create_table: bool,
    auto_create_dataset: bool,
) -> _WriteOutcome:
  from google.api_core.exceptions import InternalServerError
  from google.api_core.exceptions import ServiceUnavailable
  from google.api_core.exceptions import TooManyRequests
  from google.cloud import bigquery

  try:
    _ensure_table_exists(
        project=project,
        dataset=dataset,
        table=table,
        location=location,
        auto_create_table=auto_create_table,
        auto_create_dataset=auto_create_dataset,
    )
  except Exception as exc:
    log_to_file(config, f"DRAINER ensure_table_failed: {exc}")
    if _is_permanent_write_exception(exc):
      return _WriteOutcome("permanent", str(exc))
    return _WriteOutcome("retryable", str(exc))

  client = bigquery.Client(project=project, location=location)
  table_id = f"{project}.{dataset}.{table}"
  attempt = 0
  delay = retry.initial_delay
  while True:
    try:
      errors = client.insert_rows_json(table_id, rows)
      if errors:
        log_to_file(config, f"DRAINER insert_rows_json errors: {errors}")
        if isinstance(errors, (list, tuple)) and all(
            isinstance(e, dict)
            and isinstance(e.get("index"), int)
            and not isinstance(e.get("index"), bool)
            and 0 <= e["index"] < len(rows)
            for e in errors
        ):
          grouped_errors: dict[int, list[dict[str, Any]]] = {}
          for e in errors:
            grouped_errors.setdefault(e["index"], []).append(e)
          row_outcomes: dict[int, tuple[str, str]] = {}
          for idx, entries in grouped_errors.items():
            row_status = (
                "permanent" if _is_permanent_row_errors(entries) else "retryable"
            )
            row_outcomes[idx] = (row_status, str(entries))
          return _WriteOutcome("partial", str(errors), row_outcomes=row_outcomes)
        err_list = list(errors) if isinstance(errors, (list, tuple)) else [errors]
        status = "permanent" if _is_permanent_row_errors(err_list) else "retryable"
        return _WriteOutcome(status, str(errors))
      return _WriteOutcome("ok")
    except (
        ServiceUnavailable,
        TooManyRequests,
        InternalServerError,
    ) as exc:
      attempt += 1
      if attempt > retry.max_retries:
        log_to_file(
            config,
            f"DRAINER batch_deferred_after_{retry.max_retries + 1}: {exc}",
        )
        return _WriteOutcome("retryable", str(exc))
      sleep_for = min(delay * (1 + random.random()), retry.max_delay)
      time.sleep(sleep_for)
      delay *= retry.multiplier
    except Exception as exc:
      log_to_file(
          config,
          f"DRAINER unexpected_error: {exc}\n{traceback.format_exc()}",
      )
      if _is_permanent_write_exception(exc):
        return _WriteOutcome("permanent", str(exc))
      return _WriteOutcome("retryable", str(exc))


# ---------------------------------------------------------------------------
# Drain loop
# ---------------------------------------------------------------------------


def _defer_or_expire(
    spool: PrivateDir,
    name: str,
    config: BQAAConfig,
    error_message: str,
) -> int:
  """Defer ``name`` with ``<name>.retry``, or quarantine if retry age expired.

  Returns 1 if resolved via expiry quarantine, 0 if deferred in spool.
  """
  now = time.time()
  existing = _read_retry_meta(spool, name)
  if existing is not None:
    try:
      first_failed_at = float(existing.get("first_failed_at", now))
      attempts = max(0, int(existing.get("attempts", 0))) + 1
    except (TypeError, ValueError):
      first_failed_at = now
      attempts = 1
  else:
    first_failed_at = now
    attempts = 1

  max_age = float(getattr(config, "drain_max_retry_age_seconds", 86400.0))
  if now - first_failed_at >= max_age:
    log_to_file(
        config,
        f"DRAINER retry_expired file={name} age={now - first_failed_at:.1f}s"
        f" max_age={max_age:.1f}s last_error={error_message}",
    )
    _quarantine(spool, name, reason="write-failed")
    return 1

  backoff = _compute_backoff_seconds(attempts, config)
  meta = {
      "first_failed_at": first_failed_at,
      "attempts": attempts,
      "next_retry_at": now + backoff,
      "last_error": str(error_message)[:500],
  }
  try:
    _write_retry_meta(spool, name, meta)
  except OSError:
    pass
  return 0


async def _drain_group_async(
    *,
    key: tuple,
    envelopes: list[_Envelope],
    retry: _RetryConfig,
    config: BQAAConfig,
    use_storage_api: bool,
    spool: PrivateDir,
) -> int:
  """Drain one destination group; return count of resolved (non-deferred) files."""
  (
      project,
      dataset,
      table,
      location,
      writer_label,
      auto_create_table,
      auto_create_dataset,
  ) = key
  if not project or not dataset or not table:
    for env in envelopes:
      _quarantine(spool, env.path.name, reason="missing-config")
    return len(envelopes)

  rows = [env.row for env in envelopes]
  outcome = _WriteOutcome("retryable")
  if use_storage_api:
    outcome = await _write_batch_storage_api(
        project=project,
        dataset=dataset,
        table=table,
        location=location,
        rows=rows,
        retry=retry,
        config=config,
        writer_label=writer_label or DEFAULT_WRITER_LABEL,
    )
    if outcome.status != "ok":
      log_to_file(
          config,
          "DRAINER storage_api_failed, falling back to insert_rows_json",
      )

  if outcome.status != "ok":
    outcome = await asyncio.to_thread(
        _write_batch_insert_rows_json,
        project=project,
        dataset=dataset,
        table=table,
        location=location,
        rows=rows,
        retry=retry,
        config=config,
        auto_create_table=auto_create_table,
        auto_create_dataset=auto_create_dataset,
    )

  if outcome.status == "ok":
    for env in envelopes:
      try:
        spool.unlink(env.path.name)
      except FileNotFoundError:
        pass
      _unlink_retry(spool, env.path.name)
    return len(envelopes)

  if outcome.row_outcomes is not None:
    resolved = 0
    has_retryable = False
    first_retry_err = ""
    for idx, env in enumerate(envelopes):
      row_res = outcome.row_outcomes.get(idx)
      if row_res is None:
        try:
          spool.unlink(env.path.name)
        except FileNotFoundError:
          pass
        _unlink_retry(spool, env.path.name)
        resolved += 1
      elif row_res[0] == "permanent":
        _quarantine(spool, env.path.name, reason="write-failed")
        resolved += 1
      else:
        has_retryable = True
        if not first_retry_err:
          first_retry_err = row_res[1]
        resolved += _defer_or_expire(spool, env.path.name, config, row_res[1])
    if has_retryable:
      log_to_file(
          config,
          f"DRAINER partial batch retryable failure: {first_retry_err}",
      )
    return resolved

  if outcome.status == "permanent":
    for env in envelopes:
      _quarantine(spool, env.path.name, reason="write-failed")
    return len(envelopes)

  resolved = 0
  for env in envelopes:
    resolved += _defer_or_expire(
        spool, env.path.name, config, outcome.error or "transient write error"
    )
  return resolved


async def _drain_once(config: BQAAConfig, use_storage_api: bool) -> int:
  resolved = 0
  with ensure_private_dir(config.spool_dir) as spool:
    pending_names = _list_pending(spool)
    pending_set = set(pending_names)
    for extra in spool.names():
      if extra.endswith(".retry"):
        event_name = extra[:-len(".retry")]
        if event_name not in pending_set:
          _unlink_retry(spool, event_name)

    envelopes: list[_Envelope] = []
    now = time.time()
    max_age = float(getattr(config, "drain_max_retry_age_seconds", 86400.0))

    for name in pending_names:
      if len(envelopes) >= max(1, config.drain_batch_size):
        break
      retry_meta = _read_retry_meta(spool, name)
      if retry_meta is not None:
        try:
          first_failed_at = float(retry_meta.get("first_failed_at", now))
          next_retry_at = float(retry_meta.get("next_retry_at", 0.0))
        except (TypeError, ValueError):
          first_failed_at = now
          next_retry_at = 0.0
        if now - first_failed_at >= max_age and retry_meta.get("attempts", 0) >= 1:
          log_to_file(
              config,
              f"DRAINER retry_expired file={name} age={now - first_failed_at:.1f}s"
              f" max_age={max_age:.1f}s last_error={retry_meta.get('last_error', '')}",
          )
          _quarantine(spool, name, reason="write-failed")
          resolved += 1
          continue
        if config.drain_retry_backoff_seconds != 0.0 and next_retry_at > now:
          continue

      try:
        env = _read_envelope(spool, name, config)
      except _SkippedEnvelope as skipped:
        if str(skipped) not in _reported_skips:
          _reported_skips.add(str(skipped))
          log_to_file(config, f"DRAINER skipped {skipped}")
        continue
      if env is None:
        resolved += 1
        continue
      envelopes.append(env)

    if not envelopes:
      return resolved
    grouped = _group_envelopes(envelopes, config)
    retry = _RetryConfig()
    counts = await asyncio.gather(
        *(
            _drain_group_async(
                key=key,
                envelopes=batch,
                retry=retry,
                config=config,
                use_storage_api=use_storage_api,
                spool=spool,
            )
            for key, batch in grouped.items()
        )
    )
    return resolved + sum(counts)


async def _run(config: BQAAConfig) -> None:
  pidfile = Path(config.spool_dir).expanduser() / ".drainer.pid"
  use_storage_api = _storage_write_available()
  with _try_acquire_pidfile(pidfile) as fd:
    if fd is None:
      return
    idle = 0.0
    while idle < config.drain_idle_seconds:
      wrote = await _drain_once(config, use_storage_api=use_storage_api)
      if wrote == 0:
        await asyncio.sleep(config.drain_poll_seconds)
        idle += config.drain_poll_seconds
      else:
        idle = 0.0


def _report_invalid_config(exc: Exception) -> int:
  """One-line diagnostic for BQAA_* settings ``from_env()`` rejects."""
  message = f"DRAINER invalid BQAA configuration: {exc}"
  print(f"bqaa-drainer: {message}", file=sys.stderr)
  log_to_file(
      BQAAConfig(
          project_id="",
          dataset="",
          log_file=os.environ.get("BQAA_LOG_FILE", DEFAULT_LOG_FILE),
          state_dir=os.environ.get("BQAA_STATE_DIR", DEFAULT_STATE_DIR),
      ),
      message,
  )
  return 1


def main(argv: list[str] | None = None) -> int:
  del argv
  try:
    config = BQAAConfig.from_env()
  except Exception as exc:
    return _report_invalid_config(exc)
  if config.dry_run:
    return 0
  try:
    asyncio.run(_run(config))
  except (ModuleNotFoundError, ImportError) as exc:
    log_to_file(
        config,
        "DRAINER_SKIPPED BigQuery client library not installed on drainer"
        f" interpreter ({exc}); events remain in spool_dir until installed"
        " (run /bqaa-setup).",
    )
    return 0
  except Exception as exc:
    log_to_file(config, f"DRAINER fatal: {exc}\n{traceback.format_exc()}")
    return 1
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
