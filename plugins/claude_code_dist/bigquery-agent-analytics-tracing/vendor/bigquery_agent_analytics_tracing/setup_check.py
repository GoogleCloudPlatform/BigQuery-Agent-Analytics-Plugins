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

"""Advisory setup check for the BQAA tracing runtime."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterable

from ._utils import inspect_spool_status
from ._utils import PYTHON_C_DROP_WORKING_DIR
from .config import BQAAConfig
from .config import DEFAULT_LOG_FILE
from .config import DEFAULT_SPOOL_DIR

_REQUIRED_IMPORTS: tuple[tuple[str, str], ...] = (
    ("google.cloud.bigquery", "google-cloud-bigquery"),
)

_STORAGE_WRITE_IMPORTS: tuple[tuple[str, str], ...] = (
    ("google.cloud.bigquery_storage_v1", "google-cloud-bigquery-storage"),
    ("pyarrow", "pyarrow"),
)

_PACKAGE_IMPORTS: tuple[tuple[str, str], ...] = (
    ("bigquery_agent_analytics_tracing", "bigquery-agent-analytics-tracing"),
)


@dataclass
class _ImportProbe:
  module: str
  install_name: str
  ok: bool
  error: str | None = None


@dataclass
class _SetupReport:
  python_bin: str
  project_id: str | None
  dataset: str | None
  trace_enabled: bool
  dry_run: bool = False
  log_file: str = DEFAULT_LOG_FILE
  log_file_is_default: bool = True
  log_file_is_relative: bool = False
  spool_dir: str = DEFAULT_SPOOL_DIR
  spool_pending: int = 0
  spool_deferred: int = 0
  spool_dead_letter: int = 0
  spool_destination_mismatch: int = 0
  spool_last_error: str | None = None
  spool_inspect_error: str | None = None
  interpreter_error: str | None = None
  config_error: str | None = None
  package_imports: list[_ImportProbe] = field(default_factory=list)
  required_imports: list[_ImportProbe] = field(default_factory=list)
  storage_write_imports: list[_ImportProbe] = field(default_factory=list)

  @property
  def python_ok(self) -> bool:
    return self.interpreter_error is None

  @property
  def env_ok(self) -> bool:
    return bool(self.project_id) and bool(self.dataset)

  @property
  def config_ok(self) -> bool:
    return self.config_error is None

  @property
  def package_ok(self) -> bool:
    return self.python_ok and all(p.ok for p in self.package_imports)

  @property
  def required_deps_ok(self) -> bool:
    return self.python_ok and all(p.ok for p in self.required_imports)

  @property
  def storage_write_ok(self) -> bool:
    return self.python_ok and all(p.ok for p in self.storage_write_imports)

  @property
  def all_ok(self) -> bool:
    return (
        self.python_ok
        and self.env_ok
        and self.config_ok
        and self.package_ok
        and self.required_deps_ok
    )


def _resolve_python_bin() -> str:
  return os.environ.get("BQAA_PYTHON") or sys.executable


def _probe_interpreter(python_bin: str) -> str | None:
  try:
    proc = subprocess.run(
        [python_bin, "-c", "pass"],
        capture_output=True,
        text=True,
        timeout=15,
    )
  except FileNotFoundError:
    return f"interpreter not found: {python_bin}"
  except PermissionError:
    return f"interpreter not executable: {python_bin}"
  except OSError as exc:
    return f"interpreter exec error: {python_bin} ({exc})"
  except subprocess.TimeoutExpired:
    return f"interpreter timed out: {python_bin}"
  if proc.returncode != 0:
    err = (proc.stderr or proc.stdout or "non-zero exit").strip()
    err = err.splitlines()[-1] if err.splitlines() else err
    return f"interpreter exited non-zero: {err}"
  return None


def _probe_imports(
    python_bin: str, modules: Iterable[tuple[str, str]]
) -> list[_ImportProbe]:
  results: list[_ImportProbe] = []
  for module, install_name in modules:
    try:
      proc = subprocess.run(
          [python_bin, "-c", PYTHON_C_DROP_WORKING_DIR + f"import {module}"],
          capture_output=True,
          text=True,
          timeout=15,
      )
    except (FileNotFoundError, PermissionError, OSError) as exc:
      results.append(
          _ImportProbe(
              module=module,
              install_name=install_name,
              ok=False,
              error=f"interpreter unavailable: {exc}",
          )
      )
      continue
    except subprocess.TimeoutExpired:
      results.append(
          _ImportProbe(
              module=module,
              install_name=install_name,
              ok=False,
              error=f"interpreter timed out: {python_bin}",
          )
      )
      continue
    if proc.returncode == 0:
      results.append(
          _ImportProbe(module=module, install_name=install_name, ok=True)
      )
    else:
      err = (proc.stderr or proc.stdout or "import failed").strip()
      err = err.splitlines()[-1] if err.splitlines() else err
      results.append(
          _ImportProbe(
              module=module,
              install_name=install_name,
              ok=False,
              error=err,
          )
      )
  return results


def _probe_config() -> str | None:
  try:
    BQAAConfig.from_env()
  except Exception as exc:
    return str(exc) or type(exc).__name__
  return None


def _is_relative_path(value: str | None) -> bool:
  if not value:
    return False
  if value.startswith("~"):
    return value == "~" or value.startswith("~/")
  return not os.path.isabs(value)


def _tilde_user_note(value: str) -> str | None:
  if not value.startswith("~") or _is_relative_path(value):
    return None
  try:
    expanded = Path(value).expanduser()
  except RuntimeError:
    return (
        f'{value} (the user named after "~" was not found in the account'
        " database, so the runtime cannot expand the path and discards"
        " dry-run events)"
    )
  return (
      f"{value} (expanded through the account database to {expanded}, the"
      " same for every hook)"
  )


def collect_report(python_bin: str | None = None) -> _SetupReport:
  resolved = python_bin or _resolve_python_bin()
  interpreter_error = _probe_interpreter(resolved)
  if interpreter_error is not None:
    package_imports: list[_ImportProbe] = []
    required_imports: list[_ImportProbe] = []
    storage_write_imports: list[_ImportProbe] = []
  else:
    package_imports = _probe_imports(resolved, _PACKAGE_IMPORTS)
    required_imports = _probe_imports(resolved, _REQUIRED_IMPORTS)
    storage_write_imports = _probe_imports(resolved, _STORAGE_WRITE_IMPORTS)
  spool_dir = os.environ.get("BQAA_SPOOL_DIR", DEFAULT_SPOOL_DIR)
  spool_status = inspect_spool_status(spool_dir)
  return _SetupReport(
      python_bin=resolved,
      interpreter_error=interpreter_error,
      config_error=_probe_config(),
      project_id=(
          os.environ.get("BQAA_PROJECT_ID")
          or os.environ.get("GCP_PROJECT_ID")
          or os.environ.get("GOOGLE_CLOUD_PROJECT")
          or None
      ),
      dataset=(
          os.environ.get("BQAA_DATASET") or os.environ.get("BQ_DATASET") or None
      ),
      trace_enabled=(
          os.environ.get("BQAA_TRACE_ENABLED", "true").lower() == "true"
      ),
      dry_run=(os.environ.get("BQAA_DRY_RUN", "false").lower() == "true"),
      log_file=os.environ.get("BQAA_LOG_FILE", DEFAULT_LOG_FILE),
      log_file_is_default="BQAA_LOG_FILE" not in os.environ,
      log_file_is_relative=_is_relative_path(os.environ.get("BQAA_LOG_FILE")),
      spool_dir=spool_dir,
      spool_pending=int(spool_status["pending_count"]),
      spool_deferred=int(spool_status["deferred_count"]),
      spool_dead_letter=int(spool_status["dead_letter_count"]),
      spool_destination_mismatch=int(
          spool_status["destination_mismatch_count"]
      ),
      spool_last_error=spool_status["latest_error"],
      spool_inspect_error=spool_status["error"],
      package_imports=package_imports,
      required_imports=required_imports,
      storage_write_imports=storage_write_imports,
  )


def _append_config_error(lines: list[str], config_error: str) -> None:
  lines.append(f"  [INVALID] BQAA_* settings: {config_error}")
  lines.append(
      "  Fix: correct or unset that variable; the hooks cannot load"
      " their configuration and emit no trace events until then."
  )


def _append_spool_section(lines: list[str], report: _SetupReport) -> None:
  lines.append(f"Spool status ({report.spool_dir}):")
  if report.spool_inspect_error:
    lines.append(f"  [WARN] could not inspect spool: {report.spool_inspect_error}")
    return
  lines.append(
      f"  Pending: {report.spool_pending} event(s),"
      f" deferred/retrying: {report.spool_deferred},"
      f" dead-letter: {report.spool_dead_letter}"
      + (
          f" (destination-mismatch: {report.spool_destination_mismatch})"
          if report.spool_destination_mismatch
          else ""
      )
  )
  if report.spool_last_error:
    lines.append(f"  Latest retry error: {report.spool_last_error}")


def _format_report(report: _SetupReport) -> str:
  lines: list[str] = []
  ok_mark = "OK"
  miss_mark = "MISSING"

  lines.append("BQAA tracing setup check")
  lines.append("=" * 32)
  lines.append(f"Python interpreter: {report.python_bin}")
  if report.interpreter_error:
    lines.append(f"  [{miss_mark}] {report.interpreter_error}")
    lines.append(
        "  Fix: point BQAA_PYTHON at a working python3 interpreter,"
        " or unset it to use the default."
    )
  lines.append(f"BQAA_TRACE_ENABLED: {'on' if report.trace_enabled else 'off'}")
  if not report.trace_enabled:
    lines.append(
        "  Note: emission is disabled. Set BQAA_TRACE_ENABLED=true"
        " (or unset it) to enable."
    )
  lines.append(f"BQAA_DRY_RUN: {'on' if report.dry_run else 'off'}")
  if report.dry_run:
    lines.append(_dry_run_note(report))
  lines.append("")

  if report.interpreter_error:
    if report.config_error:
      _append_config_error(lines, report.config_error)
      lines.append("")
    lines.append("Import + env checks skipped: interpreter unavailable.")
    lines.append("")
    _append_spool_section(lines, report)
    lines.append("")
    lines.append("Status: ACTION NEEDED")
    return "\n".join(lines) + "\n"

  lines.append("Required env vars:")
  lines.append(
      f"  [{ok_mark if report.project_id else miss_mark}]"
      f" BQAA_PROJECT_ID = {report.project_id or '(unset)'}"
  )
  lines.append(
      f"  [{ok_mark if report.dataset else miss_mark}]"
      f" BQAA_DATASET    = {report.dataset or '(unset)'}"
  )
  if not report.env_ok:
    lines.append("")
    lines.append("Fix: export the missing variables, e.g.")
    if not report.project_id:
      lines.append("  export BQAA_PROJECT_ID=your-gcp-project")
    if not report.dataset:
      lines.append("  export BQAA_DATASET=agent_analytics")
  if report.config_error:
    lines.append("")
    _append_config_error(lines, report.config_error)
  lines.append("")

  lines.append("Package import:")
  for probe in report.package_imports:
    mark = ok_mark if probe.ok else miss_mark
    line = f"  [{mark}] import {probe.module}"
    if not probe.ok and probe.error:
      line += f" — {probe.error}"
    lines.append(line)
  if not report.package_ok:
    lines.append("")
    lines.append(
        "Fix: install the tracing wheel on BQAA_PYTHON, or point"
        " PYTHONPATH at the vendored plugin's vendor/ directory."
    )
    lines.append(
        f"  {report.python_bin} -m pip install bigquery-agent-analytics-tracing"
    )
  lines.append("")

  lines.append("Required runtime deps:")
  for probe in report.required_imports:
    mark = ok_mark if probe.ok else miss_mark
    line = f"  [{mark}] import {probe.module}"
    if not probe.ok and probe.error:
      line += f" — {probe.error}"
    lines.append(line)
  if not report.required_deps_ok:
    missing = [p.install_name for p in report.required_imports if not p.ok]
    lines.append("")
    lines.append(f"Fix: {report.python_bin} -m pip install {' '.join(missing)}")
  lines.append("")

  lines.append("Storage Write API deps (optional):")
  for probe in report.storage_write_imports:
    mark = ok_mark if probe.ok else miss_mark
    line = f"  [{mark}] import {probe.module}"
    if not probe.ok and probe.error:
      line += f" — {probe.error}"
    lines.append(line)
  if not report.storage_write_ok:
    missing = [p.install_name for p in report.storage_write_imports if not p.ok]
    lines.append("")
    lines.append(
        "Optional fix for the lower-latency Storage Write path"
        " (drainer falls back to insert_rows_json without these):"
    )
    lines.append(f"  {report.python_bin} -m pip install {' '.join(missing)}")
  lines.append("")

  _append_spool_section(lines, report)
  lines.append("")

  lines.append("Status: " + ("READY" if report.all_ok else "ACTION NEEDED"))
  return "\n".join(lines) + "\n"


def _dry_run_note(report: _SetupReport) -> str:
  live = " Set BQAA_DRY_RUN=false (or unset it) for live BigQuery writes."
  if not report.trace_enabled:
    return (
        "  Note: dry-run mode is active, but nothing is emitted while"
        " BQAA_TRACE_ENABLED is off."
    )
  if not report.log_file:
    return (
        "  Note: dry-run mode is active, but BQAA_LOG_FILE is empty so"
        " dry-run events are discarded (not written to a file or"
        " BigQuery). Unset BQAA_LOG_FILE (or set a file path) to record"
        " dry-run events, or set BQAA_DRY_RUN=false (or unset it) for"
        " live BigQuery writes."
    )
  if report.log_file_is_default:
    target = (
        f"the default log file ({report.log_file} for this shell's"
        " TMPDIR; the hooks resolve it from their own TMPDIR, which"
        " differs when shell commands run sandboxed. Set BQAA_LOG_FILE"
        " to an absolute path to pin it)"
    )
  elif report.log_file_is_relative:
    target = (
        f"{report.log_file} (not an absolute path: each hook resolves it"
        ' against its own working directory, and "~" or "~/" against its'
        " own HOME. Set BQAA_LOG_FILE to an absolute path to pin it)"
    )
  else:
    target = _tilde_user_note(report.log_file) or report.log_file
  return (
      "  Note: dry-run mode is active. The hooks try to write dry-run"
      f" events to {target} instead of BigQuery, and drop them silently"
      " if the runtime refuses that path (for example a read-only file,"
      " a symlink, or a file in a directory that your group or other"
      f" users can write).{live}"
  )


def _report_to_json(report: _SetupReport) -> str:
  return json.dumps(
      {
          **asdict(report),
          "python_ok": report.python_ok,
          "env_ok": report.env_ok,
          "config_ok": report.config_ok,
          "package_ok": report.package_ok,
          "required_deps_ok": report.required_deps_ok,
          "storage_write_ok": report.storage_write_ok,
          "all_ok": report.all_ok,
      },
      indent=2,
      sort_keys=True,
  )


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  parser.add_argument(
      "--json",
      action="store_true",
      help="Emit a JSON report instead of human-readable text.",
  )
  args = parser.parse_args(argv)
  reconfigure = getattr(sys.stdout, "reconfigure", None)
  if reconfigure is not None:
    reconfigure(errors="backslashreplace")
  report = collect_report()
  if args.json:
    sys.stdout.write(_report_to_json(report) + "\n")
  else:
    sys.stdout.write(_format_report(report))
  return 0 if report.all_ok else 1


if __name__ == "__main__":
  raise SystemExit(main())
