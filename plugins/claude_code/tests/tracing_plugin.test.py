#!/usr/bin/env python3
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

"""Tracing plugin tests — source/dist parity, sys.path isolation, and
offline end-to-end hook/drainer/setup tests.

Uses only the Python standard library (unittest, subprocess, json, os,
tempfile, importlib, pathlib, io, shutil) so the suite runs on Python 3.10
through 3.14 without pip installs.  No network or GCP calls are made.

Run:
    python plugins/claude_code/tests/tracing_plugin.test.py
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import tracemalloc
import types
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

# --- Paths -------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "plugins" / "claude_code"
DIST_DIR = (
    REPO_ROOT
    / "plugins"
    / "claude_code_dist"
    / "bigquery-agent-analytics-tracing"
)


def _ignore_claude_code_src(src: str, names: list[str]) -> set[str]:
    """Ignore Python bytecode everywhere, and ignore vendor/ only at SRC_DIR root."""
    ignored = set(shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")(src, names))
    if Path(src).resolve() == SRC_DIR.resolve() and "vendor" in names:
        ignored.add("vendor")
    return ignored
VENDOR_DIR = DIST_DIR / "vendor" / "bigquery_agent_analytics_tracing"
CATALOG_PATH = REPO_ROOT / ".claude-plugin" / "marketplace.json"

# Add the vendored package to sys.path so we can import it directly.
sys.path.insert(0, str(VENDOR_DIR.parent))


# =============================================================================
# 1. Source <-> Dist parity
# =============================================================================


class TestSourceDistParity(unittest.TestCase):
    """Byte-for-byte and executable-mode parity between source and dist."""

    @classmethod
    def setUpClass(cls):
        cls.src = SRC_DIR
        cls.dist = DIST_DIR

    # Files that must be byte-identical between source and dist.
    BYTE_PARITY_FILES = [
        "hooks/common.sh",
    ]

    # Files that must exist in both source and dist (content may differ
    # because the dist is a released snapshot that can lag the source).
    PARITY_EXISTENCE_FILES = [
        "hooks/common.sh",
        "hooks/notification.sh",
        "hooks/permission_request.sh",
        "hooks/post_tool_use.sh",
        "hooks/post_tool_use_failure.sh",
        "hooks/pre_tool_use.sh",
        "hooks/session_end.sh",
        "hooks/session_start.sh",
        "hooks/stop.sh",
        "hooks/stop_failure.sh",
        "hooks/subagent_stop.sh",
        "hooks/user_prompt_submit.sh",
        "scripts/run_setup_check.sh",
        "MARKETPLACE.md",
        "README.md",
        "commands/bqaa-setup.md",
    ]

    def test_byte_parity_common_sh(self):
        """hooks/common.sh must be byte-identical between source and dist."""
        for rel in self.BYTE_PARITY_FILES:
            with self.subTest(file=rel):
                src_path = self.src / rel
                dist_path = self.dist / rel
                self.assertTrue(src_path.is_file(), f"source missing: {rel}")
                self.assertTrue(dist_path.is_file(), f"dist missing: {rel}")
                self.assertEqual(
                    src_path.read_bytes(),
                    dist_path.read_bytes(),
                    f"byte mismatch: {rel}",
                )

    def test_file_existence_parity(self):
        """All expected files must exist in both source and dist."""
        for rel in self.PARITY_EXISTENCE_FILES:
            with self.subTest(file=rel):
                src_path = self.src / rel
                dist_path = self.dist / rel
                self.assertTrue(src_path.is_file(), f"source missing: {rel}")
                self.assertTrue(dist_path.is_file(), f"dist missing: {rel}")
                self.assertTrue(src_path.stat().st_size > 0,
                                f"source file empty: {rel}")
                self.assertTrue(dist_path.stat().st_size > 0,
                                f"dist file empty: {rel}")

    def test_executable_mode_parity(self):
        """Hook scripts and run_setup_check.sh executable bits must match."""
        for rel in self.PARITY_EXISTENCE_FILES:
            with self.subTest(file=rel):
                if not rel.endswith(".sh"):
                    continue
                src_mode = stat.S_IMODE(os.stat(self.src / rel).st_mode)
                dist_mode = stat.S_IMODE(os.stat(self.dist / rel).st_mode)
                self.assertEqual(
                    src_mode & 0o111,
                    dist_mode & 0o111,
                    f"executable bit mismatch: {rel}",
                )

    def test_plugin_json_fields(self):
        """plugin.json fields (except version) must match between source/dist."""
        src_json = json.loads(
            (self.src / ".claude-plugin" / "plugin.json").read_text()
        )
        dist_json = json.loads(
            (self.dist / ".claude-plugin" / "plugin.json").read_text()
        )
        for field in ("hooks", "name", "description", "author",
                      "repository", "homepage", "license", "keywords"):
            with self.subTest(field=field):
                self.assertEqual(
                    src_json[field], dist_json[field],
                    f"plugin.json {field} mismatch",
                )
        self.assertEqual(src_json["version"], "0.0.0+local")
        self.assertNotEqual(src_json["version"], dist_json["version"])

    def test_dist_version_matches_catalog(self):
        """Dist plugin.json version must match marketplace.json."""
        dist_json = json.loads(
            (self.dist / ".claude-plugin" / "plugin.json").read_text()
        )
        catalog = json.loads(CATALOG_PATH.read_text())
        tracing_entry = next(
            e for e in catalog["plugins"]
            if e["name"] == "bigquery-agent-analytics-tracing"
        )
        self.assertEqual(dist_json["version"], tracing_entry["version"])

    def test_dist_version_matches_metadata(self):
        """Dist plugin.json version must match the .dist-info METADATA."""
        dist_json = json.loads(
            (self.dist / ".claude-plugin" / "plugin.json").read_text()
        )
        dist_infos = list((DIST_DIR / "vendor").glob("bigquery_agent_analytics_tracing-*.dist-info"))
        self.assertEqual(len(dist_infos), 1, f"expected 1 dist-info dir, got {dist_infos}")
        expected_info = DIST_DIR / "vendor" / f"bigquery_agent_analytics_tracing-{dist_json['version']}.dist-info"
        self.assertEqual(dist_infos[0], expected_info)
        metadata_path = expected_info / "METADATA"
        metadata = metadata_path.read_text(encoding="utf-8")
        self.assertIn(f"Version: {dist_json['version']}", metadata.splitlines())

    def test_package_version_attribute(self):
        """bigquery_agent_analytics_tracing.__version__ must resolve."""
        import bigquery_agent_analytics_tracing
        self.assertTrue(bigquery_agent_analytics_tracing.__version__)

    def test_importlib_metadata_version(self):
        """importlib.metadata.version must resolve to the installed version."""
        try:
            from importlib import metadata
            version = metadata.version("bigquery-agent-analytics-tracing")
            self.assertTrue(version)
        except metadata.PackageNotFoundError:
            # Vendored install without wheel — fallback is "0.0.0+local".
            pass


# =============================================================================
# 2. Working-directory sys.path isolation & BQAA_TRACE_ENABLED
# =============================================================================


class TestSysPathIsolation(unittest.TestCase):
    """Hook scripts must not import poison modules from the working directory.

    Tests that sys.path is scrubbed so json.py, sitecustomize.py, and
    bigquery_agent_analytics_tracing/__init__.py in the cwd are never
    imported.
    """

    @classmethod
    def setUpClass(cls):
        cls.base_env = {
            "BQAA_PROJECT_ID": "test-project",
            "BQAA_DATASET": "test_dataset",
            "BQAA_DRY_RUN": "true",
            "BQAA_AUTO_DRAIN": "false",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": os.environ.get("HOME", "/tmp"),
        }

    def _make_poison_dir(self):
        """Create a temp dir with poison modules that would crash on import
        if the working directory leaked onto sys.path."""
        poison_dir = tempfile.mkdtemp(prefix="bqaa_poison_")
        self.addCleanup(shutil.rmtree, poison_dir, ignore_errors=True)
        # json.py that crashes if imported instead of stdlib json.
        (Path(poison_dir) / "json.py").write_text(
            "raise SystemExit(99)\n"
        )
        # sitecustomize.py that would run at interpreter startup if the cwd
        # were on sys.path (empty PYTHONPATH entries resolve to cwd).
        (Path(poison_dir) / "sitecustomize.py").write_text(
            "raise SystemExit(99)\n"
        )
        # A fake bigquery_agent_analytics_tracing package that would
        # shadow the vendored one if the working directory were on sys.path.
        pkg_dir = Path(poison_dir) / "bigquery_agent_analytics_tracing"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("raise SystemExit(99)\n")
        return poison_dir

    def _run_hook(self, hook_script: str, poison_dir: str,
                  extra_env: dict | None = None, src: bool = False):
        """Run a hook script with cwd set to poison_dir.

        The poison modules in cwd must NOT be imported: common.sh strips the
        working directory from sys.path. We do NOT put poison_dir on
        PYTHONPATH (that would be explicitly set by the caller, and the hook
        script intentionally preserves caller-set entries); the test is that
        the working directory itself does not leak onto sys.path.
        """
        env = dict(self.base_env)
        if extra_env:
            env.update(extra_env)
        # PYTHONPATH with leading/trailing/doubled colons (empty entries
        # that would resolve to the working directory). These must be
        # stripped by the hook script, not left as cwd entries.
        env["PYTHONPATH"] = ":extra_dir::"
        payload = json.dumps({
            "session_id": "test-session-123",
            "cwd": poison_dir,
        })
        base = SRC_DIR if src else DIST_DIR
        script_path = base / hook_script
        result = subprocess.run(
            ["bash", str(script_path)],
            input=payload,
            capture_output=True,
            text=True,
            env=env,
            cwd=poison_dir,
            timeout=30,
        )
        return result

    def test_dist_session_start_no_poison(self):
        """dist session_start.sh must not import poison modules from cwd."""
        poison_dir = self._make_poison_dir()
        result = self._run_hook("hooks/session_start.sh", poison_dir)
        self.assertEqual(result.returncode, 0,
                         f"stdout={result.stdout}\nstderr={result.stderr}")

    def test_src_session_start_no_poison(self):
        """source session_start.sh must not import poison modules from cwd.

        The source plugin's vendor/ is git-ignored (populated at build time),
        so this test only checks that the hook script itself does not crash
        with a poison-related SystemExit(99). An ImportError for the missing
        vendored package is expected and acceptable.
        """
        poison_dir = self._make_poison_dir()
        result = self._run_hook("hooks/session_start.sh", poison_dir, src=True)
        # Must NOT exit 99 (poison module imported).
        self.assertNotEqual(result.returncode, 99,
                           f"poison module imported!\nstdout={result.stdout}"
                           f"\nstderr={result.stderr}")

    def test_dist_run_setup_check_no_poison(self):
        """dist run_setup_check.sh must not import poison modules from cwd."""
        poison_dir = self._make_poison_dir()
        result = self._run_hook(
            "scripts/run_setup_check.sh", poison_dir,
            extra_env={"BQAA_TRACE_ENABLED": "false"},
        )
        self.assertNotEqual(result.returncode, 99,
                           f"poison module imported!\nstdout={result.stdout}"
                           f"\nstderr={result.stderr}")

    def test_src_run_setup_check_no_poison(self):
        """source run_setup_check.sh must not import poison modules from cwd."""
        poison_dir = self._make_poison_dir()
        result = self._run_hook(
            "scripts/run_setup_check.sh", poison_dir,
            extra_env={"BQAA_TRACE_ENABLED": "false"}, src=True,
        )
        self.assertNotEqual(result.returncode, 99,
                           f"poison module imported!\nstdout={result.stdout}"
                           f"\nstderr={result.stderr}")

    # --- BQAA_TRACE_ENABLED gating with state-file assertions ---

    def _run_trace_test(self, trace_enabled: str | None, session_id: str,
                        expect_enabled: bool):
        """Run session_start.sh and assert state file / no-state behavior."""
        state_dir = tempfile.mkdtemp(prefix="bqaa_trace_state_")
        self.addCleanup(shutil.rmtree, state_dir, ignore_errors=True)
        spool_dir = tempfile.mkdtemp(prefix="bqaa_trace_spool_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)

        env = dict(self.base_env)
        env["BQAA_STATE_DIR"] = state_dir
        env["BQAA_SPOOL_DIR"] = spool_dir
        env["BQAA_LOG_FILE"] = os.path.join(spool_dir, "test.log")
        if trace_enabled is not None:
            env["BQAA_TRACE_ENABLED"] = trace_enabled
        else:
            env.pop("BQAA_TRACE_ENABLED", None)

        poison_dir = self._make_poison_dir()
        env["PYTHONPATH"] = ":extra_dir::"
        payload = json.dumps({"session_id": session_id, "cwd": poison_dir})

        script = DIST_DIR / "hooks" / "session_start.sh"
        result = subprocess.run(
            ["bash", str(script)],
            input=payload, capture_output=True, text=True,
            env=env, cwd=poison_dir, timeout=30,
        )

        safe_sid = "".join(
            c for c in session_id if c.isalnum() or c in "._-"
        ) or "default"
        state_file = Path(state_dir) / f"state_{safe_sid}.json"

        if expect_enabled:
            self.assertEqual(result.returncode, 0,
                           f"stdout={result.stdout}\nstderr={result.stderr}")
            self.assertTrue(state_file.is_file(),
                           f"state file not created: {state_file}")
        else:
            self.assertEqual(result.returncode, 0)
            self.assertEqual(os.listdir(state_dir), [],
                           f"state dir not empty: {os.listdir(state_dir)}")

    def test_bqaa_trace_enabled_true(self):
        """BQAA_TRACE_ENABLED=true enables tracing and creates state file."""
        self._run_trace_test("true", "sid-true", expect_enabled=True)

    def test_bqaa_trace_enabled_TRUE(self):
        """BQAA_TRACE_ENABLED=TRUE also enables tracing (case-insensitive)."""
        self._run_trace_test("TRUE", "sid-TRUE", expect_enabled=True)

    def test_bqaa_trace_enabled_unset(self):
        """Unset BQAA_TRACE_ENABLED defaults to on (state file created)."""
        self._run_trace_test(None, "sid-unset", expect_enabled=True)

    def test_bqaa_trace_enabled_false(self):
        """BQAA_TRACE_ENABLED=false suppresses tracing (no state file).

        Also passes BQAA_PYTHON=/nonexistent/python to prove common.sh
        exits 0 at the shell layer without spawning Python.
        """
        state_dir = tempfile.mkdtemp(prefix="bqaa_trace_off_")
        self.addCleanup(shutil.rmtree, state_dir, ignore_errors=True)
        env = dict(self.base_env)
        env["BQAA_TRACE_ENABLED"] = "false"
        env["BQAA_PYTHON"] = "/nonexistent/python"
        env["BQAA_STATE_DIR"] = state_dir
        poison_dir = self._make_poison_dir()
        env["PYTHONPATH"] = ":extra_dir::"
        payload = json.dumps({"session_id": "sid-false", "cwd": poison_dir})
        result = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=payload, capture_output=True, text=True,
            env=env, cwd=poison_dir, timeout=30,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(os.listdir(state_dir), [])

    def test_bqaa_trace_enabled_zero(self):
        """BQAA_TRACE_ENABLED=0 suppresses tracing (no state file)."""
        state_dir = tempfile.mkdtemp(prefix="bqaa_trace_zero_")
        self.addCleanup(shutil.rmtree, state_dir, ignore_errors=True)
        env = dict(self.base_env)
        env["BQAA_TRACE_ENABLED"] = "0"
        env["BQAA_PYTHON"] = "/nonexistent/python"
        env["BQAA_STATE_DIR"] = state_dir
        poison_dir = self._make_poison_dir()
        env["PYTHONPATH"] = ":extra_dir::"
        payload = json.dumps({"session_id": "sid-zero", "cwd": poison_dir})
        result = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=payload, capture_output=True, text=True,
            env=env, cwd=poison_dir, timeout=30,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(os.listdir(state_dir), [])

    def test_bqaa_trace_enabled_empty(self):
        """BQAA_TRACE_ENABLED='' suppresses tracing (no state file)."""
        state_dir = tempfile.mkdtemp(prefix="bqaa_trace_empty_")
        self.addCleanup(shutil.rmtree, state_dir, ignore_errors=True)
        env = dict(self.base_env)
        env["BQAA_TRACE_ENABLED"] = ""
        env["BQAA_PYTHON"] = "/nonexistent/python"
        env["BQAA_STATE_DIR"] = state_dir
        poison_dir = self._make_poison_dir()
        env["PYTHONPATH"] = ":extra_dir::"
        payload = json.dumps({"session_id": "sid-empty", "cwd": poison_dir})
        result = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=payload, capture_output=True, text=True,
            env=env, cwd=poison_dir, timeout=30,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(os.listdir(state_dir), [])


# =============================================================================
# 3. Vendored package & all 11 hook scripts offline E2E
# =============================================================================


# The 11 Claude Code hook names (matching plugin.json hook keys).
# SubagentStop BEFORE Stop: _stop deletes current_trace_id, which would
# cause _subagent_stop to return early and not write a DRY_RUN line.
HOOK_NAMES = [
    "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
    "PostToolUseFailure", "SubagentStop", "Stop", "StopFailure",
    "Notification", "PermissionRequest", "SessionEnd",
]

# Realistic JSON payloads for each hook.
HOOK_PAYLOADS = {
    "SessionStart": {
        "session_id": "e2e-session-001",
        "cwd": "/tmp/test-project",
    },
    "UserPromptSubmit": {
        "session_id": "e2e-session-001",
        "prompt": "How do I create a BigQuery table?",
        "invocation_id": "inv-001",
        "transcript_path": "",
    },
    "PreToolUse": {
        "session_id": "e2e-session-001",
        "tool_use_id": "tool-use-001",
        "tool_name": "Write",
        "tool_input": {"file_path": "/tmp/test.py", "content": "print(1)"},
    },
    "PostToolUse": {
        "session_id": "e2e-session-001",
        "tool_use_id": "tool-use-001",
        "tool_name": "Write",
        "tool_response": {"is_error": False, "content": "File created"},
    },
    "PostToolUseFailure": {
        "session_id": "e2e-session-001",
        "tool_use_id": "tool-use-002",
        "tool_name": "Bash",
        "error": "Exit code 1",
    },
    "Stop": {
        "session_id": "e2e-session-001",
        "transcript_path": "",
        "response": "Created the file successfully.",
    },
    "StopFailure": {
        "session_id": "e2e-session-001",
        "transcript_path": "",
        "error": "rate_limit_exceeded",
    },
    "SubagentStop": {
        "session_id": "e2e-session-001",
        "agent_id": "subagent-1",
        "agent_type": "research",
        "agent_transcript_path": "",
        "output": "Research complete.",
    },
    "Notification": {
        "session_id": "e2e-session-001",
        "title": "Test notification",
        "message": "A notification was triggered.",
        "notification_type": "info",
    },
    "PermissionRequest": {
        "session_id": "e2e-session-001",
        "permission": "file_write",
        "tool_name": "Write",
        "tool_input": {"file_path": "/tmp/test.py"},
    },
    "SessionEnd": {
        "session_id": "e2e-session-001",
    },
}

# SessionStart only writes state (no event unless backlog exists). The other 10
# hooks each write one event line.
EXPECTED_DRY_RUN_LINES = 10


class TestBQAAConfig(unittest.TestCase):
    """BQAAConfig.from_env() precedence and defaults."""

    def test_defaults(self):
        env = {
            "BQAA_PROJECT_ID": "my-project",
            "BQAA_DATASET": "my_dataset",
        }
        old = {k: os.environ.pop(k, None) for k in
               ("BQAA_PROJECT_ID", "BQAA_DATASET", "BQAA_TRACE_ENABLED",
                "BQAA_DRY_RUN", "BQAA_AUTO_DRAIN")}
        for k, v in env.items():
            os.environ[k] = v
        try:
            from bigquery_agent_analytics_tracing.config import BQAAConfig
            cfg = BQAAConfig.from_env()
            self.assertEqual(cfg.project_id, "my-project")
            self.assertEqual(cfg.dataset, "my_dataset")
            self.assertEqual(cfg.table, "agent_events")
            self.assertTrue(cfg.enabled)
            self.assertFalse(cfg.dry_run)
        finally:
            for k in env:
                os.environ.pop(k, None)
            for k, v in old.items():
                if v is not None:
                    os.environ[k] = v

    def test_fallback_project_id(self):
        """GCP_PROJECT_ID falls back when BQAA_PROJECT_ID is unset."""
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        env_backup = os.environ.copy()
        try:
            os.environ.pop("BQAA_PROJECT_ID", None)
            os.environ.pop("BQAA_DATASET", None)
            os.environ["GCP_PROJECT_ID"] = "fallback-project"
            os.environ["BQ_DATASET"] = "fallback_dataset"
            cfg = BQAAConfig.from_env()
            self.assertEqual(cfg.project_id, "fallback-project")
            self.assertEqual(cfg.dataset, "fallback_dataset")
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    def test_trace_enabled_false(self):
        """BQAA_TRACE_ENABLED=false sets enabled=False."""
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_PROJECT_ID"] = "p"
            os.environ["BQAA_DATASET"] = "d"
            os.environ["BQAA_TRACE_ENABLED"] = "false"
            cfg = BQAAConfig.from_env()
            self.assertFalse(cfg.enabled)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)


class TestBQSchema(unittest.TestCase):
    """bq_schema() returns canonical agent_events columns."""

    def test_schema_columns(self):
        from bigquery_agent_analytics_tracing.schema import bq_schema

        class FakeSchemaField:
            def __init__(self, name, field_type, mode=None, fields=None):
                self.name = name
                self.field_type = field_type
                self.mode = mode
                self.fields = fields or []

        class FakeBQ:
            SchemaField = staticmethod(FakeSchemaField)

        schema = bq_schema(FakeBQ)
        names = [f.name for f in schema]
        expected = [
            "timestamp", "event_type", "agent", "session_id",
            "invocation_id", "user_id", "trace_id", "span_id",
            "parent_span_id", "content", "content_parts", "attributes",
            "latency_ms", "status", "error_message", "is_truncated",
        ]
        self.assertEqual(names, expected)
        self.assertEqual(schema[0].field_type, "TIMESTAMP")
        self.assertEqual(schema[0].mode, "REQUIRED")


class TestHookScriptsE2E(unittest.TestCase):
    """Run all 9 dist hook scripts with realistic payloads in dry-run mode.

    Uses BQAA_DRY_RUN=true so no BigQuery writes happen. The hooks write
    dry-run log entries instead. Exit 0 for every hook.
    """

    @classmethod
    def setUpClass(cls):
        cls.spool = tempfile.mkdtemp(prefix="bqaa_e2e_spool_")
        cls.state_dir = tempfile.mkdtemp(prefix="bqaa_e2e_state_")
        cls.log_file = os.path.join(cls.spool, "e2e.log")
        cls.env = {
            "BQAA_PROJECT_ID": "e2e-test-project",
            "BQAA_DATASET": "e2e_test_dataset",
            "BQAA_SPOOL_DIR": cls.spool,
            "BQAA_STATE_DIR": cls.state_dir,
            "BQAA_DRY_RUN": "true",
            "BQAA_AUTO_DRAIN": "false",
            "BQAA_LOG_FILE": cls.log_file,
            "BQAA_TRACE_ENABLED": "true",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": os.environ.get("HOME", "/tmp"),
        }

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.spool, ignore_errors=True)
        shutil.rmtree(cls.state_dir, ignore_errors=True)

    def _run_hook(self, hook_name: str, payload: dict) -> subprocess.CompletedProcess:
        hook_to_script = {
            "SessionStart": "session_start.sh",
            "UserPromptSubmit": "user_prompt_submit.sh",
            "PreToolUse": "pre_tool_use.sh",
            "PostToolUse": "post_tool_use.sh",
            "PostToolUseFailure": "post_tool_use_failure.sh",
            "Stop": "stop.sh",
            "StopFailure": "stop_failure.sh",
            "SubagentStop": "subagent_stop.sh",
            "Notification": "notification.sh",
            "PermissionRequest": "permission_request.sh",
            "SessionEnd": "session_end.sh",
        }
        script = DIST_DIR / "hooks" / hook_to_script[hook_name]
        env = dict(self.env)
        return subprocess.run(
            ["bash", str(script)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

    def test_all_hooks_exit_zero(self):
        """Every hook script must exit 0 in dry-run mode."""
        for hook_name in HOOK_NAMES:
            with self.subTest(hook=hook_name):
                result = self._run_hook(hook_name, HOOK_PAYLOADS[hook_name])
                self.assertEqual(
                    result.returncode, 0,
                    f"{hook_name} failed (exit {result.returncode}):\n"
                    f"stdout={result.stdout}\nstderr={result.stderr}",
                )

    def test_no_errors_in_log(self):
        """The dry-run log must contain only clean DRY_RUN lines and no errors."""
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        for hook_name in HOOK_NAMES:
            self._run_hook(hook_name, HOOK_PAYLOADS[hook_name])
        if os.path.exists(self.log_file):
            log_text = Path(self.log_file).read_text(
                encoding="utf-8", errors="replace"
            )
            bad_lines = [
                line for line in log_text.splitlines()
                if "DRY_RUN " not in line
            ]
            self.assertEqual(bad_lines, [],
                             f"non-DRY_RUN lines in log:\n{log_text}")

    def test_dry_run_lines_written(self):
        """All 10 event-emitting hooks must write DRY_RUN lines."""
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        for hook_name in HOOK_NAMES:
            self._run_hook(hook_name, HOOK_PAYLOADS[hook_name])
        self.assertTrue(os.path.isfile(self.log_file),
                        "log file not created")
        log_text = Path(self.log_file).read_text(
            encoding="utf-8", errors="replace"
        )
        dry_run_count = log_text.count("DRY_RUN")
        self.assertEqual(dry_run_count, EXPECTED_DRY_RUN_LINES,
                        f"expected {EXPECTED_DRY_RUN_LINES} DRY_RUN lines,"
                        f" got {dry_run_count}:\n{log_text}")

    def test_session_start_creates_state_file(self):
        """SessionStart must create a state_<sid>.json file, and the
        following UserPromptSubmit must write a DRY_RUN line."""
        state_dir = tempfile.mkdtemp(prefix="bqaa_ss_state_")
        self.addCleanup(shutil.rmtree, state_dir, ignore_errors=True)
        spool_dir = tempfile.mkdtemp(prefix="bqaa_ss_spool_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        log_file = os.path.join(spool_dir, "ss.log")
        sid = "ss-test-session"
        env = dict(self.env)
        env["BQAA_STATE_DIR"] = state_dir
        env["BQAA_SPOOL_DIR"] = spool_dir
        env["BQAA_LOG_FILE"] = log_file

        script = DIST_DIR / "hooks" / "session_start.sh"
        result = subprocess.run(
            ["bash", str(script)],
            input=json.dumps({"session_id": sid, "cwd": "/tmp"}),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(result.returncode, 0,
                        f"stdout={result.stdout}\nstderr={result.stderr}")

        safe_sid = "".join(
            c for c in sid if c.isalnum() or c in "._-"
        ) or "default"
        state_file = Path(state_dir) / f"state_{safe_sid}.json"
        self.assertTrue(state_file.is_file(),
                       f"state file not created: {state_file}")

        script2 = DIST_DIR / "hooks" / "user_prompt_submit.sh"
        result2 = subprocess.run(
            ["bash", str(script2)],
            input=json.dumps({
                "session_id": sid,
                "prompt": "test prompt",
                "transcript_path": "",
            }),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(result2.returncode, 0)
        self.assertTrue(os.path.isfile(log_file), "log file not created")
        log_text = Path(log_file).read_text(encoding="utf-8", errors="replace")
        self.assertIn("DRY_RUN", log_text,
                      "DRY_RUN not in log after UserPromptSubmit")


class TestDrainPy(unittest.TestCase):
    """drain.py batch drain against stand-in google.cloud.bigquery modules."""

    def test_drain_empty_spool_exits_zero(self):
        """drain.main() must exit 0 when the spool dir is empty.

        BQAA_DRY_RUN=false so main() actually enters _run(config) and
        verifies the empty-spool path rather than returning early.
        """
        from bigquery_agent_analytics_tracing import drain

        empty_spool = tempfile.mkdtemp(prefix="bqaa_empty_spool_")
        self.addCleanup(shutil.rmtree, empty_spool, ignore_errors=True)
        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_SPOOL_DIR"] = empty_spool
            os.environ["BQAA_DRY_RUN"] = "false"
            os.environ["BQAA_PROJECT_ID"] = "test-proj"
            os.environ["BQAA_DATASET"] = "test_ds"
            os.environ["BQAA_DRAIN_IDLE_SECONDS"] = "0.1"
            os.environ["BQAA_DRAIN_POLL_SECONDS"] = "0.1"
            code = drain.main([])
            self.assertEqual(code, 0)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    def test_drain_batch_with_standin_bigquery(self):
        """Write 2 spool rows, drain with a stand-in bigquery client.

        Injects a fake google.cloud.bigquery module into sys.modules so
        drain.py's insert_rows_json fallback path captures the rows without
        any real GCP call. Asserts the spool files are deleted on success.
        """
        spool_dir = tempfile.mkdtemp(prefix="bqaa_drain_spool_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)

        # Write two valid spool envelopes.
        for i in range(2):
            envelope = {
                "config": {
                    "project_id": "test-proj",
                    "dataset": "test_ds",
                    "table": "agent_events",
                    "location": None,
                    "auto_create_table": False,
                    "auto_create_dataset": False,
                    "writer_label": "test/0.1.0",
                },
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "agent": "test-agent",
                    "session_id": f"session-{i}",
                    "invocation_id": f"inv-{i}",
                    "user_id": "test-user",
                    "trace_id": f"trace-{i}",
                    "span_id": f"span-{i}",
                    "parent_span_id": None,
                    "content": {"prompt": f"test prompt {i}"},
                    "attributes": {"source": "test"},
                    "latency_ms": {},
                    "status": "OK",
                    "error_message": None,
                    "is_truncated": False,
                },
            }
            path = Path(spool_dir) / f"event-{i}.json"
            path.write_text(json.dumps(envelope), encoding="utf-8")

        # Build a stand-in google.cloud.bigquery module.
        captured_rows: list = []

        class FakeNotFound(Exception):
            pass

        class FakeTable:
            def __init__(self, table_id, schema=None):
                self.table_id = table_id

        class FakeClient:
            def __init__(self, project=None, location=None):
                self.project = project
                self.location = location

            def get_dataset(self, dataset_id):
                raise FakeNotFound("test")

            def get_table(self, table_id):
                # Return a fake table so _ensure_table_exists succeeds
                # without raising.
                return FakeTable(table_id)

            def create_table(self, table):
                pass

            def insert_rows_json(self, table_id, rows):
                captured_rows.extend(rows)
                return []  # no errors

        fake_bq_mod = types.ModuleType("google.cloud.bigquery")
        fake_bq_mod.Client = FakeClient
        fake_bq_mod.NotFound = FakeNotFound

        class FakeSchemaField:
            def __init__(self, name, field_type, mode=None, fields=None):
                pass

        class FakeTimePartitioning:
            def __init__(self, type_=None, field=None):
                pass

        fake_bq_mod.Table = FakeTable
        fake_bq_mod.SchemaField = FakeSchemaField
        fake_bq_mod.TimePartitioning = FakeTimePartitioning
        fake_bq_mod.TimePartitioningType = types.SimpleNamespace(DAY="DAY")

        # We need google.cloud and google.cloud.exceptions too.
        google_mod = types.ModuleType("google")
        cloud_mod = types.ModuleType("google.cloud")
        exceptions_mod = types.ModuleType("google.cloud.exceptions")
        exceptions_mod.NotFound = FakeNotFound
        exceptions_mod.ServiceUnavailable = type(
            "ServiceUnavailable", (Exception,), {}
        )
        exceptions_mod.TooManyRequests = type(
            "TooManyRequests", (Exception,), {}
        )
        exceptions_mod.InternalServerError = type(
            "InternalServerError", (Exception,), {}
        )
        cloud_mod.bigquery = fake_bq_mod
        cloud_mod.exceptions = exceptions_mod
        google_mod.cloud = cloud_mod

        # Also need google.api_core.exceptions.
        api_core_mod = types.ModuleType("google.api_core")
        api_core_exceptions = types.ModuleType("google.api_core.exceptions")
        api_core_exceptions.InternalServerError = (
            exceptions_mod.InternalServerError
        )
        api_core_exceptions.ServiceUnavailable = (
            exceptions_mod.ServiceUnavailable
        )
        api_core_exceptions.TooManyRequests = exceptions_mod.TooManyRequests
        api_core_mod.exceptions = api_core_exceptions

        injected = [
            ("google", google_mod),
            ("google.cloud", cloud_mod),
            ("google.cloud.bigquery", fake_bq_mod),
            ("google.cloud.exceptions", exceptions_mod),
            ("google.api_core", api_core_mod),
            ("google.api_core.exceptions", api_core_exceptions),
        ]
        old_modules = {k: sys.modules.get(k) for k, _ in injected}
        for k, v in injected:
            sys.modules[k] = v

        def _restore():
            for k, v in old_modules.items():
                if v is not None:
                    sys.modules[k] = v
                else:
                    sys.modules.pop(k, None)

        self.addCleanup(_restore)

        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_SPOOL_DIR"] = spool_dir
            os.environ["BQAA_DRY_RUN"] = "false"
            os.environ["BQAA_PROJECT_ID"] = "test-proj"
            os.environ["BQAA_DATASET"] = "test_ds"
            os.environ["BQAA_DRAIN_IDLE_SECONDS"] = "0.1"
            os.environ["BQAA_DRAIN_POLL_SECONDS"] = "0.1"

            from bigquery_agent_analytics_tracing import drain
            from bigquery_agent_analytics_tracing.config import BQAAConfig

            config = BQAAConfig.from_env()
            # Run one drain cycle.
            asyncio.run(drain._drain_once(config, use_storage_api=False))

            self.assertEqual(len(captured_rows), 2,
                           f"expected 2 captured rows, got {len(captured_rows)}")

            remaining = list(Path(spool_dir).glob("event-*.json"))
            self.assertEqual(remaining, [],
                           f"spool files not deleted: {remaining}")
        finally:
            os.environ.clear()
            os.environ.update(env_backup)


class TestSetupCheck(unittest.TestCase):
    """setup_check.py / run_setup_check.sh with BQAA_PYTHON fallback."""

    def test_setup_check_json(self):
        """setup_check --json must produce valid JSON with expected keys."""
        from bigquery_agent_analytics_tracing import setup_check
        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_PROJECT_ID"] = "test-project"
            os.environ["BQAA_DATASET"] = "test_dataset"
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = setup_check.main(["--json"])
            self.assertIn(code, (0, 1))
            report = json.loads(buf.getvalue())
            self.assertIsInstance(report, dict)
            self.assertIn("python_ok", report)
            self.assertIn("project_id", report)
            self.assertEqual(report["project_id"], "test-project")
            self.assertIn("dataset", report)
            self.assertEqual(report["dataset"], "test_dataset")
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    def test_setup_check_nonexistent_python(self):
        """BQAA_PYTHON=/nonexistent/python must fall back to python3 and
        surface interpreter_error in the report."""
        env = {
            "BQAA_PROJECT_ID": "test-project",
            "BQAA_DATASET": "test_dataset",
            "BQAA_PYTHON": "/nonexistent/python",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": os.environ.get("HOME", "/tmp"),
        }
        script = DIST_DIR / "scripts" / "run_setup_check.sh"
        result = subprocess.run(
            ["bash", str(script)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(result.returncode, 127,
                           f"shell exec failed:\n{result.stderr}")
        self.assertIn("interpreter", result.stdout.lower())

    def test_setup_check_nonexistent_python_fallback(self):
        """run_setup_check.sh --json with BQAA_PYTHON=/nonexistent/python:
        assert exit 1, parse JSON, assert python_ok=False and
        interpreter_error is non-empty."""
        env = {
            "BQAA_PROJECT_ID": "test-project",
            "BQAA_DATASET": "test_dataset",
            "BQAA_PYTHON": "/nonexistent/python",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": os.environ.get("HOME", "/tmp"),
        }
        script = DIST_DIR / "scripts" / "run_setup_check.sh"
        result = subprocess.run(
            ["bash", str(script), "--json"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(result.returncode, 1,
                        f"expected exit 1, got {result.returncode}:\n"
                        f"stdout={result.stdout}\nstderr={result.stderr}")
        report = json.loads(result.stdout)
        self.assertFalse(report["python_ok"],
                        "python_ok should be False with broken interpreter")
        self.assertTrue(bool(report.get("interpreter_error")),
                        "interpreter_error should be non-empty")


def _install_fake_bigquery(
    test_case: unittest.TestCase,
    *,
    insert_fn=None,
    get_table_fn=None,
    create_table_fn=None,
):
    """Install minimal stand-in google.cloud.bigquery modules for offline tests."""
    class FakeNotFound(Exception):
        code = 404

    class FakeBadRequest(Exception):
        code = 400

    class FakeServiceUnavailable(Exception):
        code = 503

    class FakeTooManyRequests(Exception):
        code = 429

    class FakeInternalServerError(Exception):
        code = 500

    class FakeTable:
        def __init__(self, table_id, schema=None):
            self.table_id = table_id

    class FakeDataset:
        def __init__(self, dataset_id):
            self.dataset_id = dataset_id
            self.location = None

    class FakeClient:
        def __init__(self, project=None, location=None):
            self.project = project
            self.location = location

        def get_dataset(self, dataset_id):
            raise FakeNotFound(f"404 Not found: Dataset {dataset_id}")

        def create_dataset(self, dataset):
            return dataset

        def get_table(self, table_id):
            if get_table_fn is not None:
                return get_table_fn(table_id)
            return FakeTable(table_id)

        def create_table(self, table):
            if create_table_fn is not None:
                return create_table_fn(table)

        def insert_rows_json(self, table_id, rows):
            if insert_fn is not None:
                return insert_fn(table_id, rows)
            return []

    fake_bq_mod = types.ModuleType("google.cloud.bigquery")
    fake_bq_mod.Client = FakeClient
    fake_bq_mod.Dataset = FakeDataset
    fake_bq_mod.NotFound = FakeNotFound
    fake_bq_mod.Table = FakeTable
    fake_bq_mod.SchemaField = lambda name, field_type, mode=None, fields=None: (name, field_type)
    fake_bq_mod.TimePartitioning = lambda type_=None, field=None: None
    fake_bq_mod.TimePartitioningType = types.SimpleNamespace(DAY="DAY")

    google_mod = types.ModuleType("google")
    cloud_mod = types.ModuleType("google.cloud")
    exceptions_mod = types.ModuleType("google.cloud.exceptions")
    exceptions_mod.NotFound = FakeNotFound
    exceptions_mod.BadRequest = FakeBadRequest
    exceptions_mod.ServiceUnavailable = FakeServiceUnavailable
    exceptions_mod.TooManyRequests = FakeTooManyRequests
    exceptions_mod.InternalServerError = FakeInternalServerError
    cloud_mod.bigquery = fake_bq_mod
    cloud_mod.exceptions = exceptions_mod
    google_mod.cloud = cloud_mod

    api_core_mod = types.ModuleType("google.api_core")
    api_core_exceptions = types.ModuleType("google.api_core.exceptions")
    api_core_exceptions.BadRequest = FakeBadRequest
    api_core_exceptions.InternalServerError = FakeInternalServerError
    api_core_exceptions.ServiceUnavailable = FakeServiceUnavailable
    api_core_exceptions.TooManyRequests = FakeTooManyRequests
    api_core_mod.exceptions = api_core_exceptions

    injected = [
        ("google", google_mod),
        ("google.cloud", cloud_mod),
        ("google.cloud.bigquery", fake_bq_mod),
        ("google.cloud.exceptions", exceptions_mod),
        ("google.api_core", api_core_mod),
        ("google.api_core.exceptions", api_core_exceptions),
    ]
    old = {k: sys.modules.get(k) for k, _ in injected}
    for k, v in injected:
        sys.modules[k] = v

    def _restore():
        for k, v in old.items():
            if v is not None:
                sys.modules[k] = v
            else:
                sys.modules.pop(k, None)

    test_case.addCleanup(_restore)
    return {
        "NotFound": FakeNotFound,
        "BadRequest": FakeBadRequest,
        "ServiceUnavailable": FakeServiceUnavailable,
    }


class TestSecurityAndConfig45(unittest.TestCase):
    """Issue #45: private dirs, malformed config exit-0, destination mismatch quarantine."""

    def test_private_dir_and_file_permissions(self):
        from bigquery_agent_analytics_tracing._utils import (
            ensure_private_dir,
            log_to_file,
        )
        from bigquery_agent_analytics_tracing.claude_code import StateStore
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_priv_45_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        state_dir = Path(tmp_root) / "state"
        spool_dir = Path(tmp_root) / "spool"
        log_path = state_dir / "bqaa.log"

        store = StateStore("perm-session", root=str(state_dir))
        store.set("k", "v")
        self.assertEqual(stat.S_IMODE(state_dir.stat().st_mode), 0o700)
        state_file = state_dir / "state_perm-session.json"
        self.assertEqual(stat.S_IMODE(state_file.stat().st_mode), 0o600)

        with ensure_private_dir(spool_dir):
            pass
        self.assertEqual(stat.S_IMODE(spool_dir.stat().st_mode), 0o700)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            state_dir=str(state_dir),
            spool_dir=str(spool_dir),
            log_file=str(log_path),
        )
        log_to_file(cfg, "hello")
        self.assertEqual(stat.S_IMODE(log_path.stat().st_mode), 0o600)

        # World-writable or symlinked directory must be refused.
        unsafe_dir = Path(tmp_root) / "world"
        unsafe_dir.mkdir()
        os.chmod(unsafe_dir, 0o777)
        with self.assertRaises(PermissionError):
            ensure_private_dir(unsafe_dir)

        symlink_dir = Path(tmp_root) / "sym"
        symlink_dir.symlink_to(state_dir)
        with self.assertRaises(PermissionError):
            ensure_private_dir(symlink_dir)

    def test_invalid_numeric_env_exits_zero_without_traceback(self):
        from bigquery_agent_analytics_tracing.config import (
            BQAAConfig,
            InvalidConfigError,
        )

        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_PROJECT_ID"] = "p"
            os.environ["BQAA_DATASET"] = "d"
            os.environ["BQAA_MAX_CONTENT_LENGTH"] = "5,000"
            with self.assertRaises(InvalidConfigError):
                BQAAConfig.from_env()
            os.environ["BQAA_MAX_CONTENT_LENGTH"] = "5000"
            os.environ["BQAA_DRAIN_IDLE_SECONDS"] = "0"
            with self.assertRaises(InvalidConfigError):
                BQAAConfig.from_env()
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

        tmp_root = tempfile.mkdtemp(prefix="bqaa_cfg_hook_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        log_file = os.path.join(tmp_root, "cfg.log")
        env = {
            "BQAA_PROJECT_ID": "p",
            "BQAA_DATASET": "d",
            "BQAA_MAX_CONTENT_LENGTH": "5,000",
            "BQAA_TRACE_ENABLED": "true",
            "BQAA_LOG_FILE": log_file,
            "BQAA_STATE_DIR": os.path.join(tmp_root, "state"),
            "BQAA_SPOOL_DIR": os.path.join(tmp_root, "spool"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": tmp_root,
        }
        res = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=json.dumps({"session_id": "cfg-bad-1", "cwd": tmp_root}),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res.returncode, 0)
        self.assertNotIn("Traceback", res.stderr)
        self.assertIn("BQAA_MAX_CONTENT_LENGTH", res.stderr)

        # Disabled tracing stays quiet on invalid config too.
        env["BQAA_TRACE_ENABLED"] = "false"
        res_off = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=json.dumps({"session_id": "cfg-bad-2", "cwd": tmp_root}),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res_off.returncode, 0)
        self.assertEqual(res_off.stderr, "")

    def test_drain_quarantines_destination_mismatch_and_drains_legacy(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_mm_spool_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        captured = []
        _install_fake_bigquery(
            self,
            insert_fn=lambda table_id, rows: (captured.append((table_id, rows)) or []),
        )

        def _row(sid):
            return {
                "timestamp": "2026-01-01T00:00:00Z",
                "event_type": "LLM_REQUEST",
                "agent": "test-agent",
                "session_id": sid,
                "invocation_id": "inv",
                "user_id": "u",
                "trace_id": "t",
                "span_id": "s",
                "parent_span_id": None,
                "content": {},
                "attributes": {},
                "latency_ms": {},
                "status": "OK",
                "error_message": None,
                "is_truncated": False,
            }

        mismatch_path = Path(spool_dir) / "event-mismatch.json"
        mismatch_path.write_text(
            json.dumps({
                "config": {"project_id": "other-proj", "dataset": "ds", "table": "agent_events"},
                "row": _row("mm"),
            }),
            encoding="utf-8",
        )
        legacy_path = Path(spool_dir) / "event-legacy.json"
        legacy_path.write_text(
            json.dumps({"config": {}, "row": _row("legacy")}),
            encoding="utf-8",
        )

        cfg = BQAAConfig(
            project_id="target-proj",
            dataset="target_ds",
            table="agent_events",
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
        )
        resolved = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertGreaterEqual(resolved, 2)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0], "target-proj.target_ds.agent_events")
        self.assertEqual(captured[0][1][0]["session_id"], "legacy")
        self.assertFalse(mismatch_path.exists())
        self.assertFalse(legacy_path.exists())
        quarantined = (
            Path(spool_dir)
            / "dead-letter"
            / "destination-mismatch"
            / "event-mismatch.destination-mismatch.json"
        )
        self.assertTrue(quarantined.is_file(), f"missing quarantine file {quarantined}")


class TestTranscriptAndHooks38(unittest.TestCase):
    """Issue #38: last_assistant_message, token dedup, late flush, failure hooks."""

    def test_pick_response_preference_order(self):
        from bigquery_agent_analytics_tracing.claude_code import _pick_response

        text, ok = _pick_response(
            {"last_assistant_message": "Final answer", "response": "Fallback"},
            "Transcript text",
            fallback_keys=("response", "output"),
            default="(none)",
        )
        self.assertEqual((text, ok), ("Final answer", True))

        text2, ok2 = _pick_response(
            {"last_assistant_message": "   ", "response": "Fallback"},
            "Transcript text",
            fallback_keys=("response", "output"),
            default="(none)",
        )
        self.assertEqual((text2, ok2), ("Transcript text", True))

        text3, ok3 = _pick_response(
            {"output": "Direct output"},
            "",
            fallback_keys=("response", "output"),
            default="(none)",
        )
        self.assertEqual((text3, ok3), ("Direct output", True))

        text4, ok4 = _pick_response(
            {}, "", fallback_keys=("response",), default="(none)"
        )
        self.assertEqual((text4, ok4), ("(none)", False))

    def test_transcript_dedup_partial_eof_and_late_flush(self):
        from bigquery_agent_analytics_tracing.claude_code import (
            ClaudeHookBQAAAdapter,
            _read_transcript_delta,
        )
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_38_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        transcript = Path(tmp_root) / "transcript.jsonl"

        line1 = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_1",
                "model": "claude-test",
                "usage": {"input_tokens": 100, "output_tokens": 20},
                "content": [{"type": "text", "text": "Part 1"}],
            },
        })
        line2 = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_1",
                "model": "claude-test",
                "usage": {"input_tokens": 100, "output_tokens": 55},
                "content": [{"type": "text", "text": "Part 2"}],
            },
        })
        # Write two complete lines plus one partial incomplete JSON line at EOF.
        transcript.write_text(
            line1 + "\n" + line2 + "\n" + '{"type": "assistant", "partial": ',
            encoding="utf-8",
        )
        text, model, usage, _, cursor, seen = _read_transcript_delta(
            str(transcript), 0, 100_000, None
        )
        self.assertEqual(model, "claude-test")
        self.assertIn("Part 1", text)
        self.assertIn("Part 2", text)
        # Per-field max within same message.id, not 100+100=200 or 20+55=75.
        self.assertEqual(usage["prompt_tokens"], 100)
        self.assertEqual(usage["completion_tokens"], 55)
        self.assertEqual(cursor, 2)

        # Complete line 3 with trailing newline + anonymous line without id.
        line3 = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_1",
                "model": "claude-test",
                "usage": {"input_tokens": 100, "output_tokens": 80},
                "content": [{"type": "text", "text": "Final block"}],
            },
        })
        line4_anon = json.dumps({
            "type": "assistant",
            "message": {
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "content": [{"type": "text", "text": "Anon"}],
            },
        })
        transcript.write_text(
            line1 + "\n" + line2 + "\n" + line3 + "\n" + line4_anon + "\n",
            encoding="utf-8",
        )
        text2, _, usage2, _, cursor2, seen2 = _read_transcript_delta(
            str(transcript), cursor, 100_000, seen
        )
        self.assertEqual(cursor2, 4)
        self.assertIn("Final block", text2)
        # Incremental delta: +25 output from msg_1, plus (10, 5) from anonymous line 4.
        self.assertEqual(usage2["prompt_tokens"], 10)
        self.assertEqual(usage2["completion_tokens"], 30)
        self.assertIn("msg_1", seen2)
        # Anonymous lines add to pass_usage without inflating seen_usage.
        self.assertNotIn("line-4", seen2)

        # FIFO cap 256 in seen_usage: simulate 300 distinct ids.
        many_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": f"m_{i}",
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "content": [{"type": "text", "text": "x"}],
                },
            })
            for i in range(300)
        ]
        t_cap = Path(tmp_root) / "cap.jsonl"
        t_cap.write_text("\n".join(many_lines) + "\n", encoding="utf-8")
        _, _, _, _, _, seen_cap = _read_transcript_delta(str(t_cap), 0, 500_000, seen2)
        self.assertLessEqual(len(seen_cap), 256)

        # Late transcript flush at SessionEnd when extra block arrives after Stop.
        log_file = os.path.join(tmp_root, "late.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_file,
            state_dir=os.path.join(tmp_root, "state"),
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        t_late = Path(tmp_root) / "late.jsonl"
        t_late.write_text("", encoding="utf-8")
        adapter.process("SessionStart", {"session_id": "late-s1", "cwd": tmp_root})
        adapter.process("UserPromptSubmit", {
            "session_id": "late-s1",
            "prompt": "hi",
            "transcript_path": str(t_late),
        })
        t_late.write_text(line1 + "\n", encoding="utf-8")
        adapter.process("Stop", {
            "session_id": "late-s1",
            "last_assistant_message": "Stop text",
            "transcript_path": str(t_late),
        })
        # Extra final usage chunk flushed between Stop and SessionEnd.
        t_late.write_text(line1 + "\n" + line3 + "\n", encoding="utf-8")
        adapter.process("SessionEnd", {"session_id": "late-s1"})
        log_text = Path(log_file).read_text(encoding="utf-8")
        late_rows = []
        for entry in log_text.splitlines():
            idx = entry.find("DRY_RUN ")
            if idx != -1:
                row = json.loads(entry[idx + len("DRY_RUN "):])
                attrs = json.loads(row["attributes"])
                if attrs.get("late_transcript_flush"):
                    late_rows.append((row, attrs))
        self.assertEqual(len(late_rows), 1)
        self.assertEqual(late_rows[0][1]["usage_metadata"]["completion_tokens"], 60)

    def test_post_tool_use_failure_and_stop_failure(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_fail_38_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        state_dir = os.path.join(tmp_root, "state")
        log_file = os.path.join(tmp_root, "fail.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_file,
            state_dir=state_dir,
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        adapter.process("SessionStart", {"session_id": "fs1", "cwd": tmp_root})
        adapter.process("UserPromptSubmit", {"session_id": "fs1", "prompt": "run"})
        adapter.process("PreToolUse", {
            "session_id": "fs1",
            "tool_use_id": "tid-fail-1",
            "tool_name": "Bash",
            "tool_input": {"command": "false"},
        })
        tool_file = Path(state_dir) / "tool_fs1_tid-fail-1.json"
        self.assertTrue(tool_file.is_file())
        adapter.process("PostToolUseFailure", {
            "session_id": "fs1",
            "tool_use_id": "tid-fail-1",
            "tool_name": "Bash",
            "error": "Non-zero exit status",
        })
        self.assertFalse(tool_file.exists(), "tool state file not popped on PostToolUseFailure")

        adapter.process("StopFailure", {
            "session_id": "fs1",
            "error": "rate_limit_error",
        })
        rows = []
        for line in Path(log_file).read_text(encoding="utf-8").splitlines():
            idx = line.find("DRY_RUN ")
            if idx != -1:
                rows.append(json.loads(line[idx + len("DRY_RUN "):]))
        tool_rows = [r for r in rows if r["event_type"] == "TOOL_COMPLETED"]
        self.assertEqual(len(tool_rows), 1)
        self.assertEqual(tool_rows[0]["status"], "ERROR")
        self.assertEqual(tool_rows[0]["error_message"], "Non-zero exit status")

        stop_rows = [r for r in rows if r["event_type"] == "LLM_ERROR"]
        self.assertEqual(len(stop_rows), 1)
        self.assertEqual(stop_rows[0]["status"], "ERROR")
        self.assertEqual(stop_rows[0]["error_message"], "rate_limit_error")
        self.assertEqual(
            [r for r in rows if r["event_type"] == "LLM_RESPONSE"],
            [],
            f"StopFailure must emit LLM_ERROR, not LLM_RESPONSE: {rows}",
        )

    def test_stop_failure_emits_llm_error_and_preserves_fields(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter
        from bigquery_agent_analytics_tracing.config import (
            BQAA_EVENT_TYPES,
            BQAAConfig,
        )
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        self.assertIn("LLM_ERROR", BQAA_EVENT_TYPES)

        tmp_root = tempfile.mkdtemp(prefix="bqaa_stopfail_llm_err_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        direct_log = os.path.join(tmp_root, "direct.log")
        direct_cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            agent_name="test-agent",
            user_id="test-user",
            dry_run=True,
            log_file=direct_log,
            state_dir=os.path.join(tmp_root, "direct_state"),
            spool_dir=os.path.join(tmp_root, "direct_spool"),
        )
        direct_logger = BigQueryAgentAnalyticsLogger(direct_cfg)
        emitted = direct_logger.log_llm_error(
            response="partial output before limit",
            session_id="sess-direct",
            invocation_id="inv-direct",
            trace_id="trace-direct",
            span_id="span-direct",
            parent_span_id="parent-direct",
            model="test-model",
            usage_metadata={"prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15},
            total_ms=42,
            error_message="synthetic_rate_limit",
            attributes={"custom_flag": True},
        )
        self.assertEqual(emitted["event_type"], "LLM_ERROR")
        self.assertEqual(emitted["status"], "ERROR")
        self.assertEqual(emitted["error_message"], "synthetic_rate_limit")
        self.assertEqual(emitted["content"], {"response": "partial output before limit"})
        self.assertEqual(emitted["trace_id"], "trace-direct")
        self.assertEqual(emitted["invocation_id"], "inv-direct")
        self.assertEqual(emitted["span_id"], "span-direct")
        self.assertEqual(emitted["parent_span_id"], "parent-direct")
        self.assertEqual(emitted["attributes"]["model"], "test-model")
        self.assertEqual(emitted["attributes"]["usage_metadata"]["total_tokens"], 15)
        self.assertTrue(emitted["attributes"]["custom_flag"])

        # Adapter UserPromptSubmit -> StopFailure end-to-end.
        hook_log = os.path.join(tmp_root, "hook_stopfail.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            agent_name="stopfail-agent",
            user_id="stopfail-user",
            dry_run=True,
            log_file=hook_log,
            state_dir=os.path.join(tmp_root, "state"),
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        transcript = Path(tmp_root) / "transcript.jsonl"
        transcript.write_text("", encoding="utf-8")

        from unittest import mock
        with mock.patch.dict(
            os.environ,
            {"BQAA_AGENT_NAME": "stopfail-agent", "BQAA_USER_ID": "stopfail-user"},
        ):
            adapter.process("SessionStart", {"session_id": "sf-turn-1", "cwd": tmp_root})
            adapter.process("UserPromptSubmit", {
                "session_id": "sf-turn-1",
                "prompt": "trigger rate limit",
                "invocation_id": "inv-sf-1",
                "transcript_path": str(transcript),
            })
            transcript.write_text(
                json.dumps({
                    "type": "assistant",
                    "message": {
                        "id": "msg_sf_1",
                        "model": "test-model-sf",
                        "usage": {"input_tokens": 17, "output_tokens": 9},
                        "content": [{"type": "text", "text": "transcript partial"}],
                    },
                }) + "\n",
                encoding="utf-8",
            )
            adapter.process("StopFailure", {
                "session_id": "sf-turn-1",
                "error": "synthetic_rate_limit",
                "error_type": "rate_limit",
                "reason": "quota_exceeded",
                "last_assistant_message": "partial reply",
                "transcript_path": str(transcript),
            })
            adapter.process("SessionEnd", {
                "session_id": "sf-turn-1",
                "transcript_path": str(transcript),
            })

        rows = []
        for line in Path(hook_log).read_text(encoding="utf-8").splitlines():
            idx = line.find("DRY_RUN ")
            if idx != -1:
                rows.append(json.loads(line[idx + len("DRY_RUN "):]))

        req_rows = [r for r in rows if r["event_type"] == "LLM_REQUEST"]
        err_rows = [r for r in rows if r["event_type"] == "LLM_ERROR"]
        resp_rows = [r for r in rows if r["event_type"] == "LLM_RESPONSE"]

        self.assertEqual(len(req_rows), 1)
        self.assertEqual(len(err_rows), 1, f"expected 1 LLM_ERROR row, got {rows}")
        self.assertEqual(resp_rows, [], f"expected 0 LLM_RESPONSE rows, got {resp_rows}")

        err = err_rows[0]
        req = req_rows[0]
        self.assertEqual(err["status"], "ERROR")
        self.assertEqual(err["error_message"], "synthetic_rate_limit")
        self.assertEqual(err["session_id"], "sf-turn-1")
        self.assertEqual(err["trace_id"], req["trace_id"])
        self.assertEqual(err["invocation_id"], req["invocation_id"])
        self.assertEqual(err["invocation_id"], "inv-sf-1")
        self.assertEqual(err["span_id"], req["span_id"])
        self.assertEqual(err["agent"], "stopfail-agent")
        self.assertEqual(err["user_id"], "stopfail-user")
        self.assertEqual(json.loads(err["content"]), {"response": "partial reply"})
        err_attrs = json.loads(err["attributes"])
        self.assertEqual(err_attrs["model"], "test-model-sf")
        self.assertEqual(
            err_attrs["usage_metadata"],
            {"prompt_tokens": 17, "completion_tokens": 9, "total_tokens": 26},
        )
        self.assertTrue(err_attrs["stop_failure"])
        self.assertEqual(err_attrs["hook_event_name"], "StopFailure")
        self.assertEqual(err_attrs["error_type"], "rate_limit")
        self.assertEqual(err_attrs["reason"], "quota_exceeded")

    def test_stop_failure_late_transcript_flush_emits_llm_error_not_response(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_sf_late_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        def _parse_dry_run_rows(path: str) -> list[dict]:
            parsed = []
            for line in Path(path).read_text(encoding="utf-8").splitlines():
                idx = line.find("DRY_RUN ")
                if idx != -1:
                    parsed.append(json.loads(line[idx + len("DRY_RUN "):]))
            return parsed

        # 1. StopFailure (empty transcript) -> late assistant text+usage -> SessionEnd.
        log1 = os.path.join(tmp_root, "sf_session_end.log")
        cfg1 = BQAAConfig(
            project_id="p",
            dataset="d",
            agent_name="sf-agent",
            user_id="sf-user",
            dry_run=True,
            log_file=log1,
            state_dir=os.path.join(tmp_root, "state1"),
            spool_dir=os.path.join(tmp_root, "spool1"),
        )
        adapter1 = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg1))
        t1 = Path(tmp_root) / "t1.jsonl"
        t1.write_text("", encoding="utf-8")

        adapter1.process("SessionStart", {"session_id": "sf-late-1", "cwd": tmp_root})
        adapter1.process("UserPromptSubmit", {
            "session_id": "sf-late-1",
            "prompt": "trigger failure",
            "invocation_id": "inv-sf-late-1",
            "transcript_path": str(t1),
        })
        adapter1.process("StopFailure", {
            "session_id": "sf-late-1",
            "error": "synthetic_rate_limit",
            "error_type": "rate_limit",
            "reason": "quota_exceeded",
            "transcript_path": str(t1),
        })
        t1.write_text(
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_late_sf_1",
                    "model": "test-model-late",
                    "usage": {"input_tokens": 20, "output_tokens": 8},
                    "content": [{"type": "text", "text": "partial before failure"}],
                },
            }) + "\n",
            encoding="utf-8",
        )
        adapter1.process("SessionEnd", {
            "session_id": "sf-late-1",
            "transcript_path": str(t1),
        })

        rows1 = _parse_dry_run_rows(log1)
        req1 = [r for r in rows1 if r["event_type"] == "LLM_REQUEST"]
        err1 = [r for r in rows1 if r["event_type"] == "LLM_ERROR"]
        resp1 = [r for r in rows1 if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(req1), 1)
        self.assertEqual(resp1, [], f"failed turn must never emit LLM_RESPONSE: {resp1}")
        self.assertEqual(len(err1), 2, f"expected initial + late LLM_ERROR rows, got {err1}")
        for row in err1:
            self.assertEqual(row["status"], "ERROR")
            self.assertEqual(row["error_message"], "synthetic_rate_limit")
            self.assertEqual(row["trace_id"], req1[0]["trace_id"])
            self.assertEqual(row["invocation_id"], "inv-sf-late-1")
            self.assertEqual(row["span_id"], req1[0]["span_id"])
        late_err1 = err1[1]
        self.assertEqual(
            json.loads(late_err1["content"]),
            {"response": "partial before failure"},
        )
        late_attrs1 = json.loads(late_err1["attributes"])
        self.assertTrue(late_attrs1.get("late_transcript_flush"))
        self.assertTrue(late_attrs1.get("stop_failure"))
        self.assertEqual(late_attrs1.get("hook_event_name"), "StopFailure")
        self.assertEqual(late_attrs1.get("error_type"), "rate_limit")
        self.assertEqual(late_attrs1.get("reason"), "quota_exceeded")
        self.assertEqual(late_attrs1.get("model"), "test-model-late")
        self.assertEqual(
            late_attrs1["usage_metadata"],
            {"prompt_tokens": 20, "completion_tokens": 8, "total_tokens": 28},
        )

        # 2. StopFailure (empty transcript) -> late text+usage -> next UserPromptSubmit
        #    -> normal Stop -> late usage -> SessionEnd.
        log2 = os.path.join(tmp_root, "sf_next_turn.log")
        cfg2 = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log2,
            state_dir=os.path.join(tmp_root, "state2"),
            spool_dir=os.path.join(tmp_root, "spool2"),
        )
        adapter2 = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg2))
        t2 = Path(tmp_root) / "t2.jsonl"
        t2.write_text("", encoding="utf-8")

        adapter2.process("SessionStart", {"session_id": "sf-late-2", "cwd": tmp_root})
        adapter2.process("UserPromptSubmit", {
            "session_id": "sf-late-2",
            "prompt": "turn 1 fails",
            "invocation_id": "inv-t1-fail",
            "transcript_path": str(t2),
        })
        adapter2.process("StopFailure", {
            "session_id": "sf-late-2",
            "error": "backend_overload",
            "error_type": "overload",
            "reason": "server_busy",
            "transcript_path": str(t2),
        })
        t1_late_line = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t1_fail",
                "model": "test-model-1",
                "usage": {"input_tokens": 14, "output_tokens": 6},
                "content": [{"type": "text", "text": "turn 1 late error text"}],
            },
        })
        t2.write_text(t1_late_line + "\n", encoding="utf-8")

        # Starting Turn 2 flushes Turn 1's late transcript as LLM_ERROR, then clears failure state.
        adapter2.process("UserPromptSubmit", {
            "session_id": "sf-late-2",
            "prompt": "turn 2 succeeds",
            "invocation_id": "inv-t2-ok",
            "transcript_path": str(t2),
        })
        t2_line_a = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t2_ok",
                "model": "test-model-2",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "content": [{"type": "text", "text": "turn 2 normal reply"}],
            },
        })
        t2.write_text(t1_late_line + "\n" + t2_line_a + "\n", encoding="utf-8")
        adapter2.process("Stop", {
            "session_id": "sf-late-2",
            "transcript_path": str(t2),
        })
        # Late usage chunk on Turn 2 after normal Stop must emit LLM_RESPONSE status=OK.
        t2_line_b = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t2_ok",
                "model": "test-model-2",
                "usage": {"input_tokens": 10, "output_tokens": 12},
                "content": [{"type": "text", "text": "turn 2 final chunk"}],
            },
        })
        t2.write_text(
            t1_late_line + "\n" + t2_line_a + "\n" + t2_line_b + "\n",
            encoding="utf-8",
        )
        adapter2.process("SessionEnd", {
            "session_id": "sf-late-2",
            "transcript_path": str(t2),
        })

        rows2 = _parse_dry_run_rows(log2)
        reqs2 = [r for r in rows2 if r["event_type"] == "LLM_REQUEST"]
        errs2 = [r for r in rows2 if r["event_type"] == "LLM_ERROR"]
        resps2 = [r for r in rows2 if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(reqs2), 2)
        self.assertEqual(len(errs2), 2, f"Turn 1 must emit 2 LLM_ERROR rows: {errs2}")
        self.assertEqual(len(resps2), 2, f"Turn 2 must emit 2 LLM_RESPONSE rows: {resps2}")

        for err_row in errs2:
            self.assertEqual(err_row["trace_id"], reqs2[0]["trace_id"])
            self.assertEqual(err_row["invocation_id"], "inv-t1-fail")
            self.assertEqual(err_row["span_id"], reqs2[0]["span_id"])
            self.assertEqual(err_row["status"], "ERROR")
            self.assertEqual(err_row["error_message"], "backend_overload")
        self.assertEqual(
            json.loads(errs2[1]["content"]),
            {"response": "turn 1 late error text"},
        )
        errs2_late_attrs = json.loads(errs2[1]["attributes"])
        self.assertTrue(errs2_late_attrs.get("late_transcript_flush"))
        self.assertTrue(errs2_late_attrs.get("stop_failure"))

        for resp_row in resps2:
            self.assertEqual(resp_row["trace_id"], reqs2[1]["trace_id"])
            self.assertEqual(resp_row["invocation_id"], "inv-t2-ok")
            self.assertEqual(resp_row["span_id"], reqs2[1]["span_id"])
            self.assertEqual(resp_row["status"], "OK")
            self.assertIsNone(resp_row["error_message"])
        resps2_late_attrs = json.loads(resps2[1]["attributes"])
        self.assertTrue(resps2_late_attrs.get("late_transcript_flush"))
        self.assertFalse(resps2_late_attrs.get("stop_failure", False))
        self.assertEqual(resps2_late_attrs["usage_metadata"]["completion_tokens"], 7)

    def test_stop_after_stop_failure_without_user_prompt_preserves_failure_and_flushes_at_session_end(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter, StateStore
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_sf_stop_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        def _parse_dry_run_rows(path: str) -> list[dict]:
            parsed = []
            for line in Path(path).read_text(encoding="utf-8").splitlines():
                idx = line.find("DRY_RUN ")
                if idx != -1:
                    parsed.append(json.loads(line[idx + len("DRY_RUN "):]))
            return parsed

        log_path = os.path.join(tmp_root, "sf_stop.log")
        state_dir = os.path.join(tmp_root, "state")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            agent_name="sf-stop-agent",
            user_id="sf-stop-user",
            dry_run=True,
            log_file=log_path,
            state_dir=state_dir,
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        transcript = Path(tmp_root) / "t_stop.jsonl"
        transcript.write_text("", encoding="utf-8")

        sid = "sf-stop-seq-1"
        adapter.process("SessionStart", {"session_id": sid, "cwd": tmp_root})
        adapter.process("UserPromptSubmit", {
            "session_id": sid,
            "prompt": "turn A fails then Stop fires",
            "invocation_id": "inv-sf-stop-1",
            "transcript_path": str(transcript),
        })
        adapter.process("StopFailure", {
            "session_id": sid,
            "error": "synthetic_overload",
            "error_type": "overload",
            "reason": "529",
            "transcript_path": str(transcript),
        })

        # Late transcript chunk lands before Stop fires with no intervening UserPromptSubmit.
        transcript.write_text(
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_sf_stop_late",
                    "model": "test-model-stop",
                    "usage": {"input_tokens": 15, "output_tokens": 9},
                    "content": [{"type": "text", "text": "late error context"}],
                },
            }) + "\n",
            encoding="utf-8",
        )
        adapter.process("Stop", {
            "session_id": sid,
            "transcript_path": str(transcript),
        })

        rows_after_stop = _parse_dry_run_rows(log_path)
        req_rows = [r for r in rows_after_stop if r["event_type"] == "LLM_REQUEST"]
        err_after_stop = [r for r in rows_after_stop if r["event_type"] == "LLM_ERROR"]
        resp_after_stop = [r for r in rows_after_stop if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(req_rows), 1)
        self.assertEqual(len(err_after_stop), 1)
        self.assertEqual(
            resp_after_stop,
            [],
            f"Stop after StopFailure must not emit LLM_RESPONSE: {resp_after_stop}",
        )

        st = StateStore(sid, state_dir)
        self.assertIsNone(st.get("current_trace_id"))
        self.assertIs(st.get("last_stop_failed"), True)
        self.assertEqual(st.get("last_stop_failed_trace_id"), req_rows[0]["trace_id"])
        self.assertEqual(st.get("last_stop_error_message"), "synthetic_overload")
        self.assertEqual(st.get("last_stop_error_type"), "overload")
        self.assertEqual(st.get("last_stop_reason"), "529")

        adapter.process("SessionEnd", {
            "session_id": sid,
            "transcript_path": str(transcript),
        })

        rows_after_end = _parse_dry_run_rows(log_path)
        resp_after_end = [r for r in rows_after_end if r["event_type"] == "LLM_RESPONSE"]
        err_after_end = [r for r in rows_after_end if r["event_type"] == "LLM_ERROR"]
        self.assertEqual(resp_after_end, [])
        self.assertEqual(len(err_after_end), 2, err_after_end)
        for err_row in err_after_end:
            self.assertEqual(err_row["status"], "ERROR")
            self.assertEqual(err_row["error_message"], "synthetic_overload")
            self.assertEqual(err_row["trace_id"], req_rows[0]["trace_id"])
            self.assertEqual(err_row["invocation_id"], "inv-sf-stop-1")
            self.assertEqual(err_row["span_id"], req_rows[0]["span_id"])

        late_attrs = json.loads(err_after_end[1]["attributes"])
        self.assertEqual(
            json.loads(err_after_end[1]["content"]),
            {"response": "late error context"},
        )
        self.assertTrue(late_attrs.get("late_transcript_flush"))
        self.assertTrue(late_attrs.get("stop_failure"))
        self.assertEqual(late_attrs.get("hook_event_name"), "StopFailure")
        self.assertEqual(late_attrs.get("error_type"), "overload")
        self.assertEqual(late_attrs.get("reason"), "529")
        self.assertEqual(late_attrs.get("model"), "test-model-stop")
        self.assertEqual(
            late_attrs["usage_metadata"],
            {"prompt_tokens": 15, "completion_tokens": 9, "total_tokens": 24},
        )

    def test_stop_failure_and_turn_transition_commit_state_atomically(self):
        import contextlib
        from unittest import mock
        from bigquery_agent_analytics_tracing.claude_code import (
            ClaudeHookBQAAAdapter,
            _LockedJSONStore,
        )
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_atomic_state_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        state_dir = os.path.join(tmp_root, "state")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=os.path.join(tmp_root, "atomic.log"),
            state_dir=state_dir,
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        transcript = Path(tmp_root) / "t_atomic.jsonl"
        transcript.write_text("", encoding="utf-8")

        snapshots: list[dict] = []
        write_snapshots: list[dict] = []
        orig_transaction = _LockedJSONStore.transaction

        @contextlib.contextmanager
        def _recording_transaction(store_self):
            with orig_transaction(store_self) as state:
                before_state = dict(state)
                yield state
                after_state = dict(state)
                snapshots.append(after_state)
                if after_state != before_state:
                    write_snapshots.append(after_state)

        sid = "atomic-sf-1"
        with mock.patch.object(_LockedJSONStore, "transaction", _recording_transaction):
            adapter.process("SessionStart", {"session_id": sid, "cwd": tmp_root})
            adapter.process("UserPromptSubmit", {
                "session_id": sid,
                "prompt": "turn 1 fails",
                "transcript_path": str(transcript),
            })
            before_sf_writes = len(write_snapshots)
            adapter.process("StopFailure", {
                "session_id": sid,
                "error": "rate_limit",
                "error_type": "429",
                "reason": "quota",
                "transcript_path": str(transcript),
            })
            after_sf_writes = len(write_snapshots)
            adapter.process("UserPromptSubmit", {
                "session_id": sid,
                "prompt": "turn 2 starts",
                "transcript_path": str(transcript),
            })
            after_ups2_writes = len(write_snapshots)
            adapter.process("Stop", {
                "session_id": sid,
                "transcript_path": str(transcript),
            })

        # StopFailure must commit failure markers and active-turn teardown in a single
        # mutating transaction (independent of how many read transactions __init__ performs).
        sf_writes = write_snapshots[before_sf_writes:after_sf_writes]
        ups2_writes = write_snapshots[after_sf_writes:after_ups2_writes]
        self.assertEqual(
            len(sf_writes),
            1,
            f"StopFailure must perform 1 atomic write transaction, saw {sf_writes}",
        )
        self.assertTrue(ups2_writes)
        for snap in snapshots:
            self.assertFalse(
                snap.get("last_stop_failed") and snap.get("current_trace_id"),
                f"intermediate torn state committed with both last_stop_failed and current_trace_id: {snap}",
            )
        self.assertIsNone(sf_writes[0].get("current_trace_id"))
        self.assertTrue(sf_writes[0].get("last_stop_failed"))
        self.assertIsNone(ups2_writes[-1].get("last_stop_failed"))
        self.assertIsNotNone(ups2_writes[-1].get("current_trace_id"))

    def test_multiturn_session_end_flushes_turn2_without_usage_after_turn1_stop(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_mt_nousage_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        log_file = os.path.join(tmp_root, "mt.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_file,
            state_dir=os.path.join(tmp_root, "state"),
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        transcript = Path(tmp_root) / "transcript.jsonl"
        t1_line = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t1",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "content": [{"type": "text", "text": "turn 1 answer"}],
            },
        })
        t2_line = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t2_nousage",
                "model": "claude-test-2",
                "content": [{"type": "text", "text": "turn 2 answer"}],
            },
        })

        transcript.write_text("", encoding="utf-8")
        adapter.process("SessionStart", {"session_id": "mt-s1", "cwd": tmp_root})
        adapter.process("UserPromptSubmit", {
            "session_id": "mt-s1",
            "prompt": "turn 1",
            "transcript_path": str(transcript),
        })
        transcript.write_text(t1_line + "\n", encoding="utf-8")
        adapter.process("Stop", {
            "session_id": "mt-s1",
            "last_assistant_message": "turn 1 answer",
            "transcript_path": str(transcript),
        })

        # Turn 2 begins cleanly, writes an assistant line with zero/absent usage,
        # and ends via SessionEnd without an intervening Stop hook.
        adapter.process("UserPromptSubmit", {
            "session_id": "mt-s1",
            "prompt": "turn 2",
            "transcript_path": str(transcript),
        })
        transcript.write_text(t1_line + "\n" + t2_line + "\n", encoding="utf-8")
        adapter.process("SessionEnd", {
            "session_id": "mt-s1",
            "transcript_path": str(transcript),
        })

        rows = []
        for line in Path(log_file).read_text(encoding="utf-8").splitlines():
            idx = line.find("DRY_RUN ")
            if idx != -1:
                rows.append(json.loads(line[idx + len("DRY_RUN "):]))
        llm_resps = [r for r in rows if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(llm_resps), 2, f"expected 2 LLM_RESPONSE rows, got {llm_resps}")
        self.assertNotEqual(llm_resps[0]["trace_id"], llm_resps[1]["trace_id"])
        self.assertEqual(json.loads(llm_resps[0]["content"])["response"], "turn 1 answer")
        self.assertEqual(json.loads(llm_resps[1]["content"])["response"], "turn 2 answer")
        attrs2 = json.loads(llm_resps[1]["attributes"])
        self.assertTrue(attrs2.get("late_transcript_flush"))

    def test_multiturn_session_end_flushes_turn2_with_usage_after_turn1_stop(self):
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_mt_usage_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        log_file = os.path.join(tmp_root, "mt_usage.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_file,
            state_dir=os.path.join(tmp_root, "state"),
            spool_dir=os.path.join(tmp_root, "spool"),
        )
        adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
        transcript = Path(tmp_root) / "transcript.jsonl"
        t1_line = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t1",
                "model": "claude-test",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "content": [{"type": "text", "text": "turn 1 answer"}],
            },
        })
        t2_line = json.dumps({
            "type": "assistant",
            "message": {
                "id": "msg_t2_usage",
                "model": "claude-test-2",
                "usage": {"input_tokens": 12, "output_tokens": 9},
                "content": [{"type": "text", "text": "turn 2 with usage"}],
            },
        })

        transcript.write_text("", encoding="utf-8")
        adapter.process("SessionStart", {"session_id": "mt-s2", "cwd": tmp_root})
        adapter.process("UserPromptSubmit", {
            "session_id": "mt-s2",
            "prompt": "turn 1",
            "transcript_path": str(transcript),
        })
        transcript.write_text(t1_line + "\n", encoding="utf-8")
        adapter.process("Stop", {
            "session_id": "mt-s2",
            "last_assistant_message": "turn 1 answer",
            "transcript_path": str(transcript),
        })

        adapter.process("UserPromptSubmit", {
            "session_id": "mt-s2",
            "prompt": "turn 2",
            "transcript_path": str(transcript),
        })
        transcript.write_text(t1_line + "\n" + t2_line + "\n", encoding="utf-8")
        adapter.process("SessionEnd", {
            "session_id": "mt-s2",
            "transcript_path": str(transcript),
        })

        rows = []
        for line in Path(log_file).read_text(encoding="utf-8").splitlines():
            idx = line.find("DRY_RUN ")
            if idx != -1:
                rows.append(json.loads(line[idx + len("DRY_RUN "):]))
        llm_resps = [r for r in rows if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(llm_resps), 2, f"expected 2 LLM_RESPONSE rows, got {llm_resps}")
        self.assertEqual(json.loads(llm_resps[1]["content"])["response"], "turn 2 with usage")
        attrs2 = json.loads(llm_resps[1]["attributes"])
        self.assertTrue(attrs2.get("late_transcript_flush"))
        self.assertEqual(attrs2["usage_metadata"]["prompt_tokens"], 12)
        self.assertEqual(attrs2["usage_metadata"]["completion_tokens"], 9)
        self.assertEqual(attrs2["usage_metadata"]["total_tokens"], 21)

    def test_transcript_rejects_symlink_and_fifo(self):
        from bigquery_agent_analytics_tracing.claude_code import (
            ClaudeHookBQAAAdapter,
            _line_count,
            _read_transcript_delta,
        )
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_c2_symlink_fifo_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        secret_file = Path(tmp_root) / "secret.jsonl"
        secret_file.write_text(
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_secret",
                    "model": "claude-test",
                    "usage": {"input_tokens": 5, "output_tokens": 5},
                    "content": [{"type": "text", "text": "LEAKED_SECRET"}],
                },
            }) + "\n",
            encoding="utf-8",
        )
        link_path = Path(tmp_root) / "link.jsonl"
        try:
            os.symlink(str(secret_file), str(link_path))
        except OSError:
            link_path = None

        if link_path is not None:
            self.assertEqual(_line_count(str(link_path)), 0)
            text, model, usage, _, cursor, _ = _read_transcript_delta(
                str(link_path), 0, 100_000, None
            )
            self.assertEqual(text, "")
            self.assertEqual(model, "")
            self.assertEqual(usage["total_tokens"], 0)
            self.assertEqual(cursor, 0)

            log_file = os.path.join(tmp_root, "symlink.log")
            cfg = BQAAConfig(
                project_id="p",
                dataset="d",
                dry_run=True,
                log_file=log_file,
                state_dir=os.path.join(tmp_root, "state"),
                spool_dir=os.path.join(tmp_root, "spool"),
            )
            adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
            adapter.process("SessionStart", {"session_id": "sym-s1", "cwd": tmp_root})
            adapter.process("UserPromptSubmit", {
                "session_id": "sym-s1",
                "prompt": "hi",
                "transcript_path": str(link_path),
            })
            adapter.process("Stop", {
                "session_id": "sym-s1",
                "transcript_path": str(link_path),
            })
            log_text = Path(log_file).read_text(encoding="utf-8")
            self.assertNotIn("LEAKED_SECRET", log_text)
            self.assertIn("(No response captured)", log_text)

        if hasattr(os, "mkfifo"):
            fifo_path = Path(tmp_root) / "pipe.jsonl"
            os.mkfifo(str(fifo_path))
            self.assertEqual(_line_count(str(fifo_path)), 0)
            text_f, _, usage_f, _, cursor_f, _ = _read_transcript_delta(
                str(fifo_path), 0, 100_000, None
            )
            self.assertEqual(text_f, "")
            self.assertEqual(usage_f["total_tokens"], 0)
            self.assertEqual(cursor_f, 0)

    def test_compacted_transcript_resets_cursor_in_turn2(self):
        from bigquery_agent_analytics_tracing.claude_code import (
            ClaudeHookBQAAAdapter,
            StateStore,
            _read_transcript_delta,
        )
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_c2_compact_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        # Verify empty/truncated transcript resets cursor to 0 directly.
        empty_t = Path(tmp_root) / "empty.jsonl"
        empty_t.write_text("", encoding="utf-8")
        _, _, _, _, reset_cursor, _ = _read_transcript_delta(
            str(empty_t), 6, 100_000, None
        )
        self.assertEqual(reset_cursor, 0)

        def _assistant_line(msg_id: str, model: str, text: str, inp: int, out: int) -> str:
            return json.dumps({
                "type": "assistant",
                "message": {
                    "id": msg_id,
                    "model": model,
                    "usage": {"input_tokens": inp, "output_tokens": out},
                    "content": [{"type": "text", "text": text}],
                },
            })

        def _user_line(n: int) -> str:
            return json.dumps({"type": "user", "message": {"content": f"note {n}"}})

        # Case A: compaction happens between Turn 1 Stop and Turn 2 UserPromptSubmit.
        log_pre = os.path.join(tmp_root, "pre_compact.log")
        cfg_pre = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_pre,
            state_dir=os.path.join(tmp_root, "state_pre"),
            spool_dir=os.path.join(tmp_root, "spool_pre"),
        )
        adapter_pre = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg_pre))
        t_pre = Path(tmp_root) / "pre.jsonl"
        t_pre.write_text("", encoding="utf-8")
        adapter_pre.process("SessionStart", {"session_id": "cmp-pre", "cwd": tmp_root})
        adapter_pre.process("UserPromptSubmit", {
            "session_id": "cmp-pre",
            "prompt": "t1",
            "transcript_path": str(t_pre),
        })
        t_pre.write_text(
            "\n".join([_user_line(i) for i in range(5)] + [
                _assistant_line("m1", "claude-test", "turn 1 pre", 10, 5)
            ]) + "\n",
            encoding="utf-8",
        )
        adapter_pre.process("Stop", {"session_id": "cmp-pre", "transcript_path": str(t_pre)})

        # Transcript compacts from 6 lines to 1 summary line before Turn 2 starts.
        t_pre.write_text(json.dumps({"type": "summary", "summary": "compact"}) + "\n", encoding="utf-8")
        adapter_pre.process("UserPromptSubmit", {
            "session_id": "cmp-pre",
            "prompt": "t2",
            "transcript_path": str(t_pre),
        })
        t_pre.write_text(
            json.dumps({"type": "summary", "summary": "compact"}) + "\n"
            + _assistant_line("m2", "claude-test-2", "turn 2 after pre-compact", 15, 8) + "\n",
            encoding="utf-8",
        )
        adapter_pre.process("Stop", {"session_id": "cmp-pre", "transcript_path": str(t_pre)})

        resps_pre = [
            json.loads(line[line.find("DRY_RUN ") + len("DRY_RUN "):])
            for line in Path(log_pre).read_text(encoding="utf-8").splitlines()
            if "DRY_RUN " in line
        ]
        llm_pre = [r for r in resps_pre if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(llm_pre), 2)
        self.assertEqual(json.loads(llm_pre[1]["content"])["response"], "turn 2 after pre-compact")
        attrs_pre2 = json.loads(llm_pre[1]["attributes"])
        self.assertEqual(attrs_pre2["model"], "claude-test-2")
        self.assertEqual(attrs_pre2["usage_metadata"]["total_tokens"], 23)

        # Case B: compaction happens mid-turn after UserPromptSubmit recorded transcript_line=6.
        log_mid = os.path.join(tmp_root, "mid_compact.log")
        state_mid_dir = os.path.join(tmp_root, "state_mid")
        cfg_mid = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=True,
            log_file=log_mid,
            state_dir=state_mid_dir,
            spool_dir=os.path.join(tmp_root, "spool_mid"),
        )
        adapter_mid = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg_mid))
        t_mid = Path(tmp_root) / "mid.jsonl"
        t_mid.write_text("", encoding="utf-8")
        adapter_mid.process("SessionStart", {"session_id": "cmp-mid", "cwd": tmp_root})
        adapter_mid.process("UserPromptSubmit", {
            "session_id": "cmp-mid",
            "prompt": "t1",
            "transcript_path": str(t_mid),
        })
        t_mid.write_text(
            "\n".join([_user_line(i) for i in range(5)] + [
                _assistant_line("m1", "claude-test", "turn 1 mid", 10, 5)
            ]) + "\n",
            encoding="utf-8",
        )
        adapter_mid.process("Stop", {"session_id": "cmp-mid", "transcript_path": str(t_mid)})
        # Turn 2 begins while transcript still has 6 lines (transcript_line = 6).
        adapter_mid.process("UserPromptSubmit", {
            "session_id": "cmp-mid",
            "prompt": "t2",
            "transcript_path": str(t_mid),
        })
        # Transcript compacts mid-turn to 2 lines (< 6).
        t_mid.write_text(
            json.dumps({"type": "summary", "summary": "compact"}) + "\n"
            + _assistant_line("m3", "claude-test-2", "turn 2 after mid-turn compact", 20, 10) + "\n",
            encoding="utf-8",
        )
        adapter_mid.process("Stop", {"session_id": "cmp-mid", "transcript_path": str(t_mid)})

        resps_mid = [
            json.loads(line[line.find("DRY_RUN ") + len("DRY_RUN "):])
            for line in Path(log_mid).read_text(encoding="utf-8").splitlines()
            if "DRY_RUN " in line
        ]
        llm_mid = [r for r in resps_mid if r["event_type"] == "LLM_RESPONSE"]
        self.assertEqual(len(llm_mid), 2)
        self.assertEqual(json.loads(llm_mid[1]["content"])["response"], "turn 2 after mid-turn compact")
        attrs_mid2 = json.loads(llm_mid[1]["attributes"])
        self.assertEqual(attrs_mid2["usage_metadata"]["total_tokens"], 30)
        st = StateStore("cmp-mid", state_mid_dir)
        self.assertEqual(st.get("transcript_line"), 2)

    @staticmethod
    def _read_spool_envelopes(spool_dir: str) -> list[dict]:
        envelopes = []
        root = Path(spool_dir)
        paths = sorted(root.glob("event-*.json")) + sorted(root.glob("dest-*/event-*.json"))
        for path in paths:
            envelopes.append(json.loads(path.read_text(encoding="utf-8")))
        return envelopes

    @staticmethod
    def _late_flush_rows(envelopes: list[dict]) -> list[tuple[dict, dict]]:
        found = []
        for env in envelopes:
            row = env["row"]
            attrs = json.loads(row["attributes"]) if isinstance(row["attributes"], str) else row["attributes"]
            if attrs.get("late_transcript_flush"):
                found.append((row, attrs))
        return found

    def test_late_transcript_flush_preserves_checkpoint_on_spool_failure_stop(self):
        from unittest import mock
        import bigquery_agent_analytics_tracing.logger as bq_logger
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter, StateStore
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_late_spool_stop_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        def _assistant_150(msg_id: str, text: str) -> str:
            return json.dumps({
                "type": "assistant",
                "message": {
                    "id": msg_id,
                    "model": "claude-late-test",
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                    "content": [{"type": "text", "text": text}],
                },
            }) + "\n"

        # Case A: Stop -> late transcript write -> SessionEnd fails via read-only spool_dir
        # (real OS spool-write failure when non-root, or fail-once _spool fallback) -> retry SessionEnd.
        state_a = os.path.join(tmp_root, "state_a")
        spool_a = os.path.join(tmp_root, "spool_a")
        cfg_a = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "a.log"),
            state_dir=state_a,
            spool_dir=spool_a,
        )
        t_a = Path(tmp_root) / "t_a.jsonl"
        t_a.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            logger_a = BigQueryAgentAnalyticsLogger(cfg_a)
            adapter_a = ClaudeHookBQAAAdapter(logger_a)
            adapter_a.process("SessionStart", {"session_id": "late-fail-stop-a", "cwd": tmp_root})
            adapter_a.process("UserPromptSubmit", {
                "session_id": "late-fail-stop-a",
                "prompt": "turn 1",
                "transcript_path": str(t_a),
            })
            adapter_a.process("Stop", {
                "session_id": "late-fail-stop-a",
                "transcript_path": str(t_a),
            })
            st_before_late = StateStore("late-fail-stop-a", state_a)
            trace_a = st_before_late.get("last_trace_id")
            self.assertEqual(st_before_late.get("transcript_line"), 0)
            self.assertFalse(st_before_late.get("last_response_captured"))

            # Late transcript usage lands after Stop.
            t_a.write_text(_assistant_150("msg_late_stop_a", "late stop recovered text"), encoding="utf-8")

            from bigquery_agent_analytics_tracing.config import destination_spool_dir
            part_a = destination_spool_dir(spool_a, "p", "d")
            use_chmod = hasattr(os, "geteuid") and os.geteuid() != 0
            if use_chmod:
                os.chmod(part_a, 0o500)
                self.addCleanup(lambda: os.path.exists(part_a) and os.chmod(part_a, 0o700))
                with self.assertRaises(OSError):
                    adapter_a.process("SessionEnd", {
                        "session_id": "late-fail-stop-a",
                        "transcript_path": str(t_a),
                    })
                os.chmod(part_a, 0o700)
            else:
                orig_spool = logger_a._spool
                calls = {"n": 0}

                def _fail_once(bq_row):
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise OSError("simulated transient spool write failure")
                    return orig_spool(bq_row)

                with mock.patch.object(logger_a, "_spool", side_effect=_fail_once):
                    with self.assertRaises(OSError):
                        adapter_a.process("SessionEnd", {
                            "session_id": "late-fail-stop-a",
                            "transcript_path": str(t_a),
                        })

            # Checkpoint must remain intact after failed spool write.
            st_after_fail = StateStore("late-fail-stop-a", state_a)
            self.assertEqual(st_after_fail.get("transcript_line"), 0)
            self.assertNotIn("msg_late_stop_a", st_after_fail.get("seen_usage") or {})
            self.assertFalse(st_after_fail.get("last_response_captured"))
            self.assertEqual(st_after_fail.get("last_trace_id"), trace_a)
            self.assertEqual(self._late_flush_rows(self._read_spool_envelopes(spool_a)), [])

            # Retry SessionEnd now that spool is writable again.
            adapter_a.process("SessionEnd", {
                "session_id": "late-fail-stop-a",
                "transcript_path": str(t_a),
            })

        envs_a = self._read_spool_envelopes(spool_a)
        late_a = self._late_flush_rows(envs_a)
        self.assertEqual(len(late_a), 1, f"expected exact 1 late-flush row, got {late_a}")
        row_a, attrs_a = late_a[0]
        self.assertEqual(row_a["event_type"], "LLM_RESPONSE")
        self.assertEqual(row_a["trace_id"], trace_a)
        self.assertEqual(
            json.loads(row_a["content"]),
            {"response": "late stop recovered text"},
        )
        self.assertEqual(
            attrs_a["usage_metadata"],
            {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        )
        self.assertFalse((Path(state_a) / "state_late-fail-stop-a.json").exists())

        # Case B: Stop -> late write -> UserPromptSubmit fails once in _spool -> retry
        # UserPromptSubmit -> verify checkpoint, exact 1 late row, and 0 duplicates on Turn 2 Stop/SessionEnd.
        state_b = os.path.join(tmp_root, "state_b")
        spool_b = os.path.join(tmp_root, "spool_b")
        cfg_b = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "b.log"),
            state_dir=state_b,
            spool_dir=spool_b,
        )
        t_b = Path(tmp_root) / "t_b.jsonl"
        t_b.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            logger_b = BigQueryAgentAnalyticsLogger(cfg_b)
            adapter_b = ClaudeHookBQAAAdapter(logger_b)
            adapter_b.process("SessionStart", {"session_id": "late-fail-stop-b", "cwd": tmp_root})
            adapter_b.process("UserPromptSubmit", {
                "session_id": "late-fail-stop-b",
                "prompt": "turn 1",
                "transcript_path": str(t_b),
            })
            adapter_b.process("Stop", {
                "session_id": "late-fail-stop-b",
                "last_assistant_message": "stop text already captured",
                "transcript_path": str(t_b),
            })
            trace_b1 = StateStore("late-fail-stop-b", state_b).get("last_trace_id")
            t_b.write_text(_assistant_150("msg_late_stop_b", "ignored duplicate text"), encoding="utf-8")

            orig_spool_b = logger_b._spool
            calls_b = {"n": 0}

            def _fail_first_b(bq_row):
                calls_b["n"] += 1
                if calls_b["n"] == 1:
                    raise OSError("simulated transient spool failure")
                return orig_spool_b(bq_row)

            with mock.patch.object(logger_b, "_spool", side_effect=_fail_first_b):
                with self.assertRaises(OSError):
                    adapter_b.process("UserPromptSubmit", {
                        "session_id": "late-fail-stop-b",
                        "prompt": "turn 2",
                        "transcript_path": str(t_b),
                    })

            st_b_fail = StateStore("late-fail-stop-b", state_b)
            self.assertEqual(st_b_fail.get("transcript_line"), 0)
            self.assertNotIn("msg_late_stop_b", st_b_fail.get("seen_usage") or {})
            self.assertIsNone(st_b_fail.get("current_trace_id"))
            self.assertEqual(st_b_fail.get("last_trace_id"), trace_b1)
            self.assertEqual(self._late_flush_rows(self._read_spool_envelopes(spool_b)), [])

            # Retry UserPromptSubmit (succeeds: flushes turn 1 late usage and starts turn 2).
            adapter_b.process("UserPromptSubmit", {
                "session_id": "late-fail-stop-b",
                "prompt": "turn 2",
                "transcript_path": str(t_b),
            })
            st_b_retry = StateStore("late-fail-stop-b", state_b)
            self.assertEqual(st_b_retry.get("transcript_line"), 1)
            self.assertIn("msg_late_stop_b", st_b_retry.get("seen_usage") or {})

            # Finish turn 2 and end session: must emit 0 additional late-flush rows.
            adapter_b.process("Stop", {
                "session_id": "late-fail-stop-b",
                "last_assistant_message": "turn 2 done",
                "transcript_path": str(t_b),
            })
            adapter_b.process("SessionEnd", {
                "session_id": "late-fail-stop-b",
                "transcript_path": str(t_b),
            })

        late_b = self._late_flush_rows(self._read_spool_envelopes(spool_b))
        self.assertEqual(len(late_b), 1, f"expected exact 1 late-flush row across retry, got {late_b}")
        self.assertEqual(late_b[0][0]["event_type"], "LLM_RESPONSE")
        self.assertEqual(late_b[0][0]["trace_id"], trace_b1)
        self.assertEqual(
            late_b[0][1]["usage_metadata"],
            {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        )

        # Case C: Direct _flush_late_transcript fail -> retry commits atomically in 1 transaction -> repeated flush emits 0 duplicates.
        from bigquery_agent_analytics_tracing.claude_code import _LockedJSONStore

        state_c = os.path.join(tmp_root, "state_c")
        spool_c = os.path.join(tmp_root, "spool_c")
        cfg_c = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "c.log"),
            state_dir=state_c,
            spool_dir=spool_c,
        )
        t_c = Path(tmp_root) / "t_c.jsonl"
        t_c.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            logger_c = BigQueryAgentAnalyticsLogger(cfg_c)
            adapter_c = ClaudeHookBQAAAdapter(logger_c)
            adapter_c.process("SessionStart", {"session_id": "late-fail-stop-c", "cwd": tmp_root})
            adapter_c.process("UserPromptSubmit", {
                "session_id": "late-fail-stop-c",
                "prompt": "turn 1",
                "transcript_path": str(t_c),
            })
            adapter_c.process("Stop", {
                "session_id": "late-fail-stop-c",
                "transcript_path": str(t_c),
            })
            t_c.write_text(_assistant_150("msg_late_stop_c", "direct recovered text"), encoding="utf-8")

            write_txns: list[dict] = []
            orig_txn = _LockedJSONStore.transaction

            @contextlib.contextmanager
            def _recording_txn(store_self):
                with orig_txn(store_self) as state:
                    before = dict(state)
                    yield state
                    after = dict(state)
                    if after != before:
                        write_txns.append(after)

            st_c_fail_arg = StateStore("late-fail-stop-c", state_c)
            with mock.patch.object(_LockedJSONStore, "transaction", _recording_txn):
                with mock.patch.object(logger_c, "_spool", side_effect=OSError("boom")):
                    with self.assertRaises(OSError):
                        adapter_c._flush_late_transcript(
                            {"session_id": "late-fail-stop-c", "transcript_path": str(t_c)},
                            st_c_fail_arg,
                        )
            self.assertEqual(write_txns, [], "failed flush must not mutate persisted state")
            st_c_fail = StateStore("late-fail-stop-c", state_c)
            self.assertEqual(st_c_fail.get("transcript_line"), 0)
            self.assertNotIn("msg_late_stop_c", st_c_fail.get("seen_usage") or {})
            self.assertFalse(st_c_fail.get("last_response_captured"))

            # Retry succeeds, commits cursor=1, seen_usage, and last_response_captured=True in 1 transaction.
            st_c_retry_arg = StateStore("late-fail-stop-c", state_c)
            with mock.patch.object(_LockedJSONStore, "transaction", _recording_txn):
                adapter_c._flush_late_transcript(
                    {"session_id": "late-fail-stop-c", "transcript_path": str(t_c)},
                    st_c_retry_arg,
                )
            self.assertEqual(
                len(write_txns),
                1,
                f"retry flush must commit cursor, seen_usage, and response flag in 1 atomic write: {write_txns}",
            )
            st_c_ok = StateStore("late-fail-stop-c", state_c)
            self.assertEqual(st_c_ok.get("transcript_line"), 1)
            self.assertIn("msg_late_stop_c", st_c_ok.get("seen_usage") or {})
            self.assertTrue(st_c_ok.get("last_response_captured"))

            # Third flush emits 0 additional rows.
            adapter_c._flush_late_transcript(
                {"session_id": "late-fail-stop-c", "transcript_path": str(t_c)},
                StateStore("late-fail-stop-c", state_c),
            )
            late_c = self._late_flush_rows(self._read_spool_envelopes(spool_c))
            self.assertEqual(len(late_c), 1)
            self.assertEqual(
                late_c[0][1]["usage_metadata"],
                {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
            )

    def test_late_transcript_flush_preserves_checkpoint_on_spool_failure_stop_failure(self):
        from unittest import mock
        import bigquery_agent_analytics_tracing.logger as bq_logger
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter, StateStore
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_late_spool_sf_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        def _assistant_150(msg_id: str, text: str) -> str:
            return json.dumps({
                "type": "assistant",
                "message": {
                    "id": msg_id,
                    "model": "claude-sf-late-test",
                    "usage": {
                        "input_tokens": 90,
                        "cache_read_input_tokens": 10,
                        "output_tokens": 50,
                    },
                    "content": [{"type": "text", "text": text}],
                },
            }) + "\n"

        # Case A: StopFailure -> late write -> SessionEnd fails on spool write -> retry SessionEnd.
        state_a = os.path.join(tmp_root, "state_sf_a")
        spool_a = os.path.join(tmp_root, "spool_sf_a")
        cfg_a = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "sf_a.log"),
            state_dir=state_a,
            spool_dir=spool_a,
        )
        t_a = Path(tmp_root) / "t_sf_a.jsonl"
        t_a.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            logger_a = BigQueryAgentAnalyticsLogger(cfg_a)
            adapter_a = ClaudeHookBQAAAdapter(logger_a)
            adapter_a.process("SessionStart", {"session_id": "late-fail-sf-a", "cwd": tmp_root})
            adapter_a.process("UserPromptSubmit", {
                "session_id": "late-fail-sf-a",
                "prompt": "trigger sf",
                "transcript_path": str(t_a),
            })
            adapter_a.process("StopFailure", {
                "session_id": "late-fail-sf-a",
                "error": "rate_limit_exceeded",
                "error_type": "rate_limit",
                "reason": "quota_exceeded",
                "transcript_path": str(t_a),
            })
            trace_a = StateStore("late-fail-sf-a", state_a).get("last_trace_id")
            t_a.write_text(_assistant_150("msg_late_sf_a", "late failure recovered text"), encoding="utf-8")

            from bigquery_agent_analytics_tracing.config import destination_spool_dir
            part_a = destination_spool_dir(spool_a, "p", "d")
            use_chmod = hasattr(os, "geteuid") and os.geteuid() != 0
            if use_chmod:
                os.chmod(part_a, 0o500)
                self.addCleanup(lambda: os.path.exists(part_a) and os.chmod(part_a, 0o700))
                with self.assertRaises(OSError):
                    adapter_a.process("SessionEnd", {
                        "session_id": "late-fail-sf-a",
                        "transcript_path": str(t_a),
                    })
                os.chmod(part_a, 0o700)
            else:
                orig_spool = logger_a._spool
                calls = {"n": 0}

                def _fail_once(bq_row):
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise OSError("simulated transient spool write failure")
                    return orig_spool(bq_row)

                with mock.patch.object(logger_a, "_spool", side_effect=_fail_once):
                    with self.assertRaises(OSError):
                        adapter_a.process("SessionEnd", {
                            "session_id": "late-fail-sf-a",
                            "transcript_path": str(t_a),
                        })

            st_after_fail = StateStore("late-fail-sf-a", state_a)
            self.assertEqual(st_after_fail.get("transcript_line"), 0)
            self.assertNotIn("msg_late_sf_a", st_after_fail.get("seen_usage") or {})
            self.assertFalse(st_after_fail.get("last_response_captured"))
            self.assertTrue(st_after_fail.get("last_stop_failed"))
            self.assertEqual(st_after_fail.get("last_stop_failed_trace_id"), trace_a)
            self.assertEqual(self._late_flush_rows(self._read_spool_envelopes(spool_a)), [])

            adapter_a.process("SessionEnd", {
                "session_id": "late-fail-sf-a",
                "transcript_path": str(t_a),
            })

        late_a = self._late_flush_rows(self._read_spool_envelopes(spool_a))
        self.assertEqual(len(late_a), 1, f"expected exact 1 late-flush error row, got {late_a}")
        row_a, attrs_a = late_a[0]
        self.assertEqual(row_a["event_type"], "LLM_ERROR")
        self.assertEqual(row_a["status"], "ERROR")
        self.assertEqual(row_a["error_message"], "rate_limit_exceeded")
        self.assertEqual(row_a["trace_id"], trace_a)
        self.assertEqual(
            json.loads(row_a["content"]),
            {"response": "late failure recovered text"},
        )
        self.assertTrue(attrs_a.get("late_transcript_flush"))
        self.assertTrue(attrs_a.get("stop_failure"))
        self.assertEqual(attrs_a.get("hook_event_name"), "StopFailure")
        self.assertEqual(attrs_a.get("error_type"), "rate_limit")
        self.assertEqual(attrs_a.get("reason"), "quota_exceeded")
        self.assertEqual(
            attrs_a["usage_metadata"],
            {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        )

        # Case B: StopFailure -> late write -> UserPromptSubmit fails once -> retry UserPromptSubmit
        # -> verify checkpoint advanced, exact 1 late error row, and 0 duplicates on subsequent flush.
        state_b = os.path.join(tmp_root, "state_sf_b")
        spool_b = os.path.join(tmp_root, "spool_sf_b")
        cfg_b = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "sf_b.log"),
            state_dir=state_b,
            spool_dir=spool_b,
        )
        t_b = Path(tmp_root) / "t_sf_b.jsonl"
        t_b.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            logger_b = BigQueryAgentAnalyticsLogger(cfg_b)
            adapter_b = ClaudeHookBQAAAdapter(logger_b)
            adapter_b.process("SessionStart", {"session_id": "late-fail-sf-b", "cwd": tmp_root})
            adapter_b.process("UserPromptSubmit", {
                "session_id": "late-fail-sf-b",
                "prompt": "turn 1 fails",
                "transcript_path": str(t_b),
            })
            adapter_b.process("StopFailure", {
                "session_id": "late-fail-sf-b",
                "error": "overload",
                "error_type": "529",
                "reason": "capacity",
                "transcript_path": str(t_b),
            })
            trace_b1 = StateStore("late-fail-sf-b", state_b).get("last_trace_id")
            t_b.write_text(_assistant_150("msg_late_sf_b", "turn 1 late error text"), encoding="utf-8")

            orig_spool_b = logger_b._spool
            calls_b = {"n": 0}

            def _fail_first_sf(bq_row):
                calls_b["n"] += 1
                if calls_b["n"] == 1:
                    raise OSError("simulated transient spool failure")
                return orig_spool_b(bq_row)

            with mock.patch.object(logger_b, "_spool", side_effect=_fail_first_sf):
                with self.assertRaises(OSError):
                    adapter_b.process("UserPromptSubmit", {
                        "session_id": "late-fail-sf-b",
                        "prompt": "turn 2",
                        "transcript_path": str(t_b),
                    })

            st_b_fail = StateStore("late-fail-sf-b", state_b)
            self.assertEqual(st_b_fail.get("transcript_line"), 0)
            self.assertNotIn("msg_late_sf_b", st_b_fail.get("seen_usage") or {})
            self.assertTrue(st_b_fail.get("last_stop_failed"))
            self.assertEqual(self._late_flush_rows(self._read_spool_envelopes(spool_b)), [])

            adapter_b.process("UserPromptSubmit", {
                "session_id": "late-fail-sf-b",
                "prompt": "turn 2",
                "transcript_path": str(t_b),
            })
            st_b_retry = StateStore("late-fail-sf-b", state_b)
            self.assertEqual(st_b_retry.get("transcript_line"), 1)
            self.assertIn("msg_late_sf_b", st_b_retry.get("seen_usage") or {})

            adapter_b.process("Stop", {
                "session_id": "late-fail-sf-b",
                "last_assistant_message": "turn 2 ok",
                "transcript_path": str(t_b),
            })
            adapter_b.process("SessionEnd", {
                "session_id": "late-fail-sf-b",
                "transcript_path": str(t_b),
            })

        late_b = self._late_flush_rows(self._read_spool_envelopes(spool_b))
        self.assertEqual(len(late_b), 1, f"expected exact 1 late-flush error row, got {late_b}")
        row_b, attrs_b = late_b[0]
        self.assertEqual(row_b["event_type"], "LLM_ERROR")
        self.assertEqual(row_b["trace_id"], trace_b1)
        self.assertTrue(attrs_b.get("stop_failure"))
        self.assertEqual(attrs_b.get("hook_event_name"), "StopFailure")
        self.assertTrue(attrs_b.get("late_transcript_flush"))
        self.assertEqual(
            attrs_b["usage_metadata"],
            {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        )

    def test_late_transcript_flush_noop_advances_cursor_and_no_trace_skips(self):
        from unittest import mock
        import bigquery_agent_analytics_tracing.logger as bq_logger
        from bigquery_agent_analytics_tracing.claude_code import ClaudeHookBQAAAdapter, StateStore
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_late_noop_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        state_dir = os.path.join(tmp_root, "state")
        spool_dir = os.path.join(tmp_root, "spool")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            dry_run=False,
            direct_write=False,
            log_file=os.path.join(tmp_root, "noop.log"),
            state_dir=state_dir,
            spool_dir=spool_dir,
        )
        t_path = Path(tmp_root) / "t_noop.jsonl"
        t_path.write_text("", encoding="utf-8")

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            adapter = ClaudeHookBQAAAdapter(BigQueryAgentAnalyticsLogger(cfg))
            # No target_trace yet (SessionStart only): _flush_late_transcript must not advance cursor.
            adapter.process("SessionStart", {"session_id": "late-noop-1", "cwd": tmp_root})
            t_path.write_text(
                json.dumps({"type": "user", "message": {"content": "orphan line"}}) + "\n",
                encoding="utf-8",
            )
            st_no_trace = StateStore("late-noop-1", state_dir)
            adapter._flush_late_transcript(
                {"session_id": "late-noop-1", "transcript_path": str(t_path)},
                st_no_trace,
            )
            self.assertIsNone(StateStore("late-noop-1", state_dir).get("transcript_line"))

            # Start Turn 1, Stop with captured text, then append a zero-token user line.
            t_path.write_text("", encoding="utf-8")
            adapter.process("UserPromptSubmit", {
                "session_id": "late-noop-1",
                "prompt": "hello",
                "transcript_path": str(t_path),
            })
            adapter.process("Stop", {
                "session_id": "late-noop-1",
                "last_assistant_message": "already captured",
                "transcript_path": str(t_path),
            })
            t_path.write_text(
                json.dumps({
                    "type": "assistant",
                    "message": {
                        "id": "msg_zero_tok",
                        "model": "claude-test",
                        "usage": {"input_tokens": 0, "output_tokens": 0},
                        "content": [{"type": "text", "text": "already captured elsewhere"}],
                    },
                }) + "\n",
                encoding="utf-8",
            )
            adapter._flush_late_transcript(
                {"session_id": "late-noop-1", "transcript_path": str(t_path)},
                StateStore("late-noop-1", state_dir),
            )
            st_after_noop = StateStore("late-noop-1", state_dir)
            self.assertEqual(st_after_noop.get("transcript_line"), 1)
            self.assertIn("msg_zero_tok", st_after_noop.get("seen_usage") or {})
            self.assertEqual(self._late_flush_rows(self._read_spool_envelopes(spool_dir)), [])


class TestReliabilityAndSetup46(unittest.TestCase):
    """Issue #46: retry deferral, missing env warning, log rotation, find_spec, setup surfacing."""

    def test_drain_deferral_backoff_recovery_and_expiry(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_46_retry_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        fail_mode = {"mode": "404"}
        captured = []

        def _get_table(table_id):
            if fail_mode["mode"] == "404":
                raise fake_classes["NotFound"]("404 Not found: Dataset p:d")
            return types.SimpleNamespace(table_id=table_id)

        def _insert(table_id, rows):
            captured.extend(rows)
            return []

        fake_classes = _install_fake_bigquery(
            self, get_table_fn=_get_table, insert_fn=_insert
        )

        env_path = Path(spool_dir) / "event-retry-1.json"
        env_path.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "session_id": "s1",
                    "attributes": {},
                    "content": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )

        # Missing dataset/table defers cleanly instead of dead-lettering.
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=60.0,
            drain_max_retry_age_seconds=86400.0,
        )
        resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved1, 0)
        self.assertTrue(env_path.is_file(), "spool file should stay buffered on 404")
        retry_path = Path(spool_dir) / "event-retry-1.json.retry"
        self.assertTrue(retry_path.is_file())
        self.assertEqual(stat.S_IMODE(retry_path.stat().st_mode), 0o600)

        # Before next_retry_at, second pass skips without retrying; setting backoff=0 forces attempt.
        fail_mode["mode"] = "ok"
        resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved2, 0)
        self.assertEqual(len(captured), 0)

        cfg.drain_retry_backoff_seconds = 0.0
        resolved3 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved3, 1)
        self.assertEqual(len(captured), 1)
        self.assertFalse(env_path.exists())
        self.assertFalse(retry_path.exists())

        # Expired retry age quarantines to dead-letter/<name>.write-failed.json and cleans .retry.
        env2 = Path(spool_dir) / "event-expire-1.json"
        env2.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST"},
            }),
            encoding="utf-8",
        )
        retry2 = Path(spool_dir) / "event-expire-1.json.retry"
        retry2.write_text(
            json.dumps({
                "first_failed_at": time.time() - 7200,
                "attempts": 4,
                "next_retry_at": 0,
                "last_error": "old 404",
            }),
            encoding="utf-8",
        )
        fail_mode["mode"] = "404"
        cfg.drain_max_retry_age_seconds = 60.0
        resolved4 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved4, 1)
        self.assertFalse(env2.exists())
        self.assertFalse(retry2.exists())
        self.assertTrue(
            (Path(spool_dir) / "dead-letter" / "event-expire-1.write-failed.json").is_file()
        )

    def test_ensure_drainer_skips_spawn_without_bigquery(self):
        from bigquery_agent_analytics_tracing import logger as bq_logger
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_46_spec_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        state_dir = os.path.join(tmp_root, "state")
        log_file = os.path.join(tmp_root, "spec.log")
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            state_dir=state_dir,
            spool_dir=os.path.join(tmp_root, "spool"),
            log_file=log_file,
            dry_run=False,
        )

        orig_find_spec = importlib.util.find_spec
        orig_popen = bq_logger.subprocess.Popen
        saved_bq = sys.modules.pop("google.cloud.bigquery", None)
        popens = []
        env_backup = os.environ.copy()
        try:
            os.environ["BQAA_AUTO_DRAIN"] = "true"
            os.environ.pop("BQAA_PYTHON", None)
            importlib.util.find_spec = lambda name, *a, **kw: None if name == "google.cloud.bigquery" else orig_find_spec(name, *a, **kw)
            bq_logger.subprocess.Popen = lambda *a, **kw: popens.append(a)
            bq_logger._ensure_drainer(cfg)
            bq_logger._ensure_drainer(cfg)
        finally:
            importlib.util.find_spec = orig_find_spec
            bq_logger.subprocess.Popen = orig_popen
            if saved_bq is not None:
                sys.modules["google.cloud.bigquery"] = saved_bq
            os.environ.clear()
            os.environ.update(env_backup)

        self.assertEqual(popens, [], "must not spawn drainer without google-cloud-bigquery")
        log_lines = Path(log_file).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(log_lines), 1, f"expected single warning, got {log_lines}")

    def test_missing_env_warns_once_per_session(self):
        tmp_root = tempfile.mkdtemp(prefix="bqaa_46_env_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        log_file = os.path.join(tmp_root, "missing_env.log")
        env = {
            "BQAA_TRACE_ENABLED": "true",
            "BQAA_DRY_RUN": "false",
            "BQAA_LOG_FILE": log_file,
            "BQAA_STATE_DIR": os.path.join(tmp_root, "state"),
            "BQAA_SPOOL_DIR": os.path.join(tmp_root, "spool"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": tmp_root,
        }
        payload = json.dumps({"session_id": "missing-env-s1", "cwd": tmp_root})
        r1 = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=payload, capture_output=True, text=True, env=env, timeout=30,
        )
        r2 = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "user_prompt_submit.sh")],
            input=json.dumps({"session_id": "missing-env-s1", "prompt": "hi"}),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(r1.returncode, 0)
        self.assertEqual(r2.returncode, 0)
        self.assertNotIn("Traceback", r1.stderr + r2.stderr)
        lines = Path(log_file).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1, f"expected 1 session warning, got {lines}")
        self.assertIn("BQAA_PROJECT_ID", lines[0])

    def test_log_rotation_at_max_bytes_with_0600_mode(self):
        from bigquery_agent_analytics_tracing._utils import log_to_file
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_46_rot_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        state_dir = Path(tmp_root) / "state"
        log_path = state_dir / "bqaa.log"
        backup_path = state_dir / "bqaa.log.1"

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            state_dir=str(state_dir),
            spool_dir=os.path.join(tmp_root, "spool"),
            log_file=str(log_path),
            max_log_bytes=200,
        )
        for i in range(6):
            log_to_file(cfg, f"entry-{i}-" + ("x" * 60))

        self.assertTrue(backup_path.is_file(), "rotated backup bqaa.log.1 missing")
        self.assertEqual(stat.S_IMODE(log_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(backup_path.stat().st_mode), 0o600)
        self.assertLess(log_path.stat().st_size, 320)

    def test_session_start_and_setup_check_surface_spool_backlog(self):
        from bigquery_agent_analytics_tracing import setup_check

        tmp_root = tempfile.mkdtemp(prefix="bqaa_46_setup_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        state_dir = Path(tmp_root) / "state"
        spool_dir.mkdir(mode=0o700)
        state_dir.mkdir(mode=0o700)
        mm_dir = spool_dir / "dead-letter" / "destination-mismatch"
        mm_dir.mkdir(parents=True, mode=0o700)

        (spool_dir / "event-1.json").write_text("{}", encoding="utf-8")
        (spool_dir / "event-1.json.retry").write_text(
            json.dumps({"last_error": "404 Not found: Dataset test"}), encoding="utf-8"
        )
        (spool_dir / "dead-letter" / "event-2.write-failed.json").write_text("{}", encoding="utf-8")
        (mm_dir / "event-3.destination-mismatch.json").write_text("{}", encoding="utf-8")

        env = {
            "BQAA_PROJECT_ID": "test-project",
            "BQAA_DATASET": "test_dataset",
            "BQAA_SPOOL_DIR": str(spool_dir),
            "BQAA_STATE_DIR": str(state_dir),
            "BQAA_TRACE_ENABLED": "true",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": tmp_root,
        }
        ss = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=json.dumps({"session_id": "backlog-s1", "cwd": tmp_root}),
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(ss.returncode, 0)
        msg = json.loads(ss.stdout)
        self.assertIn("pending=1", msg["systemMessage"])
        self.assertIn("deferred=1", msg["systemMessage"])
        self.assertIn("dead-letter=2", msg["systemMessage"])

        env_backup = os.environ.copy()
        try:
            os.environ.update(env)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                setup_check.main(["--json"])
            report = json.loads(buf.getvalue())
            self.assertEqual(report["spool_pending"], 1)
            self.assertEqual(report["spool_deferred"], 1)
            self.assertEqual(report["spool_dead_letter"], 2)
            self.assertEqual(report["spool_destination_mismatch"], 1)
            self.assertEqual(report["spool_last_error"], "404 Not found: Dataset test")
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    def test_compute_backoff_caps_large_attempts_without_overflow(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        cfg = BQAAConfig(project_id="p", dataset="d")
        self.assertEqual(drain._compute_backoff_seconds(5000, cfg), 3600.0)

    def test_drain_partial_transient_failure_does_not_replay_accepted_rows(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_r2_partial_ok_retry_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        requests = []
        accepted = []
        call_num = {"n": 0}

        def _insert(table_id, rows):
            del table_id
            call_num["n"] += 1
            markers = [r["content"]["marker"] for r in rows]
            requests.append(markers)
            if call_num["n"] == 1:
                accepted.append(markers[0])
                return [{
                    "index": 1,
                    "errors": [{"reason": "backendError", "message": "transient"}],
                }]
            accepted.extend(markers)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        for idx, marker in enumerate(("A", "B"), start=1):
            env_file = Path(spool_dir) / f"event-00{idx}.json"
            env_file.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.0,
            drain_max_retry_age_seconds=86400.0,
        )
        resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved1, 1)
        self.assertFalse((Path(spool_dir) / "event-001.json").exists())
        self.assertFalse((Path(spool_dir) / "event-001.json.retry").exists())
        self.assertTrue((Path(spool_dir) / "event-002.json").is_file())
        self.assertTrue((Path(spool_dir) / "event-002.json.retry").is_file())

        resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved2, 1)
        self.assertEqual(requests, [["A", "B"], ["B"]])
        self.assertEqual(accepted, ["A", "B"])
        self.assertEqual(list(Path(spool_dir).glob("event-*.json")), [])
        self.assertEqual(list(Path(spool_dir).glob("event-*.retry")), [])

    def test_drain_partial_mixed_ok_permanent_and_retryable(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_r2_partial_mix_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        requests = []
        accepted = []
        call_num = {"n": 0}

        def _insert(table_id, rows):
            del table_id
            call_num["n"] += 1
            markers = [r["content"]["marker"] for r in rows]
            requests.append(markers)
            if call_num["n"] == 1:
                accepted.append(markers[0])
                return [
                    {
                        "index": 1,
                        "errors": [{"reason": "invalid", "message": "bad field"}],
                    },
                    {
                        "index": 2,
                        "errors": [{"reason": "backendError", "message": "transient"}],
                    },
                ]
            accepted.extend(markers)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        for idx, marker in enumerate(("A", "B", "C"), start=1):
            env_file = Path(spool_dir) / f"event-00{idx}.json"
            env_file.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.0,
            drain_max_retry_age_seconds=86400.0,
        )
        resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved1, 2)
        self.assertFalse((Path(spool_dir) / "event-001.json").exists())
        self.assertFalse(
            (Path(spool_dir) / "dead-letter" / "event-001.write-failed.json").exists()
        )
        self.assertFalse((Path(spool_dir) / "event-002.json").exists())
        self.assertFalse((Path(spool_dir) / "event-002.json.retry").exists())
        self.assertTrue(
            (Path(spool_dir) / "dead-letter" / "event-002.write-failed.json").is_file()
        )
        self.assertTrue((Path(spool_dir) / "event-003.json").is_file())
        self.assertTrue((Path(spool_dir) / "event-003.json.retry").is_file())

        resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved2, 1)
        self.assertEqual(requests, [["A", "B", "C"], ["C"]])
        self.assertEqual(accepted, ["A", "C"])
        self.assertEqual(list(Path(spool_dir).glob("event-*.json")), [])
        self.assertEqual(list(Path(spool_dir).glob("event-*.retry")), [])

    def test_resolve_python_and_bare_bqaa_python3_no_traceback(self):
        from bigquery_agent_analytics_tracing import logger as bq_logger
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_c2_py3_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        # Bare command resolves via PATH, while distinct venv symlinks stay distinct.
        bin_dir = Path(tmp_root) / "fake_bin"
        bin_dir.mkdir(mode=0o755)
        venv_a = Path(tmp_root) / "venv_a"
        venv_b = Path(tmp_root) / "venv_b"
        for vdir in (venv_a, venv_b):
            (vdir / "bin").mkdir(parents=True, mode=0o755)
            (vdir / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
            os.symlink(sys.executable, str(vdir / "bin" / "python3"))
        self.assertNotEqual(
            bq_logger._resolve_python_executable(str(venv_a / "bin" / "python3")),
            bq_logger._resolve_python_executable(str(venv_b / "bin" / "python3")),
        )

        # In-process check: BQAA_PYTHON="python3" matching sys.executable skips Popen.
        orig_find_spec = importlib.util.find_spec
        orig_which = bq_logger.shutil.which
        orig_popen = bq_logger.subprocess.Popen
        saved_bq = sys.modules.pop("google.cloud.bigquery", None)
        popens = []
        env_backup = os.environ.copy()
        try:
            inproc_state = os.path.join(tmp_root, "inproc_state")
            inproc_log = os.path.join(tmp_root, "inproc.log")
            cfg = BQAAConfig(
                project_id="p",
                dataset="d",
                state_dir=inproc_state,
                spool_dir=os.path.join(tmp_root, "inproc_spool"),
                log_file=inproc_log,
                dry_run=False,
            )
            os.environ["BQAA_AUTO_DRAIN"] = "true"
            os.environ["BQAA_PYTHON"] = "python3"
            bq_logger.shutil.which = lambda cmd, *a, **kw: sys.executable if cmd == "python3" else orig_which(cmd, *a, **kw)
            importlib.util.find_spec = lambda name, *a, **kw: None if name == "google.cloud.bigquery" else orig_find_spec(name, *a, **kw)
            bq_logger.subprocess.Popen = lambda *a, **kw: popens.append(a)
            bq_logger._ensure_drainer(cfg)
            bq_logger._ensure_drainer(cfg)
        finally:
            importlib.util.find_spec = orig_find_spec
            bq_logger.shutil.which = orig_which
            bq_logger.subprocess.Popen = orig_popen
            if saved_bq is not None:
                sys.modules["google.cloud.bigquery"] = saved_bq
            os.environ.clear()
            os.environ.update(env_backup)

        self.assertEqual(popens, [])
        inproc_lines = Path(inproc_log).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(inproc_lines), 1)
        self.assertIn("DRAINER_SKIPPED", inproc_lines[0])
        self.assertNotIn("Traceback", "\n".join(inproc_lines))

        # E2E hook subprocess with isolated venv python3 lacking google-cloud-bigquery.
        iso_venv = Path(tmp_root) / "iso_venv"
        venv_res = subprocess.run(
            [sys.executable, "-m", "venv", "--without-pip", str(iso_venv)],
            capture_output=True, text=True, timeout=30,
        )
        if venv_res.returncode == 0:
            log_file = os.path.join(tmp_root, "hook_py3.log")
            spool_dir = os.path.join(tmp_root, "hook_spool")
            state_dir = os.path.join(tmp_root, "hook_state")
            env = {
                "PATH": f"{iso_venv / 'bin'}:{os.environ.get('PATH', '')}",
                "BQAA_PYTHON": "python3",
                "BQAA_PROJECT_ID": "p",
                "BQAA_DATASET": "d",
                "BQAA_TRACE_ENABLED": "true",
                "BQAA_DRY_RUN": "false",
                "BQAA_AUTO_DRAIN": "true",
                "BQAA_LOG_FILE": log_file,
                "BQAA_STATE_DIR": state_dir,
                "BQAA_SPOOL_DIR": spool_dir,
                "PYTHONDONTWRITEBYTECODE": "1",
                "HOME": tmp_root,
            }
            r1 = subprocess.run(
                ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
                input=json.dumps({"session_id": "py3-s1", "cwd": tmp_root}),
                capture_output=True, text=True, env=env, timeout=30,
            )
            r2 = subprocess.run(
                ["bash", str(DIST_DIR / "hooks" / "user_prompt_submit.sh")],
                input=json.dumps({"session_id": "py3-s1", "prompt": "hello"}),
                capture_output=True, text=True, env=env, timeout=30,
            )
            self.assertEqual(r1.returncode, 0, r1.stderr)
            self.assertEqual(r2.returncode, 0, r2.stderr)
            self.assertNotIn("Traceback", r1.stdout + r1.stderr + r2.stdout + r2.stderr)
            log_text = Path(log_file).read_text(encoding="utf-8")
            self.assertNotIn("DRAINER fatal:", log_text)
            self.assertNotIn("Traceback", log_text)
            skipped_lines = [l for l in log_text.splitlines() if "DRAINER_SKIPPED" in l]
            self.assertLessEqual(len(skipped_lines), 1, log_text)
            from bigquery_agent_analytics_tracing.config import destination_spool_dir
            spooled = list(Path(destination_spool_dir(spool_dir, "p", "d")).glob("event-*.json"))
            self.assertGreaterEqual(len(spooled), 1)

    def test_drain_main_without_bigquery_exits_zero_without_traceback(self):
        from bigquery_agent_analytics_tracing._utils import PYTHON_C_RUN_MODULE

        tmp_root = tempfile.mkdtemp(prefix="bqaa_c2_drain_nobq_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        state_dir = Path(tmp_root) / "state"
        spool_dir.mkdir(mode=0o700)
        state_dir.mkdir(mode=0o700)
        env_file = spool_dir / "event-001.json"
        env_file.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST"},
            }),
            encoding="utf-8",
        )
        log_file = os.path.join(tmp_root, "drain_nobq.log")
        env = {
            "PYTHONPATH": str(DIST_DIR / "vendor"),
            "BQAA_PROJECT_ID": "p",
            "BQAA_DATASET": "d",
            "BQAA_DRY_RUN": "false",
            "BQAA_LOG_FILE": log_file,
            "BQAA_STATE_DIR": str(state_dir),
            "BQAA_SPOOL_DIR": str(spool_dir),
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": tmp_root,
        }
        # `-S` omits site-packages so `google` is guaranteed absent on subprocess python.
        res = subprocess.run(
            [sys.executable, "-B", "-S", "-c", PYTHON_C_RUN_MODULE, "bigquery_agent_analytics_tracing.drain"],
            cwd=tmp_root, capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn("Traceback", res.stderr)
        log_text = Path(log_file).read_text(encoding="utf-8")
        self.assertNotIn("DRAINER fatal:", log_text)
        self.assertNotIn("Traceback", log_text)
        self.assertIn("DRAINER_SKIPPED", log_text)
        self.assertTrue(env_file.is_file(), "spooled file must remain when bigquery is absent")

    def test_storage_write_404_falls_back_without_unexpected_error_traceback(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_c2_sw404_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        spool_dir.mkdir(mode=0o700)
        log_file = os.path.join(tmp_root, "sw404.log")

        captured = []
        fake_classes = _install_fake_bigquery(
            self,
            insert_fn=lambda table_id, rows: (captured.append((table_id, rows)) or []),
        )

        # Install fake Storage Write API + pyarrow raising 404 NotFound or gRPC code=5.
        sw_mode = {"kind": "not_found_exc"}

        class FakeWriteClient:
            async def append_rows(self, requests):
                del requests
                if sw_mode["kind"] == "not_found_exc":
                    raise fake_classes["NotFound"]("404 Not found: Table p:d.agent_events")
                async def _resp_iter():
                    yield types.SimpleNamespace(
                        error=types.SimpleNamespace(code=5, message="Table not found")
                    )
                return _resp_iter()

        fake_pa = types.ModuleType("pyarrow")
        fake_pa.string = lambda: None
        fake_pa.bool_ = lambda: None
        fake_pa.int64 = lambda: None
        fake_pa.timestamp = lambda unit, tz=None: None
        fake_pa.field = lambda name, typ, nullable=True: name
        fake_pa.struct = lambda fields: fields
        fake_pa.list_ = lambda item: item
        fake_pa.schema = lambda fields: types.SimpleNamespace(
            serialize=lambda: types.SimpleNamespace(to_pybytes=lambda: b"schema")
        )
        fake_pa.RecordBatch = types.SimpleNamespace(
            from_pylist=lambda rows, schema=None: types.SimpleNamespace(
                serialize=lambda: types.SimpleNamespace(to_pybytes=lambda: b"batch")
            )
        )

        bs_mod = types.ModuleType("google.cloud.bigquery_storage_v1")
        bs_types = types.ModuleType("google.cloud.bigquery_storage_v1.types")
        bs_types.AppendRowsRequest = lambda **kw: types.SimpleNamespace(
            arrow_rows=types.SimpleNamespace(
                writer_schema=types.SimpleNamespace(serialized_schema=b""),
                rows=types.SimpleNamespace(serialized_record_batch=b""),
            )
        )
        bs_services = types.ModuleType("google.cloud.bigquery_storage_v1.services")
        bs_bqw = types.ModuleType("google.cloud.bigquery_storage_v1.services.big_query_write")
        bs_async = types.ModuleType("google.cloud.bigquery_storage_v1.services.big_query_write.async_client")
        bs_async.BigQueryWriteAsyncClient = FakeWriteClient
        bs_mod.types = bs_types
        bs_mod.services = bs_services
        bs_services.big_query_write = bs_bqw
        bs_bqw.async_client = bs_async
        sys.modules["google.cloud"].bigquery_storage_v1 = bs_mod

        extra_mods = [
            ("pyarrow", fake_pa),
            ("google.cloud.bigquery_storage_v1", bs_mod),
            ("google.cloud.bigquery_storage_v1.types", bs_types),
            ("google.cloud.bigquery_storage_v1.services", bs_services),
            ("google.cloud.bigquery_storage_v1.services.big_query_write", bs_bqw),
            ("google.cloud.bigquery_storage_v1.services.big_query_write.async_client", bs_async),
        ]
        old_extra = {k: sys.modules.get(k) for k, _ in extra_mods}
        for k, v in extra_mods:
            sys.modules[k] = v

        def _restore_extra():
            for k, v in old_extra.items():
                if v is not None:
                    sys.modules[k] = v
                else:
                    sys.modules.pop(k, None)

        self.addCleanup(_restore_extra)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=str(spool_dir),
            state_dir=str(spool_dir),
            log_file=log_file,
            dry_run=False,
        )

        for idx, kind in enumerate(("not_found_exc", "grpc_code_5"), start=1):
            sw_mode["kind"] = kind
            env_file = spool_dir / f"event-sw-{idx}.json"
            env_file.write_text(
                json.dumps({
                    "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            resolved = asyncio.run(drain._drain_once(cfg, use_storage_api=True))
            self.assertEqual(resolved, 1)
            self.assertFalse(env_file.exists())

        self.assertEqual(len(captured), 2)
        log_text = Path(log_file).read_text(encoding="utf-8")
        self.assertIn("DRAINER storage_api_failed, falling back to insert_rows_json", log_text)
        self.assertNotIn("DRAINER unexpected_error", log_text)
        self.assertNotIn("Traceback", log_text)


class TestRound3Fixes(unittest.TestCase):
    """Regression coverage for deferred drainer idle, legacy migration, and O(1) dedup."""

    def test_drain_run_keeps_alive_across_deferred_backoff_longer_than_idle(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_r3_defer_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        env_path = Path(spool_dir) / "event-001.json"
        env_path.write_text(
            json.dumps({
                "config": {
                    "project_id": "p",
                    "dataset": "d",
                    "table": "agent_events",
                },
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "defer-then-ok"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )

        attempts = {"n": 0}
        delivered = []

        def _insert(table_id, rows):
            del table_id
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("503 Service Unavailable")
            delivered.extend(rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.2,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
            drain_max_retry_age_seconds=86400.0,
        )
        asyncio.run(drain._run(cfg))

        self.assertEqual(attempts["n"], 2)
        self.assertEqual(len(delivered), 1)
        self.assertEqual(delivered[0]["content"]["marker"], "defer-then-ok")
        self.assertFalse(env_path.exists())
        self.assertFalse((Path(spool_dir) / "event-001.json.retry").exists())

    def test_drain_run_exits_idle_when_only_skipped_entry_present(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_r3_skip_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        spool_dir.mkdir(mode=0o700)
        outside = Path(tmp_root) / "outside.json"
        outside.write_text("{}", encoding="utf-8")
        sym = spool_dir / "event-sym.json"
        os.symlink(str(outside), str(sym))

        _install_fake_bigquery(self, insert_fn=lambda table_id, rows: [])

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=str(spool_dir),
            state_dir=str(spool_dir),
            log_file=str(Path(tmp_root) / "drain.log"),
            dry_run=False,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        started = time.monotonic()
        asyncio.run(drain._run(cfg))
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0)
        log_text = (Path(tmp_root) / "drain.log").read_text(encoding="utf-8")
        self.assertIn("DRAINER skipped", log_text)

    def test_drain_retry_meta_write_failure_preserves_backoff_and_expiry(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_p2_retry_enospc_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            prefix = f"{Path(spool_dir)}{os.sep}"
            for cache in (drain._in_memory_retry_meta,):
                for key in list(cache):
                    if key.startswith(prefix):
                        del cache[key]
            for s in (drain._skipped_paths, drain._retry_skipped_paths):
                for key in list(s):
                    if key.startswith(prefix):
                        s.discard(key)
            for s in (drain._reported_skips, drain._diagnostic_logs, drain._reported_saturation_logs):
                for key in list(s):
                    if prefix in key:
                        s.discard(key)

        self.addCleanup(_cleanup_drain_globals)

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        fail_env = _write_envelope("event-001-fail.json", "fail-always")
        attempts = {"fail": 0}
        delivered = []

        def _insert(table_id, rows):
            del table_id
            outcomes = []
            for idx, row in enumerate(rows):
                if row["content"]["marker"] == "fail-always":
                    attempts["fail"] += 1
                    outcomes.append({
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    })
                else:
                    delivered.append(row["content"]["marker"])
            return outcomes

        _install_fake_bigquery(self, insert_fn=_insert)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=12.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.03,
        )
        now = {"t": 1000.0}

        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved1, 0)
            self.assertEqual(attempts["fail"], 1)
            self.assertTrue(fail_env.is_file())
            self.assertFalse((Path(spool_dir) / "event-001-fail.json.retry").exists())

            mem_key = str(fail_env)
            self.assertIn(mem_key, drain._in_memory_retry_meta)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["first_failed_at"], 1000.0)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["attempts"], 1)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["next_retry_at"], 1005.0)

            wait1 = drain._next_deferred_wait_seconds(cfg)
            self.assertIsNotNone(wait1)
            self.assertAlmostEqual(wait1, 5.0, places=3)

            # During backoff window: healthy sibling drains while fail stays deferred.
            now["t"] = 1002.0
            healthy_env = _write_envelope("event-002-healthy.json", "healthy-1")
            resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved2, 1)
            self.assertEqual(delivered, ["healthy-1"])
            self.assertFalse(healthy_env.exists())
            self.assertEqual(attempts["fail"], 1)

            # Backoff elapsed: second attempt preserves first_failed_at=1000.0.
            now["t"] = 1005.0
            resolved3 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved3, 0)
            self.assertEqual(attempts["fail"], 2)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["first_failed_at"], 1000.0)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["attempts"], 2)
            self.assertEqual(drain._in_memory_retry_meta[mem_key]["next_retry_at"], 1015.0)
            wait2 = drain._next_deferred_wait_seconds(cfg)
            self.assertIsNotNone(wait2)
            self.assertAlmostEqual(wait2, 7.0, places=3)

            # Max retry age elapsed: expires and quarantines instead of resetting first_failed_at.
            now["t"] = 1012.0
            resolved4 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved4, 1)
            self.assertEqual(attempts["fail"], 2)
            self.assertFalse(fail_env.exists())
            self.assertNotIn(mem_key, drain._in_memory_retry_meta)
            self.assertTrue(
                (Path(spool_dir) / "dead-letter" / "event-001-fail.write-failed.json").is_file()
            )

        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(log_text.count("DRAINER retry_meta_write_failed"), 1, log_text)
        self.assertIn("DRAINER retry_expired file=event-001-fail.json", log_text)

        # Full _run loop check with real time: nonzero backoff sleeps and eventual expiry.
        run_fail_env = _write_envelope("event-003-run.json", "fail-always")
        attempts["fail"] = 0
        sleeps = []
        orig_sleep = asyncio.sleep

        async def _tracking_sleep(delay):
            sleeps.append(delay)
            await orig_sleep(delay)

        cfg_run = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=0.12,
            drain_max_retry_age_seconds=0.20,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.03,
        )
        with mock.patch.object(
            drain,
            "_write_retry_meta",
            side_effect=OSError(errno.ENOSPC, "No space left on device"),
        ), mock.patch.object(drain.asyncio, "sleep", side_effect=_tracking_sleep):
            asyncio.run(asyncio.wait_for(drain._run(cfg_run), timeout=3.0))

        self.assertTrue(sleeps, "expected _run to sleep during retry backoff")
        self.assertTrue(
            all(delay > 0.0 for delay in sleeps),
            f"expected strictly positive backoff/poll sleeps, got {sleeps}",
        )
        self.assertLessEqual(attempts["fail"], 3)
        self.assertFalse(run_fail_env.exists())
        self.assertTrue(
            (Path(spool_dir) / "dead-letter" / "event-003-run.write-failed.json").is_file()
        )

    def test_drain_quarantine_oserror_skips_expired_without_spinning_and_drains_healthy(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing._utils import PrivateDir
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_p2_quar_enospc_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            prefix = f"{Path(spool_dir)}{os.sep}"
            for key in list(drain._in_memory_retry_meta):
                if key.startswith(prefix):
                    del drain._in_memory_retry_meta[key]
            for key in list(drain._skipped_paths):
                if key.startswith(prefix):
                    drain._skipped_paths.discard(key)
            for key in list(drain._retry_skipped_paths):
                if key.startswith(prefix):
                    drain._retry_skipped_paths.discard(key)
            for s in (drain._reported_skips, drain._diagnostic_logs, drain._reported_saturation_logs):
                for key in list(s):
                    if prefix in key:
                        s.discard(key)

        self.addCleanup(_cleanup_drain_globals)

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        expired_env = _write_envelope("event-001-expired.json", "expired-bad")
        healthy_env = _write_envelope("event-002-healthy.json", "healthy-ok")

        delivered = []
        fail_attempts = {"n": 0}

        def _insert(table_id, rows):
            del table_id
            for row in rows:
                marker = row["content"]["marker"]
                if marker == "expired-bad":
                    fail_attempts["n"] += 1
                else:
                    delivered.append(marker)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=0.1,
            drain_max_retry_age_seconds=0.05,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )

        # Simulate combined full-disk condition: .retry sidecar write fails with ENOSPC,
        # retry age elapses, and dead-letter subdir creation also fails with ENOSPC.
        orig_subdir = PrivateDir.subdir

        def _failing_dead_letter(self_dir, sub_name):
            if sub_name == "dead-letter":
                raise OSError(errno.ENOSPC, "No space left on device")
            return orig_subdir(self_dir, sub_name)

        now = {"t": 2000.0}
        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ), \
             mock.patch.object(PrivateDir, "subdir", _failing_dead_letter):
            # Seed failure state as if prior pass failed .retry write under ENOSPC.
            with drain.ensure_private_dir(spool_dir) as spool_priv:
                drain._store_in_memory_retry_meta(
                    spool_priv,
                    "event-001-expired.json",
                    {
                        "first_failed_at": 1900.0,
                        "attempts": 2,
                        "next_retry_at": 1910.0,
                        "last_error": "prior ENOSPC deferral",
                    },
                )

            sleeps = []
            orig_sleep = asyncio.sleep

            async def _tracking_sleep(delay):
                sleeps.append(delay)
                await orig_sleep(delay)

            with mock.patch.object(drain.asyncio, "sleep", side_effect=_tracking_sleep):
                asyncio.run(asyncio.wait_for(drain._run(cfg), timeout=2.0))

        self.assertEqual(fail_attempts["n"], 0, "expired file must not retry BQ send")
        self.assertEqual(delivered, ["healthy-ok"])
        self.assertFalse(healthy_env.exists())
        self.assertTrue(expired_env.is_file(), "un-quarantinable file must stay in spool")
        self.assertFalse((Path(spool_dir) / "dead-letter").exists())
        self.assertIn(str(expired_env), drain._skipped_paths)
        self.assertIsNone(drain._next_deferred_wait_seconds(cfg))
        self.assertTrue(all(d > 0.0 for d in sleeps), f"unexpected zero sleep: {sleeps}")

        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(
            log_text.count("DRAINER retry_expiry_quarantine_failed"),
            1,
            log_text,
        )
        self.assertIn("DRAINER retry_expired file=event-001-expired.json", log_text)
        self.assertNotIn("Traceback", log_text)
        self.assertNotIn("DRAINER fatal", log_text)

    def test_drain_retry_meta_cap_does_not_evict_or_spin_and_drains_healthy(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_retry_cap_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        # Phase 1 (within 2 * cap = 4): include an event-legacy-* overflow envelope
        # ('l' > '0'..'9') to prove filename order never starves timestamped healthy files.
        fail1 = _write_envelope("event-001-fail.json", "fail-1")
        fail2 = _write_envelope("event-002-fail.json", "fail-2")
        fail3 = _write_envelope("event-003-fail.json", "fail-3")
        fail_legacy = _write_envelope("event-legacy-004-fail.json", "fail-legacy")

        attempts_by_marker: dict[str, int] = {}
        delivered: list[str] = []

        def _insert(table_id, rows):
            del table_id
            outcomes = []
            for idx, row in enumerate(rows):
                marker = row["content"]["marker"]
                if marker.startswith("fail-"):
                    attempts_by_marker[marker] = attempts_by_marker.get(marker, 0) + 1
                    outcomes.append({
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    })
                else:
                    delivered.append(marker)
            return outcomes

        _install_fake_bigquery(self, insert_fn=_insert)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=12.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        now = {"t": 1000.0}

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            # Pass 1 at t=1000.0: 4 failing envelopes with cap=2 (2 tracked + 2 skipped).
            resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved1, 0)
            self.assertEqual(
                attempts_by_marker,
                {"fail-1": 1, "fail-2": 1, "fail-3": 1, "fail-legacy": 1},
            )
            # Existing tracked entries (fail1, fail2) are NOT evicted and caps are respected.
            self.assertEqual(len(drain._in_memory_retry_meta), 2)
            self.assertEqual(drain._skipped_paths, set())
            self.assertEqual(len(drain._retry_skipped_paths), 2)
            self.assertEqual(drain._retry_meta_saturated, set())
            self.assertIn(str(fail1), drain._in_memory_retry_meta)
            self.assertIn(str(fail2), drain._in_memory_retry_meta)
            self.assertIn(str(fail3), drain._retry_skipped_paths)
            self.assertIn(str(fail_legacy), drain._retry_skipped_paths)
            for tracked in (str(fail1), str(fail2)):
                self.assertEqual(drain._in_memory_retry_meta[tracked]["first_failed_at"], 1000.0)
                self.assertEqual(drain._in_memory_retry_meta[tracked]["attempts"], 1)
                self.assertEqual(drain._in_memory_retry_meta[tracked]["next_retry_at"], 1005.0)

            # Wait calculation must NOT return 0.0 (no 0s retry spin on overflow envelopes).
            wait1 = drain._next_deferred_wait_seconds(cfg)
            self.assertIsNotNone(wait1)
            self.assertAlmostEqual(wait1, 5.0, places=3)

            # During backoff window (t=1002.0): healthy envelopes sorting both BEFORE
            # event-001-fail.json and BEFORE event-legacy-004-fail.json drain cleanly!
            now["t"] = 1002.0
            healthy_early = _write_envelope("event-000-healthy.json", "healthy-early")
            healthy_after = _write_envelope("event-006-healthy.json", "healthy-after-legacy")
            resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved2, 2)
            self.assertEqual(delivered, ["healthy-early", "healthy-after-legacy"])
            self.assertFalse(healthy_early.exists())
            self.assertFalse(healthy_after.exists())
            self.assertEqual(
                attempts_by_marker,
                {"fail-1": 1, "fail-2": 1, "fail-3": 1, "fail-legacy": 1},
            )

            # Phase 2 (saturated > 2 * cap at t=1003.0): an overflow envelope with a
            # pre-existing stale .retry sidecar (written before ENOSPC, next_retry_at <= now)
            # plus a sidecar-less overflow envelope fail under ENOSPC and saturate the spool.
            now["t"] = 1003.0
            fail_stale = _write_envelope("event-005-stale-fail.json", "fail-stale")
            stale_sidecar = Path(spool_dir) / "event-005-stale-fail.json.retry"
            stale_sidecar.write_text(
                json.dumps({
                    "first_failed_at": 995.0,
                    "attempts": 1,
                    "next_retry_at": 1000.0,
                    "last_error": "prior 503 before ENOSPC",
                }),
                encoding="utf-8",
            )
            os.chmod(stale_sidecar, 0o600)
            fail_over = _write_envelope("event-007-overflow-fail.json", "fail-over")

            resolved_sat = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_sat, 0)
            self.assertEqual(
                attempts_by_marker,
                {
                    "fail-1": 1,
                    "fail-2": 1,
                    "fail-3": 1,
                    "fail-legacy": 1,
                    "fail-stale": 1,
                    "fail-over": 1,
                },
            )
            self.assertEqual(drain._retry_meta_saturated, {str(Path(spool_dir))})
            self.assertEqual(len(drain._in_memory_retry_meta), 2)
            self.assertEqual(drain._skipped_paths, set())
            self.assertEqual(len(drain._retry_skipped_paths), 2)
            self.assertLessEqual(len(drain._reported_skips), 4)

            # Critical P2 check: stale .retry sidecar on overflow envelope must NOT
            # return 0.0 from _next_deferred_wait_seconds or spin on a second pass!
            wait_sat = drain._next_deferred_wait_seconds(cfg)
            self.assertIsNotNone(wait_sat)
            self.assertAlmostEqual(wait_sat, 2.0, places=3)
            resolved_sat_again = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_sat_again, 0)
            self.assertEqual(attempts_by_marker["fail-stale"], 1)
            self.assertEqual(attempts_by_marker["fail-over"], 1)

            # Backoff elapsed (t=1005.0): only tracked fail-1 and fail-2 retry and
            # preserve first_failed_at=1000.0; overflow envelopes do not spin or evict them.
            now["t"] = 1005.0
            resolved3 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved3, 0)
            self.assertEqual(
                attempts_by_marker,
                {
                    "fail-1": 2,
                    "fail-2": 2,
                    "fail-3": 1,
                    "fail-legacy": 1,
                    "fail-stale": 1,
                    "fail-over": 1,
                },
            )
            self.assertEqual(len(drain._in_memory_retry_meta), 2)
            for tracked in (str(fail1), str(fail2)):
                self.assertEqual(drain._in_memory_retry_meta[tracked]["first_failed_at"], 1000.0)
                self.assertEqual(drain._in_memory_retry_meta[tracked]["attempts"], 2)
                self.assertEqual(drain._in_memory_retry_meta[tracked]["next_retry_at"], 1015.0)

            # Expiry elapsed (t=1012.0): tracked fail-1 and fail-2 expire and quarantine,
            # freeing in-memory slots mid-pass so parked overflow (including expired fail-stale)
            # immediately un-parks: fail-stale quarantines (total resolved=3), while fail-3 and
            # fail-over take the two free in-memory slots and fail-legacy parks (1/2).
            now["t"] = 1012.0
            resolved4 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved4, 3)
            self.assertFalse(fail1.exists())
            self.assertFalse(fail2.exists())
            self.assertFalse(fail_stale.exists())
            self.assertTrue((Path(spool_dir) / "dead-letter" / "event-001-fail.write-failed.json").is_file())
            self.assertTrue((Path(spool_dir) / "dead-letter" / "event-002-fail.write-failed.json").is_file())
            self.assertTrue((Path(spool_dir) / "dead-letter" / "event-005-stale-fail.write-failed.json").is_file())
            self.assertEqual(set(drain._in_memory_retry_meta), {str(fail3), str(fail_over)})
            self.assertEqual(drain._retry_skipped_paths, {str(fail_legacy)})
            self.assertEqual(drain._retry_meta_saturated, set())

        # Disk recovery check: once _write_retry_meta succeeds again without any manual
        # _skipped_paths clearing, healthy envelopes drain immediately and parked retry
        # envelopes write .retry sidecars once due.
        healthy_recovered = _write_envelope("event-008-healthy.json", "healthy-after-recovery")
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]):
            resolved_rec = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_rec, 1)
            self.assertIn("healthy-after-recovery", delivered)
            self.assertFalse(healthy_recovered.exists())
            now["t"] = 1017.0
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertEqual(drain._retry_skipped_paths, set())
            self.assertTrue((Path(spool_dir) / "event-003-fail.json.retry").is_file())
            now["t"] = 1018.0
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertTrue((Path(spool_dir) / "event-legacy-004-fail.json.retry").is_file())
            self.assertEqual(drain._retry_meta_saturated, set())

        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(log_text.count("DRAINER retry_meta_cap_reached cap=2"), 1, log_text)
        self.assertEqual(log_text.count("DRAINER retry_meta_saturated cap=2"), 1, log_text)
        self.assertIn("DRAINER retry_expired file=event-001-fail.json", log_text)
        self.assertIn("DRAINER retry_expired file=event-002-fail.json", log_text)

        # Edge case: cap=0 must not crash or spin when retry sidecar writes fail.
        _cleanup_drain_globals()
        for sidecar in Path(spool_dir).glob("*.retry"):
            sidecar.unlink()
        now["t"] = 1018.0
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 0), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            resolved_zero = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_zero, 0)
            self.assertEqual(drain._in_memory_retry_meta, {})
            self.assertEqual(drain._skipped_paths, set())
            self.assertLessEqual(len(drain._retry_skipped_paths), 1)
            self.assertEqual(drain._retry_meta_saturated, {str(Path(spool_dir))})
            self.assertIsNone(drain._next_deferred_wait_seconds(cfg))

    def test_drain_non_retry_skip_cap_does_not_starve_healthy_or_pin_run(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_nonretry_skip_cap_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_healthy(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        delivered: list[str] = []
        mode = {"kind": "ok"}
        insert_calls = {"n": 0}

        def _insert(table_id, rows):
            del table_id
            insert_calls["n"] += len(rows)
            if mode["kind"] == "permanent_all":
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "invalid", "message": "bad row"}],
                    }
                    for idx in range(len(rows))
                ]
            if mode["kind"] == "permanent_partial":
                outcomes = []
                for idx, row in enumerate(rows):
                    if row["content"]["marker"] == "perm-partial":
                        outcomes.append({
                            "index": idx,
                            "errors": [{"reason": "invalid", "message": "bad row"}],
                        })
                    else:
                        delivered.append(row["content"]["marker"])
                return outcomes
            for row in rows:
                delivered.append(row["content"]["marker"])
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=86400.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )

        # 3 corrupt unquarantinable files sorting BEFORE a healthy 4th file (cap=2):
        # healthy file must still drain, _retry_meta_saturated must stay empty, and
        # _next_deferred_wait_seconds must return None once only junk remains.
        for idx in (1, 2, 3):
            corrupt = Path(spool_dir) / f"event-00{idx}-corrupt.json"
            corrupt.write_text("{corrupt-json", encoding="utf-8")
            os.chmod(corrupt, 0o600)
        healthy_env = _write_healthy("event-004-healthy.json", "healthy-after-corrupt")

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain, "_quarantine", return_value=False):
            resolved = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved, 1)
            self.assertEqual(delivered, ["healthy-after-corrupt"])
            self.assertFalse(healthy_env.exists())
            self.assertEqual(drain._retry_meta_saturated, set())
            self.assertEqual(drain._skip_cap_saturated, {str(Path(spool_dir))})
            self.assertEqual(len(drain._skipped_paths), 2)
            self.assertEqual(drain._in_memory_retry_meta, {})
            self.assertIsNone(drain._next_deferred_wait_seconds(cfg))

            # Second pass when only the 3 corrupt files remain must resolve 0 (no spin).
            resolved_idle = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_idle, 0)
            self.assertIsNone(drain._next_deferred_wait_seconds(cfg))

            # Full _run loop with a fresh healthy file drains it and exits after idle.
            delivered.clear()
            healthy_run = _write_healthy("event-005-healthy.json", "healthy-in-run")
            sleeps: list[float] = []
            orig_sleep = asyncio.sleep

            async def _tracking_sleep(delay):
                sleeps.append(delay)
                await orig_sleep(delay)

            with mock.patch.object(drain.asyncio, "sleep", side_effect=_tracking_sleep):
                asyncio.run(asyncio.wait_for(drain._run(cfg), timeout=2.0))
            self.assertEqual(delivered, ["healthy-in-run"])
            self.assertFalse(healthy_run.exists())
            self.assertTrue(sleeps)
            self.assertTrue(all(d > 0.0 for d in sleeps), sleeps)

        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(log_text.count("DRAINER skip_cap_saturated cap=2"), 1, log_text)

        # Edge case cap=0: corrupt files beyond skip cap must still let healthy files drain.
        _cleanup_drain_globals()
        delivered.clear()
        healthy_zero = _write_healthy("event-006-healthy.json", "healthy-cap-zero")
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 0), \
             mock.patch.object(drain, "_quarantine", return_value=False):
            resolved_zero = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_zero, 1)
            self.assertEqual(delivered, ["healthy-cap-zero"])
            self.assertFalse(healthy_zero.exists())
            self.assertEqual(drain._retry_meta_saturated, set())
            self.assertEqual(drain._skip_cap_saturated, {str(Path(spool_dir))})
            self.assertIsNone(drain._next_deferred_wait_seconds(cfg))

        # Quarantine-failure paths (missing-config, partial permanent, whole-batch permanent)
        # must retain quarantine_pending=True in _in_memory_retry_meta when _skipped_paths
        # is full (never re-sending to BigQuery) and recover once dead-letter is writable.
        cfg_missing = BQAAConfig(
            project_id="",
            dataset="",
            table="",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=86400.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        for label, case_fn in (
            ("missing_config", lambda: _write_healthy("event-010-perm.json", "missing").write_text(
                json.dumps({"config": {}, "row": {"event_type": "LLM_REQUEST"}}), encoding="utf-8"
            )),
            ("permanent_partial", lambda: (
                _write_healthy("event-010-perm.json", "perm-partial"),
                _write_healthy("event-011-ok.json", "ok-alongside"),
            )),
            ("permanent_all", lambda: _write_healthy("event-010-perm.json", "perm-all")),
        ):
            with self.subTest(quarantine_fail_path=label):
                _cleanup_drain_globals()
                for stale in Path(spool_dir).glob("event-*"):
                    stale.unlink()
                dead_dir = Path(spool_dir) / "dead-letter"
                if dead_dir.exists():
                    shutil.rmtree(dead_dir, ignore_errors=True)
                s1 = _write_healthy("event-001-skip.json", "s1")
                s1.write_text("{bad", encoding="utf-8")
                s2 = _write_healthy("event-002-skip.json", "s2")
                s2.write_text("{bad", encoding="utf-8")
                case_fn()
                p_perm = Path(spool_dir) / "event-010-perm.json"
                sub_cfg = cfg_missing if label == "missing_config" else cfg
                mode["kind"] = "ok" if label == "missing_config" else label
                insert_calls["n"] = 0
                now_t = time.time()
                with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
                     mock.patch.object(drain, "_quarantine", return_value=False):
                    # Realistically fill _skipped_paths (2/2) via failing quarantine on s1, s2;
                    # do NOT seed fake _retry_meta_saturated.
                    drain._skipped_paths.update({str(s1), str(s2)})
                    drain._in_memory_retry_meta[str(p_perm)] = {
                        "first_failed_at": now_t,
                        "attempts": 1,
                        "next_retry_at": 0.0,
                        "last_error": "prior transient",
                    }
                    asyncio.run(drain._drain_once(sub_cfg, use_storage_api=False))
                    self.assertIn(str(p_perm), drain._in_memory_retry_meta)
                    self.assertIs(
                        drain._in_memory_retry_meta[str(p_perm)].get("quarantine_pending"),
                        True,
                    )
                    self.assertIsNone(drain._next_deferred_wait_seconds(sub_cfg))
                    calls_after_first = insert_calls["n"]
                    if label != "missing_config":
                        self.assertGreater(calls_after_first, 0)
                    # Subsequent poll while quarantine still fails must NEVER re-call BigQuery.
                    resolved_again = asyncio.run(drain._drain_once(sub_cfg, use_storage_api=False))
                    self.assertEqual(resolved_again, 0)
                    self.assertEqual(insert_calls["n"], calls_after_first)
                    self.assertIsNone(drain._next_deferred_wait_seconds(sub_cfg))

                # Once dead-letter recovers, quarantine_pending resolves cleanly without BigQuery call.
                with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2):
                    resolved_recover = asyncio.run(drain._drain_once(sub_cfg, use_storage_api=False))
                    self.assertEqual(resolved_recover, 1)
                    self.assertEqual(insert_calls["n"], calls_after_first)
                    self.assertNotIn(str(p_perm), drain._in_memory_retry_meta)
                    self.assertFalse(p_perm.exists())

    def test_drain_in_process_disk_recovery_clears_saturation_without_clearing_skipped_paths(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_inproc_rec_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        junk = Path(spool_dir) / "event-000-corrupt.json"
        junk.write_text("{corrupt-json", encoding="utf-8")
        os.chmod(junk, 0o600)
        fail1 = _write_envelope("event-001-fail.json", "fail-1")
        fail2 = _write_envelope("event-002-fail.json", "fail-2")
        fail3 = _write_envelope("event-003-fail.json", "fail-3")
        fail4 = _write_envelope("event-004-fail.json", "fail-4")

        delivered: list[str] = []

        def _insert(table_id, rows):
            del table_id
            outcomes = []
            for idx, row in enumerate(rows):
                marker = row["content"]["marker"]
                if marker.startswith("fail-"):
                    outcomes.append({
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    })
                else:
                    delivered.append(marker)
            return outcomes

        _install_fake_bigquery(self, insert_fn=_insert)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=20.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        now = {"t": 1000.0}

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]):
            orig_quarantine = drain._quarantine

            def _fail_corrupt_quarantine(spool, name, reason, **kwargs):
                if name == junk.name:
                    return False
                return orig_quarantine(spool, name, reason, **kwargs)

            with mock.patch.object(
                drain, "_quarantine", side_effect=_fail_corrupt_quarantine
            ), mock.patch.object(
                drain,
                "_write_retry_meta",
                side_effect=OSError(errno.ENOSPC, "No space left on device"),
            ):
                # Fill non-retry _skipped_paths (junk), _in_memory_retry_meta (fail1, fail2),
                # and _retry_skipped_paths (fail3, fail4).
                self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
                self.assertEqual(drain._skipped_paths, {str(junk)})
                self.assertEqual(len(drain._in_memory_retry_meta), 2)
                self.assertEqual(drain._retry_skipped_paths, {str(fail3), str(fail4)})

                # Overflow envelope fail5 triggers _retry_meta_saturated under ENOSPC.
                now["t"] = 1001.0
                fail5 = _write_envelope("event-005-fail.json", "fail-5")
                self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
                self.assertEqual(drain._retry_meta_saturated, {str(Path(spool_dir))})

                # Healthy file written while saturated waits for recovery.
                now["t"] = 1002.0
                healthy_while_sat = _write_envelope("event-006-healthy.json", "healthy-while-sat")
                self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
                self.assertTrue(healthy_while_sat.exists())
                self.assertEqual(delivered, [])

            # Disk recovers BEFORE tracked expiry (now=1005.0 < 1020.0): sidecar write
            # clears _retry_meta_saturated and releases _retry_skipped_paths, while
            # keeping non-retry _skipped_paths ({junk}) intact!
            now["t"] = 1005.0
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertEqual(
                drain._retry_meta_saturated,
                set(),
                "successful tracked sidecar write must clear saturation in-process",
            )
            self.assertEqual(
                drain._retry_skipped_paths,
                set(),
                "_retry_skipped_paths must release in-process so parked retries get another turn",
            )
            self.assertEqual(
                drain._skipped_paths,
                {str(junk)},
                "non-retry _skipped_paths must remain intact across in-process recovery",
            )
            self.assertTrue((Path(spool_dir) / "event-001-fail.json.retry").is_file())
            self.assertTrue((Path(spool_dir) / "event-002-fail.json.retry").is_file())

            # Next pass drains healthy_while_sat AND writes sidecars for previously parked fail3..fail5.
            now["t"] = 1006.0
            resolved_healthy = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_healthy, 1)
            self.assertEqual(delivered, ["healthy-while-sat"])
            self.assertFalse(healthy_while_sat.exists())
            self.assertTrue((Path(spool_dir) / "event-003-fail.json.retry").is_file())
            self.assertTrue((Path(spool_dir) / "event-004-fail.json.retry").is_file())
            self.assertTrue((Path(spool_dir) / "event-005-fail.json.retry").is_file())
            self.assertEqual(drain._skipped_paths, {str(junk)})

    def test_drain_run_sleep_floor_zero_wait_due_and_saturated_idle_exit(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_run_floor_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        now = {"t": 1000.0}
        state = {"mode": "fail_once", "calls": 0}
        delivered: list[str] = []

        def _insert(table_id, rows):
            del table_id
            state["calls"] += 1
            if state["mode"] == "fail_once" and state["calls"] == 1:
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    }
                    for idx in range(len(rows))
                ]
            if state["mode"] == "fail_and_advance":
                now["t"] += 0.06
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    }
                    for idx in range(len(rows))
                ]
            if state["mode"] == "fail_always":
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    }
                    for idx in range(len(rows))
                ]
            delivered.extend(row["content"]["marker"] for row in rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        # 1a. Deterministic fake clock with backoff=0.0: wait_due==0.0 must clamp to poll.
        env1 = _write_envelope("event-001-floor.json", "floor-ok")
        cfg_floor = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.0,
            drain_max_retry_age_seconds=86400.0,
            drain_idle_seconds=0.6,
            drain_poll_seconds=0.2,
        )
        orig_wait_fn = drain._next_deferred_wait_seconds
        orig_sleep = asyncio.sleep
        wait_dues1: list[float | None] = []
        sleeps1: list[float] = []

        def _spy_wait_1(c):
            res = orig_wait_fn(c)
            wait_dues1.append(res)
            return res

        async def _fake_sleep_1(delay):
            sleeps1.append(delay)
            now["t"] += delay
            await orig_sleep(0)

        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain, "_next_deferred_wait_seconds", side_effect=_spy_wait_1), \
             mock.patch.object(drain.asyncio, "sleep", side_effect=_fake_sleep_1):
            asyncio.run(asyncio.wait_for(drain._run(cfg_floor), timeout=2.0))

        self.assertIn(0.0, wait_dues1, f"expected wait_due == 0.0, got {wait_dues1}")
        self.assertTrue(sleeps1)
        self.assertTrue(all(d == cfg_floor.drain_poll_seconds for d in sleeps1), sleeps1)
        self.assertEqual(delivered, ["floor-ok"])
        self.assertFalse(env1.exists())

        # 1b. Deterministic mutant killer for positive backoff (>0) hitting wait_due==0.0:
        #     event-001 fails at t=2000.0 (due=2000.25); pass 2 at t=2000.20 skips event-001,
        #     runs event-002 whose simulated RPC latency advances clock past 2000.25 so
        #     _next_deferred_wait_seconds deterministically sees wait_due == 0.0 with backoff > 0!
        _cleanup_drain_globals()
        delivered.clear()
        now["t"] = 2000.0
        state["calls"] = 0
        state["mode"] = "fail_and_advance"
        _write_envelope("event-001-late.json", "late-1")
        cfg_pos = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.25,
            drain_max_retry_age_seconds=5.0,
            drain_idle_seconds=0.6,
            drain_poll_seconds=0.2,
        )
        wait_dues_pos: list[float | None] = []
        sleeps_pos: list[float] = []
        probe_written = {"done": False}

        def _spy_wait_pos(c):
            res = orig_wait_fn(c)
            wait_dues_pos.append(res)
            return res

        async def _fake_sleep_pos(delay):
            sleeps_pos.append(delay)
            now["t"] += delay
            if not probe_written["done"]:
                probe_written["done"] = True
                _write_envelope("event-002-probe.json", "probe-2")
            else:
                state["mode"] = "ok"
            await orig_sleep(0)

        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain, "_next_deferred_wait_seconds", side_effect=_spy_wait_pos), \
             mock.patch.object(drain.asyncio, "sleep", side_effect=_fake_sleep_pos):
            asyncio.run(asyncio.wait_for(drain._run(cfg_pos), timeout=2.0))

        self.assertIn(0.0, wait_dues_pos, f"expected wait_due==0.0 under positive backoff, got {wait_dues_pos}")
        self.assertEqual(sleeps_pos[1], cfg_pos.drain_poll_seconds, sleeps_pos)
        self.assertTrue(all(d > 0.0 for d in sleeps_pos), sleeps_pos)
        self.assertEqual(sorted(delivered), ["late-1", "probe-2"])

        # 2. Deterministic fake-clock idle exit when skipped, quarantine_pending, and
        #    retry-saturated entries fill all bounded structures simultaneously.
        _cleanup_drain_globals()
        now["t"] = 3000.0
        state["mode"] = "fail_always"
        for idx in range(1, 9):
            _write_envelope(f"event-00{idx}-sat.json", f"sat-{idx}")
        cfg_sat = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.01,
            drain_max_retry_age_seconds=0.03,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        sleeps2: list[float] = []

        async def _fake_sleep_2(delay):
            sleeps2.append(delay)
            now["t"] += delay
            await orig_sleep(0)

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ), \
             mock.patch.object(drain, "_quarantine", return_value=False), \
             mock.patch.object(drain.asyncio, "sleep", side_effect=_fake_sleep_2):
            asyncio.run(asyncio.wait_for(drain._run(cfg_sat), timeout=2.0))

        self.assertEqual(drain._retry_meta_saturated, {str(Path(spool_dir))})
        self.assertEqual(len(drain._skipped_paths), 2)
        self.assertEqual(len(drain._retry_skipped_paths), 2)
        self.assertEqual(len(drain._in_memory_retry_meta), 2)
        self.assertTrue(
            all(v.get("quarantine_pending") for v in drain._in_memory_retry_meta.values()),
            drain._in_memory_retry_meta,
        )
        self.assertTrue(sleeps2)
        self.assertTrue(all(d > 0.0 for d in sleeps2), sleeps2)
        self.assertGreaterEqual(sum(sleeps2), cfg_sat.drain_idle_seconds - 1e-9)

    def test_drain_bigquery_recovery_while_saturated_and_cap_zero_delivery_release(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_bq_rec_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_envelope(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        delivered: list[str] = []
        bq_down = {"active": True, "fail_prefix": "fail-"}

        def _insert(table_id, rows):
            del table_id
            outcomes = []
            for idx, row in enumerate(rows):
                marker = row["content"]["marker"]
                if bq_down["active"] and marker.startswith(bq_down["fail_prefix"]):
                    outcomes.append({
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    })
                else:
                    delivered.append(marker)
            return outcomes

        _install_fake_bigquery(self, insert_fn=_insert)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=60.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        now = {"t": 1000.0}

        for idx in range(1, 5):
            _write_envelope(f"event-00{idx}-fail.json", f"fail-{idx}")

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            now["t"] = 1001.0
            _write_envelope("event-005-fail.json", "fail-5")
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertEqual(drain._retry_meta_saturated, {str(Path(spool_dir))})
            self.assertEqual(len(drain._retry_skipped_paths), 2)

            healthy_h = _write_envelope("event-006-healthy.json", "healthy-H")

            # BigQuery recovers even while disk sidecar writes STILL fail with ENOSPC:
            # fail-1 and fail-2 deliver, freeing _in_memory_retry_meta and releasing saturation.
            bq_down["active"] = False
            now["t"] = 1005.0
            resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved1, 2)
            self.assertEqual(drain._in_memory_retry_meta, {})
            self.assertEqual(drain._retry_meta_saturated, set())
            self.assertEqual(drain._retry_skipped_paths, set())

            # Next pass drains parked fail-3..fail-5 AND healthy-H completely!
            now["t"] = 1006.0
            resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved2, 4)
            self.assertFalse(healthy_h.exists())
            self.assertEqual(
                sorted(delivered),
                ["fail-1", "fail-2", "fail-3", "fail-4", "fail-5", "healthy-H"],
            )

        # Edge case cap=0: delivery/quarantine resolving in a batch clears retry saturation.
        _cleanup_drain_globals()
        delivered.clear()
        bq_down["active"] = True
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 0), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            # idx=0 saturates (fail-1, fail-2), idx=2 succeeds (ok-after-sat) in the same batch!
            _write_envelope("event-101-fail.json", "fail-cap0-1")
            _write_envelope("event-102-fail.json", "fail-cap0-2")
            _write_envelope("event-103-ok.json", "ok-after-cap0-sat")
            resolved_cap0 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved_cap0, 1)
            self.assertEqual(delivered, ["ok-after-cap0-sat"])
            self.assertEqual(drain._retry_meta_saturated, set())
            self.assertEqual(drain._retry_skipped_paths, set())

    def test_drain_skip_cap_saturated_keeps_valid_deferred_sidecar_wait_in_run_and_dedupes_logs(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_skip_sat_wait_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        # 3 unquarantinable envelopes (mismatch/corrupt): exceed cap=2 -> _skip_cap_saturated,
        # plus check destination_mismatch diagnostic log deduplication across repeated polls.
        mismatch_names: set[str] = set()
        for idx in (1, 2, 3):
            name = f"event-00{idx}-mismatch.json"
            mismatch_names.add(name)
            p = Path(spool_dir) / name
            p.write_text(
                json.dumps({
                    "config": {"project_id": "other-proj", "dataset": "d", "table": "t"},
                    "row": {"event_type": "LLM_REQUEST"},
                }),
                encoding="utf-8",
            )
            os.chmod(p, 0o600)

        valid_deferred = Path(spool_dir) / "event-900-deferred.json"
        valid_deferred.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "deferred-900"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(valid_deferred, 0o600)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=0.5,
            drain_max_retry_age_seconds=10.0,
            drain_idle_seconds=0.1,
            drain_poll_seconds=0.04,
        )

        now = {"t": 1000.0}
        calls = {"n": 0}
        delivered: list[str] = []

        def _insert(table_id, rows):
            del table_id
            calls["n"] += 1
            if calls["n"] == 1:
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    }
                    for idx in range(len(rows))
                ]
            delivered.extend(row["content"]["marker"] for row in rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)
        orig_quarantine = drain._quarantine

        def _selective_quarantine(spool, name, reason, **kwargs):
            if name in mismatch_names:
                return False
            return orig_quarantine(spool, name, reason, **kwargs)

        orig_sleep = asyncio.sleep
        sleeps: list[float] = []

        async def _fake_sleep(delay):
            sleeps.append(delay)
            now["t"] += delay
            await orig_sleep(0)

        # First run _run end-to-end under fake clock: event-900 fails once at t=1000.0 with
        # backoff 0.5s (> drain_idle_seconds 0.1s) while _skip_cap_saturated is active.
        # _run must NOT exit after 0.1s idle; it must wait out 0.5s, deliver event-900, then exit!
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain, "_quarantine", side_effect=_selective_quarantine), \
             mock.patch.object(drain.asyncio, "sleep", side_effect=_fake_sleep):
            asyncio.run(asyncio.wait_for(drain._run(cfg), timeout=2.0))

        self.assertEqual(delivered, ["deferred-900"])
        self.assertFalse(valid_deferred.exists())
        self.assertEqual(drain._skip_cap_saturated, {str(Path(spool_dir))})
        self.assertIsNone(drain._next_deferred_wait_seconds(cfg))
        self.assertGreaterEqual(now["t"], 1000.5)

        log_text = log_path.read_text(encoding="utf-8")
        # Overflowed event-003-mismatch.json ran across >10 polls but must log mismatch only ONCE.
        self.assertEqual(
            log_text.count("DRAINER destination_mismatch file=event-003-mismatch.json"),
            1,
            log_text,
        )

        # Also test mutant killer: when _skip_cap_saturated is ALREADY set, quarantining a
        # newly arrived corrupt file succeeds and must count resolved == 1.
        corrupt_resolvable = Path(spool_dir) / "event-901-corrupt-ok.json"
        corrupt_resolvable.write_text("{bad", encoding="utf-8")
        os.chmod(corrupt_resolvable, 0o600)
        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain, "_quarantine", side_effect=_selective_quarantine):
            resolved_corrupt = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved_corrupt, 1)
        self.assertFalse(corrupt_resolvable.exists())

    def test_drain_quarantine_pending_preserves_expiry_and_dedupes_expired_logs(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_qpending_exp_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        for idx in (1, 2, 3):
            c = Path(spool_dir) / f"event-00{idx}-junk.json"
            c.write_text("{bad", encoding="utf-8")
            os.chmod(c, 0o600)

        held_env = Path(spool_dir) / "event-009-held.json"
        held_env.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "held-9"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(held_env, 0o600)

        exp_env = Path(spool_dir) / "event-010-expired.json"
        exp_env.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "expired-10"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(exp_env, 0o600)

        exp_over_env = Path(spool_dir) / "event-011-expired-overflow.json"
        exp_over_env.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "expired-11"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(exp_over_env, 0o600)
        exp_over_sidecar = Path(spool_dir) / "event-011-expired-overflow.json.retry"
        exp_over_sidecar.write_text(
            json.dumps({
                "first_failed_at": 1000.0,
                "attempts": 1,
                "next_retry_at": 1005.0,
                "last_error": "prior 503",
            }),
            encoding="utf-8",
        )
        os.chmod(exp_over_sidecar, 0o600)

        insert_calls = {"n": 0}

        def _insert(table_id, rows):
            del table_id, rows
            insert_calls["n"] += 1
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        log_path = Path(spool_dir) / "drain.log"
        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(log_path),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=10.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        now = {"t": 1020.0}

        import errno as _errno
        orig_subdir = drain.PrivateDir.subdir

        def _deny_dead_letter(self_dir, name):
            if name == "dead-letter":
                raise OSError(_errno.EACCES, "Permission denied")
            return orig_subdir(self_dir, name)

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain.PrivateDir, "subdir", _deny_dead_letter):
            with drain.ensure_private_dir(spool_dir) as spool_priv:
                drain._store_in_memory_retry_meta(
                    spool_priv,
                    "event-009-held.json",
                    {
                        "first_failed_at": 1000.0,
                        "attempts": 2,
                        "next_retry_at": 1005.0,
                        "quarantine_pending": True,
                        "quarantine_reason": "write-failed",
                    },
                )
                drain._store_in_memory_retry_meta(
                    spool_priv,
                    "event-010-expired.json",
                    {
                        "first_failed_at": 1000.0,
                        "attempts": 2,
                        "next_retry_at": 1005.0,
                        "last_error": "prior ENOSPC",
                    },
                )
            for _ in range(4):
                self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
                self.assertEqual(len(drain._in_memory_retry_meta), 2)
                self.assertIn(str(exp_env), drain._in_memory_retry_meta)
                self.assertIs(drain._in_memory_retry_meta[str(exp_env)].get("quarantine_pending"), True)
                self.assertEqual(drain._in_memory_retry_meta[str(exp_env)]["first_failed_at"], 1000.0)
                self.assertIsNone(drain._next_deferred_wait_seconds(cfg))
            self.assertEqual(insert_calls["n"], 0, "expired quarantine_pending/overflow must never re-call BigQuery")

        # Only after dead-letter recovers (real _quarantine), exp_env and exp_over_env move cleanly.
        orig_quarantine = drain._quarantine
        def _only_exp_recovers(spool, name, reason, **kwargs):
            if name not in (exp_env.name, exp_over_env.name):
                return False
            return orig_quarantine(spool, name, reason, **kwargs)

        with mock.patch.object(drain, "_IN_MEMORY_RETRY_META_CAP", 2), \
             mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(drain, "_quarantine", side_effect=_only_exp_recovers):
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 2)
            self.assertEqual(insert_calls["n"], 0)
            self.assertFalse(exp_env.exists())
            self.assertFalse(exp_over_env.exists())
            self.assertNotIn(str(exp_env), drain._in_memory_retry_meta)
            self.assertTrue((Path(spool_dir) / "dead-letter" / "event-010-expired.write-failed.json").is_file())
            self.assertTrue((Path(spool_dir) / "dead-letter" / "event-011-expired-overflow.write-failed.json").is_file())

        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(
            log_text.count("DRAINER retry_expired file=event-010-expired.json"),
            1,
            log_text,
        )
        self.assertEqual(
            log_text.count("DRAINER retry_expired file=event-011-expired-overflow.json"),
            1,
            log_text,
        )
        self.assertEqual(
            log_text.count("DRAINER retry_expiry_quarantine_failed file=event-011-expired-overflow.json"),
            1,
            log_text,
        )

    def test_drain_read_retry_meta_prefers_fresher_in_memory_over_stale_sidecar(self):
        import errno
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_stale_sidecar_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        env_path = Path(spool_dir) / "event-001-stale.json"
        env_path.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"marker": "stale-sidecar"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(env_path, 0o600)
        sidecar_path = Path(spool_dir) / "event-001-stale.json.retry"
        sidecar_path.write_text(
            json.dumps({
                "first_failed_at": 995.0,
                "attempts": 1,
                "next_retry_at": 1000.0,
                "last_error": "prior transient before ENOSPC",
            }),
            encoding="utf-8",
        )
        os.chmod(sidecar_path, 0o600)

        calls = {"n": 0, "fail": True}
        delivered: list[str] = []

        def _insert(table_id, rows):
            del table_id
            calls["n"] += 1
            if calls["fail"]:
                return [
                    {
                        "index": idx,
                        "errors": [{"reason": "backendError", "message": "transient 503"}],
                    }
                    for idx in range(len(rows))
                ]
            delivered.extend(row["content"]["marker"] for row in rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=str(Path(spool_dir) / "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=None,
            drain_max_retry_age_seconds=60.0,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
        )
        now = {"t": 1003.0}

        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]), \
             mock.patch.object(
                 drain,
                 "_write_retry_meta",
                 side_effect=OSError(errno.ENOSPC, "No space left on device"),
             ):
            # Pass 1 at t=1003.0: sidecar is due (1000.0 <= 1003.0), fails BigQuery,
            # _write_retry_meta fails ENOSPC leaving stale disk sidecar attempts=1 untouched,
            # while in-memory records attempts=2 and next_retry_at=1013.0 (backoff=10s).
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertEqual(calls["n"], 1)

            with drain.ensure_private_dir(spool_dir) as spool_priv:
                effective = drain._read_retry_meta(spool_priv, "event-001-stale.json")
            self.assertIsNotNone(effective)
            self.assertEqual(effective["attempts"], 2)
            self.assertEqual(effective["next_retry_at"], 1013.0)

            # Advance time inside the backoff window (t=1008.0 < 1013.0): stale disk sidecar
            # still says next_retry_at=1000.0, so _drain_once MUST defer and NOT re-send!
            now["t"] = 1008.0
            calls["fail"] = False
            self.assertAlmostEqual(drain._next_deferred_wait_seconds(cfg), 5.0, places=3)
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 0)
            self.assertEqual(calls["n"], 1, "stale .retry sidecar must never bypass fresher in-memory backoff")

            # Advance past next_retry_at=1013.0: delivers cleanly and removes envelope + stale sidecar.
            now["t"] = 1013.0
            self.assertEqual(asyncio.run(drain._drain_once(cfg, use_storage_api=False)), 1)
            self.assertEqual(calls["n"], 2)
            self.assertEqual(delivered, ["stale-sidecar"])
            self.assertFalse(env_path.exists())
            self.assertFalse(sidecar_path.exists())

    def test_drain_read_retry_meta_strips_quarantine_keys_from_disk_sidecar(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_tampered_sidecar_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        def _write_env(name: str, marker: str) -> Path:
            path = Path(spool_dir) / name
            path.write_text(
                json.dumps({
                    "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"marker": marker},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            return path

        env1 = _write_env("event-001-tampered.json", "tampered-healthy")
        sidecar1 = Path(spool_dir) / "event-001-tampered.json.retry"
        sidecar1.write_text(
            json.dumps({
                "first_failed_at": 995.0,
                "attempts": 2,
                "next_retry_at": 999.0,
                "last_error": "prior transient",
                "quarantine_pending": True,
                "quarantine_reason": "../../evil",
                "quarantine_subdir": "../escape",
            }),
            encoding="utf-8",
        )
        os.chmod(sidecar1, 0o600)

        env2 = _write_env("event-002-tampered-only-q.json", "tampered-only-q")
        sidecar2 = Path(spool_dir) / "event-002-tampered-only-q.json.retry"
        sidecar2.write_text(
            json.dumps({
                "quarantine_pending": True,
                "quarantine_reason": "../../evil2",
                "quarantine_subdir": "../escape2",
                "quarantine_extra": "ignored",
            }),
            encoding="utf-8",
        )
        os.chmod(sidecar2, 0o600)

        now = {"t": 1000.0}
        with mock.patch.object(drain.time, "time", side_effect=lambda: now["t"]):
            with drain.ensure_private_dir(spool_dir) as spool_priv:
                # Even when disk sidecar has higher attempts than an older in-memory entry,
                # quarantine_* keys loaded from disk must be stripped.
                drain._in_memory_retry_meta[str(env1)] = {
                    "first_failed_at": 995.0,
                    "attempts": 1,
                    "next_retry_at": 998.0,
                    "last_error": "older",
                }
                meta_fresher_disk = drain._read_retry_meta(spool_priv, env1.name)
                self.assertIsNotNone(meta_fresher_disk)
                self.assertEqual(meta_fresher_disk["attempts"], 2)
                self.assertEqual(meta_fresher_disk["last_error"], "prior transient")
                self.assertNotIn("quarantine_pending", meta_fresher_disk)
                self.assertNotIn("quarantine_reason", meta_fresher_disk)
                self.assertNotIn("quarantine_subdir", meta_fresher_disk)

                drain._in_memory_retry_meta.clear()
                meta1 = drain._read_retry_meta(spool_priv, env1.name)
                self.assertIsNotNone(meta1)
                self.assertEqual(meta1["attempts"], 2)
                self.assertNotIn("quarantine_pending", meta1)
                self.assertNotIn("quarantine_reason", meta1)
                self.assertNotIn("quarantine_subdir", meta1)

                meta2 = drain._read_retry_meta(spool_priv, env2.name)
                self.assertEqual(meta2, {})

            delivered: list[str] = []

            def _insert(table_id, rows):
                del table_id
                delivered.extend(row["content"]["marker"] for row in rows)
                return []

            _install_fake_bigquery(self, insert_fn=_insert)
            cfg = BQAAConfig(
                project_id="p",
                dataset="d",
                table="agent_events",
                auto_create_table=False,
                spool_dir=spool_dir,
                state_dir=spool_dir,
                log_file=str(Path(spool_dir) / "drain.log"),
                dry_run=False,
                drain_retry_backoff_seconds=0.5,
                drain_max_retry_age_seconds=60.0,
            )
            self.assertEqual(drain._next_deferred_wait_seconds(cfg), 0.0)
            resolved = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved, 2)
            self.assertEqual(sorted(delivered), ["tampered-healthy", "tampered-only-q"])
            self.assertFalse(env1.exists())
            self.assertFalse(sidecar1.exists())
            self.assertFalse(env2.exists())
            self.assertFalse(sidecar2.exists())
            self.assertFalse((Path(spool_dir) / "dead-letter").exists())

    def test_drain_attempt_quarantine_unlinks_retry_once(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain

        spool_dir = tempfile.mkdtemp(prefix="bqaa_q_unlink_once_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        def _cleanup_drain_globals():
            drain._in_memory_retry_meta.clear()
            drain._skipped_paths.clear()
            drain._retry_skipped_paths.clear()
            drain._retry_meta_saturated.clear()
            drain._skip_cap_saturated.clear()
            drain._reported_skips.clear()
            drain._diagnostic_logs.clear()
            drain._reported_saturation_logs.clear()

        self.addCleanup(_cleanup_drain_globals)
        _cleanup_drain_globals()

        env_path = Path(spool_dir) / "event-001-corrupt.json"
        env_path.write_text("{bad", encoding="utf-8")
        os.chmod(env_path, 0o600)
        retry_path = Path(spool_dir) / "event-001-corrupt.json.retry"
        retry_path.write_text('{"attempts": 1}', encoding="utf-8")
        os.chmod(retry_path, 0o600)

        with drain.ensure_private_dir(spool_dir) as spool_priv:
            with mock.patch.object(
                drain, "_unlink_retry", wraps=drain._unlink_retry
            ) as unlink_spy:
                ok = drain._attempt_quarantine(
                    spool_priv, env_path.name, reason="corrupt-json"
                )
        self.assertTrue(ok)
        self.assertEqual(unlink_spy.call_count, 1)
        self.assertFalse(env_path.exists())
        self.assertFalse(retry_path.exists())
        self.assertTrue(
            (Path(spool_dir) / "dead-letter" / "event-001-corrupt.corrupt-json.json").is_file()
        )

    def test_legacy_010_mid_turn_state_and_spool_migration_single_delivery(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig
        from bigquery_agent_analytics_tracing.config import _user_tag

        tmp_root = tempfile.mkdtemp(prefix="bqaa_r3_mig_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        legacy_dir = Path(tmp_root) / "bqaa-agent-tracing"
        legacy_spool = legacy_dir / "spool"
        legacy_spool.mkdir(parents=True, mode=0o755)
        os.chmod(legacy_dir, 0o755)
        os.chmod(legacy_spool, 0o755)

        sid = "mig-s1"
        legacy_state_file = legacy_dir / f"state_{sid}.json"
        legacy_state_file.write_text(
            json.dumps({
                "session_id": sid,
                "project_name": "proj",
                "agent": "claude-code",
                "user_id": "tester",
                "trace_count": 1,
                "current_trace_id": "trace-from-010",
                "current_span_id": "span-from-010",
                "current_invocation_id": "inv-from-010",
                "current_start_ms": 1700000000000,
                "transcript_line": 0,
            }),
            encoding="utf-8",
        )
        os.chmod(legacy_state_file, 0o644)

        legacy_tool_file = legacy_dir / f"tool_{sid}_tu1.json"
        legacy_tool_file.write_text(
            json.dumps({"start_ms": 1700000000100, "span_id": "tool-span-1", "tool_name": "Bash"}),
            encoding="utf-8",
        )
        os.chmod(legacy_tool_file, 0o644)

        legacy_env_file = legacy_spool / "event-001.json"
        legacy_env_file.write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "session_id": sid,
                    "trace_id": "trace-from-010",
                    "content": {"prompt": "from 0.1.0"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(legacy_env_file, 0o644)

        # Also plant a distinct event-001.json in the new spool to verify no overwrite/loss.
        new_base = Path(tmp_root) / f"bqaa-agent-tracing-{_user_tag()}"
        new_spool = new_base / "spool"
        new_spool.mkdir(parents=True, mode=0o700)
        os.chmod(new_base, 0o700)
        os.chmod(new_spool, 0o700)
        (new_spool / "event-001.json").write_text(
            json.dumps({
                "config": {"project_id": "p", "dataset": "d", "table": "agent_events"},
                "row": {
                    "timestamp": "2026-01-01T00:00:01Z",
                    "event_type": "STATE_DELTA",
                    "session_id": sid,
                    "trace_id": "new-trace-keep",
                    "content": {"marker": "already-in-011"},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )

        # Mid-turn Stop runs under 0.1.1 without BQAA_STATE_DIR / BQAA_SPOOL_DIR set.
        hook_env = {
            "TMPDIR": tmp_root,
            "BQAA_PROJECT_ID": "p",
            "BQAA_DATASET": "d",
            "BQAA_DRY_RUN": "false",
            "BQAA_AUTO_CREATE_TABLE": "false",
            "BQAA_TRACE_ENABLED": "true",
            "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": tmp_root,
            "PATH": os.environ.get("PATH", ""),
        }
        stop_res = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "stop.sh")],
            input=json.dumps({
                "session_id": sid,
                "transcript_path": "",
                "last_assistant_message": "answer after upgrade",
            }),
            capture_output=True, text=True, env=hook_env, timeout=30,
        )
        self.assertEqual(stop_res.returncode, 0, f"stderr={stop_res.stderr}")
        self.assertFalse(legacy_state_file.exists())
        self.assertFalse(legacy_tool_file.exists())
        self.assertTrue((new_base / f"state_{sid}.json").is_file())
        self.assertTrue((new_base / f"tool_{sid}_tu1.json").is_file())

        captured = []
        _install_fake_bigquery(
            self,
            insert_fn=lambda table_id, rows: (captured.extend(rows) or []),
        )
        env_backup = os.environ.copy()
        try:
            for key in ("BQAA_STATE_DIR", "BQAA_SPOOL_DIR", "BQAA_LEGACY_STATE_DIR", "BQAA_LOG_FILE"):
                os.environ.pop(key, None)
            os.environ.update(hook_env)
            cfg = BQAAConfig.from_env()
            cfg.drain_retry_backoff_seconds = 0.0
            resolved1 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved1, 3)
            resolved2 = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
            self.assertEqual(resolved2, 0)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

        self.assertFalse(legacy_env_file.exists())
        self.assertEqual(len(captured), 3)
        traces = [r.get("trace_id") for r in captured]
        self.assertEqual(traces.count("trace-from-010"), 2)
        self.assertIn("new-trace-keep", traces)
        events = [r.get("event_type") for r in captured]
        self.assertIn("LLM_REQUEST", events)
        self.assertIn("LLM_RESPONSE", events)
        self.assertIn("STATE_DELTA", events)

    def test_legacy_010_rejects_unsafe_entries_and_reports_in_setup_check(self):
        from bigquery_agent_analytics_tracing import setup_check

        tmp_root = tempfile.mkdtemp(prefix="bqaa_r3_unsafe_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        legacy_dir = Path(tmp_root) / "legacy"
        legacy_spool = legacy_dir / "spool"
        legacy_spool.mkdir(parents=True, mode=0o755)
        os.chmod(legacy_dir, 0o755)
        os.chmod(legacy_spool, 0o755)

        new_state = Path(tmp_root) / "new_state"
        new_spool = Path(tmp_root) / "new_spool"
        new_state.mkdir(mode=0o700)
        new_spool.mkdir(mode=0o700)

        # Safe legacy envelope should migrate.
        safe_env = legacy_spool / "event-safe.json"
        safe_env.write_text(
            json.dumps({"config": {"project_id": "p", "dataset": "d"}, "row": {"event_type": "LLM_REQUEST"}}),
            encoding="utf-8",
        )
        os.chmod(safe_env, 0o600)

        # Unsafe 1: symlink
        outside_secret = Path(tmp_root) / "secret.json"
        outside_secret.write_text(json.dumps({"config": {}, "row": {"secret": "leaked"}}), encoding="utf-8")
        os.symlink(str(outside_secret), str(legacy_spool / "event-sym.json"))

        # Unsafe 2: hardlink (st_nlink == 2)
        hard_target = Path(tmp_root) / "hard_target.json"
        hard_target.write_text(json.dumps({"config": {}, "row": {"hard": True}}), encoding="utf-8")
        os.chmod(hard_target, 0o600)
        os.link(str(hard_target), str(legacy_spool / "event-hard.json"))

        # Unsafe 3: group/world-writable file (0o666)
        writable_env = legacy_spool / "event-writable.json"
        writable_env.write_text(json.dumps({"config": {}, "row": {"bad": True}}), encoding="utf-8")
        os.chmod(writable_env, 0o666)

        # Unsafe 4 & 5 in legacy state dir: symlink + 0o666 state file
        os.symlink(str(outside_secret), str(legacy_dir / "state_sym.json"))
        writable_state = legacy_dir / "state_writable.json"
        writable_state.write_text(json.dumps({"session_id": "bad"}), encoding="utf-8")
        os.chmod(writable_state, 0o666)

        env_backup = os.environ.copy()
        try:
            os.environ.update({
                "BQAA_PROJECT_ID": "p",
                "BQAA_DATASET": "d",
                "BQAA_STATE_DIR": str(new_state),
                "BQAA_SPOOL_DIR": str(new_spool),
                "BQAA_LEGACY_STATE_DIR": str(legacy_dir),
            })
            buf_json = io.StringIO()
            with contextlib.redirect_stdout(buf_json):
                setup_check.main(["--json"])
            report = json.loads(buf_json.getvalue())
            self.assertEqual(report["legacy_migrated"], 1)
            self.assertEqual(report["spool_pending"], 1)
            self.assertEqual(report["legacy_unsafe_count"], 5)
            from bigquery_agent_analytics_tracing.config import destination_spool_dir
            self.assertTrue((Path(destination_spool_dir(new_spool, "p", "d")) / "event-safe.json").is_file())
            self.assertFalse(safe_env.exists())
            self.assertFalse((new_spool / "event-sym.json").exists())
            self.assertFalse((new_spool / "event-hard.json").exists())
            self.assertFalse((new_spool / "event-writable.json").exists())

            buf_text = io.StringIO()
            with contextlib.redirect_stdout(buf_text):
                setup_check.main([])
            text_out = buf_text.getvalue()
            self.assertIn("Legacy v0.1.0 directory", text_out)
            self.assertIn("rejected unsafe entries: 5", text_out)
            self.assertIn("refused unsafe legacy entry", text_out)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

        # Unsafe 0o777 legacy directory itself must be refused and flagged on SessionStart.
        bad_legacy_dir = Path(tmp_root) / "bad_legacy_dir"
        bad_legacy_dir.mkdir(mode=0o777)
        os.chmod(bad_legacy_dir, 0o777)
        ss_res = subprocess.run(
            ["bash", str(DIST_DIR / "hooks" / "session_start.sh")],
            input=json.dumps({"session_id": "ss-bad-legacy", "cwd": tmp_root}),
            capture_output=True, text=True,
            env={
                "BQAA_PROJECT_ID": "p",
                "BQAA_DATASET": "d",
                "BQAA_STATE_DIR": str(new_state),
                "BQAA_SPOOL_DIR": str(Path(tmp_root) / "clean_spool"),
                "BQAA_LEGACY_STATE_DIR": str(bad_legacy_dir),
                "BQAA_TRACE_ENABLED": "true",
                "PYTHONDONTWRITEBYTECODE": "1",
                "HOME": tmp_root,
                "PATH": os.environ.get("PATH", ""),
            },
            timeout=30,
        )
        self.assertEqual(ss_res.returncode, 0)
        ss_msg = json.loads(ss_res.stdout)
        self.assertIn("legacy v0.1.0 warning", ss_msg["systemMessage"])

    def test_transcript_streaming_dedup_memory_bound_and_non_contiguous_ids(self):
        from bigquery_agent_analytics_tracing.claude_code import _read_transcript_delta

        tmp_root = tempfile.mkdtemp(prefix="bqaa_r3_mem_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        big_transcript = Path(tmp_root) / "big.jsonl"
        with big_transcript.open("w", encoding="utf-8") as fh:
            for i in range(10_000):
                fh.write(
                    json.dumps({
                        "type": "assistant",
                        "message": {
                            "id": f"m-{i:05d}",
                            "model": "claude-test",
                            "usage": {"input_tokens": 2, "output_tokens": 3},
                            "content": [{"type": "text", "text": "ok"}],
                        },
                    }) + "\n"
                )

        tracemalloc.start()
        try:
            _, model, usage, _, cursor, next_seen = _read_transcript_delta(
                str(big_transcript), 0, 256 * 1024, None
            )
            _, peak_bytes = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        self.assertEqual(model, "claude-test")
        self.assertEqual(cursor, 10_000)
        self.assertEqual(len(next_seen), 256)
        self.assertEqual(usage["prompt_tokens"], 20_000)
        self.assertEqual(usage["completion_tokens"], 30_000)
        self.assertEqual(usage["total_tokens"], 50_000)
        self.assertLess(
            peak_bytes,
            1_048_576,
            f"peak allocation {peak_bytes} exceeded 1 MiB",
        )

        # Non-contiguous message.id interleaved with >256 anonymous lines
        # must preserve message.id in working_seen and deduplicate accurately.
        anon_mix = Path(tmp_root) / "anon_mix.jsonl"
        with anon_mix.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_A",
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                    "content": [{"type": "text", "text": "part1"}],
                },
            }) + "\n")
            for _ in range(300):
                fh.write(json.dumps({
                    "type": "assistant",
                    "message": {
                        "usage": {"input_tokens": 1, "output_tokens": 1},
                        "content": [],
                    },
                }) + "\n")
            fh.write(json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_A",
                    "usage": {"input_tokens": 10, "output_tokens": 15},
                    "content": [{"type": "text", "text": "part2"}],
                },
            }) + "\n")

        _, _, mix_usage, _, mix_cursor, mix_seen = _read_transcript_delta(
            str(anon_mix), 0, 256 * 1024, None
        )
        self.assertEqual(mix_cursor, 302)
        self.assertEqual(mix_usage["prompt_tokens"], 310)
        self.assertEqual(mix_usage["completion_tokens"], 315)
        self.assertEqual(list(mix_seen.keys()), ["msg_A"])

    def test_legacy_010_dedup_cross_user_and_dead_letter(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import _utils
        from bigquery_agent_analytics_tracing import setup_check

        tmp_root = tempfile.mkdtemp(prefix="bqaa_r3_xuser_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        legacy_dir = Path(tmp_root) / "legacy"
        legacy_spool = legacy_dir / "spool"
        legacy_dl = legacy_spool / "dead-letter"
        legacy_dl.mkdir(parents=True, mode=0o755)
        os.chmod(legacy_dir, 0o755)
        os.chmod(legacy_spool, 0o755)
        os.chmod(legacy_dl, 0o755)

        new_state = Path(tmp_root) / "new_state"
        new_spool = Path(tmp_root) / "new_spool"
        new_state.mkdir(mode=0o700)
        new_spool.mkdir(mode=0o700)

        # Duplicate crash recovery: identical payload in both dirs unlinks legacy without duplicating.
        dup_bytes = json.dumps({
            "config": {"project_id": "p", "dataset": "d"},
            "row": {"event_type": "LLM_REQUEST", "content": {"k": "dup"}},
        }).encode("utf-8")
        (legacy_spool / "event-dup.json").write_bytes(dup_bytes)
        (new_spool / "event-dup.json").write_bytes(dup_bytes)
        os.chmod(legacy_spool / "event-dup.json", 0o600)
        os.chmod(new_spool / "event-dup.json", 0o600)

        # Also leave a legacy dead-letter file to confirm reporting.
        (legacy_dl / "event-old.write-failed.json").write_text("{}", encoding="utf-8")

        mig = _utils.migrate_legacy_spool(new_spool, legacy_spool_dir=legacy_spool)
        self.assertEqual(mig["migrated"], 0)
        self.assertFalse((legacy_spool / "event-dup.json").exists())
        self.assertEqual(len(list(new_spool.glob("event-*.json"))), 1)

        # Dead-letter legacy file is reported by setup_check without disturbing new spool.
        env_backup = os.environ.copy()
        try:
            os.environ.update({
                "BQAA_PROJECT_ID": "p",
                "BQAA_DATASET": "d",
                "BQAA_STATE_DIR": str(new_state),
                "BQAA_SPOOL_DIR": str(new_spool),
                "BQAA_LEGACY_STATE_DIR": str(legacy_dir),
            })
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                setup_check.main(["--json"])
            report = json.loads(buf.getvalue())
            self.assertEqual(report["legacy_spool_dead_letter"], 1)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

        # Cross-user check: another uid owning legacy dir/files is refused cleanly.
        (legacy_spool / "event-xuser.json").write_bytes(dup_bytes)
        os.chmod(legacy_spool / "event-xuser.json", 0o600)
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1000):
            xuser_mig = _utils.migrate_legacy_spool(
                new_spool, legacy_spool_dir=legacy_spool
            )
            self.assertEqual(xuser_mig["migrated"], 0)
            self.assertIsNotNone(xuser_mig["error"])
            self.assertTrue((legacy_spool / "event-xuser.json").is_file())

    def test_drain_malformed_and_non_finite_retry_sidecars(self):
        import math
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        spool_dir = tempfile.mkdtemp(prefix="bqaa_r3b_retry_")
        self.addCleanup(shutil.rmtree, spool_dir, ignore_errors=True)
        os.chmod(spool_dir, 0o700)

        malformed_sidecars = [
            ("event-001.json", '{"first_failed_at": 0, "next_retry_at": 0, "attempts": null}'),
            ("event-002.json", '{"attempts": 1, "first_failed_at": NaN, "next_retry_at": 99999999999.0}'),
            ("event-003.json", '{"first_failed_at": 0, "next_retry_at": Infinity, "attempts": "bad"}'),
            ("event-004.json", '{"first_failed_at": 0, "next_retry_at": 99999999999.0, "attempts": -3}'),
        ]
        for idx, (env_name, retry_raw) in enumerate(malformed_sidecars):
            env_path = Path(spool_dir) / env_name
            env_path.write_text(
                json.dumps({
                    "config": {
                        "project_id": "p",
                        "dataset": "d",
                        "table": "agent_events",
                    },
                    "row": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "event_type": "LLM_REQUEST",
                        "content": {"idx": idx},
                        "attributes": {},
                        "latency_ms": {},
                    },
                }),
                encoding="utf-8",
            )
            os.chmod(env_path, 0o600)
            retry_path = Path(spool_dir) / f"{env_name}.retry"
            retry_path.write_text(retry_raw, encoding="utf-8")
            os.chmod(retry_path, 0o600)

        calls = {"n": 0}
        delivered = []

        def _insert(table_id, rows):
            del table_id
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("503 Service Unavailable")
            delivered.extend(rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert)

        cfg = BQAAConfig(
            project_id="p",
            dataset="d",
            table="agent_events",
            auto_create_table=False,
            spool_dir=spool_dir,
            state_dir=spool_dir,
            log_file=os.path.join(spool_dir, "drain.log"),
            dry_run=False,
            drain_retry_backoff_seconds=0.12,
            drain_idle_seconds=0.06,
            drain_poll_seconds=0.02,
            drain_max_retry_age_seconds=1.0,
        )

        # Before delivery: corrupt/non-finite sidecars must return 0.0 (no TypeError/ValueError/inf).
        wait_before = drain._next_deferred_wait_seconds(cfg)
        self.assertEqual(wait_before, 0.0)

        # Call 1 hits 503: _drain_once must not crash and must reset malformed .retry fields cleanly.
        resolved = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved, 0)
        self.assertEqual(calls["n"], 1)
        for env_name, _ in malformed_sidecars:
            meta = json.loads((Path(spool_dir) / f"{env_name}.retry").read_text(encoding="utf-8"))
            self.assertEqual(meta["attempts"], 1)
            self.assertNotIsInstance(meta["attempts"], bool)
            self.assertTrue(math.isfinite(meta["first_failed_at"]))
            self.assertTrue(math.isfinite(meta["next_retry_at"]))
            self.assertGreaterEqual(meta["next_retry_at"], meta["first_failed_at"])

        # Re-inject a bool/NaN/Infinity sidecar and confirm _run completes promptly without busy-spinning.
        (Path(spool_dir) / "event-001.json.retry").write_text(
            '{"attempts": true, "first_failed_at": NaN, "next_retry_at": Infinity}',
            encoding="utf-8",
        )
        self.assertEqual(drain._next_deferred_wait_seconds(cfg), 0.0)
        asyncio.run(asyncio.wait_for(drain._run(cfg), timeout=2.0))

        self.assertEqual(len(delivered), 4)
        self.assertEqual({r["content"]["idx"] for r in delivered}, {0, 1, 2, 3})
        for env_name, _ in malformed_sidecars:
            self.assertFalse((Path(spool_dir) / env_name).exists())
            self.assertFalse((Path(spool_dir) / f"{env_name}.retry").exists())
        self.assertFalse((Path(spool_dir) / "dead-letter").exists())

        # Expired sidecar with string "attempts": "1" must parse cleanly and quarantine
        # instead of raising TypeError on integer comparison.
        str_env = Path(spool_dir) / "event-str-attempts.json"
        str_env.write_text(
            json.dumps({
                "config": {
                    "project_id": "p",
                    "dataset": "d",
                    "table": "agent_events",
                },
                "row": {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "event_type": "LLM_REQUEST",
                    "content": {"idx": 99},
                    "attributes": {},
                    "latency_ms": {},
                },
            }),
            encoding="utf-8",
        )
        os.chmod(str_env, 0o600)
        str_retry = Path(spool_dir) / "event-str-attempts.json.retry"
        str_retry.write_text(
            '{"attempts": "1", "first_failed_at": 0.0}',
            encoding="utf-8",
        )
        os.chmod(str_retry, 0o600)

        calls_before = calls["n"]
        self.assertEqual(drain._next_deferred_wait_seconds(cfg), 0.0)
        resolved_expired = asyncio.run(drain._drain_once(cfg, use_storage_api=False))
        self.assertEqual(resolved_expired, 1)
        self.assertEqual(calls["n"], calls_before)
        self.assertFalse(str_env.exists())
        self.assertFalse(str_retry.exists())
        self.assertTrue(
            (Path(spool_dir) / "dead-letter" / "event-str-attempts.write-failed.json").is_file()
        )


class TestBuildDist39(unittest.TestCase):
    """Issue #39: build_dist.sh --check verifies source/dist/tag/catalog integrity."""

    def test_repo_build_dist_check_passes(self):
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        res = subprocess.run(
            ["bash", str(SRC_DIR / "build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res.returncode, 0, f"{res.stdout}\n{res.stderr}")

    def test_tag_and_sync_invariants_in_temp_repo(self):
        tmp_repo = Path(tempfile.mkdtemp(prefix="bqaa_39_git_"))
        self.addCleanup(shutil.rmtree, tmp_repo, ignore_errors=True)
        env = dict(
            os.environ,
            PYTHONDONTWRITEBYTECODE="1",
            GIT_AUTHOR_NAME="Test",
            GIT_AUTHOR_EMAIL="test@example.com",
            GIT_COMMITTER_NAME="Test",
            GIT_COMMITTER_EMAIL="test@example.com",
        )

        # Clone worktree into temp git repo with clean HEAD + uncommitted edits included.
        for item in (".claude-plugin", "plugins/claude_code", "plugins/claude_code_dist"):
            dst = tmp_repo / item
            dst.parent.mkdir(parents=True, exist_ok=True)
            ignore_fn = _ignore_claude_code_src if item == "plugins/claude_code" else shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
            shutil.copytree(
                REPO_ROOT / item,
                dst,
                ignore=ignore_fn,
            )

        def _git(*args, cwd=tmp_repo):
            return subprocess.run(
                ["git", "-C", str(cwd), *args],
                capture_output=True, text=True, env=env, check=True, timeout=30,
            )

        _git("init", "-q")
        _git("add", "-A")
        _git("commit", "-q", "-m", "initial")

        dist_ver = json.loads(
            (tmp_repo / "plugins/claude_code_dist/bigquery-agent-analytics-tracing/.claude-plugin/plugin.json").read_text()
        )["version"]
        _git("tag", f"tracing-v{dist_ver}")

        # 1. Unrelated commit after release tag stays valid.
        (tmp_repo / "UNRELATED.txt").write_text("ok\n", encoding="utf-8")
        _git("add", "UNRELATED.txt")
        _git("commit", "-q", "-m", "unrelated change")
        ok_res = subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(ok_res.returncode, 0, f"{ok_res.stdout}\n{ok_res.stderr}")

        # 2. Editing source without syncing dist must fail.
        readme = tmp_repo / "plugins/claude_code/README.md"
        readme.write_text(readme.read_text() + "\n# drift\n", encoding="utf-8")
        drift_res = subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(drift_res.returncode, 0)

        # 3. Syncing dist without bumping version after release tag must fail.
        subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"),
             str(tmp_repo / "plugins/claude_code_dist/bigquery-agent-analytics-tracing")],
            check=True, capture_output=True, text=True, env=env, timeout=30,
        )
        same_tag_res = subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(same_tag_res.returncode, 0)
        self.assertIn("changed since release tag", same_tag_res.stdout)

        # 4. HEAD release tag or GITHUB_REF_NAME mismatch must fail even when version > old tag.
        _git("commit", "-q", "-a", "-m", "synced edit")
        _git("tag", f"tracing-v{dist_ver}-rc1")
        rc_res = subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(rc_res.returncode, 0)
        self.assertIn("HEAD carries release tag", rc_res.stdout)

        env_ref = dict(env, GITHUB_REF_NAME="tracing-v9.9.9")
        ref_res = subprocess.run(
            ["bash", str(tmp_repo / "plugins/claude_code/build_dist.sh"), "--check"],
            capture_output=True, text=True, env=env_ref, timeout=30,
        )
        self.assertNotEqual(ref_res.returncode, 0)
        self.assertIn("GITHUB_REF_NAME=", ref_res.stdout)

    def test_two_repo_sdk_release_tag_parity(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_two_repo_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = dict(
            os.environ,
            PYTHONDONTWRITEBYTECODE="1",
            GIT_AUTHOR_NAME="Test",
            GIT_AUTHOR_EMAIL="test@example.com",
            GIT_COMMITTER_NAME="Test",
            GIT_COMMITTER_EMAIL="test@example.com",
        )
        env.pop("BQAA_SDK_REPO", None)

        sdk_repo = tmp / "BigQuery-Agent-Analytics-SDK"
        plugins_repo = tmp / "BigQuery-Agent-Analytics-Plugins"

        def _git(*args, cwd):
            return subprocess.run(
                ["git", "-C", str(cwd), *args],
                capture_output=True, text=True, env=env, check=True, timeout=30,
            )

        # Populate plugins repo without any local tracing-v* tags.
        for item in (".claude-plugin", "plugins/claude_code", "plugins/claude_code_dist"):
            dst = plugins_repo / item
            dst.parent.mkdir(parents=True, exist_ok=True)
            ignore_fn = _ignore_claude_code_src if item == "plugins/claude_code" else shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
            shutil.copytree(
                REPO_ROOT / item,
                dst,
                ignore=ignore_fn,
            )
        _git("init", "-q", cwd=plugins_repo)
        _git("add", "-A", cwd=plugins_repo)
        _git("commit", "-q", "-m", "plugins initial", cwd=plugins_repo)

        dist_dir = plugins_repo / "plugins/claude_code_dist/bigquery-agent-analytics-tracing"
        dist_ver = json.loads((dist_dir / ".claude-plugin/plugin.json").read_text())["version"]

        # Populate SDK repo with plugins/claude_code + producers/src layout.
        shutil.copytree(
            REPO_ROOT / "plugins/claude_code",
            sdk_repo / "plugins/claude_code",
            ignore=_ignore_claude_code_src,
        )
        shutil.copytree(
            VENDOR_DIR,
            sdk_repo / "producers/src/bigquery_agent_analytics_tracing",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        pyproject = sdk_repo / "producers/pyproject.toml"
        pyproject.write_text(
            '[project]\nname = "bigquery-agent-analytics-tracing"\nversion = "0.1.0"\n',
            encoding="utf-8",
        )
        _git("init", "-q", cwd=sdk_repo)
        _git("add", "-A", cwd=sdk_repo)
        _git("commit", "-q", "-m", "sdk 0.1.0", cwd=sdk_repo)
        _git("tag", "tracing-v0.1.0", cwd=sdk_repo)

        pyproject.write_text(
            f'[project]\nname = "bigquery-agent-analytics-tracing"\nversion = "{dist_ver}"\n',
            encoding="utf-8",
        )
        _git("add", "-A", cwd=sdk_repo)
        _git("commit", "-q", "-m", f"sdk {dist_ver}", cwd=sdk_repo)
        _git("tag", f"tracing-v{dist_ver}", cwd=sdk_repo)

        check_script = plugins_repo / "plugins/claude_code/build_dist.sh"

        # 1. Sibling SDK tag tracing-v<dist_ver> matches plugins checkout.
        ok_res = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(ok_res.returncode, 0, f"{ok_res.stdout}\n{ok_res.stderr}")
        self.assertIn(f"tracing-v{dist_ver} exists", ok_res.stdout)

        # 2a. Editing vendored dist code in plugins repo must fail against SDK tag.
        vendored_drain = dist_dir / "vendor/bigquery_agent_analytics_tracing/drain.py"
        vendored_drain.write_text(vendored_drain.read_text() + "\n# post-tag dist edit\n", encoding="utf-8")
        vendor_drift = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(vendor_drift.returncode, 0)
        self.assertIn("differs: vendor/bigquery_agent_analytics_tracing/drain.py", vendor_drift.stdout)
        self.assertIn(f"changed since release tag tracing-v{dist_ver}", vendor_drift.stdout)
        _git("checkout", "--", ".", cwd=plugins_repo)

        # 2b. Editing source AND rebuilding dist at the same version keeps local parity
        #     but still diverges from SDK tag tracing-v<dist_ver>, so --check must fail.
        common_sh = plugins_repo / "plugins/claude_code/hooks/common.sh"
        common_sh.write_text(common_sh.read_text() + "\n# post-tag hook edit\n", encoding="utf-8")
        subprocess.run(
            ["bash", str(check_script), str(dist_dir)],
            check=True, capture_output=True, text=True, env=env, timeout=30,
        )
        synced_drift = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(synced_drift.returncode, 0)
        self.assertIn("ok - committed dist matches what plugins/claude_code builds", synced_drift.stdout)
        self.assertIn("differs: hooks/common.sh", synced_drift.stdout)
        self.assertIn(f"changed since release tag tracing-v{dist_ver}", synced_drift.stdout)
        _git("checkout", "--", ".", cwd=plugins_repo)

        # 3. Auto-discovery via derived origin remote works without a sibling SDK checkout.
        isolated_dir = tmp / "isolated"
        isolated_plugins = isolated_dir / "BigQuery-Agent-Analytics-Plugins"
        for item in (".claude-plugin", "plugins/claude_code", "plugins/claude_code_dist"):
            dst = isolated_plugins / item
            dst.parent.mkdir(parents=True, exist_ok=True)
            ignore_fn = _ignore_claude_code_src if item == "plugins/claude_code" else shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
            shutil.copytree(
                REPO_ROOT / item,
                dst,
                ignore=ignore_fn,
            )
        _git("init", "-q", cwd=isolated_plugins)
        _git("remote", "add", "origin", str(plugins_repo), cwd=isolated_plugins)
        _git("add", "-A", cwd=isolated_plugins)
        _git("commit", "-q", "-m", "isolated plugins", cwd=isolated_plugins)
        isolated_script = isolated_plugins / "plugins/claude_code/build_dist.sh"
        origin_ok = subprocess.run(
            ["bash", str(isolated_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(origin_ok.returncode, 0, f"{origin_ok.stdout}\n{origin_ok.stderr}")
        self.assertIn(f"tracing-v{dist_ver} exists", origin_ok.stdout)

        # 4. Higher SDK release tag (e.g. tracing-v0.1.2) without matching tag fails 0.1.1.
        ahead_sdk = tmp / "ahead_sdk"
        shutil.copytree(sdk_repo, ahead_sdk)
        _git("tag", "-d", f"tracing-v{dist_ver}", cwd=ahead_sdk)
        _git("tag", "tracing-v0.1.2", cwd=ahead_sdk)
        ahead_res = subprocess.run(
            ["bash", str(check_script), "--check", "--sdk-repo", str(ahead_sdk)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(ahead_res.returncode, 0)
        self.assertIn("not newer than existing tag tracing-v0.1.2", ahead_res.stdout)

        env_ahead = dict(env, BQAA_SDK_REPO=str(ahead_sdk))
        ahead_env_res = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env_ahead, timeout=30,
        )
        self.assertNotEqual(ahead_env_res.returncode, 0)
        self.assertIn("not newer than existing tag tracing-v0.1.2", ahead_env_res.stdout)

        # 5. Invalid and shallow SDK repo overrides must fail explicitly.
        bad_env = dict(env, BQAA_SDK_REPO=str(plugins_repo / "plugins"))
        bad_res = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=bad_env, timeout=30,
        )
        self.assertNotEqual(bad_res.returncode, 0)
        self.assertIn("is not a git repository", bad_res.stdout)

        shallow_sdk = tmp / "shallow_sdk"
        subprocess.run(
            ["git", "clone", "-q", "--depth=1", f"file://{sdk_repo}", str(shallow_sdk)],
            check=True, capture_output=True, text=True, env=env, timeout=30,
        )
        shallow_res = subprocess.run(
            ["bash", str(check_script), "--check", f"--sdk-repo={shallow_sdk}"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(shallow_res.returncode, 0)
        self.assertIn("SDK repository is shallow", shallow_res.stdout)

        # 6. Local tracing-dist-v* snapshot immutability and monotonicity.
        _git("tag", f"tracing-dist-v{dist_ver}", cwd=plugins_repo)
        dist_tag_ok = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(dist_tag_ok.returncode, 0, f"{dist_tag_ok.stdout}\n{dist_tag_ok.stderr}")
        self.assertIn(f"tracing-dist-v{dist_ver} exists", dist_tag_ok.stdout)

        vendored_drain.write_text(vendored_drain.read_text() + "\n# dist-tag edit\n", encoding="utf-8")
        dist_tag_drift = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(dist_tag_drift.returncode, 0)
        self.assertIn(f"changed since release tag tracing-dist-v{dist_ver}", dist_tag_drift.stdout)
        _git("checkout", "--", ".", cwd=plugins_repo)

        _git("tag", "-d", f"tracing-dist-v{dist_ver}", cwd=plugins_repo)
        _git("tag", "tracing-dist-v9.9.9", cwd=plugins_repo)
        dist_mono_fail = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(dist_mono_fail.returncode, 0)
        self.assertIn("not newer than existing dist tag tracing-dist-v9.9.9", dist_mono_fail.stdout)

    def _clean_git_env(self) -> dict[str, str]:
        env = dict(
            os.environ,
            PYTHONDONTWRITEBYTECODE="1",
            GIT_AUTHOR_NAME="Test",
            GIT_AUTHOR_EMAIL="test@example.com",
            GIT_COMMITTER_NAME="Test",
            GIT_COMMITTER_EMAIL="test@example.com",
        )
        for var in ("BQAA_SDK_REPO", "BQAA_OFFLINE", "CI", "GITHUB_REF_NAME"):
            env.pop(var, None)
        return env

    def _init_plugins_repo(self, target_dir: Path, env: dict[str, str]) -> Path:
        for item in (".claude-plugin", "plugins/claude_code", "plugins/claude_code_dist"):
            dst = target_dir / item
            dst.parent.mkdir(parents=True, exist_ok=True)
            ignore_fn = _ignore_claude_code_src if item == "plugins/claude_code" else shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
            shutil.copytree(
                REPO_ROOT / item,
                dst,
                ignore=ignore_fn,
            )
        for cmd in (
            ("init", "-q"),
            ("add", "-A"),
            ("commit", "-q", "-m", "plugins initial"),
        ):
            subprocess.run(
                ["git", "-C", str(target_dir), *cmd],
                capture_output=True, text=True, env=env, check=True, timeout=30,
            )
        return target_dir / "plugins/claude_code/build_dist.sh"

    def test_sdk_tag_discovery_reachable_empty_repo_passes(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_39_empty_sdk_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        empty_sdk = tmp / "empty_sdk.git"
        subprocess.run(
            ["git", "init", "--bare", "-q", str(empty_sdk)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )

        # 1. Configured 'sdk' remote pointing to reachable empty file:// repo exits 0.
        plugins_a = tmp / "plugins_a"
        check_a = self._init_plugins_repo(plugins_a, env)
        subprocess.run(
            ["git", "-C", str(plugins_a), "remote", "add", "sdk", empty_sdk.as_uri()],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_a = subprocess.run(
            ["bash", str(check_a), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res_a.returncode, 0, f"{res_a.stdout}\n{res_a.stderr}")
        self.assertIn("ok - no tracing-v release tags exist yet", res_a.stdout)

        # 2. Origin-derived file:// SDK repo that exists and is empty exits 0.
        pair_dir = tmp / "pair"
        pair_dir.mkdir(parents=True, exist_ok=True)
        derived_empty_sdk = pair_dir / "BigQuery-Agent-Analytics-SDK"
        subprocess.run(
            ["git", "init", "-q", str(derived_empty_sdk)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        isolated = tmp / "isolated"
        plugins_b = isolated / "checkout"
        check_b = self._init_plugins_repo(plugins_b, env)
        origin_uri = (pair_dir / "BigQuery-Agent-Analytics-Plugins").as_uri()
        subprocess.run(
            ["git", "-C", str(plugins_b), "remote", "add", "origin", origin_uri],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_b = subprocess.run(
            ["bash", str(check_b), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res_b.returncode, 0, f"{res_b.stdout}\n{res_b.stderr}")
        self.assertIn("ok - no tracing-v release tags exist yet", res_b.stdout)

        # 3. Explicit --sdk-repo file:// pointing to reachable empty repo exits 0.
        res_c = subprocess.run(
            ["bash", str(check_a), "--check", "--sdk-repo", empty_sdk.as_uri()],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(res_c.returncode, 0, f"{res_c.stdout}\n{res_c.stderr}")
        self.assertIn("ok - no tracing-v release tags exist yet", res_c.stdout)

        # 4. Reachable file:// SDK repo WITH a higher tracing-v* tag still fetches and enforces it.
        tagged_sdk = tmp / "tagged_sdk"
        tagged_sdk.mkdir(parents=True, exist_ok=True)
        (tagged_sdk / "README.md").write_text("sdk\n", encoding="utf-8")
        for cmd in (
            ("init", "-q"),
            ("add", "-A"),
            ("commit", "-q", "-m", "sdk release"),
            ("tag", "tracing-v9.9.9"),
        ):
            subprocess.run(
                ["git", "-C", str(tagged_sdk), *cmd],
                capture_output=True, text=True, env=env, check=True, timeout=30,
            )
        res_d = subprocess.run(
            ["bash", str(check_a), "--check", "--sdk-repo", tagged_sdk.as_uri()],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_d.returncode, 0)
        self.assertIn("not newer than existing tag tracing-v9.9.9", res_d.stdout)

    def test_sdk_tag_discovery_unreachable_fails_closed(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_39_unreach_sdk_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        missing_uri = (tmp / "does-not-exist-sdk").as_uri()

        # 1. Unreachable file:// configured 'sdk' remote must fail closed.
        plugins_remote_uri = tmp / "plugins_remote_uri"
        check_remote_uri = self._init_plugins_repo(plugins_remote_uri, env)
        subprocess.run(
            ["git", "-C", str(plugins_remote_uri), "remote", "add", "sdk", missing_uri],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_remote_uri = subprocess.run(
            ["bash", str(check_remote_uri), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_remote_uri.returncode, 0)
        self.assertIn(
            "not ok - failed to fetch tracing-v release tags from SDK remote",
            res_remote_uri.stdout,
        )
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_remote_uri.stdout)

        # 2. Non-repo local path configured as 'sdk' remote must fail closed.
        non_repo = tmp / "not_a_git_repo"
        non_repo.mkdir(parents=True, exist_ok=True)
        plugins_remote_path = tmp / "plugins_remote_path"
        check_remote_path = self._init_plugins_repo(plugins_remote_path, env)
        subprocess.run(
            ["git", "-C", str(plugins_remote_path), "remote", "add", "sdk", str(non_repo)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_remote_path = subprocess.run(
            ["bash", str(check_remote_path), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_remote_path.returncode, 0)
        self.assertIn(
            "not ok - failed to fetch tracing-v release tags from SDK remote",
            res_remote_path.stdout,
        )
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_remote_path.stdout)

        # 3. Unreachable origin-derived file://, local path, and closed localhost HTTPS URLs fail closed.
        origin_cases = [
            ("file_uri", (tmp / "missing_pair" / "BigQuery-Agent-Analytics-Plugins").as_uri()),
            ("local_path", str(tmp / "missing_pair" / "BigQuery-Agent-Analytics-Plugins")),
            ("closed_https", "https://127.0.0.1:1/org/BigQuery-Agent-Analytics-Plugins.git"),
        ]
        for label, origin_val in origin_cases:
            with self.subTest(origin=label):
                plugins_orig = tmp / f"plugins_orig_{label}"
                check_orig = self._init_plugins_repo(plugins_orig, env)
                subprocess.run(
                    ["git", "-C", str(plugins_orig), "remote", "add", "origin", origin_val],
                    capture_output=True, text=True, env=env, check=True, timeout=30,
                )
                res_orig = subprocess.run(
                    ["bash", str(check_orig), "--check"],
                    capture_output=True, text=True, env=env, timeout=30,
                )
                self.assertNotEqual(res_orig.returncode, 0, f"{res_orig.stdout}\n{res_orig.stderr}")
                self.assertIn(
                    "not ok - failed to fetch tracing-v release tags from derived SDK repository",
                    res_orig.stdout,
                )
                self.assertNotIn("ok - no tracing-v release tags exist yet", res_orig.stdout)

        # 4. Unreachable explicit --sdk-repo file:// URL fails closed.
        res_explicit = subprocess.run(
            ["bash", str(check_remote_uri), "--check", "--sdk-repo", missing_uri],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_explicit.returncode, 0)
        self.assertIn(f"not ok - BQAA_SDK_REPO={missing_uri} is not a git repository", res_explicit.stdout)
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_explicit.stdout)

    def test_sdk_tag_discovery_shallow_repos_fail_closed(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_39_shallow_sdk_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        upstream_sdk = tmp / "upstream_sdk"
        upstream_sdk.mkdir(parents=True, exist_ok=True)
        for idx in (1, 2):
            (upstream_sdk / "file.txt").write_text(f"v{idx}\n", encoding="utf-8")
            if idx == 1:
                subprocess.run(
                    ["git", "-C", str(upstream_sdk), "init", "-q"],
                    capture_output=True, text=True, env=env, check=True, timeout=30,
                )
            for cmd in (("add", "-A"), ("commit", "-q", "-m", f"commit {idx}")):
                subprocess.run(
                    ["git", "-C", str(upstream_sdk), *cmd],
                    capture_output=True, text=True, env=env, check=True, timeout=30,
                )

        # 1. Shallow sibling BigQuery-Agent-Analytics-SDK fails closed.
        sib_root = tmp / "sibling_case"
        sib_root.mkdir(parents=True, exist_ok=True)
        shallow_sib = sib_root / "BigQuery-Agent-Analytics-SDK"
        subprocess.run(
            ["git", "clone", "-q", "--depth=1", upstream_sdk.as_uri(), str(shallow_sib)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        plugins_sib = sib_root / "BigQuery-Agent-Analytics-Plugins"
        check_sib = self._init_plugins_repo(plugins_sib, env)
        res_sib = subprocess.run(
            ["bash", str(check_sib), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_sib.returncode, 0)
        self.assertIn("is shallow", res_sib.stdout)
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_sib.stdout)

        # 2. Shallow configured 'sdk' remote (local path) fails closed.
        plugins_remote = tmp / "plugins_shallow_remote"
        check_remote = self._init_plugins_repo(plugins_remote, env)
        subprocess.run(
            ["git", "-C", str(plugins_remote), "remote", "add", "sdk", str(shallow_sib)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_remote = subprocess.run(
            ["bash", str(check_remote), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_remote.returncode, 0)
        self.assertIn("not ok - SDK repository at remote sdk is shallow", res_remote.stdout)
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_remote.stdout)

        # 3. Shallow origin-derived local SDK path fails closed.
        plugins_orig = tmp / "plugins_shallow_origin"
        check_orig = self._init_plugins_repo(plugins_orig, env)
        subprocess.run(
            [
                "git", "-C", str(plugins_orig), "remote", "add", "origin",
                str(sib_root / "BigQuery-Agent-Analytics-Plugins"),
            ],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_orig = subprocess.run(
            ["bash", str(check_orig), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(res_orig.returncode, 0)
        self.assertIn("is shallow", res_orig.stdout)
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_orig.stdout)

    def test_sdk_tag_discovery_offline_override(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_39_offline_sdk_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        plugins_repo = tmp / "plugins_offline"
        check_script = self._init_plugins_repo(plugins_repo, env)
        missing_uri = (tmp / "missing-sdk").as_uri()
        subprocess.run(
            ["git", "-C", str(plugins_repo), "remote", "add", "sdk", missing_uri],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )

        # 1. BQAA_OFFLINE=1 without CI=true skips remote SDK discovery cleanly.
        for ci_val in (None, ""):
            offline_env = dict(env, BQAA_OFFLINE="1")
            if ci_val is not None:
                offline_env["CI"] = ci_val
            res_offline = subprocess.run(
                ["bash", str(check_script), "--check"],
                capture_output=True, text=True, env=offline_env, timeout=30,
            )
            self.assertEqual(res_offline.returncode, 0, f"{res_offline.stdout}\n{res_offline.stderr}")
            self.assertIn("ok - skipping SDK tag discovery (BQAA_OFFLINE=1)", res_offline.stdout)
            self.assertNotIn("ok - no tracing-v release tags exist yet", res_offline.stdout)

        # 2. BQAA_OFFLINE=1 with CI=true must fail closed.
        ci_offline_env = dict(env, BQAA_OFFLINE="1", CI="true")
        res_ci_offline = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=ci_offline_env, timeout=30,
        )
        self.assertNotEqual(res_ci_offline.returncode, 0)
        self.assertIn("not ok - BQAA_OFFLINE=1 is not permitted when CI=true", res_ci_offline.stdout)
        self.assertNotIn("ok - no tracing-v release tags exist yet", res_ci_offline.stdout)

        # 3. Even with BQAA_OFFLINE=1 (non-CI), local tracing-v* tags are still enforced.
        subprocess.run(
            ["git", "-C", str(plugins_repo), "tag", "tracing-v9.9.9"],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )
        res_local_tag = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=dict(env, BQAA_OFFLINE="1"), timeout=30,
        )
        self.assertNotEqual(res_local_tag.returncode, 0)
        self.assertIn("not newer than existing tag tracing-v9.9.9", res_local_tag.stdout)

    def test_sdk_tag_discovery_git_init_failure_fails_closed(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_39_git_init_fail_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        empty_sdk = tmp / "empty_sdk.git"
        subprocess.run(
            ["git", "init", "--bare", "-q", str(empty_sdk)],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )

        plugins_repo = tmp / "plugins_repo"
        check_script = self._init_plugins_repo(plugins_repo, env)
        subprocess.run(
            ["git", "-C", str(plugins_repo), "remote", "add", "sdk", empty_sdk.as_uri()],
            capture_output=True, text=True, env=env, check=True, timeout=30,
        )

        real_git = shutil.which("git")
        self.assertIsNotNone(real_git)
        wrapper_bin = tmp / "wrapper_bin"
        wrapper_bin.mkdir(parents=True, exist_ok=True)
        git_wrapper = wrapper_bin / "git"
        git_wrapper.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"${1:-}\" == \"init\" ]]; then\n"
            "  exit 1\n"
            "fi\n"
            f"exec {shlex.quote(real_git)} \"$@\"\n",
            encoding="utf-8",
        )
        os.chmod(git_wrapper, 0o755)

        wrapper_env = dict(env, PATH=f"{wrapper_bin}{os.pathsep}{env.get('PATH', '')}")
        res = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=wrapper_env, timeout=30,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertIn(
            "not ok - failed to fetch tracing-v release tags from SDK remote",
            res.stdout,
        )
        self.assertNotIn("ok - no tracing-v release tags exist yet", res.stdout)

    def test_verify_tag_snapshot_source_only_vendor_reconstruction(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_tag_vendor_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        def _git(*args, cwd):
            return subprocess.run(
                ["git", "-C", str(cwd), *args],
                capture_output=True, text=True, env=env, check=True, timeout=30,
            )

        plugins_repo = tmp / "plugins_repo"
        check_script = self._init_plugins_repo(plugins_repo, env)
        dist_dir = plugins_repo / "plugins/claude_code_dist/bigquery-agent-analytics-tracing"
        dist_ver = json.loads((dist_dir / ".claude-plugin/plugin.json").read_text())["version"]
        tag_name = f"tracing-v{dist_ver}"

        # 1. Source-only tag (no producers/, no plugins/claude_code_dist/) with archived
        #    plugins/claude_code/vendor/ matching dist/vendor -> --check PASSES.
        sdk_matching = tmp / "sdk_matching"
        shutil.copytree(
            REPO_ROOT / "plugins/claude_code",
            sdk_matching / "plugins/claude_code",
            ignore=_ignore_claude_code_src,
        )
        shutil.copytree(
            dist_dir / "vendor",
            sdk_matching / "plugins/claude_code/vendor",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        _git("init", "-q", cwd=sdk_matching)
        _git("add", "-f", "-A", cwd=sdk_matching)
        _git("commit", "-q", "-m", "source-only with archived vendor", cwd=sdk_matching)
        _git("tag", tag_name, cwd=sdk_matching)

        ok_res = subprocess.run(
            ["bash", str(check_script), "--check", "--sdk-repo", str(sdk_matching)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(ok_res.returncode, 0, f"{ok_res.stdout}\n{ok_res.stderr}")
        self.assertIn(f"ok - {tag_name} exists and committed dist matches {tag_name}", ok_res.stdout)

        # 2. Source-only tag with archived plugins/claude_code/vendor/ differing from dist/vendor
        #    must fail with differs: vendor/... (must not borrow current dist/vendor).
        sdk_diff = tmp / "sdk_diff"
        shutil.copytree(sdk_matching, sdk_diff)
        _git("tag", "-d", tag_name, cwd=sdk_diff)
        archived_drain = sdk_diff / "plugins/claude_code/vendor/bigquery_agent_analytics_tracing/drain.py"
        archived_drain.write_text(archived_drain.read_text() + "\n# archived tag marker\n", encoding="utf-8")
        _git("add", "-f", "-A", cwd=sdk_diff)
        _git("commit", "-q", "-m", "archived vendor differs", cwd=sdk_diff)
        _git("tag", tag_name, cwd=sdk_diff)

        diff_res = subprocess.run(
            ["bash", str(check_script), "--check", "--sdk-repo", str(sdk_diff)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(diff_res.returncode, 0)
        self.assertIn("differs: vendor/bigquery_agent_analytics_tracing/drain.py", diff_res.stdout)
        self.assertIn(f"changed since release tag {tag_name}", diff_res.stdout)

        # 3. Source-only tag with NEITHER producers/ NOR plugins/claude_code/vendor/
        #    must fail closed outright.
        sdk_missing = tmp / "sdk_missing"
        shutil.copytree(
            REPO_ROOT / "plugins/claude_code",
            sdk_missing / "plugins/claude_code",
            ignore=_ignore_claude_code_src,
        )
        _git("init", "-q", cwd=sdk_missing)
        _git("add", "-A", cwd=sdk_missing)
        _git("commit", "-q", "-m", "source-only without vendor", cwd=sdk_missing)
        _git("tag", tag_name, cwd=sdk_missing)

        missing_res = subprocess.run(
            ["bash", str(check_script), "--check", "--sdk-repo", str(sdk_missing)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(missing_res.returncode, 0)
        self.assertIn(
            f"not ok - release tag {tag_name} has neither producers/ nor plugins/claude_code/vendor to reconstruct vendor/",
            missing_res.stdout,
        )

        # 4. Source-only tag whose plugins/claude_code/vendor/ holds only bytecode
        #    must fail closed explicitly after strip_bytecode.
        sdk_pyc_only = tmp / "sdk_pyc_only"
        shutil.copytree(
            REPO_ROOT / "plugins/claude_code",
            sdk_pyc_only / "plugins/claude_code",
            ignore=_ignore_claude_code_src,
        )
        pyc_dir = sdk_pyc_only / "plugins/claude_code/vendor/bigquery_agent_analytics_tracing/__pycache__"
        pyc_dir.mkdir(parents=True, exist_ok=True)
        (pyc_dir / "drain.cpython-314.pyc").write_bytes(b"fake-pyc")
        _git("init", "-q", cwd=sdk_pyc_only)
        _git("add", "-f", "-A", cwd=sdk_pyc_only)
        _git("commit", "-q", "-m", "source-only with bytecode-only vendor", cwd=sdk_pyc_only)
        _git("tag", tag_name, cwd=sdk_pyc_only)

        pyc_res = subprocess.run(
            ["bash", str(check_script), "--check", "--sdk-repo", str(sdk_pyc_only)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertNotEqual(pyc_res.returncode, 0)
        self.assertIn(
            "not ok - release tag snapshot vendor/ contains no usable files after stripping bytecode",
            pyc_res.stdout,
        )

    def test_local_untracked_vendor_does_not_shadow_dist_vendor(self):
        tmp = Path(tempfile.mkdtemp(prefix="bqaa_local_vendor_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        env = self._clean_git_env()

        plugins_repo = tmp / "plugins_repo"
        check_script = self._init_plugins_repo(plugins_repo, env)
        dist_dir = plugins_repo / "plugins/claude_code_dist/bigquery-agent-analytics-tracing"
        dist_drain = dist_dir / "vendor/bigquery_agent_analytics_tracing/drain.py"
        expected_drain = dist_drain.read_text(encoding="utf-8")

        # Create an untracked, differing local plugins/claude_code/vendor/ in the checkout.
        local_vendor = plugins_repo / "plugins/claude_code/vendor"
        shutil.copytree(dist_dir / "vendor", local_vendor)
        local_drain = local_vendor / "bigquery_agent_analytics_tracing/drain.py"
        local_drain.write_text(expected_drain + "\n# stale untracked local vendor\n", encoding="utf-8")
        (local_vendor / "extra_untracked.py").write_text("# should not be copied\n", encoding="utf-8")

        check_res = subprocess.run(
            ["bash", str(check_script), "--check"],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(check_res.returncode, 0, f"{check_res.stdout}\n{check_res.stderr}")
        self.assertIn("ok - committed dist matches what plugins/claude_code builds", check_res.stdout)

        out_dir = tmp / "built_out"
        build_res = subprocess.run(
            ["bash", str(check_script), str(out_dir)],
            capture_output=True, text=True, env=env, timeout=30,
        )
        self.assertEqual(build_res.returncode, 0, f"{build_res.stdout}\n{build_res.stderr}")
        self.assertEqual(
            (out_dir / "vendor/bigquery_agent_analytics_tracing/drain.py").read_text(encoding="utf-8"),
            expected_drain,
        )
        self.assertFalse((out_dir / "vendor/extra_untracked.py").exists())

    def test_ignore_claude_code_src_scoped_to_root(self):
        self.assertEqual(
            _ignore_claude_code_src(str(SRC_DIR), ["vendor", "hooks", "__pycache__", "a.pyc"]),
            {"vendor", "__pycache__", "a.pyc"},
        )
        self.assertEqual(
            _ignore_claude_code_src(str(SRC_DIR / "hooks"), ["vendor", "common.sh", "__pycache__", "b.pyo"]),
            {"__pycache__", "b.pyo"},
        )


class TestDestinationPartitionedSpool(unittest.TestCase):
    """Per-destination spool and drainer lock partitioning."""

    def test_partition_id_collision_resistance_and_idempotence(self):
        from bigquery_agent_analytics_tracing.config import (
            destination_partition_id,
            destination_spool_dir,
        )

        p_empty = destination_partition_id("", "ds", "tbl")
        self.assertEqual(p_empty, "")
        self.assertEqual(destination_partition_id("proj", "", "tbl"), "")

        p1 = destination_partition_id("a.b", "c", "t")
        p2 = destination_partition_id("a", "b.c", "t")
        p3 = destination_partition_id("a", "b", "c.t")
        p4 = destination_partition_id("a", "b", "other_table")
        self.assertTrue(p1.startswith("dest-"))
        self.assertEqual(len({p1, p2, p3, p4}), 4)
        self.assertEqual(
            destination_partition_id("proj", "ds", ""),
            destination_partition_id("proj", "ds", "agent_events"),
        )

        base = "/tmp/example-spool"
        once = destination_spool_dir(base, "proj", "ds", "tbl")
        twice = destination_spool_dir(once, "proj", "ds", "tbl")
        self.assertEqual(once, twice)
        self.assertEqual(destination_spool_dir(base, "", "ds", "tbl"), base)

    def test_active_drainer_a_retry_backoff_does_not_block_or_quarantine_b(self):
        import threading
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing import logger as bq_logger
        from bigquery_agent_analytics_tracing.config import (
            BQAAConfig,
            default_spool_dir,
            destination_spool_dir,
        )
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_part_repro_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        os.chmod(tmp_root, 0o700)

        a_attempted = threading.Event()
        a_allow_success = threading.Event()
        delivered_lock = threading.Lock()
        delivered: dict[str, list[dict]] = {}

        def _insert(table_id, rows):
            if table_id == "proj-a.ds_a.agent_events" and not a_allow_success.is_set():
                a_attempted.set()
                raise RuntimeError("503 transient failure for destination A")
            with delivered_lock:
                delivered.setdefault(table_id, []).extend(rows)
            return []

        _install_fake_bigquery(
            self,
            insert_fn=_insert,
            get_table_fn=lambda tid: object(),
        )

        env_backup = os.environ.copy()
        try:
            os.environ["TMPDIR"] = tmp_root
            os.environ.pop("BQAA_SPOOL_DIR", None)
            os.environ["BQAA_PROJECT_ID"] = "proj-a"
            os.environ["BQAA_DATASET"] = "ds_a"
            os.environ["BQAA_TABLE"] = "agent_events"
            os.environ["BQAA_LOCATION"] = "US"
            os.environ["BQAA_DRAIN_RETRY_BACKOFF_SECONDS"] = "99.0"

            shared_default_spool = Path(default_spool_dir())
            cfg_a = BQAAConfig(
                project_id="proj-a",
                dataset="ds_a",
                table="agent_events",
                auto_create_table=False,
                spool_dir=str(shared_default_spool),
                state_dir=os.path.join(tmp_root, "state_a"),
                log_file=os.path.join(tmp_root, "a.log"),
                dry_run=False,
                drain_idle_seconds=0.15,
                drain_poll_seconds=0.02,
                drain_retry_backoff_seconds=1.5,
                drain_max_retry_age_seconds=60.0,
            )
            cfg_b = BQAAConfig(
                project_id="proj-b",
                dataset="ds_b",
                table="agent_events",
                auto_create_table=False,
                spool_dir=str(shared_default_spool),
                state_dir=os.path.join(tmp_root, "state_b"),
                log_file=os.path.join(tmp_root, "b.log"),
                dry_run=False,
                drain_idle_seconds=0.15,
                drain_poll_seconds=0.02,
                drain_retry_backoff_seconds=0.05,
                drain_max_retry_age_seconds=60.0,
            )

            # Spool an A row and start A's drainer; it fails once and enters retry backoff holding A's .drainer.pid.
            with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
                BigQueryAgentAnalyticsLogger(cfg_a).log_event(
                    event_type="LLM_REQUEST", content={"session": "A"}
                )

            errors_a: list[BaseException] = []

            def _run_a():
                try:
                    asyncio.run(drain._run(cfg_a))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors_a.append(exc)

            thread_a = threading.Thread(target=_run_a, daemon=True)
            thread_a.start()
            self.assertTrue(a_attempted.wait(timeout=5.0), "drainer A never attempted write")

            # Wait until A's retry sidecar is persisted so A is actively in backoff holding A's lock.
            part_a = Path(destination_spool_dir(shared_default_spool, "proj-a", "ds_a", "agent_events"))
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not list(part_a.glob("*.retry")):
                time.sleep(0.01)
            self.assertTrue(list(part_a.glob("*.retry")), "expected A row in retry backoff")

            # While A holds its lock in backoff, log B through real BigQueryAgentAnalyticsLogger(cfg_b).
            # _ensure_drainer(cfg_b) must NOT be blocked by A's lock and must pass B's explicit env vars.
            spawn_calls: list[dict] = []
            with mock.patch.object(
                bq_logger.subprocess,
                "Popen",
                side_effect=lambda *a, **kw: spawn_calls.append(dict(kw)) or None,
            ):
                BigQueryAgentAnalyticsLogger(cfg_b).log_event(
                    event_type="LLM_REQUEST", content={"session": "B"}
                )

            self.assertEqual(len(spawn_calls), 1, "B must spawn its own drainer even while A holds lock")
            child_env = spawn_calls[0]["env"]
            self.assertEqual(child_env["BQAA_PROJECT_ID"], "proj-b")
            self.assertEqual(child_env["BQAA_DATASET"], "ds_b")
            self.assertEqual(child_env["BQAA_TABLE"], "agent_events")
            self.assertEqual(child_env["BQAA_SPOOL_DIR"], str(shared_default_spool))
            self.assertEqual(child_env["BQAA_DRAIN_RETRY_BACKOFF_SECONDS"], "0.05")
            self.assertNotIn("BQAA_LOCATION", child_env)

            # Drainer B runs concurrently with A in backoff, delivers B promptly, and exits cleanly.
            t0 = time.monotonic()
            asyncio.run(drain._run(cfg_b))
            elapsed_b = time.monotonic() - t0
            self.assertLess(elapsed_b, 1.0, f"drainer B waited on A's backoff ({elapsed_b:.2f}s)")

            with delivered_lock:
                b_rows = list(delivered.get("proj-b.ds_b.agent_events", []))
            self.assertEqual(len(b_rows), 1)
            self.assertEqual(json.loads(b_rows[0]["content"]), {"session": "B"})
            self.assertEqual(list(shared_default_spool.rglob("*destination-mismatch*")), [])
            self.assertFalse((shared_default_spool / ".root-drain.lock").exists())

            # Clear A's transient failure and let A finish delivering its row.
            a_allow_success.set()
            thread_a.join(timeout=6.0)
            self.assertFalse(thread_a.is_alive(), "drainer A did not finish after clearing transient error")
            self.assertEqual(errors_a, [])
            with delivered_lock:
                a_rows = list(delivered.get("proj-a.ds_a.agent_events", []))
            self.assertEqual(len(a_rows), 1)
            self.assertEqual(json.loads(a_rows[0]["content"]), {"session": "A"})
            self.assertEqual(list(shared_default_spool.rglob("*destination-mismatch*")), [])
        finally:
            a_allow_success.set()
            os.environ.clear()
            os.environ.update(env_backup)

    def test_shared_explicit_spool_same_dataset_different_tables_concurrent(self):
        from unittest import mock
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing import logger as bq_logger
        from bigquery_agent_analytics_tracing.config import BQAAConfig, destination_spool_dir
        from bigquery_agent_analytics_tracing.logger import BigQueryAgentAnalyticsLogger

        tmp_root = tempfile.mkdtemp(prefix="bqaa_part_tables_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "shared_spool"
        delivered: dict[str, list[dict]] = {}

        def _insert(table_id, rows):
            delivered.setdefault(table_id, []).extend(rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert, get_table_fn=lambda tid: object())

        cfg_t1 = BQAAConfig(
            project_id="proj-shared",
            dataset="ds_shared",
            table="events_one",
            auto_create_table=False,
            spool_dir=str(spool_dir),
            state_dir=os.path.join(tmp_root, "state_1"),
            log_file=os.path.join(tmp_root, "1.log"),
            drain_idle_seconds=0.05,
            drain_poll_seconds=0.01,
        )
        cfg_t2 = BQAAConfig(
            project_id="proj-shared",
            dataset="ds_shared",
            table="events_two",
            auto_create_table=False,
            spool_dir=str(spool_dir),
            state_dir=os.path.join(tmp_root, "state_2"),
            log_file=os.path.join(tmp_root, "2.log"),
            drain_idle_seconds=0.05,
            drain_poll_seconds=0.01,
        )

        p1 = Path(destination_spool_dir(spool_dir, "proj-shared", "ds_shared", "events_one"))
        p2 = Path(destination_spool_dir(spool_dir, "proj-shared", "ds_shared", "events_two"))
        self.assertNotEqual(p1, p2)

        with mock.patch.object(bq_logger, "_ensure_drainer", return_value=None):
            BigQueryAgentAnalyticsLogger(cfg_t1).log_event(event_type="LLM_REQUEST", content={"t": 1})
            BigQueryAgentAnalyticsLogger(cfg_t2).log_event(event_type="LLM_REQUEST", content={"t": 2})

        # Seed a skipped entry in p1 and confirm concurrent drain._run(cfg_t2) does not wipe p1's tracking.
        p1_skip_key = str(p1 / "event-synthetic.json")
        drain._skipped_paths.add(p1_skip_key)
        try:
            async def _both():
                await asyncio.gather(drain._run(cfg_t1), drain._run(cfg_t2))

            asyncio.run(_both())
        finally:
            drain._skipped_paths.discard(p1_skip_key)

        self.assertEqual(len(delivered.get("proj-shared.ds_shared.events_one", [])), 1)
        self.assertEqual(len(delivered.get("proj-shared.ds_shared.events_two", [])), 1)
        self.assertEqual(list(spool_dir.rglob("*destination-mismatch*")), [])

    def test_per_partition_forged_envelope_still_quarantines(self):
        from bigquery_agent_analytics_tracing import drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig, destination_spool_dir

        tmp_root = tempfile.mkdtemp(prefix="bqaa_part_forge_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        delivered: dict[str, list[dict]] = {}

        def _insert(table_id, rows):
            delivered.setdefault(table_id, []).extend(rows)
            return []

        _install_fake_bigquery(self, insert_fn=_insert, get_table_fn=lambda tid: object())

        cfg_a = BQAAConfig(
            project_id="proj-a",
            dataset="ds_a",
            table="agent_events",
            auto_create_table=False,
            spool_dir=str(spool_dir),
            state_dir=os.path.join(tmp_root, "state"),
            log_file=os.path.join(tmp_root, "a.log"),
        )
        part_a = Path(destination_spool_dir(spool_dir, "proj-a", "ds_a", "agent_events"))
        part_a.mkdir(parents=True, mode=0o700)
        os.chmod(spool_dir, 0o700)
        os.chmod(part_a, 0o700)

        forged = part_a / "event-forged.json"
        forged.write_text(
            json.dumps({
                "config": {
                    "project_id": "other-proj",
                    "dataset": "other_ds",
                    "table": "injected_rows",
                },
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST"},
            }),
            encoding="utf-8",
        )
        os.chmod(forged, 0o600)

        resolved = asyncio.run(drain._drain_once(cfg_a, use_storage_api=False))
        self.assertEqual(resolved, 1)
        self.assertNotIn("other-proj.other_ds.injected_rows", delivered)
        self.assertTrue(
            (part_a / "dead-letter" / "destination-mismatch" / "event-forged.destination-mismatch.json").is_file()
        )

    def test_inspect_spool_status_aggregates_partitions_and_rejects_symlink(self):
        from bigquery_agent_analytics_tracing import _utils
        from bigquery_agent_analytics_tracing.config import destination_spool_dir

        tmp_root = tempfile.mkdtemp(prefix="bqaa_part_status_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)
        spool_dir = Path(tmp_root) / "spool"
        spool_dir.mkdir(mode=0o700)
        os.chmod(spool_dir, 0o700)

        part_a = Path(destination_spool_dir(spool_dir, "proj-a", "ds_a", "agent_events"))
        part_b = Path(destination_spool_dir(spool_dir, "proj-b", "ds_b", "agent_events"))
        for d in (part_a, part_b):
            d.mkdir(mode=0o700)
            os.chmod(d, 0o700)

        # Root pending + partition A pending/retry (older) + partition B dead-letter/retry (newer)
        (spool_dir / "event-root.json").write_text("{}", encoding="utf-8")
        (part_a / "event-a1.json").write_text("{}", encoding="utf-8")
        retry_a = part_a / "event-a1.json.retry"
        retry_a.write_text(json.dumps({"last_error": "older error A"}), encoding="utf-8")
        os.utime(retry_a, ns=(1_000_000_000, 1_000_000_000))

        (part_b / "event-b1.json").write_text("{}", encoding="utf-8")
        retry_b = part_b / "event-b1.json.retry"
        retry_b.write_text(json.dumps({"last_error": "newest error B"}), encoding="utf-8")
        os.utime(retry_b, ns=(2_000_000_000, 2_000_000_000))

        dl_b = part_b / "dead-letter"
        mm_b = dl_b / "destination-mismatch"
        mm_b.mkdir(parents=True, mode=0o700)
        os.chmod(dl_b, 0o700)
        os.chmod(mm_b, 0o700)
        (dl_b / "event-b2.write-failed.json").write_text("{}", encoding="utf-8")
        (mm_b / "event-b3.destination-mismatch.json").write_text("{}", encoding="utf-8")

        status = _utils.inspect_spool_status(spool_dir)
        self.assertIsNone(status["error"])
        self.assertEqual(status["pending_count"], 3)
        self.assertEqual(status["deferred_count"], 2)
        self.assertEqual(status["dead_letter_count"], 2)
        self.assertEqual(status["destination_mismatch_count"], 1)
        self.assertEqual(status["latest_error"], "newest error B")

        # Symlinked dest-* directory must be rejected and reported cleanly without creating dirs.
        outside = Path(tmp_root) / "outside_target"
        outside.mkdir(mode=0o700)
        os.symlink(str(outside), str(spool_dir / "dest-evil"))
        bad_status = _utils.inspect_spool_status(spool_dir)
        self.assertIsNotNone(bad_status["error"])

    def test_multi_destination_v010_legacy_migration(self):
        from bigquery_agent_analytics_tracing import _utils, drain
        from bigquery_agent_analytics_tracing.config import BQAAConfig

        tmp_root = tempfile.mkdtemp(prefix="bqaa_part_legacy_")
        self.addCleanup(shutil.rmtree, tmp_root, ignore_errors=True)

        legacy_spool = Path(tmp_root) / "legacy" / "spool"
        legacy_spool.mkdir(parents=True, mode=0o700)
        os.chmod(legacy_spool.parent, 0o700)
        os.chmod(legacy_spool, 0o700)

        new_spool = Path(tmp_root) / "new_spool"
        new_spool.mkdir(mode=0o700)
        os.chmod(new_spool, 0o700)

        (legacy_spool / "event-a.json").write_text(
            json.dumps({
                "config": {"project_id": "proj-a", "dataset": "ds_a", "table": "agent_events"},
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST", "content": {"k": "a"}},
            }),
            encoding="utf-8",
        )
        (legacy_spool / "event-b.json").write_text(
            json.dumps({
                "config": {"project_id": "proj-b", "dataset": "ds_b", "table": "agent_events"},
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST", "content": {"k": "b"}},
            }),
            encoding="utf-8",
        )
        (legacy_spool / "event-empty.json").write_text(
            json.dumps({
                "config": {},
                "row": {"timestamp": "2026-01-01T00:00:00Z", "event_type": "LLM_REQUEST", "content": {"k": "empty"}},
            }),
            encoding="utf-8",
        )
        for p in legacy_spool.glob("event-*.json"):
            os.chmod(p, 0o600)

        # A migrates A + empty {}, leaving B intact in legacy_spool without unsafe_skipped.
        mig_a = _utils.migrate_legacy_spool(
            new_spool,
            legacy_spool_dir=legacy_spool,
            destination=("proj-a", "ds_a", "agent_events"),
        )
        self.assertIsNone(mig_a["error"])
        self.assertEqual(mig_a["migrated"], 2)
        self.assertEqual(mig_a["unsafe_skipped"], 0)
        self.assertFalse((legacy_spool / "event-a.json").exists())
        self.assertFalse((legacy_spool / "event-empty.json").exists())
        self.assertTrue((legacy_spool / "event-b.json").is_file())

        # B migrates B cleanly before A drains, without cross-quarantine.
        mig_b = _utils.migrate_legacy_spool(
            new_spool,
            legacy_spool_dir=legacy_spool,
            destination=("proj-b", "ds_b", "agent_events"),
        )
        self.assertIsNone(mig_b["error"])
        self.assertEqual(mig_b["migrated"], 1)
        self.assertEqual(mig_b["unsafe_skipped"], 0)
        self.assertFalse((legacy_spool / "event-b.json").exists())

        delivered: dict[str, list[dict]] = {}
        _install_fake_bigquery(
            self,
            insert_fn=lambda tid, rows: delivered.setdefault(tid, []).extend(rows) or [],
            get_table_fn=lambda tid: object(),
        )
        cfg_a = BQAAConfig(
            project_id="proj-a",
            dataset="ds_a",
            table="agent_events",
            auto_create_table=False,
            spool_dir=str(new_spool),
            state_dir=os.path.join(tmp_root, "state_a"),
            log_file=os.path.join(tmp_root, "a.log"),
        )
        cfg_b = BQAAConfig(
            project_id="proj-b",
            dataset="ds_b",
            table="agent_events",
            auto_create_table=False,
            spool_dir=str(new_spool),
            state_dir=os.path.join(tmp_root, "state_b"),
            log_file=os.path.join(tmp_root, "b.log"),
        )
        self.assertEqual(asyncio.run(drain._drain_once(cfg_a, use_storage_api=False)), 2)
        self.assertEqual(asyncio.run(drain._drain_once(cfg_b, use_storage_api=False)), 1)
        self.assertEqual(len(delivered.get("proj-a.ds_a.agent_events", [])), 2)
        self.assertEqual(len(delivered.get("proj-b.ds_b.agent_events", [])), 1)
        self.assertEqual(list(new_spool.rglob("*destination-mismatch*")), [])


if __name__ == "__main__":
    unittest.main()
