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
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
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
        "hooks/pre_tool_use.sh",
        "hooks/session_end.sh",
        "hooks/session_start.sh",
        "hooks/stop.sh",
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
        metadata_path = (
            DIST_DIR
            / "vendor"
            / "bigquery_agent_analytics_tracing-0.1.0.dist-info"
            / "METADATA"
        )
        metadata = metadata_path.read_text(encoding="utf-8")
        self.assertIn(f"Version: {dist_json['version']}", metadata)

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
# 3. Vendored package & all 9 hook scripts offline E2E
# =============================================================================


# The 9 Claude Code hook names (matching plugin.json hook keys).
# SubagentStop BEFORE Stop: _stop deletes current_trace_id, which would
# cause _subagent_stop to return early and not write a DRY_RUN line.
HOOK_NAMES = [
    "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
    "SubagentStop", "Stop",
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
    "Stop": {
        "session_id": "e2e-session-001",
        "transcript_path": "",
        "response": "Created the file successfully.",
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

# SessionStart only writes state (no event). The other 8 hooks each write
# one event line. SessionEnd also writes a STATE_DELTA event.
# Total: 8 DRY_RUN lines from 8 event-emitting hooks.
EXPECTED_DRY_RUN_LINES = 8


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
            "Stop": "stop.sh",
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
        """The dry-run log must not contain ERROR lines."""
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        for hook_name in HOOK_NAMES:
            self._run_hook(hook_name, HOOK_PAYLOADS[hook_name])
        if os.path.exists(self.log_file):
            log_text = Path(self.log_file).read_text(
                encoding="utf-8", errors="replace"
            )
            self.assertNotIn("ERROR", log_text,
                            f"error in log:\n{log_text}")

    def test_dry_run_lines_written(self):
        """All 8 event-emitting hooks must write DRY_RUN lines."""
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


if __name__ == "__main__":
    unittest.main()
