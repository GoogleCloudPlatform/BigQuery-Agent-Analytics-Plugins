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

"""Unit tests for scripts/check_repo_hygiene.py.

Uses only the Python standard library (unittest, tempfile, subprocess,
json, pathlib) so the test runs on any Python 3.9+ without pip installs.

Each test builds a minimal throwaway git repository, runs the check against
it, and asserts on the exit code and output. Both the clean case and every
deliberate failure mode are covered.
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

# Load the module by path so the test file does not need a package context.
_MOD_PATH = Path(__file__).resolve().parent.parent / "scripts" / "check_repo_hygiene.py"
_spec = importlib.util.spec_from_file_location("check_repo_hygiene", _MOD_PATH)
check_repo_hygiene = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_repo_hygiene)


_APACHE_HEADER = """\
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
"""


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True, text=True, check=True,
    ).stdout


def _make_repo(files: dict[str, str]) -> Path:
    """Create a throwaway git repo with the given {relpath: content} files."""
    root = Path(tempfile.mkdtemp(prefix="hygiene_test_"))
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "Test")
    for relpath, content in files.items():
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        if relpath.endswith(".sh"):
            os.chmod(path, 0o755)
    _git(root, "add", "-A")
    _git(root, "commit", "--quiet", "-m", "init")
    return root


def _run_check(root: Path) -> tuple[int, str]:
    """Run the hygiene check against *root* and return (exit_code, output)."""
    result = subprocess.run(
        [sys.executable, str(_MOD_PATH), str(root)],
        capture_output=True, text=True,
    )
    return result.returncode, (result.stdout + result.stderr)


# Minimal valid catalog + plugin that passes the catalog check.
_VALID_CATALOG = {
    "name": "test-marketplace",
    "owner": {"name": "Test"},
    "plugins": [
        {
            "name": "test-plugin",
            "description": "A test plugin.",
            "version": "0.1.0",
            "source": "./plugins/test_plugin",
            "homepage": "https://example.com",
            "repository": "https://example.com",
            "license": "Apache-2.0",
        }
    ],
}

_VALID_MANIFEST = {
    "name": "test-plugin",
    "version": "0.1.0",
    "homepage": "https://example.com",
    "repository": "https://example.com",
    "license": "Apache-2.0",
}

_VALID_README = """\
# Test Plugin

A test plugin. See [details](plugins/test_plugin/SUBREADME.md) for more.
"""

_VALID_SUBREADME = "# Details\n\nMore info.\n"


def _clean_repo() -> dict[str, str]:
    """Files for a repo that passes every check."""
    return {
        ".claude-plugin/marketplace.json": json.dumps(_VALID_CATALOG, indent=2),
        "plugins/test_plugin/.claude-plugin/plugin.json": json.dumps(
            _VALID_MANIFEST, indent=2
        ),
        "README.md": _VALID_README,
        "plugins/test_plugin/SUBREADME.md": _VALID_SUBREADME,
        "scripts/check.py": _APACHE_HEADER + '"""doc"""\nprint("ok")\n',
        "hooks/hook.sh": _APACHE_HEADER + "echo ok\n",
    }


class TestCleanRepo(unittest.TestCase):
    """A clean repo passes with exit 0."""

    def test_clean(self):
        root = _make_repo(_clean_repo())
        code, output = _run_check(root)
        self.assertEqual(code, 0, output)
        self.assertIn("ok -", output)


class TestCatalogMismatch(unittest.TestCase):
    """Deliberate failure: catalog entry and manifest disagree on version."""

    def test_version_mismatch(self):
        files = _clean_repo()
        manifest = json.loads(files["plugins/test_plugin/.claude-plugin/plugin.json"])
        manifest["version"] = "0.2.0"
        files["plugins/test_plugin/.claude-plugin/plugin.json"] = json.dumps(
            manifest, indent=2
        )
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("version", output.lower())


class TestInvalidJSON(unittest.TestCase):
    """Deliberate failure: a tracked .json file does not parse."""

    def test_invalid_json(self):
        files = _clean_repo()
        files["broken.json"] = "{not valid json"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("invalid json", output.lower())


class TestBrokenMarkdownLink(unittest.TestCase):
    """Deliberate failure: a relative Markdown link points at a missing file."""

    def test_broken_link(self):
        files = _clean_repo()
        files["README.md"] = "# Test\n\nSee [missing](does-not-exist.md).\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("broken link", output.lower())

    def test_broken_anchor(self):
        files = _clean_repo()
        files["README.md"] = (
            "# Test\n\nSee [details]"
            "(plugins/test_plugin/SUBREADME.md#no-such-anchor).\n"
        )
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("no heading", output.lower())


class TestStaleReference(unittest.TestCase):
    """Deliberate failure: a file refers to the old personal repository."""

    # The stale reference is assembled at runtime so the check does not
    # flag this test file itself.
    _STALE_NEEDLE = "haiyuan" + "-eng-google"
    _STALE_NEEDLE_2 = "bigquery" + "_agent_analytics_skill"

    def test_stale_ref(self):
        files = _clean_repo()
        files["scripts/check.py"] = (
            _APACHE_HEADER
            + '"""doc"""\n# See ' + self._STALE_NEEDLE
            + "/" + self._STALE_NEEDLE_2 + '\nprint("ok")\n'
        )
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn(self._STALE_NEEDLE, output)


class TestShellSyntax(unittest.TestCase):
    """Deliberate failure: a shell script fails bash -n."""

    def test_bad_shell(self):
        files = _clean_repo()
        files["hooks/bad.sh"] = _APACHE_HEADER + "if [ -f ]; then\n  echo broken\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("bash -n failed", output)


class TestWorkingNotes(unittest.TestCase):
    """Deliberate failure: intent.md, plan.md, or spec.md committed."""

    def test_intent_md(self):
        files = _clean_repo()
        files["intent.md"] = "# Intent\n\nWorking note.\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("working note", output.lower())

    def test_plan_md(self):
        files = _clean_repo()
        files["plan.md"] = "# Plan\n\nWorking note.\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("working note", output.lower())

    def test_spec_md(self):
        files = _clean_repo()
        files["spec.md"] = "# Spec\n\nWorking note.\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("working note", output.lower())

    def test_case_insensitive(self):
        files = _clean_repo()
        files["INTENT.md"] = "# Intent\n\nWorking note.\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("working note", output.lower())


class TestMissingLicenseHeader(unittest.TestCase):
    """Deliberate failure: a source/script/workflow file lacks the Apache header."""

    def test_missing_py_header(self):
        files = _clean_repo()
        files["scripts/no_header.py"] = 'print("no header")\n'
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("license header", output.lower())

    def test_missing_sh_header(self):
        files = _clean_repo()
        files["hooks/no_header.sh"] = "echo no header\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("license header", output.lower())

    def test_missing_workflow_header(self):
        files = _clean_repo()
        files[".github/workflows/ci.yml"] = (
            "name: CI\non:\n  push:\n    branches: [main]\n"
        )
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("license header", output.lower())

    def test_missing_ts_header(self):
        files = _clean_repo()
        files["src/index.ts"] = 'export const x = 1;\n'
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 1, output)
        self.assertIn("license header", output.lower())


class TestHasLicenseHeader(unittest.TestCase):
    """A file with the Apache header passes the check."""

    def test_python_with_header(self):
        files = _clean_repo()
        files["scripts/with_header.py"] = _APACHE_HEADER + 'print("ok")\n'
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 0, output)

    def test_workflow_with_header(self):
        files = _clean_repo()
        files[".github/workflows/ci.yml"] = _APACHE_HEADER + "name: CI\n"
        root = _make_repo(files)
        code, output = _run_check(root)
        self.assertEqual(code, 0, output)


class TestDirectFunctions(unittest.TestCase):
    """Direct unit tests for the new check functions."""

    def test_check_working_notes_clean(self):
        root = _make_repo(_clean_repo())
        files = check_repo_hygiene.list_files(root)
        problems: list = []
        count = check_repo_hygiene.check_working_notes(root, files, problems)
        self.assertEqual(problems, [])

    def test_check_license_headers_clean(self):
        root = _make_repo(_clean_repo())
        files = check_repo_hygiene.list_files(root)
        problems: list = []
        count = check_repo_hygiene.check_license_headers(root, files, problems)
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
