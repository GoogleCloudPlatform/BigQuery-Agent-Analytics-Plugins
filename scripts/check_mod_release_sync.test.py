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

"""Unit and local-HTTP tests for scripts/check_mod_release_sync.py.

Uses only the Python standard library so it runs hermetically without pip
installs or network access outside loopback.
"""

import http.server
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_mod_release_sync.py"
_MOD_DIST_MANIFEST = (
    "plugins/claude_code_dist/bigquery-agent-analytics-mod/.claude-plugin/plugin.json"
)
_CATALOG_PATH = ".claude-plugin/marketplace.json"


def _isolated_git_env(home_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["HOME"] = str(home_dir)
    env["XDG_CONFIG_HOME"] = str(home_dir / ".config")
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _git(repo: Path, env: dict[str, str], *args: str) -> str:
    hooks_dir = repo.parent / "empty-hooks"
    hooks_dir.mkdir(exist_ok=True)
    res = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            f"core.hooksPath={hooks_dir}",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            *args,
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return res.stdout.strip()


def _write_dist_and_catalog(
    repo: Path,
    dist_version: str,
    catalog_version: str | None = None,
) -> None:
    if catalog_version is None:
        catalog_version = dist_version
    manifest_file = repo / _MOD_DIST_MANIFEST
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(
        json.dumps(
            {
                "name": "bigquery-agent-analytics-mod",
                "version": dist_version,
                "license": "Apache-2.0",
                "homepage": "https://example.com/mod",
                "repository": "https://example.com/repo",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    catalog_file = repo / _CATALOG_PATH
    catalog_file.parent.mkdir(parents=True, exist_ok=True)
    catalog_file.write_text(
        json.dumps(
            {
                "name": "bqaa-tracing",
                "plugins": [
                    {
                        "name": "bigquery-agent-analytics-mod",
                        "version": catalog_version,
                        "source": "./plugins/claude_code_dist/bigquery-agent-analytics-mod",
                        "license": "Apache-2.0",
                        "homepage": "https://example.com/mod",
                        "repository": "https://example.com/repo",
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _commit_versions(
    repo: Path,
    env: dict[str, str],
    dist_version: str,
    catalog_version: str | None = None,
    message: str | None = None,
) -> str:
    _write_dist_and_catalog(repo, dist_version, catalog_version)
    _git(repo, env, "add", "-A")
    _git(repo, env, "commit", "--no-verify", "-qm", message or f"sync {dist_version}")
    return _git(repo, env, "rev-parse", "HEAD")


class _StubServer:
    """Thread-backed local HTTP server returning scripted responses."""

    def __init__(self, handler_fn):
        outer = self

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                outer._requests.append(
                    {
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                    }
                )
                status, headers, body = handler_fn(self.path, self.headers)
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.end_headers()
                if isinstance(body, str):
                    body = body.encode("utf-8")
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        self._requests: list[dict] = []
        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def requests(self) -> list[dict]:
        return self._requests

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class TestCheckModReleaseSync(unittest.TestCase):
    """Tests for scripts/check_mod_release_sync.py."""

    def _make_repo(self, versions: list[str] = ("0.1.0", "0.1.1")) -> tuple[Path, Path, dict[str, str], dict[str, str]]:
        work = Path(tempfile.mkdtemp(prefix="mod_sync_test_"))
        self.addCleanup(shutil.rmtree, work, True)
        home = work / "home"
        (home / ".config").mkdir(parents=True)
        env = _isolated_git_env(home)
        repo = work / "repo"
        repo.mkdir()
        _git(repo, env, "-c", "init.defaultBranch=main", "init", "-q")
        commits: dict[str, str] = {}
        for ver in versions:
            commits[ver] = _commit_versions(repo, env, ver)
        return work, repo, env, commits

    def _run_checker(
        self,
        repo: Path,
        env: dict[str, str],
        args: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> tuple[int, str]:
        run_env = env.copy()
        # Ensure caller CI/BQAA_OFFLINE don't leak into tests unexpectedly.
        run_env.pop("CI", None)
        run_env.pop("BQAA_OFFLINE", None)
        run_env.pop("BQAA_MOD_RELEASES_API_URL", None)
        run_env.pop("BQAA_MOD_GITHUB_REPO", None)
        run_env.pop("GITHUB_REPOSITORY", None)
        run_env.pop("BQAA_MOD_NOW_EPOCH", None)
        run_env.pop("GH_TOKEN", None)
        run_env.pop("GITHUB_TOKEN", None)
        run_env["BQAA_MOD_HTTP_RETRY_DELAY_SECONDS"] = "0"
        if extra_env:
            run_env.update(extra_env)
        cmd = [sys.executable, "-B", str(_SCRIPT_PATH), *(args or []), str(repo)]
        res = subprocess.run(cmd, capture_output=True, text=True, env=run_env)
        return res.returncode, res.stdout + res.stderr

    def _start_server(self, handler_fn) -> _StubServer:
        srv = _StubServer(handler_fn)
        self.addCleanup(srv.close)
        return srv

    def test_synced_repo_passes(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        srv = self._start_server(
            lambda path, hdrs: (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    [
                        {"tag_name": "mod-v0.1.0", "draft": False, "prerelease": True},
                        {"tag_name": "mod-v0.1.1", "draft": False, "prerelease": True},
                    ]
                ),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertEqual(rc, 0, out)

    def test_paginated_api_over_30_releases(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.35"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.35", commits["0.1.35"])

        page1 = [
            {"tag_name": f"tracing-v0.1.{i}", "draft": False, "prerelease": False}
            for i in range(32)
        ]
        page2 = [
            {"tag_name": "mod-v0.1.0", "draft": False, "prerelease": True},
            {"tag_name": "mod-v0.1.35", "draft": False, "prerelease": False},
        ]

        srv_holder: list[_StubServer] = []

        def handler(path, _hdrs):
            base = srv_holder[0].base_url
            if path.startswith("/releases?page=2"):
                return 200, {"Content-Type": "application/json"}, json.dumps(page2)
            return (
                200,
                {
                    "Content-Type": "application/json",
                    "Link": f'<{base}/releases?page=2>; rel="next"',
                },
                json.dumps(page1),
            )

        srv = self._start_server(handler)
        srv_holder.append(srv)

        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases?page=1"},
        )
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(srv.requests), 2)

        # Also prove page 2 stale release fails (not truncated at page 1).
        page2.append({"tag_name": "mod-v0.1.36", "draft": False, "prerelease": False})
        rc2, out2 = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases?page=1"},
        )
        self.assertNotEqual(rc2, 0, out2)
        self.assertIn("0.1.36", out2)

    def test_draft_ignored_and_prerelease_counted(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        payload = [
            {"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False},
            {"tag_name": "mod-v0.1.1", "draft": False, "prerelease": True},
            {"tag_name": "mod-v0.1.2", "draft": True, "prerelease": True},
        ]
        srv = self._start_server(
            lambda _p, _h: (200, {"Content-Type": "application/json"}, json.dumps(payload))
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertEqual(rc, 0, out)

        # Flipping 0.1.2 from draft to published prerelease must now fail stale dist.
        payload[2]["draft"] = False
        rc2, out2 = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc2, 0, out2)
        self.assertIn("sync_dist.sh 0.1.2", out2)

    def test_stale_dist_fails_with_sync_hint(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.2"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.2", commits["0.1.2"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.3", "draft": False, "prerelease": True}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("sync_dist.sh 0.1.3", out)

    def test_ahead_of_published_dist_fails(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.3"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.3", commits["0.1.3"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.2", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("0.1.3", out)
        self.assertIn("0.1.2", out)

    def test_catalog_mismatch_fails(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _commit_versions(repo, env, "0.1.1", catalog_version="0.1.0", message="mismatch")
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("catalog", out.lower())

    def test_missing_prior_mod_dist_tag_fails_with_sha_and_hint(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1", "0.1.2"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        # Deliberately omit mod-dist-v0.1.1!
        _git(repo, env, "tag", "mod-dist-v0.1.2", commits["0.1.2"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.2", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("mod-dist-v0.1.1", out)
        self.assertIn(commits["0.1.1"], out)
        self.assertIn("git tag -a", out)

    def test_wrong_target_mod_dist_tag_fails(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        # Point mod-dist-v0.1.0 at the 0.1.1 commit!
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("mod-dist-v0.1.0", out)

    def test_non_ancestor_mod_dist_tag_fails(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        # Create an orphan commit holding 0.1.0 dist+catalog not on HEAD's ancestry.
        _git(repo, env, "checkout", "--orphan", "other-branch")
        orphan_sha = _commit_versions(repo, env, "0.1.0", message="orphan 0.1.0")
        _git(repo, env, "checkout", "main")
        _git(repo, env, "tag", "mod-dist-v0.1.0", orphan_sha)
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("ancestor", out.lower())

    def test_non_semver_mod_dist_git_tag_fails_closed(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0-rc1", commits["0.1.0"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("mod-dist-v0.1.0-rc1", out)

    def test_non_semver_published_mod_release_fails_closed(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    [
                        {"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False},
                        {"tag_name": "mod-v0.1.1-rc1", "draft": False, "prerelease": True},
                    ]
                ),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("mod-v0.1.1-rc1", out)

    def test_missing_current_mod_dist_tag_with_and_without_base_ref(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        # Multi-commit sync PR: base is 0.1.1 (with mod-dist-v0.1.1), PR adds two commits.
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        _git(repo, env, "checkout", "-b", "sync-0.1.2")
        _commit_versions(repo, env, "0.1.2", message="sync dist 0.1.2")
        (repo / "README.md").write_text("# Docs update\n", encoding="utf-8")
        _git(repo, env, "add", "-A")
        _git(repo, env, "commit", "--no-verify", "-qm", "docs follow-up in sync PR")

        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.2", "draft": False, "prerelease": True}]),
            )
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}

        # Strict mode (no --base-ref) must fail on missing mod-dist-v0.1.2.
        rc_strict, out_strict = self._run_checker(repo, env, extra_env=api_env)
        self.assertNotEqual(rc_strict, 0, out_strict)
        self.assertIn("mod-dist-v0.1.2", out_strict)

        # With --base-ref pointing to base (dist=0.1.1 < 0.1.2), multi-commit PR passes.
        rc_pr, out_pr = self._run_checker(
            repo, env, args=["--base-ref", commits["0.1.1"]], extra_env=api_env
        )
        self.assertEqual(rc_pr, 0, out_pr)

        # With --base-ref having equal dist (0.1.2 == 0.1.2), must fail!
        rc_eq, out_eq = self._run_checker(
            repo, env, args=["--base-ref", "HEAD~1"], extra_env=api_env
        )
        self.assertNotEqual(rc_eq, 0, out_eq)

    def test_rejects_leading_dash_or_unknown_base_ref(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            )
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}

        rc_dash, out_dash = self._run_checker(
            repo, env, args=["--base-ref=-n"], extra_env=api_env
        )
        self.assertNotEqual(rc_dash, 0, out_dash)
        self.assertIn("base-ref", out_dash.lower())

        rc_unk, out_unk = self._run_checker(
            repo, env, args=["--base-ref", "refs/heads/nonexistent"], extra_env=api_env
        )
        self.assertNotEqual(rc_unk, 0, out_unk)
        self.assertIn("base-ref", out_unk.lower())

    def test_offline_ci_vs_local(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])

        # CI=true + BQAA_OFFLINE=1 must fail.
        rc_ci, out_ci = self._run_checker(
            repo, env, extra_env={"CI": "true", "BQAA_OFFLINE": "1"}
        )
        self.assertNotEqual(rc_ci, 0, out_ci)

        # CI=true + --offline must fail.
        rc_ci2, out_ci2 = self._run_checker(
            repo, env, args=["--offline"], extra_env={"CI": "true"}
        )
        self.assertNotEqual(rc_ci2, 0, out_ci2)

        # Local (CI!=true) + BQAA_OFFLINE=1 skips network and passes when offline checks pass.
        rc_local, out_local = self._run_checker(
            repo, env, extra_env={"BQAA_OFFLINE": "1"}
        )
        self.assertEqual(rc_local, 0, out_local)

        # Local --offline still catches missing prior mod-dist-v*!
        _git(repo, env, "tag", "-d", "mod-dist-v0.1.0")
        rc_local_bad, out_local_bad = self._run_checker(
            repo, env, args=["--offline"]
        )
        self.assertNotEqual(rc_local_bad, 0, out_local_bad)
        self.assertIn("mod-dist-v0.1.0", out_local_bad)

    def test_shallow_repo_fails(self):
        work, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        shallow = work / "shallow"
        subprocess.run(
            ["git", "clone", "-q", "--depth", "1", f"file://{repo}", str(shallow)],
            check=True,
            env=env,
        )
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            shallow,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn("shallow", out.lower())

    def test_http_and_payload_failures_fail_closed(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])

        # 1. HTTP 500
        srv500 = self._start_server(lambda _p, _h: (500, {}, "internal error"))
        rc, _ = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv500.base_url}/releases"}
        )
        self.assertNotEqual(rc, 0)

        # 2. Non-JSON
        srv_bad_json = self._start_server(
            lambda _p, _h: (200, {"Content-Type": "application/json"}, "{not json")
        )
        rc, _ = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv_bad_json.base_url}/releases"}
        )
        self.assertNotEqual(rc, 0)

        # 3. Non-list JSON
        srv_obj = self._start_server(
            lambda _p, _h: (200, {"Content-Type": "application/json"}, '{"error": "bad"}')
        )
        rc, _ = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv_obj.base_url}/releases"}
        )
        self.assertNotEqual(rc, 0)

        # 4. Missing bool `draft`
        srv_no_draft = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": "false"}]),
            )
        )
        rc, _ = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv_no_draft.base_url}/releases"}
        )
        self.assertNotEqual(rc, 0)

        # 5. Cross-origin Link: rel="next" fails closed
        srv_xorigin_link = self._start_server(
            lambda _p, _h: (
                200,
                {
                    "Content-Type": "application/json",
                    "Link": '<https://evil.example.com/releases?page=2>; rel="next"',
                },
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv_xorigin_link.base_url}/releases"},
        )
        self.assertNotEqual(rc, 0, out)

        # 6. >MAX_PAGES (51 pages) fails closed
        srv_holder: list[_StubServer] = []

        def many_pages(path, _h):
            base = srv_holder[0].base_url
            page = int(path.split("page=")[-1]) if "page=" in path else 1
            return (
                200,
                {
                    "Content-Type": "application/json",
                    "Link": f'<{base}/releases?page={page + 1}>; rel="next"',
                },
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False}]),
            )

        srv_many = self._start_server(many_pages)
        srv_holder.append(srv_many)
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv_many.base_url}/releases?page=1"},
        )
        self.assertNotEqual(rc, 0, out)
        self.assertEqual(len(srv_many.requests), 50)

    def test_cross_origin_redirect_strips_authorization(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])

        # Use 127.0.0.1 for server1 and localhost (different netloc) for server2.
        srv2 = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            )
        )
        port2 = int(srv2.base_url.rsplit(":", 1)[1])
        srv1 = self._start_server(
            lambda _p, _h: (
                302,
                {"Location": f"http://localhost:{port2}/releases"},
                "",
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={
                "BQAA_MOD_RELEASES_API_URL": f"{srv1.base_url}/releases",
                "GH_TOKEN": "secret-token-value",
            },
        )
        self.assertEqual(rc, 0, out)
        self.assertEqual(srv1.requests[0]["headers"].get("authorization"), "Bearer secret-token-value")
        self.assertNotIn("authorization", srv2.requests[0]["headers"])

    def test_zero_published_bootstrap_passes(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "tracing-v0.1.0", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"},
        )
        self.assertEqual(rc, 0, out)

    def test_releases_json_and_invalid_repo_or_url_scheme(self):
        work, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        json_path = work / "releases.json"
        json_path.write_text(
            json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            encoding="utf-8",
        )
        rc, out = self._run_checker(
            repo, env, args=["--releases-json", str(json_path)]
        )
        self.assertEqual(rc, 0, out)

        rc_bad_repo, out_bad_repo = self._run_checker(
            repo,
            env,
            args=["--repo", "bad/repo/name", "--releases-json", str(json_path)],
        )
        self.assertNotEqual(rc_bad_repo, 0, out_bad_repo)

        rc_bad_url, out_bad_url = self._run_checker(
            repo,
            env,
            extra_env={"BQAA_MOD_RELEASES_API_URL": "file:///etc/passwd"},
        )
        self.assertNotEqual(rc_bad_url, 0, out_bad_url)

    def test_numeric_semver_ordering_0_1_9_vs_0_1_10(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.9"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.9", commits["0.1.9"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    [
                        {"tag_name": "mod-v0.1.9", "draft": False, "prerelease": False},
                        {"tag_name": "mod-v0.1.10", "draft": False, "prerelease": False},
                    ]
                ),
            )
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}
        rc_stale, out_stale = self._run_checker(repo, env, extra_env=api_env)
        self.assertNotEqual(rc_stale, 0, out_stale)
        self.assertIn("sync_dist.sh 0.1.10", out_stale)

        _commit_versions(repo, env, "0.1.10", message="sync 0.1.10")
        rc_ok, out_ok = self._run_checker(
            repo, env, args=["--base-ref", commits["0.1.9"]], extra_env=api_env
        )
        self.assertEqual(rc_ok, 0, out_ok)

    def test_empty_releases_fails_closed_on_default_repo_but_passes_on_tagless_fork(self):
        _, repo, env, _ = self._make_repo(["0.1.0", "0.1.1", "0.1.2"])
        srv = self._start_server(
            lambda _p, _h: (200, {"Content-Type": "application/json"}, json.dumps([]))
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}

        rc_default, out_default = self._run_checker(repo, env, extra_env=api_env)
        self.assertNotEqual(rc_default, 0, out_default)
        self.assertIn("no published mod-v* releases", out_default)

        rc_fork, out_fork = self._run_checker(
            repo, env, args=["--repo", "alice/fork"], extra_env=api_env
        )
        self.assertEqual(rc_fork, 0, out_fork)

    def test_dirty_working_tree_fails_closed(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}

        # Dirty dist manifest without committing.
        _write_dist_and_catalog(repo, "0.1.2", catalog_version="0.1.1")
        rc_dist, out_dist = self._run_checker(repo, env, extra_env=api_env)
        self.assertNotEqual(rc_dist, 0, out_dist)
        self.assertIn("uncommitted changes", out_dist.lower())

        _git(repo, env, "checkout", "--", _MOD_DIST_MANIFEST, _CATALOG_PATH)

        # Dirty catalog without committing.
        _write_dist_and_catalog(repo, "0.1.1", catalog_version="0.1.2")
        rc_cat, out_cat = self._run_checker(repo, env, extra_env=api_env)
        self.assertNotEqual(rc_cat, 0, out_cat)
        self.assertIn("uncommitted changes", out_cat.lower())

    def test_missing_current_tag_hint_names_dist_introducing_commit_not_head(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        sync_sha = _commit_versions(repo, env, "0.1.1", message="sync 0.1.1")
        # An unrelated commit touching another plugin in marketplace.json (like PR #50)
        # and an unrelated README edit must NOT hijack the mod 0.1.1 hint SHA.
        catalog_file = repo / _CATALOG_PATH
        cat_data = json.loads(catalog_file.read_text(encoding="utf-8"))
        cat_data["plugins"].append({"name": "bigquery-agent-analytics-tracing", "version": "0.1.1"})
        catalog_file.write_text(json.dumps(cat_data, indent=2) + "\n", encoding="utf-8")
        _git(repo, env, "add", "-A")
        _git(repo, env, "commit", "--no-verify", "-qm", "unrelated tracing catalog bump")
        tracing_bump_sha = _git(repo, env, "rev-parse", "HEAD")
        (repo / "README.md").write_text("# Unrelated follow-up\n", encoding="utf-8")
        _git(repo, env, "add", "-A")
        _git(repo, env, "commit", "--no-verify", "-qm", "unrelated follow-up")
        head_sha = _git(repo, env, "rev-parse", "HEAD")
        self.assertNotEqual(sync_sha, tracing_bump_sha)
        self.assertNotEqual(sync_sha, head_sha)

        srv = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        rc, out = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}
        )
        self.assertNotEqual(rc, 0, out)
        self.assertIn(sync_sha, out)
        self.assertNotIn(tracing_bump_sha, out)
        self.assertNotIn(head_sha, out)

    def test_grace_minutes_window_passes_within_and_fails_after(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        intro_ts = int(_git(repo, env, "show", "-s", "--format=%ct", commits["0.1.1"]))

        # 1. Stale release within 60m grace vs past 60m grace.
        pub_iso = "2026-11-16T10:00:00Z"
        pub_epoch = 1794823200  # 2026-11-16T10:00:00Z
        srv_stale = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps(
                    [
                        {
                            "tag_name": "mod-v0.1.2",
                            "draft": False,
                            "prerelease": False,
                            "published_at": pub_iso,
                        }
                    ]
                ),
            )
        )
        api_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv_stale.base_url}/releases"}

        rc_within, out_within = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**api_env, "BQAA_MOD_NOW_EPOCH": str(pub_epoch + 1800)},
        )
        self.assertEqual(rc_within, 0, out_within)
        self.assertIn("grace", out_within.lower())

        rc_expired, out_expired = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**api_env, "BQAA_MOD_NOW_EPOCH": str(pub_epoch + 3600)},
        )
        self.assertNotEqual(rc_expired, 0, out_expired)
        self.assertIn("sync_dist.sh 0.1.2", out_expired)

        # Ahead-of-published NEVER gets grace.
        _commit_versions(repo, env, "0.1.3", message="ahead 0.1.3")
        _git(repo, env, "tag", "mod-dist-v0.1.3", _git(repo, env, "rev-parse", "HEAD"))
        rc_ahead, out_ahead = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**api_env, "BQAA_MOD_NOW_EPOCH": str(pub_epoch + 60)},
        )
        self.assertNotEqual(rc_ahead, 0, out_ahead)

        # Reset back to 0.1.1 and drop mod-dist-v0.1.1 to test post-merge grace.
        _git(repo, env, "tag", "-d", "mod-dist-v0.1.3")
        _git(repo, env, "reset", "--hard", commits["0.1.1"])
        _git(repo, env, "tag", "-d", "mod-dist-v0.1.1")
        srv_cur = self._start_server(
            lambda _p, _h: (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
            )
        )
        cur_env = {"BQAA_MOD_RELEASES_API_URL": f"{srv_cur.base_url}/releases"}

        rc_cur_in, out_cur_in = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**cur_env, "BQAA_MOD_NOW_EPOCH": str(intro_ts + 300)},
        )
        self.assertEqual(rc_cur_in, 0, out_cur_in)
        self.assertIn("grace", out_cur_in.lower())

        rc_cur_out, out_cur_out = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**cur_env, "BQAA_MOD_NOW_EPOCH": str(intro_ts + 3600)},
        )
        self.assertNotEqual(rc_cur_out, 0, out_cur_out)
        self.assertIn(commits["0.1.1"], out_cur_out)

        # Missing PRIOR mod-dist-v0.1.0 NEVER gets grace.
        _git(repo, env, "tag", "-d", "mod-dist-v0.1.0")
        rc_prior, out_prior = self._run_checker(
            repo,
            env,
            args=["--grace-minutes", "60"],
            extra_env={**cur_env, "BQAA_MOD_NOW_EPOCH": str(intro_ts + 60)},
        )
        self.assertNotEqual(rc_prior, 0, out_prior)
        self.assertIn("mod-dist-v0.1.0", out_prior)

        # Negative --grace-minutes rejected.
        rc_neg, _ = self._run_checker(
            repo, env, args=["--grace-minutes", "-1"], extra_env=cur_env
        )
        self.assertEqual(rc_neg, 2)

    def test_transient_http_503_retries_and_succeeds(self):
        _, repo, env, commits = self._make_repo(["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        calls = {"n": 0}

        def flaky_handler(_p, _h):
            calls["n"] += 1
            if calls["n"] < 3:
                return 503, {}, "service unavailable"
            return (
                200,
                {"Content-Type": "application/json"},
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            )

        srv = self._start_server(flaky_handler)
        rc, out = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases"}
        )
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(srv.requests), 3)

    def test_rfc8288_link_quoted_comma_and_case_insensitive_rel(self):
        _, repo, env, commits = self._make_repo(["0.1.0", "0.1.1"])
        _git(repo, env, "tag", "mod-dist-v0.1.0", commits["0.1.0"])
        _git(repo, env, "tag", "mod-dist-v0.1.1", commits["0.1.1"])
        srv_holder: list[_StubServer] = []

        def handler(path, _h):
            base = srv_holder[0].base_url
            if path.startswith("/releases?page=2"):
                return (
                    200,
                    {"Content-Type": "application/json"},
                    json.dumps([{"tag_name": "mod-v0.1.1", "draft": False, "prerelease": False}]),
                )
            return (
                200,
                {
                    "Content-Type": "application/json",
                    "Link": f'<{base}/releases?page=2>; title="a, b"; REL="PREV NEXT"',
                },
                json.dumps([{"tag_name": "mod-v0.1.0", "draft": False, "prerelease": False}]),
            )

        srv = self._start_server(handler)
        srv_holder.append(srv)
        rc, out = self._run_checker(
            repo, env, extra_env={"BQAA_MOD_RELEASES_API_URL": f"{srv.base_url}/releases?page=1"}
        )
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(srv.requests), 2)

    def test_dot_segment_repo_rejected(self):
        _, repo, env, _ = self._make_repo(["0.1.0"])
        for bad_repo in ("foo/..", "./bar", "../baz", "foo/."):
            rc, out = self._run_checker(
                repo, env, args=["--repo", bad_repo, "--offline"]
            )
            self.assertEqual(rc, 2, f"expected rc=2 for {bad_repo}: {out}")


if __name__ == "__main__":
    unittest.main()
