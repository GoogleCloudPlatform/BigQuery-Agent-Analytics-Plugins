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

"""Verifies that the committed mod copy and mod-dist-v* tags stay in sync.

    python3 -B scripts/check_mod_release_sync.py [REPO_ROOT]

Checks:
  1. The checkout is not shallow (full history and tags are required) and has
     no uncommitted changes in the committed mod copy or catalog.
  2. The committed copy (plugins/claude_code_dist/bigquery-agent-analytics-mod)
     and catalog entry (.claude-plugin/marketplace.json) in HEAD agree on an
     X.Y.Z version.
  3. Every existing mod-dist-v* tag has an X.Y.Z suffix, is an ancestor of
     HEAD, and holds both the committed copy and catalog entry at that version.
  4. When release tags exist (or on the canonical repository), every previously
     served version V < dist_version seen in first-parent history has a valid
     mod-dist-v<V> tag.
  5. Unless --offline / BQAA_OFFLINE=1 (forbidden when CI=true), queries the
     GitHub Releases API (ignoring draft=true, counting prerelease=true/false)
     and fails if the latest published mod-v* release does not equal
     dist_version, or if mod-dist-v<dist_version> is missing (unless
     --base-ref resolves to a commit with a strictly lower dist version, or
     --grace-minutes covers a fresh release or merge commit).
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True

DEFAULT_REPO = "GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins"
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
MOD_RELEASE_TAG_RE = re.compile(r"^mod-v(\d+)\.(\d+)\.(\d+)$")
MOD_DIST_TAG_RE = re.compile(r"^mod-dist-v(\d+)\.(\d+)\.(\d+)$")
LINK_ITEM_RE = re.compile(r',?\s*<([^>]+)>((?:\s*;\s*(?:[^",;]+|"[^"]*")+)*)')
REL_RE = re.compile(r'\brel\s*=\s*(?:"([^"]+)"|([^\s;]+))', re.IGNORECASE)

CATALOG_REL = ".claude-plugin/marketplace.json"
DIST_DIR_REL = "plugins/claude_code_dist/bigquery-agent-analytics-mod"
DIST_MANIFEST_REL = f"{DIST_DIR_REL}/.claude-plugin/plugin.json"
MOD_PLUGIN_NAME = "bigquery-agent-analytics-mod"
MAX_PAGES = 50
MAX_RETRIES = 3
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
HTTP_TIMEOUT_SECONDS = 15


class _UnsafeRedirectError(ValueError):
    """Raised when an HTTP redirect targets a non-http(s) or malformed URL."""


class _StripAuthOnCrossOriginRedirect(urllib.request.HTTPRedirectHandler):
    """Strip Authorization when redirecting to a different scheme or netloc."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_parsed = urllib.parse.urlparse(newurl)
        if new_parsed.scheme.lower() not in ("http", "https") or not new_parsed.netloc:
            raise _UnsafeRedirectError(f"refusing redirect to unsafe URL: {newurl!r}")
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is None:
            return None
        old_parsed = urllib.parse.urlparse(req.full_url)
        if (old_parsed.scheme.lower(), old_parsed.netloc.lower()) != (
            new_parsed.scheme.lower(),
            new_parsed.netloc.lower(),
        ):
            for hdr_map in (new_req.headers, new_req.unredirected_hdrs):
                for key in list(hdr_map):
                    if key.lower() == "authorization":
                        del hdr_map[key]
        return new_req


def _parse_semver(version: str) -> tuple[int, int, int] | None:
    match = SEMVER_RE.match(version)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def _run_git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _extract_catalog_version(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    plugins = data.get("plugins")
    if not isinstance(plugins, list):
        return None
    for entry in plugins:
        if isinstance(entry, dict) and entry.get("name") == MOD_PLUGIN_NAME:
            ver = entry.get("version")
            return ver if isinstance(ver, str) else None
    return None


def _extract_manifest_version(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    ver = data.get("version")
    return ver if isinstance(ver, str) else None


def _read_json_at_rev(root: Path, rev: str, rel_path: str) -> tuple[bool, object]:
    """Return (exists, parsed_json_or_none) for a file at git revision rev."""
    exists = _run_git(root, "cat-file", "-e", f"{rev}:{rel_path}")
    if exists.returncode != 0:
        return False, None
    show = _run_git(root, "show", f"{rev}:{rel_path}")
    if show.returncode != 0:
        return True, None
    try:
        return True, json.loads(show.stdout)
    except ValueError:
        return True, None


def _next_link_url(headers) -> str | None:
    """Return the URL from Link: <...>; rel="next" if present."""
    link_values = headers.get_all("Link") if hasattr(headers, "get_all") else None
    if not link_values:
        single = headers.get("Link")
        link_values = [single] if single else []
    for raw in link_values:
        for match in LINK_ITEM_RE.finditer(raw):
            url, params = match.group(1).strip(), match.group(2)
            rel_match = REL_RE.search(params)
            if not rel_match:
                continue
            rel_raw = rel_match.group(1) or rel_match.group(2) or ""
            rel_tokens = rel_raw.lower().split()
            if "next" in rel_tokens:
                return url
    return None


def _retry_delay_seconds() -> float:
    raw = os.environ.get("BQAA_MOD_HTTP_RETRY_DELAY_SECONDS", "0.5")
    try:
        val = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"invalid BQAA_MOD_HTTP_RETRY_DELAY_SECONDS {raw!r}"
        ) from exc
    if not math.isfinite(val) or val < 0:
        raise ValueError(
            f"BQAA_MOD_HTTP_RETRY_DELAY_SECONDS must be finite and non-negative: {raw!r}"
        )
    return val


def _now_epoch() -> float:
    raw = os.environ.get("BQAA_MOD_NOW_EPOCH")
    if raw is not None and raw != "":
        try:
            val = float(raw)
        except ValueError as exc:
            raise ValueError(f"invalid BQAA_MOD_NOW_EPOCH {raw!r}") from exc
        if not math.isfinite(val):
            raise ValueError(f"BQAA_MOD_NOW_EPOCH must be finite: {raw!r}")
        return val
    return time.time()


def _parse_iso_utc_epoch(raw: object) -> float | None:
    if not isinstance(raw, str) or not raw:
        return None
    normalized = f"{raw[:-1]}+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.timestamp()


def _fetch_one_page(
    opener: urllib.request.OpenerDirector,
    url: str,
    token: str | None,
    base_delay: float,
) -> tuple[str, str | None]:
    """Fetch a single releases page with bounded retries on transient errors."""
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "BigQuery-Agent-Analytics-Plugins-Release-Sync-Check",
            },
        )
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with opener.open(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"unexpected HTTP status {resp.status}")
                body = resp.read().decode("utf-8")
                link_next = _next_link_url(resp.headers)
                return body, link_next
        except _UnsafeRedirectError as exc:
            raise RuntimeError(str(exc)) from exc
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt + 1 < MAX_RETRIES:
                delay = base_delay * (2 ** attempt)
                if delay > 0:
                    time.sleep(delay)
                continue
            raise RuntimeError(f"GitHub API returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt + 1 < MAX_RETRIES:
                delay = base_delay * (2 ** attempt)
                if delay > 0:
                    time.sleep(delay)
                continue
            raise RuntimeError(f"failed to fetch releases: {exc}") from exc
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"failed to decode releases response: {exc}") from exc
    raise RuntimeError("exhausted retries fetching releases")


def _fetch_releases(api_url: str, token: str | None) -> list[object]:
    """Fetch paginated GitHub releases JSON list, failing closed on errors."""
    parsed_initial = urllib.parse.urlparse(api_url)
    if parsed_initial.scheme.lower() not in ("http", "https") or not parsed_initial.netloc:
        raise ValueError(f"invalid releases API URL scheme or host: {api_url!r}")
    origin = (parsed_initial.scheme.lower(), parsed_initial.netloc.lower())

    base_delay = _retry_delay_seconds()
    opener = urllib.request.build_opener(_StripAuthOnCrossOriginRedirect())
    all_items: list[object] = []
    next_url: str | None = api_url

    for page in range(1, MAX_PAGES + 1):
        if next_url is None:
            break
        parsed_page = urllib.parse.urlparse(next_url)
        if (
            parsed_page.scheme.lower() not in ("http", "https")
            or not parsed_page.netloc
            or (parsed_page.scheme.lower(), parsed_page.netloc.lower()) != origin
        ):
            raise ValueError(
                f"refusing cross-origin or invalid pagination Link URL: {next_url!r}"
            )
        body, link_next = _fetch_one_page(opener, next_url, token, base_delay)

        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise RuntimeError(f"invalid JSON in releases response: {exc}") from exc
        if not isinstance(payload, list):
            raise RuntimeError("expected releases response to be a JSON list")
        all_items.extend(payload)

        if link_next is not None and page == MAX_PAGES:
            raise RuntimeError(
                f"exceeded MAX_PAGES ({MAX_PAGES}) while paginating releases"
            )
        next_url = link_next

    return all_items


def _parse_published_mod_versions(items: list[object]) -> dict[str, str | None]:
    """Validate release items and return {version: published_at} for mod-v*."""
    releases: dict[str, str | None] = {}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise RuntimeError(f"release entry {index} is not a JSON object")
        draft = item.get("draft")
        if not isinstance(draft, bool):
            raise RuntimeError(
                f"release entry {index} lacks boolean 'draft' field: {draft!r}"
            )
        tag_name = item.get("tag_name")
        if not isinstance(tag_name, str):
            raise RuntimeError(
                f"release entry {index} lacks string 'tag_name' field: {tag_name!r}"
            )
        if draft:
            continue
        if tag_name.startswith("mod-v"):
            match = MOD_RELEASE_TAG_RE.match(tag_name)
            if not match:
                raise RuntimeError(
                    f"published release {tag_name!r} does not match mod-vX.Y.Z"
                )
            ver = ".".join(match.groups())
            pub_at = item.get("published_at")
            releases[ver] = pub_at if isinstance(pub_at, str) else None
    return releases


def _check_base_ref(root: Path, base_ref: str, dist_version: str) -> tuple[bool, str]:
    """Return (allowed, description) for whether --base-ref has dist < dist_version."""
    if not base_ref or base_ref.startswith("-"):
        raise ValueError(
            f"invalid --base-ref {base_ref!r}: must not be empty or start with '-'"
        )
    rev = _run_git(
        root, "rev-parse", "--verify", "--end-of-options", f"{base_ref}^{{commit}}"
    )
    if rev.returncode != 0:
        raise ValueError(
            f"invalid or unknown --base-ref {base_ref!r}: {rev.stderr.strip()}"
        )
    base_sha = rev.stdout.strip()
    exists, base_manifest = _read_json_at_rev(root, base_sha, DIST_MANIFEST_REL)
    if not exists:
        return True, f"no prior dist manifest at --base-ref {base_sha[:12]}"
    base_ver = _extract_manifest_version(base_manifest)
    base_tuple = _parse_semver(base_ver) if base_ver else None
    if base_tuple is None:
        raise ValueError(
            f"unparseable dist manifest version at --base-ref {base_sha[:12]}: {base_ver!r}"
        )
    dist_tuple = _parse_semver(dist_version)
    assert dist_tuple is not None
    if base_tuple < dist_tuple:
        return True, f"--base-ref {base_sha[:12]} has dist {base_ver} < {dist_version}"
    return (
        False,
        f"--base-ref {base_sha[:12]} already has dist {base_ver} (not < {dist_version})",
    )


def _collect_served_commits(root: Path) -> dict[str, str]:
    """Return {version: commit_sha} from first-parent history of dist + catalog.

    For each X.Y.Z dist version's most recent contiguous run, prefers the
    introducing commit where both dist and catalog equal that version, falling
    back to the introducing commit where dist equals that version.
    """
    log = _run_git(
        root,
        "log",
        "--first-parent",
        "--format=%H",
        "--",
        DIST_MANIFEST_REL,
        CATALOG_REL,
    )
    if log.returncode != 0:
        raise RuntimeError(
            f"git log --first-parent failed: {log.stderr.strip() or log.returncode}"
        )
    both_matched: dict[str, str] = {}
    dist_only: dict[str, str] = {}
    closed_versions: set[str] = set()
    active_ver: str | None = None
    for raw_sha in log.stdout.splitlines():
        sha = raw_sha.strip()
        if not sha:
            continue
        m_exists, manifest = _read_json_at_rev(root, sha, DIST_MANIFEST_REL)
        if not m_exists:
            continue
        ver = _extract_manifest_version(manifest)
        if not ver or _parse_semver(ver) is None:
            continue
        if active_ver is not None and ver != active_ver:
            closed_versions.add(active_ver)
        active_ver = ver
        if ver in closed_versions:
            continue
        dist_only[ver] = sha
        c_exists, catalog = _read_json_at_rev(root, sha, CATALOG_REL)
        if c_exists and _extract_catalog_version(catalog) == ver:
            both_matched[ver] = sha
    return {ver: (both_matched.get(ver) or sha) for ver, sha in dist_only.items()}


def _valid_repo_slug(repo: str) -> bool:
    if not REPO_RE.match(repo):
        return False
    owner, name = repo.split("/", 1)
    return owner not in (".", "..") and name not in (".", "..")


def parse_args(argv: list[str]) -> argparse.Namespace:
    default_repo = (
        os.environ.get("BQAA_MOD_GITHUB_REPO")
        or os.environ.get("GITHUB_REPOSITORY")
        or DEFAULT_REPO
    )
    parser = argparse.ArgumentParser(
        description="Verify mod marketplace copy and mod-dist-v* tags stay in sync."
    )
    parser.add_argument(
        "repo_root",
        nargs="?",
        default=str(Path(__file__).resolve().parent.parent),
        help="Path to git repository root (defaults to parent of scripts/).",
    )
    parser.add_argument(
        "--repo",
        default=default_repo,
        help=f"GitHub owner/repo slug (default: {DEFAULT_REPO}).",
    )
    parser.add_argument(
        "--base-ref",
        default=None,
        help="Optional base git ref for an in-flight sync PR or merge push.",
    )
    parser.add_argument(
        "--grace-minutes",
        type=int,
        default=0,
        help="Optional non-negative sync/post-merge grace window in minutes (default: 0).",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip GitHub Releases API check (rejected when CI=true).",
    )
    parser.add_argument(
        "--releases-json",
        default=None,
        help="Path to a local JSON file with GitHub releases payload (for testing).",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if args.grace_minutes < 0:
        print(
            f"not ok - --grace-minutes must be non-negative (got {args.grace_minutes})",
            file=sys.stderr,
        )
        return 2

    offline = args.offline or os.environ.get("BQAA_OFFLINE") == "1"
    if offline and os.environ.get("CI", "").lower() == "true":
        print(
            "not ok - --offline / BQAA_OFFLINE=1 is not permitted when CI=true",
            file=sys.stderr,
        )
        return 1

    if not _valid_repo_slug(args.repo):
        print(
            f"not ok - invalid --repo {args.repo!r}: expected owner/name",
            file=sys.stderr,
        )
        return 2

    root = Path(args.repo_root).resolve()
    if _run_git(root, "rev-parse", "--git-dir").returncode != 0:
        print(f"not ok - {root} is not a git repository", file=sys.stderr)
        return 2

    shallow = _run_git(root, "rev-parse", "--is-shallow-repository")
    if shallow.returncode != 0 or shallow.stdout.strip() == "true":
        print(
            "not ok - this checkout is shallow, so release and mod-dist-v* history "
            "cannot be verified: fetch full history (fetch-depth: 0)"
        )
        return 1

    problems: list[str] = []

    dirty = _run_git(root, "status", "--porcelain", "--", DIST_DIR_REL, CATALOG_REL)
    if dirty.returncode != 0 or dirty.stdout.strip():
        problems.append(
            f"uncommitted changes in {DIST_DIR_REL} or {CATALOG_REL}; "
            "commit them before verifying release sync"
        )

    dist_exists, dist_data = _read_json_at_rev(root, "HEAD", DIST_MANIFEST_REL)
    if not dist_exists or dist_data is None:
        print(f"not ok - cannot read {DIST_MANIFEST_REL} at HEAD", file=sys.stderr)
        return 1
    dist_version = _extract_manifest_version(dist_data)

    cat_exists, cat_data = _read_json_at_rev(root, "HEAD", CATALOG_REL)
    if not cat_exists or cat_data is None:
        print(f"not ok - cannot read {CATALOG_REL} at HEAD", file=sys.stderr)
        return 1
    catalog_version = _extract_catalog_version(cat_data)

    if not dist_version or _parse_semver(dist_version) is None:
        problems.append(
            f"committed copy version {dist_version!r} in {DIST_MANIFEST_REL} is not X.Y.Z"
        )
    if not catalog_version or _parse_semver(catalog_version) is None:
        problems.append(
            f"catalog version {catalog_version!r} in {CATALOG_REL} is not X.Y.Z"
        )
    if dist_version and catalog_version and dist_version != catalog_version:
        problems.append(
            f"catalog entry version {catalog_version} does not match committed copy {dist_version}"
        )

    if args.base_ref is not None:
        if not dist_version or _parse_semver(dist_version) is None:
            problems.append("cannot evaluate --base-ref without valid dist_version")
            base_allows_missing_current = False
            base_note = "invalid dist_version"
        else:
            try:
                base_allows_missing_current, base_note = _check_base_ref(
                    root, args.base_ref, dist_version
                )
            except ValueError as exc:
                print(f"not ok - {exc}", file=sys.stderr)
                return 2
    else:
        base_allows_missing_current = False
        base_note = "no --base-ref supplied"

    mod_v_list = _run_git(root, "tag", "-l", "mod-v*")
    if mod_v_list.returncode != 0:
        print(f"not ok - git tag failed: {mod_v_list.stderr.strip()}", file=sys.stderr)
        return 2
    mod_v_tags = [line.strip() for line in mod_v_list.stdout.splitlines() if line.strip()]

    tag_list = _run_git(root, "tag", "-l", "mod-dist-v*")
    if tag_list.returncode != 0:
        print(f"not ok - git tag failed: {tag_list.stderr.strip()}", file=sys.stderr)
        return 2

    valid_dist_tags: dict[str, str] = {}
    for raw_tag in tag_list.stdout.splitlines():
        tag = raw_tag.strip()
        if not tag:
            continue
        match = MOD_DIST_TAG_RE.match(tag)
        if not match:
            problems.append(f"existing tag {tag!r} does not match mod-dist-vX.Y.Z")
            continue
        ver = ".".join(match.groups())
        tag_rev = _run_git(
            root, "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"
        )
        if tag_rev.returncode != 0:
            problems.append(f"tag {tag} does not resolve to a commit")
            continue
        tag_sha = tag_rev.stdout.strip()
        anc = _run_git(root, "merge-base", "--is-ancestor", tag_sha, "HEAD")
        if anc.returncode != 0:
            problems.append(f"tag {tag} ({tag_sha[:12]}) is not an ancestor of HEAD")
            continue
        m_exists, m_data = _read_json_at_rev(root, tag_sha, DIST_MANIFEST_REL)
        c_exists, c_data = _read_json_at_rev(root, tag_sha, CATALOG_REL)
        at_copy = _extract_manifest_version(m_data) if m_exists else None
        at_catalog = _extract_catalog_version(c_data) if c_exists else None
        if at_copy != ver or at_catalog != ver:
            problems.append(
                f"{tag} holds copy={at_copy!r} and catalog={at_catalog!r}, expected {ver!r}"
            )
            continue
        valid_dist_tags[ver] = tag_sha

    try:
        served_commits = _collect_served_commits(root)
    except RuntimeError as exc:
        print(f"not ok - {exc}", file=sys.stderr)
        return 1

    dist_tuple = _parse_semver(dist_version) if dist_version else None
    prior_served: dict[str, str] = {}
    if dist_tuple is not None:
        for ver, sha in served_commits.items():
            ver_tuple = _parse_semver(ver)
            if ver_tuple is not None and ver_tuple < dist_tuple:
                prior_served[ver] = sha

    enforce_prior_tags = (
        bool(mod_v_tags)
        or bool(valid_dist_tags)
        or args.repo.lower() == DEFAULT_REPO.lower()
    )
    if enforce_prior_tags:
        for ver in sorted(prior_served, key=lambda v: _parse_semver(v) or (0, 0, 0)):
            if ver not in valid_dist_tags:
                sha = prior_served[ver]
                problems.append(
                    f"missing or invalid tag mod-dist-v{ver} for previously served dist commit "
                    f"{sha}; run: git tag -a mod-dist-v{ver} -m \"mod {ver}, served copy\" {sha}"
                )

    if offline:
        if problems:
            for msg in problems:
                print(f"not ok - {msg}")
            return 1
        print(
            f"ok - offline checks passed for dist {dist_version} "
            "(skipped GitHub Releases API check)"
        )
        return 0

    try:
        now_epoch = _now_epoch()
    except ValueError as exc:
        print(f"not ok - {exc}", file=sys.stderr)
        return 2

    # Online (or --releases-json) check.
    try:
        if args.releases_json is not None:
            raw_items = json.loads(Path(args.releases_json).read_text(encoding="utf-8"))
            if not isinstance(raw_items, list):
                raise RuntimeError("releases JSON must be a list")
        else:
            api_url = (
                os.environ.get("BQAA_MOD_RELEASES_API_URL")
                or f"https://api.github.com/repos/{args.repo}/releases?per_page=100"
            )
            token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or None
            raw_items = _fetch_releases(api_url, token)
        published_releases = _parse_published_mod_versions(raw_items)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"not ok - {exc}", file=sys.stderr)
        return 1

    if not published_releases:
        if args.repo.lower() == DEFAULT_REPO.lower() and (
            bool(prior_served) or bool(valid_dist_tags) or len(mod_v_tags) > 1
        ):
            problems.append(
                f"no published mod-v* releases found for {args.repo}, "
                f"expected at least dist {dist_version}"
            )
        else:
            if problems:
                for msg in problems:
                    print(f"not ok - {msg}")
                return 1
            print(
                f"ok - no published mod-v* releases found for {args.repo} "
                f"(bootstrap/fork mode; local dist={dist_version})"
            )
            return 0

    if published_releases and dist_version and dist_tuple is not None:
        max_published = max(
            published_releases, key=lambda v: _parse_semver(v) or (0, 0, 0)
        )
        pub_tuple = _parse_semver(max_published)
        assert pub_tuple is not None
        if dist_tuple < pub_tuple:
            grace_ok = False
            if args.grace_minutes > 0:
                pub_ts = _parse_iso_utc_epoch(published_releases.get(max_published))
                if pub_ts is not None:
                    delta = now_epoch - pub_ts
                    if 0 <= delta < args.grace_minutes * 60:
                        grace_ok = True
                        print(
                            f"ok - published release mod-v{max_published} is within "
                            f"{args.grace_minutes}m sync grace window ({int(delta)}s elapsed)"
                        )
            if not grace_ok:
                problems.append(
                    f"marketplace copy is at {dist_version}, behind latest published release "
                    f"mod-v{max_published}; run: bash plugins/claude_code_mod/sync_dist.sh "
                    f"{max_published} and update .claude-plugin/marketplace.json"
                )
        elif dist_tuple > pub_tuple:
            problems.append(
                f"marketplace copy is at {dist_version}, ahead of latest published release "
                f"mod-v{max_published}"
            )
        else:
            print(
                f"ok - marketplace copy and catalog ({dist_version}) match latest "
                f"published release mod-v{max_published}"
            )

        if dist_version not in valid_dist_tags:
            dist_intro_sha = (
                served_commits.get(dist_version)
                or _run_git(root, "rev-parse", "HEAD").stdout.strip()
            )
            if base_allows_missing_current:
                print(
                    f"ok - mod-dist-v{dist_version} not yet tagged (allowed: {base_note}; "
                    f"tag merge commit after merge)"
                )
            else:
                grace_ok = False
                if args.grace_minutes > 0:
                    ct_res = _run_git(root, "show", "-s", "--format=%ct", dist_intro_sha)
                    if ct_res.returncode == 0:
                        try:
                            intro_ts = int(ct_res.stdout.strip())
                        except ValueError:
                            intro_ts = None
                        if intro_ts is not None:
                            delta = now_epoch - intro_ts
                            if 0 <= delta < args.grace_minutes * 60:
                                grace_ok = True
                                print(
                                    f"ok - mod-dist-v{dist_version} not yet tagged "
                                    f"(within {args.grace_minutes}m post-merge grace window "
                                    f"for commit {dist_intro_sha}; {int(delta)}s elapsed)"
                                )
                if not grace_ok:
                    problems.append(
                        f"missing or invalid tag mod-dist-v{dist_version} ({base_note}); "
                        f"run: git tag -a mod-dist-v{dist_version} -m \"mod {dist_version}, "
                        f"served copy\" {dist_intro_sha}"
                    )
        else:
            print(f"ok - mod-dist-v{dist_version} tag exists and matches HEAD history")

    if problems:
        for msg in problems:
            print(f"not ok - {msg}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
