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

"""Checks the things in this repository that break quietly.

    python3 scripts/check_repo_hygiene.py [REPO_ROOT]

It looks at every file git knows about (tracked, plus untracked files that
are not ignored, so a local run sees new files before they are staged) and
fails, listing each problem as path:line: message, when:

  - .claude-plugin/marketplace.json does not parse, a plugin entry's source
    is not a folder in this repository holding .claude-plugin/plugin.json,
    or that plugin.json and the entry do not both give name, version,
    license, homepage and repository, and agree on each; or any other
    tracked .json file does not parse;
  - a relative link in a Markdown file (inline, image or reference
    definition) points at a path that does not exist, leaves the
    repository, or names a heading anchor the target file does not have;
  - a file other than a MIGRATION.md still refers to the personal
    repository the mod was first published from
    (haiyuan-eng-google/bigquery_agent_analytics_skill): everything the
    plugins need now ships in this repository;
  - a shell script does not pass bash -n;
  - a git-known file has a case-insensitive basename in {intent.md,
    plan.md, spec.md}: working notes that must not be committed;
  - a source, script or workflow file (.py, .sh, .ts, .mjs, .js, .sql,
    .github/workflows/*.yml) lacks the Apache-2.0 license header
    (Copyright and "Licensed under the Apache License, Version 2.0").

Only the standard library is used, so the check runs the same on a laptop
and in the repo-ci workflow. Exit status: 0 when clean, 1 when a problem was
found, 2 when the check itself could not run.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

CATALOG = Path(".claude-plugin/marketplace.json")
MANIFEST = Path(".claude-plugin/plugin.json")
# Fields a catalog entry and the plugin's own manifest must agree on.
SHARED_FIELDS = ("name", "version", "license", "homepage", "repository")
# References that must not survive outside the migration notes.
STALE_REFERENCES = ("haiyuan-eng-google", "bigquery_agent_analytics_skill")
STALE_ALLOWED_BASENAMES = {"MIGRATION.md"}
# Working-note files that must not be committed.
WORKING_NOTE_BASENAMES = {"intent.md", "plan.md", "spec.md"}
# Extensions and workflow paths whose files need the Apache-2.0 header.
LICENSE_HEADER_SUFFIXES = {".py", ".sh", ".ts", ".mjs", ".js", ".sql"}
LICENSE_HEADER_NEEDED = ("Copyright", "Licensed under the Apache License, Version 2.0")
TEXT_SUFFIXES = {
    ".md", ".sh", ".py", ".ts", ".js", ".json", ".sql", ".yml", ".yaml",
    ".txt", ".toml", ".cfg", ".ini", ".d.ts",
}

Problem = tuple[str, int, str]


def list_files(root: Path) -> list[Path]:
    """Files git knows about: tracked, plus untracked ones that are not ignored."""
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others",
             "--exclude-standard"],
            check=True, capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        sys.exit(f"cannot list the repository's files with git: {exc}")
    files = []
    for raw in out.split(b"\0"):
        if not raw:
            continue
        path = root / os.fsdecode(raw)
        if path.is_file():  # deleted-but-still-indexed files are skipped
            files.append(path)
    return sorted(set(files))


def rel(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


# --- JSON manifests and the marketplace catalog -----------------------------

def load_json(root: Path, path: Path, problems: list[Problem]):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        line = getattr(exc, "lineno", 0) or 0
        problems.append((rel(root, path), line, f"invalid JSON: {exc}"))
        return None


def check_json_files(root: Path, files: list[Path], problems: list[Problem]) -> int:
    count = 0
    for path in files:
        if path.suffix == ".json":
            count += 1
            load_json(root, path, problems)
    return count


def check_catalog(root: Path, problems: list[Problem]) -> int:
    catalog_path = root / CATALOG
    if not catalog_path.is_file():
        problems.append((CATALOG.as_posix(), 0, "missing marketplace catalog"))
        return 0
    catalog = load_json(root, catalog_path, problems)
    if catalog is None:
        return 0
    where = CATALOG.as_posix()
    entries = catalog.get("plugins") if isinstance(catalog, dict) else None
    if not isinstance(entries, list) or not entries:
        problems.append((where, 0, "the catalog lists no plugins"))
        return 0
    seen: set[str] = set()
    for index, entry in enumerate(entries):
        label = f"plugins[{index}]"
        if not isinstance(entry, dict):
            problems.append((where, 0, f"{label} is not an object"))
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            problems.append((where, 0, f"{label} has no name"))
            continue
        label = f"plugin {name!r}"
        if name in seen:
            problems.append((where, 0, f"{label} is listed twice"))
        seen.add(name)
        source = entry.get("source")
        if not isinstance(source, str) or not source.startswith("./"):
            problems.append((where, 0, f"{label}: source must be a ./ path in this repository"))
            continue
        plugin_dir = (root / source).resolve()
        if root.resolve() not in plugin_dir.parents or not plugin_dir.is_dir():
            problems.append((where, 0, f"{label}: source {source} is not a folder in this repository"))
            continue
        manifest_path = plugin_dir / MANIFEST
        if not manifest_path.is_file():
            problems.append((where, 0, f"{label}: {source} has no {MANIFEST.as_posix()}"))
            continue
        manifest = load_json(root, manifest_path, problems)
        if not isinstance(manifest, dict):
            continue
        for field in SHARED_FIELDS:
            absent = False
            if field not in entry:
                problems.append((
                    where, 0,
                    f"{label}: {field} is missing; it must match {rel(root, manifest_path)}",
                ))
                absent = True
            if field not in manifest:
                problems.append((
                    rel(root, manifest_path), 0,
                    f"{field} is missing; it must match the catalog entry for {name!r}",
                ))
                absent = True
            if absent:
                continue
            if entry[field] != manifest[field]:
                problems.append((
                    rel(root, manifest_path), 0,
                    f"{field} is {manifest[field]!r} but the catalog entry for "
                    f"{name!r} says {entry[field]!r}",
                ))
    return len(entries)


TRACING_SRC_REL = Path("plugins/claude_code")
TRACING_DIST_REL = Path("plugins/claude_code_dist/bigquery-agent-analytics-tracing")
TRACING_SHARED_FILES = ("MARKETPLACE.md", "commands/bqaa-setup.md", "scripts/run_setup_check.sh")


def check_tracing_sync(root: Path, problems: list[Problem]) -> int:
    """Keep plugins/claude_code and the tracing marketplace copy in sync."""
    src_dir = root / TRACING_SRC_REL
    dist_dir = root / TRACING_DIST_REL
    if not src_dir.is_dir() or not dist_dir.is_dir():
        return 0
    checks = 0
    src_manifest_path = src_dir / MANIFEST
    dist_manifest_path = dist_dir / MANIFEST
    src_manifest = load_json(root, src_manifest_path, problems) if src_manifest_path.is_file() else None
    dist_manifest = load_json(root, dist_manifest_path, problems) if dist_manifest_path.is_file() else None
    if isinstance(src_manifest, dict) and isinstance(dist_manifest, dict):
        checks += 1
        for key in sorted(set(src_manifest) | set(dist_manifest)):
            if key == "version":
                continue
            if src_manifest.get(key) != dist_manifest.get(key):
                problems.append((
                    rel(root, dist_manifest_path), 0,
                    f"{key} differs from {rel(root, src_manifest_path)}",
                ))
        dist_ver = dist_manifest.get("version")
        vendor_dir = dist_dir / "vendor"
        if isinstance(dist_ver, str) and vendor_dir.is_dir():
            checks += 1
            infos = sorted(vendor_dir.glob("bigquery_agent_analytics_tracing-*.dist-info"))
            expected_info = vendor_dir / f"bigquery_agent_analytics_tracing-{dist_ver}.dist-info"
            if len(infos) != 1 or infos[0] != expected_info:
                problems.append((
                    rel(root, vendor_dir), 0,
                    f"expected {expected_info.name}, found {[p.name for p in infos]}",
                ))
            else:
                meta = expected_info / "METADATA"
                if not meta.is_file():
                    problems.append((rel(root, meta), 0, "missing METADATA"))
                elif f"Version: {dist_ver}" not in meta.read_text(encoding="utf-8").splitlines():
                    problems.append((rel(root, meta), 0, f"METADATA lacks Version: {dist_ver}"))
    src_hooks = src_dir / "hooks"
    dist_hooks = dist_dir / "hooks"
    if src_hooks.is_dir() and dist_hooks.is_dir():
        checks += 1
        src_sh = {p.name for p in src_hooks.glob("*.sh")}
        dist_sh = {p.name for p in dist_hooks.glob("*.sh")}
        for missing in sorted(src_sh - dist_sh):
            problems.append((rel(root, dist_hooks), 0, f"missing hook script {missing} from {rel(root, src_hooks)}"))
        for extra in sorted(dist_sh - src_sh):
            problems.append((rel(root, dist_hooks), 0, f"extra hook script {extra} not in {rel(root, src_hooks)}"))
        for name in sorted(src_sh & dist_sh):
            s_path, d_path = src_hooks / name, dist_hooks / name
            if s_path.read_bytes() != d_path.read_bytes() or (s_path.stat().st_mode & 0o111) != (d_path.stat().st_mode & 0o111):
                problems.append((rel(root, d_path), 0, f"hook {name} differs from {rel(root, s_path)}"))
    for rel_file in TRACING_SHARED_FILES:
        s_path, d_path = src_dir / rel_file, dist_dir / rel_file
        if s_path.is_file() != d_path.is_file():
            checks += 1
            missing = d_path if s_path.is_file() else s_path
            problems.append((rel(root, missing), 0, f"missing counterpart for {rel_file}"))
        elif s_path.is_file() and d_path.is_file():
            checks += 1
            if s_path.read_bytes() != d_path.read_bytes() or (s_path.stat().st_mode & 0o111) != (d_path.stat().st_mode & 0o111):
                problems.append((rel(root, d_path), 0, f"differs from {rel(root, s_path)}"))
    src_readme = src_dir / "README.md"
    dist_readme = dist_dir / "README.md"
    if src_readme.is_file() != dist_readme.is_file():
        checks += 1
        missing = dist_readme if src_readme.is_file() else src_readme
        problems.append((rel(root, missing), 0, "missing counterpart for README.md"))
    elif src_readme.is_file() and dist_readme.is_file():
        checks += 1
        expected_dist_readme = src_readme.read_text(encoding="utf-8").replace(
            "(../../.claude-plugin/marketplace.json)",
            "(../../../.claude-plugin/marketplace.json)",
        )
        if dist_readme.read_text(encoding="utf-8") != expected_dist_readme:
            problems.append((rel(root, dist_readme), 0, f"differs from {rel(root, src_readme)}"))
    return checks


# --- Markdown links ---------------------------------------------------------

FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
CODE_SPAN_RE = re.compile(r"(`+)[\s\S]*?\1")
HTML_COMMENT_RE = re.compile(r"<!--[\s\S]*?-->")
# [text](dest "title") and ![alt](dest); dest may be <...> or run to the
# first space/closing paren, allowing one level of balanced parentheses.
INLINE_LINK_RE = re.compile(
    r"!?\[((?:[^\[\]]|\[[^\[\]]*\])*)\]"
    r"\(\s*(<[^>\n]*>|(?:[^\s()]|\([^\s()]*\))*)"
    r"(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?\s*\)"
)
REFERENCE_DEF_RE = re.compile(r"^ {0,3}\[([^\]]+)\]:[ \t]*(<[^>\n]*>|\S+)", re.MULTILINE)
ATX_HEADING_RE = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.*?)(?:[ \t]+#+)?[ \t]*$")
SETEXT_UNDERLINE_RE = re.compile(r"^ {0,3}(=+|-+)[ \t]*$")
HTML_ANCHOR_RE = re.compile(r"<[^>]+\b(?:id|name)=[\"']([^\"']+)[\"']", re.IGNORECASE)
SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def blank_out(match: re.Match) -> str:
    """Replace a span with spaces, keeping its newlines so line numbers hold."""
    return re.sub(r"[^\n]", " ", match.group(0))


def strip_blocks(text: str) -> str:
    """Blank fenced code blocks and HTML comments."""
    lines = text.split("\n")
    fence = None
    for i, line in enumerate(lines):
        match = FENCE_RE.match(line)
        if fence is None:
            if match:
                fence = match.group(1)[0]
                lines[i] = ""
        else:
            if match and match.group(1)[0] == fence and len(match.group(1)) >= 3:
                fence = None
            lines[i] = ""
    return HTML_COMMENT_RE.sub(blank_out, "\n".join(lines))


def strip_code(text: str) -> str:
    """Blank fenced code blocks, HTML comments and inline code spans."""
    return CODE_SPAN_RE.sub(blank_out, strip_blocks(text))


def slugify(heading: str) -> str:
    """The anchor GitHub gives a heading (github-slugger's rules)."""
    # A code span keeps its text as written (even a <placeholder>), so hide it
    # while the markup around it is removed. Its backticks never reach the slug.
    spans: list[str] = []

    def hide(match: re.Match) -> str:
        n = len(match.group(1))
        spans.append(match.group(0)[n:-n])
        return f"\0{len(spans) - 1}\0"

    text = CODE_SPAN_RE.sub(hide, heading.replace("\0", ""))
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)  # link text only
    text = re.sub(r"\0(\d+)\0", lambda match: spans[int(match.group(1))], text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors_of(text: str) -> set[str]:
    """Every anchor a Markdown document offers, as GitHub renders it."""
    # Only blocks go: a heading's own code spans are part of its anchor.
    stripped = strip_blocks(text)
    lines = stripped.split("\n")
    headings = []
    for i, line in enumerate(lines):
        match = ATX_HEADING_RE.match(line)
        if match:
            headings.append(match.group(2))
            continue
        if i > 0 and SETEXT_UNDERLINE_RE.match(line):
            previous = lines[i - 1].strip()
            if previous and not previous.startswith(("-", "*", "+", "#", ">", "|")):
                headings.append(previous)
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for heading in headings:
        # Emphasis markers vanish when the heading is rendered.
        slug = slugify(heading.replace("*", ""))
        n = counts.get(slug, 0)
        counts[slug] = n + 1
        anchors.add(slug if n == 0 else f"{slug}-{n}")
    for match in HTML_ANCHOR_RE.finditer(text):
        anchors.add(match.group(1))
    return anchors


def link_targets(text: str):
    """(line, destination) for each inline link, image and reference definition."""
    stripped = strip_code(text)
    for regex, group in ((INLINE_LINK_RE, 2), (REFERENCE_DEF_RE, 2)):
        for match in regex.finditer(stripped):
            dest = match.group(group).strip()
            if dest.startswith("<") and dest.endswith(">"):
                dest = dest[1:-1].strip()
            line = stripped.count("\n", 0, match.start()) + 1
            yield line, dest


def check_markdown(root: Path, files: list[Path], problems: list[Problem]) -> tuple[int, int]:
    md_files = [p for p in files if p.suffix.lower() == ".md"]
    anchor_cache: dict[Path, set[str]] = {}

    def anchors(path: Path) -> set[str]:
        if path not in anchor_cache:
            anchor_cache[path] = anchors_of(path.read_text(encoding="utf-8", errors="replace"))
        return anchor_cache[path]

    links = 0
    for md in md_files:
        text = md.read_text(encoding="utf-8", errors="replace")
        here = rel(root, md)
        for line, dest in link_targets(text):
            if not dest or SCHEME_RE.match(dest) or dest.startswith("//"):
                continue  # absolute URLs are not checked (no network here)
            links += 1
            path_part, _, fragment = dest.partition("#")
            path_part = unquote(path_part.split("?", 1)[0])
            if path_part.startswith("/"):
                problems.append((here, line, f"link {dest!r} is an absolute path; use a relative one"))
                continue
            target = md if not path_part else (md.parent / path_part).resolve()
            if root.resolve() != target and root.resolve() not in target.parents:
                problems.append((here, line, f"link {dest!r} leaves the repository"))
                continue
            if not target.exists():
                problems.append((here, line, f"broken link {dest!r}: no such file"))
                continue
            if fragment and target.is_file() and target.suffix.lower() == ".md":
                if unquote(fragment).lower() not in anchors(target):
                    problems.append((here, line, f"link {dest!r}: {rel(root, target)} has no heading #{fragment}"))
    return len(md_files), links


# --- Stale references -------------------------------------------------------

def check_stale_references(root: Path, files: list[Path], problems: list[Problem]) -> int:
    this_file = Path(__file__).resolve()
    checked = 0
    for path in files:
        if path.resolve() == this_file or path.name in STALE_ALLOWED_BASENAMES:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES and not path.name.startswith("."):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        checked += 1
        for number, line in enumerate(text.split("\n"), 1):
            for needle in STALE_REFERENCES:
                if needle in line:
                    problems.append((rel(root, path), number,
                                     f"refers to {needle}; point at this repository's own files"))
                    break
    return checked


# --- Shell syntax -----------------------------------------------------------

def check_shell(root: Path, files: list[Path], problems: list[Problem]) -> int:
    scripts = [p for p in files if p.suffix == ".sh"]
    for path in scripts:
        result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip().split("\n")[0]
            problems.append((rel(root, path), 0, f"bash -n failed: {detail}"))
    return len(scripts)


# --- Working notes -----------------------------------------------------------

def check_working_notes(root: Path, files: list[Path], problems: list[Problem]) -> int:
    """Fail on committed working-note files (intent.md, plan.md, spec.md)."""
    checked = 0
    for path in files:
        if path.name.lower() in WORKING_NOTE_BASENAMES:
            checked += 1
            problems.append((
                rel(root, path), 0,
                f"{path.name} is a working note and must not be committed",
            ))
    return checked


# --- License headers ---------------------------------------------------------

def _needs_license_header(root: Path, path: Path) -> bool:
    """True for source/script/workflow files that must carry the Apache header."""
    rel_path = path.relative_to(root)
    # GitHub Actions workflows under .github/workflows/*.yml and *.yaml.
    if rel_path.parts[:2] == (".github", "workflows") and path.suffix in (".yml", ".yaml"):
        return True
    return path.suffix in LICENSE_HEADER_SUFFIXES


def check_license_headers(root: Path, files: list[Path], problems: list[Problem]) -> int:
    """Fail if a source/script/workflow file lacks the Apache-2.0 header."""
    checked = 0
    for path in files:
        if not _needs_license_header(root, path):
            continue
        checked += 1
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        # The header is expected in the first 25 lines.
        header = "\n".join(text.split("\n")[:25])
        for needle in LICENSE_HEADER_NEEDED:
            if needle not in header:
                problems.append((
                    rel(root, path), 1,
                    f"missing Apache-2.0 license header ({needle!r} not found"
                    f" in the first 25 lines)",
                ))
                break
    return checked


def main(argv: list[str]) -> int:
    if argv[1:] in (["-h"], ["--help"]):
        print(__doc__.strip())
        return 0
    if len(argv) > 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    root = Path(argv[1] if len(argv) == 2 else Path(__file__).resolve().parent.parent).resolve()
    if not (root / ".git").exists() and not (root / CATALOG).exists():
        print(f"{root} does not look like the repository root", file=sys.stderr)
        return 2
    files = list_files(root)
    problems: list[Problem] = []
    entries = check_catalog(root, problems)
    check_tracing_sync(root, problems)
    json_files = check_json_files(root, files, problems)
    md_files, links = check_markdown(root, files, problems)
    text_files = check_stale_references(root, files, problems)
    scripts = check_shell(root, files, problems)
    working_notes = check_working_notes(root, files, problems)
    licensed = check_license_headers(root, files, problems)
    for path, line, message in sorted(problems):
        print(f"{path}:{line}: {message}" if line else f"{path}: {message}")
    summary = (
        f"{entries} catalog entries, {json_files} JSON files, {links} relative"
        f" links in {md_files} Markdown files, {text_files} text files,"
        f" {scripts} shell scripts, {working_notes} working notes,"
        f" {licensed} license-checked files"
    )
    if problems:
        print(f"FAIL - {len(problems)} problem(s) ({summary})")
        return 1
    print(f"ok - {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
