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
  - a shell script does not pass bash -n.

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
    json_files = check_json_files(root, files, problems)
    md_files, links = check_markdown(root, files, problems)
    text_files = check_stale_references(root, files, problems)
    scripts = check_shell(root, files, problems)
    for path, line, message in sorted(problems):
        print(f"{path}:{line}: {message}" if line else f"{path}: {message}")
    summary = (f"{entries} catalog entries, {json_files} JSON files, {links} relative links in "
               f"{md_files} Markdown files, {text_files} text files, {scripts} shell scripts")
    if problems:
        print(f"FAIL - {len(problems)} problem(s) ({summary})")
        return 1
    print(f"ok - {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
