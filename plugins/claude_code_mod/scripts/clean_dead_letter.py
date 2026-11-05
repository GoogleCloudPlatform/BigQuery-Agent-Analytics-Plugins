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

"""Remove dead-letter files that no longer guard any rows.

The mod writes unsent rows to ``spill-*.jsonl``, ``retry-*.jsonl`` and
``rejected-*.jsonl`` under ``~/.bqaa-mod/dead-letter/<project.dataset.table>/``.
A replay that finishes a source file writes ``outcome-*`` files beside it,
each ending with a completion record (``{"v":1,"complete":true,...,"parent":...}``)
that names the source file it came from in ``parent``.

This helper removes a ``(source, outcome-*)`` group **only** when:

- the source file (``spill-*``, ``retry-*`` or ``rejected-*``) is **0 bytes**,
  and
- **no** non-empty file in the directory names that source as ``parent`` in
  its completion record.

It never deletes a non-empty source file, or an ``outcome-*`` file whose
source is non-empty, unreadable, or missing.

Run this **only while no session is running**: it cannot tell a live
writer's marks from stale ones. Dry-run by default; pass ``--execute`` to
delete.

Usage:
  python3 scripts/clean_dead_letter.py [--dir DIR] [--execute]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

DEFAULT_DIR = Path.home() / ".bqaa-mod" / "dead-letter"
SOURCE_PREFIXES = ("spill-", "retry-", "rejected-")
MARKED_RE = re.compile(r"^outcome-(?:sending|done)-[0-9a-z]+-(.+)$")


def record_of(name: str) -> str | None:
    """Return the source filename for an ``outcome-*`` record file, or None."""
    m = MARKED_RE.match(name)
    if m is not None:
        return m.group(1)
    for prefix in ("outcome-b-", "outcome-"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return None


def _read_completion_parent(path: Path) -> str | None:
    """Return the ``parent`` field from the last line of ``path``, or None."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return None
            # Read the last 4 KB to find the completion record.
            chunk = min(size, 4096)
            f.seek(-chunk, os.SEEK_END)
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("complete") is True:
            parent = record.get("parent")
            if isinstance(parent, str):
                return parent
    return None


def _is_source(name: str) -> bool:
    return any(name.startswith(p) for p in SOURCE_PREFIXES)


def _plan_single_dir(directory: Path) -> list[Path]:
    entries = sorted(directory.iterdir())
    sources: dict[str, Path] = {}
    outcomes_by_source: dict[str, list[Path]] = {}
    non_empty_parents: set[str] = set()

    for entry in entries:
        if entry.is_symlink() or not entry.is_file():
            continue
        name = entry.name
        if _is_source(name):
            sources[name] = entry
            try:
                size = entry.stat().st_size
            except OSError:
                non_empty_parents.add(name)
                continue
            if size > 0:
                parent = _read_completion_parent(entry)
                if parent is not None:
                    non_empty_parents.add(parent)
            continue
        of = record_of(name)
        if of is not None:
            outcomes_by_source.setdefault(of, []).append(entry)

    deletable: list[Path] = []
    for name, path in sources.items():
        try:
            is_empty = path.stat().st_size == 0
        except OSError:
            is_empty = False
        if is_empty and name not in non_empty_parents:
            deletable.append(path)
            deletable.extend(outcomes_by_source.get(name, []))

    return deletable


def plan_deletions(directory: Path) -> list[Path]:
    """Return the list of files that may be deleted from ``directory``.

    Scans ``directory`` and its immediate ``<project.dataset.table>``
    subdirectories. A source file and all its ``outcome-*`` files are deletable
    when the source is empty (0 bytes) and no non-empty leftover file names it
    as ``parent``.
    """
    if not directory.is_dir():
        return []

    dirs = [directory, *[d for d in sorted(directory.iterdir()) if d.is_dir() and not d.is_symlink()]]
    deletable: list[Path] = []
    for d in dirs:
        deletable.extend(_plan_single_dir(d))
    return sorted(set(deletable))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove dead-letter files that no longer guard any rows.")
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR,
                        help=f"dead-letter directory (default: {DEFAULT_DIR})")
    parser.add_argument("--execute", action="store_true",
                        help="delete the files; without it, only report them.")
    args = parser.parse_args(argv)

    base = args.dir
    if not base.is_dir():
        print(f"dead-letter directory not found: {base}")
        return 0

    to_delete = plan_deletions(base)
    if not to_delete:
        print("nothing to clean")
        return 0

    action = "deleted" if args.execute else "would delete"
    for path in to_delete:
        rel = path.relative_to(base)
        print(f"{action}: {rel}")
        if args.execute:
            try:
                path.unlink()
            except OSError as exc:
                print(f"  ERROR: {exc}", file=sys.stderr)

    print(f"{len(to_delete)} file(s) {action}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
