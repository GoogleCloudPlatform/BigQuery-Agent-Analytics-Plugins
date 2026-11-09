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
source is non-empty, unreadable, or missing. It refuses a dead-letter path
when the directory or any non-root ancestor is a symbolic link, exits 1 on
stderr if the root dead-letter directory cannot be listed, and deletes nothing
in a partition directory whose entries cannot all be listed and read, or that
holds a non-empty source file whose completion record cannot be read (a file
that cannot be opened, is not valid UTF-8, or does not end with a
``"complete": true`` record, as one still being written or cut short): such
a file may name any source as ``parent``.

Run this **only while no session is running**: it cannot tell a live
writer's marks from stale ones. Dry-run by default; pass ``--execute`` to
delete. With ``--execute``, a file that cannot be deleted is reported on
stderr and the exit status is 1.

Usage:
  python3 scripts/clean_dead_letter.py [DIR | --dir DIR] [--execute]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
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


def _read_completion_parent(path: Path) -> tuple[bool, str | None]:
    """Read the completion record that ends a non-empty file.

    Returns ``(True, parent)`` only when the end of the file reads, decodes as
    strict UTF-8, and its last line is a JSON object with ``"complete": true``
    whose ``parent`` is a string or absent (``None``). Returns
    ``(False, None)`` when the file cannot be read, is empty, does not decode,
    or does not end with such a record, so that no caller can take it for a
    sealed file that names no parent.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return False, None
            # Read the last 4 KB to find the completion record.
            chunk = min(size, 4096)
            f.seek(-chunk, os.SEEK_END)
            tail = f.read()
        if chunk < size:
            # A chunk from the middle of the file can start inside a line,
            # even inside a character: only the lines after its first line
            # break are whole.
            tail = tail[tail.find(b"\n") + 1:]
        text = tail.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return False, None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return False, None
    try:
        record = json.loads(lines[-1])
    except json.JSONDecodeError:
        return False, None
    if not isinstance(record, dict) or record.get("complete") is not True:
        return False, None
    parent = record.get("parent")
    if parent is not None and not isinstance(parent, str):
        return False, None
    return True, parent


def _is_source(name: str) -> bool:
    return any(name.startswith(p) for p in SOURCE_PREFIXES)


def _plan_single_dir_groups(directory: Path) -> list[tuple[Path, list[Path]]]:
    """Plan one partition directory as ``(source, sorted_outcomes)`` groups:
    nothing in it (``[]``) where an entry cannot be listed or lstat'ed, or a
    non-empty source file's completion record cannot be read (see
    ``_read_completion_parent``)."""
    try:
        entries = sorted(directory.iterdir())
    except OSError:
        return []
    sources: dict[str, tuple[Path, int]] = {}
    outcomes_by_source: dict[str, list[Path]] = {}
    non_empty_parents: set[str] = set()

    for entry in entries:
        try:
            st = entry.lstat()
        except OSError:
            return []
        if not stat.S_ISREG(st.st_mode):
            continue
        name = entry.name
        if _is_source(name):
            size = st.st_size
            sources[name] = (entry, size)
            if size > 0:
                ok, parent = _read_completion_parent(entry)
                if not ok:
                    return []
                if parent is not None:
                    non_empty_parents.add(parent)
            continue
        of = record_of(name)
        if of is not None:
            outcomes_by_source.setdefault(of, []).append(entry)

    groups: list[tuple[Path, list[Path]]] = []
    for name in sorted(sources):
        path, size = sources[name]
        if size == 0 and name not in non_empty_parents:
            groups.append((path, sorted(outcomes_by_source.get(name, []))))

    return groups


def _has_symlink_component(path: Path) -> bool:
    """Return True if ``path`` or any non-root ancestor is a symbolic link."""
    root = Path("/")
    for candidate in (path, *path.parents):
        try:
            if not candidate.is_symlink():
                continue
        except OSError:
            return True
        if sys.platform == "darwin" and candidate.parent == root and candidate.name in ("var", "tmp", "etc"):
            continue
        return True
    return False


def plan_deletion_groups(directory: Path) -> list[tuple[Path, list[Path]]]:
    """Return ``[(source_path, sorted_outcome_paths), ...]`` for ``directory``.

    Scans ``directory`` and its immediate ``<project.dataset.table>``
    subdirectories. A source file and all its ``outcome-*`` files are deletable
    when the source is empty (0 bytes) and no non-empty leftover file names it
    as ``parent``; nothing is in a partition directory that cannot be read
    whole (see ``_plan_single_dir_groups``).
    """
    if _has_symlink_component(directory) or not directory.is_dir():
        return []

    try:
        subdirs = [d for d in sorted(directory.iterdir())
                   if not d.is_symlink() and d.is_dir() and d.name != "dry-run"]
    except OSError:
        return []

    dirs = [directory, *subdirs]
    groups: list[tuple[Path, list[Path]]] = []
    for d in dirs:
        groups.extend(_plan_single_dir_groups(d))
    return groups


def plan_deletions(directory: Path) -> list[Path]:
    """Return the list of files that may be deleted from ``directory``.

    Scans ``directory`` and its immediate ``<project.dataset.table>``
    subdirectories. For each deletable group, ``<source>.jsonl`` precedes its
    ``outcome-*`` files.
    """
    return [path for source, outcomes in plan_deletion_groups(directory) for path in (source, *outcomes)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove dead-letter files that no longer guard any rows.")
    parser.add_argument("path", nargs="?", type=Path, default=None,
                        help=f"dead-letter directory (default: {DEFAULT_DIR})")
    parser.add_argument("--dir", type=Path, default=None,
                        help=f"dead-letter directory (default: {DEFAULT_DIR})")
    parser.add_argument("--execute", action="store_true",
                        help="delete the files; without it, only report them.")
    args = parser.parse_args(argv)
    if args.path is not None and args.dir is not None:
        parser.error("pass either positional path or --dir, not both")

    base = (args.path if args.path is not None else args.dir if args.dir is not None else DEFAULT_DIR).expanduser()
    if _has_symlink_component(base):
        print(f"ERROR: refusing symlinked dead-letter directory: {base}", file=sys.stderr)
        return 1
    if not base.is_dir():
        print(f"dead-letter directory not found: {base}")
        return 0
    try:
        list(base.iterdir())
    except OSError as exc:
        print(f"ERROR: cannot read dead-letter directory {base}: {exc}", file=sys.stderr)
        return 1

    groups = plan_deletion_groups(base)
    to_delete = [path for source, outcomes in groups for path in (source, *outcomes)]
    if not to_delete:
        print("nothing to clean")
        return 0

    if not args.execute:
        for path in to_delete:
            print(f"would delete: {path.relative_to(base)}")
        print(f"{len(to_delete)} file(s) would delete")
        return 0

    # Each group unlinks its 0-byte regular source file first (re-verifying
    # lstat() right before unlink()), and unlinks its outcome-* files only
    # after the source is gone so outcome records are never orphaned beside a
    # source file that became non-empty or could not be removed.
    deleted_count = 0
    error_count = 0
    for source, outcomes in groups:
        rel_source = source.relative_to(base)
        try:
            st = source.lstat()
        except OSError as exc:
            error_count += 1
            print(f"  ERROR: {rel_source}: {exc}", file=sys.stderr)
            continue
        if not stat.S_ISREG(st.st_mode) or st.st_size != 0:
            error_count += 1
            print(f"  ERROR: {rel_source}: no longer an empty regular file", file=sys.stderr)
            continue
        try:
            source.unlink()
        except OSError as exc:
            error_count += 1
            print(f"  ERROR: {rel_source}: {exc}", file=sys.stderr)
            continue
        deleted_count += 1
        print(f"deleted: {rel_source}")
        for outcome in outcomes:
            rel_outcome = outcome.relative_to(base)
            try:
                ost = outcome.lstat()
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_outcome}: {exc}", file=sys.stderr)
                continue
            if not stat.S_ISREG(ost.st_mode):
                error_count += 1
                print(f"  ERROR: {rel_outcome}: not a regular file", file=sys.stderr)
                continue
            try:
                outcome.unlink()
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_outcome}: {exc}", file=sys.stderr)
                continue
            deleted_count += 1
            print(f"deleted: {rel_outcome}")

    print(f"{deleted_count} file(s) deleted")
    return 1 if error_count else 0


if __name__ == "__main__":
    sys.exit(main())
