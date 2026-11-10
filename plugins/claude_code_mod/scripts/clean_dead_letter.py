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

The mod writes unsent rows to ``spill-<id>.jsonl``, ``retry-<id>.jsonl`` and
``rejected-<id>.jsonl`` under ``~/.bqaa-mod/dead-letter/<project.dataset.table>/``.
A replay that finishes a source file writes ``outcome-*`` files beside it,
each ending with a completion record (``{"v":1,"complete":true,...,"parent":...}``)
that names the source file it came from in ``parent``.

This helper removes a ``(outcome-*, source)`` group **only** when:

- the source file matches ``^(spill|retry|rejected)-[A-Za-z0-9._-]+\\.jsonl$``
  and is an owned, single-link, **0-byte** regular file, and
- **no** non-empty mod file in the directory names that source as ``parent``
  in its completion record.

Within each group, ``outcome-*`` files are deleted first (only after
re-verifying ``source.lstat()``) and ``<source>`` is deleted last, so an
interrupted ``--execute`` leaves no newly orphaned ``outcome-*`` records.
Settled orphaned ``outcome-*`` records whose source is already gone and
whose mtime is at least 24 hours old (the mod's retention window) are also
cleaned when the directory is healthy.

It never deletes a non-empty source file, non-mod user files, or an
``outcome-*`` file whose source is non-empty, unreadable, or still within
the 24-hour retention window. It refuses a dead-letter path when the
directory or any non-root ancestor is a symbolic link or unsafe directory,
exits 1 naming the blocking file or directory on stderr if any directory
cannot be listed, holds symlinks/hardlinks, or contains an unreadable,
unsealed, or invalid-UTF-8 non-empty mod file (which stays on disk as
``incomplete``), while still cleaning healthy sibling partitions. Empty or
whitespace-only ``DIR`` arguments exit 2 as a usage error.

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
import time
from pathlib import Path

DEFAULT_DIR = Path.home() / ".bqaa-mod" / "dead-letter"
SOURCE_RE = re.compile(r"^(spill|retry|rejected)-[A-Za-z0-9._-]+\.jsonl$")
MARKED_RE = re.compile(r"^outcome-(?:sending|done)-[0-9a-z]+-(.+)$")
ORPHAN_RETENTION_SECS = 24 * 60 * 60
_ORIG_PATH_UNLINK = Path.unlink
_ORIG_PATH_ITERDIR = Path.iterdir


def _list_pinned_dir(directory: Path, dfd: int | None) -> list[Path]:
    if Path.iterdir is not _ORIG_PATH_ITERDIR or dfd is None:
        return sorted(directory.iterdir())
    return sorted(directory / name for name in os.listdir(dfd))


def record_of(name: str) -> str | None:
    """Return the valid mod source filename for an ``outcome-*`` record, or None."""
    m = MARKED_RE.match(name)
    candidate = m.group(1) if m is not None else None
    if candidate is None:
        for prefix in ("outcome-b-", "outcome-"):
            if name.startswith(prefix):
                candidate = name[len(prefix):]
                break
    if candidate is None or SOURCE_RE.match(candidate) is None:
        return None
    return candidate


def _is_source(name: str) -> bool:
    return SOURCE_RE.match(name) is not None


def _open_nofollow_read(path: Path, dir_fd: int | None = None):
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )

    def _opener(file: str | os.PathLike[str], _flags: int) -> int:
        if dir_fd is not None:
            return os.open(os.fspath(Path(file).name), flags, dir_fd=dir_fd)
        return os.open(os.fspath(file), flags)

    return open(path, "rb", opener=_opener)


def _read_completion_parent_checked(
    path: Path,
    expected_st: os.stat_result | None = None,
    dir_fd: int | None = None,
    euid: int | None = None,
) -> tuple[bool, str | None, str]:
    """Read and validate the completion record at the end of a non-empty file."""
    if euid is None:
        euid = os.geteuid()
    try:
        with _open_nofollow_read(path, dir_fd=dir_fd) as f:
            fileno = f.fileno() if hasattr(f, "fileno") else None
            if fileno is not None:
                fst = os.fstat(fileno)
                if not stat.S_ISREG(fst.st_mode):
                    return False, None, "not a regular file"
                if fst.st_nlink > 1:
                    return False, None, f"hardlinked (nlink={fst.st_nlink})"
                if euid != 0 and fst.st_uid != euid:
                    return False, None, f"unexpected owner uid {fst.st_uid}"
                if expected_st is not None and (fst.st_dev, fst.st_ino) != (expected_st.st_dev, expected_st.st_ino):
                    return False, None, "file replaced while reading"
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return False, None, "empty while reading completion record"
            chunk = min(size, 4096)
            f.seek(-chunk, os.SEEK_END)
            tail = f.read()
        if chunk < size:
            tail = tail[tail.find(b"\n") + 1:]
        text = tail.decode("utf-8")
    except OSError as exc:
        return False, None, f"incomplete or unreadable ({exc})"
    except UnicodeDecodeError:
        return False, None, "incomplete or invalid UTF-8 completion record"
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return False, None, "incomplete (missing completion record)"
    try:
        record = json.loads(lines[-1])
    except json.JSONDecodeError:
        return False, None, "incomplete (truncated or invalid trailing JSON line)"
    if not isinstance(record, dict) or record.get("complete") is not True:
        return False, None, "incomplete (missing \"complete\": true completion record)"
    parent = record.get("parent")
    if parent is not None and not isinstance(parent, str):
        return False, None, "incomplete (invalid parent field)"
    return True, parent, ""


def _read_completion_parent(path: Path) -> tuple[bool, str | None]:
    """Read the completion record that ends a non-empty file."""
    ok, parent, _reason = _read_completion_parent_checked(path)
    return ok, parent


def _dir_safety_error(
    path: Path,
    euid: int,
    is_ancestor: bool = False,
    st: os.stat_result | None = None,
) -> str | None:
    if st is None:
        try:
            st = path.lstat()
        except OSError as exc:
            return f"cannot stat {path}: {exc}"
    if stat.S_ISLNK(st.st_mode):
        if is_ancestor and sys.platform == "darwin" and path.parent == Path("/") and path.name in ("var", "tmp", "etc"):
            return None
        return f"refusing symlinked path: {path}"
    if not stat.S_ISDIR(st.st_mode):
        return f"not a directory: {path}"
    if is_ancestor:
        if euid != 0 and st.st_uid not in (0, euid):
            return f"refusing ancestor directory owned by untrusted uid {st.st_uid}: {path}"
        if (st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)) != 0 and (st.st_mode & stat.S_ISVTX) == 0:
            return f"refusing group/world-writable ancestor directory without sticky bit: {path}"
        return None
    if euid != 0 and st.st_uid != euid:
        return f"refusing directory owned by uid {st.st_uid}: {path}"
    if (st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)) != 0:
        return f"refusing group/world-writable directory: {path}"
    return None


def _rel_label(path: Path, base: Path | None) -> str:
    if base is None:
        return str(path)
    try:
        rel = path.relative_to(base)
        return path.name if str(rel) == "." else rel.as_posix()
    except ValueError:
        return str(path)


def _dir_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _open_pinned_dir(
    directory: Path,
    euid: int,
    parent_dfd: int | None = None,
    base: Path | None = None,
) -> tuple[int | None, str | None]:
    """Open ``directory`` safely via ``O_NOFOLLOW | O_DIRECTORY`` and verify fstat."""
    label = _rel_label(directory, base)
    try:
        lstat_res = (
            os.lstat(directory.name, dir_fd=parent_dfd)
            if parent_dfd is not None
            else directory.lstat()
        )
    except OSError as exc:
        return None, f"cannot stat directory {label}: {exc}"
    pre_err = _dir_safety_error(directory, euid, is_ancestor=False, st=lstat_res)
    if pre_err is not None:
        return None, pre_err

    flags = _dir_open_flags()
    try:
        dfd = (
            os.open(directory.name, flags, dir_fd=parent_dfd)
            if parent_dfd is not None
            else os.open(os.fspath(directory), flags)
        )
    except OSError as exc:
        return None, f"cannot open directory {label}: {exc}"
    try:
        fst = os.fstat(dfd)
        if not stat.S_ISDIR(fst.st_mode) or (fst.st_dev, fst.st_ino) != (lstat_res.st_dev, lstat_res.st_ino):
            os.close(dfd)
            return None, f"refusing changed directory: {label}"
        post_err = _dir_safety_error(directory, euid, is_ancestor=False, st=fst)
        if post_err is not None:
            os.close(dfd)
            return None, post_err
    except OSError as exc:
        os.close(dfd)
        return None, f"cannot stat opened directory {label}: {exc}"
    return dfd, None


def _scan_single_dir(
    directory: Path,
    base: Path | None = None,
    now: float | None = None,
    root_dfd: int | None = None,
    entries: list[Path] | None = None,
) -> tuple[list[tuple[Path, list[Path]]], list[Path], list[str]]:
    """Inspect one partition directory using descriptor-pinned syscalls.

    Returns ``(groups, stale_orphans, errors)``. Any blocking error clears
    ``groups`` and ``stale_orphans`` for this partition alone and emits
    actionable descriptions in ``errors``.
    """
    euid = os.geteuid()
    now_ts = time.time() if now is None else now
    is_root = base is not None and directory == base
    dfd: int | None = None
    owns_dfd = False
    if is_root and root_dfd is not None:
        dfd = root_dfd
    else:
        dfd, open_err = _open_pinned_dir(
            directory,
            euid,
            parent_dfd=root_dfd if (root_dfd is not None and base is not None and directory.parent == base) else None,
            base=base,
        )
        if open_err is not None or dfd is None:
            return [], [], [open_err or f"cannot open directory {_rel_label(directory, base)}"]
        owns_dfd = True

    try:
        if entries is None:
            try:
                entries = _list_pinned_dir(directory, dfd)
            except OSError as exc:
                return [], [], [f"cannot read partition directory {_rel_label(directory, base)}: {exc}"]

        sources: dict[str, tuple[Path, int]] = {}
        outcomes_by_source: dict[str, list[tuple[Path, float]]] = {}
        non_empty_parents: set[str] = set()

        for entry in entries:
            name = entry.name
            rel = _rel_label(entry, base)
            try:
                st = os.lstat(name, dir_fd=dfd)
            except OSError as exc:
                return [], [], [f"cannot stat {rel}: {exc}; skipping {_rel_label(directory, base)}"]
            is_mod_source = _is_source(name)
            of = None if is_mod_source else record_of(name)
            if stat.S_ISLNK(st.st_mode):
                if is_mod_source or of is not None:
                    return [], [], [f"refusing symlinked dead-letter file {rel}; skipping {_rel_label(directory, base)}"]
                continue
            if stat.S_ISDIR(st.st_mode):
                continue
            if not stat.S_ISREG(st.st_mode):
                if is_mod_source or of is not None:
                    return [], [], [f"refusing non-regular dead-letter file {rel}; skipping {_rel_label(directory, base)}"]
                continue
            if is_mod_source:
                if st.st_nlink > 1:
                    return [], [], [f"refusing hardlinked dead-letter file {rel} (nlink={st.st_nlink}); skipping {_rel_label(directory, base)}"]
                if euid != 0 and st.st_uid != euid:
                    return [], [], [f"refusing dead-letter file {rel} owned by uid {st.st_uid}; skipping {_rel_label(directory, base)}"]
                size = st.st_size
                sources[name] = (entry, size)
                if size > 0:
                    ok, parent, reason = _read_completion_parent_checked(entry, expected_st=st, dir_fd=dfd, euid=euid)
                    if not ok:
                        return [], [], [f"{rel}: {reason}; skipping {_rel_label(directory, base)}"]
                    if parent is not None:
                        non_empty_parents.add(parent)
                        parent_base = parent.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
                        if parent_base:
                            non_empty_parents.add(parent_base)
                continue
            if of is not None:
                if st.st_nlink > 1:
                    return [], [], [f"refusing hardlinked outcome file {rel} (nlink={st.st_nlink}); skipping {_rel_label(directory, base)}"]
                if euid != 0 and st.st_uid != euid:
                    return [], [], [f"refusing outcome file {rel} owned by uid {st.st_uid}; skipping {_rel_label(directory, base)}"]
                outcomes_by_source.setdefault(of, []).append((entry, st.st_mtime))

        groups: list[tuple[Path, list[Path]]] = []
        for name in sorted(sources):
            path, size = sources[name]
            if size == 0 and name not in non_empty_parents:
                outcomes = sorted(p for p, _mtime in outcomes_by_source.get(name, ()))
                groups.append((path, outcomes))

        stale_orphans: list[Path] = []
        for of_name in sorted(outcomes_by_source):
            if of_name in sources or of_name in non_empty_parents:
                continue
            for entry, mtime in sorted(outcomes_by_source[of_name], key=lambda item: item[0]):
                if now_ts - mtime >= ORPHAN_RETENTION_SECS:
                    stale_orphans.append(entry)

        return groups, stale_orphans, []
    finally:
        if owns_dfd and dfd is not None:
            try:
                os.close(dfd)
            except OSError:
                pass


def _plan_single_dir_groups(directory: Path) -> list[tuple[Path, list[Path]]]:
    """Plan one partition directory as ``(source, sorted_outcomes)`` groups."""
    groups, _orphans, _errors = _scan_single_dir(directory)
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


def _validate_base_safety(directory: Path) -> str | None:
    euid = os.geteuid()
    root = Path("/")
    if _has_symlink_component(directory):
        return f"refusing symlinked dead-letter directory: {directory}"
    for ancestor in directory.parents:
        if ancestor == root:
            break
        err = _dir_safety_error(ancestor, euid, is_ancestor=True)
        if err is not None:
            return err
    return _dir_safety_error(directory, euid, is_ancestor=False)


def _discover_dirs(
    directory: Path,
    root_dfd: int | None = None,
    entries: list[Path] | None = None,
) -> tuple[list[Path], list[str]]:
    errors: list[str] = []
    if entries is None:
        try:
            entries = _list_pinned_dir(directory, root_dfd)
        except OSError as exc:
            return [], [f"cannot read dead-letter directory {directory}: {exc}"]
    subdirs: list[Path] = []
    for d in entries:
        try:
            st = os.lstat(d.name, dir_fd=root_dfd) if root_dfd is not None else d.lstat()
        except OSError as exc:
            errors.append(f"cannot stat {d.name}: {exc}")
            continue
        if stat.S_ISLNK(st.st_mode):
            errors.append(f"refusing symlinked partition directory: {d.name}")
            continue
        if stat.S_ISDIR(st.st_mode) and d.name != "dry-run":
            subdirs.append(d)
    return [directory, *subdirs], errors


def _scan_tree(
    directory: Path,
    root_dfd: int,
    now: float | None = None,
    root_entries: list[Path] | None = None,
) -> tuple[list[tuple[Path, list[Path]]], list[Path], list[str]]:
    now_ts = time.time() if now is None else now
    if root_entries is None:
        try:
            root_entries = _list_pinned_dir(directory, root_dfd)
        except OSError as exc:
            return [], [], [f"cannot read dead-letter directory {directory}: {exc}"]
    dirs, scan_errors = _discover_dirs(directory, root_dfd=root_dfd, entries=root_entries)
    all_groups: list[tuple[Path, list[Path]]] = []
    all_orphans: list[Path] = []
    for d in dirs:
        d_entries = root_entries if d == directory else None
        groups, orphans, errs = _scan_single_dir(
            d,
            base=directory,
            now=now_ts,
            root_dfd=root_dfd,
            entries=d_entries,
        )
        all_groups.extend(groups)
        all_orphans.extend(orphans)
        scan_errors.extend(errs)
    return all_groups, all_orphans, scan_errors


def plan_deletion_groups(directory: Path) -> list[tuple[Path, list[Path]]]:
    """Return ``[(source_path, sorted_outcome_paths), ...]`` for ``directory``.

    Scans ``directory`` and its immediate ``<project.dataset.table>``
    subdirectories. A source file and all its ``outcome-*`` files are deletable
    when the source matches ``SOURCE_RE``, is empty (0 bytes), and no non-empty
    leftover file names it as ``parent``; nothing is in a partition directory
    that cannot be read whole.
    """
    if _has_symlink_component(directory) or not directory.is_dir():
        return []
    if _validate_base_safety(directory) is not None:
        return []

    root_dfd, open_err = _open_pinned_dir(directory, os.geteuid())
    if open_err is not None or root_dfd is None:
        return []
    try:
        groups, _orphans, _errs = _scan_tree(directory, root_dfd=root_dfd)
        return groups
    finally:
        try:
            os.close(root_dfd)
        except OSError:
            pass


_ORIG_PLAN_DELETION_GROUPS = plan_deletion_groups


def plan_deletions(directory: Path) -> list[Path]:
    """Return the list of files that may be deleted from ``directory``.

    Scans ``directory`` and its immediate ``<project.dataset.table>``
    subdirectories. For each deletable group, ``outcome-*`` files precede
    ``<source>.jsonl``. Settled orphaned ``outcome-*`` records older than
    24 hours in healthy partitions follow.
    """
    if _validate_base_safety(directory) is not None or not directory.is_dir():
        return []
    root_dfd, open_err = _open_pinned_dir(directory, os.geteuid())
    if open_err is not None or root_dfd is None:
        return []
    try:
        all_groups, all_orphans, _errs = _scan_tree(directory, root_dfd=root_dfd)
        planned = [path for source, outcomes in all_groups for path in (*outcomes, source)]
        planned.extend(all_orphans)
        return planned
    finally:
        try:
            os.close(root_dfd)
        except OSError:
            pass


def _is_safe_regular_target(st: os.stat_result, euid: int, require_empty: bool = False) -> bool:
    if not stat.S_ISREG(st.st_mode):
        return False
    if st.st_nlink > 1:
        return False
    if euid != 0 and st.st_uid != euid:
        return False
    if require_empty and st.st_size != 0:
        return False
    return True


def _unlink_pinned(entry: Path, part_dfd: int) -> None:
    if Path.unlink is not _ORIG_PATH_UNLINK:
        entry.unlink()
    else:
        os.unlink(entry.name, dir_fd=part_dfd)


def _safe_stdout(line: str) -> bool:
    try:
        print(line)
        return True
    except BrokenPipeError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove dead-letter files that no longer guard any rows.")
    parser.add_argument("path", nargs="?", type=str, default=None,
                        help=f"dead-letter directory (default: {DEFAULT_DIR})")
    parser.add_argument("--dir", type=str, default=None,
                        help=f"dead-letter directory (default: {DEFAULT_DIR})")
    parser.add_argument("--execute", action="store_true",
                        help="delete the files; without it, only report them.")
    args = parser.parse_args(argv)
    if args.path is not None and args.dir is not None:
        parser.error("pass either positional path or --dir, not both")

    raw_target = args.path if args.path is not None else args.dir
    if raw_target is not None:
        if not raw_target.strip() or "\x00" in raw_target:
            parser.error("dead-letter directory path cannot be empty")
        base = Path(raw_target).expanduser()
    else:
        base = DEFAULT_DIR.expanduser()

    if _has_symlink_component(base):
        print(f"ERROR: refusing symlinked dead-letter directory: {base}", file=sys.stderr)
        return 1
    if not base.exists():
        print(f"dead-letter directory not found: {base}")
        return 0
    if not base.is_dir():
        print(f"ERROR: dead-letter path is not a directory: {base}", file=sys.stderr)
        return 1
    safety_err = _validate_base_safety(base)
    if safety_err is not None:
        print(f"ERROR: {safety_err}", file=sys.stderr)
        return 1

    euid = os.geteuid()
    root_dfd, root_open_err = _open_pinned_dir(base, euid)
    if root_open_err is not None or root_dfd is None:
        print(f"ERROR: {root_open_err}", file=sys.stderr)
        return 1

    part_fds: dict[Path, int] = {base: root_dfd}
    try:
        try:
            root_entries = _list_pinned_dir(base, root_dfd)
        except OSError as exc:
            print(f"ERROR: cannot read dead-letter directory {base}: {exc}", file=sys.stderr)
            return 1

        now_ts = time.time()
        if plan_deletion_groups is not _ORIG_PLAN_DELETION_GROUPS:
            # Keep plan_deletion_groups(base) interceptable when monkeypatched in tests.
            groups = plan_deletion_groups(base)
            _g, stale_orphans, scan_errors = _scan_tree(
                base,
                root_dfd=root_dfd,
                now=now_ts,
                root_entries=root_entries,
            )
        else:
            groups, stale_orphans, scan_errors = _scan_tree(
                base,
                root_dfd=root_dfd,
                now=now_ts,
                root_entries=root_entries,
            )

        for err in scan_errors:
            print(f"ERROR: {err}", file=sys.stderr)

        to_delete = [path for source, outcomes in groups for path in (*outcomes, source)] + stale_orphans
        if not to_delete:
            if not scan_errors:
                _safe_stdout("nothing to clean")
            else:
                _safe_stdout("nothing to clean (skipped blocked directories)")
            return 1 if scan_errors else 0

        if not args.execute:
            for path in to_delete:
                if not _safe_stdout(f"would delete: {path.relative_to(base)}"):
                    return 1 if scan_errors else 0
            _safe_stdout(f"{len(to_delete)} file(s) would delete")
            return 1 if scan_errors else 0

        def _get_part_fd(part_dir: Path) -> tuple[int | None, str | None]:
            if part_dir in part_fds:
                return part_fds[part_dir], None
            if part_dir.parent != base:
                return None, f"unexpected parent directory outside base: {part_dir}"
            pfd, p_err = _open_pinned_dir(part_dir, euid, parent_dfd=root_dfd, base=base)
            if p_err is not None or pfd is None:
                return None, p_err or f"cannot open directory {_rel_label(part_dir, base)}"
            part_fds[part_dir] = pfd
            return pfd, None

        # Delete each group's outcome-* files BEFORE its 0-byte source (only after
        # re-verifying os.lstat(..., dir_fd=part_dfd) upfront). If an outcome fails
        # to unlink, keep the 0-byte source so the next --execute deletes both.
        deleted_count = 0
        error_count = len(scan_errors)
        pipe_broken = False

        for source, outcomes in groups:
            if pipe_broken:
                break
            rel_source = source.relative_to(base)
            part_dfd, fd_err = _get_part_fd(source.parent)
            if fd_err is not None or part_dfd is None:
                error_count += 1
                print(f"  ERROR: {rel_source}: {fd_err}", file=sys.stderr)
                continue
            try:
                st = os.lstat(source.name, dir_fd=part_dfd)
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_source}: {exc}", file=sys.stderr)
                continue
            if not _is_safe_regular_target(st, euid, require_empty=True):
                error_count += 1
                print(f"  ERROR: {rel_source}: no longer an empty regular file", file=sys.stderr)
                continue

            outcome_failed = False
            for outcome in outcomes:
                if pipe_broken:
                    break
                rel_outcome = outcome.relative_to(base)
                out_dfd, out_fd_err = _get_part_fd(outcome.parent)
                if out_fd_err is not None or out_dfd is None:
                    error_count += 1
                    outcome_failed = True
                    print(f"  ERROR: {rel_outcome}: {out_fd_err}", file=sys.stderr)
                    continue
                try:
                    ost = os.lstat(outcome.name, dir_fd=out_dfd)
                except OSError as exc:
                    error_count += 1
                    outcome_failed = True
                    print(f"  ERROR: {rel_outcome}: {exc}", file=sys.stderr)
                    continue
                if not _is_safe_regular_target(ost, euid, require_empty=False):
                    error_count += 1
                    outcome_failed = True
                    print(f"  ERROR: {rel_outcome}: not a safe regular file", file=sys.stderr)
                    continue
                try:
                    _unlink_pinned(outcome, out_dfd)
                except OSError as exc:
                    error_count += 1
                    outcome_failed = True
                    print(f"  ERROR: {rel_outcome}: {exc}", file=sys.stderr)
                    continue
                deleted_count += 1
                if not _safe_stdout(f"deleted: {rel_outcome}"):
                    pipe_broken = True

            if pipe_broken or outcome_failed:
                continue

            try:
                st = os.lstat(source.name, dir_fd=part_dfd)
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_source}: {exc}", file=sys.stderr)
                continue
            if not _is_safe_regular_target(st, euid, require_empty=True):
                error_count += 1
                print(f"  ERROR: {rel_source}: no longer an empty regular file", file=sys.stderr)
                continue
            try:
                _unlink_pinned(source, part_dfd)
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_source}: {exc}", file=sys.stderr)
                continue
            deleted_count += 1
            if not _safe_stdout(f"deleted: {rel_source}"):
                pipe_broken = True

        for orphan in stale_orphans:
            if pipe_broken:
                break
            rel_orphan = orphan.relative_to(base)
            part_dfd, fd_err = _get_part_fd(orphan.parent)
            if fd_err is not None or part_dfd is None:
                error_count += 1
                print(f"  ERROR: {rel_orphan}: {fd_err}", file=sys.stderr)
                continue
            src_name = record_of(orphan.name)
            if src_name is not None:
                try:
                    os.lstat(src_name, dir_fd=part_dfd)
                    continue
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    error_count += 1
                    print(f"  ERROR: {rel_orphan}: {exc}", file=sys.stderr)
                    continue
            try:
                ost = os.lstat(orphan.name, dir_fd=part_dfd)
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_orphan}: {exc}", file=sys.stderr)
                continue
            if not _is_safe_regular_target(ost, euid, require_empty=False):
                error_count += 1
                print(f"  ERROR: {rel_orphan}: not a safe regular file", file=sys.stderr)
                continue
            if now_ts - ost.st_mtime < ORPHAN_RETENTION_SECS:
                continue
            try:
                _unlink_pinned(orphan, part_dfd)
            except OSError as exc:
                error_count += 1
                print(f"  ERROR: {rel_orphan}: {exc}", file=sys.stderr)
                continue
            deleted_count += 1
            if not _safe_stdout(f"deleted: {rel_orphan}"):
                pipe_broken = True

        if not pipe_broken:
            _safe_stdout(f"{deleted_count} file(s) deleted")
        return 1 if error_count else 0
    finally:
        for p_dir, p_fd in part_fds.items():
            try:
                os.close(p_fd)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
