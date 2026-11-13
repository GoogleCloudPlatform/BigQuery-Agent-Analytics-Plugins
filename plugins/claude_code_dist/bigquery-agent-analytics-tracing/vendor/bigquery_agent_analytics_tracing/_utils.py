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

"""Shared helpers for serialization, hashing, timestamps, private
directories, log emission, and starting Python subprocesses. No
dependencies on other package modules so this can be safely imported
from anywhere in the package."""

from __future__ import annotations

import contextlib
from datetime import datetime
from datetime import timezone
import errno
import fcntl
import fnmatch
import getpass
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time
from typing import Any, Iterator, TYPE_CHECKING
import uuid
import weakref

if TYPE_CHECKING:
  from .config import BQAAConfig

# ``python -c`` code that drops the entry -c (like -m) puts first on
# sys.path: the working directory, which for a hook is usually a
# checked-out repository. Python 3.11+ leaves that entry out under
# PYTHONSAFEPATH or -P (``sys.flags.safe_path``); 3.10 always adds it. It
# runs before anything else is imported.
PYTHON_C_DROP_WORKING_DIR = (
    "import sys\n"
    'if not getattr(sys.flags, "safe_path", False):\n'
    "    del sys.path[0]\n"
)
# ``[python, "-c", PYTHON_C_RUN_MODULE, module, *args]`` runs ``module``
# like ``python -m module *args``, minus the working directory on
# sys.path. The plugin's shell launchers carry the same code.
PYTHON_C_RUN_MODULE = PYTHON_C_DROP_WORKING_DIR + (
    "del sys.argv[0]\n"
    "import runpy\n"
    'runpy.run_module(sys.argv[0], run_name="__main__", alter_sys=True)\n'
)

SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "client_secret",
        "id_token",
        "password",
        "refresh_token",
        "secret",
        "token",
    }
)


def utc_now() -> datetime:
  return datetime.now(timezone.utc)


def timestamp_ms() -> int:
  return int(time.time() * 1000)


def iso_timestamp(value: datetime | None = None) -> str:
  ts = value or utc_now()
  return ts.isoformat(timespec="microseconds").replace("+00:00", "Z")


def hex_id(chars: int) -> str:
  return uuid.uuid4().hex[:chars]


def deterministic_span(seed: str, chars: int = 16) -> str:
  """Stable hex span id derived from a seed (e.g. tool_use_id).

  Used so that a PostToolUse without a matching PreToolUse can still emit a
  span_id that correlates with whatever the (missing) start would have
  produced. Better than ``hex_id`` which yields a fresh random id and breaks
  span correlation.
  """
  if not seed:
    return hex_id(chars)
  digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
  return digest[:chars]


def to_jsonable(value: Any) -> Any:
  if value is None or isinstance(value, (str, int, float, bool)):
    return value
  if isinstance(value, dict):
    return {str(k): to_jsonable(v) for k, v in value.items()}
  if isinstance(value, (list, tuple, set)):
    return [to_jsonable(v) for v in value]
  if hasattr(value, "model_dump"):
    return to_jsonable(value.model_dump())
  if hasattr(value, "to_dict"):
    return to_jsonable(value.to_dict())
  return str(value)


def truncate(value: Any, max_len: int) -> tuple[Any, bool]:
  """Recursively truncate strings and redact sensitive dict keys.

  Returns ``(clipped_value, was_truncated)``. ``max_len == -1`` disables
  truncation. Keys whose lowercase name is in ``SENSITIVE_KEYS`` or starts
  with ``"temp:"`` are replaced with ``"[REDACTED]"``.
  """
  value = to_jsonable(value)
  if max_len == -1:
    return value, False
  if isinstance(value, str):
    if len(value) > max_len:
      return value[:max_len] + "...[TRUNCATED]", True
    return value, False
  if isinstance(value, list):
    out = []
    truncated = False
    for item in value:
      next_item, did_truncate = truncate(item, max_len)
      out.append(next_item)
      truncated = truncated or did_truncate
    return out, truncated
  if isinstance(value, dict):
    out: dict[str, Any] = {}
    truncated = False
    for key, item in value.items():
      key_lower = str(key).lower()
      if key_lower in SENSITIVE_KEYS or key_lower.startswith("temp:"):
        out[key] = "[REDACTED]"
        continue
      next_item, did_truncate = truncate(item, max_len)
      out[key] = next_item
      truncated = truncated or did_truncate
    return out, truncated
  return value, False


def safe_json_loads(value: str | None, default: Any) -> Any:
  if not value:
    return default
  try:
    return json.loads(value)
  except json.JSONDecodeError:
    return default


# POSIX-only pieces (the effective uid, O_NOFOLLOW and other open flags,
# the *at() calls behind ``dir_fd``) are looked up when used. Where one is
# missing, protection degrades to best effort instead of breaking import
# or the first write.


def _flag(name: str) -> int:
  """``os.<name>`` open flag, or 0 where this platform lacks it."""
  return getattr(os, name, 0)


def _euid() -> int | None:
  """Effective uid, or None without POSIX ownership (e.g. Windows)."""
  geteuid = getattr(os, "geteuid", None)
  return geteuid() if geteuid is not None else None


def _dir_fd_supported() -> bool:
  """Whether files can be used relative to a directory descriptor."""
  needed = {"open", "stat", "mkdir", "rename", "unlink"}
  return (
      hasattr(os, "O_DIRECTORY")
      and hasattr(os, "O_NOFOLLOW")
      and needed <= {f.__name__ for f in os.supports_dir_fd}
      and "listdir" in {f.__name__ for f in os.supports_fd}
  )


def _check_dir(info: os.stat_result, path: Path) -> None:
  """Raise unless ``info`` is a directory that only we can modify."""
  euid = _euid()
  if not stat.S_ISDIR(info.st_mode):
    problem = "is a symlink or not a directory"
  elif euid is None:
    return
  elif info.st_uid != euid:
    problem = f"is owned by uid {info.st_uid}, not {euid}"
  elif info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
    problem = "is writable by its group or other users"
  else:
    return
  raise PermissionError(f"Refusing BQAA tracing dir {path}: it {problem}.")


def _check_own_file(info: os.stat_result, path: Path) -> None:
  """Raise unless ``info`` is a regular file that we own."""
  euid = _euid()
  if not stat.S_ISREG(info.st_mode):
    problem = "is not a regular file"
  elif euid is not None and info.st_uid != euid:
    problem = f"is owned by uid {info.st_uid}, not {euid}"
  else:
    return
  raise PermissionError(f"Refusing BQAA tracing file {path}: it {problem}.")


def _open_nofollow(target: str | Path, flags: int, mode: int, **at: int) -> int:
  """``os.open`` that never follows a symlink as the last component.

  O_NONBLOCK keeps a FIFO planted under the name from hanging the open.
  """
  if not _flag("O_NOFOLLOW") and os.path.islink(target):
    raise PermissionError(
        f"Refusing BQAA tracing file {target}: it is a symlink."
    )
  flags |= _flag("O_NOFOLLOW") | _flag("O_CLOEXEC") | _flag("O_NONBLOCK")
  return os.open(target, flags, mode, **at)


def _require_trusted_chain(directory: Path) -> None:
  """Raise unless ``directory`` and all its parents belong to root or us."""
  euid = _euid()
  if euid is None:
    return
  real = Path(os.path.realpath(directory))
  for current in (real, *real.parents):
    owner = os.stat(current).st_uid
    if owner not in (0, euid):
      raise PermissionError(
          f"Refusing BQAA tracing path under {current}: it is owned by uid"
          f" {owner}."
      )


def _require_trusted_parents(fd: int, path: Path) -> None:
  """``_require_trusted_chain`` for the parents of the directory ``fd``."""
  euid = _euid()
  if euid is None:
    return
  current, up = os.fstat(fd), ".."
  for _ in range(256):
    parent = os.stat(up, dir_fd=fd)
    if (parent.st_dev, parent.st_ino) == (current.st_dev, current.st_ino):
      return  # "/" is its own parent.
    if parent.st_uid not in (0, euid):
      raise PermissionError(
          f"Refusing BQAA tracing path under {path / up}: it is owned by"
          f" uid {parent.st_uid}."
      )
    current, up = parent, f"{up}/.."
  raise PermissionError(f"Refusing BQAA tracing dir {path}: it is too deep.")


class PrivateDir:
  """A directory only we can modify, pinned by the descriptor checked."""

  def __init__(self, path: Path, fd: int | None):
    self.path = path
    self.fd = fd
    self._close = None if fd is None else weakref.finalize(self, os.close, fd)

  def __enter__(self) -> PrivateDir:
    return self

  def __exit__(self, *exc_info: object) -> None:
    self.close()

  def close(self) -> None:
    if self._close is not None:
      self._close()

  def _at(self, name: str) -> tuple[str, dict[str, int]]:
    if self.fd is None:
      return os.path.join(self.path, name), {}
    return name, {"dir_fd": self.fd}

  def names(self) -> list[str]:
    return os.listdir(self.path if self.fd is None else self.fd)

  def lstat(self, name: str) -> os.stat_result:
    target, at = self._at(name)
    return os.lstat(target, **at)

  def exists(self, name: str) -> bool:
    try:
      self.lstat(name)
    except FileNotFoundError:
      return False
    return True

  def open(self, name: str, flags: int, mode: int = 0o600) -> int:
    """Open a regular file of ours in this dir, never through a symlink."""
    target, at = self._at(name)
    fd = _open_nofollow(target, flags, mode, **at)
    try:
      _check_own_file(os.fstat(fd), self.path / name)
    except BaseException:
      os.close(fd)
      raise
    return fd

  def unlink(self, name: str) -> None:
    target, at = self._at(name)
    os.unlink(target, **at)

  def replace(
      self, name: str, new_name: str, new_dir: PrivateDir | None = None
  ) -> None:
    """Rename ``name`` to ``new_name`` in ``new_dir`` (default: here)."""
    new_dir = new_dir or self
    if self.fd is None or new_dir.fd is None:
      os.replace(
          os.path.join(self.path, name), os.path.join(new_dir.path, new_name)
      )
    else:
      os.replace(name, new_name, src_dir_fd=self.fd, dst_dir_fd=new_dir.fd)

  def subdir(self, name: str) -> PrivateDir:
    """``ensure_private_dir`` for ``name`` inside this dir."""
    if self.fd is None:
      return ensure_private_dir(self.path / name)
    try:
      os.mkdir(name, 0o700, dir_fd=self.fd)
    except FileExistsError:
      pass
    return _open_private_dir(self.path / name, name, self.fd)


def ensure_private_dir(path: str | os.PathLike[str]) -> PrivateDir:
  """Create ``path`` owner-only (0o700), or verify an existing one."""
  directory = Path(path).expanduser()
  missing = []
  current = directory
  while not os.path.lexists(current):
    missing.append(current)
    current = current.parent
  for component in reversed(missing):
    try:
      os.mkdir(component, 0o700)
    except FileExistsError:
      pass
  if not _dir_fd_supported():
    _check_dir(os.lstat(directory), directory)
    _require_trusted_chain(directory.parent)
    return PrivateDir(directory, None)
  return _open_private_dir(directory, directory, None)


def _open_private_dir(
    path: Path, target: str | Path, dir_fd: int | None
) -> PrivateDir:
  flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | _flag("O_CLOEXEC")
  try:
    fd = os.open(target, flags, dir_fd=dir_fd)
  except OSError as exc:
    if exc.errno in (errno.ELOOP, errno.EMLINK, errno.ENOTDIR):
      raise PermissionError(
          f"Refusing BQAA tracing dir {path}: it is a symlink or not a"
          " directory."
      ) from exc
    raise
  try:
    _check_dir(os.fstat(fd), path)
    _require_trusted_parents(fd, path)
  except BaseException:
    os.close(fd)
    raise
  return PrivateDir(path, fd)


# The process's own standard streams: written to as they are.
_STREAM_ALIASES = frozenset({"/dev/stdout", "/dev/stderr"})


def _check_log_dir(info: os.stat_result, path: Path) -> bool:
  """Raise unless ``info`` may hold our log; return whether others own it."""
  euid = _euid()
  if euid is not None and info.st_uid not in (0, euid):
    raise PermissionError(
        f"Refusing BQAA tracing dir {path}: it is owned by uid {info.st_uid}."
    )
  shared = info.st_uid != euid  # Root's, such as /tmp or /dev.
  if not shared:
    _check_dir(info, path)
  return shared


def _rotate_log_in_private_dir(
    directory: PrivateDir,
    path: Path,
    fd: int,
    info: os.stat_result,
    append: int,
    max_bytes: int,
) -> int:
  """Rotate ``path.name`` -> ``path.name.1`` if ``st_size >= max_bytes``.

  On entry owns ``fd``; closes ``fd`` before raising or returning a new
  descriptor when rotation runs, and leaves ``fd`` open only when no
  rotation is needed.
  """
  if max_bytes <= 0 or info.st_size < max_bytes:
    return fd
  try:
    try:
      fcntl.flock(fd, fcntl.LOCK_EX)
      cur_info = os.fstat(fd)
      if cur_info.st_size >= max_bytes:
        try:
          disk_info = directory.lstat(path.name)
        except FileNotFoundError:
          disk_info = None
        if (
            disk_info is not None
            and (disk_info.st_dev, disk_info.st_ino)
            == (cur_info.st_dev, cur_info.st_ino)
        ):
          backup = f"{path.name}.1"
          if directory.exists(backup):
            backup_info = directory.lstat(backup)
            _check_own_file(backup_info, path.parent / backup)
          if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
          directory.replace(path.name, backup)
    finally:
      try:
        fcntl.flock(fd, fcntl.LOCK_UN)
      except OSError:
        pass
  finally:
    os.close(fd)
  new_fd = directory.open(path.name, append | os.O_CREAT, 0o600)
  try:
    new_info = os.fstat(new_fd)
    _check_own_file(new_info, path)
    if new_info.st_mode & 0o077 and hasattr(os, "fchmod"):
      os.fchmod(new_fd, 0o600)
  except BaseException:
    os.close(new_fd)
    raise
  return new_fd


def _maybe_rotate_fd_in_log_dir(
    path: Path,
    dir_fd: int | None,
    shared: bool,
    fd: int,
    flags: int,
    max_bytes: int,
) -> int:
  """Validate ``fd``, rotate to ``<path>.1`` if oversized, return open ``fd``.

  Closes ``fd`` on any exception so the caller never double-closes.
  """
  closed = False
  try:
    info = os.fstat(fd)
    if stat.S_ISCHR(info.st_mode):
      return fd
    _check_own_file(info, path)
    if shared and info.st_nlink != 1:
      raise PermissionError(
          f"Refusing BQAA tracing file {path}: it has other hard links."
      )
    if info.st_mode & 0o077 and hasattr(os, "fchmod"):
      os.fchmod(fd, 0o600)
    if max_bytes <= 0 or info.st_size < max_bytes:
      return fd

    try:
      try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        cur_info = os.fstat(fd)
        if cur_info.st_size >= max_bytes:
          try:
            disk_info = (
                os.lstat(path)
                if dir_fd is None
                else os.lstat(path.name, dir_fd=dir_fd)
            )
          except FileNotFoundError:
            disk_info = None
          if (
              disk_info is not None
              and (disk_info.st_dev, disk_info.st_ino)
              == (cur_info.st_dev, cur_info.st_ino)
          ):
            backup_name = f"{path.name}.1"
            backup_path = path.parent / backup_name
            try:
              binfo = (
                  os.lstat(backup_path)
                  if dir_fd is None
                  else os.lstat(backup_name, dir_fd=dir_fd)
              )
            except FileNotFoundError:
              binfo = None
            if binfo is not None:
              _check_own_file(binfo, backup_path)
              if shared and binfo.st_nlink != 1:
                raise PermissionError(
                    f"Refusing BQAA tracing file {backup_path}: it has other"
                    " hard links."
                )
            if hasattr(os, "fchmod"):
              os.fchmod(fd, 0o600)
            if dir_fd is None:
              os.replace(path, backup_path)
            else:
              os.replace(
                  path.name,
                  backup_name,
                  src_dir_fd=dir_fd,
                  dst_dir_fd=dir_fd,
              )
      finally:
        try:
          fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
          pass
    finally:
      closed = True
      os.close(fd)

    new_fd = (
        _open_nofollow(path, flags, 0o600)
        if dir_fd is None
        else _open_nofollow(path.name, flags, 0o600, dir_fd=dir_fd)
    )
    fd = new_fd
    closed = False
    new_info = os.fstat(fd)
    if stat.S_ISCHR(new_info.st_mode):
      return fd
    _check_own_file(new_info, path)
    if shared and new_info.st_nlink != 1:
      raise PermissionError(
          f"Refusing BQAA tracing file {path}: it has other hard links."
      )
    if new_info.st_mode & 0o077 and hasattr(os, "fchmod"):
      os.fchmod(fd, 0o600)
    return fd
  except BaseException:
    if not closed:
      os.close(fd)
    raise


def _open_in_log_dir(
    path: Path, flags: int, max_bytes: int
) -> tuple[int, bool]:
  """Open ``path`` relative to its existing dir, checked on a descriptor.

  Rotates to ``<name>.1`` (preserving ``0600`` and verifying the backup
  is safe) if ``st_size >= max_bytes > 0``.
  """
  directory = path.parent
  if not _dir_fd_supported():  # Best effort, by path.
    _require_trusted_chain(directory)
    shared = _check_log_dir(os.stat(directory), directory)
    fd = _open_nofollow(path, flags, 0o600)
    return _maybe_rotate_fd_in_log_dir(
        path, None, shared, fd, flags, max_bytes
    ), shared

  lookup = _flag("O_SEARCH") or _flag("O_PATH") or os.O_RDONLY
  dir_fd = os.open(directory, lookup | os.O_DIRECTORY | _flag("O_CLOEXEC"))
  try:
    shared = _check_log_dir(os.fstat(dir_fd), directory)
    _require_trusted_parents(dir_fd, directory)
    fd = _open_nofollow(path.name, flags, 0o600, dir_fd=dir_fd)
    return _maybe_rotate_fd_in_log_dir(
        path, dir_fd, shared, fd, flags, max_bytes
    ), shared
  finally:
    os.close(dir_fd)


def _open_log(config: "BQAAConfig") -> int:
  """Open ``config.log_file`` for appending; raise if it is unsafe.

  Rotates to ``<log_file>.1`` with ``0600`` mode when size reaches
  ``config.max_log_bytes`` (default 1 MiB).
  """
  path = Path(config.log_file).expanduser()
  append = os.O_WRONLY | os.O_APPEND | _flag("O_CLOEXEC") | _flag("O_NOCTTY")
  if str(path) in _STREAM_ALIASES:
    return os.open(path, append)  # Symlinks into /dev/fd or /proc.
  max_bytes = getattr(config, "max_log_bytes", 1_048_576)
  in_state_dir = path.parent == Path(config.state_dir).expanduser()
  if in_state_dir or not path.parent.is_dir():
    with ensure_private_dir(path.parent) as directory:
      fd = directory.open(path.name, append | os.O_CREAT, 0o600)
      rotated = False
      try:
        info = os.fstat(fd)
        if stat.S_ISCHR(info.st_mode):
          if hasattr(os, "set_blocking"):
            os.set_blocking(fd, True)
          return fd
        _check_own_file(info, path)
        if info.st_mode & 0o077 and hasattr(os, "fchmod"):
          os.fchmod(fd, 0o600)
        if max_bytes > 0 and info.st_size >= max_bytes:
          rotated = True
          fd = _rotate_log_in_private_dir(
              directory, path, fd, info, append, max_bytes
          )
      except BaseException:
        if not rotated:
          os.close(fd)
        raise
      return fd
  else:
    fd, shared = _open_in_log_dir(path, append | os.O_CREAT, max_bytes)
    try:
      info = os.fstat(fd)
      if stat.S_ISCHR(info.st_mode):
        if hasattr(os, "set_blocking"):
          os.set_blocking(fd, True)
        return fd
      _check_own_file(info, path)
      if shared and info.st_nlink != 1:
        raise PermissionError(
            f"Refusing BQAA tracing file {path}: it has other hard links."
        )
      if info.st_mode & 0o077 and hasattr(os, "fchmod"):
        os.fchmod(fd, 0o600)
    except BaseException:
      os.close(fd)
      raise
    return fd


def log_to_file(config: "BQAAConfig", message: str) -> None:
  """Append a single log line to ``config.log_file``. Silent on failure."""
  if not config.log_file:
    return
  try:
    fd = _open_log(config)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
      handle.write(f"[{iso_timestamp()}] {message}\n")
  except (OSError, RuntimeError):  # RuntimeError: "~" with no home dir.
    pass


def open_existing_private_dir(path: str | os.PathLike[str]) -> PrivateDir:
  """Open an existing owner-only dir without creating it."""
  directory = Path(path).expanduser()
  if not _dir_fd_supported():
    _check_dir(os.lstat(directory), directory)
    _require_trusted_chain(directory.parent)
    return PrivateDir(directory, None)
  return _open_private_dir(directory, directory, None)


@contextlib.contextmanager
def open_destination_spool_dir(
    base_spool_dir: str | os.PathLike[str],
    project_id: str,
    dataset: str,
    table: str = "agent_events",
    *,
    create: bool = True,
) -> Iterator[PrivateDir]:
  """Open (optionally creating) the verified per-destination spool subdir."""
  from .config import destination_partition_id

  root_path = Path(base_spool_dir).expanduser()
  part = destination_partition_id(project_id, dataset, table)
  root = (
      ensure_private_dir(root_path)
      if create
      else open_existing_private_dir(root_path)
  )
  if not part or root_path.name == part:
    with root:
      yield root
    return
  with root:
    if create:
      sub = root.subdir(part)
    elif root.fd is None:
      sub = open_existing_private_dir(root_path / part)
    else:
      sub = _open_private_dir(root_path / part, part, root.fd)
    with sub:
      yield sub


def extract_envelope_config(data: dict[str, Any]) -> dict[str, Any]:
  """Return destination config dict from a spool envelope."""
  cfg = data.get("config")
  if isinstance(cfg, dict) and any(
      cfg.get(k) for k in ("project_id", "dataset", "table")
  ):
    return cfg
  bqaa = data.get("_bqaa")
  if isinstance(bqaa, dict) and isinstance(bqaa.get("config"), dict):
    return bqaa["config"]
  return cfg if isinstance(cfg, dict) else {}


def _norm_dest_value(value: Any) -> str:
  if value is None:
    return ""
  if isinstance(value, str):
    return value.strip()
  return str(value).strip()


def has_envelope_destination(envelope_cfg: dict[str, Any]) -> bool:
  """Return True if ``envelope_cfg`` specifies any non-empty destination field."""
  return any(
      bool(_norm_dest_value(envelope_cfg.get(k)))
      for k in ("project_id", "dataset", "table")
  )


def is_destination_mismatch_for(
    envelope_cfg: dict[str, Any],
    project_id: str,
    dataset: str,
    table: str = "agent_events",
) -> bool:
  """Return True if an envelope embeds a different non-empty destination."""
  exp_proj = _norm_dest_value(project_id)
  exp_ds = _norm_dest_value(dataset)
  exp_tbl = _norm_dest_value(table) or "agent_events"
  for key, expected in (
      ("project_id", exp_proj),
      ("dataset", exp_ds),
      ("table", exp_tbl),
  ):
    val = _norm_dest_value(envelope_cfg.get(key))
    if not val:
      continue
    if val != expected:
      return True
  return False


def _inspect_single_spool_dir(
    spool: PrivateDir,
    dir_path: Path,
    status: dict[str, Any],
    latest_mtime: int,
) -> tuple[list[str], int]:
  names = spool.names()
  pending_names = [
      n for n in names if n.startswith("event-") and n.endswith(".json")
  ]
  status["pending_count"] += len(pending_names)

  retry_names = [n for n in names if n.endswith(".retry")]
  status["deferred_count"] += len(retry_names)
  for rname in retry_names:
    try:
      rinfo = spool.lstat(rname)
      if not stat.S_ISREG(rinfo.st_mode):
        continue
      fd = spool.open(rname, os.O_RDONLY)
      with os.fdopen(fd, "r", encoding="utf-8", errors="replace") as handle:
        meta = safe_json_loads(handle.read(), {})
      if isinstance(meta, dict):
        err = str(meta.get("last_error") or "").strip()
        if err and rinfo.st_mtime_ns >= latest_mtime:
          latest_mtime = rinfo.st_mtime_ns
          status["latest_error"] = err
    except OSError:
      continue

  if spool.exists("dead-letter"):
    try:
      dinfo = spool.lstat("dead-letter")
      if stat.S_ISDIR(dinfo.st_mode):
        dead_dir = (
            open_existing_private_dir(dir_path / "dead-letter")
            if spool.fd is None
            else _open_private_dir(
                dir_path / "dead-letter", "dead-letter", spool.fd
            )
        )
        with dead_dir as dead:
          for dname in dead.names():
            if dname == "destination-mismatch":
              try:
                minfo = dead.lstat(dname)
                if stat.S_ISDIR(minfo.st_mode):
                  mm_dir = (
                      open_existing_private_dir(
                          dir_path / "dead-letter" / dname
                      )
                      if dead.fd is None
                      else _open_private_dir(
                          dir_path / "dead-letter" / dname, dname, dead.fd
                      )
                  )
                  with mm_dir as mm:
                    mm_files = [
                        m for m in mm.names() if m.endswith(".json")
                    ]
                    status["destination_mismatch_count"] += len(mm_files)
                    status["dead_letter_count"] += len(mm_files)
              except OSError:
                pass
            elif dname.endswith(".json"):
              status["dead_letter_count"] += 1
    except OSError as exc:
      if not status["error"]:
        status["error"] = str(exc)

  return names, latest_mtime


def inspect_spool_status(spool_dir: str | os.PathLike[str]) -> dict[str, Any]:
  """Read-only summary of pending, deferred, and dead-letter spool counts.

  Inspects both the root spool directory and all ``dest-*`` destination
  partitions without creating directories or following unsafe symlinks.
  """
  status: dict[str, Any] = {
      "pending_count": 0,
      "deferred_count": 0,
      "dead_letter_count": 0,
      "destination_mismatch_count": 0,
      "latest_error": None,
      "error": None,
  }
  if not spool_dir:
    return status
  base = Path(spool_dir).expanduser()
  if not os.path.lexists(base):
    return status
  try:
    with open_existing_private_dir(base) as spool:
      latest_mtime = -1
      names, latest_mtime = _inspect_single_spool_dir(
          spool, base, status, latest_mtime
      )
      for entry in sorted(names):
        if not entry.startswith("dest-"):
          continue
        try:
          part_dir = (
              open_existing_private_dir(base / entry)
              if spool.fd is None
              else _open_private_dir(base / entry, entry, spool.fd)
          )
          with part_dir as part_spool:
            _, latest_mtime = _inspect_single_spool_dir(
                part_spool, base / entry, status, latest_mtime
            )
        except (OSError, RuntimeError) as exc:
          if not status["error"]:
            status["error"] = str(exc)
  except (OSError, RuntimeError) as exc:
    status["error"] = str(exc)
  return status


_MAX_LEGACY_FILE_BYTES = 10 * 1024 * 1024  # 10 MiB cap per legacy JSON file.
_MAX_LEGACY_UNSAFE_ENTRIES = 10


def _default_temp_root() -> str:
  for var in ("TMPDIR", "TEMP", "TMP"):
    val = os.environ.get(var)
    if val:
      return val
  return tempfile.gettempdir()


def _legacy_user_tag() -> str:
  getuid = getattr(os, "getuid", None)
  if getuid is not None:
    return str(getuid())
  try:
    name = getpass.getuser()
  except Exception:
    name = ""
  tag = "".join(ch for ch in name if ch.isalnum() or ch in "._-")
  return tag or f"pid{os.getpid()}"


def legacy_default_state_dir() -> Path:
  """Return the v0.1.0 shared default state directory path."""
  explicit = os.environ.get("BQAA_LEGACY_STATE_DIR", "").strip()
  if explicit:
    return Path(explicit).expanduser()
  return Path(_default_temp_root()) / "bqaa-agent-tracing"


def legacy_default_spool_dir() -> Path:
  """Return the v0.1.0 shared default spool directory path."""
  return legacy_default_state_dir() / "spool"


def _is_default_user_dir(
    path: str | os.PathLike[str] | None, *, is_spool: bool
) -> bool:
  if path is None:
    return True
  target = os.path.abspath(os.path.expanduser(str(path)))
  tag = f"bqaa-agent-tracing-{_legacy_user_tag()}"
  for root in {tempfile.gettempdir(), _default_temp_root()}:
    candidate = os.path.join(root, tag, "spool") if is_spool else os.path.join(root, tag)
    if target == os.path.abspath(candidate):
      return True
  return False


def resolve_legacy_state_dir(
    state_dir: str | os.PathLike[str] | None = None,
    *,
    legacy_state_dir: str | os.PathLike[str] | None = None,
) -> Path | None:
  """Return the legacy state dir if migration should inspect it, else None."""
  if legacy_state_dir is not None:
    candidate = Path(legacy_state_dir).expanduser()
  elif os.environ.get("BQAA_LEGACY_STATE_DIR", "").strip():
    candidate = legacy_default_state_dir()
  elif (
      not os.environ.get("BQAA_STATE_DIR", "").strip()
      and _is_default_user_dir(state_dir, is_spool=False)
  ):
    candidate = legacy_default_state_dir()
  else:
    return None
  if state_dir is not None and os.path.abspath(
      os.path.expanduser(str(state_dir))
  ) == os.path.abspath(str(candidate)):
    return None
  return candidate


def resolve_legacy_spool_dir(
    spool_dir: str | os.PathLike[str] | None = None,
    *,
    legacy_spool_dir: str | os.PathLike[str] | None = None,
) -> Path | None:
  """Return the legacy spool dir if migration should inspect it, else None."""
  if legacy_spool_dir is not None:
    candidate = Path(legacy_spool_dir).expanduser()
  elif os.environ.get("BQAA_LEGACY_STATE_DIR", "").strip():
    candidate = legacy_default_spool_dir()
  elif (
      not os.environ.get("BQAA_SPOOL_DIR", "").strip()
      and _is_default_user_dir(spool_dir, is_spool=True)
  ):
    candidate = legacy_default_spool_dir()
  else:
    return None
  if spool_dir is not None and os.path.abspath(
      os.path.expanduser(str(spool_dir))
  ) == os.path.abspath(str(candidate)):
    return None
  return candidate


def _open_verified_legacy_dir(path: str | os.PathLike[str]) -> PrivateDir:
  """Open a legacy directory securely via O_NOFOLLOW + owner/mode checks."""
  return open_existing_private_dir(path)


def _open_verified_legacy_spool_dir(legacy_spool: Path) -> PrivateDir:
  """Open legacy spool dir after verifying its parent when nested under state."""
  if legacy_spool.name == "spool" and os.path.lexists(legacy_spool.parent):
    with _open_verified_legacy_dir(legacy_spool.parent) as parent:
      if parent.fd is not None:
        return _open_private_dir(legacy_spool, "spool", parent.fd)
  return _open_verified_legacy_dir(legacy_spool)


def _check_legacy_file_stat(
    info: os.stat_result, path: Path, *, max_bytes: int = _MAX_LEGACY_FILE_BYTES
) -> None:
  euid = _euid()
  if not stat.S_ISREG(info.st_mode):
    problem = "is not a regular file"
  elif euid is not None and info.st_uid != euid:
    problem = f"is owned by uid {info.st_uid}, not {euid}"
  elif info.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
    problem = "is writable by its group or other users"
  elif info.st_nlink != 1:
    problem = f"has {info.st_nlink} hard links (expected 1)"
  elif info.st_size > max_bytes:
    problem = f"exceeds max size ({info.st_size} > {max_bytes} bytes)"
  else:
    return
  raise PermissionError(
      f"Refusing legacy BQAA tracing file {path}: it {problem}."
  )


def _read_verified_legacy_file(
    directory: PrivateDir,
    name: str,
    *,
    max_bytes: int = _MAX_LEGACY_FILE_BYTES,
) -> bytes:
  """Read ``name`` inside ``directory`` only if owned, regular, non-symlink, single-link, non-writable by group/other."""
  if not name or "/" in name or "\\" in name or name in (".", ".."):
    raise PermissionError(f"Refusing unsafe legacy filename: {name!r}")
  target, at = directory._at(name)
  try:
    fd = _open_nofollow(target, os.O_RDONLY, 0o600, **at)
  except OSError as exc:
    if exc.errno in (errno.ELOOP, errno.EMLINK):
      raise PermissionError(
          f"Refusing legacy BQAA tracing file {directory.path / name}: it is a"
          " symlink."
      ) from exc
    raise
  try:
    info = os.fstat(fd)
    _check_legacy_file_stat(info, directory.path / name, max_bytes=max_bytes)
    with os.fdopen(fd, "rb", closefd=True) as handle:
      fd = -1
      data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
      raise PermissionError(
          f"Refusing legacy BQAA tracing file {directory.path / name}: exceeds"
          f" max size ({max_bytes} bytes)."
      )
    return data
  finally:
    if fd >= 0:
      os.close(fd)


def _same_private_dir(a: PrivateDir, b: PrivateDir) -> bool:
  if a.fd is not None and b.fd is not None:
    sa, sb = os.fstat(a.fd), os.fstat(b.fd)
    return (sa.st_dev, sa.st_ino) == (sb.st_dev, sb.st_ino)
  return os.path.abspath(str(a.path)) == os.path.abspath(str(b.path))


def _write_atomic_private_bytes(
    directory: PrivateDir, name: str, data: bytes
) -> None:
  tmp_name = f".migrate.{name}.{os.getpid()}.tmp"
  fd = directory.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
  try:
    if hasattr(os, "fchmod"):
      os.fchmod(fd, 0o600)
    view = memoryview(data)
    while view:
      written = os.write(fd, view)
      view = view[written:]
    os.fsync(fd)
  except BaseException:
    os.close(fd)
    try:
      directory.unlink(tmp_name)
    except OSError:
      pass
    raise
  os.close(fd)
  directory.replace(tmp_name, name)


def _read_private_bytes(directory: PrivateDir, name: str) -> bytes | None:
  try:
    fd = directory.open(name, os.O_RDONLY)
  except FileNotFoundError:
    return None
  except OSError:
    return None
  with os.fdopen(fd, "rb", closefd=True) as handle:
    return handle.read(_MAX_LEGACY_FILE_BYTES + 1)


def migrate_legacy_session_state(
    dst_dir: PrivateDir,
    safe_key: str,
    *,
    legacy_state_dir: str | os.PathLike[str] | None = None,
) -> int:
  """Safely migrate owned v0.1.0 ``state_<key>.json`` and ``tool_<key>_*.json`` into ``dst_dir``."""
  legacy_path = resolve_legacy_state_dir(
      dst_dir.path, legacy_state_dir=legacy_state_dir
  )
  if legacy_path is None or not os.path.lexists(legacy_path):
    return 0
  try:
    legacy = _open_verified_legacy_dir(legacy_path)
  except (OSError, RuntimeError):
    return 0
  migrated = 0
  with legacy:
    if _same_private_dir(legacy, dst_dir):
      return 0
    tool_prefix = f"tool_{safe_key}_"
    candidates = [f"state_{safe_key}.json"]
    try:
      for entry in sorted(legacy.names()):
        if entry.startswith(tool_prefix) and entry.endswith(".json"):
          candidates.append(entry)
    except OSError:
      return 0
    existing_candidates = [name for name in candidates if legacy.exists(name)]
    if not existing_candidates:
      return 0
    lock_fd = dst_dir.open(".migrate.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
      fcntl.flock(lock_fd, fcntl.LOCK_EX)
      for fname in existing_candidates:
        if not legacy.exists(fname):
          continue
        try:
          raw = _read_verified_legacy_file(legacy, fname)
          text = raw.decode("utf-8", "strict")
        except (OSError, UnicodeDecodeError):
          continue
        parsed = safe_json_loads(text, None)
        if not isinstance(parsed, dict):
          continue
        if dst_dir.exists(fname):
          cur_bytes = _read_private_bytes(dst_dir, fname)
          if cur_bytes == raw:
            try:
              legacy.unlink(fname)
            except OSError:
              pass
          continue
        _write_atomic_private_bytes(dst_dir, fname, raw)
        try:
          legacy.unlink(fname)
        except FileNotFoundError:
          pass
        migrated += 1
    finally:
      try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
      except OSError:
        pass
      try:
        os.close(lock_fd)
      except OSError:
        pass
  return migrated


def migrate_legacy_spool(
    spool_dir: str | os.PathLike[str],
    *,
    legacy_spool_dir: str | os.PathLike[str] | None = None,
    destination: tuple[str, str, str] | None = None,
) -> dict[str, Any]:
  """Safely import owned v0.1.0 spooled envelopes into ``spool_dir`` without duplicates.

  When ``destination=(project_id, dataset, table)`` has non-empty
  ``project_id`` and ``dataset``, only envelopes matching that destination
  (or carrying no destination fields) are migrated into that destination's
  partition; envelopes targeting other destinations stay untouched in the
  legacy spool so their own sessions/drainers can claim them. When
  ``destination`` is unset or incomplete, migration is skipped so
  multi-destination legacy files are never dumped into the shared root spool.
  """
  result: dict[str, Any] = {
      "migrated": 0,
      "unsafe_skipped": 0,
      "unsafe_entries": [],
      "skipped_reasons": {},
      "error": None,
  }
  dest_proj = dest_ds = dest_tbl = ""
  if destination is not None:
    raw_proj, raw_ds, raw_tbl = destination
    dest_proj = _norm_dest_value(raw_proj)
    dest_ds = _norm_dest_value(raw_ds)
    dest_tbl = _norm_dest_value(raw_tbl) or "agent_events"
  has_dest = bool(dest_proj and dest_ds)
  if not has_dest:
    return result

  legacy_path = resolve_legacy_spool_dir(
      spool_dir, legacy_spool_dir=legacy_spool_dir
  )
  if legacy_path is None or not os.path.lexists(legacy_path):
    return result
  try:
    legacy = _open_verified_legacy_spool_dir(legacy_path)
  except (OSError, RuntimeError) as exc:
    result["error"] = str(exc)
    return result

  with legacy:
    try:
      event_names = sorted(
          n for n in legacy.names() if fnmatch.fnmatchcase(n, "event-*.json")
      )
    except OSError as exc:
      result["error"] = str(exc)
      return result
    if not event_names:
      return result

    try:
      legacy_pid_fd = legacy.open(
          ".drainer.pid", os.O_CREAT | os.O_RDWR, 0o600
      )
    except OSError as exc:
      result["error"] = str(exc)
      return result

    try:
      try:
        fcntl.flock(legacy_pid_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
      except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
          return result
        result["error"] = str(exc)
        return result

      try:
        try:
          event_names = sorted(
              n
              for n in legacy.names()
              if fnmatch.fnmatchcase(n, "event-*.json")
          )
        except OSError as exc:
          result["error"] = str(exc)
          return result
        if not event_names:
          return result

        with open_destination_spool_dir(
            spool_dir, dest_proj, dest_ds, dest_tbl, create=True
        ) as dst:
          if _same_private_dir(legacy, dst):
            return result
          lock_fd = dst.open(".migrate.lock", os.O_CREAT | os.O_RDWR, 0o600)
          try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            for name in event_names:
              if not legacy.exists(name):
                continue
              try:
                raw = _read_verified_legacy_file(legacy, name)
                parsed = safe_json_loads(raw.decode("utf-8", "strict"), None)
              except (OSError, UnicodeDecodeError) as exc:
                result["unsafe_skipped"] += 1
                reason = str(exc)
                result["skipped_reasons"][name] = reason
                if len(result["unsafe_entries"]) < _MAX_LEGACY_UNSAFE_ENTRIES:
                  result["unsafe_entries"].append(
                      f"{legacy_path / name}: {reason}"
                  )
                continue
              if not isinstance(parsed, dict):
                result["unsafe_skipped"] += 1
                reason = "not a JSON object envelope"
                result["skipped_reasons"][name] = reason
                if len(result["unsafe_entries"]) < _MAX_LEGACY_UNSAFE_ENTRIES:
                  result["unsafe_entries"].append(
                      f"{legacy_path / name}: {reason}"
                  )
                continue
              if has_dest and is_destination_mismatch_for(
                  extract_envelope_config(parsed), dest_proj, dest_ds, dest_tbl
              ):
                continue

              retry_name = f"{name}.retry"
              retry_bytes: bytes | None = None
              if legacy.exists(retry_name):
                try:
                  retry_bytes = _read_verified_legacy_file(legacy, retry_name)
                except OSError as exc:
                  result["unsafe_skipped"] += 1
                  reason = f"unsafe retry sidecar: {exc}"
                  result["skipped_reasons"][name] = reason
                  if (
                      len(result["unsafe_entries"])
                      < _MAX_LEGACY_UNSAFE_ENTRIES
                  ):
                    result["unsafe_entries"].append(
                        f"{legacy_path / retry_name}: {reason}"
                    )
                  continue

              dst_name = name
              if dst.exists(dst_name):
                existing = _read_private_bytes(dst, dst_name)
                if existing == raw:
                  try:
                    legacy.unlink(name)
                  except FileNotFoundError:
                    pass
                  if retry_bytes is not None:
                    try:
                      legacy.unlink(retry_name)
                    except (FileNotFoundError, OSError):
                      pass
                  continue
                digest = hashlib.sha256(raw).hexdigest()[:12]
                dst_name = f"event-legacy-{digest}-{name[len('event-'):]}"
                if dst.exists(dst_name):
                  existing_dedup = _read_private_bytes(dst, dst_name)
                  if existing_dedup == raw:
                    try:
                      legacy.unlink(name)
                    except FileNotFoundError:
                      pass
                    if retry_bytes is not None:
                      try:
                        legacy.unlink(retry_name)
                      except (FileNotFoundError, OSError):
                        pass
                    continue

              dst_retry_name = f"{dst_name}.retry"
              wrote_retry = False
              if retry_bytes is not None and not dst.exists(dst_retry_name):
                _write_atomic_private_bytes(dst, dst_retry_name, retry_bytes)
                wrote_retry = True
              try:
                _write_atomic_private_bytes(dst, dst_name, raw)
              except OSError:
                if wrote_retry:
                  try:
                    dst.unlink(dst_retry_name)
                  except OSError:
                    pass
                raise
              try:
                legacy.unlink(name)
              except FileNotFoundError:
                pass
              if retry_bytes is not None:
                try:
                  legacy.unlink(retry_name)
                except (FileNotFoundError, OSError):
                  pass
              result["migrated"] += 1
          finally:
            try:
              fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
              pass
            try:
              os.close(lock_fd)
            except OSError:
              pass
      finally:
        try:
          fcntl.flock(legacy_pid_fd, fcntl.LOCK_UN)
        except OSError:
          pass
    finally:
      try:
        os.close(legacy_pid_fd)
      except OSError:
        pass
  return result


def _inspect_legacy_file_safe(
    directory: PrivateDir, name: str, *, require_json_dict: bool = False
) -> str | None:
  """Return None if ``name`` passes legacy security checks, else reason string."""
  try:
    raw = _read_verified_legacy_file(directory, name)
    if require_json_dict:
      parsed = safe_json_loads(raw.decode("utf-8", "strict"), None)
      if not isinstance(parsed, dict):
        return "not a JSON object"
  except (OSError, UnicodeDecodeError) as exc:
    return str(exc)
  return None


def inspect_legacy_backlog(
    state_dir: str | os.PathLike[str] | None = None,
    spool_dir: str | os.PathLike[str] | None = None,
    *,
    legacy_state_dir: str | os.PathLike[str] | None = None,
    legacy_spool_dir: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
  """Read-only summary of remaining or unsafe v0.1.0 legacy state and spool files."""
  legacy_state = resolve_legacy_state_dir(
      state_dir, legacy_state_dir=legacy_state_dir
  )
  legacy_spool = resolve_legacy_spool_dir(
      spool_dir, legacy_spool_dir=legacy_spool_dir
  )
  summary: dict[str, Any] = {
      "legacy_dir": str(legacy_state) if legacy_state else None,
      "legacy_spool_dir": str(legacy_spool) if legacy_spool else None,
      "state_count": 0,
      "pending_count": 0,
      "deferred_count": 0,
      "dead_letter_count": 0,
      "unsafe_count": 0,
      "unsafe_entries": [],
      "error": None,
  }

  def _record_unsafe(entry_path: Path, reason: str) -> None:
    summary["unsafe_count"] += 1
    if len(summary["unsafe_entries"]) < _MAX_LEGACY_UNSAFE_ENTRIES:
      summary["unsafe_entries"].append(f"{entry_path}: {reason}")

  state_checked_spool = False
  if legacy_state is not None and os.path.lexists(legacy_state):
    try:
      with _open_verified_legacy_dir(legacy_state) as lstate:
        for name in sorted(lstate.names()):
          if (
              name.startswith("state_") or name.startswith("tool_")
          ) and name.endswith(".json"):
            bad = _inspect_legacy_file_safe(
                lstate, name, require_json_dict=True
            )
            if bad is None:
              summary["state_count"] += 1
            else:
              _record_unsafe(legacy_state / name, bad)
          elif (
              name == "spool"
              and legacy_spool is not None
              and os.path.abspath(str(legacy_spool))
              == os.path.abspath(str(legacy_state / "spool"))
          ):
            state_checked_spool = True
            _inspect_legacy_spool_dir(
                legacy_spool, summary, _record_unsafe, parent=lstate
            )
    except (OSError, RuntimeError) as exc:
      summary["error"] = str(exc)
      return summary

  if (
      not state_checked_spool
      and legacy_spool is not None
      and os.path.lexists(legacy_spool)
  ):
    _inspect_legacy_spool_dir(legacy_spool, summary, _record_unsafe)
  return summary


def _inspect_legacy_spool_dir(
    legacy_spool: Path,
    summary: dict[str, Any],
    record_unsafe: Any,
    *,
    parent: PrivateDir | None = None,
) -> None:
  try:
    if parent is not None and parent.fd is not None:
      spool_ctx = _open_private_dir(legacy_spool, "spool", parent.fd)
    else:
      spool_ctx = _open_verified_legacy_spool_dir(legacy_spool)
    with spool_ctx as lspool:
      for name in sorted(lspool.names()):
        if fnmatch.fnmatchcase(name, "event-*.json"):
          bad = _inspect_legacy_file_safe(
              lspool, name, require_json_dict=True
          )
          if bad is None:
            summary["pending_count"] += 1
          else:
            record_unsafe(legacy_spool / name, bad)
        elif name.endswith(".retry"):
          bad = _inspect_legacy_file_safe(lspool, name)
          if bad is None:
            summary["deferred_count"] += 1
          else:
            record_unsafe(legacy_spool / name, bad)
        elif name == "dead-letter":
          try:
            dinfo = lspool.lstat("dead-letter")
            if not stat.S_ISDIR(dinfo.st_mode):
              record_unsafe(
                  legacy_spool / "dead-letter", "is not a directory"
              )
              continue
            dead_dir = (
                _open_verified_legacy_dir(legacy_spool / "dead-letter")
                if lspool.fd is None
                else _open_private_dir(
                    legacy_spool / "dead-letter", "dead-letter", lspool.fd
                )
            )
            with dead_dir as dead:
              for dname in dead.names():
                if dname == "destination-mismatch":
                  try:
                    minfo = dead.lstat(dname)
                    if not stat.S_ISDIR(minfo.st_mode):
                      record_unsafe(
                          legacy_spool / "dead-letter" / dname,
                          "is not a directory",
                      )
                      continue
                    mm_dir = (
                        _open_verified_legacy_dir(
                            legacy_spool / "dead-letter" / dname
                        )
                        if dead.fd is None
                        else _open_private_dir(
                            legacy_spool / "dead-letter" / dname,
                            dname,
                            dead.fd,
                        )
                    )
                    with mm_dir as mm:
                      summary["dead_letter_count"] += sum(
                          1 for m in mm.names() if m.endswith(".json")
                      )
                  except (OSError, RuntimeError) as exc:
                    record_unsafe(
                        legacy_spool / "dead-letter" / dname, str(exc)
                    )
                elif dname.endswith(".json"):
                  summary["dead_letter_count"] += 1
          except (OSError, RuntimeError) as exc:
            record_unsafe(legacy_spool / "dead-letter", str(exc))
  except (OSError, RuntimeError) as exc:
    if not summary["error"]:
      summary["error"] = str(exc)
