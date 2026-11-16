#!/usr/bin/env bash
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

# Builds the mod's released files into OUT_DIR: what the marketplace serves
# from plugins/claude_code_dist/bigquery-agent-analytics-mod, and what a
# mod-vX.Y.Z release archives. The tests, the example rows and the
# TypeScript config stay here; so does .claude-plugin/types, which the host
# CLI writes for the editor.
#   bash plugins/claude_code_mod/build_dist.sh OUT_DIR
#
# With --check, verifies the committed copy:
#   - the catalog entry's version is the copy's;
#   - the source's version is not lower than the copy's;
#   - the copy is exactly what the source at the copy's release tag,
#     mod-v<copy version>, builds with that tag's own build_dist.sh (as the
#     release workflow built its archive), whatever the source holds now:
#     the same files, contents and executable bits. Only while no mod-v tag
#     exists at all (before the first release) is it checked against what
#     the source builds now, which must be at the same version; once any
#     release is tagged, a copy whose own tag is missing fails;
#   - every mod-dist-v<version> tag, the ref an organization registers this
#     repository at, holds the copy and the catalog entry at <version>, and
#     (once any mod-v tag exists) every prior served version in first-parent
#     history has its mod-dist-v<version> tag.
# The tags and history need a full checkout (fetch-depth: 0): a shallow one
# fails unconditionally.
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
dist="$repo/plugins/claude_code_dist/bigquery-agent-analytics-mod"
FILES=(.claude-plugin/plugin.json hooks types sql setup.sh README.md QUICKSTART.md MIGRATION.md docs scripts)

build() { # source folder, out folder
  rm -rf "$2"
  mkdir -p "$2/.claude-plugin"
  for item in "${FILES[@]}"; do cp -Rp "$1/$item" "$2/$item"; done
  python3 -B - "$2" <<'PY'
import os, shutil, sys
for folder, dirs, files in os.walk(sys.argv[1], topdown=False):
    for name in files:
        if name.endswith((".pyc", ".pyo")):
            os.unlink(os.path.join(folder, name))
    for d in dirs:
        if d == "__pycache__":
            shutil.rmtree(os.path.join(folder, d))
PY
}
version_of() { python3 -B -c 'import json, sys; print(json.load(open(sys.argv[1]))["version"])' "$1/.claude-plugin/plugin.json"; }
# Whether the first version is lower than the second (X.Y.Z, numerically):
# 0 when it is, 1 when it is not, 2 when either is not X.Y.Z.
lower() {
  python3 -B - "$1" "$2" <<'PY'
import re, sys
def parse(v):
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", v)
    if not m:
        print(f"not a version: {v!r}", file=sys.stderr)
        sys.exit(2)
    return tuple(int(x) for x in m.groups())
sys.exit(0 if parse(sys.argv[1]) < parse(sys.argv[2]) else 1)
PY
}
# Whether two folders hold the same files, contents and executable bits
# (the host CLI's .claude-plugin/types and Python bytecode left out).
same_tree() {
  python3 -B - "$1" "$2" <<'PY'
import os, stat, sys
def walk(root):
    found = {}
    for folder, dirs, files in os.walk(root):
        rel_folder = os.path.relpath(folder, root)
        dirs[:] = [
            d for d in dirs
            if d != "__pycache__" and not (rel_folder == ".claude-plugin" and d == "types")
        ]
        for name in files:
            if name.endswith((".pyc", ".pyo")):
                continue
            path = os.path.join(folder, name)
            with open(path, "rb") as handle:
                found[os.path.relpath(path, root)] = (handle.read(), bool(os.lstat(path).st_mode & stat.S_IXUSR))
    return found
built, copy = walk(sys.argv[1]), walk(sys.argv[2])
differ = sorted(set(built) ^ set(copy)) + sorted(k for k in set(built) & set(copy) if built[k] != copy[k])
for name in differ[:20]:
    print(f"  differs: {name}")
sys.exit(1 if differ else 0)
PY
}

if [[ "${1:-}" != --check ]]; then
  build "$here" "${1:?usage: build_dist.sh OUT_DIR | --check}"
  exit 0
fi
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
source_version="$(version_of "$here")"
dist_version="$(version_of "$dist")"
catalog_version="$(python3 -B -c 'import json, sys; print(next(p["version"] for p in json.load(open(sys.argv[1]))["plugins"] if p["name"] == "bigquery-agent-analytics-mod"))' "$repo/.claude-plugin/marketplace.json")"
status=0
if [[ "$(git -C "$repo" rev-parse --is-shallow-repository 2>/dev/null || echo true)" == true ]]; then
  echo "not ok - this checkout is shallow, so release and mod-dist-v* history cannot be checked: fetch full history (fetch-depth: 0)"
  status=1
fi
if [[ "$catalog_version" != "$dist_version" ]]; then
  echo "not ok - the catalog entry says $catalog_version, the committed copy is $dist_version"
  status=1
else
  echo "ok - the catalog entry and the committed copy are both $dist_version"
fi
compared=0
lower "$source_version" "$dist_version" || compared=$?
if [[ "$compared" == 0 ]]; then
  echo "not ok - the source is at $source_version, lower than the released copy's $dist_version"
  status=1
elif [[ "$compared" != 1 ]]; then
  echo "not ok - the source's version $source_version and the copy's $dist_version are not both X.Y.Z"
  status=1
fi
tag="mod-v$dist_version"
if git -C "$repo" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null; then
  mkdir -p "$work/tagged"
  git -C "$repo" archive "$tag" plugins/claude_code_mod | tar -x -C "$work/tagged"
  # With the tag's own build_dist.sh: its list of released files, not this one's.
  if bash "$work/tagged/plugins/claude_code_mod/build_dist.sh" "$work/built" >/dev/null; then
    against="the source at $tag, built by its own build_dist.sh"
  else
    echo "not ok - the source at $tag does not build with its own build_dist.sh"
    rm -rf "$work/built"
    status=1
  fi
elif [[ -n "$(git -C "$repo" tag -l 'mod-v*')" ]]; then
  echo "not ok - there is no tag $tag to check the copy against, though other mod-v releases are tagged: a copy is checked against its own release"
  status=1
elif [[ "$source_version" == "$dist_version" ]]; then
  build "$here" "$work/built"
  against="the source now (no mod-v release is tagged yet)"
else
  echo "not ok - there is no tag $tag to check the copy against (the source is at $source_version; fetch the tags)"
  status=1
fi
if [[ -d "$work/built" ]]; then
  if same_tree "$work/built" "$dist"; then
    echo "ok - the committed copy is exactly what $against builds: files, contents and executable bits"
  else
    echo "not ok - the committed copy is not what $against builds"
    status=1
  fi
fi
while IFS= read -r dist_tag; do
  [[ -z "$dist_tag" ]] && continue
  want="${dist_tag#mod-dist-v}"
  if [[ ! "$want" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "not ok - $dist_tag is not a valid mod-dist-vX.Y.Z tag"
    status=1
    continue
  fi
  if ! git -C "$repo" merge-base --is-ancestor "refs/tags/$dist_tag^{commit}" HEAD 2>/dev/null; then
    echo "not ok - $dist_tag is not an ancestor of HEAD"
    status=1
    continue
  fi
  at_copy="$(git -C "$repo" show "$dist_tag:plugins/claude_code_dist/bigquery-agent-analytics-mod/.claude-plugin/plugin.json" 2>/dev/null \
    | python3 -B -c 'import json, sys; print(json.load(sys.stdin)["version"])' 2>/dev/null || echo none)"
  at_catalog="$(git -C "$repo" show "$dist_tag:.claude-plugin/marketplace.json" 2>/dev/null \
    | python3 -B -c 'import json, sys; print(next(p["version"] for p in json.load(sys.stdin)["plugins"] if p["name"] == "bigquery-agent-analytics-mod"))' 2>/dev/null || echo none)"
  if [[ "$at_copy" == "$want" && "$at_catalog" == "$want" ]]; then
    echo "ok - $dist_tag holds the copy and the catalog entry at $want"
  else
    echo "not ok - $dist_tag holds the copy at $at_copy and the catalog entry at $at_catalog, not $want: an organization pinned to it installs that"
    status=1
  fi
done < <(git -C "$repo" tag -l 'mod-dist-v*')
if [[ -n "$(git -C "$repo" tag -l 'mod-v*')" ]]; then
  if ! prior_list="$(python3 -B - "$repo" "$dist_version" <<'PY'
import json, re, subprocess, sys

repo, dist_version = sys.argv[1:3]
semver_re = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
m_dist = semver_re.match(dist_version)
if not m_dist:
    sys.exit(0)
dist_tuple = tuple(int(x) for x in m_dist.groups())

dist_rel = "plugins/claude_code_dist/bigquery-agent-analytics-mod/.claude-plugin/plugin.json"
catalog_rel = ".claude-plugin/marketplace.json"
log = subprocess.run(
    ["git", "-C", repo, "log", "--first-parent", "--format=%H", "--", dist_rel, catalog_rel],
    capture_output=True, text=True, check=False,
)
if log.returncode != 0:
    print(log.stderr.strip(), file=sys.stderr)
    sys.exit(1)

both_matched = {}
dist_only = {}
closed_versions = set()
active_ver = None
for sha in log.stdout.splitlines():
    sha = sha.strip()
    if not sha:
        continue
    show_dist = subprocess.run(
        ["git", "-C", repo, "show", f"{sha}:{dist_rel}"],
        capture_output=True, text=True, check=False,
    )
    if show_dist.returncode != 0:
        continue
    try:
        ver = json.loads(show_dist.stdout).get("version", "")
    except Exception:
        continue
    m = semver_re.match(ver)
    if not m:
        continue
    if active_ver is not None and ver != active_ver:
        closed_versions.add(active_ver)
    active_ver = ver
    ver_tuple = tuple(int(x) for x in m.groups())
    if ver_tuple >= dist_tuple or ver in closed_versions:
        continue
    dist_only[ver] = sha
    show_cat = subprocess.run(
        ["git", "-C", repo, "show", f"{sha}:{catalog_rel}"],
        capture_output=True, text=True, check=False,
    )
    if show_cat.returncode == 0:
        try:
            plugins = json.loads(show_cat.stdout).get("plugins", [])
            cat_ver = next(
                (p.get("version") for p in plugins if isinstance(p, dict) and p.get("name") == "bigquery-agent-analytics-mod"),
                None,
            )
            if cat_ver == ver:
                both_matched[ver] = sha
        except Exception:
            pass

for ver in sorted(dist_only, key=lambda v: tuple(int(x) for x in v.split("."))):
    sha = both_matched.get(ver) or dist_only[ver]
    print(f"{ver}\t{sha}")
PY
  )"; then
    echo "not ok - failed to inspect prior served dist history"
    status=1
  else
    while IFS=$'\t' read -r prior_ver prior_sha; do
      [[ -z "$prior_ver" ]] && continue
      if ! git -C "$repo" rev-parse -q --verify "refs/tags/mod-dist-v$prior_ver^{commit}" >/dev/null; then
        echo "not ok - missing tag mod-dist-v$prior_ver for previously served dist commit $prior_sha; run: git tag -a mod-dist-v$prior_ver -m \"mod $prior_ver, served copy\" $prior_sha"
        status=1
      fi
    done <<< "$prior_list"
  fi
fi
exit "$status"
