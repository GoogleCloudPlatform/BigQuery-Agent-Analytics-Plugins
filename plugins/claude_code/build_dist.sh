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

# Builds or verifies the tracing plugin's marketplace distribution under
# plugins/claude_code_dist/bigquery-agent-analytics-tracing/.
#
# Usage:
#   bash plugins/claude_code/build_dist.sh OUT_DIR [VERSION]
#   bash plugins/claude_code/build_dist.sh --check
#
# In --check mode, verifies:
#   - plugins/claude_code_dist/bigquery-agent-analytics-tracing/.claude-plugin/plugin.json
#     has an X.Y.Z release version matching .claude-plugin/marketplace.json,
#     vendor/bigquery_agent_analytics_tracing-X.Y.Z.dist-info/METADATA, and
#     the manual install VERSION=X.Y.Z snippet in README.md;
#   - plugins/claude_code/.claude-plugin/plugin.json is either "0.0.0+local"
#     or X.Y.Z >= dist version;
#   - shared files (hooks, commands, scripts, MARKETPLACE.md, README.md with
#     depth-adjusted marketplace link) and non-version plugin.json fields
#     match identically between plugins/claude_code and dist (including
#     executable bits on all .sh scripts), with zero Python bytecode in dist;
#   - if git checkout is full (fails on shallow clones so release tags are
#     never skipped), and release tags tracing-v* exist:
#     * if tracing-v<dist_version> already exists, dist/ must have zero diff
#       against that tag (editing dist after release requires a version bump);
#     * if tracing-v<dist_version> does not exist yet, dist_version must be
#       strictly greater than the highest existing tracing-vX.Y.Z tag;
#     * any tracing-dist-v* tags agree on their version across dist and catalog.
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
dist="$repo/plugins/claude_code_dist/bigquery-agent-analytics-tracing"
SHARED_ITEMS=(hooks commands scripts MARKETPLACE.md README.md)

version_of() {
  python3 -B - "$1/.claude-plugin/plugin.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["version"])
PY
}

strip_bytecode() {
  python3 -B - "$1" <<'PY'
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

build() {
  local src="$1"
  local out="$2"
  local target_version="${3:-}"
  local tmp_stage
  tmp_stage="$(mktemp -d)"

  if [[ -z "$target_version" ]]; then
    if [[ -f "$out/.claude-plugin/plugin.json" ]]; then
      target_version="$(version_of "$out")"
    elif [[ -f "$dist/.claude-plugin/plugin.json" ]]; then
      target_version="$(version_of "$dist")"
    else
      target_version="$(version_of "$src")"
    fi
  fi

  mkdir -p "$tmp_stage/.claude-plugin"
  for item in "${SHARED_ITEMS[@]}"; do
    cp -Rp "$src/$item" "$tmp_stage/$item"
  done

  # Adjust relative link depth to .claude-plugin/marketplace.json between
  # plugins/claude_code/README.md (depth 2) and plugins/claude_code_dist/<name>/README.md (depth 3).
  python3 -B - "$tmp_stage/README.md" <<'PY'
import pathlib, sys
readme = pathlib.Path(sys.argv[1])
text = readme.read_text(encoding="utf-8")
updated = text.replace(
    "(../../.claude-plugin/marketplace.json)",
    "(../../../.claude-plugin/marketplace.json)",
)
if updated != text:
    readme.write_text(updated, encoding="utf-8")
PY

  # Copy non-version manifest fields from source, stamping target_version.
  python3 -B - "$src/.claude-plugin/plugin.json" "$tmp_stage/.claude-plugin/plugin.json" "$target_version" <<'PY'
import json, pathlib, sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
manifest["version"] = sys.argv[3]
pathlib.Path(sys.argv[2]).write_text(
    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
)
PY

  # Preserve vendor/ from existing out dir if present, else committed dist/vendor.
  if [[ -d "$out/vendor" ]]; then
    cp -Rp "$out/vendor" "$tmp_stage/vendor"
  elif [[ -d "$dist/vendor" ]]; then
    cp -Rp "$dist/vendor" "$tmp_stage/vendor"
  fi
  strip_bytecode "$tmp_stage"

  rm -rf "$out"
  mkdir -p "$(dirname "$out")"
  mv "$tmp_stage" "$out"
}

same_tree() {
  python3 -B - "$1" "$2" <<'PY'
import os, stat, sys
def walk(root):
    found = {}
    for folder, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            if name.endswith((".pyc", ".pyo")):
                continue
            path = os.path.join(folder, name)
            rel = os.path.relpath(path, root)
            with open(path, "rb") as handle:
                found[rel] = (handle.read(), bool(os.lstat(path).st_mode & stat.S_IXUSR))
    return found

built, copy = walk(sys.argv[1]), walk(sys.argv[2])
differ = sorted(set(built) ^ set(copy)) + sorted(
    k for k in set(built) & set(copy) if built[k] != copy[k]
)
for name in differ[:30]:
    print(f"  differs: {name}")
sys.exit(1 if differ else 0)
PY
}

if [[ "${1:-}" != "--check" ]]; then
  out_dir="${1:?usage: build_dist.sh OUT_DIR [VERSION] | --check}"
  build "$here" "$out_dir" "${2:-}"
  exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

source_version="$(version_of "$here")"
dist_version="$(version_of "$dist")"
catalog_version="$(
  python3 -B - "$repo/.claude-plugin/marketplace.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print(next(p["version"] for p in data["plugins"] if p["name"] == "bigquery-agent-analytics-tracing"))
PY
)"

status=0

# 1. Version format and agreement across source, dist, catalog, METADATA, README.
python3 -B - "$source_version" "$dist_version" "$catalog_version" "$dist" "$here" <<'PY' || status=1
import pathlib, re, sys

source_ver, dist_ver, catalog_ver = sys.argv[1], sys.argv[2], sys.argv[3]
dist_dir = pathlib.Path(sys.argv[4])
src_dir = pathlib.Path(sys.argv[5])
errors = []

m_dist = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", dist_ver)
if not m_dist:
    errors.append(f"dist version {dist_ver!r} is not X.Y.Z")

if source_ver != "0.0.0+local":
    m_src = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", source_ver)
    if not m_src:
        errors.append(f"source version {source_ver!r} must be '0.0.0+local' or X.Y.Z")
    elif m_dist and tuple(int(x) for x in m_src.groups()) < tuple(int(x) for x in m_dist.groups()):
        errors.append(f"source version {source_ver} is lower than dist version {dist_ver}")

if catalog_ver != dist_ver:
    errors.append(f"catalog entry says {catalog_ver}, committed dist copy is {dist_ver}")

vendor_dir = dist_dir / "vendor"
dist_infos = sorted(vendor_dir.glob("bigquery_agent_analytics_tracing-*.dist-info")) if vendor_dir.is_dir() else []
expected_info = vendor_dir / f"bigquery_agent_analytics_tracing-{dist_ver}.dist-info"
if len(dist_infos) != 1 or dist_infos[0] != expected_info:
    found = [p.name for p in dist_infos]
    errors.append(f"expected single {expected_info.name}, found {found}")
else:
    meta = expected_info / "METADATA"
    if not meta.is_file():
        errors.append(f"missing {meta}")
    else:
        meta_text = meta.read_text(encoding="utf-8")
        if f"Version: {dist_ver}" not in meta_text.splitlines():
            errors.append(f"{meta} does not contain 'Version: {dist_ver}'")

for label, readme_path in (("source README.md", src_dir / "README.md"), ("dist README.md", dist_dir / "README.md")):
    if readme_path.is_file():
        text = readme_path.read_text(encoding="utf-8")
        vers = re.findall(r"(?m)^VERSION=([0-9]+\.[0-9]+\.[0-9]+)\s*$", text)
        if not vers:
            errors.append(f"{label} is missing VERSION=X.Y.Z install snippet")
        elif any(v != dist_ver for v in vers):
            errors.append(f"{label} VERSION={vers} does not match dist {dist_ver}")

for err in errors:
    print(f"not ok - {err}")
if not errors:
    print(f"ok - catalog, dist, source ({source_ver}), METADATA, and READMEs agree on {dist_ver}")
sys.exit(1 if errors else 0)
PY

# 2. Full source <-> dist tree parity (plus zero bytecode / executable scripts).
build "$here" "$work/built" "$dist_version"
if same_tree "$work/built" "$dist"; then
  echo "ok - committed dist matches what plugins/claude_code builds (files, contents, executable bits)"
else
  echo "not ok - committed dist differs from what plugins/claude_code builds"
  status=1
fi

# 3. Release tag immutability, zero tracked bytecode, and monotonic tag bump verification.
if git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  tracked_pyc="$(git -C "$repo" ls-files plugins/claude_code plugins/claude_code_dist/bigquery-agent-analytics-tracing | python3 -B -c 'import sys; bad = [l.strip() for l in sys.stdin if "__pycache__" in l or l.strip().endswith((".pyc", ".pyo"))]; print("\n".join(bad))')"
  if [[ -n "$tracked_pyc" ]]; then
    echo "not ok - tracked Python bytecode files in tracing plugin:"
    echo "$tracked_pyc"
    status=1
  fi
  if [[ "$(git -C "$repo" rev-parse --is-shallow-repository)" == true ]]; then
    echo "not ok - this checkout is shallow, so tracing-v tags cannot be checked: fetch them (fetch-depth: 0)"
    status=1
  else
    tag="tracing-v$dist_version"
    if git -C "$repo" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null; then
      if git -C "$repo" diff --quiet "$tag" -- plugins/claude_code_dist/bigquery-agent-analytics-tracing; then
        echo "ok - $tag exists and committed dist matches $tag"
      else
        echo "not ok - plugins/claude_code_dist/bigquery-agent-analytics-tracing changed since release tag $tag; bump dist version"
        status=1
      fi
    else
      tags_list="$(git -C "$repo" tag -l 'tracing-v*')"
      if [[ -n "$tags_list" ]]; then
        python3 -B - "$dist_version" "$tags_list" <<'PY' || status=1
import re, sys
dist_v = tuple(int(x) for x in sys.argv[1].split("."))
tag_vers = []
for raw in sys.argv[2].splitlines():
    m = re.fullmatch(r"tracing-v(\d+)\.(\d+)\.(\d+)", raw.strip())
    if m:
        tag_vers.append((tuple(int(x) for x in m.groups()), raw.strip()))
if tag_vers:
    max_v, max_tag = max(tag_vers)
    if dist_v <= max_v:
        print(f"not ok - dist version {sys.argv[1]} is not newer than existing tag {max_tag}")
        sys.exit(1)
    print(f"ok - unreleased dist version {sys.argv[1]} > highest tag {max_tag}")
else:
    print(f"ok - no release tracing-vX.Y.Z tags exist yet")
PY
      else
        echo "ok - no tracing-v release tags exist yet"
      fi
    fi

    for head_tag in $(git -C "$repo" tag --points-at HEAD 'tracing-v*'); do
      if [[ "$head_tag" != "$tag" ]]; then
        echo "not ok - HEAD carries release tag $head_tag which does not match dist version $dist_version ($tag)"
        status=1
      fi
    done
    if [[ "${GITHUB_REF_NAME:-}" == tracing-v* && "${GITHUB_REF_NAME:-}" != "$tag" ]]; then
      echo "not ok - GITHUB_REF_NAME=${GITHUB_REF_NAME} does not match dist version $dist_version ($tag)"
      status=1
    fi

    for dist_tag in $(git -C "$repo" tag -l 'tracing-dist-v*'); do
      want="${dist_tag#tracing-dist-v}"
      at_copy="$(
        git -C "$repo" show "$dist_tag:plugins/claude_code_dist/bigquery-agent-analytics-tracing/.claude-plugin/plugin.json" 2>/dev/null \
          | python3 -B -c 'import json, sys; print(json.load(sys.stdin)["version"])' 2>/dev/null || echo none
      )"
      at_catalog="$(
        git -C "$repo" show "$dist_tag:.claude-plugin/marketplace.json" 2>/dev/null \
          | python3 -B -c 'import json, sys; print(next(p["version"] for p in json.load(sys.stdin)["plugins"] if p["name"] == "bigquery-agent-analytics-tracing"))' 2>/dev/null || echo none
      )"
      if [[ "$at_copy" == "$want" && "$at_catalog" == "$want" ]]; then
        echo "ok - $dist_tag holds dist copy and catalog entry at $want"
      else
        echo "not ok - $dist_tag holds dist at $at_copy and catalog at $at_catalog, not $want"
        status=1
      fi
    done
  fi
fi

exit "$status"
