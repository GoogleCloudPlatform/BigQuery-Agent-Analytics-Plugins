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
#   bash plugins/claude_code/build_dist.sh --check [--sdk-repo PATH_OR_URL]
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
#     never skipped), discovers tracing-v* release tags from BQAA_SDK_REPO /
#     --sdk-repo, remote 'sdk', sibling BigQuery-Agent-Analytics-SDK, or
#     derived SDK origin remote (failing closed if discovery fails or a target
#     SDK repo is shallow; BQAA_OFFLINE=1 skips remote SDK discovery outside
#     CI=true), and checks local tracing-v* / tracing-dist-v*:
#     * if tracing-v<dist_version> exists (in SDK or this repo), dist/ must
#       have zero diff against that release's built snapshot (reconstructed
#       from plugins/claude_code + producers/src on SDK tags, or committed dist);
#     * if tracing-v<dist_version> does not exist yet, dist_version must be
#       strictly greater than the highest existing tracing-vX.Y.Z tag;
#     * if tracing-dist-v<dist_version> exists locally, dist/ must have zero
#       diff against it; otherwise dist_version must exceed max tracing-dist-v*;
#     * every tracing-dist-v* tag holds matching dist and catalog versions.
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
  local allow_dist_vendor="${4:-1}"
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

  # Vendor directly from producers/ when building from an SDK layout, else
  # use archived vendor/ from src when present, else (when permitted) preserve
  # vendor/ from existing out dir or committed dist/vendor.
  local sdk_pkg_src="$src/../../producers/src/bigquery_agent_analytics_tracing"
  if [[ -d "$sdk_pkg_src" ]]; then
    mkdir -p "$tmp_stage/vendor"
    cp -Rp "$sdk_pkg_src" "$tmp_stage/vendor/bigquery_agent_analytics_tracing"
    local dist_info_dir="$tmp_stage/vendor/bigquery_agent_analytics_tracing-${target_version}.dist-info"
    mkdir -p "$dist_info_dir"
    printf 'Metadata-Version: 2.1\nName: bigquery-agent-analytics-tracing\nVersion: %s\n' \
      "$target_version" > "$dist_info_dir/METADATA"
    chmod 0644 "$dist_info_dir/METADATA"
  elif [[ -d "$src/vendor" ]]; then
    cp -Rp "$src/vendor" "$tmp_stage/vendor"
  elif [[ "$allow_dist_vendor" -eq 1 && -d "$out/vendor" ]]; then
    cp -Rp "$out/vendor" "$tmp_stage/vendor"
  elif [[ "$allow_dist_vendor" -eq 1 && -d "$dist/vendor" ]]; then
    cp -Rp "$dist/vendor" "$tmp_stage/vendor"
  elif [[ "$allow_dist_vendor" -eq 0 ]]; then
    rm -rf "$tmp_stage"
    echo "not ok - cannot reconstruct vendor/ for snapshot (neither producers/ nor archived vendor/ exists)"
    return 1
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

is_git_url() {
  [[ "$1" =~ ^(https?|ssh|git|file|rpc):// ]] || [[ "$1" == git@* ]]
}

fetch_sdk_tags() {
  local remote_url="$1"
  local dest_dir="$2"
  local tag_refs=""
  if ! tag_refs="$(GIT_TERMINAL_PROMPT=0 git ls-remote --tags "$remote_url" 'refs/tags/tracing-v*' 2>/dev/null)"; then
    return 1
  fi
  git init -q "$dest_dir" || return 1
  if [[ -n "$tag_refs" ]]; then
    if ! GIT_TERMINAL_PROMPT=0 git -C "$dest_dir" fetch -q --depth=1 "$remote_url" 'refs/tags/tracing-v*:refs/tags/tracing-v*' >/dev/null 2>&1; then
      return 1
    fi
  fi
  return 0
}

git_sdk() {
  local repo_dir="$1"
  shift
  if [[ -d "$repo_dir" && ! -e "$repo_dir/.git" && -f "$repo_dir/HEAD" && -d "$repo_dir/objects" ]]; then
    git --git-dir="$repo_dir" "$@"
  else
    git -C "$repo_dir" "$@"
  fi
}

is_git_repo_root() {
  local repo_dir="$1"
  [[ -d "$repo_dir" ]] || return 1
  if ! git_sdk "$repo_dir" rev-parse --git-dir >/dev/null 2>&1; then
    return 1
  fi
  if [[ "$(git_sdk "$repo_dir" rev-parse --is-bare-repository 2>/dev/null || echo false)" == true ]]; then
    return 0
  fi
  local top
  top="$(git_sdk "$repo_dir" rev-parse --show-toplevel 2>/dev/null || true)"
  [[ -n "$top" ]] || return 1
  local resolved_dir resolved_top
  resolved_dir="$(cd "$repo_dir" 2>/dev/null && pwd -P)"
  resolved_top="$(cd "$top" 2>/dev/null && pwd -P)"
  [[ "$resolved_dir" == "$resolved_top" ]]
}

check_monotonic_tags() {
  python3 -B - "$1" "$2" "$3" "$4" <<'PY'
import re, sys

dist_ver_str, raw_tags, prefix, label = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
dist_v = tuple(int(x) for x in dist_ver_str.split("."))
pattern = re.compile(re.escape(prefix) + r"(\d+)\.(\d+)\.(\d+)")
tag_vers = []
for raw in raw_tags.splitlines():
    m = pattern.fullmatch(raw.strip())
    if m:
        tag_vers.append((tuple(int(x) for x in m.groups()), raw.strip()))
if tag_vers:
    max_v, max_tag = max(tag_vers)
    if dist_v <= max_v:
        print(f"not ok - dist version {dist_ver_str} is not newer than existing {label} {max_tag}")
        sys.exit(1)
    print(f"ok - unreleased dist version {dist_ver_str} > highest {label} {max_tag}")
else:
    print(f"ok - no release {prefix}X.Y.Z tags exist yet")
PY
}

mode="build"
sdk_repo_arg=""
sdk_repo_arg_set=0
positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      mode="check"
      shift
      ;;
    --sdk-repo=*)
      sdk_repo_arg="${1#--sdk-repo=}"
      sdk_repo_arg_set=1
      shift
      ;;
    --sdk-repo)
      if [[ $# -lt 2 ]]; then
        echo "not ok - --sdk-repo requires a path or URL" >&2
        exit 2
      fi
      sdk_repo_arg="$2"
      sdk_repo_arg_set=1
      shift 2
      ;;
    --)
      shift
      positional+=("$@")
      break
      ;;
    -*)
      echo "usage: build_dist.sh OUT_DIR [VERSION] | --check [--sdk-repo PATH_OR_URL]" >&2
      exit 2
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

if [[ "$mode" != "check" ]]; then
  if [[ "$sdk_repo_arg_set" -ne 0 || ${#positional[@]} -lt 1 || ${#positional[@]} -gt 2 ]]; then
    echo "usage: build_dist.sh OUT_DIR [VERSION] | --check [--sdk-repo PATH_OR_URL]" >&2
    exit 2
  fi
  build "$here" "${positional[0]}" "${positional[1]:-}"
  exit 0
fi

if [[ ${#positional[@]} -gt 0 ]]; then
  echo "usage: build_dist.sh OUT_DIR [VERSION] | --check [--sdk-repo PATH_OR_URL]" >&2
  exit 2
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

verify_tag_snapshot() {
  local tag_repo="$1"
  local tag_name="$2"
  local target_ver="${3:-$dist_version}"
  local tag_stage tag_built

  if git_sdk "$tag_repo" cat-file -e "$tag_name:plugins/claude_code_dist/bigquery-agent-analytics-tracing/.claude-plugin/plugin.json" 2>/dev/null; then
    tag_stage="$(mktemp -d "$work/tag_dist_XXXXXX")"
    if ! git_sdk "$tag_repo" archive "$tag_name" plugins/claude_code_dist/bigquery-agent-analytics-tracing | tar -x -C "$tag_stage"; then
      return 1
    fi
    same_tree "$tag_stage/plugins/claude_code_dist/bigquery-agent-analytics-tracing" "$dist"
    return $?
  elif git_sdk "$tag_repo" cat-file -e "$tag_name:plugins/claude_code/.claude-plugin/plugin.json" 2>/dev/null; then
    tag_stage="$(mktemp -d "$work/sdk_tag_XXXXXX")"
    tag_built="$(mktemp -d "$work/sdk_built_XXXXXX")"
    if git_sdk "$tag_repo" cat-file -e "$tag_name:producers/src/bigquery_agent_analytics_tracing/__init__.py" 2>/dev/null; then
      if ! git_sdk "$tag_repo" archive "$tag_name" plugins/claude_code producers | tar -x -C "$tag_stage"; then
        return 1
      fi
    elif git_sdk "$tag_repo" ls-tree -r --name-only "$tag_name" -- plugins/claude_code/vendor | grep -q .; then
      if ! git_sdk "$tag_repo" archive "$tag_name" plugins/claude_code | tar -x -C "$tag_stage"; then
        return 1
      fi
    else
      echo "not ok - release tag $tag_name has neither producers/ nor plugins/claude_code/vendor to reconstruct vendor/"
      return 1
    fi
    build "$tag_stage/plugins/claude_code" "$tag_built" "$target_ver" 0 || return 1
    same_tree "$tag_built" "$dist"
    return $?
  else
    echo "not ok - release tag $tag_name has neither plugins/claude_code_dist/bigquery-agent-analytics-tracing nor plugins/claude_code"
    return 1
  fi
}

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
    sdk_repo=""
    sdk_candidate=""
    sdk_explicit=0
    sdk_discovery_failed=0
    sdk_discovery_skipped=0
    if [[ "$sdk_repo_arg_set" -eq 1 ]]; then
      sdk_candidate="$sdk_repo_arg"
      sdk_explicit=1
    elif [[ -n "${BQAA_SDK_REPO:-}" ]]; then
      sdk_candidate="$BQAA_SDK_REPO"
      sdk_explicit=1
    fi

    if [[ "${BQAA_OFFLINE:-0}" == "1" && "${CI:-}" == "true" ]]; then
      echo "not ok - BQAA_OFFLINE=1 is not permitted when CI=true"
      status=1
      sdk_discovery_failed=1
    elif [[ "$sdk_explicit" -eq 1 ]]; then
      if [[ -z "$sdk_candidate" ]]; then
        echo "not ok - BQAA_SDK_REPO= is not a git repository"
        status=1
        sdk_discovery_failed=1
      elif is_git_url "$sdk_candidate"; then
        if fetch_sdk_tags "$sdk_candidate" "$work/sdk_override"; then
          sdk_repo="$work/sdk_override"
        else
          echo "not ok - BQAA_SDK_REPO=$sdk_candidate is not a git repository"
          status=1
          sdk_discovery_failed=1
        fi
      elif is_git_repo_root "$sdk_candidate"; then
        if [[ "$(git_sdk "$sdk_candidate" rev-parse --is-shallow-repository 2>/dev/null || echo true)" == true ]]; then
          echo "not ok - SDK repository is shallow"
          status=1
          sdk_discovery_failed=1
        else
          sdk_repo="$sdk_candidate"
        fi
      else
        echo "not ok - BQAA_SDK_REPO=$sdk_candidate is not a git repository"
        status=1
        sdk_discovery_failed=1
      fi
    elif [[ "${BQAA_OFFLINE:-0}" == "1" ]]; then
      echo "ok - skipping SDK tag discovery (BQAA_OFFLINE=1)"
      sdk_discovery_skipped=1
    else
      sdk_remote_url="$(git -C "$repo" remote get-url sdk 2>/dev/null || true)"
      if [[ -n "$sdk_remote_url" ]]; then
        if ! is_git_url "$sdk_remote_url"; then
          if is_git_repo_root "$sdk_remote_url"; then
            if [[ "$(git_sdk "$sdk_remote_url" rev-parse --is-shallow-repository 2>/dev/null || echo true)" == true ]]; then
              echo "not ok - SDK repository at remote sdk is shallow"
              status=1
              sdk_discovery_failed=1
            else
              sdk_repo="$sdk_remote_url"
            fi
          else
            echo "not ok - failed to fetch tracing-v release tags from SDK remote ($sdk_remote_url)"
            status=1
            sdk_discovery_failed=1
          fi
        else
          if fetch_sdk_tags "$sdk_remote_url" "$work/sdk_remote"; then
            sdk_repo="$work/sdk_remote"
          else
            echo "not ok - failed to fetch tracing-v release tags from SDK remote ($sdk_remote_url)"
            status=1
            sdk_discovery_failed=1
          fi
        fi
      else
        sibling_sdk=""
        direct_sibling="$(dirname "$repo")/BigQuery-Agent-Analytics-SDK"
        if [[ -d "$direct_sibling" ]]; then
          sibling_sdk="$direct_sibling"
        else
          common_git_dir="$(git -C "$repo" rev-parse --git-common-dir 2>/dev/null || true)"
          if [[ -n "$common_git_dir" ]]; then
            if [[ "$common_git_dir" != /* ]]; then
              common_git_dir="$repo/$common_git_dir"
            fi
            common_git_dir="$(cd "$common_git_dir" 2>/dev/null && pwd || true)"
            if [[ -n "$common_git_dir" && "$(basename "$common_git_dir")" == ".git" ]]; then
              main_parent="$(dirname "$(dirname "$common_git_dir")")"
              if [[ -d "$main_parent/BigQuery-Agent-Analytics-SDK" ]]; then
                sibling_sdk="$main_parent/BigQuery-Agent-Analytics-SDK"
              fi
            fi
          fi
        fi
        if [[ -n "$sibling_sdk" ]] && is_git_repo_root "$sibling_sdk"; then
          if [[ "$(git_sdk "$sibling_sdk" rev-parse --is-shallow-repository 2>/dev/null || echo true)" == true ]]; then
            echo "not ok - SDK repository at $sibling_sdk is shallow"
            status=1
            sdk_discovery_failed=1
          else
            sdk_repo="$sibling_sdk"
          fi
        fi
        if [[ "$sdk_discovery_failed" -eq 0 && "$sdk_discovery_skipped" -eq 0 && -z "$sdk_repo" && -z "$(git -C "$repo" tag -l 'tracing-v*')" ]]; then
          origin_url="$(git -C "$repo" remote get-url origin 2>/dev/null || true)"
          if [[ "$origin_url" == *BigQuery-Agent-Analytics-Plugins* ]]; then
            sdk_url="${origin_url%BigQuery-Agent-Analytics-Plugins*}BigQuery-Agent-Analytics-SDK${origin_url##*BigQuery-Agent-Analytics-Plugins}"
            if ! is_git_url "$sdk_url"; then
              if is_git_repo_root "$sdk_url"; then
                if [[ "$(git_sdk "$sdk_url" rev-parse --is-shallow-repository 2>/dev/null || echo true)" == true ]]; then
                  echo "not ok - SDK repository at $sdk_url is shallow"
                  status=1
                  sdk_discovery_failed=1
                else
                  sdk_repo="$sdk_url"
                fi
              else
                echo "not ok - failed to fetch tracing-v release tags from derived SDK repository ($sdk_url)"
                status=1
                sdk_discovery_failed=1
              fi
            else
              if fetch_sdk_tags "$sdk_url" "$work/sdk_remote"; then
                sdk_repo="$work/sdk_remote"
              else
                echo "not ok - failed to fetch tracing-v release tags from derived SDK repository ($sdk_url)"
                status=1
                sdk_discovery_failed=1
              fi
            fi
          fi
        fi
      fi
    fi

    tag="tracing-v$dist_version"
    found_tag_repos=()
    if [[ -n "$sdk_repo" ]] && git_sdk "$sdk_repo" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null 2>&1; then
      found_tag_repos+=("$sdk_repo")
    fi
    if git -C "$repo" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null 2>&1; then
      found_tag_repos+=("$repo")
    fi

    if [[ ${#found_tag_repos[@]} -gt 0 ]]; then
      for tag_repo in "${found_tag_repos[@]}"; do
        if verify_tag_snapshot "$tag_repo" "$tag" "$dist_version"; then
          echo "ok - $tag exists and committed dist matches $tag"
        else
          echo "not ok - plugins/claude_code_dist/bigquery-agent-analytics-tracing changed since release tag $tag; bump dist version"
          status=1
        fi
      done
    else
      all_tracing_tags=""
      if [[ -n "$sdk_repo" ]]; then
        all_tracing_tags="$(git_sdk "$sdk_repo" tag -l 'tracing-v*')"
      fi
      repo_tracing_tags="$(git -C "$repo" tag -l 'tracing-v*')"
      if [[ -n "$repo_tracing_tags" ]]; then
        all_tracing_tags="${all_tracing_tags:+$all_tracing_tags$'\n'}$repo_tracing_tags"
      fi
      if [[ -n "$all_tracing_tags" ]]; then
        check_monotonic_tags "$dist_version" "$all_tracing_tags" "tracing-v" "tag" || status=1
      elif [[ "$sdk_discovery_failed" -eq 0 && "$sdk_discovery_skipped" -eq 0 ]]; then
        echo "ok - no tracing-v release tags exist yet"
      fi
    fi

    while IFS= read -r head_tag; do
      [[ -n "$head_tag" ]] || continue
      if [[ "$head_tag" != "$tag" ]]; then
        echo "not ok - HEAD carries release tag $head_tag which does not match dist version $dist_version ($tag)"
        status=1
      fi
    done < <(git -C "$repo" tag --points-at HEAD 'tracing-v*')
    if [[ "${GITHUB_REF_NAME:-}" == tracing-v* && "${GITHUB_REF_NAME:-}" != "$tag" ]]; then
      echo "not ok - GITHUB_REF_NAME=${GITHUB_REF_NAME} does not match dist version $dist_version ($tag)"
      status=1
    fi

    dist_tag_curr="tracing-dist-v$dist_version"
    if git -C "$repo" rev-parse -q --verify "refs/tags/$dist_tag_curr^{commit}" >/dev/null 2>&1; then
      if verify_tag_snapshot "$repo" "$dist_tag_curr" "$dist_version"; then
        echo "ok - $dist_tag_curr exists and committed dist matches $dist_tag_curr"
      else
        echo "not ok - plugins/claude_code_dist/bigquery-agent-analytics-tracing changed since release tag $dist_tag_curr; bump dist version"
        status=1
      fi
    else
      dist_tags_list="$(git -C "$repo" tag -l 'tracing-dist-v*')"
      if [[ -n "$dist_tags_list" ]]; then
        check_monotonic_tags "$dist_version" "$dist_tags_list" "tracing-dist-v" "dist tag" || status=1
      fi
    fi

    while IFS= read -r head_dist_tag; do
      [[ -n "$head_dist_tag" ]] || continue
      if [[ "$head_dist_tag" != "$dist_tag_curr" ]]; then
        echo "not ok - HEAD carries release tag $head_dist_tag which does not match dist version $dist_version ($dist_tag_curr)"
        status=1
      fi
    done < <(git -C "$repo" tag --points-at HEAD 'tracing-dist-v*')
    if [[ "${GITHUB_REF_NAME:-}" == tracing-dist-v* && "${GITHUB_REF_NAME:-}" != "$dist_tag_curr" ]]; then
      echo "not ok - GITHUB_REF_NAME=${GITHUB_REF_NAME} does not match dist version $dist_version ($dist_tag_curr)"
      status=1
    fi

    while IFS= read -r dist_tag; do
      [[ -n "$dist_tag" ]] || continue
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
    done < <(git -C "$repo" tag -l 'tracing-dist-v*')
  fi
fi

exit "$status"
