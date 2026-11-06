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

# The mod's version is in two places: the manifest, which the host reads, and
# PLUGIN_VERSION, which the welcome card greets and every row's writer label
# carries. They must agree, and whenever the source builds a served tree that
# differs from the committed copy or the latest mod-dist-v* release tag (or
# after mod-v<version> has been tagged), the source version must be bumped.
# Run from anywhere:
#   bash tests/version.test.sh
set -euo pipefail

plugin="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo="$(cd "$plugin/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

version_of() {
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["version"])' "$1/.claude-plugin/plugin.json"
}

is_semver() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

semver_gt() {
  python3 - "$1" "$2" <<'PY'
import sys
def parse(v):
    return tuple(int(x) for x in v.split("."))
sys.exit(0 if parse(sys.argv[1]) > parse(sys.argv[2]) else 1)
PY
}

same_tree() {
  python3 - "$1" "$2" <<'PY'
import os, stat, sys
def walk(root):
    found = {}
    for folder, dirs, files in os.walk(root):
        if os.path.relpath(folder, root) == ".claude-plugin":
            dirs[:] = [d for d in dirs if d != "types"]
        for name in files:
            path = os.path.join(folder, name)
            with open(path, "rb") as handle:
                found[os.path.relpath(path, root)] = (
                    handle.read(),
                    bool(os.lstat(path).st_mode & stat.S_IXUSR),
                )
    return found
built, copy = walk(sys.argv[1]), walk(sys.argv[2])
differ = sorted(set(built) ^ set(copy)) + sorted(
    k for k in set(built) & set(copy) if built[k] != copy[k]
)
for name in differ[:20]:
    print(f"  differs: {name}")
sys.exit(1 if differ else 0)
PY
}

latest_mod_dist_tag() {
  python3 - "$1" <<'PY'
import re, subprocess, sys
out = subprocess.check_output(
    ["git", "-C", sys.argv[1], "tag", "-l", "mod-dist-v*"], text=True
)
tags = []
for line in out.splitlines():
    tag = line.strip()
    m = re.match(r"^mod-dist-v(\d+)\.(\d+)\.(\d+)$", tag)
    if m:
        tags.append((tuple(int(x) for x in m.groups()), tag))
if tags:
    tags.sort()
    print(tags[-1][1])
PY
}

check_version_tree() {
  local target_repo="$1"
  local target_plugin="$target_repo/plugins/claude_code_mod"
  local target_dist="$target_repo/plugins/claude_code_dist/bigquery-agent-analytics-mod"
  local scratch
  scratch="$work/check-$(date +%s%N)-$RANDOM"
  mkdir -p "$scratch"

  local manifest
  manifest="$(sed -n 's/^ *"version": *"\([^"]*\)".*/\1/p' "$target_plugin/.claude-plugin/plugin.json" | head -n 1)"
  local code
  code="$(sed -n "s/^export const PLUGIN_VERSION = '\([^']*\)'.*/\1/p" "$target_plugin/hooks/lib/config.ts" | head -n 1)"

  if [[ -z "$manifest" || "$manifest" != "$code" ]]; then
    echo "not ok - plugin.json says '$manifest', PLUGIN_VERSION says '$code'"
    return 1
  fi
  if ! is_semver "$manifest"; then
    echo "not ok - version '$manifest' is not X.Y.Z semver"
    return 1
  fi
  echo "ok - plugin.json and PLUGIN_VERSION agree ($manifest)"

  bash "$target_plugin/build_dist.sh" "$scratch/current"

  if [[ -d "$target_dist" ]]; then
    local dist_ver
    dist_ver="$(version_of "$target_dist")"
    if ! same_tree "$scratch/current" "$target_dist" >"$scratch/dist.diff" 2>&1; then
      if semver_gt "$manifest" "$dist_ver"; then
        echo "ok - source ($manifest) is ahead of committed dist ($dist_ver)"
      else
        echo "not ok - source ($manifest) differs from plugins/claude_code_dist ($dist_ver) without a version bump:"
        cat "$scratch/dist.diff"
        return 1
      fi
    else
      echo "ok - source ($manifest) matches committed dist ($dist_ver)"
    fi
  fi

  if git -C "$target_repo" rev-parse --git-dir >/dev/null 2>&1; then
    local latest_dist_tag
    latest_dist_tag="$(latest_mod_dist_tag "$target_repo")"
    if [[ -n "$latest_dist_tag" ]]; then
      mkdir -p "$scratch/tag_dist_extract"
      git -C "$target_repo" archive "$latest_dist_tag" plugins/claude_code_dist/bigquery-agent-analytics-mod \
        | tar -x -C "$scratch/tag_dist_extract"
      local tag_dist_dir="$scratch/tag_dist_extract/plugins/claude_code_dist/bigquery-agent-analytics-mod"
      local tag_dist_ver
      tag_dist_ver="$(version_of "$tag_dist_dir")"
      if ! same_tree "$scratch/current" "$tag_dist_dir" >"$scratch/tag_dist.diff" 2>&1; then
        if semver_gt "$manifest" "$tag_dist_ver"; then
          echo "ok - source ($manifest) is ahead of $latest_dist_tag ($tag_dist_ver)"
        else
          echo "not ok - source ($manifest) differs from $latest_dist_tag ($tag_dist_ver) without a version bump:"
          cat "$scratch/tag_dist.diff"
          return 1
        fi
      else
        echo "ok - source ($manifest) matches $latest_dist_tag ($tag_dist_ver)"
      fi
    fi

    local mod_tag="mod-v$manifest"
    if git -C "$target_repo" rev-parse -q --verify "refs/tags/$mod_tag^{commit}" >/dev/null 2>&1; then
      mkdir -p "$scratch/tag_mod_extract"
      git -C "$target_repo" archive "$mod_tag" plugins/claude_code_mod \
        | tar -x -C "$scratch/tag_mod_extract"
      bash "$scratch/tag_mod_extract/plugins/claude_code_mod/build_dist.sh" "$scratch/tag_mod"
      if ! same_tree "$scratch/current" "$scratch/tag_mod" >"$scratch/tag_mod.diff" 2>&1; then
        echo "not ok - source ($manifest) differs from already-tagged $mod_tag without a version bump:"
        cat "$scratch/tag_mod.diff"
        return 1
      fi
      echo "ok - source ($manifest) matches $mod_tag"
    fi
  fi
}

set_repo_version() {
  local target_plugin="$1"
  local ver="$2"
  python3 - "$target_plugin/.claude-plugin/plugin.json" "$target_plugin/hooks/lib/config.ts" "$ver" <<'PY'
import json, re, sys
manifest_path, config_path, ver = sys.argv[1:4]
with open(manifest_path, encoding="utf-8") as fh:
    data = json.load(fh)
data["version"] = ver
with open(manifest_path, "w", encoding="utf-8") as fh:
    fh.write(json.dumps(data, indent=2) + "\n")
with open(config_path, encoding="utf-8") as fh:
    code = fh.read()
code = re.sub(
    r"^export const PLUGIN_VERSION = '[^']*'",
    f"export const PLUGIN_VERSION = '{ver}'",
    code,
    count=1,
    flags=re.MULTILINE,
)
with open(config_path, "w", encoding="utf-8") as fh:
    fh.write(code)
PY
}

run_selftest() {
  local selftest="$work/${1:-selftest}"
  local st_plugin="$selftest/plugins/claude_code_mod"
  local st_dist="$selftest/plugins/claude_code_dist/bigquery-agent-analytics-mod"
  local log="$selftest.log"
  mkdir -p "$selftest/plugins/claude_code_dist" "$selftest/.claude-plugin"
  cp -Rp "$plugin" "$st_plugin"
  rm -rf "$st_plugin/.claude-plugin/types" "$st_plugin/tests/node_modules"
  set_repo_version "$st_plugin" "0.1.0"
  bash "$st_plugin/build_dist.sh" "$st_dist"

  # The self-test's repository is its own: the caller's Git settings (a
  # global hook that refuses commits, signing that needs a key) must not
  # decide it. Its Git reads no global or system configuration and runs no
  # hook; the checks of the real repository run as the caller.
  mkdir -p "$work/git-home/.config" "$work/empty-hooks"
  local HOME="$work/git-home" XDG_CONFIG_HOME="$work/git-home/.config" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
  export HOME XDG_CONFIG_HOME GIT_CONFIG_NOSYSTEM GIT_CONFIG_GLOBAL
  local -a st_git=(git -C "$selftest" -c core.hooksPath="$work/empty-hooks" -c commit.gpgsign=false -c tag.gpgSign=false
    -c user.name=test -c user.email=test@example.com)
  # Checked step by step: run as a condition, a function is not stopped by
  # set -e.
  if ! { "${st_git[@]}" -c init.defaultBranch=main init -q &&
    "${st_git[@]}" add -A &&
    "${st_git[@]}" commit --no-verify -qm "baseline 0.1.0" &&
    "${st_git[@]}" tag mod-v0.1.0 &&
    "${st_git[@]}" tag mod-dist-v0.1.0; } >"$log" 2>&1; then
    echo "not ok - selftest could not commit and tag its baseline 0.1.0:"
    cat "$log"
    return 1
  fi

  if ! check_version_tree "$selftest" >"$log" 2>&1; then
    echo "not ok - selftest baseline 0.1.0 failed:"
    cat "$log"
    return 1
  fi

  echo "" >>"$st_plugin/README.md"
  if check_version_tree "$selftest" >"$log" 2>&1; then
    echo "not ok - selftest accepted an unbumped 0.1.0 edit"
    return 1
  fi

  set_repo_version "$st_plugin" "0.1.1"
  if ! check_version_tree "$selftest" >"$log" 2>&1; then
    echo "not ok - selftest rejected a bumped 0.1.1 edit:"
    cat "$log"
    return 1
  fi
  echo "ok - selftest verified unbumped edits fail and bumped edits pass"
}

check_version_tree "$repo"
run_selftest

# A caller whose own Git settings refuse every commit (a pre-commit hook
# that fails, by core.hooksPath and by init's template) and sign every
# commit and tag (with a signing program that fails) runs the self-test
# just the same.
poisoned="$work/poisoned-home"
mkdir -p "$poisoned/.config/git/hooks" "$poisoned/.config/git/template/hooks"
for hook in "$poisoned/.config/git/hooks/pre-commit" "$poisoned/.config/git/template/hooks/pre-commit"; do
  printf '#!/bin/sh\necho "refused by the caller pre-commit hook" >&2\nexit 1\n' >"$hook"
  chmod +x "$hook"
done
printf '[core]\n\thooksPath = %s\n[init]\n\ttemplateDir = %s\n' \
  "$poisoned/.config/git/hooks" "$poisoned/.config/git/template" >"$poisoned/.gitconfig"
printf '[commit]\n\tgpgSign = true\n[tag]\n\tgpgSign = true\n[gpg]\n\tprogram = false\n' >"$poisoned/.config/git/config"
if ! HOME="$poisoned" XDG_CONFIG_HOME="$poisoned/.config" run_selftest selftest-poisoned >"$work/poisoned.log" 2>&1; then
  echo "not ok - selftest failed under a caller's refusing hook and signing settings:"
  cat "$work/poisoned.log"
  exit 1
fi
echo "ok - selftest ignores the caller's Git hooks and signing settings"
