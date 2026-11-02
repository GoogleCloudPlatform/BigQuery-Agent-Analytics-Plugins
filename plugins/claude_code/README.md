# BigQuery Agent Analytics — Claude Code plugin

Native Claude Code plugin that streams hook events to the BigQuery
Agent Analytics `agent_events` table.

## Layout

```
plugins/claude_code/
├── .claude-plugin/
│   └── plugin.json          # native Claude Code manifest
├── hooks/
│   ├── common.sh            # shared hook entry (PYTHONPATH + python -m)
│   └── <hook>.sh            # one per Claude Code hook
├── commands/
│   └── bqaa-setup.md        # /bqaa-setup slash command
├── scripts/
│   └── run_setup_check.sh   # /bqaa-setup wrapper (PYTHONPATH + python -m)
├── MARKETPLACE.md           # submission checklist + IAM + verification
└── vendor/                  # generated at build time (git-ignored)
    ├── bigquery_agent_analytics_tracing/
    │                          # vendored package source — copied from
    │                          # the SDK repo's producers/src/ by build_claude_plugin.py
    └── bigquery_agent_analytics_tracing-<version>.dist-info/
                               # PEP 376 metadata so importlib.metadata
                               # resolves the right version at runtime
```

`vendor/` is never source-controlled. The build script
[`producers/scripts/build_claude_plugin.py`](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK/blob/main/producers/scripts/build_claude_plugin.py)
in the [SDK repository](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK),
which owns the Python package, copies the canonical package source into
`vendor/` and stamps the resolved version into `.claude-plugin/plugin.json`
at release time — so the wheel and the plugin always ship the same code.

## Runtime model

1. Claude Code fires a hook (e.g. `PreToolUse`) per the manifest.
2. The matching `hooks/<hook>.sh` execs `hooks/common.sh <HookName>`.
3. `common.sh` prepends `${CLAUDE_PLUGIN_ROOT}/vendor` to `PYTHONPATH`,
   dropping empty entries, and runs
   `bigquery_agent_analytics_tracing.claude_code <HookName>` like
   `python -m` does, but without the working directory (usually the
   session's repository) on `sys.path`, so nothing is imported from it.
4. The hook process writes one spool envelope and spawns the drainer
   subprocess the same way. The drainer inherits the same `PYTHONPATH`
   via `os.environ.copy()` so the vendored package resolves on the
   spawned process too — no separate wheel install required on
   `BQAA_PYTHON`.

## Required runtime deps on `BQAA_PYTHON`

The plugin does **not** require installing `bigquery-agent-analytics-tracing`
itself, but the Python under `BQAA_PYTHON` still needs:

- `google-cloud-bigquery` — required for both writer paths.
- `google-cloud-bigquery-storage` + `pyarrow` — required only for the
  Storage Write API path; the drainer falls back to `insert_rows_json`
  when these are unavailable.

Run the `/bqaa-setup` slash command inside Claude Code to check env
vars + runtime deps and get a copy-pasteable `pip install …` line for
anything missing. The command is advisory only — it never mutates
shell rc files or installs packages on your behalf. See
[`commands/bqaa-setup.md`](commands/bqaa-setup.md).

BigQuery IAM requirements and submission checklist live in
[`MARKETPLACE.md`](MARKETPLACE.md).

## Installing the plugin

> **If you previously installed an older BQAA Claude Code plugin from
> another marketplace or local path, remove or disable it before
> installing this catalog version.** Running multiple BQAA tracing
> plugins at once can duplicate hook events in BigQuery. Check
> existing installs with `/plugin list` and uninstall any duplicates
> with `/plugin uninstall <name>@<marketplace>` before continuing.

This repo serves a Claude Code marketplace catalog at
[`/.claude-plugin/marketplace.json`](../../.claude-plugin/marketplace.json).
From a Claude Code session:

```
/plugin marketplace add GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins
/plugin install bigquery-agent-analytics-tracing@bqaa-tracing
```

> **Important: add the marketplace via the Git source form
> (`<owner>/<repo>`), not via a direct URL to `marketplace.json`.**
> The catalog uses a relative `source` path
> (`./plugins/claude_code_dist/...`), which Claude Code can only
> resolve when the marketplace was added as a Git checkout — that's
> what provides the surrounding repo files the relative path points
> into. A direct-URL `add` would fetch only the catalog JSON and the
> plugin install would fail to resolve.

For a faster clone — this repo carries the plugins' sources + tests
in addition to the built plugin copies — use a sparse checkout if your
Claude Code version supports it:

```
/plugin marketplace add GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins --sparse .claude-plugin plugins/claude_code_dist
/plugin install bigquery-agent-analytics-tracing@bqaa-tracing
```

Then configure BigQuery destination + runtime deps (one-time):

```bash
export BQAA_PROJECT_ID=your-gcp-project
export BQAA_DATASET=agent_analytics
pip install google-cloud-bigquery                  # always
pip install google-cloud-bigquery-storage pyarrow  # Storage Write path (optional)
```

Run `/bqaa-setup` inside Claude Code to verify the env vars and runtime
deps are correctly configured before relying on tracing.

### Switching Google Cloud projects

`bigquery-agent-analytics-tracing` resolves the target project from
`BQAA_PROJECT_ID` (falling back to `GCP_PROJECT_ID`, then
`GOOGLE_CLOUD_PROJECT`) in your Claude Code settings (`~/.claude/settings.json`
`env` block or `.claude/settings.local.json`) or shell environment; it does
not read `gcloud config get-value project`.
To switch to a new Google Cloud project:

1. Create the BigQuery dataset and `agent_events` table in the new project
   (see [`MARKETPLACE.md`](MARKETPLACE.md#bigquery-iam-requirements)).
2. Update `BQAA_PROJECT_ID` (and `BQAA_DATASET` if changing datasets) in
   `~/.claude/settings.json` (`"env"`), `.claude/settings.local.json`, and
   your shell profile (`~/.zshrc` or `~/.bashrc`).
3. Start a new Claude Code session so hooks inherit the updated environment,
   then run `/bqaa-setup` to confirm `BQAA_PROJECT_ID` points to the new
   project.

### Submission to Anthropic's official marketplace

Tracked in [#251](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK/issues/251)
(track B5). Until then, the self-hosted catalog above is the canonical
install path.

### Manual install from the GitHub release (fallback)

If you can't use the marketplace catalog, download the plugin tarball
attached to the matching `tracing-vX.Y.Z` GitHub release:

```bash
VERSION=0.1.0
mkdir -p ~/.claude/plugins
curl -L \
  "https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK/releases/download/tracing-v${VERSION}/bigquery-agent-analytics-tracing-claude-code-${VERSION}.tar.gz" \
  | tar -xz -C ~/.claude/plugins
```

Then point Claude Code at the unpacked plugin directory per the
[Claude Code plugins docs](https://docs.claude.com/en/docs/claude-code/plugins).

## Building the artifact

The build runs from a checkout of the
[SDK repository](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK),
which holds the Python package under `producers/`:

```bash
cd producers
python scripts/build_claude_plugin.py
# → plugins/claude_code/vendor/bigquery_agent_analytics_tracing/
# → producers/dist/bigquery-agent-analytics-tracing-claude-code-X.Y.Z.tar.gz
```

CI there builds the same tarball on every release tag (see
[producers/RELEASING.md](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK/blob/main/producers/RELEASING.md)).
On regular PRs the plugin tarball is uploaded as a workflow artifact via
the `producers-ci.yml` `build` job.
