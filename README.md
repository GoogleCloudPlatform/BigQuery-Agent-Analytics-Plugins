# BigQuery Agent Analytics Plugins

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Repo CI](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/repo-ci.yml/badge.svg)](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/repo-ci.yml)
[![Mod CI](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/mod-ci.yml/badge.svg)](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/mod-ci.yml)
[![Tracing CI](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/tracing-ci.yml/badge.svg)](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/tracing-ci.yml)

Claude Code plugins that stream coding-agent sessions to BigQuery in the
[BigQuery Agent Analytics](https://adk.dev/integrations/bigquery-agent-analytics/)
`agent_events` schema, so you can analyze coding-agent activity alongside your
ADK agents.

This repository is the plugin marketplace (`bqaa-tracing`) for those plugins:
their sources, their tests, and the built copies the marketplace serves.

## Plugins

| Plugin | What it is | Docs |
| --- | --- | --- |
| `bigquery-agent-analytics-mod` | Early access. An in-process [mod](https://code.claude.com/docs/en/plugins/mods/overview), written in TypeScript, for Claude Code CLI v2.1.296 or newer. It records sessions, turns, model requests and tool calls inside the CLI and writes them to BigQuery from the background, so no hook waits on BigQuery. | [`plugins/claude_code_mod`](plugins/claude_code_mod/): [README](plugins/claude_code_mod/README.md), [Quickstart](plugins/claude_code_mod/QUICKSTART.md) |
| `bigquery-agent-analytics-tracing` | Claude Code hook traces. Hook scripts (bash) call a vendored copy of the SDK's Python tracing package, which spools each event and writes it to BigQuery. | [`plugins/claude_code`](plugins/claude_code/): [README](plugins/claude_code/README.md) |

Run one tracer per table. The two plugins write the same events, so with both
on, every event is written twice.

## Install

Add the marketplace, then install a plugin from it. For the mod:

```bash
claude plugin marketplace add GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins
claude plugin install bigquery-agent-analytics-mod@bqaa-tracing \
  --config project=my-gcp-project --config dataset=agent_analytics
```

Inside an open `claude` session, run `/reload-plugins` (or start a new session)
after installing. The BigQuery dataset has to exist before the mod can write;
[`setup.sh`](plugins/claude_code_mod/setup.sh) creates it, and the
[Quickstart](plugins/claude_code_mod/QUICKSTART.md) walks through the whole
setup.

For the tracing plugin instead, run
`claude plugin install bigquery-agent-analytics-tracing@bqaa-tracing`, then
follow its [README](plugins/claude_code/README.md#installing-the-plugin) to set
the BigQuery destination and runtime dependencies.

Notes:

- **Served marketplace copy vs. `main` docs**: Marketplace installs (`bqaa-tracing`) serve the committed release snapshot in [`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json) and [`plugins/claude_code_dist/bigquery-agent-analytics-mod/README.md`](plugins/claude_code_dist/bigquery-agent-analytics-mod/README.md), whereas `plugins/claude_code_mod/` on `main` holds development HEAD and can be ahead of the marketplace between releases. In particular, `0.1.1` exposes `/bqaa-setup`, while `0.1.2+` introduces `/bqaa-mod-setup` (keeping `/bqaa-setup` as an alias in `0.1.x`).
- **Verify commands before `claude -p`**: Passing an unknown slash command to `claude -p "/..."` does **not** fail fast—Claude Code treats it as a normal billed model prompt. Check your installed version and registered commands first with `claude plugin list` or `/bqaa-status`.
- Add the marketplace with the `<owner>/<repo>` form, not with a URL to
  `marketplace.json`: the catalog's plugin `source` paths are relative, so they
  resolve only in a Git checkout.
- To fetch only the catalog and the built plugin copies, add
  `--sparse .claude-plugin plugins/claude_code_dist` to the
  `claude plugin marketplace add` command.
- Organization-wide deployment of the mod is covered in
  [`docs/managed-settings.md`](plugins/claude_code_mod/docs/managed-settings.md).

### Switching Google Cloud projects

- **`bigquery-agent-analytics-mod`**: Passing `--config project=...` (or setting
  `project` in `/config`) saves `"project"` in `~/.claude/settings.json`
  (`pluginConfigs`), which takes precedence over `BQAA_PROJECT_ID` and
  `gcloud config set project`. To switch to `NEW_PROJECT`, provision the new
  project with `/bqaa-mod-setup` (or run `setup.sh` from your installed mod
  cache or `./plugins/claude_code_mod/setup.sh` in a checkout), update
  `project` via `/config` or
  `claude plugin install bigquery-agent-analytics-mod@bqaa-tracing --config project=NEW_PROJECT --config dataset=agent_analytics`
  (or set `"project": ""` in `~/.claude/settings.json` to fall back to
  `BQAA_PROJECT_ID`), and run `/reload-plugins` (see
  [Changing the GCP project or dataset](plugins/claude_code_mod/README.md#changing-the-gcp-project-or-dataset)).
- **`bigquery-agent-analytics-tracing`**: Provision the dataset and
  `agent_events` table in the new project, update `BQAA_PROJECT_ID` in
  `~/.claude/settings.json` (`"env"`), `.claude/settings.local.json`, or your
  shell profile (`~/.zshrc` / `~/.bashrc`), start a new session, and verify with
  `/bqaa-setup` (see
  [Switching Google Cloud projects](plugins/claude_code/README.md#switching-google-cloud-projects)).

## Repository layout

```text
.claude-plugin/marketplace.json  the bqaa-tracing catalog
plugins/claude_code_mod/         mod source, tests, SQL, scripts and docs
plugins/claude_code/             tracing plugin source: manifest, hooks, command, docs
plugins/claude_code_dist/        built copies the catalog serves
  bigquery-agent-analytics-mod/       built from plugins/claude_code_mod
  bigquery-agent-analytics-tracing/   built from the SDK repository, per release
scripts/                         checks that span the whole repository
.github/                         workflows (Repo CI, Mod CI, Tracing CI, the mod release),
                                 issue and pull request templates
```

[`plugins/claude_code_dist/README.md`](plugins/claude_code_dist/README.md)
explains how each built copy is produced and kept in step with its source.

## Relationship to the BigQuery Agent Analytics SDK

The [BigQuery Agent Analytics SDK](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK)
is the consumption layer for the same `agent_events` tables: trace
reconstruction, evaluation and observability dashboards. These plugins are the
producing side for Claude Code.

The SDK repository also holds the Python package behind the tracing plugin
(`producers/`). The tracing plugin's built copy in `plugins/claude_code_dist/`
comes from that package's `tracing-vX.Y.Z` releases there. The mod is
developed and released here (`mod-vX.Y.Z`).

## Development and testing

The mod's offline checks are the ones Mod CI runs. They need a full clone with
tags (`git fetch --unshallow --tags` on a shallow one), Node 24 or newer,
Python 3.11 or newer for DuckDB SQL validation, and Python 3.10 or newer for
helper-script checks:

```bash
cd plugins/claude_code_mod
bash build_dist.sh --check
bash tests/build_dist.test.sh
bash tests/setup.test.sh
bash tests/version.test.sh
node tests/reload.test.mjs
npm ci --ignore-scripts --prefix tests
./tests/node_modules/.bin/tsc -p tsconfig.ci.json --noEmit
node --import ./tests/shim/register.mjs --test --test-concurrency=1 tests/*.test.ts tests/state-contract/state.contract.ts
python3 -m venv .venv && .venv/bin/pip install duckdb==2.0.0   # requires Python 3.11+
.venv/bin/python tests/views.test.py
.venv/bin/python tests/queries.test.py
python3 tests/scripts.test.py
```

Repository-wide checks need only Python 3.9 or newer, and ShellCheck for the
last command: repository hygiene (the marketplace catalog against each plugin
manifest, links and anchors in the Markdown, stale references, shell syntax,
working notes and license headers), the tests for the mod's `scripts/`, and
ShellCheck over every shell script. Run them from the repository root:

```bash
python3 scripts/check_repo_hygiene.py
python3 scripts/check_repo_hygiene.test.py
python3 plugins/claude_code_mod/tests/scripts.test.py
git ls-files -z '*.sh' | xargs -0 shellcheck -S warning
```

[Repo CI](.github/workflows/repo-ci.yml) runs them on every pull request and
every push to `main`. The tracing plugin's offline parity, hook and drainer
tests run on Python 3.10 through 3.15 in
[Tracing CI](.github/workflows/tracing-ci.yml):

```bash
python3 plugins/claude_code/tests/tracing_plugin.test.py
```

The served copy in `plugins/claude_code_dist/bigquery-agent-analytics-mod` is
updated by [`plugins/claude_code_mod/sync_dist.sh`](plugins/claude_code_mod/sync_dist.sh)
on each tagged `mod-vX.Y.Z` release (see
[Where a change goes](CONTRIBUTING.md#where-a-change-goes)). The mod's
[README](plugins/claude_code_mod/README.md#testing) lists the remaining checks,
which need the host CLI or a Google Cloud project. The tracing plugin's Python
package lives in the SDK repository; offline parity and hook tests run here in
[Tracing CI](.github/workflows/tracing-ci.yml).

## Releases

- **Mod**: push a `mod-vX.Y.Z` tag whose version is the one in
  `plugins/claude_code_mod/.claude-plugin/plugin.json`. The
  [`release-mod.yml`](.github/workflows/release-mod.yml) workflow verifies it
  and drafts a GitHub release (initially marked pre-release) holding
  `bigquery-agent-analytics-mod-X.Y.Z.tar.gz` and `SHA256SUMS`. Once
  published (for example `mod-v0.1.0`, published as Latest), the served copy is
  synced from that archive with `plugins/claude_code_mod/sync_dist.sh` and the
  merge commit tagged `mod-dist-vX.Y.Z`, the ref for organizations that
  register this repository as their marketplace.
- **Tracing plugin**: released from the SDK repository as `tracing-vX.Y.Z`; its
  built copy is committed here after each release.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
Contributor License Agreement and the review process, and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for how we work together. To report a
security issue, see [SECURITY.md](SECURITY.md).

## License

Apache License 2.0; see [LICENSE](LICENSE).

## Terms of Service

These plugins write data to Google BigQuery. Use of Google Cloud services,
including BigQuery, is subject to the
[Google Cloud Terms of Service](https://cloud.google.com/terms). See also the
[Google Privacy Policy](https://policies.google.com/privacy).

## Disclaimer

This is not an officially supported Google product. This project is not
eligible for the [Google Open Source Software Vulnerability Rewards
Program](https://bughunters.google.com/open-source-security).
