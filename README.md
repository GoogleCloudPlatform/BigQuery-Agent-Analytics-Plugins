# BigQuery Agent Analytics Plugins

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Mod CI](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/mod-ci.yml/badge.svg)](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/actions/workflows/mod-ci.yml)

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

- Add the marketplace with the `<owner>/<repo>` form, not with a URL to
  `marketplace.json`: the catalog's plugin `source` paths are relative, so they
  resolve only in a Git checkout.
- To fetch only the catalog and the built plugin copies, add
  `--sparse .claude-plugin plugins/claude_code_dist` to the
  `claude plugin marketplace add` command.
- Organization-wide deployment of the mod is covered in
  [`docs/managed-settings.md`](plugins/claude_code_mod/docs/managed-settings.md).

## Repository layout

```text
.claude-plugin/marketplace.json  the bqaa-tracing catalog
plugins/claude_code_mod/         mod source, tests, SQL and docs
plugins/claude_code/             tracing plugin source: manifest, hooks, command, docs
plugins/claude_code_dist/        built copies the catalog serves
  bigquery-agent-analytics-mod/       built from plugins/claude_code_mod
  bigquery-agent-analytics-tracing/   built from the SDK repository, per release
.github/workflows/               Mod CI and the mod release workflow
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
tags (`git fetch --unshallow --tags` on a shallow one), Node 24 or newer, and
Python 3 with DuckDB:

```bash
cd plugins/claude_code_mod
bash build_dist.sh --check
bash tests/build_dist.test.sh
bash tests/setup.test.sh
bash tests/version.test.sh
node tests/reload.test.mjs
python3 -m venv .venv && .venv/bin/pip install duckdb==1.5.6
.venv/bin/python tests/views.test.py
```

After changing the mod's source, rebuild the served copy with
`bash plugins/claude_code_mod/build_dist.sh plugins/claude_code_dist/bigquery-agent-analytics-mod`
so `build_dist.sh --check` passes. The mod's
[README](plugins/claude_code_mod/README.md#testing) lists the remaining checks,
which need the host CLI or a Google Cloud project. The tracing plugin's Python
package and its tests live in the SDK repository.

## Releases

- **Mod**: push a `mod-vX.Y.Z` tag whose version is the one in
  `plugins/claude_code_mod/.claude-plugin/plugin.json`. The
  [`release-mod.yml`](.github/workflows/release-mod.yml) workflow verifies it
  and drafts a pre-release holding `bigquery-agent-analytics-mod-X.Y.Z.tar.gz`
  and `SHA256SUMS`. The served copy is then synced from that archive and the
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

## Disclaimer

This is not an officially supported Google product. This project is not
eligible for the [Google Open Source Software Vulnerability Rewards
Program](https://bughunters.google.com/open-source-security).
