# How to contribute

We'd love to accept your patches and contributions to this project.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our community guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Contribution process

### Where a change goes

This repository is a plugin marketplace: the source of two plugins, their
tests, and the built copies the marketplace serves. Change the source, not the
built copy.

| To change | Edit | Served copy |
| --- | --- | --- |
| The mod, `bigquery-agent-analytics-mod`: hooks, SQL, scripts, setup and docs | [`plugins/claude_code_mod/`](plugins/claude_code_mod/) | [`plugins/claude_code_dist/bigquery-agent-analytics-mod/`](plugins/claude_code_dist/bigquery-agent-analytics-mod/), built from the source by [`build_dist.sh`](plugins/claude_code_mod/build_dist.sh) |
| The tracing plugin, `bigquery-agent-analytics-tracing`: manifest, hooks, command and docs | [`plugins/claude_code/`](plugins/claude_code/) | [`plugins/claude_code_dist/bigquery-agent-analytics-tracing/`](plugins/claude_code_dist/bigquery-agent-analytics-tracing/), the expanded release from the SDK repository |
| The Python package the tracing plugin vendors | `producers/` in the [SDK repository](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK) | the next tracing release |
| The marketplace catalog | [`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json) | none: it is the catalog |

The mod ships `.claude-plugin/plugin.json`, `hooks/`, `types/`, `sql/`,
`scripts/`, `docs/`, `setup.sh`, `README.md`, `QUICKSTART.md` and
`MIGRATION.md`; its `tests/` and `examples/` stay in the source. Before the
first `mod-vX.Y.Z` release is tagged, `build_dist.sh --check` requires the
served copy to be exactly what the source builds now (file contents and
executable bits both count), so rebuild it in the same pull request as any
change to a file the mod ships:

```bash
bash plugins/claude_code_mod/build_dist.sh plugins/claude_code_dist/bigquery-agent-analytics-mod
```

Once a release is tagged, the copy is checked against its own release tag
instead and moves only when a maintainer syncs a published release, so leave it
out of your change. The
[distribution notes](plugins/claude_code_dist/README.md#the-coding-agent-mod)
have the details.

### Run the checks

Three workflows run on GitHub. [Repo CI](.github/workflows/repo-ci.yml) runs on
every pull request and every push to `main`, and needs only Python 3.9 or newer
and ShellCheck:

- [`scripts/check_repo_hygiene.py`](scripts/check_repo_hygiene.py): the
  marketplace catalog agrees with each plugin's manifest, JSON files parse,
  relative links and heading anchors in Markdown resolve, no file refers to the
  repository the mod was first published from (apart from the
  [migration notes](plugins/claude_code_mod/MIGRATION.md)), shell scripts pass
  `bash -n`, no working notes (intent.md, plan.md, spec.md) are committed, and
  source/script/workflow files carry the Apache-2.0 license header.
- [`scripts/check_repo_hygiene.test.py`](scripts/check_repo_hygiene.test.py):
  unit tests for the repo hygiene check, covering both clean repos and every
  deliberate failure mode.
- [`plugins/claude_code_mod/tests/scripts.test.py`](plugins/claude_code_mod/tests/scripts.test.py):
  the mod's `scripts/` (`setup_gcp_prereqs.py` and `run_bq.py`), run against
  stand-ins for the BigQuery client and `gcloud`, so it makes no Google Cloud
  calls.
- ShellCheck, at warning level, over every `*.sh` file.

[Tracing CI](.github/workflows/tracing-ci.yml) runs the tracing plugin tests on
Python 3.10 through 3.15:

- [`plugins/claude_code/tests/tracing_plugin.test.py`](plugins/claude_code/tests/tracing_plugin.test.py):
  source/dist parity, working-directory `sys.path` isolation, `BQAA_TRACE_ENABLED`,
  and offline end-to-end hook/drainer/setup tests. Uses only the Python standard
  library.

[Mod CI](.github/workflows/mod-ci.yml) runs the mod's other offline checks on
every pull request and every push to `main`.
[Development and testing](README.md#development-and-testing) in the README has
the commands for both; run them before you push. The mod's
[README](plugins/claude_code_mod/README.md#testing) lists the checks that need
the host CLI or a Google Cloud project; run those too when you change how the
mod behaves at run time.

### Pull requests

- Keep each pull request to one logical change, and fill in the
  [pull request template](.github/PULL_REQUEST_TEMPLATE.md): what changed, why,
  and which checks you ran.
- Write each commit message with a short summary line in the imperative mood
  (about 50 characters), a blank line, and a body wrapped at 72 characters that
  explains why.
- Never commit credentials: service-account keys, `.env` files or access
  tokens. Redact project IDs, dataset names, user names and prompts from the
  logs, sample rows and screenshots you attach.

### Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Reporting issues

Use the [issue forms](https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/issues/new/choose)
for a bug or a feature request; they ask which plugin and which versions are
involved. To report a security issue, do not open a public issue; follow
[SECURITY.md](SECURITY.md).

## Releases

Maintainers cut releases. [Releases](README.md#releases) in the README and the
[distribution notes](plugins/claude_code_dist/README.md) describe the tags
(`mod-vX.Y.Z`, `mod-dist-vX.Y.Z` and `tracing-vX.Y.Z`) and how each served copy
is kept in step with its source.
