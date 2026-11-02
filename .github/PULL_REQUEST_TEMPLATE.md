## What and why

<!-- What does this change do, and why? Link the issue it fixes, if there is one. -->

## Which plugin

- [ ] The mod (`plugins/claude_code_mod/`)
- [ ] The tracing plugin (`plugins/claude_code/`)
- [ ] The marketplace catalog, repository tooling or docs only

## Checks

<!--
Tick what you ran. The commands are in the README ("Development and testing")
and https://github.com/GoogleCloudPlatform/BigQuery-Agent-Analytics-Plugins/blob/main/CONTRIBUTING.md#run-the-checks
-->

- [ ] `python3 scripts/check_repo_hygiene.py`
- [ ] `python3 plugins/claude_code_mod/tests/scripts.test.py`
- [ ] `git ls-files -z '*.sh' | xargs -0 shellcheck -S warning`
- [ ] The mod's offline checks, if I changed anything under `plugins/claude_code_mod/` (`build_dist.sh --check`, `tests/build_dist.test.sh`, `tests/setup.test.sh`, `tests/version.test.sh`, `tests/reload.test.mjs`, the offline `tsc` check and TypeScript suites, `tests/views.test.py`, `tests/queries.test.py`)
- [ ] If I changed a file the mod ships and no `mod-vX.Y.Z` release is tagged yet: I rebuilt the served copy (`bash plugins/claude_code_mod/build_dist.sh plugins/claude_code_dist/bigquery-agent-analytics-mod`) and `build_dist.sh --check` passes
- [ ] I updated the docs, if behavior or commands changed
- [ ] Nothing in this change holds credentials, project IDs, dataset names or user data
