#!/usr/bin/env python3
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

"""Offline test of scripts/run_bq.py and scripts/setup_gcp_prereqs.py.

Nothing reaches Google Cloud and nothing needs the Python BigQuery client:
the client is a stand-in module, gcloud and bq are a stand-in for
subprocess.run, and the plans and messages are the ones the scripts print.
  - run_bq.py: the names that go into the SQL (a plain name is filled in, any
    other is refused before a client is made), the timestamps, the billing
    limit, the dry run, the errors, the variables it reads and its exit status;
  - setup_gcp_prereqs.py: a dry run (the default) calls no program, the
    arguments it refuses, the plan it prints for the documented command, what
    it checks before it grants anything, the dataset access it rewrites, and an
    --execute run that stops before any change or applies only the narrow
    grants;
  - the repository: the docs, the SQL headers and setup.sh name scripts that
    exist, the commands they show parse, and build_dist.sh ships the folder.
Needs only the standard library (and bash, for the setup.sh check). Run from
anywhere:
  python3 plugins/claude_code_mod/tests/scripts.test.py
Exit 0: every check passed; 1: a check failed
"""

import argparse
import contextlib
import dataclasses
import datetime
import importlib.util
import io
import json
import os
import pathlib
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import traceback
import types

sys.dont_write_bytecode = True

PLUGIN = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS = PLUGIN / 'scripts'
UTC = datetime.timezone.utc

passed = 0
failed = 0


def check(what, ok, detail=''):
    global passed, failed
    if ok:
        passed += 1
        print(f'ok - {what}')
    else:
        failed += 1
        print(f'not ok - {what}' + (f': {detail}' if detail else ''))


def refused(what, function, *params, error=Exception):
    """A check that function(*params) raises `error`."""
    try:
        function(*params)
    except error:
        check(what, True)
    else:
        check(what, False, 'it was accepted')


def load(name, alias=None):
    """A script, imported from its file (it is not a package), under the name `alias` when one is given."""
    spec = importlib.util.spec_from_file_location(alias or f'bqaa_{name}', SCRIPTS / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # a dataclass finds its module here
    spec.loader.exec_module(module)
    return module


@contextlib.contextmanager
def environment(**values):
    """The process environment without any variable the scripts read, then with these."""
    saved = dict(os.environ)
    for key in list(os.environ):
        if key.startswith(('BQAA_', 'BQ_', 'GCP_', 'GOOGLE_CLOUD_', 'GOOGLE_APPLICATION')):
            del os.environ[key]
    os.environ.update(values)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


@contextlib.contextmanager
def patched(target, **attributes):
    """Attributes of an object replaced for a while."""
    saved = {name: getattr(target, name) for name in attributes}
    for name, value in attributes.items():
        setattr(target, name, value)
    try:
        yield
    finally:
        for name, value in saved.items():
            setattr(target, name, value)


@contextlib.contextmanager
def modules(replacements):
    """sys.modules with these entries replaced (None makes an import fail), put back after."""
    missing = object()
    saved = {name: sys.modules.get(name, missing) for name in replacements}
    sys.modules.update(replacements)
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


NO_CLIENT = {'google': None, 'google.cloud': None, 'google.cloud.bigquery': None, 'google.cloud.exceptions': None}


def call(function, *argv):
    """(status, stdout, stderr) of function(argv). A SystemExit is its code, or, with a message, 1."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            status = function(list(argv))
        except SystemExit as exit_:
            status = exit_.code
            if isinstance(status, str):
                print(status, file=err)
                status = 1
            elif status is None:
                status = 0
    return status, out.getvalue(), err.getvalue()


def clean_environment():
    """A copy of the environment for a child process, without the variables the scripts read."""
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('BQAA_', 'BQ_', 'GCP_', 'GOOGLE_CLOUD_', 'GOOGLE_APPLICATION'))}
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


# ---------------------------------------------------------------------------
# run_bq.py
# ---------------------------------------------------------------------------

run_bq = load('run_bq')


class FakeBigQuery:
    """Stands in for the google.cloud.bigquery module, and keeps what run_bq asks of it."""

    def __init__(self, rows=(), scanned=0, error=None):
        self.clients, self.queries, self.fetched = [], [], 0
        outer = self

        class Job:
            total_bytes_processed = scanned

            def result(self):
                outer.fetched += 1
                return iter(list(rows))

        class Client:
            def __init__(self, project=None):
                self.project = project
                outer.clients.append(self)

            def query(self, sql, job_config=None):
                outer.queries.append((sql, job_config))
                if error:
                    raise RuntimeError(error)
                return Job()

        class QueryJobConfig:
            def __init__(self, **options):
                self.__dict__.update(options)

        class ScalarQueryParameter:
            def __init__(self, name, type_, value):
                self.name, self.type_, self.value = name, type_, value

        self.Client = Client
        self.QueryJobConfig = QueryJobConfig
        self.ScalarQueryParameter = ScalarQueryParameter


def run_bq_with(fake, *argv, **env):
    """(status, stdout, stderr) of run_bq.main(argv) with this stand-in client and environment."""
    with environment(**env), patched(run_bq, load_bigquery=lambda: fake):
        return call(run_bq.main, *argv)


def parameters(config):
    return {p.name: (p.type_, p.value) for p in config.query_parameters}


def dt(*parts):
    return datetime.datetime(*parts, tzinfo=UTC)


def run_bq_checks():
    sql = 'SELECT * FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE timestamp BETWEEN @start AND @end'
    base = ['--project', 'my-gcp-project', '--dataset', 'agent_analytics']

    # The names go into the SQL text: a plain name is filled in, anything else is refused.
    twice = 'SELECT 1 FROM `{PROJECT}.{DATASET}.{TABLE}` UNION ALL SELECT 2 FROM `{PROJECT}.{DATASET}.{TABLE}`'
    filled = run_bq.inject_placeholders(twice, 'my-gcp-project', 'agent_analytics', 'agent_events')
    check('every placeholder is filled in', filled == twice.replace('{PROJECT}', 'my-gcp-project')
          .replace('{DATASET}', 'agent_analytics').replace('{TABLE}', 'agent_events'), filled)
    check('a domain-scoped project id is taken',
          run_bq.inject_placeholders('{PROJECT}', 'example.com:my-project', 'agent_analytics', 'agent_events')
          == 'example.com:my-project')
    check('a name of 1024 characters is taken',
          run_bq.inject_placeholders('{DATASET}', 'my-gcp-project', 'a' * 1024, 'agent_events') == 'a' * 1024)
    good = ('my-gcp-project', 'agent_analytics', 'agent_events')
    for what, names in {
        'a dataset with a semicolon': (good[0], 'x;DROP', good[2]),
        'a dataset with a dot': (good[0], 'a.b', good[2]),
        'a table with a backtick': (good[0], good[1], 't`s'),
        'a dataset that ends in a newline': (good[0], 'agent_analytics\n', good[2]),
        'a table that ends in a newline': (good[0], good[1], 'agent_events\n'),
        'a project that ends in a newline': ('my-gcp-project\n', good[1], good[2]),
        'an uppercase project': ('My_Project', good[1], good[2]),
        'a project that is too short': ('abc', good[1], good[2]),
        'a project with a space': ('my gcp project', good[1], good[2]),
        'a dataset over 1024 characters': (good[0], 'a' * 1025, good[2]),
        'no project': ('', good[1], good[2]),
        'no dataset': (good[0], '', good[2]),
        'an empty table': (good[0], good[1], ''),
    }.items():
        refused(f'refused: {what}', run_bq.inject_placeholders, sql, *names, error=ValueError)
    for dataset in ('x;DROP', 'a.b', 'agent_analytics\n'):
        fake = FakeBigQuery()
        status, out, err = run_bq_with(fake, '--project', 'my-gcp-project', '--dataset', dataset, 'SELECT 1')
        check(f'a dataset of {dataset!r}: exit 1 before any client is made',
              status == 1 and fake.clients == [] and fake.queries == [], f'{status} {err!r}')

    # Timestamps.
    check('a bare start is the first instant of the day', run_bq.parse_ts('2026-10-01') == dt(2026, 10, 1))
    check('a bare end is the last microsecond of the day',
          run_bq.parse_ts('2026-10-01', is_end=True) == dt(2026, 10, 1, 23, 59, 59, 999999))
    check('--end takes a bare date as the end of that day',
          run_bq.parse_end_ts('2026-10-01') == dt(2026, 10, 1, 23, 59, 59, 999999))
    check('a timestamp ending in Z keeps its time', run_bq.parse_ts('2026-10-01T12:30:00Z') == dt(2026, 10, 1, 12, 30))
    check('a timestamp with no offset is UTC', run_bq.parse_ts('2026-10-01T12:30:00') == dt(2026, 10, 1, 12, 30))
    check('a timestamp with an offset keeps it', run_bq.parse_ts('2026-10-01T12:30:00+02:00') == dt(2026, 10, 1, 10, 30))
    check('an end timestamp keeps its time too',
          run_bq.parse_end_ts('2026-10-01T12:30:00Z') == dt(2026, 10, 1, 12, 30))
    for text in ('yesterday', '2026-13-01', '2026-02-30', ''):
        refused(f'a timestamp of {text!r} is refused', run_bq.parse_ts, text, error=argparse.ArgumentTypeError)
    check('--max-gb takes a positive number', run_bq.positive_float('0.5') == 0.5)
    for text in ('0', '-1', 'abc', 'nan', 'inf', ''):
        refused(f'--max-gb {text!r} is refused', run_bq.positive_float, text, error=argparse.ArgumentTypeError)
    check('bytes are written for a person',
          [run_bq.format_bytes(n) for n in (0, 1536, 2 ** 30)] == ['0.00 B', '1.50 KB', '1.00 GB'])

    # A run: the names filled in, the parameters bound, the bytes limited, one JSON object per row.
    fake = FakeBigQuery(rows=[{'day': datetime.date(2026, 10, 1), 'n': 3, 'tool': None},
                              {'day': datetime.date(2026, 10, 2), 'n': 4, 'tool': 'Bash'}])
    status, out, err = run_bq_with(fake, *base, '--start', '2026-10-01', '--end', '2026-10-31', '--max-gb', '2', sql)
    check('a run succeeds, and writes nothing to stderr', status == 0 and err == '', f'{status} {err!r}')
    check('it prints one JSON object per row, a line each',
          [json.loads(line) for line in out.splitlines()]
          == [{'day': '2026-10-01', 'n': 3, 'tool': None}, {'day': '2026-10-02', 'n': 4, 'tool': 'Bash'}], out)
    query, config = fake.queries[0] if fake.queries else ('', None)
    check('one client, for the project', len(fake.clients) == 1 and fake.clients[0].project == 'my-gcp-project')
    check('the names are filled into the SQL',
          'FROM `my-gcp-project.agent_analytics.agent_events`' in query and '{' not in query, query)
    check('the bytes billed are limited to --max-gb', config.maximum_bytes_billed == 2 * 2 ** 30)
    check('the query is standard SQL, and not a dry run',
          config.use_legacy_sql is False and not getattr(config, 'dry_run', False))
    check('@start and @end are bound as TIMESTAMPs, a bare end at the end of its day',
          parameters(config) == {'start': ('TIMESTAMP', dt(2026, 10, 1)),
                                 'end': ('TIMESTAMP', dt(2026, 10, 31, 23, 59, 59, 999999))}, parameters(config))
    fake = FakeBigQuery()
    run_bq_with(fake, *base, '--trace-id', 'trace-1', '--start', '2026-10-01T00:00:00Z', 'SELECT 1')
    check('@trace_id is bound as a STRING, and only the flags given are bound',
          parameters(fake.queries[0][1]) == {'start': ('TIMESTAMP', dt(2026, 10, 1)), 'trace_id': ('STRING', 'trace-1')})
    fake = FakeBigQuery()
    run_bq_with(fake, *base, 'SELECT 1')
    check('with no flags, nothing is bound, and 1 GB is the limit',
          parameters(fake.queries[0][1]) == {} and fake.queries[0][1].maximum_bytes_billed == 2 ** 30)
    fake = FakeBigQuery(rows=[{'x': 1}])
    status, out, err = run_bq_with(fake, *base, '-- 1. Event counts\nSELECT 1')
    check('a query that begins with a SQL comment is a query, not an option',
          status == 0 and fake.queries and fake.queries[0][0] == '-- 1. Event counts\nSELECT 1', f'{status} {err!r}')
    status, out, err = run_bq_with(FakeBigQuery(), *base, '--start', '2026-10-01', '--end', '2026-10-01', 'SELECT 1')
    check('a start and an end on one day are a whole day', status == 0, err)

    # A dry run: BigQuery's estimate, nothing fetched, nothing billed.
    fake = FakeBigQuery(scanned=1536)
    status, out, err = run_bq_with(fake, *base, '--dry-run', sql)
    check('a dry run succeeds', status == 0 and err == '', f'{status} {err!r}')
    check('it prints the estimate as one JSON object',
          out.strip() and json.loads(out) == {'dry_run': True, 'total_bytes_processed': 1536,
                                             'human_readable': '1.50 KB', 'exceeds_limit': False, 'limit_gb': 1}, out)
    config = fake.queries[0][1] if fake.queries else None
    check('it asks BigQuery for an estimate that skips the cache',
          config is not None and config.dry_run is True and config.use_query_cache is False)
    check('it fetches no rows, and sets no billing limit',
          fake.fetched == 0 and not hasattr(config, 'maximum_bytes_billed'))
    check('a dry run over the limit says so',
          json.loads(run_bq_with(FakeBigQuery(scanned=2 ** 30 + 1), *base, '--dry-run', 'SELECT 1')[1])['exceeds_limit'] is True)
    check('a dry run at the limit does not',
          json.loads(run_bq_with(FakeBigQuery(scanned=2 ** 30), *base, '--dry-run', 'SELECT 1')[1])['exceeds_limit'] is False)
    check('a fractional limit is reported as given',
          json.loads(run_bq_with(FakeBigQuery(scanned=1), *base, '--dry-run', '--max-gb', '0.5', 'SELECT 1')[1])['limit_gb'] == 0.5)

    # Errors: an ERROR line on stderr, exit 1; a bad argument, exit 2.
    fake = FakeBigQuery(error='Query exceeded limit for bytes billed: 1073741824. 2147483648 or higher are needed.')
    status, out, err = run_bq_with(fake, *base, sql)
    check('a client error is an ERROR line and exit 1',
          status == 1 and out == '' and err.startswith('ERROR: Query exceeded limit for bytes billed'), f'{status} {err!r}')
    status, out, err = run_bq_with(fake, *base, '--dry-run', sql)
    check('a dry run says so in its ERROR line', status == 1 and err.startswith('ERROR (dry-run): '), f'{status} {err!r}')
    fake = FakeBigQuery()
    status, out, err = run_bq_with(fake, '--dataset', 'agent_analytics', 'SELECT 1')
    check('no project: exit 1, naming the variable, with no client made',
          status == 1 and 'BQAA_PROJECT_ID' in err and fake.clients == [], f'{status} {err!r}')
    status, out, err = run_bq_with(fake, '--project', 'my-gcp-project', 'SELECT 1')
    check('no dataset: exit 1, naming the variable, with no client made',
          status == 1 and 'BQAA_DATASET' in err and fake.clients == [], f'{status} {err!r}')
    for what, argv in (('a start after the end', ['--start', '2026-10-31', '--end', '2026-10-01', 'SELECT 1']),
                       ('a limit of 0', ['--max-gb', '0', 'SELECT 1']),
                       ('a start that is no date', ['--start', 'yesterday', 'SELECT 1']),
                       ('no query', []),
                       ('both positional query and --query', ['--query', '1', 'SELECT 1']),
                       ('an unknown --query ID', ['--query', 'V99'])):
        fake = FakeBigQuery()
        status, out, err = run_bq_with(fake, *base, *argv)
        check(f'{what}: exit 2 with the usage', status == 2 and 'usage:' in err and fake.clients == [], f'{status} {err!r}')
    # Named queries via --query / -q (1..8 and V1..V7, case-insensitive) and ADC quota-project warning filter.
    named_map = run_bq.load_named_queries()
    check('load_named_queries loads 1..8 and V1..V7',
          list(named_map) == ['1', '2', '3', '4', '5', '6', '7', '8', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7'],
          list(named_map))
    for flag, qid, expected_id in (('--query', '4', '4'), ('-q', 'V5', 'V5'), ('--query', 'v1', 'V1')):
        fake = FakeBigQuery(rows=[{'ok': 1}])
        status, out, err = run_bq_with(fake, *base, flag, qid)
        expected_sql = run_bq.inject_placeholders(named_map[expected_id], 'my-gcp-project', 'agent_analytics', 'agent_events')
        check(f'{flag} {qid} resolves to query {expected_id} and runs it',
              status == 0 and len(fake.queries) == 1 and fake.queries[0][0] == expected_sql, f'{status} {err!r}')

    class QuotaWarningBigQuery(FakeBigQuery):
        def __init__(self):
            super().__init__(rows=[{'ok': 1}])
            orig_client = self.Client

            class WarnClient(orig_client):
                def __init__(self, project=None):
                    import warnings as _w
                    _w.warn(
                        'Your application has authenticated using end user credentials from Google Cloud SDK '
                        'without a quota project. You might receive a "quota exceeded" or "API not enabled" error.',
                        UserWarning,
                        stacklevel=2,
                    )
                    super().__init__(project=project)

            self.Client = WarnClient

    status, out, err = run_bq_with(QuotaWarningBigQuery(), *base, '--query', '1')
    check('run_bq suppresses the Google Cloud SDK ADC quota-project UserWarning on stderr',
          status == 0 and 'without a quota project' not in err and err == '', f'{status} {err!r}')
    with environment(), modules(NO_CLIENT):
        status, out, err = call(run_bq.main, '--help')
    check('--help needs no BigQuery client and documents --query and set-quota-project',
          status == 0 and '--max-gb' in out and '--query' in out and 'set-quota-project' in out.replace('-\n', '-'),
          f'{status} {out!r} {err!r}')
    with environment(), modules(NO_CLIENT):
        status, out, err = call(run_bq.main, *base, 'SELECT 1')
    check('without the BigQuery client: exit 1, and how to install it',
          status == 1 and err.startswith('ERROR:') and 'pip install google-cloud-bigquery' in err, f'{status} {err!r}')

    # The variables it reads, first one set wins.
    with environment(BQAA_PROJECT_ID='bqaa-one-aaa', GCP_PROJECT_ID='gcp-two-aaaa', GOOGLE_CLOUD_PROJECT='gcp-three-aa',
                     BQAA_DATASET='bqaa_dataset', BQ_DATASET='bq_dataset', BQAA_TABLE='bqaa_table', BQ_TABLE='bq_table'):
        args = run_bq.parse_args(['SELECT 1'])
    check('the BQAA_ variables come first',
          (args.project, args.dataset, args.table) == ('bqaa-one-aaa', 'bqaa_dataset', 'bqaa_table'))
    with environment(GCP_PROJECT_ID='gcp-two-aaaa', GOOGLE_CLOUD_PROJECT='gcp-three-aa', BQ_DATASET='bq_dataset',
                     BQ_TABLE='bq_table'):
        args = run_bq.parse_args(['SELECT 1'])
    check('GCP_PROJECT_ID, BQ_DATASET and BQ_TABLE come next',
          (args.project, args.dataset, args.table) == ('gcp-two-aaaa', 'bq_dataset', 'bq_table'))
    with environment(GOOGLE_CLOUD_PROJECT='gcp-three-aa'):
        args = run_bq.parse_args(['SELECT 1'])
    check('GOOGLE_CLOUD_PROJECT is the last project, and the table defaults to agent_events',
          (args.project, args.dataset, args.table) == ('gcp-three-aa', '', 'agent_events'))
    with environment(BQAA_PROJECT_ID='bqaa-one-aaa'):
        args = run_bq.parse_args(['--project', 'flag-project-a', 'SELECT 1'])
    check('a flag comes before any variable', args.project == 'flag-project-a')

    # As a program.
    for argv, status_wanted, text in (([], 2, 'usage:'), (['--help'], 0, '--max-gb')):
        result = subprocess.run([sys.executable, str(SCRIPTS / 'run_bq.py'), *argv], capture_output=True, text=True,
                                env=clean_environment())
        check(f'run_bq.py {" ".join(argv) or "with no query"}: exit {status_wanted}',
              result.returncode == status_wanted and text in result.stdout + result.stderr,
              f'{result.returncode} {result.stderr!r}')


# ---------------------------------------------------------------------------
# setup_gcp_prereqs.py
# ---------------------------------------------------------------------------

setup = load('setup_gcp_prereqs')

PROJECT = 'my-gcp-project'
DATASET = 'agent_analytics'
VIEWS = 'agent_analytics_reporting'
TABLE = 'agent_events'
WRITER_EMAIL = f'bqaa-row-writer@{PROJECT}.iam.gserviceaccount.com'
WRITER = f'serviceAccount:{WRITER_EMAIL}'
ROLE = f'projects/{PROJECT}/roles/bqaaRowWriter'
TOKEN_CREATORS = 'group:bqaa-writers@example.com'
READERS = 'group:bqaa-readers@example.com'
BASE = ['--project', PROJECT, '--dataset', DATASET]
# The command the docs show (docs/service-account-auth.md, "Set it up").
DOCUMENTED = [*BASE, '--no-create-dataset', '--no-create-table', '--service-account', 'bqaa-row-writer',
              '--writer-role', 'row-writer', '--token-creator', TOKEN_CREATORS, '--reader', READERS]


class Called(Exception):
    """A program was started by a run that must start none."""


@contextlib.contextmanager
def no_programs():
    """subprocess raises Called instead of starting anything."""
    def refuse(*args, **kwargs):
        raise Called(f'a program was started: {args[0] if args else kwargs}')
    with patched(subprocess, run=refuse, call=refuse, check_call=refuse, check_output=refuse, Popen=refuse):
        yield


def attempt(*argv, **env):
    """(status, stdout, stderr) of setup.main(argv) with no program and no BigQuery client to use."""
    with environment(**env), no_programs(), modules(NO_CLIENT):
        try:
            return call(setup.main, *argv)
        except Called as error:
            return 'called', '', str(error)


def steps_of(out):
    """[(label, what it runs)] from a printed plan, whose steps read `N. label` and an indented line."""
    steps = []
    for line in out.splitlines():
        numbered = re.fullmatch(r'(\d+)\. (.+)', line)
        if numbered and int(numbered.group(1)) == len(steps) + 1:
            steps.append([numbered.group(2), ''])
        elif line.startswith('   ') and steps and not steps[-1][1]:
            steps[-1][1] = line.strip()
    return [tuple(step) for step in steps]


def plan(*argv, **env):
    """The steps of the plan for argv, or None when the script did not print one."""
    status, out, err = attempt(*argv, **env)
    return steps_of(out) if status == 0 else None


def parsed(*argv, **env):
    """The arguments setup.main settles on for argv (a service account's principal filled in), or None.

    No program may start. A run that tries to (an --execute nobody asked for would check the credentials)
    raises Called, which fails the group, instead of calling the gcloud on this machine.
    """
    seen = []
    with environment(**env), no_programs(), patched(setup, _build_steps=lambda args: seen.append(args) or []):
        call(setup.main, *argv)
    return seen[0] if seen else None


def commands_of(steps):
    """The command lines of a plan: the steps that run gcloud or bq themselves, not a Python action."""
    return [command for _, command in steps if not command.startswith('python action:')]


def setup_dry_run_checks():
    status, out, err = attempt(*DOCUMENTED)
    check('the documented command is a dry run: exit 0, no program started, no BigQuery client needed',
          status == 0 and 'DRY RUN' in out and err == '', f'{status} {out!r} {err!r}')
    check('it says that nothing was changed or called, and how to apply the plan',
          'No changes made, and nothing called' in out and '--execute' in out, out)
    check('it prints the project, dataset, table and the writer',
          all(text in out for text in (f'project:  {PROJECT}', f'dataset:  {DATASET}', f'table:    {TABLE}',
                                       f'principal: {WRITER}')), out)
    status, out, err = attempt(*DOCUMENTED, '--dry-run')
    check('--dry-run is the same dry run', status == 0 and 'DRY RUN' in out, f'{status} {err!r}')
    status, out, err = attempt(*DOCUMENTED, '--dry-run', '--execute')
    check('--dry-run with --execute: exit 2', status == 2 and 'not allowed with argument' in err, f'{status} {err!r}')
    status, out, err = attempt('--help')
    check('--help: exit 0, needs no program and no client, and describes --project, --dataset, --table and --location defaults',
          status == 0 and '--writer-role' in out and 'BQAA_PROJECT_ID' in out and 'BQAA_DATASET' in out
          and 'BQAA_TABLE' in out and 'BQAA_LOCATION' in out,
          f'{status} {out!r} {err!r}')
    status, out, err = attempt(*BASE, '--no-create-dataset', '--no-create-table')
    check('--no-create-dataset and --no-create-table are accepted, and change nothing',
          status == 0 and plan(*BASE) == steps_of(out), f'{status} {err!r}')
    check('the plan is the same with or without --no-create-dataset and --no-create-table',
          plan(*DOCUMENTED) is not None
          and plan(*DOCUMENTED) == plan(*[a for a in DOCUMENTED if a not in ('--no-create-dataset', '--no-create-table')]))

    # The defaults come from the same variables as setup.sh, first one set wins; a flag beats them all.
    args = parsed(BQAA_PROJECT_ID='bqaa-one-aaa', GCP_PROJECT_ID='gcp-two-aaaa', GOOGLE_CLOUD_PROJECT='gcp-three-aa',
                  BQAA_DATASET='bqaa_dataset', BQ_DATASET='bq_dataset', BQAA_TABLE='bqaa_table', BQ_TABLE='bq_table',
                  BQAA_LOCATION='EU', BQAA_VIEWS_DATASET='reports')
    check('the BQAA_ variables come first',
          args is not None and (args.project, args.dataset, args.table, args.location, args.views_dataset)
          == ('bqaa-one-aaa', 'bqaa_dataset', 'bqaa_table', 'EU', 'reports'))
    args = parsed(GCP_PROJECT_ID='gcp-two-aaaa', GOOGLE_CLOUD_PROJECT='gcp-three-aa', BQ_DATASET='bq_dataset',
                  BQ_TABLE='bq_table')
    check('GCP_PROJECT_ID, BQ_DATASET and BQ_TABLE come next',
          args is not None and (args.project, args.dataset, args.table) == ('gcp-two-aaaa', 'bq_dataset', 'bq_table'))
    args = parsed(GOOGLE_CLOUD_PROJECT='gcp-three-aa')
    check('GOOGLE_CLOUD_PROJECT is the last project; the dataset, table and location have defaults',
          args is not None and (args.project, args.dataset, args.table, args.location, args.views_dataset)
          == ('gcp-three-aa', 'agent_analytics', 'agent_events', 'US', ''))
    args = parsed('--project', 'flag-project-a', BQAA_PROJECT_ID='bqaa-one-aaa')
    check('a flag comes before any variable', args is not None and args.project == 'flag-project-a')


def setup_refusal_checks():
    cases = (
        ('no project', [], '--project is required'),
        ('an empty dataset', ['--project', PROJECT, '--dataset', ''], '--dataset is required'),
        ('a token creator with no service account', [*BASE, '--token-creator', TOKEN_CREATORS],
         '--token-creator goes with --service-account'),
        ('a token creator with the data-editor role',
         [*BASE, '--service-account', 'w', '--token-creator', TOKEN_CREATORS],
         '--token-creator goes with --writer-role row-writer'),
        ('a principal that is not the service account',
         [*BASE, '--service-account', 'w', '--principal', 'serviceAccount:other@x.iam.gserviceaccount.com'],
         'is not --service-account'),
        ('a reader when the views dataset is the events dataset',
         [*BASE, '--views-dataset', DATASET, '--reader', READERS], 'the views dataset is the events dataset'),
        ('a domain as a reader', [*BASE, '--reader', 'domain:example.com'],
         'take user:, group: or serviceAccount: members'),
        ('a domain as a token creator',
         [*BASE, '--service-account', 'w', '--writer-role', 'row-writer', '--token-creator', 'domain:example.com'],
         'take user:, group: or serviceAccount: members'),
        ('a member that is everyone', [*BASE, '--reader', 'allUsers'],
         'take user:, group: or serviceAccount: members'),
        ('a principal with no prefix', [*BASE, '--principal', 'someone@example.com'],
         '--principal must start with one of'),
        ('a project that is no project id', ['--project', 'My Project', '--dataset', DATASET],
         'is not a project id'),
        ('a project that ends in a newline', ['--project', PROJECT + '\n', '--dataset', DATASET],
         'is not a project id'),
        ('a dataset with SQL in it', ['--project', PROJECT, '--dataset', 'x;DROP'],
         'is not a plain dataset or table name'),
        ('a dataset that ends in a newline', ['--project', PROJECT, '--dataset', DATASET + '\n'],
         'is not a plain dataset or table name'),
        ('a table with a backtick', [*BASE, '--table', 't`s'], 'is not a plain dataset or table name'),
        ('a views dataset with a hyphen', [*BASE, '--views-dataset', 'v-w'],
         'is not a plain dataset or table name'),
        ('a location with a space', [*BASE, '--location', 'us east'], 'is not a location'),
    )
    for what, argv, fragment in cases:
        for mode in ([], ['--execute']):
            status, out, err = attempt(*argv, *mode)
            check(f'refused, before any program or plan{" (even with --execute)" if mode else ""}: {what}',
                  status == 1 and fragment in err and out == '', f'{status} {out!r} {err!r}')
    status, out, err = attempt(*BASE, '--service-account', 'bqaa-row-writer', '--principal',
                               f'serviceAccount:{WRITER_EMAIL.upper()}')
    check('a principal that is the service account in other letters is not refused', status == 0, f'{status} {err!r}')
    check('a domain-scoped project is taken',
          attempt('--project', 'example.com:my-project', '--dataset', DATASET)[0] == 0)


def setup_plan_checks():
    steps = plan(*DOCUMENTED) or []
    labels = [label for label, _ in steps]
    order = ('Check that the events dataset and table are there', 'Enable Google Cloud APIs',
             f'Ensure service account {WRITER_EMAIL}', f'Check that {WRITER} holds nothing beyond the row-writer role',
             'Check who reads every row, and that no reader or token creator does', f'Ensure role {ROLE} (',
             'Grant the row-writer role to runtime principal, on the table alone',
             "Take the project's Viewer and Editor groups off ", f'Grant token creation on {WRITER_EMAIL} to {TOKEN_CREATORS}',
             f'Grant views dataset dataViewer to reader {READERS}', f'Grant project jobUser to reader {READERS}, to run queries',
             f'Authorize {", ".join(setup.VIEW_NAMES)} of {PROJECT}.{VIEWS} on {PROJECT}.{DATASET}')
    check('the documented command plans twelve steps: check, APIs, account, two checks, role, table grant, '
          'dataset access, token creator, reader grants, views',
          len(labels) == len(order) and all(label.startswith(start) for label, start in zip(labels, order)),
          '\n'.join(labels))
    checks = [i for i, label in enumerate(labels) if label.startswith(('Check that ' + WRITER, 'Check who reads'))]
    check('both checks come before the first grant',
          len(checks) == 2 and len(labels) == len(order) and max(checks) < labels.index(
              'Grant the row-writer role to runtime principal, on the table alone'), checks)
    commands = commands_of(steps)
    lines = [command for _, command in steps]
    check('it enables the APIs impersonation needs',
          any(c.startswith('gcloud services enable') and 'iam.googleapis.com' in c and 'iamcredentials.googleapis.com' in c
              and f'--project {PROJECT}' in c for c in commands), commands)
    check('the writer gets one binding: the row-writer role, on the table alone',
          [c for c in commands if WRITER in c]
          == [f'bq add-iam-policy-binding --member {WRITER} --role {ROLE} {PROJECT}:{DATASET}.{TABLE}'],
          [c for c in commands if WRITER in c])
    check('no command grants dataEditor', not any('dataEditor' in c for c in commands), commands)
    jobs = [c for c in commands if 'roles/bigquery.jobUser' in c]
    check('one command grants jobUser, to the reader and not to the writer',
          len(jobs) == 1 and READERS in jobs[0] and WRITER not in jobs[0], jobs)
    check('no command makes a dataset or a table',
          not any(re.search(r'\bmk\b|CREATE', c) for c in commands), commands)
    check('the reader gets dataViewer on the views dataset, and nothing is granted on the events dataset',
          any('roles/bigquery.dataViewer' in c and f'ON SCHEMA `{PROJECT}.{VIEWS}`' in c and READERS in c for c in commands)
          and not any('GRANT' in c and f'`{PROJECT}.{DATASET}`' in c for c in commands), commands)
    tokens = [line for line in lines if 'roles/iam.serviceAccountTokenCreator' in line]
    check('one step lets the group impersonate the writer',
          len(tokens) == 1 and f'--member {TOKEN_CREATORS}' in tokens[0] and WRITER_EMAIL in tokens[0], tokens)
    check('nothing runs --quiet unless it is asked to', not any('--quiet' in line for line in lines))

    # Other options change the plan the way their names say.
    quiet = plan(*DOCUMENTED, '--non-interactive') or []
    asked = commands_of(quiet) + [c for _, c in quiet if 'service-accounts add-iam-policy-binding' in c]
    check('--non-interactive makes every gcloud and bq command --quiet',
          len(asked) >= 5 and all(re.match(r'(python action: )?(gcloud|bq) --quiet ', c) for c in asked), asked)
    kept = plan(*DOCUMENTED, '--keep-project-groups') or []
    check('--keep-project-groups drops the step that takes the project groups off the datasets',
          len(kept) == len(order) - 1 and not any(label.startswith("Take the project's") for label, _ in kept))
    named = plan(*DOCUMENTED, '--views-dataset', 'reports') or []
    check('--views-dataset moves the reader grant, the views and the dataset access',
          any(f'ON SCHEMA `{PROJECT}.reports`' in c for c in commands_of(named))
          and any(f'of {PROJECT}.reports on {PROJECT}.{DATASET}' in label for label, _ in named)
          and any(label.endswith(f'Editors off {PROJECT}.reports') for label, _ in named), named)
    eu = plan(*DOCUMENTED, '--location', 'EU') or []
    check('--location goes to the GRANT statements',
          any('--location=EU' in c and 'GRANT' in c for c in commands_of(eu)) and not any('--location=US' in c for c in commands_of(eu)))
    skipped = plan(*DOCUMENTED, '--no-enable-apis') or []
    check('--no-enable-apis enables nothing, and checks the API impersonation needs',
          not any(label.startswith('Enable Google Cloud APIs') for label, _ in skipped)
          and any(label == 'Check that iamcredentials.googleapis.com is enabled' for label, _ in skipped))
    granting = plan(*DOCUMENTED, '--no-grant-iam') or []
    glabels = [label for label, _ in granting]
    check('--no-grant-iam grants nothing to the writer, and checks the role it would hold',
          not any(label.startswith(('Ensure role', 'Grant the row-writer role')) for label in glabels)
          and any(label.startswith(f'Check that role {ROLE}') for label in glabels)
          and any(label.startswith('Check that ' + WRITER) for label in glabels)
          and not any('bq add-iam-policy-binding' in c for c in commands_of(granting)), glabels)
    check('--no-grant-iam keeps what is for the readers and the token creator',
          any(label.startswith('Grant token creation') for label in glabels)
          and any(label.startswith('Grant views dataset dataViewer') for label in glabels))

    # The default writer role is the one that also reads, and it takes none of the reader steps.
    editor = plan(*BASE, '--service-account', 'bqaa-writer') or []
    elabels = [label for label, _ in editor]
    check('without --writer-role, the writer gets dataEditor and jobUser on the project',
          elabels[:3] == ['Check that the events dataset is there', 'Enable Google Cloud APIs',
                          f'Ensure service account bqaa-writer@{PROJECT}.iam.gserviceaccount.com']
          and elabels[3:] == ['Grant dataset dataEditor to runtime principal',
                              'Grant project jobUser for verification queries'],
          elabels)
    check('the dataEditor grant is on the events dataset, to the writer',
          any('roles/bigquery.dataEditor' in c and f'ON SCHEMA `{PROJECT}.{DATASET}`' in c
              and f'serviceAccount:bqaa-writer@{PROJECT}.iam.gserviceaccount.com' in c for c in commands_of(editor)), editor)
    check('without a principal, no IAM grant is planned and the plan says so',
          [label for label, _ in (plan(*BASE) or [])] == ['Enable Google Cloud APIs']
          and 'IAM grants are skipped because --principal was not provided.' in attempt(*BASE)[1])
    only_readers = plan(*BASE, '--reader', READERS) or []
    rlabels = [label for label, _ in only_readers]
    check('readers alone: the events dataset is checked first, then who reads every row, and no writer is touched',
          rlabels[:1] == ['Check that the events dataset is there'] and rlabels[1] == 'Enable Google Cloud APIs'
          and rlabels[2].startswith('Check who reads every row')
          and not any('service account' in label or 'row-writer' in label for label in rlabels), rlabels)
    check('the project jobUser grant for a reader comes after the checks',
          rlabels.index(f'Grant project jobUser to reader {READERS}, to run queries') > 2)


def reach_checks():
    user, other = 'user:ann@example.com', 'user:bob@example.com'
    group, other_group = 'group:team@example.com', 'group:ops@example.com'
    for what, named, member, wanted in (
        ('allUsers reaches everyone', 'allUsers', user, 'yes'),
        ('allAuthenticatedUsers reaches everyone', 'allAuthenticatedUsers', WRITER, 'yes'),
        ('a special group of that name reaches everyone', 'specialGroup:allAuthenticatedUsers', user, 'yes'),
        ('a person reaches the person', user, user, 'yes'),
        ('the letters of an address do not matter', 'user:ANN@Example.com', user, 'yes'),
        ('a service account reaches itself', WRITER, WRITER, 'yes'),
        ('a dataset writes a service account as a user', f'user:{WRITER_EMAIL}', WRITER, 'yes'),
        ('a group reaches itself', group, group, 'yes'),
        ('a domain reaches its people', 'domain:example.com', user, 'yes'),
        ('a person is not reached by another person', other, user, 'no'),
        ('a person is not reached by a special group of the project', 'specialGroup:projectOwners', user, 'no'),
        ('another domain may reach a person, under another name', 'domain:other.com', user, 'maybe'),
        ('a service account belongs to no domain but its own', 'domain:other.com', WRITER, 'no'),
        ('a domain may hold any group member', 'domain:example.com', group, 'maybe'),
        ('a group may hold a person', group, user, 'maybe'),
        ('a person may be in a group', user, group, 'maybe'),
        ('a group may hold another group', other_group, group, 'maybe'),
        ('an identity pool may hold a person', 'principalSet://iam.googleapis.com/pool/x', user, 'maybe'),
        ('a single pool identity belongs to no domain', 'domain:example.com', 'principal://iam.googleapis.com/pool/x', 'no'),
    ):
        got = setup._reach(named, member)
        check(f'_reach: {what}', got == wanted, f'{named} -> {member}: {got!r}, not {wanted!r}')


# What a stand-in project answers with: the datasets' default access, and two roles gcloud iam roles describe finds.
DEFAULT_ACCESS = ({'role': 'OWNER', 'specialGroup': 'projectOwners'}, {'role': 'WRITER', 'specialGroup': 'projectWriters'},
                  {'role': 'READER', 'specialGroup': 'projectReaders'})
JOB_USER = {'includedPermissions': ['bigquery.jobs.create']}
ROW_WRITER = {'includedPermissions': list(setup.ROW_WRITER_PERMISSIONS), 'stage': 'GA'}


class FakeCloud:
    """Stands in for subprocess.run: answers gcloud and bq from a small model of a project, and logs what it is asked.

    `calls` holds every command and `mutations` the ones that change something. The model starts as a project with
    the events dataset and table and the views dataset (the default access each has), no writer, and no grant.
    """

    READS = (('show',), ('get-iam-policy',), ('iam', 'service-accounts', 'describe'), ('iam', 'roles', 'describe'),
             ('projects', 'get-iam-policy'), ('identity', 'groups', 'memberships'), ('services', 'list'))

    def __init__(self):
        self.calls, self.mutations = [], []
        self.datasets = {name: [dict(entry) for entry in DEFAULT_ACCESS] for name in (DATASET, VIEWS)}
        self.tables = {f'{DATASET}.{TABLE}'}
        self.project_bindings, self.table_bindings = [], []
        self.service_account = False
        self.roles = {'roles/bigquery.jobUser': JOB_USER}
        self.unreadable = set()  # roles that gcloud iam roles describe is denied
        self.membership = {}  # (group, member) -> True or False; a pair left out is one Cloud Identity cannot answer
        self.enabled = set()  # what gcloud services list shows
        self.failing = set()  # kinds of command that fail, like `gcloud services enable`

    def __call__(self, command, **options):
        command = list(command)
        self.calls.append(command)
        words = [word for word in command if word != '--quiet']
        plain = tuple(word for word in words[1:] if not word.startswith('-'))
        if any(plain[:len(prefix)] == prefix for prefix in self.READS):
            stdout, returncode, stderr = self.read(words, plain)
        elif kind(command) in self.failing:
            self.mutations.append(command)
            stdout, returncode, stderr = '', 1, 'ERROR: this command was made to fail'
        else:
            self.mutations.append(command)
            stdout, returncode, stderr = self.change(words, plain)
        if returncode and options.get('check'):
            raise subprocess.CalledProcessError(returncode, command, stdout, stderr)
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    def read(self, words, plain):
        if words[0] == 'bq' and plain[0] == 'show':
            name = plain[1]
            dataset, _, table = name.partition(':')[2].partition('.')
            if (f'{dataset}.{table}' in self.tables) if table else (dataset in self.datasets):
                return json.dumps({'id': name, 'access': self.datasets.get(dataset, [])}), 0, ''
            return '', 1, f'BigQuery error in show operation: Not found: {"Table" if table else "Dataset"} {name}'
        if words[0] == 'bq':
            return json.dumps({'bindings': self.table_bindings}), 0, ''
        if plain[:2] == ('projects', 'get-iam-policy'):
            return json.dumps({'bindings': self.project_bindings}), 0, ''
        if plain[:3] == ('iam', 'service-accounts', 'describe'):
            return ('{}', 0, '') if self.service_account else ('', 1, 'ERROR: NOT_FOUND: Unknown service account')
        if plain[:3] == ('iam', 'roles', 'describe'):
            return self.describe(words, plain[3])
        if plain[:3] == ('identity', 'groups', 'memberships'):
            flags = dict(word.split('=', 1) for word in words if word.startswith(('--group-email=', '--member-email=')))
            answer = self.membership.get((flags['--group-email'].lower(), flags['--member-email'].lower()))
            if answer is None:
                return '', 1, 'ERROR: PERMISSION_DENIED (403): the caller cannot check this membership'
            return (json.dumps({'hasMembership': True}) if answer else '{}'), 0, ''  # a false answer is left out
        if plain[:2] == ('services', 'list'):
            service = next(word.rpartition('=')[2] for word in words if word.startswith('--filter=config.name='))
            return (service + '\n' if service in self.enabled else ''), 0, ''
        raise AssertionError(f'a command this stand-in does not answer: {words}')

    def describe(self, words, name):
        if '--project' in words:
            key = f'projects/{words[words.index("--project") + 1]}/roles/{name}'
        elif '--organization' in words:
            key = f'organizations/{words[words.index("--organization") + 1]}/roles/{name}'
        else:
            key = name
        if key in self.unreadable:
            return '', 1, 'ERROR: PERMISSION_DENIED: iam.roles.get denied'
        if key not in self.roles:
            return '', 1, 'ERROR: (gcloud.iam.roles.describe) NOT_FOUND: The role was not found.'
        return json.dumps(self.roles[key]), 0, ''

    def change(self, words, plain):
        project = words[words.index('--project') + 1] if '--project' in words else PROJECT
        if plain[:3] == ('iam', 'service-accounts', 'create'):
            self.service_account = True
        elif plain[:3] == ('iam', 'roles', 'create'):
            permissions = words[words.index('--permissions') + 1].split(',')
            self.roles[f'projects/{project}/roles/{plain[3]}'] = {'includedPermissions': permissions, 'stage': 'GA'}
        elif plain[:3] == ('iam', 'roles', 'update'):
            key = f'projects/{project}/roles/{plain[3]}'  # a new dict: the one given to the stand-in may be shared
            added = words[words.index('--add-permissions') + 1].split(',')
            self.roles[key] = {**self.roles[key], 'includedPermissions': [*self.roles[key]['includedPermissions'], *added]}
        elif plain[:2] == ('services', 'enable'):
            self.enabled.update(word for word in plain[2:] if word.endswith('.googleapis.com'))
        return '', 0, ''


def kind(command):
    """What a command is: `gcloud iam roles create`, `bq GRANT`, `bq add-iam-policy-binding`, ..."""
    plain = [word for word in command[1:] if not word.startswith('-')]
    if command[0] == 'bq':
        return 'bq GRANT' if plain[0] == 'query' and plain[1].startswith('GRANT') else f'bq {plain[0]}'
    return 'gcloud ' + ' '.join(plain[:3] if plain[0] == 'iam' else plain[:2])


class NotFound(Exception):
    """Stands in for google.cloud.exceptions.NotFound."""


class AccessEntry:
    """Stands in for bigquery.AccessEntry."""

    def __init__(self, role, entity_type, entity_id):
        self.role, self.entity_type, self.entity_id = role, entity_type, entity_id


class Table:
    def __init__(self, project, dataset_id, table_id, table_type):
        self.project, self.dataset_id, self.table_id, self.table_type = project, dataset_id, table_id, table_type


class Dataset:
    def __init__(self, entries):
        self.access_entries = list(entries)


class FakeClient:
    """Stands in for bigquery.Client over a few datasets and tables; `updates` keeps each update_dataset."""

    def __init__(self, datasets, tables):
        self.datasets, self.tables, self.updates = datasets, tables, []

    def get_dataset(self, reference):
        try:
            return self.datasets[reference.split('.')[1]]
        except KeyError:
            raise NotFound(reference) from None

    def get_table(self, reference):
        if reference not in self.tables:
            raise NotFound(reference)
        project, dataset_id, table_id = reference.split('.')
        return Table(project, dataset_id, table_id, self.tables[reference])

    def update_dataset(self, dataset, fields):
        self.updates.append((dataset, list(fields), list(dataset.access_entries)))


DEFAULT_ENTRIES = (('OWNER', 'specialGroup', 'projectOwners'), ('WRITER', 'specialGroup', 'projectWriters'),
                   ('READER', 'specialGroup', 'projectReaders'))


def fake_client(events=DEFAULT_ENTRIES, views=DEFAULT_ENTRIES, tables=None):
    """A stand-in client over the events dataset and the views dataset, with the mod's two views in the latter."""
    if tables is None:
        tables = {f'{PROJECT}.{VIEWS}.{name}': 'VIEW' for name in setup.VIEW_NAMES}
    return FakeClient({DATASET: Dataset([AccessEntry(*entry) for entry in events]),
                       VIEWS: Dataset([AccessEntry(*entry) for entry in views])}, tables)


def google_modules(client):
    """The sys.modules entries that make `from google.cloud import bigquery` give this client."""
    bigquery = types.ModuleType('google.cloud.bigquery')
    bigquery.Client = lambda project=None: client
    bigquery.AccessEntry = AccessEntry
    exceptions = types.ModuleType('google.cloud.exceptions')
    exceptions.NotFound = NotFound
    cloud = types.ModuleType('google.cloud')
    cloud.bigquery, cloud.exceptions = bigquery, exceptions
    google = types.ModuleType('google')
    google.cloud = cloud
    return {'google': google, 'google.cloud': cloud, 'google.cloud.bigquery': bigquery,
            'google.cloud.exceptions': exceptions}


def held(dataset):
    """What a stand-in dataset's access names, one string an entry."""
    return [f'view:{e.entity_id["datasetId"]}.{e.entity_id["tableId"]}' if isinstance(e.entity_id, dict)
            else f'{e.role} {e.entity_type}:{e.entity_id}' for e in dataset.access_entries]


def outcome(function, *params, cloud=None, **named):
    """(value, stop, printed) of function(*params): what it returns, the message of its SystemExit, what it prints.

    Programs go to `cloud` (none may start without one), and sleeping does not wait.
    """
    out = io.StringIO()
    with contextlib.ExitStack() as stack:
        stack.enter_context(patched(subprocess, run=cloud) if cloud is not None else no_programs())
        stack.enter_context(patched(time, sleep=lambda seconds: None))
        stack.enter_context(contextlib.redirect_stdout(out))
        try:
            return function(*params, **named), None, out.getvalue()
        except SystemExit as stop:
            return None, str(stop), out.getvalue()


def writer_grants_checks():
    def writer_check(cloud, *flags):
        return outcome(setup._check_writer_grants, parsed(*DOCUMENTED, *flags), cloud=cloud)

    def binding(role, *members):
        return {'role': role, 'members': list(members)}

    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/owner', 'user:ann@example.com')]
    cloud.table_bindings = [binding(ROLE, WRITER, 'user:bob@example.com')]
    _, stop, out = writer_check(cloud)
    check('a writer that holds only its row-writer binding on the table: OK',
          stop is None and out.startswith(f'OK {WRITER} holds nothing'), f'{stop!r} {out!r}')
    check('the check reads the project, the events and views datasets and the table, and changes nothing',
          cloud.mutations == [] and [c[-1] for c in cloud.calls if 'show' in c] == [f'{PROJECT}:{DATASET}', f'{PROJECT}:{VIEWS}']
          and any(c[:3] == ['gcloud', 'projects', 'get-iam-policy'] for c in cloud.calls)
          and any(c[1:3] == ['--format=json', 'get-iam-policy'] for c in cloud.calls), cloud.calls)

    for what, setup_state, fragment in (
        ('dataEditor on the project',
         lambda c: c.project_bindings.append(binding('roles/bigquery.dataEditor', WRITER)),
         f'roles/bigquery.dataEditor on the project {PROJECT} (granted to {WRITER})'),
        ('a role of no consequence on the project (any grant counts)',
         lambda c: c.project_bindings.append(binding('roles/viewer', WRITER)), f'roles/viewer on the project {PROJECT}'),
        ('a grant to everyone',
         lambda c: c.project_bindings.append(binding('roles/bigquery.dataViewer', 'allAuthenticatedUsers')),
         '(granted to allAuthenticatedUsers)'),
        ('an entry of the events dataset',
         lambda c: c.datasets[DATASET].append({'role': 'WRITER', 'userByEmail': WRITER_EMAIL}),
         f'WRITER on the dataset {PROJECT}.{DATASET}'),
        ('an entry of the views dataset',
         lambda c: c.datasets[VIEWS].append({'role': 'READER', 'userByEmail': WRITER_EMAIL}),
         f'READER on the dataset {PROJECT}.{VIEWS}'),
        ('another role on the table',
         lambda c: c.table_bindings.append(binding('roles/bigquery.dataViewer', WRITER)),
         f'roles/bigquery.dataViewer on the table {PROJECT}.{DATASET}.{TABLE}'),
        ('the row-writer role granted to everyone',
         lambda c: c.table_bindings.append(binding(ROLE, 'allUsers')), '(granted to allUsers)'),
    ):
        cloud = FakeCloud()
        setup_state(cloud)
        _, stop, out = writer_check(cloud)
        check(f'refused, naming the grant: {what}',
              stop is not None and f'{WRITER} already holds more than the row-writer role' in stop and fragment in stop
              and '--allow-existing-grants keeps them' in stop, stop)
        _, stop, out = writer_check(cloud, '--allow-existing-grants')
        check(f'--allow-existing-grants keeps it, and says so: {what}',
              stop is None and 'KEPT (--allow-existing-grants)' in out, f'{stop!r} {out!r}')

    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/viewer', 'group:eng@example.com')]
    _, stop, out = writer_check(cloud)
    check('a group Cloud Identity cannot answer for is refused as not ruled out',
          stop is not None and 'could not rule out that these reach further' in stop and 'group:eng@example.com' in stop, stop)
    _, stop, out = writer_check(cloud, '--allow-existing-grants')
    check('--allow-existing-grants does not accept what is not ruled out', stop is not None and 'could not rule out' in stop, stop)
    _, stop, out = writer_check(cloud, '--accept-unverified')
    check('--accept-unverified accepts it, and says so',
          stop is None and 'ACCEPTED (--accept-unverified), not ruled out' in out, f'{stop!r} {out!r}')
    cloud.membership[('eng@example.com', WRITER_EMAIL)] = False
    _, stop, out = writer_check(cloud)
    check('a group the writer is not in, as Cloud Identity answers, is let through', stop is None and out.startswith('OK '), f'{stop!r} {out!r}')
    cloud.membership[('eng@example.com', WRITER_EMAIL)] = True
    _, stop, out = writer_check(cloud)
    check('a group the writer is in, as Cloud Identity answers, is refused',
          stop is not None and '(the writer is a member, as Cloud Identity answers)' in stop, stop)
    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/viewer', 'domain:other.com'), binding('roles/viewer', 'user:bob@example.com')]
    _, stop, out = writer_check(cloud)
    check("another domain's grant, and another person's, do not reach a service account", stop is None and out.startswith('OK '), f'{stop!r} {out!r}')


def row_writer_role_checks():
    args = parsed(*DOCUMENTED)

    def cloud_with(role=None, unreadable=False):
        cloud = FakeCloud()
        if role is not None:
            cloud.roles[ROLE] = role
        if unreadable:
            cloud.unreadable = {ROLE}
        return cloud

    both, get = list(setup.ROW_WRITER_PERMISSIONS), ['bigquery.tables.get']
    for what, cloud, wanted in (
        ('no role yet', cloud_with(), None),
        ('a role with the two permissions', cloud_with(ROW_WRITER), []),
        ('a role missing one', cloud_with({'includedPermissions': get}), ['bigquery.tables.updateData']),
    ):
        value, stop, out = outcome(setup._row_writer_role, args, cloud=cloud)
        check(f'_row_writer_role: {what}', stop is None and value == wanted, f'{value!r} {stop!r}')
        check(f'_row_writer_role: {what}: it read the role of the project, and changed nothing',
              cloud.mutations == [] and any('describe' in c and 'bqaaRowWriter' in c and PROJECT in c for c in cloud.calls), cloud.calls)
    for what, cloud, fragment in (
        ('a permission more', cloud_with({'includedPermissions': [*both, 'bigquery.tables.getData']}),
         'also holds bigquery.tables.getData'),
        ('a role that is deleted', cloud_with({'includedPermissions': both, 'deleted': True}), 'is deleted'),
        ('a role that is disabled', cloud_with({'includedPermissions': both, 'stage': 'DISABLED'}), 'is disabled'),
        ('a role it may not read', cloud_with(ROW_WRITER, unreadable=True), 'Could not read'),
    ):
        value, stop, out = outcome(setup._row_writer_role, args, cloud=cloud)
        check(f'_row_writer_role stops: {what}', stop is not None and fragment in stop, f'{value!r} {stop!r}')

    # The role the plan makes: only what inserting rows takes.
    cloud = cloud_with()
    value, stop, out = outcome(setup._ensure_row_writer_role, args, cloud=cloud)
    create = cloud.mutations[0] if cloud.mutations else []
    check('a missing role is made with the two permissions and no other',
          stop is None and len(cloud.mutations) == 1 and kind(create) == 'gcloud iam roles create'
          and create[create.index('--permissions') + 1] == 'bigquery.tables.get,bigquery.tables.updateData'
          and 'CREATED role' in out, f'{cloud.mutations} {stop!r}')
    cloud = cloud_with(ROW_WRITER)
    value, stop, out = outcome(setup._ensure_row_writer_role, args, cloud=cloud)
    check('a role that is right is left alone', stop is None and cloud.mutations == [] and 'OK role exists' in out, f'{cloud.mutations} {out!r}')
    cloud = cloud_with({'includedPermissions': get})
    value, stop, out = outcome(setup._ensure_row_writer_role, args, cloud=cloud)
    update = cloud.mutations[0] if cloud.mutations else []
    check('a role that lacks one permission gets that one',
          stop is None and len(cloud.mutations) == 1 and kind(update) == 'gcloud iam roles update'
          and update[update.index('--add-permissions') + 1] == 'bigquery.tables.updateData' and 'UPDATED role' in out, f'{cloud.mutations} {stop!r}')
    cloud = cloud_with({'includedPermissions': [*both, 'bigquery.tables.delete']})
    value, stop, out = outcome(setup._ensure_row_writer_role, args, cloud=cloud)
    check('a role that holds more is not touched', stop is not None and 'bigquery.tables.delete' in stop and cloud.mutations == [], f'{stop!r}')

    # --no-grant-iam: the role is only looked at.
    nogrant = parsed(*DOCUMENTED, '--no-grant-iam')
    cloud = cloud_with()
    value, stop, out = outcome(setup._check_row_writer_role, nogrant, cloud=cloud)
    check('--no-grant-iam, no role: OK, and nothing is made', stop is None and out.startswith(f'OK no role {ROLE}') and cloud.mutations == [], f'{stop!r} {out!r}')
    cloud = cloud_with()
    cloud.table_bindings = [{'role': ROLE, 'members': [WRITER]}]
    value, stop, out = outcome(setup._check_row_writer_role, nogrant, cloud=cloud)
    check('--no-grant-iam, no role but the writer bound to it: stops', stop is not None and 'the role cannot be found' in stop, f'{stop!r}')
    cloud = cloud_with({'includedPermissions': get})
    value, stop, out = outcome(setup._check_row_writer_role, nogrant, cloud=cloud)
    check('--no-grant-iam, a role that lacks one: says so, and adds nothing',
          stop is None and 'MISSING' in out and 'bigquery.tables.updateData' in out and cloud.mutations == [], f'{stop!r} {out!r}')
    cloud = cloud_with(ROW_WRITER)
    value, stop, out = outcome(setup._check_row_writer_role, nogrant, cloud=cloud)
    check('--no-grant-iam, a role that is right: OK', stop is None and out.startswith(f'OK role {ROLE} holds'), f'{stop!r} {out!r}')
    cloud = cloud_with({'includedPermissions': [*both, 'bigquery.tables.getData']})
    value, stop, out = outcome(setup._check_row_writer_role, nogrant, cloud=cloud)
    check('--no-grant-iam, a role that holds more: stops', stop is not None and 'bigquery.tables.getData' in stop, f'{stop!r}')


def resources_checks():
    args = parsed(*DOCUMENTED)
    cloud = FakeCloud()
    value, stop, out = outcome(setup._check_resources_exist, args, True, cloud=cloud)
    check('the events dataset and table are there: OK', stop is None and out == f'OK {PROJECT}.{DATASET}.{TABLE} is there\n', f'{stop!r} {out!r}')
    cloud = FakeCloud()
    cloud.datasets, cloud.tables = {}, set()
    value, stop, out = outcome(setup._check_resources_exist, args, True, cloud=cloud)
    check('no dataset: stops, says to run setup.sh, and nothing was changed',
          stop is not None and f'The events dataset {PROJECT}:{DATASET} is not there' in stop and "mod's setup.sh" in stop
          and 'Nothing was changed' in stop and cloud.mutations == [], stop)
    cloud = FakeCloud()
    cloud.tables = set()
    value, stop, out = outcome(setup._check_resources_exist, args, True, cloud=cloud)
    check('no table, when the plan grants on it: stops', stop is not None and f'The events table {PROJECT}:{DATASET}.{TABLE} is not there' in stop, stop)
    value, stop, out = outcome(setup._check_resources_exist, args, False, cloud=cloud)
    check('no table, when the plan does not grant on it: OK', stop is None and out == f'OK {PROJECT}.{DATASET} is there\n', f'{stop!r} {out!r}')


def readers_checks():
    def readers_check(cloud, *flags):
        return outcome(setup._check_project_readers, parsed(*DOCUMENTED, *flags), cloud=cloud)

    def binding(role, *members):
        return {'role': role, 'members': list(members)}

    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/bigquery.jobUser', 'user:ann@example.com')]
    _, stop, out = readers_check(cloud)
    check('nobody reads beyond the views: OK',
          stop is None and out.splitlines()[-1].startswith('OK no grant to everyone'), f'{stop!r} {out!r}')
    check('the check changes nothing', cloud.mutations == [])

    for what, setup_state, fragment in (
        ('a group that is a reader, with READER on the events dataset',
         lambda c: c.datasets[DATASET].append({'role': 'READER', 'groupByEmail': 'bqaa-readers@example.com'}),
         'a reader or token creator can read every row, views or not'),
        ('a group that is a reader, with dataViewer on the table',
         lambda c: c.table_bindings.append(binding('roles/bigquery.dataViewer', READERS)),
         'a reader or token creator can read every row, views or not'),
        ('everyone, on the table',
         lambda c: c.table_bindings.append(binding('roles/bigquery.dataViewer', 'allUsers')),
         'everyone can read the table'),
        ('everyone, with dataViewer on the project',
         lambda c: c.project_bindings.append(binding('roles/bigquery.dataViewer', 'allAuthenticatedUsers')),
         'everyone can read the table'),
        ('a token creator who may edit the custom roles',
         lambda c: (c.project_bindings.append(binding('roles/iam.roleAdmin', TOKEN_CREATORS)),
                    c.roles.update({'roles/iam.roleAdmin': {'includedPermissions': ['iam.roles.update', 'iam.roles.get']}})),
         "a token creator may edit the project's custom roles"),
    ):
        cloud = FakeCloud()
        setup_state(cloud)
        _, stop, out = readers_check(cloud)
        check(f'refused: {what}', stop is not None and 'the table can be read beyond the views' in stop and fragment in stop, stop)

    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/owner', 'user:ann@example.com')]
    _, stop, out = readers_check(cloud)
    check('an owner who may be in a reader group, with Cloud Identity silent: not ruled out',
          stop is not None and 'could not rule out' in stop and 'user:ann@example.com' in stop, stop)
    _, stop, out = readers_check(cloud, '--accept-unverified')
    check('--accept-unverified accepts that', stop is None and 'ACCEPTED (--accept-unverified)' in out, f'{stop!r} {out!r}')
    cloud.membership.update({(group, 'ann@example.com'): False for group in ('bqaa-readers@example.com', 'bqaa-writers@example.com')})
    _, stop, out = readers_check(cloud)
    check('an owner who is in neither group, as Cloud Identity answers: OK, and the note says so',
          stop is None and 'not a member, as Cloud Identity answers' in out.splitlines()[-1], f'{stop!r} {out!r}')
    cloud.membership[('bqaa-readers@example.com', 'ann@example.com')] = True
    _, stop, out = readers_check(cloud)
    check('an owner who is in the reader group: refused', stop is not None and '(a member, as Cloud Identity answers)' in stop, stop)
    _, stop, out = readers_check(cloud, '--allow-existing-grants')
    check('--allow-existing-grants keeps that, and says so', stop is None and 'KEPT (--allow-existing-grants)' in out, f'{stop!r} {out!r}')

    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/viewer', 'user:dev@example.com')]
    cloud.membership[('bqaa-readers@example.com', 'dev@example.com')] = True
    cloud.membership[('bqaa-writers@example.com', 'dev@example.com')] = True
    _, stop, out = readers_check(cloud)
    check('a project Viewer in a reader group is let through: the setup takes the Viewer group off the dataset',
          stop is None and out.splitlines()[-1].startswith('OK '), f'{stop!r} {out!r}')
    _, stop, out = readers_check(cloud, '--keep-project-groups')
    check('with --keep-project-groups it is refused: Viewers read every row',
          stop is not None and 'roles/viewer on the project' in stop, stop)
    cloud = FakeCloud()
    cloud.project_bindings = [binding('roles/custom.readsAll', TOKEN_CREATORS)]
    cloud.unreadable = {'roles/custom.readsAll'}
    _, stop, out = readers_check(cloud)
    check('a role whose permissions cannot be read is not ruled out',
          stop is not None and 'a role whose permissions could not be read' in stop, stop)
    cloud = FakeCloud()
    cloud.project_bindings = [binding(f'projects/{PROJECT}/roles/custom.readsAll', TOKEN_CREATORS)]
    cloud.roles[f'projects/{PROJECT}/roles/custom.readsAll'] = {'includedPermissions': ['bigquery.tables.getData']}
    _, stop, out = readers_check(cloud)
    check('a custom role that reads table data counts as reading, by its permissions',
          stop is not None and 'a reader or token creator can read every row' in stop, stop)


def api_checks():
    args = parsed(*DOCUMENTED, '--no-enable-apis')
    cloud = FakeCloud()
    value, stop, out = outcome(setup._check_api_enabled, args, 'iamcredentials.googleapis.com', cloud=cloud)
    check('an API that is not enabled stops the run, with the command that enables it',
          stop is not None and 'gcloud services enable iamcredentials.googleapis.com' in stop and cloud.mutations == [], stop)
    cloud.enabled.add('iamcredentials.googleapis.com')
    value, stop, out = outcome(setup._check_api_enabled, args, 'iamcredentials.googleapis.com', cloud=cloud)
    check('an API that is enabled: OK', stop is None and out == 'OK iamcredentials.googleapis.com is enabled\n', f'{stop!r} {out!r}')


def with_client(function, client, *params):
    """outcome() of function(*params, client=client), with stand-ins for the modules it imports."""
    with modules(google_modules(client)):
        return outcome(function, *params, client=client)


def dataset_access_checks():
    owner = ('OWNER', 'userByEmail', 'owner@example.com')
    reader = ('READER', 'groupByEmail', 'bqaa-readers@example.com')
    person = ('READER', 'iamMember', 'user:ann@example.com')
    first, second = [(None, 'view', {'projectId': PROJECT, 'datasetId': VIEWS, 'tableId': name}) for name in setup.VIEW_NAMES]
    on_events, on_views = f'{PROJECT}.{DATASET}', f'{PROJECT}.{VIEWS}'
    by_user, by_model = [f'view:{VIEWS}.{name}' for name in setup.VIEW_NAMES]

    # The project's Viewers and Editors, and everyone, come off the events dataset; its Editors and everyone, off the
    # views dataset (its Viewers stay: they read aggregates).
    events = (owner, *DEFAULT_ENTRIES, ('READER', 'specialGroup', 'allAuthenticatedUsers'),
              ('READER', 'iamMember', 'allAuthenticatedUsers'), reader, person, first)
    reporting = (*DEFAULT_ENTRIES, ('READER', 'iamMember', 'allUsers'))
    client = fake_client(events, reporting)
    value, stop, out = with_client(setup._restrict_dataset_access, client, PROJECT, DATASET, VIEWS)
    check('the events dataset loses the project Viewers and Editors and everyone; the owners, the named readers '
          'and the authorized view stay',
          held(client.datasets[DATASET]) == ['OWNER userByEmail:owner@example.com', 'OWNER specialGroup:projectOwners',
                                            'READER groupByEmail:bqaa-readers@example.com',
                                            'READER iamMember:user:ann@example.com', by_user],
          held(client.datasets[DATASET]))
    check('the views dataset loses its Editors and everyone, and keeps its Viewers',
          held(client.datasets[VIEWS]) == ['OWNER specialGroup:projectOwners', 'READER specialGroup:projectReaders'],
          held(client.datasets[VIEWS]))
    check('it names what it took off, a line for each dataset',
          out == f'REMOVED from {on_events}: WRITER projectWriters, READER projectReaders, READER allAuthenticatedUsers, '
                 f'READER allAuthenticatedUsers\nREMOVED from {on_views}: WRITER projectWriters, READER allUsers\n', out)
    check('each dataset is updated once, its access entries alone',
          [(dataset, fields) for dataset, fields, _ in client.updates]
          == [(client.datasets[DATASET], ['access_entries']), (client.datasets[VIEWS], ['access_entries'])])
    value, stop, out = with_client(setup._restrict_dataset_access, client, PROJECT, DATASET, VIEWS)
    check('run again, it finds nothing to take off, and updates nothing',
          out == f'OK {on_events} gives project Viewers and Editors nothing\n'
                 f'OK {on_views} gives project Viewers and Editors nothing\n' and len(client.updates) == 2, out)
    client = fake_client()
    del client.datasets[VIEWS]
    value, stop, out = with_client(setup._restrict_dataset_access, client, PROJECT, DATASET, VIEWS)
    check('a dataset that is not there is skipped, and the other is still done',
          out == f'REMOVED from {on_events}: WRITER projectWriters, READER projectReaders\n'
                 f'SKIPPED: no dataset {on_views}\n' and len(client.updates) == 1, out)

    # The mod's two views are authorized on the events dataset, and no other.
    client = fake_client(events=(owner,))
    client.tables[f'{PROJECT}.{VIEWS}.raw_dump'] = 'VIEW'
    value, stop, out = with_client(setup._authorize_views, client, PROJECT, DATASET, VIEWS)
    check('the two views are authorized on the events dataset, and the owner stays; another view is not',
          held(client.datasets[DATASET]) == ['OWNER userByEmail:owner@example.com', by_user, by_model],
          held(client.datasets[DATASET]))
    check('it says which, and updates the access entries once',
          out == f'AUTHORIZED on {on_events}: {", ".join(setup.VIEW_NAMES)}\n'
          and [fields for _, fields, _ in client.updates] == [['access_entries']], out)
    value, stop, out = with_client(setup._authorize_views, client, PROJECT, DATASET, VIEWS)
    check('run again, it finds them authorized, and updates nothing',
          out == f'OK views already authorized on {on_events}\n' and len(client.updates) == 1, out)
    client = fake_client(events=(owner, first))
    value, stop, out = with_client(setup._authorize_views, client, PROJECT, DATASET, VIEWS)
    check('with one authorized already, it adds the other',
          out == f'AUTHORIZED on {on_events}: {setup.VIEW_NAMES[1]}\n'
          and held(client.datasets[DATASET]) == ['OWNER userByEmail:owner@example.com', by_user, by_model],
          f'{out!r} {held(client.datasets[DATASET])}')
    skipped = f"SKIPPED: no view {{}} in {on_views} (the mod's setup.sh makes them)\n"
    client = fake_client(events=(owner,), tables={f'{PROJECT}.{VIEWS}.{setup.VIEW_NAMES[0]}': 'VIEW',
                                                  f'{PROJECT}.{VIEWS}.{setup.VIEW_NAMES[1]}': 'TABLE'})
    value, stop, out = with_client(setup._authorize_views, client, PROJECT, DATASET, VIEWS)
    check('a table with a view\'s name is no view: it is skipped, and not authorized',
          out == skipped.format(setup.VIEW_NAMES[1]) + f'AUTHORIZED on {on_events}: {setup.VIEW_NAMES[0]}\n'
          and held(client.datasets[DATASET]) == ['OWNER userByEmail:owner@example.com', by_user], f'{out!r}')
    client = fake_client(events=(owner,), tables={})
    value, stop, out = with_client(setup._authorize_views, client, PROJECT, DATASET, VIEWS)
    check('no view at all: they are skipped, and the events dataset is not touched',
          out == skipped.format(', '.join(setup.VIEW_NAMES)) and client.updates == [], out)


# The preflight of a project where gcloud, Application Default Credentials and a gcloud account are all in order.
READY = setup._PreflightResult(adc_ok=True, adc_message='ok', cli_auth_ok=True, cli_auth_account='admin@example.com',
                               cli_auth_message='ok', gcloud_project=PROJECT, gcloud_available=True)


def execute(cloud, *argv, client=None, libraries=None, preflight=READY):
    """(status, stdout, stderr) of setup.main(argv + --execute) against this stand-in project and BigQuery client.

    A command that fails raises subprocess.CalledProcessError, as it does for a person.
    """
    client = fake_client() if client is None else client
    with contextlib.ExitStack() as stack:
        stack.enter_context(environment())
        stack.enter_context(patched(subprocess, run=cloud))
        stack.enter_context(patched(time, sleep=lambda seconds: None))
        stack.enter_context(patched(setup, _run_preflight=lambda: preflight))
        stack.enter_context(modules(google_modules(client) if libraries is None else libraries))
        return call(setup.main, *argv, '--execute')


def kinds(commands):
    return [kind(command) for command in commands]


def calls_before(cloud, wanted):
    """The commands run before the first one of this kind (all of them when there is none)."""
    for index, command in enumerate(cloud.calls):
        if kind(command) == wanted:
            return cloud.calls[:index]
    return list(cloud.calls)


def failure_of(cloud, *argv, **named):
    """The CalledProcessError that an --execute run ends with, or None."""
    try:
        execute(cloud, *argv, **named)
    except subprocess.CalledProcessError as error:
        return error
    return None


def execute_stop_checks():
    # A run that cannot go on stops, and says nothing was changed, when what it acts on is not there.
    cloud, client = FakeCloud(), fake_client()
    cloud.datasets, cloud.tables = {}, set()
    status, out, err = execute(cloud, *DOCUMENTED, client=client)
    check('--execute, no events dataset: it stops at the first step, with nothing changed',
          status == 1 and f'The events dataset {PROJECT}:{DATASET} is not there' in err and 'Nothing was changed' in err
          and cloud.mutations == [] and client.updates == [] and out.count('==> ') == 1, f'{status} {err!r}')
    check('the one command it ran was bq show of the dataset',
          cloud.calls == [['bq', '--format=json', 'show', f'{PROJECT}:{DATASET}']], cloud.calls)
    cloud = FakeCloud()
    cloud.tables = set()
    status, out, err = execute(cloud, *DOCUMENTED)
    check('--execute, no events table: it stops at the first step too',
          status == 1 and f'The events table {PROJECT}:{DATASET}.{TABLE} is not there' in err and cloud.mutations == [],
          f'{status} {err!r}')

    # It stops before it calls anything when the credentials or the client it needs are missing.
    cloud = FakeCloud()
    status, out, err = execute(cloud, *DOCUMENTED, libraries=NO_CLIENT)
    check('--execute, no BigQuery client to import: it stops before it calls anything, and says nothing was changed',
          status == 1 and 'cannot import' in err and 'Nothing was changed' in err and cloud.calls == [], f'{status} {err!r}')
    for what, preflight, fragment in (
        ('no gcloud', dataclasses.replace(READY, gcloud_available=False), 'gcloud is required for --execute'),
        ('no Application Default Credentials', dataclasses.replace(READY, adc_ok=False),
         'Application Default Credentials are not configured'),
        ('no gcloud account', dataclasses.replace(READY, cli_auth_ok=False), 'gcloud CLI auth is not configured'),
    ):
        cloud = FakeCloud()
        status, out, err = execute(cloud, *DOCUMENTED, preflight=preflight)
        check(f'--execute, {what}: it stops before it calls anything',
              status == 1 and fragment in err and cloud.calls == [], f'{status} {err!r}')

    # A writer that already holds more is refused before any grant (the APIs are the one thing enabled before the check).
    cloud, client = FakeCloud(), fake_client()
    cloud.service_account = True
    cloud.project_bindings = [{'role': 'roles/bigquery.dataEditor', 'members': [WRITER]}]
    status, out, err = execute(cloud, *DOCUMENTED, client=client)
    check('--execute, a writer that holds dataEditor already: it stops before any role, grant or dataset change',
          status == 1 and f'{WRITER} already holds more than the row-writer role' in err
          and kinds(cloud.mutations) == ['gcloud services enable'] and client.updates == [], f'{status} {err!r}')

    # A command that fails stops the run there.
    cloud, client = FakeCloud(), fake_client()
    cloud.failing = {'gcloud services enable'}
    error = failure_of(cloud, *DOCUMENTED, client=client)
    check('--execute, a command that fails: the run stops there, and nothing after it is done',
          error is not None and error.returncode == 1 and kinds(cloud.mutations) == ['gcloud services enable']
          and client.updates == [], f'{error!r} {kinds(cloud.mutations)}')
    cloud = FakeCloud()
    cloud.failing = {'gcloud iam service-accounts create'}
    error = failure_of(cloud, *DOCUMENTED)
    check('--execute, an account that cannot be made: it is tried three times, then the run stops',
          error is not None and kinds(cloud.mutations) == ['gcloud services enable'] + ['gcloud iam service-accounts create'] * 3,
          f'{error!r} {kinds(cloud.mutations)}')
    binding = 'gcloud iam service-accounts add-iam-policy-binding'
    cloud, client = FakeCloud(), fake_client()
    cloud.failing = {binding}
    error = failure_of(cloud, *DOCUMENTED, client=client)
    check('--execute, a token-creator binding that IAM does not take: tried seven times, then the run stops, and no reader is granted',
          error is not None and kinds(cloud.mutations).count(binding) == 7 and kinds(cloud.mutations)[-1] == binding
          and 'bq GRANT' not in kinds(cloud.mutations) and len(client.updates) == 2, f'{error!r} {kinds(cloud.mutations)}')


def execute_apply_checks():
    cloud, client = FakeCloud(), fake_client()
    status, out, err = execute(cloud, *DOCUMENTED, client=client)
    labels = [label for label, _ in steps_of(out)]
    check('the documented command, applied: exit 0, a last line that says so, and nothing on stderr',
          status == 0 and out.rstrip().endswith('Access setup completed.') and err == '', f'{status} {err!r} {out[-200:]!r}')
    check('every step of the printed plan runs, in the order printed',
          len(labels) == 12 and [line[4:] for line in out.splitlines() if line.startswith('==> ')] == labels, labels)
    check('it changes seven things: the APIs, the account, the role, the grant on the table, the token creator, '
          "the readers' views dataset, the readers' job rights",
          kinds(cloud.mutations) == ['gcloud services enable', 'gcloud iam service-accounts create', 'gcloud iam roles create',
                                     'bq add-iam-policy-binding', 'gcloud iam service-accounts add-iam-policy-binding',
                                     'bq GRANT', 'gcloud projects add-iam-policy-binding'], kinds(cloud.mutations))
    before = kinds(calls_before(cloud, 'gcloud iam roles create'))
    check("both checks read the project's policy, and the table's, before the role is made",
          before.count('gcloud projects get-iam-policy') == 2 and before.count('bq get-iam-policy') == 2, before)
    check('the writer gets the row-writer role on the table, and nothing else; no command grants dataEditor',
          [' '.join(c) for c in cloud.mutations if WRITER in c]
          == [f'bq add-iam-policy-binding --member {WRITER} --role {ROLE} {PROJECT}:{DATASET}.{TABLE}']
          and not any('dataEditor' in ' '.join(c) for c in cloud.mutations), [' '.join(c) for c in cloud.mutations])
    check('the role it made holds the two permissions and no other',
          cloud.roles[ROLE]['includedPermissions'] == ['bigquery.tables.get', 'bigquery.tables.updateData'], cloud.roles.get(ROLE))
    jobs = [c for c in cloud.mutations if 'roles/bigquery.jobUser' in c]
    check('one command grants jobUser, to the reader', len(jobs) == 1 and READERS in jobs[0] and WRITER not in jobs[0], jobs)
    check('the events dataset ends with its owners and the two views, the views dataset with its owners and Viewers',
          held(client.datasets[DATASET]) == ['OWNER specialGroup:projectOwners', f'view:{VIEWS}.{setup.VIEW_NAMES[0]}',
                                            f'view:{VIEWS}.{setup.VIEW_NAMES[1]}']
          and held(client.datasets[VIEWS]) == ['OWNER specialGroup:projectOwners', 'READER specialGroup:projectReaders'],
          f'{held(client.datasets[DATASET])} {held(client.datasets[VIEWS])}')
    check('through three updates of the access entries: the events dataset, the views dataset, the events dataset again',
          [dataset for dataset, _, _ in client.updates]
          == [client.datasets[DATASET], client.datasets[VIEWS], client.datasets[DATASET]]
          and all(fields == ['access_entries'] for _, fields, _ in client.updates))

    # The data-editor plan makes no BigQuery client call, so it asks for no client and no Application Default Credentials.
    cloud = FakeCloud()
    status, out, err = execute(cloud, *BASE, '--service-account', 'bqaa-writer', libraries=NO_CLIENT,
                               preflight=dataclasses.replace(READY, adc_ok=False))
    check('a data-editor plan needs no BigQuery client and no Application Default Credentials',
          status == 0 and kinds(cloud.mutations) == ['gcloud services enable', 'gcloud iam service-accounts create',
                                                     'bq GRANT', 'gcloud projects add-iam-policy-binding'],
          f'{status} {err!r} {kinds(cloud.mutations)}')

    # Run again on what the first run left.
    def after_first_run():
        cloud = FakeCloud()
        cloud.service_account = True
        cloud.roles[ROLE] = ROW_WRITER
        cloud.table_bindings = [{'role': ROLE, 'members': [WRITER]}]
        cloud.project_bindings = [{'role': 'roles/bigquery.jobUser', 'members': [READERS]}]
        cloud.datasets = {DATASET: [dict(DEFAULT_ACCESS[0])],
                          VIEWS: [dict(DEFAULT_ACCESS[0]), dict(DEFAULT_ACCESS[2]),
                                  {'role': 'READER', 'groupByEmail': 'bqaa-readers@example.com'}]}
        first, second = [(None, 'view', {'projectId': PROJECT, 'datasetId': VIEWS, 'tableId': name}) for name in setup.VIEW_NAMES]
        return cloud, fake_client(events=(DEFAULT_ENTRIES[0], first, second), views=(DEFAULT_ENTRIES[0], DEFAULT_ENTRIES[2]))

    cloud, client = after_first_run()
    status, out, err = execute(cloud, *DOCUMENTED, client=client)
    check('run again, the readers group holds a grant on the views dataset that may reach the writer: it stops, not ruled out',
          status == 1 and 'could not rule out' in err and 'READER on the dataset' in err and 'bqaa-readers@example.com' in err
          and 'Nothing' not in err, f'{status} {err!r}')
    cloud, client = after_first_run()
    cloud.membership[('bqaa-readers@example.com', WRITER_EMAIL)] = False
    status, out, err = execute(cloud, *DOCUMENTED, client=client)
    check('run again, with Cloud Identity saying the writer is not in the readers group: exit 0',
          status == 0 and out.rstrip().endswith('Access setup completed.'), f'{status} {err!r}')
    check('it makes no account and no role, and grants the same things again (idempotent)',
          kinds(cloud.mutations) == ['gcloud services enable', 'bq add-iam-policy-binding',
                                     'gcloud iam service-accounts add-iam-policy-binding', 'bq GRANT',
                                     'gcloud projects add-iam-policy-binding'], kinds(cloud.mutations))
    check('it finds the dataset access done, the views authorized, and updates nothing',
          client.updates == [] and out.count('gives project Viewers and Editors nothing') == 2
          and 'OK views already authorized' in out and 'OK service account exists' in out and 'OK role exists' in out, out)
    cloud, client = after_first_run()
    status, out, err = execute(cloud, *DOCUMENTED, '--accept-unverified', client=client)
    check('run again, with --accept-unverified: exit 0, and it says what it accepted',
          status == 0 and 'ACCEPTED (--accept-unverified), not ruled out' in out, f'{status} {err!r}')


# ---------------------------------------------------------------------------
# The repository: what names the scripts, and what shows how to run them
# ---------------------------------------------------------------------------

THIS = pathlib.Path(__file__).resolve()
# Where a script may be named or a command shown: the docs, the SQL, the shell scripts and the tests of the mod.
PATTERNS = ('*.md', '*.sh', 'docs/*.md', 'sql/*.sql', 'sql/views/*.sql', 'tests/*.py', 'tests/*.sh')
SCRIPT_NAME = re.compile(r'\bscripts/([A-Za-z0-9_]+\.py)\b')
COMMAND = re.compile(r'(?:\S*/)?python3?\s+(?:\S*/)?scripts/(run_bq|setup_gcp_prereqs)\.py(?:\s+(.*))?')
PLACEHOLDERS = {'YOUR_PROJECT': PROJECT}  # what the quickstart asks a person to replace


def plugin_files():
    """The files that may name a script or show a command (this one is left out). No glob goes into a folder
    a person may add, like a .venv."""
    return sorted({path for pattern in PATTERNS for path in PLUGIN.glob(pattern) if path.resolve() != THIS})


def documented_commands():
    """[(file, script, arguments)] for each `python3 scripts/<script>.py ...` the docs and the SQL headers show.

    A command may run over lines that end in a backslash; in a .sql file every line of it begins with `-- `.
    """
    found = []
    for path in plugin_files():
        lines = path.read_text(encoding='utf-8').splitlines()
        if path.suffix == '.sql':
            lines = [re.sub(r'^--\s?', '', line) for line in lines if line.startswith('--')]
        logical, pending = [], ''
        for line in lines:
            line = line.strip()
            if line.endswith('\\'):
                pending += line[:-1].rstrip() + ' '
            else:
                logical.append(pending + line)
                pending = ''
        for line in logical:
            match = COMMAND.fullmatch(line)
            if match:
                words = [PLACEHOLDERS.get(word, word) for word in shlex.split(match.group(2) or '')]
                found.append((path.relative_to(PLUGIN).as_posix(), match.group(1), words))
    return found


def repo_checks():
    named = sorted({(path.relative_to(PLUGIN).as_posix(), name) for path in plugin_files()
                    for name in SCRIPT_NAME.findall(path.read_text(encoding='utf-8'))})
    missing = [f'{where}: {name}' for where, name in named if not (SCRIPTS / name).is_file()]
    check('the docs, the SQL headers, setup.sh and the tests name only scripts that are there',
          len(named) >= 8 and not missing, f'{len(named)} names; missing {missing}')
    check('setup.sh points at the bootstrap in the scripts folder',
          'scripts/setup_gcp_prereqs.py' in (PLUGIN / 'setup.sh').read_text(encoding='utf-8'))
    shipped = re.search(r'^FILES=\((.*?)\)', (PLUGIN / 'build_dist.sh').read_text(encoding='utf-8'), re.M | re.S)
    check('build_dist.sh ships the scripts folder', shipped is not None and 'scripts' in shipped.group(1).split(),
          shipped and shipped.group(1))

    scripts = sorted(path.name for path in SCRIPTS.iterdir() if path.is_file() and path.suffix == '.py')
    check('the scripts folder holds the runner, the bootstrap, and the dead-letter cleaner', scripts == ['clean_dead_letter.py', 'run_bq.py', 'setup_gcp_prereqs.py'], scripts)
    for name in scripts:
        text = (SCRIPTS / name).read_text(encoding='utf-8')
        check(f'{name}: a python3 shebang, then the Apache license and the copyright of Google LLC, before the docstring',
              text.startswith('#!/usr/bin/env python3\n# Copyright 2026 Google LLC\n')
              and 'Apache License, Version 2.0' in text.split('"""', 1)[0], text[:80])
        if os.name == 'posix':
            check(f'{name} is executable', bool((SCRIPTS / name).stat().st_mode & stat.S_IXUSR))
        alias = f'bqaa_without_client_{name[:-3]}'
        try:
            with modules(NO_CLIENT):
                load(name[:-3], alias=alias)
        except Exception as error:  # an import of the client at the top of the file
            check(f'{name} imports with no BigQuery client installed', False, repr(error))
        else:
            check(f'{name} imports with no BigQuery client installed', True)
        finally:
            sys.modules.pop(alias, None)

    made = re.search(r'^for view in (.*); do$', (PLUGIN / 'setup.sh').read_text(encoding='utf-8'), re.M)
    files = sorted(path.stem for path in (PLUGIN / 'sql' / 'views').glob('*.sql'))
    check('the views setup.sh makes, the files of sql/views and the views the bootstrap authorizes are the same',
          made is not None and sorted(made.group(1).split()) == files == sorted(setup.VIEW_NAMES) and len(files) == 2,
          f'{made and made.group(1)} {files} {setup.VIEW_NAMES}')

    # Every command the docs and the SQL headers show is one the scripts take.
    commands = documented_commands()
    check('the docs and the SQL headers show at least five commands, for both scripts',
          len(commands) >= 5 and {script for _, script, _ in commands} == {'run_bq', 'setup_gcp_prereqs'},
          [(where, script) for where, script, _ in commands])
    for where, script, argv in commands:
        if script == 'setup_gcp_prereqs':
            status, out, err = attempt(*argv)
            check(f'{where}: the bootstrap command it shows is a dry run, and exits 0', status == 0 and 'DRY RUN' in out,
                  f'{status} {err!r}')
        else:
            fake = FakeBigQuery()
            status, out, err = run_bq_with(fake, *argv, BQAA_PROJECT_ID=PROJECT, BQAA_DATASET=DATASET)
            with environment(BQAA_PROJECT_ID=PROJECT, BQAA_DATASET=DATASET):
                parsed_args = run_bq.parse_args(argv) if status == 0 else None
            expected = (run_bq.inject_placeholders(parsed_args.query, parsed_args.project, parsed_args.dataset, parsed_args.table)
                        if parsed_args is not None else None)
            check(f'{where}: the runner command it shows runs, with its last word or --query as the query',
                  status == 0 and fake.queries and fake.queries[-1][0] in (argv[-1], expected), f'{status} {err!r}')


def setup_sh_checks():
    bash = shutil.which('bash')
    if bash is None:
        check('setup.sh prints a bootstrap command that the bootstrap takes (skipped: no bash)', True)
        return
    solo = subprocess.run([bash, str(PLUGIN / 'setup.sh'), '--dry-run'], capture_output=True, text=True,
                          env={**clean_environment(), 'BQAA_PROJECT_ID': PROJECT, 'BQAA_DATASET': DATASET},
                          cwd=tempfile.gettempdir())
    check('setup.sh --dry-run (solo default) points to setup.sh --team and docs/service-account-auth.md without the full bootstrap command',
          solo.returncode == 0 and 'setup.sh --team' in solo.stdout and 'docs/service-account-auth.md' in solo.stdout
          and 'scripts/setup_gcp_prereqs.py' not in solo.stdout,
          f'{solo.returncode} {solo.stdout!r} {solo.stderr!r}')
    for what, env, wanted in (
        ('with the variables the docs name', {'BQAA_PROJECT_ID': PROJECT, 'BQAA_DATASET': DATASET},
         (PROJECT, DATASET, TABLE, '', 'US')),
        ('with the other variables, and a table, a views dataset and a location of its own',
         {'GOOGLE_CLOUD_PROJECT': 'other-project', 'BQ_DATASET': 'events', 'BQ_TABLE': 'coding_events',
          'BQAA_LOCATION': 'EU', 'BQAA_VIEWS_DATASET': 'reports'},
         ('other-project', 'events', 'coding_events', 'reports', 'EU')),
    ):
        result = subprocess.run([bash, str(PLUGIN / 'setup.sh'), '--dry-run', '--team'], capture_output=True, text=True,
                                env={**clean_environment(), **env}, cwd=tempfile.gettempdir())
        line = next((line for line in result.stdout.replace('\\\n', ' ').splitlines()
                     if 'scripts/setup_gcp_prereqs.py' in line), '')
        words = shlex.split(line)
        check(f'setup.sh --dry-run --team, {what}: it prints a command that runs the bootstrap of the scripts folder',
              result.returncode == 0 and len(words) > 2 and words[0] == 'python3'
              and pathlib.Path(words[1]).resolve() == (SCRIPTS / 'setup_gcp_prereqs.py').resolve()
              and 'google-cloud-bigquery' in result.stdout,
              f'{result.returncode} {line!r} {result.stderr!r}')
        args = parsed(*words[2:])
        check('  ... naming the project, dataset, table, views dataset and location it was given',
              args is not None and (args.project, args.dataset, args.table, args.views_dataset, args.location) == wanted,
              args and (args.project, args.dataset, args.table, args.views_dataset, args.location))
        status, out, err = attempt(*words[2:])
        check('  ... which the bootstrap takes: a dry run, exit 0', status == 0 and 'DRY RUN' in out, f'{status} {err!r}')


# ---------------------------------------------------------------------------
# clean_dead_letter.py
# ---------------------------------------------------------------------------

def clean_dead_letter_checks():
    """The offline helper removes a (source, outcome-*) group only when the
    source is empty and no non-empty leftover file names it as parent."""
    clean = load('clean_dead_letter')

    def make_dir(tmp: pathlib.Path) -> pathlib.Path:
        d = tmp / 'dead-letter' / 'proj.ds.tbl'
        d.mkdir(parents=True)
        return d

    def seal(text: str, parent: str | None = None) -> str:
        """A dead-letter file: its content lines, then a completion record."""
        record = {"v": 1, "complete": True, "lines": text.count("\n"), "fnv1a": "deadbeef"}
        if parent is not None:
            record["parent"] = parent
        return text + json.dumps(record) + "\n"

    # 1. Empty source + outcome variants (outcome-, outcome-b-, outcome-done-<writer>-): all removed under --execute.
    with tempfile.TemporaryDirectory() as tmp:
        d = make_dir(pathlib.Path(tmp))
        source = d / 'spill-001.jsonl'
        source.write_bytes(b'')
        outcome = d / 'outcome-spill-001.jsonl'
        outcome.write_text(seal('{"journal":true}\n'), encoding='utf-8')
        outcome_done = d / 'outcome-done-w1a2b3-spill-001.jsonl'
        outcome_done.write_text(seal('{"journal":true,"retired":true}\n'), encoding='utf-8')
        orphan_outcome = d / 'outcome-done-w1a2b3-missing.jsonl'
        orphan_outcome.write_text(seal('{"journal":true}\n'), encoding='utf-8')
        to_delete = clean.plan_deletions(pathlib.Path(tmp) / 'dead-letter')
        check('empty source and its outcome/outcome-done files in partition subdir are deletable (orphan outcome kept)',
              set(p.name for p in to_delete) == {'spill-001.jsonl', 'outcome-spill-001.jsonl', 'outcome-done-w1a2b3-spill-001.jsonl'},
              str(sorted(p.name for p in to_delete)))
        status, out, err = call(clean.main, '--dir', str(pathlib.Path(tmp) / 'dead-letter'), '--execute')
        check('under --execute: empty source and its outcome files are deleted while orphan outcome stays, exit 0',
              status == 0 and not source.exists() and not outcome.exists() and not outcome_done.exists() and orphan_outcome.exists(),
              f'{status} {out!r}')

    # 2. Non-empty source + outcome: both kept.
    with tempfile.TemporaryDirectory() as tmp:
        d = make_dir(pathlib.Path(tmp))
        source = d / 'retry-002.jsonl'
        source.write_text(seal('{"row":1}\n'), encoding='utf-8')
        outcome = d / 'outcome-retry-002.jsonl'
        outcome.write_text(seal('{"journal":true}\n'), encoding='utf-8')
        to_delete = clean.plan_deletions(d)
        check('non-empty source: nothing is deletable', to_delete == [], str(sorted(p.name for p in to_delete)))
        call(clean.main, '--dir', str(d), '--execute')
        check('non-empty source and its outcome are kept', source.exists() and outcome.exists(),
              f'{source.exists()} {outcome.exists()}')

    # 3. Empty source referenced as parent by a non-empty leftover: kept
    #    until the leftover is empty too.
    with tempfile.TemporaryDirectory() as tmp:
        d = make_dir(pathlib.Path(tmp))
        source = d / 'rejected-003.jsonl'
        source.write_bytes(b'')
        leftover = d / 'rejected-003-leftover.jsonl'
        leftover.write_text(seal('{"row":1}\n', parent='rejected-003.jsonl'), encoding='utf-8')
        to_delete = clean.plan_deletions(d)
        check('empty source with a non-empty leftover: nothing is deletable', to_delete == [], str(sorted(p.name for p in to_delete)))
        call(clean.main, '--dir', str(d), '--execute')
        check('empty source and its non-empty leftover are kept', source.exists() and leftover.exists(),
              f'{source.exists()} {leftover.exists()}')
        # Now empty the leftover: the source becomes deletable.
        leftover.write_bytes(b'')
        to_delete = clean.plan_deletions(d)
        check('once the leftover is empty too: the source is deletable', any(p.name == 'rejected-003.jsonl' for p in to_delete),
              str(sorted(p.name for p in to_delete)))
        call(clean.main, '--dir', str(d), '--execute')
        check('empty source is deleted', not source.exists(), f'{source.exists()}')

    # 4. Dry-run default: nothing is deleted.
    with tempfile.TemporaryDirectory() as tmp:
        d = make_dir(pathlib.Path(tmp))
        source = d / 'spill-004.jsonl'
        source.write_bytes(b'')
        outcome = d / 'outcome-spill-004.jsonl'
        outcome.write_text(seal('{"journal":true}\n'), encoding='utf-8')
        status, out, err = call(clean.main, '--dir', str(d))
        check('dry-run: exit 0 and says "would delete"', status == 0 and 'would delete' in out, f'{status} {out!r} {err!r}')
        check('dry-run: nothing is actually deleted', source.exists() and outcome.exists(),
              f'{source.exists()} {outcome.exists()}')


GROUPS = (run_bq_checks, setup_dry_run_checks, setup_refusal_checks, setup_plan_checks, reach_checks,
          writer_grants_checks, row_writer_role_checks, resources_checks, readers_checks, api_checks,
          dataset_access_checks, execute_stop_checks, execute_apply_checks, repo_checks, setup_sh_checks,
          clean_dead_letter_checks)


def main():
    for group in GROUPS:
        try:
            group()
        except Exception as error:  # a group that did not run to the end is a failure, never a pass
            traceback.print_exc()
            check(f'{group.__name__} ran to the end', False, f'{type(error).__name__}: {error}')
    print(f'{passed} passed, {failed} failed')
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
