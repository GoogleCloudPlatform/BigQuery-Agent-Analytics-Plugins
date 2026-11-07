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

"""Offline DuckDB test of all 15 queries in sql/sample_queries.sql and sql/value_queries.sql.

Runs:
  1. All 7 value queries (V1..V7, plus window/lookback/empty-day cases) and all
     8 sample queries (1..8), plus docs/managed-settings.md's rollout query,
     against tests/queries.live.py's multi-producer fixture.
  2. All 15 queries over examples/sample-rows.jsonl (plus a replayed duplicate
     row), verifying Query 5 turn_tokens and responses_without_usage match V1.
  3. All 15 queries over malformed non-integer, float ('12.5'), and scientific
     ('1e3') JSON strings, verifying BigQuery-faithful SAFE_CAST NULL semantics
     and paired cache_read_share ratios.
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import pathlib
import re
import sys
from typing import Callable

try:
    import duckdb
except ImportError:
    print('skip - DuckDB for Python is not installed (python3 -m pip install duckdb)')
    sys.exit(77)

HERE = pathlib.Path(__file__).resolve().parent
PLUGIN = HERE.parent

spec = importlib.util.spec_from_file_location('queries_live', HERE / 'queries.live.py')
assert spec is not None and spec.loader is not None
live = importlib.util.module_from_spec(spec)
spec.loader.exec_module(live)

passed = 0
failed = 0


def check(what: str, ok: bool, detail: str = '') -> None:
    global passed, failed
    if ok:
        passed += 1
        print(f'ok - {what}')
    else:
        failed += 1
        print(f'not ok - {what}{": " + detail if detail else ""}')


def _balanced_end(sql: str, open_after: int) -> int:
    depth, in_str, j = 1, False, open_after
    while j < len(sql) and depth > 0:
        ch = sql[j]
        if ch == "'" and (j == 0 or sql[j - 1] != '\\'):
            in_str = not in_str
        elif not in_str and ch in '()':
            depth += 1 if ch == '(' else -1
        j += 1
    return j


def _sub_call(sql: str, head_re: str, fn: Callable[[str], str | None]) -> str:
    out, i = [], 0
    while True:
        m = re.search(head_re, sql[i:], flags=re.I)
        if not m:
            out.append(sql[i:])
            break
        out.append(sql[i : i + m.start()])
        j = _balanced_end(sql, i + m.end())
        inner = _sub_call(sql[i + m.end() : j - 1], head_re, fn)
        repl = fn(inner)
        out.append(repl if repl is not None else f'{sql[i + m.start() : i + m.end()]}{inner})')
        i = j
    return ''.join(out)


def has_bare_int64_cast(sql: str) -> bool:
    """Returns True if sql contains a non-SAFE_CAST `CAST(... AS INT64)` expression."""
    cleaned = re.sub(r"'([^'\\]|\\.)*'|--[^\n]*", '', sql)
    for m in re.finditer(r'(?<!SAFE_)(?<!TRY_)\bCAST\s*\(', cleaned, flags=re.I):
        j = _balanced_end(cleaned, m.end())
        if re.search(r'\s+AS\s+INT64\s*$', cleaned[m.end() : j - 1], flags=re.I | re.S):
            return True
    return False


def as_duckdb(sql: str) -> str:
    """Translates BigQuery SQL query templates into DuckDB's dialect.

    DuckDB's raw TRY_CAST('12.5' AS BIGINT) -> 13 and TRY_CAST('1e3' AS BIGINT)
    -> 1000 diverge from BigQuery SAFE_CAST(STRING AS INT64) -> NULL, so
    SAFE_CAST(... AS INT64) is guarded with an explicit integer-literal regex.
    DuckDB's COUNTIF also returns NULL on all-NULL groups where BigQuery returns 0.
    """
    sql = sql.replace('`{PROJECT}.{DATASET}.{TABLE}`', 'agent_events')
    sql = sql.replace('`{PROJECT}.{DATASET}.team_roster`', 'team_roster')
    sql = re.sub(r'@(start|end|trace_id)\b', r'$\1', sql)
    sql = _sub_call(sql, r'\bCOUNTIF\s*\(', lambda inner: f'COUNT(CASE WHEN ({inner}) THEN 1 END)')

    def _safe_int64(inner: str) -> str | None:
        m = re.match(r'^(.*?)\s+AS\s+INT64\s*$', inner, flags=re.I | re.S)
        if not m:
            return None
        expr = m.group(1)
        return f"(CASE WHEN regexp_matches(trim(CAST({expr} AS VARCHAR)), '^[+-]?[0-9]+$') THEN TRY_CAST({expr} AS BIGINT) ELSE NULL END)"

    def _bare_int64(inner: str) -> str | None:
        m = re.match(r'^(.*?)\s+AS\s+INT64\s*$', inner, flags=re.I | re.S)
        return f'CAST({m.group(1)} AS BIGINT)' if m else None

    sql = _sub_call(sql, r'\bSAFE_CAST\s*\(', _safe_int64)
    sql = _sub_call(sql, r'(?<!SAFE_)(?<!TRY_)\bCAST\s*\(', _bare_int64)
    sql = re.sub(r'\bLOGICAL_OR\(', 'BOOL_OR(', sql)
    sql = re.sub(
        r'\bAPPROX_QUANTILES\((.+?),\s*100\)\[OFFSET\((\d+)\)\]',
        lambda m: f'QUANTILE_DISC({m.group(1)}, {int(m.group(2)) / 100.0})',
        sql,
    )
    sql = re.sub(r'\bTIMESTAMP_DIFF\(([^,]+),\s*([^,]+),\s*SECOND\)', r'CAST(TRUNC(EPOCH((\1) - (\2))) AS BIGINT)', sql)
    sql = re.sub(r'\bTIMESTAMP_SUB\(([^,]+),\s*INTERVAL\s+1\s+DAY\)', r'((\1) - INTERVAL 1 DAY)', sql)
    sql = re.sub(r'\bTIMESTAMP_SUB\(([^,]+),\s*INTERVAL\s+([^\s)]+)\s+MILLISECOND\)', r'((\1) - to_milliseconds(\2))', sql)
    sql = re.sub(
        r'SELECT day FROM UNNEST\(GENERATE_DATE_ARRAY\(DATE\(\$start\),\s*DATE\(\$end\)\)\) AS day',
        'SELECT CAST(day AS DATE) AS day FROM UNNEST(GENERATE_SERIES(CAST($start AS DATE), CAST($end AS DATE), INTERVAL 1 DAY)) AS _d(day)',
        sql,
    )
    sql = re.sub(r'\bDATE\(([^()]+)\)', r'CAST(\1 AS DATE)', sql)

    def _unnest_structs(m: re.Match[str]) -> str:
        selects = ['SELECT ' + sm.group(1) for sm in re.finditer(r'STRUCT\((.*?)\)(?:,\s*|\s*$)', m.group(1), flags=re.S)]
        return '(' + ' UNION ALL '.join(selects) + ')'

    return re.sub(r'UNNEST\(\[\s*(STRUCT\(.*?\))\s*\]\)', _unnest_structs, sql, flags=re.S)


def parse_ts(text: str, is_end: bool) -> datetime.datetime:
    if 'T' in text:
        return datetime.datetime.fromisoformat(text.removesuffix('Z'))
    return datetime.datetime.fromisoformat(text + ('T23:59:59.999999' if is_end else 'T00:00:00'))


def make_db(rows: list[dict], roster: list[str]) -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    db.execute('CREATE MACRO JSON_VALUE(doc, path) AS json_extract_string(doc, path)')
    db.execute('CREATE MACRO SAFE_DIVIDE(a, b) AS CASE WHEN b = 0 THEN NULL ELSE CAST(a AS DOUBLE) / b END')
    db.execute('CREATE MACRO GENERATE_UUID() AS CAST(uuid() AS VARCHAR)')
    db.execute(
        'CREATE TABLE agent_events ('
        'timestamp TIMESTAMP, event_id VARCHAR, event_type VARCHAR, agent VARCHAR, '
        'session_id VARCHAR, invocation_id VARCHAR, user_id VARCHAR, trace_id VARCHAR, '
        'span_id VARCHAR, parent_span_id VARCHAR, content VARCHAR, attributes VARCHAR, '
        'latency_ms VARCHAR, status VARCHAR, is_truncated BOOLEAN)'
    )
    payload = []
    for r in rows:
        content = r['content'] if isinstance(r['content'], str) else json.dumps(r['content'])
        attrs = r['attributes'] if isinstance(r['attributes'], str) else json.dumps(r['attributes'])
        lat_val = r.get('latency_ms')
        lat = lat_val if isinstance(lat_val, str) else json.dumps(lat_val if lat_val is not None else {})
        payload.append((
            datetime.datetime.fromisoformat(r['timestamp'].removesuffix('Z')),
            r.get('event_id'), r['event_type'], r.get('agent'), r.get('session_id'),
            r.get('invocation_id'), r.get('user_id'), r.get('trace_id'), r.get('span_id'),
            r.get('parent_span_id'), content, attrs, lat, r.get('status'), bool(r.get('is_truncated', False)),
        ))
    db.executemany('INSERT INTO agent_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', payload)
    db.execute('CREATE TABLE team_roster (user_id VARCHAR)')
    if roster:
        db.executemany('INSERT INTO team_roster VALUES (?)', [(u,) for u in roster])
    return db


def exec_query(db: duckdb.DuckDBPyConnection, sql: str, start: str, end: str, trace_id: str = 'trace-1') -> list[dict]:
    dsql = as_duckdb(sql)
    params: dict[str, object] = {'start': parse_ts(start, False), 'end': parse_ts(end, True)}
    if '$trace_id' in dsql:
        params['trace_id'] = trace_id
    cur = db.execute(dsql, params)
    cols = [c[0] for c in cur.description]
    return [{k: (v.isoformat() if isinstance(v, (datetime.date, datetime.datetime)) else v) for k, v in zip(cols, row)} for row in cur.fetchall()]


sample_text = (PLUGIN / 'sql' / 'sample_queries.sql').read_text()
value_text = (PLUGIN / 'sql' / 'value_queries.sql').read_text()
sample_qs = live.queries(PLUGIN / 'sql' / 'sample_queries.sql')
value_qs = live.queries(PLUGIN / 'sql' / 'value_queries.sql')

check('sample_queries.sql holds queries 1..8', list(sample_qs) == ['1', '2', '3', '4', '5', '6', '7', '8'])
check('value_queries.sql holds queries V1..V7', list(value_qs) == ['V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7'])
check(
    'has_bare_int64_cast catches spaced/nested bare CAST without cross-expression false positives',
    has_bare_int64_cast('SELECT CAST (x AS INT64)')
    and has_bare_int64_cast('SELECT CAST(CAST(x AS INT64) AS STRING)')
    and not has_bare_int64_cast("SELECT CAST(x AS STRING), SAFE_CAST (y AS INT64) -- CAST(z AS INT64)"),
)
check('sample_queries.sql uses SAFE_CAST (no bare CAST ... AS INT64)', not has_bare_int64_cast(sample_text))
check('value_queries.sql uses SAFE_CAST (no bare CAST ... AS INT64)', not has_bare_int64_cast(value_text))

# 1. Full multi-producer fixture from tests/queries.live.py.
db_live = make_db(live.FIXTURE, live.ROSTER)
runs: list[tuple[str, str, str, str, str]] = []
for filename, qmap in (('value_queries.sql', value_qs), ('sample_queries.sql', sample_qs)):
    for name, sql in qmap.items():
        if name == 'V4':
            sql = re.sub(r'-- rates: begin.*?-- rates: end', live.RATES, sql, flags=re.S)
        runs.append((f'{filename} {name}', name, sql, live.START, live.END))
        if name == 'V1':
            runs.append((f'{filename} V1 notifications', 'V1 notifications', sql, '2026-10-22', '2026-10-22'))
        if name == '5':
            runs.append((f'{filename} 5 notifications', '5 notifications', sql, '2026-10-22', '2026-10-22'))
        if name == 'V7':
            runs.append((f'{filename} V7 clipped', 'V7 clipped', sql, '2026-10-20T15:00:01Z', live.END))
            runs.append((f'{filename} V7 later finals only', 'V7 later finals only', sql, '2026-10-20T19:00:05Z', '2026-10-20T19:00:08Z'))
        if name == 'V2':
            runs.append(('managed-settings.md rollout query', 'rollout', live.guide_rollout_query(), live.START, live.END))
            for case, s, e in (
                ('V2 ends mid-turn', '2026-10-22T20:00:00Z', '2026-10-22T20:00:10Z'),
                ('V2 after the turn', '2026-10-22T20:00:00Z', '2026-10-22T20:00:30Z'),
                ('V2 starts mid-turn', '2026-10-22T20:00:15Z', '2026-10-22T20:00:30Z'),
                ('V2 past the lookback', '2026-10-24T11:00:00Z', '2026-10-24T11:00:10Z'),
                ('V2 whole long turn', '2026-10-24T09:00:00Z', '2026-10-24T11:00:10Z'),
            ):
                runs.append((f'{filename} {case}', case, sql, s, e))
        if name == 'V3':
            runs.append((f'{filename} V3 silent day', 'V3 silent day', sql, live.START, '2026-10-21'))
            runs.append((f'{filename} V3 empty window', 'V3 empty window', sql, '2026-09-01', '2026-09-02'))

q5_rows: dict[str, dict] = {}
for label, name, sql, start, end in runs:
    rows = exec_query(db_live, sql, start, end)
    if name == '5':
        q5_rows = live.by(rows, 'invocation_id')
    for what, ok in live.checks(name, rows):
        check(f'{label}: {what}', bool(ok))

check('sample_queries.sql 5: I1 has 195 turn_tokens and 0 responses_without_usage', (q5_rows.get('I1', {}).get('turn_tokens'), q5_rows.get('I1', {}).get('responses_without_usage')) == (195, 0), json.dumps(q5_rows.get('I1')))
check('sample_queries.sql 5: I2 sums main (240) + subagent (50) = 290 turn_tokens (0 missing)', (q5_rows.get('I2', {}).get('turn_tokens'), q5_rows.get('I2', {}).get('responses_without_usage')) == (290, 0), json.dumps(q5_rows.get('I2')))
check('sample_queries.sql 5: I3 has 30 turn_tokens and flags 1 responses_without_usage', (q5_rows.get('I3', {}).get('turn_tokens'), q5_rows.get('I3', {}).get('responses_without_usage')) == (30, 1), json.dumps(q5_rows.get('I3')))
check(
    'sample_queries.sql 5 projects trace_id per turn (preserving NULL when absent) and feeds directly into Query 6',
    (q5_rows.get('I1', {}).get('trace_id'), q5_rows.get('I2', {}).get('trace_id'), q5_rows.get('I3', {}).get('trace_id')) == ('trace-1', None, None)
    and len(exec_query(db_live, sample_qs['6'], live.START, live.END, trace_id=q5_rows['I1']['trace_id'])) == 14,
    json.dumps({k: v.get('trace_id') for k, v in q5_rows.items()}),
)

# 2. Canonical examples/sample-rows.jsonl + duplicate event_id replay.
sample_rows = [json.loads(line) for line in (PLUGIN / 'examples' / 'sample-rows.jsonl').read_text().splitlines() if line.strip()]
sample_rows.append(dict(sample_rows[4]))  # replay first LLM_RESPONSE
db_examples = make_db(sample_rows, ['dev'])
q1_ex = exec_query(db_examples, sample_qs['1'], '2026-10-14', '2026-10-14')
q5_ex = exec_query(db_examples, sample_qs['5'], '2026-10-14', '2026-10-14')
v1_ex = exec_query(db_examples, value_qs['V1'], '2026-10-14', '2026-10-14')
check('examples/sample-rows.jsonl: Query 1 deduplicates replayed event_id to 26 rows', len(q1_ex) == 26)
check(
    'examples/sample-rows.jsonl: Query 5 turn_tokens (90798), responses_without_usage (0), and trace_id match V1 and Query 6',
    len(q5_ex) == 1
    and (q5_ex[0]['model_requests'], q5_ex[0]['subagents'], q5_ex[0]['turn_tokens'], q5_ex[0].get('responses_without_usage'), q5_ex[0].get('trace_id'))
    == (5, 1, 90798, 0, 'a4db76890e3b711d381938f9c05e0935')
    and (v1_ex[0]['tokens'], v1_ex[0]['responses_without_usage']) == (90798, 0)
    and len(exec_query(db_examples, sample_qs['6'], '2026-10-14', '2026-10-14', trace_id=q5_ex[0]['trace_id'])) == 24,
    json.dumps(q5_ex),
)
for name, sql in {**sample_qs, **value_qs}.items():
    exec_query(db_examples, sql, '2026-10-14', '2026-10-14', trace_id='a4db76890e3b711d381938f9c05e0935')

# 3. Malformed, float ('12.5'), and scientific ('1e3') strings + paired cache_read_share probe.
BAD_USAGE = {k: 'not-an-int' for k in ('input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens', 'prompt_tokens', 'total_tokens')}
FLOAT_EXP_USAGE = {'input_tokens': '12.5', 'output_tokens': '1e3', 'cache_read_input_tokens': '12.5', 'cache_creation_input_tokens': 0, 'prompt_tokens': 90, 'total_tokens': '1e3'}
BAD_LAT, FLOAT_EXP_LAT = {'total_ms': 'slow', 'time_to_first_token_ms': 'na'}, {'total_ms': '12.5', 'time_to_first_token_ms': '1e3'}
bad_fixture = [
    live.row('b01', '2026-10-19T12:00:00Z', 'INVOCATION_STARTING', 'SB', invocation='IB', trace='trace-b', span='inv-b'),
    live.row('b02', '2026-10-19T12:00:01Z', 'LLM_RESPONSE', 'SB', invocation='IB', trace='trace-b', span='req-b1', parent='inv-b', attributes={'model': 'model-a', 'usage_metadata': BAD_USAGE}, latency=BAD_LAT),
    live.row('b03', '2026-10-19T12:00:02Z', 'LLM_RESPONSE', 'SB', invocation='IB', trace='trace-b', span='req-b2', parent='inv-b', attributes={'model': 'model-a', 'usage_metadata': live.usage(6, 3, cache_read=4)}, latency={'total_ms': 250, 'time_to_first_token_ms': 50}),
    live.row('b04', '2026-10-19T12:00:03Z', 'LLM_RESPONSE', 'SB', invocation='IB', trace='trace-b', span='req-b3', parent='inv-b', attributes={'model': 'model-a', 'usage_metadata': FLOAT_EXP_USAGE}, latency=FLOAT_EXP_LAT),
    live.row('b05', '2026-10-19T12:00:04Z', 'TOOL_COMPLETED', 'SB', invocation='IB', trace='trace-b', span='tool-b1', parent='req-b1', content={'tool': 'Bash', 'tool_origin': 'builtin'}, latency=FLOAT_EXP_LAT),
    live.row('b06', '2026-10-19T12:00:05Z', 'AGENT_COMPLETED', 'SB', invocation='IB', trace='trace-b', agent='Explore', attributes={'usage_metadata': FLOAT_EXP_USAGE}, latency=BAD_LAT),
    live.row('b07', '2026-10-19T12:00:06Z', 'INVOCATION_COMPLETED', 'SB', invocation='IB', trace='trace-b', span='inv-b', attributes={'usage_metadata': BAD_USAGE, 'finish_reason': 'end_turn'}, latency=FLOAT_EXP_LAT),
]
db_bad = make_db(bad_fixture, ['alice'])
probe = db_bad.execute(as_duckdb("SELECT SAFE_CAST('12.5' AS INT64), SAFE_CAST('1e3' AS INT64), SAFE_CAST(' 12 ' AS INT64)")).fetchone()
check("as_duckdb SAFE_CAST rejects '12.5' and '1e3' as NULL while keeping ' 12 ' -> 12", probe == (None, None, 12), str(probe))
for filename, qmap in (('sample_queries.sql', sample_qs), ('value_queries.sql', value_qs)):
    for name, sql in qmap.items():
        try:
            res = exec_query(db_bad, sql, '2026-10-19', '2026-10-19', trace_id='trace-b')
            ok, detail = True, json.dumps(res)
            if name == '2':
                ok = len(res) == 1 and (res[0]['input_tokens'], res[0]['output_tokens'], res[0]['cache_read_tokens'], res[0]['cache_read_share']) == (6, 3, 4, 0.4)
            elif name == '3':
                ok = len(res) == 1 and (res[0]['p50_total_ms'], res[0]['p50_ttft_ms']) == (250, 50)
            elif name == '5':
                ok = len(res) == 1 and (res[0]['turn_tokens'], res[0].get('responses_without_usage'), res[0]['turn_ms']) == (13, 2, None)
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, str(exc).splitlines()[0]
        check(f'malformed/float/scientific JSON numbers survive {filename} {name}', ok, detail)

print(f'{passed} passed, {failed} failed')
sys.exit(0 if failed == 0 else 1)
