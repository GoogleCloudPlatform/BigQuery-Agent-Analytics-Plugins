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

# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "google-cloud-bigquery>=3.11",
# ]
# ///
"""Runs one query over the mod's agent_events table, with a billing limit.

The queries in sql/sample_queries.sql and sql/value_queries.sql name their
table {PROJECT}.{DATASET}.{TABLE} and take their window as @start and @end
(and @trace_id, for one turn). This fills the names in, binds the parameters,
runs the query as you (your Application Default Credentials, not the mod's
settings) and prints one JSON object per row, a line each. Pass one query at a
time:

    python3 scripts/run_bq.py --project my-gcp-project --dataset agent_analytics \\
      --start 2026-10-01 --end 2026-10-31 "<one query>"

  --start, --end  Bind @start and @end: a date (UTC; a bare --start is that
                  day's first instant and a bare --end its last, so both days
                  are counted) or an ISO 8601 timestamp.
  --trace-id ID   Bind @trace_id (sample query 6, the span tree of one turn).
  --max-gb N      Refuse a query that would bill more than N GB (default 1).
  --dry-run       Run nothing: print, as one JSON object, the bytes the query
                  would scan (BigQuery estimates it; nothing is billed).

The project, dataset and table come from --project, --dataset and --table, or
from the variables setup.sh reads: BQAA_PROJECT_ID (or GCP_PROJECT_ID,
GOOGLE_CLOUD_PROJECT), BQAA_DATASET (or BQ_DATASET) and BQAA_TABLE (or
BQ_TABLE; default agent_events). They go into the SQL text, so only plain
names are taken.

Needs the Python BigQuery client (python3 -m pip install google-cloud-bigquery,
or run it with uv) and Application Default Credentials (gcloud auth
application-default login) that may run queries and read the table. Exit
status: 0 on success; 1 when the query or the setup fails, with an ERROR line
on stderr; 2 for a bad argument.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

# A project id is 6 to 30 lowercase letters, digits and hyphens, starting with
# a letter; a domain-scoped one has its domain and a colon in front. A dataset
# or table is letters, digits and underscores (setup.sh takes the same). They
# are matched whole (fullmatch): "$" would let a trailing newline through.
PROJECT_PATTERN = re.compile(r"([a-z0-9.-]+:)?[a-z][-a-z0-9]{4,28}[a-z0-9]")
NAME_PATTERN = re.compile(r"[A-Za-z0-9_]+")
MAX_NAME_LENGTH = 1024
GB = 1 << 30


def first_env(*names: str, default: str = "") -> str:
    """The first of these environment variables that is set and not empty."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def check_names(project: str, dataset: str, table: str) -> None:
    """Raises ValueError unless the three names are plain: they are put into
    the SQL text, where anything else could change the query."""
    if not project:
        raise ValueError("set BQAA_PROJECT_ID (or GCP_PROJECT_ID) or pass --project")
    if not dataset:
        raise ValueError("set BQAA_DATASET (or BQ_DATASET) or pass --dataset")
    if not PROJECT_PATTERN.fullmatch(project):
        raise ValueError(f"{project!r} is not a project id")
    for what, name in (("dataset", dataset), ("table", table)):
        if not (NAME_PATTERN.fullmatch(name) and len(name) <= MAX_NAME_LENGTH):
            raise ValueError(f"{name!r} is not a plain {what} name (letters, digits and underscores)")


def inject_placeholders(sql: str, project: str, dataset: str, table: str) -> str:
    """The query with {PROJECT}, {DATASET} and {TABLE} filled in, after the
    names are checked."""
    check_names(project, dataset, table)
    return sql.replace("{PROJECT}", project).replace("{DATASET}", dataset).replace("{TABLE}", table)


def parse_ts(value: str, is_end: bool = False) -> datetime:
    """An ISO 8601 timestamp or a YYYY-MM-DD date, as UTC.

    A bare date is the first instant of that day, or, with is_end, the last
    microsecond of it (23:59:59.999999), so that --end 2026-10-15 counts every
    event of the 15th under BETWEEN @start AND @end. A timestamp keeps its
    time, and is UTC when it names no offset.
    """
    try:
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return day + timedelta(days=1) - timedelta(microseconds=1) if is_end else day
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid timestamp {value!r}: {error}") from error
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_end_ts(value: str) -> datetime:
    """argparse adapter: --end, where a bare date means the end of that day."""
    return parse_ts(value, is_end=True)


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        number = 0.0
    if not number > 0 or number == float("inf"):
        raise argparse.ArgumentTypeError(f"{value!r} is not a positive number of GB")
    return number


def format_bytes(count: float) -> str:
    """A byte count a person can read."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(count) < 1024:
            return f"{count:.2f} {unit}"
        count /= 1024
    return f"{count:.2f} PB"


def build_query_parameters(bigquery, args: argparse.Namespace) -> list:
    """The BigQuery query parameters for the flags given: @start, @end and
    @trace_id, each only when its flag is."""
    params = []
    if args.start is not None:
        params.append(bigquery.ScalarQueryParameter("start", "TIMESTAMP", args.start))
    if args.end is not None:
        params.append(bigquery.ScalarQueryParameter("end", "TIMESTAMP", args.end))
    if args.trace_id is not None:
        params.append(bigquery.ScalarQueryParameter("trace_id", "STRING", args.trace_id))
    return params


def load_bigquery():
    """The Python BigQuery client, imported when it is first needed so that
    --help and the checks of the arguments work without it."""
    from google.cloud import bigquery

    return bigquery


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one query over the agent_events table, with a billing limit.",
        epilog="Placeholders in the query: {PROJECT}, {DATASET}, {TABLE}; parameters: @start, @end, @trace_id.",
    )
    parser.add_argument("query", help="one SQL query (see sql/sample_queries.sql, sql/value_queries.sql)")
    parser.add_argument(
        "--project",
        default=first_env("BQAA_PROJECT_ID", "GCP_PROJECT_ID", "GOOGLE_CLOUD_PROJECT"),
        help="the project of the table (default: BQAA_PROJECT_ID, GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT)",
    )
    parser.add_argument(
        "--dataset",
        default=first_env("BQAA_DATASET", "BQ_DATASET"),
        help="the dataset of the table (default: BQAA_DATASET or BQ_DATASET)",
    )
    parser.add_argument(
        "--table",
        default=first_env("BQAA_TABLE", "BQ_TABLE", default="agent_events"),
        help="the events table (default: BQAA_TABLE, BQ_TABLE or agent_events)",
    )
    parser.add_argument(
        "--start",
        type=parse_ts,
        default=None,
        help="bind @start: an ISO 8601 timestamp or a date (UTC)",
    )
    parser.add_argument(
        "--end",
        type=parse_end_ts,
        default=None,
        help="bind @end: an ISO 8601 timestamp or a date (UTC; a bare date is the end of that day)",
    )
    parser.add_argument("--trace-id", default=None, help="bind @trace_id (a string)")
    parser.add_argument(
        "--max-gb",
        type=positive_float,
        default=1.0,
        help="refuse a query that would bill more than this many GB (default: 1)",
    )
    parser.add_argument("--dry-run", action="store_true", help="estimate the bytes the query would scan and run nothing")
    args = parser.parse_args(argv)
    if args.start is not None and args.end is not None and args.start > args.end:
        parser.error(f"--start {args.start.isoformat()} is after --end {args.end.isoformat()}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        sql = inject_placeholders(args.query, args.project, args.dataset, args.table)
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    try:
        bigquery = load_bigquery()
    except ImportError:
        print(
            "ERROR: this needs the Python BigQuery client: python3 -m pip install google-cloud-bigquery "
            "(or run it with uv: uv run scripts/run_bq.py ...)",
            file=sys.stderr,
        )
        return 1
    limit = int(args.max_gb * GB)
    try:
        client = bigquery.Client(project=args.project)
        parameters = build_query_parameters(bigquery, args)
        if args.dry_run:
            config = bigquery.QueryJobConfig(
                dry_run=True, use_query_cache=False, use_legacy_sql=False, query_parameters=parameters
            )
            scanned = client.query(sql, job_config=config).total_bytes_processed
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "total_bytes_processed": scanned,
                        "human_readable": format_bytes(scanned),
                        "exceeds_limit": scanned > limit,
                        "limit_gb": int(args.max_gb) if args.max_gb.is_integer() else args.max_gb,
                    }
                )
            )
            return 0
        config = bigquery.QueryJobConfig(maximum_bytes_billed=limit, use_legacy_sql=False, query_parameters=parameters)
        for row in client.query(sql, job_config=config).result():
            print(json.dumps(dict(row), default=str))
    except Exception as error:  # the client's errors: auth, access, a bad query, the limit
        print(f"ERROR{' (dry-run)' if args.dry_run else ''}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
