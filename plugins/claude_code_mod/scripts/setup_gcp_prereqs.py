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

"""Sets up who writes to and who reads the mod's BigQuery table.

setup.sh makes the dataset, the table and the two usage views; this script
sets up the access around them: the writer's service account, who may
impersonate it, and who reads the views. It is dry-run by default: it prints
its plan, calls nothing and changes nothing. Pass --execute when the printed
plan looks right. It uses the caller's current Google Cloud credentials for
setup, and grants narrower runtime roles to --principal (or to
--service-account) when given. It never makes the dataset or the table:
--no-create-dataset and --no-create-table are accepted, and change nothing.

--writer-role row-writer grants the writer only what inserting rows takes: a
custom role with bigquery.tables.updateData and bigquery.tables.get, on the
events table alone, so the writer reads no rows, runs no queries and deletes
nothing. --token-creator lets a group impersonate the writer service account,
and --reader gives a group the views (in the views dataset), with the views
authorized on the events dataset so readers need no access to the table.
Before it grants anything, it refuses a writer that already holds other
grants on the project, the datasets or the table (--allow-existing-grants
keeps them), and a bqaaRowWriter role that holds more than those two
permissions; --token-creator is taken only with --writer-role row-writer.
It also takes the project's Viewer and Editor groups off the events dataset
(and Editors off the views dataset), so those project roles read no rows
(--keep-project-groups leaves them), lists who still reads every row through
a project role (custom ones too, by their permissions), and stops if a
--reader or --token-creator member does (--allow-existing-grants keeps it),
or may: through a domain, or a group whose membership Cloud Identity does not
answer for (--accept-unverified accepts that, and only that).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable

BIGQUERY_API_SERVICES = ("bigquery.googleapis.com", "bigquerystorage.googleapis.com")
# The custom role --writer-role row-writer grants: streaming inserts check
# bigquery.tables.get as well as bigquery.tables.updateData ("Permission
# bigquery.tables.get denied on table" without it).
ROW_WRITER_ROLE_ID = "bqaaRowWriter"
ROW_WRITER_PERMISSIONS = ("bigquery.tables.get", "bigquery.tables.updateData")
# The views the mod's setup.sh makes, the only ones --reader authorizes on
# the events dataset.
VIEW_NAMES = ("daily_usage_by_user", "daily_usage_by_model")
# The permissions that read a table's rows: reading them (queries, the
# tabledata API, the Storage Read API) and extracting them.
READ_PERMISSIONS = ("bigquery.tables.getData", "bigquery.tables.export")
# The permission that edits the project's custom roles, bqaaRowWriter
# included: with Token Creator, its holder could add a reading permission to
# the writer's role and read every row as the writer.
ROLE_EDIT_PERMISSIONS = ("iam.roles.update",)
# Project roles known to read every table of the project whatever a
# dataset's access says (any other project role is read with gcloud iam
# roles describe, and reads when it holds READ_PERMISSIONS); and the basic
# roles that read the events dataset only through its projectReaders and
# projectWriters entries, which this setup removes.
PROJECT_READ_ROLES = (
    "roles/owner",
    "roles/bigquery.admin",
    "roles/bigquery.dataOwner",
    "roles/bigquery.dataEditor",
    "roles/bigquery.dataViewer",
)
PROJECT_GROUP_ROLES = ("roles/editor", "roles/viewer")

PRINCIPAL_PREFIXES = (
    "user:",
    "serviceAccount:",
    "group:",
    "domain:",
    "principal://",
    "principalSet://",
)

# The names go into SQL (the GRANT statements), so only plain ones are taken,
# the ones setup.sh takes: a project id of 6 to 30 lowercase letters, digits
# and hyphens, starting with a letter (a domain-scoped one has its domain and
# a colon in front); a dataset or table of letters, digits and underscores; a
# location of letters, digits and hyphens. They are matched whole (fullmatch):
# "$" would let a trailing newline through.
PROJECT_ID_PATTERN = re.compile(r"([a-z0-9.-]+:)?[a-z][-a-z0-9]{4,28}[a-z0-9]")
PLAIN_NAME_PATTERN = re.compile(r"[A-Za-z0-9_]+")
LOCATION_PATTERN = re.compile(r"[A-Za-z0-9-]+")
MAX_NAME_LENGTH = 1024


@dataclass(frozen=True)
class Step:
    """One unit of bootstrap work.

    Each step declares which credential surfaces it actually needs so
    the preflight gate can fail-fast on the right one. ``Step.action``
    callbacks aren't all ADC-backed — ``_ensure_service_account`` is
    an action but it shells out to ``gcloud iam service-accounts``,
    which uses the gcloud CLI account, not ADC. Inferring needs from
    ``command`` vs ``action`` was wrong; explicit flags are the fix.
    """

    label: str
    command: list[str] | None = None
    action: Callable[[], None] | None = None
    detail: str | None = None
    needs_adc: bool = False
    needs_cli_auth: bool = False


@dataclass(frozen=True)
class _PreflightResult:
    """Captured state of the caller's gcloud auth + config.

    Two distinct credential surfaces matter and they fail independently:

    * **ADC** — used by every google-cloud-* Python client (so
      ``bigquery.Client.create_dataset``, ``create_table``, etc.)
      Validated with ``gcloud auth application-default print-access-token``.
    * **gcloud CLI auth** — used by every ``gcloud`` and ``bq`` subprocess
      this script spawns (``services enable``, ``projects
      add-iam-policy-binding``, ``iam service-accounts create``,
      ``bq add-iam-policy-binding``). Validated with ``gcloud auth list``
      and ``gcloud auth print-access-token``.

    They share an identity in most local-dev setups (one ``gcloud auth
    login`` plus one ``application-default login``), but on agent boxes
    or CI they're often separate, so check both. Reporting them
    separately also makes the failure message actionable.
    """

    adc_ok: bool
    adc_message: str
    cli_auth_ok: bool
    cli_auth_account: str
    cli_auth_message: str
    gcloud_project: str
    gcloud_available: bool


def _gcloud_command(args: argparse.Namespace, *parts: str) -> list[str]:
    """Build a gcloud invocation; in --non-interactive mode injects --quiet.

    gcloud's --quiet is a global flag that auto-confirms prompts and
    suppresses interactive checks. Required when an agent runs the
    bootstrap unattended so a confirmation prompt can never block.
    """
    cmd = ["gcloud"]
    if args.non_interactive:
        cmd.append("--quiet")
    cmd.extend(parts)
    return cmd


def _bq_command(args: argparse.Namespace, *parts: str) -> list[str]:
    """Build a bq invocation; in --non-interactive mode injects --quiet.

    bq's --quiet is a global flag that suppresses status messages and
    interactive confirms. Same purpose as gcloud's --quiet.
    """
    cmd = ["bq"]
    if args.non_interactive:
        cmd.append("--quiet")
    cmd.extend(parts)
    return cmd


def _default_project() -> str:
    return (
        os.environ.get("BQAA_PROJECT_ID")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or ""
    )


def _default_dataset() -> str:
    return os.environ.get("BQAA_DATASET") or os.environ.get("BQ_DATASET") or "agent_analytics"


def _default_table() -> str:
    return os.environ.get("BQAA_TABLE") or os.environ.get("BQ_TABLE") or "agent_events"


def _quote_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def _service_account_email(project: str, value: str) -> str:
    if "@" in value:
        return value
    return f"{value}@{project}.iam.gserviceaccount.com"


def _service_account_id(value: str) -> str:
    return value.split("@", 1)[0]


def _ensure_service_account(args: argparse.Namespace) -> None:
    project = args.project
    value = args.service_account
    email = _service_account_email(project, value)
    describe = subprocess.run(
        _gcloud_command(
            args,
            "iam",
            "service-accounts",
            "describe",
            email,
            "--project",
            project,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if describe.returncode == 0:
        print(f"OK service account exists: {email}")
        return
    command = _gcloud_command(
        args,
        "iam",
        "service-accounts",
        "create",
        _service_account_id(value),
        "--project",
        project,
        "--display-name",
        "BigQuery Agent Analytics writer",
    )
    last_result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, 4):
        last_result = subprocess.run(command, check=False)
        if last_result.returncode == 0:
            print(f"CREATED service account: {email}")
            return
        if attempt < 3:
            print(
                "Service account create failed; retrying after API/IAM propagation "
                f"delay (attempt {attempt}/3).",
                file=sys.stderr,
            )
            time.sleep(5)
    returncode = last_result.returncode if last_result else 1
    raise subprocess.CalledProcessError(returncode, command)


def _validate_principal(value: str) -> None:
    if value in ("allUsers", "allAuthenticatedUsers"):
        return
    if any(value.startswith(prefix) for prefix in PRINCIPAL_PREFIXES):
        return
    raise SystemExit(
        "--principal must start with one of: "
        "user:, serviceAccount:, group:, domain:, principal://, principalSet:// "
        "(or be allUsers/allAuthenticatedUsers)"
    )


def _require_project_dataset(args: argparse.Namespace) -> None:
    if not args.project:
        raise SystemExit(
            "--project is required, or set BQAA_PROJECT_ID/GCP_PROJECT_ID/GOOGLE_CLOUD_PROJECT"
        )
    if not args.dataset:
        raise SystemExit("--dataset is required, or set BQAA_DATASET")
    if not PROJECT_ID_PATTERN.fullmatch(args.project):
        raise SystemExit(f"--project {args.project!r} is not a project id")
    for flag, name in (("--dataset", args.dataset), ("--table", args.table), ("--views-dataset", _views_dataset(args))):
        if not (PLAIN_NAME_PATTERN.fullmatch(name) and len(name) <= MAX_NAME_LENGTH):
            raise SystemExit(f"{flag} {name!r} is not a plain dataset or table name (letters, digits and underscores)")
    if not LOCATION_PATTERN.fullmatch(args.location):
        raise SystemExit(f"--location {args.location!r} is not a location")
    if args.principal:
        _validate_principal(args.principal)
    if args.token_creator and not args.service_account:
        raise SystemExit("--token-creator goes with --service-account")
    if args.token_creator and args.writer_role != "row-writer":
        raise SystemExit(
            "--token-creator goes with --writer-role row-writer: whoever may impersonate the "
            "account gets what it holds, and data-editor reads every row"
        )
    if args.service_account and args.principal:
        email = _service_account_email(args.project, args.service_account)
        if args.principal.lower() != f"serviceAccount:{email}".lower():
            raise SystemExit(
                f"--principal {args.principal} is not --service-account {email}: setup would check "
                "one and let the token creators impersonate the other. Give one of them."
            )
    if args.reader and _views_dataset(args) == args.dataset:
        raise SystemExit(
            f"the views dataset is the events dataset ({args.dataset}): readers granted on it would "
            "read every row. Name another with --views-dataset."
        )
    for member in [*args.token_creator, *args.reader]:
        if not any(member.startswith(prefix) for prefix in ("user:", "group:", "serviceAccount:")):
            raise SystemExit(
                f"{member!r}: --token-creator and --reader take user:, group: or serviceAccount: "
                "members. Not a domain: Token Creator for a domain lets everyone in it mint the writer's "
                "tokens, and a domain's members cannot be checked against the grants on the events data."
            )


def _dataset_grant_command(args: argparse.Namespace, dataset: str, member: str, role: str) -> list[str]:
    """Grant a role on a dataset with BigQuery's GRANT statement.

    ``bq add-iam-policy-binding`` takes a dataset only on allowlisted
    projects ("This feature requires allowlisting"), and stops reading flags
    at its first positional argument, so the SQL statement is the reliable
    way. Members are checked before this (a principal prefix and no quotes).
    """
    if '"' in member or "`" in member or "`" in role:
        raise SystemExit(f"{member!r} or {role!r} holds a quote")
    statement = f'GRANT `{role}` ON SCHEMA `{args.project}.{dataset}` TO "{member}"'
    return _bq_command(
        args,
        f"--project_id={args.project}",
        "query",
        "--use_legacy_sql=false",
        f"--location={args.location or 'US'}",
        statement,
    )


def _views_dataset(args: argparse.Namespace) -> str:
    return args.views_dataset or f"{args.dataset}_reporting"


def _json_output(command: list[str], missing_ok: bool = False) -> dict:
    """Run a gcloud or bq command that prints JSON, and parse it.

    With ``missing_ok``, a resource that does not exist reads as ``{}``.
    """
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        said = f"{result.stderr or ''}\n{result.stdout or ''}"
        if missing_ok and ("Not found" in said or "NOT_FOUND" in said):
            return {}
        raise SystemExit(f"{_quote_command(command)} failed: {_summarize_stderr(said, 'no output')}")
    try:
        value = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as error:
        raise SystemExit(f"{_quote_command(command)} printed no JSON: {error}") from error
    return value if isinstance(value, dict) else {}


def _row_writer_role(args: argparse.Namespace) -> list[str] | None:
    """The row-writer role's missing permissions, or None when there is no
    such role. A role that holds more than ROW_WRITER_PERMISSIONS, or is
    deleted or disabled, is refused: bound on the table, it would let the
    writer read or change it. Any failure to read it other than NOT_FOUND
    (access denied, a network error) stops the run too: an unread role is
    not a role known to be narrow."""
    project = args.project
    name = f"projects/{project}/roles/{ROW_WRITER_ROLE_ID}"
    describe = subprocess.run(
        _gcloud_command(args, "iam", "roles", "describe", ROW_WRITER_ROLE_ID, "--project", project, "--format=json"),
        capture_output=True,
        text=True,
        check=False,
    )
    if describe.returncode != 0:
        said = f"{describe.stderr or ''}\n{describe.stdout or ''}"
        if "NOT_FOUND" in said:
            return None
        raise SystemExit(
            f"Could not read {name}: {_summarize_stderr(said, 'no output')}. It must be read before "
            "anyone may impersonate the writer: run as someone who may read the project's roles "
            "(iam.roles.get), and run again."
        )
    try:
        role = json.loads(describe.stdout or "{}")
    except json.JSONDecodeError as error:
        raise SystemExit(f"gcloud iam roles describe {name} printed no JSON: {error}") from error
    if role.get("deleted"):
        raise SystemExit(
            f"{name} is deleted. Undelete it (gcloud iam roles undelete {ROW_WRITER_ROLE_ID} "
            f"--project {project}) and run again."
        )
    if role.get("stage") == "DISABLED":
        raise SystemExit(
            f"{name} is disabled (stage DISABLED). Set its stage back (gcloud iam roles update "
            f"{ROW_WRITER_ROLE_ID} --project {project} --stage GA) and run again."
        )
    held = set(role.get("includedPermissions") or [])
    extra = sorted(held - set(ROW_WRITER_PERMISSIONS))
    if extra:
        raise SystemExit(
            f"{name} also holds {', '.join(extra)}: bound on the table, it would let the writer "
            f"do more than insert rows. Remove them (gcloud iam roles update {ROW_WRITER_ROLE_ID} "
            f"--project {project} --remove-permissions {','.join(extra)}) and run again."
        )
    return [permission for permission in ROW_WRITER_PERMISSIONS if permission not in held]


def _ensure_row_writer_role(args: argparse.Namespace) -> None:
    """Create the row-writer role, or add the permissions it lacks."""
    project = args.project
    name = f"projects/{project}/roles/{ROW_WRITER_ROLE_ID}"
    missing = _row_writer_role(args)
    if missing is None:
        permissions = ",".join(ROW_WRITER_PERMISSIONS)
        subprocess.run(
            _gcloud_command(
                args, "iam", "roles", "create", ROW_WRITER_ROLE_ID, "--project", project,
                "--title", "BQAA row writer",
                "--description", "Inserts rows into the tables it is granted on, and reads none",
                "--permissions", permissions, "--stage", "GA",
            ),
            stdout=subprocess.DEVNULL,
            check=True,
        )
        print(f"CREATED role: {name} ({permissions})")
    elif missing:
        subprocess.run(
            _gcloud_command(
                args, "iam", "roles", "update", ROW_WRITER_ROLE_ID, "--project", project,
                "--add-permissions", ",".join(missing), "--quiet",
            ),
            stdout=subprocess.DEVNULL,
            check=True,
        )
        print(f"UPDATED role {name}: added {', '.join(missing)}")
    else:
        print(f"OK role exists, with {', '.join(ROW_WRITER_PERMISSIONS)} only: {name}")


def _writer_holds_row_writer_binding(args: argparse.Namespace) -> bool:
    """Whether the writer holds the row-writer role on the table."""
    role = f"projects/{args.project}/roles/{ROW_WRITER_ROLE_ID}"
    table = f"{args.project}:{args.dataset}.{args.table}"
    policy = _json_output(_bq_command(args, "--format=json", "get-iam-policy", table), missing_ok=True)
    principal = args.principal.lower()
    return any(
        binding.get("role") == role and principal in (m.lower() for m in binding.get("members") or [])
        for binding in policy.get("bindings") or []
    )


def _check_row_writer_role(args: argparse.Namespace) -> None:
    """With --no-grant-iam: refuse a row-writer role that holds more, or that
    the writer is bound to but cannot be found, and change nothing."""
    missing = _row_writer_role(args)
    name = f"projects/{args.project}/roles/{ROW_WRITER_ROLE_ID}"
    if missing is None:
        if _writer_holds_row_writer_binding(args):
            raise SystemExit(
                f"{args.principal} holds {name} on the table, but the role cannot be found: "
                "what the binding grants cannot be checked. Run without --no-grant-iam to make the role, "
                "or remove the binding."
            )
        print(f"OK no role {name}: the writer holds no row-writer grant from this setup")
    elif missing:
        print(
            f"MISSING: {name} lacks {', '.join(missing)}: the writer cannot insert until it has them "
            "(run without --no-grant-iam to add them)"
        )
    else:
        print(f"OK role {name} holds {', '.join(ROW_WRITER_PERMISSIONS)} and nothing else")


# Members a grant to which reaches every account.
_PUBLIC = {"allusers", "allauthenticatedusers", "specialgroup:allauthenticatedusers", "specialgroup:allusers"}
# What _restrict_dataset_access takes off each dataset (unless
# --keep-project-groups): the project's Viewer and Editor groups, and the
# entries that reach everyone.
_EVENTS_CLEARED = {"specialgroup:projectreaders", "specialgroup:projectwriters"} | _PUBLIC
_VIEWS_CLEARED = {"specialgroup:projectwriters"} | _PUBLIC


def _dataset_member(entry: dict) -> str | None:
    """The member a dataset access entry names, written as IAM writes it."""
    for key, kind in (("userByEmail", "user"), ("groupByEmail", "group"), ("domain", "domain"),
                      ("specialGroup", "specialGroup")):
        if entry.get(key):
            return f"{kind}:{entry[key]}"
    return entry.get("iamMember")


def _reach(named: str, member: str) -> str:
    """How far a grant to `named` reaches `member`.

    "yes": everyone; the member itself (a dataset's access writes service
    accounts as userByEmail); or the domain of a person's or service
    account's address.
    "maybe": another domain, for a person (IAM resolves a domain by its
    identity customer, whose other domains an address does not show); any
    domain, for a group or identity pool (a group's own address says nothing
    of its members'); a group or identity pool on either side, whose members
    this setup cannot see.
    "no": anything else, and another domain for a service account or a
    single identity-pool principal, which belong to no domain.
    The project's special groups (projectOwners, ...) stand for project role
    holders, whose bindings are looked at themselves."""
    if named.lower() in _PUBLIC:
        return "yes"
    kind, _, email = member.partition(":")
    other_kind, _, other = named.partition(":")
    if other_kind == "domain":
        if kind in ("user", "serviceAccount") and email.lower().rpartition("@")[2] == other.lower():
            return "yes"
        return "no" if kind == "serviceAccount" or member.startswith("principal://") else "maybe"
    if other_kind == "specialGroup":
        return "no"
    if email.lower() == other.lower() and (kind == other_kind or {kind, other_kind} == {"serviceAccount", "user"}):
        return "yes"
    return "maybe" if _unexpanded(member) or _unexpanded(named) else "no"


def _unexpanded(named: str) -> bool:
    """A member whose members this setup cannot see: a group, or a set of
    identity-pool principals."""
    return named.lower().startswith(("group:", "principalset://"))


def _member_of(args: argparse.Namespace, group: str, member: str, cache: dict) -> bool | None:
    """Whether `member` (an address) is in `group`, directly or through other
    groups, as Cloud Identity answers (gcloud identity groups memberships
    check-transitive-membership); None when it cannot answer. The method
    answers only for Google Workspace Enterprise Standard, Enterprise Plus
    and Enterprise for Education, and Cloud Identity Premium, accounts, and
    with 403 elsewhere; nor does it answer for a group the caller cannot
    see."""
    key = (group.lower(), member.lower())
    if key not in cache:
        result = subprocess.run(
            _gcloud_command(args, "identity", "groups", "memberships", "check-transitive-membership",
                            f"--group-email={group}", f"--member-email={member}", "--format=json"),
            capture_output=True,
            text=True,
            check=False,
        )
        answer = None
        if result.returncode == 0:
            try:
                # The API leaves a false hasMembership out.
                answer = json.loads(result.stdout or "{}").get("hasMembership") is True
            except (json.JSONDecodeError, AttributeError):
                answer = None
        cache[key] = answer
    return cache[key]


def _role_permissions(args: argparse.Namespace, role: str, cache: dict) -> frozenset[str] | None:
    """The permissions gcloud iam roles describe lists for a role: a custom
    role of a project or an organization, or a predefined one. A role that
    does not exist grants nothing (an empty set); None when one that does
    cannot be read."""
    if role not in cache:
        parts = role.split("/")
        if len(parts) == 4 and parts[0] in ("projects", "organizations") and parts[2] == "roles":
            scope = "--project" if parts[0] == "projects" else "--organization"
            command = _gcloud_command(args, "iam", "roles", "describe", parts[3], scope, parts[1], "--format=json")
        else:
            command = _gcloud_command(args, "iam", "roles", "describe", role, "--format=json")
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        answer = None
        if result.returncode == 0:
            try:
                answer = frozenset(json.loads(result.stdout or "{}").get("includedPermissions") or [])
            except (json.JSONDecodeError, AttributeError):
                answer = None
        elif "NOT_FOUND" in f"{result.stderr or ''}\n{result.stdout or ''}":
            answer = frozenset()
        cache[role] = answer
    return cache[role]


def _role_edits_roles(args: argparse.Namespace, role: str, cache: dict) -> bool:
    """Whether a role can edit the project's custom roles (ROLE_EDIT_PERMISSIONS);
    a role whose permissions cannot be read is left to _role_reads, which
    counts it as not ruled out."""
    held = _role_permissions(args, role, cache)
    return held is not None and bool(held & set(ROLE_EDIT_PERMISSIONS))


def _role_reads(args: argparse.Namespace, role: str, cache: dict) -> bool | None:
    """Whether a role reads table data (READ_PERMISSIONS): True for
    PROJECT_READ_ROLES and a dataset's READER, WRITER and OWNER; otherwise
    from its permissions; None when they cannot be read."""
    if role in PROJECT_READ_ROLES or role in ("READER", "WRITER", "OWNER"):
        return True
    held = _role_permissions(args, role, cache)
    return None if held is None else bool(held & set(READ_PERMISSIONS))


def _grants(args: argparse.Namespace, datasets) -> list[tuple[str, str, str, str]]:
    """(member, what it holds, the role, "project", "dataset" or "table")
    for every project binding, the access of `datasets` (leaving out what
    the dataset-access step takes off), and the events table's IAM policy."""
    grants: list[tuple[str, str, str, str]] = []
    policy = _json_output(_gcloud_command(args, "projects", "get-iam-policy", args.project, "--format=json"))
    for binding in policy.get("bindings") or []:
        for member in binding.get("members") or []:
            grants.append((member, f"{binding.get('role')} on the project {args.project}", binding.get("role") or "", "project"))
    for dataset in datasets:
        cleared = set() if args.keep_project_groups else (_EVENTS_CLEARED if dataset == args.dataset else _VIEWS_CLEARED)
        info = _json_output(_bq_command(args, "--format=json", "show", f"{args.project}:{dataset}"), missing_ok=True)
        for entry in info.get("access") or []:
            member = _dataset_member(entry)
            if member and entry.get("role") and member.lower() not in cleared:
                grants.append((member, f"{entry['role']} on the dataset {args.project}.{dataset}", entry["role"], "dataset"))
    table = f"{args.project}:{args.dataset}.{args.table}"
    table_policy = _json_output(_bq_command(args, "--format=json", "get-iam-policy", table), missing_ok=True)
    for binding in table_policy.get("bindings") or []:
        for member in binding.get("members") or []:
            grants.append((member, f"{binding.get('role')} on the table {args.project}.{args.dataset}.{args.table}",
                           binding.get("role") or "", "table"))
    return grants


_UNREAD_ROLE = " (a role whose permissions could not be read)"


def _settle(definite: list[str], unverified: list[str], args: argparse.Namespace, what: str, fix: str) -> None:
    """Stop on what definitely reaches too far unless --allow-existing-grants,
    and on what could not be ruled out unless --accept-unverified: each flag
    keeps its own kind only."""
    stops = []
    if definite:
        if args.allow_existing_grants:
            print("KEPT (--allow-existing-grants): " + "; ".join(definite))
        else:
            stops.append(f"{what}: " + "; ".join(definite) + ". " + fix + " --allow-existing-grants keeps them.")
    if unverified:
        if args.accept_unverified:
            print("ACCEPTED (--accept-unverified), not ruled out: " + "; ".join(unverified))
        else:
            stops.append(
                "this setup could not rule out that these reach further (a group's or an identity pool's members, "
                "a domain's other names, or a role whose permissions it could not read): " + "; ".join(unverified)
                + ". Check them, then --accept-unverified accepts them (and nothing that is known to reach further)."
            )
    if stops:
        raise SystemExit(" | ".join(stops))


def _check_writer_grants(args: argparse.Namespace) -> None:
    """Refuse a writer that already holds, or may hold, more than the
    row-writer role.

    Looks at the project's IAM policy, the events and views datasets' access
    and the events table's IAM policy, for grants to the writer itself, to
    everyone, to its domain, or to a group it may be in (Cloud Identity is
    asked). A reused service account (one an earlier setup gave dataEditor
    and jobUser, say) would otherwise hand every token creator a read of
    every row. Every grant that reaches the writer counts, whatever its
    role: the writer is to hold its row-writer binding on the table and
    nothing else, and a role that looks harmless can still widen that role
    (iam.roles.update) or delete the project. --allow-existing-grants keeps
    an audited one.
    """
    principal = args.principal
    writer = principal.partition(":")[2]
    role = f"projects/{args.project}/roles/{ROW_WRITER_ROLE_ID}"
    memberships: dict = {}
    definite: list[str] = []
    unverified: list[str] = []
    for named, held, granted, scope in _grants(args, dict.fromkeys([args.dataset, _views_dataset(args)])):
        if scope == "table" and granted == role and named.lower() == principal.lower():
            continue
        reach = _reach(named, principal)
        if reach == "no":
            continue
        line = f"{held} (granted to {named})"
        if reach == "maybe":
            kind, _, other = named.partition(":")
            answer = _member_of(args, other, writer, memberships) if kind == "group" else None
            if answer is False:
                continue
            if answer is None:
                unverified.append(line)
                continue
            line += " (the writer is a member, as Cloud Identity answers)"
        definite.append(line)
    if not definite and not unverified:
        print(f"OK {principal} holds nothing on the project, the datasets or the table beyond {role}")
        return
    _settle(definite, unverified, args, f"{principal} already holds more than the row-writer role",
            "Its tokens could read or change the table. Use a new service account for the writer, or remove "
            "those grants and run again;")


def _check_project_readers(args: argparse.Namespace) -> None:
    """Name who can read every row: a project role that reads tables (a
    predefined one, or any role whose permissions say so), or a grant on the
    events dataset or table that reads it. Stop when everyone can, and when
    a --reader or --token-creator member can, unless --allow-existing-grants;
    and when one may, which this setup cannot rule out (a domain that may
    hold the member under another name, a group or identity pool on either
    side whose membership Cloud Identity does not answer for, a role whose
    permissions could not be read), unless --accept-unverified."""
    roles: dict = {}
    grants: list[tuple[str, str, bool]] = []
    editors: list[tuple[str, str]] = []
    for named, held, granted, scope in _grants(args, [args.dataset]):
        if scope == "project" and granted in PROJECT_GROUP_ROLES:
            if args.keep_project_groups:
                grants.append((named, held, True))
            continue
        reads = _role_reads(args, granted, roles)
        if reads is None:
            grants.append((named, held + _UNREAD_ROLE, False))
        elif reads:
            grants.append((named, held, True))
        elif scope == "project" and _role_edits_roles(args, granted, roles):
            editors.append((named, held))
    if grants:
        listed = "\n  - ".join(f"{member}: {held}" for member, held, _ in grants)
        print(f"These hold a project role that reads every table, or a grant that reads the events dataset or table:\n  - {listed}")
    if editors:
        listed = "\n  - ".join(f"{member}: {held}" for member, held in editors)
        print(f"These may edit the project's custom roles (iam.roles.update), bqaaRowWriter included:\n  - {listed}")
    members = [*args.reader, *args.token_creator]
    public = [f"{named} ({held})" for named, held, known in grants if known and named.lower() in _PUBLIC]
    clashes: list[str] = []
    editing: list[str] = []
    unverified = [f"{named} ({held})" for named, held, known in grants if not known and named.lower() in _PUBLIC]
    checked: list[str] = []
    memberships: dict = {}

    def judge(member: str, named: str, held: str) -> tuple[str | None, str]:
        """Whether a grant reaches a member: "reaches", "unverified",
        "checked" (not a member, as Cloud Identity answers) or None."""
        reach = _reach(named, member)
        line = f"{member}: {named} ({held})"
        if reach == "no":
            return None, line
        if reach == "maybe":
            kind, _, email = member.partition(":")
            other_kind, _, other = named.partition(":")
            answer = None
            if kind == "group" and other_kind in ("user", "serviceAccount"):
                # Is the person or service account in the member group?
                answer = _member_of(args, email, other, memberships)
            elif other_kind == "group" and kind in ("user", "serviceAccount", "group"):
                # Is the member (or the whole member group) in the granted group?
                answer = _member_of(args, other, email, memberships)
                if answer is False and kind == "group":
                    # Not inside it as a whole, but some of its members may be.
                    answer = None
            if answer is False:
                return "checked", line
            if answer is None:
                return "unverified", line
            line += " (a member, as Cloud Identity answers)"
        return "reaches", line

    for member in members:
        for named, held, known in grants:
            if named.lower() in _PUBLIC and not known:
                continue
            verdict, line = judge(member, named, held)
            if verdict == "reaches":
                (clashes if known else unverified).append(line)
            elif verdict == "unverified":
                unverified.append(line)
            elif verdict == "checked":
                checked.append(line)
    # A token creator who may edit bqaaRowWriter could read every row as the
    # writer once it may impersonate it.
    for member in args.token_creator:
        for named, held in editors:
            verdict, line = judge(member, named, held)
            if verdict == "reaches":
                editing.append(line)
            elif verdict == "unverified":
                unverified.append(line)
            elif verdict == "checked":
                checked.append(line)
    definite = []
    if public:
        definite.append("everyone can read the table: " + "; ".join(public))
    if clashes:
        definite.append("a reader or token creator can read every row, views or not: " + "; ".join(clashes))
    if editing:
        definite.append(
            "a token creator may edit the project's custom roles, so as the writer it could add a reading "
            "permission to bqaaRowWriter and read every row: " + "; ".join(editing)
        )
    if not definite and not unverified:
        note = f" (not a member, as Cloud Identity answers: {'; '.join(checked)})" if checked else ""
        print("OK no grant to everyone, and no grant that reads the events dataset or table, or project role "
              "that reads it, reaches a --reader or --token-creator member, and no --token-creator member may "
              f"edit the project's custom roles{note}")
        return
    _settle(definite, unverified, args, "the table can be read beyond the views",
            "Take those grants off (see docs/service-account-auth.md, Moving from dataEditor), or keep the "
            "events dataset in a project of its own;")


def _check_api_enabled(args: argparse.Namespace, service: str) -> None:
    """Fail when a service the plan relies on is not enabled (--no-enable-apis)."""
    result = subprocess.run(
        _gcloud_command(
            args, "services", "list", "--enabled", "--project", args.project,
            f"--filter=config.name={service}", "--format=value(config.name)",
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or service not in (result.stdout or "").split():
        raise SystemExit(
            f"{service} is not enabled on {args.project}: impersonation needs it. Enable it "
            f"(gcloud services enable {service} --project {args.project}), or run without --no-enable-apis."
        )
    print(f"OK {service} is enabled")


def _run_with_retry(command: list[str], what: str) -> None:
    """A binding, tried again for a minute: a service account made moments ago
    is not yet a member IAM accepts everywhere."""
    for attempt in range(1, 8):
        result = subprocess.run(command, stdout=subprocess.DEVNULL, check=False)
        if result.returncode == 0:
            return
        if attempt < 7:
            print(f"{what} failed; retrying after IAM propagation delay (attempt {attempt}/7).", file=sys.stderr)
            time.sleep(10)
    raise subprocess.CalledProcessError(result.returncode, command)


def _authorize_views(project: str, dataset: str, views_dataset: str, client=None) -> None:
    """Authorize the mod's views (VIEW_NAMES) of the views dataset on the
    events dataset, so readers of the views need no access to the table.
    Other views there are left out: one could select raw columns."""
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    client = client or bigquery.Client(project=project)
    source = client.get_dataset(f"{project}.{dataset}")
    views, missing = [], []
    for name in VIEW_NAMES:
        try:
            table = client.get_table(f"{project}.{views_dataset}.{name}")
        except NotFound:
            missing.append(name)
            continue
        if table.table_type == "VIEW":
            views.append(table)
        else:
            missing.append(name)
    if missing:
        print(f"SKIPPED: no view {', '.join(missing)} in {project}.{views_dataset} (the mod's setup.sh makes them)")
    entries = list(source.access_entries)
    known = {
        (e.entity_id.get("projectId"), e.entity_id.get("datasetId"), e.entity_id.get("tableId"))
        for e in entries
        if e.entity_type == "view" and isinstance(e.entity_id, dict)
    }
    added = []
    for view in views:
        key = (view.project, view.dataset_id, view.table_id)
        if key in known:
            continue
        entries.append(
            bigquery.AccessEntry(
                None, "view", {"projectId": view.project, "datasetId": view.dataset_id, "tableId": view.table_id}
            )
        )
        added.append(view.table_id)
    if added:
        source.access_entries = entries
        client.update_dataset(source, ["access_entries"])
        print(f"AUTHORIZED on {project}.{dataset}: {', '.join(added)}")
    elif views:
        print(f"OK views already authorized on {project}.{dataset}")


# What a dataset's default access gives project roles: Viewers read every
# row (projectReaders), Editors change or delete tables and views
# (projectWriters). The groups that read the whole world are taken off too.
_EVENTS_DATASET_GROUPS = ("projectReaders", "projectWriters", "allAuthenticatedUsers")
_VIEWS_DATASET_GROUPS = ("projectWriters", "allAuthenticatedUsers")


def _restrict_dataset_access(project: str, dataset: str, views_dataset: str, client=None) -> None:
    """Take the project's Viewer and Editor groups off the events dataset, and
    Editors off the views dataset, so project roles read and change nothing
    there. Owners, named members and authorized views stay."""
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    client = client or bigquery.Client(project=project)
    for name, groups in ((dataset, _EVENTS_DATASET_GROUPS), (views_dataset, _VIEWS_DATASET_GROUPS)):
        try:
            target = client.get_dataset(f"{project}.{name}")
        except NotFound:
            print(f"SKIPPED: no dataset {project}.{name}")
            continue
        kept, removed = [], []
        for entry in target.access_entries:
            group = entry.entity_id if entry.entity_type == "specialGroup" else None
            everyone = entry.entity_type == "iamMember" and entry.entity_id in ("allUsers", "allAuthenticatedUsers")
            if group in groups or everyone:
                removed.append(f"{entry.role} {entry.entity_id}")
            else:
                kept.append(entry)
        if removed:
            target.access_entries = kept
            client.update_dataset(target, ["access_entries"])
            print(f"REMOVED from {project}.{name}: {', '.join(removed)}")
        else:
            print(f"OK {project}.{name} gives project Viewers and Editors nothing")


def _check_resources_exist(args: argparse.Namespace, need_table: bool) -> None:
    """Stop, before anything changes, unless the events dataset (and the table,
    when the plan grants on it) is there. This script never makes them: the
    mod's setup.sh does, with the mod's schema."""
    dataset = f"{args.project}:{args.dataset}"
    table = f"{dataset}.{args.table}"
    for name, kind, needed in ((dataset, "dataset", True), (table, "table", need_table)):
        if needed and not _json_output(_bq_command(args, "--format=json", "show", name), missing_ok=True):
            raise SystemExit(
                f"The events {kind} {name} is not there. Make it first with the mod's setup.sh "
                "(docs/service-account-auth.md, Set it up), and run again. Nothing was changed."
            )
    print(f"OK {args.project}.{args.dataset}{'.' + args.table if need_table else ''} is there")


def _readers_check_step(args: argparse.Namespace) -> Step:
    return Step(
        "Check who reads every row, and that no reader or token creator does",
        action=lambda: _check_project_readers(args),
        detail=(
            "gcloud projects get-iam-policy, bq show of the events dataset, bq get-iam-policy "
            f"of the table, gcloud iam roles describe of project roles other than {', '.join(PROJECT_READ_ROLES)}: "
            f"list the holders of a project role that reads tables ({', '.join(READ_PERMISSIONS)}; and, "
            f"with --keep-project-groups, {', '.join(PROJECT_GROUP_ROLES)}) and of any grant on the "
            "events dataset or table; stop on a grant to everyone, and on one that reaches a --reader "
            "or --token-creator member, unless --allow-existing-grants; and on one that may (another "
            "domain; a group or identity pool, unless gcloud identity groups memberships "
            "check-transitive-membership says the member is not in it; a role whose permissions cannot "
            "be read), unless --accept-unverified"
        ),
        needs_cli_auth=True,
    )


def _build_steps(args: argparse.Namespace) -> list[Step]:
    steps: list[Step] = []
    row_writer = args.writer_role == "row-writer"
    # The table is needed where the row-writer role is bound on it.
    table_bound = row_writer and bool(args.principal) and args.grant_iam
    if row_writer or args.reader or (args.principal and args.grant_iam):
        what = "dataset and table are" if table_bound else "dataset is"
        steps.append(
            Step(
                f"Check that the events {what} there",
                action=lambda: _check_resources_exist(args, table_bound),
                detail=(
                    "bq show of the events dataset" + (" and the events table" if table_bound else "")
                    + "; stop, before anything changes, if one is missing (the mod's setup.sh makes them)"
                ),
                needs_cli_auth=True,
            )
        )
    if args.enable_apis:
        services = list(BIGQUERY_API_SERVICES)
        if args.service_account:
            services.append("iam.googleapis.com")
        if args.token_creator:
            # Impersonation's tokens come from the IAM Service Account
            # Credentials API.
            services.append("iamcredentials.googleapis.com")
        steps.append(
            Step(
                "Enable Google Cloud APIs",
                _gcloud_command(
                    args,
                    "services",
                    "enable",
                    *services,
                    "--project",
                    args.project,
                ),
                needs_cli_auth=True,
            )
        )

    if args.service_account:
        steps.append(
            Step(
                f"Ensure service account {_service_account_email(args.project, args.service_account)}",
                action=lambda: _ensure_service_account(args),
                detail=(
                    "gcloud iam service-accounts describe; on missing, "
                    "create service account with a short retry for API/IAM propagation"
                ),
                # This action shells out to gcloud, so it depends on CLI
                # auth, not ADC.
                needs_cli_auth=True,
            )
        )

    if row_writer and args.principal and (args.grant_iam or args.token_creator):
        # Before any grant, Token Creator's included: the writer, and its role.
        role = f"projects/{args.project}/roles/{ROW_WRITER_ROLE_ID}"
        steps.append(
            Step(
                f"Check that {args.principal} holds nothing beyond the row-writer role",
                action=lambda: _check_writer_grants(args),
                detail=(
                    "gcloud projects get-iam-policy; bq show of the events and views "
                    "datasets; bq get-iam-policy of the table; Cloud Identity for a group's grant. "
                    "Stops on any other grant that reaches it, whatever the role: its own, everyone's, "
                    "its domain's, or a group's it is in, unless --allow-existing-grants; and on a "
                    "group's grant Cloud Identity does not answer for, unless --accept-unverified"
                ),
                needs_cli_auth=True,
            )
        )
        steps.append(_readers_check_step(args))
        if args.grant_iam:
            steps.extend(
                [
                    Step(
                        f"Ensure role {role} ({', '.join(ROW_WRITER_PERMISSIONS)})",
                        action=lambda: _ensure_row_writer_role(args),
                        detail=(
                            "gcloud iam roles describe; on missing, create with "
                            f"{','.join(ROW_WRITER_PERMISSIONS)}; else add any it lacks, "
                            "and stop if it holds any other"
                        ),
                        needs_cli_auth=True,
                    ),
                    Step(
                        "Grant the row-writer role to runtime principal, on the table alone",
                        _bq_command(
                            args,
                            "add-iam-policy-binding",
                            "--member",
                            args.principal,
                            "--role",
                            role,
                            f"{args.project}:{args.dataset}.{args.table}",
                        ),
                        needs_cli_auth=True,
                    ),
                ]
            )
        else:
            steps.append(
                Step(
                    f"Check that role {role}, if there is one, holds nothing beyond {', '.join(ROW_WRITER_PERMISSIONS)}",
                    action=lambda: _check_row_writer_role(args),
                    detail="gcloud iam roles describe; stops if it holds any other permission or is deleted",
                    needs_cli_auth=True,
                )
            )
    elif args.grant_iam and args.principal:
        steps.extend(
            [
                Step(
                    "Grant dataset dataEditor to runtime principal",
                    _dataset_grant_command(args, args.dataset, args.principal, "roles/bigquery.dataEditor"),
                    needs_cli_auth=True,
                ),
                Step(
                    "Grant project jobUser for verification queries",
                    _gcloud_command(
                        args,
                        "projects",
                        "add-iam-policy-binding",
                        args.project,
                        "--member",
                        args.principal,
                        "--role",
                        "roles/bigquery.jobUser",
                    ),
                    needs_cli_auth=True,
                ),
            ]
        )
    if row_writer or args.reader:
        views_dataset = _views_dataset(args)
        if not (row_writer and args.principal and (args.grant_iam or args.token_creator)):
            # Readers alone: the check still runs before any change.
            steps.append(_readers_check_step(args))
        if not args.keep_project_groups:
            steps.append(
                Step(
                    f"Take the project's Viewer and Editor groups off {args.project}.{args.dataset}, "
                    f"and Editors off {args.project}.{views_dataset}",
                    action=lambda: _restrict_dataset_access(args.project, args.dataset, views_dataset),
                    detail=(
                        "bigquery.Client: remove projectReaders and projectWriters (and allUsers, "
                        "allAuthenticatedUsers) from the events dataset's access, and projectWriters "
                        "from the views dataset's; owners, named members and authorized views stay"
                    ),
                    needs_adc=True,
                )
            )
    if args.service_account:
        email = _service_account_email(args.project, args.service_account)
        if args.token_creator and not args.enable_apis:
            steps.append(
                Step(
                    "Check that iamcredentials.googleapis.com is enabled",
                    action=lambda: _check_api_enabled(args, "iamcredentials.googleapis.com"),
                    detail="gcloud services list --enabled; impersonation's tokens come from this API",
                    needs_cli_auth=True,
                )
            )
        for member in args.token_creator:
            command = _gcloud_command(
                args, "iam", "service-accounts", "add-iam-policy-binding", email,
                "--project", args.project, "--member", member,
                "--role", "roles/iam.serviceAccountTokenCreator",
            )
            steps.append(
                Step(
                    f"Grant token creation on {email} to {member}",
                    action=lambda command=command, member=member: _run_with_retry(command, f"Token Creator for {member}"),
                    detail=_quote_command(command),
                    needs_cli_auth=True,
                )
            )

    views_dataset = _views_dataset(args)
    for member in args.reader:
        steps.extend(
            [
                Step(
                    f"Grant views dataset dataViewer to reader {member}",
                    _dataset_grant_command(args, views_dataset, member, "roles/bigquery.dataViewer"),
                    needs_cli_auth=True,
                ),
                Step(
                    f"Grant project jobUser to reader {member}, to run queries",
                    _gcloud_command(
                        args, "projects", "add-iam-policy-binding", args.project,
                        "--member", member, "--role", "roles/bigquery.jobUser",
                    ),
                    needs_cli_auth=True,
                ),
            ]
        )
    if args.reader:
        steps.append(
            Step(
                f"Authorize {', '.join(VIEW_NAMES)} of {args.project}.{views_dataset} on {args.project}.{args.dataset}",
                action=lambda: _authorize_views(args.project, args.dataset, views_dataset),
                detail=(
                    "bigquery.Client: add those two views to the events dataset's access "
                    "entries, so readers need no access to the table; no other view"
                ),
                needs_adc=True,
            )
        )
    return steps


def _summarize_stderr(stderr: str, fallback: str) -> str:
    """Compact a multi-line gcloud error to a one-line preflight summary.

    Joins the last few non-empty stderr lines with `` | `` so the
    actionable parts (`ERROR:`, `Reauthentication required`,
    `to select an already authenticated account run: gcloud config set
    account ACCOUNT`) all survive into the one-screen preflight output.
    Earlier code took only the last line, which on real gcloud failures
    discarded the part of the message that named the fix.
    """
    lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    if not lines:
        return fallback
    return " | ".join(lines[-3:])


def _run_preflight() -> _PreflightResult:
    """Capture gcloud auth + config state without raising.

    Tests both credential surfaces:

    * ADC, via ``gcloud auth application-default print-access-token``.
      What the google-cloud-bigquery Python client uses.
    * gcloud CLI auth, via ``gcloud auth list`` (active account name)
      and ``gcloud auth print-access-token`` (token actually works).
      What every ``gcloud`` and ``bq`` subprocess this script spawns
      uses.

    Each subprocess call is bounded by a short timeout so a stuck
    gcloud install can't hang the bootstrap.
    """

    def _empty_result(reason: str, gcloud_available: bool) -> _PreflightResult:
        return _PreflightResult(
            adc_ok=False,
            adc_message=reason,
            cli_auth_ok=False,
            cli_auth_account="",
            cli_auth_message=reason,
            gcloud_project="",
            gcloud_available=gcloud_available,
        )

    try:
        adc = subprocess.run(
            ["gcloud", "auth", "application-default", "print-access-token"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except FileNotFoundError:
        return _empty_result("gcloud not on PATH", False)
    except subprocess.TimeoutExpired:
        return _empty_result("gcloud timed out", True)
    adc_ok = adc.returncode == 0
    if adc_ok:
        adc_message = "ADC token reachable"
    else:
        adc_message = "not configured — " + _summarize_stderr(
            adc.stderr, fallback="no token"
        )

    cli_auth_account = ""
    cli_auth_ok = False
    cli_auth_message = "not configured"
    try:
        listed = subprocess.run(
            [
                "gcloud",
                "auth",
                "list",
                "--filter=status:ACTIVE",
                "--format=value(account)",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
        cli_auth_account = listed.stdout.strip().splitlines()[0] if listed.stdout.strip() else ""
    except (FileNotFoundError, subprocess.TimeoutExpired):
        cli_auth_account = ""
    if cli_auth_account:
        # An active account is listed. Confirm the token actually works —
        # ``gcloud auth list`` happily reports an account whose token is
        # expired or whose grant has been revoked, so the list-only check
        # alone is too optimistic. ``print-access-token`` is the cheap
        # ground-truth probe.
        try:
            tok = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                check=False,
                timeout=15,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            tok = None
            cli_auth_message = f"token probe failed: {exc}"
        else:
            if tok.returncode == 0:
                cli_auth_ok = True
                cli_auth_message = f"active account {cli_auth_account}"
            else:
                cli_auth_message = (
                    f"active account {cli_auth_account} but token probe failed — "
                    + _summarize_stderr(tok.stderr, fallback="no token")
                )
    else:
        cli_auth_message = (
            "no active account — run `gcloud auth login` "
            "or `gcloud config set account ACCOUNT`"
        )

    try:
        proj = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
        gcloud_project = proj.stdout.strip() if proj.returncode == 0 else ""
    except (FileNotFoundError, subprocess.TimeoutExpired):
        gcloud_project = ""
    if gcloud_project == "(unset)":
        gcloud_project = ""
    return _PreflightResult(
        adc_ok=adc_ok,
        adc_message=adc_message,
        cli_auth_ok=cli_auth_ok,
        cli_auth_account=cli_auth_account,
        cli_auth_message=cli_auth_message,
        gcloud_project=gcloud_project,
        gcloud_available=True,
    )


def _print_preflight(result: _PreflightResult, args: argparse.Namespace) -> None:
    print("Preflight:")
    if not result.gcloud_available:
        print(f"  gcloud: NOT AVAILABLE — {result.adc_message}")
        print()
        return
    adc_label = "OK" if result.adc_ok else "NOT CONFIGURED"
    print(f"  ADC: {adc_label} — {result.adc_message}")
    cli_label = "OK" if result.cli_auth_ok else "NOT CONFIGURED"
    print(f"  gcloud CLI auth: {cli_label} — {result.cli_auth_message}")
    if result.gcloud_project:
        if result.gcloud_project == args.project:
            note = "matches --project"
        else:
            note = (
                f"differs from --project ({args.project}); "
                "subcommands pass --project explicitly so this is OK, "
                "but cross-check before --execute"
            )
        print(f"  gcloud config project: {result.gcloud_project} ({note})")
    else:
        print("  gcloud config project: (unset)")
    print()


def _bigquery_client_importable() -> bool:
    try:
        import google.cloud.bigquery  # noqa: F401
    except ImportError:
        return False
    return True


def _steps_need_adc(steps: list[Step]) -> bool:
    """Whether any step in the plan needs Application Default Credentials."""
    return any(step.needs_adc for step in steps)


def _steps_need_cli_auth(steps: list[Step]) -> bool:
    """Whether any step in the plan needs the gcloud CLI account."""
    return any(step.needs_cli_auth for step in steps)


def _print_plan(args: argparse.Namespace, steps: list[Step]) -> None:
    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"BigQuery Agent Analytics access setup ({mode})")
    print(f"project:  {args.project}")
    print(f"dataset:  {args.dataset}")
    print(f"table:    {args.table}")
    print(f"location: {args.location or '(client default)'}")
    principal = args.principal or "(IAM grants skipped)"
    print(f"principal: {principal}")
    if args.principal and args.grant_iam:
        print(f"writer role: {args.writer_role}")
    print()
    if args.grant_iam and not args.principal:
        print("IAM grants are skipped because --principal was not provided.")
        print("Pass --principal serviceAccount:name@project.iam.gserviceaccount.com")
        print("or --principal user:name@example.com to configure runtime IAM.")
        print()
    for index, step in enumerate(steps, start=1):
        print(f"{index}. {step.label}")
        if step.command:
            print(f"   {_quote_command(step.command)}")
        elif step.action:
            print(f"   python action: {step.detail or 'run setup action'}")
    print()


def run(args: argparse.Namespace) -> int:
    if args.service_account and not args.principal:
        email = _service_account_email(args.project, args.service_account)
        args.principal = f"serviceAccount:{email}"
    _require_project_dataset(args)
    steps = _build_steps(args)
    if args.execute:
        preflight = _run_preflight()
        _print_preflight(preflight, args)
        if not preflight.gcloud_available:
            raise SystemExit(
                "gcloud is required for --execute but is not available. "
                "Install the Google Cloud SDK and re-run."
            )
        if _steps_need_adc(steps) and not preflight.adc_ok:
            raise SystemExit(
                "Application Default Credentials are not configured but "
                "this plan calls google-cloud-bigquery directly. "
                "Run: gcloud auth application-default login "
                "(or set GOOGLE_APPLICATION_CREDENTIALS to a service-"
                "account JSON key) and re-run."
            )
        if _steps_need_adc(steps) and not _bigquery_client_importable():
            raise SystemExit(
                "This plan calls the Python BigQuery client (google-cloud-bigquery), which this Python "
                "cannot import. Install it, for example python3 -m venv .venv && .venv/bin/pip install "
                "google-cloud-bigquery, run the script with .venv/bin/python, and run again. Nothing "
                "was changed."
            )
        if _steps_need_cli_auth(steps) and not preflight.cli_auth_ok:
            raise SystemExit(
                "gcloud CLI auth is not configured but this plan shells "
                "out to gcloud / bq. "
                "Run: gcloud auth login (and `gcloud config set account "
                "ACCOUNT` if multiple identities are listed) and re-run. "
                "ADC alone is not enough: gcloud subcommands ignore ADC "
                "and use the active CLI account."
            )
    _print_plan(args, steps)
    if not args.execute:
        print("No changes made, and nothing called: the plan came from the arguments alone.")
        print("Re-run with --execute to apply it; the credentials are checked then.")
        return 0
    for step in steps:
        print(f"==> {step.label}")
        if step.command:
            subprocess.run(step.command, check=True)
        elif step.action:
            step.action()
    print("Access setup completed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Set up who writes to and who reads the mod's BigQuery table: a writer service "
            "account, who may impersonate it, and the readers of the views. A dry run unless "
            "--execute is given."
        )
    )
    parser.add_argument(
        "--project",
        default=_default_project(),
        help="Google Cloud project ID (default: BQAA_PROJECT_ID, GCP_PROJECT_ID, or GOOGLE_CLOUD_PROJECT).",
    )
    parser.add_argument(
        "--dataset",
        default=_default_dataset(),
        help="BigQuery dataset for the events table (default: BQAA_DATASET, BQ_DATASET, or agent_analytics).",
    )
    parser.add_argument(
        "--table",
        default=_default_table(),
        help="BigQuery events table name (default: BQAA_TABLE, BQ_TABLE, or agent_events).",
    )
    parser.add_argument(
        "--location",
        default=os.environ.get("BQAA_LOCATION") or "US",
        help="BigQuery dataset location (default: BQAA_LOCATION or US).",
    )
    parser.add_argument(
        "--principal",
        help=(
            "Runtime IAM member to grant, e.g. "
            "serviceAccount:bqaa-writer@PROJECT.iam.gserviceaccount.com or "
            "user:person@example.com"
        ),
    )
    parser.add_argument(
        "--service-account",
        metavar="NAME_OR_EMAIL",
        help=(
            "Create this service account if missing and grant it runtime IAM. "
            "Use an account id such as bqaa-writer or a full service account email."
        ),
    )
    parser.add_argument(
        "--writer-role",
        choices=("data-editor", "row-writer"),
        default="data-editor",
        help=(
            "What the runtime principal gets. data-editor (default): dataset "
            "dataEditor and project jobUser, which also read every row. "
            "row-writer: a custom role with "
            "bigquery.tables.updateData and bigquery.tables.get on the table "
            "alone (inserts only: no reads, queries or deletes)."
        ),
    )
    parser.add_argument(
        "--token-creator",
        action="append",
        default=[],
        metavar="MEMBER",
        help=(
            "Grant roles/iam.serviceAccountTokenCreator on --service-account to "
            "this member (best a group:), so it can impersonate the writer. Repeatable."
        ),
    )
    parser.add_argument(
        "--reader",
        action="append",
        default=[],
        metavar="MEMBER",
        help=(
            "Grant this member (best a group:) dataViewer on the views dataset and "
            "project jobUser, and authorize the views on the events dataset, so "
            "readers need no access to the table. Repeatable."
        ),
    )
    parser.add_argument(
        "--allow-existing-grants",
        action="store_true",
        help=(
            "Keep a writer that already holds other grants on the project, the "
            "datasets or the table (an audited account), and grants that reach "
            "readers or token creators, or everyone. It does not accept what "
            "could not be ruled out (see --accept-unverified). Without it, setup "
            "stops before granting anything."
        ),
    )
    parser.add_argument(
        "--accept-unverified",
        action="store_true",
        help=(
            "Accept grants that may reach the writer, a reader or a token creator "
            "but could not be ruled out: a group or identity pool whose members "
            "Cloud Identity does not answer for (its membership check answers only "
            "for Google Workspace Enterprise and Cloud Identity Premium accounts), "
            "another domain's grant, or a role whose permissions could not be read. "
            "Grants known to reach further still stop setup unless "
            "--allow-existing-grants is also given."
        ),
    )
    parser.add_argument(
        "--keep-project-groups",
        action="store_true",
        help=(
            "Leave the project's Viewer and Editor groups on the events dataset (and "
            "Editors on the views dataset). Without it, --writer-role row-writer and "
            "--reader take them off, so project roles read and change no rows."
        ),
    )
    parser.add_argument(
        "--views-dataset",
        default=os.environ.get("BQAA_VIEWS_DATASET") or "",
        help="The views dataset (default: <dataset>_reporting).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="Apply the printed plan.")
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan and change nothing (the default).",
    )
    parser.add_argument(
        "--no-enable-apis",
        dest="enable_apis",
        action="store_false",
        help="Do not enable BigQuery APIs.",
    )
    parser.add_argument(
        "--no-create-dataset",
        action="store_true",
        help="Accepted, and changes nothing: this script never makes the dataset (setup.sh does).",
    )
    parser.add_argument(
        "--no-create-table",
        action="store_true",
        help="Accepted, and changes nothing: this script never makes the table (setup.sh does).",
    )
    parser.add_argument(
        "--no-grant-iam",
        dest="grant_iam",
        action="store_false",
        help="Do not grant runtime IAM roles.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help=(
            "Run without interactive prompts. Adds --quiet to every gcloud "
            "and bq subcommand so a script or CI job can run --execute "
            "unattended without a confirmation hanging the setup."
        ),
    )
    parser.set_defaults(
        enable_apis=True,
        grant_iam=True,
        non_interactive=False,
    )
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
