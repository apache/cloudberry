# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Writing extraction results back into a Cloudberry table

describe_result_table reports whether a table can take results and how;
write_results writes one batch.  Both refuse any table that is not on the
MCP_WRITE_TABLES whitelist before they look it up, and every name that
reaches statement text is one the server read back from the catalog.
"""

import asyncio
import json
import logging
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

from .config import WriteConfig
from .security import SQLValidator

logger = logging.getLogger(__name__)

UPSERT = "upsert"
APPEND = "append"
MODES = (UPSERT, APPEND)

# At most this many failed rows are itemised in a result.
MAX_FAILED_DETAILS = 100

# SQLSTATE classes that belong to one row's data: data exceptions and
# integrity constraint violations.  Any other error ends the whole call.
ROW_ERROR_CLASSES = ("22", "23")

# Columns the tool fills itself when the table has them.
MODEL_COLUMN = "model"
EXTRACTED_AT_COLUMN = "extracted_at"
MODEL_TYPES = ("text", "varchar")
EXTRACTED_AT_TYPES = ("timestamptz", "timestamp", "date")


class WriteError(ValueError):
    """A request write_results refuses as a whole; nothing was written."""


class DatabaseUnreachable(Exception):
    """Opening a connection failed, before any statement of the tool ran."""


class CommitOutcomeUnknown(WriteError):
    """The connection failed while committing: the batch may have been written."""


# SQLSTATEs about reaching the database rather than about a statement:
# connection exceptions (08), a rejected role (28), an unknown database
# (3D000), too many connections (53300) and a server shutting down (57P).
# Their messages can name the configured role or database, so they are never
# passed on.  A statement timeout or cancel (57014) is not among them.
CONNECTION_ERROR_PREFIXES = ("08", "28", "3D000", "53300", "57P")


def sqlstate(error: BaseException) -> str:
    return getattr(error, "sqlstate", None) or ""


def is_connection_error(error: BaseException) -> bool:
    if isinstance(error, DatabaseUnreachable):
        return True
    if isinstance(error, asyncpg.PostgresError):
        return sqlstate(error).startswith(CONNECTION_ERROR_PREFIXES)
    return not isinstance(error, (asyncio.TimeoutError, TimeoutError))


def public_error(error: Exception, describe_error) -> str:
    """The text of an unexpected error that is safe to hand to the caller.

    A message from the server about the statement and its data is passed on,
    and a timeout says which setting fired.  Anything about reaching the
    database - a refused or dropped connection, a socket or TLS error, a
    rejected role or an unknown database - can name the host, port, user or
    database, so the caller gets a fixed message and the details go to the
    log.
    """
    if is_connection_error(error):
        cause = error.__cause__ if isinstance(error, DatabaseUnreachable) else error
        logger.error(f"write-back database error: {cause!r}")
        return (
            "the database could not be reached or the connection was lost; "
            "see the server log"
        )
    return describe_error(error)


def quote(name: str) -> str:
    return SQLValidator.quote_identifier(name)


@dataclass
class Column:
    attnum: int
    name: str
    type_sql: str            # format_type with the typmod: the declared type
    cast_type_sql: str       # format_type without it; INSERT applies the typmod
    not_null: bool
    has_default: bool
    generated: bool
    identity: bool           # any identity column; it supplies its own value
    identity_always: bool
    collation_sql: Optional[str]
    kind: str                # scalar, json or array
    base_type: str
    array_delimiter: Optional[str]
    insert_privilege: bool
    update_privilege: bool
    select_privilege: bool

    @property
    def writable(self) -> bool:
        return not self.generated and not self.identity_always

    @property
    def not_writable_reason(self) -> Optional[str]:
        if self.generated:
            return "generated column"
        if self.identity_always:
            return "GENERATED ALWAYS identity column"
        return None

    @property
    def required(self) -> bool:
        """A NOT NULL column that nothing fills when a row leaves it out."""
        return (
            self.not_null and not self.has_default and not self.identity
            and not self.generated
        )


@dataclass
class UniqueIndex:
    name: str
    column: Optional[str]
    opclass_sql: str
    reasons: List[str]

    @property
    def usable(self) -> bool:
        return not self.reasons


@dataclass
class TableInfo:
    relid: int
    schema: str
    table: str
    relkind: str
    columns: List[Column]
    unique_indexes: List[UniqueIndex]
    triggers: List[Dict[str, Any]]
    distribution_policy: Optional[str]
    distribution_key: List[str]
    table_privileges: Dict[str, bool]
    # Reasons that rule the table out whatever the mode.
    problems: List[str] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        return SQLValidator.quote_qualified_name(self.schema, self.table)

    @property
    def partitioned(self) -> bool:
        return self.relkind == "p"

    def column(self, name: str) -> Optional[Column]:
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def upsert_index(self, column_name: str) -> Optional[UniqueIndex]:
        """The first usable unique index on exactly this column."""
        for index in self.unique_indexes:
            if index.usable and index.column == column_name:
                return index
        return None

    @property
    def upsert_keys(self) -> List[str]:
        keys = []
        for index in self.unique_indexes:
            if index.usable and index.column not in keys:
                keys.append(index.column)
        return keys

    def lock_mode(self, mode: str) -> str:
        return lock_mode(mode, self.partitioned)

    def missing_table_privilege(self, mode: str) -> Optional[str]:
        return missing_table_privilege(self.table_privileges, mode, self.partitioned)

    def mode_reasons(self, mode: str) -> List[str]:
        reasons = list(self.problems)
        missing = self.missing_table_privilege(mode)
        if missing:
            reasons.append(f"the current role lacks {missing}")
        if mode == UPSERT and not self.upsert_keys:
            reasons.append(
                "no column has a usable single-column unique index or primary key "
                "to upsert on"
            )
        return reasons

    def describe(self) -> Dict[str, Any]:
        model = self.column(MODEL_COLUMN)
        extracted_at = self.column(EXTRACTED_AT_COLUMN)
        reasons = {mode: self.mode_reasons(mode) for mode in MODES}
        return {
            "schema": self.schema,
            "table": self.table,
            "partitioned": self.partitioned,
            "writable": {mode: not reasons[mode] for mode in MODES},
            "reasons": reasons,
            "upsert_keys": self.upsert_keys,
            "unique_indexes": [
                {
                    "name": index.name,
                    "column": index.column,
                    "usable": index.usable,
                    "reasons": index.reasons,
                }
                for index in self.unique_indexes
            ],
            "columns": [
                {
                    "name": column.name,
                    "type": column.type_sql,
                    "nullable": not column.not_null,
                    "has_default": column.has_default,
                    "writable": column.writable,
                    "not_writable_reason": column.not_writable_reason,
                    "insert_privilege": column.insert_privilege,
                    "update_privilege": column.update_privilege,
                    "select_privilege": column.select_privilege,
                }
                for column in self.columns
            ],
            "provenance": {
                MODEL_COLUMN: model is not None,
                EXTRACTED_AT_COLUMN: extracted_at is not None,
            },
            "triggers": self.triggers,
            "distribution_policy": self.distribution_policy,
            "distribution_key": self.distribution_key,
        }


def lock_mode(mode: str, partitioned: bool) -> str:
    """The lock write_results takes before it checks anything else.

    upsert takes what Cloudberry takes for it anyway when the global
    deadlock detector is off, so the lock never has to be upgraded midway.
    An append on a partitioned table needs SHARE UPDATE EXCLUSIVE to keep a
    concurrent ATTACH PARTITION out; on a plain table ROW EXCLUSIVE already
    conflicts with changing its access method.
    """
    if mode == UPSERT:
        return "EXCLUSIVE"
    return "SHARE UPDATE EXCLUSIVE" if partitioned else "ROW EXCLUSIVE"


def missing_table_privilege(
    privileges: Dict[str, bool], mode: str, partitioned: bool
) -> Optional[str]:
    """What the account lacks to take the lock for this mode, if anything.

    LOCK TABLE checks table-level privileges only, and only on the table it
    names, not its partitions: UPDATE, DELETE or TRUNCATE allow any lock
    mode, and INSERT allows ROW EXCLUSIVE and weaker.  Column-level grants
    do not count.
    """
    if mode == UPSERT:
        if not privileges["UPDATE"]:
            return "table-level UPDATE privilege, which upsert needs to lock the table"
        return None
    lock_any = privileges["UPDATE"] or privileges["DELETE"] or privileges["TRUNCATE"]
    if partitioned:
        if not lock_any:
            return (
                "table-level UPDATE, DELETE or TRUNCATE privilege, which append "
                "on a partitioned table needs to lock it in SHARE UPDATE "
                "EXCLUSIVE mode; INSERT alone is not enough"
            )
        return None
    if not (privileges["INSERT"] or lock_any):
        return "table-level INSERT privilege, which append needs to lock the table"
    return None


async def read_table_privileges(conn, relid: int) -> Dict[str, bool]:
    return dict(await conn.fetchrow(
        "SELECT has_table_privilege($1::oid, 'INSERT') AS \"INSERT\", "
        "has_table_privilege($1::oid, 'UPDATE') AS \"UPDATE\", "
        "has_table_privilege($1::oid, 'DELETE') AS \"DELETE\", "
        "has_table_privilege($1::oid, 'TRUNCATE') AS \"TRUNCATE\"",
        relid,
    ))


async def resolve_relation(conn, schema: str, table: str) -> Optional[Dict[str, Any]]:
    """Find a relation by its exact catalog names, without locking it."""
    return await conn.fetchrow(
        "SELECT c.oid AS relid, c.relkind::text AS relkind, n.nspname, c.relname "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = $2",
        schema, table,
    )


def check_relkind(schema: str, table: str, relkind: str) -> None:
    if relkind not in ("r", "p"):
        kinds = {
            "v": "a view", "m": "a materialized view", "f": "a foreign table",
            "S": "a sequence", "i": "an index", "I": "a partitioned index",
            "c": "a composite type", "t": "a TOAST table",
        }
        raise WriteError(
            f'"{schema}"."{table}" is {kinds.get(relkind, f"a relation of kind {relkind!r}")}; '
            f"results can only be written to a heap table"
        )


async def inspect_table(conn, relid: int, schema: str, table: str, relkind: str) -> TableInfo:
    """Read everything both tools need to know about a table from the catalog."""
    problems: List[str] = []
    check_relkind(schema, table, relkind)

    # The table and, when it is partitioned, every partition below it: rows
    # are routed to a leaf, and the executor then uses that leaf's own
    # storage, constraints and triggers.
    # pg_partition_tree() returns no rows at all for a table that is not
    # partitioned, so a plain table stands for itself.
    members = await conn.fetch(
        "SELECT t.relid, t.isleaf, c.relkind::text AS relkind, a.amname, "
        "quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name "
        "FROM ("
        "  SELECT relid::oid AS relid, isleaf, level FROM pg_partition_tree($1::oid::regclass) "
        "  WHERE $2 = 'p' "
        "  UNION ALL SELECT $1::oid, true, 0 WHERE $2 <> 'p'"
        ") t "
        "JOIN pg_class c ON c.oid = t.relid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN pg_am a ON a.oid = c.relam "
        "ORDER BY t.level, c.relname",
        relid, relkind,
    )
    if not members:
        raise WriteError(f'"{schema}"."{table}" was dropped while it was being inspected')
    member_oids = [member["relid"] for member in members]
    for member in members:
        if not member["isleaf"]:
            continue
        if member["relkind"] != "r" or member["amname"] != "heap":
            storage = member["amname"] or {"f": "foreign table"}.get(member["relkind"], member["relkind"])
            what = "the table" if member["relid"] == relid else f"partition {member['name']}"
            problems.append(f"{what} is not a heap table (it uses {storage})")

    deferrable = await conn.fetch(
        "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name, "
        "x.what, x.objname FROM ("
        "  SELECT conrelid AS relid, 'constraint' AS what, conname AS objname "
        "  FROM pg_constraint WHERE conrelid = ANY($1::oid[]) AND condeferrable "
        "  UNION ALL "
        "  SELECT tgrelid, 'constraint trigger', tgname "
        "  FROM pg_trigger WHERE tgrelid = ANY($1::oid[]) AND tgdeferrable"
        ") x JOIN pg_class c ON c.oid = x.relid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace ORDER BY 1, 3",
        member_oids,
    )
    for row in deferrable:
        problems.append(
            f"{row['name']} has deferrable {row['what']} {row['objname']}, which would "
            f"only fail at commit, after the rows were reported written"
        )

    # Any trigger on INSERT or UPDATE can leave the table holding something
    # other than what the tool wrote and reports: a BEFORE ROW trigger can
    # rewrite any column or return NULL and drop the row without an error,
    # and with allow_segment_DML or gp_enable_statement_trigger, both settable
    # by any user, an AFTER or statement trigger can delete or change the rows
    # just written.  Internal triggers are left alone.  tgtype bits: 4 insert,
    # 16 update.
    triggers = await conn.fetch(
        "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name, t.tgname "
        "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE t.tgrelid = ANY($1::oid[]) AND NOT t.tgisinternal AND t.tgenabled <> 'D' "
        "AND t.tgtype & 20 <> 0 ORDER BY 1, 2",
        member_oids,
    )
    for row in triggers:
        problems.append(
            f"{row['name']} has trigger {row['tgname']} on INSERT or UPDATE, "
            f"which can change, drop or delete the rows written; disable or drop it, "
            f"or write to another table"
        )

    # A rule ON INSERT or ON UPDATE rewrites the statement itself: DO INSTEAD
    # NOTHING makes an INSERT, even one with ON CONFLICT, report zero rows
    # and drop them without an error.  ev_type: 2 update, 3 insert.
    rules = await conn.fetch(
        "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name, r.rulename, "
        "CASE r.ev_type WHEN '3' THEN 'INSERT' ELSE 'UPDATE' END AS event "
        "FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE r.ev_class = ANY($1::oid[]) AND r.ev_type IN ('2', '3') ORDER BY 1, 2",
        member_oids,
    )
    for row in rules:
        problems.append(
            f"{row['name']} has rule {row['rulename']} ON {row['event']}, which can rewrite "
            f"or silently drop the rows written; drop it, or write to another table"
        )

    triggers = await conn.fetch(
        "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS table_name, "
        "t.tgname, t.tgenabled <> 'D' AS enabled "
        "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE t.tgrelid = ANY($1::oid[]) AND NOT t.tgisinternal ORDER BY 1, 2",
        member_oids,
    )

    records = await conn.fetch(
        "WITH RECURSIVE base(typid, baseid) AS ("
        "  SELECT a.atttypid, a.atttypid FROM pg_attribute a "
        "  WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped "
        "  UNION "
        "  SELECT b.typid, t.typbasetype FROM base b JOIN pg_type t ON t.oid = b.baseid "
        "  WHERE t.typtype = 'd'"
        ") "
        "SELECT a.attnum, a.attname, "
        "format_type(a.atttypid, a.atttypmod) AS type_sql, "
        "format_type(a.atttypid, NULL) AS cast_type_sql, "
        "a.attnotnull, a.atthasdef, a.attgenerated <> '' AS generated, "
        "a.attidentity <> '' AS identity, a.attidentity = 'a' AS identity_always, "
        "(SELECT quote_ident(cn.nspname) || '.' || quote_ident(co.collname) "
        " FROM pg_collation co JOIN pg_namespace cn ON cn.oid = co.collnamespace "
        " WHERE co.oid = a.attcollation) AS collation_sql, "
        "bt.typname AS base_type, bt.typcategory = 'A' AND bt.typelem <> 0 AS is_array, "
        "et.typdelim::text AS array_delimiter, "
        "has_column_privilege($1::oid, a.attnum, 'INSERT') AS insert_privilege, "
        "has_column_privilege($1::oid, a.attnum, 'UPDATE') AS update_privilege, "
        "has_column_privilege($1::oid, a.attnum, 'SELECT') AS select_privilege "
        "FROM pg_attribute a "
        "JOIN base b ON b.typid = a.atttypid "
        "JOIN pg_type bt ON bt.oid = b.baseid AND bt.typtype <> 'd' "
        "LEFT JOIN pg_type et ON et.oid = bt.typelem "
        "WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        relid,
    )
    columns = []
    for record in records:
        if record["is_array"]:
            kind = "array"
        elif record["base_type"] in ("json", "jsonb"):
            kind = "json"
        else:
            kind = "scalar"
        columns.append(Column(
            attnum=record["attnum"],
            name=record["attname"],
            type_sql=record["type_sql"],
            cast_type_sql=record["cast_type_sql"],
            not_null=record["attnotnull"],
            has_default=record["atthasdef"],
            generated=record["generated"],
            identity=record["identity"],
            identity_always=record["identity_always"],
            collation_sql=record["collation_sql"],
            kind=kind,
            base_type=record["base_type"],
            array_delimiter=record["array_delimiter"] if record["is_array"] else None,
            insert_privilege=record["insert_privilege"],
            update_privilege=record["update_privilege"],
            select_privilege=record["select_privilege"],
        ))
    by_attnum = {column.attnum: column for column in columns}

    # The declared type, not the base type, decides for the provenance
    # columns, except that model may be a domain over text or varchar.
    model = next((c for c in columns if c.name == MODEL_COLUMN), None)
    if model is not None:
        if not model.writable or model.kind != "scalar" or model.base_type not in MODEL_TYPES:
            problems.append(
                f"column {MODEL_COLUMN} must be a writable text or varchar column "
                f"(or a domain over one), got {model.type_sql}"
                + (f", {model.not_writable_reason}" if not model.writable else "")
            )
    extracted_at = next((c for c in columns if c.name == EXTRACTED_AT_COLUMN), None)
    if extracted_at is not None:
        declared = await conn.fetchval(
            "SELECT t.typname FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid "
            "WHERE a.attrelid = $1 AND a.attnum = $2",
            relid, extracted_at.attnum,
        )
        if not extracted_at.writable or declared not in EXTRACTED_AT_TYPES:
            problems.append(
                f"column {EXTRACTED_AT_COLUMN} must be a writable timestamptz, timestamp "
                f"or date column, got {extracted_at.type_sql}"
                + (f", {extracted_at.not_writable_reason}" if not extracted_at.writable else "")
            )

    index_records = await conn.fetch(
        "SELECT ic.relname AS index_name, i.indimmediate, i.indisvalid, i.indisready, "
        "i.indnkeyatts, i.indkey[0] AS attnum, i.indexprs IS NOT NULL AS has_expressions, "
        "i.indpred IS NOT NULL AS partial, opc.opcdefault, "
        "quote_ident(opn.nspname) || '.' || quote_ident(opc.opcname) AS opclass_sql, "
        "i.indcollation[0] AS collation, a.attcollation "
        "FROM pg_index i "
        "JOIN pg_class ic ON ic.oid = i.indexrelid "
        "JOIN pg_opclass opc ON opc.oid = i.indclass[0] "
        "JOIN pg_namespace opn ON opn.oid = opc.opcnamespace "
        "LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] "
        "WHERE i.indrelid = $1 AND i.indisunique ORDER BY ic.relname",
        relid,
    )
    unique_indexes = []
    for record in index_records:
        reasons = []
        if not record["indimmediate"]:
            reasons.append("DEFERRABLE, so ON CONFLICT cannot use it")
        if not record["indisvalid"] or not record["indisready"]:
            reasons.append("not valid or not ready")
        if record["indnkeyatts"] != 1:
            reasons.append("has more than one key column")
        if record["has_expressions"] or record["attnum"] == 0:
            reasons.append("is an expression index")
        if record["partial"]:
            reasons.append("is a partial index")
        if not record["opcdefault"]:
            reasons.append(f"uses the non-default operator class {record['opclass_sql']}")
        if record["attcollation"] is not None and record["collation"] != record["attcollation"]:
            reasons.append("uses a collation other than the column's")
        column = by_attnum.get(record["attnum"]) if record["indnkeyatts"] == 1 else None
        unique_indexes.append(UniqueIndex(
            name=record["index_name"],
            column=column.name if column is not None else None,
            opclass_sql=record["opclass_sql"],
            reasons=reasons,
        ))

    table_privileges = await read_table_privileges(conn, relid)

    distribution_policy, distribution_key = None, []
    if await conn.fetchval("SELECT to_regclass('pg_catalog.gp_distribution_policy') IS NOT NULL"):
        policy = await conn.fetchrow(
            "SELECT p.policytype::text AS policytype, ARRAY(SELECT a.attname FROM unnest(p.distkey::int2[]) "
            "WITH ORDINALITY AS k(attnum, position) JOIN pg_attribute a "
            "ON a.attrelid = p.localoid AND a.attnum = k.attnum ORDER BY k.position) AS key "
            "FROM gp_distribution_policy p WHERE p.localoid = $1",
            relid,
        )
        if policy is not None:
            distribution_key = list(policy["key"])
            if policy["policytype"] == "r":
                distribution_policy = "replicated"
            else:
                distribution_policy = "hash" if distribution_key else "random"

    return TableInfo(
        relid=relid,
        schema=schema,
        table=table,
        relkind=relkind,
        columns=columns,
        unique_indexes=unique_indexes,
        triggers=[
            {"table": row["table_name"], "name": row["tgname"], "enabled": row["enabled"]}
            for row in triggers
        ],
        distribution_policy=distribution_policy,
        distribution_key=distribution_key,
        table_privileges=table_privileges,
        problems=problems,
    )


class ResultWriter:
    """The two write-back tools, over the server's connection pool."""

    def __init__(self, db_manager, config: WriteConfig):
        self.db_manager = db_manager
        self.config = config

    def _check_whitelist(self, schema: str, table: str) -> None:
        # Checked on the names as given, before any lookup: the tool
        # parameters are exact catalog names, so a table outside the list is
        # neither described nor locked, and whether it exists is not revealed.
        if not isinstance(schema, str) or not isinstance(table, str) or not schema or not table:
            raise WriteError("schema and table must be non-empty names")
        if not self.config.allows(schema, table):
            raise WriteError(
                f'"{schema}"."{table}" is not in MCP_WRITE_TABLES. Names are matched '
                f"exactly as stored in the catalog: case-sensitive, no quotes, and "
                f"search_path does not apply"
            )

    @asynccontextmanager
    async def _connection(self):
        """A pooled connection; any failure to open one hides its details.

        While a connection is being opened the server can refuse it for many
        reasons - an unknown role or database, no CONNECT privilege (42501), a
        database that allows no connections (55000) - and the messages name
        the configured role or database.  Whatever the error, it is reported
        as unreachable; only errors of statements on an open connection are
        passed on.

        Handing the connection back never changes the outcome: asyncpg resets
        a connection on release and raises when that fails, by which time a
        write may have committed.  Such a failure is logged and the connection
        dropped, and the call keeps its own result or error.
        """
        context = self.db_manager.get_connection()
        try:
            conn = await context.__aenter__()
        except Exception as e:
            raise DatabaseUnreachable() from e
        try:
            yield conn
        except BaseException as e:
            try:
                suppressed = await context.__aexit__(type(e), e, e.__traceback__)
            except Exception as release_error:
                logger.error(f"releasing a write-back connection failed: {release_error!r}")
                suppressed = False
            if not suppressed:
                raise
        else:
            try:
                await context.__aexit__(None, None, None)
            except Exception as release_error:
                logger.error(f"releasing a write-back connection failed: {release_error!r}")

    async def describe(self, schema: str, table: str) -> Dict[str, Any]:
        self._check_whitelist(schema, table)
        async with self._connection() as conn:
            async with conn.transaction(readonly=True):
                relation = await resolve_relation(conn, schema, table)
                if relation is None:
                    raise WriteError(f'table "{schema}"."{table}" does not exist')
                # The names read back from the catalog, not the arguments,
                # are what any statement text is built from.
                info = await inspect_table(
                    conn, relation["relid"], relation["nspname"], relation["relname"],
                    relation["relkind"],
                )
                return info.describe()

    async def write(
        self,
        schema: str,
        table: str,
        source_id_column: str,
        fields: Sequence[str],
        rows: Sequence[Any],
        mode: str = UPSERT,
        field_mapping: Optional[Dict[str, str]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        if mode not in MODES:
            raise WriteError(f"mode must be 'upsert' or 'append', got {mode!r}")
        self._check_whitelist(schema, table)
        self._check_input_shape(fields, rows, field_mapping, model)

        try:
            async with self._connection() as conn:
                transaction = conn.transaction()
                await transaction.start()
                try:
                    result = await self._write_locked(
                        conn, schema, table, source_id_column, list(fields), rows,
                        mode, field_mapping or {}, model,
                    )
                except BaseException:
                    try:
                        await transaction.rollback()
                    except Exception as e:
                        logger.debug(f"rollback after a failed write also failed: {e!r}")
                    raise
                await self._commit(transaction)
                return result
        except asyncpg.exceptions.LockNotAvailableError as e:
            raise WriteError(
                f'gave up waiting for a lock on "{schema}"."{table}" after '
                f"MCP_WRITE_LOCK_TIMEOUT ({self.config.lock_timeout:g} s): another "
                f"session holds a conflicting lock. Nothing was written. ({e})"
            ) from e

    @staticmethod
    async def _commit(transaction) -> None:
        """Commit, telling a refused commit from one whose outcome is unknown.

        When the server answers, it either committed or it did not.  When the
        connection fails instead, the commit may have happened, and saying
        that nothing was written would invite a retry that, for append,
        writes every row twice.
        """
        try:
            await transaction.commit()
        except asyncpg.PostgresError as e:
            # asyncpg reports a broken connection (class 08) and a server
            # going away (57P) as PostgresErrors too; only an answer about the
            # transaction itself is final.
            if not sqlstate(e).startswith(("08", "57P")):
                raise WriteError(f"the commit failed and nothing was written: {e}") from e
            logger.error(f"write-back commit outcome unknown: {e!r}")
            raise CommitOutcomeUnknown(
                "the connection failed while committing, so the batch may or may not "
                "have been written; check the table before retrying, since retrying "
                "an append can insert the rows twice"
            ) from e
        except Exception as e:
            logger.error(f"write-back commit outcome unknown: {e!r}")
            raise CommitOutcomeUnknown(
                "the connection failed while committing, so the batch may or may not "
                "have been written; check the table before retrying, since retrying "
                "an append can insert the rows twice"
            ) from e

    def _check_input_shape(self, fields, rows, field_mapping, model) -> None:
        if not isinstance(fields, (list, tuple)) or not fields:
            raise WriteError("fields must be a non-empty list of field names")
        for name in fields:
            if not isinstance(name, str) or not name:
                raise WriteError(f"fields must hold non-empty strings, got {name!r}")
        duplicates = sorted({name for name in fields if list(fields).count(name) > 1})
        if duplicates:
            raise WriteError(f"fields lists {', '.join(map(repr, duplicates))} more than once")
        if not isinstance(rows, (list, tuple)) or not rows:
            raise WriteError("rows must be a non-empty list of objects")
        if len(rows) > self.config.max_rows:
            raise WriteError(
                f"{len(rows)} rows is more than MCP_WRITE_MAX_ROWS ({self.config.max_rows}); "
                f"split the batch"
            )
        if field_mapping is not None and not isinstance(field_mapping, dict):
            raise WriteError("field_mapping must be an object from field name to column name")
        if model is not None and not isinstance(model, str):
            raise WriteError(f"model must be a string, got {type(model).__name__}")
        # The whole call, as the client sent it, is what the limit bounds.
        size = len(json.dumps(
            {"fields": fields, "rows": rows, "field_mapping": field_mapping, "model": model},
            ensure_ascii=False, default=str,
        ).encode("utf-8"))
        if size > self.config.max_bytes:
            raise WriteError(
                f"the call is {size} bytes as JSON, more than MCP_WRITE_MAX_BYTES "
                f"({self.config.max_bytes}); split the batch"
            )

    async def _write_locked(
        self, conn, schema, table, source_id_column, fields, rows, mode, field_mapping, model,
    ) -> Dict[str, Any]:
        # SET LOCAL: the setting ends with this transaction.  It bounds every
        # lock wait below, the explicit LOCK TABLE first of all.
        await conn.execute(
            "SELECT set_config('lock_timeout', $1, true)",
            f"{max(1, int(self.config.lock_timeout * 1000))}ms",
        )

        # Find the table without locking it, check what LOCK TABLE will need,
        # lock it, and make sure the name still means the same table.  Only
        # then is it inspected, so nothing checked can change before commit.
        relation = await resolve_relation(conn, schema, table)
        if relation is None:
            raise WriteError(f'table "{schema}"."{table}" does not exist')
        check_relkind(schema, table, relation["relkind"])
        partitioned = relation["relkind"] == "p"
        missing = missing_table_privilege(
            await read_table_privileges(conn, relation["relid"]), mode, partitioned
        )
        if missing:
            raise WriteError(f'cannot {mode} into "{schema}"."{table}": the current role lacks {missing}')
        await conn.execute(
            f"LOCK TABLE {SQLValidator.quote_qualified_name(relation['nspname'], relation['relname'])} "
            f"IN {lock_mode(mode, partitioned)} MODE"
        )
        locked = await resolve_relation(conn, schema, table)
        if locked is None or locked["relid"] != relation["relid"]:
            raise WriteError(
                f'"{schema}"."{table}" was dropped or replaced while waiting for the lock; '
                f"nothing was written"
            )

        info = await inspect_table(
            conn, locked["relid"], locked["nspname"], locked["relname"], locked["relkind"]
        )
        reasons = info.mode_reasons(mode)
        if reasons:
            raise WriteError(f'cannot {mode} into "{schema}"."{table}": ' + "; ".join(reasons))

        plan = await self._plan(conn, info, source_id_column, fields, mode, field_mapping, model)
        failures: Dict[int, str] = {}
        texts = self._encode_rows(plan, rows, failures)
        await self._validate_types(conn, plan, texts, failures)
        if plan.deduplicate:
            await self._find_duplicates(conn, plan, rows, texts, failures)

        pending = [index for index in range(len(rows)) if index not in failures]
        written = await self._insert(conn, plan, texts, pending, failures) if pending else 0

        failed = [
            {
                "index": index,
                "source_id": source_value(rows[index], plan.source_field),
                "error": failures[index],
            }
            for index in sorted(failures)
        ]
        return {
            "written": written,
            "failed_count": len(failed),
            "failed": failed[:MAX_FAILED_DETAILS],
            "failed_truncated": len(failed) > MAX_FAILED_DETAILS,
        }

    async def _plan(self, conn, info, source_id_column, fields, mode, field_mapping, model):
        unknown = sorted(set(field_mapping) - set(fields))
        if unknown:
            raise WriteError(
                f"field_mapping names {', '.join(map(repr, unknown))}, which "
                f"{'is' if len(unknown) == 1 else 'are'} not in fields"
            )
        columns: List[Column] = []
        seen: Dict[str, str] = {}
        for name in fields:
            target = field_mapping.get(name, name)
            if not isinstance(target, str):
                raise WriteError(f"field_mapping[{name!r}] must be a column name, got {target!r}")
            column = info.column(target)
            if column is None:
                via = f" (mapped to {target!r})" if target != name else ""
                raise WriteError(
                    f"field {name!r}{via} matches no column of {info.qualified_name}; "
                    f"columns are: {', '.join(c.name for c in info.columns)}"
                )
            if target in seen:
                raise WriteError(f"fields {seen[target]!r} and {name!r} both map to column {target!r}")
            if target in (MODEL_COLUMN, EXTRACTED_AT_COLUMN):
                raise WriteError(
                    f"field {name!r} maps to {target!r}, which write_results fills itself"
                )
            if not column.writable:
                raise WriteError(f"field {name!r} maps to {target!r}, a {column.not_writable_reason}")
            seen[target] = name
            columns.append(column)

        if source_id_column not in seen:
            raise WriteError(
                f"source_id_column {source_id_column!r} is not among the columns the fields "
                f"map to: {', '.join(seen)}"
            )
        source = info.column(source_id_column)
        index = info.upsert_index(source_id_column)
        if mode == UPSERT and index is None:
            raise WriteError(
                f"column {source_id_column!r} cannot be upserted on: it needs a usable "
                f"single-column unique index or primary key; usable keys are: "
                f"{', '.join(info.upsert_keys) or 'none'}"
            )

        missing_required = [
            c.name for c in info.columns
            if c.required and c.name not in seen
            and c.name not in (MODEL_COLUMN, EXTRACTED_AT_COLUMN)
        ]
        if missing_required:
            raise WriteError(
                f"NOT NULL columns without a default must be written: {', '.join(missing_required)}"
            )

        model_column = info.column(MODEL_COLUMN)
        extracted_at_column = info.column(EXTRACTED_AT_COLUMN)
        provenance = [c for c in (model_column, extracted_at_column) if c is not None]
        if model_column is not None:
            if model is None:
                raise WriteError(
                    f"{info.qualified_name} has a {MODEL_COLUMN} column, so the model "
                    f"argument is required"
                )
            problem = await conn.fetchrow(
                "SELECT e.message, e.detail FROM pg_input_error_info($1, $2) e "
                "WHERE NOT pg_input_is_valid($1, $2)",
                model, model_column.type_sql,
            )
            if problem is not None:
                raise WriteError(f"model {model!r} does not fit column {MODEL_COLUMN}: "
                                 + error_text(problem["message"], problem["detail"]))

        missing_privileges = []
        for column in columns + provenance:
            if not column.insert_privilege:
                missing_privileges.append(f"INSERT on column {column.name}")
        if mode == UPSERT:
            for column in columns + provenance:
                if column.name != source_id_column and not column.update_privilege:
                    missing_privileges.append(f"UPDATE on column {column.name}")
            if not source.select_privilege:
                # ON CONFLICT reads the key column to find the conflicting row.
                missing_privileges.append(f"SELECT on column {source_id_column}")
        if missing_privileges:
            raise WriteError("the current role lacks " + ", ".join(missing_privileges))

        return WritePlan(
            info=info,
            mode=mode,
            columns=columns,
            source=source,
            source_field=seen[source_id_column],
            fields=[seen[c.name] for c in columns],
            index=index,
            model_column=model_column,
            extracted_at_column=extracted_at_column,
            model=model,
            # append keeps repeated source_ids unless the column is itself a key.
            deduplicate=mode == UPSERT or index is not None,
        )

    def _encode_rows(self, plan, rows, failures) -> List[Optional[List[Optional[str]]]]:
        expected = set(plan.fields)
        texts: List[Optional[List[Optional[str]]]] = []
        for position, row in enumerate(rows):
            texts.append(None)
            if not isinstance(row, dict):
                failures[position] = f"row is a {json_type(row)}, not an object"
                continue
            missing = [name for name in plan.fields if name not in row]
            extra = sorted(set(row) - expected)
            if missing or extra:
                parts = []
                if missing:
                    parts.append(f"missing {', '.join(map(repr, missing))}")
                if extra:
                    parts.append(f"not in fields: {', '.join(map(repr, extra))}")
                failures[position] = "row fields do not match fields: " + "; ".join(parts)
                continue
            if row[plan.source_field] is None:
                failures[position] = f"source_id field {plan.source_field!r} is null"
                continue
            try:
                texts[position] = [
                    encode_value(row[name], column)
                    for name, column in zip(plan.fields, plan.columns)
                ]
            except RowError as e:
                failures[position] = str(e)
        return texts

    async def _validate_types(self, conn, plan, texts, failures) -> None:
        """Check every value against its column type in one statement.

        pg_input_is_valid applies the type's input function with the typmod
        and any domain constraint, without raising; table constraints such as
        NOT NULL and CHECK are left to the insert.
        """
        values, types, owners = [], [], []
        for position, row in enumerate(texts):
            if row is None or position in failures:
                continue
            for value, column in zip(row, plan.columns):
                if value is not None:
                    values.append(value)
                    types.append(column.type_sql)
                    owners.append((position, column.name))
        if not values:
            return
        invalid = await conn.fetch(
            "SELECT s.k, (pg_input_error_info(s.v, s.t)).* FROM ("
            "  SELECT u.v, u.t, u.k FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS u(v, t, k)"
            "  WHERE NOT pg_input_is_valid(u.v, u.t)"
            ") s ORDER BY s.k",
            values, types,
        )
        for record in invalid:
            position, column_name = owners[record["k"] - 1]
            if position not in failures:
                failures[position] = f"column {column_name}: " + error_text(
                    record["message"], record["detail"]
                )

    async def _find_duplicates(self, conn, plan, rows, texts, failures) -> None:
        """Fail every row whose source_id repeats within the batch.

        The database decides what repeats, by the column's type and
        collation, so that "01" and 1 are one integer key.  No row of a
        repeated key wins: which one was meant cannot be known.
        """
        source_position = plan.columns.index(plan.source)
        positions = [
            position for position, row in enumerate(texts)
            if row is not None and position not in failures
        ]
        if len(positions) < 2:
            return
        key = f"u.v::{plan.source.type_sql}"
        if plan.source.collation_sql:
            key = f"({key} COLLATE {plan.source.collation_sql})"
        groups = await conn.fetch(
            f"SELECT array_agg(u.k ORDER BY u.k) AS members "
            f"FROM unnest($1::text[]) WITH ORDINALITY AS u(v, k) "
            f"GROUP BY {key} HAVING count(*) > 1",
            [texts[position][source_position] for position in positions],
        )
        for group in groups:
            members = [positions[k - 1] for k in group["members"]]
            for position in members:
                others = [str(other) for other in members if other != position]
                failures[position] = (
                    f"source_id {rows[position][plan.source_field]!r} appears more than once "
                    f"in this batch (also at index {', '.join(others)}); none of these rows "
                    f"was written"
                )

    async def _insert(self, conn, plan, texts, pending, failures) -> int:
        """Write the pending rows in one statement, row by row if that fails.

        A data or constraint error in the batch rolls back to a savepoint and
        every row is retried under its own savepoint, so the bad rows are
        found and the rest kept.  Any other error ends the whole call.
        """
        sql = build_insert_sql(plan)
        extra = [plan.model] if plan.model_column is not None else []

        def arrays(positions):
            return [
                [texts[position][i] for position in positions]
                for i in range(len(plan.columns))
            ]

        try:
            async with conn.transaction():
                await conn.execute(sql, *arrays(pending), *extra)
            return len(pending)
        except asyncpg.PostgresError as e:
            if not is_row_error(e):
                raise

        written = 0
        for position in pending:
            try:
                async with conn.transaction():
                    await conn.execute(sql, *arrays([position]), *extra)
                written += 1
            except asyncpg.PostgresError as e:
                if not is_row_error(e):
                    raise
                failures[position] = error_text(e.message, getattr(e, "detail", None))
        return written


@dataclass
class WritePlan:
    info: TableInfo
    mode: str
    columns: List[Column]       # in the order of fields
    source: Column
    source_field: str
    fields: List[str]           # the field feeding each of columns
    index: Optional[UniqueIndex]
    model_column: Optional[Column]
    extracted_at_column: Optional[Column]
    model: Optional[str]
    deduplicate: bool


def build_insert_sql(plan: WritePlan) -> str:
    """The INSERT for a batch: one text[] parameter per column, then model.

    Values are cast to the type without its typmod and the INSERT applies
    the typmod as an assignment, which raises on a value that does not fit;
    an explicit cast to varchar(3) would silently cut it.  The conflict
    target names the operator class and collation of the chosen index, or
    every unique index on the column would act as an arbiter.
    """
    targets = [quote(c.name) for c in plan.columns]
    values = [f"v.c{i}::{c.cast_type_sql}" for i, c in enumerate(plan.columns, 1)]
    updates = [quote(c.name) for c in plan.columns if c is not plan.source]
    if plan.model_column is not None:
        targets.append(quote(MODEL_COLUMN))
        values.append(f"${len(plan.columns) + 1}::text::{plan.model_column.cast_type_sql}")
        updates.append(quote(MODEL_COLUMN))
    if plan.extracted_at_column is not None:
        targets.append(quote(EXTRACTED_AT_COLUMN))
        values.append("now()")
        updates.append(quote(EXTRACTED_AT_COLUMN))
    unnest = ", ".join(f"${i}::text[]" for i in range(1, len(plan.columns) + 1))
    aliases = ", ".join(f"c{i}" for i in range(1, len(plan.columns) + 1))
    sql = (
        f"INSERT INTO {plan.info.qualified_name} ({', '.join(targets)}) "
        f"SELECT {', '.join(values)} FROM unnest({unnest}) AS v({aliases})"
    )
    if plan.mode == UPSERT:
        key = quote(plan.source.name)
        if plan.source.collation_sql:
            key += f" COLLATE {plan.source.collation_sql}"
        key += f" {plan.index.opclass_sql}"
        if updates:
            assignments = ", ".join(f"{name} = EXCLUDED.{name}" for name in updates)
            sql += f" ON CONFLICT ({key}) DO UPDATE SET {assignments}"
        else:
            sql += f" ON CONFLICT ({key}) DO NOTHING"
    return sql


class RowError(ValueError):
    """One row cannot be written; the others go on."""


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def scalar_text(value: Any) -> str:
    """The text form of a JSON scalar, as PostgreSQL input functions read it."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return repr(value)
    if isinstance(value, int):
        return str(value)
    if "\x00" in value:
        raise RowError("a string contains a NUL character, which PostgreSQL text cannot hold")
    return value


def encode_value(value: Any, column: Column) -> Optional[str]:
    if value is None:
        return None
    if column.kind == "json":
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False)
        except ValueError as e:
            raise RowError(f"column {column.name}: not valid JSON ({e})") from e
    if column.kind == "array":
        if not isinstance(value, (list, tuple)):
            raise RowError(
                f"column {column.name} ({column.type_sql}) takes a JSON array, "
                f"got a {json_type(value)}"
            )
        return encode_array(value, column)
    if isinstance(value, (dict, list, tuple)):
        raise RowError(
            f"column {column.name} ({column.type_sql}) takes a single value, "
            f"got a {json_type(value)}"
        )
    return scalar_text(value)


def encode_array(values: Sequence[Any], column: Column) -> str:
    """A one-dimensional array literal: NULL bare, every other element quoted."""
    elements = []
    for value in values:
        if value is None:
            elements.append("NULL")
        elif isinstance(value, (list, tuple)):
            raise RowError(f"column {column.name}: nested arrays are not supported")
        elif isinstance(value, dict):
            raise RowError(f"column {column.name}: an array element is an object")
        else:
            text = scalar_text(value).replace("\\", "\\\\").replace('"', '\\"')
            elements.append(f'"{text}"')
    return "{" + column.array_delimiter.join(elements) + "}"


def is_row_error(error: asyncpg.PostgresError) -> bool:
    return (getattr(error, "sqlstate", None) or "")[:2] in ROW_ERROR_CLASSES


def error_text(message: str, detail: Optional[str]) -> str:
    return f"{message} ({detail})" if detail else message


def source_value(row: Any, source_field: str) -> Any:
    if isinstance(row, dict):
        return row.get(source_field)
    return None
