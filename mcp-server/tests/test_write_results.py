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
Result write-back test module

Covers the write settings, the whitelist, describe_result_table and
write_results.  The integration tests build their own schema and login roles
and drop them afterwards, so the account in DB_USER must be able to create
roles, and pg_hba must let a new role connect from DB_HOST without a
password.  Unlike the other modules these tests do not skip without a
cluster: they fail, because a write path that was never exercised must not
look green.
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import replace

import asyncpg
import pytest
import pytest_asyncio
from fastmcp import Client
from fastmcp.exceptions import ToolError

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig, ServerConfig, WriteConfig, parse_write_tables
from cbmcp.server import CloudberryMCPServer

SCHEMA = f"cbmcp_write_test_{uuid.uuid4().hex[:8]}"
# Allowed by the whitelist but never created; and a schema that is not allowed.
OTHER_SCHEMA = f"{SCHEMA}_other"
ROLE_PREFIX = f"{SCHEMA}_r"

WRITE_ENV = ("MCP_ENABLE_WRITE", "MCP_WRITE_TABLES", "MCP_WRITE_MAX_ROWS",
             "MCP_WRITE_MAX_BYTES", "MCP_WRITE_LOCK_TIMEOUT")


def unwrap(result):
    return result.data if hasattr(result, "data") else result


def write_config(**overrides):
    return replace(WriteConfig(enabled=True, tables=((SCHEMA, None),)), **overrides)


def make_server(write=None, user=None):
    db_config = DatabaseConfig.from_env()
    if user is not None:
        db_config = replace(db_config, username=user, password="")
    return CloudberryMCPServer(ServerConfig.from_env(), db_config, write or write_config())


async def open_connection(user=None):
    config = DatabaseConfig.from_env()
    return await asyncpg.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=user or config.username,
        password=(config.password or None) if user is None else None,
    )


class ServerClient:
    """An in-process server and a client on it, closed together."""

    def __init__(self, server):
        self.server = server
        self.client = Client(server.mcp)

    async def __aenter__(self):
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *exc):
        try:
            await self.client.__aexit__(*exc)
        finally:
            await self.server.close()

    async def call(self, tool, **arguments):
        return unwrap(await self.client.call_tool(tool, arguments))

    async def tool_names(self):
        return {tool.name for tool in await self.client.list_tools()}


@pytest.fixture(autouse=True)
def clean_write_env(monkeypatch):
    # A developer's .env must not switch writing on or off underneath a test;
    # load_dotenv() does not override a variable that is already set.
    for name in WRITE_ENV:
        monkeypatch.setenv(name, "")


# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------

class TestWriteTablesParsing:
    def test_unquoted_names_fold_to_lower_case(self):
        assert parse_write_tables("Results.Answers") == (("results", "answers"),)

    def test_quoted_names_keep_case_and_doubled_quotes(self):
        assert parse_write_tables('"MySchema"."a""b"') == (("MySchema", 'a"b'),)

    def test_quoted_names_may_hold_commas_and_dots(self):
        assert parse_write_tables('"a,b"."c.d", s.t') == (("a,b", "c.d"), ("s", "t"))

    def test_schema_star(self):
        assert parse_write_tables(" s.* , t.u ") == (("s", None), ("t", "u"))

    @pytest.mark.parametrize("raw", [
        "", " ", "table_only", "s.", ".t", "s.t,", "s.t,,u.v", "s.t u.v",
        '"s.t', '"".t', "1s.t", "s.$t", "s.t.u", "s.*x",
        "s." + "a" * 64,
    ])
    def test_invalid_entries_are_refused(self, raw):
        with pytest.raises(ValueError):
            parse_write_tables(raw)


class TestWriteSettings:
    def test_writing_is_off_by_default(self):
        assert WriteConfig.from_env(60.0).enabled is False

    def test_other_settings_are_ignored_while_writing_is_off(self, monkeypatch):
        monkeypatch.setenv("MCP_WRITE_MAX_ROWS", "not a number")
        assert WriteConfig.from_env(60.0) == WriteConfig()

    def test_enabled_reads_every_setting(self, monkeypatch):
        monkeypatch.setenv("MCP_ENABLE_WRITE", "TRUE")
        monkeypatch.setenv("MCP_WRITE_TABLES", "s.t, u.*")
        monkeypatch.setenv("MCP_WRITE_MAX_ROWS", "10")
        monkeypatch.setenv("MCP_WRITE_MAX_BYTES", "2048")
        monkeypatch.setenv("MCP_WRITE_LOCK_TIMEOUT", "1.5")
        assert WriteConfig.from_env(60.0) == WriteConfig(
            enabled=True, tables=(("s", "t"), ("u", None)),
            max_rows=10, max_bytes=2048, lock_timeout=1.5,
        )

    @pytest.mark.parametrize("settings, named", [
        ({"MCP_ENABLE_WRITE": "yes"}, "MCP_ENABLE_WRITE"),
        ({"MCP_ENABLE_WRITE": "true"}, "MCP_WRITE_TABLES"),
        ({"MCP_ENABLE_WRITE": "true", "MCP_WRITE_TABLES": "s.t",
          "MCP_WRITE_MAX_ROWS": "0"}, "MCP_WRITE_MAX_ROWS"),
        ({"MCP_ENABLE_WRITE": "true", "MCP_WRITE_TABLES": "s.t",
          "MCP_WRITE_MAX_BYTES": "lots"}, "MCP_WRITE_MAX_BYTES"),
        ({"MCP_ENABLE_WRITE": "true", "MCP_WRITE_TABLES": "s.t",
          "MCP_WRITE_LOCK_TIMEOUT": "-1"}, "MCP_WRITE_LOCK_TIMEOUT"),
        ({"MCP_ENABLE_WRITE": "true", "MCP_WRITE_TABLES": "s.t",
          "MCP_WRITE_LOCK_TIMEOUT": "60"}, "MCP_WRITE_LOCK_TIMEOUT"),
        ({"MCP_ENABLE_WRITE": "true", "MCP_WRITE_TABLES": "s.t,"}, "MCP_WRITE_TABLES"),
    ])
    def test_invalid_setting_stops_the_server_and_names_it(self, monkeypatch, settings, named):
        for name, value in settings.items():
            monkeypatch.setenv(name, value)
        with pytest.raises(ValueError, match=named):
            CloudberryMCPServer(ServerConfig.from_env(), DatabaseConfig.from_env())

    def test_whitelist_compares_catalog_names_exactly(self):
        config = WriteConfig(enabled=True, tables=(("s", "t"), ("u", None)))
        assert config.allows("s", "t")
        assert config.allows("u", "anything")
        assert not config.allows("s", "T")
        assert not config.allows("S", "t")
        assert not config.allows('"s"', "t")
        assert not config.allows("s", "other")


@pytest.mark.asyncio
class TestRegistration:
    async def test_write_tools_absent_while_writing_is_off(self):
        server = CloudberryMCPServer(ServerConfig.from_env(), DatabaseConfig.from_env())
        async with ServerClient(server) as session:
            names = await session.tool_names()
        assert "describe_result_table" not in names
        assert "write_results" not in names

    async def test_write_tools_absent_when_switched_off_explicitly(self, monkeypatch):
        monkeypatch.setenv("MCP_ENABLE_WRITE", "false")
        monkeypatch.setenv("MCP_WRITE_TABLES", "s.t")
        server = CloudberryMCPServer(ServerConfig.from_env(), DatabaseConfig.from_env())
        assert server.write_config.enabled is False
        async with ServerClient(server) as session:
            names = await session.tool_names()
        assert not {"describe_result_table", "write_results"} & names

    async def test_write_tools_registered_and_annotated_when_on(self):
        async with ServerClient(make_server()) as session:
            tools = {tool.name: tool for tool in await session.client.list_tools()}
        assert {"describe_result_table", "write_results"} <= set(tools)
        describe = tools["describe_result_table"].annotations.model_dump(by_alias=True)
        assert describe["readOnlyHint"] is True
        write = tools["write_results"].annotations.model_dump(by_alias=True)
        assert write["readOnlyHint"] is False
        assert write["destructiveHint"] is True
        assert write["idempotentHint"] is False


# ----------------------------------------------------------------------
# Integration fixtures
# ----------------------------------------------------------------------

FIXTURE_DDL = [
    # The table the write tests use.
    f"""CREATE TABLE {SCHEMA}.results (
        doc_id text PRIMARY KEY,
        summary text,
        score integer CHECK (score >= 0),
        label varchar(3),
        tags text[],
        boxes box[],
        meta jsonb,
        price numeric(30,10),
        big bigint,
        ratio float8,
        flag boolean,
        day date,
        required_note text NOT NULL DEFAULT 'none',
        model text,
        extracted_at timestamptz
    ) DISTRIBUTED BY (doc_id)""",
    f"""CREATE TABLE {SCHEMA}.plain (
        sid integer, note text NOT NULL, g integer GENERATED ALWAYS AS (sid * 2) STORED,
        i integer GENERATED ALWAYS AS IDENTITY, d integer GENERATED BY DEFAULT AS IDENTITY
    ) DISTRIBUTED BY (sid)""",
    f"CREATE UNIQUE INDEX plain_sid ON {SCHEMA}.plain (sid) INCLUDE (note)",
    f"CREATE TABLE {SCHEMA}.ao (sid integer, v text) WITH (appendonly=true) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.aoco (sid integer, v text) "
    f"WITH (appendonly=true, orientation=column) DISTRIBUTED BY (sid)",
    f"CREATE VIEW {SCHEMA}.v AS SELECT 1 AS sid",
    f"CREATE MATERIALIZED VIEW {SCHEMA}.mv AS SELECT 1 AS sid DISTRIBUTED BY (sid)",
    f"CREATE READABLE EXTERNAL WEB TABLE {SCHEMA}.ext (sid integer) EXECUTE 'echo 1' FORMAT 'TEXT'",
    # Partitioned: all heap; one AO leaf; a deferrable constraint on a leaf only.
    f"CREATE TABLE {SCHEMA}.part (sid integer, v text) PARTITION BY RANGE (sid) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_a PARTITION OF {SCHEMA}.part FOR VALUES FROM (0) TO (100)",
    f"CREATE TABLE {SCHEMA}.part_b PARTITION OF {SCHEMA}.part FOR VALUES FROM (100) TO (200)",
    f"CREATE UNIQUE INDEX part_sid ON {SCHEMA}.part (sid)",
    f"CREATE TABLE {SCHEMA}.part_ao (sid integer, v text) PARTITION BY RANGE (sid) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_ao_a PARTITION OF {SCHEMA}.part_ao FOR VALUES FROM (0) TO (100)",
    f"CREATE TABLE {SCHEMA}.part_ao_b PARTITION OF {SCHEMA}.part_ao FOR VALUES FROM (100) TO (200) "
    f"WITH (appendonly=true)",
    f"CREATE TABLE {SCHEMA}.part_def (sid integer, v text) PARTITION BY RANGE (sid) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_def_a PARTITION OF {SCHEMA}.part_def FOR VALUES FROM (0) TO (100)",
    f"ALTER TABLE {SCHEMA}.part_def_a ADD CONSTRAINT part_def_a_u UNIQUE (sid) DEFERRABLE",
    f"CREATE TABLE {SCHEMA}.part_ct (sid integer, v text) PARTITION BY RANGE (sid) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_ct_a PARTITION OF {SCHEMA}.part_ct FOR VALUES FROM (0) TO (100)",
    f"""CREATE FUNCTION {SCHEMA}.bump() RETURNS trigger LANGUAGE plpgsql AS
        $f$ BEGIN RETURN NEW; END $f$""",
    f"CREATE CONSTRAINT TRIGGER late AFTER INSERT ON {SCHEMA}.part_ct_a "
    f"DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    # Unique indexes that cannot serve as an upsert key.
    # Replicated, so that a unique index need not contain a distribution key.
    f"CREATE TABLE {SCHEMA}.keys (sid text, k2 text, other integer) DISTRIBUTED REPLICATED",
    f"CREATE UNIQUE INDEX keys_pattern ON {SCHEMA}.keys (sid text_pattern_ops)",
    f'CREATE UNIQUE INDEX keys_collation ON {SCHEMA}.keys (sid COLLATE "C")',
    f"CREATE UNIQUE INDEX keys_multi ON {SCHEMA}.keys (sid, k2)",
    f"CREATE UNIQUE INDEX keys_expr ON {SCHEMA}.keys (lower(sid))",
    f"CREATE UNIQUE INDEX keys_partial ON {SCHEMA}.keys (sid) WHERE other > 0",
    f"CREATE TABLE {SCHEMA}.deferred_key (sid integer, v text) DISTRIBUTED BY (sid)",
    f"ALTER TABLE {SCHEMA}.deferred_key ADD CONSTRAINT deferred_key_u UNIQUE (sid) DEFERRABLE",
    # Both a default and a non-default unique index on the same column.
    f"CREATE TABLE {SCHEMA}.two_keys (sid text, v text) DISTRIBUTED BY (sid)",
    f"CREATE UNIQUE INDEX two_keys_default ON {SCHEMA}.two_keys (sid)",
    f"CREATE UNIQUE INDEX two_keys_pattern ON {SCHEMA}.two_keys (sid text_pattern_ops)",
    # Provenance columns of the wrong kind.
    f"CREATE TABLE {SCHEMA}.bad_model (sid integer PRIMARY KEY, model integer) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.generated_model (sid integer PRIMARY KEY, "
    f"model text GENERATED ALWAYS AS ('x') STORED) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.bad_time (sid integer PRIMARY KEY, extracted_at text) DISTRIBUTED BY (sid)",
    f"CREATE DOMAIN {SCHEMA}.model_name AS varchar(40)",
    f"CREATE TABLE {SCHEMA}.domain_model (sid integer PRIMARY KEY, model {SCHEMA}.model_name, "
    f"extracted_at date) DISTRIBUTED BY (sid)",
    f'CREATE TABLE {SCHEMA}.capital ("Model" text, sid integer PRIMARY KEY) DISTRIBUTED BY (sid)',
    # User triggers, one of them disabled, on a table and a partition.
    f"CREATE TABLE {SCHEMA}.trig (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER on_insert BEFORE INSERT ON {SCHEMA}.trig "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"CREATE TRIGGER switched_off BEFORE UPDATE ON {SCHEMA}.trig "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"ALTER TABLE {SCHEMA}.trig DISABLE TRIGGER switched_off",
    # A trigger on DELETE: the tool never deletes, so part stays writable.
    f"CREATE TRIGGER leaf_trigger AFTER DELETE ON {SCHEMA}.part_b "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"CREATE TABLE {SCHEMA}.random_dist (sid integer, v text) DISTRIBUTED RANDOMLY",
    f'CREATE TABLE {SCHEMA}."MixedCase" (sid integer PRIMARY KEY) DISTRIBUTED BY (sid)',
    # For write_results.
    f"CREATE TABLE {SCHEMA}.key_only (sid integer PRIMARY KEY) DISTRIBUTED BY (sid)",
    f"CREATE DOMAIN {SCHEMA}.positive AS integer CHECK (VALUE > 0)",
    f"CREATE TABLE {SCHEMA}.typed (sid integer PRIMARY KEY, n {SCHEMA}.positive) DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.other_key (sid integer, other integer UNIQUE) DISTRIBUTED REPLICATED",
    # Triggers on INSERT or UPDATE, of any timing or level, make a table
    # unwritable.
    f"""CREATE FUNCTION {SCHEMA}.drop_row() RETURNS trigger LANGUAGE plpgsql AS
        $f$ BEGIN RETURN NULL; END $f$""",
    f"CREATE TABLE {SCHEMA}.dropping (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER drops BEFORE INSERT ON {SCHEMA}.dropping "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.drop_row()",
    f"CREATE TABLE {SCHEMA}.on_update (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER stamps BEFORE UPDATE ON {SCHEMA}.on_update "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"CREATE TABLE {SCHEMA}.part_trig (sid integer, v text) PARTITION BY RANGE (sid) "
    f"DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_trig_a PARTITION OF {SCHEMA}.part_trig "
    f"FOR VALUES FROM (0) TO (100)",
    f"CREATE UNIQUE INDEX part_trig_sid ON {SCHEMA}.part_trig (sid)",
    f"CREATE TRIGGER leaf_drops BEFORE INSERT ON {SCHEMA}.part_trig_a "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.drop_row()",
    f"CREATE TABLE {SCHEMA}.after_insert (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER audits AFTER INSERT ON {SCHEMA}.after_insert "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"CREATE TABLE {SCHEMA}.after_update (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER audits AFTER UPDATE ON {SCHEMA}.after_update "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"CREATE TABLE {SCHEMA}.statement (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    "SET gp_enable_statement_trigger = on",
    f"CREATE TRIGGER once AFTER INSERT ON {SCHEMA}.statement "
    f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.bump()",
    "RESET gp_enable_statement_trigger",
    f"CREATE TABLE {SCHEMA}.part_after (sid integer, v text) PARTITION BY RANGE (sid) "
    f"DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_after_a PARTITION OF {SCHEMA}.part_after "
    f"FOR VALUES FROM (0) TO (100)",
    f"CREATE UNIQUE INDEX part_after_sid ON {SCHEMA}.part_after (sid)",
    f"CREATE TRIGGER leaf_audits AFTER INSERT ON {SCHEMA}.part_after_a "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    # Triggers enabled for replicas only or always, and a user constraint
    # trigger that is not deferrable, count as enabled triggers too.
    f"CREATE TABLE {SCHEMA}.replica (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER on_replica AFTER INSERT ON {SCHEMA}.replica "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"ALTER TABLE {SCHEMA}.replica ENABLE REPLICA TRIGGER on_replica",
    f"CREATE TABLE {SCHEMA}.always (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER on_always AFTER UPDATE ON {SCHEMA}.always "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"ALTER TABLE {SCHEMA}.always ENABLE ALWAYS TRIGGER on_always",
    f"CREATE TABLE {SCHEMA}.checked (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE CONSTRAINT TRIGGER checks AFTER INSERT ON {SCHEMA}.checked "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    # An AFTER trigger that deletes the row just inserted.  Cloudberry lets a
    # segment run it only on a replicated table and with allow_segment_DML,
    # which the function sets itself.
    f"CREATE TABLE {SCHEMA}.wiping (sid integer PRIMARY KEY, v text) DISTRIBUTED REPLICATED",
    f"""CREATE FUNCTION {SCHEMA}.wipe() RETURNS trigger LANGUAGE plpgsql
        SET allow_segment_DML = on AS
        $f$ BEGIN DELETE FROM {SCHEMA}.wiping WHERE sid = NEW.sid; RETURN NULL; END $f$""",
    f"CREATE TRIGGER wipes AFTER INSERT ON {SCHEMA}.wiping "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.wipe()",
    # Rules ON INSERT or ON UPDATE make a table unwritable.
    f"CREATE TABLE {SCHEMA}.ruled (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"CREATE RULE skip AS ON INSERT TO {SCHEMA}.ruled DO INSTEAD NOTHING",
    f"CREATE TABLE {SCHEMA}.ruled_update (sid integer PRIMARY KEY, v text) DISTRIBUTED BY (sid)",
    f"INSERT INTO {SCHEMA}.ruled_update VALUES (1, 'before')",
    f"CREATE RULE keep AS ON UPDATE TO {SCHEMA}.ruled_update DO INSTEAD NOTHING",
    f"CREATE TABLE {SCHEMA}.part_rule (sid integer, v text) PARTITION BY RANGE (sid) "
    f"DISTRIBUTED BY (sid)",
    f"CREATE TABLE {SCHEMA}.part_rule_a PARTITION OF {SCHEMA}.part_rule "
    f"FOR VALUES FROM (0) TO (100)",
    f"CREATE UNIQUE INDEX part_rule_sid ON {SCHEMA}.part_rule (sid)",
    f"CREATE RULE leaf_skip AS ON INSERT TO {SCHEMA}.part_rule_a DO INSTEAD NOTHING",
    # A disabled trigger, a trigger on DELETE and the internal triggers of a
    # foreign key leave a table writable.
    f"CREATE TABLE {SCHEMA}.harmless_parent (id integer PRIMARY KEY) DISTRIBUTED BY (id)",
    f"CREATE TABLE {SCHEMA}.harmless (sid integer PRIMARY KEY, v text, "
    f"parent integer REFERENCES {SCHEMA}.harmless_parent (id)) DISTRIBUTED BY (sid)",
    f"CREATE TRIGGER off_drops BEFORE INSERT ON {SCHEMA}.harmless "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.drop_row()",
    f"ALTER TABLE {SCHEMA}.harmless DISABLE TRIGGER off_drops",
    f"CREATE TRIGGER after_delete AFTER DELETE ON {SCHEMA}.harmless "
    f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.bump()",
    f"""CREATE TABLE {SCHEMA}.more_types (sid integer PRIMARY KEY, ts timestamp,
        tstz timestamptz, t time, f float8, pos {SCHEMA}.positive) DISTRIBUTED BY (sid)""",
    # Catalog names that need quoting must still produce valid statements.
    f'CREATE TABLE {SCHEMA}."we""ird,t.b" ("Id" integer PRIMARY KEY, "a b" text) '
    f'DISTRIBUTED BY ("Id")',
]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_schema():
    conn = await open_connection()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        for statement in FIXTURE_DDL:
            await conn.execute(statement)
        yield conn
    finally:
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            roles = await conn.fetch(
                "SELECT rolname FROM pg_roles WHERE rolname LIKE $1", f"{ROLE_PREFIX}%"
            )
            for role in roles:
                await conn.execute(f'DROP OWNED BY "{role["rolname"]}"')
                await conn.execute(f'DROP ROLE "{role["rolname"]}"')
        finally:
            await conn.close()


async def create_role(conn, grants):
    """A login role with the given GRANT statements; {role} is replaced."""
    role = f"{ROLE_PREFIX}{uuid.uuid4().hex[:6]}"
    await conn.execute(f'CREATE ROLE "{role}" LOGIN')
    await conn.execute(f'GRANT USAGE ON SCHEMA {SCHEMA} TO "{role}"')
    for grant in grants:
        await conn.execute(grant.format(role=f'"{role}"'))
    return role


async def describe(table, server=None):
    async with ServerClient(server or make_server()) as session:
        return await session.call("describe_result_table", schema=SCHEMA, table=table)


# ----------------------------------------------------------------------
# describe_result_table
# ----------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="module")
class TestDescribeResultTable:
    async def test_reports_a_writable_table(self, fixture_schema):
        info = await describe("results")
        assert info["writable"] == {"upsert": True, "append": True}
        assert info["reasons"] == {"upsert": [], "append": []}
        assert info["upsert_keys"] == ["doc_id"]
        assert info["provenance"] == {"model": True, "extracted_at": True}
        assert info["distribution_policy"] == "hash"
        assert info["distribution_key"] == ["doc_id"]
        columns = {column["name"]: column for column in info["columns"]}
        assert columns["label"]["type"] == "character varying(3)"
        assert columns["price"]["type"] == "numeric(30,10)"
        assert columns["doc_id"]["nullable"] is False
        assert columns["required_note"]["has_default"] is True
        assert columns["summary"]["insert_privilege"] is True

    async def test_reports_generated_and_identity_columns(self, fixture_schema):
        info = await describe("plain")
        columns = {column["name"]: column for column in info["columns"]}
        assert columns["g"]["writable"] is False
        assert columns["g"]["not_writable_reason"] == "generated column"
        assert columns["i"]["writable"] is False
        assert columns["d"]["writable"] is True
        # A single-key unique index with INCLUDE columns still counts.
        assert info["upsert_keys"] == ["sid"]

    @pytest.mark.parametrize("table, expected", [
        ("ao", "not a heap table (it uses ao_row)"),
        ("aoco", "not a heap table (it uses ao_column)"),
        ("part_ao", "partition"),
        ("part_def", "deferrable constraint part_def_a_u"),
        ("part_ct", "deferrable constraint trigger late"),
        ("deferred_key", "deferrable constraint deferred_key_u"),
        ("bad_model", "column model must be"),
        ("generated_model", "generated column"),
        ("bad_time", "column extracted_at must be"),
    ])
    async def test_reports_why_a_table_cannot_be_written(self, fixture_schema, table, expected):
        info = await describe(table)
        assert info["writable"] == {"upsert": False, "append": False}
        for mode in ("upsert", "append"):
            assert any(expected in reason for reason in info["reasons"][mode]), info["reasons"]

    async def test_leaf_partition_problems_name_the_partition(self, fixture_schema):
        info = await describe("part_ao")
        assert any("part_ao_b" in reason for reason in info["reasons"]["append"])
        info = await describe("part_def")
        assert any("part_def_a" in reason for reason in info["reasons"]["append"])

    @pytest.mark.parametrize("table", ["v", "mv", "ext"])
    async def test_refuses_relations_that_are_not_tables(self, fixture_schema, table):
        with pytest.raises(ToolError, match="heap table"):
            await describe(table)

    async def test_an_all_heap_partitioned_table_is_writable(self, fixture_schema):
        info = await describe("part")
        assert info["partitioned"] is True
        assert info["writable"] == {"upsert": True, "append": True}
        assert info["upsert_keys"] == ["sid"]

    async def test_explains_unusable_unique_indexes(self, fixture_schema):
        info = await describe("keys")
        assert info["upsert_keys"] == []
        assert info["writable"] == {"upsert": False, "append": True}
        indexes = {index["name"]: index for index in info["unique_indexes"]}
        assert any("operator class" in r for r in indexes["keys_pattern"]["reasons"])
        assert any("collation" in r for r in indexes["keys_collation"]["reasons"])
        assert any("more than one key" in r for r in indexes["keys_multi"]["reasons"])
        assert any("expression" in r for r in indexes["keys_expr"]["reasons"])
        assert any("partial" in r for r in indexes["keys_partial"]["reasons"])
        assert all(not index["usable"] for index in indexes.values())

    async def test_deferrable_unique_is_not_an_upsert_key(self, fixture_schema):
        info = await describe("deferred_key")
        index = info["unique_indexes"][0]
        assert index["usable"] is False
        assert any("DEFERRABLE" in r for r in index["reasons"])

    async def test_a_column_with_a_default_index_too_is_an_upsert_key(self, fixture_schema):
        info = await describe("two_keys")
        assert info["upsert_keys"] == ["sid"]
        usable = {index["name"]: index["usable"] for index in info["unique_indexes"]}
        assert usable == {"two_keys_default": True, "two_keys_pattern": False}

    async def test_provenance_columns_need_exact_lower_case_names(self, fixture_schema):
        info = await describe("capital")
        assert info["provenance"] == {"model": False, "extracted_at": False}
        info = await describe("domain_model")
        assert info["provenance"] == {"model": True, "extracted_at": True}
        assert info["writable"] == {"upsert": True, "append": True}

    async def test_lists_triggers_on_the_table_and_its_partitions(self, fixture_schema):
        info = await describe("trig")
        triggers = {(t["table"], t["name"]): t["enabled"] for t in info["triggers"]}
        assert triggers == {
            (f"{SCHEMA}.trig", "on_insert"): True,
            (f"{SCHEMA}.trig", "switched_off"): False,
        }
        info = await describe("part")
        assert [(t["table"], t["name"]) for t in info["triggers"]] == [
            (f"{SCHEMA}.part_b", "leaf_trigger")
        ]

    async def test_reports_random_distribution(self, fixture_schema):
        info = await describe("random_dist")
        assert info["distribution_policy"] == "random"
        assert info["distribution_key"] == []

    async def test_names_are_matched_exactly(self, fixture_schema):
        assert (await describe("MixedCase"))["table"] == "MixedCase"
        for table in ("mixedcase", '"MixedCase"'):
            with pytest.raises(ToolError, match="does not exist"):
                await describe(table)

    @pytest.mark.parametrize("schema, table", [
        (OTHER_SCHEMA, "results"),
        (SCHEMA.upper(), "results"),
        (f'"{SCHEMA}"', "results"),
        ("public", "results"),
    ])
    async def test_refuses_tables_outside_the_whitelist(self, fixture_schema, schema, table):
        async with ServerClient(make_server()) as session:
            with pytest.raises(ToolError, match="not in MCP_WRITE_TABLES"):
                await session.call("describe_result_table", schema=schema, table=table)

    async def test_single_table_entry_allows_only_that_table(self, fixture_schema):
        server = make_server(write_config(tables=((SCHEMA, "results"),)))
        async with ServerClient(server) as session:
            await session.call("describe_result_table", schema=SCHEMA, table="results")
            with pytest.raises(ToolError, match="not in MCP_WRITE_TABLES"):
                await session.call("describe_result_table", schema=SCHEMA, table="plain")


@pytest.mark.asyncio(loop_scope="module")
class TestDescribePrivileges:
    async def test_column_grants_alone_cannot_lock_the_table(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT (doc_id), INSERT (doc_id, summary), UPDATE (summary) "
            f"ON {SCHEMA}.results TO {{role}}",
        ])
        info = await describe("results", make_server(user=role))
        assert info["writable"] == {"upsert": False, "append": False}
        assert any("table-level UPDATE" in r for r in info["reasons"]["upsert"])
        assert any("table-level INSERT" in r for r in info["reasons"]["append"])
        columns = {column["name"]: column for column in info["columns"]}
        assert columns["summary"]["insert_privilege"] is True
        assert columns["score"]["insert_privilege"] is False

    async def test_insert_alone_cannot_append_to_a_partitioned_table(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT, INSERT ON {SCHEMA}.part TO {{role}}",
            f"GRANT SELECT, INSERT ON {SCHEMA}.plain TO {{role}}",
        ])
        server = make_server(user=role)
        info = await describe("part", server)
        assert info["writable"]["append"] is False
        assert any("INSERT alone is not enough" in r for r in info["reasons"]["append"])
        info = await describe("plain", make_server(user=role))
        assert info["writable"] == {"upsert": False, "append": True}

    async def test_table_update_allows_both_modes(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.part TO {{role}}",
        ])
        info = await describe("part", make_server(user=role))
        assert info["writable"] == {"upsert": True, "append": True}


# ----------------------------------------------------------------------
# write_results
# ----------------------------------------------------------------------

def new_ids(count, prefix="d"):
    tag = uuid.uuid4().hex[:6]
    return [f"{prefix}{tag}-{i}" for i in range(count)]


async def write(table, fields, rows, server=None, **arguments):
    arguments.setdefault("source_id_column", fields[0])
    async with ServerClient(server or make_server()) as session:
        return await session.call(
            "write_results", schema=SCHEMA, table=table, fields=fields, rows=rows, **arguments
        )


async def fetch_rows(conn, table, ids, columns, key="doc_id"):
    records = await conn.fetch(
        f"SELECT {', '.join(columns)} FROM {SCHEMA}.{table} WHERE {key} = ANY($1) ORDER BY {key}",
        ids,
    )
    return [tuple(record) for record in records]


@pytest.mark.asyncio(loop_scope="module")
class TestWriteSemantics:
    async def test_upsert_is_idempotent_for_business_columns(self, fixture_schema):
        ids = new_ids(3)
        rows = [{"doc_id": i, "summary": f"s {i}", "score": n} for n, i in enumerate(ids)]
        first = await write("results", ["doc_id", "summary", "score"], rows, model="m1")
        before = await fetch_rows(fixture_schema, "results", ids, ["doc_id", "summary", "score", "model", "extracted_at"])
        second = await write("results", ["doc_id", "summary", "score"], rows, model="m1")
        after = await fetch_rows(fixture_schema, "results", ids, ["doc_id", "summary", "score", "model", "extracted_at"])
        assert first == second == {"written": 3, "failed_count": 0, "failed": [], "failed_truncated": False}
        assert [row[:4] for row in before] == [row[:4] for row in after]
        assert all(b[4] < a[4] for b, a in zip(before, after))

    async def test_do_nothing_rows_count_as_written(self, fixture_schema):
        rows = [{"sid": n} for n in (11, 12)]
        for _ in range(2):
            result = await write("key_only", ["sid"], rows)
            assert result["written"] == 2 and result["failed_count"] == 0

    async def test_rounds_update_only_their_own_columns(self, fixture_schema):
        ids = new_ids(2)
        await write("results", ["doc_id", "summary"],
                    [{"doc_id": i, "summary": "first"} for i in ids], model="m")
        await write("results", ["doc_id", "score"],
                    [{"doc_id": ids[0], "score": 7}, {"doc_id": ids[1], "score": None}], model="m")
        assert await fetch_rows(fixture_schema, "results", ids, ["summary", "score"]) == [
            ("first", 7), ("first", None)
        ]
        await write("results", ["doc_id", "summary"],
                    [{"doc_id": ids[0], "summary": None}], model="m")
        assert (await fetch_rows(fixture_schema, "results", ids[:1], ["summary", "score"])) == [(None, 7)]

    async def test_field_mapping_and_provenance(self, fixture_schema):
        ids = new_ids(1)
        result = await write(
            "results", ["id", "text"], [{"id": ids[0], "text": "hello"}],
            source_id_column="doc_id",
            field_mapping={"id": "doc_id", "text": "summary"}, model="gpt-x",
        )
        assert result["written"] == 1
        record = await fixture_schema.fetchrow(
            f"SELECT summary, model, extracted_at FROM {SCHEMA}.results WHERE doc_id = $1", ids[0]
        )
        assert record["summary"] == "hello" and record["model"] == "gpt-x"
        assert record["extracted_at"] is not None
        await write("results", ["doc_id"], [{"doc_id": ids[0]}], model="gpt-y")
        assert await fixture_schema.fetchval(
            f"SELECT model FROM {SCHEMA}.results WHERE doc_id = $1", ids[0]
        ) == "gpt-y"

    async def test_model_is_required_when_the_table_has_the_column(self, fixture_schema):
        with pytest.raises(ToolError, match="model argument is required"):
            await write("results", ["doc_id"], [{"doc_id": "x"}])
        with pytest.raises(ToolError, match="does not fit column model"):
            await write("domain_model", ["sid"], [{"sid": 1}], model="m" * 41)
        result = await write("domain_model", ["sid"], [{"sid": 1}], model="short")
        assert result["written"] == 1

    async def test_append_without_a_key_keeps_repeated_source_ids(self, fixture_schema):
        result = await write("random_dist", ["sid", "v"],
                             [{"sid": 5, "v": "a"}, {"sid": 5, "v": "b"}], mode="append")
        assert result["written"] == 2
        assert await fixture_schema.fetchval(
            f"SELECT count(*) FROM {SCHEMA}.random_dist WHERE sid = 5"
        ) == 2

    async def test_append_on_a_key_fails_repeats_and_existing_keys(self, fixture_schema):
        ids = new_ids(3)
        await write("results", ["doc_id"], [{"doc_id": ids[0]}], model="m")
        result = await write(
            "results", ["doc_id"],
            [{"doc_id": ids[0]}, {"doc_id": ids[1]}, {"doc_id": ids[1]}, {"doc_id": ids[2]}],
            mode="append", model="m",
        )
        assert result["written"] == 1
        failed = {f["index"]: f["error"] for f in result["failed"]}
        assert set(failed) == {0, 1, 2}
        assert "duplicate key" in failed[0]
        assert "more than once" in failed[1] and "more than once" in failed[2]

    async def test_append_reports_other_unique_violations_per_row(self, fixture_schema):
        result = await write("other_key", ["sid", "other"],
                             [{"sid": 1, "other": 1}, {"sid": 2, "other": 1}, {"sid": 3, "other": 3}],
                             mode="append")
        assert result["written"] == 2
        assert [f["index"] for f in result["failed"]] == [1]

    async def test_values_round_trip(self, fixture_schema):
        ids = new_ids(2)
        tricky = 'quote " backslash \\ comma , braces {} 中文'
        rows = [{
            "doc_id": ids[0],
            "summary": tricky,
            "label": "abc",
            "tags": ["a,b", "{c}", 'd"e', "f\\g", None, "NULL", ""],
            # box[] separates its elements with ';', not ','.
            "boxes": ["1,1,0,0", "(2,2),(1,1)"],
            "meta": {"k": [1, "two", None], "s": "x"},
            "price": "12345678901234567890.0123456789",
            "big": "9007199254740993",
            # JSON has no NaN or Infinity; a client sends them as strings.
            "ratio": "NaN",
            "flag": False,
            "day": "2026-10-14",
        }, {
            "doc_id": ids[1],
            "summary": "",
            "label": None,
            "tags": [],
            "boxes": None,
            "meta": "just a string",
            "price": 1.5,
            "big": 9007199254740993,
            "ratio": "-Infinity",
            "flag": True,
            "day": None,
        }]
        fields = list(rows[0])
        result = await write("results", fields, rows, model="m")
        assert result == {"written": 2, "failed_count": 0, "failed": [], "failed_truncated": False}
        records = await fixture_schema.fetch(
            f"SELECT summary, label, tags, boxes::text[] AS boxes, meta::text AS meta, "
            f"price::text AS price, big, ratio, flag, day::text AS day "
            f"FROM {SCHEMA}.results WHERE doc_id = ANY($1) ORDER BY doc_id", ids,
        )
        first, second = records
        assert first["summary"] == tricky
        assert first["label"] == "abc"
        assert first["tags"] == ["a,b", "{c}", 'd"e', "f\\g", None, "NULL", ""]
        assert first["boxes"] == ["(1,1),(0,0)", "(2,2),(1,1)"]
        assert first["meta"] == '{"k": [1, "two", null], "s": "x"}'
        assert first["price"] == "12345678901234567890.0123456789"
        assert first["big"] == 9007199254740993
        assert first["ratio"] != first["ratio"]
        assert first["flag"] is False and first["day"] == "2026-10-14"
        assert second["summary"] == "" and second["label"] is None
        assert second["tags"] == [] and second["boxes"] is None
        assert second["meta"] == '"just a string"'
        assert second["price"] == "1.5000000000"
        assert second["big"] == 9007199254740993
        assert second["ratio"] == float("-inf") and second["flag"] is True


@pytest.mark.asyncio(loop_scope="module")
class TestWriteFailures:
    async def test_type_typmod_and_domain_errors_fail_only_their_rows(self, fixture_schema):
        ids = new_ids(4)
        result = await write("results", ["doc_id", "score", "label"], [
            {"doc_id": ids[0], "score": "abc", "label": "ok"},
            {"doc_id": ids[1], "score": 1, "label": "toolong"},
            {"doc_id": ids[2], "score": 2.5, "label": "ok"},
            {"doc_id": ids[3], "score": 3, "label": "ok"},
        ], model="m")
        assert result["written"] == 1
        failed = {f["index"]: f for f in result["failed"]}
        assert set(failed) == {0, 1, 2}
        assert "invalid input syntax for type integer" in failed[0]["error"]
        assert "too long" in failed[1]["error"]
        assert failed[0]["source_id"] == ids[0]
        result = await write("typed", ["sid", "n"], [{"sid": 1, "n": -1}, {"sid": 2, "n": 5}])
        assert result["written"] == 1
        assert "positive" in result["failed"][0]["error"]

    async def test_constraint_violations_fail_only_their_rows(self, fixture_schema):
        ids = new_ids(4)
        result = await write("results", ["doc_id", "score", "required_note"], [
            {"doc_id": ids[0], "score": 1, "required_note": "x"},
            {"doc_id": ids[1], "score": -5, "required_note": "x"},
            {"doc_id": ids[2], "score": 2, "required_note": None},
            {"doc_id": ids[3], "score": 3, "required_note": "x"},
        ], model="m")
        assert result["written"] == 2
        assert result["written"] + result["failed_count"] == 4
        failed = {f["index"]: f["error"] for f in result["failed"]}
        assert "check constraint" in failed[1]
        assert "null value" in failed[2]
        assert await fetch_rows(fixture_schema, "results", ids, ["doc_id"]) == [(ids[0],), (ids[3],)]

    async def test_bad_rows_are_reported_and_the_rest_written(self, fixture_schema):
        result = await write("plain", ["sid", "note"], [
            {"sid": "01", "note": "a"},
            {"sid": 1, "note": "b"},
            {"sid": None, "note": "c"},
            {"sid": 20, "note": {"no": "object"}},
            {"sid": 21},
            {"sid": 22, "note": "x", "extra": 1},
            "not an object",
            {"sid": 23, "note": "fine"},
        ], mode="upsert")
        assert result["written"] == 1
        failed = {f["index"]: f["error"] for f in result["failed"]}
        assert set(failed) == {0, 1, 2, 3, 4, 5, 6}
        assert "more than once" in failed[0] and "more than once" in failed[1]
        assert "is null" in failed[2]
        assert "single value" in failed[3]
        assert "missing" in failed[4]
        assert "not in fields" in failed[5]
        assert "not an object" in failed[6]

    async def test_nested_arrays_fail_the_row(self, fixture_schema):
        ids = new_ids(2)
        result = await write("results", ["doc_id", "tags"], [
            {"doc_id": ids[0], "tags": [["a"]]},
            {"doc_id": ids[1], "tags": "a,b"},
        ], model="m")
        failed = {f["index"]: f["error"] for f in result["failed"]}
        assert "nested arrays" in failed[0]
        assert "takes a JSON array" in failed[1]

    @pytest.mark.parametrize("table, fields, rows, arguments, message", [
        ("results", [], [{"doc_id": "x"}], {"source_id_column": "doc_id"}, "fields must be"),
        ("results", ["doc_id", "summary"], [{"doc_id": "x", "summary": "y"}],
         {"model": None}, "model argument is required"),
        ("results", ["doc_id", "doc_id"], [{"doc_id": "x"}], {}, "more than once"),
        ("results", ["doc_id", "nope"], [{"doc_id": "x", "nope": 1}], {}, "matches no column"),
        ("results", ["doc_id", "a", "b"], [{"doc_id": "x", "a": 1, "b": 2}],
         {"field_mapping": {"a": "summary", "b": "summary"}}, "both map to"),
        ("results", ["doc_id", "m"], [{"doc_id": "x", "m": 1}],
         {"field_mapping": {"m": "model"}}, "fills itself"),
        ("results", ["doc_id"], [{"doc_id": "x"}],
         {"field_mapping": {"ghost": "summary"}}, "not in fields"),
        ("plain", ["sid", "g"], [{"sid": 1, "g": 1}], {}, "generated column"),
        ("plain", ["sid"], [{"sid": 1}], {}, "NOT NULL columns without a default"),
        ("results", ["doc_id", "summary"], [{"doc_id": "x", "summary": "y"}],
         {"source_id_column": "summary"}, "cannot be upserted on"),
        ("results", ["doc_id"], [{"doc_id": "x"}],
         {"source_id_column": "score"}, "not among the columns"),
        ("results", ["doc_id"], [], {}, "rows must be"),
        ("ao", ["sid"], [{"sid": 1}], {"mode": "append"}, "not a heap table"),
        ("v", ["sid"], [{"sid": 1}], {}, "heap table"),
        ("mv", ["sid"], [{"sid": 1}], {}, "heap table"),
        ("generated_model", ["sid"], [{"sid": 1}], {}, "generated column"),
    ])
    async def test_whole_call_refusals_write_nothing(
        self, fixture_schema, table, fields, rows, arguments, message
    ):
        # Count the table each case targets; a view or AO table counts too.
        count = f"SELECT count(*) FROM {SCHEMA}.{table}"
        before = await fixture_schema.fetchval(count)
        arguments.setdefault("model", "m")
        with pytest.raises(ToolError, match=message):
            await write_raw(table, fields, rows, **arguments)
        assert await fixture_schema.fetchval(count) == before

    async def test_row_and_byte_limits(self, fixture_schema):
        server = make_server(write_config(max_rows=2))
        with pytest.raises(ToolError, match="MCP_WRITE_MAX_ROWS"):
            await write("key_only", ["sid"], [{"sid": n} for n in range(3)], server=server)
        server = make_server(write_config(max_bytes=200))
        with pytest.raises(ToolError, match="MCP_WRITE_MAX_BYTES"):
            await write("key_only", ["sid"], [{"sid": 1}], server=server,
                        field_mapping={"sid": "sid"}, model="x" * 200)

    async def test_failed_details_stop_at_one_hundred(self, fixture_schema):
        rows = [{"sid": n, "n": -1} for n in range(1000, 1150)]
        result = await write("typed", ["sid", "n"], rows)
        assert result["failed_count"] == 150
        assert len(result["failed"]) == 100
        assert result["failed_truncated"] is True
        assert result["written"] == 0

    @pytest.mark.parametrize("table, trigger", [
        ("dropping", f"{SCHEMA}.dropping has trigger drops"),
        ("on_update", f"{SCHEMA}.on_update has trigger stamps"),
        ("part_trig", f"{SCHEMA}.part_trig_a has trigger leaf_drops"),
        ("trig", f"{SCHEMA}.trig has trigger on_insert"),
        ("after_insert", f"{SCHEMA}.after_insert has trigger audits"),
        ("after_update", f"{SCHEMA}.after_update has trigger audits"),
        ("statement", f"{SCHEMA}.statement has trigger once"),
        ("part_after", f"{SCHEMA}.part_after_a has trigger leaf_audits"),
        ("wiping", f"{SCHEMA}.wiping has trigger wipes"),
        ("replica", f"{SCHEMA}.replica has trigger on_replica"),
        ("always", f"{SCHEMA}.always has trigger on_always"),
        ("checked", f"{SCHEMA}.checked has trigger checks"),
    ])
    async def test_insert_and_update_triggers_make_a_table_unwritable(self, fixture_schema, table, trigger):
        info = await describe(table)
        assert info["writable"] == {"upsert": False, "append": False}
        for mode in ("upsert", "append"):
            assert any(trigger in reason for reason in info["reasons"][mode]), info["reasons"]
            with pytest.raises(ToolError, match="has trigger .* on INSERT or UPDATE"):
                await write(table, ["sid", "v"], [{"sid": 1, "v": "x"}], mode=mode)
        assert await fixture_schema.fetchval(f"SELECT count(*) FROM {SCHEMA}.{table}") == 0

    @pytest.mark.parametrize("table, rule", [
        ("ruled", f"{SCHEMA}.ruled has rule skip ON INSERT"),
        ("ruled_update", f"{SCHEMA}.ruled_update has rule keep ON UPDATE"),
        ("part_rule", f"{SCHEMA}.part_rule_a has rule leaf_skip ON INSERT"),
    ])
    async def test_insert_and_update_rules_make_a_table_unwritable(self, fixture_schema, table, rule):
        info = await describe(table)
        assert info["writable"] == {"upsert": False, "append": False}
        for mode in ("upsert", "append"):
            assert any(rule in reason for reason in info["reasons"][mode]), info["reasons"]
            with pytest.raises(ToolError, match="has rule"):
                await write(table, ["sid", "v"], [{"sid": 1, "v": "x"}, {"sid": 2, "v": "y"}],
                            mode=mode)
        rows = await fixture_schema.fetch(f"SELECT sid, v FROM {SCHEMA}.{table} ORDER BY sid")
        expected = [(1, "before")] if table == "ruled_update" else []
        assert [tuple(row) for row in rows] == expected

    async def test_an_after_trigger_can_delete_the_rows_an_insert_reports(self, fixture_schema):
        # Why AFTER triggers are refused too: the insert reports its rows,
        # and the trigger has already deleted them.
        async with fixture_schema.transaction():
            status = await fixture_schema.execute(
                f"INSERT INTO {SCHEMA}.wiping SELECT g, 'x' FROM generate_series(1, 10) g"
            )
            assert status == "INSERT 0 10"
            assert await fixture_schema.fetchval(f"SELECT count(*) FROM {SCHEMA}.wiping") == 0

    async def test_disabled_delete_and_internal_triggers_leave_a_table_writable(self, fixture_schema):
        info = await describe("harmless")
        assert info["writable"] == {"upsert": True, "append": True}
        assert {t["name"]: t["enabled"] for t in info["triggers"]} == {
            "after_delete": True, "off_drops": False,
        }
        assert await fixture_schema.fetchval(
            f"SELECT count(*) FROM pg_trigger WHERE tgrelid = '{SCHEMA}.harmless'::regclass "
            f"AND tgisinternal"
        ) > 0
        result = await write("harmless", ["sid", "v"], [{"sid": 1, "v": "x"}, {"sid": 2, "v": "y"}])
        assert result["written"] == 2
        assert await fixture_schema.fetchval(f"SELECT count(*) FROM {SCHEMA}.harmless") == 2


async def write_raw(table, fields, rows, **arguments):
    arguments.setdefault("source_id_column", fields[0] if fields else "doc_id")
    async with ServerClient(make_server()) as session:
        return await session.call(
            "write_results", schema=SCHEMA, table=table, fields=fields, rows=rows, **arguments
        )


@pytest.mark.asyncio(loop_scope="module")
class TestWriteAccess:
    async def test_tables_outside_the_whitelist_are_neither_written_nor_locked(self, fixture_schema):
        holder = await open_connection()
        try:
            await holder.execute("BEGIN")
            await holder.execute(f"LOCK TABLE {SCHEMA}.plain IN ACCESS EXCLUSIVE MODE")
            server = make_server(write_config(tables=((SCHEMA, "results"),), lock_timeout=5))
            started = time.monotonic()
            with pytest.raises(ToolError, match="not in MCP_WRITE_TABLES"):
                await write("plain", ["sid", "note"], [{"sid": 1, "note": "x"}], server=server)
            assert time.monotonic() - started < 2
        finally:
            await holder.close()

    @pytest.mark.parametrize("schema, table, message", [
        (SCHEMA.upper(), "results", "not in MCP_WRITE_TABLES"),
        (f'"{SCHEMA}"', "results", "not in MCP_WRITE_TABLES"),
        # schema.* lets the name through; the exact lookup then finds nothing.
        (SCHEMA, "RESULTS", "does not exist"),
        (SCHEMA, '"results"', "does not exist"),
    ])
    async def test_name_variants_do_not_reach_the_table(self, fixture_schema, schema, table, message):
        async with ServerClient(make_server()) as session:
            with pytest.raises(ToolError, match=message):
                await session.call("write_results", schema=schema, table=table,
                                   source_id_column="doc_id", fields=["doc_id"],
                                   rows=[{"doc_id": "x"}], model="m")

    async def test_column_grants_alone_are_refused_before_locking(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT (doc_id), INSERT (doc_id, summary, model, extracted_at), "
            f"UPDATE (summary, model, extracted_at) ON {SCHEMA}.results TO {{role}}",
        ])
        holder = await open_connection()
        try:
            await holder.execute("BEGIN")
            await holder.execute(f"LOCK TABLE {SCHEMA}.results IN ACCESS EXCLUSIVE MODE")
            started = time.monotonic()
            with pytest.raises(ToolError, match="table-level UPDATE"):
                await write("results", ["doc_id", "summary"], [{"doc_id": "x", "summary": "y"}],
                            server=make_server(write_config(lock_timeout=5), user=role), model="m")
            assert time.monotonic() - started < 2
        finally:
            await holder.close()

    async def test_missing_column_privileges_are_named(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT UPDATE ON {SCHEMA}.results TO {{role}}",
            f"GRANT INSERT (doc_id, summary, model, extracted_at) ON {SCHEMA}.results TO {{role}}",
        ])
        server = make_server(user=role)
        with pytest.raises(ToolError, match="INSERT on column score") as refused:
            await write("results", ["doc_id", "score"], [{"doc_id": "x", "score": 1}],
                        server=server, model="m")
        assert "SELECT on column doc_id" in str(refused.value)

    async def test_upsert_needs_select_on_the_source_column(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT INSERT, UPDATE ON {SCHEMA}.results TO {{role}}",
        ])
        with pytest.raises(ToolError, match="SELECT on column doc_id"):
            await write("results", ["doc_id"], [{"doc_id": "x"}],
                        server=make_server(user=role), model="m")
        ids = new_ids(1)
        result = await write("results", ["doc_id"], [{"doc_id": ids[0]}], mode="append",
                             server=make_server(user=role), model="m")
        assert result["written"] == 1


@pytest.mark.asyncio(loop_scope="module")
class TestWriteLocks:
    async def held(self, table, mode):
        holder = await open_connection()
        await holder.execute("BEGIN")
        await holder.execute(f"LOCK TABLE {SCHEMA}.{table} IN {mode} MODE")
        return holder

    async def test_lock_wait_times_out_and_the_pool_recovers(self, fixture_schema):
        server = make_server(write_config(lock_timeout=0.5))
        ids = new_ids(1)
        async with ServerClient(server) as session:
            holder = await self.held("results", "SHARE")
            try:
                started = time.monotonic()
                with pytest.raises(ToolError, match="MCP_WRITE_LOCK_TIMEOUT"):
                    await session.call("write_results", schema=SCHEMA, table="results",
                                       source_id_column="doc_id", fields=["doc_id"],
                                       rows=[{"doc_id": ids[0]}], model="m")
                assert 0.4 < time.monotonic() - started < 5
            finally:
                await holder.close()
            result = await session.call("write_results", schema=SCHEMA, table="results",
                                        source_id_column="doc_id", fields=["doc_id"],
                                        rows=[{"doc_id": ids[0]}], model="m")
            assert result["written"] == 1

    async def test_upsert_locks_exclusive_and_append_row_exclusive(self, fixture_schema):
        server = make_server(write_config(lock_timeout=0.5))
        holder = await self.held("key_only", "ROW SHARE")
        try:
            with pytest.raises(ToolError, match="MCP_WRITE_LOCK_TIMEOUT"):
                await write("key_only", ["sid"], [{"sid": 31}], server=server)
            result = await write("key_only", ["sid"], [{"sid": 32}], mode="append",
                                 server=make_server(write_config(lock_timeout=0.5)))
            assert result["written"] == 1
        finally:
            await holder.close()

    async def test_partitioned_append_keeps_attach_partition_out(self, fixture_schema):
        # SHARE UPDATE EXCLUSIVE conflicts with itself and with what ATTACH
        # PARTITION takes; ROW EXCLUSIVE on a plain table conflicts with neither.
        holder = await self.held("part", "SHARE UPDATE EXCLUSIVE")
        try:
            with pytest.raises(ToolError, match="MCP_WRITE_LOCK_TIMEOUT"):
                await write("part", ["sid", "v"], [{"sid": 1, "v": "a"}], mode="append",
                            server=make_server(write_config(lock_timeout=0.5)))
        finally:
            await holder.close()
        holder = await self.held("random_dist", "SHARE UPDATE EXCLUSIVE")
        try:
            result = await write("random_dist", ["sid", "v"], [{"sid": 40, "v": "a"}],
                                 mode="append", server=make_server(write_config(lock_timeout=0.5)))
            assert result["written"] == 1
        finally:
            await holder.close()

    async def test_partitioned_upsert_and_append_route_rows(self, fixture_schema):
        result = await write("part", ["sid", "v"],
                             [{"sid": 5, "v": "a"}, {"sid": 150, "v": "b"}, {"sid": 500, "v": "c"}])
        assert result["written"] == 2
        assert "no partition" in result["failed"][0]["error"]
        assert await fixture_schema.fetchval(
            f"SELECT count(*) FROM {SCHEMA}.part_b WHERE sid = 150"
        ) == 1


@pytest.mark.asyncio(loop_scope="module")
class TestConflictTarget:
    async def test_conflict_target_names_the_chosen_index(self, fixture_schema):
        from cbmcp.writeback import ResultWriter, build_insert_sql, inspect_table, resolve_relation

        relation = await resolve_relation(fixture_schema, SCHEMA, "two_keys")
        info = await inspect_table(fixture_schema, relation["relid"], SCHEMA, "two_keys", "r")
        writer = ResultWriter(None, write_config())
        plan = await writer._plan(fixture_schema, info, "sid", ["sid", "v"], "upsert", {}, None)
        sql = build_insert_sql(plan)
        assert 'ON CONFLICT ("sid" COLLATE pg_catalog."default" pg_catalog.text_ops)' in sql

    async def test_upsert_with_a_second_non_default_index(self, fixture_schema):
        await write("two_keys", ["sid", "v"], [{"sid": "a", "v": "1"}])
        result = await write("two_keys", ["sid", "v"], [{"sid": "a", "v": "2"}])
        assert result["written"] == 1
        assert await fixture_schema.fetchval(
            f"SELECT v FROM {SCHEMA}.two_keys WHERE sid = 'a'"
        ) == "2"


@pytest_asyncio.fixture(loop_scope="module")
async def http_server(fixture_schema):
    """A server of its own over HTTP, with writing on for the test schema."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = dict(os.environ, MCP_ENABLE_WRITE="true", MCP_WRITE_TABLES=f"{SCHEMA}.*")
    process = subprocess.Popen(
        [sys.executable, "-m", "cbmcp", "--mode", "http", "--host", "127.0.0.1",
         "--port", str(port), "--path", "/mcp/"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"HTTP server exited: {process.stderr.read().decode()[-2000:]}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("HTTP server did not start within 20 s")
                await asyncio.sleep(0.1)
        yield f"http://127.0.0.1:{port}/mcp/"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.mark.asyncio(loop_scope="module")
async def test_write_tools_over_http(http_server, fixture_schema):
    client = await CloudberryMCPClient.create(mode="http", server_url=http_server)
    try:
        info = unwrap(await client.call_tool(
            "describe_result_table", {"schema": SCHEMA, "table": "results"}
        ))
        assert info["upsert_keys"] == ["doc_id"]
        ids = new_ids(2)
        result = unwrap(await client.call_tool("write_results", {
            "schema": SCHEMA, "table": "results", "source_id_column": "doc_id",
            "fields": ["doc_id", "score"],
            "rows": [{"doc_id": ids[0], "score": 1}, {"doc_id": ids[1], "score": "x"}],
            "model": "http",
        }))
        assert result["written"] == 1
        assert result["failed"][0]["source_id"] == ids[1]
        with pytest.raises(ToolError, match="not in MCP_WRITE_TABLES"):
            await client.call_tool("write_results", {
                "schema": "public", "table": "results", "source_id_column": "doc_id",
                "fields": ["doc_id"], "rows": [{"doc_id": "x"}],
            })
    finally:
        await client.close()


@pytest.mark.asyncio(loop_scope="module")
class TestReviewFollowUps:
    """Cases added after the independent review of the first version."""

    @pytest.mark.parametrize("table, mode, expected", [
        ("aoco", "append", "not a heap table"),
        ("part_ao", "append", "partition"),
        ("part_def", "append", "deferrable constraint"),
        ("part_ct", "append", "deferrable constraint trigger"),
        ("deferred_key", "append", "deferrable constraint"),
        ("bad_model", "upsert", "column model must be"),
        ("bad_time", "upsert", "column extracted_at must be"),
        ("ext", "append", "foreign table"),
    ])
    async def test_write_refuses_every_unwritable_kind(self, fixture_schema, table, mode, expected):
        with pytest.raises(ToolError, match=expected):
            await write(table, ["sid"], [{"sid": 1}], mode=mode, model="m")

    async def test_insert_only_role_cannot_append_to_a_partitioned_table(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT, INSERT ON {SCHEMA}.part TO {{role}}",
        ])
        holder = await open_connection()
        try:
            await holder.execute("BEGIN")
            await holder.execute(f"LOCK TABLE {SCHEMA}.part IN ACCESS EXCLUSIVE MODE")
            started = time.monotonic()
            with pytest.raises(ToolError, match="INSERT alone is not enough"):
                await write("part", ["sid", "v"], [{"sid": 1, "v": "a"}], mode="append",
                            server=make_server(write_config(lock_timeout=5), user=role))
            assert time.monotonic() - started < 2
        finally:
            await holder.close()

    async def test_search_path_does_not_resolve_table_names(self, fixture_schema):
        role = await create_role(fixture_schema, [
            f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.results TO {{role}}",
            f"ALTER ROLE {{role}} SET search_path = {SCHEMA}",
        ])
        server = make_server(write_config(tables=((SCHEMA, None), ("public", None))), user=role)
        async with ServerClient(server) as session:
            with pytest.raises(ToolError, match="does not exist"):
                await session.call("describe_result_table", schema="public", table="results")
            with pytest.raises(ToolError, match="does not exist"):
                await session.call(
                    "write_results", schema="public", table="results",
                    source_id_column="doc_id", fields=["doc_id"], rows=[{"doc_id": "sp"}],
                    model="m",
                )
            info = await session.call("describe_result_table", schema=SCHEMA, table="results")
        assert info["table"] == "results"
        assert await fixture_schema.fetchval(
            f"SELECT count(*) FROM {SCHEMA}.results WHERE doc_id = 'sp'"
        ) == 0

    async def test_names_that_need_quoting(self, fixture_schema):
        table = 'we"ird,t.b'
        info = await describe(table)
        assert info["upsert_keys"] == ["Id"]
        result = await write(table, ["Id", "a b"], [{"Id": 1, "a b": "x"}, {"Id": 1, "a b": "y"},
                                                     {"Id": 2, "a b": "z"}])
        assert result["written"] == 1
        result = await write(table, ["Id", "a b"], [{"Id": 2, "a b": "w"}])
        assert result["written"] == 1
        assert await fixture_schema.fetchval(
            f'SELECT "a b" FROM {SCHEMA}."we""ird,t.b" WHERE "Id" = 2'
        ) == "w"

    async def test_more_value_types_round_trip(self, fixture_schema):
        result = await write("more_types", ["sid", "ts", "tstz", "t", "f", "pos"], [{
            "sid": 1, "ts": "2026-10-14 08:30:00", "tstz": "2026-10-14T08:30:00+00:00",
            "t": "23:59:59.5", "f": "Infinity", "pos": 7,
        }])
        assert result["written"] == 1
        record = await fixture_schema.fetchrow(
            f"SELECT ts::text AS ts, (tstz AT TIME ZONE 'UTC')::text AS tstz, t::text AS t, "
            f"f, pos FROM {SCHEMA}.more_types WHERE sid = 1"
        )
        assert dict(record) == {
            "ts": "2026-10-14 08:30:00", "tstz": "2026-10-14 08:30:00",
            "t": "23:59:59.5", "f": float("inf"), "pos": 7,
        }

    @pytest.mark.parametrize("change", [
        {"port": 1},                                    # refused: not a PostgresError
        {"username": "secret_user_name"},               # unknown role: SQLSTATE 28000
        {"database": "secret_database_name"},           # unknown database: 3D000
    ])
    async def test_connection_errors_do_not_reveal_the_server(self, fixture_schema, change):
        db_config = replace(DatabaseConfig.from_env(), **change)
        server = CloudberryMCPServer(ServerConfig.from_env(), db_config, write_config())
        async with ServerClient(server) as session:
            for tool, arguments in [
                ("describe_result_table", {"schema": SCHEMA, "table": "results"}),
                ("write_results", {"schema": SCHEMA, "table": "results",
                                   "source_id_column": "doc_id", "fields": ["doc_id"],
                                   "rows": [{"doc_id": "x"}], "model": "m"}),
            ]:
                with pytest.raises(ToolError) as refused:
                    await session.call(tool, **arguments)
                message = str(refused.value)
                assert "could not be reached" in message
                for secret in (db_config.host, ":1", "secret_user_name",
                               "secret_database_name", "Errno"):
                    assert secret not in message, (secret, message)


@pytest.mark.asyncio(loop_scope="module")
class TestConnectionRefusals:
    """Refusals that come from the server while a connection is opened."""

    async def test_no_connect_privilege_and_closed_database_stay_hidden(self, fixture_schema):
        database = f"{SCHEMA}_secretdb"
        role = await create_role(fixture_schema, [])
        await fixture_schema.execute(f'CREATE DATABASE "{database}"')
        try:
            await fixture_schema.execute(f'REVOKE CONNECT ON DATABASE "{database}" FROM PUBLIC')
            cases = [
                # The role exists, the database exists, but CONNECT is revoked: 42501.
                replace(DatabaseConfig.from_env(), database=database, username=role, password=""),
            ]
            for db_config in cases:
                server = CloudberryMCPServer(ServerConfig.from_env(), db_config, write_config())
                async with ServerClient(server) as session:
                    with pytest.raises(ToolError) as refused:
                        await session.call("describe_result_table", schema=SCHEMA, table="results")
                message = str(refused.value)
                assert "could not be reached" in message
                assert database not in message and role not in message
            # A database that accepts no connections at all: 55000.
            await fixture_schema.execute(f'ALTER DATABASE "{database}" ALLOW_CONNECTIONS false')
            db_config = replace(DatabaseConfig.from_env(), database=database)
            server = CloudberryMCPServer(ServerConfig.from_env(), db_config, write_config())
            async with ServerClient(server) as session:
                with pytest.raises(ToolError) as refused:
                    await session.call("write_results", schema=SCHEMA, table="results",
                                       source_id_column="doc_id", fields=["doc_id"],
                                       rows=[{"doc_id": "x"}], model="m")
            message = str(refused.value)
            assert "could not be reached" in message and database not in message
        finally:
            await fixture_schema.execute(f'DROP DATABASE IF EXISTS "{database}"')


class FailingTransaction:
    def __init__(self, error):
        self.error = error

    async def commit(self):
        raise self.error


@pytest.mark.asyncio
class TestCommitOutcome:
    async def test_a_lost_connection_at_commit_is_reported_as_unknown(self):
        from cbmcp.writeback import CommitOutcomeUnknown, ResultWriter

        with pytest.raises(CommitOutcomeUnknown, match="may or may not have been written"):
            await ResultWriter._commit(FailingTransaction(ConnectionResetError("peer reset")))

    @pytest.mark.parametrize("error", [
        asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed"),
        asyncpg.exceptions.ConnectionFailureError("connection failure"),
        asyncpg.exceptions.AdminShutdownError("terminating connection"),
    ])
    async def test_a_connection_error_from_asyncpg_at_commit_is_unknown(self, error):
        from cbmcp.writeback import CommitOutcomeUnknown, ResultWriter

        with pytest.raises(CommitOutcomeUnknown, match="may or may not have been written"):
            await ResultWriter._commit(FailingTransaction(error))

    async def test_a_refused_commit_says_nothing_was_written(self):
        from cbmcp.writeback import CommitOutcomeUnknown, ResultWriter, WriteError

        error = asyncpg.exceptions.SerializationError("could not serialize")
        with pytest.raises(WriteError, match="nothing was written") as refused:
            await ResultWriter._commit(FailingTransaction(error))
        assert not isinstance(refused.value, CommitOutcomeUnknown)


def failing_release(server):
    """Make handing a connection back to the pool fail, as asyncpg does when
    resetting it fails because the connection broke after the last reply."""
    from contextlib import asynccontextmanager

    acquire = server.db_manager.get_connection

    @asynccontextmanager
    async def get_connection():
        # The release fails whether the call succeeded or raised.
        try:
            async with acquire() as conn:
                yield conn
        finally:
            raise asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed")

    server.db_manager.get_connection = get_connection
    return server


@pytest.mark.asyncio(loop_scope="module")
class TestConnectionRelease:
    async def test_a_failed_release_after_commit_keeps_the_result(self, fixture_schema):
        ids = new_ids(2)
        result = await write("results", ["doc_id", "summary"],
                             [{"doc_id": ids[0], "summary": "a"}, {"doc_id": ids[1], "summary": "b"}],
                             server=failing_release(make_server()), model="m")
        assert result["written"] == 2
        assert await fixture_schema.fetchval(
            f"SELECT count(*) FROM {SCHEMA}.results WHERE doc_id = ANY($1::text[])", ids
        ) == 2

    async def test_a_failed_release_keeps_the_error_of_the_call(self, fixture_schema):
        with pytest.raises(ToolError, match="has trigger drops") as refused:
            await write("dropping", ["sid", "v"], [{"sid": 1, "v": "x"}],
                        server=failing_release(make_server()))
        assert "connection was closed" not in str(refused.value)

    async def test_a_failed_release_does_not_swallow_a_cancellation(self, fixture_schema):
        from cbmcp.writeback import ResultWriter

        writer = ResultWriter(failing_release(make_server()).db_manager, write_config())
        with pytest.raises(asyncio.CancelledError):
            async with writer._connection():
                raise asyncio.CancelledError()

    async def test_a_failed_release_after_describe_keeps_the_answer(self, fixture_schema):
        async with ServerClient(failing_release(make_server())) as session:
            info = await session.call("describe_result_table", schema=SCHEMA, table="results")
        assert info["upsert_keys"] == ["doc_id"]


@pytest_asyncio.fixture(loop_scope="module")
async def stdio_client(fixture_schema):
    """The server as a separate stdio process, with writing on for the test schema."""
    from fastmcp.client.transports import StdioTransport

    env = dict(os.environ, MCP_ENABLE_WRITE="true", MCP_WRITE_TABLES=f"{SCHEMA}.*")
    transport = StdioTransport(
        command=sys.executable, args=["-m", "cbmcp", "--mode", "stdio"], env=env,
    )
    async with Client(transport) as client:
        yield client


@pytest.mark.asyncio(loop_scope="module")
async def test_write_tools_over_stdio(stdio_client, fixture_schema):
    info = unwrap(await stdio_client.call_tool(
        "describe_result_table", {"schema": SCHEMA, "table": "results"}
    ))
    assert info["upsert_keys"] == ["doc_id"]
    ids = new_ids(2)
    result = unwrap(await stdio_client.call_tool("write_results", {
        "schema": SCHEMA, "table": "results", "source_id_column": "doc_id",
        "fields": ["doc_id", "score"],
        "rows": [{"doc_id": ids[0], "score": 1}, {"doc_id": ids[1], "score": "x"}],
        "model": "stdio",
    }))
    assert result["written"] == 1
    assert result["failed"][0]["source_id"] == ids[1]
    with pytest.raises(ToolError, match="not in MCP_WRITE_TABLES"):
        await stdio_client.call_tool("write_results", {
            "schema": "public", "table": "results", "source_id_column": "doc_id",
            "fields": ["doc_id"], "rows": [{"doc_id": "x"}],
        })
