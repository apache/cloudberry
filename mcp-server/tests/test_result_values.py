#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
Query result value test module

Covers how execute_query and the inspection tools hand values back: types
that have no JSON form of their own, the row and value limits, and the
declared types that list_columns and get_table_ddl report.

These are integration tests. They build their own fixture tables and need a
reachable cluster, but no extension; without a cluster the module skips.
"""

import json
import os
import uuid

import pytest
import pytest_asyncio

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig

# A suffix keeps two runs against one database from dropping each other's
# fixtures.
SCHEMA = f"cbmcp_values_test_{uuid.uuid4().hex[:8]}"
MANY_ROWS = 1500

# The limits under test, set here so that a developer's .env cannot change
# them.  load_dotenv() does not override a variable that is already set.
MAX_ROWS = 1000
MAX_VALUE_LENGTH = 10000
MAX_RESULT_BYTES = 1000000
LIMITS = {
    "MCP_MAX_ROWS": str(MAX_ROWS),
    "MCP_MAX_VALUE_LENGTH": str(MAX_VALUE_LENGTH),
    "MCP_MAX_RESULT_BYTES": str(MAX_RESULT_BYTES),
}


def unwrap(result):
    """Return the payload of a tool call across fastmcp result shapes."""
    return result.data if hasattr(result, "data") else result


async def open_connection():
    import asyncpg

    config = DatabaseConfig.from_env()
    return await asyncpg.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.username,
        password=config.password or None,
    )


@pytest.fixture(scope="module", autouse=True)
def result_limits():
    with pytest.MonkeyPatch.context() as patch:
        for name, value in LIMITS.items():
            patch.setenv(name, value)
        yield


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_tables(result_limits):
    try:
        conn = await open_connection()
    except Exception as e:
        pytest.skip(f"Skipping result value tests - no reachable cluster: {e}")

    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        await conn.execute(f"CREATE TYPE {SCHEMA}.pair AS (a integer, b text)")
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.typed (
                id integer NOT NULL,
                emb real[],
                price numeric(10,2) DEFAULT 0,
                label varchar(20),
                p {SCHEMA}.pair,
                ps {SCHEMA}.pair[],
                raw bytea,
                span int4range
            ) DISTRIBUTED BY (id)
            """
        )
        await conn.execute(
            f"INSERT INTO {SCHEMA}.typed VALUES "
            f"(1, '{{1.5,2.5}}', 9.50, 'x', ROW(1, 'one'), "
            f"ARRAY[ROW(2, 'two')::{SCHEMA}.pair, ROW(3, NULL)::{SCHEMA}.pair], "
            f"'\\xff00fe'::bytea, '[1,5)')"
        )
        await conn.execute(
            f"CREATE TABLE {SCHEMA}.many AS "
            f"SELECT g AS id FROM generate_series(1, {MANY_ROWS}) g DISTRIBUTED BY (id)"
        )
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.generated (
                id integer GENERATED ALWAYS AS IDENTITY,
                a integer,
                b integer GENERATED ALWAYS AS (a * 2) STORED,
                c text DEFAULT 'x'
            ) DISTRIBUTED BY (a)
            """
        )
        await conn.execute(f"CREATE SEQUENCE {SCHEMA}.seq")
        await conn.execute(f"CREATE TABLE {SCHEMA}.calls (n integer) DISTRIBUTED RANDOMLY")
        await conn.execute(
            f"CREATE FUNCTION {SCHEMA}.log_call(i integer) RETURNS integer LANGUAGE sql "
            f"VOLATILE AS 'INSERT INTO {SCHEMA}.calls VALUES (i) RETURNING i'"
        )
        yield conn
    finally:
        try:
            if not conn.is_closed():
                await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        finally:
            await conn.close()


@pytest_asyncio.fixture(loop_scope="module")
async def client(fixture_tables):
    instance = await CloudberryMCPClient.create(mode="stdio")
    yield instance
    await instance.close()


async def query(client, sql, **arguments):
    return unwrap(await client.call_tool("execute_query", {"query": sql, **arguments}))


@pytest.mark.asyncio(loop_scope="module")
class TestValuesWithoutAJsonForm:
    """Values the serializer cannot write on its own used to fail the call."""

    async def test_non_utf8_bytea_comes_back_as_hex(self, client):
        result = await query(client, f"SELECT raw FROM {SCHEMA}.typed")
        assert result["rows"] == [["\\xff00fe"]]

    async def test_composite_becomes_an_object(self, client):
        result = await query(client, f"SELECT p, ps FROM {SCHEMA}.typed")
        assert result["rows"] == [
            [{"a": 1, "b": "one"}, [{"a": 2, "b": "two"}, {"a": 3, "b": None}]]
        ]

    async def test_range_becomes_an_object(self, client):
        result = await query(client, f"SELECT span FROM {SCHEMA}.typed")
        assert result["rows"][0][0] == {
            "lower": 1, "upper": 5, "lower_inc": True, "upper_inc": False, "isempty": False
        }

    async def test_real_array_stays_a_list_of_numbers(self, client):
        result = await query(client, f"SELECT emb FROM {SCHEMA}.typed")
        assert result["rows"] == [[[1.5, 2.5]]]

    async def test_numeric_comes_back_as_a_string(self, client):
        result = await query(client, f"SELECT price FROM {SCHEMA}.typed")
        assert result["rows"] == [["9.50"]]

    async def test_nan_and_infinity_are_not_null(self, client):
        result = await query(
            client,
            "SELECT 'NaN'::float8, 'Infinity'::float8, '-Infinity'::float4, "
            "'NaN'::numeric, NULL::float8",
        )
        assert result["rows"] == [["NaN", "Infinity", "-Infinity", "NaN", None]]

    async def test_bit_string_and_geometry(self, client):
        result = await query(
            client,
            "SELECT B'101', '((0,0),(1,1))'::path, '[(0,0),(1,1)]'::path, "
            "'((0,0),(1,1),(1,0))'::polygon, '(1,2)'::point",
        )
        assert result["rows"] == [[
            "101",
            {"points": [[0.0, 0.0], [1.0, 1.0]], "is_closed": True},
            {"points": [[0.0, 0.0], [1.0, 1.0]], "is_closed": False},
            [[0.0, 0.0], [1.0, 1.0], [1.0, 0.0]],
            [1.0, 2.0],
        ]]

    async def test_a_type_with_a_text_form_uses_it(self, client):
        result = await query(client, "SELECT '10.0.0.1'::inet, '10.0.0.0/8'::cidr")
        assert result["rows"] == [["10.0.0.1", "10.0.0.0/8"]]


@pytest.mark.asyncio(loop_scope="module")
class TestResultLimits:
    """How much of a result one call returns."""

    async def test_select_without_limit_stops_at_max_rows(self, client):
        result = await query(client, f"SELECT id FROM {SCHEMA}.many ORDER BY id", max_rows=10)
        assert result["rows"] == [[i] for i in range(1, 11)]
        assert result["truncated"] is True
        assert result["max_rows"] == 10

    async def test_default_limit_is_the_server_maximum(self, client):
        maximum = MAX_ROWS
        result = await query(client, f"SELECT id FROM {SCHEMA}.many")
        assert result["row_count"] == min(MANY_ROWS, maximum)
        assert result["truncated"] is (MANY_ROWS > maximum)
        assert result["max_rows"] == maximum

    async def test_result_exactly_at_the_limit_is_not_truncated(self, client):
        result = await query(client, f"SELECT id FROM {SCHEMA}.many WHERE id <= 5", max_rows=5)
        assert result["row_count"] == 5
        assert result["truncated"] is False

    async def test_max_rows_above_the_server_maximum_is_rejected(self, client):
        maximum = MAX_ROWS
        result = await query(client, "SELECT 1", max_rows=maximum + 1)
        assert result["error"] == f"max_rows must be between 1 and {maximum}, got {maximum + 1}"

    async def test_long_value_is_cut_and_reported(self, client):
        maximum = MAX_VALUE_LENGTH
        result = await query(client, f"SELECT 1 AS id, repeat('x', {maximum + 5}) AS t")
        assert len(result["rows"][0][1]) == maximum
        assert result["truncated_values"] == 1
        assert result["truncated_columns"] == ["t"]
        assert result["max_value_length"] == maximum

    async def test_a_cut_inside_an_array_names_the_outer_column(self, client):
        result = await query(
            client, f"SELECT 1 AS id, ARRAY['short', repeat('y', {MAX_VALUE_LENGTH + 1})] AS words"
        )
        assert [len(word) for word in result["rows"][0][1]] == [5, MAX_VALUE_LENGTH]
        assert result["truncated_columns"] == ["words"]

    async def test_a_cut_bytea_still_reads_back(self, client, fixture_tables):
        result = await query(
            client, f"SELECT decode(repeat('ab', {MAX_VALUE_LENGTH}), 'hex') AS raw"
        )
        value = result["rows"][0][0]
        assert len(value) <= MAX_VALUE_LENGTH
        assert result["truncated_columns"] == ["raw"]
        # An even number of hex digits after \x, so bytea accepts it.
        assert await fixture_tables.fetchval("SELECT length($1::text::bytea)", value) == (len(value) - 2) // 2

    async def test_one_row_over_the_byte_budget_returns_no_rows(self, client):
        result = await query(client, "SELECT array_agg(g) FROM generate_series(1, 300000) g")
        assert result["rows"] == []
        assert result["truncated"] is True
        assert result["truncated_by"] == "max_result_bytes"
        assert result["max_result_bytes"] == MAX_RESULT_BYTES

    async def test_rows_stop_at_the_byte_budget(self, client):
        result = await query(
            client, "SELECT g, repeat('z', 9000) FROM generate_series(1, 200) g ORDER BY g"
        )
        assert 100 <= result["row_count"] < 200
        assert result["rows"][0][0] == 1
        assert result["truncated_by"] == "max_result_bytes"
        # The budget bounds the whole result, not only its rows.
        assert len(json.dumps(result).encode()) <= MAX_RESULT_BYTES

    async def test_row_limit_reports_what_stopped_it(self, client):
        result = await query(client, f"SELECT id FROM {SCHEMA}.many", max_rows=3)
        assert result["truncated_by"] == "max_rows"

    async def test_params_must_be_a_list(self, client):
        # The tool schema refuses it before the query runs.
        with pytest.raises(Exception, match="params"):
            await client.call_tool("execute_query", {"query": "SELECT $1::int", "params": {"a": 1}})

    async def test_empty_params_run_the_query(self, client):
        result = await query(client, "SELECT 1", params=[])
        assert result["rows"] == [[1]]

    async def test_short_values_report_nothing(self, client):
        result = await query(client, "SELECT 'short' AS t")
        assert "truncated_values" not in result

    async def test_empty_result_still_names_its_columns(self, client):
        result = await query(client, f"SELECT id, label FROM {SCHEMA}.typed WHERE false")
        assert result["columns"] == ["id", "label"]
        assert result["rows"] == []


@pytest.mark.asyncio(loop_scope="module")
class TestReadOnly:
    """execute_query with readonly left on runs in a READ ONLY transaction."""

    async def test_the_transaction_is_read_only(self, client):
        result = await query(client, "SELECT current_setting('transaction_read_only')")
        assert result["rows"] == [["on"]]

    async def test_a_select_that_writes_is_refused(self, client):
        result = await query(client, f"SELECT nextval('{SCHEMA}.seq')")
        assert "read-only transaction" in result["error"]

    async def test_without_readonly_one_row_past_the_limit_is_evaluated(self, client, fixture_tables):
        # Reading one row past max_rows is how truncation is detected, so a
        # function with a side effect runs for that row too.
        # A sequence would not show it: Cloudberry hands out values in
        # batches, so last_value runs ahead of the calls.
        result = await query(
            client,
            f"SELECT {SCHEMA}.log_call(g) FROM generate_series(1, 10) g",
            readonly=False, max_rows=1,
        )
        assert result["row_count"] == 1
        assert result["truncated"] is True
        assert await fixture_tables.fetchval(f"SELECT count(*) FROM {SCHEMA}.calls") == 2


@pytest.mark.asyncio(loop_scope="module")
class TestDeclaredTypes:
    """list_columns and get_table_ddl report the type as declared."""

    async def test_list_columns_reports_the_declared_type(self, client):
        columns = unwrap(
            await client.call_tool("list_columns", {"schema": SCHEMA, "table": "typed"})
        )
        by_name = {column["column_name"]: column for column in columns}
        assert by_name["emb"]["data_type"] == "ARRAY"
        assert by_name["emb"]["type"] == "real[]"
        assert by_name["price"]["type"] == "numeric(10,2)"
        assert by_name["label"]["type"] == "character varying(20)"

    async def test_ddl_uses_declared_types(self, client):
        ddl = unwrap(
            await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "typed"})
        )
        assert ddl.startswith(f"CREATE TABLE {SCHEMA}.typed (\n")
        assert "  id integer NOT NULL,\n" in ddl
        assert "  emb real[],\n" in ddl
        assert "  price numeric(10,2) DEFAULT 0" in ddl
        assert f"  ps {SCHEMA}.pair[],\n" in ddl

    async def test_ddl_recreates_the_table(self, client, fixture_tables):
        ddl = unwrap(
            await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "typed"})
        )
        await fixture_tables.execute(
            ddl.replace(f"{SCHEMA}.typed (", f"{SCHEMA}.typed_copy (", 1)
        )
        copied = await fixture_tables.fetch(
            "SELECT attname, format_type(atttypid, atttypmod) AS type FROM pg_attribute "
            "WHERE attrelid = $1::regclass AND attnum > 0 ORDER BY attnum",
            f"{SCHEMA}.typed_copy",
        )
        original = await fixture_tables.fetch(
            "SELECT attname, format_type(atttypid, atttypmod) AS type FROM pg_attribute "
            "WHERE attrelid = $1::regclass AND attnum > 0 ORDER BY attnum",
            f"{SCHEMA}.typed",
        )
        await fixture_tables.execute(f"DROP TABLE {SCHEMA}.typed_copy")
        assert [tuple(r) for r in copied] == [tuple(r) for r in original]

    async def test_ddl_spells_generated_and_identity_columns(self, client):
        ddl = unwrap(
            await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "generated"})
        )
        assert "  id integer NOT NULL GENERATED ALWAYS AS IDENTITY,\n" in ddl
        assert "  b integer GENERATED ALWAYS AS ((a * 2)) STORED,\n" in ddl
        assert "  c text DEFAULT 'x'::text\n" in ddl

    async def test_ddl_of_something_that_is_not_a_table(self, client):
        ddl = unwrap(
            await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "pair"})
        )
        assert ddl == f'Table DDL not found: "{SCHEMA}"."pair" is not a table'

    async def test_ddl_of_a_partitioned_table(self, client, fixture_tables):
        await fixture_tables.execute(
            f'CREATE TABLE {SCHEMA}.parted (a integer, b text COLLATE "C") '
            f"PARTITION BY RANGE (a) DISTRIBUTED BY (a)"
        )
        try:
            ddl = unwrap(
                await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "parted"})
            )
        finally:
            await fixture_tables.execute(f"DROP TABLE {SCHEMA}.parted")
        assert ddl == (
            f"CREATE TABLE {SCHEMA}.parted (\n  a integer,\n"
            f'  b text COLLATE pg_catalog."C"\n)\nPARTITION BY RANGE (a);'
        )

    async def test_ddl_shows_no_option_value_of_another_wrapper(self, client, fixture_tables):
        # An FDW with no validator takes any option, which is how a PXF or s3
        # location carrying keys looks to the catalog.  No pattern recognises
        # every way of writing a credential, so none of these values is shown.
        wrapper = f"{SCHEMA}_fdw"
        try:
            await fixture_tables.execute(f"CREATE FOREIGN DATA WRAPPER {wrapper}")
        except Exception as e:
            pytest.skip(f"cannot create a foreign data wrapper: {e}")
        try:
            await fixture_tables.execute(f"CREATE SERVER {wrapper}_srv FOREIGN DATA WRAPPER {wrapper}")
            await fixture_tables.execute(
                f"CREATE FOREIGN TABLE {SCHEMA}.ext (id integer OPTIONS (api_token 't0k')) "
                f"SERVER {wrapper}_srv OPTIONS ("
                f"location 's3://b/x?ACCESS_KEY=AKIA1&SECRET_KEY=abc/def', "
                f"sas 'https://a.blob.core.windows.net/c?sp=r&sig=SIG1', "
                f"dsn 'https://user:pw1@host/x', conf '{{\"password\":\"pw2\"}}', "
                f"note 'it''s fine')"
            )
            ddl = unwrap(
                await client.call_tool("get_table_ddl", {"schema": SCHEMA, "table": "ext"})
            )
        finally:
            await fixture_tables.execute(f"DROP FOREIGN DATA WRAPPER {wrapper} CASCADE")
        for secret in ("AKIA1", "abc/def", "SIG1", "pw1", "pw2", "t0k"):
            assert secret not in ddl
        assert "id integer OPTIONS (api_token '<redacted>')" in ddl
        assert (
            "OPTIONS (location '<redacted>', sas '<redacted>', dsn '<redacted>', "
            "conf '<redacted>', note '<redacted>');"
        ) in ddl

    async def test_list_tables_rejects_an_unknown_type(self, client):
        with pytest.raises(Exception, match="table_type 'view' is not supported"):
            await client.call_tool("list_tables", {"schema": SCHEMA, "table_type": "view"})

    async def test_list_tables_base_lists_ordinary_tables(self, client):
        tables = unwrap(
            await client.call_tool("list_tables", {"schema": SCHEMA, "table_type": "base"})
        )
        assert tables == ["calls", "generated", "many", "typed"]
