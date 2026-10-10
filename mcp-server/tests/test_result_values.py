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

import pytest
import pytest_asyncio

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig

SCHEMA = "cbmcp_values_test"
MANY_ROWS = 1500


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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_tables():
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


@pytest.mark.asyncio(loop_scope="module")
class TestResultLimits:
    """How much of a result one call returns."""

    async def test_select_without_limit_stops_at_max_rows(self, client):
        result = await query(client, f"SELECT id FROM {SCHEMA}.many ORDER BY id", max_rows=10)
        assert result["rows"] == [[i] for i in range(1, 11)]
        assert result["truncated"] is True
        assert result["max_rows"] == 10

    async def test_default_limit_is_the_server_maximum(self, client):
        maximum = DatabaseConfig.from_env().max_rows
        result = await query(client, f"SELECT id FROM {SCHEMA}.many")
        assert result["row_count"] == min(MANY_ROWS, maximum)
        assert result["truncated"] is (MANY_ROWS > maximum)
        assert result["max_rows"] == maximum

    async def test_result_exactly_at_the_limit_is_not_truncated(self, client):
        result = await query(client, f"SELECT id FROM {SCHEMA}.many WHERE id <= 5", max_rows=5)
        assert result["row_count"] == 5
        assert result["truncated"] is False

    async def test_max_rows_above_the_server_maximum_is_rejected(self, client):
        maximum = DatabaseConfig.from_env().max_rows
        result = await query(client, "SELECT 1", max_rows=maximum + 1)
        assert result["error"] == f"max_rows must be between 1 and {maximum}, got {maximum + 1}"

    async def test_long_value_is_cut_and_reported(self, client):
        maximum = DatabaseConfig.from_env().max_value_length
        result = await query(client, f"SELECT 1 AS id, repeat('x', {maximum + 5}) AS t")
        assert len(result["rows"][0][1]) == maximum
        assert result["truncated_values"] == 1
        assert result["truncated_columns"] == ["t"]
        assert result["max_value_length"] == maximum

    async def test_short_values_report_nothing(self, client):
        result = await query(client, "SELECT 'short' AS t")
        assert "truncated_values" not in result

    async def test_empty_result_still_names_its_columns(self, client):
        result = await query(client, f"SELECT id, label FROM {SCHEMA}.typed WHERE false")
        assert result["columns"] == ["id", "label"]
        assert result["rows"] == []


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

    async def test_list_tables_rejects_an_unknown_type(self, client):
        with pytest.raises(Exception, match="table_type 'view' is not supported"):
            await client.call_tool("list_tables", {"schema": SCHEMA, "table_type": "view"})

    async def test_list_tables_base_lists_ordinary_tables(self, client):
        tables = unwrap(
            await client.call_tool("list_tables", {"schema": SCHEMA, "table_type": "base"})
        )
        assert tables == ["many", "typed"]
