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
Lance foreign table test module

Covers reading lance_fdw foreign tables through the MCP tools: discovery, plain
reads, Lance struct and binary columns, and vector search with and without a
filter.

Most are integration tests. They write their own Lance datasets with pylance
into the directory named by CBMCP_LANCE_TEST_DIR, which every segment of the
cluster must be able to read at the same path. Without that variable, pylance,
or lance_fdw 0.2 or later (the version that defines the distance operators),
they skip. The tests of what happens on lance_fdw older than 0.2 need none of
that: they run against a stand-in connection.
"""

import asyncio
import json
import os
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig
from cbmcp.database import DatabaseManager

# A suffix keeps two runs against one database from dropping each other's
# fixtures.
SUFFIX = uuid.uuid4().hex[:8]
SCHEMA = f"cbmcp_lance_test_{SUFFIX}"
SERVER = f"cbmcp_lance_test_{SUFFIX}"
ROWS = 300
DIMENSIONS = 8
QUERY_VECTOR = [0.25, -1.0, 0.5, 2.0, -0.75, 1.5, 0.0, -2.5]
MAX_VALUE_LENGTH = 10000
LIMITS = {
    "MCP_MAX_ROWS": "1000",
    "MCP_MAX_VALUE_LENGTH": str(MAX_VALUE_LENGTH),
    "MCP_MAX_RESULT_BYTES": "1000000",
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


def embeddings():
    """The docs embeddings, as float32 so that they match what Lance stores."""
    import numpy as np

    return np.random.default_rng(7).normal(size=(ROWS, DIMENSIONS)).astype(np.float32)


def write_datasets(directory):
    """Write the two datasets: docs in two fragments, and shapes."""
    import lance
    import pyarrow as pa

    vectors = embeddings()
    docs = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int32()),
            "cat": pa.array([i % 5 for i in range(ROWS)], pa.int32()),
            "title": pa.array([f"doc {i}" for i in range(ROWS)]),
            "emb": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), pa.float32()), DIMENSIONS
            ),
        }
    )
    lance.write_dataset(docs, os.path.join(directory, "docs.lance"), max_rows_per_file=ROWS // 2)

    meta = pa.struct([("a", pa.int32()), ("b", pa.string())])
    shapes = pa.table(
        {
            "id": pa.array([0, 1], pa.int32()),
            "payload": pa.array([bytes([0xFF, 0x00, 0x00]), None], pa.binary()),
            "meta": pa.array([{"a": 0, "b": "m0"}, None], meta),
            "parts": pa.array([[{"a": 1, "b": "p1"}, {"a": 2, "b": None}], None], pa.list_(meta)),
        }
    )
    lance.write_dataset(shapes, os.path.join(directory, "shapes.lance"))

    notes = pa.table(
        {
            "id": pa.array([0, 1, 2], pa.int32()),
            "body": pa.array(["n" * (MAX_VALUE_LENGTH + 5)] * 3),
            "emb": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors[:3].reshape(-1), pa.float32()), DIMENSIONS
            ),
        }
    )
    lance.write_dataset(notes, os.path.join(directory, "notes.lance"))


def exact_nearest(metric, limit=5, cat=None):
    """The true nearest docs, computed outside the database."""
    import numpy as np

    vectors = embeddings().astype(np.float64)
    query = np.array(QUERY_VECTOR, dtype=np.float32).astype(np.float64)
    if metric == "l2":
        distance = np.sqrt(((vectors - query) ** 2).sum(axis=1))
    elif metric == "cosine":
        distance = 1 - (vectors @ query) / (np.linalg.norm(vectors, axis=1) * np.linalg.norm(query))
    else:
        distance = -(vectors @ query)
    ids = [i for i in np.argsort(distance, kind="stable") if cat is None or i % 5 == cat]
    return [(int(i), float(distance[i])) for i in ids[:limit]]


@pytest.fixture(scope="module")
def result_limits():
    # load_dotenv() does not override a variable that is already set, so a
    # developer's .env cannot change the limits these tests assume.
    with pytest.MonkeyPatch.context() as patch:
        for name, value in LIMITS.items():
            patch.setenv(name, value)
        yield


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_tables(result_limits):
    base = os.environ.get("CBMCP_LANCE_TEST_DIR")
    if not base:
        pytest.skip("Skipping Lance tests - CBMCP_LANCE_TEST_DIR is not set")
    try:
        import lance  # noqa: F401
        import numpy  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError as e:
        pytest.skip(f"Skipping Lance tests - pylance is not installed: {e}")
    try:
        conn = await open_connection()
    except Exception as e:
        pytest.skip(f"Skipping Lance tests - no reachable cluster: {e}")

    directory = None
    created_extension = False
    try:
        installed = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'lance_fdw')"
        try:
            created_extension = not await conn.fetchval(installed)
            await conn.execute("CREATE EXTENSION IF NOT EXISTS lance_fdw")
        except Exception as e:
            # Another run may have created it in the meantime.
            if not await conn.fetchval(installed):
                created_extension = False
                pytest.skip(f"Skipping Lance tests - lance_fdw unavailable: {e}")
        if not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_operator WHERE oprname = '<->' "
            "AND oprleft = 'real[]'::regtype AND oprright = 'real[]'::regtype)"
        ):
            pytest.skip("Skipping Lance tests - lance_fdw is older than 0.2")

        directory = tempfile.mkdtemp(prefix="cbmcp_lance_", dir=base)
        write_datasets(directory)

        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"DROP SERVER IF EXISTS {SERVER} CASCADE")
        await conn.execute(
            f"CREATE SERVER {SERVER} FOREIGN DATA WRAPPER lance_fdw "
            f"OPTIONS (base_uri '{directory}')"
        )
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        await conn.execute(f"CREATE TYPE {SCHEMA}.meta AS (a integer, b text)")
        await conn.execute(
            f"CREATE FOREIGN TABLE {SCHEMA}.docs (id integer, cat integer, title text, emb real[]) "
            f"SERVER {SERVER} OPTIONS (uri 'docs.lance')"
        )
        await conn.execute(
            f"CREATE FOREIGN TABLE {SCHEMA}.shapes (id integer, payload bytea, "
            f"meta {SCHEMA}.meta, parts {SCHEMA}.meta[]) "
            f"SERVER {SERVER} OPTIONS (uri 'shapes.lance')"
        )
        await conn.execute(
            f"CREATE FOREIGN TABLE {SCHEMA}.notes (id integer, body text, emb real[]) "
            f"SERVER {SERVER} OPTIONS (uri 'notes.lance')"
        )
        # A real[] column outside lance_fdw is just an array.
        await conn.execute(
            f"CREATE TABLE {SCHEMA}.plain_arrays (id integer, emb real[]) DISTRIBUTED BY (id)"
        )
        yield conn
    finally:
        try:
            if not conn.is_closed():
                await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
                await conn.execute(f"DROP SERVER IF EXISTS {SERVER} CASCADE")
                # Leave the database as it was found.  Without CASCADE the
                # drop fails while another run still has a server on it, and
                # that run then drops it.
                if created_extension:
                    try:
                        await conn.execute("DROP EXTENSION IF EXISTS lance_fdw")
                    except Exception:
                        pass
        finally:
            await conn.close()
            if directory:
                shutil.rmtree(directory, ignore_errors=True)


@pytest_asyncio.fixture(loop_scope="module")
async def client(fixture_tables):
    instance = await CloudberryMCPClient.create(mode="stdio")
    yield instance
    await instance.close()


async def call(client, tool, **arguments):
    return unwrap(await client.call_tool(tool, arguments))


@pytest.mark.asyncio(loop_scope="module")
class TestDiscovery:
    """An agent has to find a Lance table before it can read one."""

    async def test_list_tables_includes_foreign_tables(self, client):
        assert await call(client, "list_tables", schema=SCHEMA) == ["docs", "notes", "plain_arrays", "shapes"]

    async def test_list_tables_can_narrow_to_foreign_tables(self, client):
        assert await call(client, "list_tables", schema=SCHEMA, table_type="foreign") == ["docs", "notes", "shapes"]
        assert await call(client, "list_tables", schema=SCHEMA, table_type="base") == ["plain_arrays"]

    async def test_list_columns_reports_real_array(self, client):
        columns = await call(client, "list_columns", schema=SCHEMA, table="docs")
        assert [(c["column_name"], c["type"]) for c in columns] == [
            ("id", "integer"), ("cat", "integer"), ("title", "text"), ("emb", "real[]")
        ]

    async def test_ddl_recreates_the_foreign_table(self, client, fixture_tables):
        ddl = await call(client, "get_table_ddl", schema=SCHEMA, table="docs")
        assert ddl == (
            f"CREATE FOREIGN TABLE {SCHEMA}.docs (\n"
            f"  id integer,\n  cat integer,\n  title text,\n  emb real[]\n)\n"
            f"SERVER {SERVER}\nOPTIONS (uri 'docs.lance');"
        )
        await fixture_tables.execute(ddl.replace(f"{SCHEMA}.docs (", f"{SCHEMA}.docs_copy (", 1))
        try:
            assert await fixture_tables.fetchval(f"SELECT count(*) FROM {SCHEMA}.docs_copy") == ROWS
        finally:
            await fixture_tables.execute(f"DROP FOREIGN TABLE {SCHEMA}.docs_copy")

    async def test_ddl_takes_credentials_out_of_a_uri(self, client, fixture_tables):
        await fixture_tables.execute(
            f"CREATE FOREIGN TABLE {SCHEMA}.remote (id integer) "
            f"SERVER {SERVER} OPTIONS (uri 's3://ak:sk@bucket/remote.lance', version '3')"
        )
        try:
            ddl = await call(client, "get_table_ddl", schema=SCHEMA, table="remote")
        finally:
            await fixture_tables.execute(f"DROP FOREIGN TABLE {SCHEMA}.remote")
        assert ddl.endswith("OPTIONS (uri 's3://bucket/remote.lance', version '3');")

    async def test_searchable_columns_report_the_lance_embedding(self, client):
        rows = await call(client, "list_searchable_columns", schema=SCHEMA)
        vectors = [(r["table"], r["column"], r["kind"], r["embedding"], r["dimension"], r["indexes"])
                   for r in rows if r["kind"] == "vector"]
        assert vectors == [
            ("docs", "emb", "vector", "lance", None, []),
            ("notes", "emb", "vector", "lance", None, []),
        ]

    async def test_database_summary_counts_foreign_tables(self, client):
        summary = await call(client, "get_database_summary")
        assert summary[SCHEMA]["tables"] == ["docs", "notes", "plain_arrays", "shapes"]


@pytest.mark.asyncio(loop_scope="module")
class TestReading:
    """Plain reads through execute_query."""

    async def test_rows_come_back(self, client):
        result = await call(
            client, "execute_query",
            query=f"SELECT id, cat, title FROM {SCHEMA}.docs WHERE cat = $1 ORDER BY id LIMIT 3",
            params=[2],
        )
        assert result["rows"] == [[2, 2, "doc 2"], [7, 2, "doc 7"], [12, 2, "doc 12"]]

    async def test_unbounded_select_stops_at_max_rows(self, client):
        result = await call(client, "execute_query", query=f"SELECT * FROM {SCHEMA}.docs", max_rows=7)
        assert result["row_count"] == 7
        assert result["truncated"] is True

    async def test_embedding_reads_as_a_list(self, client):
        result = await call(client, "execute_query", query=f"SELECT emb FROM {SCHEMA}.docs WHERE id = 0")
        assert result["rows"][0][0] == pytest.approx([float(v) for v in embeddings()[0]])

    async def test_struct_and_binary_columns(self, client):
        result = await call(client, "execute_query", query=f"SELECT * FROM {SCHEMA}.shapes ORDER BY id")
        assert result["rows"] == [
            [0, "\\xff0000", {"a": 0, "b": "m0"}, [{"a": 1, "b": "p1"}, {"a": 2, "b": None}]],
            [1, None, None, None],
        ]


@pytest.mark.asyncio(loop_scope="module")
class TestTopKQueries:
    """ORDER BY distance LIMIT k written by hand, through execute_query."""

    async def test_matches_the_exact_nearest(self, client):
        result = await call(
            client, "execute_query",
            query=f"SELECT id FROM {SCHEMA}.docs ORDER BY emb <-> $1 LIMIT 5",
            params=[QUERY_VECTOR],
        )
        assert [r[0] for r in result["rows"]] == [i for i, _ in exact_nearest("l2")]

    async def test_filter_applies_before_ranking(self, client):
        result = await call(
            client, "execute_query",
            query=f"SELECT id FROM {SCHEMA}.docs WHERE cat = $2 ORDER BY emb <-> $1::real[] LIMIT 5",
            params=[QUERY_VECTOR, 3],
        )
        assert [r[0] for r in result["rows"]] == [i for i, _ in exact_nearest("l2", cat=3)]

    async def test_search_and_filter_go_to_lance(self, client):
        plan = await call(
            client, "explain_query",
            query=f"SELECT id FROM {SCHEMA}.docs WHERE cat = $2 ORDER BY emb <-> $1 LIMIT 5",
            params=[QUERY_VECTOR, 3],
        )
        assert "Lance Vector Search: emb <-> (L2), k=5" in plan
        assert "Lance Filter: (`cat` = 3)" in plan


@pytest.mark.asyncio(loop_scope="module")
class TestVectorSearch:
    """vector_search on a Lance column."""

    @pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
    async def test_matches_the_exact_nearest(self, client, metric):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, limit=5, metric=metric,
        )
        expected = exact_nearest(metric)
        assert [r[0] for r in result["rows"]] == [i for i, _ in expected]
        assert [r[-1] for r in result["rows"]] == pytest.approx([d for _, d in expected], rel=1e-4)
        assert result["search"]["embedding"] == "lance"

    async def test_filter_applies_before_ranking(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, limit=5,
            filters=[{"column": "cat", "operator": "eq", "value": 3}],
        )
        assert [r[0] for r in result["rows"]] == [i for i, _ in exact_nearest("l2", cat=3)]

    async def test_embedding_is_left_out_unless_asked_for(self, client):
        default = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, limit=1,
        )
        assert default["columns"] == ["id", "cat", "title", "distance"]
        chosen = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, limit=1, select_columns=["id", "emb"],
        )
        assert len(chosen["rows"][0][1]) == DIMENSIONS

    async def test_l1_is_rejected(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, metric="l1",
        )
        assert "metric 'l1' is not available on the Lance column 'emb'" in result["error"]

    async def test_pgvector_settings_are_rejected(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=QUERY_VECTOR, probes=4,
        )
        assert "do not apply to the Lance column 'emb'" in result["error"]

    async def test_dimension_mismatch_is_reported(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="docs", vector_column="emb",
            query_vector=[1.0, 2.0],
        )
        assert "vectors of different dimensions" in result["error"]

    async def test_real_array_outside_lance_is_not_an_embedding(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="plain_arrays", vector_column="emb",
            query_vector=QUERY_VECTOR,
        )
        assert "which is not an embedding type" in result["error"]

    async def test_long_values_in_search_results_are_cut_and_reported(self, client):
        result = await call(
            client, "vector_search", schema=SCHEMA, table="notes", vector_column="emb",
            query_vector=QUERY_VECTOR, limit=3, select_columns=["id", "body"],
        )
        assert [len(r[1]) for r in result["rows"]] == [MAX_VALUE_LENGTH] * 3
        assert result["truncated_values"] == 3
        assert result["truncated_columns"] == ["body"]

    async def test_byte_budget_bounds_search_results(self, fixture_tables):
        # Each notes row carries a body cut to MAX_VALUE_LENGTH, so a budget of
        # two and a half rows keeps two of the three.
        budget = int(MAX_VALUE_LENGTH * 2.5)
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("MCP_MAX_RESULT_BYTES", str(budget))
            instance = await CloudberryMCPClient.create(mode="stdio")
            try:
                result = await call(
                    instance, "vector_search", schema=SCHEMA, table="notes", vector_column="emb",
                    query_vector=QUERY_VECTOR, limit=3, select_columns=["id", "body"],
                )
            finally:
                await instance.close()
        assert len(result["rows"]) == 2
        assert result["row_count"] == 2
        assert result["truncated"] is True
        assert result["truncated_by"] == "max_result_bytes"
        assert len(json.dumps(result).encode()) <= budget

    async def test_a_timed_out_lance_query_leaves_nothing_running(self, fixture_tables):
        # Three Lance scans joined into 27 million rows take seconds; the
        # timeout cancels the statement on the coordinator and every segment.
        sql = (
            f"SELECT count(*) FROM {SCHEMA}.docs a, {SCHEMA}.docs b, {SCHEMA}.docs c "
            f"WHERE a.cat + b.cat + c.cat >= 0"
        )
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("MCP_QUERY_TIMEOUT", "0.3")
            instance = await CloudberryMCPClient.create(mode="stdio")
            try:
                result = await call(instance, "execute_query", query=sql)
                after = await call(instance, "execute_query", query=f"SELECT count(*) FROM {SCHEMA}.docs")
            finally:
                await instance.close()
        assert "did not finish within MCP_QUERY_TIMEOUT (0.3 s) and was cancelled" in result["error"]
        assert after["rows"] == [[ROWS]]
        running = (
            "SELECT count(*) FROM (SELECT query, state, pid FROM pg_stat_activity "
            "UNION ALL SELECT query, state, pid FROM gp_dist_random('pg_stat_activity')) a "
            "WHERE state = 'active' AND query LIKE $1"
        )
        for _ in range(100):
            if not await fixture_tables.fetchval(running, f"%{SCHEMA}.docs a, %"):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("the cancelled Lance scans were still running 5 s later")

    async def test_hybrid_search_is_rejected(self, client):
        result = await call(
            client, "hybrid_search", schema=SCHEMA, table="docs", vector_column="emb",
            text_column="title", query_vector=QUERY_VECTOR, query="doc",
        )
        assert "hybrid_search does not support the Lance column 'emb'" in result["error"]


class StandInConnection:
    """Answers the catalog queries of a database whose lance_fdw is older than 0.2."""

    def __init__(self, records):
        self.records = records

    async def fetch(self, sql, *args):
        return self.records if "FROM pg_class c" in sql else []

    async def fetchval(self, sql, *args):
        # LANCE_OPERATOR_SQL: no lance_fdw operator on real[].
        return False


class StandInManager(DatabaseManager):
    def __init__(self, conn):
        super().__init__(DatabaseConfig("localhost", 5432, "db", "user", ""))
        self.conn = conn

    @asynccontextmanager
    async def get_connection(self):
        yield self.conn


@pytest.mark.asyncio
class TestLanceOlderThanTheOperators:
    """lance_fdw 0.1 has the foreign tables but no distance operators."""

    async def test_vector_search_says_to_update_the_extension(self):
        column = {"column_name": "emb"}
        with pytest.raises(ValueError, match="ALTER EXTENSION lance_fdw UPDATE"):
            await DatabaseManager._check_lance_search(
                StandInConnection([]), column, "l2", "<->", None, None
            )

    async def test_the_column_is_not_reported_as_searchable(self):
        record = {
            "relid": 1, "schema_name": "s", "table_name": "t", "column_name": "emb",
            "attribute_number": 1, "type_name": "_float4", "type_display": "real[]",
            "type_modifier": -1, "foreign_data_wrapper": "lance_fdw",
        }
        manager = StandInManager(StandInConnection([record]))
        assert await manager.list_searchable_columns() == []
