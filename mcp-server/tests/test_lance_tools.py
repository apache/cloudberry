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

These are integration tests. They write their own Lance datasets with pylance
into the directory named by CBMCP_LANCE_TEST_DIR, which every segment of the
cluster must be able to read at the same path. Without that variable, pylance,
or lance_fdw 0.2 or later (the version that defines the distance operators),
the whole module skips.
"""

import os
import shutil
import tempfile

import pytest
import pytest_asyncio

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig

SCHEMA = "cbmcp_lance_test"
SERVER = "cbmcp_lance_test"
ROWS = 300
DIMENSIONS = 8
QUERY_VECTOR = [0.25, -1.0, 0.5, 2.0, -0.75, 1.5, 0.0, -2.5]


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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def fixture_tables():
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
    try:
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS lance_fdw")
        except Exception as e:
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
        assert await call(client, "list_tables", schema=SCHEMA) == ["docs", "plain_arrays", "shapes"]

    async def test_list_tables_can_narrow_to_foreign_tables(self, client):
        assert await call(client, "list_tables", schema=SCHEMA, table_type="foreign") == ["docs", "shapes"]
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

    async def test_searchable_columns_report_the_lance_embedding(self, client):
        rows = await call(client, "list_searchable_columns", schema=SCHEMA)
        vectors = [(r["table"], r["column"], r["kind"], r["embedding"], r["dimension"])
                   for r in rows if r["kind"] == "vector"]
        assert vectors == [("docs", "emb", "vector", "lance", None)]


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

    async def test_hybrid_search_is_rejected(self, client):
        result = await call(
            client, "hybrid_search", schema=SCHEMA, table="docs", vector_column="emb",
            text_column="title", query_vector=QUERY_VECTOR, query="doc",
        )
        assert "hybrid_search does not support the Lance column 'emb'" in result["error"]
