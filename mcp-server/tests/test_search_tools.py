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
Retrieval tool test module

Covers vector_search, fulltext_search, hybrid_search and
list_searchable_columns against a live Apache Cloudberry cluster.

These are integration tests. They build their own fixture tables, so they need
a reachable cluster with the pgvector extension available; without one the
whole module skips.
"""

import re

import pytest
import pytest_asyncio

from cbmcp.client import CloudberryMCPClient
from cbmcp.config import DatabaseConfig
from cbmcp.search import (
    format_vector_literal,
    tsquery_has_blocking_operator,
    unique_alias,
)

SCHEMA = "cbmcp_search_test"
DOCS = "docs"
EMPTY = "docs_empty"
COLLIDING = "docs_colliding"
SPARSE = "docs_sparse"
DIMENSIONS = 8
ROWS = 2000
LISTS = 20

# One embedding per row, spread over the unit cube by a handful of coprime
# moduli so that neighbours are not simply adjacent ids.
EMBEDDING_SQL = (
    "ARRAY[(i%7)/7.0, (i%11)/11.0, (i%13)/13.0, (i%3)/3.0, "
    "(i%5)/5.0, (i%17)/17.0, (i%19)/19.0, (i%23)/23.0]::vector(8)"
)

QUERY_VECTOR = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
QUERY_VECTOR_LITERAL = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]"


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
    """Build the tables the retrieval tools are exercised against.

    The docs table is distributed by id so that the rows land on several
    segments, which is the case the fusion row key and the distributed top-K
    merge actually have to handle.
    """
    try:
        conn = await open_connection()
    except Exception as e:
        pytest.skip(f"Skipping retrieval tests - no reachable cluster: {e}")

    try:
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        except Exception as e:
            await conn.close()
            pytest.skip(f"Skipping retrieval tests - pgvector unavailable: {e}")

        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {SCHEMA}")
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.{DOCS} (
                id bigint,
                customer_id bigint,
                content text,
                embedding vector({DIMENSIONS})
            ) DISTRIBUTED BY (id)
            """
        )
        await conn.execute(
            f"""
            INSERT INTO {SCHEMA}.{DOCS}
            SELECT i,
                   (i % 10) + 1,
                   CASE WHEN i % 3 = 0 THEN 'query latency is slow and timeouts happen'
                        WHEN i % 3 = 1 THEN 'billing invoice payment problem'
                        ELSE 'connection refused during upgrade' END,
                   {EMBEDDING_SQL}
            FROM generate_series(1, {ROWS}) i
            """
        )
        await conn.execute(
            f"CREATE INDEX docs_embedding_ivf ON {SCHEMA}.{DOCS} "
            f"USING ivfflat (embedding vector_l2_ops) WITH (lists = {LISTS})"
        )
        await conn.execute(
            f"CREATE INDEX docs_content_gin ON {SCHEMA}.{DOCS} "
            f"USING gin (to_tsvector('english', content))"
        )
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.{EMPTY} (
                id bigint,
                content text,
                embedding vector({DIMENSIONS})
            ) DISTRIBUTED BY (id)
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.{COLLIDING} (
                id bigint,
                rank int,
                score int,
                distance int,
                content text,
                embedding vector({DIMENSIONS})
            ) DISTRIBUTED BY (id)
            """
        )
        await conn.execute(
            f"INSERT INTO {SCHEMA}.{COLLIDING} VALUES "
            f"(1, 99, 98, 97, 'query latency is slow', "
            f"'[0,0,0,0,0,0,0,0]')"
        )
        # sparsevec stores only the non-zero entries, so its text form differs
        # from every other embedding type.
        await conn.execute(
            f"""
            CREATE TABLE {SCHEMA}.{SPARSE} (
                id bigint,
                embedding sparsevec({DIMENSIONS})
            ) DISTRIBUTED BY (id)
            """
        )
        await conn.execute(
            f"INSERT INTO {SCHEMA}.{SPARSE} VALUES "
            f"(1, '{{1:0.9,2:0.1}}/{DIMENSIONS}'), "
            f"(2, '{{3:0.8,7:0.2}}/{DIMENSIONS}'), "
            f"(3, '{{1:0.2,5:0.7}}/{DIMENSIONS}')"
        )
        await conn.execute(f"ANALYZE {SCHEMA}.{DOCS}")

        yield conn
    finally:
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        finally:
            await conn.close()


@pytest_asyncio.fixture(loop_scope="module")
async def client(fixture_tables):
    """An in-process MCP client.

    Only stdio is exercised: it needs no separately started server, so the
    retrieval behaviour under test is what fails, not the transport.
    """
    instance = await CloudberryMCPClient.create(mode="stdio")
    yield instance
    await instance.close()


async def exact_nearest(conn, limit=5, predicate=None):
    """Compute the true nearest neighbours with the index scan disabled."""
    where = f"WHERE {predicate}" if predicate else ""
    async with conn.transaction():
        await conn.execute("SET LOCAL enable_indexscan = off")
        await conn.execute("SET LOCAL enable_indexonlyscan = off")
        records = await conn.fetch(
            f"SELECT id, (embedding <-> '{QUERY_VECTOR_LITERAL}'::vector)::float8 AS distance "
            f"FROM {SCHEMA}.{DOCS} {where} ORDER BY 2, 1 LIMIT {limit}"
        )
    return [(r["id"], r["distance"]) for r in records]


@pytest.mark.asyncio(loop_scope="module")
class TestListSearchableColumns:
    """Discovery of what can be searched."""

    async def test_reports_vector_column_with_dimension(self, client):
        rows = unwrap(
            await client.call_tool(
                "list_searchable_columns", {"schema": SCHEMA, "table": DOCS}
            )
        )
        by_column = {row["column"]: row for row in rows}

        assert "embedding" in by_column
        embedding = by_column["embedding"]
        assert embedding["kind"] == "vector"
        assert embedding["dimension"] == DIMENSIONS
        assert any(index["name"] == "docs_embedding_ivf" for index in embedding["indexes"])

    async def test_reports_text_column_behind_an_expression_index(self, client):
        rows = unwrap(
            await client.call_tool(
                "list_searchable_columns", {"schema": SCHEMA, "table": DOCS}
            )
        )
        by_column = {row["column"]: row for row in rows}

        # The GIN index is on to_tsvector(content), and pg_get_indexdef leaves
        # such a column name unquoted, so this is the case that a naive
        # quoted-name match misses.
        assert "content" in by_column
        content = by_column["content"]
        assert content["kind"] == "text"
        assert content["full_text_indexed"] is True
        assert any(index["on_expression"] for index in content["indexes"])

    async def test_unindexed_text_is_hidden_by_default(self, client):
        default = unwrap(
            await client.call_tool(
                "list_searchable_columns", {"schema": SCHEMA, "table": EMPTY}
            )
        )
        assert all(row["column"] != "content" for row in default)

        included = unwrap(
            await client.call_tool(
                "list_searchable_columns",
                {"schema": SCHEMA, "table": EMPTY, "include_unindexed_text": True},
            )
        )
        assert any(row["column"] == "content" for row in included)

    async def test_unknown_relation_yields_no_rows(self, client):
        rows = unwrap(
            await client.call_tool(
                "list_searchable_columns", {"schema": SCHEMA, "table": "no_such_table"}
            )
        )
        assert rows == []


@pytest.mark.asyncio(loop_scope="module")
class TestVectorSearch:
    """Nearest neighbour search, with and without a pre-filter."""

    async def test_returns_rows_ordered_by_distance(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 5,
                    "select_columns": ["id"],
                },
            )
        )
        assert result["row_count"] == 5
        assert result["columns"] == ["id", "distance"]

        distances = [row[1] for row in result["rows"]]
        assert distances == sorted(distances)

    async def test_embedding_column_is_dropped_from_the_default_projection(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                },
            )
        )
        assert "embedding" not in result["columns"]
        assert {"id", "customer_id", "content", "distance"} <= set(result["columns"])

    async def test_embedding_column_comes_back_as_text_when_asked_for(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                    "select_columns": ["id", "embedding"],
                },
            )
        )
        embedding = result["rows"][0][1]
        assert isinstance(embedding, str)
        assert embedding.startswith("[")

    async def test_pre_filter_restricts_the_candidates(self, client):
        """The pre-filtered ANN shape: the filter rides along with the scan."""
        wanted = [1, 2, 3]
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 10,
                    "select_columns": ["id", "customer_id"],
                    "probes": LISTS,
                    "filters": [
                        {"column": "customer_id", "operator": "in", "value": wanted}
                    ],
                },
            )
        )
        assert result["row_count"] > 0
        assert all(row[1] in wanted for row in result["rows"])

    async def test_pre_filtered_result_matches_an_exact_filtered_search(
        self, client, fixture_tables
    ):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 5,
                    "select_columns": ["id"],
                    "probes": LISTS,
                    "filters": [
                        {"column": "customer_id", "operator": "eq", "value": 4}
                    ],
                },
            )
        )
        exact = await exact_nearest(fixture_tables, limit=5, predicate="customer_id = 4")
        assert [row[0] for row in result["rows"]] == [row[0] for row in exact]

    async def test_probes_reaches_the_segments(self, client, fixture_tables):
        """Regression test for a recall setting that never left the coordinator.

        Probing every list must reproduce the exact answer. When the setting
        fails to reach the segment scans the search silently falls back to one
        probe, which is a wrong answer rather than an error, so only comparing
        against the exact result catches it.
        """
        exact = await exact_nearest(fixture_tables, limit=5)
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 5,
                    "select_columns": ["id"],
                    "probes": LISTS,
                },
            )
        )
        assert result["search"]["settings"] == {"ivfflat.probes": LISTS}
        assert [row[0] for row in result["rows"]] == [row[0] for row in exact]

    async def test_recall_setting_does_not_leak_to_later_searches(self, client):
        """A pooled connection must be handed back without the session setting."""
        await client.call_tool(
            "vector_search",
            {
                "schema": SCHEMA,
                "table": DOCS,
                "vector_column": "embedding",
                "query_vector": QUERY_VECTOR,
                "limit": 1,
                "select_columns": ["id"],
                "probes": LISTS,
            },
        )
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                    "select_columns": ["id"],
                },
            )
        )
        assert "settings" not in result["search"]

    async def test_empty_table_returns_no_rows(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": EMPTY,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 5,
                },
            )
        )
        assert result["row_count"] == 0
        assert result["rows"] == []

    async def test_dimension_mismatch_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": [1.0, 2.0],
                },
            )
        )
        assert "2 dimensions" in result["error"]
        assert "vector(8)" in result["error"]

    async def test_non_embedding_column_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "content",
                    "query_vector": QUERY_VECTOR,
                },
            )
        )
        assert "not an embedding type" in result["error"]

    async def test_unknown_relation_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": "no_such_table",
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                },
            )
        )
        assert "was not found" in result["error"]

    async def test_empty_query_vector_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": [],
                },
            )
        )
        assert "non-empty list" in result["error"]

    async def test_limit_outside_the_allowed_range_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 0,
                },
            )
        )
        assert "must be between 1 and" in result["error"]

    async def test_empty_filter_list_value_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "filters": [
                        {"column": "customer_id", "operator": "in", "value": []}
                    ],
                },
            )
        )
        assert "non-empty list value" in result["error"]

    async def test_unknown_filter_column_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "filters": [
                        {"column": "no_such_column", "operator": "eq", "value": 1}
                    ],
                },
            )
        )
        assert "does not exist" in result["error"]

    async def test_identifier_injection_is_rejected_and_changes_nothing(
        self, client, fixture_tables
    ):
        before = await fixture_tables.fetchval(f"SELECT count(*) FROM {SCHEMA}.{DOCS}")
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": f'embedding"; DROP TABLE {SCHEMA}.{DOCS}; --',
                    "query_vector": QUERY_VECTOR,
                },
            )
        )
        assert "does not exist" in result["error"]
        assert await fixture_tables.fetchval(f"SELECT count(*) FROM {SCHEMA}.{DOCS}") == before


@pytest.mark.asyncio(loop_scope="module")
class TestFullTextSearch:
    """Keyword search over a text column."""

    async def test_ranks_matching_rows(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency",
                    "limit": 5,
                    "select_columns": ["id", "content"],
                },
            )
        )
        assert result["row_count"] == 5
        assert result["columns"] == ["id", "content", "rank"]
        assert all("latency" in row[1] for row in result["rows"])

        ranks = [row[2] for row in result["rows"]]
        assert ranks == sorted(ranks, reverse=True)

    async def test_filter_applies_to_the_text_arm(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "billing invoice",
                    "limit": 5,
                    "select_columns": ["id", "customer_id"],
                    "filters": [
                        {"column": "customer_id", "operator": "eq", "value": 5}
                    ],
                },
            )
        )
        assert result["row_count"] > 0
        assert all(row[1] == 5 for row in result["rows"])

    async def test_no_match_returns_no_rows(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "zzzzzunmatchablezzzzz",
                },
            )
        )
        assert result["row_count"] == 0

    async def test_blank_query_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "   ",
                },
            )
        )
        assert "non-empty search string" in result["error"]

    async def test_unknown_language_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "latency",
                    "language": "no_such_config",
                },
            )
        )
        assert "not a known text search configuration" in result["error"]

    async def test_strict_match_stays_strict_when_it_finds_rows(self, client):
        """Both terms appear in the fixture, so nothing should be widened."""
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency",
                    "limit": 3,
                },
            )
        )
        assert result["row_count"] == 3
        assert result["search"]["matched_with"] == "all"
        assert result["search"]["widened"] is False

    async def test_widens_when_every_term_together_matches_nothing(self, client):
        """A term that appears nowhere must not empty out the whole result.

        Requiring every term is the precise reading, but an empty result reads
        to a caller as "this database holds nothing on the subject", which is
        the wrong conclusion when most of the terms do match something.
        """
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency zzzznotpresentzzzz",
                    "limit": 3,
                    "select_columns": ["id", "content"],
                },
            )
        )
        assert result["row_count"] == 3
        assert result["search"]["match_mode"] == "all_then_any"
        assert result["search"]["matched_with"] == "any"
        assert result["search"]["widened"] is True
        assert all("latency" in row[1] for row in result["rows"])

    async def test_strict_mode_can_be_forced(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency zzzznotpresentzzzz",
                    "match_mode": "all",
                },
            )
        )
        assert result["row_count"] == 0
        assert result["search"]["matched_with"] == "all"
        assert result["search"]["widened"] is False

    async def test_any_mode_does_not_report_widening(self, client):
        """Asking for 'any' up front is not a fallback, so nothing widened."""
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency zzzznotpresentzzzz",
                    "match_mode": "any",
                    "limit": 3,
                },
            )
        )
        assert result["row_count"] == 3
        assert result["search"]["matched_with"] == "any"
        assert result["search"]["widened"] is False

    async def test_widening_ranks_fuller_matches_first(self, client):
        """ts_rank scores a row matching more of the terms above one matching fewer."""
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "query latency billing zzzznotpresentzzzz",
                    "limit": 50,
                    "select_columns": ["id", "content"],
                },
            )
        )
        assert result["search"]["widened"] is True
        # 'query latency ...' carries two of the terms, 'billing invoice ...'
        # only one, so the two-term rows have to come first.
        ranks = [row[2] for row in result["rows"]]
        assert ranks == sorted(ranks, reverse=True)
        assert "latency" in result["rows"][0][1]

    async def test_negation_is_not_undone_by_widening(self, client):
        """Widening must never return the rows the caller asked to exclude.

        ORing the lexemes of a negated query inverts it, so the strict reading
        is kept and an empty result is the honest answer.
        """
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "latency -timeouts",
                    "limit": 5,
                    "select_columns": ["id", "content"],
                },
            )
        )
        assert result["search"]["widening_refused"] is True
        assert result["search"]["widened"] is False
        assert result["search"]["matched_with"] == "all"
        assert all("timeouts" not in row[1] for row in result["rows"])

    async def test_a_phrase_is_not_flattened_by_widening(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "timeouts latency",
                    "query_mode": "phrase",
                    "limit": 5,
                },
            )
        )
        # The two words exist but not adjacent in that order, so a phrase
        # query correctly matches nothing and must stay that way.
        assert result["row_count"] == 0
        assert result["search"]["widening_refused"] is True

    async def test_document_expression_keeps_an_index_match_possible(self, client):
        """The generated SQL must be shaped the way a full-text index is.

        A coalesce() wrapper or a parameterised configuration both read as
        harmless, and both stop the planner matching a to_tsvector() index, so
        the search silently degrades to a sequential scan.
        """
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "latency",
                    "limit": 1,
                },
            )
        )
        sql = result["sql"]
        assert "coalesce" not in sql.lower()
        assert "::regconfig" in sql
        # The configuration is written as the catalog OID, which reproduces
        # the constant an expression index was built with.
        assert re.search(r"to_tsvector\(\d+::regconfig", sql)

    async def test_a_query_of_only_stop_words_still_returns_nothing(self, client):
        """Widening cannot invent lexemes where the parser found none."""
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "the and of",
                },
            )
        )
        assert result["row_count"] == 0

    async def test_unknown_match_mode_is_rejected(self, client):
        with pytest.raises(Exception) as excinfo:
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "content",
                    "query": "latency",
                    "match_mode": "sometimes",
                },
            )
        assert "all_then_any" in str(excinfo.value)

    async def test_non_text_column_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "text_column": "embedding",
                    "query": "latency",
                },
            )
        )
        assert "cannot be searched as text" in result["error"]


@pytest.mark.asyncio(loop_scope="module")
class TestHybridSearch:
    """Reciprocal rank fusion across both arms."""

    async def test_fuses_rows_from_both_arms(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "query latency",
                    "limit": 10,
                    "select_columns": ["id"],
                    "probes": LISTS,
                },
            )
        )
        assert result["row_count"] == 10
        assert result["columns"] == ["id", "score", "vector_rank", "text_rank"]

        scores = [row[1] for row in result["rows"]]
        assert scores == sorted(scores, reverse=True)

        # Both arms have to contribute, otherwise fusion is doing nothing.
        assert any(row[2] is not None for row in result["rows"])
        assert any(row[3] is not None for row in result["rows"])

    async def test_row_key_includes_the_segment_on_cloudberry(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "latency",
                    "limit": 3,
                    "select_columns": ["id"],
                },
            )
        )
        # A ctid repeats across segments, so fusing on it alone would merge
        # unrelated rows.
        assert result["search"]["row_key"] == ["gp_segment_id", "ctid"]

    async def test_rows_are_distinct(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "query latency",
                    "limit": 20,
                    "select_columns": ["id"],
                },
            )
        )
        ids = [row[0] for row in result["rows"]]
        assert len(ids) == len(set(ids))

    async def test_filter_applies_to_both_arms(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "query latency",
                    "limit": 10,
                    "select_columns": ["id", "customer_id"],
                    "filters": [
                        {"column": "customer_id", "operator": "in", "value": [2, 7]}
                    ],
                },
            )
        )
        assert result["row_count"] > 0
        assert all(row[1] in (2, 7) for row in result["rows"])

    async def test_empty_table_returns_no_rows(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": EMPTY,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "anything",
                    "limit": 5,
                },
            )
        )
        assert result["row_count"] == 0

    async def test_text_arm_still_contributes_after_widening(self, client):
        """Without widening the text arm can go silently empty.

        The fused result would then be pure vector search while still
        presenting itself as hybrid, which the caller cannot see.
        """
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "query latency zzzznotpresentzzzz",
                    "limit": 10,
                    "select_columns": ["id"],
                    "probes": LISTS,
                },
            )
        )
        assert result["search"]["widened"] is True
        assert result["search"]["matched_with"] == "any"
        assert any(row[3] is not None for row in result["rows"])

    async def test_strict_mode_can_leave_the_text_arm_empty(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "query latency zzzznotpresentzzzz",
                    "limit": 5,
                    "select_columns": ["id"],
                    "match_mode": "all",
                },
            )
        )
        assert result["search"]["widened"] is False
        assert all(row[3] is None for row in result["rows"])

    async def test_negation_is_not_undone_in_the_text_arm(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "latency -timeouts",
                    "limit": 5,
                    "select_columns": ["id", "content"],
                },
            )
        )
        assert result["search"]["widening_refused"] is True

        # The vector arm has no opinion about words and legitimately returns
        # rows containing the excluded term, so only rows the text arm ranked
        # are checked. Columns are read by the names the result reports rather
        # than by position.
        text_rank_column = result["search"]["text_rank_column"]
        rows = [dict(zip(result["columns"], row)) for row in result["rows"]]
        from_text = [row for row in rows if row[text_rank_column] is not None]
        assert all("timeouts" not in row["content"] for row in from_text)

    async def test_candidates_below_limit_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "latency",
                    "limit": 10,
                    "candidates": 5,
                },
            )
        )
        assert "candidates must be at least limit" in result["error"]


@pytest.mark.asyncio(loop_scope="module")
class TestQueryTools:
    """The query tools the retrieval work depends on."""

    async def test_positional_parameters_are_bound(self, client):
        """Regression test: parameters used to be passed as keyword arguments."""
        result = unwrap(
            await client.call_tool(
                "execute_query",
                {
                    "query": f"SELECT id FROM {SCHEMA}.{DOCS} WHERE id = $1 OR id = $2 ORDER BY id",
                    "params": [42, 77],
                },
            )
        )
        assert result["rows"] == [[42], [77]]

    async def test_parameters_must_be_a_list(self, client):
        """A mapping is refused at the tool boundary, before any SQL is built."""
        with pytest.raises(Exception) as excinfo:
            await client.call_tool(
                "execute_query",
                {
                    "query": f"SELECT id FROM {SCHEMA}.{DOCS} WHERE id = $1",
                    "params": {"id": 42},
                },
            )
        assert "list" in str(excinfo.value)

    async def test_explain_refuses_a_write_statement(self, client):
        """EXPLAIN ANALYZE executes what it is given, so writes must not pass."""
        result = unwrap(
            await client.call_tool(
                "explain_query",
                {"query": f"DELETE FROM {SCHEMA}.{DOCS} WHERE id = 1"},
            )
        )
        assert "Blocked SQL operation" in result or "read-only" in result

    async def test_explain_returns_a_plan_for_a_select(self, client):
        result = unwrap(
            await client.call_tool(
                "explain_query",
                {"query": f"SELECT count(*) FROM {SCHEMA}.{DOCS}"},
            )
        )
        assert "Aggregate" in result or "Gather" in result


@pytest.mark.asyncio(loop_scope="module")
class TestResultAliases:
    """A computed column must not shadow one the table already has."""

    async def test_rank_alias_avoids_a_real_rank_column(self, client):
        result = unwrap(
            await client.call_tool(
                "fulltext_search",
                {
                    "schema": SCHEMA,
                    "table": COLLIDING,
                    "text_column": "content",
                    "query": "latency",
                    "limit": 1,
                },
            )
        )
        assert len(result["columns"]) == len(set(result["columns"]))
        rank_column = result["search"]["rank_column"]
        assert rank_column != "rank"
        # The stored column keeps its value; the score lands in the new name.
        row = dict(zip(result["columns"], result["rows"][0]))
        assert row["rank"] == 99
        assert isinstance(row[rank_column], float)

    async def test_distance_alias_avoids_a_real_distance_column(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": COLLIDING,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                },
            )
        )
        assert len(result["columns"]) == len(set(result["columns"]))
        distance_column = result["search"]["distance_column"]
        assert distance_column != "distance"
        row = dict(zip(result["columns"], result["rows"][0]))
        assert row["distance"] == 97

    async def test_hybrid_aliases_avoid_real_columns(self, client):
        result = unwrap(
            await client.call_tool(
                "hybrid_search",
                {
                    "schema": SCHEMA,
                    "table": COLLIDING,
                    "vector_column": "embedding",
                    "text_column": "content",
                    "query_vector": QUERY_VECTOR,
                    "query": "latency",
                    "limit": 1,
                },
            )
        )
        assert len(result["columns"]) == len(set(result["columns"]))
        assert result["search"]["score_column"] != "score"
        row = dict(zip(result["columns"], result["rows"][0]))
        assert row["score"] == 98


class TestQueryShapeHelpers:
    """Pure helpers, exercised without a cluster."""

    @pytest.mark.parametrize(
        "rendered,blocking",
        [
            ("'queri' & 'latenc' & 'slow'", False),
            ("'latenc' & !'dashboard'", True),
            ("'queri' <-> 'latenc'", True),
            ("'a' | 'b'", False),
            ("'latenc'", False),
            ("", False),
            (None, False),
            # An operator inside a lexeme is data, not an operator.
            ("'ex!clam' & 'other'", False),
            ("'a<->b' & 'other'", False),
            ("'has''quote' & 'other'", False),
        ],
    )
    def test_blocking_operator_detection(self, rendered, blocking):
        assert tsquery_has_blocking_operator(rendered) is blocking

    @pytest.mark.parametrize(
        "base,taken,expected",
        [
            ("rank", ["id", "body"], "rank"),
            ("rank", ["id", "rank"], "rank_2"),
            ("rank", ["rank", "rank_2"], "rank_3"),
            ("score", [], "score"),
        ],
    )
    def test_unique_alias(self, base, taken, expected):
        assert unique_alias(base, taken) == expected


@pytest.mark.asyncio(loop_scope="module")
class TestSparseEmbeddings:
    """A sparsevec column is reported as searchable, so it has to work.

    Its text form is not the dense bracket list every other embedding type
    uses, and pgvector's input function rejects the dense form outright.
    """

    SPARSE_QUERY = [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    async def test_vector_search_works_on_a_sparsevec_column(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": SPARSE,
                    "vector_column": "embedding",
                    "query_vector": self.SPARSE_QUERY,
                    "limit": 3,
                    "select_columns": ["id"],
                },
            )
        )
        assert "error" not in result
        assert result["row_count"] == 3
        # Row 1 is the query itself, so it has to come back first.
        assert result["rows"][0][0] == 1
        distances = [row[1] for row in result["rows"]]
        assert distances == sorted(distances)

    async def test_result_matches_a_hand_written_sparse_query(self, client, fixture_tables):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": SPARSE,
                    "vector_column": "embedding",
                    "query_vector": self.SPARSE_QUERY,
                    "limit": 3,
                    "select_columns": ["id"],
                },
            )
        )
        records = await fixture_tables.fetch(
            f"SELECT id FROM {SCHEMA}.{SPARSE} "
            f"ORDER BY embedding <-> '{{1:0.9,2:0.1}}/{DIMENSIONS}'::sparsevec, id"
        )
        assert [row[0] for row in result["rows"]] == [r["id"] for r in records]

    async def test_dimension_mismatch_is_caught_on_sparsevec(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": SPARSE,
                    "vector_column": "embedding",
                    "query_vector": [0.1, 0.2],
                },
            )
        )
        assert "2 dimensions" in result["error"]
        assert "sparsevec(8)" in result["error"]

    async def test_an_all_zero_query_vector_is_accepted(self, client):
        """The sparse form of an all-zero vector is an empty entry list."""
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": SPARSE,
                    "vector_column": "embedding",
                    "query_vector": [0.0] * DIMENSIONS,
                    "limit": 1,
                    "select_columns": ["id"],
                },
            )
        )
        assert "error" not in result
        assert result["row_count"] == 1


@pytest.mark.asyncio(loop_scope="module")
class TestRecallSettingBounds:
    """The real limits belong to the installed pgvector, not to this wrapper."""

    async def test_ef_search_above_the_server_maximum_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                    "ef_search": 5000,
                },
            )
        )
        # pgvector caps hnsw.ef_search at 1000; the error has to say so rather
        # than let the SET fail half way through applying the settings.
        assert "hnsw.ef_search" in result["error"]
        assert "1000" in result["error"]

    async def test_probes_above_the_server_maximum_is_rejected(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                    "probes": 99999,
                },
            )
        )
        assert "ivfflat.probes" in result["error"]
        assert "32768" in result["error"]

    async def test_a_rejected_setting_leaves_the_connection_clean(self, client):
        """A refused pair must not leave the first setting applied."""
        await client.call_tool(
            "vector_search",
            {
                "schema": SCHEMA,
                "table": DOCS,
                "vector_column": "embedding",
                "query_vector": QUERY_VECTOR,
                "limit": 1,
                "probes": LISTS,
                "ef_search": 5000,
            },
        )
        result = unwrap(
            await client.call_tool("execute_query", {"query": "SHOW ivfflat.probes"})
        )
        assert result["rows"][0][0] == "1"

    async def test_both_settings_apply_together_when_valid(self, client):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "limit": 1,
                    "probes": LISTS,
                    "ef_search": 100,
                },
            )
        )
        assert result["search"]["settings"] == {
            "ivfflat.probes": LISTS,
            "hnsw.ef_search": 100,
        }

    @pytest.mark.parametrize("value", [0, -1])
    async def test_a_non_positive_setting_is_rejected(self, client, value):
        result = unwrap(
            await client.call_tool(
                "vector_search",
                {
                    "schema": SCHEMA,
                    "table": DOCS,
                    "vector_column": "embedding",
                    "query_vector": QUERY_VECTOR,
                    "probes": value,
                },
            )
        )
        assert "at least 1" in result["error"]


class TestVectorLiteralForms:
    """Pure helper, exercised without a cluster."""

    @pytest.mark.parametrize(
        "values,type_name,expected",
        [
            ([0.5, 0.25], "vector", "[0.5,0.25]"),
            ([0.5, 0.25], "halfvec", "[0.5,0.25]"),
            ([0.9, 0.1, 0.0], "sparsevec", "{1:0.9,2:0.1}/3"),
            ([0.0, 0.0], "sparsevec", "{}/2"),
            ([0.0, 4.0], "sparsevec", "{2:4.0}/2"),
        ],
    )
    def test_literal_matches_the_type(self, values, type_name, expected):
        assert format_vector_literal(values, type_name) == expected

    def test_a_non_finite_value_is_rejected(self):
        with pytest.raises(ValueError, match="finite"):
            format_vector_literal([1.0, float("inf")], "sparsevec")
