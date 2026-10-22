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
Retrieval SQL builders for the Apache Cloudberry MCP server.

These helpers turn catalog-verified identifiers plus caller supplied values
into parameterized SQL for vector, full-text, and hybrid search.

Two rules hold throughout:

* Identifiers (schema, table, column) are never interpolated before they have
  been matched against the catalog, and they are always quoted afterwards.
* Every caller supplied *value* is bound as a ``$n`` query parameter, never
  formatted into the statement text.
"""

import math
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pydantic import BaseModel, Field

from .security import SQLValidator

# Column types that hold embeddings.  All are provided by pgvector.
VECTOR_TYPES = frozenset({"vector", "halfvec", "sparsevec"})

# Column types that full-text search can build a tsvector from.
TEXT_TYPES = frozenset({"text", "varchar", "bpchar", "name", "citext"})

# A column already stored as a parsed document needs no to_tsvector() call.
TSVECTOR_TYPE = "tsvector"

# The one embedding type whose text form is not a dense bracket list.
SPARSEVEC_TYPE = "sparsevec"

# Literal used to force the pgvector shared library to load so that its GUCs
# (ivfflat.probes, hnsw.ef_search) become settable in the current session.
# sparsevec has its own text representation and cannot reuse the '[...]' form.
WARMUP_LITERALS: Dict[str, str] = {
    "vector": "[1]",
    "halfvec": "[1]",
    "sparsevec": "{1:1}/1",
}

# Upper bounds that keep a single tool call from returning or scanning an
# unbounded amount of data.
MAX_LIMIT = 1000
MAX_CANDIDATES = 10000


class SearchMetric(str, Enum):
    """Distance metric used to rank vector search results."""

    L2 = "l2"
    COSINE = "cosine"
    INNER_PRODUCT = "inner_product"
    L1 = "l1"


# pgvector distance operators.  Every one of them orders ascending, including
# inner_product: '<#>' returns the *negative* inner product precisely so that
# "smaller is better" holds for all four metrics.
METRIC_OPERATORS: Dict[str, str] = {
    SearchMetric.L2.value: "<->",
    SearchMetric.COSINE.value: "<=>",
    SearchMetric.INNER_PRODUCT.value: "<#>",
    SearchMetric.L1.value: "<+>",
}


class TextQueryMode(str, Enum):
    """How a full-text query string is parsed into a tsquery."""

    PLAIN = "plain"
    PHRASE = "phrase"
    WEBSEARCH = "websearch"


TEXT_QUERY_FUNCTIONS: Dict[str, str] = {
    TextQueryMode.PLAIN.value: "plainto_tsquery",
    TextQueryMode.PHRASE.value: "phraseto_tsquery",
    TextQueryMode.WEBSEARCH.value: "websearch_to_tsquery",
}


class TextMatchMode(str, Enum):
    """How the terms of a full-text query are combined."""

    ALL = "all"
    ANY = "any"
    ALL_THEN_ANY = "all_then_any"


# Escapes one lexeme into the quoted form tsquery's own input function reads.
# The result is cast straight to tsquery rather than passed through
# to_tsquery(): to_tsquery re-parses what it is given, so a lexeme carrying
# punctuation would be split into a phrase again, and these lexemes came out
# of to_tsvector already normalised.
_ESCAPE_LEXEME = "'''' || replace(replace({lexeme}, '\\', '\\\\'), '''', '''''') || ''''"


def build_tsquery(
    query_function: str,
    language_sql: str,
    query_placeholder: str,
    match_mode: str,
) -> str:
    """Build the tsquery expression for one match mode.

    PostgreSQL's query constructors require every term to be present, so a
    caller who writes a natural sentence usually matches nothing. The 'any'
    form re-parses the same string into its lexemes and ORs them, which keeps
    the configuration's own stemming and stop-word handling rather than
    splitting words here.

    Ranking still favours the fuller match: ts_rank scores a document that
    matched more of the terms above one that matched fewer.
    """
    if match_mode == TextMatchMode.ANY.value:
        escaped = _ESCAPE_LEXEME.format(lexeme="lexemes.lexeme")
        return (
            f"(SELECT string_agg({escaped}, ' | ') "
            f"FROM unnest(to_tsvector({language_sql}, {query_placeholder})) "
            f"AS lexemes)::tsquery"
        )
    return f"{query_function}({language_sql}, {query_placeholder})"


def tsquery_has_blocking_operator(rendered: Optional[str]) -> bool:
    """Report whether a tsquery carries an operator that widening would undo.

    Widening ORs the query's lexemes together. That is harmless for a query
    that only ANDs its terms, but it inverts a negation and flattens a phrase,
    handing the caller exactly the rows they asked to exclude. Negation and
    phrase distance are therefore read as an instruction not to widen.

    Only operators outside a quoted lexeme count, because a lexeme may itself
    contain any character.
    """
    if not rendered:
        return False

    index = 0
    length = len(rendered)
    while index < length:
        char = rendered[index]
        if char == "'":
            # Walk to the closing quote, where '' is an escaped quote.
            index += 1
            while index < length:
                if rendered[index] == "'":
                    if index + 1 < length and rendered[index + 1] == "'":
                        index += 2
                        continue
                    break
                index += 1
            index += 1
            continue
        if char == "!" or char == "<":
            return True
        index += 1
    return False


def unique_alias(base: str, taken: Sequence[str]) -> str:
    """Pick an output name that does not collide with a selected column.

    A table is free to have a column of its own called rank or score, and
    results come back as positional rows, so two columns sharing a name leave
    the caller reading the wrong one with nothing to warn them.
    """
    existing = set(taken)
    if base not in existing:
        return base
    suffix = 2
    while f"{base}_{suffix}" in existing:
        suffix += 1
    return f"{base}_{suffix}"


def resolve_match_mode(match_mode: Any) -> str:
    """Validate a term combination mode."""
    value = match_mode.value if isinstance(match_mode, TextMatchMode) else str(match_mode)
    supported = {item.value for item in TextMatchMode}
    if value not in supported:
        raise ValueError(
            f"match_mode '{value}' is not supported; supported modes are: "
            f"{', '.join(sorted(supported))}"
        )
    return value


class FilterOperator(str, Enum):
    """Comparison used by a single pre-filter condition."""

    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"
    IN = "in"
    NOT_IN = "not_in"
    LIKE = "like"
    ILIKE = "ilike"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


BINARY_OPERATORS: Dict[str, str] = {
    FilterOperator.EQ.value: "=",
    FilterOperator.NE.value: "<>",
    FilterOperator.LT.value: "<",
    FilterOperator.LTE.value: "<=",
    FilterOperator.GT.value: ">",
    FilterOperator.GTE.value: ">=",
    FilterOperator.LIKE.value: "LIKE",
    FilterOperator.ILIKE.value: "ILIKE",
}

LIST_OPERATORS: Dict[str, str] = {
    FilterOperator.IN.value: "IN",
    FilterOperator.NOT_IN.value: "NOT IN",
}

NULLARY_OPERATORS: Dict[str, str] = {
    FilterOperator.IS_NULL.value: "IS NULL",
    FilterOperator.IS_NOT_NULL.value: "IS NOT NULL",
}


class SearchFilter(BaseModel):
    """One pre-filter condition, applied before ranking.

    Pre-filtering matters for approximate nearest neighbour search: the
    condition is pushed into the same scan that walks the vector index, so the
    engine ranks only rows that already satisfy it.
    """

    column: str = Field(
        description="Column to filter on. Must exist in the table being searched."
    )
    operator: FilterOperator = Field(
        default=FilterOperator.EQ,
        description=(
            "Comparison to apply. 'in'/'not_in' take a list value; "
            "'is_null'/'is_not_null' take no value."
        ),
    )
    value: Any = Field(
        default=None,
        description=(
            "Value to compare against. A list for 'in'/'not_in', omitted for "
            "'is_null'/'is_not_null'."
        ),
    )


class SqlParams:
    """Collects bind values and hands out the matching ``$n`` placeholders."""

    def __init__(self) -> None:
        self.values: List[Any] = []

    def add(self, value: Any) -> str:
        """Bind one value and return the placeholder that refers to it."""
        self.values.append(value)
        return f"${len(self.values)}"


def coerce_filters(filters: Optional[Iterable[Any]]) -> List[SearchFilter]:
    """Accept filters as models or plain dicts and return validated models."""
    if not filters:
        return []

    coerced: List[SearchFilter] = []
    for index, item in enumerate(filters):
        if isinstance(item, SearchFilter):
            coerced.append(item)
        elif isinstance(item, dict):
            try:
                coerced.append(SearchFilter(**item))
            except Exception as exc:
                raise ValueError(f"filters[{index}] is not a valid filter: {exc}") from exc
        else:
            raise ValueError(
                f"filters[{index}] must be an object with 'column', 'operator' and "
                f"'value', got {type(item).__name__}"
            )
    return coerced


def resolve_column(name: str, columns: Sequence[Dict[str, Any]], label: str) -> Dict[str, Any]:
    """Look a column up in catalog metadata, or raise a descriptive error.

    Resolving through the catalog is what makes it safe to interpolate the
    name into SQL afterwards: only names the server itself read back from
    ``pg_attribute`` ever reach the statement text.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{label} must be a non-empty column name, got {name!r}")

    for column in columns:
        if column["column_name"] == name:
            return column

    available = ", ".join(column["column_name"] for column in columns)
    raise ValueError(f"{label} '{name}' does not exist; available columns are: {available}")


def quote_column(column: Dict[str, Any], alias: Optional[str] = None) -> str:
    """Quote a catalog-resolved column, optionally qualified by a table alias."""
    quoted = SQLValidator.quote_identifier(column["column_name"])
    return f"{alias}.{quoted}" if alias else quoted


def build_select_list(
    select_columns: Optional[Sequence[str]],
    columns: Sequence[Dict[str, Any]],
    alias: Optional[str] = None,
) -> tuple:
    """Build the projection for a search result, and the names it produces.

    Embedding columns are cast to text so that a client with no pgvector type
    codec still receives a readable value.  When the caller names no columns,
    embeddings are dropped entirely: they are large and of no use to an agent
    that is reading the rows.
    """
    if select_columns is None:
        chosen = [column for column in columns if column["type_name"] not in VECTOR_TYPES]
        if not chosen:
            chosen = list(columns)
    else:
        if not isinstance(select_columns, (list, tuple)) or not select_columns:
            raise ValueError(
                "select_columns must be a non-empty list of column names, or omitted "
                "to select every non-embedding column"
            )
        chosen = [
            resolve_column(name, columns, f"select_columns[{index}]")
            for index, name in enumerate(select_columns)
        ]

    projection: List[str] = []
    names: List[str] = []
    for column in chosen:
        reference = quote_column(column, alias)
        output_name = SQLValidator.quote_identifier(column["column_name"])
        names.append(column["column_name"])
        if column["type_name"] in VECTOR_TYPES:
            projection.append(f"{reference}::text AS {output_name}")
        else:
            projection.append(f"{reference} AS {output_name}")
    return projection, names


def build_filter_sql(
    filters: Sequence[SearchFilter],
    columns: Sequence[Dict[str, Any]],
    params: SqlParams,
    alias: Optional[str] = None,
) -> str:
    """Build a parameterized boolean expression, without the WHERE keyword."""
    clauses: List[str] = []

    for index, condition in enumerate(filters):
        label = f"filters[{index}].column"
        column = resolve_column(condition.column, columns, label)
        reference = quote_column(column, alias)
        operator = (
            condition.operator.value
            if isinstance(condition.operator, FilterOperator)
            else str(condition.operator)
        )

        if operator in NULLARY_OPERATORS:
            clauses.append(f"{reference} {NULLARY_OPERATORS[operator]}")
        elif operator in LIST_OPERATORS:
            if not isinstance(condition.value, (list, tuple)) or not condition.value:
                raise ValueError(
                    f"filters[{index}] operator '{operator}' on column "
                    f"'{condition.column}' requires a non-empty list value, got "
                    f"{condition.value!r}"
                )
            placeholders = ", ".join(params.add(item) for item in condition.value)
            clauses.append(f"{reference} {LIST_OPERATORS[operator]} ({placeholders})")
        elif operator in BINARY_OPERATORS:
            if condition.value is None:
                raise ValueError(
                    f"filters[{index}] operator '{operator}' on column "
                    f"'{condition.column}' requires a value; use 'is_null' to test "
                    f"for NULL"
                )
            clauses.append(
                f"{reference} {BINARY_OPERATORS[operator]} {params.add(condition.value)}"
            )
        else:
            supported = ", ".join(sorted(item.value for item in FilterOperator))
            raise ValueError(
                f"filters[{index}] operator '{operator}' is not supported; "
                f"supported operators are: {supported}"
            )

    return " AND ".join(clauses)


def validate_query_vector(
    query_vector: Sequence[float], label: str = "query_vector"
) -> List[float]:
    """Check an embedding's shape and values, and return it as floats.

    Kept separate from rendering so that a malformed vector is rejected on its
    own terms, before the column lookup turns the complaint into a dimension
    mismatch.
    """
    if not isinstance(query_vector, (list, tuple)) or not query_vector:
        raise ValueError(f"{label} must be a non-empty list of numbers, got {query_vector!r}")

    values: List[float] = []
    for index, item in enumerate(query_vector):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(
                f"{label}[{index}] must be a number, got {type(item).__name__} ({item!r})"
            )
        value = float(item)
        if not math.isfinite(value):
            raise ValueError(f"{label}[{index}] must be finite, got {value}")
        values.append(value)
    return values


def format_vector_literal(
    query_vector: Sequence[float],
    type_name: str = "vector",
    label: str = "query_vector",
) -> str:
    """Render an embedding as the text literal its pgvector type accepts.

    The caller always supplies a dense list, because that is what an embedding
    model produces. A sparsevec column stores the same vector as its non-zero
    entries, so the literal has to be written in that form: dense brackets are
    rejected outright by its input function.
    """
    values = validate_query_vector(query_vector, label)

    if type_name == SPARSEVEC_TYPE:
        # pgvector's sparse form lists only the non-zero entries, indexed from
        # one, with the full width after the slash. An all-zero vector is
        # written as an empty entry list.
        entries = [
            f"{index}:{value!r}"
            for index, value in enumerate(values, start=1)
            if value != 0.0
        ]
        return "{" + ",".join(entries) + "}/" + str(len(values))

    return "[" + ",".join(repr(value) for value in values) + "]"


def resolve_metric(metric: Any) -> str:
    """Map a metric name onto its pgvector operator."""
    value = metric.value if isinstance(metric, SearchMetric) else str(metric)
    try:
        return METRIC_OPERATORS[value]
    except KeyError:
        supported = ", ".join(sorted(METRIC_OPERATORS))
        raise ValueError(
            f"metric '{value}' is not supported; supported metrics are: {supported}"
        ) from None


def resolve_query_mode(query_mode: Any) -> str:
    """Map a query mode onto its tsquery constructor."""
    value = query_mode.value if isinstance(query_mode, TextQueryMode) else str(query_mode)
    try:
        return TEXT_QUERY_FUNCTIONS[value]
    except KeyError:
        supported = ", ".join(sorted(TEXT_QUERY_FUNCTIONS))
        raise ValueError(
            f"query_mode '{value}' is not supported; supported modes are: {supported}"
        ) from None


def validate_positive_int(value: Any, label: str) -> int:
    """Reject anything that is not a positive integer.

    Used for the pgvector recall knobs, whose real upper bounds belong to the
    server and are read from pg_settings rather than guessed here.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer, got {type(value).__name__} ({value!r})")
    if value < 1:
        raise ValueError(f"{label} must be at least 1, got {value}")
    return value


def validate_limit(limit: Any, label: str = "limit", maximum: int = MAX_LIMIT) -> int:
    """Reject a row limit that is not a positive integer within bounds."""
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError(f"{label} must be an integer, got {type(limit).__name__} ({limit!r})")
    if limit < 1 or limit > maximum:
        raise ValueError(f"{label} must be between 1 and {maximum}, got {limit}")
    return limit


def validate_vector_column(column: Dict[str, Any], query_vector: Sequence[float]) -> None:
    """Check that a column holds embeddings and that the dimensions agree."""
    if column["type_name"] not in VECTOR_TYPES:
        supported = ", ".join(sorted(VECTOR_TYPES))
        raise ValueError(
            f"vector_column '{column['column_name']}' has type "
            f"{column['type_display']}, which is not an embedding type; expected one "
            f"of: {supported}"
        )

    # pgvector stores the declared dimension directly in atttypmod; -1 means the
    # column was declared without one and accepts any width.
    declared = column["type_modifier"]
    if declared and declared > 0 and len(query_vector) != declared:
        raise ValueError(
            f"query_vector has {len(query_vector)} dimensions but column "
            f"'{column['column_name']}' is {column['type_display']}; they must match"
        )


def warmup_literal(type_name: str) -> str:
    """Return a literal of the given embedding type, used to load pgvector."""
    return WARMUP_LITERALS.get(type_name, "[1]")
