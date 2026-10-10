<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Cloudberry MCP Server

A Model Communication Protocol (MCP) server for Apache Cloudberry database interaction, providing secure and efficient database management capabilities through AI-ready interfaces.

## Features

- **Database Metadata Resources**: Access schemas, tables, views, indexes, and column information
- **Retrieval Tools**: Vector, full-text, and hybrid search for agent and RAG workloads
- **Lance Datasets**: Read and vector-search Lance datasets exposed through `lance_fdw` foreign tables
- **Safe Query Tools**: Execute parameterized SQL queries with security validation
- **Administrative Tools**: Table statistics, large table analysis, and query optimization
- **Context-Aware Prompts**: Predefined prompts for common database tasks
- **Security-First Design**: SQL injection prevention, read-only constraints, and connection pooling
- **Async Performance**: Built with asyncpg for high-performance database operations

## Prerequisites

- Python 3.8+
- uv (for dependency management)

## Installation

### Install uv

```bash
curl -sSfL https://astral.sh/uv/install.sh | sh
```

### Install Dependencies

```bash
cd mcp-server
uv venv
source .venv/bin/activate
uv sync
```

### Install Project

```bash
uv pip install -e .
```

### Build Project

```bash
uv build
```

## Configuration

Create a `.env` file in the project root:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password

# Server Configuration
MCP_HOST=localhost
MCP_PORT=8000
MCP_DEBUG=false

# Query result limits (see "Query Results")
MCP_MAX_ROWS=1000
MCP_MAX_VALUE_LENGTH=10000
MCP_MAX_RESULT_BYTES=1000000
```

## Usage

### Running the Server

```bash
# Run the MCP server
python -m cbmcp.server

# Or run with cloudberry-mcp-server
cloudberry-mcp-server

# Or run with custom configuration
MCP_HOST=0.0.0.0 MCP_PORT=8080 python -m cbmcp.server
```

### Testing the Client

```bash
# Run the test client
python -m cbmcp.client
```

## API Reference

### Resources

- `postgres://schemas` - List all database schemas
- `postgres://database/info` - Get general database info
- `postgres://database/summary` - Get detailed database summary

### Tools

#### Retrieval Tools
- `list_searchable_columns(schema, table, include_unindexed_text)` - Discover embedding columns, tsvector columns, and text columns with a full-text index
- `vector_search(schema, table, vector_column, query_vector, limit, select_columns, filters, metric, probes, ef_search)` - Nearest neighbour search, optionally pre-filtered, on a pgvector column or a Lance vector column
- `fulltext_search(schema, table, text_column, query, limit, select_columns, filters, language, query_mode, match_mode)` - Keyword search ranked by `ts_rank`
- `hybrid_search(schema, table, vector_column, text_column, query_vector, query, limit, select_columns, filters, metric, language, query_mode, match_mode, rrf_k, candidates, probes, ef_search)` - Both of the above, fused by reciprocal rank

#### Query Tools
- `execute_query(query, params, readonly, max_rows)` - Execute a SQL query. `params` is a list bound to `$1, $2, ...` in order; with `readonly` on, the statement runs in a `READ ONLY` transaction; at most `max_rows` rows come back (see "Query Results")
- `explain_query(query, params)` - Get query execution plan. Read-only statements only, because `EXPLAIN ANALYZE` runs what it is given
- `get_table_stats(schema, table)` - Get table statistics
- `list_large_tables(limit)` - List largest tables

#### User & Permission Management
- `list_users()` - List all database users
- `list_user_permissions(username)` - List permissions for a specific user
- `list_table_privileges(schema, table)` - List privileges for a specific table

#### Schema & Structure
- `list_tables(schema, table_type)` - List the tables in a schema, foreign tables included; `table_type` narrows it to `base` or `foreign`
- `list_columns(schema, table)` - List a table's columns; `type` is the declared type, such as `real[]` or `numeric(10,2)`, where `data_type` is the information_schema name
- `list_constraints(schema, table)` - List constraints for a table
- `list_foreign_keys(schema, table)` - List foreign keys for a table
- `list_referenced_tables(schema, table)` - List tables that reference this table
- `get_table_ddl(schema, table)` - Get the column definitions of a table as DDL, with a partitioned table's partition key; for a foreign table, `CREATE FOREIGN TABLE` with its server and table options, values redacted except for `lance_fdw`'s own options. It describes the columns rather than recreating the table: constraints, indexes, the distribution policy and identity sequence parameters are not included

#### Performance & Monitoring
- `get_slow_queries(limit)` - List slow queries
- `get_index_usage()` - Analyze index usage statistics
- `get_table_bloat_info()` - Analyze table bloat information
- `get_database_activity()` - Show current database activity
- `get_vacuum_info()` - Get vacuum and analyze statistics

#### Database Objects
- `list_functions(schema)` - List functions in a schema
- `get_function_definition(schema, function)` - Get function definition
- `list_triggers(schema, table)` - List triggers for a table
- `list_materialized_views(schema)` - List materialized views in a schema
- `list_active_connections()` - List active database connections

### Prompts

- `analyze_query_performance` - Query optimization assistance
- `suggest_indexes` - Index recommendation guidance
- `database_health_check` - Database health assessment

## Retrieval

The retrieval tools let an agent search rather than only query. They run over
Apache Cloudberry's own storage: embeddings in `pgvector` columns, documents in
`text` or `tsvector` columns. Nothing is cached or indexed outside the database.

### Prerequisites

Vector search covers `vector`, `halfvec` and `sparsevec` columns. A query
vector is always supplied as a plain list of numbers; for a `sparsevec` column
it is converted to that type's own text form, which lists only the non-zero
entries.

Vector search needs the `pgvector` extension in the target database:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Full-text search needs no extension. A GIN index over `to_tsvector(...)` is
what makes it fast, and is also what `list_searchable_columns` looks for when
deciding whether a text column is worth reporting.

### Discovering what to search

An agent that cannot see which columns hold embeddings has to guess from
names. `list_searchable_columns` reports the column kind, the declared
dimension, and the indexes defined on it:

```json
[
  {"schema": "public", "table": "docs", "column": "embedding", "kind": "vector",
   "type": "vector(8)", "dimension": 8, "full_text_indexed": false,
   "indexes": [{"name": "docs_embedding_ivf", "method": "ivfflat", "on_expression": false}]},
  {"schema": "public", "table": "docs", "column": "content", "kind": "text",
   "type": "text", "dimension": null, "full_text_indexed": true,
   "indexes": [{"name": "docs_content_gin", "method": "gin", "on_expression": true}]}
]
```

### Filters

`filters` is a list of conditions applied *before* ranking, so an approximate
nearest neighbour search ranks only rows that already satisfy them:

```json
{"filters": [{"column": "customer_id", "operator": "in", "value": [1, 2, 3]}]}
```

Operators are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `not_in`, `like`,
`ilike`, `is_null`, `is_not_null`. Column names are checked against the catalog
and values are bound as query parameters, so a filter cannot inject SQL. For
anything these do not express, use `execute_query`.

### Matching every term, or any of them

PostgreSQL's query constructors require *every* term to be present, so
`query latency slow` becomes `'queri' & 'latenc' & 'slow'`. A document holding
the first two but not the third does not match. The longer and more natural the
query, the likelier it is that nothing matches at all, and the caller gets an
empty result rather than an error. To an agent that reads as "this database
holds nothing on the subject", which is the wrong conclusion when most of the
terms did match something.

`match_mode` decides how the terms are combined:

| Mode | Behaviour |
| --- | --- |
| `all` | Every term must appear. Precise, and empty when one term is missing |
| `any` | Any term may appear. `ts_rank` still puts the fuller matches first |
| `all_then_any` | Requires every term, and widens to any of them only if that matched nothing. The default |

A query that carries an explicit operator is never widened. ORing the lexemes
of `latency -dashboard` would return exactly the rows the caller excluded, and
flattening a phrase is the opposite of asking for one, so a negation or a
phrase is read as an instruction to leave the query alone. The result then
reports `widening_refused: true` alongside an empty row set.

The result always says which reading produced the rows, so widening is never
silent:

```json
{"search": {"match_mode": "all_then_any", "matched_with": "any", "widened": true}}
```

Under `all_then_any` the existence check is how the decision is made, so it
runs on every call, not only when the strict reading fails. With a full-text
index in place it is an index probe. Widening cannot rescue a query made
entirely of stop words, because the parser produces no lexemes to widen to.

This matters most in `hybrid_search`. Without it the keyword arm can go empty
while the vector arm still returns rows, so the fused result is pure vector
search presenting itself as hybrid, and nothing in the response says so.

### Recall

An IVFFlat index reads `ivfflat.probes` lists per scan, and the default of 1
can miss the true nearest neighbour outright. `probes` (IVFFlat) and
`ef_search` (HNSW) trade latency for recall, and the settings that were applied
come back in the result so a run can be reproduced:

```json
{"search": {"mode": "vector", "settings": {"ivfflat.probes": 20}}}
```

The settings are session level and reset once the statement finishes, because
Apache Cloudberry does not dispatch `SET LOCAL` to the segments where the index
scan runs. Their upper bounds come from the installed pgvector rather than from
this server, so a value it would refuse is rejected before anything is applied:
`hnsw.ef_search` stops at 1000 while `ivfflat.probes` runs to 32768.

Recall also decides whether a pre-filtered search finds anything at all. The
filter is applied inside the index scan, so a selective filter combined with a
low probe count can return no rows even though matching rows exist. Raise
`probes` before concluding that a filter matched nothing.

### Results

Every retrieval tool returns the rows, the generated SQL, and what it searched:

```json
{
  "columns": ["id", "content", "distance"],
  "rows": [[4138, "...", 0.27]],
  "row_count": 1,
  "sql": "SELECT ... ORDER BY \"embedding\" <-> $1::text::\"vector\" LIMIT $2",
  "search": {"mode": "vector", "metric": "l2", "limit": 1}
}
```

Embedding columns are left out of the default projection because they are large
and of no use to a reader; name one in `select_columns` and it comes back as
text.

A table is free to have a column of its own called `rank` or `score`. Because
rows come back as positional lists, the computed column is renamed rather than
duplicated, and `search` names it: `distance_column`, `rank_column`,
`score_column`, `vector_rank_column`, `text_rank_column`. Read the name from
there rather than assuming it.

`hybrid_search` adds `score`, `vector_rank` and `text_rank`. A `null` rank means
that arm did not return the row. A row scores
`1 / (rrf_k + rank)` from each arm it appears in, so a row that only one arm
found still places. Ranks are fused rather than raw scores because a distance
and a `ts_rank` share no scale, but their orderings do.

## Query Results

A tool result is JSON, so `execute_query` and the retrieval tools convert the
values that have no JSON form of their own:

| Column type | Comes back as |
|---|---|
| `bytea` | its hex text form, `"\\x4530a8"`, which `'...'::bytea` reads back |
| composite (including a Lance struct) | an object keyed by attribute name |
| array | a list, converted element by element |
| range | `{"lower", "upper", "lower_inc", "upper_inc", "isempty"}` |
| `numeric` | a string, so that no digits are lost |
| `float4`, `float8` NaN or infinity | `"NaN"`, `"Infinity"`, `"-Infinity"`, as PostgreSQL spells them; JSON has no such numbers, and `null` would read as SQL NULL |
| `bit`, `varbit` | the bit string, `"101"` |
| `path` | `{"points": [[x, y], ...], "is_closed": ...}`; `polygon` is the list of points; `point` is `[x, y]` and the other geometric types are nested lists of points and numbers |
| any other type | its text form, such as `"10.0.0.1"` for `inet`; a type with no text form in the driver is named instead of shown |

With `readonly` left at its default, `execute_query` runs the statement in a
`READ ONLY` transaction, so a `SELECT` that writes to a table, or calls a
function that does, such as `nextval()`, is refused. That is all it refuses:
PostgreSQL's read-only transactions stop writes to ordinary tables, not every
side effect, and functions such as `pg_terminate_backend()` or
`pg_read_file()` still run for a role allowed to call them. It is not a
sandbox; see "Security Features". With `readonly` off, rows are read up to one
past the row limit below, so a function with a side effect runs for at most
one more row than comes back.

Everything a tool returns lands in the agent's context, so a result is bounded
three ways:

- `execute_query` returns at most `MCP_MAX_ROWS` rows (default 1000), or fewer
  when `max_rows` asks for fewer. The query is read through a cursor and stops
  one row past the limit, so a `SELECT` with no `LIMIT` costs what it returns,
  not the whole table.
- A string longer than `MCP_MAX_VALUE_LENGTH` characters (default 10000) is cut
  to that length, with no marker in the value itself; a `bytea` is cut on a
  byte boundary, so it still reads back. Strings inside arrays and composites
  are cut too, and reported under the outer column.
- Rows stop being added once the result, rows and every other field together,
  reaches `MCP_MAX_RESULT_BYTES` of JSON (default 1000000). This is what bounds arrays, which have no length to cut:
  a thousand rows of 1536-dimension embeddings would otherwise be tens of
  megabytes. A single row larger than the budget comes back as no rows; select
  fewer columns.

`"truncated": true` means there were more rows, and `truncated_by` says which
limit stopped them; page with `ORDER BY` and `LIMIT`/`OFFSET`. When a value was
cut, the result says how many and in which columns:

```json
{"columns": ["id", "txt"], "rows": [[4, "70c16db31fb8"]], "row_count": 1,
 "truncated": false, "max_rows": 1000,
 "truncated_values": 1, "truncated_columns": ["txt"], "max_value_length": 10000}
```

The retrieval tools apply the value limit and the byte budget in the same way.
All three limits must be positive integers; the server refuses to start
otherwise.

## Lance Datasets

`lance_fdw` is a separately installed foreign data wrapper, not part of Apache
Cloudberry, that exposes a Lance dataset as a foreign table. The tools read it
like any other table, and `lance_fdw` hands filters, projections and vector
searches to Lance. Vector search needs `lance_fdw` 0.2 or later.

- **Finding the tables.** `list_tables` lists foreign tables beside ordinary
  ones, and `table_type: "foreign"` lists only them. `get_table_ddl` shows the
  server and the table options, such as the dataset `uri`. It shows the values
  of `lance_fdw`'s own table and column options, which hold no credentials,
  with any `user:password@` taken out of a URI; every option of another
  wrapper is shown by name with its value as `<redacted>`, since a wrapper may
  keep keys there. Server options and user mappings are never read.
- **Reading.** `execute_query` reads a Lance table with plain SQL. A Lance
  vector column is `real[]` and comes back as a list of numbers; a struct comes
  back as an object.
- **Vector search.** `lance_fdw` defines `<->` (Euclidean), `<=>` (cosine
  distance) and `<#>` (negative inner product) on `real[]`. `vector_search`
  accepts a `real[]` column of a `lance_fdw` table, and so does a hand-written
  query:

  ```sql
  SELECT id FROM lance.docs WHERE cat = $2 ORDER BY emb <-> $1 LIMIT 10
  ```

  `explain_query` shows whether the search went to Lance: look for
  `Lance Vector Search` and `Lance Filter` under the foreign scan.
- **Discovery.** `list_searchable_columns` reports such a column with
  `"embedding": "lance"`. Its `dimension` is `null`, because `real[]` declares
  none; a query vector of the wrong length fails when the search starts, with
  `lance_fdw: vectors of different dimensions`. Its `indexes` list is empty,
  because a Lance index lives in the dataset, not in `pg_index`.

On a Lance column `vector_search` supports the `l2`, `cosine` and
`inner_product` metrics, not `l1`. `probes` and `ef_search` are pgvector
settings and are rejected; a Lance search is tuned by the server settings
`lance_fdw.nprobes` and `lance_fdw.refine_factor`. `hybrid_search` does not
accept a Lance column yet; run `vector_search` and `fulltext_search` on it
separately.

Every `real[]` column of a `lance_fdw` table counts as an embedding, because
the catalog cannot tell a vector from any other list of floats: a variable
length list of scores reads as `real[]` too, and is reported and searchable
like a vector. A `real[]` column outside a `lance_fdw` table is an ordinary
array, not an embedding.

## Security Features

- **SQL Injection Prevention**: Comprehensive query validation
- **Read-Only Constraints**: With `readonly` on, `execute_query` refuses statements that write and runs the rest in a `READ ONLY` transaction. This guards against writes, not against every side effect: run the server as a role with the least privilege it needs, without superuser and without `EXECUTE` on administrative functions such as `pg_terminate_backend()`, `pg_reload_conf()` and `pg_read_file()`
- **Parameterized Queries**: Safe parameter handling
- **Connection Pooling**: Secure connection management
- **Sensitive Table Protection**: Blocks access to system tables
- **Catalog-Checked Identifiers**: Retrieval tools resolve every schema, table and column name against the catalog before it reaches a statement, and quote it afterwards


## Quick Start with Cloudberry Demo Cluster

This section shows how to quickly set up and test the Cloudberry MCP Server using a local Cloudberry demo cluster. This is ideal for development and testing purposes. 

Assume you already have a running [Cloudberry demo cluster](https://cloudberry.apache.org/docs/deployment/set-demo-cluster) and install & build MCP server as described above.

1. Configure local connections in `pg_hba.conf`

**Note**: This configuration is for demo purposes only. Do not use `trust` authentication in production environments.

```bash
[gpadmin@cdw]$ vi ~/cloudberry/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_hba.conf
```

Add the following lines to the end of the pg_hba.conf:

```
# IPv4 local connections
host    all     all     127.0.0.1/32    trust
# IPv6 local connections
host    all     all     ::1/128         trust
```

After modifying `pg_hba.conf`, reload the configuration parameters:
```bash
[gpadmin@cdw]$ gpstop -u
```

2. Create environment configuration

Create a `.env` in the project root directory:

```
# Database Configuration (Demo cluster defaults)
DB_HOST=localhost
DB_PORT=7000
DB_NAME=postgres
DB_USER=gpadmin
# No password required for demo cluster

# Server Configuration
MCP_HOST=localhost
MCP_PORT=8000
MCP_DEBUG=false
```

3. Start the MCP server

```bash
MCP_HOST=0.0.0.0 MCP_PORT=8000 python -m cbmcp.server
```

You should see output indicating the server is running:
```
[09/17/25 14:07:50] INFO     Starting MCP server 'Apache Cloudberry MCP Server' with transport        server.py:1572
                             'streamable-http' on http://0.0.0.0:8000/mcp/
```

4. Configure your MCP client.

Add the following server configuration to your MCP client:

- Server Type: Streamable-HTTP
- URL: http://[YOUR_HOST_IP]:8000/mcp

Replace `[YOUR_HOST_IP]` with your actual host IP address.


## LLM Client Integration

### Claude Desktop Configuration

Add the following configuration to your Claude Desktop configuration file:

#### Stdio Transport (Recommended)

```json
{
  "mcpServers": {
    "cloudberry-mcp-server": {
      "command": "uvx",
      "args": [
        "--with",
        "PATH/TO/cbmcp-0.1.0-py3-none-any.whl",
        "python",
        "-m",
        "cbmcp.server",
        "--mode",
        "stdio"
      ],
      "env": {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "dvdrental",
        "DB_USER": "yangshengwen",
        "DB_PASSWORD": ""
      }
    }
  }
}
```

#### HTTP Transport

```json
{
  "mcpServers": {
    "cloudberry-mcp-server": {
      "type": "streamable-http",
      "url": "https://localhost:8000/mcp/",
      "headers": {
        "Authorization": ""
      }
    }
  }
}
```

### Cursor Configuration

For Cursor IDE, add the configuration to your `.cursor/mcp.json` file:

```json
{
  "mcpServers": {
    "cloudberry-mcp": {
      "command": "uvx",
      "args": ["--with", "cbmcp", "python", "-m", "cbmcp.server", "--mode", "stdio"],
      "env": {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "dvdrental",
        "DB_USER": "postgres",
        "DB_PASSWORD": "your_password"
      }
    }
  }
}
```

### Windsurf Configuration

For Windsurf IDE, configure in your settings:

```json
{
  "mcp": {
    "servers": {
      "cloudberry-mcp": {
        "type": "stdio",
        "command": "uvx",
        "args": ["--with", "cbmcp", "python", "-m", "cbmcp.server", "--mode", "stdio"],
        "env": {
          "DB_HOST": "localhost",
          "DB_PORT": "5432",
          "DB_NAME": "dvdrental",
          "DB_USER": "postgres",
          "DB_PASSWORD": "your_password"
        }
      }
    }
  }
}
```

### VS Code with Cline

For VS Code with the Cline extension, add to your settings:

```json
{
  "cline.mcpServers": {
    "cloudberry-mcp": {
      "command": "uvx",
      "args": ["--with", "cbmcp", "python", "-m", "cbmcp.server", "--mode", "stdio"],
      "env": {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "dvdrental",
        "DB_USER": "postgres",
        "DB_PASSWORD": "your_password"
      }
    }
  }
}
```

### Installation via pip

If you prefer to install the package globally instead of using uvx:

```bash
# Install the package
pip install cbmcp-0.1.0-py3-none-any.whl

# Or using pip install from source
pip install -e .

# Then use in configuration
{
  "command": "python",
  "args": ["-m", "cbmcp.server", "--mode", "stdio"]
}
```

### Environment Variables

All configurations support the following environment variables:

- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: postgres)
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `MCP_HOST`: Server host for HTTP mode (default: localhost)
- `MCP_PORT`: Server port for HTTP mode (default: 8000)
- `MCP_DEBUG`: Enable debug logging (default: false)

### Troubleshooting

#### Common Issues

1. **Connection refused**: Ensure Apache Cloudberry is running and accessible
2. **Authentication failed**: Check database credentials in environment variables
3. **Module not found**: Ensure the package is installed correctly
4. **Permission denied**: Check file permissions for the package

#### Debug Mode

Enable debug logging by setting:
```bash
export MCP_DEBUG=true
```

## License

Apache License 2.0