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
- `vector_search(schema, table, vector_column, query_vector, limit, select_columns, filters, metric, probes, ef_search)` - Nearest neighbour search, optionally pre-filtered
- `fulltext_search(schema, table, text_column, query, limit, select_columns, filters, language, query_mode, match_mode)` - Keyword search ranked by `ts_rank`
- `hybrid_search(schema, table, vector_column, text_column, query_vector, query, limit, select_columns, filters, metric, language, query_mode, match_mode, rrf_k, candidates, probes, ef_search)` - Both of the above, fused by reciprocal rank

#### Query Tools
- `execute_query(query, params, readonly)` - Execute a SQL query. `params` is a list bound to `$1, $2, ...` in order
- `explain_query(query, params)` - Get query execution plan. Read-only statements only, because `EXPLAIN ANALYZE` runs what it is given
- `get_table_stats(schema, table)` - Get table statistics
- `list_large_tables(limit)` - List largest tables

#### User & Permission Management
- `list_users()` - List all database users
- `list_user_permissions(username)` - List permissions for a specific user
- `list_table_privileges(schema, table)` - List privileges for a specific table

#### Schema & Structure
- `list_constraints(schema, table)` - List constraints for a table
- `list_foreign_keys(schema, table)` - List foreign keys for a table
- `list_referenced_tables(schema, table)` - List tables that reference this table
- `get_table_ddl(schema, table)` - Get DDL statement for a table

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

## Security Features

- **SQL Injection Prevention**: Comprehensive query validation
- **Read-Only Constraints**: Configurable write protection
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