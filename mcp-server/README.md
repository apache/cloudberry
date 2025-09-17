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

#### Query Tools
- `execute_query(query, params, readonly)` - Execute a SQL query
- `explain_query(query, params)` - Get query execution plan
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

## Security Features

- **SQL Injection Prevention**: Comprehensive query validation
- **Read-Only Constraints**: Configurable write protection
- **Parameterized Queries**: Safe parameter handling
- **Connection Pooling**: Secure connection management
- **Sensitive Table Protection**: Blocks access to system tables


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