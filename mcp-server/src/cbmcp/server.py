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
Apache Cloudberry MCP Server Implementation

A Model Communication Protocol server for Apache Cloudberry database interaction
providing resources, tools, and prompts for database management.
"""

from typing import Annotated, Any, Dict, List, Optional
import logging
from fastmcp import FastMCP
from pydantic import Field

from .config import DatabaseConfig, ServerConfig
from .database import DatabaseManager
from .prompt import (
    ANALYZE_QUERY_PERFORMANCE_PROMPT,
    SUGGEST_INDEXES_PROMPT,
    DATABASE_HEALTH_CHECK_PROMPT
)

logger = logging.getLogger(__name__)

class CloudberryMCPServer:
    """Apache Cloudberry MCP Server implementation"""
    
    def __init__(self, server_config: ServerConfig, db_config: DatabaseConfig):
        self.server_config = server_config
        self.db_config = db_config
        self.mcp = FastMCP("Apache Cloudberry MCP Server")
        self.db_manager = DatabaseManager(db_config)
        
        self._setup_resources()
        self._setup_tools()
        self._setup_prompts()

    
    def _setup_resources(self):
        """Setup MCP resources for database metadata"""
        
        @self.mcp.resource("postgres://schemas", mime_type="application/json")
        async def list_schemas() -> List[str]:
            """List all database schemas"""
            logger.info("Listing schemas")
            return await self.db_manager.list_schemas()
        
        @self.mcp.resource("postgres://database/info", mime_type="application/json")
        async def database_info() -> Dict[str, str]:
            """Get general database information"""
            logger.info("Getting database info")
            return await self.db_manager.get_database_info()
        
        @self.mcp.resource("postgres://database/summary", mime_type="application/json")
        async def database_summary() -> Dict[str, dict]:
            """Get comprehensive database summary"""
            logger.info("Getting database summary")
            return await self.db_manager.get_database_summary()

            
    def _setup_tools(self):
        """Setup MCP tools for database operations"""
        
        @self.mcp.tool()
        async def list_tables(
            schema: Annotated[str, Field(description="The schema name to list tables from")]
        ) -> List[str]:
            """List tables in a specific schema"""
            logger.info(f"Listing tables in schema: {schema}")
            try:
                return await self.db_manager.list_tables(schema)
            except Exception as e:
                return f"Error listing tables: {str(e)}"
        
        @self.mcp.tool()
        async def list_views(
            schema: Annotated[str, Field(description="The schema name to list views from")]
        ) -> List[str]:
            """List views in a specific schema"""
            logger.info(f"Listing views in schema: {schema}")
            try:
                return await self.db_manager.list_views(schema)
            except Exception as e:
                return f"Error listing views: {str(e)}"
        
        @self.mcp.tool()
        async def list_indexes(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name to list indexes for")]
        ) -> List[str]:
            """List indexes for a specific table"""
            logger.info(f"Listing indexes for table: {schema}.{table}")
            try:
                indexes = await self.db_manager.list_indexes(schema, table)
                return [f"{idx['indexname']}: {idx['indexdef']}" for idx in indexes]
            except Exception as e:
                return f"Error listing indexes: {str(e)}"
        
        @self.mcp.tool()
        async def list_columns(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name to list columns for")]
        ) -> List[Dict[str, Any]]:
            """List columns for a specific table"""
            logger.info(f"Listing columns for table: {schema}.{table}")
            try:
                return await self.db_manager.list_columns(schema, table)
            except Exception as e:
                return f"Error listing columns: {str(e)}"
        
        @self.mcp.tool()
        async def execute_query(
            query: Annotated[str, Field(description="The SQL query to execute")],
            params: Annotated[Optional[Dict[str, Any]], Field(description="The parameters for the query")] = None,
            readonly: Annotated[bool, Field(description="Whether the query is read-only")] = True
        ) -> Dict[str, Any]:
            """
            Execute a safe SQL query with parameters
            """
            logger.info(f"Executing query: {query}")
            try:
                return await self.db_manager.execute_query(query, params, readonly)
            except Exception as e:
                return {"error": f"Error executing query: {str(e)}"}
        
        @self.mcp.tool()
        async def explain_query(
            query: Annotated[str, Field(description="The SQL query to explain")],
            params: Annotated[Optional[Dict[str, Any]], Field(description="The parameters for the query")] = None
        ) -> str:
            """
            Get the execution plan for a query
            """
            logger.info(f"Explaining query: {query}")
            try:
                return await self.db_manager.explain_query(query, params)
            except Exception as e:
                return f"Error explaining query: {str(e)}"
        
        @self.mcp.tool()
        async def get_table_stats(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")],
        ) -> Dict[str, Any]:
            """
            Get statistics for a table
            """
            logger.info(f"Getting table stats for: {schema}.{table}")
            try:
                result = await self.db_manager.get_table_stats(schema, table)
                if "error" in result:
                    return result["error"]
                return result
            except Exception as e:
                return f"Error getting table stats: {str(e)}"
        
        @self.mcp.tool()
        async def list_large_tables(limit: Annotated[int, Field(description="Number of tables to return")] = 10) -> List[Dict[str, Any]]:
            """
            List the largest tables in the database
            """
            logger.info(f"Listing large tables, limit: {limit}")
            try:
                return await self.db_manager.list_large_tables(limit)
            except Exception as e:
                return f"Error listing large tables: {str(e)}"

        @self.mcp.tool()
        async def get_database_schemas() -> List[str]:
            """Get database schemas"""
            logger.info("Getting database schemas")
            try:
                return await self.db_manager.list_schemas()
            except Exception as e:
                return f"Error getting schemas: {str(e)}"
        
        @self.mcp.tool()
        async def get_database_information() -> Dict[str, str]:
            """Get general database information"""
            logger.info("Getting database information")
            try:
                return await self.db_manager.get_database_info()
            except Exception as e:
                return f"Error getting database info: {str(e)}"
        
        @self.mcp.tool()
        async def get_database_summary() -> Dict[str, dict]:
            """Get detailed database summary"""
            logger.info("Getting database summary")
            try:
                return await self.db_manager.get_database_summary()
            except Exception as e:
                return f"Error getting database summary: {str(e)}"

        @self.mcp.tool()
        async def list_users() -> List[str]:
            """List all database users"""
            logger.info("Listing database users")
            try:
                return await self.db_manager.list_users()
            except Exception as e:
                return f"Error listing users: {str(e)}"

        @self.mcp.tool()
        async def list_user_permissions(
            username: Annotated[str, Field(description="The username to check permissions for")]
        ) -> List[Dict[str, Any]]:
            """List permissions for a specific user"""
            logger.info(f"Listing permissions for user: {username}")
            try:
                return await self.db_manager.list_user_permissions(username)
            except Exception as e:
                return f"Error listing user permissions: {str(e)}"

        @self.mcp.tool()
        async def list_table_privileges(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> List[Dict[str, Any]]:
            """List privileges for a specific table"""
            logger.info(f"Listing table privileges for: {schema}.{table}")
            try:
                return await self.db_manager.list_table_privileges(schema, table)
            except Exception as e:
                return f"Error listing table privileges: {str(e)}"

        @self.mcp.tool()
        async def list_constraints(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> List[Dict[str, Any]]:
            """List constraints for a specific table"""
            logger.info(f"Listing constraints for table: {schema}.{table}")
            try:
                return await self.db_manager.list_constraints(schema, table)
            except Exception as e:
                return f"Error listing constraints: {str(e)}"

        @self.mcp.tool()
        async def list_foreign_keys(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> List[Dict[str, Any]]:
            """List foreign keys for a specific table"""
            logger.info(f"Listing foreign keys for table: {schema}.{table}")
            try:
                return await self.db_manager.list_foreign_keys(schema, table)
            except Exception as e:
                return f"Error listing foreign keys: {str(e)}"

        @self.mcp.tool()
        async def list_referenced_tables(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> List[Dict[str, Any]]:
            """List tables that reference this table"""
            logger.info(f"Listing referenced tables for: {schema}.{table}")
            try:
                return await self.db_manager.list_referenced_tables(schema, table)
            except Exception as e:
                return f"Error listing referenced tables: {str(e)}"

        @self.mcp.tool()
        async def get_slow_queries(
            limit: Annotated[int, Field(description="Number of slow queries to return")] = 10
        ) -> List[Dict[str, Any]]:
            """Get slow queries from database statistics"""
            logger.info(f"Getting slow queries, limit: {limit}")
            try:
                return await self.db_manager.get_slow_queries(limit)
            except Exception as e:
                return f"Error getting slow queries: {str(e)}"

        @self.mcp.tool()
        async def get_index_usage() -> List[Dict[str, Any]]:
            """Get index usage statistics"""
            logger.info("Getting index usage statistics")
            try:
                return await self.db_manager.get_index_usage()
            except Exception as e:
                return f"Error getting index usage: {str(e)}"

        @self.mcp.tool()
        async def get_table_bloat_info() -> List[Dict[str, Any]]:
            """Get table bloat information"""
            logger.info("Getting table bloat information")
            try:
                return await self.db_manager.get_table_bloat_info()
            except Exception as e:
                return f"Error getting table bloat info: {str(e)}"

        @self.mcp.tool()
        async def get_database_activity() -> List[Dict[str, Any]]:
            """Get current database activity"""
            logger.info("Getting database activity")
            try:
                return await self.db_manager.get_database_activity()
            except Exception as e:
                return f"Error getting database activity: {str(e)}"

        @self.mcp.tool()
        async def list_functions(
            schema: Annotated[str, Field(description="The schema name")]
        ) -> List[Dict[str, Any]]:
            """List functions in a specific schema"""
            logger.info(f"Listing functions in schema: {schema}")
            try:
                return await self.db_manager.list_functions(schema)
            except Exception as e:
                return f"Error listing functions: {str(e)}"

        @self.mcp.tool()
        async def get_function_definition(
            schema: Annotated[str, Field(description="The schema name")],
            function_name: Annotated[str, Field(description="The function name")]
        ) -> str:
            """Get function definition"""
            logger.info(f"Getting function definition: {schema}.{function_name}")
            try:
                return await self.db_manager.get_function_definition(schema, function_name)
            except Exception as e:
                return f"Error getting function definition: {str(e)}"

        @self.mcp.tool()
        async def list_triggers(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> List[Dict[str, Any]]:
            """List triggers for a specific table"""
            logger.info(f"Listing triggers for table: {schema}.{table}")
            try:
                return await self.db_manager.list_triggers(schema, table)
            except Exception as e:
                return f"Error listing triggers: {str(e)}"

        @self.mcp.tool()
        async def get_table_ddl(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> str:
            """Get DDL statement for a table"""
            logger.info(f"Getting table DDL: {schema}.{table}")
            try:
                return await self.db_manager.get_table_ddl(schema, table)
            except Exception as e:
                return f"Error getting table DDL: {str(e)}"

        @self.mcp.tool()
        async def list_materialized_views(
            schema: Annotated[str, Field(description="The schema name")]
        ) -> List[str]:
            """List materialized views in a specific schema"""
            logger.info(f"Listing materialized views in schema: {schema}")
            try:
                return await self.db_manager.list_materialized_views(schema)
            except Exception as e:
                return f"Error listing materialized views: {str(e)}"

        @self.mcp.tool()
        async def get_vacuum_info(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")]
        ) -> Dict[str, Any]:
            """Get vacuum information for a table"""
            logger.info(f"Getting vacuum info for table: {schema}.{table}")
            try:
                return await self.db_manager.get_vacuum_info(schema, table)
            except Exception as e:
                return f"Error getting vacuum info: {str(e)}"

        @self.mcp.tool()
        async def list_active_connections() -> List[Dict[str, Any]]:
            """List active database connections"""
            logger.info("Listing active connections")
            try:
                return await self.db_manager.list_active_connections()
            except Exception as e:
                return f"Error listing active connections: {str(e)}"
    
    def _setup_prompts(self):
        """Setup MCP prompts for common database tasks"""
        
        @self.mcp.prompt()
        def analyze_query_performance(
            sql: Annotated[str, Field(description="The SQL query to analyze")],
            explain: Annotated[str, Field(description="The EXPLAIN ANALYZE output")],
            table_info: Annotated[str, Field(description="The table schema information")],
        ) -> str:
            """Prompt for analyzing query performance"""
            logger.info(f"Analyzing query performance for: {sql}")
            return ANALYZE_QUERY_PERFORMANCE_PROMPT.format(
                sql=sql,
                explain=explain,
                table_info=table_info
            )        
        @self.mcp.prompt()
        def suggest_indexes(
            query: Annotated[str, Field(description="The common query pattern")],
            table_info: Annotated[str, Field(description="The table schema information")],
            table_stats: Annotated[str, Field(description="The table statistics")],
        ) -> str:
            """Prompt for suggesting indexes"""
            logger.info(f"Suggesting indexes for query: {query}")
            return SUGGEST_INDEXES_PROMPT.format(
                query=query,
                table_info=table_info,
                table_stats=table_stats
            )


        @self.mcp.prompt()
        def database_health_check() -> str:
            """Prompt for database health check"""
            logger.info(f"Checking database health")
            return DATABASE_HEALTH_CHECK_PROMPT
    
    def run(self, mode: str="http"):
        """Run the MCP server"""
        if mode == "stdio":
            return self.mcp.run(
                transport="stdio",
            )
        elif mode == "http":
            return self.mcp.run(
                transport="streamable-http",
                host=self.server_config.host,
                port=self.server_config.port,
                path=self.server_config.path,
                stateless_http=True
            )
    
    async def close(self):
        """Close the server and cleanup resources"""
        await self.db_manager.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Cloudberry MCP Server - Cloudberry database management tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode stdio
  %(prog)s --mode http --host 0.0.0.0 --port 8080
  %(prog)s --mode http --log-level INFO
  %(prog)s --help
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["stdio", "http"],
        default="http",
        help="Server mode: stdio for stdin/stdout communication, http for HTTP server (default: http)"
    )
    
    parser.add_argument(
        "--host",
        default=None,
        help="HTTP server host (default: from CLOUDBERRY_MCP_HOST env var or 127.0.0.1)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP server port (default: from CLOUDBERRY_MCP_PORT env var or 8080)"
    )
    
    parser.add_argument(
        "--path",
        default=None,
        help="HTTP server path (default: from CLOUDBERRY_MCP_PATH env var or /mcp)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="WARNING",
        help="Logging level (default: WARNING)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="Cloudberry MCP Server 1.0.0"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Create configurations
    server_config = ServerConfig.from_env()
    db_config = DatabaseConfig.from_env()
    
    # Override with command line arguments
    if args.host:
        server_config.host = args.host
    if args.port:
        server_config.port = args.port
    if args.path:
        server_config.path = args.path
    
    server = CloudberryMCPServer(server_config, db_config)
    
    try:
        logger.info(f"Starting server in {args.mode} mode...")
        server.run(args.mode)
    except KeyboardInterrupt:
        logger.error("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)
    finally:
        import asyncio
        asyncio.run(server.close())


if __name__ == "__main__":
    import sys
    
    main()
