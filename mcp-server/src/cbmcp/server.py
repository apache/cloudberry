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
from .search import SearchFilter, SearchMetric, TextMatchMode, TextQueryMode
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
            params: Annotated[Optional[List[Any]], Field(description="Values bound to $1, $2, ... in the query, in order")] = None,
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
            params: Annotated[Optional[List[Any]], Field(description="Values bound to $1, $2, ... in the query, in order")] = None
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

        # --------------------------------------------------------------
        # Retrieval tools: what an agent needs to search, not just query
        # --------------------------------------------------------------

        @self.mcp.tool()
        async def list_searchable_columns(
            schema: Annotated[Optional[str], Field(description="Restrict the result to one schema")] = None,
            table: Annotated[Optional[str], Field(description="Restrict the result to one table")] = None,
            include_unindexed_text: Annotated[bool, Field(description="Also report text columns that have no full-text index")] = False
        ) -> List[Dict[str, Any]]:
            """Discover which columns can be searched, and how.

            Reports embedding columns with their dimension, tsvector columns,
            and text columns carrying a full-text index, each with the indexes
            defined on it. Call this before vector_search, fulltext_search or
            hybrid_search to choose a target instead of guessing from names.
            """
            logger.info(f"Listing searchable columns in schema: {schema}, table: {table}")
            try:
                return await self.db_manager.list_searchable_columns(
                    schema, table, include_unindexed_text
                )
            except Exception as e:
                return f"Error listing searchable columns: {str(e)}"

        @self.mcp.tool()
        async def vector_search(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")],
            vector_column: Annotated[str, Field(description="The embedding column to rank by")],
            query_vector: Annotated[List[float], Field(description="The query embedding; its length must match the column's declared dimension")],
            limit: Annotated[int, Field(description="How many rows to return")] = 10,
            select_columns: Annotated[Optional[List[str]], Field(description="Columns to return; defaults to every non-embedding column")] = None,
            filters: Annotated[Optional[List[SearchFilter]], Field(description="Conditions applied before ranking, pushed into the index scan")] = None,
            metric: Annotated[SearchMetric, Field(description="Distance metric; all four order ascending, so smaller is always better")] = SearchMetric.L2,
            probes: Annotated[Optional[int], Field(description="ivfflat lists to probe. The default of 1 can miss the true nearest neighbour; raise it to trade latency for recall")] = None,
            ef_search: Annotated[Optional[int], Field(description="HNSW candidate list size; the same recall-for-latency trade on an HNSW index")] = None
        ) -> Dict[str, Any]:
            """Find the rows whose embedding is nearest to a query vector.

            Filters are applied inside the same scan that walks the vector
            index, so this is a pre-filtered nearest neighbour search, not a
            filter over an already truncated result. The generated SQL is
            returned alongside the rows.
            """
            logger.info(f"Vector search on {schema}.{table}.{vector_column}, limit: {limit}")
            try:
                return await self.db_manager.vector_search(
                    schema=schema,
                    table=table,
                    vector_column=vector_column,
                    query_vector=query_vector,
                    limit=limit,
                    select_columns=select_columns,
                    filters=filters,
                    metric=metric,
                    probes=probes,
                    ef_search=ef_search,
                )
            except Exception as e:
                return {"error": f"Error running vector search: {str(e)}"}

        @self.mcp.tool()
        async def fulltext_search(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")],
            text_column: Annotated[str, Field(description="The text or tsvector column to search")],
            query: Annotated[str, Field(description="The search string")],
            limit: Annotated[int, Field(description="How many rows to return")] = 10,
            select_columns: Annotated[Optional[List[str]], Field(description="Columns to return; defaults to every non-embedding column")] = None,
            filters: Annotated[Optional[List[SearchFilter]], Field(description="Conditions applied before ranking")] = None,
            language: Annotated[str, Field(description="Text search configuration used to parse both document and query")] = "english",
            query_mode: Annotated[TextQueryMode, Field(description="How the search string is parsed: websearch accepts quotes and OR, plain treats it as words, phrase requires the exact sequence")] = TextQueryMode.WEBSEARCH,
            match_mode: Annotated[TextMatchMode, Field(description="Whether every term must appear. all_then_any requires all terms and widens to any of them only when that matched nothing, so a natural sentence does not come back empty; the result reports which was used")] = TextMatchMode.ALL_THEN_ANY
        ) -> Dict[str, Any]:
            """Rank rows by full-text relevance over one text column.

            A tsvector column is searched as stored; a text column is parsed on
            the fly, which matches an expression index built over to_tsvector().
            Results are ordered by ts_rank.
            """
            logger.info(f"Full-text search on {schema}.{table}.{text_column}, limit: {limit}")
            try:
                return await self.db_manager.fulltext_search(
                    schema=schema,
                    table=table,
                    text_column=text_column,
                    query=query,
                    limit=limit,
                    select_columns=select_columns,
                    filters=filters,
                    language=language,
                    query_mode=query_mode,
                    match_mode=match_mode,
                )
            except Exception as e:
                return {"error": f"Error running full-text search: {str(e)}"}

        @self.mcp.tool()
        async def hybrid_search(
            schema: Annotated[str, Field(description="The schema name")],
            table: Annotated[str, Field(description="The table name")],
            vector_column: Annotated[str, Field(description="The embedding column to rank by")],
            text_column: Annotated[str, Field(description="The text or tsvector column to search")],
            query_vector: Annotated[List[float], Field(description="The query embedding; its length must match the column's declared dimension")],
            query: Annotated[str, Field(description="The search string")],
            limit: Annotated[int, Field(description="How many rows to return")] = 10,
            select_columns: Annotated[Optional[List[str]], Field(description="Columns to return; defaults to every non-embedding column")] = None,
            filters: Annotated[Optional[List[SearchFilter]], Field(description="Conditions applied before ranking, to both arms")] = None,
            metric: Annotated[SearchMetric, Field(description="Distance metric for the vector arm")] = SearchMetric.L2,
            language: Annotated[str, Field(description="Text search configuration for the full-text arm")] = "english",
            query_mode: Annotated[TextQueryMode, Field(description="How the search string is parsed")] = TextQueryMode.WEBSEARCH,
            match_mode: Annotated[TextMatchMode, Field(description="Whether every term must appear. all_then_any requires all terms and widens to any of them only when that matched nothing, so a natural sentence does not come back empty; the result reports which was used")] = TextMatchMode.ALL_THEN_ANY,
            rrf_k: Annotated[int, Field(description="Reciprocal rank fusion constant; larger flattens the weight given to top ranks")] = 60,
            candidates: Annotated[Optional[int], Field(description="Rows each arm contributes before fusion; defaults to five times limit, at least 50")] = None,
            probes: Annotated[Optional[int], Field(description="ivfflat lists to probe in the vector arm")] = None,
            ef_search: Annotated[Optional[int], Field(description="HNSW candidate list size for the vector arm")] = None
        ) -> Dict[str, Any]:
            """Search by meaning and by keyword at once, then fuse the rankings.

            Each arm returns its own ranked candidates and a row scores
            1 / (rrf_k + rank) from each arm it appears in. Fusing ranks rather
            than raw scores is what makes the two comparable: a distance and a
            ts_rank share no scale, but their orderings do. Rows found by only
            one arm still place, which is the point of running both.
            """
            logger.info(f"Hybrid search on {schema}.{table}, limit: {limit}")
            try:
                return await self.db_manager.hybrid_search(
                    schema=schema,
                    table=table,
                    vector_column=vector_column,
                    text_column=text_column,
                    query_vector=query_vector,
                    query=query,
                    limit=limit,
                    select_columns=select_columns,
                    filters=filters,
                    metric=metric,
                    language=language,
                    query_mode=query_mode,
                    match_mode=match_mode,
                    rrf_k=rrf_k,
                    candidates=candidates,
                    probes=probes,
                    ef_search=ef_search,
                )
            except Exception as e:
                return {"error": f"Error running hybrid search: {str(e)}"}

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
