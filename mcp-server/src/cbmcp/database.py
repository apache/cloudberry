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
Database utilities for the Apache Cloudberry MCP server
"""

import logging
import re
from typing import Any, Dict, List, Optional, Sequence
from contextlib import asynccontextmanager
import asyncpg

from .config import DatabaseConfig
from .search import (
    MAX_CANDIDATES,
    SearchMetric,
    SqlParams,
    TEXT_TYPES,
    TSVECTOR_TYPE,
    TextMatchMode,
    TextQueryMode,
    VECTOR_TYPES,
    build_filter_sql,
    build_select_list,
    build_tsquery,
    coerce_filters,
    format_vector_literal,
    quote_column,
    resolve_column,
    resolve_match_mode,
    resolve_metric,
    resolve_query_mode,
    tsquery_has_blocking_operator,
    unique_alias,
    validate_limit,
    validate_positive_int,
    validate_query_vector,
    validate_vector_column,
    warmup_literal,
)
from .security import SQLValidator


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection_pool: Optional[asyncpg.Pool] = None
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool"""
        if not self._connection_pool:
            self._connection_pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                min_size=1,
                max_size=10,
                command_timeout=60.0,
                init=self._register_vector_codecs,
            )
        
        try:
            async with self._connection_pool.acquire() as conn:
                yield conn
        except Exception as e:
            logger.error(f"Error while using database connection: {e}")
            raise
    
    @staticmethod
    async def _register_vector_codecs(conn) -> None:
        """Decode pgvector columns as their text literal.

        asyncpg ships no codec for the types pgvector adds, so selecting one
        of those columns otherwise fails with an unknown-type error. The types
        exist only where the extension is installed, so a database without it
        simply registers nothing.
        """
        try:
            records = await conn.fetch(
                "SELECT t.typname, n.nspname FROM pg_type t "
                "JOIN pg_namespace n ON n.oid = t.typnamespace "
                "WHERE t.typname = ANY($1::text[])",
                sorted(VECTOR_TYPES),
            )
        except Exception as e:
            logger.debug(f"Could not look up pgvector types: {e}")
            return

        for record in records:
            try:
                await conn.set_type_codec(
                    record["typname"],
                    schema=record["nspname"],
                    encoder=str,
                    decoder=str,
                    format="text",
                )
            except Exception as e:
                logger.debug(f"Could not register codec for {record['typname']}: {e}")

    async def execute_query(
        self, 
        query: str, 
        params: Optional[Sequence[Any]] = None,
        readonly: bool = True
    ) -> Dict[str, Any]:
        """Execute a SQL query with safety validation"""
        # Validate query for security
        is_valid, error_msg = SQLValidator.validate_query(query)
        if not is_valid:
            return {"error": f"Query validation failed: {error_msg}"}
        
        # Check readonly constraint
        if readonly and not SQLValidator.is_readonly_query(query):
            return {"error": "Only read-only queries are allowed"}
        
        try:
            async with self.get_connection() as conn:
                if params:
                    # asyncpg binds by position, so the query uses $1, $2, ...
                    # and the values arrive in the same order.
                    if not isinstance(params, (list, tuple)):
                        return {
                            "error": (
                                f"params must be a list of values bound to $1, $2, ... "
                                f"in order, got {type(params).__name__}"
                            )
                        }
                    result = await conn.fetch(query, *params)
                else:
                    result = await conn.fetch(query)
                
                if not result:
                    return {"columns": [], "rows": [], "row_count": 0}
                
                columns = list(result[0].keys())
                rows = [list(row.values()) for row in result]
                
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows)
                }
                
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return {"error": f"Error executing query: {str(e)}"}
    
    
    async def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed information about a table"""
        try:
            async with self.get_connection() as conn:
                # Get column information
                columns = await conn.fetch(
                    "SELECT column_name, data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_schema = $1 AND table_name = $2 "
                    "ORDER BY ordinal_position",
                    schema, table
                )
                
                # Get index information
                indexes = await conn.fetch(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE schemaname = $1 AND tablename = $2 "
                    "ORDER BY indexname",
                    schema, table
                )
                
                # Get table statistics
                # The size functions take a regclass, which can be bound as a
                # parameter. COUNT(*) needs the relation in the FROM clause,
                # where a parameter is not allowed, so the name is quoted
                # instead. Quoting is what makes that safe: an embedded quote
                # is doubled, so the name can only ever read as one identifier.
                qualified_name = SQLValidator.quote_qualified_name(schema, table)
                stats = await conn.fetchrow(
                    "SELECT "
                    "pg_size_pretty(pg_total_relation_size($1::regclass)) as total_size, "
                    "pg_size_pretty(pg_relation_size($1::regclass)) as table_size, "
                    "pg_size_pretty(pg_total_relation_size($1::regclass) "
                    "- pg_relation_size($1::regclass)) as indexes_size, "
                    f"(SELECT COUNT(*) FROM {qualified_name}) as row_count",
                    qualified_name
                )
                
                return {
                    "columns": [dict(col) for col in columns],
                    "indexes": [dict(idx) for idx in indexes],
                    "statistics": dict(stats) if stats else {}
                }
                
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Close the connection pool"""
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
    
    async def list_schemas(self) -> list[str]:
        """List all database schemas"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema' "
                "ORDER BY schema_name"
            )
            return [r["schema_name"] for r in records]
    
    async def get_database_info(self) -> dict[str, str]:
        """Get general database information"""
        async with self.get_connection() as conn:
            version = await conn.fetchval("SELECT version()")
            size = await conn.fetchval("SELECT pg_size_pretty(pg_database_size(current_database()))")
            stats = await conn.fetchrow(
                "SELECT COUNT(*) as total_tables FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' AND table_schema NOT LIKE 'pg_%'"
            )
            
            return {
                "Version": version,
                "Size": size,
                "Total Tables": str(stats['total_tables'])
            }
    
    async def get_database_summary(self) -> dict[str, dict]:
        """Get comprehensive database summary"""
        summary = {}
        
        async with self.get_connection() as conn:
            # Get schemas
            schemas = await conn.fetch(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema' "
                "ORDER BY schema_name"
            )
            
            for schema_row in schemas:
                schema = schema_row["schema_name"]
                summary[schema] = {}
                
                # Get tables
                tables = await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = $1 AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name",
                    schema
                )
                summary[schema]["tables"] = [t["table_name"] for t in tables]
                
                # Get views
                views = await conn.fetch(
                    "SELECT table_name FROM information_schema.views "
                    "WHERE table_schema = $1 "
                    "ORDER BY table_name",
                    schema
                )
                summary[schema]["views"] = [v["table_name"] for v in views]
        
        return summary

    async def list_tables(self, schema: str) -> list[str]:
        """List tables in a specific schema"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = $1 AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                schema
            )
            return [r["table_name"] for r in records]

    async def list_views(self, schema: str) -> list[str]:
        """List views in a specific schema"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = $1 "
                "ORDER BY table_name",
                schema
            )
            return [r["table_name"] for r in records]

    async def list_indexes(self, schema: str, table: str) -> list[dict]:
        """List indexes for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = $1 AND tablename = $2 "
                "ORDER BY indexname",
                schema, table
            )
            return [{"indexname": r["indexname"], "indexdef": r["indexdef"]} for r in records]

    async def list_columns(self, schema: str, table: str) -> list[dict]:
        """List columns for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY ordinal_position",
                schema, table
            )
            return [
                {
                    "column_name": r["column_name"],
                    "data_type": r["data_type"],
                    "is_nullable": r["is_nullable"],
                    "column_default": r["column_default"]
                }
                for r in records
            ]

    async def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Get statistics for a table"""
        try:
            # Validate schema and table names to prevent SQL injection
            if not schema.replace('_', '').replace('-', '').isalnum():
                return {"error": "Invalid schema name"}
            if not table.replace('_', '').replace('-', '').isalnum():
                return {"error": "Invalid table name"}

            async with self.get_connection() as conn:
                # Use format() with proper identifier quoting
                qualified_name = f"{schema}.{table}"
                sql = (
                    f"SELECT "
                    f"pg_size_pretty(pg_total_relation_size('{qualified_name}')) as total_size, "
                    f"pg_size_pretty(pg_relation_size('{qualified_name}')) as table_size, "
                    f"pg_size_pretty(pg_total_relation_size('{qualified_name}') - pg_relation_size('{qualified_name}')) as indexes_size, "
                    f"(SELECT COUNT(*) FROM {qualified_name}) as row_count"
                )
                result = await conn.fetchrow(sql)

                if not result:
                    return {"error": f"Table {schema}.{table} not found"}

                return {
                    "total_size": result["total_size"],
                    "table_size": result["table_size"],
                    "indexes_size": result["indexes_size"],
                    "row_count": result["row_count"]
                }
        except Exception as e:
            return {"error": f"Error getting table stats: {str(e)}"}

    async def list_large_tables(self, limit: int = 10) -> list[dict]:
        """List the largest tables in the database"""
        async with self.get_connection() as conn:
            result = await conn.fetch(
                "SELECT "
                "schemaname, tablename, "
                "pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size, "
                "pg_total_relation_size(schemaname||'.'||tablename) as size_bytes "
                "FROM pg_tables "
                "WHERE schemaname NOT LIKE 'pg_%' "
                "ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC "
                "LIMIT $1",
                limit
            )

            return [
                {
                    "schema": row["schemaname"],
                    "table": row["tablename"],
                    "size": row["size"],
                    "size_bytes": row["size_bytes"]
                }
                for row in result
            ]

    async def list_users(self) -> list[str]:
        """List all database users"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT usename FROM pg_user WHERE usename != 'cloudberry' "
                "ORDER BY usename"
            )
            return [r["usename"] for r in records]

    async def list_user_permissions(self, username: str) -> list[dict]:
        """List permissions for a specific user"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "n.nspname as schema_name, "
                "c.relname as object_name, "
                "c.relkind as object_type, "
                "p.perm as permission "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL aclexplode(c.relacl) p "
                "WHERE p.grantee = (SELECT oid FROM pg_user WHERE usename = $1) "
                "AND n.nspname NOT LIKE 'pg_%' "
                "ORDER BY n.nspname, c.relname",
                username
            )
            return [
                {
                    "schema": r["schema_name"],
                    "object": r["object_name"],
                    "type": r["object_type"],
                    "permission": r["permission"]
                }
                for r in records
            ]

    async def list_table_privileges(self, schema: str, table: str) -> list[dict]:
        """List privileges for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "grantee, privilege_type "
                "FROM information_schema.table_privileges "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY grantee, privilege_type",
                schema, table
            )
            return [
                {
                    "user": r["grantee"],
                    "privilege": r["privilege_type"]
                }
                for r in records
            ]

    async def list_constraints(self, schema: str, table: str) -> list[dict]:
        """List constraints for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "c.conname as constraint_name, "
                "c.contype as constraint_type, "
                "pg_get_constraintdef(c.oid) as constraint_definition, "
                "f.relname as foreign_table_name, "
                "nf.nspname as foreign_schema_name "
                "FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "LEFT JOIN pg_class f ON f.oid = c.confrelid "
                "LEFT JOIN pg_namespace nf ON nf.oid = f.relnamespace "
                "WHERE n.nspname = $1 AND t.relname = $2 "
                "ORDER BY c.conname",
                schema, table
            )
            constraints = []
            for r in records:
                constraint_type = {
                    'p': 'PRIMARY KEY',
                    'f': 'FOREIGN KEY',
                    'u': 'UNIQUE',
                    'c': 'CHECK',
                    'x': 'EXCLUSION'
                }.get(r["constraint_type"], r["constraint_type"])
                
                constraints.append({
                    "name": r["constraint_name"],
                    "type": constraint_type,
                    "definition": r["constraint_definition"],
                    "foreign_table": r["foreign_table_name"],
                    "foreign_schema": r["foreign_schema_name"]
                })
            return constraints

    async def list_foreign_keys(self, schema: str, table: str) -> list[dict]:
        """List foreign keys for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "tc.constraint_name, "
                "tc.table_name, "
                "kcu.column_name, "
                "ccu.table_name AS foreign_table_name, "
                "ccu.column_name AS foreign_column_name, "
                "ccu.table_schema AS foreign_schema_name "
                "FROM information_schema.table_constraints AS tc "
                "JOIN information_schema.key_column_usage AS kcu "
                "ON tc.constraint_name = kcu.constraint_name "
                "JOIN information_schema.constraint_column_usage AS ccu "
                "ON ccu.constraint_name = tc.constraint_name "
                "WHERE tc.constraint_type = 'FOREIGN KEY' "
                "AND tc.table_schema = $1 AND tc.table_name = $2 "
                "ORDER BY tc.constraint_name",
                schema, table
            )
            return [
                {
                    "constraint_name": r["constraint_name"],
                    "column": r["column_name"],
                    "foreign_schema": r["foreign_schema_name"],
                    "foreign_table": r["foreign_table_name"],
                    "foreign_column": r["foreign_column_name"]
                }
                for r in records
            ]

    async def list_referenced_tables(self, schema: str, table: str) -> list[dict]:
        """List tables that reference this table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "tc.table_schema, "
                "tc.table_name, "
                "tc.constraint_name "
                "FROM information_schema.table_constraints AS tc "
                "JOIN information_schema.constraint_column_usage AS ccu "
                "ON ccu.constraint_name = tc.constraint_name "
                "WHERE tc.constraint_type = 'FOREIGN KEY' "
                "AND ccu.table_schema = $1 AND ccu.table_name = $2 "
                "ORDER BY tc.table_schema, tc.table_name",
                schema, table
            )
            return [
                {
                    "schema": r["table_schema"],
                    "table": r["table_name"],
                    "constraint": r["constraint_name"]
                }
                for r in records
            ]

    async def explain_query(self, query: str, params: Optional[Sequence[Any]] = None) -> str:
        """Get the execution plan for a query

        EXPLAIN ANALYZE runs the statement it is given, so the query is held to
        the same read-only rule as execute_query rather than passed straight
        through.
        """
        is_valid, error_msg = SQLValidator.validate_query(query)
        if not is_valid:
            return f"Query validation failed: {error_msg}"
        if not SQLValidator.is_readonly_query(query):
            return (
                "Only read-only queries can be explained, because EXPLAIN ANALYZE "
                "executes the statement"
            )

        try:
            async with self.get_connection() as conn:
                if params:
                    if not isinstance(params, (list, tuple)):
                        return (
                            f"params must be a list of values bound to $1, $2, ... in "
                            f"order, got {type(params).__name__}"
                        )
                    result = await conn.fetch(f"EXPLAIN (ANALYZE, BUFFERS) {query}", *params)
                else:
                    result = await conn.fetch(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
                
                return "\n".join([row["QUERY PLAN"] for row in result])
        except Exception as e:
            return f"Error explaining query: {str(e)}"

    async def get_slow_queries(self, limit: int = 10) -> list[dict]:
        """Get slow queries from pg_stat_statements"""
        async with self.get_connection() as conn:
            try:
                records = await conn.fetch(
                    "SELECT "
                    "query, "
                    "calls, "
                    "total_time, "
                    "mean_time, "
                    "rows "
                    "FROM pg_stat_statements "
                    "ORDER BY mean_time DESC "
                    "LIMIT $1",
                    limit
                )
                return [
                    {
                        "query": r["query"],
                        "calls": r["calls"],
                        "total_time": r["total_time"],
                        "mean_time": r["mean_time"],
                        "rows": r["rows"]
                    }
                    for r in records
                ]
            except Exception:
                # pg_stat_statements might not be available
                return []

    async def get_index_usage(self) -> list[dict]:
        """Get index usage statistics"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "schemaname, "
                "relname as tablename, "
                "indexrelname as indexname, "
                "idx_scan, "
                "idx_tup_read, "
                "idx_tup_fetch "
                "FROM pg_stat_user_indexes "
                "WHERE schemaname NOT LIKE 'pg_%' "
                "ORDER BY idx_scan DESC"
            )
            return [
                {
                    "schema": r["schemaname"],
                    "table": r["tablename"],
                    "index": r["indexname"],
                    "scans": r["idx_scan"],
                    "tup_read": r["idx_tup_read"],
                    "tup_fetch": r["idx_tup_fetch"]
                }
                for r in records
            ]

    async def get_table_bloat_info(self) -> list[dict]:
        """Get table bloat information"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "schemaname, "
                "relname as tablename, "
                "pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size, "
                "round(100 * (relpages - (relpages * fillfactor / 100)) / relpages, 2) as bloat_ratio "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_stat_user_tables s ON s.relid = c.oid "
                "WHERE c.relkind = 'r' AND n.nspname NOT LIKE 'pg_%' "
                "ORDER BY bloat_ratio DESC "
                "LIMIT 20"
            )
            return [
                {
                    "schema": r["schemaname"],
                    "table": r["tablename"],
                    "total_size": r["total_size"],
                    "bloat_ratio": r["bloat_ratio"]
                }
                for r in records
            ]

    async def get_database_activity(self) -> list[dict]:
        """Get current database activity"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "pid, "
                "usename, "
                "application_name, "
                "client_addr, "
                "state, "
                "query_start, "
                "query "
                "FROM pg_stat_activity "
                "WHERE state != 'idle' AND usename != 'cloudberry' "
                "ORDER BY query_start"
            )
            return [
                {
                    "pid": r["pid"],
                    "username": r["usename"],
                    "application": r["application_name"],
                    "client_addr": str(r["client_addr"]) if r["client_addr"] else None,
                    "state": r["state"],
                    "query_start": str(r["query_start"]),
                    "query": r["query"]
                }
                for r in records
            ]

    async def list_functions(self, schema: str) -> list[dict]:
        """List functions in a specific schema"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "proname as function_name, "
                "pg_get_function_identity_arguments(p.oid) as arguments, "
                "pg_get_function_result(p.oid) as return_type, "
                "prokind as function_type "
                "FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = $1 AND p.prokind IN ('f', 'p') "
                "ORDER BY proname",
                schema
            )
            return [
                {
                    "name": r["function_name"],
                    "arguments": r["arguments"],
                    "return_type": r["return_type"],
                    "type": "function" if r["function_type"] == "f" else "procedure"
                }
                for r in records
            ]

    async def get_function_definition(self, schema: str, function_name: str) -> str:
        """Get function definition"""
        try:
            async with self.get_connection() as conn:
                definition = await conn.fetchval(
                    "SELECT pg_get_functiondef(p.oid) "
                    "FROM pg_proc p "
                    "JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = $1 AND p.proname = $2 "
                    "LIMIT 1",
                    schema, function_name
                )
                return definition or "Function definition not found"
        except Exception as e:
            return f"Error getting function definition: {str(e)}"

    async def list_triggers(self, schema: str, table: str) -> list[dict]:
        """List triggers for a specific table"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "trigger_name, "
                "event_manipulation, "
                "action_timing, "
                "action_statement "
                "FROM information_schema.triggers "
                "WHERE event_object_schema = $1 AND event_object_table = $2 "
                "ORDER BY trigger_name",
                schema, table
            )
            return [
                {
                    "name": r["trigger_name"],
                    "event": r["event_manipulation"],
                    "timing": r["action_timing"],
                    "action": r["action_statement"]
                }
                for r in records
            ]

    async def get_table_ddl(self, schema: str, table: str) -> str:
        """Get DDL statement for a table"""
        try:
            async with self.get_connection() as conn:
                # Try the newer method first
                try:
                    ddl = await conn.fetchval(
                        "SELECT pg_get_tabledef($1, $2, true)",
                        schema, table
                    )
                    if ddl:
                        return ddl
                except Exception:
                    pass
                
                # Fallback to a more compatible approach
                ddl_query = """
                SELECT 'CREATE TABLE ' || $1 || '.' || $2 || ' (' || E'\n' ||
                       string_agg(
                         '  ' || column_name || ' ' || 
                         data_type || 
                         CASE WHEN character_maximum_length IS NOT NULL 
                              THEN '(' || character_maximum_length || ')' 
                              ELSE '' END ||
                         CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
                         CASE WHEN column_default IS NOT NULL 
                              THEN ' DEFAULT ' || column_default 
                              ELSE '' END,
                         E',\n' ORDER BY ordinal_position
                       ) || E'\n);' as ddl
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                """
                ddl = await conn.fetchval(ddl_query, schema, table)
                return ddl or "Table DDL not found"
        except Exception as e:
            return f"Error getting table DDL: {str(e)}"

    async def list_materialized_views(self, schema: str) -> list[str]:
        """List materialized views in a specific schema"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT matviewname "
                "FROM pg_matviews "
                "WHERE schemaname = $1 "
                "ORDER BY matviewname",
                schema
            )
            return [r["matviewname"] for r in records]

    async def get_vacuum_info(self, schema: str, table: str) -> dict:
        """Get vacuum information for a table"""
        async with self.get_connection() as conn:
            record = await conn.fetchrow(
                "SELECT "
                "last_vacuum, "
                "last_autovacuum, "
                "n_dead_tup, "
                "n_live_tup, "
                "vacuum_count, "
                "autovacuum_count "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = $1 AND relname = $2",
                schema, table
            )
            if record:
                return {
                    "last_vacuum": str(record["last_vacuum"]) if record["last_vacuum"] else None,
                    "last_autovacuum": str(record["last_autovacuum"]) if record["last_autovacuum"] else None,
                    "dead_tuples": record["n_dead_tup"],
                    "live_tuples": record["n_live_tup"],
                    "vacuum_count": record["vacuum_count"],
                    "autovacuum_count": record["autovacuum_count"]
                }
            return {"error": "Table not found"}

    async def list_active_connections(self) -> list[dict]:
        """List active database connections"""
        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT "
                "pid, "
                "usename, "
                "application_name, "
                "client_addr, "
                "state, "
                "backend_start, "
                "query_start "
                "FROM pg_stat_activity "
                "WHERE usename != 'cloudberry' "
                "ORDER BY backend_start"
            )
            return [
                {
                    "pid": r["pid"],
                    "username": r["usename"],
                    "application": r["application_name"],
                    "client_addr": str(r["client_addr"]) if r["client_addr"] else None,
                    "state": r["state"],
                    "backend_start": str(r["backend_start"]),
                    "query_start": str(r["query_start"]) if r["query_start"] else None
                }
                for r in records
            ]

    # ------------------------------------------------------------------
    # Retrieval: vector, full-text, and hybrid search
    # ------------------------------------------------------------------

    async def _describe_relation(self, conn, schema: str, table: str) -> dict:
        """Read a relation's columns from the catalog.

        Every retrieval method starts here. Resolving caller supplied names
        against the catalog is what lets the builders put identifiers into
        statement text at all: a name that no column matches never gets there.
        """
        records = await conn.fetch(
            "SELECT c.oid AS relid, a.attname AS column_name, t.typname AS type_name, "
            "format_type(a.atttypid, a.atttypmod) AS type_display, "
            "a.atttypmod AS type_modifier, a.attnum AS attribute_number "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "JOIN pg_type t ON t.oid = a.atttypid "
            "WHERE n.nspname = $1 AND c.relname = $2 "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            schema, table
        )
        if not records:
            raise ValueError(
                f'Relation "{schema}"."{table}" was not found, or it has no readable columns'
            )

        return {
            "relid": records[0]["relid"],
            "schema": schema,
            "table": table,
            "qualified_name": SQLValidator.quote_qualified_name(schema, table),
            "columns": [
                {
                    "column_name": record["column_name"],
                    "type_name": record["type_name"],
                    "type_display": record["type_display"],
                    "type_modifier": record["type_modifier"],
                    "attribute_number": record["attribute_number"],
                }
                for record in records
            ],
        }

    async def _row_key_columns(self, conn, relid: int) -> List[str]:
        """Return the system columns that identify a row while fusing results.

        Hybrid search ranks the same table twice and has to recognise a row
        that both arms returned. On Cloudberry a ctid is unique only within one
        segment, so the segment id belongs in the key; plain PostgreSQL has no
        gp_segment_id and ctid alone suffices.
        """
        has_segment_id = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute "
            "WHERE attrelid = $1 AND attname = 'gp_segment_id' AND attnum < 0)",
            relid,
        )
        return ["gp_segment_id", "ctid"] if has_segment_id else ["ctid"]

    async def _resolve_language(self, conn, language: str) -> tuple:
        """Resolve a text search configuration to its name and OID.

        The OID matters for more than validation. A full-text index is built
        over to_tsvector('english', col), which the catalog stores with the
        configuration already resolved to an OID constant. Writing that same
        OID into the query reproduces the constant exactly, so the expression
        index still matches; binding the name as a parameter does not, and the
        index is quietly skipped once the plan is cached and generic.
        """
        if not isinstance(language, str) or not language.strip():
            raise ValueError(
                f"language must be a text search configuration name, got {language!r}"
            )

        oid = await conn.fetchval(
            "SELECT oid FROM pg_ts_config WHERE cfgname = $1", language
        )
        if oid is None:
            records = await conn.fetch("SELECT cfgname FROM pg_ts_config ORDER BY cfgname")
            available = ", ".join(record["cfgname"] for record in records)
            raise ValueError(
                f"language '{language}' is not a known text search configuration; "
                f"available configurations are: {available}"
            )
        # An OID is an integer read out of the catalog, so interpolating it
        # cannot inject anything.
        return language, f"{int(oid)}::regconfig"

    # Cloudberry dispatches a SET to the segments only when a query executor
    # gang already exists. Issued on an otherwise idle connection the value
    # reaches the coordinator and is echoed back by current_setting(), yet the
    # segment scans that actually read it keep the built-in default. A trivial
    # distributed statement creates the gang first.
    GANG_WARMUP_SQL = "SELECT count(*) FROM gp_dist_random('gp_id')"

    async def _prepare_vector_gucs(
        self,
        conn,
        type_name: Optional[str],
        probes: Optional[int],
        ef_search: Optional[int],
    ) -> Dict[str, int]:
        """Set the pgvector recall knobs for the next statement on this connection.

        Recall is not a property of the query alone: at the default
        ivfflat.probes of 1 an index scan reads a single list and can miss the
        true nearest neighbour outright. Exposing these lets a caller trade
        latency for recall, and measure the difference.

        The settings are session level rather than transaction local because
        SET LOCAL and set_config(..., true) are not dispatched to the segments,
        so a transaction-scoped setting would silently do nothing. The caller
        resets them once the statement is done.
        """
        requested: Dict[str, int] = {}
        if probes is not None:
            requested["ivfflat.probes"] = probes
        if ef_search is not None:
            requested["hnsw.ef_search"] = ef_search
        if not requested:
            return requested

        if type_name:
            # Load pgvector on the coordinator, which matters when the scan
            # does not go out to the segments at all.
            literal = warmup_literal(type_name)
            quoted_type = SQLValidator.quote_identifier(type_name)
            await conn.fetchval(f"SELECT '{literal}'::{quoted_type} IS NOT NULL")

        try:
            await conn.fetchval(self.GANG_WARMUP_SQL)
        except Exception as e:
            # Plain PostgreSQL has no segments and no gp_dist_random().
            logger.debug(f"Skipping segment warm-up: {e}")

        await self._check_guc_bounds(conn, requested)

        applied: Dict[str, int] = {}
        try:
            for name, value in requested.items():
                # Both the name and the value are server-controlled: the names
                # are module constants and the values were checked to be
                # integers.
                await conn.execute(f"SET {name} = {int(value)}")
                applied[name] = value
        except Exception:
            # Undo whatever already took effect rather than leaving it to the
            # driver's reset when the connection goes back to the pool.
            await self._reset_vector_gucs(conn, applied)
            raise
        return applied

    @staticmethod
    async def _check_guc_bounds(conn, requested: Dict[str, int]) -> None:
        """Reject a recall setting the server would not accept.

        The real limits belong to the installed pgvector, not to this wrapper:
        hnsw.ef_search stops at 1000 while ivfflat.probes runs to 32768. Asking
        the server for its own declared range gives an accurate error at the
        API boundary instead of a raw failure part way through applying the
        settings.
        """
        records = await conn.fetch(
            "SELECT name, min_val, max_val FROM pg_settings WHERE name = ANY($1::text[])",
            sorted(requested),
        )
        bounds = {
            record["name"]: (int(record["min_val"]), int(record["max_val"]))
            for record in records
        }

        for name, value in requested.items():
            if name not in bounds:
                raise ValueError(
                    f"{name} is not available on this server; it is provided by the "
                    f"pgvector extension, which may not be installed"
                )
            low, high = bounds[name]
            if not low <= value <= high:
                raise ValueError(
                    f"{name} must be between {low} and {high} on this server, got {value}"
                )

    async def _reset_vector_gucs(self, conn, applied: Dict[str, int]) -> None:
        """Undo _prepare_vector_gucs so a pooled connection is handed back clean."""
        for name in applied:
            try:
                await conn.execute(f"RESET {name}")
            except Exception as e:
                logger.warning(f"Could not reset {name}: {e}")

    async def _run_search(
        self,
        conn,
        sql: str,
        params: SqlParams,
        vector_type: Optional[str] = None,
        probes: Optional[int] = None,
        ef_search: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a built statement and shape the result for an agent."""
        applied = await self._prepare_vector_gucs(conn, vector_type, probes, ef_search)
        try:
            records = await conn.fetch(sql, *params.values)
        finally:
            if applied:
                await self._reset_vector_gucs(conn, applied)

        search_meta = dict(meta or {})
        if applied:
            search_meta["settings"] = applied

        return {
            "columns": list(records[0].keys()) if records else [],
            "rows": [list(record.values()) for record in records],
            "row_count": len(records),
            "sql": sql,
            "search": search_meta,
        }

    @staticmethod
    def _expression_names_column(expression: Optional[str], column_name: str) -> bool:
        """Report whether an index expression references a given column."""
        if not expression:
            return False
        pattern = rf'(?<![A-Za-z0-9_$]){re.escape(column_name)}(?![A-Za-z0-9_$])'
        if re.search(pattern, expression):
            return True
        return f'"{column_name}"' in expression

    async def list_searchable_columns(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        include_unindexed_text: bool = False,
    ) -> List[dict]:
        """Report the columns an agent can actually run a search against.

        An agent that cannot see which columns hold embeddings, and which text
        columns carry a full-text index, has no way to choose a target other
        than guessing from names.
        """
        searchable_types = sorted(VECTOR_TYPES | TEXT_TYPES | {TSVECTOR_TYPE})

        conditions = [
            "c.relkind IN ('r', 'p', 'f', 'm', 'v')",
            "a.attnum > 0",
            "NOT a.attisdropped",
            "n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
            "t.typname = ANY($1::text[])",
        ]
        values: List[Any] = [searchable_types]
        if schema:
            values.append(schema)
            conditions.append(f"n.nspname = ${len(values)}")
        if table:
            values.append(table)
            conditions.append(f"c.relname = ${len(values)}")

        async with self.get_connection() as conn:
            records = await conn.fetch(
                "SELECT c.oid AS relid, n.nspname AS schema_name, c.relname AS table_name, "
                "a.attname AS column_name, a.attnum AS attribute_number, "
                "t.typname AS type_name, "
                "format_type(a.atttypid, a.atttypmod) AS type_display, "
                "a.atttypmod AS type_modifier "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = c.oid "
                "JOIN pg_type t ON t.oid = a.atttypid "
                f"WHERE {' AND '.join(conditions)} "
                "ORDER BY n.nspname, c.relname, a.attnum",
                *values,
            )
            if not records:
                return []

            relids = sorted({record["relid"] for record in records})
            index_records = await conn.fetch(
                "SELECT i.indrelid AS relid, ic.relname AS index_name, "
                "am.amname AS method, pg_get_indexdef(i.indexrelid) AS definition, "
                "pg_get_expr(i.indexprs, i.indrelid) AS expression, "
                "string_to_array(i.indkey::text, ' ')::int[] AS index_keys "
                "FROM pg_index i "
                "JOIN pg_class ic ON ic.oid = i.indexrelid "
                "JOIN pg_am am ON am.oid = ic.relam "
                "WHERE i.indrelid = ANY($1::oid[]) "
                "ORDER BY ic.relname",
                relids,
            )

        indexes_by_relid: Dict[int, List[dict]] = {}
        for record in index_records:
            indexes_by_relid.setdefault(record["relid"], []).append(dict(record))

        results: List[dict] = []
        for record in records:
            column_name = record["column_name"]
            attribute_number = record["attribute_number"]
            type_name = record["type_name"]

            matching: List[dict] = []
            for index in indexes_by_relid.get(record["relid"], []):
                keys = index["index_keys"] or []
                on_column = attribute_number in keys
                # An index key of 0 marks an expression. pg_get_indexdef only
                # quotes an identifier that needs it, so a to_tsvector(content)
                # index names the column bare; match it either way.
                on_expression = 0 in keys and self._expression_names_column(
                    index.get("expression"), column_name
                )
                if not on_column and not on_expression:
                    continue
                matching.append(
                    {
                        "name": index["index_name"],
                        "method": index["method"],
                        "definition": index["definition"],
                        "on_expression": on_expression and not on_column,
                    }
                )

            if type_name in VECTOR_TYPES:
                kind = "vector"
            elif type_name == TSVECTOR_TYPE:
                kind = "tsvector"
            else:
                kind = "text"

            full_text_indexed = any(
                index["method"] in ("gin", "gist")
                and ("to_tsvector" in index["definition"] or kind == "tsvector")
                for index in matching
            )

            # Plain text columns without a full-text index are the bulk of any
            # schema and drown out the columns worth searching.
            if kind == "text" and not full_text_indexed and not include_unindexed_text:
                continue

            dimension = None
            if kind == "vector" and record["type_modifier"] and record["type_modifier"] > 0:
                dimension = record["type_modifier"]

            results.append(
                {
                    "schema": record["schema_name"],
                    "table": record["table_name"],
                    "column": column_name,
                    "kind": kind,
                    "type": record["type_display"],
                    "dimension": dimension,
                    "full_text_indexed": full_text_indexed,
                    "indexes": matching,
                }
            )

        return results

    async def vector_search(
        self,
        schema: str,
        table: str,
        vector_column: str,
        query_vector: Sequence[float],
        limit: int = 10,
        select_columns: Optional[Sequence[str]] = None,
        filters: Optional[Sequence[Any]] = None,
        metric: str = SearchMetric.L2.value,
        probes: Optional[int] = None,
        ef_search: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Rank rows by embedding distance, optionally pre-filtered.

        Filters are applied in the same scan that walks the vector index, so
        this is a pre-filtered nearest neighbour search rather than a filter
        over an already truncated result.
        """
        limit = validate_limit(limit)
        if probes is not None:
            validate_positive_int(probes, "probes")
        if ef_search is not None:
            validate_positive_int(ef_search, "ef_search")
        conditions = coerce_filters(filters)
        operator = resolve_metric(metric)
        # Checked before the column lookup so a malformed vector is reported
        # as such, rather than as a dimension mismatch.
        validate_query_vector(query_vector)

        async with self.get_connection() as conn:
            relation = await self._describe_relation(conn, schema, table)
            columns = relation["columns"]
            column = resolve_column(vector_column, columns, "vector_column")
            validate_vector_column(column, query_vector)
            vector_literal = format_vector_literal(query_vector, column["type_name"])

            params = SqlParams()
            quoted_type = SQLValidator.quote_identifier(column["type_name"])
            # The literal is bound as text and cast, because asyncpg has no
            # wire format for an embedding type.
            distance = (
                f"{quote_column(column)} {operator} "
                f"{params.add(vector_literal)}::text::{quoted_type}"
            )

            projection, output_names = build_select_list(select_columns, columns)
            distance_column = unique_alias("distance", output_names)
            projection.append(
                f"({distance})::float8 AS "
                f"{SQLValidator.quote_identifier(distance_column)}"
            )

            where = build_filter_sql(conditions, columns, params)
            sql = (
                f"SELECT {', '.join(projection)}\n"
                f"FROM {relation['qualified_name']}\n"
                + (f"WHERE {where}\n" if where else "")
                + f"ORDER BY {distance}\n"
                f"LIMIT {params.add(limit)}"
            )

            return await self._run_search(
                conn,
                sql,
                params,
                vector_type=column["type_name"],
                probes=probes,
                ef_search=ef_search,
                meta={
                    "mode": "vector",
                    "schema": schema,
                    "table": table,
                    "vector_column": column["column_name"],
                    "metric": metric.value if isinstance(metric, SearchMetric) else metric,
                    "distance_column": distance_column,
                    "dimensions": len(query_vector),
                    "limit": limit,
                    "filters": len(conditions),
                },
            )

    async def fulltext_search(
        self,
        schema: str,
        table: str,
        text_column: str,
        query: str,
        limit: int = 10,
        select_columns: Optional[Sequence[str]] = None,
        filters: Optional[Sequence[Any]] = None,
        language: str = "english",
        query_mode: str = TextQueryMode.WEBSEARCH.value,
        match_mode: str = TextMatchMode.ALL_THEN_ANY.value,
    ) -> Dict[str, Any]:
        """Rank rows by full-text relevance over one text or tsvector column."""
        limit = validate_limit(limit)
        conditions = coerce_filters(filters)
        query_function = resolve_query_mode(query_mode)
        match_mode = resolve_match_mode(match_mode)
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"query must be a non-empty search string, got {query!r}")

        async with self.get_connection() as conn:
            relation = await self._describe_relation(conn, schema, table)
            columns = relation["columns"]
            column = resolve_column(text_column, columns, "text_column")
            if column["type_name"] != TSVECTOR_TYPE and column["type_name"] not in TEXT_TYPES:
                supported = ", ".join(sorted(TEXT_TYPES | {TSVECTOR_TYPE}))
                raise ValueError(
                    f"text_column '{column['column_name']}' has type "
                    f"{column['type_display']}, which cannot be searched as text; "
                    f"expected one of: {supported}"
                )
            language, language_sql = await self._resolve_language(conn, language)
            matched_with, widening_refused = await self._choose_match_mode(
                conn, relation, column, language_sql, query, query_function,
                match_mode, conditions,
            )

            params = SqlParams()
            query_placeholder = params.add(query)

            document, tsquery, rank = self._build_text_expressions(
                column, language_sql, query_placeholder, query_function,
                matched_with,
            )

            projection, output_names = build_select_list(select_columns, columns)
            rank_column = unique_alias("rank", output_names)
            projection.append(
                f"({rank})::float8 AS {SQLValidator.quote_identifier(rank_column)}"
            )

            where_parts = [f"{document} @@ {tsquery}"]
            filter_sql = build_filter_sql(conditions, columns, params)
            if filter_sql:
                where_parts.append(filter_sql)

            sql = (
                f"SELECT {', '.join(projection)}\n"
                f"FROM {relation['qualified_name']}\n"
                f"WHERE {' AND '.join(where_parts)}\n"
                f"ORDER BY {rank} DESC\n"
                f"LIMIT {params.add(limit)}"
            )

            return await self._run_search(
                conn,
                sql,
                params,
                meta={
                    "mode": "fulltext",
                    "schema": schema,
                    "table": table,
                    "text_column": column["column_name"],
                    "language": language,
                    "query_mode": (
                        query_mode.value
                        if isinstance(query_mode, TextQueryMode)
                        else query_mode
                    ),
                    "match_mode": match_mode,
                    "matched_with": matched_with,
                    # True only when a strict reading was wanted and gave up,
                    # which is the signal a caller has to notice.
                    "widened": (
                        matched_with == TextMatchMode.ANY.value
                        and match_mode != TextMatchMode.ANY.value
                    ),
                    "widening_refused": widening_refused,
                    "rank_column": rank_column,
                    "limit": limit,
                    "filters": len(conditions),
                },
            )

    @staticmethod
    def _build_text_expressions(
        column: Dict[str, Any],
        language_sql: str,
        query_placeholder: str,
        query_function: str,
        match_mode: str = TextMatchMode.ALL.value,
        alias: Optional[str] = None,
    ) -> tuple:
        """Build the document, tsquery, and rank expressions for a column.

        A column already stored as tsvector is used as-is; anything else is
        parsed on the fly, spelled exactly as a to_tsvector() expression index
        would be so that such an index still matches.

        The column is deliberately not wrapped in coalesce(). It would read as
        harmless NULL handling, but to_tsvector is strict, so a NULL document
        already yields NULL and NULL @@ query already drops the row. The
        wrapper buys nothing and costs the index: matching compares expression
        trees, and coalesce(col, '') is not col, so every search would fall
        back to a sequential scan.
        """
        reference = quote_column(column, alias)
        if column["type_name"] == TSVECTOR_TYPE:
            document = reference
        else:
            document = f"to_tsvector({language_sql}, {reference})"
        tsquery = build_tsquery(
            query_function, language_sql, query_placeholder, match_mode
        )
        return document, tsquery, f"ts_rank({document}, {tsquery})"

    async def _choose_match_mode(
        self,
        conn,
        relation: Dict[str, Any],
        column: Dict[str, Any],
        language_sql: str,
        query: str,
        query_function: str,
        match_mode: str,
        conditions: Sequence[Any],
    ) -> tuple:
        """Decide which term combination the search will actually use.

        Requiring every term is the precise reading of a query, and it is the
        right one whenever it matches something. When it matches nothing the
        caller gets an empty result rather than an error, which reads as "this
        database holds nothing on the subject" and is usually wrong. So check
        first, and widen only when the strict reading found nothing.

        Widening is refused outright when the query carries a negation or a
        phrase. ORing the lexemes of `latency -dashboard` returns precisely the
        rows the caller excluded, and flattening a phrase is the opposite of
        asking for one; an empty result is the better answer there.

        Returns the mode to use and whether widening was refused.
        """
        if match_mode != TextMatchMode.ALL_THEN_ANY.value:
            return match_mode, False

        probe = SqlParams()
        document, tsquery, _ = self._build_text_expressions(
            column,
            language_sql,
            probe.add(query),
            query_function,
            TextMatchMode.ALL.value,
        )
        where = [f"{document} @@ {tsquery}"]
        filter_sql = build_filter_sql(conditions, relation["columns"], probe)
        if filter_sql:
            where.append(filter_sql)

        record = await conn.fetchrow(
            f"SELECT EXISTS (SELECT 1 FROM {relation['qualified_name']} "
            f"WHERE {' AND '.join(where)}) AS matched, "
            f"({tsquery})::text AS strict_query",
            *probe.values,
        )
        if record["matched"]:
            return TextMatchMode.ALL.value, False
        if tsquery_has_blocking_operator(record["strict_query"]):
            return TextMatchMode.ALL.value, True
        return TextMatchMode.ANY.value, False

    async def hybrid_search(
        self,
        schema: str,
        table: str,
        vector_column: str,
        text_column: str,
        query_vector: Sequence[float],
        query: str,
        limit: int = 10,
        select_columns: Optional[Sequence[str]] = None,
        filters: Optional[Sequence[Any]] = None,
        metric: str = SearchMetric.L2.value,
        language: str = "english",
        query_mode: str = TextQueryMode.WEBSEARCH.value,
        match_mode: str = TextMatchMode.ALL_THEN_ANY.value,
        rrf_k: int = 60,
        candidates: Optional[int] = None,
        probes: Optional[int] = None,
        ef_search: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Combine vector and full-text ranking with reciprocal rank fusion.

        Each arm contributes 1 / (rrf_k + rank). Fusing ranks rather than
        scores is what makes the two comparable: a distance and a ts_rank have
        no common scale, but their orderings do.
        """
        limit = validate_limit(limit)
        if candidates is None:
            candidates = min(max(limit * 5, 50), MAX_CANDIDATES)
        candidates = validate_limit(candidates, "candidates", maximum=MAX_CANDIDATES)
        if candidates < limit:
            raise ValueError(
                f"candidates must be at least limit; got candidates={candidates} "
                f"and limit={limit}"
            )
        rrf_k = validate_limit(rrf_k, "rrf_k", maximum=MAX_CANDIDATES)
        if probes is not None:
            validate_positive_int(probes, "probes")
        if ef_search is not None:
            validate_positive_int(ef_search, "ef_search")

        conditions = coerce_filters(filters)
        operator = resolve_metric(metric)
        query_function = resolve_query_mode(query_mode)
        match_mode = resolve_match_mode(match_mode)
        validate_query_vector(query_vector)
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"query must be a non-empty search string, got {query!r}")

        async with self.get_connection() as conn:
            relation = await self._describe_relation(conn, schema, table)
            columns = relation["columns"]
            qualified_name = relation["qualified_name"]

            embedding = resolve_column(vector_column, columns, "vector_column")
            validate_vector_column(embedding, query_vector)
            vector_literal = format_vector_literal(query_vector, embedding["type_name"])
            document_column = resolve_column(text_column, columns, "text_column")
            if (
                document_column["type_name"] != TSVECTOR_TYPE
                and document_column["type_name"] not in TEXT_TYPES
            ):
                supported = ", ".join(sorted(TEXT_TYPES | {TSVECTOR_TYPE}))
                raise ValueError(
                    f"text_column '{document_column['column_name']}' has type "
                    f"{document_column['type_display']}, which cannot be searched as "
                    f"text; expected one of: {supported}"
                )
            language, language_sql = await self._resolve_language(conn, language)
            # Decided before the fused statement is built: re-running it after
            # the fact would redo the vector arm's work for nothing.
            matched_with, widening_refused = await self._choose_match_mode(
                conn, relation, document_column, language_sql, query,
                query_function, match_mode, conditions,
            )

            params = SqlParams()
            quoted_type = SQLValidator.quote_identifier(embedding["type_name"])
            distance = (
                f"{quote_column(embedding)} {operator} "
                f"{params.add(vector_literal)}::text::{quoted_type}"
            )
            document, tsquery, rank = self._build_text_expressions(
                document_column,
                language_sql,
                params.add(query),
                query_function,
                matched_with,
            )

            # Built once and referenced by both arms: the placeholders are
            # already assigned, so reusing the text reuses the same values.
            filter_sql = build_filter_sql(conditions, columns, params)
            candidates_placeholder = params.add(candidates)
            rrf_placeholder = params.add(rrf_k)

            key_columns = await self._row_key_columns(conn, relation["relid"])
            key_aliases = [f"rowkey_{index}" for index in range(len(key_columns))]
            key_select = ", ".join(
                f"{SQLValidator.quote_identifier(name)} AS {alias}"
                for name, alias in zip(key_columns, key_aliases)
            )

            vector_arm = (
                f"  SELECT {key_select},\n"
                f"         row_number() OVER (ORDER BY {distance}) AS rnk\n"
                f"  FROM {qualified_name}\n"
                + (f"  WHERE {filter_sql}\n" if filter_sql else "")
                + f"  ORDER BY {distance}\n"
                f"  LIMIT {candidates_placeholder}"
            )

            text_where = [f"{document} @@ {tsquery}"]
            if filter_sql:
                text_where.append(filter_sql)
            text_arm = (
                f"  SELECT {key_select},\n"
                f"         row_number() OVER (ORDER BY {rank} DESC) AS rnk\n"
                f"  FROM {qualified_name}\n"
                f"  WHERE {' AND '.join(text_where)}\n"
                f"  ORDER BY {rank} DESC\n"
                f"  LIMIT {candidates_placeholder}"
            )

            fused_keys = ", ".join(
                f"COALESCE(v.{alias}, f.{alias}) AS {alias}" for alias in key_aliases
            )
            join_condition = " AND ".join(f"v.{alias} = f.{alias}" for alias in key_aliases)
            rejoin_condition = " AND ".join(
                f"d.{SQLValidator.quote_identifier(name)} = fused.{alias}"
                for name, alias in zip(key_columns, key_aliases)
            )
            fusion = (
                f"  SELECT {fused_keys},\n"
                f"         COALESCE(1.0 / ({rrf_placeholder} + v.rnk), 0)\n"
                f"         + COALESCE(1.0 / ({rrf_placeholder} + f.rnk), 0) AS score,\n"
                f"         v.rnk AS vector_rank, f.rnk AS text_rank\n"
                f"  FROM vector_hits v FULL JOIN text_hits f ON {join_condition}"
            )

            projection, output_names = build_select_list(
                select_columns, columns, alias="d"
            )
            score_column = unique_alias("score", output_names)
            vector_rank_column = unique_alias(
                "vector_rank", output_names + [score_column]
            )
            text_rank_column = unique_alias(
                "text_rank", output_names + [score_column, vector_rank_column]
            )
            projection.extend(
                [
                    f"fused.score::float8 AS "
                    f"{SQLValidator.quote_identifier(score_column)}",
                    f"fused.vector_rank AS "
                    f"{SQLValidator.quote_identifier(vector_rank_column)}",
                    f"fused.text_rank AS "
                    f"{SQLValidator.quote_identifier(text_rank_column)}",
                ]
            )

            sql = (
                f"WITH vector_hits AS (\n{vector_arm}\n),\n"
                f"text_hits AS (\n{text_arm}\n),\n"
                f"fused AS (\n{fusion}\n)\n"
                f"SELECT {', '.join(projection)}\n"
                f"FROM fused JOIN {qualified_name} d ON {rejoin_condition}\n"
                f"ORDER BY fused.score DESC\n"
                f"LIMIT {params.add(limit)}"
            )

            return await self._run_search(
                conn,
                sql,
                params,
                vector_type=embedding["type_name"],
                probes=probes,
                ef_search=ef_search,
                meta={
                    "mode": "hybrid",
                    "schema": schema,
                    "table": table,
                    "vector_column": embedding["column_name"],
                    "text_column": document_column["column_name"],
                    "metric": metric.value if isinstance(metric, SearchMetric) else metric,
                    "language": language,
                    "query_mode": (
                        query_mode.value
                        if isinstance(query_mode, TextQueryMode)
                        else query_mode
                    ),
                    "match_mode": match_mode,
                    "matched_with": matched_with,
                    # True only when a strict reading was wanted and gave up,
                    # which is the signal a caller has to notice.
                    "widened": (
                        matched_with == TextMatchMode.ANY.value
                        and match_mode != TextMatchMode.ANY.value
                    ),
                    "widening_refused": widening_refused,
                    "score_column": score_column,
                    "vector_rank_column": vector_rank_column,
                    "text_rank_column": text_rank_column,
                    "rrf_k": rrf_k,
                    "candidates": candidates,
                    "row_key": key_columns,
                    "limit": limit,
                    "filters": len(conditions),
                },
            )
