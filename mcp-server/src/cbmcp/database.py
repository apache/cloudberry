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
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager
import asyncpg

from .config import DatabaseConfig
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
            )
        
        try:
            async with self._connection_pool.acquire() as conn:
                yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
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
                    # Sanitize parameter names
                    sanitized_params = {
                        SQLValidator.sanitize_parameter_name(k): v 
                        for k, v in params.items()
                    }
                    result = await conn.fetch(query, **sanitized_params)
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
                stats = await conn.fetchrow(
                    "SELECT "
                    "pg_size_pretty(pg_total_relation_size($1)) as total_size, "
                    "pg_size_pretty(pg_relation_size($1)) as table_size, "
                    "pg_size_pretty(pg_total_relation_size($1) - pg_relation_size($1)) as indexes_size, "
                    "(SELECT COUNT(*) FROM $1) as row_count",
                    f"{schema}.{table}"
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

    async def explain_query(self, query: str, params: Optional[dict] = None) -> str:
        """Get the execution plan for a query"""
        try:
            async with self.get_connection() as conn:
                if params:
                    result = await conn.fetch(f"EXPLAIN (ANALYZE, BUFFERS) {query}", **params)
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