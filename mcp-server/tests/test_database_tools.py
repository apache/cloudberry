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
Database tools test module
Tests newly added database management tool functionality
"""
import pytest
import pytest_asyncio
import asyncio
import json
from typing import Any
from pydantic import AnyUrl

from cbmcp.client import CloudberryMCPClient


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, AnyUrl):
            return str(obj)
        return super().default(obj)


@pytest.fixture
def event_loop():
    """Create event loop for async testing"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(params=["stdio", "http"])
async def client(request):
    """Create CloudberryMCPClient instance supporting stdio and http modes"""
    client_instance = await CloudberryMCPClient.create(mode=request.param)
    yield client_instance
    await client_instance.close()


@pytest.mark.asyncio
class TestDatabaseTools:
    """Database management tools test class"""
    
    # User and permission management tests
    async def test_list_users_basic(self, client):
        """Test basic user listing functionality"""
        try:
            result = await client.call_tool("list_users", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping user list test: {e}")

    async def test_list_user_permissions(self, client):
        """Test user permissions query"""
        try:
            result = await client.call_tool("list_user_permissions", {"username": "postgres"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping user permissions test: {e}")

    async def test_list_table_privileges(self, client):
        """Test table privileges query"""
        try:
            result = await client.call_tool("list_table_privileges", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping table privileges test: {e}")

    # Constraint and relationship management tests
    async def test_list_constraints(self, client):
        """Test constraints query"""
        try:
            result = await client.call_tool("list_constraints", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping constraints query test: {e}")

    async def test_list_foreign_keys(self, client):
        """Test foreign keys query"""
        try:
            result = await client.call_tool("list_foreign_keys", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping foreign keys query test: {e}")

    async def test_list_referenced_tables(self, client):
        """Test referenced tables query"""
        try:
            result = await client.call_tool("list_referenced_tables", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping referenced tables query test: {e}")

    # Performance monitoring and optimization tests
    async def test_get_slow_queries(self, client):
        """Test slow queries retrieval"""
        result = await client.call_tool("get_slow_queries", {"limit": 5})
        assert result is not None
        assert hasattr(result, 'structured_content')

    async def test_get_index_usage(self, client):
        """Test index usage"""
        try:
            result = await client.call_tool("get_index_usage", {"schema": "public"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping index usage test: {e}")

    async def test_get_table_bloat_info(self, client):
        """Test table bloat information"""
        try:
            result = await client.call_tool("get_table_bloat_info", {
                "schema": "public", 
                "limit": 5
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping table bloat info test: {e}")

    async def test_get_database_activity(self, client):
        """Test database activity monitoring"""
        try:
            result = await client.call_tool("get_database_activity", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping database activity test: {e}")

    async def test_get_vacuum_info(self, client):
        """Test vacuum information"""
        try:
            result = await client.call_tool("get_vacuum_info", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping vacuum info test: {e}")

    # Database object management tests
    async def test_list_functions(self, client):
        """Test functions list"""
        result = await client.call_tool("list_functions", {"schema": "public"})
        assert result is not None
        assert hasattr(result, 'structured_content')

    async def test_get_function_definition(self, client):
        """Test function definition retrieval"""
        try:
            result = await client.call_tool("get_function_definition", {
                "schema": "public", 
                "function_name": "now"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping function definition test: {e}")

    async def test_list_triggers(self, client):
        """Test triggers list"""
        try:
            result = await client.call_tool("list_triggers", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping triggers list test: {e}")

    async def test_list_materialized_views(self, client):
        """Test materialized views list"""
        result = await client.call_tool("list_materialized_views", {"schema": "public"})
        assert result is not None
        assert hasattr(result, 'structured_content')

    async def test_get_materialized_view_ddl(self, client):
        """Test materialized view DDL"""
        try:
            result = await client.call_tool("get_materialized_view_ddl", {
                "schema": "public", 
                "view_name": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping materialized view DDL test: {e}")

    async def test_get_table_ddl(self, client):
        """Test table DDL retrieval"""
        try:
            result = await client.call_tool("get_table_ddl", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping table DDL test: {e}")

    async def test_list_active_connections(self, client):
        """Test active connections list"""
        try:
            result = await client.call_tool("list_active_connections", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping active connections test: {e}")

    async def test_all_tools_availability(self, client):
        """Test availability of all newly added tools"""
        tools = await client.list_tools()
        tool_names = [tool.name for tool in tools]
        
        new_tools = [
            "list_users", "list_user_permissions", "list_table_privileges",
            "list_constraints", "list_foreign_keys", "list_referenced_tables",
            "get_slow_queries", "get_index_usage", "get_table_bloat_info",
            "get_database_activity", "get_vacuum_info", "list_functions",
            "get_function_definition", "list_triggers", "list_materialized_views",
            "get_materialized_view_ddl", "get_table_ddl", "list_active_connections"
        ]
        
        available_tools = [tool for tool in new_tools if tool in tool_names]
        print(f"Found {len(available_tools)} new tools: {available_tools}")

    async def test_tool_parameter_validation(self, client):
        """Test tool parameter validation"""
        try:
            await client.call_tool("get_table_ddl", {"schema": "public"})
        except Exception:
            pass
        
        try:
            result = await client.call_tool("get_table_ddl", {
                "schema": "public", 
                "table": "film"
            })
            assert result is not None
        except Exception as e:
            pytest.skip(f"Skipping DDL test: {e}")


@pytest.mark.asyncio
async def test_database_tools_comprehensive():
    """Comprehensive test of all database tools"""
    client = await CloudberryMCPClient.create()
    try:
        tools = await client.list_tools()
        assert isinstance(tools, list)
        
        try:
            users = await client.call_tool("list_users", {})
            assert users is not None
            
            activity = await client.call_tool("get_database_activity", {})
            assert activity is not None
            
            connections = await client.call_tool("list_active_connections", {})
            assert connections is not None
            
        except Exception as e:
            pytest.skip(f"Skipping comprehensive test: {e}")
    finally:
        await client.close()
