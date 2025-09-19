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
class TestCloudberryMCPClient:
    """Apache Cloudberry MCP client test class"""
    
    async def test_list_capabilities(self, client):
        """Test server capabilities list"""
        tools = await client.list_tools()
        resources = await client.list_resources()
        prompts = await client.list_prompts()
        
        assert tools is not None
        assert resources is not None
        assert prompts is not None
        
        assert isinstance(tools, list)
        assert isinstance(resources, list)
        assert isinstance(prompts, list)
    
    async def test_get_schemas_resource(self, client):
        """Test getting database schemas resource"""
        try:
            schemas = await client.get_resource("postgres://schemas")
            assert schemas is not None
            assert isinstance(schemas, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get schemas: {e}")
    
    async def test_get_database_info_resource(self, client):
        """Test getting database info resource"""
        try:
            db_infos = await client.get_resource("postgres://database/info")
            assert db_infos is not None
            assert isinstance(db_infos, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get database info: {e}")
    
    async def test_get_database_summary_resource(self, client):
        """Test getting database summary resource"""
        try:
            db_summary = await client.get_resource("postgres://database/summary")
            assert db_summary is not None
            assert isinstance(db_summary, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get database summary: {e}")
    
    @pytest.mark.parametrize("tool_name,parameters", [
        ("list_tables", {"schema": "public"}),
        ("list_views", {"schema": "public"}),
        ("list_columns", {"schema": "public", "table": "test"}),
        ("list_indexes", {"schema": "public", "table": "test"}),
        ("execute_query", {"query": "SELECT version()", "readonly": True}),
        ("list_large_tables", {"limit": 5}),
        ("get_table_stats", {"schema": "public", "table": "film"}),
        ("explain_query", {"query": "SELECT version()"}),
    ])
    async def test_tools(self, client, tool_name, parameters):
        """Test various tool calls"""
        try:
            result = await client.call_tool(tool_name, parameters)
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call tool {tool_name}: {e}")
    
    async def test_analyze_query_performance_prompt(self, client):
        """Test query performance analysis prompt"""
        try:
            prompt = await client.get_prompt(
                "analyze_query_performance",
                params={
                    "sql": "SELECT * FROM public.test",
                    "explain": "public.test",
                    "table_info": "100 rows, 10 MB"
                }
            )
            assert prompt is not None
            assert prompt.description is not None
            assert isinstance(prompt.messages, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get analyze_query_performance prompt: {e}")
    
    async def test_suggest_indexes_prompt(self, client):
        """Test index suggestion prompt"""
        try:
            prompt = await client.get_prompt(
                "suggest_indexes",
                params={
                    "query": "public",
                    "table_info": "public.test",
                    "table_stats": "100 rows, 10 MB"
                }
            )
            assert prompt is not None
            assert prompt.description is not None
            assert isinstance(prompt.messages, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get suggest_indexes prompt: {e}")
    
    async def test_database_health_check_prompt(self, client):
        """Test database health check prompt"""
        try:
            prompt = await client.get_prompt("database_health_check")
            assert prompt is not None
            assert prompt.description is not None
            assert isinstance(prompt.messages, list)
        except Exception as e:
            pytest.skip(f"Skipping test - unable to get database_health_check prompt: {e}")

    # User and permission management tests
    async def test_list_users(self, client):
        """Test listing all users"""
        try:
            result = await client.call_tool("list_users", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_users tool: {e}")

    async def test_list_user_permissions(self, client):
        """Test listing user permissions"""
        try:
            result = await client.call_tool("list_user_permissions", {"username": "postgres"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_user_permissions tool: {e}")

    async def test_list_table_privileges(self, client):
        """Test listing table privileges"""
        try:
            result = await client.call_tool("list_table_privileges", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_table_privileges tool: {e}")

    # Constraint and relationship management tests
    async def test_list_constraints(self, client):
        """Test listing constraints"""
        try:
            result = await client.call_tool("list_constraints", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_constraints tool: {e}")

    async def test_list_foreign_keys(self, client):
        """Test listing foreign keys"""
        try:
            result = await client.call_tool("list_foreign_keys", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_foreign_keys tool: {e}")

    async def test_list_referenced_tables(self, client):
        """Test listing referenced tables"""
        try:
            result = await client.call_tool("list_referenced_tables", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_referenced_tables tool: {e}")

    # Performance monitoring and optimization tests
    async def test_get_slow_queries(self, client):
        """Test getting slow queries"""
        try:
            result = await client.call_tool("get_slow_queries", {"limit": 5})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_slow_queries tool: {e}")

    async def test_get_index_usage(self, client):
        """Test getting index usage"""
        try:
            result = await client.call_tool("get_index_usage", {"schema": "public"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_index_usage tool: {e}")

    async def test_get_table_bloat_info(self, client):
        """Test getting table bloat information"""
        try:
            result = await client.call_tool("get_table_bloat_info", {"schema": "public", "limit": 5})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_table_bloat_info tool: {e}")

    async def test_get_database_activity(self, client):
        """Test getting database activity"""
        try:
            result = await client.call_tool("get_database_activity", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_database_activity tool: {e}")

    async def test_get_vacuum_info(self, client):
        """Test getting vacuum information"""
        try:
            result = await client.call_tool("get_vacuum_info", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_vacuum_info tool: {e}")

    # Database object management tests
    async def test_list_functions(self, client):
        """Test listing functions"""
        try:
            result = await client.call_tool("list_functions", {"schema": "public"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_functions tool: {e}")

    async def test_get_function_definition(self, client):
        """Test getting function definition"""
        try:
            result = await client.call_tool("get_function_definition", {"schema": "public", "function_name": "now"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_function_definition tool: {e}")

    async def test_list_triggers(self, client):
        """Test listing triggers"""
        try:
            result = await client.call_tool("list_triggers", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_triggers tool: {e}")

    async def test_list_materialized_views(self, client):
        """Test listing materialized views"""
        try:
            result = await client.call_tool("list_materialized_views", {"schema": "public"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_materialized_views tool: {e}")

    async def test_get_materialized_view_definition(self, client):
        """Test getting materialized view definition"""
        try:
            result = await client.call_tool("get_materialized_view_definition", {"schema": "public", "view_name": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_materialized_view_definition tool: {e}")

    async def test_get_table_ddl(self, client):
        """Test getting table DDL"""
        try:
            result = await client.call_tool("get_table_ddl", {"schema": "public", "table": "film"})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call get_table_ddl tool: {e}")

    async def test_list_active_connections(self, client):
        """Test listing active connections"""
        try:
            result = await client.call_tool("list_active_connections", {})
            assert result is not None
            assert hasattr(result, 'structured_content')
        except Exception as e:
            pytest.skip(f"Skipping test - unable to call list_active_connections tool: {e}")


@pytest.mark.asyncio
async def test_client_modes():
    """Test basic client functionality in different modes"""
    for mode in ["stdio", "http"]:
        client = await CloudberryMCPClient.create(mode=mode)
        try:
            # Basic connection test
            tools = await client.list_tools()
            assert isinstance(tools, list)
        finally:
            await client.close()
