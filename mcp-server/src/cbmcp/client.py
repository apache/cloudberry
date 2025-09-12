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
MCP Client for testing the Apache Cloudberry MCP Server

A client using the fastmcp SDK to interact with the Apache Cloudberry MCP server implementation.
"""

from typing import Any, Dict, Optional
from fastmcp import Client

from .config import DatabaseConfig, ServerConfig
from .server import CloudberryMCPServer

class CloudberryMCPClient:
    """MCP client for testing the Apache Cloudberry server using fastmcp SDK
    
    Usage:
        # Method 1: Using async context manager
        async with CloudberryMCPClient() as client:
            tools = await client.list_tools()
            resources = await client.list_resources()
        
        # Method 2: Using create class method
        client = await CloudberryMCPClient.create()
        tools = await client.list_tools()
        await client.close()
        
        # Method 3: Manual initialization
        client = CloudberryMCPClient()
        await client.initialize()
        tools = await client.list_tools()
        await client.close()
    """
    
    def __init__(self, mode: str = "stdio", server_url: str = "http://localhost:8000/mcp/"):
        self.mode = mode
        self.server_url = server_url
        self.client: Optional[Client] = None
    
    @classmethod
    async def create(cls, mode: str = "stdio", server_url: str = "http://localhost:8000/mcp/") -> "CloudberryMCPClient":
        """Asynchronously create and initialize the client"""
        instance = cls(mode, server_url)
        await instance.initialize()
        return instance
    
    async def initialize(self):
        """Initialize the client connection"""
        if self.mode == "stdio":
            server_config = ServerConfig.from_env()
            db_config = DatabaseConfig.from_env()
            server = CloudberryMCPServer(server_config, db_config)
            self.client = Client(server.mcp)
        else:
            self.client = Client(self.server_url)
        
        await self.client.__aenter__()
    
    async def close(self):
        """Close the client connection"""
        if self.client:
            await self.client.__aexit__(None, None, None)
            self.client = None
    
    async def __aenter__(self):
        if self.mode == "stdio":
            server_config = ServerConfig.from_env()
            db_config = DatabaseConfig.from_env()
            server = CloudberryMCPServer(server_config, db_config)
            self.client = Client(server.mcp)
        else:
            self.client = Client(self.server_url)

        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Call a tool on the MCP server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.call_tool(tool_name, arguments)
        
    async def get_resource(self, resource_uri: str):
        """Get a resource from the MCP server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.read_resource(resource_uri)
    
    async def get_prompt(self, prompt_name: str, params: Dict[str, Any]=None):
        """Get a prompt from the MCP server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.get_prompt(prompt_name, params)
    
    async def list_tools(self) -> list:
        """List available tools on the server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.list_tools()
    
    async def list_resources(self) -> list:
        """List available resources on the server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.list_resources()
    
    async def list_prompts(self) -> list:
        """List available prompts on the server"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async with statement.")
        
        return await self.client.list_prompts()


if __name__ == "__main__":
    import asyncio

    async def main():
        async with CloudberryMCPClient(mode="http") as client:
            results = await client.call_tool("execute_query", {
                "query": "SELECT * FROM film LIMIT 5"
            })
            print("Results:", results)

            results = await client.call_tool("list_columns", {
                "table": "film",
                "schema": "public"
            })
            print("Columns:", results)
    
    asyncio.run(main())