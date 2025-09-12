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
Configuration utilities for the Apache Cloudberry MCP server
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create config from environment variables"""
        load_dotenv()
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "postgres"),
            username=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
        )
    
    @property
    def connection_string(self) -> str:
        """Get Apache Cloudberry connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class ServerConfig:
    """MCP server configuration"""
    host: str
    port: int
    path: str
    debug: bool
    
    @classmethod
    def from_env(cls) -> "ServerConfig":
        """Create config from environment variables"""
        load_dotenv()
        return cls(
            host=os.getenv("MCP_HOST", "localhost"),
            port=int(os.getenv("MCP_PORT", "8000")),
            path=os.getenv("MCP_PATH", "/mcp/"),
            debug=os.getenv("MCP_DEBUG", "false").lower() == "true",
        )