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
from typing import Optional, Tuple
from dotenv import load_dotenv


# How much of a query result one tool call returns.  Everything a tool returns
# lands in the agent's context, and nothing else bounds a SELECT the agent
# writes itself.
DEFAULT_MAX_ROWS = 1000
DEFAULT_MAX_VALUE_LENGTH = 10000
DEFAULT_MAX_RESULT_BYTES = 1000000

# Seconds a statement may run before the driver cancels it on the server.
DEFAULT_QUERY_TIMEOUT = 60.0

# Writing results back.  Off unless MCP_ENABLE_WRITE says otherwise; the
# limits bound one write_results call.
DEFAULT_WRITE_MAX_ROWS = 1000
DEFAULT_WRITE_MAX_BYTES = 4 * 1024 * 1024
DEFAULT_WRITE_LOCK_TIMEOUT = 10.0

# PostgreSQL keeps NAMEDATALEN - 1 bytes of an identifier.
MAX_IDENTIFIER_BYTES = 63


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(f"{name} must be a positive integer, got {value}")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive number of seconds, got {raw!r}") from None
    if not value > 0 or value == float("inf"):
        raise ValueError(f"{name} must be a positive number of seconds, got {raw!r}")
    return value


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    max_rows: int = DEFAULT_MAX_ROWS
    max_value_length: int = DEFAULT_MAX_VALUE_LENGTH
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES
    query_timeout: float = DEFAULT_QUERY_TIMEOUT
    
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
            max_rows=_positive_int_env("MCP_MAX_ROWS", DEFAULT_MAX_ROWS),
            max_value_length=_positive_int_env(
                "MCP_MAX_VALUE_LENGTH", DEFAULT_MAX_VALUE_LENGTH
            ),
            max_result_bytes=_positive_int_env(
                "MCP_MAX_RESULT_BYTES", DEFAULT_MAX_RESULT_BYTES
            ),
            query_timeout=_positive_float_env("MCP_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT),
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

def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    value = raw.strip().lower()
    if value not in ("true", "false"):
        raise ValueError(f"{name} must be true or false, got {raw!r}")
    return value == "true"


def _parse_identifier(text: str, position: int, entry: str) -> Tuple[str, int]:
    """Read one identifier the way the server's parser does.

    A quoted identifier keeps its case and turns "" into "; an unquoted one
    folds ASCII letters to lower case and may not start with a digit or $.
    """
    if position < len(text) and text[position] == '"':
        name = []
        position += 1
        while True:
            end = text.find('"', position)
            if end < 0:
                raise ValueError(f"MCP_WRITE_TABLES: unterminated quoted identifier in {entry!r}")
            name.append(text[position:end])
            if text[end + 1:end + 2] == '"':
                name.append('"')
                position = end + 2
                continue
            position = end + 1
            break
        identifier = "".join(name)
        if not identifier:
            raise ValueError(f"MCP_WRITE_TABLES: empty quoted identifier in {entry!r}")
    else:
        start = position
        while position < len(text) and (
            text[position].isalnum() or text[position] in "_$" or ord(text[position]) > 127
        ):
            position += 1
        identifier = text[start:position]
        if not identifier or identifier[0].isdigit() or identifier[0] == "$":
            raise ValueError(
                f"MCP_WRITE_TABLES: expected an identifier at "
                f"{text[start:] or 'the end'!r} in {entry!r}"
            )
        identifier = "".join(
            c.lower() if "A" <= c <= "Z" else c for c in identifier
        )
    if len(identifier.encode("utf-8")) > MAX_IDENTIFIER_BYTES:
        raise ValueError(
            f"MCP_WRITE_TABLES: identifier {identifier!r} in {entry!r} is longer than "
            f"{MAX_IDENTIFIER_BYTES} bytes, so no table can have it"
        )
    return identifier, position


def parse_write_tables(raw: str) -> Tuple[Tuple[str, Optional[str]], ...]:
    """Parse MCP_WRITE_TABLES into (schema, table) pairs.

    Entries are schema.table or schema.* separated by commas.  Names follow
    the PostgreSQL identifier rules, so the pairs are catalog names and are
    compared with the catalog exactly.  A table of None stands for every
    table in the schema.
    """
    entries = []
    position = 0
    text = raw.strip()
    if not text:
        raise ValueError("MCP_WRITE_TABLES is empty")
    while True:
        while position < len(text) and text[position].isspace():
            position += 1
        schema, position = _parse_identifier(text, position, raw)
        if text[position:position + 1] != ".":
            raise ValueError(
                f"MCP_WRITE_TABLES entries must be schema.table or schema.*, "
                f"got {text[:position + 1]!r}..."
            )
        position += 1
        if text[position:position + 1] == "*":
            table = None
            position += 1
        else:
            table, position = _parse_identifier(text, position, raw)
        entries.append((schema, table))
        while position < len(text) and text[position].isspace():
            position += 1
        if position == len(text):
            return tuple(entries)
        if text[position] != ",":
            raise ValueError(
                f"MCP_WRITE_TABLES: expected a comma after {text[:position]!r}"
            )
        position += 1


@dataclass(frozen=True)
class WriteConfig:
    """Whether and where write_results may write."""
    enabled: bool = False
    tables: Tuple[Tuple[str, Optional[str]], ...] = ()
    max_rows: int = DEFAULT_WRITE_MAX_ROWS
    max_bytes: int = DEFAULT_WRITE_MAX_BYTES
    lock_timeout: float = DEFAULT_WRITE_LOCK_TIMEOUT

    @classmethod
    def from_env(cls, query_timeout: float) -> "WriteConfig":
        """Read the write settings; any invalid value stops the server.

        With writing off the other settings are not read at all, so a server
        that never writes starts exactly as it did before they existed.
        """
        load_dotenv()
        if not _bool_env("MCP_ENABLE_WRITE", False):
            return cls()
        raw_tables = os.getenv("MCP_WRITE_TABLES", "")
        if not raw_tables.strip():
            raise ValueError(
                "MCP_WRITE_TABLES must list the tables write_results may write "
                "when MCP_ENABLE_WRITE is true"
            )
        lock_timeout = _positive_float_env(
            "MCP_WRITE_LOCK_TIMEOUT", DEFAULT_WRITE_LOCK_TIMEOUT
        )
        if lock_timeout >= query_timeout:
            raise ValueError(
                f"MCP_WRITE_LOCK_TIMEOUT ({lock_timeout:g} s) must be less than "
                f"MCP_QUERY_TIMEOUT ({query_timeout:g} s), or a lock wait would "
                f"surface as a query timeout"
            )
        return cls(
            enabled=True,
            tables=parse_write_tables(raw_tables),
            max_rows=_positive_int_env("MCP_WRITE_MAX_ROWS", DEFAULT_WRITE_MAX_ROWS),
            max_bytes=_positive_int_env("MCP_WRITE_MAX_BYTES", DEFAULT_WRITE_MAX_BYTES),
            lock_timeout=lock_timeout,
        )

    def allows(self, schema: str, table: str) -> bool:
        """Whether schema.table, as catalog names, is on the whitelist."""
        return any(
            schema == allowed_schema and (allowed_table is None or table == allowed_table)
            for allowed_schema, allowed_table in self.tables
        )
