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
Security utilities for the Apache Cloudberry MCP server
"""

from typing import Set
import re


class SQLValidator:
    """Validates SQL queries for security"""
    
    # Allowed SQL operations for safety
    ALLOWED_OPERATIONS: Set[str] = {
        "SELECT", "WITH", "SHOW", "EXPLAIN", "DESCRIBE", "PRAGMA"
    }
    
    # Blocked SQL operations
    BLOCKED_OPERATIONS: Set[str] = {
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", 
        "TRUNCATE", "GRANT", "REVOKE", "REPLACE"
    }
    
    # Sensitive tables that should not be queried
    SENSITIVE_TABLES: Set[str] = {
        "pg_user", "pg_shadow", "pg_authid", "pg_passfile",
        "information_schema.user_privileges"
    }
    
    @classmethod
    def validate_query(cls, query: str) -> tuple[bool, str]:
        """Validate a SQL query for security
        
        Returns:
            tuple: (is_valid, error_message)
        """
        query_upper = query.upper().strip()
        
        # Check for blocked operations
        for blocked in cls.BLOCKED_OPERATIONS:
            if re.search(rf"\b{blocked}\b", query_upper):
                return False, f"Blocked SQL operation: {blocked}"
        
        # Check if query starts with allowed operation
        if not any(query_upper.startswith(op) for op in cls.ALLOWED_OPERATIONS):
            return False, f"Query must start with one of: {', '.join(cls.ALLOWED_OPERATIONS)}"
        
        # Check for sensitive table access
        for sensitive_table in cls.SENSITIVE_TABLES:
            if re.search(rf"\b{sensitive_table}\b", query_upper):
                return False, f"Access to sensitive table not allowed: {sensitive_table}"
        
        # Check for potential SQL injection patterns
        injection_patterns = [
            r";.*--",  # Comments after statements
            r"/\*.*\*/",  # Block comments
            r"'OR'1'='1",  # Basic SQL injection
            r"'UNION.*SELECT",  # Union attacks
            r"EXEC\s*\(",  # Dynamic SQL execution
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, query_upper):
                return False, f"Potential SQL injection detected"
        
        return True, "Query is valid"
    
    @classmethod
    def sanitize_parameter_name(cls, param_name: str) -> str:
        """Sanitize parameter names to prevent injection"""
        # Remove any non-alphanumeric characters except underscores
        return re.sub(r"[^a-zA-Z0-9_]", "", param_name)
    
    @classmethod
    def is_readonly_query(cls, query: str) -> bool:
        """Check if a query is read-only"""
        query_upper = query.upper().strip()
        return query_upper.startswith(("SELECT", "WITH", "SHOW", "EXPLAIN"))