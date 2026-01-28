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

"""Prompt templates for Apache Cloudberry database analysis."""

ANALYZE_QUERY_PERFORMANCE_PROMPT = """Please help me analyze and optimize a PostgreSQL query.

I'll provide you with:
1. The SQL query: {sql}
2. The EXPLAIN ANALYZE output: {explain}
3. Table schema information: {table_info}

Please analyze:
- Query execution plan
- Potential performance bottlenecks
- Index usage
- Suggested optimizations
- Alternative query approaches
"""

SUGGEST_INDEXES_PROMPT = """Please help me suggest optimal indexes for your PostgreSQL tables.

I'll provide you with:
1. The table schema(s): {table_info}
2. Common query patterns: {query}
3. Current indexes: {table_info}
4. Table size and row count: {table_stats}

Please analyze:
- Missing indexes based on query patterns
- Index type recommendations (B-tree, GIN, GiST, etc.)
- Composite index suggestions
- Index maintenance considerations
"""

DATABASE_HEALTH_CHECK_PROMPT = """Let's perform a comprehensive health check of your PostgreSQL database.

Please analyze:
- Database size and growth trends
- Large tables and indexes
- Query performance metrics
- Connection pool usage
- Vacuum and analyze statistics
- Index fragmentation
- Table bloat
"""