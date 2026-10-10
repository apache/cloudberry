<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Cloudberry MCP Testing Guide

## Test Structure

This project uses the `pytest` framework for testing, supporting both asynchronous testing and parameterized testing.

### Test Files
- `test_cbmcp.py` - Main test file containing all MCP client functionality tests
- `test_database_tools.py` - Database management tools
- `test_search_tools.py` - Retrieval tools; needs the `pgvector` extension, and skips without it
- `test_result_values.py` - How query results carry bytea, composite and range values, and the row and value limits
- `test_lance_tools.py` - Reading and vector-searching `lance_fdw` foreign tables; see below

### Lance Tests
`test_lance_tools.py` writes its own Lance datasets with pylance and reads them
back through `lance_fdw`, so it needs:

- `lance_fdw` 0.2 or later installed in the cluster;
- `pip install pylance pyarrow numpy` in the test environment;
- `CBMCP_LANCE_TEST_DIR`, a directory the tests can write to and every segment
  can read at the same path - any local directory on a single-host demo
  cluster.

Without any of these the module skips.

```bash
CBMCP_LANCE_TEST_DIR=/home/gpadmin/lance_test pytest tests/test_lance_tools.py
```

### Test Categories
- **Unit Tests** - Test individual features independently
- **Integration Tests** - Test overall system functionality
- **Parameterized Tests** - Test both stdio and http modes simultaneously

## Running Tests

### Install Test Dependencies
```bash
pip install -e ".[dev]"
```

### Run All Tests
```bash
pytest tests/
```

### Run Specific Tests
```bash
# Run specific test file
pytest tests/test_cbmcp.py

# Run specific test class
pytest tests/test_cbmcp.py::TestCloudberryMCPClient

# Run specific test method
pytest tests/test_cbmcp.py::TestCloudberryMCPClient::test_list_capabilities

# Run tests for specific mode
pytest tests/test_cbmcp.py -k "stdio"
```

### Verbose Output
```bash
pytest tests/ -v
```

### Coverage Testing
```bash
pytest tests/ --cov=src.cbmcp --cov-report=html --cov-report=term
```

## Test Features

### 1. Server Capabilities Tests
- `test_list_capabilities` - Test tool, resource, and prompt listings

### 2. Resource Tests
- `test_get_schemas_resource` - Get database schemas
- `test_get_tables_resource` - Get table listings
- `test_get_database_info_resource` - Get database information
- `test_get_database_summary_resource` - Get database summary

### 3. Tool Tests
- `test_tools` - Parameterized testing of all tool calls
  - list_tables
  - list_views
  - list_columns
  - list_indexes
  - execute_query
  - list_large_tables
  - get_table_stats
  - explain_query

### 4. Prompt Tests
- `test_analyze_query_performance_prompt` - Query performance analysis prompts
- `test_suggest_indexes_prompt` - Index suggestion prompts
- `test_database_health_check_prompt` - Database health check prompts

## Test Modes

Tests support two modes:
- **stdio** - Standard input/output mode
- **http** - HTTP mode

## Notes

1. Tests will skip inaccessible features (e.g., when database is not connected)
2. Ensure Apache Cloudberry service is started and configured correctly
3. Check database connection configuration in .env file

## Using Scripts to Run

You can use the provided script to run tests:
```bash
./run_tests.sh
```