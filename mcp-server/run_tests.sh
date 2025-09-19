#!/bin/bash
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


# Test script for Apache Cloudberry MCP Server

echo "=== Install test dependencies ==="
uv pip install -e ".[dev]"

echo "=== Run all tests ==="
uv run pytest tests/ -v

echo "=== Run specific test patterns ==="
echo "Run stdio mode test:"
uv run pytest tests/test_cbmcp.py::TestCloudberryMCPClient::test_list_capabilities -v

echo "Run http mode test:"
uv run pytest tests/test_cbmcp.py::TestCloudberryMCPClient::test_list_capabilities -v

echo "=== Run coverage tests ==="
uv run pytest tests/ --cov=cbmcp --cov-report=html --cov-report=term

echo "=== Test completed ==="
