#!/bin/bash

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
