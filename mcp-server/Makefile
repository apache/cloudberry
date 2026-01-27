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
#
# Makefile for Apache Cloudberry MCP Server
#
# This Makefile integrates the Python-based MCP server into the
# Cloudberry build system. It uses uv (preferred) or pip for
# dependency management and installation.
#

# Get build configuration from top-level Makefile
subdir = mcp-server
top_builddir = ..
include $(top_builddir)/src/Makefile.global

# Installation directories
# Note: Use $(prefix) directly to avoid automatic /postgresql suffix added by $(datadir)
MCP_SERVER_BIN_DIR = $(prefix)/bin

# Python and package manager
# Note: fastmcp requires Python 3.10+
PYTHON3 ?= python3.11
UV ?= uv
PIP3 ?= pip3.11

# Determine which package manager to use
USE_UV := $(shell command -v $(UV) 2> /dev/null)

# Get Python version at parse time for use in install target
PYTHON_VER := $(shell $(PYTHON3) -c "import sys; print('%d.%d' % (sys.version_info.major, sys.version_info.minor))" 2>/dev/null || echo "3.11")

# Build directory
BUILD_DIR = dist
VENV_DIR = .venv

.PHONY: all build install installdirs uninstall clean distclean

all: build

# Build the MCP server package
build:
	@echo "Building Apache Cloudberry MCP Server..."
ifdef USE_UV
	@echo "Using uv for build..."
	$(UV) build
else
	@echo "Using pip for build..."
	$(PYTHON3) -m pip install --upgrade build
	$(PYTHON3) -m build
endif
	@echo "MCP Server build complete."

# Install the MCP server
install: build installdirs
	@echo "Installing Apache Cloudberry MCP Server..."
ifdef USE_UV
	@echo "Installing with uv..."
	$(UV) pip install --system --prefix=$(DESTDIR)$(prefix) $(BUILD_DIR)/*.whl
else
	@echo "Installing with pip..."
	$(PIP3) install --target=$(DESTDIR)$(prefix)/lib/python$(PYTHON_VER)/site-packages $(BUILD_DIR)/*.whl
endif
	@# Create a wrapper script for easier invocation
	@echo '#!/bin/sh' > '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo '# Wrapper script for Apache Cloudberry MCP Server' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo '' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo '# Set PYTHONPATH for installed packages' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo 'GPHOME="$$(cd "$$(dirname "$$0")/.." && pwd)"' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo 'export PYTHONPATH="$$GPHOME/lib/python$(PYTHON_VER)/site-packages:$$PYTHONPATH"' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo '' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo 'exec $(PYTHON3) -m cbmcp.server "$$@"' >> '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@chmod +x '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
	@echo "MCP Server installation complete."
	@echo "Installed to: $(DESTDIR)$(prefix)"
	@echo "You can run it with: cloudberry-mcp-server"

# Create installation directories
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(MCP_SERVER_BIN_DIR)'

# Uninstall the MCP server
uninstall:
	@echo "Uninstalling Apache Cloudberry MCP Server..."
	rm -f '$(DESTDIR)$(MCP_SERVER_BIN_DIR)/cloudberry-mcp-server'
ifdef USE_UV
	-$(UV) pip uninstall --system cloudberry-mcp-server 2>/dev/null || true
else
	-$(PIP3) uninstall -y cloudberry-mcp-server 2>/dev/null || true
endif
	@echo "MCP Server uninstalled."

# Clean build artifacts
clean:
	@echo "Cleaning MCP Server build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -rf $(VENV_DIR)
	rm -rf build
	rm -rf *.egg-info
	rm -rf src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	find . -type f -name '*.pyo' -delete 2>/dev/null || true
	@echo "MCP Server cleaned."

# Distclean - remove all generated files
distclean: clean
	@echo "Deep cleaning MCP Server..."
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	@echo "MCP Server distclean complete."

# Help target
help:
	@echo "Apache Cloudberry MCP Server Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  all        - Build the MCP server (default)"
	@echo "  build      - Build the MCP server package"
	@echo "  install    - Install the MCP server"
	@echo "  uninstall  - Uninstall the MCP server"
	@echo "  clean      - Remove build artifacts"
	@echo "  distclean  - Remove all generated files"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Configuration:"
	@echo "  PYTHON3    = $(PYTHON3)"
	@echo "  PYTHON_VER = $(PYTHON_VER)"
ifdef USE_UV
	@echo "  UV         = $(UV) (using uv)"
else
	@echo "  PIP3       = $(PIP3) (using pip)"
endif
	@echo "  Install to = $(prefix)"
