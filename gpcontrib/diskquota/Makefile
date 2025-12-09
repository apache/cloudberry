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
# Makefile for diskquota extension
#
# This Makefile wraps the CMake build system for integration with
# the Cloudberry build process.
#
# Usage:
#   make                    # build the extension
#   make install            # install the extension
#   make installcheck       # run regression tests
#   make clean              # clean build artifacts

ifdef USE_PGXS
# Standalone build: pg_config must be in PATH
PG_CONFIG_ABS := $(shell which pg_config)
PG_PREFIX := $(shell $(PG_CONFIG_ABS) --prefix)
CMAKE_OPTS := -DPG_CONFIG=$(PG_CONFIG_ABS)
else
# In-tree build
subdir = gpcontrib/diskquota
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
# Get absolute source directory path
PG_SRC_DIR_ABS := $(shell cd $(top_builddir) && pwd)
PG_CONFIG_ABS := $(PG_SRC_DIR_ABS)/src/bin/pg_config/pg_config
PG_PREFIX := $(prefix)
# Pass PG_SRC_DIR to CMake so it doesn't try to derive it from pg_config
CMAKE_OPTS := -DPG_CONFIG=$(PG_CONFIG_ABS) -DPG_SRC_DIR=$(PG_SRC_DIR_ABS)
endif

.PHONY: all
all: build

.PHONY: build
build:
	@echo "Building diskquota with CMake..."
	@if [ ! -f build/Makefile ]; then \
		mkdir -p build && \
		cd build && \
		cmake $(CMAKE_OPTS) -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(PG_PREFIX) .. ; \
	fi
	cd build && $(MAKE)

.PHONY: install
install: build
	cd build && $(MAKE) install

.PHONY: installcheck
installcheck:
	@echo "Running diskquota regression tests..."
	@if [ ! -f build/Makefile ]; then \
		mkdir -p build && \
		cd build && \
		cmake $(CMAKE_OPTS) -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(PG_PREFIX) .. ; \
	fi
	cd build && $(MAKE) installcheck

.PHONY: clean
clean:
	rm -rf build
