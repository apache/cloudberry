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
# contrib/udp2/Makefile

top_builddir = ../..

# Include the standard PostgreSQL build system to get variables like prefix, DESTDIR, etc.
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/udp2
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Use CMake for building this module
CMAKE_BUILD_DIR = build

# Default target
all: $(CMAKE_BUILD_DIR)/udp2.so

# Create build directory and configure with CMake
$(CMAKE_BUILD_DIR)/Makefile:
	@echo "Configuring udp2 with CMake..."
	@mkdir -p $(CMAKE_BUILD_DIR)
	@cd $(CMAKE_BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) ..

# Build the project using CMake
$(CMAKE_BUILD_DIR)/udp2.so: $(CMAKE_BUILD_DIR)/Makefile
	@echo "Building udp2 with CMake..."
	@cd $(CMAKE_BUILD_DIR) && $(MAKE)
	@cp $(CMAKE_BUILD_DIR)/udp2.so udp2.so

# Install target
install: $(CMAKE_BUILD_DIR)/udp2.so
	@echo "Installing udp2..."
	@cd $(CMAKE_BUILD_DIR) && $(MAKE) install

# Clean target
clean:
	@echo "Cleaning udp2..."
	@rm -rf $(CMAKE_BUILD_DIR)
	@rm -f udp2.so

# Ensure ic_common is built first
ic_common:
	@echo "Building ic_common..."
	@mkdir -p ic_common/build
	@cd ic_common/build && cmake -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) .. && $(MAKE) && $(MAKE) install

# Make sure ic_common is built before udp2
$(CMAKE_BUILD_DIR)/udp2.so: ic_common

.PHONY: all install clean ic_common
