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
# Makefile for diskquota extension (PGXS mode)
#
# This Makefile provides an alternative to the CMake build system.
# It allows diskquota to be built as part of the standard Cloudberry/PostgreSQL
# extension build process using PGXS.
#
# Usage:
#   make                    # build the extension
#   make install            # install the extension
#   make installcheck       # run regression tests
#   make clean              # clean build artifacts
#
# For CMake build (with more features like packaging), use:
#   mkdir build && cd build && cmake .. && make

# Read version from VERSION file
DISKQUOTA_VERSION := $(shell cat VERSION | tr -d '[:space:]')
DISKQUOTA_MAJOR_VERSION := $(shell echo $(DISKQUOTA_VERSION) | cut -d. -f1)
DISKQUOTA_MINOR_VERSION := $(shell echo $(DISKQUOTA_VERSION) | cut -d. -f2)
DISKQUOTA_PATCH_VERSION := $(shell echo $(DISKQUOTA_VERSION) | cut -d. -f3)

# Determine binary name based on version
ifeq ($(DISKQUOTA_MAJOR_VERSION).$(DISKQUOTA_MINOR_VERSION),1.0)
  DISKQUOTA_BINARY_NAME = diskquota
else
  DISKQUOTA_BINARY_NAME = diskquota-$(DISKQUOTA_MAJOR_VERSION).$(DISKQUOTA_MINOR_VERSION)
endif

MODULE_big = $(DISKQUOTA_BINARY_NAME)
PGFILEDESC = "diskquota - disk usage enforcement for database objects"

OBJS = src/diskquota.o \
       src/diskquota_utility.o \
       src/enforcement.o \
       src/gp_activetable.o \
       src/monitored_db.o \
       src/quotamodel.o \
       src/relation_cache.o

EXTENSION = diskquota

# SQL files for extension (installed to share/extension/)
DATA = control/ddl/diskquota--1.0.sql \
       control/ddl/diskquota--1.0--2.0.sql \
       control/ddl/diskquota--2.0.sql \
       control/ddl/diskquota--2.0--1.0.sql \
       control/ddl/diskquota--2.0--2.1.sql \
       control/ddl/diskquota--2.1.sql \
       control/ddl/diskquota--2.1--2.0.sql \
       control/ddl/diskquota--2.1--2.2.sql \
       control/ddl/diskquota--2.2.sql \
       control/ddl/diskquota--2.2--2.1.sql \
       control/ddl/diskquota--2.2--2.3.sql \
       control/ddl/diskquota--2.3.sql \
       control/ddl/diskquota--2.3--2.2.sql

# Add version definitions to CPPFLAGS
PG_CPPFLAGS = -I$(srcdir)/src \
              -DDISKQUOTA_VERSION=\"$(DISKQUOTA_VERSION)\" \
              -DDISKQUOTA_MAJOR_VERSION=$(DISKQUOTA_MAJOR_VERSION) \
              -DDISKQUOTA_MINOR_VERSION=$(DISKQUOTA_MINOR_VERSION) \
              -DDISKQUOTA_PATCH_VERSION=$(DISKQUOTA_PATCH_VERSION) \
              -DDISKQUOTA_BINARY_NAME=\"$(DISKQUOTA_BINARY_NAME)\"

# Link with libpq
PG_CPPFLAGS += -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

# Regression tests
REGRESS_OPTS = --schedule=$(srcdir)/tests/regress/diskquota_schedule
REGRESS_OPTS += --init-file=$(srcdir)/tests/init_file
REGRESS_OPTS += --load-extension=gp_inject_fault
REGRESS_OPTS += --load-extension=diskquota_test
REGRESS_OPTS += --dbname=contrib_regression

# Use PGXS
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/diskquota
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# PGXS expects diskquota.control in the current directory, but ours is in control/ddl/
# Create a symlink to the control file for PGXS compatibility
.PHONY: all
all: diskquota.control
diskquota.control: control/ddl/diskquota.control
	rm -f $@
	$(LN_S) $< $@

.PHONY: install
install: install-control
install-control:
	$(INSTALL_DATA) $(srcdir)/control/ddl/diskquota.control '$(DESTDIR)$(datadir)/extension/'

.PHONY: clean
clean: clean-control
clean-control:
	rm -f diskquota.control

# Install test extension before running installcheck
.PHONY: installcheck
installcheck: install-test-extension

install-test-extension:
	$(INSTALL_DATA) $(srcdir)/control/test/diskquota_test.control '$(DESTDIR)$(datadir)/extension/'
	$(INSTALL_DATA) $(srcdir)/control/test/diskquota_test--1.0.sql '$(DESTDIR)$(datadir)/extension/'
