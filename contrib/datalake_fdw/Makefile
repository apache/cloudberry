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
# contrib/datalake_fdw/Makefile

MODULE_big = datalake_fdw
EXTENSION = datalake_fdw
DATA = datalake_fdw--1.0.sql

OBJS = \
	src/am_iceberg/pg_iceberg_am_handler.o \
	src/am_iceberg/pg_iceberg_extensible.o \
	src/am_iceberg/pg_iceberg_ddl.o \
	src/am_iceberg/pg_iceberg_options.o \
	src/am_iceberg/pg_iceberg_guc.o \
	src/am_iceberg/pg_iceberg_reject.o \
	src/iceberg_catalog_fdw/iceberg_catalog_fdw.o \
	src/iceberg_catalog_fdw/iceberg_catalog_option.o \
	src/iceberg_volume_fdw/iceberg_volume_fdw.o \
	src/iceberg_volume_fdw/iceberg_volume_option.o \
	src/meta/meta_engine_registry.o \
	src/meta/meta_engine_init.o \
	src/meta/engine_stub/stub_engine.o \
	src/format/format_registry.o \
	src/common/dl_err.o \
	src/common/dl_option_util.o \
	src/common/parser_option.o \
	src/common/file_system_wrapper.o \
	src/common/s3_file_system.o \
	src/common/backend_registry.o

# Use the documented PGXS knobs: pgxs.mk appends these AFTER the flags configure
# chose, so optimization/warning settings survive.  A pre-include
# "override CFLAGS +=" would give CFLAGS override origin and silently discard
# Makefile.global's own "CFLAGS = @CFLAGS@" assignment.
PG_CFLAGS = -fvisibility=hidden
PG_CXXFLAGS = -fvisibility=hidden -fvisibility-inlines-hidden -std=c++17
PG_CPPFLAGS = -I$(srcdir)/src

# The regression cases live with the rest of the test material rather than in a
# second place of their own; pg_regress is pointed at them.  REGRESS_OPTS is
# expanded after the --inputdir that Makefile.global supplies, so this wins.
#
# _PG_init refuses to run outside shared_preload_libraries, so any server used
# to test this module has to be started with it.  For an in-tree "make check"
# that is what the temp-config supplies.  For "make installcheck" -- which is
# what CI runs, against a cluster created with the library already preloaded --
# pg_regress ignores it.  Note that "make check" exists in-tree only; under PGXS
# pgxs.mk refuses the target outright.
REGRESS = iceberg_am_ddl iceberg_am_reject iceberg_am_acl
REGRESS_OPTS = --temp-config=$(srcdir)/datalake_fdw.conf \
	--inputdir=$(srcdir)/test/automation/sqlrepo/smoke/iceberg_am

EXTRA_CLEAN = exports_darwin.list exports.map

# Keep the aggregate target as make's default goal.
all:

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/datalake_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Everything below needs variables that Makefile.global defines (PORTNAME), and
# SHLIB_LINK additions still apply because the link recipe expands it when it
# runs.

# Shared libraries are linked with $(CC) (see src/Makefile.shlib COMPILER), so a
# module containing C++ translation units must pull in the C++ runtime itself.
SHLIB_LINK += -lstdc++

# Arrow and other C++ dependencies land in this module later; the export list is
# the single place that decides what stays visible, so the mechanism ships now.
ifeq ($(PORTNAME), darwin)
EXPORT_LIST = exports_darwin.list
SHLIB_LINK += -Wl,-exported_symbols_list,exports_darwin.list

exports_darwin.list: exports.txt
	sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$$/d' -e 's/^/_/' $< > $@
else
EXPORT_LIST = exports.map
SHLIB_LINK += -Wl,--version-script=exports.map -Wl,--exclude-libs,ALL

exports.map: exports.txt
	{ echo '{ global:'; sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$$/d' -e 's/$$/;/' $<; echo 'local: *; };'; } > $@
endif

all: $(EXPORT_LIST)
$(shlib): $(EXPORT_LIST)
