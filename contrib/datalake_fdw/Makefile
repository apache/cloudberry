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

# A second extension, so that installing datalake_fdw does not put the test
# entry points in a production database.  One library, so they reach internals.
EXTENSION = datalake_fdw datalake_fdw_test
DATA = datalake_fdw--1.0.sql datalake_fdw_test--1.0.sql

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
	src/format/arrow_support.o \
	src/format/arrow_builder.o \
	src/format/arrow_decode.o \
	src/format/parquet/parquet_format.o \
	src/format/parquet/parquet_read.o \
	src/format/parquet/parquet_write.o \
	src/common/dl_err.o \
	src/common/dl_resource.o \
	src/common/dl_option_util.o \
	src/common/parser_option.o \
	src/common/file_system_wrapper.o \
	src/common/s3_file_system.o \
	src/common/backend_registry.o \
	src/test/datalake_fdw_test.o

# libparquet is written in terms of Arrow's types, so linking one links both.
PKG_CONFIG ?= pkg-config
ARROW_MODULES = arrow parquet
HAVE_ARROW := $(shell $(PKG_CONFIG) --exists $(ARROW_MODULES) 2>/dev/null && echo yes)
# Without the filter, whichever C++ standard Arrow's .pc file names wins: these
# land in CPPFLAGS, which pgxs.mk puts after CXXFLAGS on the command line.  The
# Arrow project's own packages say -std=c++11, and their headers then fail to
# compile against themselves.
ARROW_CPPFLAGS := $(filter-out -std=%,\
	$(shell $(PKG_CONFIG) --cflags $(ARROW_MODULES) 2>/dev/null))
ARROW_LIBS := $(shell $(PKG_CONFIG) --libs $(ARROW_MODULES) 2>/dev/null)

# This has to refuse at parse time.  A recipe hung off `all` would run after
# pgxs.mk's own all-lib, so the compiler would fail on a missing arrow/api.h
# first and this would never be reached.  Not for the clean targets, because
# contrib/Makefile recurses here for those even when the module is not
# configured in.
ifneq ($(HAVE_ARROW),yes)
ifeq ($(filter clean distclean maintainer-clean,$(MAKECMDGOALS)),)
$(error datalake_fdw needs the Apache Arrow and Parquet C++ libraries, and \
pkg-config found neither "arrow" nor "parquet".  They are libarrow-devel and \
parquet-libs-devel on Rocky and RHEL, libarrow-dev and libparquet-dev on \
Debian and Ubuntu)
endif
endif

# Use the documented PGXS knobs: pgxs.mk appends these AFTER the flags configure
# chose, so optimization/warning settings survive.  A pre-include
# "override CFLAGS +=" would give CFLAGS override origin and silently discard
# Makefile.global's own "CFLAGS = @CFLAGS@" assignment.
PG_CFLAGS = -fvisibility=hidden
PG_CXXFLAGS = -fvisibility=hidden -fvisibility-inlines-hidden -std=c++17
PG_CPPFLAGS = -I$(srcdir)/src $(ARROW_CPPFLAGS)

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

# A second category, and pg_regress takes one --inputdir, so it is a second run.
FORMAT_PARQUET_REGRESS = parquet_roundtrip
FORMAT_PARQUET_INPUTDIR = $(srcdir)/test/automation/sqlrepo/smoke/format_parquet

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
SHLIB_LINK += -lstdc++ $(ARROW_LIBS)

# The export list is the single place that decides what stays visible -- which
# now also means none of Arrow's symbols become symbols this module offers.
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

# Hung off check and installcheck so that both get both categories.  REGRESS_OPTS
# has to come along: pgxs.mk is where --dbname=$(CONTRIB_TESTDB) is added to it,
# and without that pg_regress falls back to "regression" -- which it DROPs and
# recreates, taking the core suite's database with it.  The second --inputdir
# wins over the one in REGRESS_OPTS.  submake and REGRESS_PREP are the same
# prerequisites pgxs.mk gives its own targets, so that a parallel make cannot
# start pg_regress before it has been built.
installcheck: installcheck-format-parquet

installcheck-format-parquet: submake $(REGRESS_PREP)
	$(pg_regress_installcheck) $(REGRESS_OPTS) \
		--inputdir=$(FORMAT_PARQUET_INPUTDIR) $(FORMAT_PARQUET_REGRESS)

.PHONY: installcheck-format-parquet

# "make check" is in-tree only -- under PGXS pgxs.mk refuses the target -- and
# it is the only run that supplies the temp-config that preloads this module.
ifndef USE_PGXS
check: check-format-parquet

check-format-parquet: submake $(REGRESS_PREP)
	$(pg_regress_check) $(REGRESS_OPTS) \
		--inputdir=$(FORMAT_PARQUET_INPUTDIR) $(FORMAT_PARQUET_REGRESS)

.PHONY: check-format-parquet
endif
