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
# contrib/lance_fdw/Makefile
#
# lance_fdw - a read-only foreign-data wrapper for Lance datasets.
#
# It links liblance_c, the C binding of the Lance format, which is written in
# Rust and is not part of this tree.  In-tree, configure --with-lance finds it
# through --with-includes and --with-libraries.  With PGXS, name where it is:
#
#   make USE_PGXS=1 LANCE_C_PREFIX=/opt/lance-c       (expects include/ and lib/)
#
# The regression suites read their Lance datasets from a local directory and
# from an S3 endpoint; see test/README.md for how to provide both.

MODULE_big = lance_fdw
EXTENSION = lance_fdw
DATA = sql/lance_fdw--1.0.sql
PGFILEDESC = "lance_fdw - foreign-data wrapper for Lance datasets"

OBJS = \
	src/lance_fdw.o \
	src/lance_option.o \
	src/lance_runtime.o \
	src/lance_arrow.o \
	src/lance_deparse.o \
	src/lance_dispatch.o \
	src/lance_scan.o \
	src/lance_import.o \
	src/lance_vector.o \
	third_party/nanoarrow/src/nanoarrow.o

PG_CPPFLAGS = -I$(srcdir)/src -I$(srcdir)/third_party/nanoarrow/include
SHLIB_LINK = -llance_c -lpthread

ifdef LANCE_C_PREFIX
PG_CPPFLAGS += -I$(LANCE_C_PREFIX)/include
SHLIB_LINK := -L$(LANCE_C_PREFIX)/lib -Wl,-rpath,$(LANCE_C_PREFIX)/lib $(SHLIB_LINK)
endif

# "install" first: it creates the extension and the lance_feature schema every
# other suite builds on.
REGRESS = install ddl import errors_ddl scan_core parallel snapshot explain \
	pushdown pushdown_errors vector creds errors_scan types types_errors \
	types_nested types_nested_errors sigmask
REGRESS_OPTS = --inputdir=$(srcdir)/test/feature --outputdir=$(srcdir)/test/feature \
	--use-existing

# The suites compare text the way a C.UTF-8 database does, which is also the
# only collation under which a text comparison is pushed down to Lance, so they
# create their database themselves instead of inheriting the cluster's locale.
REGRESS_PREP = lance-regress-db

EXTRA_CLEAN = test/feature/results test/feature/regression.diffs test/feature/regression.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lance_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

abs_srcdir ?= $(CURDIR)

# The vendored nanoarrow is kept as released.  Two of its printf-style helpers
# have no format attribute, which an in-tree build turns into an error.
third_party/nanoarrow/src/nanoarrow.o: CFLAGS += -Wno-missing-format-attribute

# Where the suites find their datasets.  test/regress.mk, when present, sets
# these; README.md says how to generate and upload the datasets.  Included
# here, because under PGXS srcdir is set only once pgxs.mk is.
-include $(srcdir)/test/regress.mk
LANCE_FIXTURE_DIR ?= $(abs_srcdir)/test/fixtures/data
LANCE_S3_ENDPOINT ?=
LANCE_S3_BUCKET ?=
LANCE_S3_REGION ?=
LANCE_S3_KEY ?=
LANCE_S3_SECRET ?=

# pg_regress takes no settings of its own, so the dataset locations reach the
# suites as custom GUCs of each session it opens.
installcheck: export PGOPTIONS += -c regress.fixture_dir=$(LANCE_FIXTURE_DIR) \
	-c regress.s3_endpoint=$(LANCE_S3_ENDPOINT) -c regress.s3_bucket=$(LANCE_S3_BUCKET) \
	-c regress.s3_region=$(LANCE_S3_REGION) -c regress.s3_key=$(LANCE_S3_KEY) \
	-c regress.s3_secret=$(LANCE_S3_SECRET)

.PHONY: lance-regress-db
lance-regress-db:
	$(bindir)/psql -X -q -d postgres -v ON_ERROR_STOP=1 \
		-c 'DROP DATABASE IF EXISTS "$(CONTRIB_TESTDB)"' \
		-c "CREATE DATABASE \"$(CONTRIB_TESTDB)\" TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C.UTF-8'"
