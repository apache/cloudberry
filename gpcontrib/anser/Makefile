#-------------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Makefile for the anser extension.
#
# IDENTIFICATION
#    gpcontrib/anser/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = anser
OBJS = \
	$(WIN32RES) \
	src/anserbloomconsume.o \
	src/anserbloomproduce.o \
	src/anserdispatch.o \
	src/anserfilter.o \
	src/anserinit.o \
	src/anserplan.o \
	src/anserplanexec.o \
	src/ansersideband.o \
	src/anser_test.o

PGFILEDESC = "anser - adaptive information sharing runtime filters"

# The subsystem itself needs no catalog objects -- it is configured entirely by
# GUCs and travels over the dispatch connection.  The only extension here is
# anser_test, which exposes the internal C API to the regression tests from
# inside the same library.
EXTENSION = anser_test
DATA = anser_test--1.0.sql

REGRESS = anser_test anser_runtime_filter

# src/anserdispatch.c writes sideband messages onto the dispatcher's own libpq
# connections, so it needs libpq's internal headers (as the dispatcher itself
# does); the symbols resolve against the backend.
PG_CPPFLAGS = -I$(srcdir)/include -I$(libpq_srcdir)
SHLIB_PREREQS = submake-libpq

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/anser
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# The tests need the live gather/send services, which only exist when the
# library is preloaded and anser.enable is on -- both postmaster-context
# settings.  Ensure them (and restart) before installcheck, as
# gp_relsizes_stats does for its worker.
installcheck: preload-anser

.PHONY: preload-anser
preload-anser:
	@if [ -z "$$COORDINATOR_DATA_DIRECTORY$$MASTER_DATA_DIRECTORY" ]; then \
	    echo "ERROR: COORDINATOR_DATA_DIRECTORY (or MASTER_DATA_DIRECTORY) is not set;" >&2; \
	    echo "       source cloudberry-env.sh and gpdemo-env.sh before running installcheck." >&2; \
	    exit 1; \
	fi
	@current=`psql -d postgres -At -c "SHOW shared_preload_libraries;" | tr -d ' '`; \
	case ",$$current," in \
	  *,anser,*) \
	    echo "==> shared_preload_libraries already contains anser" ;; \
	  *) \
	    if [ -z "$$current" ]; then new=anser; else new="$$current,anser"; fi; \
	    echo "==> Adding anser to shared_preload_libraries ($$new)"; \
	    gpconfig -c shared_preload_libraries -v "'$$new'" --skipvalidation ;; \
	esac
	gpconfig -c anser.enable -v on --skipvalidation
	gpstop -ra
	psql -d postgres -c "SHOW shared_preload_libraries;"

# CI runs a single make target per test config, so offer one target that
# installs the extension, arms the cluster, and runs the tests.
.PHONY: installcheck-with-install
installcheck-with-install:
	$(MAKE) install
	$(MAKE) installcheck
