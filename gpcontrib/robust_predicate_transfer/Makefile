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
# Makefile for the robust_predicate_transfer extension.
#
# IDENTIFICATION
#    gpcontrib/robust_predicate_transfer/Makefile
#
#-------------------------------------------------------------------------

MODULE_big = robust_predicate_transfer
OBJS = \
	$(WIN32RES) \
	src/rptbackward.o \
	src/rptbackwardplan.o \
	src/rptgraph.o \
	src/rpteligible.o \
	src/rptforward.o \
	src/rptforwardplan.o \
	src/rptinit.o \
	src/rptinject.o \
	src/rptlocal.o \
	src/rptplan.o \
	src/rptregion.o

PGFILEDESC = "robust_predicate_transfer - RPT+ transfer plans over Anser"

EXTENSION = robust_predicate_transfer
DATA = robust_predicate_transfer--1.0.sql

REGRESS = rpt_regions rpt_graph rpt_backward_eligibility rpt_key_types rpt_forward rpt_forward_faults rpt_backward rpt_backward_faults

# Anser is a sibling module, not a linked library: it is preloaded and dfmgr.c
# loads modules RTLD_GLOBAL, so the AnserXxx entry points resolve at load time.
# That is what the PGDLLEXPORT markers on Anser's public headers are for.
PG_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/../anser/include

# C++ module; LLVM bitcode is not generated for it (as gpcloud).
with_llvm = no

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/robust_predicate_transfer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Link with the C++ driver so the module owns its STL/exception runtime
# dependency even when the server was built without ORCA.
COMPILER = $(CXX) $(CXXFLAGS)

# The analysis needs anser's library installed and preloaded: our module
# resolves AnserIsRuntimeFilterScan from it at load time (RTLD_NOW), and
# load order must be anser first.  Injection also needs anser.enable on.
# Mirrors gpcontrib/anser's preload machinery.
# Backward fault tests use anser_test, which Anser's normal install omits.
installcheck: install-anser-test preload-rpt

.PHONY: install-anser-test
install-anser-test:
	$(MAKE) -C ../anser install-anser-test

.PHONY: preload-rpt
preload-rpt:
	@if [ -z "$$COORDINATOR_DATA_DIRECTORY$$MASTER_DATA_DIRECTORY" ]; then \
	    echo "ERROR: COORDINATOR_DATA_DIRECTORY (or MASTER_DATA_DIRECTORY) is not set;" >&2; \
	    echo "       source cloudberry-env.sh and gpdemo-env.sh before running installcheck." >&2; \
	    exit 1; \
	fi
	@current=`psql -d postgres -At -c "SHOW shared_preload_libraries;"` || { \
	    echo "ERROR: cannot read shared_preload_libraries; is the cluster up?" >&2; \
	    exit 1; \
	}; \
	current=`echo "$$current" | tr -d ' '`; \
	case ",$$current," in \
	  *,anser,*robust_predicate_transfer,*) \
	    echo "==> shared_preload_libraries already carries anser + robust_predicate_transfer" ;; \
	  *) \
	    without=`echo "$$current" | sed -e 's/anser//g' -e 's/robust_predicate_transfer//g' -e 's/,,*/,/g' -e 's/^,//' -e 's/,$$//'`; \
	    if [ -z "$$without" ]; then new="anser,robust_predicate_transfer"; else new="$$without,anser,robust_predicate_transfer"; fi; \
	    echo "==> Setting shared_preload_libraries to $$new"; \
	    gpconfig -c shared_preload_libraries -v "'$$new'" --skipvalidation || exit 1; \
	    gpstop -ra || exit 1 ;; \
	esac
	@# With both libraries in place, Anser's own target turns anser.enable on
	@# (it defaults to off) and restarts only if that is still needed.
	$(MAKE) -C ../anser preload-anser

# CI runs a single make target per test config, so offer one target that
# installs the extension (and Anser, which we load-resolve against), arms the
# cluster, and runs the tests.
.PHONY: installcheck-with-install
installcheck-with-install:
	$(MAKE) unitcheck surveycheck diagnosticcheck
	$(MAKE) -C ../anser install
	$(MAKE) install
	$(MAKE) installcheck

# Uses PostgreSQL headers but no server: synthetic plans exercise the exact
# production graph code and deterministic failure paths.
ANSER_SHARED_HEADERS = $(wildcard $(srcdir)/../anser/include/*.h)

.PHONY: unitcheck
unitcheck: test/graph_test test/inject_test test/forward_walk_test test/local_hash_test
	./test/graph_test
	./test/inject_test
	./test/forward_walk_test
	./test/local_hash_test

# Exercise Anser's real shared key checks, not copies in the test fixture.
test/anserplankeys.o: $(srcdir)/../anser/src/anserplankeys.c $(srcdir)/../anser/include/anserplan.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -c $< -o $@

test/graph_test: $(srcdir)/test/graph_test.cpp test/anserplankeys.o $(wildcard $(srcdir)/src/*.cpp) $(srcdir)/include/rpt.h $(srcdir)/include/rptqual.h $(ANSER_SHARED_HEADERS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) $< test/anserplankeys.o -o $@

test/inject_test: $(srcdir)/test/inject_test.cpp $(wildcard $(srcdir)/src/*.cpp) $(srcdir)/include/rpt.h $(srcdir)/include/rptinject.h $(srcdir)/include/rptchannel.h $(srcdir)/../anser/include/ansertlist.h $(ANSER_SHARED_HEADERS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -ffunction-sections -fdata-sections \
	  $(WALK_TEST_LDFLAGS) $< -o $@

EXTRA_CLEAN += test/graph_test test/inject_test test/anserplankeys.o

SURVEY_PYTHON ?= python3
.PHONY: surveycheck
surveycheck:
	PYTHONDONTWRITEBYTECODE=1 $(SURVEY_PYTHON) $(srcdir)/test/survey_test.py

# Link the production backend walker; unused planner code is discarded so the
# fixture needs no server. Expression-list traversal is stubbed in the test.
ifeq ($(PORTNAME),darwin)
WALK_TEST_LDFLAGS = -Wl,-dead_strip
else
WALK_TEST_LDFLAGS = -Wl,--gc-sections
endif

test/forward_walk_backend.o: $(top_srcdir)/src/backend/optimizer/util/walkers.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test/forward_walk_test: $(srcdir)/test/forward_walk_test.cpp $(srcdir)/src/rptforward.cpp $(srcdir)/include/rpt.h $(srcdir)/include/rptinject.h $(srcdir)/include/rptchannel.h test/forward_walk_backend.o $(ANSER_SHARED_HEADERS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -ffunction-sections -fdata-sections \
	  $(WALK_TEST_LDFLAGS) $< test/forward_walk_backend.o -o $@

EXTRA_CLEAN += test/forward_walk_test test/forward_walk_backend.o

.PHONY: diagnosticcheck
diagnosticcheck:
	PYTHONDONTWRITEBYTECODE=1 $(SURVEY_PYTHON) $(srcdir)/test/diagnose_rpt_test.py

# Run the real local reducer producer/consumer over storage-shaped Datums.
test/local_hash_test: $(srcdir)/test/local_hash_test.c $(srcdir)/src/rptlocal.c $(srcdir)/../anser/include/anserbloom.h $(ANSER_SHARED_HEADERS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -ffunction-sections -fdata-sections \
	  $(WALK_TEST_LDFLAGS) $< -o $@

EXTRA_CLEAN += test/local_hash_test
