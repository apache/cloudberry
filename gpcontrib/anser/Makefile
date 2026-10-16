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
	src/anserpayload.o \
	src/anserplan.o \
	src/anserplanexec.o \
	src/ansersideband.o

PGFILEDESC = "anser - adaptive information sharing runtime filters"

# No catalog objects: the subsystem is configured entirely by GUCs and travels
# over the dispatch connection, so there is no extension to install here.  The
# harness the tests use to reach the C API from SQL is a library of its own in
# anser_test/ -- this one is preloaded into every backend and has no business
# carrying test code.
# anser_faults is split out rather than gated: everything in it calls
# gp_inject_fault, which exists only in a build with the debug extensions, and
# the regression driver drops any file that calls one (faultinjector_prep in
# src/Makefile.global.in).  Keeping them in anser_runtime_filter would take the
# rest of that file down with them wherever the extensions are off.
REGRESS = anser_test anser_runtime_filter anser_faults anser_multijoin

# src/anserdispatch.c writes sideband messages onto the dispatcher's own libpq
# connections, so it needs libpq's internal headers (as the dispatcher itself
# does).  Deliberately NOT linked against libpq: the pq* writers we use stay
# global in the postgres binary, while the public PQ* API is made local there
# on purpose (src/backend/Makefile), so linking libpq.so would put a second
# copy of libpq on connections the backend's copy created.
#
# Headers only means no SHLIB_PREREQS either.  submake-libpq is there to build
# what a module links against, and under PGXS libpq_builddir is $(libdir), so
# it would run "make -C $(libdir) all" and fail outright; in tree it stopped
# the build on a libpq that had not been built yet, before compiling anything.
PG_CPPFLAGS = -I$(srcdir)/include -I$(libpq_srcdir)

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
# gp_relsizes_stats does for its worker.  They also need the anser_test
# extension, which install deliberately leaves out: it is a test tool, so it is
# put in place here rather than shipped with the subsystem.
installcheck: unitcheck install-anser-test preload-anser

.PHONY: install-anser-test
install-anser-test:
	$(MAKE) -C anser_test install

# The unit tests need no cluster of their own.  Hanging them off installcheck
# is what gets them run at all: a failure here stops make before the cluster is
# touched.  (Prerequisites are unordered under -j; what matters is that a
# broken one fails the target, not which runs first.)
.PHONY: unitcheck
unitcheck:
	$(MAKE) -C test unitcheck

# Neither the harness nor the unit tests are part of this library's build, so
# they are not part of its clean either unless said so.
clean: clean-anser-test

.PHONY: clean-anser-test
clean-anser-test:
	$(MAKE) -C test clean
	$(MAKE) -C anser_test clean

# Arm the cluster, and only disturb it when that is actually needed.
#
# The decision takes one shell: make runs each recipe line in its own, and
# whether a restart is called for is only known after the first half.  That
# gives up make's per-line status check, so every command below ends in
# "|| exit 1".
#
# SHOW reports what the postmaster really loaded rather than what
# postgresql.conf says, which is the question being asked -- a library
# configured but never restarted into still needs the restart.  anser.enable is
# only worth asking about once anser is loaded: until then the GUC does not
# exist and SHOW errors out instead of answering "off".
.PHONY: preload-anser
preload-anser:
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
	need_restart=; \
	case ",$$current," in \
	  *,anser,*) \
	    echo "==> shared_preload_libraries already contains anser" ;; \
	  *) \
	    if [ -z "$$current" ]; then new=anser; else new="$$current,anser"; fi; \
	    echo "==> Adding anser to shared_preload_libraries ($$new)"; \
	    gpconfig -c shared_preload_libraries -v "'$$new'" --skipvalidation || exit 1; \
	    need_restart=1 ;; \
	esac; \
	if [ -z "$$need_restart" ] && \
	   [ "`psql -d postgres -At -c 'SHOW anser.enable' 2>/dev/null`" = on ]; then \
	    echo "==> anser.enable is already on; leaving the cluster alone"; \
	else \
	    gpconfig -c anser.enable -v on --skipvalidation || exit 1; \
	    gpstop -ra || exit 1; \
	fi; \
	psql -d postgres -c "SHOW shared_preload_libraries;" || exit 1

# CI runs a single make target per test config, so offer one target that
# installs the extension, arms the cluster, and runs the tests.
.PHONY: installcheck-with-install
installcheck-with-install:
	$(MAKE) install
	$(MAKE) installcheck
