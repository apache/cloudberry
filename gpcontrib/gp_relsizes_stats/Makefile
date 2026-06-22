#-------------------------------------------------------------------------
#
# Makefile for gp_relsizes_stats extension.
#
# By default this Makefile is intended to be included from the surrounding
# Cloudberry source tree (in-tree build); pass USE_PGXS=1 to build against an
# already-installed Cloudberry/PostgreSQL using pg_config.
#
# IDENTIFICATION
#    gpcontrib/gp_relsizes_stats/Makefile
#
#-------------------------------------------------------------------------

MODULE_big      = gp_relsizes_stats
OBJS            = ./src/gp_relsizes_stats.o
EXTENSION       = gp_relsizes_stats
EXTVERSION      = 1.0
DATA            = $(wildcard sql/*--*.sql)
REGRESS         = grants gp_relsizes_stats
REGRESS_OPTS    = --inputdir=test/ --temp-config=$(srcdir)/test/postgresql.conf.add
PGFILEDESC      = "gp_relsizes_stats - an extension to track table on-disc sizes in Cloudberry"
PG_CXXFLAGS     += $(COMMON_CPP_FLAGS)

# Auto-detect build mode: if we are sitting inside a Cloudberry source tree
# (i.e. ../../src/Makefile.global is reachable), build in-tree; otherwise fall
# back to a standalone PGXS build driven by pg_config. USE_PGXS=1 forces PGXS
# even when an in-tree Makefile.global is available.
ifeq ($(USE_PGXS),)
    ifeq ($(wildcard ../../src/Makefile.global),)
        USE_PGXS = 1
    endif
endif

ifdef USE_PGXS
	PG_CONFIG ?= pg_config
	PGXS := $(shell $(PG_CONFIG) --pgxs)
	include $(PGXS)
else
	subdir = gpcontrib/gp_relsizes_stats
	top_builddir = ../..
	include $(top_builddir)/src/Makefile.global
	include $(top_srcdir)/contrib/contrib-global.mk
endif

# The extension registers its background worker only when loaded
# via shared_preload_libraries (see _PG_init in src/gp_relsizes_stats.c).
# Without that, relsizes_collect_stats_once() silently does nothing and the
# regression tests get (0 rows) everywhere. Ensure the preload is set up
# before installcheck runs.
installcheck: preload-bgworker

.PHONY: preload-bgworker
preload-bgworker:
	@if [ -z "$$COORDINATOR_DATA_DIRECTORY$$MASTER_DATA_DIRECTORY" ]; then \
	    echo "ERROR: COORDINATOR_DATA_DIRECTORY (or MASTER_DATA_DIRECTORY) is not set;" >&2; \
	    echo "       source cloudberry-env.sh and gpdemo-env.sh before running installcheck." >&2; \
	    exit 1; \
	fi
	@echo "==> Ensuring shared_preload_libraries contains gp_relsizes_stats and restarting cluster"
	gpconfig -c shared_preload_libraries -v "'gp_relsizes_stats'" --skipvalidation
	gpstop -ra
	psql -d postgres -c "SHOW shared_preload_libraries;"
