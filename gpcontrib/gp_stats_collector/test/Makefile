# Regression tests for the gp_stats_collector pg_query_state signal API.
#
# Self-contained installcheck suite: the extension must already be built and
# installed (make -C .. install) and loaded via shared_preload_libraries in the
# target cluster.  Run with:
#
#   make -C gpcontrib/gp_stats_collector/test installcheck
#
# pg_regress defaults to ./sql/<name>.sql and ./expected/<name>.out.

REGRESS = gpsc_pg_query_state

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp_stats_collector/test
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
