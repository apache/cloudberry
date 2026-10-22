# src/test/modules/parallel_customscan/Makefile

MODULE_big = parallel_customscan
OBJS = \
	$(WIN32RES) \
	parallel_customscan.o

EXTENSION = parallel_customscan
DATA = parallel_customscan--1.0.sql
PGFILEDESC = "parallel_customscan - exercise parallel CustomScan dispatch"

REGRESS = parallel_customscan

# Run against an existing cluster (gpdemo).  The cluster must have
# 'parallel_customscan' in shared_preload_libraries so segment backends
# and parallel workers have the CustomScan methods registered:
#   gpconfig -c shared_preload_libraries -v "'time_series,parallel_customscan'"
#   gpstop -ra

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/parallel_customscan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
