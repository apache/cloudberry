EXTENSION = gp_interconnect_stats
MODULES = gp_interconnect_stats

EXTENSION_VERSION = 1.0


DATA = gp_interconnect_stats--1.0.sql

OBJS = gp_interconnect_stats.o

MODULE_big = gp_interconnect_stats


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp_interconnect_stats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
