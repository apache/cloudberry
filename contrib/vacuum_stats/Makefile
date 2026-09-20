# contrib/vacuum_stats/Makefile

MODULE_big = vacuum_stats
OBJS = vacuum_stats.o

EXTENSION = vacuum_stats
DATA = vacuum_stats--1.0.sql
PGFILEDESC = "vacuum_stats - per-relation and per-database vacuum statistics"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/vacuum_stats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check-tap:
	$(prove_check)

installcheck-tap:
	$(prove_installcheck)

.PHONY: check-tap installcheck-tap
