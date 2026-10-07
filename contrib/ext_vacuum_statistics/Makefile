# contrib/ext_vacuum_statistics/Makefile

EXTENSION = ext_vacuum_statistics
MODULE_big = ext_vacuum_statistics
OBJS = vacuum_statistics.o
DATA = ext_vacuum_statistics--1.0.sql
PGFILEDESC = "ext_vacuum_statistics - convenience views for extended vacuum statistics"

TAP_TESTS = 1

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/ext_vacuum_statistics
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
