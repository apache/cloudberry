# src/test/modules/test_mcxt_peak_tracker/Makefile

MODULE_big = test_mcxt_peak_tracker
OBJS = \
	$(WIN32RES) \
	test_mcxt_peak_tracker.o
PGFILEDESC = "test_mcxt_peak_tracker - test context allocation accounting"

EXTENSION = test_mcxt_peak_tracker
DATA = test_mcxt_peak_tracker--1.0.sql

REGRESS = test_mcxt_peak_tracker

installcheck: install

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_mcxt_peak_tracker
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
