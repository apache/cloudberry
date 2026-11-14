#
# pg_hint_plan: Makefile
#
# Copyright (c) 2012-2023, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#

MODULES = pg_hint_plan
HINTPLANVER = 1.6.3

# CBDB: the ut-* tests (other than ut-fini) and ut-fdw are not adapted to
# Cloudberry yet.
REGRESS = init base_plan pg_hint_plan hints_anywhere plpgsql ut-fini \
	hint_table oldextversions
REGRESS_OPTS = --encoding=UTF8

EXTENSION = pg_hint_plan
DATA = \
	pg_hint_plan--1.3.0.sql \
	pg_hint_plan--1.3.0--1.3.1.sql \
	pg_hint_plan--1.3.1--1.3.2.sql \
	pg_hint_plan--1.3.2--1.3.3.sql \
	pg_hint_plan--1.3.3--1.3.4.sql \
	pg_hint_plan--1.3.5--1.3.6.sql \
	pg_hint_plan--1.3.4--1.3.5.sql \
	pg_hint_plan--1.3.6--1.3.7.sql \
	pg_hint_plan--1.3.7--1.3.8.sql \
	pg_hint_plan--1.3.8--1.3.9.sql \
	pg_hint_plan--1.3.9--1.3.10.sql \
	pg_hint_plan--1.3.10--1.3.11.sql \
	pg_hint_plan--1.3.11--1.4.sql \
	pg_hint_plan--1.4--1.4.1.sql \
	pg_hint_plan--1.4.1--1.4.2.sql \
	pg_hint_plan--1.4.2--1.4.3.sql \
	pg_hint_plan--1.4.3--1.4.4.sql \
	pg_hint_plan--1.4.4--1.4.5.sql \
	pg_hint_plan--1.4.5--1.5.sql \
	pg_hint_plan--1.5--1.5.1.sql \
	pg_hint_plan--1.5.1--1.5.2.sql \
	pg_hint_plan--1.5.2--1.5.3.sql \
	pg_hint_plan--1.5.3--1.5.4.sql \
	pg_hint_plan--1.5.4--1.6.0.sql \
	pg_hint_plan--1.6.0--1.6.1.sql \
	pg_hint_plan--1.6.1--1.6.2.sql \
	pg_hint_plan--1.6.2--1.6.3.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/pg_hint_plan
top_builddir = ../..
# CBDB: plpgsql.h is not installed in the in-tree build
PG_CPPFLAGS += -I$(top_srcdir)/src/pl/plpgsql/src/
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# pg_hint_plan.c includes pg_stat_statements.c
pg_hint_plan.o: pg_stat_statements.c
