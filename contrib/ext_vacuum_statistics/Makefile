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

# Cloudberry: the test of the cluster-wide views needs a running cluster
# with the module in shared_preload_libraries of every instance, which the
# temporary instances of "make check" are not, so it has a target of its
# own, run against such a cluster.
.PHONY: installcheck-cluster

installcheck-cluster:
	$(pg_regress_installcheck) --dbname=contrib_regression_vacstat gp_vacuum_statistics
