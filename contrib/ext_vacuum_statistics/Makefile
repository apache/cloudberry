# contrib/ext_vacuum_statistics/Makefile

EXTENSION = ext_vacuum_statistics
MODULE_big = ext_vacuum_statistics
OBJS = vacuum_statistics.o
DATA = ext_vacuum_statistics--1.0.sql
PGFILEDESC = "ext_vacuum_statistics - convenience views for extended vacuum statistics"

ISOLATION = vacuum-extending-in-repetable-read
ISOLATION_OPTS = --temp-config=$(top_srcdir)/contrib/ext_vacuum_statistics/ext_vacuum_statistics.conf
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
.PHONY: installcheck-cluster installcheck-cluster-isolation2

installcheck-cluster:
	$(pg_regress_installcheck) --dbname=contrib_regression_vacstat gp_vacuum_statistics gp_vacuum_statistics_ao gp_vacuum_statistics_gc

# Concurrent checks at the segment COMMIT PREPARED boundary need isolation2.
installcheck-cluster-isolation2:
	$(MAKE) -C $(top_builddir)/src/test/isolation2 install
	cd $(top_builddir)/src/test/isolation2 && ./pg_isolation2_regress \
	  --inputdir=$(abspath $(srcdir))/isolation2 \
	  --outputdir=$(CURDIR)/output_isolation2 --bindir='$(bindir)' \
	  --dbname=contrib_vacstat_isolation2 vacuum_statistics_drop
