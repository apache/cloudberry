# src/test/modules/test_misc/Makefile

MODULES = test_custom_stats

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_misc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Native maintenance statistics coverage for the ic-tap CI job.
.PHONY: installcheck-vacuum
installcheck-vacuum: PROVE_TESTS = t/016_index_vacuum_time.pl t/017_vacuum_failsafe.pl t/018_vacuum_interrupts.pl t/021_vacuum_vm_pages.pl
installcheck-vacuum: installcheck
