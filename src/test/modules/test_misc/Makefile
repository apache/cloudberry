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

# Keep the native VACUUM work coverage enabled in autoconf-based CI builds.
VACUUM_WORK_TESTS = t/021_heap_page_counters.pl \
	t/022_ao_hidden_tuples.pl t/023_dead_pages.pl t/024_ao_dead_pages.pl \
	t/027_freeze_age_vacuum_count.pl t/028_vacuum_work_core.pl \
	t/029_vacuum_resource_verbose.pl t/030_vacuum_work_reset.pl \
	t/031_vacuum_index_work.pl

.PHONY: installcheck-vacuum-work
installcheck-vacuum-work:
	$(MAKE) installcheck PROVE_TESTS="$(VACUUM_WORK_TESTS)"
