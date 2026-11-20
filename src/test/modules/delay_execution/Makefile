# src/test/modules/delay_execution/Makefile

PGFILEDESC = "delay_execution - allow delay between parsing and execution"

MODULE_big = delay_execution
OBJS = \
	$(WIN32RES) \
	delay_execution.o

# Cloudberry: partition-removal-1 is not run.  It tests ALTER TABLE ...
# DETACH PARTITION ... CONCURRENTLY, which Cloudberry does not support.
ISOLATION = partition-addition

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/delay_execution
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
