MODULE_big = reject_partition_fullscan
OBJS = reject_partition_fullscan.o

EXTENSION = reject_partition_fullscan
DATA = reject_partition_fullscan--1.0.sql

REGRESS = partition_fullscan_reject

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/reject_partition_fullscan
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
