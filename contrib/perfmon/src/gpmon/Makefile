top_builddir = ../../../../

MODULE_big = gpmon
OBJS = gpmon.o
PG_CPPFLAGS = -I../include

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/perfmon/src/gpmmon
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif
