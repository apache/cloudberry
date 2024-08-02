top_builddir = ../../../../

MODULE_big = gpmmon
OBJS = gpmmon.o gpmondb.o gpmon_agg.o  ../common/gpmonlib.o
SHLIB_LINK += -levent  -lapr-1 -laprutil-1 -lm
PG_CFLAGS += -Wno-error=vla -Wno-vla
PG_CPPFLAGS = -I$(libpq_srcdir) -I../include
SHLIB_LINK_INTERNAL = -Wl,-Bsymbolic -Wl,-Bstatic -Wl,-Bstatic $(libpq) -lpgcommon_shlib -Wl,-Bdynamic

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/perfmon/src/gpmmon
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif
