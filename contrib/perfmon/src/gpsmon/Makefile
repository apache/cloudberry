top_builddir = ../../../../

PG_CPPFLAGS = -I$(libpq_srcdir) -I../include -I. -Wno-error=vla -Wno-vla
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/perfmon/src/gpsmon
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif

PLATFORM  = $(shell uname -s)
ifeq ($(PLATFORM), darwin)
GPSMON_LIBS=-framework CoreServices -framework IOKit
SIGTEST_LIBS=-framework CoreServices -framework IOKit
endif

LDLIBS = $(LDFLAGS) $(LIBS)

.PHONY: gpsmon
GPSMON_OBJS=gpsmon.o ../common/gpmonlib.o
GPSMON_LIBS+= -levent  -lapr-1 -laprutil-1 -lm -lsigar
SIGTEST_LIBS+= -lsigar
gpsmon: $(GPSMON_OBJS)
	$(CC) -o gpsmon $(CFLAGS) $(GPSMON_OBJS) $(LDLIBS) $(GPSMON_LIBS)
all: gpsmon

sigartest: $(SIGTEST_OBJS)
	$(CC) -o sigartest $(CFLAGS) $(SIGTEST_OBJS) $(LDLIBS) $(GPSMON_LIBS)

clean distclean:
	rm -f *.o gpsmon

install: gpsmon
	$(INSTALL_SCRIPT) gpsmon '$(DESTDIR)$(bindir)'
