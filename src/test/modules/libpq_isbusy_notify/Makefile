# src/test/modules/libpq_isbusy_notify/Makefile

PGFILEDESC = "libpq_isbusy_notify - test program for PQisBusy and queued notifications"
PGAPPICON = win32

PROGRAM = libpq_isbusy_notify
OBJS = $(WIN32RES) libpq_isbusy_notify.o

NO_INSTALL = 1

# libpq-int.h: the test builds a connection state by hand rather than reading
# one off a socket, which is the only way to reach the case without a server.
PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL += $(libpq_pgport)

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/libpq_isbusy_notify
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
