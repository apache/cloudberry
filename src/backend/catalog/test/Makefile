top_builddir = ../../../..
subdir = src/backend/catalog


include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/Makefile.mock

TARGETS += storage_tablespace
TARGETS += storage_pending_deletes
TARGETS += storage_pending_deletes_redo

include $(top_builddir)/src/backend/mock.mk

storage_tablespace.t: $(top_srcdir)/src/backend/catalog/storage_tablespace.o
	make -C $(top_srcdir)/src/backend/catalog/ && \
	$(CC) $(CFLAGS) $(LDFLAGS) $(CMOCKERY_OBJS) $(CPPFLAGS) \
		$(top_srcdir)/src/backend/catalog/storage_tablespace.o \
		storage_tablespace_test.c \
		-o storage_tablespace.t

storage_pending_deletes.t: \
	$(top_builddir)/src/backend/catalog/storage_pending_deletes.o

storage_pending_deletes_redo.t: \
	$(top_builddir)/src/backend/catalog/storage_pending_deletes_redo.o \
	$(top_builddir)/src/backend/catalog/storage_pending_deletes.o
