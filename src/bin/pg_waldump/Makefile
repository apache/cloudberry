# src/bin/pg_waldump/Makefile

PGFILEDESC = "pg_waldump - decode and display WAL"
PGAPPICON=win32

subdir = src/bin/pg_waldump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_pax), yes)
PAX_OBJS = paxc_desc.o
endif

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	compat.o \
	pg_waldump.o \
	rmgrdesc.o \
	$(PAX_OBJS) \
	xlogreader.o \
	xlogstats.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc*.c)))
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))


all: pg_waldump

pg_waldump: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

ifeq ($(enable_pax), yes)
paxc_desc.c: % : $(top_srcdir)/contrib/pax_storage/src/cpp/storage/wal/%
	rm -f $@ && $(LN_S) $< $@

paxc_desc.h: % : $(top_srcdir)/contrib/pax_storage/src/cpp/storage/wal/%
	rm -f $@ && $(LN_S) $< $@

rmgrdesc.o: paxc_desc.h

paxc_desc.o: paxc_desc.h
endif
xlogstats.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_waldump$(X) '$(DESTDIR)$(bindir)/pg_waldump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_waldump$(X)'

clean distclean maintainer-clean:
	rm -f pg_waldump$(X) $(OBJS) $(RMGRDESCSOURCES) xlogreader.c paxc_desc.c paxc_desc.h xlogstats.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
