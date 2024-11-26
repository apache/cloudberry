#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for libpq subsystem (backend half of libpq interface)
#
# IDENTIFICATION
#    src/backend/libpq/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/libpq
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS += -I$(libpq_srcdir) -I$(top_builddir)/src/port

# be-fsstubs is here for historical reasons, probably belongs elsewhere

OBJS = \
	auth-scram.o \
	auth.o \
	be-fsstubs.o \
	be-secure-common.o \
	be-secure.o \
	crypt.o \
	hba.o \
	ifaddr.o \
	pqcomm.o \
	pqformat.o \
	pqmq.o \
	pqsignal.o

ifeq ($(with_ssl),openssl)
OBJS += be-secure-openssl.o
endif

ifeq ($(with_gssapi),yes)
OBJS += be-gssapi-common.o be-secure-gssapi.o
endif

# Cloudberry objects follow
OBJS += fe-protocol3.o fe-connect.o \
        fe-exec.o pqexpbuffer.o fe-auth.o fe-misc.o fe-secure.o \
        fe-auth-scram.o fe-trace.o \
        $(filter getpeereid.o, $(LIBOBJS))

# Cloudberry OpenSSL objects follow
ifeq ($(with_ssl),openssl)
OBJS += fe-secure-common.o fe-secure-openssl.o
endif

# Cloudberry GSSAPI objects follow
ifeq ($(with_gssapi),yes)
OBJS += fe-gssapi-common.o fe-secure-gssapi.o
endif

fe-protocol3.c fe-connect.c fe-exec.c pqexpbuffer.c fe-auth.c fe-auth-scram.c fe-misc.c fe-protocol2.c fe-secure.c fe-secure-openssl.c fe-secure-common.c fe-secure-gssapi.c fe-gssapi-common.c fe-trace.c: % : $(top_srcdir)/src/interfaces/libpq/%
	rm -f $@ && $(LN_S) $< .

getpeereid.c: % : $(top_srcdir)/src/port/%
	rm -f $@ && $(LN_S) $< .

fe-connect.o: fe-connect.c $(top_builddir)/src/port/pg_config_paths.h

$(top_builddir)/src/port/pg_config_paths.h:
	$(MAKE) -C $(top_builddir)/src/port pg_config_paths.h

clean distclean: clean-symlinks

clean-symlinks:
	rm -f fe-protocol3.c fe-connect.c fe-exec.c pqexpbuffer.c fe-auth.c fe-auth-scram.c fe-misc.c fe-protocol2.c fe-secure.c fe-secure-openssl.c fe-secure-common.c fe-secure-gssapi.c fe-gssapi-common.c fe-trace.c
	rm -f getpeereid.c

include $(top_srcdir)/src/backend/common.mk
