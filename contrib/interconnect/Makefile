# contrib/interconnect/Makefile
# Makefile.interconnect will be generate when src/Makefile.global generated
# Used to get $(enable_ic_proxy)
top_builddir = ../..
include $(top_builddir)/contrib/interconnect/Makefile.interconnect

MODULE_big = interconnect
PGFILEDESC = "interconnect - inter connection module"

EXTENSION = interconnect
EXTENSION_VERSION = 1.0
DATA = interconnect--$(EXTENSION_VERSION).sql

OBJS =  \
	$(WIN32RES) \
	ic_common.o \
	tcp/ic_tcp.o \
	udp/ic_udpifc.o \
	ic_modules.o 

ifeq ($(enable_ic_proxy), yes)
# servere
OBJS += proxy/ic_proxy_main.o
OBJS += proxy/ic_proxy_client.o
OBJS += proxy/ic_proxy_peer.o
OBJS += proxy/ic_proxy_router.o

# backend
OBJS += proxy/ic_proxy_backend.o

# utils
OBJS += proxy/ic_proxy_addr.o
OBJS += proxy/ic_proxy_key.o
OBJS += proxy/ic_proxy_packet.o
OBJS += proxy/ic_proxy_pkt_cache.o
OBJS += proxy/ic_proxy_iobuf.o
SHLIB_LINK += $(filter -luv, $(LIBS))
endif  # enable_ic_proxy

REGRESS = interconnect

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/interconnect
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif