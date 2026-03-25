MODULE_big = gp_stats_collector
EXTENSION  = gp_stats_collector
DATA       = $(wildcard *--*.sql)
REGRESS    = gpsc_cursors gpsc_dist gpsc_select gpsc_utf8_trim gpsc_utility gpsc_guc_cache gpsc_uds gpsc_locale

PROTO_BASES = gpsc_plan gpsc_metrics gpsc_set_service
PROTO_OBJS  = $(patsubst %,src/protos/%.pb.o,$(PROTO_BASES))

C_OBJS   = $(patsubst %.c,%.o,$(wildcard src/*.c src/*/*.c))
CPP_OBJS = $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp))
OBJS     = $(C_OBJS) $(CPP_OBJS) $(PROTO_OBJS)

override CXXFLAGS = -Werror -fPIC -g3 -Wall -Wpointer-arith -Wendif-labels \
	-Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv \
	-Wno-unused-but-set-variable -Wno-address -Wno-format-truncation \
	-Wno-stringop-truncation -g -ggdb -std=c++17 -Iinclude -Isrc/protos -Isrc -DGPBUILD

PG_CXXFLAGS += -Isrc -Iinclude
SHLIB_LINK += -lprotobuf -lpthread -lstdc++
EXTRA_CLEAN = src/protos

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp_stats_collector
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

src/protos/%.pb.cpp src/protos/%.pb.h: protos/%.proto
	@mkdir -p src/protos
	protoc -I /usr/include -I /usr/local/include -I . --cpp_out=src $^
	mv src/protos/$*.pb.cc src/protos/$*.pb.cpp

$(CPP_OBJS): src/protos/gpsc_metrics.pb.h src/protos/gpsc_plan.pb.h src/protos/gpsc_set_service.pb.h
src/protos/gpsc_set_service.pb.o: src/protos/gpsc_metrics.pb.h
