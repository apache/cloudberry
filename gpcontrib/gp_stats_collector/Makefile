MODULE_big = gp_stats_collector
EXTENSION  = gp_stats_collector
DATA       = $(wildcard *--*.sql)
REGRESS    = gpsc_cursors gpsc_dist gpsc_select gpsc_utf8_trim gpsc_utility gpsc_guc_cache gpsc_uds gpsc_locale

PROTO_BASES = gpsc_plan gpsc_metrics gpsc_set_service
PROTO_OBJS  = $(patsubst %,src/protos/%.pb.o,$(PROTO_BASES))

C_OBJS   = $(patsubst %.c,%.o,$(wildcard src/*.c src/*/*.c))
CPP_OBJS = $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp))
OBJS     = $(C_OBJS) $(CPP_OBJS) $(PROTO_OBJS)

PG_CXXFLAGS += -Werror -Wall -Wno-unused-but-set-variable -std=c++17 -Isrc/protos -Isrc -Iinclude -DGPBUILD
SHLIB_LINK += -lprotobuf -lstdc++
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

src/protos/.done: $(wildcard protos/*.proto)
	@mkdir -p src/protos
	protoc -I /usr/include -I /usr/local/include -I . --cpp_out=src $^
	for f in src/protos/*.pb.cc; do mv "$$f" "$${f%.cc}.cpp"; done
	touch $@

src/protos/%.pb.cpp src/protos/%.pb.h: src/protos/.done ;
$(CPP_OBJS): src/protos/.done
