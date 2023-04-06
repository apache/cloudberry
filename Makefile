override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD
override CXXFLAGS = -fPIC -lstdc++ -lpthread -g3 -ggdb -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -std=c++14 -fPIC -Iinclude -Isrc/protos -Isrc -g -DGPBUILD
COMMON_CPP_FLAGS := -Isrc -Iinclude -Isrc/stat_statements_parser
PG_CXXFLAGS += $(COMMON_CPP_FLAGS)
SHLIB_LINK += -lprotobuf -lgrpc++

PROTOC = protoc
SRC_DIR = ./src
GEN_DIR = ./src/protos
PROTO_DIR = ./protos
PROTO_GEN_OBJECTS = $(GEN_DIR)/yagpcc_plan.pb.o $(GEN_DIR)/yagpcc_metrics.pb.o \
					$(GEN_DIR)/yagpcc_set_service.pb.o $(GEN_DIR)/yagpcc_set_service.grpc.pb.o

GRPC_CPP_PLUGIN 		:= grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH	?= `which $(GRPC_CPP_PLUGIN)`

$(GEN_DIR)/%.pb.cpp : $(PROTO_DIR)/%.proto
	sed -i 's/optional //g' $^
	sed -i 's/cloud\/mdb\/yagpcc\/api\/proto\/common\//\protos\//g' $^
	$(PROTOC) --cpp_out=$(SRC_DIR) $^
	mv $(GEN_DIR)/$*.pb.cc $(GEN_DIR)/$*.pb.cpp



$(GEN_DIR)/yagpcc_set_service.grpc.pb.cpp : $(PROTO_DIR)/yagpcc_set_service.proto
	$(PROTOC) --grpc_out=$(SRC_DIR) --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $^
	mv $(GEN_DIR)/yagpcc_set_service.grpc.pb.cc $(GEN_DIR)/yagpcc_set_service.grpc.pb.cpp

PG_STAT_DIR		:= $(SRC_DIR)/stat_statements_parser
PG_STAT_OBJS	:= $(PG_STAT_DIR)/pg_stat_statements_ya_parser.o

OBJS			:=	$(PG_STAT_OBJS)						\
					$(PROTO_GEN_OBJECTS)			 	\
					$(SRC_DIR)/ProcStats.o				\
					$(SRC_DIR)/SpillInfoWrapper.o		\
					$(SRC_DIR)/GrpcConnector.o			\
					$(SRC_DIR)/EventSender.o 			\
					$(SRC_DIR)/hook_wrappers.o		 	\
					$(SRC_DIR)/yagp_hooks_collector.o
EXTRA_CLEAN     := $(GEN_DIR)
DATA			:= $(wildcard sql/*--*.sql)
EXTENSION		:= yagp-hooks-collector
EXTVERSION		:= $(shell grep default_version $(EXTENSION).control | \
			   	sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
MODULE_big		:= yagp-hooks-collector
PG_CONFIG		:= pg_config
PGXS			:= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

gen: $(PROTO_GEN_OBJECTS)

.DEFAULT_GOAL := all
