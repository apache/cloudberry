cmake_minimum_required (VERSION 3.11.0)

# protobuf

set(protobuf_files
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/micro_partition_stats.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/pax.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/orc_proto.proto
)


set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto)
set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto)

## we generate these files in the cmake -B build phase.
## when we start the make phase, all dependent files exist, and we can compile with multiple threads.
FOREACH(FIL ${protobuf_files})
    GET_FILENAME_COMPONENT(FIL_WE ${FIL} NAME_WE)
    string(REGEX REPLACE ".+/(.+)\\..*" "\\1" FILE_NAME ${FIL})
    string(REGEX REPLACE "(.+)\\${FILE_NAME}.*" "\\1" FILE_PATH ${FIL})

    set(PROTO_SRCS ${PROTO_SRCS} "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.cc")
    set(PROTO_HDRS ${PROTO_SRCS} "${PROTO_OUTPUT_DIR}/${FIL_WE}.pb.h")

    EXECUTE_PROCESS(
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I ${PROTO_DIR} --cpp_out=${PROTO_OUTPUT_DIR} ${FIL}
    )
ENDFOREACH()
add_custom_target(generate_protobuf DEPENDS ${PROTO_SRCS} ${PROTO_HDRS})

link_directories($ENV{GPHOME}/lib)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
## build pax_format.so
include(pax_format)
## build pax.so
include(pax)
