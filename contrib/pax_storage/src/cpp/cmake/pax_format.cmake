# paxformat.so
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(pax_comm_src
    comm/bitmap.cc
    comm/bloomfilter.cc
    comm/byte_buffer.cc
    comm/fast_io.cc
    comm/guc.cc
    comm/paxc_wrappers.cc
    comm/pax_memory.cc
    comm/pax_resource.cc
    comm/cbdb_wrappers.cc
    comm/vec_numeric.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/columns/pax_column_traits.cc
    storage/columns/pax_column.cc
    storage/columns/pax_compress.cc
    storage/columns/pax_columns.cc
    storage/columns/pax_encoding_utils.cc
    storage/columns/pax_encoding_non_fixed_column.cc
    storage/columns/pax_encoding_column.cc
    storage/columns/pax_dict_encoding.cc
    storage/columns/pax_decoding.cc
    storage/columns/pax_encoding.cc
    storage/columns/pax_delta_encoding.cc
    storage/columns/pax_rlev2_decoding.cc
    storage/columns/pax_rlev2_encoding.cc
    storage/columns/pax_vec_column.cc
    storage/columns/pax_vec_bitpacked_column.cc
    storage/columns/pax_vec_bpchar_column.cc
    storage/columns/pax_vec_encoding_column.cc
    storage/columns/pax_vec_numeric_column.cc
    storage/oper/pax_oper_udf.cc
    storage/filter/pax_filter.cc
    storage/filter/pax_row_filter.cc
    storage/filter/pax_sparse_filter.cc
    storage/filter/pax_sparse_pg_path.cc
    storage/filter/pax_sparse_vec_path.cc
    storage/oper/pax_oper.cc
    storage/oper/pax_stats.cc
    storage/file_system.cc
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/micro_partition_file_factory.cc
    storage/micro_partition_metadata.cc
    storage/micro_partition_row_filter_reader.cc
    storage/micro_partition_stats.cc
    storage/micro_partition_stats_updater.cc
    storage/micro_partition_udf.cc
    storage/orc/orc_dump_reader.cpp
    storage/orc/orc_format_reader.cc
    storage/orc/orc_group.cc
    storage/orc/orc_vec_group.cc
    storage/orc/orc_reader.cc
    storage/orc/orc_type.cc
    storage/orc/orc_writer.cc
    storage/pax_buffer.cc
    storage/proto/protobuf_stream.cc
    storage/toast/pax_toast.cc
    storage/wal/pax_wal.cc
    storage/wal/paxc_desc.c
    storage/wal/paxc_wal.cc
   )

   set(pax_clustering_src
   clustering/clustering.cc
   clustering/sorter_tuple.cc
   clustering/sorter_index.cc
   clustering/zorder_clustering.cc
   clustering/index_clustering.cc
   clustering/lexical_clustering.cc
   clustering/zorder_utils.cc
 )
 
set(pax_vec_src
  storage/vec/arrow_wrapper.cc
  storage/vec/pax_porc_adpater.cc
  storage/vec/pax_porc_vec_adpater.cc
  storage/vec/pax_vec_adapter.cc
  storage/vec/pax_vec_comm.cc
  storage/vec/pax_vec_reader.cc
)

if(VEC_BUILD)
set(pax_vec_src ${pax_vec_src}
  storage/vec_parallel_common.cc
   )

endif()

set(pax_target_include ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR} contrib/tabulate/include)
set(pax_target_link_libs uuid protobuf zstd z uring)
if (PAX_USE_LZ4)
  list(APPEND pax_target_link_libs lz4)
endif()
set(pax_target_link_directories ${PROJECT_SOURCE_DIR}/../../src/backend/)

# vec build
if (VEC_BUILD)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GLIB REQUIRED glib-2.0)
  set(pax_target_include
      ${pax_target_include}
      ${VEC_HOME}/src/include # for utils/tuptable_vec.h
      ${INSTALL_HOME}/include  # for arrow-glib/arrow-glib.h and otehr arrow interface
      ${GLIB_INCLUDE_DIRS} # for glib-object.h
  )
  set(pax_target_link_directories
      ${pax_target_link_directories}
      ${INSTALL_HOME}/lib)
  set(pax_target_link_libs
      ${pax_target_link_libs}
      arrow arrow_dataset)
endif(VEC_BUILD)

add_library(paxformat SHARED ${PROTO_SRCS} ${pax_storage_src} ${pax_clustering_src} ${pax_exceptions_src} ${pax_comm_src} ${pax_vec_src})
target_include_directories(paxformat PUBLIC ${pax_target_include})
target_link_directories(paxformat PUBLIC ${pax_target_link_directories})
target_link_libraries(paxformat PRIVATE ${pax_target_link_libs})
   
set_target_properties(paxformat PROPERTIES
  OUTPUT_NAME paxformat)
add_dependencies(paxformat generate_protobuf)

# export headers
set(PAX_COMM_HEADERS
  comm/bitmap.h
  comm/cbdb_api.h
  comm/log.h
  comm/cbdb_wrappers.h
  comm/pax_rel.h
  comm/pax_memory.h
  comm/guc.h
)

set(PAX_EXCEPTION_HEADERS
  exceptions/CException.h
)

set(PAX_CLUSTERING_HEADERS
  clustering/clustering.h
  clustering/clustering_reader.h
  clustering/clustering_writer.h
  clustering/index_clustering.h
  clustering/lexical_clustering.h
  clustering/zorder_clustering.h
  clustering/zorder_utils.h
)

# TODO(gongxun):
# We should explicitly specify the headers
# that need to be exported, and use the syntax of
# install(FILES,...) to install the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/storage
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax
  FILES_MATCHING
  PATTERN "*.h"
)

install(FILES ${PAX_COMM_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/comm
)

install(FILES ${PAX_EXCEPTION_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/exceptions
)

install(FILES ${PAX_CLUSTERING_HEADERS}
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/clustering
)

## install dynamic libraray
install(TARGETS paxformat
  LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

## test whether libpaxformat.so can be linked normally
add_executable(paxformat_test paxformat_test.cc)
target_include_directories(paxformat_test PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR})
add_dependencies(paxformat_test paxformat)
target_link_libraries(paxformat_test PRIVATE paxformat postgres)
