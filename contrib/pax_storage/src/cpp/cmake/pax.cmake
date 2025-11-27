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

## generate_sql
add_executable(generate_sql_script_program "${CMAKE_CURRENT_SOURCE_DIR}/../../tools/gen_sql.c")
target_include_directories(generate_sql_script_program PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
add_custom_command(OUTPUT generate_sql_file
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/generate_sql_script_program > "${CMAKE_CURRENT_SOURCE_DIR}/../../pax-cdbinit--1.0.sql"
  DEPENDS generate_sql_script_program
  COMMENT "dynamically generate sql script file"
)
add_custom_target(create_sql_script DEPENDS generate_sql_script_program generate_sql_file)


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
    storage/columns/pax_vec_bitpacked_column.cc
    storage/columns/pax_vec_bpchar_column.cc
    storage/columns/pax_vec_column.cc
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
    storage/pax_itemptr.cc
    storage/proto/protobuf_stream.cc
    storage/pax.cc 
    storage/paxc_smgr.cc
    storage/toast/pax_toast.cc
    storage/strategy.cc
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
  clustering/pax_clustering_reader.cc
  clustering/pax_clustering_writer.cc
  clustering/zorder_utils.cc
)


set(pax_access_src
    access/paxc_rel_options.cc
    access/pax_access_handle.cc
    access/pax_access_method_internal.cc
    access/pax_deleter.cc
    access/pax_dml_state.cc
    access/pax_inserter.cc
    access/pax_table_cluster.cc
    access/pax_updater.cc
    access/pax_visimap.cc
    access/pax_scanner.cc)

set(pax_catalog_src
    catalog/pax_fastsequence.cc
    catalog/pax_manifest.cc
    )
if (USE_PAX_CATALOG)
  set(pax_catalog_src ${pax_catalog_src}
      catalog/pax_aux_table.cc
      catalog/pg_pax_tables.cc)
endif()

set(manifest_src
  manifest/manifest.c
  manifest/scan.c
  manifest/tuple.c
  manifest/manifest_wrapper.cc
)

if (USE_MANIFEST_API)
  set(pax_storage_src ${pax_storage_src} storage/micro_partition_iterator_manifest.cc)
  if (USE_PAX_CATALOG)
    set(pax_catalog_src ${pax_catalog_src} catalog/pax_manifest_impl.cc)
  else()
    # use manifest implementation
    set(pax_target_include ${pax_target_include} ${TOP_DIR}/dependency/yyjson/src)
    set(pax_catalog_src ${pax_catalog_src} ${manifest_src})
  endif()
else() # USE_MANIFEST_API
  set(pax_storage_src ${pax_storage_src} storage/micro_partition_iterator.cc)
endif()

set(pax_vec_src
  storage/vec/arrow_wrapper.cc
  storage/vec/pax_porc_adpater.cc
  storage/vec/pax_porc_vec_adpater.cc
  storage/vec/pax_vec_adapter.cc
  storage/vec/pax_vec_comm.cc
  storage/vec/pax_vec_reader.cc
)
if (VEC_BUILD)
set(pax_vec_src ${pax_vec_src}
  storage/vec_parallel_common.cc
  storage/vec_parallel_pax.cc
)
endif()

# add tabulate which used in the UDF
SET(INSTALL_TABULATE OFF)
add_subdirectory(contrib/tabulate)

#### pax.so
set(pax_target_src  ${PROTO_SRCS} ${pax_storage_src} ${pax_clustering_src} ${pax_exceptions_src}
  ${pax_access_src} ${pax_comm_src} ${pax_catalog_src} ${pax_vec_src})
set(pax_target_include ${pax_target_include} ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR} contrib/tabulate/include)
set(pax_target_link_libs ${pax_target_link_libs} protobuf zstd z postgres uring)
if (PAX_USE_LZ4)
  list(APPEND pax_target_link_libs lz4)
endif()
set(pax_target_link_directories ${PROJECT_SOURCE_DIR}/../../src/backend/)
set(pax_target_dependencies generate_protobuf create_sql_script)

add_library(pax SHARED ${pax_target_src})
set_target_properties(pax PROPERTIES OUTPUT_NAME pax)

if(USE_MANIFEST_API AND NOT USE_PAX_CATALOG)
  set(pax_target_link_libs ${pax_target_link_libs} yyjson)
endif()

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

target_include_directories(pax PUBLIC ${pax_target_include})
target_link_directories(pax PUBLIC ${pax_target_link_directories})
target_link_libraries(pax PRIVATE ${pax_target_link_libs})
set_target_properties(pax PROPERTIES
  BUILD_RPATH_USE_ORIGIN ON
  BUILD_WITH_INSTALL_RPATH ON
  INSTALL_RPATH "$ORIGIN:$ORIGIN/.."
  LINK_FLAGS "-Wl,--enable-new-dtags"
)

add_dependencies(pax ${pax_target_dependencies})
add_custom_command(TARGET pax POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E
                copy_if_different $<TARGET_FILE:pax> ${CMAKE_CURRENT_SOURCE_DIR}/../../pax.so)

if (BUILD_GTEST)
  add_subdirectory(contrib/googletest)
  ADD_DEFINITIONS(-DRUN_GTEST)
  file(GLOB test_case_sources
    pax_gtest_helper.cc
    pax_gtest.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/*/*_test.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/*/*/*_test.cc)

  add_executable(test_main ${pax_target_src} ${test_case_sources})
  add_dependencies(test_main ${pax_target_dependencies} gtest gmock)
  target_include_directories(test_main PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR} ${gtest_SOURCE_DIR}/include contrib/cpp-stub/src/ contrib/cpp-stub/src_linux/)

  target_link_directories(test_main PRIVATE ${pax_target_link_directories})
  target_link_libraries(test_main PRIVATE ${pax_target_link_libs} gtest gmock postgres)
endif(BUILD_GTEST)

if(BUILD_GBENCH)
  add_subdirectory(contrib/googlebench)
  ADD_DEFINITIONS(-DRUN_GBENCH)
  file(GLOB bench_sources
      pax_gbench.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/*/*_bench.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/*/*/*_bench.cc)

    add_executable(bench_main ${pax_target_src} ${bench_sources})
    add_dependencies(bench_main ${pax_target_dependencies} gtest gmock)
    target_include_directories(bench_main PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR} contrib/googlebench/include contrib/cpp-stub/src/ contrib/cpp-stub/src_linux/)
    link_directories(contrib/googlebench/src)
    target_link_directories(bench_main PUBLIC ${pax_target_link_directories})
    target_link_libraries(bench_main PUBLIC ${pax_target_link_libs} gtest gmock benchmark postgres)
    if (VEC_BUILD)
      target_link_libraries(bench_main PRIVATE arrow arrow_dataset)
    endif(VEC_BUILD)
endif(BUILD_GBENCH)

if (BUILD_TOOLS) 
  link_directories($ENV{GPHOME}/lib)

  add_executable(pax_dump storage/tools/pax_dump.cpp storage/orc/orc_dump_reader.cpp)
  target_include_directories(pax_dump PUBLIC ${pax_target_include} ${CMAKE_CURRENT_SOURCE_DIR})
  add_dependencies(pax_dump ${pax_target_dependencies})
  target_link_libraries(pax_dump PUBLIC pax protobuf)
endif(BUILD_TOOLS)

# no need install dynamic libraray into `/lib/postgresql/`
# Makefile will do that
