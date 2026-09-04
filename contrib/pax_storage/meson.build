# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

#
# PAX ships a standalone CMake project driven from its Makefile. The source
# lists here mirror the groups in src/cpp/cmake/pax.cmake for the default
# configuration (USE_PAX_CATALOG=ON, USE_MANIFEST_API=OFF, VEC_BUILD off).

subdir('src/cpp/storage/proto')

# --- generated extension SQL ---------------------------------------------
# cmake/pax.cmake builds tools/gen_sql.c and pipes its stdout into
# pax-cdbinit--1.0.sql, writing into the source tree; capture it instead.
pax_gen_sql = executable('pax_gen_sql',
  files('tools/gen_sql.c'),
  include_directories: include_directories('src/cpp'),
  dependencies: [backend_code],
  build_by_default: false,
  install: false,
)

pax_cdbinit_sql = custom_target('pax-cdbinit--1.0.sql',
  output: 'pax-cdbinit--1.0.sql',
  command: [pax_gen_sql],
  capture: true,
  build_by_default: true,
  install: true,
  install_dir: dir_data / 'cdb_init.d',
)

# --- the module -----------------------------------------------------------
pax_sources = files(
  'src/cpp/access/pax_access_handle.cc',
  'src/cpp/access/pax_access_method_internal.cc',
  'src/cpp/access/pax_deleter.cc',
  'src/cpp/access/pax_dml_state.cc',
  'src/cpp/access/pax_inserter.cc',
  'src/cpp/access/pax_scanner.cc',
  'src/cpp/access/pax_table_cluster.cc',
  'src/cpp/access/pax_updater.cc',
  'src/cpp/access/pax_visimap.cc',
  'src/cpp/access/paxc_rel_options.cc',
  'src/cpp/catalog/pax_aux_table.cc',
  'src/cpp/catalog/pax_fastsequence.cc',
  'src/cpp/catalog/pax_manifest.cc',
  'src/cpp/catalog/pg_pax_tables.cc',
  'src/cpp/clustering/clustering.cc',
  'src/cpp/clustering/index_clustering.cc',
  'src/cpp/clustering/lexical_clustering.cc',
  'src/cpp/clustering/pax_clustering_reader.cc',
  'src/cpp/clustering/pax_clustering_writer.cc',
  'src/cpp/clustering/sorter_index.cc',
  'src/cpp/clustering/sorter_tuple.cc',
  'src/cpp/clustering/zorder_clustering.cc',
  'src/cpp/clustering/zorder_utils.cc',
  'src/cpp/comm/bitmap.cc',
  'src/cpp/comm/bloomfilter.cc',
  'src/cpp/comm/byte_buffer.cc',
  'src/cpp/comm/cbdb_wrappers.cc',
  'src/cpp/comm/fast_io.cc',
  'src/cpp/comm/guc.cc',
  'src/cpp/comm/pax_memory.cc',
  'src/cpp/comm/pax_resource.cc',
  'src/cpp/comm/paxc_wrappers.cc',
  'src/cpp/comm/vec_numeric.cc',
  'src/cpp/exceptions/CException.cc',
  'src/cpp/storage/columns/pax_column.cc',
  'src/cpp/storage/columns/pax_column_traits.cc',
  'src/cpp/storage/columns/pax_columns.cc',
  'src/cpp/storage/columns/pax_compress.cc',
  'src/cpp/storage/columns/pax_decoding.cc',
  'src/cpp/storage/columns/pax_delta_encoding.cc',
  'src/cpp/storage/columns/pax_dict_encoding.cc',
  'src/cpp/storage/columns/pax_encoding.cc',
  'src/cpp/storage/columns/pax_encoding_column.cc',
  'src/cpp/storage/columns/pax_encoding_non_fixed_column.cc',
  'src/cpp/storage/columns/pax_encoding_utils.cc',
  'src/cpp/storage/columns/pax_rlev2_decoding.cc',
  'src/cpp/storage/columns/pax_rlev2_encoding.cc',
  'src/cpp/storage/columns/pax_vec_bitpacked_column.cc',
  'src/cpp/storage/columns/pax_vec_bpchar_column.cc',
  'src/cpp/storage/columns/pax_vec_column.cc',
  'src/cpp/storage/columns/pax_vec_encoding_column.cc',
  'src/cpp/storage/columns/pax_vec_numeric_column.cc',
  'src/cpp/storage/file_system.cc',
  'src/cpp/storage/filter/pax_filter.cc',
  'src/cpp/storage/filter/pax_row_filter.cc',
  'src/cpp/storage/filter/pax_sparse_filter.cc',
  'src/cpp/storage/filter/pax_sparse_pg_path.cc',
  'src/cpp/storage/filter/pax_sparse_vec_path.cc',
  'src/cpp/storage/local_file_system.cc',
  'src/cpp/storage/micro_partition.cc',
  'src/cpp/storage/micro_partition_file_factory.cc',
  'src/cpp/storage/micro_partition_iterator.cc',
  'src/cpp/storage/micro_partition_metadata.cc',
  'src/cpp/storage/micro_partition_row_filter_reader.cc',
  'src/cpp/storage/micro_partition_stats.cc',
  'src/cpp/storage/micro_partition_stats_updater.cc',
  'src/cpp/storage/micro_partition_udf.cc',
  'src/cpp/storage/oper/pax_oper.cc',
  'src/cpp/storage/oper/pax_oper_udf.cc',
  'src/cpp/storage/oper/pax_stats.cc',
  'src/cpp/storage/orc/orc_dump_reader.cpp',
  'src/cpp/storage/orc/orc_format_reader.cc',
  'src/cpp/storage/orc/orc_group.cc',
  'src/cpp/storage/orc/orc_reader.cc',
  'src/cpp/storage/orc/orc_type.cc',
  'src/cpp/storage/orc/orc_vec_group.cc',
  'src/cpp/storage/orc/orc_writer.cc',
  'src/cpp/storage/pax.cc',
  'src/cpp/storage/pax_buffer.cc',
  'src/cpp/storage/pax_itemptr.cc',
  'src/cpp/storage/paxc_smgr.cc',
  'src/cpp/storage/proto/protobuf_stream.cc',
  'src/cpp/storage/strategy.cc',
  'src/cpp/storage/toast/pax_toast.cc',
  'src/cpp/storage/vec/arrow_wrapper.cc',
  'src/cpp/storage/vec/pax_porc_adpater.cc',
  'src/cpp/storage/vec/pax_porc_vec_adpater.cc',
  'src/cpp/storage/vec/pax_vec_adapter.cc',
  'src/cpp/storage/vec/pax_vec_comm.cc',
  'src/cpp/storage/vec/pax_vec_reader.cc',
  'src/cpp/storage/wal/pax_wal.cc',
  'src/cpp/storage/wal/paxc_desc.c',
  'src/cpp/storage/wal/paxc_wal.cc',

)

pax_deps = [zstd, zlib, protobuf]
if get_option('lz4').enabled()
  pax_deps += lz4
endif
if host_machine.system() == 'linux'
  # cmake/pax.cmake: list(APPEND pax_target_link_libs uring)
  pax_deps += dependency('liburing', required: true)
endif

pax_inc = include_directories(
  'src/cpp',
  'src/cpp/contrib/tabulate/include',
)

pax = shared_module('pax',
  pax_sources,
  pax_proto_sources,
  include_directories: pax_inc,
  override_options: ['cpp_std=c++17'],
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + pax_deps,
  },
)
contrib_targets += pax
