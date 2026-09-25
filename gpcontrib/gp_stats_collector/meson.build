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

# protoc's proto_path must be the extension root (the .proto files import each
# other as "protos/xxx.proto"), so pass both roots down to protos/meson.build.
gpsc_root_src = meson.current_source_dir()
gpsc_root_build = meson.current_build_dir()

subdir('protos')

# PG_CXXFLAGS += -Werror -Wall -Wno-unused-but-set-variable -std=c++17
#                -Isrc/protos -Isrc -Iinclude -DGPBUILD
gpsc_cpp_args = [
  '-Wall',
  '-Wno-unused-but-set-variable',
  '-DGPBUILD',
]

gp_stats_collector = shared_module('gp_stats_collector',
  files(
    'src/gp_stats_collector.c',
    'src/stat_statements_parser/pg_stat_statements_parser.c',
    'src/Config.cpp',
    'src/EventSender.cpp',
    'src/GpscStat.cpp',
    'src/PgUtils.cpp',
    'src/ProcStats.cpp',
    'src/ProtoUtils.cpp',
    'src/UDSConnector.cpp',
    'src/hook_wrappers.cpp',
    'src/log/LogOps.cpp',
    'src/log/LogSchema.cpp',
    'src/memory/gpdbwrappers.cpp',
  ),
  gpsc_proto_sources,
  include_directories: include_directories('.', 'src', 'protos'),
  override_options: ['cpp_std=c++17'],
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [protobuf],
    'cpp_args': contrib_mod_args.get('cpp_args', []) + gpsc_cpp_args,
  },
)
contrib_targets += gp_stats_collector

install_data(
  'gp_stats_collector--1.0.sql',
  'gp_stats_collector--1.0--1.1.sql',
  'gp_stats_collector--1.1.sql',
  'gp_stats_collector.control',
  kwargs: contrib_data_args,
)
