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

# diskquota ships a standalone CMake project whose Makefile is a thin shim.
# Almost all of that CMake exists to discover PostgreSQL's paths and flags via
# pg_config (cmake/Gpdb.cmake), which meson already knows, so the extension is
# built natively here instead of shelling out to cmake. Only 7 source files.

diskquota_version = run_command('cat', files('VERSION'), check: true).stdout().strip()
diskquota_version_parts = diskquota_version.split('.')
diskquota_major = diskquota_version_parts[0]
diskquota_minor = diskquota_version_parts[1]
diskquota_patch = diskquota_version_parts[2]

# CMakeLists.txt: 1.0 keeps the bare name, everything else is suffixed.
if diskquota_major + '.' + diskquota_minor == '1.0'
  diskquota_binary_name = 'diskquota'
else
  diskquota_binary_name = 'diskquota-' + diskquota_major + '.' + diskquota_minor
endif

diskquota_c_args = [
  '-DDISKQUOTA_VERSION="' + diskquota_version + '"',
  '-DDISKQUOTA_MAJOR_VERSION=' + diskquota_major,
  '-DDISKQUOTA_MINOR_VERSION=' + diskquota_minor,
  '-DDISKQUOTA_PATCH_VERSION=' + diskquota_patch,
  '-DDISKQUOTA_BINARY_NAME="' + diskquota_binary_name + '"',
]

diskquota = shared_module(diskquota_binary_name,
  files(
    'src/diskquota.c',
    'src/diskquota_utility.c',
    'src/enforcement.c',
    'src/gp_activetable.c',
    'src/monitored_db.c',
    'src/quotamodel.c',
    'src/relation_cache.c',
  ),
  override_options: ['c_std=c99'],
  kwargs: contrib_mod_args + {
    'c_args': contrib_mod_args.get('c_args', []) + diskquota_c_args,
    'dependencies': contrib_mod_args['dependencies'] + [libpq],
  },
)
contrib_targets += diskquota

# install(FILES ${diskquota_DDL} DESTINATION "share/postgresql/extension/")
install_data(
  'control/ddl/diskquota.control',
  'control/ddl/diskquota--2.2.sql',
  'control/ddl/diskquota--2.3.sql',
  'control/ddl/diskquota--2.2--2.3.sql',
  'control/ddl/diskquota--2.3--2.2.sql',
  kwargs: contrib_data_args,
)
