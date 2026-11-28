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

install_data(
  '__init__.py',
  'compare_segment_guc.py',
  'database_segment_guc.py',
  'file_segment_guc.py',
  'guc_collection.py',
  'parse_guc_metadata.py',
  'segment_guc.py',
  install_dir: dir_bin / 'gpconfig_modules',
  install_mode: 'rwxr-xr-x',
)

# gpconfig refuses to put a GUC marked GUC_DISALLOW_IN_FILE into
# postgresql.conf, and learns which ones those are from a list the Makefile's
# `core` target writes at install time by scanning guc.c and guc_gp.c. Without
# it every gpconfig call warns that the file is missing and validates nothing.
# Generate it here with the same parser, into the same place under the prefix.
custom_target('gucs_disallowed_in_file.txt',
  output: 'gucs_disallowed_in_file.txt',
  depend_files: files(
    'parse_guc_metadata.py',
    '../../../src/backend/utils/misc/guc.c',
    '../../../src/backend/utils/misc/guc_gp.c',
  ),
  command: [python, '-c',
    'import sys; sys.path.insert(0, sys.argv[1]); '
    + 'from parse_guc_metadata import ParseGuc; '
    + 'ParseGuc().write_gucs_to_file(sys.argv[2])',
    meson.current_source_dir(), '@OUTPUT@'],
  build_by_default: true,
  install: true,
  # share/greenplum under the prefix, not datadir: the parser's
  # DESTINATION_DIR, and where gpconfig looks.
  install_dir: get_option('datadir') / 'greenplum',
)
