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

# The test prints __FILE__ with each result, and its expected output has the
# path make gives the compiler, relative to this directory:
# integration_tests/planner_integration_tests.c. meson compiles from the build
# root, through the path from there to the source tree, so map that back.
test_planner_src = run_command(python, '-c',
  'import os, sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))',
  meson.current_source_dir(), meson.build_root(),
  check: true).stdout().strip()

test_planner = shared_module('test_planner',
  files(
    'test_planner.c',
    'src/planner_test_helpers.c',
    'src/assertions.c',
    'integration_tests/planner_integration_tests.c',
  ),
  # integration_tests/ includes "src/assertions.h": the module's own
  # directory, which make reaches through -I.
  include_directories: include_directories('.'),
  c_args: cc.get_supported_arguments(
    '-fmacro-prefix-map=@0@/='.format(test_planner_src)),
  kwargs: pg_test_mod_args,
)
test_install_libs += test_planner

test_install_data += files(
  'test_planner.control',
  'test_planner--1.0.sql',
)
