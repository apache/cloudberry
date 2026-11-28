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

# The regression tests' way into anser's C API from SQL. The Makefile installs
# it only from installcheck, as a test tool, so it is built here and not
# installed.
anser_test = shared_module('anser_test',
  files('anser_test.c'),
  include_directories: include_directories('../include'),
  kwargs: pg_test_mod_args,
)
test_install_libs += anser_test
test_install_data += files(
  'anser_test--1.0.sql',
  'anser_test.control',
)
