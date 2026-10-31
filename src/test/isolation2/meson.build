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
# Cloudberry's isolation2 harness: pg_isolation2_regress drives
# sql_isolation_testcase.py, which speaks to the cluster through PyGreSQL, and
# the suites load isolation2_regress.so and run three small libpq clients.
#
# The autoconf build never installs any of it -- `make -C src/test/isolation2
# installcheck` builds it on demand -- so nothing here is installed either. It
# is built by default because it is small and a harness that only builds when
# a test is run is one nobody notices breaking.

pg_isolation2_regress = executable('pg_isolation2_regress',
  pg_regress_c + files('isolation2_main.c'),
  c_args: pg_regress_cflags,
  include_directories: pg_regress_inc,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {'install': false},
)

# NAME = isolation2_regress, OBJS = isolation2_regress.o workfile_mgr_test.o
isolation2_regress_module = shared_module('isolation2_regress',
  files('isolation2_regress.c', 'workfile_mgr_test.c'),
  include_directories: include_directories('.', '../regress'),
  kwargs: pg_test_mod_args,
)

foreach p : [
  'extended_protocol_test',
  'test_parallel_retrieve_cursor_extended_query',
  'test_parallel_retrieve_cursor_extended_query_error',
]
  executable(p,
    files(p + '.c'),
    dependencies: [frontend_code, libpq],
    kwargs: default_bin_args + {'install': false},
  )
endforeach

# pg_isolation2_regress needs gpdiff.pl and its modules beside it, exactly as
# pg_regress does; the Makefile symlinks them over from ../regress. Copy them,
# so that the build tree is runnable on its own.
foreach f : ['atmsort.pm', 'explain.pm', 'gpdiff.pl', 'gpstringsubs.pl']
  configure_file(input: '../regress' / f, output: f, copy: true)
endforeach
configure_file(input: gptest_pm, output: 'GPTest.pm', copy: true)
