# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

libpq_isbusy_notify_sources = files(
  'libpq_isbusy_notify.c',
)

if host_system == 'windows'
  libpq_isbusy_notify_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'libpq_isbusy_notify',
    '--FILEDESC', 'libpq_isbusy_notify - test program for PQisBusy and queued notifications',])
endif

libpq_isbusy_notify = executable('libpq_isbusy_notify',
  libpq_isbusy_notify_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += libpq_isbusy_notify

tests += {
  'name': 'libpq_isbusy_notify',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_isbusy_notify.pl',
    ],
    'deps': [libpq_isbusy_notify],
  },
}
