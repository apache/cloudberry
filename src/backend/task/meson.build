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

# This directory's Makefile adds -Wno-implicit-fallthrough to CPPFLAGS. meson
# compiles the whole backend as one target, so the sources are split into their
# own static library to carry the flag, the same way src/backend/nodes does for
# its include path.
task_sources = files(
  'entry.c',
  'job_metadata.c',
  'misc.c',
  'pg_cron.c',
  'task_states.c',
)

task_lib = static_library('task',
  task_sources,
  c_args: ['-Wno-implicit-fallthrough'],
  dependencies: [backend_code],
  kwargs: internal_lib_args,
)
backend_link_with += task_lib
