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

backend_sources += files(
  'resgroup.c',
  'resgroup_helper.c',
)

# cgroup support is Linux-only; other platforms get the dummy implementation.
# Mirrors the ifeq ($(PORTNAME),linux) split in this directory's Makefile.
if host_machine.system() == 'linux'
  backend_sources += files(
    'cgroup.c',
    'cgroup-ops-linux-v1.c',
    'cgroup-ops-linux-v2.c',
    'cgroup_io_limit.c',
  )

  io_limit_scanner = custom_target('io_limit_scanner',
    input: 'io_limit_scanner.l',
    output: 'io_limit_scanner.c',
    command: flex_cmd,
  )
  generated_sources += io_limit_scanner
  backend_sources += io_limit_scanner

  io_limit_gram = custom_target('io_limit_gram',
    input: 'io_limit_gram.y',
    kwargs: bison_kw,
  )
  generated_sources += io_limit_gram.to_list()
  backend_sources += io_limit_gram
else
  backend_sources += files('cgroup-ops-dummy.c')
endif
