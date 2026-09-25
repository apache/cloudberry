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

pg_alterckey_sources = files('pg_alterckey.c')

if host_system == 'windows'
  pg_alterckey_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_alterckey',
    '--FILEDESC', 'pg_alterckey - alter the cluster key',])
endif

bin_targets += executable('pg_alterckey',
  pg_alterckey_sources,
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
