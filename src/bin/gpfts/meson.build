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

# etcd.c is shared with the backend. The Makefile symlinks it into this
# directory; meson references it where it lives.
bin_targets += executable('gpfts',
  files(
    'fts.c',
    'fts_etcd.c',
    'ftsprobe.c',
    '../../backend/utils/etcd_lib/etcd.c',
  ),
  include_directories: include_directories('.'),
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)

# Failover helper scripts, installed under $bindir/failover
install_data(
  'failover/docker_master_check_back.sh',
  'failover/docker_segment_check_back.sh',
  'failover/docker_standby_check_back.sh',
  'failover/master_check_back.sh',
  'failover/segment_all_down.sh',
  'failover/segment_check_back.sh',
  'failover/standby_check_back.sh',
  install_dir: dir_bin / 'failover',
  install_mode: 'rwxr-xr-x',
)

install_data(
  'config/cbdb_etcd_default.conf',
  install_dir: dir_bin / 'config',
)
