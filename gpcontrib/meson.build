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

# Extensions built unconditionally, matching the base recurse_targets list in
# gpcontrib/Makefile. The flag-gated ones (gpcloud, gpmapreduce, orafce,
# diskquota, zstd, gp_stats_collector, pxf_fdw and the debug set) are not
# wired up yet.
subdir('gp_distribution_policy')
subdir('gp_exttable_fdw')
subdir('gp_internal_tools')
subdir('gp_legacy_string_agg')
subdir('gp_relsizes_stats')
subdir('gp_sparse_vector')
subdir('gp_toolkit')
subdir('pg_hint_plan')

# --enable-pxf (default yes); requires libcurl
if get_option('pxf').enabled()
  subdir('pxf_fdw')
endif

# --enable-debug-extensions (default yes)
if get_option('debug_extensions')
  subdir('gp_debug_numsegments')
  subdir('gp_inject_fault')
  subdir('gp_replica_check')
  subdir('reject_partition_fullscan')
endif

# --with-zstd
if zstd.found()
  subdir('zstd')
endif

# --enable-orafce
if get_option('orafce').enabled()
  subdir('orafce')
endif

# --enable-mapreduce; requires libyaml and perl
if get_option('mapreduce').enabled()
  subdir('gpmapreduce')
endif

# --with-gp-stats-collector; requires protobuf and protoc
if get_option('gp_stats_collector').enabled()
  subdir('gp_stats_collector')
endif

# --enable-gpcloud; requires libcurl, libxml2, openssl
if get_option('gpcloud').enabled()
  subdir('gpcloud')
endif

# --with-diskquota
if get_option('diskquota').enabled()
  subdir('diskquota')
endif
