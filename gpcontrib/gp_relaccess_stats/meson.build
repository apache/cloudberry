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

gp_relaccess_stats = shared_module('gp_relaccess_stats',
  files('src/gp_relaccess_stats.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_relaccess_stats

# The Makefile installs $(wildcard sql/*--*.sql); spelled out here, as the
# rest of this port does, so a new upgrade script is a visible change.
install_data(
  'gp_relaccess_stats.control',
  'sql/gp_relaccess_stats--1.0--1.1.sql',
  'sql/gp_relaccess_stats--1.1.sql',
  kwargs: contrib_data_args,
)
