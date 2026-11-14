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

gp_replica_check = shared_module('gp_replica_check',
  files('gp_replica_check.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_replica_check

install_data(
  'gp_replica_check--0.0.1.sql',
  'gp_replica_check.control',
  kwargs: contrib_data_args,
)

# SCRIPTS = gp_replica_check.py
install_data('gp_replica_check.py', install_dir: dir_bin)
