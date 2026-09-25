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

gp_inject_fault_args = contrib_mod_args + {
  'dependencies': contrib_mod_args['dependencies'] + [libpq],
}

gp_inject_fault = shared_module('gp_inject_fault',
  files(
    'gp_inject_fault.c',
  ),
  kwargs: gp_inject_fault_args,
)
contrib_targets += gp_inject_fault

install_data(
  'gp_inject_fault--1.0.sql',
  'gp_inject_fault.control',
  kwargs: contrib_data_args,
)
