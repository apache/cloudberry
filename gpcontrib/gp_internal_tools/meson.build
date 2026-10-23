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

gp_ao_co_diagnostics = shared_module('gp_ao_co_diagnostics',
  files('gp_ao_co_diagnostics.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_ao_co_diagnostics

gp_workfile_mgr = shared_module('gp_workfile_mgr',
  files('gp_workfile_mgr.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_workfile_mgr

gp_session_state_memory_stats = shared_module('gp_session_state_memory_stats',
  files('gp_session_state_memory_stats.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_session_state_memory_stats

gp_instrument_shmem = shared_module('gp_instrument_shmem',
  files('gp_instrument_shmem.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += gp_instrument_shmem

install_data(
  'gp_internal_tools--1.0.0.sql',
  'gp_internal_tools.control',
  'uninstall_gp_session_state.sql',
  kwargs: contrib_data_args,
)
