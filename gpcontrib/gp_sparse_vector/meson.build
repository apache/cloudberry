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

gp_svec_sources = files(
  'SparseData.c',
  'gp_sfv.c',
  'operators.c',
  'sparse_vector.c',
)

gp_svec = shared_module('gp_svec',
  gp_svec_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += gp_svec

install_data(
  'gp_sparse_vector--1.0.1.sql',
  'gp_sparse_vector--1.0.0--1.0.1.sql',
  'gp_sparse_vector.control',
  kwargs: contrib_data_args,
)
