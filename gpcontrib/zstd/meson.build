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

# MODULE_big = gp_zstd_compression (note: differs from the directory name)
gp_zstd_compression = shared_module('gp_zstd_compression',
  files('zstd_compression.c'),
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [zstd],
  },
)
contrib_targets += gp_zstd_compression

# Plain SQL, not an EXTENSION
install_data(
  kwargs: contrib_data_args,
)
