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

# SHLIB_LINK += -lcurl (hardcoded in the Makefile; use the dependency instead)
pxf_fdw_args = contrib_mod_args + {
  'dependencies': contrib_mod_args['dependencies'] + [libcurl],
}

pxf_fdw = shared_module('pxf_fdw',
  files(
    'libchurl.c',
    'pxf_bridge.c',
    'pxf_deparse.c',
    'pxf_fdw.c',
    'pxf_filter.c',
    'pxf_fragment.c',
    'pxf_header.c',
    'pxf_option.c',
  ),
  kwargs: pxf_fdw_args,
)
contrib_targets += pxf_fdw

install_data(
  'pxf_fdw--1.0.sql',
  'pxf_fdw.control',
  kwargs: contrib_data_args,
)
