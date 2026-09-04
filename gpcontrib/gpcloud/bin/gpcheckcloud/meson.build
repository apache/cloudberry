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

# Same sources as the module, rebuilt standalone with the S3_STANDALONE
# defines. The Makefile does this with a hand-rolled cross-directory pattern
# rule (%.o: ../../src/%.cpp); meson just lists the paths.
gpcheckcloud = executable('gpcheckcloud',
  files('gpcheckcloud.cpp'),
  gpcloud_common_sources,
  gpcloud_lib_sources,
  cpp_args: ['-DS3_STANDALONE', '-DS3_STANDALONE_CHECKCLOUD'],
  include_directories: gpcloud_inc,
  override_options: ['cpp_std=c++11'],
  dependencies: [frontend_code] + gpcloud_deps,
  kwargs: default_bin_args,
)
bin_targets += gpcheckcloud
