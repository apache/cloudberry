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

# gpcheckcat reads the foreign-key relationships of the catalog out of this
# file rather than deriving them at runtime, so an install without it can load
# but not check a catalog.
#
# The Makefile pipes the catalog headers into process_foreign_keys.pl with cat;
# the script reads them with Perl's <>, which takes filenames from ARGV just as
# happily as it takes stdin, so the file list can be passed directly. Output is
# keyed and sorted by catalog name, so argument order does not matter.
gppylib_catalog_json = custom_target(gp_majorversion + '.json',
  output: gp_majorversion + '.json',
  input: postgres_bki_srcs + files('../../../../src/include/catalog/catversion.h'),
  command: [perl, files('../../../../src/backend/catalog/process_foreign_keys.pl'),
            '@INPUT@'],
  capture: true,
  build_by_default: true,
  install: true,
  install_dir: gppylib_dir / 'data',
)
