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

install_data(
  '__init__.py',
  'configurationImplGpdb.py',
  'configurationInterface.py',
  'environment.py',
  'faultProberImplGpdb.py',
  'faultProberInterface.py',
  'fileSystemImplOs.py',
  'fileSystemImplTest.py',
  'fileSystemInterface.py',
  'info.py',
  'osImplNative.py',
  'osInterface.py',
  install_dir: gppylib_dir / 'system',
)

# Installed executable by the Makefile, unlike its neighbours above.
install_data(
  'ComputeCatalogUpdate.py',
  install_dir: gppylib_dir / 'system',
  install_mode: 'rwxr-xr-x',
)
