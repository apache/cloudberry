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
  'dca.py',
  'gp.py',
  'pg.py',
  'unix.py',
  install_dir: gppylib_dir / 'commands',
)

# The Makefile installs this one with INSTALL_SCRIPT rather than INSTALL_DATA.
# It is imported, not executed, so the mode is of no consequence -- but it is
# what the autoconf install produces, and the two trees should be comparable
# file for file.
install_data(
  'base.py',
  install_dir: gppylib_dir / 'commands',
  install_mode: 'rwxr-xr-x',
)
