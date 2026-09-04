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

subdir('pexpect')

install_data(
  '__init__.py',
  'gp_bash_functions.sh',
  'gpconfigurenewsegment',
  'gpcreateseg.sh',
  'gppinggpfdist.py',
  'gpstate.py',
  'multidd',
  install_dir: dir_bin / 'lib',
  install_mode: 'rwxr-xr-x',
)

# gp_bash_version.sh is generated from a template. configure.ac does this with
# an inline sed because the placeholder is ##version## rather than @VERSION@,
# and writes the result into the source tree; do it properly here.
install_data(
  configure_file(
    input: 'gp_bash_version.sh.in',
    output: 'gp_bash_version.sh',
    command: [sed, 's,##.*##,' + gp_version_long + ',', '@INPUT@'],
    capture: true,
  ),
  install_dir: dir_bin / 'lib',
  install_mode: 'rwxr-xr-x',
)

# Not a program: gpmemreport feeds this to gdb as a batch script. The Makefile
# installs it with INSTALL_DATA for that reason.
install_data(
  'crashreport.gdb',
  install_dir: dir_bin / 'lib',
)
