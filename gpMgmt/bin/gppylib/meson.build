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

gppylib_dir = get_option('libdir') / 'python' / 'gppylib'

foreach s : ['commands', 'data', 'db', 'gpMgmttest', 'operations', 'programs', 'system', 'util']
  subdir(s)
endforeach

install_data(
  'gparray.py',
  'gpunit',
  'unit2',
  install_dir: gppylib_dir,
  install_mode: 'rwxr-xr-x',
)

install_data(
  '__init__.py',
  'datetimeutils.py',
  'gp_era.py',
  'gpcatalog.py',
  'gplog.py',
  'gpparseopts.py',
  'gpresgroup.py',
  'gpsubprocess.py',
  'gpversion.py',
  'heapchecksum.py',
  'logfilter.py',
  'parseutils.py',
  'pgconf.py',
  'recoveryinfo.py',
  'userinput.py',
  'utils.py',
  install_dir: gppylib_dir,
)

# putversion is applied to mainUtils.py specifically
install_data(
  configure_file(input: 'mainUtils.py', output: 'mainUtils.py',
    command: [python, gpmgmt_subst_version, '@INPUT@', '@OUTPUT@', gp_version_long]),
  install_dir: gppylib_dir,
)
