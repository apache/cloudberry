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
  'programIoUtils.py',
  install_dir: gppylib_dir / 'programs',
)

# putversion is applied to these specific files by this directory's Makefile
foreach f : [
  'clsAddMirrors.py',
  'clsRecoverSegment.py',
  'clsRecoverSegment_triples.py',
  'clsSystemState.py',
]
  install_data(
    configure_file(input: f, output: f,
      command: [python, gpmgmt_subst_version, '@INPUT@', '@OUTPUT@', gp_version_long]),
    install_dir: gppylib_dir / 'programs',
  )
endforeach

# gppkg.py gets both treatments the Makefile gives it: installed executable,
# and run through putversion like the cls* modules above.
install_data(
  configure_file(input: 'gppkg.py', output: 'gppkg.py',
    command: [python, gpmgmt_subst_version, '@INPUT@', '@OUTPUT@', gp_version_long]),
  install_dir: gppylib_dir / 'programs',
  install_mode: 'rwxr-xr-x',
)
