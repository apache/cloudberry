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

subdir('stream')
subdir('ifaddrs')
subdir('gpcheckcat_modules')
subdir('gpconfig_modules')
subdir('gpssh_modules')
subdir('gppylib')
subdir('lib')

# Top-level management utilities. putversion rewrites these after install in
# the autoconf build; meson substitutes on the way into the build directory.
foreach f : [
  'analyzedb',
  'gpactivatestandby',
  'gpaddmirrors',
  'gpcheckcat',
  'gpcheckperf',
  'gpcheckresgroupimpl',
  'gpcheckresgroupv2impl',
  'gpconfig',
  'gpdeletesystem',
  'gpdemo',
  'gpdirtableload',
  'gpexpand',
  'gpinitstandby',
  'gpinitsystem',
  'gpload',
  'gpload.py',
  'gplogfilter',
  'gpmemreport',
  'gpmemwatcher',
  'gpmovemirrors',
  'gppkg',
  'gprecoverseg',
  'gpreload',
  'gpsd',
  'gpshrink',
  'gpssh',
  'gpssh-exkeys',
  'gpstart',
  'gpstate',
  'gpstop',
  'gpsync',
  'minirepro',
]
  install_data(
    configure_file(input: f, output: f,
      command: [python, gpmgmt_subst_version, '@INPUT@', '@OUTPUT@', gp_version_long]),
    install_dir: dir_bin,
    install_mode: 'rwxr-xr-x',
  )
endforeach

install_data('gpload.bat', install_dir: dir_bin)

# GPDEMO_LIBS: the demo-cluster scripts live in gpAux/gpdemo but are installed
# from here (bin/Makefile), into $bindir/lib/gpdemo.
install_data(
  '../../gpAux/gpdemo/README',
  '../../gpAux/gpdemo/demo_cluster.sh',
  '../../gpAux/gpdemo/generate_certs.sh',
  '../../gpAux/gpdemo/gpdemo-defaults.sh',
  '../../gpAux/gpdemo/lalshell',
  '../../gpAux/gpdemo/probe_config.sh',
  install_dir: dir_bin / 'lib' / 'gpdemo',
  install_mode: 'rwxr-xr-x',
)

# if [ ! -L $(bindir)/lib/gpcheckcat ]; then ln -s ../gpcheckcat gpcheckcat
meson.add_install_script(gp_install_symlink,
  dir_bin / 'lib' / 'gpcheckcat',
  '..' / 'gpcheckcat',
)
