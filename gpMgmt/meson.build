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

# The management utilities are almost entirely file installs; the autoconf
# build spends four levels of recursive make on them. Two small C programs
# (stream, ifaddrs) are the only things compiled here.

gpmgmt_subst_version = files('bin/subst_version.py')

# gpMgmt/Makefile generates $prefix/cloudberry-env.sh by running
# bin/generate-cloudberry-env.sh and redirecting its stdout. The generator uses
# a quoted heredoc, so its output is entirely static and derives GPHOME at
# runtime -- nothing from the build environment leaks in.
#
# gpAux/gpdemo/demo_cluster.sh sources this file, and it is how users set up
# their environment, so an install without it is not usable.
cloudberry_env_sh = custom_target('cloudberry-env.sh',
  output: 'cloudberry-env.sh',
  command: [bash, files('bin/generate-cloudberry-env.sh')],
  capture: true,
  build_by_default: true,
  install: true,
  install_dir: dir_prefix,
)

subdir('sbin')
subdir('bin')
subdir('doc')

# psutil, PyGreSQL and PyYAML land in $prefix/lib/python if they have been
# staged in gpMgmt/bin/ext, the same conditional copy gpMgmt/Makefile does.
# It has to be an install script rather than an install_subdir: whether the
# modules are there is decided after configure, by a separate build step that
# itself needs a finished install to find pg_config. See the script's header.
meson.add_install_script('install_python_modules.py')
