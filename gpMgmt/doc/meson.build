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

subdir('gpconfigs')

# The long --help text of the management utilities, which they read from
# $GPHOME/docs/cli_help and replace with a short summary when it is missing.
# The list is DOCS in gpMgmt/doc/Makefile.
install_data(
  'gpactivatestandby_help',
  'gpaddmirrors_help',
  'gpcheckperf_help',
  'gpconfig_help',
  'gpdeletesystem_help',
  'gpexpand_help',
  'gpfdist_help',
  'gpinitstandby_help',
  'gpinitsystem_help',
  'gpload_help',
  'gplogfilter_help',
  'gppkg_help',
  'gprecoverseg_help',
  'gpreload_help',
  'gpscp_help',
  'gpssh-exkeys_help',
  'gpssh_help',
  'gpstart_help',
  'gpstate_help',
  'gpstop_help',
  install_dir: dir_prefix / 'docs' / 'cli_help',
)
