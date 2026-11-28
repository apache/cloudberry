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

backend_sources += files(
  'bufenc.c',
  'kmgr.c',
  'sm4_ofb.c',
)

# src/backend/Makefile's install: the samples for cluster_key_command and
# ssl_passphrase_command.
install_data(
  'ckey_aws.sh.sample',
  'ckey_direct.sh.sample',
  'ckey_passphrase.sh.sample',
  'ckey_piv_nopin.sh.sample',
  'ckey_piv_pin.sh.sample',
  'ssl_passphrase.sh.sample',
  install_dir: dir_data / 'auth_commands',
)
