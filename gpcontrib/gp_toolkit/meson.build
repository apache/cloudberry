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

gp_toolkit_sources = files(
  'gp_partition_maint.c',
)

# resource group support is Linux-only; elsewhere the dummy is used.
# Mirrors: ifeq ($(shell uname -s), Linux) in this directory's Makefile.
if host_machine.system() == 'linux'
  gp_toolkit_sources += files('resgroup.c')
else
  gp_toolkit_sources += files('resgroup-dummy.c')
endif

gp_toolkit = shared_module('gp_toolkit',
  gp_toolkit_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += gp_toolkit

install_data(
  'gp_toolkit--1.1--1.2.sql',
  'gp_toolkit--1.0--1.1.sql',
  'gp_toolkit--1.0.sql',
  'gp_toolkit--1.2--1.3.sql',
  'gp_toolkit--1.3.sql',
  'gp_toolkit--1.3--1.4.sql',
  'gp_toolkit--1.4--1.5.sql',
  'gp_toolkit--1.5--1.6.sql',
  'gp_toolkit.control',
  kwargs: contrib_data_args,
)
