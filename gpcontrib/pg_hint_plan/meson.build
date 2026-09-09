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

# PG_CPPFLAGS += -I$(top_srcdir)/src/pl/plpgsql/src/
pg_hint_plan = shared_module('pg_hint_plan',
  files('pg_hint_plan.c'),
  include_directories: include_directories('../../src/pl/plpgsql/src'),
  kwargs: contrib_mod_args,
)
contrib_targets += pg_hint_plan

install_data(
  'pg_hint_plan--1.3.0.sql',
  'pg_hint_plan--1.3.0--1.3.1.sql',
  'pg_hint_plan--1.3.1--1.3.2.sql',
  'pg_hint_plan--1.3.2--1.3.3.sql',
  'pg_hint_plan--1.3.3--1.3.4.sql',
  'pg_hint_plan--1.3.5--1.3.6.sql',
  'pg_hint_plan--1.3.4--1.3.5.sql',
  'pg_hint_plan--1.3.6--1.3.7.sql',
  'pg_hint_plan--1.3.7--1.3.8.sql',
  'pg_hint_plan--1.3.8--1.3.9.sql',
  'pg_hint_plan.control',
  kwargs: contrib_data_args,
)
