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

orafce = shared_module('orafce',
  files(
    'aggregate.c',
    'alert.c',
    'assert.c',
    'charlen.c',
    'charpad.c',
    'convert.c',
    'datefce.c',
    'dbms_sql.c',
    'file.c',
    'magic.c',
    'math.c',
    'nvarchar2.c',
    'orafce.c',
    'others.c',
    'parse_keyword.c',
    'pipe.c',
    'plunit.c',
    'plvdate.c',
    'plvlex.c',
    'plvstr.c',
    'plvsubst.c',
    'putline.c',
    'random.c',
    'regexp.c',
    'replace_empty_string.c',
    'shmmc.c',
    'sqlparse.c',
    'utility.c',
    'varchar2.c',
  ),
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [libintl],
  },
)
contrib_targets += orafce

install_data(
  'orafce--4.9.sql',
  'orafce--3.2--3.3.sql',
  'orafce--3.3--3.4.sql',
  'orafce--3.4--3.5.sql',
  'orafce--3.5--3.6.sql',
  'orafce--3.6--3.7.sql',
  'orafce--3.7--3.8.sql',
  'orafce--3.8--3.9.sql',
  'orafce--3.9--3.10.sql',
  'orafce--3.10--3.11.sql',
  'orafce--3.11--3.12.sql',
  'orafce--3.12--3.13.sql',
  'orafce--3.13--3.14.sql',
  'orafce--3.14--3.15.sql',
  'orafce--3.15--3.16.sql',
  'orafce--3.16--3.17.sql',
  'orafce--3.17--3.18.sql',
  'orafce--3.18--3.19.sql',
  'orafce--3.19--3.20.sql',
  'orafce--3.20--3.21.sql',
  'orafce--3.21--3.22.sql',
  'orafce--3.22--3.23.sql',
  'orafce--3.23--3.24.sql',
  'orafce--3.24--3.25.sql',
  'orafce--3.25--4.0.sql',
  'orafce--4.0--4.1.sql',
  'orafce--4.1--4.2.sql',
  'orafce--4.2--4.3.sql',
  'orafce--4.3--4.4.sql',
  'orafce--4.4--4.5.sql',
  'orafce--4.5--4.6.sql',
  'orafce--4.6--4.7.sql',
  'orafce--4.7--4.8.sql',
  'orafce--4.8--4.9.sql',
  'orafce.control',
  kwargs: contrib_data_args,
)
