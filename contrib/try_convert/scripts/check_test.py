#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re

regression_path = './regression.diffs'

f = open(regression_path)
lines = f.read().split('\n')

needed_types = [
    'bool',
    
    'int2',
    'int4',
    'int8',
    'float4',
    'float8',
    'numeric',

    'date',
    'time',
    'timestamp',
    'timetz',
    'timestamptz',
    'interval'

    'regproc',
    'pg_catalog',
    'regclass',
    'regtype',

    'value_day',
    'oid',
    'jsonb',
    'json',

    # 'text',
    # 'bpchar',
    # 'varchar',
    # 'char'
]

failed_tests = {}
c = 0

for i in range(1, len(lines)):
    preline = lines[i-1]
    line = lines[i]
    if len(line) > 0 and len(preline) > 0 and (line[0] == '-' or line[0] == '+') and (preline[0] != '-' and preline[0] != '+'):
        words = re.split('::|\*|;|\n| |\(|\)|,|\.|\".*\"|\'.*\'|<.*>', preline)
        ans = []
        is_prining = False
        for word in words:
            if word not in ['select', 'from', 'count', 
                            'try_convert', 'try_convert_by_sql', 'try_convert_by_sql_text', 'try_convert_by_sql_with_len_out', 
                            'NULL', 'v', 'v1', 'v2', 'where', 'is', 'not', 'distinct', 'as', 't', '']:
                ans += [word]
                for w in word.split('_'):
                    if w in needed_types:
                        is_prining = True
        if is_prining:
            failed_tests[' '.join(ans)] = True
            # print(ans, line)
            c += 1

for ft in failed_tests:
    print(ft)

print(f'Summary: {c}')
        