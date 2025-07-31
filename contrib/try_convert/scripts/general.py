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

source_filenames = [
        'time.c', 'date.c', 'datetime.c', 'timestamp.c', 'nabstime.c', 'formatting.c', 
        'int.c', 'int8.c', 'float.c', 'float.c', 'bool.c', 'numeric.c', 'numutils.c',
        'format_type.c',
        # 'varbit.c',
        'char.c', 'varchar.c',
        'json.c', 'jsonb.c', 'xml.c', 
        'network.c',
        # 'cash.c',
        'reg_proc.c',
        'postgres.c',
        'stringinfo.c',
    ] + [
        'citext.c', 'oracle_compat.c',
        'hstore_io.c',
    ]

supported_types = [
    'int8',             # NUMBERS
    'int4',
    'int2',
    'float8',
    'float4',
    'numeric',
    # 'complex',

    'bool',

    # 'bit',              # BITSTRING
    # 'varbit',

    'date',             # TIME
    'time',
    'timetz',
    'timestamp',
    'timestamptz',
    'interval',

    # 'point',            # GEOMENTY
    # 'circle',
    # 'line',
    # 'lseg',
    # 'path',
    # 'box',
    # 'polygon',

    # 'cidr',             # IP
    # 'inet',
    # 'macaddr',

    'json',             # OBJ
    'jsonb',
    # 'xml',

    # 'bytea',   

    'char',             # STRINGS
    # 'bpchar',
    'varchar',
    'text',

    # 'money',
    # # 'pg_lsn',
    # # 'tsquery',
    # # 'tsvector',
    # # 'txid_snapshot',
    # 'uuid',

    # 'regtype',          # SYSTEM
    # 'regproc',
    # 'regclass',
    # 'oid',
] + [
    'citext',
    'hstore',
]

supported_extensions = [
    'citext',
    'hstore',
]



string_types = [
    'text',
    'citext',
    'char',
    # 'bpchar',
    'varchar',
]

typmod_types = [
    'bit',
    'varbit',
    'char',
    'varchar',
    # 'bpchar',
]

typmod_lens = [
    None, 1, 5, 10, 20
]



uncomparable_types = [
    'json',
    'xml',
    'point',
]

has_corrupt_data = [
    'time', 'timetz', 'timestamp', 'timestamptz', 'date', 
    'json',
    'int2', 'int4', 'int8', 'float4', 'float8', 'numeric', 
]