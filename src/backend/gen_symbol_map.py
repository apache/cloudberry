#!/usr/bin/env python3
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
"""Generate the linker symbol map that keeps libpq symbols private in postgres.

Cloudberry compiles frontend symbols into the postgres binary, because the
backend connects to other segments, while extensions dynamically link
libpq.so. Some symbols are unsafe to export from both -- for example
functions that allocate with palloc/pfree in the backend but malloc/free in
the frontend. These are kept private in the postgres binary.

Equivalent to the awk/grep pipelines behind SYMBOL_MAP_FILE in
src/backend/Makefile.

Usage: gen_symbol_map.py <darwin|linux> <exports.txt> <output>
"""
import re
import sys

platform, exports, output = sys.argv[1], sys.argv[2], sys.argv[3]

# SAFELY_EXPORTED_SYMBOLS_PATTERN in src/backend/Makefile
safe = re.compile(r'(pqsignal|pg_*)')

symbols = []
with open(exports, encoding='utf-8') as fh:
    for line in fh:
        # awk '/^[^#]/': skip comments and blank lines
        if not line.strip() or line.startswith('#'):
            continue
        name = line.split()[0]
        if safe.search(name):
            continue
        symbols.append(name)

with open(output, 'w', encoding='utf-8') as fh:
    if platform == 'darwin':
        # -unexported_symbols_list wants mangled names, one per line
        for name in symbols:
            fh.write('_%s\n' % name)
    else:
        # --version-script: everything global except these
        fh.write('{ global: *; \n local:\n')
        for name in symbols:
            fh.write('%s;\n' % name)
        fh.write('};\n')
