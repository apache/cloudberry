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
"""Substitute the Cloudberry version into a management utility.

The autoconf build does this with ./putversion, which rewrites files *after*
they have been installed. meson never modifies installed files, so the
substitution happens on the way into the build directory instead and the
result is what gets installed.

putversion replaces a literal $Revision...$ placeholder with the contents of
the VERSION file, with '-' turned into ' '.

Usage: subst_version.py <input> <output> <version>
"""
import re
import sys

src, dst, version = sys.argv[1], sys.argv[2], sys.argv[3]

with open(src, encoding='utf-8', errors='surrogateescape') as fh:
    text = fh.read()

text = re.sub(r'\$Revision.*?\$', version.replace('-', ' '), text)

with open(dst, 'w', encoding='utf-8', errors='surrogateescape') as fh:
    fh.write(text)
