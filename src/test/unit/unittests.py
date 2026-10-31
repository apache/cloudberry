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

"""List the unit tests `make unittest-check` builds, for the meson build.

Usage: unittests.py <source-root>

The tests are defined in the Makefiles under src/*/test, and this reads them
there rather than have meson keep a second copy that drifts. One line per
test program:

    mock|<dir>|<target>|<mocked sources>|<wrapped functions>|<mkdir>
    custom|<dir>|<target>|||

`mock` tests use src/backend/mock.mk: the test links the backend with the
listed sources replaced by mocks that src/test/unit/mock/mocker.py generates.
<mocked sources> are paths relative to the source root, comma-separated.
<wrapped functions> are those the test defines as __wrap_<name>, which mock.mk
turns into -Wl,--defsym,<name>=__wrap_<name>. <mkdir> is a directory the
Makefile creates before running the test.

`custom` tests have a rule of their own in their Makefile, and meson.build
has to say how to build each one; it refuses one it does not know.
"""

import os
import re
import sys


def makefile_text(path):
    with open(path) as f:
        # Join continuation lines, drop comments.
        text = f.read().replace('\\\n', ' ')
    return re.sub(r'#.*', '', text)


def targets(text):
    out = []
    for m in re.finditer(r'^TARGETS\s*\+?=(.*)$', text, re.M):
        out += m.group(1).split()
    return out


def mocked_sources(root, text, target):
    srcs = []
    for m in re.finditer(r'^%s\.t\s*:(.*)$' % re.escape(target), text, re.M):
        for dep in m.group(1).split():
            mm = re.match(r'\$\(MOCK_DIR\)/(.+)_mock\.o$', dep)
            if not mm:
                continue
            src = 'src/' + mm.group(1) + '.c'
            # src/backend/libpq/Makefile symlinks libpq's fe-*.c and
            # pqexpbuffer.c into its own directory before building them; the
            # meson build compiles them where they are.
            if not os.path.exists(os.path.join(root, src)) and \
                    src.startswith('src/backend/libpq/'):
                src = 'src/interfaces/libpq/' + os.path.basename(src)
            srcs.append(src)
    return srcs


def wrapped(test_c):
    # mock.mk: sed -n 's/.*__wrap_\(\w*\)(.*/\1/p'
    names = set()
    with open(test_c, errors='replace') as f:
        for line in f:
            m = re.match(r'.*__wrap_(\w*)\(', line)
            if m:
                names.add(m.group(1))
    return sorted(names)


def main(argv):
    root = argv[1]
    for top in ('src/backend', 'src/bin'):
        for dirpath, dirnames, filenames in sorted(os.walk(os.path.join(root, top))):
            dirnames.sort()
            if os.path.basename(dirpath) != 'test' or 'Makefile' not in filenames:
                continue
            testdir = os.path.relpath(dirpath, root)
            text = makefile_text(os.path.join(dirpath, 'Makefile'))
            kind = 'mock' if re.search(r'^include .*/mock\.mk', text, re.M) else 'custom'
            m = re.search(r"\$\(MKDIR_P\)\s+'?([\w/]+)'?", text)
            mkdir = m.group(1) if m else ''
            for t in targets(text):
                if kind == 'custom':
                    print('custom|%s|%s|||' % (testdir, t))
                    continue
                test_c = os.path.join(dirpath, t + '_test.c')
                print('mock|%s|%s|%s|%s|%s' % (
                    testdir, t, ','.join(mocked_sources(root, text, t)),
                    ','.join(wrapped(test_c)), mkdir))


if __name__ == '__main__':
    main(sys.argv)
