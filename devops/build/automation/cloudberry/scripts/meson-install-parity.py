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

"""Check that the meson build installs every file the Makefiles install.

The meson port lists Cloudberry's installed files by hand, so a file added to
a Makefile after the port lands is silently dropped from the meson install.
That failure is invisible until something imports the missing module at
runtime -- which is how gppylib/commands/base.py, gppylib/util/ and the
gpcheckcat catalog JSON were found: not by a build failure, but by gpstop
dying on a cluster that had come up cleanly.

This is the static check for that class of bug. For every Makefile with an
install target, it reads the filenames the recipe installs and looks for each
one in the meson.build next to it. It is deliberately textual: a name that
appears anywhere in meson.build counts, because the point is to catch
omissions, not to model meson.

Scope is the Cloudberry-specific trees, where meson.build spells filenames out
literally. Upstream PostgreSQL directories are excluded: they generate install
lists programmatically, so a name-for-name comparison there reports noise
rather than bugs.

Usage: meson-install-parity.py [source-root]
"""

import os
import re
import sys

# Cloudberry's own trees. Everything under them is checked.
ROOTS = ['gpMgmt', 'gpcontrib', 'gpAux', 'contrib/pax_storage']

# Directories never walked into: tests are not installed, and pythonSrc/ext
# holds unpacked third-party tarballs.
SKIP_DIRS = {'test', 'tests', '__pycache__', 'ext', 'pythonSrc', 'regress',
             'unit', 'build', 'third-party'}

# Files a Makefile mentions but neither build system installs. Each needs a
# reason, so that this list cannot quietly become a way to silence real gaps.
EXPECTED_ABSENT = {
    # Built only under cmake's BUILD_TOOLS, which defaults to OFF and which
    # contrib/pax_storage/Makefile never turns on, so the `if [ -f ... ]`
    # around its install never fires in either build.
    'contrib/pax_storage': {'pax_dump'},
}


def makefile_vars(text):
    """Resolve the simple `NAME = a b c` assignments a Makefile uses."""
    variables = {}
    for m in re.finditer(r'^(\w+)\s*[:+]?=\s*((?:.*\\\n)*.*)$', text, re.M):
        variables[m.group(1)] = m.group(2).replace('\\\n', ' ')
    return variables


def installed_names(makefile_text):
    """Filenames the install recipe installs, from $(VAR) loops and literals."""
    body = makefile_text.split('\ninstall:')[1].split('\n\n')[0]
    names = set()
    variables = makefile_vars(makefile_text)
    for var in re.findall(r'\$\((\w+)\)', body):
        for token in variables.get(var, '').split():
            # Skip make functions, paths and anything without an extension:
            # those are directories or variables, not installed files.
            if re.match(r'^[\w.+-]+$', token) and '.' in token:
                names.add(token)
    for token in re.findall(r'INSTALL_(?:SCRIPT|DATA|PROGRAM|SHLIB)\)?\s+([\w./+-]+)', body):
        names.add(os.path.basename(token))
    return names


def names_meson(name, meson):
    """Is this installed file accounted for in meson.build?

    Usually the filename appears verbatim in an install_data() list. A
    compiled module is the exception: the Makefile installs pax.so, while
    meson declares shared_module('pax') and derives the suffix, so fall back
    to matching the target name for the extensions a build produces.
    """
    if name in meson:
        return True
    stem, ext = os.path.splitext(name)
    return ext in ('.so', '.dylib', '.dll', '.a') and "'%s'" % stem in meson


def main(argv) -> int:
    root = argv[1] if len(argv) > 1 else '.'
    gaps = []

    for tree in ROOTS:
        for dirpath, dirnames, filenames in os.walk(os.path.join(root, tree)):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            if 'Makefile' not in filenames:
                continue

            rel = os.path.relpath(dirpath, root)
            with open(os.path.join(dirpath, 'Makefile')) as f:
                makefile = f.read()
            if not re.search(r'^install:', makefile, re.M):
                continue

            wanted = installed_names(makefile) - EXPECTED_ABSENT.get(rel, set())
            if not wanted:
                continue

            meson_path = os.path.join(dirpath, 'meson.build')
            if not os.path.exists(meson_path):
                gaps.append((rel, sorted(wanted), 'no meson.build'))
                continue
            with open(meson_path) as f:
                meson = f.read()
            missing = sorted(n for n in wanted if not names_meson(n, meson))
            if missing:
                gaps.append((rel, missing, 'not named in meson.build'))

    if not gaps:
        print('meson install parity: no gaps')
        return 0

    for rel, missing, why in gaps:
        print('::error::%s installs %s via make but %s'
              % (rel, ' '.join(missing), why))
    print()
    print('%d file(s) are installed by the autoconf build and not by meson.'
          % sum(len(m) for _, m, _ in gaps))
    print('Add them to the meson.build alongside the Makefile, or, if neither')
    print('build should install them, record why in EXPECTED_ABSENT here.')
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
