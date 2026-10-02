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

"""Install the Python modules gpMgmt needs into $prefix/lib/python.

The gpMgmt utilities import psutil, pg (PyGreSQL) and yaml at runtime, and
cloudberry-env.sh puts $GPHOME/lib/python on PYTHONPATH for exactly that
reason.  gpMgmt/Makefile copies them out of gpMgmt/bin/ext during `make
install`, if that directory happens to exist.

This is the meson equivalent, and it is deliberately the same conditional
copy rather than a build step.  autoconf grows the directory from
--with-pythonsrc-ext, which downloads three tarballs from PyPI in the middle
of the build; meson has no honest way to express that, and a build that
reaches the network is not one we want.  So the modules stay an external
dependency: stage them yourself, then install.  The supported way to do that
is the target the autoconf build already uses, which needs pg_config from a
finished install:

    ninja -C build install
    PATH=$prefix/bin:$PATH make -C gpMgmt/bin TAR=tar psutil pygresql pyyaml
    ninja -C build install

That target fetches the three tarballs from PyPI the first time, and skips
any it already finds, so an air-gapped or offline build only has to put them
in gpMgmt/bin/pythonSrc/ext beforehand -- at the exact versions named at the
top of gpMgmt/bin/Makefile:

    psutil-5.7.0.tar.gz  PyGreSQL-5.2.tar.gz  PyYAML-5.4.1.tar.gz

Fetching them is then a step you control, and nothing after `meson setup`
reaches the network.  (They are not vendored in the repo; downloading them
is what replaced that, under the Apache release policy.)

Anything else that leaves importable modules in gpMgmt/bin/ext works too --
a virtualenv's site-packages copied in, or distribution packages.  An
install with nothing staged is still a valid install; it just cannot run the
management utilities until the three modules are on PYTHONPATH some other
way.  Hence: no staged modules, no output, no error.
"""

import os
import shutil
import sys
from glob import glob
from pathlib import Path


def main() -> int:
    source_root = Path(os.environ['MESON_SOURCE_ROOT'])
    # DESTDIR-prefixed, so `meson install --destdir` stages correctly.
    prefix = Path(os.environ['MESON_INSTALL_DESTDIR_PREFIX'])
    # meson sets this to 0 rather than unsetting it for a non-quiet install.
    quiet = os.environ.get('MESON_INSTALL_QUIET', '') not in ('', '0', 'false')

    ext = source_root / 'gpMgmt' / 'bin' / 'ext'

    # cloudberry-env.sh hardcodes PYTHONPATH=$GPHOME/lib/python, so this is a
    # literal path and not libdir -- an install that honoured a libdir of
    # lib64 here would put the modules somewhere nothing looks.
    dest = prefix / 'lib' / 'python'

    # Mirrors the gpMgmt/Makefile install target. Each group is independent:
    # staging only psutil is a legitimate thing to do.
    groups = [
        ['__init__.py'],
        ['psutil'],
        ['pgdb.py', 'pg.py', '_pg*.so'],
        ['yaml'],
    ]

    installed = []
    for group in groups:
        matches = [Path(m) for pattern in group for m in sorted(glob(str(ext / pattern)))]
        # A group is all-or-nothing: pg.py without _pg*.so is not importable,
        # and half-installing it would fail at runtime instead of at install
        # time. Skipping the whole group leaves the same clean "not staged"
        # state as staging nothing at all.
        if len(matches) < len(group):
            continue
        dest.mkdir(parents=True, exist_ok=True)
        for src in matches:
            target = dest / src.name
            if src.is_dir():
                shutil.copytree(src, target, dirs_exist_ok=True)
            else:
                shutil.copy2(src, target)
            installed.append(src.name)

    if installed and not quiet:
        print('Installing gpMgmt Python modules to {}: {}'.format(
            dest, ', '.join(installed)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
