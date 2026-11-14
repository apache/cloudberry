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

This is the meson equivalent, and it copies the same way.  What fills
gpMgmt/bin/ext is the Makefile's psutil, pygresql and pyyaml targets, which
download the three source tarballs from PyPI into gpMgmt/bin/pythonSrc/ext
unless they are already there, and build them; PyGreSQL builds against
pg_config, so they need a finished install.  autoconf runs them from `make
install` when configured --with-pythonsrc-ext.

With -Dpythonsrc_ext=true, meson's counterpart, this script runs those same
targets first (--build), against the install it is part of, so one
`ninja -C build install` leaves the utilities runnable.  The versions and
URLs stay in gpMgmt/bin/Makefile alone.  An offline build puts the tarballs
in gpMgmt/bin/pythonSrc/ext beforehand, named as its download-python-deps
target names them, and nothing is downloaded.  (They are not vendored in the
repo; downloading them is what replaced that, under the Apache release
policy.)

Without the option, as configure without --with-pythonsrc-ext, nothing is
built or downloaded, and whatever is already in gpMgmt/bin/ext is copied:
the targets above run by hand, a virtualenv's site-packages copied in, or
distribution packages.  An install with nothing staged is still a valid
install; it just cannot run the management utilities until the three
modules are on PYTHONPATH some other way.  Hence: no staged modules, no
output, no error.
"""

import os
import shutil
import subprocess
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

    if '--build' in sys.argv[1:]:
        # The Makefile finds pg_config on PATH. Under DESTDIR it reports the
        # final prefix rather than the staged one, as an autoconf DESTDIR
        # install would.
        env = dict(os.environ, PATH=f"{prefix / 'bin'}{os.pathsep}{os.environ['PATH']}")
        if not quiet:
            print('Building gpMgmt Python modules: psutil, PyGreSQL, PyYAML')
        subprocess.run(['make', '-C', str(source_root / 'gpMgmt' / 'bin'),
                        'TAR=tar', 'psutil', 'pygresql', 'pyyaml'],
                       env=env, check=True,
                       stdout=subprocess.DEVNULL if quiet else None)

    # cloudberry-env.sh hardcodes PYTHONPATH=$GPHOME/lib/python, so this is a
    # literal path and not libdir -- an install that honoured a libdir of
    # lib64 here would put the modules somewhere nothing looks.
    dest = prefix / 'lib' / 'python'

    # Mirrors the gpMgmt/Makefile install target. Each group is independent:
    # staging only psutil is a legitimate thing to do. The second list is
    # what the Makefile also deletes before copying: a stale _yaml*.so from
    # an older PyYAML, which kept its extension beside the package.
    groups = [
        (['__init__.py'], []),
        (['psutil'], []),
        (['pgdb.py', 'pg.py', '_pg*.so'], []),
        (['yaml'], ['_yaml*.so']),
    ]

    installed = []
    for group, stale in groups:
        found = [sorted(glob(str(ext / pattern))) for pattern in group]
        # A group is all-or-nothing, pattern by pattern: pg.py without
        # _pg*.so is not importable, and half-installing it would fail at
        # runtime instead of at install time. Skipping the whole group leaves
        # the same clean "not staged" state as staging nothing at all.
        if not all(found):
            continue
        dest.mkdir(parents=True, exist_ok=True)
        # Remove the previous copy first, as the Makefile does: copying over
        # it would keep whatever the new one no longer has -- an extension
        # module built for another Python, a file psutil has dropped.
        for pattern in group + stale:
            for old in glob(str(dest / pattern)):
                if os.path.isdir(old) and not os.path.islink(old):
                    shutil.rmtree(old)
                else:
                    os.remove(old)
        for src in (Path(m) for matches in found for m in matches):
            target = dest / src.name
            if src.is_dir():
                shutil.copytree(src, target)
            else:
                shutil.copy2(src, target)
            installed.append(src.name)

    if installed and not quiet:
        print('Installing gpMgmt Python modules to {}: {}'.format(
            dest, ', '.join(installed)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
