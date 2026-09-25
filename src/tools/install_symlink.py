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

"""Create a symlink in the install tree.

Usage: install_symlink.py <link> <target>

  link    where the symlink goes, relative to the install prefix
  target  what it points to, verbatim -- relative links stay relative

Cloudberry's Makefiles create three symlinks that the upstream PostgreSQL
build has no equivalent for:

    $bindir/stream/stream  -> ../lib/stream
    $bindir/lib/gpcheckcat -> ../gpcheckcat
    $pkglibdir/gps3ext.so  -> gpcloud.so

meson grew install_symlink() in 0.61, but PostgreSQL declares a floor of
0.54 and means it -- and raising the floor here turns roughly thirty
deprecations in upstream's own meson files into warnings on every configure,
because meson only reports a deprecated API once the project claims a version
that has it deprecated. Three symlinks are not worth that, nor worth
diverging from upstream on the one line every future merge touches.
"""

import os
import sys
from pathlib import Path


def main(argv) -> int:
    if len(argv) != 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    link_rel, target = argv[1], argv[2]

    # DESTDIR-prefixed, so `meson install --destdir` stages correctly.
    prefix = Path(os.environ['MESON_INSTALL_DESTDIR_PREFIX'])
    quiet = os.environ.get('MESON_INSTALL_QUIET', '') not in ('', '0', 'false')

    link = prefix / link_rel
    link.parent.mkdir(parents=True, exist_ok=True)

    # Installing twice must not fail, and neither must installing over a real
    # file left by an earlier layout. This is what `ln -sf` gives the
    # Makefiles.
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(target)

    if not quiet:
        print('Installing symlink {} pointing to {}'.format(link, target))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
