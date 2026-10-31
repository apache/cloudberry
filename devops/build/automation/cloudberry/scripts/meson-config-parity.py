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

"""Compare the pg_config.h that configure writes with the one meson writes.

Usage: meson-config-parity.py <autoconf pg_config.h> <meson pg_config.h> [srcdir]

The same source compiled against two different pg_config.h files is two
different programs, and nothing at build time says so. This is how the meson
build came to have 8 kB pages where configure gives 32 kB, float4 passed by
reference in the dozen places Cloudberry tests USE_FLOAT4_BYVAL -- which
crashed every GiST distance ordering on float4 -- and a UDP interconnect that
times retransmissions by the wall clock because HAVE_LIBRT was never defined.
Each built, installed and started cleanly.

Two headers from the same options should agree on every macro the code looks
at. The two build systems do not define the same set, though: autoconf carries
boilerplate upstream's meson build dropped as unneeded (HAVE_STDLIB_H,
STDC_HEADERS), and records library checks nothing tests. So the rule is
mechanical rather than a list to keep current: a macro that differs -- present
in one and not the other, or with a different value -- is an error if any
source file names it, and harmless if none does. The few differences that are
referenced and deliberate are in ACCEPTED below, each with its reason.
"""

import os
import re
import sys

# Differences that source does look at, and that are intended.
ACCEPTED = {
    # configure's command line; meson has none, and pg_config --configure
    # reports it. Upstream's meson build leaves it empty too.
    'CONFIGURE_ARGS': 'no configure command line in a meson build',
    # Host and compiler are spelled differently (meson has no config.guess
    # triplet and reports the compiler as id-version); gpversion.py only
    # parses the "(Apache Cloudberry ...)" part, which is identical.
    'PG_VERSION_STR': 'host and compiler spelling',
    # configure passes this with -D on the compile line of guc_tables.c alone,
    # not through pg_config.h; meson puts it in pg_config.h. The value -- the
    # default krb_server_keyfile -- is the same.
    'PG_KRB_SRVTAB': 'autoconf passes it with -D, not through pg_config.h',
    # configure's AC_C_RESTRICT maps the C99 keyword to __restrict, which
    # means the same to every compiler that builds Cloudberry; the code that
    # must also compile as C++ uses pg_restrict, which both define alike.
    # Upstream's meson build leaves restrict alone for the same reason.
    'restrict': 'C99 keyword, same meaning as __restrict',
}

SOURCE_EXTS = ('.c', '.h', '.cc', '.cpp', '.y', '.l')
SKIP_DIRS = {'.git', 'third-party', 'build', 'build-meson', 'tmp_install'}
# Headers either build system generates, which name every macro there is.
GENERATED = {'pg_config.h', 'pg_config.h.in', 'pg_config_ext.h', 'pg_config_os.h'}


def defines(path):
    out = {}
    with open(path) as f:
        for m in re.finditer(r'^#define\s+(\w+)(?:\s+(.*?))?\s*$', f.read(), re.M):
            out[m.group(1)] = (m.group(2) or '').strip()
    return out


# Comments and string literals: a macro named in either cannot change what is
# compiled. buffile.c mentions HAVE_ZSTD in an #endif comment, and "restrict"
# is a psql meta-command, not the C keyword.
NOT_CODE = re.compile(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\'',
                      re.S)


def referenced(names, srcdir):
    """Which of `names` any source file uses as code, as a whole word."""
    if not names:
        return set()
    pattern = re.compile(r'\b(%s)\b' % '|'.join(map(re.escape, sorted(names))))
    found = set()
    for dirpath, dirnames, filenames in os.walk(srcdir):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in filenames:
            if not fn.endswith(SOURCE_EXTS) or fn in GENERATED:
                continue
            try:
                with open(os.path.join(dirpath, fn), errors='replace') as f:
                    found.update(pattern.findall(NOT_CODE.sub(' ', f.read())))
            except OSError:
                pass
            if found == names:
                return found
    return found


def main(argv) -> int:
    if len(argv) < 3:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    autoconf, meson = defines(argv[1]), defines(argv[2])
    srcdir = argv[3] if len(argv) > 3 else '.'

    only_a = set(autoconf) - set(meson)
    only_m = set(meson) - set(autoconf)
    differ = {k for k in set(autoconf) & set(meson) if autoconf[k] != meson[k]}
    candidates = (only_a | only_m | differ) - set(ACCEPTED)
    used = referenced(candidates, srcdir)

    errors = []
    for k in sorted(used):
        if k in only_a:
            errors.append('%s is defined by configure (%s) but not by meson'
                          % (k, autoconf[k] or 'empty'))
        elif k in only_m:
            errors.append('%s is defined by meson (%s) but not by configure'
                          % (k, meson[k] or 'empty'))
        else:
            errors.append('%s is %s from configure but %s from meson'
                          % (k, autoconf[k], meson[k]))

    unused = sorted(candidates - used)
    print('pg_config.h: %d macros from configure, %d from meson; %d differ, '
          '%d of them referenced by source' % (len(autoconf), len(meson),
                                               len(candidates), len(used)))
    if unused:
        print('differences no source refers to, and so cannot change the '
              'build: ' + ' '.join(unused))
    for k in sorted(set(ACCEPTED) & (only_a | only_m | differ)):
        print('accepted: %s (%s)' % (k, ACCEPTED[k]))

    if not errors:
        print('meson config parity: no gaps')
        return 0
    for e in errors:
        print('::error::' + e)
    print()
    print('Each of these is compiled differently by the two build systems.')
    print('Make meson define it as configure does, or, if the difference is')
    print('intended, record why in ACCEPTED here.')
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
