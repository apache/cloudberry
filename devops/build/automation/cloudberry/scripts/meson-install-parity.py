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

"""Check that the meson build builds and installs what the Makefiles do.

Four checks, all static and all textual:

  install   every file a Makefile installs is named in the meson.build beside
            it -- including PGXS-style Makefiles, which install without an
            install target
  source    every source a Makefile compiles into something meson also builds
            is named in a meson.build at or above it
  contrib   every contrib module the top-level GNUmakefile builds by default
            is subdir()'d by contrib/meson.build
  options   the options that fix the on-disk format -- block sizes, segment
            size -- and the default port have the same default as configure

The first is described below; the others at their functions.

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

Scope is the trees where meson.build spells filenames out literally:
Cloudberry's own, and contrib, whose upstream meson.build files list every
extension script by hand -- and where Cloudberry adds scripts upstream never
had, like pg_buffercache--1.4--1.4.1.sql. The rest of upstream's tree is
excluded: it generates install lists programmatically, so a name-for-name
comparison there reports noise rather than bugs.

Usage: meson-install-parity.py [source-root]
"""

import os
import re
import sys

# Cloudberry's own trees, contrib, and the one upstream directory Cloudberry's
# Makefile installs a great deal more from than upstream does:
# src/test/regress puts gpdiff.pl and its modules, GPTest.pm, regress.so and
# the hook modules into the install, and without them no extension can run
# its tests through PGXS. Everything under a root is checked.
ROOTS = ['gpMgmt', 'gpcontrib', 'gpAux', 'contrib', 'src/test/regress']

# Directories never walked into: tests are not installed, and pythonSrc/ext
# holds unpacked third-party tarballs.
SKIP_DIRS = {'test', 'tests', '__pycache__', 'ext', 'pythonSrc',
             'unit', 'build', 'third-party', 'sql', 'expected', 'input',
             'output', 'data', 'results'}

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


def recipe(makefile_text, target):
    """The recipe lines of every rule for `target`, joined."""
    out = []
    for m in re.finditer(r'^%s:[^=\n]*\n((?:\t.*\n?)*)' % re.escape(target),
                         makefile_text, re.M):
        out.append(m.group(1))
    return ''.join(out)


def install_recipes(makefile_text):
    """install's own recipe, plus those of the targets it depends on.

    Many Makefiles do the work in a helper -- `install: install-data`, with
    install-data holding the INSTALL_DATA line -- and reading only install's
    recipe misses it. gpcontrib/zstd did exactly that with the SQL that
    registers zstd in pg_compression, and the meson port installed nothing in
    its place.
    """
    body = recipe(makefile_text, 'install')
    for m in re.finditer(r'^install:([^=\n]*)$', makefile_text, re.M):
        for dep in m.group(1).split():
            if dep.startswith('$') or dep == 'install':
                continue
            # Only helpers that install: `install: stream` names the build
            # rule too, and its $(OBJS) are not installed files.
            dep_body = recipe(makefile_text, dep)
            if '$(INSTALL_' in dep_body:
                body += dep_body
    return body


def installed_names(makefile_text):
    """Filenames the install recipe installs, from $(VAR) loops and literals."""
    body = install_recipes(makefile_text)
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


def pgxs_names(makefile_text):
    """What a PGXS-style Makefile builds and installs, with no install target.

    Most extensions never write an install recipe: they set MODULE_big or
    MODULES, EXTENSION and DATA, and pgxs.mk installs them. Such a directory
    has nothing for installed_names() to read, which is how a whole new
    extension can arrive without a meson.build and not be noticed -- or a
    new upgrade script, which leaves CREATE EXTENSION failing with "no
    installation script" for the version the control file names.
    """
    names = set()
    variables = makefile_vars(makefile_text)
    for var in ('MODULE_big', 'MODULES', 'PROGRAM'):
        for token in variables.get(var, '').split():
            if re.match(r'^[\w.+-]+$', token):
                names.add(token)
    for var in ('DATA', 'DATA_built', 'DATA_TSEARCH', 'SCRIPTS', 'SCRIPTS_built'):
        for token in variables.get(var, '').split():
            if re.match(r'^[\w.+-]+$', token) and '.' in token:
                names.add(token)
    for token in variables.get('EXTENSION', '').split():
        if re.match(r'^[\w.+-]+$', token):
            names.add(token + '.control')
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


def install_gaps(root):
    """Files a Makefile installs that no meson.build beside it names."""
    gaps = []
    for tree in ROOTS:
        for dirpath, dirnames, filenames in os.walk(os.path.join(root, tree)):
            rel = os.path.relpath(dirpath, root)
            if rel in NOT_BUILT:
                dirnames[:] = []
                continue
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            # GNU make reads GNUmakefile first, and src/test/regress has only
            # that one.
            mk = next((m for m in ('GNUmakefile', 'Makefile') if m in filenames), None)
            if mk is None:
                continue

            with open(os.path.join(dirpath, mk)) as f:
                makefile = f.read()
            wanted = set()
            if re.search(r'^install:', makefile, re.M):
                wanted |= installed_names(makefile)
            wanted |= pgxs_names(makefile)
            wanted -= EXPECTED_ABSENT.get(rel, set())
            if not wanted:
                continue

            meson_path = os.path.join(dirpath, 'meson.build')
            if not os.path.exists(meson_path):
                gaps.append('%s installs %s via make but has no meson.build'
                            % (rel, ' '.join(sorted(wanted))))
                continue
            # A Makefile may install what a subdirectory builds --
            # src/test/regress installs hooktest/test_hook.so -- so the
            # meson.build files below this one count too.
            meson = ''
            for sub, subdirs, subfiles in os.walk(dirpath):
                subdirs[:] = [d for d in subdirs if d not in SKIP_DIRS]
                if 'meson.build' in subfiles:
                    with open(os.path.join(sub, 'meson.build')) as f:
                        meson += f.read()
            missing = sorted(n for n in wanted if not names_meson(n, meson))
            if missing:
                gaps.append('%s installs %s via make but no meson.build names it'
                            % (rel, ' '.join(missing)))
    return gaps


# Directories with a Makefile that the meson build deliberately does not
# build, for both checks. Each with its reason, for the same reason as
# EXPECTED_ABSENT.
NOT_BUILT = {
    # Cloudberry does not build ecpg in either build system.
    'src/interfaces/ecpg': 'ecpg is not built by Cloudberry',
    # Built with PGXS against the installed server, in CI as by users; it
    # needs Arrow and Parquet, which nothing else does.
    'contrib/datalake_fdw': 'built with PGXS, not in tree',
    # A standalone example with its own gcc Makefile, which no build
    # system recurses into.
    'contrib/sasdemo': 'standalone demo, built by neither',
}

SOURCE_EXTS = ('.c', '.cc', '.cpp', '.y', '.l')
SOURCE_SKIP = SKIP_DIRS | {'.git', 'regress', 'isolation', 'isolation2'}


def makefile_objects(makefile_text):
    """The object files a Makefile's OBJS* variables name."""
    objs = set()
    for m in re.finditer(r'^(OBJS\w*)\s*[:+]?=\s*((?:.*\\\n)*.*)$', makefile_text, re.M):
        for token in m.group(2).replace('\\\n', ' ').split():
            if re.match(r'^[\w./-]+\.o$', token):
                objs.add(token[:-2])
    return objs


def source_gaps(root):
    """Sources a Makefile compiles into something meson also builds, but not
    in meson.

    Cloudberry adds files to Makefiles in upstream directories, and those
    directories came with upstream's meson.build and upstream's source list.
    For the backend that fails at link time; for a loadable module it does
    not fail at all until CREATE EXTENSION reports a missing symbol -- which
    is how pageinspect was found without bmfuncs.c. So: for every Makefile
    object whose source exists, some meson.build in the source's own
    directory or above it has to name it.
    """
    meson_text = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ('.git', 'third-party')]
        if 'meson.build' in filenames:
            with open(os.path.join(dirpath, 'meson.build'), errors='replace') as f:
                meson_text[os.path.relpath(dirpath, root)] = f.read()

    def named(src_rel):
        # Walk up from the source's directory; each meson.build on the way
        # may name it by its path relative to that directory.
        d = os.path.dirname(src_rel)
        while True:
            text = meson_text.get(d or '.')
            if text is not None:
                tail = os.path.relpath(src_rel, d or '.')
                if "'%s'" % tail in text or "/%s'" % tail in text:
                    return True
            if not d:
                return False
            d = os.path.dirname(d)

    gaps = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        dirnames[:] = [d for d in dirnames if d not in SOURCE_SKIP
                       and os.path.normpath(os.path.join(rel, d)) not in NOT_BUILT]
        mk = next((m for m in ('GNUmakefile', 'Makefile') if m in filenames), None)
        if mk is None or rel in NOT_BUILT:
            continue
        with open(os.path.join(dirpath, mk), errors='replace') as f:
            objs = makefile_objects(f.read())

        sources = []
        for o in objs:
            stem = os.path.normpath(os.path.join(rel, o))
            src = next((stem + e for e in SOURCE_EXTS
                        if os.path.exists(os.path.join(root, stem + e))), None)
            if src:        # a generated source has nothing to find
                sources.append(src)
        # Only directories meson builds at all: one meson.build naming any
        # of the sources. A directory meson never touches is a different
        # question, answered by the default-contrib check below and, for
        # the backend, by the link.
        if not sources or not any(named(s) for s in sources):
            continue
        missing = sorted(os.path.basename(s) for s in sources if not named(s))
        if missing:
            gaps.append('%s compiles %s via make but no meson.build names it'
                        % (rel, ' '.join(missing)))
    return gaps


def default_contrib_gaps(root):
    """contrib modules the top-level GNUmakefile builds by default, that
    contrib/meson.build does not.

    Upstream builds contrib only for `make world`; Cloudberry's GNUmakefile.in
    picks a list out of it and builds that on every `make`, including five
    Cloudberry modules no upstream meson.build knows about. Missing any of
    them is a smaller install that nothing complains about until a test
    creates a formatter or an external protocol.
    """
    with open(os.path.join(root, 'GNUmakefile.in')) as f:
        top = f.read()
    all_body = top.split('\nall:')[1].split('\n\n')[0]
    wanted = set(re.findall(r'-C contrib/([\w-]+) all', all_body))
    with open(os.path.join(root, 'contrib', 'meson.build')) as f:
        meson = f.read()
    missing = sorted(m for m in wanted - DEFAULT_CONTRIB_ABSENT
                     if "subdir('%s')" % m not in meson)
    if missing:
        return ['GNUmakefile.in builds contrib/%s by default but contrib/meson.build '
                'does not' % ', contrib/'.join(missing)]
    return []


# Default contrib modules the meson build does not build, and why.
DEFAULT_CONTRIB_ABSENT = {
    # --enable-ic-udp2, off by default, is a CMake project in tree; the meson
    # option refuses to be turned on until it is ported.
    'udp2',
}


# configure.ac option -> meson option, for the defaults that decide the
# on-disk format and the server's identity. Cloudberry changed some of them
# from upstream's values in configure.ac, and meson_options.txt came from
# upstream: the block sizes stayed at 8 kB for a meson build while autoconf
# builds used 32, and every page-count and cost in the regression suite moved.
FORMAT_OPTIONS = {
    'blocksize': 'blocksize',
    'wal-blocksize': 'wal_blocksize',
    'segsize': 'segsize',
    'segsize-blocks': 'segsize_blocks',
    'pgport': 'pgport',
}


def option_default_gaps(root):
    """Format-defining options whose meson default differs from configure's."""
    with open(os.path.join(root, 'configure.ac')) as f:
        ac = f.read()
    with open(os.path.join(root, 'meson_options.txt')) as f:
        mo = f.read()
    gaps = []
    for ac_name, meson_name in FORMAT_OPTIONS.items():
        # PGAC_ARG_REQ(with, NAME, [...], [...], [var=$withval], [var=DEFAULT])
        m = re.search(r'PGAC_ARG_REQ\(with,\s*%s,.*?\[\w+=\$withval\],\s*'
                      r'\[\w+=([^\]]*)\]' % re.escape(ac_name), ac, re.S)
        n = re.search(r"option\('%s',.*?value:\s*'?([^',)\s]+)" % re.escape(meson_name),
                      mo, re.S)
        if not m or not n:
            gaps.append('cannot find the default of --with-%s in configure.ac or of '
                        '%s in meson_options.txt' % (ac_name, meson_name))
            continue
        if m.group(1).strip() != n.group(1).strip():
            gaps.append('--with-%s defaults to %s in configure.ac but %s defaults to %s '
                        'in meson_options.txt' % (ac_name, m.group(1).strip(),
                                                  meson_name, n.group(1).strip()))
    return gaps


def main(argv) -> int:
    root = argv[1] if len(argv) > 1 else '.'
    gaps = (install_gaps(root) + source_gaps(root) + default_contrib_gaps(root)
            + option_default_gaps(root))

    if not gaps:
        print('meson install parity: no gaps')
        return 0

    for g in gaps:
        print('::error::' + g)
    print()
    print('%d gap(s) between what the autoconf build builds and installs and'
          ' what meson does.' % len(gaps))
    print('Add the missing pieces to the meson.build that belongs to them, or,')
    print('if neither build should have them, record why in the exemption')
    print('tables of this script.')
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
