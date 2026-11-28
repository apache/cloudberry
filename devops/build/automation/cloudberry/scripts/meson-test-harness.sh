#!/usr/bin/env bash
# --------------------------------------------------------------------
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
# --------------------------------------------------------------------
#
# Run Cloudberry's make-driven regression suites against a meson build.
#
# The suites are defined in Makefiles -- installcheck-good, installcheck-
# isolation2 and the rest -- and they are the definition of what "passing"
# means for this project: build-cloudberry.yml runs them against the autoconf
# build. Re-encoding every target's schedules and options in meson would give
# two copies of each suite that must be kept in step forever, and a green run
# of the copy would not prove the thing that matters, which is that the meson
# build passes the same tests. So the tests stay where they are, and this
# script gives them what they expect instead.
#
# What they expect is an in-tree configured build: `make -C src/test/regress
# installcheck-good` reads src/Makefile.global, runs ./pg_regress, loads
# ./regress.so, and has psql shell out to ./twophase_pqexecparams. Every one
# of those is a build product. The meson build produces all of them, at the
# same paths relative to its build directory, so the job here is only to move
# them:
#
#   pack <builddir> <tarball>
#       Collect the harness from a meson build directory: the files meson
#       renders where configure would -- src/Makefile.global and the rest of
#       RENDERED below -- and everything meson built under the test
#       directories.
#
#   unpack <tarball> <srcdir>
#       Lay them into a source checkout, do the two things configure would
#       have done there -- copy GNUmakefile.in to GNUmakefile, point
#       Makefile.global at an in-tree layout -- and write
#       <srcdir>/.meson-harness/make, for the suites to run with first in
#       PATH.
#
# That last needs explaining. The Makefiles build their harness as a
# prerequisite of every test target -- pg_regress from pg_regress.o, regress.so
# from its objects -- and those objects do not exist, because meson built the
# harness, not make. make would recompile them. `-o <target>` tells make to
# treat a target as up to date and ignore its rules, which is exactly
# "someone else built this". But make does not pass -o on to the makes it
# starts, and several suites start make themselves: diskquota's installcheck
# is a CMake project that runs `make -C src/test/isolation2 install`, and
# PAX's targets call make by name. So the -o flags go into a wrapper that
# every make in the run resolves to, nested ones included.
#
# contrib and gpcontrib run in-tree too, as build-cloudberry.yml runs them,
# rather than with USE_PGXS=1. A dozen of Cloudberry's contrib Makefiles pass
# --init-file=$(top_srcdir)/src/test/regress/init_file, which under PGXS
# points into an install that has no init_file -- in either build system. So
# PGXS mode is not what these Makefiles support here, and a run in it would
# not be the run the project relies on.
#
# Writing into the source checkout is deliberate and is why this is meant for
# a disposable one, as CI's test job has: meson refuses to reuse a source tree
# that holds configure output. Use a separate clone or worktree locally.
# --------------------------------------------------------------------
set -euo pipefail

die() { echo "::error::$*" >&2; exit 1; }
usage() { sed -n '/^#   pack/,/^#       which the suites/p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

# Test directories whose build products make would otherwise have to build.
# src/test/regress is also where isolation2 and the contrib Makefiles look
# for gpdiff.pl and pg_regress, so it has to be complete even for suites that
# do not live there.
# src/test/modules, src/test/walrep and libpq's test programs are what the
# "install installcheck" suites install and run.
HARNESS_DIRS=(src/test/regress src/test/isolation src/test/isolation2
              src/test/modules src/test/walrep src/interfaces/libpq/test)

# Build products elsewhere that a suite's Makefile reaches for: the singlenode
# regress suite copies refint and autoinc out of contrib/spi, and diskquota's
# CMake project is pointed at the in-tree pg_config.
HARNESS_FILES=(contrib/spi/refint.so contrib/spi/autoinc.so
               src/bin/pg_config/pg_config)

# configure's outputs that the suites' Makefiles include, which meson renders
# at the same paths. contrib/interconnect's Makefile includes its own, and
# diskquota's CMake project reads the version out of src/include/pg_config.h
# when it is built in-tree.
RENDERED=(src/Makefile.global src/Makefile.port
          contrib/interconnect/Makefile.interconnect
          src/include/pg_config.h src/include/pg_config_ext.h
          src/include/pg_config_os.h)

# What a suite that compiles at test time needs from the build tree, where an
# in-tree build keeps it: the headers the build generates, which the sources'
# own headers include (utils/errcodes.h, catalog/pg_type_d.h, ...), and the
# static libraries programs link. Only MAKE_OWN_DIRS below compile anything.
BUILT_HEADERS=src/include
BUILT_LIBS=(src/port/libpgport.a src/common/libpgcommon.a)

# Every prerequisite, across the test directories and pgxs.mk, whose recipe
# compiles something. `all` covers the programs and modules each directory
# builds; `submake` is pgxs.mk's way of building pg_regress before a contrib
# installcheck; the hook modules are phony targets installcheck-good names
# directly; `checkprep` and `install-tests` build and install a suite's extra
# modules (src/test/recovery's pg_prewarm and friends, src/test/isolation's
# pageinspect), which the meson install already holds; and
# `generated-headers` is what PAX's isolation2 Makefile asks src/backend for
# before it builds isolation2, when nothing is compiled.
MAKE_SKIP=(all submake hooktest query_info_hook_test checkprep install-tests
           generated-headers)

# A list like that one fails open: a prerequisite it does not name is simply
# built by make, and a suite passes on something the meson build lacks. So
# make compiles through a guard, written by unpack, that fails any compile of
# a source in the checkout and names it.
#
# Except here. gpcontrib/anser is opt-in at configure time and neither CI
# workflow turns it on; its installcheck-with-install builds it, its test
# extension and its unit tests with make, in both. Under these directories make
# compiles with the real compiler, against BUILT_HEADERS and BUILT_LIBS, and
# skips nothing.
MAKE_OWN_DIRS=(gpcontrib/anser)

# Where unpack writes the make wrapper, relative to the checkout.
MAKE_DIR=.meson-harness

cmd=${1:-}; shift || true
case "${cmd}" in
pack)
  builddir=$(cd "${1:?builddir}" && pwd)
  tarball=${2:?tarball}
  case "${tarball}" in /*) ;; *) tarball="${PWD}/${tarball}" ;; esac
  [ -f "${builddir}/src/Makefile.global" ] \
    || die "${builddir} has no src/Makefile.global -- not a meson build directory?"

  cd "${builddir}"
  manifest=$(mktemp)
  trap 'rm -f "${manifest}"' EXIT
  for f in "${RENDERED[@]}" "${HARNESS_FILES[@]}" "${BUILT_LIBS[@]}"; do
    [ -f "${f}" ] || die "${builddir} has no ${f}"
  done
  {
    printf '%s\n' "${RENDERED[@]}" "${HARNESS_FILES[@]}" "${BUILT_LIBS[@]}"
    find "${BUILT_HEADERS}" -name '*.p' -prune -o -type f -name '*.h' -print
    # Build products only: meson keeps its object files in *.p directories,
    # and a directory tests have already run in holds their output.
    for d in "${HARNESS_DIRS[@]}"; do
      [ -d "${d}" ] || continue
      find "${d}" \( -name '*.p' -o -name results -o -name tmp_check \
                     -o -name output_iso -o -name testrun \) -prune \
                  -o -type f -print
    done
  } | sort > "${manifest}"

  tar -czf "${tarball}" -T "${manifest}"
  echo "packed $(wc -l < "${manifest}") files into ${tarball}"
  ;;

unpack)
  tarball=$(cd "$(dirname "${1:?tarball}")" && pwd)/$(basename "$1")
  srcdir=$(cd "${2:?srcdir}" && pwd)
  [ -f "${srcdir}/GNUmakefile.in" ] || die "${srcdir} is not a Cloudberry source tree"

  # -m: stamp every file with the time of extraction, not the time it was
  # built. The checkout is necessarily newer than the build -- CI's test job
  # checks out after the build job finished -- so with the build times, make
  # finds twophase_pqexecparams.c newer than twophase_pqexecparams and
  # recompiles it. The harness was built from this same source, so "newer
  # than the source" is the truth, and this is what makes make see it.
  tar -xzmf "${tarball}" -C "${srcdir}"

  # configure copies GNUmakefile.in verbatim; it has no substitutions. If it
  # ever gains one, a plain copy is no longer what configure does.
  if grep -q '@[A-Za-z_][A-Za-z_]*@' "${srcdir}/GNUmakefile.in"; then
    die "GNUmakefile.in now has @substitutions@; teach this script to render it"
  fi
  cp "${srcdir}/GNUmakefile.in" "${srcdir}/GNUmakefile"

  # meson renders Makefile.global for its own layout: a VPATH build whose
  # build directory is the meson one. Here the source tree is the build tree.
  sed -i \
    -e "s|^vpath_build = .*|vpath_build = no|" \
    -e "s|^abs_top_builddir = .*|abs_top_builddir = ${srcdir}|" \
    -e "s|^abs_top_srcdir = .*|abs_top_srcdir = ${srcdir}|" \
    "${srcdir}/src/Makefile.global"

  # isolation2 links ../regress/data into its own directory as part of `all`,
  # which the wrapper below tells make to skip, and its suites COPY from
  # @abs_srcdir@/data. Make the link with the Makefile's own rule rather than
  # repeat its path here.
  make -s -C "${srcdir}/src/test/isolation2" data

  # Every make the suites start goes through this, once its directory leads
  # PATH: exec -a keeps $(MAKE) as plain `make`, so the makes it starts find
  # the wrapper again rather than the real one.
  mkdir -p "${srcdir}/${MAKE_DIR}"

  # The compile guard, as $(CC) and $(CXX). It knows the compiler meson
  # configured from the Makefile.global meson rendered. It is not named cc:
  # this directory is on PATH, and a cc here would be the compiler that plain
  # make and CMake find there -- and the one the guard itself execs.
  for lang in CC CXX; do
    real=$(sed -n "s/^${lang} = //p" "${srcdir}/src/Makefile.global" | head -1)
    [ -n "${real}" ] || die "src/Makefile.global sets no ${lang}"
    guard="${srcdir}/${MAKE_DIR}/$(echo "${lang}" | tr '[:upper:]' '[:lower:]')-guard"
    cat > "${guard}" <<GUARD
#!/bin/bash
# Written by $(basename "$0"): make compiles through this, and a source in the
# checkout is something meson should have built for the harness. Not CMake's
# own probes, though: make exports CC to its recipes, so diskquota's CMake
# project identifies this as its compiler, with sources under CMakeFiles/.
for a in "\$@"; do
  case "\${a}" in
    *.c|*.cc|*.cpp|*.cxx)
      p=\$(cd "\$(dirname "\${a}")" 2>/dev/null && pwd)/\$(basename "\${a}")
      case "\${p}" in
        */CMakeFiles/*) ;;
        ${srcdir}/*)
          echo "::error::make compiled \${p#${srcdir}/} for a suite; the meson" \\
               "test harness lacks it, or MAKE_SKIP in meson-test-harness.sh" \\
               "does not name the target that builds it" >&2
          exit 1 ;;
      esac ;;
  esac
done
exec ${real} "\$@"
GUARD
    chmod +x "${guard}"
  done

  {
    echo '#!/bin/bash'
    echo "# Written by $(basename "$0"): meson built the test harness, not make."
    # Where this make runs: its -C options, applied in order to the
    # directory it was started in.
    echo 'dir=$PWD; prev='
    echo 'for a in "$@"; do'
    echo '  if [ "$prev" = -C ]; then dir=$(cd "$dir" && cd "$a" && pwd); fi'
    echo '  case "$a" in -C?*) dir=$(cd "$dir" && cd "${a#-C}" && pwd) ;; esac'
    echo '  prev=$a'
    echo 'done'
    printf 'case "${dir}/" in
'
    for d in "${MAKE_OWN_DIRS[@]}"; do
      printf '  %s/%s/*) exec -a make %s NO_GENERATED_HEADERS=yes "$@" ;;\n' \
        "${srcdir}" "${d}" "$(command -v make)"
    done
    printf 'esac
'
    printf 'exec -a make %s NO_GENERATED_HEADERS=yes CC=%s CXX=%s' \
      "$(command -v make)" "${srcdir}/${MAKE_DIR}/cc-guard" \
      "${srcdir}/${MAKE_DIR}/cxx-guard"
    printf -- ' -o %s' "${MAKE_SKIP[@]}"
    echo ' "$@"'
  } > "${srcdir}/${MAKE_DIR}/make"
  chmod +x "${srcdir}/${MAKE_DIR}/make"

  # The singlenode suites' `all` links or copies what other directories
  # built, and builds nothing of its own. Make those links with the
  # Makefiles' own rules, since the wrapper skips `all`.
  export PATH="${srcdir}/${MAKE_DIR}:${PATH}"
  make -s -C "${srcdir}/src/test/singlenode_regress" regress.so refint.so autoinc.so
  make -s -C "${srcdir}/src/test/singlenode_isolation2" data submake-isolation2

  # The harness is only usable if pg_regress can find gpdiff.pl beside it and
  # agrees with it about the version; check that now rather than let every
  # test fail on it.
  "${srcdir}/src/test/regress/gpdiff.pl" --version >/dev/null \
    || die "gpdiff.pl does not run; is GPTest.pm missing from the harness?"
  echo "unpacked the meson test harness into ${srcdir}"
  ;;

*)
  usage
  ;;
esac
