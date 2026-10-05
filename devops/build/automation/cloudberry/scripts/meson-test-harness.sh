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
#       Lay them into a source checkout, and do the two things configure
#       would have done there: copy GNUmakefile.in to GNUmakefile, and point
#       Makefile.global at an in-tree layout.
#
#   make-args <dir> <target>
#       Print the make arguments that run <target> in <dir> against that
#       tree without make rebuilding the harness itself.
#
# The last needs explaining. The Makefiles build their harness as a
# prerequisite of every test target -- pg_regress from pg_regress.o, regress.so
# from its objects -- and those objects do not exist, because meson built the
# harness, not make. make would recompile them. `-o <target>` tells make to
# treat a target as up to date and ignore its rules, which is exactly
# "someone else built this".
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
usage() { sed -n '/^#   pack/,/^#       tree without/p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

# Test directories whose build products make would otherwise have to build.
# src/test/regress is also where isolation2 and the contrib Makefiles look
# for gpdiff.pl and pg_regress, so it has to be complete even for suites that
# do not live there.
HARNESS_DIRS=(src/test/regress src/test/isolation src/test/isolation2)

# configure's outputs that the suites' Makefiles include, which meson renders
# at the same paths. contrib/interconnect's Makefile includes its own.
RENDERED=(src/Makefile.global src/Makefile.port
          contrib/interconnect/Makefile.interconnect)

# Every prerequisite, across the test directories and pgxs.mk, whose recipe
# compiles something. `all` covers the programs and modules each directory
# builds; `submake` is pgxs.mk's way of building pg_regress before a contrib
# installcheck; the hook modules are phony targets installcheck-good names
# directly; `checkprep` builds and installs a suite's EXTRA_INSTALL modules
# (src/test/recovery's pg_prewarm and friends), which the meson install
# already holds.
MAKE_SKIP=(all submake hooktest query_info_hook_test checkprep)

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
  for f in "${RENDERED[@]}"; do
    [ -f "${f}" ] || die "${builddir} has no ${f}"
  done
  {
    printf '%s\n' "${RENDERED[@]}"
    # Build products only: meson keeps its object files in *.p directories,
    # and a directory tests have already run in holds their output.
    for d in "${HARNESS_DIRS[@]}"; do
      [ -d "${d}" ] || continue
      find "${d}" \( -name '*.p' -o -name results -o -name tmp_check \
                     -o -name output_iso -o -name testrun \) -prune \
                  -o -type f -print
    done
  } | sort > "${manifest}"

  # Record where the tree was built, so unpack can rewrite the two absolute
  # paths Makefile.global carries.
  printf 'builddir=%s\n' "${builddir}" > meson-test-harness.info
  echo meson-test-harness.info >> "${manifest}"
  tar -czf "${tarball}" -T "${manifest}"
  rm -f meson-test-harness.info
  echo "packed $(($(wc -l < "${manifest}") - 1)) files into ${tarball}"
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
  rm -f "${srcdir}/meson-test-harness.info"

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
  # which make-args tells make to skip, and its suites COPY from
  # @abs_srcdir@/data. Make the link with the Makefile's own rule rather than
  # repeat its path here.
  make -s -C "${srcdir}/src/test/isolation2" data

  # The harness is only usable if pg_regress can find gpdiff.pl beside it and
  # agrees with it about the version; check that now rather than let every
  # test fail on it.
  "${srcdir}/src/test/regress/gpdiff.pl" --version >/dev/null \
    || die "gpdiff.pl does not run; is GPTest.pm missing from the harness?"
  echo "unpacked the meson test harness into ${srcdir}"
  ;;

make-args)
  dir=${1:?dir}; target=${2:?target}
  makefile=
  for m in GNUmakefile Makefile; do
    [ -f "${dir}/${m}" ] && { makefile="${dir}/${m}"; break; }
  done
  [ -n "${makefile}" ] || die "no Makefile in ${dir}"

  # Makefile.global makes every install and installcheck first run the
  # backend's header generation, and isolation2's suites go through
  # `install`. Nothing is compiled here, so there is nothing to generate
  # headers for: NO_GENERATED_HEADERS says so, exactly as pgxs.mk does for the
  # same reason.
  printf 'NO_GENERATED_HEADERS=yes '
  printf -- '-o %s ' "${MAKE_SKIP[@]}"
  echo "${target}"
  ;;

*)
  usage
  ;;
esac
