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
# Smoke test for a meson-built Apache Cloudberry installation.
#
# Checks that the install is complete, that initdb succeeds (which exercises
# catalog generation end to end), and that the MPP-specific pieces are really
# there rather than merely linked.
#
# Usage: meson-smoke-test.sh <install-prefix> <build-dir>
#
# The build directory is read, not written: which optional components an
# install should contain depends on how it was configured, so the expected set
# is taken from meson rather than hardcoded. That is what lets one smoke test
# serve every entry in the build matrix instead of only the full one.
# --------------------------------------------------------------------
set -euo pipefail

PREFIX="${1:?usage: $0 <install-prefix> <build-dir>}"
BUILDDIR="${2:?usage: $0 <install-prefix> <build-dir>}"
export LD_LIBRARY_PATH="${PREFIX}/lib:${LD_LIBRARY_PATH:-}"

fail() { echo "::error::$*" >&2; exit 1; }

INTRO="${BUILDDIR}/meson-info/intro-buildoptions.json"
[ -f "${INTRO}" ] || fail "no ${INTRO} -- is ${BUILDDIR} a meson build directory?"

# True when the named option was configured on. Feature options read
# "enabled"; booleans read true. An option meson does not know is a typo here,
# and is reported rather than quietly treated as off.
enabled() {
  python3 - "$1" "${INTRO}" <<'PY'
import json, sys
name, intro = sys.argv[1], sys.argv[2]
options = {o["name"]: o["value"] for o in json.load(open(intro))}
if name not in options:
    sys.stderr.write("::error::no such meson option: %s\n" % name)
    sys.exit(2)
sys.exit(0 if options[name] in (True, "enabled") else 1)
PY
}

# --------------------------------------------------------------------
# 1. Installed artifacts
# --------------------------------------------------------------------
check_bin() {
  [ -x "${PREFIX}/bin/$1" ] || fail "missing bin/$1"
  echo "  ok  bin/$1"
}

echo "== checking installed binaries =="
for b in postgres initdb pg_ctl psql pg_dump pg_upgrade gpfts pg_alterckey; do
  check_bin "${b}"
done
# Gated on the option that builds each one, so that a configuration which did
# not ask for a component is not failed for not having it -- and a
# configuration that did ask still has to produce it.
if enabled gpfdist;   then check_bin gpfdist;     else echo "  skip  gpfdist (disabled)"; fi
if enabled mapreduce; then check_bin gpmapreduce; else echo "  skip  gpmapreduce (disabled)"; fi
if enabled gpcloud;   then check_bin gpcheckcloud; else echo "  skip  gpcheckcloud (disabled)"; fi

echo "== checking GP extensions =="
for e in interconnect gp_exttable_fdw gp_toolkit gp_distribution_policy; do
  [ -f "${PREFIX}/share/postgresql/extension/${e}.control" ] || fail "missing extension ${e}"
  echo "  ok  ${e}"
done
if enabled pxf; then
  [ -f "${PREFIX}/share/postgresql/extension/pxf_fdw.control" ] || fail "missing extension pxf_fdw"
  echo "  ok  pxf_fdw"
else
  echo "  skip  pxf_fdw (disabled)"
fi

echo "== checking PAX =="
if enabled pax; then
  [ -f "${PREFIX}/share/postgresql/cdb_init.d/pax-cdbinit--1.0.sql" ] \
    || fail "missing generated pax-cdbinit--1.0.sql"
  echo "  ok  pax-cdbinit--1.0.sql"
else
  echo "  skip  pax (disabled)"
fi

echo "== checking library layout =="
# These paths are not interchangeable: cloudberry-env.sh hardcodes
# PYTHONPATH=$GPHOME/lib/python and LD_LIBRARY_PATH=$GPHOME/lib, so an install
# that lands them in lib64 (meson's default on RHEL) is broken even though
# every binary is present. Check the paths, not just the files.
[ -d "${PREFIX}/lib/postgresql" ] || fail "no lib/postgresql -- is libdir set to lib?"
echo "  ok  lib/postgresql"
[ -f "${PREFIX}/lib/python/gppylib/gpversion.py" ] \
  || fail "gppylib is not under lib/python -- is libdir set to lib?"
echo "  ok  lib/python/gppylib"
[ -d "${PREFIX}/lib64" ] && fail "install split across lib and lib64"
echo "  ok  nothing in lib64"

echo "== checking gpMgmt =="
[ -f "${PREFIX}/cloudberry-env.sh" ] || fail "missing cloudberry-env.sh"
echo "  ok  cloudberry-env.sh"
for b in gpinitsystem gpstart gpstop gpstate; do
  [ -x "${PREFIX}/bin/${b}" ] || fail "missing bin/${b}"
  echo "  ok  bin/${b}"
done
[ -f "${PREFIX}/bin/lib/gpdemo/demo_cluster.sh" ] || fail "missing gpdemo scripts"
echo "  ok  bin/lib/gpdemo"

echo "== checking generated catalog data =="
for f in system_views_gp.sql cdb_init.d/cdb_schema.sql postgres.bki; do
  [ -f "${PREFIX}/share/postgresql/${f}" ] || fail "missing share/postgresql/${f}"
  echo "  ok  ${f}"
done

"${PREFIX}/bin/postgres" --version

# --------------------------------------------------------------------
# 2. initdb
# --------------------------------------------------------------------
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT
DATADIR="${WORKDIR}/pgdata"

echo "== running initdb =="
"${PREFIX}/bin/initdb" -D "${DATADIR}" --no-locale --encoding=UTF8 >"${WORKDIR}/initdb.log" 2>&1 || {
  tail -30 "${WORKDIR}/initdb.log" >&2
  fail "initdb failed"
}
echo "  ok  initdb"

# --------------------------------------------------------------------
# 3. Runtime checks, in single-user mode
# --------------------------------------------------------------------
# A full postmaster needs an MPP identity (-b dbid) and a segment
# configuration; single-user mode is enough to prove the catalogs, the
# optimizer and the storage layer are functional.
# optimizer=on is fatal in a build without ORCA -- "ORCA is not supported by
# this build" -- so it goes on only when there is an optimizer to ask for.
PG_OPTS=(-c gp_role=utility)
if enabled orca; then PG_OPTS+=(-c optimizer=on); fi

run_sql() {
  printf '%s\n' "$1" | "${PREFIX}/bin/postgres" --single -D "${DATADIR}" \
    "${PG_OPTS[@]}" postgres 2>&1
}

echo "== checking GP catalogs =="
run_sql "select count(*) as n from pg_class where relname in ('gp_segment_configuration', 'gp_distribution_policy', 'pg_resgroup', 'pg_appendonly');" \
  >"${WORKDIR}/catalogs.out"
grep -q 'n = "4"' "${WORKDIR}/catalogs.out" || {
  cat "${WORKDIR}/catalogs.out" >&2
  fail "expected 4 GP catalogs"
}
echo "  ok  4/4 GP catalogs present"

echo "== checking the gp_* views generated from system_views_gp.in =="
run_sql "select count(*) as n from pg_views where viewname like 'gp\\_%';" >"${WORKDIR}/views.out"
grep -qE 'n = "[1-9][0-9]*"' "${WORKDIR}/views.out" || {
  cat "${WORKDIR}/views.out" >&2
  fail "no gp_* views were created"
}
echo "  ok  $(sed -n 's/.*n = "\([0-9]*\)".*/\1/p' "${WORKDIR}/views.out" | head -1) gp_* views"

echo "== checking the version string =="
# gpMgmt parses select version() with gpversion.py, which insists on
# "(Apache Cloudberry <version> build <build>)". Upstream's PG_VERSION_STR has
# no such part, so a build that inherits it starts and serves queries but makes
# every management utility fail; check the string, not just that it exists.
run_sql "select version();" >"${WORKDIR}/version.out"
grep -q "(Apache Cloudberry " "${WORKDIR}/version.out" || {
  cat "${WORKDIR}/version.out" >&2
  fail "version() does not identify Cloudberry -- is PG_VERSION_STR upstream's?"
}
echo "  ok  $(sed -n 's/.*(\(Apache Cloudberry [^)]*\)).*/\1/p' "${WORKDIR}/version.out" | head -1)"

echo "== checking ORCA =="
if enabled orca; then
  run_sql "select gp_opt_version();" >"${WORKDIR}/orca.out"
  grep -q "GPOPT version" "${WORKDIR}/orca.out" || {
    cat "${WORKDIR}/orca.out" >&2
    fail "ORCA is not available"
  }
  echo "  ok  $(sed -n 's/.*gp_opt_version = "\([^"]*\)".*/\1/p' "${WORKDIR}/orca.out" | head -1)"
else
  echo "  skip  orca (disabled)"
fi

echo "== checking append-only storage =="
run_sql "create table t_ao(a int) with (appendonly=true);" >/dev/null
run_sql "insert into t_ao select generate_series(1,100);" >/dev/null
run_sql "select count(*) as n from t_ao;" >"${WORKDIR}/ao.out"
grep -q 'n = "100"' "${WORKDIR}/ao.out" || {
  cat "${WORKDIR}/ao.out" >&2
  fail "append-only table did not return 100 rows"
}
echo "  ok  append-only table returned 100 rows"

echo
echo "Smoke test passed."
