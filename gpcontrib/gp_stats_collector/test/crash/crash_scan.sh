#!/bin/bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
# crash_scan.sh <log-root>
#
# The crash gate for the tracer run.  Independent of test diffs: it decides
# whether Cloudberry survived the tracing.  Exits non-zero (and prints why) on
# any of:
#   - a crash marker in a coordinator/segment log under <log-root>;
#   - a segment reported down / resyncing by gpstate -e;
#   - the coordinator failing to answer a trivial query.
#
# The demo cluster env (gpdemo-env.sh) must be sourced before calling.
# --------------------------------------------------------------------
set -uo pipefail

log_root="${1:?usage: crash_scan.sh <log-root>}"
status=0

# Unambiguous crash markers only.  The workload (installcheck-parallel) injects
# no faults, so a PANIC / signal here is a genuine crash, not a fault-injection
# recovery test.  Excluded on purpose:
#   - plain FATAL: routine during regression (missing role, duplicate object).
#   - "server closed the connection unexpectedly" / "the database system is in
#     recovery mode": routine mirror/walreceiver churn on every restart
#     (gpstop -ar), not a crash.
# Real crashes are caught here (PANIC, postmaster-wide crash restart, a process
# killed by a signal) and corroborated by gpstate -e + SELECT 1 below.
patterns='PANIC|terminating connection because of crash of another server process|was terminated by signal [0-9]'

echo "== crash_scan: log markers under ${log_root} =="
if hits=$(grep -rERn "${patterns}" "${log_root}" 2>/dev/null); then
    if [ -n "${hits}" ]; then
        echo "CRASH: crash markers found:"
        echo "${hits}" | head -50
        status=1
    fi
fi
[ "${status}" -eq 0 ] && echo "  none"

echo "== crash_scan: segment health (gpstate -e) =="
if command -v gpstate >/dev/null 2>&1; then
    gpstate_out=$(gpstate -e 2>&1 || true)
    echo "${gpstate_out}" | tail -30
    if echo "${gpstate_out}" | grep -Eiq 'down|resynchroniz|not synchronized|Unsynchronized'; then
        echo "CRASH: gpstate reports segments down / resyncing"
        status=1
    fi
else
    echo "  gpstate not on PATH (env not sourced?)"
    status=1
fi

echo "== crash_scan: coordinator responsive? =="
if echo 'SELECT 1;' | psql -X -q -A -t -d postgres >/dev/null 2>&1; then
    echo "  SELECT 1 ok"
else
    echo "CRASH: coordinator did not answer SELECT 1"
    status=1
fi

if [ "${status}" -eq 0 ]; then
    echo "== crash_scan: PASS (cluster healthy) =="
else
    echo "== crash_scan: FAIL (see markers above) =="
fi
exit "${status}"
