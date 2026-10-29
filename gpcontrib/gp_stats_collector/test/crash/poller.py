#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# poller.py -- crash-test tracer for gp_stats_collector.
#
# A single process that, in a loop, finds every active client backend in
# pg_stat_activity and calls gpsc.pg_query_state(pid, trace_id) against it --
# "trace everything that moves" -- while a heavy test suite runs concurrently.
#
# Deliberately single-process: the extension does not support overlapping polls
# of the same pid, so no pid is ever polled from two sessions at once.  A
# per-pid cooldown keeps a gap larger than a collection's latency, so even this
# one session never re-fires a pid whose previous poll may still be in flight.
#
# Errors from a poll (backend gone, permission gate, races) are swallowed on
# purpose: this test cares about cluster liveness, not trace correctness.
#
# Stops when the --stop-file appears.  Talks to the server through psql, so it
# needs no Python database driver; the gpdemo environment must be sourced first
# (PGPORT etc.).

import argparse
import os
import secrets
import subprocess
import sys
import time

APP_NAME = "gpsc_crash_poller"

# Backends carrying this application_name are our own psql calls; never trace
# them, or the poller would chase its own tail.
ACTIVE_PIDS_SQL = (
    "SELECT pid FROM pg_stat_activity "
    "WHERE state = 'active' "
    "AND backend_type = 'client backend' "
    "AND coalesce(application_name, '') <> '{app}' "
    "AND pid <> pg_backend_pid();"
).format(app=APP_NAME)


def psql(dbname, sql, timeout):
    """Run one SQL statement through psql; return (rc, stdout). Never raises."""
    env = dict(os.environ, PGAPPNAME=APP_NAME)
    try:
        proc = subprocess.run(
            ["psql", "-X", "-q", "-A", "-t", "-d", dbname, "-c", sql],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            text=True,
        )
        return proc.returncode, proc.stdout
    except subprocess.TimeoutExpired:
        return -1, "psql timeout"
    except Exception as exc:  # noqa: BLE001 -- liveness test, swallow everything
        return -1, str(exc)


def main():
    ap = argparse.ArgumentParser(description="gp_stats_collector crash-test poller")
    ap.add_argument("--dbname", default="postgres",
                    help="stable DB to read pg_stat_activity from (default: postgres)")
    ap.add_argument("--stop-file", required=True,
                    help="poller exits once this path exists")
    ap.add_argument("--cooldown", type=float, default=1.0,
                    help="min seconds between two polls of the same pid")
    ap.add_argument("--round-sleep", type=float, default=0.05,
                    help="seconds to sleep between scan rounds")
    ap.add_argument("--call-timeout", type=float, default=10.0,
                    help="per-psql-call timeout in seconds")
    ap.add_argument("--log-every", type=int, default=100,
                    help="print a heartbeat every N rounds")
    args = ap.parse_args()

    last_polled = {}
    rounds = 0
    polls = 0
    errors = 0
    logged_error = False  # print the first poll error body once, for diagnosis
    started = time.monotonic()

    print("poller: start (app_name={}, cooldown={}s)".format(APP_NAME, args.cooldown),
          flush=True)

    while not os.path.exists(args.stop_file):
        rounds += 1
        rc, out = psql(args.dbname, ACTIVE_PIDS_SQL, args.call_timeout)
        if rc != 0:
            # The coordinator may be momentarily busy/restarting a session; the
            # crash gate, not the poller, decides whether that is fatal.
            errors += 1
            time.sleep(args.round_sleep)
            continue

        now = time.monotonic()
        for line in out.splitlines():
            pid = line.strip()
            if not pid:
                continue
            if now - last_polled.get(pid, 0.0) < args.cooldown:
                continue
            trace_hex = secrets.token_hex(16)  # exactly 16 bytes -> bytea
            sql = ("SELECT gpsc.pg_query_state({pid}, '\\x{tid}'::bytea);"
                   .format(pid=pid, tid=trace_hex))
            prc, pout = psql(args.dbname, sql, args.call_timeout)
            last_polled[pid] = now
            polls += 1
            if prc != 0:
                errors += 1  # gate/race/backend-gone: expected, not fatal here
                if not logged_error:
                    # A wall of errors usually means a setup problem (e.g. the
                    # function is missing); surface the first one so the log is
                    # not opaque.
                    print("poller: first poll error: {}".format(pout.strip()),
                          flush=True)
                    logged_error = True

        if rounds % args.log_every == 0:
            print("poller: rounds={} polls={} errors={} tracked_pids={} elapsed={:.0f}s"
                  .format(rounds, polls, errors, len(last_polled),
                          time.monotonic() - started),
                  flush=True)

        time.sleep(args.round_sleep)

    print("poller: stop (rounds={} polls={} errors={} elapsed={:.0f}s)"
          .format(rounds, polls, errors, time.monotonic() - started),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
