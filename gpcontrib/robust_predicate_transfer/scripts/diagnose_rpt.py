#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run RPT regressions with decision summaries and an independent watchdog.

Use an isolated test database with both libraries installed and preloaded.
The runner never terminates a server backend or restarts the cluster.
"""
import argparse
import json
import os
import re
from collections import Counter
from pathlib import Path
import subprocess
import time
import uuid


def instrument_sql(source, suite="forward"):
    """Keep regression assertions, but identify every dynamic query first."""
    source = source.replace("rpt_transfer_plan(", "pg_temp.rpt_diag_transfer_plan(")
    anchor = "DO $$\nDECLARE\n    opt text;"
    if suite == "forward" and source.count(anchor) != 1:
        raise ValueError("forward regression assertion block changed")
    helper = """CREATE FUNCTION pg_temp.rpt_diag_transfer_plan(q text) RETURNS text
LANGUAGE plpgsql AS $diag$
DECLARE
    dump text;
BEGIN
    RAISE NOTICE 'RPT STEP transfer_plan SQL=%', q;
    dump := public.rpt_transfer_plan(q);
    RAISE NOTICE 'RPT DUMP BEGIN SQL=%', replace(q, chr(10), ' ');
    RAISE NOTICE '%', dump;
    RAISE NOTICE 'RPT DUMP END';
    RETURN dump;
END
$diag$;
"""
    if suite != "forward":
        if "CREATE EXTENSION robust_predicate_transfer;" not in source:
            raise ValueError("backward regression assertion block changed")
        source = source.replace("CREATE EXTENSION robust_predicate_transfer;",
                                "CREATE EXTENSION robust_predicate_transfer;\n" + helper, 1)
        source = source.replace("    EXECUTE q INTO r;",
                                "    RAISE NOTICE 'RPT STEP results SQL=%', q;\n    EXECUTE q INTO r;")
        source = source.replace("    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) ' || q INTO plan;",
                                "    RAISE NOTICE 'RPT STEP explain SQL=%', q;\n"
                                "    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) ' || q INTO plan;")
        return source
    source = source.replace(anchor, helper + anchor)
    edits = {
        "    EXECUTE format('EXPLAIN (%s VERBOSE, FORMAT JSON) %s',":
        "    RAISE NOTICE 'RPT STEP explain analyze=% SQL=%', do_analyze, q;\n",
        "    EXECUTE format('SELECT string_agg(t::text, %L ORDER BY t::text) FROM (%s) t',":
        "    RAISE NOTICE 'RPT STEP results rpt=% SQL=%', rpt, q;\n",
        "        EXECUTE 'PREPARE rpt_fwd_prep AS ' || star;":
        "        RAISE NOTICE 'RPT STEP prepare optimizer=%', opt;\n",
        "            EXECUTE 'EXECUTE rpt_fwd_prep' INTO prep_n, prep_s;":
        "            RAISE NOTICE 'RPT STEP execute prepared optimizer=% run=%', opt, i;\n",
        "    FOR r IN SELECT f.v FROM rpt_fwd_fact f JOIN rpt_fwd_d1":
        "    RAISE NOTICE 'RPT STEP SPI dimension scan';\n",
        "    FOR r IN SELECT f.v FROM rpt_fwd_fact f JOIN rpt_fwd_dl":
        "    RAISE NOTICE 'RPT STEP SPI local chain';\n",
    }
    for anchor, marker in edits.items():
        if source.count(anchor) != 1:
            raise ValueError(f"forward regression instrumentation anchor changed: {anchor}")
        source = source.replace(anchor, marker + anchor)
    return source


def summarize_dump(dump):
    """Summarize only runtime decisions, excluding the legacy survey lines."""
    classes = []
    reasons = Counter()
    injected = 0
    cost = 0.0
    for line in dump.splitlines():
        if line.startswith("backward class "):
            classes.append(line)
            match = re.search(r"est_cost_ms=([0-9.]+)", line)
            if match and line.endswith("decision=inject"):
                cost += float(match.group(1))
        elif line.startswith("backward consumer ") and " decision=" in line:
            reason = line.split(" decision=", 1)[1]
            if reason == "inject":
                injected += 1
            else:
                reasons[reason] += 1
    return {"classes": len(classes), "injected_consumers": injected,
            "refused_consumers": dict(reasons), "estimated_cost_ms": cost,
            "forward": [line for line in dump.splitlines()
                        if line.startswith("forward component ")]}


def summarize_log(log):
    reports = []
    query = None
    lines = []
    for line in log.splitlines():
        # psql prefixes the first line of a NOTICE, but not continuation lines.
        line = re.sub(r"^.*NOTICE:\s+", "", line)
        if line.startswith("RPT DUMP BEGIN SQL="):
            query = line.split("SQL=", 1)[1]
            lines = []
        elif line == "RPT DUMP END" and query is not None:
            reports.append(dict(sql=query, **summarize_dump("\n".join(lines))))
            query = None
        elif query is not None:
            lines.append(line)
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("forward", "backward", "backward_faults"),
                        default="forward")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--wall-timeout", type=float, default=180)
    parser.add_argument("--cancel-grace", type=float, default=15)
    parser.add_argument("--emit-only", action="store_true")
    args = parser.parse_args()
    if args.wall_timeout <= 0 or args.cancel_grace < 0:
        parser.error("wall-timeout must be positive and cancel-grace nonnegative")
    args.output.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parents[1] / "sql" / ("rpt_" + args.suite + ".sql")
    application = "rpt_diagnostic_" + uuid.uuid4().hex
    workload = args.output / "workload.sql"
    prefix = ("\\set ON_ERROR_STOP on\n\\timing on\n"
              f"SET application_name = '{application}';\n"
              "SET client_min_messages = notice;\n"
              "SELECT pg_backend_pid() AS diagnostic_backend_pid;\n")
    workload.write_text(prefix + instrument_sql(source.read_text(), args.suite))
    if args.emit_only:
        print(workload)
        return 0

    command = [args.psql, "-X", "-v", "ON_ERROR_STOP=1", "-d", args.database]
    env = dict(os.environ)
    env["PGCONNECT_TIMEOUT"] = "5"

    def monitor(filename, sql):
        try:
            result = subprocess.run(command + ["-A", "-t", "-c", sql],
                                    env=env, capture_output=True, text=True, timeout=8)
            (args.output / filename).write_text(result.stdout + result.stderr)
            return result.returncode
        except subprocess.TimeoutExpired as error:
            (args.output / filename).write_text(f"monitor timed out: {error.timeout}s\n")
            return -1

    with (args.output / "client.log").open("w") as output:
        proc = subprocess.Popen(command + ["-e", "-f", str(workload)],
                                env=env, stdout=output, stderr=subprocess.STDOUT)
        start = time.monotonic()
        timed_out = False
        try:
            proc.wait(timeout=args.wall_timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            monitor("activity-before-cancel.json", "SELECT coalesce(json_agg(t), '[]'::json) FROM "
                    "(SELECT pid, application_name, state, wait_event_type, wait_event, "
                    "query_start, query FROM pg_stat_activity WHERE application_name = "
                    f"'{application}') t;")
            monitor("cancel.txt", "SELECT pid, pg_cancel_backend(pid) FROM pg_stat_activity "
                    f"WHERE application_name = '{application}';")
            try:
                proc.wait(timeout=args.cancel_grace)
            except subprocess.TimeoutExpired:
                monitor("activity-after-cancel.json", "SELECT coalesce(json_agg(t), '[]'::json) FROM "
                        "(SELECT pid, state, wait_event_type, wait_event, query FROM pg_stat_activity "
                        f"WHERE application_name = '{application}') t;")
                # Stop only this client. Leave a stuck server available for inspection.
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        result = {"application_name": application, "timed_out": timed_out,
                  "client_returncode": proc.returncode,
                  "elapsed_seconds": time.monotonic() - start}
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    reports = summarize_log((args.output / "client.log").read_text())
    (args.output / "decisions.json").write_text(json.dumps(reports, indent=2) + "\n")
    print(f"Saved diagnostic output in {args.output}")
    if timed_out:
        print("Watchdog expired. Server backends were not terminated; inspect activity and server logs.")
        return 2
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
