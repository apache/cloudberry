#!/usr/bin/env python3
#-------------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Plan benchmark queries only; compare RPT eligibility to Anser injection.

Files may contain multiple SELECT/WITH statements, counted separately.
TPC-H Q15 must use a read-only CTE instead of CREATE/DROP VIEW.
The caller must load schemas/statistics and install/preload both extensions.
No ANALYZE, query execution, schema creation or persistent GUC writes occur.
"""
import argparse
import csv
import json
from pathlib import Path
import re
import subprocess
import uuid

EXPECTED = {"JOB": 113, "TPC-H": 22, "TPC-DS": 103}
BEGIN_DUMP = "__RPT_SURVEY_DUMP__"
BEGIN_ANSER = "__RPT_SURVEY_ANSER__"
END = "__RPT_SURVEY_END__"


def split_select_statements(sql):
    """Split instantiated benchmark SQL without splitting quoted semicolons.

    Keep comments in the statement text. Comment-only fragments are ignored.
    Reject DDL up front, so standard Q15 gets an actionable normalization error.
    This is a lexer, not a SQL parser; the server validates each SELECT later.
    """
    statements = []
    start = i = 0
    state = "normal"
    depth = 0
    dollar = ""
    escape_string = False
    first_keyword = None
    while i < len(sql):
        ch = sql[i]
        pair = sql[i:i + 2]
        if state == "line":
            if ch == "\n":
                state = "normal"
        elif state == "block":
            if pair == "/*":
                depth += 1
                i += 1
            elif pair == "*/":
                depth -= 1
                i += 1
                if depth == 0:
                    state = "normal"
        elif state == "dollar":
            if sql.startswith(dollar, i):
                i += len(dollar) - 1
                state = "normal"
        elif state in ("single", "double"):
            quote = "'" if state == "single" else '"'
            if ch == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    i += 1
                else:
                    state = "normal"
            elif state == "single" and escape_string and ch == "\\":
                i += 1
        elif pair == "--":
            state = "line"
            i += 1
        elif pair == "/*":
            state = "block"
            depth = 1
            i += 1
        elif ch == ";":
            if first_keyword is not None:
                statements.append((sql[start:i].strip(), first_keyword))
            start = i + 1
            first_keyword = None
        elif not ch.isspace():
            if first_keyword is None:
                word = re.match(r"[A-Za-z_]+", sql[i:])
                first_keyword = word.group().upper() if word else ""
            if ch == "'":
                state = "single"
                escape_string = (i > 0 and sql[i - 1] in "eE" and
                                 (i < 2 or not (sql[i - 2].isalnum() or sql[i - 2] == "_")))
            elif ch == '"':
                state = "double"
            elif ch == "$":
                tag = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
                if tag:
                    dollar = tag.group()
                    state = "dollar"
                    i += len(dollar) - 1
        i += 1
    if state not in ("normal", "line"):
        raise ValueError("unterminated SQL quote or comment")
    if first_keyword is not None:
        statements.append((sql[start:].strip(), first_keyword))
    for statement, keyword in statements:
        if keyword not in ("SELECT", "WITH"):
            raise ValueError("benchmark statements must be SELECT/WITH; use the read-only CTE form of TPC-H Q15 instead of view DDL")
    return [statement for statement, keyword in statements]


def load_queries(directory):
    result = []
    for path in sorted(directory.rglob("*.sql")):
        try:
            statements = split_select_statements(path.read_text(encoding="utf-8"))
        except ValueError as error:
            raise ValueError(str(path) + ": " + str(error)) from error
        for index, statement in enumerate(statements, 1):
            result.append(dict(source_file=str(path.relative_to(directory)),
                               statement=index, sql=statement))
    return result


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def survey_sql(query, optimizer, timeout_ms, search_path):
    tag = "$rpt_" + uuid.uuid4().hex + "$"
    query = query.strip().rstrip(";")
    return "\n".join([
        "BEGIN READ ONLY;",
        "SET LOCAL standard_conforming_strings = on;",
        "SET LOCAL statement_timeout = " + str(timeout_ms) + ";",
        "SET LOCAL optimizer = " + sql_literal(optimizer) + ";",
        "SELECT set_config('search_path', " + sql_literal(search_path) + ", true);",
        "SET LOCAL rpt.enable = off;",
        "SET LOCAL rpt.backward = on;",
        "SET LOCAL anser.enable = on;",
        "SET LOCAL anser.runtime_filter = off;",
        "SELECT " + sql_literal(BEGIN_DUMP) + ";",
        "SELECT rpt_transfer_plan(" + tag + query + tag + ");",
        "SELECT " + sql_literal(BEGIN_ANSER) + ";",
        "SET LOCAL anser.runtime_filter = on;",
        "EXPLAIN (FORMAT JSON) " + query + "\n;",
        "SELECT " + sql_literal(END) + ";",
        "ROLLBACK;",
    ])


def parse_dump(dump):
    if dump.strip() == "no transfer regions":
        return dict(classes=0, root_classes=0, global_edges=0, rule9=0,
                    strict=0, eligible=0, known_rows_removed=0.0,
                    unknown_estimates=0, runtime_survey_available=True,
                    runtime_classes=0, runtime_root_classes=0, runtime_strict=0,
                    runtime_eligible=0, runtime_injected_consumers=0, runtime_refusals={})
    lines = [line for line in dump.splitlines() if line.startswith("survey ")]
    if len(lines) != 1:
        raise ValueError("missing survey metrics; install the analysis/survey build")
    metrics = dict(re.findall(r"([a-z0-9_]+)=([^ ]+)", lines[0]))
    fields = ("classes", "root_classes", "global_edges", "rule9", "strict",
              "eligible", "unknown_estimates")
    result = {key: int(metrics[key]) for key in fields}
    result["known_rows_removed"] = float(metrics["known_rows_removed"])
    if (any(value < 0 for value in result.values()) or
            result["root_classes"] > result["classes"] or
            not result["eligible"] <= result["strict"] <= result["rule9"] <= result["global_edges"] or
            result["unknown_estimates"] > result["strict"]):
        raise ValueError("inconsistent survey metrics")
    runtime = [line for line in dump.splitlines() if line.startswith("runtime survey ")]
    result["runtime_survey_available"] = bool(runtime)
    if runtime:
        if len(runtime) != 1:
            raise ValueError("duplicate runtime survey metrics")
        fields = dict(re.findall(r"([a-z_]+)=([0-9]+)", runtime[0]))
        for key in ("classes", "root_classes", "strict", "eligible"):
            result["runtime_" + key] = int(fields[key])
        consumers = [line for line in dump.splitlines()
                     if line.startswith("backward consumer ") and " decision=" in line]
        result["runtime_injected_consumers"] = sum(line.endswith(" decision=inject") for line in consumers)
        reasons = {}
        for line in consumers:
            reason = line.split(" decision=", 1)[1]
            if reason != "inject":
                reasons[reason] = reasons.get(reason, 0) + 1
        result["runtime_refusals"] = reasons
    return result


def count_anser_producers(value):
    if isinstance(value, dict):
        own = int(value.get("Custom Plan Provider") == "Anser Bloom Producer")
        return own + sum(count_anser_producers(child) for child in value.values())
    if isinstance(value, list):
        return sum(count_anser_producers(child) for child in value)
    return 0


def optimizer_used(value):
    if isinstance(value, dict):
        if "Optimizer" in value:
            return str(value["Optimizer"])
        for child in value.values():
            found = optimizer_used(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = optimizer_used(child)
            if found:
                return found
    return ""


def parse_output(output):
    before, sep, rest = output.partition(BEGIN_DUMP + "\n")
    if not sep:
        raise ValueError("missing RPT output marker")
    dump, sep, rest = rest.partition(BEGIN_ANSER + "\n")
    if not sep:
        raise ValueError("missing Anser output marker")
    explain, sep, after = rest.partition(END)
    if not sep:
        raise ValueError("missing completion marker")
    plan = json.loads(explain)
    metrics = parse_dump(dump)
    metrics["anser_joins"] = count_anser_producers(plan)
    metrics["anser_optimizer_used"] = optimizer_used(plan)
    return metrics, dump, plan


def summarize(records, complete):
    lines = ["# RPT backward eligibility survey", "",
             "Survey complete: " + ("yes" if complete else "no"), "",
             "Planning only. Each SELECT statement is counted separately, not each file. "
             "No benchmark query was executed. Counts labelled "
             "strict precede the per-class fan-out cap; eligible follows it.", "",
             "| Suite | Optimizer | Planned/total | RPT strict SELECTs | RPT capped SELECTs | Anser SELECTs |",
             "| --- | --- | --- | --- | --- | --- |"]
    for suite in EXPECTED:
        for optimizer in ("off", "on"):
            group = [row for row in records if row["suite"] == suite and
                     row["optimizer"] == optimizer]
            good = [row for row in group if row["status"] == "ok"]
            strict = sum(row["strict"] > 0 for row in good)
            eligible = sum(row["eligible"] > 0 for row in good)
            anser = sum(row["anser_joins"] > 0 for row in good)
            lines.append("| %s | %s | %d/%d | %d/%d | %d/%d | %d/%d |" % (
                suite, optimizer, len(good), len(group), strict, len(group),
                eligible, len(group), anser, len(group)))
    lines += ["", "Failures remain in the denominator. See results.json/CSV "
              "and raw artifacts for refusals, errors and optimizer fallback.", "",
              "NDV estimates are rough: known_rows_removed sums scan-plan-row "
              "estimates across strict edges, so classes can double-count a "
              "table. unknown_estimates counts missing statistics; it is not zero benefit.", "",
              "The survey is evidence for the plan's go/no-go decision; this "
              "script never authorizes runtime injection."]
    runtime = [row for row in records if row.get("runtime_survey_available")]
    if runtime:
        lines += ["", "Largest-root runtime schedule (successful runtime dumps only):", "",
                  "| Suite | Optimizer | Queries | Root classes | Proven consumers | Selected consumers |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for suite in sorted({row["suite"] for row in runtime}):
            for optimizer in ("off", "on"):
                group = [row for row in runtime if row["suite"] == suite and row["optimizer"] == optimizer]
                lines.append("| %s | %s | %d | %d | %d | %d |" % (
                    suite, optimizer, len(group), sum(row["runtime_root_classes"] for row in group),
                    sum(row["runtime_strict"] for row in group),
                    sum(row["runtime_injected_consumers"] for row in group)))
        lines += ["", "Runtime refusal reasons are retained per query in results.json."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("job", "tpch", "tpcds"):
        parser.add_argument("--" + flag, required=True, type=Path,
                            help="directory containing instantiated SELECT/WITH .sql files")
    for flag in ("job", "tpch", "tpcds"):
        parser.add_argument("--" + flag + "-dbname", help="override database for this suite")
        parser.add_argument("--" + flag + "-search-path", help="override search_path for this suite")
    parser.add_argument("--dbname", default="postgres", help="database or libpq service; credentials may use PG* environment")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--search-path", default="public")
    parser.add_argument("--statement-timeout-ms", type=int, default=30000)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-partial", action="store_true", help="pilot only; never marks the blocking survey complete")
    args = parser.parse_args()
    if args.statement_timeout_ms <= 0:
        parser.error("statement timeout must be positive")
    suites = {"JOB": args.job, "TPC-H": args.tpch, "TPC-DS": args.tpcds}
    connections = {"JOB": args.job_dbname or args.dbname,
                   "TPC-H": args.tpch_dbname or args.dbname,
                   "TPC-DS": args.tpcds_dbname or args.dbname}
    schemas = {"JOB": args.job_search_path or args.search_path,
               "TPC-H": args.tpch_search_path or args.search_path,
               "TPC-DS": args.tpcds_search_path or args.search_path}
    queries = {}
    for name, directory in suites.items():
        if not directory.is_dir():
            parser.error("not a query directory: " + str(directory))
        try:
            queries[name] = load_queries(directory)
        except (OSError, ValueError) as error:
            parser.error(str(error))
        if len(queries[name]) != EXPECTED[name] and not args.allow_partial:
            parser.error("%s needs %d SELECT statements, found %d; split TPC-DS variants and use Q15 CTE form; --allow-partial is for pilots only" %
                         (name, EXPECTED[name], len(queries[name])))
    args.output.mkdir(parents=True, exist_ok=True)
    raw = args.output / "raw"
    raw.mkdir(exist_ok=True)
    records = []
    for suite, statements in queries.items():
        for optimizer in ("off", "on"):
            for index, statement in enumerate(statements):
                identity = "%s-%s-%03d" % (suite, optimizer, index + 1)
                row = dict(suite=suite, optimizer=optimizer,
                           query=statement["source_file"] + "#" + str(statement["statement"]),
                           source_file=statement["source_file"],
                           statement=statement["statement"], status="error")
                try:
                    query = statement["sql"]
                    result = subprocess.run(
                        [args.psql, "-X", "-q", "-A", "-t", "--no-password", "-v", "ON_ERROR_STOP=1", "-d", connections[suite]],
                        input=survey_sql(query, optimizer, args.statement_timeout_ms, schemas[suite]),
                        text=True, capture_output=True,
                        timeout=2 * args.statement_timeout_ms / 1000 + 15)
                    if result.returncode:
                        # Do not record connection strings or arbitrary stderr,
                        # which can contain credentials. Raw query plans only.
                        raise ValueError("psql failed (exit %d); inspect the connection/query on the cluster" % result.returncode)
                    metrics, dump, explain = parse_output(result.stdout)
                    row.update(metrics)
                    row["status"] = "ok"
                    (raw / (identity + ".rpt.txt")).write_text(dump, encoding="utf-8")
                    (raw / (identity + ".anser.json")).write_text(json.dumps(explain, indent=2), encoding="utf-8")
                except (OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
                    if isinstance(error, subprocess.TimeoutExpired):
                        row["error"] = "psql timed out"
                    elif isinstance(error, OSError):
                        row["error"] = type(error).__name__
                    else:
                        row["error"] = str(error)
                records.append(row)
                print("%s: %s" % (identity, row["status"]), flush=True)
    complete = (not args.allow_partial and
                all(len(queries[name]) == count for name, count in EXPECTED.items()) and
                all(row["status"] == "ok" for row in records))
    report = dict(survey_complete=complete, expected_statements=EXPECTED, results=records)
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    fields = sorted({field for row in records for field in row})
    with (args.output / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    (args.output / "summary.md").write_text(summarize(records, complete), encoding="utf-8")
    return int(any(row["status"] != "ok" for row in records))


if __name__ == "__main__":
    raise SystemExit(main())
