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

"""Compare exact query result multisets across baseline, Anser, and RPT."""

import argparse
from collections import Counter
from pathlib import Path
import subprocess

from survey import split_select_statements


def read_rows(psql, database, optimizer, query, anser, rpt):
    # COPY text escapes embedded newlines and represents NULL separately from
    # an empty string, so each output line identifies exactly one result row.
    sql = f"""SET optimizer = {optimizer};
SET anser.runtime_filter = {anser};
SET rpt.enable = {rpt};
COPY (
{query}
) TO STDOUT;
"""
    result = subprocess.run(
        [psql, "-Xq", "-v", "ON_ERROR_STOP=1", "-d", database],
        input=sql, text=True, capture_output=True, check=True,
    )
    return Counter(result.stdout.split("\n")[:-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--optimizer", choices=("off", "on"), default="off")
    parser.add_argument("queries", type=Path, nargs="+")
    args = parser.parse_args()
    failed = False

    for path in args.queries:
        statements = split_select_statements(path.read_text(encoding="utf-8"))
        if not statements:
            parser.error(f"{path}: no SELECT/WITH statements")
        for number, query in enumerate(statements, 1):
            try:
                baseline = read_rows(args.psql, args.database, args.optimizer,
                                     query, "off", "off")
                for mode, anser, rpt in (("anser", "on", "off"), ("rpt", "off", "on")):
                    rows = read_rows(args.psql, args.database, args.optimizer,
                                     query, anser, rpt)
                    missing = sum((baseline - rows).values())
                    extra = sum((rows - baseline).values())
                    print(f"{path.name}#{number} {mode}: missing={missing} extra={extra}")
                    failed |= bool(missing or extra)
            except subprocess.CalledProcessError as error:
                print(f"{path.name}#{number}: {error.stderr.strip()}")
                failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
