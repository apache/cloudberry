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

"""Offline checks for the forward diagnostic runner and watchdog."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "diagnose_rpt.py"
spec = importlib.util.spec_from_file_location("diagnose_rpt", SCRIPT)
diag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diag)


class DiagnosticTest(unittest.TestCase):
    def test_instrumentation_retains_assertions_and_reports_dynamic_queries(self):
        source = (ROOT / "sql" / "rpt_forward.sql").read_text()
        output = diag.instrument_sql(source)
        self.assertIn("IF pg_temp.rpt_fwd_removed(plan) <= 0 THEN", output)
        self.assertIn("RPT STEP explain analyze=% SQL=%", output)
        self.assertIn("RPT STEP results rpt=% SQL=%", output)
        self.assertIn("RPT STEP transfer_plan SQL=%", output)
        self.assertIn("dump := public.rpt_transfer_plan(q);", output)
        self.assertIn("RPT STEP SPI local chain", output)

    def test_changed_fixture_fails_instead_of_silently_losing_markers(self):
        with self.assertRaises(ValueError):
            diag.instrument_sql("SELECT 1;")

    def test_backward_decision_summary(self):
        dump = """backward consumer 2 producer 1 eligible=yes reason=eligible
backward class root=1 rep=1.2 consumers=1 bytes=1048576 est_cost_ms=12.900 decision=inject
backward consumer 2 root=1 rep=1.2 key=rpt_b_1_1_2 defer_first=yes rows_removed=98 decision=inject
backward consumer 3 root=1 rep=1.2 key=- defer_first=no rows_removed=-1 decision=sort between consumer and join
"""
        report = diag.summarize_dump(dump)
        self.assertEqual(report["classes"], 1)
        self.assertEqual(report["injected_consumers"], 1)
        self.assertEqual(report["refused_consumers"], {"sort between consumer and join": 1})
        self.assertAlmostEqual(report["estimated_cost_ms"], 12.9)
        log = "NOTICE: RPT DUMP BEGIN SQL=SELECT 1\nNOTICE: " + dump + "NOTICE: RPT DUMP END\n"
        self.assertEqual(diag.summarize_log(log), [dict(sql="SELECT 1", **report)])

    def test_backward_instrumentation(self):
        for suite in ("backward", "backward_faults"):
            output = diag.instrument_sql((ROOT / "sql" / ("rpt_" + suite + ".sql")).read_text(), suite)
            self.assertIn("RPT DUMP BEGIN SQL=%", output)
            self.assertIn("RPT STEP results SQL=%", output)
            self.assertIn("RAISE EXCEPTION", output)

    def run_fixture(self, hangs):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_psql = root / "psql"
            fake_psql.write_text("#!" + sys.executable + "\n"
                                 "import sys, time\n"
                                 "if '-c' in sys.argv:\n"
                                 "    print(sys.argv[sys.argv.index('-c') + 1])\n"
                                 "    sys.exit(0)\n"
                                 "print('RPT STEP fixture', flush=True)\n"
                                 + ("time.sleep(30)\n" if hangs else ""))
            fake_psql.chmod(0o755)
            output = root / "output"
            result = subprocess.run([sys.executable, str(SCRIPT), "--psql", str(fake_psql),
                                     "--output", str(output), "--wall-timeout",
                                     "0.2" if hangs else "5", "--cancel-grace", "0.1"],
                                    capture_output=True, text=True, timeout=10)
            state = json.loads((output / "result.json").read_text())
            self.assertEqual(state["timed_out"], hangs)
            self.assertEqual(result.returncode, 2 if hangs else 0, result.stderr)
            if hangs:
                self.assertIn("pg_stat_activity", (output / "activity-before-cancel.json").read_text())
                self.assertIn("pg_cancel_backend", (output / "cancel.txt").read_text())
                self.assertTrue((output / "activity-after-cancel.json").is_file())
                self.assertIn("not terminated", result.stdout)
            self.assertIn("RPT STEP fixture", (output / "client.log").read_text())

    def test_successful_workload(self):
        self.run_fixture(False)

    def test_external_watchdog_survives_an_unresponsive_client(self):
        self.run_fixture(True)


if __name__ == "__main__":
    unittest.main()
