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
"""Tests for survey accounting and non-executing SQL generation."""
import importlib.util
import json
from pathlib import Path
import unittest
import tempfile
from unittest.mock import patch
from contextlib import redirect_stdout
import io
import subprocess

spec = importlib.util.spec_from_file_location(
    "rpt_survey", Path(__file__).resolve().parents[1] / "scripts" / "survey.py")
survey = importlib.util.module_from_spec(spec)
spec.loader.exec_module(survey)


class SurveyTests(unittest.TestCase):
    def test_no_region_is_zero_not_missing_data(self):
        self.assertEqual(survey.parse_dump("no transfer regions\n")["strict"], 0)
        with self.assertRaises(ValueError):
            survey.parse_dump("transfer order: 1 2\n")

    def test_comparison_counts_channels_not_both_endpoints(self):
        explain = [{"Plan": {"Node Type": "Custom Scan",
                             "Custom Plan Provider": "Anser Bloom Producer",
                             "Plans": [{"Custom Plan Provider": "Anser Bloom Consumer"}]},
                    "Optimizer": "GPORCA"}]
        output = (survey.BEGIN_DUMP + "\n" +
                  "survey classes=2 root_classes=1 global_edges=3 rule9=2 "
                  "strict=1 eligible=1 known_rows_removed=250 unknown_estimates=0\n" +
                  survey.BEGIN_ANSER + "\n" + json.dumps(explain) + "\n" + survey.END + "\n")
        metrics, dump, plan = survey.parse_output(output)
        self.assertEqual(metrics["anser_joins"], 1)
        self.assertEqual(metrics["strict"], 1)
        self.assertEqual(metrics["anser_optimizer_used"], "GPORCA")
        self.assertEqual(metrics["known_rows_removed"], 250)

    def test_largest_root_runtime_metrics_are_separate(self):
        dump = ("survey classes=1 root_classes=1 global_edges=1 rule9=1 strict=1 eligible=1 "
                "known_rows_removed=90 unknown_estimates=0\n"
                "runtime survey classes=2 root_classes=1 strict=2 eligible=1\n"
                "backward consumer 2 root=1 rep=1.1 decision=inject\n"
                "backward consumer 3 root=1 rep=1.1 decision=backward consumer cap exceeded\n")
        result = survey.parse_dump(dump)
        self.assertEqual(result["classes"], 1)
        self.assertEqual(result["runtime_classes"], 2)
        self.assertEqual(result["runtime_strict"], 2)
        self.assertEqual(result["runtime_injected_consumers"], 1)
        self.assertEqual(result["runtime_refusals"], {"backward consumer cap exceeded": 1})

    def test_incomplete_output_cannot_be_reported_as_zero(self):
        with self.assertRaises(ValueError):
            survey.parse_output(survey.BEGIN_DUMP + "\nno transfer regions\n")
        with self.assertRaises(ValueError):
            survey.parse_dump("survey classes=1 root_classes=1 global_edges=1 "
                              "rule9=1 strict=2 eligible=1 known_rows_removed=0 unknown_estimates=0")

    def test_queries_are_planned_in_read_only_transaction(self):
        sql = survey.survey_sql("SELECT 'O''Brien' -- trailing comment", "on", 1000, "imdb,public")
        self.assertIn("BEGIN READ ONLY;", sql)
        self.assertIn("SET LOCAL rpt.enable = off;", sql)
        self.assertIn("SET LOCAL rpt.backward = on;", sql)
        self.assertIn("SET LOCAL anser.runtime_filter = on;", sql)
        self.assertIn("EXPLAIN (FORMAT JSON) SELECT 'O''Brien' -- trailing comment\n;", sql)
        self.assertNotIn("EXPLAIN ANALYZE", sql)
        self.assertTrue(sql.endswith("ROLLBACK;"))
        self.assertEqual(survey.sql_literal("a'b"), "'a''b'")

    def test_templates_count_selects_not_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "14.sql").write_text("-- variant a\nSELECT 14; /* variant b */ SELECT 15; -- end\n")
            (path / "15.sql").write_text("WITH revenue AS (SELECT 1 n) SELECT * FROM revenue;")
            queries = survey.load_queries(path)
            self.assertEqual(len(queries), 3)
            self.assertEqual([q["statement"] for q in queries], [1, 2, 1])
            self.assertEqual(survey.EXPECTED["TPC-DS"], 103)

    def test_standard_tpcds_layout_and_split_variants_have_103_statements(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            for number in range(1, 100):
                sql = "SELECT %d;" % number
                if number in (14, 23, 24, 39):
                    sql += " SELECT %d;" % number
                (path / (str(number) + ".sql")).write_text(sql)
            self.assertEqual(len(list(path.glob("*.sql"))), 99)
            self.assertEqual(len(survey.load_queries(path)), 103)
            for number in (14, 23, 24, 39):
                (path / (str(number) + ".sql")).unlink()
                for suffix in ("a", "b"):
                    (path / (str(number) + suffix + ".sql")).write_text("SELECT %d;" % number)
            self.assertEqual(len(list(path.glob("*.sql"))), 103)
            self.assertEqual(len(survey.load_queries(path)), 103)

    def test_driver_completes_by_statement_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arguments = ["survey.py"]
            for suite, flag in [("JOB", "job"), ("TPC-H", "tpch"), ("TPC-DS", "tpcds")]:
                path = root / suite
                path.mkdir()
                (path / "one.sql").write_text("SELECT 1; SELECT 2;" if flag == "tpcds" else "SELECT 1;")
                arguments += ["--" + flag, str(path)]
            output = root / "output"
            arguments += ["--output", str(output)]
            text = (survey.BEGIN_DUMP + "\nno transfer regions\n" + survey.BEGIN_ANSER +
                    "\n" + json.dumps([{"Plan": {"Node Type": "Result"}}]) + "\n" + survey.END + "\n")
            result = subprocess.CompletedProcess([], 0, stdout=text, stderr="")
            with patch("sys.argv", arguments), patch.object(survey, "EXPECTED", {"JOB": 1, "TPC-H": 1, "TPC-DS": 2}), \
                    patch.object(survey.subprocess, "run", return_value=result), redirect_stdout(io.StringIO()):
                self.assertEqual(survey.main(), 0)
            report = json.loads((output / "results.json").read_text())
            self.assertTrue(report["survey_complete"])
            self.assertEqual(report["expected_statements"]["TPC-DS"], 2)
            self.assertEqual(len(report["results"]), 8)
            tpcds = [row for row in report["results"] if row["suite"] == "TPC-DS"]
            self.assertEqual([row["statement"] for row in tpcds], [1, 2, 1, 2])

    def test_split_respects_literals_and_nested_comments(self):
        sql = "SELECT ';', $$x;y$$, $tag$a;b$tag$, \"a;b\"; /* outer /* inner ; */ end */ SELECT 2;"
        self.assertEqual(len(survey.split_select_statements(sql)), 2)
        self.assertEqual(len(survey.split_select_statements("SELECT E'a\\\';b'; SELECT 2;")), 2)
        self.assertEqual(survey.split_select_statements("-- only comment\n /* another */"), [])
        for sql in ["SELECT 'unterminated", "SELECT $$unterminated", "/* unterminated"]:
            with self.assertRaises(ValueError):
                survey.split_select_statements(sql)

    def test_q15_view_form_is_rejected_with_cte_instruction(self):
        with self.assertRaisesRegex(ValueError, "CTE form of TPC-H Q15"):
            survey.split_select_statements("CREATE VIEW revenue AS SELECT 1; SELECT * FROM revenue; DROP VIEW revenue;")

    def test_failed_queries_stay_in_denominator(self):
        records = [dict(suite="JOB", optimizer="on", status="ok", strict=1,
                        eligible=1, anser_joins=1),
                   dict(suite="JOB", optimizer="on", status="error")]
        text = survey.summarize(records, False)
        self.assertIn("Survey complete: no", text)
        self.assertIn("| JOB | on | 1/2 | 1/2 | 1/2 | 1/2 |", text)


if __name__ == "__main__":
    unittest.main()
