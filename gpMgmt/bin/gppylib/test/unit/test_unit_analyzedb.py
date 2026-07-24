import imp
import os

from gppylib.test.unit.gp_unittest import GpTestCase, run_tests


class AnalyzeDbTestCase(GpTestCase):
    def setUp(self):
        analyzedb_file = os.path.abspath(os.path.dirname(__file__) + "/../../../analyzedb")
        self.subject = imp.load_source('analyzedb', analyzedb_file)

    def test_create_psql_command_keeps_utf8_sql_but_uses_ascii_safe_display_name(self):
        query = 'analyze "public"."spiegelungssätze"'

        cmd = self.subject.create_psql_command('special_encoding_db', query)

        self.assertEqual(cmd.name, 'analyze "public"."spiegelungss\\xe4tze"')
        self.assertIn('spiegelungssätze', cmd.cmdStr)

    def test_regclass_schema_tbl_handles_utf8_identifiers(self):
        self.assertEqual(
            self.subject.regclass_schema_tbl('public', 'spiegelungssätze'),
            'to_regclass(\'public."spiegelungssätze"\')'
        )

    def test_regclass_schema_tbl_escapes_sql_literal_quotes(self):
        self.assertEqual(
            self.subject.regclass_schema_tbl('public', "table'quote"),
            'to_regclass(\'public."table\'\'quote"\')'
        )


if __name__ == '__main__':
    run_tests()
