import unittest
from unittest.mock import Mock, patch
from gppylib.commands import pg


class TestUnitPgBaseBackup(unittest.TestCase):
    def test_replication_slot_not_passed_when_not_given_slot_name(self):
        base_backup = pg.PgBaseBackup(
            replication_slot_name = None,
            target_datadir="foo",
            source_host = "bar",
            source_port="baz",
            )

        tokens = base_backup.command_tokens

        self.assertNotIn("--slot", tokens)
        self.assertNotIn("some-replication-slot-name", tokens)
        self.assertIn("--wal-method", tokens)
        self.assertIn("fetch", tokens)
        self.assertNotIn("stream", tokens)

    def test_base_backup_passes_parameters_necessary_to_create_replication_slot_when_given_slotname(self):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )

        self.assertIn("--slot", base_backup.command_tokens)
        self.assertIn("some-replication-slot-name", base_backup.command_tokens)
        self.assertIn("--wal-method", base_backup.command_tokens)
        self.assertIn("stream", base_backup.command_tokens)

    def test_base_backup_does_not_pass_conflicting_xlog_method_argument_when_given_replication_slot(self):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )
        self.assertNotIn("-x", base_backup.command_tokens)
        self.assertNotIn("--xlog", base_backup.command_tokens)

    @patch('gppylib.commands.pg.dbconn.querySingleton', return_value=1)
    @patch('gppylib.commands.pg.dbconn.connect')
    @patch('gppylib.commands.pg.dbconn.DbURL')
    def test_ensure_replication_slot_exists_returns_false_when_slot_exists(self, mock_dburl,
                                                                           mock_connect,
                                                                           mock_query_singleton):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        created = pg.ensure_replication_slot_exists('source-host', 5432, 'slot_name')

        self.assertFalse(created)
        mock_dburl.assert_called_once_with(hostname='source-host', port=5432, dbname='template1')
        mock_connect.assert_called_once_with(mock_dburl.return_value, utility=True)
        self.assertEqual(1, mock_query_singleton.call_count)
        self.assertIn("FROM pg_catalog.pg_replication_slots", mock_query_singleton.call_args[0][1])
        mock_conn.close.assert_called_once_with()

    @patch('gppylib.commands.pg.dbconn.execSQL')
    @patch('gppylib.commands.pg.dbconn.querySingleton', return_value=0)
    @patch('gppylib.commands.pg.dbconn.connect')
    @patch('gppylib.commands.pg.dbconn.DbURL')
    def test_ensure_replication_slot_exists_creates_missing_slot(self, mock_dburl,
                                                                 mock_connect,
                                                                 mock_query_singleton,
                                                                 mock_exec_sql):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        created = pg.ensure_replication_slot_exists('source-host', 5432, 'slot_name')

        self.assertTrue(created)
        mock_dburl.assert_called_once_with(hostname='source-host', port=5432, dbname='template1')
        mock_connect.assert_called_once_with(mock_dburl.return_value, utility=True)
        self.assertEqual(1, mock_query_singleton.call_count)
        self.assertIn("FROM pg_catalog.pg_replication_slots", mock_query_singleton.call_args[0][1])
        mock_exec_sql.assert_called_once()
        self.assertIn("pg_create_physical_replication_slot('slot_name')",
                      mock_exec_sql.call_args[0][1])
        mock_conn.close.assert_called_once_with()

    @patch('gppylib.commands.pg.dbconn.querySingleton')
    @patch('gppylib.commands.pg.dbconn.connect')
    @patch('gppylib.commands.pg.dbconn.DbURL')
    def test_ensure_replication_slot_exists_skips_empty_slot_name(self, mock_dburl,
                                                                  mock_connect,
                                                                  mock_query_singleton):
        created = pg.ensure_replication_slot_exists('source-host', 5432, None)

        self.assertFalse(created)
        mock_dburl.assert_not_called()
        mock_connect.assert_not_called()
        mock_query_singleton.assert_not_called()


if __name__ == '__main__':
    unittest.main()
