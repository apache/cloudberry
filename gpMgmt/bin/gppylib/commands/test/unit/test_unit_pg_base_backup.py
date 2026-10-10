import unittest
from mock import patch
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

    @patch('gppylib.commands.pg.PgReplicationSlot.slot_exists', return_value=True)
    @patch('gppylib.commands.pg.PgReplicationSlot.drop_slot', return_value=True)
    def test_base_backup_passes_parameters_necessary_to_create_replication_slot_when_given_slotname(self, mock1, mock2):
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
        self.assertIn("--create-slot", base_backup.command_tokens)

    @patch('gppylib.commands.pg.PgReplicationSlot.slot_exists', return_value=False)
    def test_base_backup_does_not_pass_conflicting_xlog_method_argument_when_given_replication_slot(self, mock1):
        base_backup = pg.PgBaseBackup(
            create_slot=True,
            replication_slot_name='some-replication-slot-name',
            target_datadir="foo",
            source_host="bar",
            source_port="baz",
            )
        self.assertNotIn("-x", base_backup.command_tokens)
        self.assertNotIn("--xlog", base_backup.command_tokens)


if __name__ == '__main__':
    unittest.main()
