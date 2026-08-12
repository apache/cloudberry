#!/usr/bin/env python3

import imp
import os

from mock import Mock, call, patch

from gppylib.test.unit.gp_unittest import GpTestCase, run_tests


class GpActivateStandbyTestCase(GpTestCase):
    def setUp(self):
        gpactivatestandby_file = os.path.abspath(
            os.path.dirname(__file__) + '/../../../gpactivatestandby')
        self.subject = imp.load_source('gpactivatestandby', gpactivatestandby_file)
        self.subject.logger = Mock(spec=['info', 'debug', 'fatal'])
        self.options = Mock(coordinator_data_dir='/data/coordinator', force=True,
                            logfile=None)

    @patch('gpactivatestandby.time.monotonic', return_value=4.5)
    def test_get_remaining_time_uses_existing_deadline(self, mock_monotonic):
        self.assertEqual(5.5, self.subject.get_remaining_time(10, 'promotion'))

    @patch('gpactivatestandby.time.monotonic', return_value=10)
    def test_get_remaining_time_reports_timed_out_stage(self, mock_monotonic):
        with self.assertRaisesRegex(
                self.subject.GpActivateStandbyException,
                'Timed out waiting for standby postmaster to start'):
            self.subject.get_remaining_time(10, 'standby postmaster to start')

    @patch('gpactivatestandby.time.sleep')
    @patch('gpactivatestandby.time.monotonic', return_value=1)
    @patch('gpactivatestandby.gp.get_postmaster_pid_locally', side_effect=[-1, 1234])
    def test_wait_for_postmaster_retries_with_existing_deadline(
            self, mock_get_pid, mock_monotonic, mock_sleep):
        self.subject.wait_for_postmaster(self.options, 10)

        self.assertEqual(2, mock_get_pid.call_count)
        mock_sleep.assert_called_once_with(1)

    @patch.dict(os.environ, {'PGPORT': '5432'})
    @patch('gpactivatestandby.wait_for_postmaster')
    @patch('gpactivatestandby.gp.GpStandbyStart.local')
    def test_start_coordinator_does_not_use_gpstart(
            self, mock_standby_start, mock_wait_for_postmaster):
        self.subject.start_coordinator(self.options, 700)

        mock_standby_start.assert_called_once_with(
            'Start standby coordinator', '/data/coordinator', 5432)
        mock_wait_for_postmaster.assert_called_once_with(self.options, 700)

    @patch('gpactivatestandby.time.sleep')
    @patch('gpactivatestandby.time.monotonic', side_effect=[100, 100, 101, 101])
    @patch('gpactivatestandby.dbconn.execSQL')
    @patch('gpactivatestandby.dbconn.connect')
    @patch('gpactivatestandby.dbconn.DbURL')
    @patch('gpactivatestandby.gp.Command')
    def test_promote_standby_reuses_deadline_for_connection_retry(
            self, mock_command, mock_dburl, mock_connect, mock_exec_sql,
            mock_monotonic, mock_sleep):
        conn = Mock()
        mock_connect.side_effect = [
            self.subject.pygresql.InternalError('starting up'),
            conn,
        ]

        self.subject.promote_standby('/data/coordinator', 600)

        mock_command.assert_called_once_with(
            'pg_ctl promote',
            'pg_ctl promote -D /data/coordinator -t 500')
        mock_command.return_value.run.assert_called_once_with(validateAfter=True)
        self.assertEqual(2, mock_connect.call_count)
        self.assertEqual([
            call(timeout=2, retries=1),
            call(timeout=2, retries=1),
        ], mock_dburl.call_args_list)
        mock_exec_sql.assert_called_once_with(conn, 'CHECKPOINT')
        conn.close.assert_called_once_with()
        mock_sleep.assert_called_once_with(1)

    @patch('gpactivatestandby.time.monotonic', side_effect=[100, 600])
    @patch('gpactivatestandby.gp.Command')
    def test_promote_standby_reports_stage_when_pg_ctl_exhausts_deadline(
            self, mock_command, mock_monotonic):
        mock_command.return_value.run.side_effect = self.subject.ExecutionError(
            'pg_ctl timed out', mock_command.return_value)

        with self.assertRaisesRegex(
                self.subject.GpActivateStandbyException,
                'Timed out waiting for standby promotion'):
            self.subject.promote_standby('/data/coordinator', 600)

    @patch('gpactivatestandby.print_results')
    @patch('gpactivatestandby.get_config')
    @patch('gpactivatestandby.promote_standby')
    @patch('gpactivatestandby.check_or_start_standby', return_value=False)
    @patch('gpactivatestandby.print_summary', return_value=False)
    @patch('gpactivatestandby.parseargs')
    @patch('gpactivatestandby.setup_tool_logging')
    @patch('gpactivatestandby.signal.signal')
    @patch('gpactivatestandby.time.monotonic', return_value=100)
    @patch('gpactivatestandby.gp.GpStop.local')
    def test_main_shares_one_deadline_across_start_and_promotion(
            self, mock_gpstop, mock_monotonic, mock_signal, mock_setup_logging,
            mock_parseargs, mock_print_summary, mock_check_or_start,
            mock_promote, mock_get_config, mock_print_results):
        mock_parseargs.return_value = (self.options, [])
        array = Mock()
        mock_get_config.return_value = array

        self.assertEqual(0, self.subject.main())

        deadline = 100 + self.subject.STANDBY_ACTIVATION_TIMEOUT
        self.assertEqual([
            call(self.options, deadline),
        ], mock_check_or_start.call_args_list)
        mock_promote.assert_called_once_with('/data/coordinator', deadline)
        mock_gpstop.assert_called_once_with(
            'CBDB restart', restart=True, datadir='/data/coordinator')
        mock_print_results.assert_called_once_with(
            array, self.subject.unix.getLocalHostname(), self.options)


if __name__ == '__main__':
    run_tests()
