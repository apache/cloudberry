-- Test UDS socket
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_stats_collector;
-- end_ignore

\set UDS_PATH '/tmp/gpsc_test.sock'

-- Configure extension to send via UDS
SET gpsc.uds_path TO :'UDS_PATH';
SET gpsc.ignored_users_list TO '';
SET gpsc.enable TO TRUE;
SET gpsc.logging_mode TO 'UDS';

-- Start receiver
SELECT gpsc.__test_uds_start_server(:'UDS_PATH');

-- Send
SELECT 1;

-- Receive
SELECT gpsc.__test_uds_receive() > 0 as received;

-- Stop receiver
SELECT gpsc.__test_uds_stop_server();

-- Cleanup
DROP EXTENSION gp_stats_collector;
RESET gpsc.uds_path;
RESET gpsc.ignored_users_list;
RESET gpsc.enable;
RESET gpsc.logging_mode;
