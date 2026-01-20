-- Test UDS socket
-- start_ignore
CREATE EXTENSION IF NOT EXISTS yagp_hooks_collector;
-- end_ignore

\set UDS_PATH '/tmp/yagpcc_test.sock'

-- Configure extension to send via UDS
SET yagpcc.uds_path TO :'UDS_PATH';
SET yagpcc.ignored_users_list TO '';
SET yagpcc.enable TO TRUE;
SET yagpcc.logging_mode TO 'UDS';

-- Start receiver
SELECT yagpcc.__test_uds_start_server(:'UDS_PATH');

-- Send
SELECT 1;

-- Receive
SELECT yagpcc.__test_uds_receive() > 0 as received;

-- Stop receiver
SELECT yagpcc.__test_uds_stop_server();

-- Cleanup
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.uds_path;
RESET yagpcc.ignored_users_list;
RESET yagpcc.enable;
RESET yagpcc.logging_mode;
