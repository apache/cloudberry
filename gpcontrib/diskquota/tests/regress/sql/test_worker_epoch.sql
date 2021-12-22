-- Test if the UDF returns successfully.
-- NOTE: This test should be the first one since the UDF is supposed
--       to be used in all other tests.

SELECT diskquota.wait_for_worker_new_epoch();
