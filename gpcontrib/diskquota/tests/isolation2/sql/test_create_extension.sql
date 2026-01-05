CREATE EXTENSION diskquota;

SELECT diskquota.init_table_size_table();

-- Wait after init so that diskquota.state is clean
SELECT diskquota.wait_for_worker_new_epoch();
