DROP TABLE badquota.t1;
DROP ROLE testbody;
DROP SCHEMA badquota;

SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();
DROP EXTENSION diskquota;
