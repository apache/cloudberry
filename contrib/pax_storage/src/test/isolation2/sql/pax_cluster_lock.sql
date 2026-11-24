-- The column-based PAX CLUSTER rewrites tuples and rebuilds indexes.
-- Concurrent DML must wait until the rewrite and index rebuild finish.
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE TABLE pax_cluster_lock (k int, v int) USING pax
  WITH (minmax_columns='k,v') DISTRIBUTED BY (k);
ALTER TABLE pax_cluster_lock SET (cluster_columns='k,v');
CREATE INDEX pax_cluster_lock_idx ON pax_cluster_lock(k);
INSERT INTO pax_cluster_lock
SELECT i, i FROM generate_series(1,1000) i ORDER BY (i*541)%1000;

-- Suspend the rewrite on one QE after the QD has acquired its table lock.
SELECT gp_inject_fault('orc_writer_write_tuple', 'suspend', '', '', '',
                       1, 1, 0, dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
1&: CLUSTER pax_cluster_lock;
SELECT gp_wait_until_triggered_fault('orc_writer_write_tuple', 1, dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
2&: UPDATE pax_cluster_lock SET v=9999 WHERE k=42;
SELECT gp_inject_fault('orc_writer_write_tuple', 'reset', dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
2<:

SET enable_seqscan=on;
SET enable_indexscan=off;
SET enable_bitmapscan=off;
SELECT * FROM pax_cluster_lock WHERE k=42;
SET enable_seqscan=off;
SET enable_indexscan=on;
SET enable_bitmapscan=on;
SELECT * FROM pax_cluster_lock WHERE k=42;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- The database-wide CLUSTER uses a separate utility hook path.
SELECT gp_inject_fault('orc_writer_write_tuple', 'suspend', '', '', '',
                       1, 1, 0, dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
1&: CLUSTER;
SELECT gp_wait_until_triggered_fault('orc_writer_write_tuple', 1, dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
2&: INSERT INTO pax_cluster_lock VALUES (1001,1001);
SELECT gp_inject_fault('orc_writer_write_tuple', 'reset', dbid)
FROM gp_segment_configuration WHERE role='p' AND content=0;
1<:
2<:
SET enable_seqscan=off;
SELECT * FROM pax_cluster_lock WHERE k=1001;
RESET enable_seqscan;
DROP TABLE pax_cluster_lock;
