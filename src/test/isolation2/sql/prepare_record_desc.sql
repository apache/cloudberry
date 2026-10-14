-- Check that the XLOG_XACT_PREPARE records segments write for distributed
-- transactions are parsed correctly by ParsePrepareRecord(), which is shared
-- by pg_waldump, pg_walinspect and two-phase logical decoding.
--
-- Cloudberry's 2PC state file header carries fields that upstream's
-- xl_xact_prepare does not (delete-on-commit/abort databases, tablespaces
-- to delete).  When the two structs drifted apart the parser read the GID
-- and every trailing array from the wrong offsets: pg_waldump printed an
-- empty gid and two-phase logical decoding emitted PREPARE TRANSACTION ''
-- (github issue #1973).  The description of each PREPARE record below must
-- therefore start with "gid <distributed xid>:" and carry exactly the
-- sections the transaction produced.

CREATE EXTENSION IF NOT EXISTS pg_walinspect;

-- A one-row table on segment 0 remembering where its WAL was before each
-- step, so we only look at the records that step produced.
0U: CREATE TEMP TABLE prepare_desc_lsn AS SELECT pg_current_wal_lsn() AS lsn;

-- DDL: the prepare carries delete-on-abort rels, stats to drop on abort and
-- invalidation messages, in that order.
CREATE TABLE prepare_desc_t(id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
0U: SELECT record_type,
       description ~ '^gid [0-9]+: [^;]+; rels\(abort\): [^;]+; abort dropped stats: [^;]+; inval msgs: ' AS desc_ok
    FROM pg_get_wal_records_info((SELECT lsn FROM prepare_desc_lsn), pg_current_wal_lsn())
    WHERE resource_manager = 'Transaction' AND record_type = 'PREPARE';

-- Plain DML: nothing but the gid and the prepare timestamp.
0U: UPDATE prepare_desc_lsn SET lsn = pg_current_wal_lsn();
INSERT INTO prepare_desc_t SELECT g, 'x' FROM generate_series(1, 20) g;
0U: SELECT record_type,
       description ~ '^gid [0-9]+: [^;]+$' AS desc_ok
    FROM pg_get_wal_records_info((SELECT lsn FROM prepare_desc_lsn), pg_current_wal_lsn())
    WHERE resource_manager = 'Transaction' AND record_type = 'PREPARE';

-- CREATE DATABASE with the FILE_COPY strategy: the new database directory
-- is registered for deletion on abort, so a delete-on-abort database entry
-- follows the abort rels in the record.
0U: UPDATE prepare_desc_lsn SET lsn = pg_current_wal_lsn();
CREATE DATABASE prepare_desc_db STRATEGY = FILE_COPY;
0U: SELECT record_type,
       description ~ '^gid [0-9]+: ' AS gid_ok,
       description ~ '; dbs\(abort\): base/[0-9]+' AS abort_db_ok
    FROM pg_get_wal_records_info((SELECT lsn FROM prepare_desc_lsn), pg_current_wal_lsn())
    WHERE resource_manager = 'Transaction' AND record_type = 'PREPARE';

-- ALTER DATABASE SET TABLESPACE: the old directory is deleted on commit and
-- the new one on abort, so both database arrays are present.
!\retcode rm -rf /tmp/prepare_desc_ts;
!\retcode mkdir -p /tmp/prepare_desc_ts;
CREATE TABLESPACE prepare_desc_ts LOCATION '/tmp/prepare_desc_ts';
0U: UPDATE prepare_desc_lsn SET lsn = pg_current_wal_lsn();
ALTER DATABASE prepare_desc_db SET TABLESPACE prepare_desc_ts;
0U: SELECT record_type,
       description ~ '^gid [0-9]+: ' AS gid_ok,
       description ~ '; dbs\(commit\): base/[0-9]+' AS commit_db_ok,
       description ~ '; dbs\(abort\): pg_tblspc/[0-9]+/[^/ ]+/[0-9]+' AS abort_db_ok
    FROM pg_get_wal_records_info((SELECT lsn FROM prepare_desc_lsn), pg_current_wal_lsn())
    WHERE resource_manager = 'Transaction' AND record_type = 'PREPARE';

-- DROP DATABASE: the database's stats are dropped on commit.
0U: UPDATE prepare_desc_lsn SET lsn = pg_current_wal_lsn();
DROP DATABASE prepare_desc_db;
0U: SELECT record_type,
       description ~ '^gid [0-9]+: ' AS gid_ok,
       description ~ '; commit dropped stats: 1/[0-9]+/0' AS commit_stats_ok
    FROM pg_get_wal_records_info((SELECT lsn FROM prepare_desc_lsn), pg_current_wal_lsn())
    WHERE resource_manager = 'Transaction' AND record_type = 'PREPARE';

DROP TABLESPACE prepare_desc_ts;
!\retcode rm -rf /tmp/prepare_desc_ts;
DROP TABLE prepare_desc_t;
DROP EXTENSION pg_walinspect;
