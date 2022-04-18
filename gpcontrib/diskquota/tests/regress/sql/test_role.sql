-- Test role quota

CREATE SCHEMA srole;
SET search_path TO srole;

CREATE ROLE u1 NOLOGIN;
CREATE ROLE u2 NOLOGIN;
CREATE TABLE b (t TEXT) DISTRIBUTED BY (t);
ALTER TABLE b OWNER TO u1;
CREATE TABLE b2 (t TEXT) DISTRIBUTED BY (t);
ALTER TABLE b2 OWNER TO u1;

SELECT diskquota.set_role_quota('u1', '1 MB');

INSERT INTO b SELECT generate_series(1,100);
-- expect insert success
INSERT INTO b SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO b2 SELECT generate_series(1,100);
-- Delete role quota
SELECT diskquota.set_role_quota('u1', '-1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);
-- Reset role quota
SELECT diskquota.set_role_quota('u1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);
SELECT role_name, quota_in_mb, rolsize_in_bytes FROM diskquota.show_fast_role_quota_view WHERE role_name='u1';

SELECT tableid::regclass, size, segid
FROM diskquota.table_size
WHERE tableid = 'b'::regclass
ORDER BY segid;

SELECT tableid::regclass, size, segid
FROM diskquota.table_size
WHERE tableid = 'b2'::regclass
ORDER BY segid;


ALTER TABLE b OWNER TO u2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);
-- expect insert succeed
INSERT INTO b2 SELECT generate_series(1,100);

-- superuser is blocked to set quota
--start_ignore
SELECT rolname from pg_roles where rolsuper=true;
--end_ignore
\gset
select diskquota.set_role_quota(:'rolname', '1mb');

CREATE ROLE "Tn" NOLOGIN;
SELECT diskquota.set_role_quota('Tn', '-1 MB'); -- fail
SELECT diskquota.set_role_quota('"tn"', '-1 MB'); -- fail
SELECT diskquota.set_role_quota('"Tn"', '-1 MB');

DROP TABLE b, b2;
DROP ROLE u1, u2, "Tn";
RESET search_path;
DROP SCHEMA srole;
