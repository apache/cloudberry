-- Test schema
CREATE SCHEMA s1;
SELECT diskquota.set_schema_quota('s1', '1 MB');
-- Test delete disk quota
CREATE SCHEMA deleteschema;
SELECT diskquota.set_schema_quota('deleteschema', '1 MB');
-- test rename schema
CREATE SCHEMA srs1;
SELECT diskquota.set_schema_quota('srs1', '1 MB');
-- test rename role
CREATE SCHEMA srr1;
DROP ROLE IF EXISTS srerole;
CREATE ROLE srerole NOLOGIN;
SELECT diskquota.set_role_quota('srerole', '1MB');
-- Test re-set_schema_quota
CREATE SCHEMA srE;
SELECT diskquota.set_schema_quota('srE', '1 MB');
-- Test role quota
CREATE SCHEMA srole;

DROP ROLE IF EXISTS u1;
DROP ROLE IF EXISTS u2;
CREATE ROLE u1 NOLOGIN;
CREATE ROLE u2 NOLOGIN;
CREATE TABLE b (t TEXT) DISTRIBUTED BY (i);
ALTER TABLE b OWNER TO u1;
CREATE TABLE b2 (t TEXT) DISTRIBUTED BY (i);
ALTER TABLE b2 OWNER TO u1;

SELECT diskquota.set_role_quota('u1', '1 MB');
-- Test temp table restrained by role id
CREATE SCHEMA strole;
DROP ROLE IF EXISTS u3temp;
CREATE ROLE u3temp NOLOGIN;
SELECT diskquota.set_role_quota('u3temp', '1MB');
-- Test toast
CREATE SCHEMA s5;
SELECT diskquota.set_schema_quota('s5', '1 MB');
-- Test truncate
CREATE SCHEMA s7;
SELECT diskquota.set_schema_quota('s7', '1 MB');
