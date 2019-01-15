-- Test temp table restrained by role id
CREATE SCHEMA strole;
CREATE ROLE u3temp NOLOGIN;
SET search_path TO strole;

SELECT diskquota.set_role_quota('u3temp', '1MB');
CREATE TABLE a(i int);
ALTER TABLE a OWNER TO u3temp;
CREATE TEMP TABLE ta(i int);
ALTER TABLE ta OWNER TO u3temp;

-- expected failed: fill temp table
INSERT INTO ta SELECT generate_series(1,100000000);
-- expected failed: 
INSERT INTO a SELECT generate_series(1,100);
DROP TABLE ta;
SELECT pg_sleep(20);
INSERT INTO a SELECT generate_series(1,100);

DROP TABLE a;
DROP ROLE u3temp;
RESET search_path;
DROP SCHEMA strole;
