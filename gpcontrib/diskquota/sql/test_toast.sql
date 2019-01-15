-- Test toast
CREATE SCHEMA s5;
SELECT diskquota.set_schema_quota('s5', '1 MB');
SET search_path TO s5;
CREATE TABLE a5 (message text);
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,10000)) 
FROM generate_series(1,10);

SELECT pg_sleep(20);
-- expect insert toast fail
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,100000)) 
FROM generate_series(1,1000000);

DROP TABLE a5;
RESET search_path;
DROP SCHEMA s5;

