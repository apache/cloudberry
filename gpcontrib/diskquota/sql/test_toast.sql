-- Test toast
create schema s5;
select diskquota.set_schema_quota('s5', '1 MB');
set search_path to s5;
CREATE TABLE a5 (message text);
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,10000)) 
FROM generate_series(1,10);

select pg_sleep(5);
-- expect insert toast fail
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,100000)) 
FROM generate_series(1,1000000);

drop table a5;
reset search_path;
drop schema s5;

