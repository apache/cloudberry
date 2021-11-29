-- Test partition table
CREATE SCHEMA s8;
SELECT diskquota.SET_schema_quota('s8', '1 MB');
SET search_path TO s8;
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
)PARTITION BY RANGE (logdate)
(
	PARTITION Feb06 START (date '2006-02-01') INCLUSIVE,
	PARTITION Mar06 START (date '2006-03-01') INCLUSIVE
    END (date '2016-04-01') EXCLUSIVE
);

INSERT INTO measurement SELECT generate_series(1,100), '2006-02-02' ,1,1;
SELECT pg_sleep(20);
INSERT INTO measurement SELECT 1, '2006-02-02' ,1,1;
-- expect insert fail
INSERT INTO measurement SELECT generate_series(1,100000), '2006-03-02' ,1,1;
SELECT pg_sleep(10);
-- expect insert fail
INSERT INTO measurement SELECT 1, '2006-02-02' ,1,1;
-- expect insert fail
INSERT INTO measurement SELECT 1, '2006-03-03' ,1,1;
DELETE FROM measurement WHERE logdate='2006-03-02';
VACUUM FULL measurement;
SELECT pg_sleep(20);
INSERT INTO measurement SELECT 1, '2006-02-02' ,1,1;
INSERT INTO measurement SELECT 1, '2006-03-03' ,1,1;

DROP TABLE measurement;
RESET search_path;
DROP SCHEMA s8;

