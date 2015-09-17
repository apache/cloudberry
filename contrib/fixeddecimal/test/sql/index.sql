CREATE TABLE fixdec (id INT PRIMARY KEY, d FIXEDDECIMAL(5,2));

INSERT INTO fixdec (id,d) VALUES(1,-123.45);
INSERT INTO fixdec (id,d) VALUES(2,-123);
INSERT INTO fixdec (id,d) VALUES(3,-12.34);
INSERT INTO fixdec (id,d) VALUES(4,-1.34);
INSERT INTO fixdec (id,d) VALUES(5, 0.12);
INSERT INTO fixdec (id,d) VALUES(6, 1.23);
INSERT INTO fixdec (id,d) VALUES(7, 12.34);
INSERT INTO fixdec (id,d) VALUES(8, 123.45);
INSERT INTO fixdec (id,d) VALUES(9, 123.456);

-- Should fail
CREATE UNIQUE INDEX fixdec_d_idx ON fixdec (d);

DELETE FROM fixdec WHERE id = 9;

CREATE UNIQUE INDEX fixdec_d_idx ON fixdec (d);

SET enable_seqscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM fixdec ORDER BY d;

SELECT * FROM fixdec ORDER BY d;

EXPLAIN (COSTS OFF) SELECT * FROM fixdec WHERE d = '12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '-12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '123.45'::FIXEDDECIMAL;

DROP INDEX fixdec_d_idx;

CREATE INDEX fixdec_d_idx ON fixdec USING hash (d);

EXPLAIN (COSTS OFF) SELECT * FROM fixdec WHERE d = '12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '-12.34'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d = '123.45'::FIXEDDECIMAL;

DROP TABLE fixdec;

-- Test BRIN indexes

CREATE TABLE fixdec (d FIXEDDECIMAL, txt TEXT);
INSERT INTO fixdec SELECT s.i,REPEAT('0',64) FROM generate_series(1,10000) s(i);

CREATE INDEX fixdec_d_idx ON fixdec USING BRIN (d);

EXPLAIN (COSTS OFF) SELECT * FROM fixdec WHERE d > '9999'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d > '9999'::FIXEDDECIMAL;

DROP TABLE fixdec;

SET enable_seqscan = on;
