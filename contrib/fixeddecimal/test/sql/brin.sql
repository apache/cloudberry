-- Test BRIN indexes
SET enable_seqscan = off;
CREATE TABLE fixdec (d FIXEDDECIMAL, txt TEXT);
INSERT INTO fixdec SELECT s.i,REPEAT('0',64) FROM generate_series(1,10000) s(i);

CREATE INDEX fixdec_d_idx ON fixdec USING BRIN (d);

EXPLAIN (COSTS OFF) SELECT * FROM fixdec WHERE d > '9999'::FIXEDDECIMAL;

SELECT * FROM fixdec WHERE d > '9999'::FIXEDDECIMAL;

DROP TABLE fixdec;

RESET enable_seqscan;
