SET default_table_access_method=ao_column;
--
-- Tests for external toast datums
--

-- Other compression algorithms may cause the compressed data to be stored
-- inline.  pglz guarantees that the data is externalized, so stick to it.
SET default_toast_compression = 'pglz';

CREATE TABLE indtoasttest(descr text, cnt int DEFAULT 0, f1 text, f2 text);

INSERT INTO indtoasttest(descr, f1, f2) VALUES('two-compressed', repeat('1234567890',1000), repeat('1234567890',1000));
INSERT INTO indtoasttest(descr, f1, f2) VALUES('two-toasted', repeat('1234567890',30000), repeat('1234567890',50000));
INSERT INTO indtoasttest(descr, f1, f2) VALUES('one-compressed,one-null', NULL, repeat('1234567890',1000));
INSERT INTO indtoasttest(descr, f1, f2) VALUES('one-toasted,one-null', NULL, repeat('1234567890',50000));

-- check whether indirect tuples works on the most basic level
SELECT descr, substring(make_tuple_indirect(indtoasttest)::text, 1, 200) FROM indtoasttest;

-- modification without changing varlenas
UPDATE indtoasttest SET cnt = cnt +1 RETURNING substring(indtoasttest::text, 1, 200);

-- modification without modifying assigned value
UPDATE indtoasttest SET cnt = cnt +1, f1 = f1 RETURNING substring(indtoasttest::text, 1, 200);

-- modification modifying, but effectively not changing
UPDATE indtoasttest SET cnt = cnt +1, f1 = f1||'' RETURNING substring(indtoasttest::text, 1, 200);

UPDATE indtoasttest SET cnt = cnt +1, f1 = '-'||f1||'-' RETURNING substring(indtoasttest::text, 1, 200);

SELECT substring(indtoasttest::text, 1, 200) FROM indtoasttest;
-- check we didn't screw with main/toast tuple visibility
VACUUM FREEZE indtoasttest;
SELECT substring(indtoasttest::text, 1, 200) FROM indtoasttest;

DROP TABLE indtoasttest;
RESET default_toast_compression;