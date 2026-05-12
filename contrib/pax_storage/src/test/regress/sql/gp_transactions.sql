--
-- Transactions (GPDB-specific tests)
--
\getenv abs_builddir PG_ABS_BUILDDIR

CREATE TEMPORARY TABLE temptest (a int);
INSERT INTO temptest VALUES (generate_series(1, 10));

CREATE TEMPORARY SEQUENCE tempseq;

SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;

-- Make sure COPY works with temp tables during a READ ONLY transaction.
\set filename :abs_builddir '/results/xacttemp.data'

COPY temptest TO :'filename';
DELETE FROM temptest;
COPY temptest FROM :'filename';
SELECT * from temptest;

-- Ensure temporary sequences function correctly as well.
SELECT nextval('tempseq');
SELECT setval('tempseq', 5);
SELECT nextval('tempseq');
