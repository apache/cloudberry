CREATE TABLE fixed_decimal(a FIXEDDECIMAL NOT NULL);

INSERT INTO fixed_decimal VALUES('92233720368547758.07'),('0.01'),('-92233720368547758.08'),('-0.01');

SELECT SUM(a) FROM fixed_decimal WHERE a > 0;

SELECT SUM(a) FROM fixed_decimal WHERE a < 0;

TRUNCATE TABLE fixed_decimal;

INSERT INTO fixed_decimal VALUES('11.11'),('22.22'),('33.33');

SELECT SUM(a) FROM fixed_decimal;

SELECT MAX(a) FROM fixed_decimal;

SELECT MIN(a) FROM fixed_decimal;

SELECT AVG(a) FROM fixed_decimal;

DROP TABLE fixed_decimal;