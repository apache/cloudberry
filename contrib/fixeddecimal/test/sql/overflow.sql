-- Ensure the expected extreme values can be represented
SELECT '-92233720368547758.08'::FIXEDDECIMAL as minvalue,'92233720368547758.07'::FIXEDDECIMAL as maxvalue;

SELECT '-92233720368547758.09'::FIXEDDECIMAL;

SELECT '92233720368547758.08'::FIXEDDECIMAL;

-- Ensure casts from numeric to fixeddecimal work
SELECT '92233720368547758.07'::numeric::FIXEDDECIMAL;

-- The literal below must be quoted as the parser seems to read the literal as
-- a positive number first and then us the - unary operator to make it negaive.
-- This would overflow without the quotes as this number cannot be represented
-- in a positive fixeddecimal.
SELECT '-92233720368547758.08'::numeric::FIXEDDECIMAL;

-- Ensure casts from numeric to fixed decimal detect overflow
SELECT '92233720368547758.08'::numeric::FIXEDDECIMAL;

SELECT '-92233720368547758.09'::numeric::FIXEDDECIMAL;

SELECT '-92233720368547758.08'::FIXEDDECIMAL - '0.01'::FIXEDDECIMAL;

SELECT '92233720368547758.07'::FIXEDDECIMAL + '0.01'::FIXEDDECIMAL;

-- Should not overflow
SELECT '46116860184273879.03'::FIXEDDECIMAL * '2.00'::FIXEDDECIMAL;

-- Ensure this overflows
SELECT '46116860184273879.04'::FIXEDDECIMAL * '2.00'::FIXEDDECIMAL;

-- Should not overflow
SELECT '46116860184273879.03'::FIXEDDECIMAL / '0.50'::FIXEDDECIMAL;

-- Ensure this overflows
SELECT '46116860184273879.04'::FIXEDDECIMAL / '0.50'::FIXEDDECIMAL;

-- Ensure limits of int2 can be represented
SELECT '32767'::FIXEDDECIMAL::INT2,'-32768'::FIXEDDECIMAL::INT2;

-- Ensure overflow of int2 is detected
SELECT '32768'::FIXEDDECIMAL::INT2;

-- Ensure underflow of int2 is detected
SELECT '-32769'::FIXEDDECIMAL::INT2;

-- Ensure limits of int4 can be represented
SELECT '2147483647'::FIXEDDECIMAL::INT4,'-2147483648'::FIXEDDECIMAL::INT4;

-- Ensure overflow of int4 is detected
SELECT '2147483648'::FIXEDDECIMAL::INT4;

-- Ensure underflow of int4 is detected
SELECT '-2147483649'::FIXEDDECIMAL::INT4;

-- Ensure overflow is detected
SELECT SUM(a) FROM (VALUES('92233720368547758.07'::FIXEDDECIMAL),('0.01'::FIXEDDECIMAL)) a(a);

-- Ensure underflow is detected
SELECT SUM(a) FROM (VALUES('-92233720368547758.08'::FIXEDDECIMAL),('-0.01'::FIXEDDECIMAL)) a(a);

-- Test typmods
SELECT 12345.33::FIXEDDECIMAL(3,2); -- Fail

SELECT 12345.33::FIXEDDECIMAL(5,2); -- Fail

-- scale of 2 should be enforced.
SELECT 12345.44::FIXEDDECIMAL(7,0);

-- should work.
SELECT 12345.33::FIXEDDECIMAL(7,2);

-- error, precision limit should be 17
SELECT 12345.33::FIXEDDECIMAL(18,2);

CREATE TABLE fixdec (d FIXEDDECIMAL(3,2));
INSERT INTO fixdec VALUES(12.34); -- Fail
INSERT INTO fixdec VALUES(1.23); -- Pass
DROP TABLE fixdec;
