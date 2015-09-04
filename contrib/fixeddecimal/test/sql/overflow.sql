-- Ensure the expected extreme values can be represented
SELECT '-92233720368547758.08'::fixeddecimal as minvalue,'92233720368547758.07'::fixeddecimal as maxvalue;

SELECT '-92233720368547758.09'::fixeddecimal;

SELECT '92233720368547758.08'::fixeddecimal;

SELECT '-92233720368547758.08'::fixeddecimal - '0.01'::fixeddecimal;

SELECT '92233720368547758.07'::fixeddecimal + '0.01'::fixeddecimal;

-- Should not overflow
SELECT '46116860184273879.03'::fixeddecimal * '2.00'::fixeddecimal;

-- Ensure this overflows
SELECT '46116860184273879.04'::fixeddecimal * '2.00'::fixeddecimal;

-- Should not overflow
SELECT '46116860184273879.03'::fixeddecimal / '0.50'::fixeddecimal;

-- Ensure this overflows
SELECT '46116860184273879.04'::fixeddecimal / '0.50'::fixeddecimal;

-- Ensure limits of int2 can be represented
SELECT '32767'::fixeddecimal::int2,'-32768'::fixeddecimal::int2;

-- Ensure overflow of int2 is detected
SELECT '32768'::fixeddecimal::int2;

-- Ensure underflow of int2 is detected
SELECT '-32769'::fixeddecimal::int2;

-- Ensure limits of int4 can be represented
SELECT '2147483647'::fixeddecimal::int4,'-2147483648'::fixeddecimal::int4;

-- Ensure overflow of int4 is detected
SELECT '2147483648'::fixeddecimal::int4;

-- Ensure underflow of int4 is detected
SELECT '-2147483649'::fixeddecimal::int4;

-- Ensure overflow is detected
SELECT SUM(a) FROM (VALUES('92233720368547758.07'::fixeddecimal),('0.01'::fixeddecimal)) a(a);

-- Ensure underflow is detected
SELECT SUM(a) FROM (VALUES('-92233720368547758.08'::fixeddecimal),('-0.01'::fixeddecimal)) a(a);
