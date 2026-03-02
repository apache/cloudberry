FIXEDDECIMAL
============

Works with PostgreSQL 9.5 or higher.
The latest test was executed on version 12.

Overview
--------

FixedDecimal is a fixed precision decimal type which provides a subset of the
features of PostgreSQL's builtin NUMERIC type, but with vastly increased
performance. Fixeddecimal is targeted to cases where performance and disk space
are a critical.

Just use FIXEDDECIMAL(n, 2) rather than NUMERIC(n, 2) for n=3..17

Often there are data storage requirements where the built in REAL and
DOUBLE PRECISION types cannot be used due to the non-exact representation of
numbers using these types, e.g. where monetary values need to be stored. In many
of these cases NUMERIC is an almost perfect type, although with NUMERIC
performance is no match for the performance of REAL or DOUBLE PRECISION, as
these use CPU native processor types. 

FixedDecimal delivers performance advantages over NUMERIC with full precision for
addition and subtraction. Just as occurs with REAL and DOUBLE PRECISION, there
are some caveats for multiplication and division.

Behavioural differences between FIXEDDECIMAL and NUMERIC
--------------------------------------------------------

It should be noted that there are cases were FIXEDDECIMAL behaves differently
from NUMERIC.

1.	FIXEDDECIMAL has a much more limited range of values than NUMERIC. By
	default this type can represent a maximum range of FIXEDDECIMAL(17,2),
	although the underlying type is unable to represent the full range of
	of the 17th significant digit.

2.	FIXEDDECIMAL always rounds towards zero.

3.	FIXEDDECIMAL does not support NaN.

4.	Any attempt to use a numerical scale other than the default fixed scale
	will result in an error. e.g. SELECT '123.223'::FIXEDDECIMAL(4,1) will fail
	by default, as the default scale is 2, not 1.

Internals
---------

FIXEDDECIMAL internally uses a 64bit integer type for its underlying storage.
This is what gives the type the performance advantage over NUMERIC, as most
calculations are performed as native processor operations rather than software
implementations as in the case with NUMERIC.

FIXEDDECIMAL has a fixed scale value, which by default is 2. Internally numbers
are stores as the actual value multiplied by 100. e.g. 50 would be stored as
5000, and 1.23 would be stored as 123. This internal representation allows very
fast and accurate addition and subtraction between two fixeddecimal types.

Multiplication between two fixeddecimal types is slightly more complex.  If we
perform 2.00 * 3.00 in fixeddecimal, internally these numbers would be 200 and
300 respectively, so internally 200 * 300 becomes 60000, which must be divided
by 100 in order to obtain the correct internal result of 600, which of course
externally is 6.00. This method of multiplication is hazard to overflowing the
internal 64bit integer type, for this reason all multiplication and division is
performed using 128bit integer types.

Internally, by default, FIXEDDECIMAL is limited to a maximum value of
92233720368547758.07 and a minimum value of -92233720368547758.08. If any of
these limits are exceeded the query will fail with an error.

By default the scale of FIXEDDECIMAL is 2 decimal digits after the decimal
point. This value may be changed only by recompiling FIXEDDECIMAL from source,
which is done by altering the FIXEDDECIMAL_MULTIPLIER and FIXEDDECIMAL_SCALE
constants. If the FIXEDDECIMAL_SCALE was set to 4, then the
FIXEDDECIMAL_MULTIPLIER should be set to 10000. Doing this will mean that the
absolute limits of the type decrease to a range of -922337203685477.5808 to
922337203685477.5807.

Caution
-------

FIXEDDECIMAL is mainly intended as a fast and efficient data type which will
suit a limited set numerical data storage and retrieval needs. Complex
arithmetic could be said to be one of fixeddecimal's limits. As stated above
division always rounds towards zero. Please observe the following example:

```
test=# select '2.00'::fixeddecimal / '3.00'::fixeddecimal;
 ?column?
----------
 0.66
(1 row)
```

A workaround of this would be to perform all calculations in NUMERIC, and
ROUND() the result into the maximum scale of FIXEDDECIMAL:

```
test=# select round('2.00'::numeric / '3.00'::numeric, 2)::fixeddecimal;
 ?column?
----------
 0.67
(1 row)
```

It should also be noted that excess precision is ignored by fixeddecimal.
With a FIXEDDECIMAL_PRECISION of 2, any value after the 2nd digit following
the decimal point is completely ignored rather than rounded. The following
example demonstrates this:

```
test=# select '1.239'::fixeddecimal;
 fixeddecimal
--------------
 1.23
(1 row)
```

It is especially important to remember that this truncation also occurs during
arithmetic. Notice in the following example the result is 1120 rather than
1129, since 1.129 is immediately rounded to 1.12 on input.

```
test=# select '1000'::fixeddecimal * '1.129'::fixeddecimal;
 ?column?
----------
 1120.00
(1 row)
```

Installation
------------

To install fixeddecimal you must build the extension from source code.

First ensure that your PATH environment variable is setup to find the correct
PostgreSQL installation first. You can check this by typing running the
pg_config command and checking the paths listed.

Once you are confident your PATH variable is set correctly

```
make
make install
make installcheck
```

From psql, in order to create the extension you must type:

```
CREATE EXTENSION fixeddecimal;
```

Credits
-------

fixeddecimal is open source using The PostgreSQL Licence, copyright is novated to the PostgreSQL Global Development Group.

Source code developed by 2ndQuadrant, as part of the AXLE project (http://axleproject.eu) which received funding from the European Union’s Seventh Framework Programme (FP7/2007-2015) under grant agreement n° 318633

Lead Developer - David Rowley
