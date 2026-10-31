#include "postgres.h"

#include <math.h>

#include "funcapi.h"
#include "fmgr.h"
#include "utils/numeric.h"
#include "utils/builtins.h"


#include "orafce.h"
#include "builtins.h"

PG_FUNCTION_INFO_V1(orafce_reminder_smallint);
PG_FUNCTION_INFO_V1(orafce_reminder_int);
PG_FUNCTION_INFO_V1(orafce_reminder_bigint);
PG_FUNCTION_INFO_V1(orafce_reminder_numeric);

static int64
integer_remainder(int64 dividend, int64 divisor)
{
	int64		result;
	uint64		divisor_magnitude;
	uint64		remainder_magnitude;

	if (divisor == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	if (divisor == -1)
		return 0;

	result = dividend % divisor;

	/* Unsigned magnitudes also represent the magnitude of INT64_MIN. */
	divisor_magnitude = divisor < 0 ? -(uint64) divisor : (uint64) divisor;
	remainder_magnitude = result < 0 ? -(uint64) result : (uint64) result;

	if (remainder_magnitude >= divisor_magnitude - remainder_magnitude)
	{
		if ((result > 0) == (divisor > 0))
			result -= divisor;
		else
			result += divisor;
	}

	return result;
}

/*
 * CREATE OR REPLACE FUNCTION oracle.remainder(smallint, smallint)
 * RETURNS smallint
 */
Datum
orafce_reminder_smallint(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT16(integer_remainder(PG_GETARG_INT16(0), PG_GETARG_INT16(1)));
}

/*
 * CREATE OR REPLACE FUNCTION oracle.remainder(int, int)
 * RETURNS int
 */
Datum
orafce_reminder_int(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(integer_remainder(PG_GETARG_INT32(0), PG_GETARG_INT32(1)));
}

/*
 * CREATE OR REPLACE FUNCTION oracle.remainder(bigint, bigint)
 * RETURNS bigint
 */
Datum
orafce_reminder_bigint(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(integer_remainder(PG_GETARG_INT64(0), PG_GETARG_INT64(1)));
}

/*
 * This will handle NaN and Infinity cases
 */
static Numeric
duplicate_numeric(Numeric num)
{
	Numeric		res;

	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));
	return res;
}

static Numeric
get_numeric_in(const char *str)
{
	return DatumGetNumeric(
						   DirectFunctionCall3(numeric_in,
											   CStringGetDatum(str),
											   ObjectIdGetDatum(0),
											   Int32GetDatum(-1)));
}

static bool
orafce_numeric_is_inf(Numeric num)
{

#if PG_VERSION_NUM >= 140000

	return numeric_is_inf(num);

#else

	/* older releases doesn't support +-Infinitity in numeric type */

	return false;

#endif

}

/*
 * CREATE OR REPLACE FUNCTION oracle.remainder(numeric, numeric)
 * RETURNS numeric
 */
Datum
orafce_reminder_numeric(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Datum		result;
	Datum		zero;
	Datum		magnitude;
	Datum		divisor_magnitude;

	if (numeric_is_nan(num1))
		PG_RETURN_NUMERIC(duplicate_numeric(num1));
	if (numeric_is_nan(num2))
		PG_RETURN_NUMERIC(duplicate_numeric(num2));

	zero = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
	if (DatumGetBool(DirectFunctionCall2(numeric_eq, NumericGetDatum(num2), zero)))
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	if (orafce_numeric_is_inf(num1))
		PG_RETURN_NUMERIC(get_numeric_in("NaN"));

	if (orafce_numeric_is_inf(num2))
		PG_RETURN_NUMERIC(duplicate_numeric(num1));

	result = DirectFunctionCall2(numeric_mod, NumericGetDatum(num1), NumericGetDatum(num2));
	magnitude = DirectFunctionCall1(numeric_abs, result);
	divisor_magnitude = DirectFunctionCall1(numeric_abs, NumericGetDatum(num2));

	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp,
										 DirectFunctionCall2(numeric_add, magnitude, magnitude),
										 divisor_magnitude)) >= 0)
	{
		if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, result, zero)) > 0)
			result = DirectFunctionCall2(numeric_sub, result, divisor_magnitude);
		else
			result = DirectFunctionCall2(numeric_add, result, divisor_magnitude);
	}

	PG_RETURN_DATUM(result);
}
