/*-------------------------------------------------------------------------
 *
 * fixeddecimal.c
 *		  Fixed Decimal numeric type extension
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  fixeddecimal.c
 *
 * The research leading to these results has received funding from the European
 * Union’s Seventh Framework Programme (FP7/2007-2015) under grant agreement
 * n° 318633
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "funcapi.h"
#include "libpq/pqformat.h"
#include "access/hash.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#define MAXINT8LEN		25

/*
 * The scale which the number is actually stored.
 * For example: 100 will allow 2 decimal places of precision
 * This must always be a '1' followed by a number of '0's.
 */
#define FIXEDDECIMAL_MULTIPLIER 100LL

/*
 * Number of decimal places to store.
 * This number should be the number of decimal digits that it takes to
 * represent FIXEDDECIMAL_MULTIPLIER - 1
 */
#define FIXEDDECIMAL_SCALE 2

/*
 * This is bounded by the maximum and minimum values of int64.
 * 9223372036854775807 is 19 decimal digits long, but we we can only represent
 * this number / FIXEDDECIMAL_MULTIPLIER, so we must subtract
 * FIXEDDECIMAL_SCALE
 */
#define FIXEDDECIMAL_MAX_PRECISION 19 - FIXEDDECIMAL_SCALE

/* Define this if your compiler has _builtin_add_overflow() */
/* #define HAVE_BUILTIN_OVERFLOW */

#ifndef HAVE_BUILTIN_OVERFLOW
#define SAMESIGN(a,b)	(((a) < 0) == ((b) < 0))
#endif /* HAVE_BUILTIN_OVERFLOW */

/* 128bit int is required for multiply and divide */
#ifndef HAVE_INT128
#error "A working 128bit int type is required for fixed decimal"
#endif /* HAVE_INT128 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(fixeddecimalin);
PG_FUNCTION_INFO_V1(fixeddecimaltypmodin);
PG_FUNCTION_INFO_V1(fixeddecimaltypmodout);
PG_FUNCTION_INFO_V1(fixeddecimalout);
PG_FUNCTION_INFO_V1(fixeddecimalrecv);
PG_FUNCTION_INFO_V1(fixeddecimalsend);
PG_FUNCTION_INFO_V1(fixeddecimaleq);
PG_FUNCTION_INFO_V1(fixeddecimalne);
PG_FUNCTION_INFO_V1(fixeddecimallt);
PG_FUNCTION_INFO_V1(fixeddecimalgt);
PG_FUNCTION_INFO_V1(fixeddecimalle);
PG_FUNCTION_INFO_V1(fixeddecimalge);
PG_FUNCTION_INFO_V1(fixeddecimal_cmp);
PG_FUNCTION_INFO_V1(fixeddecimal_hash);
PG_FUNCTION_INFO_V1(fixeddecimalum);
PG_FUNCTION_INFO_V1(fixeddecimalup);
PG_FUNCTION_INFO_V1(fixeddecimalpl);
PG_FUNCTION_INFO_V1(fixeddecimalmi);
PG_FUNCTION_INFO_V1(fixeddecimalmul);
PG_FUNCTION_INFO_V1(fixeddecimaldiv);
PG_FUNCTION_INFO_V1(fixeddecimalabs);
PG_FUNCTION_INFO_V1(fixeddecimallarger);
PG_FUNCTION_INFO_V1(fixeddecimalsmaller);
PG_FUNCTION_INFO_V1(fixeddecimalint4pl);
PG_FUNCTION_INFO_V1(fixeddecimalint4mi);
PG_FUNCTION_INFO_V1(fixeddecimalint4mul);
PG_FUNCTION_INFO_V1(fixeddecimalint4div);
PG_FUNCTION_INFO_V1(fixeddecimal);
PG_FUNCTION_INFO_V1(int4fixeddecimalpl);
PG_FUNCTION_INFO_V1(int4fixeddecimalmi);
PG_FUNCTION_INFO_V1(int4fixeddecimalmul);
PG_FUNCTION_INFO_V1(int4fixeddecimaldiv);
PG_FUNCTION_INFO_V1(fixeddecimalint2pl);
PG_FUNCTION_INFO_V1(fixeddecimalint2mi);
PG_FUNCTION_INFO_V1(fixeddecimalint2mul);
PG_FUNCTION_INFO_V1(fixeddecimalint2div);
PG_FUNCTION_INFO_V1(int2fixeddecimalpl);
PG_FUNCTION_INFO_V1(int2fixeddecimalmi);
PG_FUNCTION_INFO_V1(int2fixeddecimalmul);
PG_FUNCTION_INFO_V1(int2fixeddecimaldiv);
PG_FUNCTION_INFO_V1(int4fixeddecimal);
PG_FUNCTION_INFO_V1(fixeddecimalint4);
PG_FUNCTION_INFO_V1(int2fixeddecimal);
PG_FUNCTION_INFO_V1(fixeddecimalint2);
PG_FUNCTION_INFO_V1(fixeddecimaltod);
PG_FUNCTION_INFO_V1(dtofixeddecimal);
PG_FUNCTION_INFO_V1(fixeddecimaltof);
PG_FUNCTION_INFO_V1(ftofixeddecimal);
PG_FUNCTION_INFO_V1(numeric_fixeddecimal);
PG_FUNCTION_INFO_V1(fixeddecimal_numeric);
PG_FUNCTION_INFO_V1(fixeddecimal_avg_accum);
PG_FUNCTION_INFO_V1(fixeddecimal_avg);
PG_FUNCTION_INFO_V1(fixeddecimal_sum);

/* Aggregate Internal State */
typedef struct FixedDecimalAggState
{
	MemoryContext agg_context;	/* context we're calculating in */
	int64		N;				/* count of processed numbers */
	int64		sumX;			/* sum of processed numbers */
} FixedDecimalAggState;

static char *pg_int64tostr(char *str, int64 value);
static char *pg_int64tostr_zeropad(char *str, int64 value, int64 padding);
static void apply_typmod(int64 value, int32 typmod, int precision, int scale);
static int64 scanfixeddecimal(const char *str, int *precision, int *scale);
static FixedDecimalAggState *makeFixedDecimalAggState(FunctionCallInfo fcinfo);
static void fixeddecimal_accum(FixedDecimalAggState *state, int64 newval);

/***********************************************************************
 **
 **		Routines for fixeddecimal
 **
 ***********************************************************************/

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

 /*
 * pg_int64tostr
 *		Converts 'value' into a decimal string representation of the number.
 *
 * Caller must ensure that 'str' points to enough memory to hold the result
 * (at least 21 bytes, counting a leading sign and trailing NUL).
 * Return value is a pointer to the new NUL terminated end of string.
 */
static char *
pg_int64tostr(char *str, int64 value)
{
	char *start;
	char *end;

	/*
	 * Handle negative numbers in a special way. We can't just append a '-'
	 * prefix and reverse the sign as on two's complement machines negative
	 * numbers can be 1 further from 0 than positive numbers, we do it this way
	 * so we properly handle the smallest possible value.
	 */
	if (value < 0)
	{
		*str++ = '-';

		/* mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int64		remainder;
			int64		oldval = value;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + -remainder;
		} while (value != 0);
	}
	else
	{
		/* mark the position we must reverse the string from. */
		start = str;
		do
		{
			int64		remainder;
			int64		oldval = value;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + remainder;
		} while (value != 0);
	}

	/* Add trailing NUL byte, and back up 'str' to the last character. */
	end = str;
	*str-- = '\0';

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;
		*start++ = *str;
		*str-- = swap;
	}
	return end;
}

/*
 * pg_int64tostr_zeropad
 *		Converts 'value' into a decimal string representation of the number.
 *		'padding' specifies the minimum width of the number. Any extra space
 *		is filled up by prefixing the number with zeros. The return value is a
 *		pointer to the NUL terminated end of the string.
 *
 * Note: Callers should ensure that 'padding' is above zero.
 * Note: This function is optimized for the case where the number is not too
 *		 big to fit inside of the specified padding.
 * Note: Caller must ensure that 'str' points to enough memory to hold the
		 result (at least 21 bytes, counting a leading sign and trailing NUL,
		 or padding + 1 bytes, whichever is larger).
 */
static char *
pg_int64tostr_zeropad(char *str, int64 value, int64 padding)
{
	char	   *start = str;
	char	   *end = &str[padding];
	int64 		num = value;

	Assert(padding > 0);

	/*
	 * Handle negative numbers in a special way. We can't just append a '-'
	 * prefix and reverse the sign as on two's complement machines negative
	 * numbers can be 1 further from 0 than positive numbers, we do it this way
	 * so we properly handle the smallest possible value.
	 */
	if (num < 0)
	{
		*start++ = '-';
		padding--;

		/*
		 * Build the number starting at the end. Here remainder will be a
		 * negative number, we must reverse this sign on this before adding
		 * '0' in order to get the correct ASCII digit
		 */
		while (padding--)
		{
			int64		remainder;
			int64		oldval = num;

			num /= 10;
			remainder = oldval - num * 10;
			start[padding] = '0' + -remainder;
		}
	}
	else
	{
		/* build the number starting at the end */
		while (padding--)
		{
			int64		remainder;
			int64		oldval = num;

			num /= 10;
			remainder = oldval - num * 10;
			start[padding] = '0' + remainder;
		}
	}

	/*
	 * If padding was not high enough to fit this number then num won't have
	 * been divided down to zero. We'd better have another go, this time we
	 * know there won't be any zero padding required so we can just enlist the
	 * help of pg_int64tostr()
	 */
	if (num != 0)
		return pg_int64tostr(str, value);

	*end = '\0';
	return end;
}

/*
 * scanfixeddecimal --- try to parse a string into a fixeddecimal.
 */
static int64
scanfixeddecimal(const char *str, int *precision, int *scale)
{
	const char *ptr = str;
	int64		integralpart = 0;
	int64		fractionalpart = 0;
	bool		negative;
	int			vprecision = 0;
	int			vscale = 0;

	/*
	 * Do our own scan, rather than relying on sscanf which might be broken
	 * for long long.
	 */

	/* skip leading spaces */
	while (isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		negative = true;
		ptr++;

		while (isdigit((unsigned char) *ptr))
		{
			int64		tmp = integralpart * 10 - (*ptr++ - '0');

			vprecision++;
			if ((tmp / 10) != integralpart)		/* underflow? */
			{
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					   errmsg("value \"%s\" is out of range for type fixeddecimal",
							  str)));
			}
			integralpart = tmp;
		}
	}
	else
	{
		negative = false;

		if (*ptr == '+')
			ptr++;

		while (isdigit((unsigned char) *ptr))
		{
			int64		tmp = integralpart * 10 + (*ptr++ - '0');

			vprecision++;
			if ((tmp / 10) != integralpart)		/* overflow? */
			{
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					   errmsg("value \"%s\" is out of range for type fixeddecimal",
							  str)));
			}
			integralpart = tmp;
		}
	}

	/* process the part after the decimal point */
	if (*ptr == '.')
	{
		int64 multiplier = FIXEDDECIMAL_MULTIPLIER;
		ptr++;

		while (isdigit((unsigned char) *ptr) && multiplier > 1)
		{
			multiplier /= 10;
			fractionalpart += (*ptr++ - '0') * multiplier;
			vscale++;
		}

		/*
		 * Eat into any excess precision digits.
		 * XXX These are ignored, should we error instead?
		 */
		while (isdigit((unsigned char) *ptr))
			ptr++, vscale++;
	}

	/* consume any remaining space chars */
	while (isspace((unsigned char) *ptr))
		ptr++;

	if (*ptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			   errmsg("value \"%s\" is out of range for type fixeddecimal", str)));

	*precision = vprecision;
	*scale = vscale;

	if (negative)
	{

		int64 value;

#ifdef HAVE_BUILTIN_OVERFLOW
		int64 multiplier = FIXEDDECIMAL_MULTIPLIER;
		if (__builtin_mul_overflow(integralpart, multiplier, &value))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));

		if (__builtin_sub_overflow(value, fractionalpart, &value))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));
		return value;

#else
		value = integralpart * FIXEDDECIMAL_MULTIPLIER;
		if (value != 0 && (!SAMESIGN(value, integralpart) ||
						   !SAMESIGN(value - fractionalpart, value) ||
						   !SAMESIGN(value - fractionalpart, value)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));

		return value - fractionalpart;
#endif /* HAVE_BUILTIN_OVERFLOW */

	}
	else
	{
		int64 value;

#ifdef HAVE_BUILTIN_OVERFLOW
		if (__builtin_mul_overflow(integralpart, FIXEDDECIMAL_MULTIPLIER, &value))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));

		if (__builtin_add_overflow(value, fractionalpart, &value))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));
		return value;
#else
		value = integralpart * FIXEDDECIMAL_MULTIPLIER;
		if (value != 0 && (!SAMESIGN(value, integralpart) ||
						   !SAMESIGN(value - fractionalpart, value) ||
						   !SAMESIGN(value + fractionalpart, value)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				   errmsg("value \"%s\" is out of range for type fixeddecimal",
						  str)));

		return value + fractionalpart;
#endif /* HAVE_BUILTIN_OVERFLOW */

	}
}

/*
 * fixeddecimalin()
 */
Datum
fixeddecimalin(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	int			precision;
	int			scale;
	int64		result = scanfixeddecimal(str, &precision, &scale);

	apply_typmod(result, typmod, precision, scale);

	PG_RETURN_INT64(result);
}

static void
apply_typmod(int64 value, int32 typmod, int precision, int scale)
{
	int			precisionlimit;
	int			scalelimit;
	int			maxdigits;

	/* Do nothing if we have a default typmod (-1) */
	if (typmod < (int32) (VARHDRSZ))
		return;

	typmod -= VARHDRSZ;
	precisionlimit = (typmod >> 16) & 0xffff;
	scalelimit = typmod & 0xffff;
	maxdigits = precisionlimit - scalelimit;

	if (scale > scalelimit)

	if (scale != FIXEDDECIMAL_SCALE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FIXEDDECIMAL scale must be %d",
						FIXEDDECIMAL_SCALE)));

	if (precision > maxdigits)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("FIXEDDECIMAL field overflow"),
				 errdetail("A field with precision %d, scale %d must round to an absolute value less than %s%d.",
						   precision, scale,
						   /* Display 10^0 as 1 */
						   maxdigits ? "10^" : "",
						   maxdigits ? maxdigits : 1
						   )));

}

Datum
fixeddecimaltypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n == 2)
	{
		/*
		 * we demand that the precision is at least the scale, since later we
		 * enforce that the scale is exactly FIXEDDECIMAL_SCALE
		 */
		if (tl[0] < FIXEDDECIMAL_SCALE || tl[0] > FIXEDDECIMAL_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("FIXEDDECIMAL precision %d must be between %d and %d",
							tl[0], FIXEDDECIMAL_SCALE, FIXEDDECIMAL_MAX_PRECISION)));

		if (tl[1] != FIXEDDECIMAL_SCALE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FIXEDDECIMAL scale must be %d",
							FIXEDDECIMAL_SCALE)));

		typmod = ((tl[0] << 16) | tl[1]) + VARHDRSZ;
	}
	else if (n == 1)
	{
		if (tl[0] < FIXEDDECIMAL_SCALE || tl[0] > FIXEDDECIMAL_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("FIXEDDECIMAL precision %d must be between %d and %d",
							tl[0], FIXEDDECIMAL_SCALE, FIXEDDECIMAL_MAX_PRECISION)));

		/* scale defaults to FIXEDDECIMAL_SCALE */
		typmod = ((tl[0] << 16) | FIXEDDECIMAL_SCALE) + VARHDRSZ;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid FIXEDDECIMAL type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

Datum
fixeddecimaltypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *res = (char *) palloc(64);

	if (typmod >= 0)
		snprintf(res, 64, "(%d,%d)",
				 ((typmod - VARHDRSZ) >> 16) & 0xffff,
				 (typmod - VARHDRSZ) & 0xffff);
	else
		*res = '\0';

	PG_RETURN_CSTRING(res);
}


/*
 * fixeddecimalout()
 */
Datum
fixeddecimalout(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);
	char		buf[MAXINT8LEN + 1];
	char	   *ptr = &buf[0];
	int64		integralpart = val / FIXEDDECIMAL_MULTIPLIER;
	int64		fractionalpart = val % FIXEDDECIMAL_MULTIPLIER;


	if (val < 0)
	{
		fractionalpart = -fractionalpart;

		/*
		 * Handle special case for negative numbers where the intergral part
		 * is zero. pg_int64tostr() won't prefix with "-0" in this case, so
		 * we'll do it manually
		 */
		if (integralpart == 0)
			*ptr++ = '-';
	}
	ptr = pg_int64tostr(ptr, integralpart);
	*ptr++ = '.';
	ptr = pg_int64tostr_zeropad(ptr, fractionalpart, FIXEDDECIMAL_SCALE);

	PG_RETURN_CSTRING(pnstrdup(buf, ptr - &buf[0]));
}

/*
 *		fixeddecimalrecv			- converts external binary format to int8
 */
Datum
fixeddecimalrecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_INT64(pq_getmsgint64(buf));
}

/*
 *		fixeddecimalsend			- converts int8 to binary format
 */
Datum
fixeddecimalsend(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*----------------------------------------------------------
 *	Relational operators for fixeddecimals, including cross-data-type comparisons.
 *---------------------------------------------------------*/

Datum
fixeddecimaleq(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 == val2);
}

Datum
fixeddecimalne(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 != val2);
}

Datum
fixeddecimallt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 < val2);
}

Datum
fixeddecimalgt(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 > val2);
}

Datum
fixeddecimalle(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 <= val2);
}

Datum
fixeddecimalge(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	PG_RETURN_BOOL(val1 >= val2);
}

Datum
fixeddecimal_cmp(PG_FUNCTION_ARGS)
{
	int64		val1 = PG_GETARG_INT64(0);
	int64		val2 = PG_GETARG_INT64(1);

	if (val1 == val2)
		PG_RETURN_INT32(0);
	else if (val1 < val2)
		PG_RETURN_INT32(-1);
	else
		PG_RETURN_INT32(1);
}

Datum
fixeddecimal_hash(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);
	Datum		result;

	result = hash_any((unsigned char *) &val, sizeof(int64));
	PG_RETURN_DATUM(result);
}

/*----------------------------------------------------------
 *	Arithmetic operators on fixeddecimal.
 *---------------------------------------------------------*/

Datum
fixeddecimalum(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	int64 zero = 0;

 	if (__builtin_sub_overflow(zero, arg, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = -arg;
	/* overflow check (needed for INT64_MIN) */
	if (arg != 0 && SAMESIGN(result, arg))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */
	PG_RETURN_INT64(result);
}

Datum
fixeddecimalup(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);

	PG_RETURN_INT64(arg);
}

Datum
fixeddecimalpl(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(arg1, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 + arg2;

	/*
	 * Overflow check.  If the inputs are of different signs then their sum
	 * cannot overflow.  If the inputs are of the same sign, their sum had
	 * better be that sign too.
	 */
	if (SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalmi(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_sub_overflow(arg1, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 - arg2;

	/*
	 * Overflow check.  If the inputs are of the same sign then their
	 * difference cannot overflow.  If they are of different signs then the
	 * result should be of the same sign as the first input.
	 */
	if (!SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalmul(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int128		result;

	/* We need to promote this to 128bit as we may overflow int64 here.
	 * Remember that arg2 is the number multiplied by
	 * FIXEDDECIMAL_MULTIPLIER, we must divide the result by this to get
	 * the correct result.
	 */
	result = (int128) arg1 * arg2 / FIXEDDECIMAL_MULTIPLIER;

	if (result != ((int64) result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));

	PG_RETURN_INT64((int64) result);
}

Datum
fixeddecimaldiv(PG_FUNCTION_ARGS)
{
	int64		dividend = PG_GETARG_INT64(0);
	int64		divisor = PG_GETARG_INT64(1);
	int128		result;

	if (divisor == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	if (divisor == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * this can't overflow, but we can end up with a number that's too big for
	 * int64
	 */
	result = (int128) dividend * FIXEDDECIMAL_MULTIPLIER / divisor;

	if (result != ((int64) result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));

	PG_RETURN_INT64((int64) result);
}

/* fixeddecimalabs()
 * Absolute value
 */
Datum
fixeddecimalabs(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		result;

	result = (arg1 < 0) ? -arg1 : arg1;
	/* overflow check (needed for INT64_MIN) */
	if (result < 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
	PG_RETURN_INT64(result);
}


Datum
fixeddecimallarger(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

	result = ((arg1 > arg2) ? arg1 : arg2);

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalsmaller(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

	result = ((arg1 < arg2) ? arg1 : arg2);

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint4pl(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		adder = PG_GETARG_INT32(1) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(arg1, adder, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 + adder;

	/*
	 * Overflow check.  If the inputs are of different signs then their sum
	 * cannot overflow.  If the inputs are of the same sign, their sum had
	 * better be that sign too.
	 */
	if (SAMESIGN(arg1, adder) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint4mi(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		subtractor = PG_GETARG_INT32(1) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;


#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_sub_overflow(arg1, subtractor, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 - subtractor;

	/*
	 * Overflow check.  If the inputs are of the same sign then their
	 * difference cannot overflow.  If they are of different signs then the
	 * result should be of the same sign as the first input.
	 */
	if (!SAMESIGN(arg1, subtractor) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint4mul(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_mul_overflow(arg1, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 * arg2;

	/*
	 * Overflow check.  We basically check to see if result / arg1 gives arg2
	 * again.  There is one case where this fails: arg1 = 0 (which cannot
	 * overflow).
	 *
	 * Since the division is likely much more expensive than the actual
	 * multiplication, we'd like to skip it where possible.  The best bang for
	 * the buck seems to be to check whether both inputs are in the int32
	 * range; if so, no overflow is possible.
	 */
	if (arg1 != (int64) ((int32) arg1) &&
		result / arg1 != arg2)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint4div(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int32		arg2 = PG_GETARG_INT32(1);
	int64		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
#ifdef HAVE_BUILTIN_OVERFLOW
		int64 zero = 0;
		if (__builtin_sub_overflow(zero, arg1, &result))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("fixeddecimal out of range")));
#else
		result = -arg1;
		/* overflow check (needed for INT64_MIN) */
		if (arg1 != 0 && SAMESIGN(result, arg1))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

		PG_RETURN_INT64(result);
	}

	/* No overflow is possible */

	result = arg1 / arg2;

	PG_RETURN_INT64(result);
}

Datum
int4fixeddecimalpl(PG_FUNCTION_ARGS)
{
	int64		adder = PG_GETARG_INT32(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(adder, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = adder + arg2;

	/*
	 * Overflow check.  If the inputs are of different signs then their sum
	 * cannot overflow.  If the inputs are of the same sign, their sum had
	 * better be that sign too.
	 */
	if (SAMESIGN(adder, arg2) && !SAMESIGN(result, adder))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int4fixeddecimalmi(PG_FUNCTION_ARGS)
{
	int64		subtractor = PG_GETARG_INT32(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_sub_overflow(subtractor, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = subtractor - arg2;

	/*
	 * Overflow check.  If the inputs are of the same sign then their
	 * difference cannot overflow.  If they are of different signs then the
	 * result should be of the same sign as the first input.
	 */
	if (!SAMESIGN(subtractor, arg2) && !SAMESIGN(result, subtractor))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int4fixeddecimalmul(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_mul_overflow(arg1, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 * arg2;

	/*
	 * Overflow check.  We basically check to see if result / arg2 gives arg1
	 * again.  There is one case where this fails: arg2 = 0 (which cannot
	 * overflow).
	 *
	 * Since the division is likely much more expensive than the actual
	 * multiplication, we'd like to skip it where possible.  The best bang for
	 * the buck seems to be to check whether both inputs are in the int32
	 * range; if so, no overflow is possible.
	 */
	if (arg2 != (int64) ((int32) arg2) &&
		result / arg2 != arg1)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int4fixeddecimaldiv(PG_FUNCTION_ARGS)
{
	int32		arg1 = PG_GETARG_INT32(0);
	float8		arg2 = (float8) PG_GETARG_INT64(1) / (float8) FIXEDDECIMAL_MULTIPLIER;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/* No overflow is possible */
	PG_RETURN_FLOAT8((float8) arg1 / arg2);
}

Datum
fixeddecimalint2pl(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		adder = PG_GETARG_INT16(1) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(arg1, adder, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 + adder;

	/*
	 * Overflow check.  If the inputs are of different signs then their sum
	 * cannot overflow.  If the inputs are of the same sign, their sum had
	 * better be that sign too.
	 */
	if (SAMESIGN(arg1, adder) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint2mi(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int64		subtractor = PG_GETARG_INT16(1) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_sub_overflow(arg1, subtractor, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 - subtractor;

	/*
	 * Overflow check.  If the inputs are of the same sign then their
	 * difference cannot overflow.  If they are of different signs then the
	 * result should be of the same sign as the first input.
	 */
	if (!SAMESIGN(arg1, subtractor) && !SAMESIGN(result, arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint2mul(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_mul_overflow(arg1, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = arg1 * arg2;

	/*
	 * Overflow check.  We basically check to see if result / arg1 gives arg2
	 * again.  There is one case where this fails: arg1 = 0 (which cannot
	 * overflow).
	 *
	 * Since the division is likely much more expensive than the actual
	 * multiplication, we'd like to skip it where possible.  The best bang for
	 * the buck seems to be to check whether both inputs are in the int32
	 * range; if so, no overflow is possible.
	 */
	if (arg1 != (int64) ((int32) arg1) &&
		result / arg1 != arg2)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
fixeddecimalint2div(PG_FUNCTION_ARGS)
{
	int64		arg1 = PG_GETARG_INT64(0);
	int16		arg2 = PG_GETARG_INT16(1);
	int64		result;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/*
	 * INT64_MIN / -1 is problematic, since the result can't be represented on
	 * a two's-complement machine.  Some machines produce INT64_MIN, some
	 * produce zero, some throw an exception.  We can dodge the problem by
	 * recognizing that division by -1 is the same as negation.
	 */
	if (arg2 == -1)
	{
#ifdef HAVE_BUILTIN_OVERFLOW
		int64 zero = 0;
		if (__builtin_sub_overflow(zero, arg1, &result))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("fixeddecimal out of range")));
#else
		result = -arg1;
		/* overflow check (needed for INT64_MIN) */
		if (arg1 != 0 && SAMESIGN(result, arg1))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

		PG_RETURN_INT64(result);
	}

	/* No overflow is possible */
	result = arg1 / arg2;

	PG_RETURN_INT64(result);
}

Datum
int2fixeddecimalpl(PG_FUNCTION_ARGS)
{
	int64		adder = PG_GETARG_INT16(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(adder, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = adder + arg2;

	/*
	 * Overflow check.  If the inputs are of different signs then their sum
	 * cannot overflow.  If the inputs are of the same sign, their sum had
	 * better be that sign too.
	 */
	if (SAMESIGN(adder, arg2) && !SAMESIGN(result, adder))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int2fixeddecimalmi(PG_FUNCTION_ARGS)
{
	int64		subtractor = PG_GETARG_INT16(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_sub_overflow(subtractor, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = subtractor - arg2;

	/*
	 * Overflow check.  If the inputs are of the same sign then their
	 * difference cannot overflow.  If they are of different signs then the
	 * result should be of the same sign as the first input.
	 */
	if (!SAMESIGN(subtractor, arg2) && !SAMESIGN(result, subtractor))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int2fixeddecimalmul(PG_FUNCTION_ARGS)
{
	int64		multiplier = PG_GETARG_INT16(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		arg2 = PG_GETARG_INT64(1);
	int64		result;

#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_mul_overflow(multiplier, arg2, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#else
	result = multiplier * arg2;

	/*
	 * Overflow check.  We basically check to see if result / arg2 gives
	 * multiplier again.  There is one case where this fails: arg2 = 0 (which
	 * cannot overflow).
	 *
	 * Since the division is likely much more expensive than the actual
	 * multiplication, we'd like to skip it where possible.  The best bang for
	 * the buck seems to be to check whether both inputs are in the int32
	 * range; if so, no overflow is possible.
	 */
	if (arg2 != (int64) ((int32) arg2) &&
		result / arg2 != multiplier)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
#endif /* HAVE_BUILTIN_OVERFLOW */

	PG_RETURN_INT64(result);
}

Datum
int2fixeddecimaldiv(PG_FUNCTION_ARGS)
{
	int16		arg1 = PG_GETARG_INT16(0);
	float8		arg2 = PG_GETARG_INT64(1) / (float8) FIXEDDECIMAL_MULTIPLIER;

	if (arg2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		/* ensure compiler realizes we mustn't reach the division (gcc bug) */
		PG_RETURN_NULL();
	}

	/* No overflow is possible */
	PG_RETURN_INT64((float8) arg1 / arg2);
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

/*
 * fixeddecimal serves as casting function for fixeddecimal to fixeddecimal.
 * The only serves to generate an error if the fixedecimal is too big for the
 * specified typmod.
 */
Datum
fixeddecimal(PG_FUNCTION_ARGS)
{
	int64		num = PG_GETARG_INT64(0);
	int32		typmod = PG_GETARG_INT32(1);
	Datum		result;

	/* no need to check typmod if it's -1 */
	if (typmod != -1)
	{
		result = DirectFunctionCall1(fixeddecimalout, num);
		result = DirectFunctionCall3(fixeddecimalin, result, 0, typmod);
	}
	PG_RETURN_INT64(num);
}

Datum
int4fixeddecimal(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT32(0);

	PG_RETURN_INT64(arg * FIXEDDECIMAL_MULTIPLIER);
}

Datum
fixeddecimalint4(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0) / FIXEDDECIMAL_MULTIPLIER;

	if ((int32) arg != arg)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	PG_RETURN_INT32((int32) arg);
}

Datum
int2fixeddecimal(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT16(0);

	PG_RETURN_INT64(arg * FIXEDDECIMAL_MULTIPLIER);
}

Datum
fixeddecimalint2(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0) / FIXEDDECIMAL_MULTIPLIER;

	if ((int16) arg != arg)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16((int16) arg);
}

Datum
fixeddecimaltod(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);
	float8		result;

	result = (float8) arg / FIXEDDECIMAL_MULTIPLIER;

	PG_RETURN_FLOAT8(result);
}

/* dtofixeddecimal()
 * Convert float8 to fixeddecimal
 */
Datum
dtofixeddecimal(PG_FUNCTION_ARGS)
{
	float8		arg = PG_GETARG_FLOAT8(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;

	/* Round arg to nearest integer (but it's still in float form) */
	arg = rint(arg);

	/*
	 * Does it fit in an int64?  Avoid assuming that we have handy constants
	 * defined for the range boundaries, instead test for overflow by
	 * reverse-conversion.
	 */
	result = (int64) arg;

	if ((float8) result != arg)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));

	PG_RETURN_INT64(result);
}

Datum
fixeddecimaltof(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);
	float4		result;

	result = (float4) arg / FIXEDDECIMAL_MULTIPLIER;

	PG_RETURN_FLOAT4(result);
}

/* ftofixeddecimal()
 * Convert float4 to fixeddecimal.
 */
Datum
ftofixeddecimal(PG_FUNCTION_ARGS)
{
	float4		arg = PG_GETARG_FLOAT4(0) * FIXEDDECIMAL_MULTIPLIER;
	int64		result;
	float8		darg;

	/* Round arg to nearest integer (but it's still in float form) */
	darg = rint(arg);

	/*
	 * Does it fit in an int64?  Avoid assuming that we have handy constants
	 * defined for the range boundaries, instead test for overflow by
	 * reverse-conversion.
	 */
	result = (int64) darg;

	if ((float8) result != darg)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));

	PG_RETURN_INT64(result);
}


Datum
fixeddecimal_numeric(PG_FUNCTION_ARGS)
{
	int64		num = PG_GETARG_INT64(0);
	char	   *tmp;
	Datum		result;

	tmp = DatumGetCString(DirectFunctionCall1(fixeddecimalout,
											  Int64GetDatum(num)));

	result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmp), 0, -1);

	pfree(tmp);

	PG_RETURN_DATUM(result);
}

Datum
numeric_fixeddecimal(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (numeric_is_nan(num))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot convert NaN to fixeddecimal")));

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall3(fixeddecimalin, CStringGetDatum(tmp), 0, -1);

	pfree(tmp);

	PG_RETURN_DATUM(result);
}


/* Aggregate Support */

static FixedDecimalAggState *
makeFixedDecimalAggState(FunctionCallInfo fcinfo)
{
	FixedDecimalAggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (FixedDecimalAggState *) palloc0(sizeof(FixedDecimalAggState));
	state->agg_context = agg_context;

	MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * Accumulate a new input value for fixeddecimal aggregate functions.
 */
static void
fixeddecimal_accum(FixedDecimalAggState *state, int64 newval)
{
#ifdef HAVE_BUILTIN_OVERFLOW
	if (__builtin_add_overflow(state->sumX, newval, &state->sumX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("fixeddecimal out of range")));
	state->N++;
#else
	if (state->N++ > 0)
	{
		int64 result = state->sumX + newval;

		if (SAMESIGN(state->sumX, newval) && !SAMESIGN(result, state->sumX))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("fixeddecimal out of range")));

		state->sumX = result;
	}
	else
		state->sumX = newval;
#endif /* HAVE_BUILTIN_OVERFLOW */
}

Datum
fixeddecimal_avg_accum(PG_FUNCTION_ARGS)
{
	FixedDecimalAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (FixedDecimalAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeFixedDecimalAggState(fcinfo);

	if (!PG_ARGISNULL(1))
		fixeddecimal_accum(state, PG_GETARG_INT64(1));

	PG_RETURN_POINTER(state);
}

Datum
fixeddecimal_avg(PG_FUNCTION_ARGS)
{
	FixedDecimalAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (FixedDecimalAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(state->sumX / state->N);
}

Datum
fixeddecimal_sum(PG_FUNCTION_ARGS)
{
	FixedDecimalAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (FixedDecimalAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(state->sumX);
}
