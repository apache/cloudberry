/*
 * utl_raw.c
 *
 * C implementations of the UTL_RAW bitwise operators (BIT_AND, BIT_OR,
 * BIT_XOR).  These process a raw (bytea) value one byte at a time, which
 * is quadratic and slow when written as an SQL/PLpgSQL byte loop that
 * rebuilds the accumulator on every iteration, so they live in C.
 *
 * Oracle semantics for two operands of unequal length: the operator is
 * applied up to the length of the shorter operand and the untouched tail
 * of the longer operand is appended, so the result length equals the
 * longer of the two inputs.
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

#include "orafce.h"
#include "builtins.h"

PG_FUNCTION_INFO_V1(orafce_utl_raw_bit_and);
PG_FUNCTION_INFO_V1(orafce_utl_raw_bit_or);
PG_FUNCTION_INFO_V1(orafce_utl_raw_bit_xor);

static bytea *
ora_raw_bitop(bytea *a, bytea *b, char op)
{
	int				la = VARSIZE_ANY_EXHDR(a);
	int				lb = VARSIZE_ANY_EXHDR(b);
	int				n = Min(la, lb);
	int				m = Max(la, lb);
	bytea		   *result = (bytea *) palloc(VARHDRSZ + m);
	unsigned char  *pa = (unsigned char *) VARDATA_ANY(a);
	unsigned char  *pb = (unsigned char *) VARDATA_ANY(b);
	unsigned char  *pr = (unsigned char *) VARDATA(result);
	int				i;

	SET_VARSIZE(result, VARHDRSZ + m);

	switch (op)
	{
		case '&':
			for (i = 0; i < n; i++)
				pr[i] = pa[i] & pb[i];
			break;
		case '|':
			for (i = 0; i < n; i++)
				pr[i] = pa[i] | pb[i];
			break;
		case '^':
			for (i = 0; i < n; i++)
				pr[i] = pa[i] ^ pb[i];
			break;
	}

	/* append the untouched tail of the longer operand */
	if (la > n)
		memcpy(pr + n, pa + n, la - n);
	else if (lb > n)
		memcpy(pr + n, pb + n, lb - n);

	return result;
}

Datum
orafce_utl_raw_bit_and(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(ora_raw_bitop(PG_GETARG_BYTEA_PP(0),
									PG_GETARG_BYTEA_PP(1), '&'));
}

Datum
orafce_utl_raw_bit_or(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(ora_raw_bitop(PG_GETARG_BYTEA_PP(0),
									PG_GETARG_BYTEA_PP(1), '|'));
}

Datum
orafce_utl_raw_bit_xor(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(ora_raw_bitop(PG_GETARG_BYTEA_PP(0),
									PG_GETARG_BYTEA_PP(1), '^'));
}
