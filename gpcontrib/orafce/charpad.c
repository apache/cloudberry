/*----------------------------------------------------------------------------
 *
 *  charpad.c
 *  LPAD and RPAD SQL functions for PostgreSQL.
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "mb/pg_wchar.h"
#include "fmgr.h"

#include "orafce.h"
#include "builtins.h"

#if PG_VERSION_NUM >= 160000

#include "varatt.h"

#endif

/* flags */
#define ON	true
#define OFF	false

/* Upper limit on total width of the padded output of *pad functions */
#define PAD_MAX 4000

PG_FUNCTION_INFO_V1(orafce_lpad);
PG_FUNCTION_INFO_V1(orafce_rpad);

/*
 * pg_dsplen() returns -1 for characters that cannot be displayed, like the
 * ASCII control characters.  A negative width would raise the remaining width
 * of the padding loops below instead of lowering it, so that these loops would
 * run until the counters overflow.  Handle such characters like the characters
 * of zero display width, which the loops already handle safely.
 */
static int
orafce_dsplen(const char *ptr)
{
	int			dsplen = pg_dsplen(ptr);

	return dsplen > 0 ? dsplen : 0;
}

/*
 * Note - when third argument of rpad function contains some
 * combine characters, then these functions returned different
 * results than Oracle and Postgres. Oracle has known unfixed
 * bug because doesn't calculate with special unicode chars with
 * display with zero. Postgres is in this case Oracle buggy
 * compatible. Original implementation in Orafce returned result
 * that is more near to correct result, but last multibyte char
 * was broken too.
 *
 * select rpad('x', 10, U&'a\0301') -> xááááa (Postgres)
 * select rpad('x', 10, unistr('a\0301')) -> xááááa  (Oracle)
 * select oracle.rpad( ...) -> xááááááááa (Orafce)
 *
 * Because Orafce in this case was not compatible with Oracle,
 * then it is possible to fix this issue.
 */

/*
 * orafce_lpad(string text, length int32 [, fill text])
 *
 * Fill up the string to length 'length' by prepending
 * the characters fill (a half-width space by default)
 */
Datum
orafce_lpad(PG_FUNCTION_ARGS)
{
	text	   *string1 = PG_GETARG_TEXT_PP(0);
	int32		output_width = PG_GETARG_INT32(1);
	text	   *string2 = PG_GETARG_TEXT_PP(2);
	text	   *ret;
	char	   *ptr1,
			   *ptr1end,
			   *ptr2 = NULL,
			   *ptr2start = NULL,
			   *ptr2end = NULL,
			   *ptr_ret,
			   *spc = " ";
	int			mlen,
				dsplen,
				s1blen,
				s2blen,
				hslen,
				total_blen = 0,
				s1_width = 0,
				s1_add_blen = 0,
				s2_add_blen = 0;
	bool		s2_operate = ON,
				half_space = OFF,
				init_ptr = ON;

	/* validate output width (the 2nd argument) */
	if (output_width < 0)
		output_width = 0;
	if (output_width > PAD_MAX)
		output_width = PAD_MAX;

	/* get byte-length of the 1st and 3rd argument strings */
	s1blen = VARSIZE_ANY_EXHDR(string1);
	s2blen = VARSIZE_ANY_EXHDR(string2);

	/* validate the lengths */
	if (s1blen < 0)
		s1blen = 0;
	if (s2blen < 0)
		s2blen = 0;

	/* if the filler length is zero disable filling */
	if (s2blen == 0)
	{
		s2_operate = OFF;		/* turn off string2 processing flag */
		output_width = 0;		/* same behavior as Oracle database */
	}

	/* byte-length of half-width space */
	hslen = pg_mblen_cstr(spc);

	/*
	 * Calculate the length of the portion of string1 to include in the final
	 * output
	 */
	ptr1 = VARDATA_ANY(string1);
	ptr1end = ptr1 + s1blen;
	while (s1blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* byte-length and display length per character of string1 */
		mlen = pg_mblen_range(ptr1, ptr1end);
		dsplen = orafce_dsplen(ptr1);

		/* accumulate display length of string1 */
		s1_width += dsplen;

		/*
		 * if string1 is longer/wider than the requested output_width, discard
		 * this character and prepend a half-width space instead
		 */
		if (s1_width >= output_width)
		{
			if (s1_width != output_width)
			{
				/* secure bytes for a half-width space in the final output */
				if (output_width != 0)
				{
					s1_add_blen += hslen;
					half_space = ON;
				}
			}
			else				/* exactly fits, so include this character */
			{
				s1_add_blen += mlen;
			}

			/*
			 * turn off string2 processing because string1 already consumed
			 * output_width
			 */
			s2_operate = OFF;

			/* done with string1 */
			break;
		}

		/* accumulate string1's portion of byte-length of the output */
		s1_add_blen += mlen;

		/* advance one character within string1 */
		ptr1 += mlen;

		/* loop counter */
		s1blen -= mlen;
	}

	/*
	 * Calculate the length of the portion composed of string2 to use for
	 * padding
	 */
	if (s2_operate)
	{
		int			s2_add_width;
		int			s2_add_width_initial;

		/* remaining part of output_width is composed of string2 */
		s2_add_width = output_width - s1_width;
		s2_add_width_initial = s2_add_width;

		ptr2 = ptr2start = VARDATA_ANY(string2);
		ptr2end = ptr2 + s2blen;

		while (s2_add_width > 0)
		{
			CHECK_FOR_INTERRUPTS();

			/* byte-length and display length per character of string2 */
			mlen = pg_mblen_range(ptr2, ptr2end);
			dsplen = orafce_dsplen(ptr2);

			/*
			 * output_width can not fit this character of string2, so discard
			 * it and prepend a half-width space instead
			 */
			if (dsplen > s2_add_width)
			{
				s2_add_blen += hslen;
				half_space = ON;

				/* done with string2 */
				break;
			}

			/* accumulate string2's portion of byte-length of the output */
			s2_add_blen += mlen;

			/* loop counter */
			s2_add_width -= dsplen;

			/* advance one character within string2 */
			ptr2 += mlen;

			/* when get to the end of string2, reset ptr2 to the start */
			if (ptr2 == ptr2end)
			{
				ptr2 = ptr2start;

				/*
				 * Now we are at the end of string2. When there are not
				 * change at s2_add_width then string2 has zero display
				 * width and we have to go out if we don't want to be
				 * in never finish loop.
				 */
				if (s2_add_width == s2_add_width_initial)
					PG_RETURN_TEXT_P(cstring_to_text(""));
			}
		}
	}

	/* allocate enough space to contain output_width worth of characters */
	total_blen = s1_add_blen + s2_add_blen;
	ret = (text *) palloc(VARHDRSZ + total_blen);
	ptr_ret = VARDATA(ret);

	/*
	 * add a half-width space as a padding necessary to satisfy the required
	 * output_width
	 *
	 * (memory already allocated as reserved by either s1_add_blen or
	 * s2_add_blen)
	 */
	if (half_space)
	{
		memcpy(ptr_ret, spc, hslen);
		ptr_ret += hslen;
	}

	/* prepend string2 padding */
	while (s2_add_blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* reset ptr2 to the string2 start */
		if (init_ptr)
		{
			init_ptr = OFF;
			ptr2 = ptr2start;
		}

		mlen = pg_mblen_range(ptr2, ptr2end);
		if (s2_add_blen < mlen)
			break;

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;

		/* loop counter */
		s2_add_blen -= mlen;

		/* when get to the end of string2, reset ptr2 back to the start */
		if (ptr2 == ptr2end)
			ptr2 = ptr2start;
	}

	init_ptr = ON;

	/* string1 */
	while (s1_add_blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* reset ptr1 back to the start of string1 */
		if (init_ptr)
		{
			init_ptr = OFF;
			ptr1 = VARDATA_ANY(string1);
		}

		mlen = pg_mblen_range(ptr1, ptr1end);

		if (s1_add_blen < mlen)
			break;

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;

		/* loop counter */
		s1_add_blen -= mlen;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	PG_RETURN_TEXT_P(ret);
}

/*
 * orafce_rpad(string text, length int32 [, fill text])
 *
 * Fill up the string to length 'length' by appending
 * the characters fill (a half-width space by default)
 */
Datum
orafce_rpad(PG_FUNCTION_ARGS)
{
	text	   *string1 = PG_GETARG_TEXT_PP(0);
	int32		output_width = PG_GETARG_INT32(1);
	text	   *string2 = PG_GETARG_TEXT_PP(2);
	text	   *ret;
	char	   *ptr1,
			   *ptr1end,
			   *ptr2 = NULL,
			   *ptr2start = NULL,
			   *ptr2end = NULL,
			   *ptr_ret,
			   *spc = " ";
	int			mlen,
				dsplen,
				s1blen,
				s2blen,
				hslen,
				total_blen = 0,
				s1_width = 0,
				s1_add_blen = 0,
				s2_add_blen = 0;
	bool		s2_operate = ON,
				half_space = OFF,
				init_ptr = ON;

	/* validate output width (the 2nd argument) */
	if (output_width < 0)
		output_width = 0;
	if (output_width > PAD_MAX)
		output_width = PAD_MAX;

	/* get byte-length of the 1st and 3rd argument strings */
	s1blen = VARSIZE_ANY_EXHDR(string1);
	s2blen = VARSIZE_ANY_EXHDR(string2);

	/* validate the lengths */
	if (s1blen < 0)
		s1blen = 0;
	if (s2blen < 0)
		s2blen = 0;

	/* if the filler length is zero disable filling */
	if (s2blen == 0)
	{
		s2_operate = OFF;		/* turn off string2 processing flag */
		output_width = 0;		/* same behavior as Oracle database */
	}

	/* byte-length of half-width space */
	hslen = pg_mblen_cstr(spc);

	/*
	 * Calculate the length of the portion of string1 to include in the final
	 * output
	 */
	ptr1 = VARDATA_ANY(string1);
	ptr1end = ptr1 + s1blen;
	while (s1blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* byte-length and display length per character of string1 */
		mlen = pg_mblen_range(ptr1, ptr1end);
		dsplen = orafce_dsplen(ptr1);

		/* accumulate display length of string1 */
		s1_width += dsplen;

		/*
		 * if string1 is longer/wider than the requested output_width, discard
		 * this character and prepend a half-width space instead
		 */
		if (s1_width >= output_width)
		{
			if (s1_width != output_width)
			{
				/* secure bytes for a half-width space in the final output */
				if (output_width != 0)
				{
					s1_add_blen += hslen;
					half_space = ON;
				}
			}
			else				/* exactly fits, so include this character */
			{
				s1_add_blen += mlen;
			}

			/*
			 * turn off string2 processing because string1 already consumed
			 * output_width
			 */
			s2_operate = OFF;

			/* done with string1 */
			break;
		}

		/* accumulate string1's portion of byte-length of the output */
		s1_add_blen += mlen;

		/* advance one character within string1 */
		ptr1 += mlen;

		/* loop counter */
		s1blen -= mlen;
	}

	/*
	 * Calculate the length of the portion composed of string2 to use for
	 * padding
	 */
	if (s2_operate)
	{
		int			s2_add_width;
		int			s2_add_width_initial;

		/* remaining part of output_width is composed of string2 */
		s2_add_width = output_width - s1_width;
		s2_add_width_initial = s2_add_width;

		ptr2 = ptr2start = VARDATA_ANY(string2);
		ptr2end = ptr2 + s2blen;

		while (s2_add_width > 0)
		{
			CHECK_FOR_INTERRUPTS();

			/* byte-length and display length per character of string2 */
			mlen = pg_mblen_range(ptr2, ptr2end);
			dsplen = orafce_dsplen(ptr2);

			/*
			 * output_width can not fit this character of string2, so discard
			 * it and prepend a half-width space instead
			 */
			if (dsplen > s2_add_width)
			{
				s2_add_blen += hslen;
				half_space = ON;

				/* done with string2 */
				break;
			}

			/* accumulate string2's portion of byte-length of the output */
			s2_add_blen += mlen;

			/* loop counter */
			s2_add_width -= dsplen;

			/* advance one character within string2 */
			ptr2 += mlen;

			/* when get to the end of string2, reset ptr2 to the start */
			if (ptr2 == ptr2end)
			{
				ptr2 = ptr2start;
				if (s2_add_width == s2_add_width_initial)
					PG_RETURN_TEXT_P(cstring_to_text(""));
			}
		}
	}

	/* allocate enough space to contain output_width worth of characters */
	total_blen = s1_add_blen + s2_add_blen;
	ret = (text *) palloc(VARHDRSZ + total_blen);
	ptr_ret = VARDATA(ret);

	/* string1 */
	while (s1_add_blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* reset ptr1 back to the start of string1 */
		if (init_ptr)
		{
			init_ptr = OFF;
			ptr1 = VARDATA_ANY(string1);
		}

		mlen = pg_mblen_range(ptr1, ptr1end);

		if (s1_add_blen < mlen)
			break;

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;

		/* loop counter */
		s1_add_blen -= mlen;
	}

	init_ptr = ON;

	/* append string2 padding */
	while (s2_add_blen > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/* reset ptr2 to the string2 start */
		if (init_ptr)
		{
			init_ptr = OFF;
			ptr2 = ptr2start;
		}

		mlen = pg_mblen_range(ptr2, ptr2end);
		if (s2_add_blen < mlen)
			break;

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;

		/* loop counter */
		s2_add_blen -= mlen;

		/* when get to the end of string2, reset ptr2 back to the start */
		if (ptr2 == ptr2end)
			ptr2 = ptr2start;
	}

	/*
	 * add a half-width space as a padding necessary to satisfy the required
	 * output_width
	 *
	 * (memory already allocated as reserved by either s1_add_blen or
	 * s2_add_blen)
	 */
	if (half_space)
	{
		memcpy(ptr_ret, spc, hslen);
		ptr_ret += hslen;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	PG_RETURN_TEXT_P(ret);
}
