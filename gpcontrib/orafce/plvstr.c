/*
 * This code implements one part of functonality of
 * free available library PL/Vision. Please look www.quest.com
 *
 * Original author: Steven Feuerstein, 1996 - 2002
 * PostgreSQL implementation author: Pavel Stehule, 2006-2026
 *
 * This module is under BSD Licence
 *
 * History:
 * 1.0. first public version 13. March 2006
 */


#include "postgres.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "orafce.h"
#include "builtins.h"

#include <string.h>
#include <stdlib.h>

PG_FUNCTION_INFO_V1(plvstr_rvrs);
PG_FUNCTION_INFO_V1(plvstr_normalize);
PG_FUNCTION_INFO_V1(plvstr_is_prefix_text);
PG_FUNCTION_INFO_V1(plvstr_is_prefix_int);
PG_FUNCTION_INFO_V1(plvstr_is_prefix_int64);
PG_FUNCTION_INFO_V1(plvstr_lpart);
PG_FUNCTION_INFO_V1(plvstr_rpart);
PG_FUNCTION_INFO_V1(plvstr_lstrip);
PG_FUNCTION_INFO_V1(plvstr_rstrip);
PG_FUNCTION_INFO_V1(plvstr_left);
PG_FUNCTION_INFO_V1(plvstr_right);
PG_FUNCTION_INFO_V1(plvstr_substr2);
PG_FUNCTION_INFO_V1(plvstr_substr3);
PG_FUNCTION_INFO_V1(plvstr_instr2);
PG_FUNCTION_INFO_V1(plvstr_instr3);
PG_FUNCTION_INFO_V1(plvstr_instr4);
PG_FUNCTION_INFO_V1(plvstr_betwn_i);
PG_FUNCTION_INFO_V1(plvstr_betwn_c);
PG_FUNCTION_INFO_V1(plvstr_swap);

PG_FUNCTION_INFO_V1(plvchr_nth);
PG_FUNCTION_INFO_V1(plvchr_first);
PG_FUNCTION_INFO_V1(plvchr_last);
PG_FUNCTION_INFO_V1(plvchr_is_kind_i);
PG_FUNCTION_INFO_V1(plvchr_is_kind_a);
PG_FUNCTION_INFO_V1(plvchr_char_name);

PG_FUNCTION_INFO_V1(oracle_substr2);
PG_FUNCTION_INFO_V1(oracle_substr3);

PG_FUNCTION_INFO_V1(oracle_substrb2);
PG_FUNCTION_INFO_V1(oracle_substrb3);

int			orafce_substring_length_is_zero = ORAFCE_COMPATIBILITY_WARNING_ORACLE;

static text *ora_substr(Datum str, int start, int len);

#define ora_substr_text(str, start, len) \
	ora_substr(PointerGetDatum((str)), (start), (len))

static const char *char_names[] = {
	"NULL", "SOH", "STX", "ETX", "EOT", "ENQ", "ACK", "DEL",
	"BS", "HT", "NL", "VT", "NP", "CR", "SO", "SI",
	"DLE", "DC1", "DC2", "DC3", "DC4", "NAK", "SYN", "ETB",
	"CAN", "EM", "SUB", "ESC", "FS", "GS", "RS", "US", "SP"
};

#define NON_EMPTY_CHECK(str) \
if (VARSIZE_ANY_EXHDR(str) == 0) \
	ereport(ERROR, \
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
			 errmsg("invalid parameter"), \
		 errdetail("Not allowed empty string.")));

#define PARAMETER_ERROR(detail) \
	ereport(ERROR, \
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
		 errmsg("invalid parameter"), \
		 errdetail(detail)));

#ifndef ORAFCE_HAVE_PG_MBLEN_RANGE

int
orafce_mblen_range(const char *mbstr, const char *end)
{
	int			length = pg_mblen(mbstr);

	Assert(end > mbstr);

	if (unlikely(mbstr + length > end))
		ereport(ERROR,
				(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
				 errmsg("invalid byte sequence for current encoding")));

	return length;
}

#endif

/*
 * Make substring, can handle negative start
 *
 */
int
ora_mb_strlen(text *str, char **sizes, int **positions)
{
	int			r_len;
	int			cur_size = 0;
	char	   *p, *end;
	int			cur = 0;

	p = VARDATA_ANY(str);
	r_len = VARSIZE_ANY_EXHDR(str);
	end = p + r_len;

	if (NULL != sizes)
		*sizes = palloc(r_len * sizeof(char));
	if (NULL != positions)
		*positions = palloc(r_len * sizeof(int));

	while (cur < r_len)
	{
		int			sz;

		sz = pg_mblen_range(p, end);
		if (sizes)
			(*sizes)[cur_size] = sz;
		if (positions)
			(*positions)[cur_size] = cur;
		cur += sz;
		p += sz;
		cur_size += 1;
	}

	return cur_size;
}


int
ora_text_mbstrlen(text *str)
{
	return pg_mbstrlen_with_len(VARDATA_ANY(str),
								VARSIZE_ANY_EXHDR(str));
}

/*
 * len < 0 means "length is not specified".
 */
static text *
ora_substr(Datum str, int start, int len)
{
	if (start == 0)
		start = 1;				/* 0 is interpreted as 1 */
	else if (start < 0)
	{
		text	   *t;
		int32		n;

		t = DatumGetTextPP(str);
		n = pg_mbstrlen_with_len(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
		start = n + start + 1;
		if (start <= 0)
			return cstring_to_text("");
		str = PointerGetDatum(t);	/* save detoasted text */
	}

	if (len < 0)
		return DatumGetTextP(DirectFunctionCall2(text_substr_no_len,
												 str, Int32GetDatum(start)));
	else
		return DatumGetTextP(DirectFunctionCall3(text_substr,
												 str, Int32GetDatum(start), Int32GetDatum(len)));
}

/* simply search algorhitm - can be better */

static int
ora_instr_mb(text *txt, text *pattern, int start, int nth)
{
	int			c_len_txt,
				c_len_pat;
	int			b_len_pat;
	int		   *pos_txt;
	const char *str_txt,
			   *str_pat;
	int			beg,
				end,
				i,
				dx;

	str_txt = VARDATA_ANY(txt);
	c_len_txt = ora_mb_strlen(txt, NULL, &pos_txt);
	str_pat = VARDATA_ANY(pattern);
	b_len_pat = VARSIZE_ANY_EXHDR(pattern);
	c_len_pat = pg_mbstrlen_with_len(str_pat, b_len_pat);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = c_len_txt - c_len_pat + 1;
		if (beg >= end)
			return 0;			/* out of range */
	}
	else if (start < 0)
	{
		dx = -1;
		beg = Min(c_len_txt + start, c_len_txt - c_len_pat);
		end = -1;
		if (beg <= end)
			return 0;			/* out of range */
	}
	else
		return 0;

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + pos_txt[i], str_pat, b_len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}


int
ora_instr(text *txt, text *pattern, int start, int nth)
{
	int			len_txt,
				len_pat;
	const char *str_txt,
			   *str_pat;
	int			beg,
				end,
				i,
				dx;

	if (nth <= 0)
		PARAMETER_ERROR("Four parameter isn't positive.");

	/* Forward for multibyte strings */
	if (pg_database_encoding_max_length() > 1)
		return ora_instr_mb(txt, pattern, start, nth);

	str_txt = VARDATA_ANY(txt);
	len_txt = VARSIZE_ANY_EXHDR(txt);
	str_pat = VARDATA_ANY(pattern);
	len_pat = VARSIZE_ANY_EXHDR(pattern);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = len_txt - len_pat + 1;
		if (beg >= end)
			return 0;			/* out of range */
	}
	else if (start < 0)
	{
		dx = -1;
		beg = Min(len_txt + start, len_txt - len_pat);
		end = -1;
		if (beg <= end)
			return 0;			/* out of range */
	}
	else
		return 0;

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + i, str_pat, len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}


/****************************************************************
 * PLVstr.normalize
 *
 * Syntax:
 *   FUNCTION plvstr.normalize (string_in IN VARCHAR)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Normalize string - replace white chars by space, replace
 * spaces by space
 *
 ****************************************************************/

Datum
plvstr_normalize(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	int			len = VARSIZE_ANY_EXHDR(str);
	char	   *end;
	char	   *ptr, *write_ptr;
	bool		write_spc;
	text	   *result;

	result = palloc(len + VARHDRSZ);
	ptr = VARDATA_ANY(str);
	end = ptr + len;
	write_ptr = VARDATA(result);
	write_spc = false;

	while (ptr < end)
	{
		char		c = *ptr;
		int			mblen = pg_mblen_range(ptr, end);

		switch (c)
		{
			case '\t':
			case '\n':
			case '\r':
			case ' ':
				write_spc = true;
				break;

			default:
				/* ignore all other unvisible chars */
				if (mblen > 1 || c > 32)
				{
					if (write_spc)
					{
						*write_ptr++ = ' ';
						write_spc = false;
					}

					memcpy(write_ptr, ptr, mblen);
					write_ptr += mblen;
				}
		}

		ptr += mblen;
	}

	SET_VARSIZE(result, write_ptr - VARDATA(result) + VARHDRSZ);

	PG_RETURN_TEXT_P(result);
}


/****************************************************************
 * PLVstr.instr
 *
 * Syntax:
 *   FUNCTION plvstr.instr (string_in VARCHAR, pattern VARCHAR)
 *   FUNCTION plvstr.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER)
 *   FUNCTION plvstr.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER, nth INTEGER)
 *            RETURN INT;
 *
 * Purpouse:
 *   Search pattern in string.
 *
 ****************************************************************/

Datum
plvstr_instr2(PG_FUNCTION_ARGS)
{
	text	   *arg1 = PG_GETARG_TEXT_PP(0);
	text	   *arg2 = PG_GETARG_TEXT_PP(1);

	PG_RETURN_INT32(ora_instr(arg1, arg2, 1, 1));
}

Datum
plvstr_instr3(PG_FUNCTION_ARGS)
{
	text	   *arg1 = PG_GETARG_TEXT_PP(0);
	text	   *arg2 = PG_GETARG_TEXT_PP(1);
	int			arg3 = PG_GETARG_INT32(2);

	PG_RETURN_INT32(ora_instr(arg1, arg2, arg3, 1));
}

Datum
plvstr_instr4(PG_FUNCTION_ARGS)
{
	text	   *arg1 = PG_GETARG_TEXT_PP(0);
	text	   *arg2 = PG_GETARG_TEXT_PP(1);
	int			arg3 = PG_GETARG_INT32(2);
	int			arg4 = PG_GETARG_INT32(3);

	PG_RETURN_INT32(ora_instr(arg1, arg2, arg3, arg4));
}


/****************************************************************
 * PLVstr.is_prefix
 *
 * Syntax:
 *   FUNCTION plvstr.is_prefix (string_in IN VARCHAR,
 *                    prefix_in VARCHAR,
 *                    case_sensitive BOOL := true)
 *      RETURN bool;
 *   FUNCTION plvstr.is_prefix (num_in IN NUMERIC,
 *                    prefix_in NUMERIC) RETURN bool;
 *   FUNCTION plvstr.is_prefix (int_in IN INT,
 *                    prefix_in INT)  RETURN bool;
 *
 * Purpouse:
 *   Returns true, if prefix_in is prefix of string_in
 *
 ****************************************************************/


Datum
plvstr_is_prefix_text(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	text	   *prefix = PG_GETARG_TEXT_PP(1);
	bool		case_sens = PG_GETARG_BOOL(2);
	int			str_len;
	int			pref_len;

	if (!case_sens)
	{
		str = DatumGetTextP(DirectFunctionCall1Coll(lower, PG_GET_COLLATION(),
												   PointerGetDatum(str)));
		prefix = DatumGetTextP(DirectFunctionCall1Coll(lower, PG_GET_COLLATION(),
													  PointerGetDatum(prefix)));
	}

	str_len = VARSIZE_ANY_EXHDR(str);
	pref_len = VARSIZE_ANY_EXHDR(prefix);

	PG_RETURN_BOOL(str_len >= pref_len &&
				   memcmp(VARDATA_ANY(str), VARDATA_ANY(prefix), pref_len) == 0);
}

Datum
plvstr_is_prefix_int(PG_FUNCTION_ARGS)
{
	int			n = PG_GETARG_INT32(0);
	int			prefix = PG_GETARG_INT32(1);
	bool		result = false;

	if (prefix == 0)
		PG_RETURN_BOOL(n == 0);

	do
	{
		if (n == prefix)
		{
			result = true;
			break;
		}
		n = n / 10;

	} while (n != 0);

	PG_RETURN_BOOL(result);
}

Datum
plvstr_is_prefix_int64(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		prefix = PG_GETARG_INT64(1);
	bool		result = false;

	if (prefix == 0)
		PG_RETURN_BOOL(n == 0);

	do
	{
		if (n == prefix)
		{
			result = true;
			break;
		}
		n = n / 10;

	} while (n != 0);

	PG_RETURN_BOOL(result);
}


/****************************************************************
 * PLVstr.rvrs
 *
 * Syntax:
 *   FUNCTION plvstr.rvrs (string_in IN VARCHAR,
 *					  start_in IN INTEGER := 1,
 *					  end_in IN INTEGER := NULL)
 *  	RETURN VARCHAR2;
 *
 * Purpouse:
 *   Reverse string or part of string
 *
 ****************************************************************/

static text *
empty_string()
{
	text	   *result;

	result = palloc(VARHDRSZ);
	SET_VARSIZE(result, VARHDRSZ);

	return result;
}

Datum
plvstr_rvrs(PG_FUNCTION_ARGS)
{
	text	   *str;
	int			start;
	int			end;
	int			len;
	int			i;
	int			new_len;
	text	   *result;
	char	   *data;
	char	   *sizes = NULL;
	int		   *positions = NULL;
	bool		mb_encode;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	str = PG_GETARG_TEXT_PP(0);

	mb_encode = pg_database_encoding_max_length() > 1;

	if (!mb_encode)
		len = VARSIZE_ANY_EXHDR(str);
	else
		len = ora_mb_strlen(str, &sizes, &positions);

	if (PG_ARGISNULL(1) && PG_ARGISNULL(2))
	{
		start = 1;
		end = len;
	}
	else if (PG_ARGISNULL(2))
	{
		start = PG_GETARG_INT32(1);
		if (start < 0)
			end = PG_INT32_MIN;
		else
			end = PG_INT32_MAX;
	}
	else if (PG_ARGISNULL(1))
	{
		end = PG_GETARG_INT32(2);
		if (end < 0)
			start = -1;
		else
			start = 0;
	}
	else
	{
		start = PG_GETARG_INT32(1);
		end = PG_GETARG_INT32(2);
	}

	if (start == 0 && end == 0)
		PG_RETURN_TEXT_P(empty_string());

	/* transform +/-0 to +/-1 */
	if (start == 0)
		start = end > 0 ? 1 : -1;

	if ((start > end && start >= 0) || (start < end && start < 0))
		PARAMETER_ERROR("Second parameter is bigger than third.");

	/*
	 * Reject an out of range start.  Do not use abs() here: abs(PG_INT32_MIN)
	 * is undefined and in practice returns PG_INT32_MIN again, which would
	 * pass the test and let the negative branch below compute a range far
	 * outside the string.
	 */
	if (start > len || start < -len || end == 0)
		PG_RETURN_TEXT_P(empty_string());

	if (start < 0)
	{
		int64		new_start,
					new_end;

		new_start = ((int64) len) + start + 1;
		new_end = ((int64) len) + end + 1;
		if (new_end < 1)
			new_end = 1;

		start = (int) new_end;
		end = (int) new_start;
	}
	else
	{
		if (end > len)
			end = len;
	}

	Assert(start > 0 && start <= len);
	Assert(end > 0 && end <= len);
	Assert(start <= end);

	new_len = end - start + 1;

	if (mb_encode)
	{
		int64		max_size;
		int			cur_size;
		char	   *p;
		int			j;
		int			fz_size;

		fz_size = VARSIZE_ANY_EXHDR(str);

		/*
		 * The result cannot be longer than original string and cannot be
		 * longer than new_len * encoding_max_length, take smaller value.
		 *
		 * the product can exceed int for a long string in a wide encoding,
		 * so use int64.
		 */
		if ((max_size = (int64) new_len * pg_database_encoding_max_length()) > fz_size)
			result = palloc(fz_size + VARHDRSZ);
		else
			result = palloc((int) max_size + VARHDRSZ);
		data = (char *) VARDATA(result);

		cur_size = 0;
		p = VARDATA_ANY(str);
		for (i = end - 1; i >= start - 1; i--)
		{
			for (j = 0; j < sizes[i]; j++)
				*data++ = *(p + positions[i] + j);
			cur_size += sizes[i];
		}
		SET_VARSIZE(result, cur_size + VARHDRSZ);
	}
	else
	{
		char	   *p = VARDATA_ANY(str);

		result = palloc(new_len + VARHDRSZ);
		data = (char *) VARDATA(result);
		SET_VARSIZE(result, new_len + VARHDRSZ);

		for (i = end - 1; i >= start - 1; i--)
			*data++ = p[i];
	}

	PG_RETURN_TEXT_P(result);
}


/****************************************************************
 * PLVstr.lpart
 *
 * Syntax:
 *   FUNCTION PLVstr.lpart (string_in IN VARCHAR,
 *					   divider_in IN VARCHAR,
 *					   start_in IN INTEGER := 1,
 *					   nth_in IN INTEGER := 1,
 *					   all_if_notfound_in IN BOOLEAN := FALSE)
 *	RETURN VARCHAR2;
 *
 * Purpouse:
 *   Call this function to return the left part of a string.
 *
 ****************************************************************/

Datum
plvstr_lpart(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_P(0);
	text	   *div = PG_GETARG_TEXT_P(1);
	int			start = PG_GETARG_INT32(2);
	int			nth = PG_GETARG_INT32(3);
	bool		all_if_notfound = PG_GETARG_BOOL(4);
	int			loc;

	loc = ora_instr(str, div, start, nth);
	if (loc == 0)
	{
		if (all_if_notfound)
			PG_RETURN_TEXT_P(TextPCopy(str));
		else
			PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ora_substr_text(str, 1, loc - 1));
}


/****************************************************************
 * PLVstr.rpart
 *
 * Syntax:
 *   FUNCTION PLVstr.rpart (string_in IN VARCHAR,
 *					   divider_in IN VARCHAR,
 *					   start_in IN INTEGER := 1,
 *					   nth_in IN INTEGER := 1,
 *					   all_if_notfound_in IN BOOLEAN := FALSE)
 *	RETURN VARCHAR2;
 *
 * Purpouse:
 *   Call this function to return the right part of a string.
 *
 ****************************************************************/

Datum
plvstr_rpart(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_P(0);
	text	   *div = PG_GETARG_TEXT_P(1);
	int			start = PG_GETARG_INT32(2);
	int			nth = PG_GETARG_INT32(3);
	bool		all_if_notfound = PG_GETARG_BOOL(4);
	int			loc;

	loc = ora_instr(str, div, start, nth);
	if (loc == 0)
	{
		if (all_if_notfound)
			PG_RETURN_TEXT_P(TextPCopy(str));
		else
			PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ora_substr_text(str, loc + ora_text_mbstrlen(div), -1));
}


/****************************************************************
 * PLVstr.lstrip
 *
 * Syntax:
 *   FUNCTION plvstr.lstrip (string_in IN VARCHAR,
 *							substring_in IN VARCHAR,
 *							num_in IN INTEGER := 1)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to remove characters from the beginning
 * (left) of a string.
 *
 ****************************************************************/

Datum
plvstr_lstrip(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	text	   *pat = PG_GETARG_TEXT_PP(1);
	int			num = PG_GETARG_INT32(2);
	int			count = 0;
	int			len_p,
				len_s;
	char	   *str_p;

	len_p = VARSIZE_ANY_EXHDR(pat);
	len_s = VARSIZE_ANY_EXHDR(str);

	str_p = VARDATA_ANY(str);
	while (count < num)
	{
		int			i;
		char	   *aux_str_p,
				   *pat_p;

		pat_p = VARDATA_ANY(pat);
		aux_str_p = str_p;

		if (len_s < len_p)
			break;

		for (i = 0; i < len_p; i++)
			if (*aux_str_p++ != *pat_p++)
				break;

		if (i >= len_p)
		{
			count++;
			/* found */
			str_p = aux_str_p;
			len_s -= len_p;
			continue;
		}
		break;
	}

	PG_RETURN_TEXT_P(cstring_to_text_with_len(str_p, len_s));
}


/****************************************************************
 * PLVstr.rstrip
 *
 * Syntax:
 *   FUNCTION plvstr.rstrip (string_in IN VARCHAR,
 *							substring_in IN VARCHAR,
 *							num_in IN INTEGER := 1)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to remove characters from the end
 * (right) of a string.
 *
 ****************************************************************/

Datum
plvstr_rstrip(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	text	   *pat = PG_GETARG_TEXT_PP(1);
	int			num = PG_GETARG_INT32(2);
	int			count = 0;
	int			len_p,
				len_s,
				i;
	char	   *str_p,
			   *aux_str_p;

	len_p = VARSIZE_ANY_EXHDR(pat);
	len_s = VARSIZE_ANY_EXHDR(str);

	str_p = VARDATA_ANY(str) + len_s - 1;

	while (count < num)
	{
		char	   *pat_p;

		pat_p = VARDATA_ANY(pat) + len_p - 1;
		aux_str_p = str_p;

		if (len_s < len_p)
			break;

		for (i = 0; i < len_p; i++)
			if (*aux_str_p-- != *pat_p--)
				break;

		if (i >= len_p)
		{
			count++;
			/* found */
			str_p = aux_str_p;
			len_s -= len_p;
			continue;
		}
		break;
	}

	PG_RETURN_TEXT_P(cstring_to_text_with_len(VARDATA_ANY(str), len_s));
}


/****************************************************************
 * PLVstr.left
 *
 * Syntax:
 *   FUNCTION plvstr.left (string_in IN VARCHAR,
 *							num_in INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns firs num_in charaters. You can use negative num_in
 *   left('abcde', -2) -> abc
 *
 ****************************************************************/


Datum
plvstr_left(PG_FUNCTION_ARGS)
{
	return text_left(fcinfo);
}


/****************************************************************
 * PLVstr.right
 *
 * Syntax:
 *   FUNCTION plvstr.right (string_in IN VARCHAR,
 *							num_in INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns last (right) num_in characters.
 *
 ****************************************************************/

Datum
plvstr_right(PG_FUNCTION_ARGS)
{
	return text_right(fcinfo);
}

/****************************************************************
 * PLVstr.substr2
 *
 * Syntax:
 *   FUNCTION plvstr.substr (string_in IN VARCHAR,
 *							start INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns substring started on start_in to end
 *
 ****************************************************************/

Datum
plvstr_substr2(PG_FUNCTION_ARGS)
{
	return oracle_substr2(fcinfo);
}


/****************************************************************
 * PLVstr.substr3
 *
 * Syntax:
 *   FUNCTION plvstr.substr (string_in IN VARCHAR,
 *							start INTEGER, len INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns len chars from start_in position
 *
 ****************************************************************/

Datum
plvstr_substr3(PG_FUNCTION_ARGS)
{
	return oracle_substr3(fcinfo);
}


/****************************************************************
 * PLVchr.nth
 *
 * Syntax:
 *   FUNCTION plvchr.nth (string_in IN VARCHAR,
 * 					 nth_in IN INTEGER)
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the Nth character in a string.
 *
 ****************************************************************/

Datum
plvchr_nth(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), 1));
}


/****************************************************************
 * PLVchr.first
 *
 * Syntax:
 *   FUNCTION plvchr.first (string_in IN VARCHAR,
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the first character in a string.
 *
 ****************************************************************/

Datum
plvchr_first(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), 1, 1));
}


/****************************************************************
 * PLVchr.last
 *
 * Syntax:
 *   FUNCTION plvchr.last (string_in IN VARCHAR,
 *  	RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to return the last character in a string.
 *
 ****************************************************************/

Datum
plvchr_last(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), -1, 1));
}


/****************************************************************
 * PLVchr.is_blank, plvchr.is_digit, ...
 *
 * Syntax:
 *   FUNCTION plvchr.is_kind (string_in IN VARCHAR,
 *      kind INT)
 *          RETURN VARCHAR;
 *
 * Purpouse:
 *   Call this function to see if a character is blank, ...
 *   1 blank, 2 digit, 3 quote, 4 other, 5 letter
 *
 ****************************************************************/

static bool
is_kind(char c, int kind)
{
	switch (kind)
	{
		case 1:
			return c == ' ';
		case 2:
			return '0' <= c && c <= '9';
		case 3:
			return c == '\'';
		case 4:
			return
				(32 <= c && c <= 47) ||
				(58 <= c && c <= 64) ||
				(91 <= c && c <= 96) || (123 <= c && c <= 126);
		case 5:
			return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
		default:
			PARAMETER_ERROR("Second parametr isn't in enum {1,2,3,4,5}");
			return false;
	}
}

Datum
plvchr_is_kind_i(PG_FUNCTION_ARGS)
{
	int32		c = PG_GETARG_INT32(0);
	int32		k = PG_GETARG_INT32(1);

	PG_RETURN_INT32(is_kind((char) c, k));
}

Datum
plvchr_is_kind_a(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	int32		k = PG_GETARG_INT32(1);
	char		c;

	NON_EMPTY_CHECK(str);
	if (pg_database_encoding_max_length() > 1)
	{
		if (pg_mblen_range(VARDATA_ANY(str), VARDATA_ANY(str) + VARSIZE_ANY_EXHDR(str)) > 1)
			PG_RETURN_INT32((k == 5));
	}

	c = *VARDATA_ANY(str);
	PG_RETURN_INT32(is_kind(c, k));
}


/****************************************************************
 * PLVchr.char_name
 *
 * Syntax:
 *   FUNCTION plvchr.char_name (letter_in IN VARCHAR)
 *   	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns the name of the character to ascii code as a VARCHAR.
 *
 ****************************************************************/

Datum
plvchr_char_name(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	text	   *result;
	unsigned char c;

	NON_EMPTY_CHECK(str);
	c = (unsigned char) *(VARDATA_ANY(str));

	if (c >= lengthof(char_names))
		result = ora_substr_text(str, 1, 1);
	else
		result = cstring_to_text(char_names[c]);

	PG_RETURN_TEXT_P(result);
}


/****************************************************************
 * substr
 *
 * Syntax:
 *   FUNCTION substr (string, start_position, [length])
 *   	RETURN VARCHAR;
 *
 * Purpouse:
 *   Returns len chars from start_in position, compatible with Oracle
 *
 ****************************************************************/

Datum
oracle_substr3(PG_FUNCTION_ARGS)
{
	int32		len = PG_GETARG_INT32(2);

	if (len < 0)
		PG_RETURN_NULL();

	if (len == 0)
	{
		if (orafce_substring_length_is_zero == ORAFCE_COMPATIBILITY_WARNING_ORACLE ||
			orafce_substring_length_is_zero == ORAFCE_COMPATIBILITY_WARNING_ORAFCE)
			elog(WARNING, "zero substring_length is used in substr function");

		if (orafce_substring_length_is_zero == ORAFCE_COMPATIBILITY_WARNING_ORACLE ||
			orafce_substring_length_is_zero == ORAFCE_COMPATIBILITY_ORACLE)
			PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), len));
}

Datum
oracle_substr2(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(ora_substr(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), -1));
}


static text *
ora_concat2(text *str1, text *str2)
{
	int			l1;
	int			l2;
	text	   *result;

	l1 = VARSIZE_ANY_EXHDR(str1);
	l2 = VARSIZE_ANY_EXHDR(str2);

	result = palloc(l1 + l2 + VARHDRSZ);
	memcpy(VARDATA(result), VARDATA_ANY(str1), l1);
	memcpy(VARDATA(result) + l1, VARDATA_ANY(str2), l2);
	SET_VARSIZE(result, l1 + l2 + VARHDRSZ);

	return result;
}


static text *
ora_concat3(text *str1, text *str2, text *str3)
{
	int			l1;
	int			l2;
	int			l3;
	text	   *result;

	l1 = VARSIZE_ANY_EXHDR(str1);
	l2 = VARSIZE_ANY_EXHDR(str2);
	l3 = VARSIZE_ANY_EXHDR(str3);

	result = palloc(l1 + l2 + l3 + VARHDRSZ);
	memcpy(VARDATA(result), VARDATA_ANY(str1), l1);
	memcpy(VARDATA(result) + l1, VARDATA_ANY(str2), l2);
	memcpy(VARDATA(result) + l1 + l2, VARDATA_ANY(str3), l3);
	SET_VARSIZE(result, l1 + l2 + l3 + VARHDRSZ);

	return result;
}


/****************************************************************
 * PLVchr.swap
 *
 * Syntax:
 *    FUNCTION swap
 *      (string_in IN VARCHAR2,
 *       replace_in IN VARCHAR2,
 *       start_in IN INTEGER := 1,
 *       oldlen_in IN INTEGER := NULL)
 *  RETURN VARCHAR2
 *
 * Purpouse:
 *   Replace a substring in a string with a specified string.
 *
 ****************************************************************/

Datum
plvstr_swap(PG_FUNCTION_ARGS)
{
	text	   *string_in;
	text	   *replace_in;
	int			start_in = 1;
	int			oldlen_in;
	int			v_len;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		string_in = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	else
		replace_in = PG_GETARG_TEXT_P(1);

	if (!PG_ARGISNULL(2))
		start_in = PG_GETARG_INT32(2);

	if (PG_ARGISNULL(3))
		oldlen_in = ora_text_mbstrlen(replace_in);
	else
		oldlen_in = PG_GETARG_INT32(3);

	v_len = ora_text_mbstrlen(string_in);

	start_in = start_in > 0 ? start_in : v_len + start_in + 1;

	/*
	 * A start outside the string replaces nothing.  Without the test for a
	 * start before the first character, ora_substr() below reads the negative
	 * position as a position counted back from the end of the string and the
	 * negative length as "to the end of the string", and the source string
	 * ends up in the result a second time.
	 */
	if (start_in < 1 || start_in > v_len)
		PG_RETURN_TEXT_P(TextPCopy(string_in));

	/*
	 * A negative length removes nothing, and a length longer than the string
	 * removes the rest of it.  Clamping also keeps the two sums below inside
	 * the range of int.
	 */
	if (oldlen_in < 0)
		oldlen_in = 0;
	else if (oldlen_in > v_len)
		oldlen_in = v_len;

	if (start_in == 1)
		PG_RETURN_TEXT_P(ora_concat2(
									 replace_in, ora_substr_text(string_in, oldlen_in + 1, -1)));
	else
		PG_RETURN_TEXT_P(ora_concat3(
									 ora_substr_text(string_in, 1, start_in - 1),
									 replace_in,
									 ora_substr_text(string_in, start_in + oldlen_in, -1)));
}

/****************************************************************
 * PLVchr.betwn
 *
 * Find the Substring Between Start and End Locations
 *
 * Syntax:
 *     FUNCTION plvstr.betwn (string_in IN VARCHAR2,
 *       start_in IN INTEGER,
 *       end_in IN INTEGER,
 *       inclusive IN BOOLEAN := TRUE)
 *      RETURN VARCHAR2;
 *
 *     FUNCTION plvstr.betwn (string_in IN VARCHAR2,
 *       start_in IN VARCHAR2,
 *       end_in IN VARCHAR2 := NULL,
 *       startnth_in IN INTEGER := 1,
 *       endnth_in IN INTEGER := 1,
 *       inclusive IN BOOLEAN := TRUE,
 *       gotoend IN BOOLEAN := FALSE)
 *      RETURN VARCHAR2;
 *
 * Purpouse:
 *   Call this function to extract a sub-string from a string. This
 * function is overloaded. You can either provide the start and end
 * locations or you can provide start and end substrings.
 *
 ****************************************************************/


Datum
plvstr_betwn_i(PG_FUNCTION_ARGS)
{
	text	   *string_in = PG_GETARG_TEXT_P(0);
	int			start_arg = PG_GETARG_INT32(1);
	int			end_arg = PG_GETARG_INT32(2);
	bool		inclusive = PG_GETARG_BOOL(3);
	int64		start_in = start_arg;
	int64		end_in = end_arg;

	if ((start_arg < 0 && end_arg > 0) ||
		(start_arg > 0 && end_arg < 0) ||
		(start_arg > end_arg))
		PARAMETER_ERROR("Wrong positions.");

	if (start_in < 0)
	{
		int			v_len = ora_text_mbstrlen(string_in);

		start_in = v_len + start_in + 1;
		end_in = v_len + end_in + 1;
	}

	if (!inclusive)
	{
		start_in += 1;
		end_in -= 1;
	}

	/*
	 * A position before the first character is not a position in the string.
	 * Start at the first character instead of handing the negative value to
	 * ora_substr(), which would read it as a position counted back from the
	 * end of the string.
	 */
	if (start_in < 1)
		start_in = 1;

	if (end_in < start_in)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	PG_RETURN_TEXT_P(ora_substr_text(string_in,
									 (int) Min(start_in, PG_INT32_MAX),
									 (int) Min(end_in - start_in + 1, PG_INT32_MAX)));
}


Datum
plvstr_betwn_c(PG_FUNCTION_ARGS)
{
	text	   *string_in;
	text	   *start_in;
	text	   *end_in;
	int			startnth_in;
	int			endnth_in;
	bool		inclusive;
	bool		gotoend;

	int			v_start;
	int			v_end;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4) ||
		PG_ARGISNULL(5) || PG_ARGISNULL(6))
		PG_RETURN_NULL();


	string_in = PG_GETARG_TEXT_P(0);
	start_in = PG_GETARG_TEXT_P(1);
	end_in = PG_ARGISNULL(2) ? start_in : PG_GETARG_TEXT_P(2);
	startnth_in = PG_GETARG_INT32(3);
	endnth_in = PG_GETARG_INT32(4);
	inclusive = PG_GETARG_BOOL(5);
	gotoend = PG_GETARG_BOOL(6);

	if (startnth_in == 0)
	{
		v_start = 1;
		v_end = ora_instr(string_in, end_in, 1, endnth_in);
	}
	else
	{
		v_start = ora_instr(string_in, start_in, 1, startnth_in);
		v_end = ora_instr(string_in, end_in, v_start + 1, endnth_in);
	}

	if (v_start == 0)
		PG_RETURN_NULL();

	if (!inclusive)
	{
		if (startnth_in > 0)
			v_start += ora_text_mbstrlen(start_in);

		v_end -= 1;
	}
	else
		v_end += (ora_text_mbstrlen(end_in) - 1);

	if (((v_start > v_end) && (v_end > 0)) ||
		(v_end <= 0 && !gotoend))
		PG_RETURN_NULL();

	if (v_end <= 0)
		v_end = ora_text_mbstrlen(string_in);

	PG_RETURN_TEXT_P(ora_substr_text(string_in,
									 v_start,
									 v_end - v_start + 1));
}

static bytea *
empty_bytea(void)
{
	bytea	   *result;

	result = palloc(VARHDRSZ);
	SET_VARSIZE(result, VARHDRSZ);

	return result;
}

/*
 * len < 0 means "length is not specified".
 *
 * The result is typed oracle.varchar2, which is binary coercible to text, so
 * it must be valid in the database encoding.  A byte range can start or end in
 * the middle of a multibyte character, and returning those bytes would let an
 * invalid string escape into the text world (it could be stored in a table and
 * would then break every text function applied to it).  Characters that are
 * only partially covered by the requested range are therefore dropped.
 */
static bytea *
ora_substrb(Datum str, int start, int len)
{
	bytea	   *t;
	char	   *data;
	int			n;
	int64		s;				/* 1 based position of the first byte */
	int64		e;				/* 1 based position after the last byte */

	t = DatumGetByteaPP(str);
	data = VARDATA_ANY(t);
	n = VARSIZE_ANY_EXHDR(t);

	if (start == 0)
		s = 1;					/* 0 is interpreted as 1 */
	else if (start < 0)
	{
		s = (int64) n + start + 1;
		if (s <= 0)
			return empty_bytea();
	}
	else
		s = start;

	e = len < 0 ? (int64) n + 1 : s + len;

	if (s < 1)
		s = 1;
	if (e > (int64) n + 1)
		e = (int64) n + 1;

	if (e <= s)
		return empty_bytea();

	if (pg_database_encoding_max_length() > 1)
	{
		char	   *p = data;
		char	   *endp = data + n;
		int64		pos = 1;
		int64		first = 0;
		int64		last = 0;
		bool		found = false;

		/* keep only the characters that fit completely into [s, e) */
		while (p < endp && pos < e)
		{
			int			clen = pg_mblen_range(p, endp);

			if (pos >= s && pos + clen <= e)
			{
				if (!found)
				{
					first = pos;
					found = true;
				}
				last = pos + clen - 1;
			}
			else if (found)
				break;

			p += clen;
			pos += clen;
		}

		if (!found)
			return empty_bytea();

		s = first;
		e = last + 1;
	}

	return DatumGetByteaP(DirectFunctionCall3(bytea_substr,
											  PointerGetDatum(t),
											  Int32GetDatum((int32) s),
											  Int32GetDatum((int32) (e - s))));
}

Datum
oracle_substrb2(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(ora_substrb(PG_GETARG_DATUM(0),
								  PG_GETARG_INT32(1),
								  -1));
}

Datum
oracle_substrb3(PG_FUNCTION_ARGS)
{
	PG_RETURN_BYTEA_P(ora_substrb(PG_GETARG_DATUM(0),
								  PG_GETARG_INT32(1),
								  PG_GETARG_INT32(2)));
}
