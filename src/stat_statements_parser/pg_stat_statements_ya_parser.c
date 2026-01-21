/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * pg_stat_statements_ya_parser.c
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/stat_statements_parser/pg_stat_statements_ya_parser.c
 *
 *-------------------------------------------------------------------------
 */

// NOTE: this file is just a bunch of code borrowed from pg_stat_statements for PG 9.4
// and from our own inhouse implementation of pg_stat_statements for managed PG

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "common/hashfn.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/scanner.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/queryjumble.h"

#include "pg_stat_statements_ya_parser.h"

#ifndef FCONST
#define FCONST 260
#endif
#ifndef SCONST
#define SCONST 261
#endif
#ifndef BCONST
#define BCONST 263
#endif
#ifndef XCONST
#define XCONST 264
#endif
#ifndef ICONST
#define ICONST 266
#endif

static void fill_in_constant_lengths(JumbleState *jstate, const char *query);
static int comp_location(const void *a, const void *b);
StringInfo gen_normplan(const char *execution_plan);
static bool need_replace(int token);
static char *generate_normalized_query(JumbleState *jstate, const char *query,
									   int *query_len_p, int encoding);

void stat_statements_parser_init(void)
{
	EnableQueryId();
}

void stat_statements_parser_deinit(void)
{
	/* NO-OP */
}

/* check if token should be replaced by substitute varable */
static bool
need_replace(int token)
{
	return (token == FCONST) || (token == ICONST) || (token == SCONST) || (token == BCONST) || (token == XCONST);
}

/*
 * gen_normplan - parse execution plan using flex and replace all CONST to
 * substitute variables.
 */
StringInfo
gen_normplan(const char *execution_plan)
{
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE yylloc;
	int tok;
	int bind_prefix = 1;
	char *tmp_str;
	YYLTYPE last_yylloc = 0;
	int last_tok = 0;
	StringInfo plan_out = makeStringInfo();
	;

	yyscanner = scanner_init(execution_plan,
							 &yyextra,
#if PG_VERSION_NUM >= 120000
							 &ScanKeywords,
							 ScanKeywordTokens
#else
							 ScanKeywords,
							 NumScanKeywords
#endif
	);

	for (;;)
	{
		/* get the next lexem */
		tok = core_yylex(&yylval, &yylloc, yyscanner);

		/* now we store end previsous lexem in yylloc - so could prcess it */
		if (need_replace(last_tok))
		{
			/* substitute variable instead of CONST */
			int s_len = asprintf(&tmp_str, "$%i", bind_prefix++);
			if (s_len > 0)
			{
				appendStringInfoString(plan_out, tmp_str);
				free(tmp_str);
			}
			else
			{
				appendStringInfoString(plan_out, "??");
			}
		}
		else
		{
			/* do not change - just copy as-is */
			tmp_str = strndup((char *)execution_plan + last_yylloc, yylloc - last_yylloc);
			appendStringInfoString(plan_out, tmp_str);
			free(tmp_str);
		}
		/* check if further parsing not needed */
		if (tok == 0)
			break;
		last_tok = tok;
		last_yylloc = yylloc;
	}

	scanner_finish(yyscanner);

	return plan_out;
}

/*
 * comp_location: comparator for qsorting LocationLen structs by location
 */
static int
comp_location(const void *a, const void *b)
{
	int			l = ((const LocationLen *) a)->location;
	int			r = ((const LocationLen *) b)->location;

	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
static void
fill_in_constant_lengths(JumbleState *jstate, const char *query)
{
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			last_loc = -1;
	int			i;

	/*
	 * Sort the records by location so that we can process them in order while
	 * scanning the query text.
	 */
	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);
	locs = jstate->clocations;

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query,
							 &yyextra,
							 &ScanKeywords,
							 ScanKeywordTokens);

	/* Search for each constant, in sequence */
	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			loc = locs[i].location;
		int			tok;

		Assert(loc >= 0);

		if (loc <= last_loc)
			continue;			/* Duplicate constant, ignore */

		/* Lex tokens until we find the desired constant */
		for (;;)
		{
			tok = core_yylex(&yylval, &yylloc, yyscanner);

			/* We should not hit end-of-string, but if we do, behave sanely */
			if (tok == 0)
				break;			/* out of inner for-loop */

			/*
			 * We should find the token position exactly, but if we somehow
			 * run past it, work with that.
			 */
			if (yylloc >= loc)
			{
				if (query[loc] == '-')
				{
					/*
					 * It's a negative value - this is the one and only case
					 * where we replace more than a single token.
					 *
					 * Do not compensate for the core system's special-case
					 * adjustment of location to that of the leading '-'
					 * operator in the event of a negative constant.  It is
					 * also useful for our purposes to start from the minus
					 * symbol.  In this way, queries like "select * from foo
					 * where bar = 1" and "select * from foo where bar = -2"
					 * will have identical normalized query strings.
					 */
					tok = core_yylex(&yylval, &yylloc, yyscanner);
					if (tok == 0)
						break;	/* out of inner for-loop */
				}

				/*
				 * We now rely on the assumption that flex has placed a zero
				 * byte after the text of the current token in scanbuf.
				 */
				locs[i].length = strlen(yyextra.scanbuf + loc);
				break;			/* out of inner for-loop */
			}
		}

		/* If we hit end-of-string, give up, leaving remaining lengths -1 */
		if (tok == 0)
			break;

		last_loc = loc;
	}

	scanner_finish(yyscanner);
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length (which cannot be longer) on exit.
 *
 * Returns a palloc'd string.
 */
static char *
generate_normalized_query(JumbleState *jstate, const char *query,
						  int *query_len_p, int encoding)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			i,
				len_to_wrt,		/* Length (in bytes) to write */
				quer_loc = 0,	/* Source query byte location */
				n_quer_loc = 0, /* Normalized query byte location */
				last_off = 0,	/* Offset from start for previous tok */
				last_tok_len = 0;		/* Length (in bytes) of that tok */

	/*
	 * Get constants' lengths (core system only gives us locations).  Note
	 * this also ensures the items are sorted by location.
	 */
	fill_in_constant_lengths(jstate, query);

	/* Allocate result buffer */
	norm_query = palloc(query_len + 1);

	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			off,		/* Offset from start for cur tok */
					tok_len;	/* Length (in bytes) of that tok */

		off = jstate->clocations[i].location;
		tok_len = jstate->clocations[i].length;

		if (tok_len < 0)
			continue;			/* ignore any duplicates */

		/* Copy next chunk (what precedes the next constant) */
		len_to_wrt = off - last_off;
		len_to_wrt -= last_tok_len;

		Assert(len_to_wrt >= 0);
		memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
		n_quer_loc += len_to_wrt;

		/* And insert a '?' in place of the constant token */
		norm_query[n_quer_loc++] = '?';

		quer_loc = off + tok_len;
		last_off = off;
		last_tok_len = tok_len;
	}

	/*
	 * We've copied up until the last ignorable constant.  Copy over the
	 * remaining bytes of the original query string.
	 */
	len_to_wrt = query_len - quer_loc;

	Assert(len_to_wrt >= 0);
	memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
	n_quer_loc += len_to_wrt;

	Assert(n_quer_loc <= query_len);
	norm_query[n_quer_loc] = '\0';

	*query_len_p = n_quer_loc;
	return norm_query;
}

char *gen_normquery(const char *query)
{
	if (!query) {
		return NULL;
	}
	JumbleState jstate;
	jstate.jumble = (unsigned char *)palloc(JUMBLE_SIZE);
	jstate.jumble_len = 0;
	jstate.clocations_buf_size = 32;
	jstate.clocations = (LocationLen *)
		palloc(jstate.clocations_buf_size * sizeof(LocationLen));
	jstate.clocations_count = 0;
	int query_len = strlen(query);
	return generate_normalized_query(&jstate, query, &query_len, GetDatabaseEncoding());
}