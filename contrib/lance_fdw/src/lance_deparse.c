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
 * lance_deparse.c
 *	  Two passes over a qual: is every node on the whitelist, and what does it
 *	  render to.
 *
 * Every entry on the whitelist costs an argument that Lance and PostgreSQL
 * agree exactly, because a wrong argument shows up as silently wrong rows
 * rather than as an error.  Those arguments were made against a real cluster
 * and are recorded in the pushdown suite; the side conditions below -
 * collation, encoding, typmod, the absence of a timezone - are the places
 * where the two only agree some of the time.
 *
 * IDENTIFICATION
 *	  src/lance_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "lance_deparse.h"
#include "lance_option.h"

#include "access/table.h"
#include "access/transam.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/* `<>` is not a btree strategy; it is the negator of equality. */
#define LANCE_STRAT_NE		(BTMaxStrategyNumber + 1)

typedef struct LanceDeparseCtx
{
	Oid			foreigntableid;
	Index		varno;
	List	   *attnums;
} LanceDeparseCtx;

static bool lance_expr_ok(Node *node, LanceDeparseCtx *ctx);
static bool lance_render(Node *node, LanceDeparseCtx *ctx, StringInfo buf);
static bool lance_render_const_as(Const *con, Oid astype, StringInfo buf);

/*
 * Types this block can compare at all.  Membership is necessary, never
 * sufficient: text carries a collation condition, timestamp a typmod one, and
 * timestamptz is absent on purpose (see lance_type_ok_for_var).
 */
static bool
lance_type_ok(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case TEXTOID:
		case VARCHAROID:
		case DATEOID:
		case TIMESTAMPOID:
			return true;
		default:
			/*
			 * TIMESTAMPTZOID is deliberately not here.  It is equivalent only
			 * when the Arrow column's timezone is UTC, and a non-UTC one
			 * diverges silently - measured at 3 rows short on equality and 5
			 * rows long on `>`.  Planning does no I/O, so
			 * there is no way to tell the two apart here.
			 */
			return false;
	}
}

/* Is this column, as declared, one we may compare? */
static bool
lance_type_ok_for_var(Oid typid, int32 typmod)
{
	if (!lance_type_ok(typid))
		return false;

	/*
	 * A length-limited varchar is refused for the same reason the read side
	 * refuses it: enforcing the limit would mean truncating.
	 */
	if (typid == VARCHAROID && typmod != -1)
		return false;

	/*
	 * timestamp must be able to hold microseconds.  The read side decides this
	 * from the Arrow unit, which planning cannot see, so this rule is stricter
	 * than the read side's on purpose.
	 */
	if (typid == TIMESTAMPOID && typmod != -1 && typmod < 6)
		return false;

	return true;
}

/*
 * A coarse type-level gate for the elements of an IN list, where the value
 * test happens per element later.  Numeric into numeric, text into text.
 */
static bool
lance_promotion_allowed(Oid from, Oid to)
{
	if (from == to)
		return true;
	switch (to)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
			return from == INT2OID || from == INT4OID || from == INT8OID ||
				from == FLOAT4OID || from == FLOAT8OID;
		case TEXTOID:
		case VARCHAROID:
			return from == TEXTOID || from == VARCHAROID;
		default:
			return false;
	}
}

/* The constant's value as an int64, if it is an integer constant. */
static bool
lance_const_as_int64(Const *con, int64 *out)
{
	switch (con->consttype)
	{
		case INT2OID:
			*out = (int64) DatumGetInt16(con->constvalue);
			return true;
		case INT4OID:
			*out = (int64) DatumGetInt32(con->constvalue);
			return true;
		case INT8OID:
			*out = DatumGetInt64(con->constvalue);
			return true;
		default:
			return false;
	}
}

/*
 * Can this constant be carried into `astype` - the column's type - without
 * changing?  The test is on the *value*, not on which type is wider: 1 is
 * exactly representable in a smallint even though the literal arrives as int4,
 * while 100000 is not, and 0.1 is exact in float8 but not in float4.  Carrying
 * an exactly representable value across is what makes the rendered comparison
 * mean what PostgreSQL's cross-type operator means.
 */
static bool
lance_const_fits(Const *con, Oid astype)
{
	int64		v;

	if (con->consttype == astype)
		return true;

	/* Integer constant into any numeric column: check the round trip. */
	if (lance_const_as_int64(con, &v))
	{
		switch (astype)
		{
			case INT2OID:
				return v >= PG_INT16_MIN && v <= PG_INT16_MAX;
			case INT4OID:
				return v >= PG_INT32_MIN && v <= PG_INT32_MAX;
			case INT8OID:
				return true;
			case FLOAT4OID:
				return (int64) (float) v == v;
			case FLOAT8OID:
				return (int64) (double) v == v;
			default:
				return false;
		}
	}

	/* float4 constant into a float8 column is exact; the reverse may not be. */
	if (con->consttype == FLOAT4OID && astype == FLOAT8OID)
		return true;
	if (con->consttype == FLOAT8OID && astype == FLOAT4OID)
	{
		double		d = DatumGetFloat8(con->constvalue);

		return (double) (float) d == d;
	}

	return false;
}

/*
 * The collation conditions.
 *
 * Equality needs a deterministic collation, where "equal" is byte equality on
 * both sides.  Ordering needs more: PostgreSQL compares the bytes of the
 * *database encoding* while Lance compares UTF-8 bytes, so the two orders
 * coincide only under a C/POSIX collation in a UTF-8 database.
 */
static bool
lance_collation_ok(Oid typid, Oid collid, bool ordering)
{
	if (typid != TEXTOID && typid != VARCHAROID)
		return true;

	if (!OidIsValid(collid))
		return false;

	if (ordering)
		return lc_collate_is_c(collid) && GetDatabaseEncoding() == PG_UTF8;

	return get_collation_isdeterministic(collid);
}

/*
 * Resolve a comparison operator to a btree strategy, or 0.  The strategy comes
 * from the catalog rather than from the operator's name, which is what makes
 * the whitelist concrete: a user-defined `=` belongs to no built-in opfamily.
 *
 * `vartype` is the column's type; the comparison is understood as happening in
 * that type, with the constant promoted into it.
 */
static int
lance_op_strategy(Oid opno, Oid vartype)
{
	Oid			opclass;
	Oid			opfamily;
	int			strategy;
	Oid			eq;

	if (opno >= FirstNormalObjectId)	/* never a user-defined operator */
		return 0;

	opclass = GetDefaultOpClass(vartype, BTREE_AM_OID);
	if (!OidIsValid(opclass))
		return 0;
	opfamily = get_opclass_family(opclass);
	if (!OidIsValid(opfamily))
		return 0;

	strategy = get_op_opfamily_strategy(opno, opfamily);
	if (strategy >= BTLessStrategyNumber && strategy <= BTGreaterStrategyNumber)
		return strategy;

	eq = get_opfamily_member(opfamily, vartype, vartype, BTEqualStrategyNumber);
	if (OidIsValid(eq) && get_negator(eq) == opno)
		return LANCE_STRAT_NE;

	return 0;
}

static const char *
lance_strategy_text(int strategy)
{
	switch (strategy)
	{
		case BTLessStrategyNumber:			return "<";
		case BTLessEqualStrategyNumber:		return "<=";
		case BTEqualStrategyNumber:			return "=";
		case BTGreaterEqualStrategyNumber:	return ">=";
		case BTGreaterStrategyNumber:		return ">";
		case LANCE_STRAT_NE:				return "<>";
		default:							return NULL;
	}
}

/*
 * Quote a Lance column name.  The dialect's identifier quote is
 * the backtick: a double-quoted word is a string literal there, so rendering
 * "tag" would compare against the text 'tag'.  Backtick quoting is exact and
 * case-sensitive, which keeps a dataset holding both `Tag` and `tag` from
 * silently matching the wrong one - an unquoted name falls back to a
 * case-insensitive match when no exact one exists.
 *
 * A backtick inside the name has no escape, so such a column cannot be named
 * at all and the caller turns that into "do not push down".
 */
char *
lance_deparse_quote_ident(const char *name)
{
	StringInfoData buf;

	if (name == NULL || strchr(name, '`') != NULL)
		return NULL;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '`');
	appendStringInfoString(&buf, name);
	appendStringInfoChar(&buf, '`');
	return buf.data;
}

/*
 * A string literal.  Single quotes double; backslash is NOT an escape and is
 * emitted as itself; control characters go in raw.  The bytes
 * have to reach Lance as UTF-8, so a database in another encoding is converted
 * here - and a value that will not convert is simply not pushed down.
 */
static bool
lance_render_text(const char *str, StringInfo buf)
{
	char	   *utf8;
	const char *p;

	utf8 = pg_server_to_any(str, strlen(str), PG_UTF8);
	if (utf8 == NULL)
		return false;

	appendStringInfoChar(buf, '\'');
	for (p = utf8; *p; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
	return true;
}

/*
 * Render a constant as `astype` - the column's type, after promotion.  Returns
 * false for anything that cannot be written exactly, which is an ordinary
 * "do not push down".
 */
static bool
lance_render_const_as(Const *con, Oid astype, StringInfo buf)
{
	int64		iv;

	switch (astype)
	{
		case BOOLOID:
			if (con->consttype != BOOLOID)
				return false;
			appendStringInfoString(buf,
								   DatumGetBool(con->constvalue) ? "true" : "false");
			return true;

		case INT2OID:
		case INT4OID:
		case INT8OID:
			if (!lance_const_as_int64(con, &iv))
				return false;
			appendStringInfo(buf, INT64_FORMAT, iv);
			return true;

		case FLOAT4OID:
		case FLOAT8OID:
			{
				double		d;

				if (lance_const_as_int64(con, &iv))
					d = (double) iv;
				else if (con->consttype == FLOAT4OID)
					d = (double) DatumGetFloat4(con->constvalue);
				else if (con->consttype == FLOAT8OID)
					d = DatumGetFloat8(con->constvalue);
				else
					return false;

				/*
				 * A NaN or infinite *constant* is refused: we measured how
				 * Lance compares NaN column values (like PostgreSQL, on a total
				 * order) but never how it parses such a literal, and an
				 * unmeasured literal syntax is not something to guess at.
				 */
				if (isnan(d) || isinf(d))
					return false;

				appendStringInfoString(buf,
									   DatumGetCString(DirectFunctionCall1(float8out,
																		   Float8GetDatum(d))));
				return true;
			}

		case TEXTOID:
		case VARCHAROID:
			if (con->consttype != TEXTOID && con->consttype != VARCHAROID)
				return false;
			return lance_render_text(TextDatumGetCString(con->constvalue), buf);

		case DATEOID:
			{
				DateADT		d;
				struct pg_tm tm;
				char		iso[MAXDATELEN + 1];

				if (con->consttype != DATEOID)
					return false;
				d = DatumGetDateADT(con->constvalue);
				if (DATE_NOT_FINITE(d))
					return false;	/* Lance has no infinity */

				/*
				 * ISO, unconditionally.  date_out honours the session's
				 * DateStyle, so it would render 2000-02-29 as '02-29-2000'
				 * under MDY - which Lance rejects.  A filter's meaning must not
				 * depend on a GUC that has nothing to do with it.
				 */
				j2date(d + POSTGRES_EPOCH_JDATE,
					   &tm.tm_year, &tm.tm_mon, &tm.tm_mday);
				EncodeDateOnly(&tm, USE_ISO_DATES, iso);
				appendStringInfo(buf, "DATE '%s'", iso);
				return true;
			}

		case TIMESTAMPOID:
			{
				Timestamp	t;
				struct pg_tm tm;
				fsec_t		fsec;
				char		iso[MAXDATELEN + 1];

				if (con->consttype != TIMESTAMPOID)
					return false;
				t = DatumGetTimestamp(con->constvalue);
				if (TIMESTAMP_NOT_FINITE(t))
					return false;
				if (timestamp2tm(t, NULL, &tm, &fsec, NULL, NULL) != 0)
					return false;

				/* ISO for the same reason as above. */
				EncodeDateTime(&tm, fsec, false, 0, NULL, USE_ISO_DATES, iso);
				appendStringInfo(buf, "TIMESTAMP '%s'", iso);
				return true;
			}

		default:
			return false;
	}
}

static Node *
lance_strip(Node *node)
{
	while (node != NULL && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	return node;
}

/* The column of a comparison, if this node is one of ours. */
static Var *
lance_as_var(Node *node, LanceDeparseCtx *ctx)
{
	Var		   *var;

	node = lance_strip(node);
	if (node == NULL || !IsA(node, Var))
		return NULL;
	var = (Var *) node;
	if (var->varno != ctx->varno || var->varlevelsup != 0 || var->varattno <= 0)
		return NULL;
	if (!lance_type_ok_for_var(var->vartype, var->vartypmod))
		return NULL;
	return var;
}

static Const *
lance_as_const(Node *node)
{
	node = lance_strip(node);
	if (node == NULL || !IsA(node, Const))
		return NULL;
	return ((Const *) node)->constisnull ? NULL : (Const *) node;
}

/* Pass 1: judge.  Produces nothing, so it can refuse anything. */
static bool
lance_expr_ok(Node *node, LanceDeparseCtx *ctx)
{
	node = lance_strip(node);
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
			/* A bare boolean column used as a qualifier. */
			{
				Var		   *var = lance_as_var(node, ctx);

				return var != NULL && var->vartype == BOOLOID;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				Var		   *var;
				Const	   *con;
				int			strategy;
				bool		ordering;

				if (list_length(op->args) != 2)
					return false;

				var = lance_as_var((Node *) linitial(op->args), ctx);
				con = lance_as_const((Node *) lsecond(op->args));
				if (var == NULL || con == NULL)
				{
					var = lance_as_var((Node *) lsecond(op->args), ctx);
					con = lance_as_const((Node *) linitial(op->args));
				}
				if (var == NULL || con == NULL)
					return false;	/* Var-to-Var and everything else */

				if (!lance_const_fits(con, var->vartype))
					return false;

				strategy = lance_op_strategy(op->opno, var->vartype);
				if (strategy == 0)
					return false;

				/* bool has no ordering we argue about. */
				if (var->vartype == BOOLOID &&
					strategy != BTEqualStrategyNumber &&
					strategy != LANCE_STRAT_NE)
					return false;

				ordering = (strategy != BTEqualStrategyNumber &&
							strategy != LANCE_STRAT_NE);
				return lance_collation_ok(var->vartype, op->inputcollid,
										  ordering);
			}

		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				ListCell   *lc;

				if (b->boolop != AND_EXPR && b->boolop != OR_EXPR &&
					b->boolop != NOT_EXPR)
					return false;
				foreach(lc, b->args)
					if (!lance_expr_ok((Node *) lfirst(lc), ctx))
						return false;
				return true;
			}

		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				if (nt->argisrow)
					return false;
				return lance_as_var((Node *) nt->arg, ctx) != NULL;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				Var		   *var;
				Const	   *arr;
				ArrayType  *a;

				/*
				 * `IN` and nothing else (DECISIONS #4).  useOr alone only says
				 * ANY, which `x <> ANY (...)` satisfies too - rendering that as
				 * IN would turn false into true.  So the operator must be the
				 * family's own equality, and the array a plain one-dimensional
				 * non-NULL constant.
				 */
				if (!sa->useOr || list_length(sa->args) != 2)
					return false;

				var = lance_as_var((Node *) linitial(sa->args), ctx);
				arr = lance_as_const((Node *) lsecond(sa->args));
				if (var == NULL || arr == NULL)
					return false;

				if (lance_op_strategy(sa->opno, var->vartype) !=
					BTEqualStrategyNumber)
					return false;
				if (!lance_collation_ok(var->vartype, sa->inputcollid, false))
					return false;

				a = DatumGetArrayTypeP(arr->constvalue);
				if (ARR_NDIM(a) != 1)
					return false;
				if (ARR_ELEMTYPE(a) != var->vartype &&
					!lance_promotion_allowed(ARR_ELEMTYPE(a), var->vartype))
					return false;
				return true;
			}

		default:
			return false;
	}
}

/* Pass 2: render.  May still refuse; that is why it runs in the same call. */
static bool
lance_render(Node *node, LanceDeparseCtx *ctx, StringInfo buf)
{
	node = lance_strip(node);
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;
				Relation	rel;
				TupleDesc	tupdesc;
				Form_pg_attribute att;
				char	   *lname;
				char	   *quoted;

				rel = table_open(ctx->foreigntableid, NoLock);
				tupdesc = RelationGetDescr(rel);
				if (var->varattno > tupdesc->natts)
				{
					table_close(rel, NoLock);
					return false;
				}
				att = TupleDescAttr(tupdesc, var->varattno - 1);
				if (att->attisdropped)
				{
					table_close(rel, NoLock);
					return false;
				}
				lname = lance_get_column_name(ctx->foreigntableid,
											  var->varattno,
											  NameStr(att->attname));
				table_close(rel, NoLock);

				quoted = lance_deparse_quote_ident(lname);
				if (quoted == NULL)
					return false;
				appendStringInfoString(buf, quoted);

				if (!list_member_int(ctx->attnums, var->varattno))
					ctx->attnums = lappend_int(ctx->attnums, var->varattno);
				return true;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				Var		   *var;
				Const	   *con;
				bool		var_left = true;
				int			strategy;
				const char *opstr;

				var = lance_as_var((Node *) linitial(op->args), ctx);
				con = lance_as_const((Node *) lsecond(op->args));
				if (var == NULL || con == NULL)
				{
					var = lance_as_var((Node *) lsecond(op->args), ctx);
					con = lance_as_const((Node *) linitial(op->args));
					var_left = false;
				}
				if (var == NULL || con == NULL)
					return false;

				strategy = lance_op_strategy(op->opno, var->vartype);
				opstr = lance_strategy_text(strategy);
				if (opstr == NULL)
					return false;

				appendStringInfoChar(buf, '(');
				if (var_left)
				{
					if (!lance_render((Node *) var, ctx, buf))
						return false;
					appendStringInfo(buf, " %s ", opstr);
					if (!lance_render_const_as(con, var->vartype, buf))
						return false;
				}
				else
				{
					if (!lance_render_const_as(con, var->vartype, buf))
						return false;
					appendStringInfo(buf, " %s ", opstr);
					if (!lance_render((Node *) var, ctx, buf))
						return false;
				}
				appendStringInfoChar(buf, ')');
				return true;
			}

		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				ListCell   *lc;
				bool		first = true;

				if (b->boolop == NOT_EXPR)
				{
					appendStringInfoString(buf, "(NOT ");
					if (!lance_render((Node *) linitial(b->args), ctx, buf))
						return false;
					appendStringInfoChar(buf, ')');
					return true;
				}

				appendStringInfoChar(buf, '(');
				foreach(lc, b->args)
				{
					if (!first)
						appendStringInfoString(buf,
											   b->boolop == AND_EXPR ? " AND " : " OR ");
					first = false;
					if (!lance_render((Node *) lfirst(lc), ctx, buf))
						return false;
				}
				appendStringInfoChar(buf, ')');
				return true;
			}

		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				appendStringInfoChar(buf, '(');
				if (!lance_render((Node *) nt->arg, ctx, buf))
					return false;
				appendStringInfoString(buf,
									   nt->nulltesttype == IS_NULL
									   ? " IS NULL)" : " IS NOT NULL)");
				return true;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				Var		   *var = lance_as_var((Node *) linitial(sa->args), ctx);
				Const	   *arr = lance_as_const((Node *) lsecond(sa->args));
				ArrayType  *a;
				Datum	   *elems;
				bool	   *nulls;
				int			nelems;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				int			i;
				bool		first = true;

				if (var == NULL || arr == NULL)
					return false;

				a = DatumGetArrayTypeP(arr->constvalue);
				get_typlenbyvalalign(ARR_ELEMTYPE(a), &elmlen, &elmbyval,
									 &elmalign);
				deconstruct_array(a, ARR_ELEMTYPE(a), elmlen, elmbyval,
								  elmalign, &elems, &nulls, &nelems);
				if (nelems == 0)
					return false;	/* an empty IN needs its own argument */

				if (!lance_render((Node *) var, ctx, buf))
					return false;
				appendStringInfoString(buf, " IN (");
				for (i = 0; i < nelems; i++)
				{
					Const		item;

					if (!first)
						appendStringInfoString(buf, ", ");
					first = false;

					if (nulls[i])
					{
						/*
						 * A NULL in the list: both systems make the whole
						 * comparison NULL for a row that matches nothing else,
						 * and both were measured to agree.
						 */
						appendStringInfoString(buf, "NULL");
						continue;
					}

					memset(&item, 0, sizeof(item));
					item.xpr.type = T_Const;
					item.consttype = ARR_ELEMTYPE(a);
					item.constvalue = elems[i];
					item.constisnull = false;
					if (!lance_const_fits(&item, var->vartype) ||
						!lance_render_const_as(&item, var->vartype, buf))
						return false;
				}
				appendStringInfoChar(buf, ')');
				return true;
			}

		default:
			return false;
	}
}

bool
lance_deparse_qual(Expr *expr, Oid foreigntableid, Index varno,
				   LanceDeparsed *out)
{
	LanceDeparseCtx ctx;
	StringInfoData buf;

	ctx.foreigntableid = foreigntableid;
	ctx.varno = varno;
	ctx.attnums = NIL;

	if (!lance_expr_ok((Node *) expr, &ctx))
		return false;

	initStringInfo(&buf);
	if (!lance_render((Node *) expr, &ctx, &buf))
		return false;

	out->sql = buf.data;
	out->attnums = ctx.attnums;
	return true;
}
