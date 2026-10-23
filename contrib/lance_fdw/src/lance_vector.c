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
 * lance_vector.c
 *	  Distance operators on real[] and the planner's vector Top-K matcher.
 *
 * The operators are ordinary SQL operators: they work on any two real[]
 * values, foreign table or not, and they are what PostgreSQL sorts by in the
 * end.  Lance only ever chooses *which* k rows reach that sort, so
 * nothing here has to agree with Lance's own _distance - which is L2 squared,
 * and 1 - a.b for the inner product.
 *
 * The matcher is a whitelist.  A LIMIT that Lance applies before PostgreSQL
 * has seen every row it would have filtered, joined or grouped is silently
 * wrong rows, so the question asked here is never "can this be pushed" but
 * "is this exactly the shape where cutting to k first is the same thing".
 *
 * IDENTIFICATION
 *	  src/lance_vector.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "lance_deparse.h"
#include "lance_option.h"
#include "lance_runtime.h"
#include "lance_vector.h"

#include "catalog/namespace.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/lsyscache.h"

PG_FUNCTION_INFO_V1(lance_vector_l2_distance);
PG_FUNCTION_INFO_V1(lance_vector_cosine_distance);
PG_FUNCTION_INFO_V1(lance_vector_negative_inner_product);

static const char *const metric_operators[LANCE_VECTOR_NMETRICS] = {
	[LANCE_VECTOR_L2] = "<->",
	[LANCE_VECTOR_COSINE] = "<=>",
	[LANCE_VECTOR_DOT] = "<#>",
};

static const char *const metric_names[LANCE_VECTOR_NMETRICS] = {
	[LANCE_VECTOR_L2] = "L2",
	[LANCE_VECTOR_COSINE] = "cosine",
	[LANCE_VECTOR_DOT] = "inner product",
};

const char *
lance_vector_metric_name(LanceVectorMetric metric)
{
	return metric_names[metric];
}

const char *
lance_vector_metric_operator(LanceVectorMetric metric)
{
	return metric_operators[metric];
}

/* ------------------------------------------------------------------------
 * The operators
 * ------------------------------------------------------------------------
 */

/*
 * The elements of a one-dimensional real[] without NULLs.  Anything else is an
 * error rather than a guess: a NULL element has no distance, and a 2-D array
 * flattened silently would compare the wrong elements.
 */
static const float4 *
lance_vector_elements(ArrayType *a, int *n)
{
	if (ARR_NDIM(a) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("lance_fdw: a vector must be a one-dimensional array"),
				 errdetail("The array has %d dimensions.", ARR_NDIM(a))));

	if (ARR_HASNULL(a) && array_contains_nulls(a))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("lance_fdw: a vector must not contain NULL elements")));

	*n = ARR_NDIM(a) == 0 ? 0 : ARR_DIMS(a)[0];
	return (const float4 *) ARR_DATA_PTR(a);
}

static void
lance_vector_pair(FunctionCallInfo fcinfo, const float4 **x, const float4 **y,
				  int *n)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			na;
	int			nb;

	*x = lance_vector_elements(a, &na);
	*y = lance_vector_elements(b, &nb);

	if (na != nb)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("lance_fdw: vectors of different dimensions"),
				 errdetail("The left vector has %d elements and the right one %d.",
						   na, nb)));
	*n = na;
}

/* real[] <-> real[]: Euclidean distance, sqrt(sum((a - b)^2)) */
Datum
lance_vector_l2_distance(PG_FUNCTION_ARGS)
{
	const float4 *x;
	const float4 *y;
	int			n;
	int			i;
	double		sum = 0.0;

	lance_vector_pair(fcinfo, &x, &y, &n);

	for (i = 0; i < n; i++)
	{
		double		d = (double) x[i] - (double) y[i];

		sum += d * d;
	}

	PG_RETURN_FLOAT8(sqrt(sum));
}

/*
 * real[] <=> real[]: cosine distance, 1 - cos.  A zero vector has no
 * direction, and like pgvector the answer is NaN rather than an error; the
 * similarity is clamped so that rounding cannot step outside [-1, 1].
 */
Datum
lance_vector_cosine_distance(PG_FUNCTION_ARGS)
{
	const float4 *x;
	const float4 *y;
	int			n;
	int			i;
	double		dot = 0.0;
	double		nx = 0.0;
	double		ny = 0.0;
	double		similarity;

	lance_vector_pair(fcinfo, &x, &y, &n);

	for (i = 0; i < n; i++)
	{
		dot += (double) x[i] * (double) y[i];
		nx += (double) x[i] * (double) x[i];
		ny += (double) y[i] * (double) y[i];
	}

	similarity = dot / sqrt(nx * ny);
	if (isnan(similarity))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (similarity > 1.0)
		similarity = 1.0;
	else if (similarity < -1.0)
		similarity = -1.0;

	PG_RETURN_FLOAT8(1.0 - similarity);
}

/* real[] <#> real[]: the negative inner product, so that ascending is nearest */
Datum
lance_vector_negative_inner_product(PG_FUNCTION_ARGS)
{
	const float4 *x;
	const float4 *y;
	int			n;
	int			i;
	double		dot = 0.0;

	lance_vector_pair(fcinfo, &x, &y, &n);

	for (i = 0; i < n; i++)
		dot += (double) x[i] * (double) y[i];

	PG_RETURN_FLOAT8(-dot);
}

/* ------------------------------------------------------------------------
 * The matcher
 * ------------------------------------------------------------------------
 */

/*
 * The OIDs of the three operators, as this extension defines them.  Found by
 * the schema lance_fdw is installed in - read from pg_extension, because the
 * extension is relocatable - and by argument types, never through search_path:
 * an operator of the same name and signature in another schema is somebody
 * else's and must not be pushed down as if it were ours.
 */
static bool
lance_vector_operators(Oid ops[LANCE_VECTOR_NMETRICS])
{
	Oid			extoid = get_extension_oid("lance_fdw", true);
	char	   *nspname;
	int			m;

	if (!OidIsValid(extoid))
		return false;

	nspname = get_namespace_name(get_extension_schema(extoid));
	if (nspname == NULL)
		return false;

	for (m = 0; m < LANCE_VECTOR_NMETRICS; m++)
	{
		List	   *name = list_make2(makeString(nspname),
									  makeString(pstrdup(metric_operators[m])));

		ops[m] = OpernameGetOprid(name, FLOAT4ARRAYOID, FLOAT4ARRAYOID);
		if (!OidIsValid(ops[m]))
			return false;
	}

	return true;
}

static int
lance_vector_metric_of(Oid opno, const Oid ops[LANCE_VECTOR_NMETRICS])
{
	int			m;

	for (m = 0; m < LANCE_VECTOR_NMETRICS; m++)
		if (opno == ops[m])
			return m;
	return -1;
}

/* Does any ORDER BY item use one of our operators?  Decides whether to explain a refusal. */
static bool
lance_vector_is_candidate(Query *parse, const Oid ops[LANCE_VECTOR_NMETRICS])
{
	ListCell   *lc;

	foreach(lc, parse->sortClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		Node	   *expr = (Node *) get_sortgroupclause_expr(sgc, parse->targetList);

		if (IsA(expr, OpExpr) &&
			lance_vector_metric_of(((OpExpr *) expr)->opno, ops) >= 0)
			return true;
	}

	return false;
}

/* A query vector the QD can evaluate at execution: a non-NULL Const or $n. */
static bool
lance_vector_query_ok(Node *node)
{
	if (IsA(node, Const))
		return ((Const *) node)->consttype == FLOAT4ARRAYOID &&
			!((Const *) node)->constisnull;

	if (IsA(node, Param))
		return ((Param *) node)->paramkind == PARAM_EXTERN &&
			((Param *) node)->paramtype == FLOAT4ARRAYOID;

	return false;
}

static bool
lance_vector_column_ok(Node *node, Index relid)
{
	Var		   *var;

	if (!IsA(node, Var))
		return false;

	var = (Var *) node;
	return var->varno == relid && var->varlevelsup == 0 &&
		var->varattno > 0 && var->vartype == FLOAT4ARRAYOID;
}

#define REFUSE(why) \
	do { \
		if (candidate) \
			elog(DEBUG1, "lance_fdw: vector Top-K not pushed down: %s", (why)); \
		return false; \
	} while (0)

bool
lance_vector_match(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
				   LanceTopK *out)
{
	Query	   *parse = root->parse;
	Oid			ops[LANCE_VECTOR_NMETRICS];
	bool		candidate;
	ForeignTable *table;
	SortGroupClause *sgc;
	OpExpr	   *op;
	Node	   *left;
	Node	   *right;
	Var		   *var;
	Node	   *query;
	Const	   *limit;
	int64		k;
	int			metric;
	ListCell   *lc;

	if (parse == NULL || parse->sortClause == NIL)
		return false;
	if (!lance_vector_operators(ops))
		return false;

	candidate = lance_vector_is_candidate(parse, ops);
	if (!candidate)
		return false;

	/* 1. The escape hatch, and the planner this was written against. */
	if (!lance_enable_vector_pushdown)
		REFUSE("lance_fdw.enable_vector_pushdown is off");
	if (root->is_from_orca)
		REFUSE("planned by ORCA");

	/*
	 * 2. 'any' may put the scan on a QE that never hears the QD's evaluation
	 * of the query vector or its GUCs.
	 */
	table = GetForeignTable(foreigntableid);
	if (table->exec_location != FTEXECLOCATION_ALL_SEGMENTS &&
		table->exec_location != FTEXECLOCATION_COORDINATOR)
		REFUSE("mpp_execute is not 'all segments' or 'coordinator'");

	/* 3. The top level of a plain SELECT over this one table. */
	if (root->query_level != 1)
		REFUSE("the foreign table is inside a subquery or CTE");
	if (parse->commandType != CMD_SELECT)
		REFUSE("not a SELECT");
	if (bms_membership(root->all_baserels) != BMS_SINGLETON)
		REFUSE("the query reads more than one relation");
	if (parse->rowMarks != NIL)
		REFUSE("the query locks rows");

	/* 4. Anything that sits between the scan and the LIMIT. */
	if (parse->groupClause != NIL || parse->groupingSets != NIL ||
		parse->distinctClause != NIL || parse->hasAggs ||
		parse->hasWindowFuncs || root->hasHavingQual ||
		parse->setOperations != NULL || parse->hasTargetSRFs)
		REFUSE("grouping, DISTINCT, aggregates, window functions, set operations or set-returning functions");

	/* 5. LIMIT k, a constant, no OFFSET, not WITH TIES. */
	if (parse->limitOffset != NULL)
		REFUSE("OFFSET");
	if (parse->limitOption != LIMIT_OPTION_COUNT)
		REFUSE("not a plain LIMIT");
	if (parse->limitCount == NULL || !IsA(parse->limitCount, Const))
		REFUSE("the LIMIT is not a constant");
	limit = (Const *) parse->limitCount;
	if (limit->constisnull || limit->consttype != INT8OID)
		REFUSE("the LIMIT is not a constant");
	k = DatumGetInt64(limit->constvalue);
	if (k < 1)
		REFUSE("the LIMIT is below 1");
	if (k > lance_vector_max_k)
		REFUSE("the LIMIT exceeds lance_fdw.vector_pushdown_max_k");
	if (root->limit_tuples != (double) k)
		REFUSE("the planner does not apply the LIMIT to this scan");

	/* 6. ORDER BY exactly one distance, ascending, NULLs last. */
	if (list_length(parse->sortClause) != 1)
		REFUSE("more than one ORDER BY item");
	sgc = linitial_node(SortGroupClause, parse->sortClause);
	if (sgc->sortop != Float8LessOperator || sgc->nulls_first)
		REFUSE("the ORDER BY is not ascending with NULLS LAST");

	{
		Node	   *expr = (Node *) get_sortgroupclause_expr(sgc, parse->targetList);

		if (!IsA(expr, OpExpr))
			REFUSE("the ORDER BY is not a distance");
		op = (OpExpr *) expr;
	}
	metric = lance_vector_metric_of(op->opno, ops);
	if (metric < 0 || list_length(op->args) != 2)
		REFUSE("the ORDER BY is not a distance");

	left = (Node *) linitial(op->args);
	right = (Node *) lsecond(op->args);
	if (lance_vector_column_ok(left, baserel->relid) && lance_vector_query_ok(right))
	{
		var = (Var *) left;
		query = right;
	}
	else if (lance_vector_column_ok(right, baserel->relid) && lance_vector_query_ok(left))
	{
		/* All three distances are symmetric. */
		var = (Var *) right;
		query = left;
	}
	else
		REFUSE("the distance is not between a real[] column of this table and a non-NULL constant or parameter");

	/*
	 * 7. Every qual goes to Lance.  One that stayed would be applied after
	 * Lance has cut the rows to k, and the query would come back short.  The
	 * judgement is GetForeignPlan's own, so the two cannot disagree.
	 * Pseudoconstant quals are a gating Result above the scan - all rows or
	 * none - and GetForeignPlan leaves them out too.
	 */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		LanceDeparsed deparsed;

		if (rinfo->pseudoconstant)
			continue;
		if (!lance_deparse_pushdown(rinfo->clause, foreigntableid,
									baserel->relid, &deparsed))
			REFUSE("a WHERE clause would be evaluated after the LIMIT");
	}

	out->attnum = var->varattno;
	out->column = lance_get_column_name(foreigntableid, var->varattno,
										get_attname(foreigntableid,
													var->varattno, false));
	out->metric = (LanceVectorMetric) metric;
	out->k = k;
	out->query = (Expr *) query;

	return true;
}
