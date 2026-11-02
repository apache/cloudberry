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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserplankeys.c
 *    Shared equality and plan-output checks for runtime Bloom filters.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anserplan.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

/*
 * The key types we may filter on: by-value types for which SQL equality is
 * provably bitwise equality of the stored bytes, so that hashing the width-normalized
 * Datum on both sides agrees with the equality operator.  Producer and
 * consumer hash raw bytes, so a type with a non-canonical representation
 * would hash two different ways under one equality operator -- a false
 * negative that drops a joinable row.  Floats are the canonical
 * counterexample (-0.0 and 0.0 compare equal but are not bitwise equal);
 * everything else that does not make this guarantee is refused by omission.
 * Domains resolve through getBaseType() before landing here.
 */
bool
AnserKeyTypeSupportsRawHash(Oid keytype)
{
	switch (keytype)
	{
		case BOOLOID:
		case CHAROID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case XIDOID:
		case CIDOID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case MONEYOID:
		case PG_LSNOID:
		case XID8OID:
			return true;
		default:
			return false;
	}
}

/*
 * Match a single-column equijoin over plain (by-value) Vars and return the
 * outer and inner Vars, plus the validated base key type.
 * After set_plan_references the
 * operands are INNER_VAR / OUTER_VAR references into the join's child tlists.
 */
bool
AnserEqualityKeys(Node *clause, Var **outer_out, Var **inner_out, Oid *key_type)
{
	OpExpr	   *op;
	Node	   *l;
	Node	   *r;
	Var		   *outer_var;
	Var		   *inner_var;
	Oid			keytype;
	TypeCacheEntry *tce;

	if (clause == NULL || !IsA(clause, OpExpr))
		return false;
	op = (OpExpr *) clause;
	if (list_length(op->args) != 2)
		return false;

	l = (Node *) linitial(op->args);
	r = (Node *) lsecond(op->args);
	while (l != NULL && IsA(l, RelabelType))
		l = (Node *) ((RelabelType *) l)->arg;
	while (r != NULL && IsA(r, RelabelType))
		r = (Node *) ((RelabelType *) r)->arg;
	if (l == NULL || r == NULL || !IsA(l, Var) || !IsA(r, Var))
		return false;

	if (((Var *) l)->varno == OUTER_VAR && ((Var *) r)->varno == INNER_VAR)
	{
		outer_var = (Var *) l;
		inner_var = (Var *) r;
	}
	else if (((Var *) l)->varno == INNER_VAR && ((Var *) r)->varno == OUTER_VAR)
	{
		outer_var = (Var *) r;
		inner_var = (Var *) l;
	}
	else
		return false;

	/*
	 * Producer and consumer hash width-normalized Datum bytes, which is only correct
	 * when SQL equality coincides with bitwise Datum equality of the key.
	 * That requires the same type on both sides -- cross-type equijoins (e.g.
	 * float4 = float8, date = timestamp) hash different bit patterns for equal
	 * values -- and the type on the whitelist (see
	 * AnserKeyTypeSupportsRawHash), which is where the bitwise-equality claim
	 * is made.  Anything looser could prune rows that actually join -- a wrong
	 * answer, not a slow one.
	 */
	if (inner_var->vartype != outer_var->vartype)
		return false;

	keytype = getBaseType(inner_var->vartype);
	if (!AnserKeyTypeSupportsRawHash(keytype))
		return false;

	/*
	 * Finally, the clause has to be the type's own equality rather than just
	 * some hashable operator spelled "=".  This does not prove bitwise
	 * equality -- float8's default equality is float8eq, which is why floats
	 * need the exclusion above -- but it keeps an operator with semantics of
	 * its own from being read as if it were the canonical one.
	 */
	tce = lookup_type_cache(keytype, TYPECACHE_EQ_OPR);
	if (!OidIsValid(tce->eq_opr) || op->opno != tce->eq_opr)
		return false;

	*inner_out = inner_var;
	*outer_out = outer_var;
	*key_type = keytype;
	return true;
}

/*
 * Node types the descent may pass through: the single-child passthroughs
 * (Hash, Motion), the joins whose sides carry the column up (HashJoin), and
 * our own filter nodes.  Anything else -- WindowAgg, Limit, Agg, Result, an
 * Append or SetOp whose column comes from several relations at once -- ends
 * the descent: the superset argument for landing the filter on the base scan
 * (anser_try_inject) only holds for joins and passthroughs; an operator whose
 * output depends on the whole input (a window over it, an aggregate of it,
 * its first N rows) would be computed over the filtered rows instead.
 */
bool
AnserKeyPassthroughNode(const Plan *plan)
{
	return IsA(plan, Hash) || IsA(plan, Motion) || IsA(plan, HashJoin) ||
		AnserIsRuntimeFilterScan(plan);
}

/* A plan output position is not a base-table attribute number. */
Var *
AnserPlanOutputVar(const Plan *plan, AttrNumber position)
{
	TargetEntry *tle;

	if (plan == NULL || position < 1 || position > list_length(plan->targetlist))
		return NULL;
	tle = (TargetEntry *) list_nth(plan->targetlist, position - 1);
	if (tle == NULL || !IsA(tle->expr, Var))
		return NULL;
	return (Var *) tle->expr;
}
