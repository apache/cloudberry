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
 * anserplan.c
 *	  Post-planning transformation that injects Anser runtime bloom-filter
 *	  producer/consumer CustomScan nodes into a finished plan tree.
 *
 * The pass runs once from planner() (after both the Postgres planner and ORCA,
 * and after set_plan_references / the cdbllize slice passes), recognizes one
 * supported join shape, and inserts a producer on the hash build side and a
 * consumer above the probe scan.  See anserplan.h for the
 * rationale.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserplan.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

/* Runtime-filter bloom size bounds (realized bitset bytes). */
#define ANSER_RF_MIN_BYTES		(1024 * 1024)		/* bloom_create's 1 MB floor */
#define ANSER_RF_MAX_BYTES		(64 * 1024 * 1024)
#define ANSER_RF_HEADER_ROOM	64

/* Per-statement state for the injection pass. */
typedef struct AnserInjectCtx
{
	uint32		next_condition_id;
	int			next_plan_node_id;
	List	   *consumer_keys;	/* condition_keys already given a consumer node;
								 * enforces one consumer per channel (see
								 * anser_try_inject) */
} AnserInjectCtx;

static int	anser_max_plan_node_id(Plan *plan);
static bool anser_rf_size(double est_rows, int64 *total_elems,
						  int64 *max_payload, int64 *planned_bytes);
static bool anser_hashjoin_keys(HashJoin *hj, AttrNumber *inner_attno,
								AttrNumber *outer_attno);
static bool anser_resolve_build_scan(Plan *hash, AttrNumber inner_attno,
									 Plan **parent_out, Plan **scan_out,
									 AttrNumber *attno_out);
static void anser_try_inject(HashJoin *hj, AnserInjectCtx *ctx);
static void anser_inject_walk(Plan *plan, AnserInjectCtx *ctx);

void
AnserApplyRuntimeFilters(PlannedStmt *stmt)
{
	/*
	 * Opt-in and coordinator-only: the pass runs on the QD where the whole
	 * PlannedStmt is available, and only when the operator has enabled the
	 * Anser subsystem and the runtime-filter feature.  Anything else is left
	 * completely untouched.
	 */
	if (!gp_anser_enable || !gp_anser_runtime_filter)
		return;
	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	if (stmt == NULL || stmt->commandType != CMD_SELECT || stmt->planTree == NULL)
		return;

	{
		AnserInjectCtx ctx;
		ListCell   *lc;
		int			maxid;

		/*
		 * Injected nodes need plan_node_ids unique across the whole statement.
		 * set_plan_references already numbered every existing node, so continue
		 * past the current maximum (planTree + subplans).
		 *
		 * We walk the tree because neither planner's id counter survives to
		 * this hook (the Postgres planner counts in a standard_planner()
		 * local; ORCA counts inside its DXL translation context) and
		 * PlannedStmt carries no max-id field -- walking is the only
		 * planner-agnostic option, and cheap at this hook point.
		 *
		 * We take the max, not the node count: "count == next free id"
		 * assumes dense numbering, which ORCA's CIdGenerator and third-party
		 * planner_hooks do not promise.  max + 1 is correct under any
		 * assignment scheme.
		 */
		maxid = anser_max_plan_node_id(stmt->planTree);
		foreach(lc, stmt->subplans)
			maxid = Max(maxid, anser_max_plan_node_id((Plan *) lfirst(lc)));

		ctx.next_condition_id = 0;
		ctx.next_plan_node_id = maxid + 1;
		ctx.consumer_keys = NIL;

		anser_inject_walk(stmt->planTree, &ctx);
	}
}

/*
 * Largest plan_node_id in a plan subtree.  Recurses the spine plus CustomScan
 * children; sufficient for the supported (simple) plan shape.
 */
static int
anser_max_plan_node_id(Plan *plan)
{
	int			m;

	if (plan == NULL)
		return 0;

	m = plan->plan_node_id;
	m = Max(m, anser_max_plan_node_id(outerPlan(plan)));
	m = Max(m, anser_max_plan_node_id(innerPlan(plan)));
	if (IsA(plan, CustomScan))
	{
		ListCell   *lc;

		foreach(lc, ((CustomScan *) plan)->custom_plans)
			m = Max(m, anser_max_plan_node_id((Plan *) lfirst(lc)));
	}

	return m;
}

/*
 * Compute the bloom sizing to hand both the producer and consumer helpers, from
 * the estimated build cardinality.  Both call AnserBloomCreate (== bloom_create)
 * with the SAME (total_elems, max_payload) so they realize an identical filter;
 * we mirror bloom_create's own math here so `planned_bytes` (shown in EXPLAIN)
 * equals the realized bitset: target ~2 bytes/element, floor at 1 MB, cap at the
 * server payload budget, round DOWN to a power of two.
 */
static bool
anser_rf_size(double est_rows, int64 *total_elems, int64 *max_payload,
			  int64 *planned_bytes)
{
	int64		cap_bytes;
	int64		elems;
	int64		target_bytes;
	int64		realized;

	/* Largest bitset that fits the server payload cap, and our own ceiling. */
	cap_bytes = Min((int64) ANSER_RF_MAX_BYTES,
					(int64) gp_anser_max_info_size - ANSER_RF_HEADER_ROOM);
	if (cap_bytes < ANSER_RF_MIN_BYTES)
		return false;			/* cap too small to hold even a floor-sized filter */

	/*
	 * Clamp the element estimate so 2*elems never exceeds the cap; this also keeps
	 * total_elems within int range for custom_private (cap/2 <= 32M elements).
	 */
	elems = (est_rows > 0.0) ? (int64) est_rows : 1;
	if (elems > cap_bytes / 2)
		elems = cap_bytes / 2;
	if (elems < 1)
		elems = 1;

	target_bytes = Max((int64) ANSER_RF_MIN_BYTES, elems * 2);
	realized = ANSER_RF_MIN_BYTES;
	while ((realized << 1) <= target_bytes)
		realized <<= 1;

	*total_elems = elems;
	*max_payload = cap_bytes + ANSER_RF_HEADER_ROOM;
	*planned_bytes = realized;
	return true;
}

/*
 * Match a single-column equijoin over plain (by-value) Vars and return the
 * inner (build) and outer (probe) key attnos.  After set_plan_references the
 * operands are INNER_VAR / OUTER_VAR references into the join's child tlists.
 */
static bool
anser_hashjoin_keys(HashJoin *hj, AttrNumber *inner_attno, AttrNumber *outer_attno)
{
	OpExpr	   *op;
	Node	   *l;
	Node	   *r;
	Var		   *outer_var;
	Var		   *inner_var;

	if (list_length(hj->hashclauses) != 1)
		return false;
	op = (OpExpr *) linitial(hj->hashclauses);
	if (!IsA(op, OpExpr) || list_length(op->args) != 2)
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
	 * Producer and consumer hash the raw Datum bytes, which is only correct
	 * when SQL equality coincides with bitwise Datum equality of the key.
	 * That requires the SAME by-value type on both sides: cross-type equijoins
	 * (e.g. float4 = float8, date = timestamp) hash different bit patterns for
	 * equal values, and floats are excluded even same-typed because -0.0 and
	 * 0.0 compare equal but are not bitwise equal.  Anything looser could
	 * prune rows that actually join.
	 */
	if (inner_var->vartype != outer_var->vartype)
		return false;
	if (!get_typbyval(inner_var->vartype))
		return false;
	if (inner_var->vartype == FLOAT4OID || inner_var->vartype == FLOAT8OID)
		return false;

	*inner_attno = inner_var->varattno;
	*outer_attno = outer_var->varattno;
	return true;
}

/*
 * Follow the build side down from the Hash to the base SeqScan, mapping the key
 * attno through each passthrough targetlist.  Wrapping the base scan (rather than
 * an intermediate Motion) keeps the injected CustomScan's custom_scan_tlist made
 * of base-relation Vars, which (a) deparses cleanly in EXPLAIN and (b) is the
 * proven-safe "leaf child" case for MPP slice/gang setup.  Only plain single-
 * child passthroughs (Hash, Motion) with Var targetlist entries are supported.
 * On success *parent_out is the node whose outerPlan is the base scan.
 */
static bool
anser_resolve_build_scan(Plan *hash, AttrNumber inner_attno, Plan **parent_out,
						 Plan **scan_out, AttrNumber *attno_out)
{
	Plan	   *node = hash;
	AttrNumber	attno = inner_attno;

	for (;;)
	{
		TargetEntry *tle;
		Var		   *var;
		Plan	   *child;

		if (node == NULL ||
			attno < 1 || attno > list_length(node->targetlist))
			return false;

		tle = (TargetEntry *) list_nth(node->targetlist, attno - 1);
		if (tle == NULL || !IsA(tle->expr, Var))
			return false;
		var = (Var *) tle->expr;
		if (var->varno != OUTER_VAR)	/* single-child passthrough only */
			return false;

		child = outerPlan(node);
		if (child == NULL)
			return false;

		if (IsA(child, SeqScan))
		{
			*parent_out = node;
			*scan_out = child;
			*attno_out = var->varattno;
			return true;
		}
		if (!IsA(child, Hash) && !IsA(child, Motion))
			return false;

		node = child;
		attno = var->varattno;
	}
}

/*
 * If this HashJoin is the supported shape, inject a producer above the build
 * base scan and a consumer above the probe scan.
 */
static void
anser_try_inject(HashJoin *hj, AnserInjectCtx *ctx)
{
	Plan	   *hash = innerPlan(hj);	/* build side */
	Plan	   *probe = outerPlan(hj);	/* probe side */
	Plan	   *build_parent;
	Plan	   *build_scan;
	AttrNumber	inner_attno;
	AttrNumber	outer_attno;
	AttrNumber	build_attno;
	int64		total_elems;
	int64		max_payload;
	int64		planned_bytes;
	uint32		condition_id;
	char		condition_key[ANSER_CONDITION_KEY_SIZE];
	CustomScan *producer;
	CustomScan *consumer;
	ListCell   *lc;

	if (hj->join.jointype != JOIN_INNER && hj->join.jointype != JOIN_RIGHT)
		return;
	if (hash == NULL || !IsA(hash, Hash))
		return;
	if (probe == NULL || !IsA(probe, SeqScan))
		return;

	if (!anser_hashjoin_keys(hj, &inner_attno, &outer_attno))
		return;
	if (!anser_resolve_build_scan(hash, inner_attno, &build_parent, &build_scan,
								  &build_attno))
		return;
	if (!anser_rf_size(hash->plan_rows, &total_elems, &max_payload, &planned_bytes))
		return;

	condition_id = ctx->next_condition_id++;
	snprintf(condition_key, sizeof(condition_key), "anser_rf_%u", condition_id);

	/*
	 * One consumer per channel.  The consumer wait table budgets exactly
	 * anser.max_consumers_per_channel slots per channel, sized for one
	 * consumer instance per segment (nseg).  A second consumer plan node on the
	 * same channel would need 2*nseg slots and could exhaust that budget, so we
	 * never inject one -- skip the whole join and fail open instead.
	 *
	 * Minting a unique condition_id per injection makes this hold by
	 * construction, so the check never fires.  It stays as a guard against
	 * channel-key collisions if the key derivation ever changes (e.g. keys
	 * derived from the build's semantic identity, where two joins could share
	 * one channel).
	 */
	foreach(lc, ctx->consumer_keys)
	{
		if (strcmp((const char *) lfirst(lc), condition_key) == 0)
			return;
	}

	/* Producer wraps the build base scan; keyed by the mapped build attno. */
	producer = AnserBuildBloomProducerScan(build_scan, build_attno, condition_id,
										   condition_key, total_elems, max_payload,
										   planned_bytes);
	producer->scan.plan.plan_node_id = ctx->next_plan_node_id++;
	outerPlan(build_parent) = (Plan *) producer;

	/* Consumer wraps the probe scan; keyed by the outer (probe) attno. */
	consumer = AnserBuildBloomConsumerScan(probe, outer_attno, condition_id,
										   condition_key, total_elems, max_payload,
										   planned_bytes);
	consumer->scan.plan.plan_node_id = ctx->next_plan_node_id++;
	outerPlan(hj) = (Plan *) consumer;

	/* Record the channel so no later join can add a second consumer on it. */
	ctx->consumer_keys = lappend(ctx->consumer_keys, pstrdup(condition_key));
}

/*
 * Manual in-place traversal (recurse spine + custom_plans).  We do not use
 * plan_tree_mutator, whose CustomScan arm does not descend into custom_plans.
 */
static void
anser_inject_walk(Plan *plan, AnserInjectCtx *ctx)
{
	if (plan == NULL)
		return;

	if (IsA(plan, HashJoin))
		anser_try_inject((HashJoin *) plan, ctx);

	anser_inject_walk(outerPlan(plan), ctx);
	anser_inject_walk(innerPlan(plan), ctx);
	if (IsA(plan, CustomScan))
	{
		ListCell   *lc;

		foreach(lc, ((CustomScan *) plan)->custom_plans)
			anser_inject_walk((Plan *) lfirst(lc), ctx);
	}
}
