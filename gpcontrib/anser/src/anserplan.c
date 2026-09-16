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
#include "anserfilter.h"
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
	PlannedStmt *stmt;			/* for slices[]: see anser_slice_producers */
	int			slice_index;	/* slice the subtree being walked runs in */
	uint32		next_condition_id;
	int			next_plan_node_id;
	List	   *consumer_keys;	/* condition_keys already given a consumer node;
								 * enforces one consumer per channel (see
								 * anser_try_inject) */
} AnserInjectCtx;

static int	anser_max_plan_node_id(Plan *plan);
static int	anser_slice_producers(AnserInjectCtx *ctx, int slice_index);
static bool anser_hashjoin_keys(HashJoin *hj, AttrNumber *inner_attno,
								AttrNumber *outer_attno);
static bool anser_resolve_build_scan(Plan *hash, AttrNumber inner_attno,
									 int slice_index, Plan **parent_out,
									 Plan **scan_out, AttrNumber *attno_out,
									 int *slice_out);
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

		ctx.stmt = stmt;
		ctx.slice_index = 0;	/* the root runs in the coordinator's slice */
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
bool
AnserRuntimeFilterSize(double est_rows, int64 *total_elems, int64 *max_payload,
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
	 * The element count stays the honest estimate; only the *size* is clamped.
	 *
	 * It is tempting to clamp elems instead -- it is the number that drives the
	 * size, so bounding it bounds the bitset in one step.  That is a trap.
	 * total_elems is also what optimal_k() sizes the hash count from, so
	 * understating it understates k: a 128M-row build side reported as 33.5M
	 * gets k=10, when the optimum for 128M keys in a 512 Mbit filter is k=3.
	 * Since the producer inserts all 128M keys regardless of what we wrote
	 * down, the result is a filter with a 38% false positive rate where 15% was
	 * available.  Lowering the declared count never makes a filter fit; it only
	 * relabels it, and then misconfigures it.
	 *
	 * INT32 is the real bound on elems: custom_private carries it as an Integer
	 * node.  The density check below refuses anything remotely near that.
	 */
	elems = (est_rows > 0.0) ? (int64) est_rows : 1;
	if (elems < 1)
		elems = 1;
	if (elems > PG_INT32_MAX)
		elems = PG_INT32_MAX;

	target_bytes = Min(cap_bytes, Max((int64) ANSER_RF_MIN_BYTES, elems * 2));
	realized = ANSER_RF_MIN_BYTES;
	while ((realized << 1) <= target_bytes)
		realized <<= 1;

	/*
	 * Refuse a join whose build side cannot be usefully summarized within the
	 * payload cap.
	 *
	 * Refusing here is the cheapest possible outcome: no producer, no consumer,
	 * no channel, and nothing for a consumer to wait on and time out against.
	 * The runtime checks in anserfilter.c exist for when this estimate turns
	 * out to be wrong, not instead of this one.
	 */
	if (realized * (double) BITS_PER_BYTE / (double) elems
		< ANSER_BLOOM_MIN_BITS_PER_KEY)
		return false;

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
 * Follow the build side down from the Hash to the base SeqScan the key column
 * comes from, mapping the key attno through each targetlist on the way.
 *
 * Wrapping the base scan (rather than an intermediate node) keeps the injected
 * CustomScan's custom_scan_tlist made of base-relation Vars, which (a) deparses
 * cleanly in EXPLAIN and (b) is the proven-safe "leaf child" case for MPP
 * slice/gang setup.  (a) is not a preference: after set_plan_references an
 * intermediate node's targetlist holds OUTER_VAR/INNER_VAR Vars, and a
 * CustomScan keeps its child in custom_plans rather than in lefttree, so
 * copying such a targetlist into custom_scan_tlist leaves EXPLAIN resolving an
 * OUTER_VAR against a node with no outer plan -- get_variable() raises "bogus
 * varno" (ruleutils.c) and the plan cannot be printed at all.
 *
 * The descent goes through HashJoins as well as the single-child passthroughs
 * (Hash, Motion), which is what lets a chain of joins be filtered: the build
 * side of the third join is the result of the first two, and the key column
 * still traces back to one base relation through it.  The filter then holds
 * that relation's whole key column rather than the subset the earlier joins
 * would have left -- a superset, so no row that could join is ever rejected,
 * but less selective than filtering on the intermediate result itself.  See
 * docs/local-fold.md for what the exact version would need.
 *
 * On success *parent_out is the node whose outerPlan is the base scan.  A
 * SeqScan is only ever accepted there: the inner child of a HashJoin is a Hash,
 * so the scan is always reached through an outer link.
 */
static bool
anser_resolve_build_scan(Plan *hash, AttrNumber inner_attno, int slice_index,
						 Plan **parent_out, Plan **scan_out,
						 AttrNumber *attno_out, int *slice_out)
{
	Plan	   *node = hash;
	AttrNumber	attno = inner_attno;

	for (;;)
	{
		TargetEntry *tle;
		Var		   *var;
		Plan	   *child;
		bool		via_outer;

		if (node == NULL ||
			attno < 1 || attno > list_length(node->targetlist))
			return false;

		tle = (TargetEntry *) list_nth(node->targetlist, attno - 1);
		if (tle == NULL || !IsA(tle->expr, Var))
			return false;
		var = (Var *) tle->expr;

		/*
		 * Which child supplies this column.  A join is the only node here with
		 * two of them, and only a HashJoin is descended through -- an Append or
		 * a SetOp would make the column come from several relations at once,
		 * and then no single scan holds the keys the filter would claim.
		 */
		if (var->varno == OUTER_VAR)
		{
			child = outerPlan(node);
			via_outer = true;
		}
		else if (var->varno == INNER_VAR && IsA(node, HashJoin))
		{
			child = innerPlan(node);
			via_outer = false;
		}
		else
			return false;

		if (child == NULL)
			return false;

		if (via_outer && IsA(child, SeqScan))
		{
			*parent_out = node;
			*scan_out = child;
			*attno_out = var->varattno;
			*slice_out = slice_index;
			return true;
		}
		if (!IsA(child, Hash) && !IsA(child, Motion) && !IsA(child, HashJoin))
			return false;

		/*
		 * Descending through a Motion moves us into its sending slice, which
		 * is where the producer will end up -- and whose width decides how
		 * many parts the coordinator has to wait for.
		 */
		if (IsA(child, Motion))
			slice_index = ((Motion *) child)->motionID;

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
	int			build_slice;
	int			n_producers;
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
	if (!anser_resolve_build_scan(hash, inner_attno, ctx->slice_index,
								  &build_parent, &build_scan, &build_attno,
								  &build_slice))
		return;

	/*
	 * How many producers will there be?  One per process of the build scan's
	 * slice.  Declining when we cannot tell is the safe direction: a count that
	 * is too low makes the coordinator complete the channel early and ship a
	 * filter missing keys, which drops joinable rows.
	 */
	n_producers = anser_slice_producers(ctx, build_slice);
	if (n_producers <= 0)
		return;
	/*
	 * Sized from the scan the producer wraps, not from the Hash: those are the
	 * rows whose keys actually go into the filter.  They are the same thing
	 * when the build side is one scan under a Hash, and they are not when the
	 * descent came through a join -- there the Hash holds the joined result and
	 * the scan holds the whole key column, which is what gets inserted.
	 */
	if (!AnserRuntimeFilterSize(build_scan->plan_rows, &total_elems, &max_payload,
								&planned_bytes))
		return;

	condition_id = ctx->next_condition_id++;
	snprintf(condition_key, sizeof(condition_key), "anser_rf_%u", condition_id);

	/*
	 * One consumer per channel.  Two consumer nodes sharing a channel would
	 * both be served -- the coordinator pushes to every subscriber -- but they
	 * would also both count toward nothing: the channel's part count comes from
	 * the producers, so a second consumer only multiplies deliveries.  More to
	 * the point, the two would be indistinguishable in the debug trace and in
	 * any future per-channel accounting, so keep the invariant.
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
										   planned_bytes, n_producers);
	producer->scan.plan.plan_node_id = ctx->next_plan_node_id++;
	outerPlan(build_parent) = (Plan *) producer;

	/* Consumer wraps the probe scan; keyed by the outer (probe) attno. */
	consumer = AnserBuildBloomConsumerScan(probe, outer_attno, condition_id,
										   condition_key, total_elems, max_payload,
										   planned_bytes, n_producers);
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
	int			save_slice = ctx->slice_index;

	if (plan == NULL)
		return;

	/*
	 * A Motion is the boundary between slices, and everything below one runs
	 * in the sending slice.  cdbllize stamps that slice's index into motionID
	 * (cdbllize.c: "motion->motionID = sendSlice->sliceIndex") and then clears
	 * senderSliceInfo, so motionID is the only durable way to get here from a
	 * finished plan.
	 */
	if (IsA(plan, Motion))
		ctx->slice_index = ((Motion *) plan)->motionID;

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

	ctx->slice_index = save_slice;
}

/*
 * How many processes execute a slice -- which is how many producers will
 * publish a part, and therefore how many the coordinator must wait for.
 *
 * This mirrors FillSliceGangInfo (execUtils.c): a slice runs
 * numsegments * parallel_workers processes, with parallel_workers == 0 meaning
 * one.  Both optimizers are covered, and they do differ: for the same query the
 * Postgres planner may give a serial build slice (Broadcast Motion 2:8 over a
 * plain Seq Scan, so 2 producers) where GPORCA gives a parallel one (Broadcast
 * Motion 16:16 over a Parallel Seq Scan, so 16).  Reading it from the slice
 * means neither case needs special handling.
 */
static int
anser_slice_producers(AnserInjectCtx *ctx, int slice_index)
{
	PlanSlice  *slice;
	int			factor;

	if (ctx->stmt->slices == NULL ||
		slice_index < 0 || slice_index >= ctx->stmt->numSlices)
		return 0;				/* unknown: caller declines to inject */

	slice = &ctx->stmt->slices[slice_index];

	/* The coordinator's own slice is one process and has no gang. */
	if (slice->gangType == GANGTYPE_UNALLOCATED ||
		slice->gangType == GANGTYPE_ENTRYDB_READER)
		return 1;

	factor = slice->parallel_workers > 0 ? slice->parallel_workers : 1;
	if (slice->numsegments <= 0)
		return 0;

	return slice->numsegments * factor;
}
