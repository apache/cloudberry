/*-------------------------------------------------------------------------
 *
 * joinpartprune.c
 *	  Transforms to produce plans that achieve dynamic partition elimination.
 *
 * This file contains GPDB specific code, to perform partition pruning
 * based on tuples seen on the "other" side of a join. Join pruning is
 * performed in co-operation between an Append node and a PartitionSelector
 * node. If you have a join between a partitioned table and something else:
 *
 * Hash Join
 *  Hash Join Cond: partitioned_table.id = othertable.id
 *   -> Append
 *      -> Seq Scan partition1
 *      -> Seq Scan partition2
 *      -> Seq Scan partition3
 *   -> Hash
 *      -> SeqScan othertable
 *         Filter: <other conditions>
 *
 * it would be good to skip those partitions that cannot contain any
 * tuples coming from the other side of the join. To do that, we
 * transform the plan into this:
 *
 * Join
 *  Join Cond: partitioned_table.id = othertable.id
 *   -> Append
 *       Partition Selectors: $0
 *            -> Seq Scan partition1
 *            -> Seq Scan partition2
 *            -> Seq Scan partition3
 *   -> Hash
 *      -> PartitionSelector (selector id: $0)
 *         Filter: othertable.id
 *         -> SeqScan othertable
 *            Filter: <other conditions>
 *
 * While the executor builds the Hash table, the Partition Selector
 * computes for each row which partitions in partitioned_table can contain
 * tuples matching the join condition. After we have built the hash
 * table, and start to evaluate the outer side of the join, we can
 * skip those partitions that we didn't see any matching inner tuples
 * for. The Partition Selector constructs a list of surviving partitions,
 * and the Append node skips ones that didn't survive.
 *
 * Planning these join pruning plans happens in the create_plan() stage:
 *
 * 1. Whenever a join is being processed, create_*join_plan() calls
 *    the push_partition_selector_candidate_for_join() function.
 *    push_partition_selector_candidate_for_join() decides if it's possible
 *    to put a partition selector to the inner side of the join, and if so,
 *    it records information about the join in a stack, in
 *    root->partition_selector_candidates. (In this phase, we don't yet
 *    determine if a Partition Selector would actually be useful, only
 *    if it would be valid to place one at this join.)
 *
 * 2. When create_append_plan() is called to create an Append plan for
 *    a partitioned table on the outer side of the join, create_append_plan()
 *    calls make_partition_join_pruneinfos() to check if any of the partitions
 *    could be pruned based on any of the joins recorded in the
 *    root->partition_selector_candidates stack. If so, it builds the pruning
 *    steps into a PartitionPruneInfo struct, and adds it to the stack entry.
 *    At this point, we have decided that a Partition Selector will be used at
 *    the join.
 *
 * 3. After finishing the outer side of the join, the create_*join_plan()
 *    calls pop_and_inject_partition_selectors(). If we decided to use
 *    any Partition Selectors at this join, the Partition Selectors are added
 *    to the inner side of the join.
 *
 *
 * Note: All this happens in the create_plan() stage,  after we have already
 * constructed the paths and decided the plan shape. To let the possibility
 * of pruning affect the chosen plan shape, final_cost_hashjoin() estimates
 * the effect of pruning with cost_join_partition_pruning(), which applies the
 * same checks that are made here at create_plan() time.
 *
 * Portions Copyright (c) 2011-2013, EMC Corporation
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/joinpartprune.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/joinpartprune.h"
#include "optimizer/optimizer.h"
#include "optimizer/paramassign.h"
#include "optimizer/planmain.h"
#include "optimizer/pathnode.h"
#include "partitioning/partprune.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "cdb/cdbvars.h"

static Path *create_partition_selector_path(PlannerInfo *root,
											Path *subpath,
											int paramid,
											PartitionPruneInfo *pruneinfo);

/*
 * Check if a Partition Selector could be placed at this join.
 *
 * This doesn't check if it would actually be useful here; that decision
 * is made in create_append_plan(). This merely checks if this is the
 * right kind of join.
 */
void
push_partition_selector_candidate_for_join(PlannerInfo *root, JoinPath *join_path)
{
	Path	   *innerpath = join_path->innerjoinpath;
	Relids		inner_relids = innerpath->parent->relids;
	bool		good_type = false;
	PartitionSelectorCandidateInfo *candidate;

	/* SINGLENODE_FIXME: enable partition selector in the future? */
	if (Gp_role != GP_ROLE_DISPATCH || !gp_dynamic_partition_pruning)
	{
		root->partition_selector_candidates = lcons(NULL, root->partition_selector_candidates);
		return;
	}

	if (!(join_path->jointype == JOIN_INNER ||
		  join_path->jointype == JOIN_RIGHT ||
		  join_path->jointype == JOIN_SEMI))
	{
		root->partition_selector_candidates = lcons(NULL, root->partition_selector_candidates);
		return;
	}

	/*
	 * For the partition selector in the inner side of the join to have any
	 * effect on the scans in the outer side, the inner side must be evaluated
	 * first. For a hash join, that is always true.
	 */
	if (join_path->path.pathtype == T_HashJoin)
	{
		good_type = true;
	}
	/*
	 * If we're doing a merge join, and will sort the inner side, then the sort
	 * will process all the inner rows before scanning the outer side. The Sort
	 * node will later be placed on top of the PartitionSelector we create here.
	 */
	else if (join_path->path.pathtype == T_MergeJoin && ((MergePath *) join_path)->innersortkeys)
	{
		good_type = true;
	}
	/*
	 * For a nested loop join, if there happens to be a Material node on the inner
	 * side, then we can place the Partition Selector below it. Even though a nested
	 * loop join will rescan the inner side, the Material or Sort will be executed
	 * all in one go.
	 *
	 * We could add a Material node, if there isn't one already, but a Material
	 * node is expensive. It would almost certainly make this plan worse than
	 * something else that the planner considered and rejected.
	 */
	else if (join_path->path.pathtype == T_NestLoop && !path_contains_inner_index(innerpath))
	{
		if (IsA(innerpath, MaterialPath))
			good_type = true;
		else
			good_type = false;
	}
	else if (join_path->path.pathtype == T_MergeJoin)
	{
		if (IsA(innerpath, MaterialPath))
			good_type = true;
		else if (((MergePath *) join_path)->innersortkeys)
			good_type = true;
		else
			good_type = false;
	}
	else
		good_type = false;

	if (!good_type)
	{
		root->partition_selector_candidates = lcons(NULL, root->partition_selector_candidates);
		return;
	}

	/* OK! */
	candidate = palloc(sizeof(PartitionSelectorCandidateInfo));

	candidate->joinrestrictinfo = join_path->joinrestrictinfo;
	candidate->inner_relids = inner_relids;
	candidate->slice = root->curSlice;
	candidate->selectors = NIL;

	root->partition_selector_candidates = lcons(candidate, root->partition_selector_candidates);
	return;
}

/*
 * Called after processing the outer side of a join. If the outer side
 * contained any Append nodes that want to do join pruning at this join,
 * adds Partition Selector nodes to the inner path, and returns 'true'.
 *
 * NB: This modifies the original Path. That's ugly, but this is only
 * used in the create_plan() stage where we've already decided the plan.
 * If we wanted to do this earlier while we could still affect the
 * plan shape, we shouldn't do that.
 */
bool
pop_and_inject_partition_selectors(PlannerInfo *root, JoinPath *jpath)
{
	PartitionSelectorCandidateInfo *candidate;
	MaterialPath *origmat = NULL;
	Path	   *subpath;
	ListCell   *lc;

	candidate = linitial(root->partition_selector_candidates);

	root->partition_selector_candidates =
		list_delete_first(root->partition_selector_candidates);

	if (!candidate || !candidate->selectors)
		return false;

	subpath = jpath->innerjoinpath;

	/*
	 * If the inner side contains a Material, then we put the Partition
	 * Selector under the Material node. The Material shields us from
	 * rescanning, and ensures that the inner side is scanned fully before
	 * the outer side.
	 */
	if (IsA(subpath, MaterialPath))
	{
		origmat = (MaterialPath *) subpath;
		subpath = origmat->subpath;
	}

	/* inject paths for the Partition Selectors to inner path */
	foreach (lc, candidate->selectors)
	{
		PartitionSelectorInfo *psinfo = (PartitionSelectorInfo *) lfirst(lc);

		subpath = create_partition_selector_path(root,
												 subpath,
												 psinfo->paramid,
												 psinfo->part_prune_info);
	}

	if (origmat)
	{
		origmat->subpath = subpath;
		subpath = &origmat->path;

		/*
		 * The inner side, with the Partition Selector, must be fully executed
		 * before the outer side.
		 */
		origmat->cdb_strict = true;
	}

	jpath->innerjoinpath = subpath;

	return true;
}

/* create a PartitionSelectorPath, for the inner side of a join. */
static Path *
create_partition_selector_path(PlannerInfo *root,
							   Path *subpath,
							   int paramid,
							   PartitionPruneInfo *pruneinfo)
{
	PartitionSelectorPath *pathnode = makeNode(PartitionSelectorPath);

	pathnode->path.pathtype = T_PartitionSelector;
	pathnode->path.parent = subpath->parent;

	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->path.locus = subpath->locus;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.motionHazard = subpath->motionHazard;
	pathnode->path.barrierHazard = subpath->barrierHazard;
	pathnode->path.rescannable = subpath->rescannable;
	pathnode->path.sameslice_relids = subpath->sameslice_relids;

	pathnode->subpath = subpath;
	pathnode->paramid = paramid;
	pathnode->part_prune_info = pruneinfo;

	return (Path *) pathnode;
}

/*
 * Create a PartitionSelector plan from a Path.
 *
 * This would logically belong in createplan.c, but let's keep everything
 * related to Partition Selectors in this file.
 */
Plan *
create_partition_selector_plan(PlannerInfo *root, PartitionSelectorPath *best_path)
{
	PartitionSelector *ps;
	Plan	   *subplan;
	PartitionPruneInfo *part_prune_info = best_path->part_prune_info;

	Assert(part_prune_info);

	subplan = create_plan_recurse(root, best_path->subpath, 0);

	ps = makeNode(PartitionSelector);
	ps->plan.targetlist = subplan->targetlist;
	ps->plan.qual = NIL;
	ps->plan.lefttree = subplan;
	ps->plan.righttree = NULL;

	ps->plan.startup_cost = subplan->startup_cost;
	ps->plan.total_cost = subplan->total_cost;
	ps->plan.plan_rows = subplan->plan_rows;
	ps->plan.plan_width = subplan->plan_width;

	ps->part_prune_info = part_prune_info;
	ps->paramid = best_path->paramid;

	return (Plan *) ps;
}

/*
 * Gather information needed to do join pruning.
 *
 * This is called from create_append_plan(), but the pruning steps are
 * actually put into the Partition Selector node on the other side of
 * the join.
 *
 * Returns list of special executor Param IDs that will contain
 * the results of the Partition Selectors at runtime.
 */
List *
make_partition_join_pruneinfos(PlannerInfo *root, RelOptInfo *parentrel,
							   List *subpaths)
{
	PartitionPruneInfo *part_prune_info;
	List	   *result = NIL;
	ListCell   *lc;

	foreach (lc, root->partition_selector_candidates)
	{
		PartitionSelectorCandidateInfo *candidate =
			(PartitionSelectorCandidateInfo *) lfirst(lc);

		if (!candidate)
			continue;

		/*
		 * Does the outer side contain this dynamic scan? And it must be in
		 * the same slice as the join! XXX
		 */
		if (candidate->slice != root->curSlice)
		{
			/*
			 * There's a Motion between this Partition Selector candidate and
			 * this Append. That won't work, the parameter containing the
			 * partition pruning results cannot be passed through a Motion.
			 * No need to look further, none of the nodes above this candidate
			 * can be in the same slice either.
			 */
			break;
		}

		part_prune_info =
			make_partition_pruneinfo_ext(root, parentrel,
										 subpaths,
										 candidate->joinrestrictinfo,
										 candidate->inner_relids);

		if (part_prune_info)
		{
			/*
			 * Stash the prune steps for the Partition Selector that
			 * will be put on the other side of the join.
			 */
			PartitionSelectorInfo *psinfo;

			psinfo = palloc(sizeof(PartitionSelectorInfo));
			psinfo->part_prune_info = part_prune_info;
			psinfo->paramid = assign_special_exec_param(root);

			candidate->selectors = lappend(candidate->selectors, psinfo);

			result = lappend_int(result, psinfo->paramid);
		}
	}

	return result;
}

/*
 * The fraction of the estimated saving from join partition pruning that
 * cost_join_partition_pruning() subtracts from a join's cost.
 *
 * The planner's row estimates for long join chains can be off by orders of
 * magnitude (e.g. 5 rows estimated vs 175k actual in TPC-DS query 64), and
 * the full saving readily tips the choice towards such a misestimated plan.
 */
#define PARTITION_PRUNING_COSTING_FACTOR	0.25

static List *join_prune_clauses_for_append(PlannerInfo *root, JoinPath *jpath,
										   Path *apath);

/*
 * Is this a join at which create_plan() could place Partition Selectors?
 * Same conditions as push_partition_selector_candidate_for_join(), but we
 * only handle hash joins for now.
 */
static bool
is_join_prune_candidate(JoinPath *jpath)
{
	return jpath->path.pathtype == T_HashJoin &&
		(jpath->jointype == JOIN_INNER ||
		 jpath->jointype == JOIN_RIGHT ||
		 jpath->jointype == JOIN_SEMI);
}

/*
 * Collect the Append and MergeAppend paths under 'path' that a Partition
 * Selector at a join above 'path' could prune.
 *
 * This mirrors what create_plan() does later: create_append_plan() looks for
 * Partition Selector candidates among all the joins it's on the outer side of,
 * within the same slice. So we descend into both sides of joins, and through
 * nodes that don't start a new slice or a new planner level, and stop at
 * Motions.
 *
 * Appends that a join further down can already prune are left out: their
 * pruning has already been accounted for in that join's cost, and counting it
 * again would make plans that put more joins below the pruning join look
 * cheaper than they are.
 */
static void
collect_join_prunable_appends(PlannerInfo *root, Path *path, List **appends)
{
	check_stack_depth();

	if (path == NULL)
		return;

	switch (nodeTag(path))
	{
		case T_AppendPath:
		case T_MergeAppendPath:
			*appends = lappend(*appends, path);
			break;

		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			{
				JoinPath   *jpath = (JoinPath *) path;
				List	   *outer_appends = NIL;
				ListCell   *lc;

				collect_join_prunable_appends(root, jpath->outerjoinpath, &outer_appends);
				foreach(lc, outer_appends)
				{
					Path	   *apath = (Path *) lfirst(lc);

					if (is_join_prune_candidate(jpath) &&
						join_prune_clauses_for_append(root, jpath, apath) != NIL)
						continue;	/* already accounted for at this join */
					*appends = lappend(*appends, apath);
				}
				collect_join_prunable_appends(root, jpath->innerjoinpath, appends);
			}
			break;

		case T_ProjectionPath:
			collect_join_prunable_appends(root, ((ProjectionPath *) path)->subpath, appends);
			break;
		case T_RuntimeFilterPath:
			collect_join_prunable_appends(root, ((RuntimeFilterPath *) path)->subpath, appends);
			break;
		case T_MaterialPath:
			collect_join_prunable_appends(root, ((MaterialPath *) path)->subpath, appends);
			break;
		case T_SortPath:
		case T_IncrementalSortPath:
			collect_join_prunable_appends(root, ((SortPath *) path)->subpath, appends);
			break;
		case T_UniquePath:
			collect_join_prunable_appends(root, ((UniquePath *) path)->subpath, appends);
			break;

		default:
			/*
			 * CdbMotionPath: the pruning result can't be passed through a
			 * Motion. Scans, subquery scans, aggregates etc.: we don't
			 * bother.
			 */
			break;
	}
}

/*
 * Return the join clauses of 'jpath' that create_plan() could build pruning
 * steps from, to prune the Append 'apath' on its outer side. NIL if none.
 *
 * Each clause is checked separately: only the clauses on the partition key
 * determine how many partitions are skipped.
 */
static List *
join_prune_clauses_for_append(PlannerInfo *root, JoinPath *jpath, Path *apath)
{
	RelOptInfo *arel = apath->parent;
	Relids		inner_relids = jpath->innerjoinpath->parent->relids;
	List	   *subpaths;
	List	   *clauses = NIL;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;
	ListCell   *lc;

	/* create_append_plan() doesn't do join pruning for these */
	if (apath->param_info)
		return NIL;

	if (IsA(apath, AppendPath))
		subpaths = ((AppendPath *) apath)->subpaths;
	else
		subpaths = ((MergeAppendPath *) apath)->subpaths;
	if (list_length(subpaths) < 2)
		return NIL;

	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "partition pruning costing",
								   ALLOCSET_SMALL_SIZES);
	foreach(lc, jpath->joinrestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		prunable;

		if (!bms_overlap(rinfo->clause_relids, arel->relids) ||
			!bms_overlap(rinfo->clause_relids, inner_relids) ||
			!bms_is_subset(rinfo->clause_relids,
						   bms_union(arel->relids, inner_relids)))
			continue;

		/* Do the same check create_plan() does, and throw away the result */
		oldcxt = MemoryContextSwitchTo(tmpcxt);
		prunable = (make_partition_pruneinfo_ext(root, arel, subpaths,
												 list_make1(rinfo),
												 inner_relids) != NULL);
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(tmpcxt);

		if (prunable)
			clauses = lappend(clauses, rinfo);
	}
	MemoryContextDelete(tmpcxt);

	return clauses;
}

/*
 * Does joining on 'clauses' leave the number of outer rows unchanged, i.e. is
 * the inner side a single table on which the join key is unique?
 *
 * The saving estimate in cost_join_partition_pruning() assumes that. If the
 * inner side is a join, or the key isn't unique, the pruning join can multiply
 * the rows that survive pruning, and the planner tends to underestimate by
 * how much (e.g. in TPC-DS query 72, inventory is pruned by a join of two
 * date_dim rows per day of the same week: the pruning join produces 40 times
 * more rows than estimated). The extra cost of that then easily outweighs
 * the saving, so we don't count on pruning at such joins.
 */
static bool
inner_join_key_is_unique(PlannerInfo *root, JoinPath *jpath, List *clauses)
{
	RelOptInfo *inner_rel = jpath->innerjoinpath->parent;
	ListCell   *lc;

	if (inner_rel->reloptkind != RELOPT_BASEREL ||
		inner_rel->rtekind != RTE_RELATION)
		return false;

	foreach(lc, clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr	   *opexpr;
		Node	   *inner_arg;
		VariableStatData vardata;
		double		ndistinct;
		double		tuples;
		bool		isdefault;

		if (!is_opclause(rinfo->clause) ||
			list_length(((OpExpr *) rinfo->clause)->args) != 2)
			return false;
		opexpr = (OpExpr *) rinfo->clause;

		if (bms_is_subset(rinfo->left_relids, inner_rel->relids))
			inner_arg = linitial(opexpr->args);
		else if (bms_is_subset(rinfo->right_relids, inner_rel->relids))
			inner_arg = lsecond(opexpr->args);
		else
			return false;

		examine_variable(root, inner_arg, 0, &vardata);
		ndistinct = get_variable_numdistinct(&vardata, &isdefault);
		tuples = vardata.rel ? vardata.rel->tuples : 0;
		ReleaseVariableStats(vardata);

		if (isdefault || tuples <= 0 || ndistinct < 0.9 * tuples)
			return false;
	}

	return true;
}

/*
 * Estimate how much cheaper the outer side of a hash join gets, if a
 * Partition Selector on the inner side lets Appends on the outer side skip
 * partitions.
 *
 * Partition Selectors are only added in the create_plan() stage, after the
 * plan shape has been chosen (see the comment at the top of this file). If we
 * don't account for them while comparing paths, a join order that would
 * allow pruning (e.g. joining a large fact table to a filtered date dimension
 * first) is costed as if it scanned all partitions, and loses to join orders
 * that cannot prune at all.
 *
 * The estimate: the fraction of outer rows that have a match on the inner
 * side is selectivity(partition key clauses) * inner rows, assuming the inner
 * join key is unique (if it isn't, we overestimate, which is the safe side).
 * Assuming further that the partition key is correlated with partitions, as
 * it is with range partitioning, those rows live in about that fraction of
 * the partitions, plus one for the partition boundaries.
 *
 * Returns the cost that can be subtracted from the join path's run cost.
 */
Cost
cost_join_partition_pruning(PlannerInfo *root, JoinPath *jpath)
{
	Path	   *inner_path = jpath->innerjoinpath;
	Relids		inner_relids = inner_path->parent->relids;
	List	   *appends = NIL;
	Cost		saving = 0;
	ListCell   *lc;

	if (!gp_dynamic_partition_pruning ||
		Gp_role != GP_ROLE_DISPATCH)
		return 0;

	if (!is_join_prune_candidate(jpath))
		return 0;

	collect_join_prunable_appends(root, jpath->outerjoinpath, &appends);

	foreach(lc, appends)
	{
		Path	   *apath = (Path *) lfirst(lc);
		RelOptInfo *arel = apath->parent;
		List	   *clauses;
		int			nparts;
		ListCell   *lc2;
		SpecialJoinInfo sjinfo;
		Selectivity selec = 1.0;
		double		match_frac;
		double		scan_frac;
		Cost		run_cost;

		clauses = join_prune_clauses_for_append(root, jpath, apath);
		if (clauses == NIL)
			continue;

		if (!inner_join_key_is_unique(root, jpath, clauses))
			continue;

		if (IsA(apath, AppendPath))
			nparts = list_length(((AppendPath *) apath)->subpaths);
		else
			nparts = list_length(((MergeAppendPath *) apath)->subpaths);

		/* Fraction of the Append's rows that will find a match */
		memset(&sjinfo, 0, sizeof(sjinfo));
		sjinfo.type = T_SpecialJoinInfo;
		sjinfo.min_lefthand = arel->relids;
		sjinfo.min_righthand = inner_relids;
		sjinfo.syn_lefthand = arel->relids;
		sjinfo.syn_righthand = inner_relids;
		sjinfo.jointype = JOIN_INNER;
		foreach(lc2, clauses)
			selec *= clause_selectivity(root, (Node *) lfirst(lc2), 0,
										JOIN_INNER, &sjinfo, false);
		match_frac = selec * inner_path->parent->rows;
		match_frac = Min(Max(match_frac, 0.0), 1.0);

		/* ... and the fraction of partitions those are in */
		scan_frac = (ceil(match_frac * nparts) + 1) / nparts;
		scan_frac = Min(scan_frac, 1.0);

		run_cost = apath->total_cost - apath->startup_cost;
		saving += (1.0 - scan_frac) * run_cost;

		elog(DEBUG1, "partition pruning costing: rel %s, %d partitions, "
			 "selectivity %g, inner rows %.0f, match fraction %g, "
			 "scan fraction %g, saving %.2f of %.2f",
			 bmsToString(arel->relids), nparts, selec,
			 inner_path->parent->rows, match_frac, scan_frac,
			 (1.0 - scan_frac) * run_cost, run_cost);
	}

	return saving * PARTITION_PRUNING_COSTING_FACTOR;
}
