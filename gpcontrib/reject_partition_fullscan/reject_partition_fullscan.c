/*-------------------------------------------------------------------------
 *
 * reject_partition_fullscan.c
 *		Extension to reject queries that scan all partitions of a
 *		partitioned table without effective partition pruning.
 *
 * This extension installs a planner_hook that wraps the standard planner
 * (including ORCA).  After the planner produces a PlannedStmt, the hook
 * walks the plan tree looking for partition scan nodes that indicate no
 * effective pruning occurred.
 *
 * Detection strategy (three complementary checks):
 *
 * 1) Nodes with PartitionPruneInfo (Planner Append with WHERE, or
 *    PartitionSelector in ORCA JOIN path): compare present_parts vs
 *    nparts.  Exempt nodes with initial/exec pruning steps (runtime
 *    pruning capable).
 *
 * 2) Planner Append/MergeAppend without PartitionPruneInfo (no WHERE,
 *    WHERE 1=1, WHERE on non-partition-key): the Planner does not
 *    generate PartitionPruneInfo when there are no useful pruning quals.
 *    We detect these by checking if apprelids references a partitioned
 *    table RTE (relkind='p', inh=true).
 *
 * 3) ORCA DynamicSeqScan (and similar Dynamic nodes): ORCA never sets
 *    part_prune_info on Dynamic scan nodes.  We detect full scans by
 *    comparing list_length(partOids) against the total partition count
 *    from catalog.  Nodes with join_prune_paramids are skipped (JOIN
 *    dynamic pruning).
 *
 * GUC parameters (registered via DefineCustomXxxVariable):
 *   reject_partition_fullscan   (bool, default true)
 *   partition_fullscan_threshold (int, default 0)
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "cdb/cdbllize.h"
#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

/* GUC variables */
static bool reject_fullscan_enabled = true;
static int	fullscan_threshold = 0;

/* Saved previous hook */
static planner_hook_type prev_planner_hook = NULL;

/* Forward declarations */
void		_PG_init(void);
void		_PG_fini(void);

static PlannedStmt *rpf_planner_hook(Query *parse,
									 const char *query_string,
									 int cursorOptions,
									 ParamListInfo boundParams,
									 OptimizerOptions *optimizer_options);
static void check_partition_fullscan(PlannedStmt *stmt);

/* ----------------------------------------------------------------
 * Utility: raise the rejection ERROR
 * ----------------------------------------------------------------
 */
static void
reject_fullscan(const char *nspname, const char *relname, int nparts)
{
	ereport(ERROR,
			(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
			 errmsg("partitioned table \"%s.%s\" full partition "
					"scan is not allowed, %d partitions would "
					"be scanned",
					nspname, relname, nparts),
			 errhint("Add a WHERE clause on the partition key "
					 "to enable partition pruning.")));
}

/* ----------------------------------------------------------------
 * Check 1: Nodes with PartitionPruneInfo
 *   Used by: Planner Append (with pruning quals), PartitionSelector
 * ----------------------------------------------------------------
 */
static PartitionPruneInfo *
get_part_prune_info(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_Append:
			return ((Append *) plan)->part_prune_info;
		case T_MergeAppend:
			return ((MergeAppend *) plan)->part_prune_info;
		case T_PartitionSelector:
			return ((PartitionSelector *) plan)->part_prune_info;
		case T_DynamicSeqScan:
			return ((DynamicSeqScan *) plan)->part_prune_info;
		case T_DynamicIndexScan:
			return ((DynamicIndexScan *) plan)->part_prune_info;
		case T_DynamicIndexOnlyScan:
			return ((DynamicIndexOnlyScan *) plan)->part_prune_info;
		case T_DynamicBitmapHeapScan:
			return ((DynamicBitmapHeapScan *) plan)->part_prune_info;
		case T_DynamicForeignScan:
			return ((DynamicForeignScan *) plan)->part_prune_info;
		default:
			return NULL;
	}
}

static void
check_ppi_fullscan(PartitionPruneInfo *ppi, List *rtable)
{
	ListCell   *lc1;

	foreach(lc1, ppi->prune_infos)
	{
		List	   *prune_info_list = (List *) lfirst(lc1);
		ListCell   *lc2;

		foreach(lc2, prune_info_list)
		{
			PartitionedRelPruneInfo *pinfo =
				(PartitionedRelPruneInfo *) lfirst(lc2);
			int			total = pinfo->nparts;
			int			present = bms_num_members(pinfo->present_parts);
			int			threshold = fullscan_threshold;
			bool		do_reject = false;

			if (total <= 1)
				continue;

			if (pinfo->initial_pruning_steps != NIL ||
				pinfo->exec_pruning_steps != NIL)
				continue;

			if (threshold == 0)
				do_reject = (present == total);
			else
				do_reject = (present > threshold);

			if (do_reject)
			{
				RangeTblEntry *rte = rt_fetch(pinfo->rtindex, rtable);
				char   *relname = get_rel_name(rte->relid);
				char   *nspname = get_namespace_name(
										get_rel_namespace(rte->relid));

				reject_fullscan(nspname, relname, present);
			}
		}
	}
}

/* ----------------------------------------------------------------
 * Check 2: Planner Append/MergeAppend without PartitionPruneInfo
 *   Covers: no WHERE, WHERE 1=1, WHERE on non-partition-key
 * ----------------------------------------------------------------
 */
static Index
find_partitioned_parent_rti(Bitmapset *apprelids, List *rtable)
{
	int			rti = -1;

	while ((rti = bms_next_member(apprelids, rti)) >= 0)
	{
		RangeTblEntry *rte = rt_fetch(rti, rtable);

		if (rte->rtekind == RTE_RELATION &&
			rte->inh &&
			rte->relkind == RELKIND_PARTITIONED_TABLE)
			return (Index) rti;
	}
	return 0;
}

static int
count_append_subplans(Plan *plan)
{
	if (IsA(plan, Append))
		return list_length(((Append *) plan)->appendplans);
	else if (IsA(plan, MergeAppend))
		return list_length(((MergeAppend *) plan)->mergeplans);
	return 0;
}

static void
check_append_no_pruneinfo(Plan *plan, List *rtable)
{
	Bitmapset  *apprelids = NULL;
	Index		parent_rti;
	int			nsubplans;
	int			threshold;

	if (IsA(plan, Append))
		apprelids = ((Append *) plan)->apprelids;
	else if (IsA(plan, MergeAppend))
		apprelids = ((MergeAppend *) plan)->apprelids;
	else
		return;

	if (apprelids == NULL)
		return;

	parent_rti = find_partitioned_parent_rti(apprelids, rtable);
	if (parent_rti == 0)
		return;

	nsubplans = count_append_subplans(plan);
	threshold = fullscan_threshold;

	if (nsubplans <= 1)
		return;

	if (threshold == 0 || nsubplans > threshold)
	{
		RangeTblEntry *rte = rt_fetch(parent_rti, rtable);
		char   *relname = get_rel_name(rte->relid);
		char   *nspname = get_namespace_name(
								get_rel_namespace(rte->relid));

		reject_fullscan(nspname, relname, nsubplans);
	}
}

/* ----------------------------------------------------------------
 * Check 3: ORCA DynamicSeqScan (and other Dynamic nodes)
 *   ORCA never sets part_prune_info on Dynamic scan nodes.
 *   We check partOids count vs total partition count.
 *   Nodes with join_prune_paramids are skipped (JOIN pruning).
 * ----------------------------------------------------------------
 */
static void
check_dynamic_scan_fullscan(Plan *plan, List *rtable)
{
	List	   *partOids = NIL;
	List	   *join_prune_paramids = NIL;
	Index		scanrelid = 0;
	int			nscanned;
	int			ntotal;
	int			threshold;
	RangeTblEntry *rte;
	List	   *children;

	switch (nodeTag(plan))
	{
		case T_DynamicSeqScan:
			partOids = ((DynamicSeqScan *) plan)->partOids;
			join_prune_paramids =
				((DynamicSeqScan *) plan)->join_prune_paramids;
			scanrelid = ((DynamicSeqScan *) plan)->seqscan.scanrelid;
			break;
		case T_DynamicIndexScan:
			partOids = ((DynamicIndexScan *) plan)->partOids;
			join_prune_paramids =
				((DynamicIndexScan *) plan)->join_prune_paramids;
			scanrelid = ((DynamicIndexScan *) plan)->indexscan.scan.scanrelid;
			break;
		case T_DynamicIndexOnlyScan:
			partOids = ((DynamicIndexOnlyScan *) plan)->partOids;
			join_prune_paramids =
				((DynamicIndexOnlyScan *) plan)->join_prune_paramids;
			scanrelid =
				((DynamicIndexOnlyScan *) plan)->indexscan.scan.scanrelid;
			break;
		case T_DynamicBitmapHeapScan:
			partOids = ((DynamicBitmapHeapScan *) plan)->partOids;
			join_prune_paramids =
				((DynamicBitmapHeapScan *) plan)->join_prune_paramids;
			scanrelid =
				((DynamicBitmapHeapScan *) plan)->bitmapheapscan.scan.scanrelid;
			break;
		case T_DynamicForeignScan:
			partOids = ((DynamicForeignScan *) plan)->partOids;
			join_prune_paramids =
				((DynamicForeignScan *) plan)->join_prune_paramids;
			scanrelid =
				((DynamicForeignScan *) plan)->foreignscan.scan.scanrelid;
			break;
		default:
			return;
	}

	/* Skip JOIN dynamic pruning -- runtime selection */
	if (join_prune_paramids != NIL)
		return;

	nscanned = list_length(partOids);
	if (nscanned <= 1)
		return;

	/* Verify this is a partitioned table */
	rte = rt_fetch(scanrelid, rtable);
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_PARTITIONED_TABLE)
		return;

	threshold = fullscan_threshold;

	if (threshold > 0)
	{
		/* Threshold mode: reject if scanned count > threshold */
		if (nscanned > threshold)
		{
			char   *relname = get_rel_name(rte->relid);
			char   *nspname = get_namespace_name(
									get_rel_namespace(rte->relid));

			reject_fullscan(nspname, relname, nscanned);
		}
		return;
	}

	/*
	 * threshold == 0: reject only true full scans (all partitions).
	 * Compare scanned count against total partition count from catalog.
	 * Use NoLock since the planner already holds a lock on this rel.
	 */
	children = find_inheritance_children(rte->relid, NoLock);
	ntotal = list_length(children);
	list_free(children);

	if (ntotal > 1 && nscanned >= ntotal)
	{
		char   *relname = get_rel_name(rte->relid);
		char   *nspname = get_namespace_name(
								get_rel_namespace(rte->relid));

		reject_fullscan(nspname, relname, nscanned);
	}
}

/* ----------------------------------------------------------------
 * Plan tree walker
 * ----------------------------------------------------------------
 */
typedef struct rpf_walker_context
{
	plan_tree_base_prefix base;
	List	   *rtable;
} rpf_walker_context;

static bool
rpf_plan_walker(Node *node, void *context)
{
	rpf_walker_context *ctx = (rpf_walker_context *) context;

	if (node == NULL)
		return false;

	if (is_plan_node(node))
	{
		Plan	   *plan = (Plan *) node;
		PartitionPruneInfo *ppi = get_part_prune_info(plan);

		if (ppi != NULL)
		{
			/* Check 1: node has PartitionPruneInfo */
			check_ppi_fullscan(ppi, ctx->rtable);
		}
		else if (IsA(node, Append) || IsA(node, MergeAppend))
		{
			/* Check 2: Planner Append without pruning info */
			check_append_no_pruneinfo(plan, ctx->rtable);
		}
		else if (IsA(node, DynamicSeqScan) ||
				 IsA(node, DynamicIndexScan) ||
				 IsA(node, DynamicIndexOnlyScan) ||
				 IsA(node, DynamicBitmapHeapScan) ||
				 IsA(node, DynamicForeignScan))
		{
			/* Check 3: ORCA Dynamic scan without pruning info */
			check_dynamic_scan_fullscan(plan, ctx->rtable);
		}
	}

	return plan_tree_walker(node, rpf_plan_walker, context, true);
}

/* ----------------------------------------------------------------
 * Entry point and planner hook
 * ----------------------------------------------------------------
 */
static void
check_partition_fullscan(PlannedStmt *stmt)
{
	rpf_walker_context ctx;

	if (!reject_fullscan_enabled || !enable_partition_pruning)
		return;

	if (stmt->planTree == NULL)
		return;

	exec_init_plan_tree_base(&ctx.base, stmt);
	ctx.rtable = stmt->rtable;

	rpf_plan_walker((Node *) stmt->planTree, &ctx);
}

static PlannedStmt *
rpf_planner_hook(Query *parse,
				 const char *query_string,
				 int cursorOptions,
				 ParamListInfo boundParams,
				 OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								  boundParams, optimizer_options);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								 boundParams, optimizer_options);

	if (result != NULL)
		check_partition_fullscan(result);

	return result;
}

void
_PG_init(void)
{
	DefineCustomBoolVariable(
		"reject_partition_fullscan",
		"Rejects queries that scan all partitions without pruning.",
		"When enabled, queries on partitioned tables that cannot "
		"prune any partition will be rejected with an error, "
		"requiring a WHERE clause on the partition key.",
		&reject_fullscan_enabled,
		true,
		PGC_USERSET,
		GUC_EXPLAIN,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"partition_fullscan_threshold",
		"Maximum partitions allowed after pruning before rejecting.",
		"When reject_partition_fullscan is on, queries are rejected "
		"if remaining partitions after pruning exceed this threshold. "
		"0 means reject only when no pruning occurs at all.",
		&fullscan_threshold,
		0, 0, INT_MAX,
		PGC_USERSET,
		GUC_EXPLAIN,
		NULL, NULL, NULL);

	prev_planner_hook = planner_hook;
	planner_hook = rpf_planner_hook;
}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
}
