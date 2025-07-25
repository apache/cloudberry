/*-------------------------------------------------------------------------
 *
 * allpaths.c
 *	  Routines to find possible search paths for processing a query
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/allpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/relation.h"
#include "access/sysattr.h"
#include "access/tsmapi.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/planshare.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "partitioning/partbounds.h"
#include "partitioning/partprune.h"
#include "rewrite/rewriteManip.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "cdb/cdbmutate.h"		/* cdbmutate_warn_ctid_without_segid */
#include "cdb/cdbpath.h"		/* cdbpath_rows() */
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"

// TODO: these planner gucs need to be refactored into PlannerConfig.
bool		gp_enable_sort_limit = false;

/* results of subquery_is_pushdown_safe */
typedef struct pushdown_safety_info
{
	bool	   *unsafeColumns;	/* which output columns are unsafe to use */
	bool		unsafeVolatile; /* don't push down volatile quals */
	bool		unsafeLeaky;	/* don't push down leaky quals */
} pushdown_safety_info;

/* These parameters are set by GUC */
bool		enable_geqo = false;	/* just in case GUC doesn't set it */
int			geqo_threshold;
int			min_parallel_table_scan_size;
int			min_parallel_index_scan_size;

/* Hook for plugins to get control in set_rel_pathlist() */
set_rel_pathlist_hook_type set_rel_pathlist_hook = NULL;

/* Hook for plugins to replace standard_join_search() */
join_search_hook_type join_search_hook = NULL;


static void set_base_rel_consider_startup(PlannerInfo *root);
static void set_base_rel_sizes(PlannerInfo *root);
static void set_base_rel_pathlists(PlannerInfo *root);
static void set_rel_size(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte);
static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 Index rti, RangeTblEntry *rte);
static void set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel,
							   RangeTblEntry *rte);
static void set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
									  RangeTblEntry *rte);
static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
								   RangeTblEntry *rte);
static void set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte);
static void set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
										 RangeTblEntry *rte);
static void set_foreign_size(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte);
static void set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel,
								 RangeTblEntry *rte);
static void set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
								Index rti, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
                                    Index rti, RangeTblEntry *rte);
static bool has_multiple_baserels(PlannerInfo *root);
static void generate_orderedappend_paths(PlannerInfo *root, RelOptInfo *rel,
										 List *live_childrels,
										 List *all_child_pathkeys);
static Path *get_cheapest_parameterized_child_path(PlannerInfo *root,
												   RelOptInfo *rel,
												   Relids required_outer);
static void accumulate_append_subpath(Path *path,
									  List **subpaths,
									  List **special_subpaths);
static Path *get_singleton_append_subpath(Path *path);
static void set_dummy_rel_pathlist(PlannerInfo *root, RelOptInfo *rel);
static void set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
								  Index rti, RangeTblEntry *rte);
static void set_function_pathlist(PlannerInfo *root, RelOptInfo *rel,
                                  RangeTblEntry *rte);
static void set_tablefunction_pathlist(PlannerInfo *root, RelOptInfo *rel,
						   RangeTblEntry *rte);
static void set_values_pathlist(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte);
static void set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel,
								   RangeTblEntry *rte);
static void set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte);
static void set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel,
										 RangeTblEntry *rte);
static void set_result_pathlist(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte);
static void set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel,
								   RangeTblEntry *rte);
static void
add_grouped_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				 AggStrategy aggstrategy, RelAggInfo *agg_info,
				 bool partial);
static RelOptInfo *make_rel_from_joinlist(PlannerInfo *root, List *joinlist);
static Query *push_down_restrict(PlannerInfo *root, RelOptInfo *rel,
				   RangeTblEntry *rte, Index rti, Query *subquery);
static bool subquery_is_pushdown_safe(Query *subquery, Query *topquery,
									  pushdown_safety_info *safetyInfo);
static bool recurse_pushdown_safe(Node *setOp, Query *topquery,
								  pushdown_safety_info *safetyInfo);
static void check_output_expressions(Query *subquery,
									 pushdown_safety_info *safetyInfo);
static void compare_tlist_datatypes(List *tlist, List *colTypes,
									pushdown_safety_info *safetyInfo);
static bool targetIsInAllPartitionLists(TargetEntry *tle, Query *query);
static bool qual_is_pushdown_safe(Query *subquery, Index rti,
								  RestrictInfo *rinfo,
								  pushdown_safety_info *safetyInfo);
static void subquery_push_qual(Query *subquery,
							   RangeTblEntry *rte, Index rti, Node *qual);
static void recurse_push_qual(Node *setOp, Query *topquery,
							  RangeTblEntry *rte, Index rti, Node *qual);
static void remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel);

static void bring_to_outer_query(PlannerInfo *root, RelOptInfo *rel, List *outer_quals);
static void bring_to_singleQE(PlannerInfo *root, RelOptInfo *rel);
static bool is_query_contain_limit_groupby(Query *parse);
static void handle_gen_seggen_volatile_path(PlannerInfo *root, RelOptInfo *rel);


/*
 * make_one_rel
 *	  Finds all possible access paths for executing a query, returning a
 *	  single rel that represents the join of all base rels in the query.
 */
RelOptInfo *
make_one_rel(PlannerInfo *root, List *joinlist)
{
	RelOptInfo *rel;
	Index		rti;
	double		total_pages;

	/*
	 * Construct the all_baserels Relids set.
	 */
	root->all_baserels = NULL;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind != RELOPT_BASEREL)
			continue;

		root->all_baserels = bms_add_member(root->all_baserels, brel->relid);
	}

	/* Mark base rels as to whether we care about fast-start plans */
	set_base_rel_consider_startup(root);

	/*
	 * Compute size estimates and consider_parallel flags for each base rel.
	 */
	set_base_rel_sizes(root);

	/*
	 * We should now have size estimates for every actual table involved in
	 * the query, and we also know which if any have been deleted from the
	 * query by join removal, pruned by partition pruning, or eliminated by
	 * constraint exclusion.  So we can now compute total_table_pages.
	 *
	 * Note that appendrels are not double-counted here, even though we don't
	 * bother to distinguish RelOptInfos for appendrel parents, because the
	 * parents will have pages = 0.
	 *
	 * XXX if a table is self-joined, we will count it once per appearance,
	 * which perhaps is the wrong thing ... but that's not completely clear,
	 * and detecting self-joins here is difficult, so ignore it for now.
	 */
	total_pages = 0;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		if (IS_DUMMY_REL(brel))
			continue;

		if (IS_SIMPLE_REL(brel))
			total_pages += (double) brel->pages;
	}
	root->total_table_pages = total_pages;

	/*
	 * Generate access paths for each base rel.
	 */
	set_base_rel_pathlists(root);

	/*
	 * Generate access paths for the entire join tree.
	 */
	rel = make_rel_from_joinlist(root, joinlist);

	/*
	 * The result should join all and only the query's base rels.
	 */
	Assert(bms_equal(rel->relids, root->all_baserels));

	return rel;
}

/*
 * set_base_rel_consider_startup
 *	  Set the consider_[param_]startup flags for each base-relation entry.
 *
 * For the moment, we only deal with consider_param_startup here; because the
 * logic for consider_startup is pretty trivial and is the same for every base
 * relation, we just let build_simple_rel() initialize that flag correctly to
 * start with.  If that logic ever gets more complicated it would probably
 * be better to move it here.
 */
static void
set_base_rel_consider_startup(PlannerInfo *root)
{
	/*
	 * Since parameterized paths can only be used on the inside of a nestloop
	 * join plan, there is usually little value in considering fast-start
	 * plans for them.  However, for relations that are on the RHS of a SEMI
	 * or ANTI join, a fast-start plan can be useful because we're only going
	 * to care about fetching one tuple anyway.
	 *
	 * To minimize growth of planning time, we currently restrict this to
	 * cases where the RHS is a single base relation, not a join; there is no
	 * provision for consider_param_startup to get set at all on joinrels.
	 * Also we don't worry about appendrels.  costsize.c's costing rules for
	 * nestloop semi/antijoins don't consider such cases either.
	 */
	ListCell   *lc;

	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
		int			varno;

		if ((sjinfo->jointype == JOIN_SEMI || sjinfo->jointype == JOIN_ANTI) &&
			bms_get_singleton_member(sjinfo->syn_righthand, &varno))
		{
			RelOptInfo *rel = find_base_rel(root, varno);

			rel->consider_param_startup = true;
		}
	}
}

/*
 * set_base_rel_sizes
 *	  Set the size estimates (rows and widths) for each base-relation entry.
 *	  Also determine whether to consider parallel paths for base relations.
 *
 * We do this in a separate pass over the base rels so that rowcount
 * estimates are available for parameterized path generation, and also so
 * that each rel's consider_parallel flag is set correctly before we begin to
 * generate paths.
 */
static void
set_base_rel_sizes(PlannerInfo *root)
{
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		rte = root->simple_rte_array[rti];

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this rel in particular.  We have to do this
		 * before set_rel_size(), because (a) if this rel is an inheritance
		 * parent, set_append_rel_size() will use and perhaps change the rel's
		 * consider_parallel flag, and (b) for some RTE types, set_rel_size()
		 * goes ahead and makes paths immediately.
		 */
		if (root->glob->parallelModeOK)
			set_rel_consider_parallel(root, rel, rte);

		set_rel_size(root, rel, rti, rte);
	}
}

/*
 * set_base_rel_pathlists
 *	  Finds all paths available for scanning each base-relation entry.
 *	  Sequential scan and any available indices are considered.
 *	  Each useful path is attached to its relation's 'pathlist' field.
 */
static void
set_base_rel_pathlists(PlannerInfo *root)
{
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		/* CDB: Warn if ctid column is referenced but gp_segment_id is not. */
		cdbmutate_warn_ctid_without_segid(root, rel);

		set_rel_pathlist(root, rel, rti, root->simple_rte_array[rti]);
	}
}

/*
 * set_rel_size
 *	  Set size estimates for a base relation
 */
static void
set_rel_size(PlannerInfo *root, RelOptInfo *rel,
			 Index rti, RangeTblEntry *rte)
{
	if (rel->reloptkind == RELOPT_BASEREL &&
		relation_excluded_by_constraints(root, rel, rte))
	{
		/*
		 * We proved we don't need to scan the rel via constraint exclusion,
		 * so set up a single dummy path for it.  Here we only check this for
		 * regular baserels; if it's an otherrel, CE was already checked in
		 * set_append_rel_size().
		 *
		 * In this case, we go ahead and set up the relation's path right away
		 * instead of leaving it for set_rel_pathlist to do.  This is because
		 * we don't have a convention for marking a rel as dummy except by
		 * assigning a dummy path to it.
		 */
		set_dummy_rel_pathlist(root, rel);
	}
	else if (rte->inh)
	{
		/* It's an "append relation", process accordingly */
		set_append_rel_size(root, rel, rti, rte);
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_size(root, rel, rte);
				}
				else if (rte->relkind == RELKIND_PARTITIONED_TABLE)
				{
					/*
					 * We could get here if asked to scan a partitioned table
					 * with ONLY.  In that case we shouldn't scan any of the
					 * partitions, so mark it as a dummy rel.
					 */
					set_dummy_rel_pathlist(root, rel);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_size(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_size(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:

				/*
				 * Subqueries don't support making a choice between
				 * parameterized and unparameterized paths, so just go ahead
				 * and build their paths immediately.
				 */
				set_subquery_pathlist(root, rel, rti, rte);
				break;
			case RTE_FUNCTION:
				set_function_size_estimates(root, rel);
				break;
			case RTE_TABLEFUNCTION:
				set_tablefunction_pathlist(root, rel, rte);
				break;
			case RTE_TABLEFUNC:
				set_tablefunc_size_estimates(root, rel);
				break;
			case RTE_VALUES:
				set_values_size_estimates(root, rel);
				break;
			case RTE_CTE:

				/*
				 * CTEs don't support making a choice between parameterized
				 * and unparameterized paths, so just go ahead and build their
				 * paths immediately.
				 */
				if (rte->self_reference)
					set_worktable_pathlist(root, rel, rte);
				else
					set_cte_pathlist(root, rel, rte);
				break;
			case RTE_NAMEDTUPLESTORE:
				/* Might as well just build the path immediately */
				set_namedtuplestore_pathlist(root, rel, rte);
				break;
			case RTE_RESULT:
				/* Might as well just build the path immediately */
				set_result_pathlist(root, rel, rte);
				break;
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * We insist that all non-dummy rels have a nonzero rowcount estimate.
	 */
	Assert(rel->rows > 0 || IS_DUMMY_REL(rel));
}

/*
 * Decorate the Paths of 'rel' with Motions to bring the relation's
 * result to OuterQuery locus. The final plan will look something like
 * this:
 *
 *   Result (with quals from 'outer_quals')
 *           \
 *            \_Material
 *                   \
 *                    \_Broadcast (or Gather)
 *                           \
 *                            \_SeqScan (with quals from 'baserestrictinfo')
 */
static void
bring_to_outer_query(PlannerInfo *root, RelOptInfo *rel, List *outer_quals)
{
	List	   *origpathlist;
	ListCell   *lc;

	origpathlist = rel->pathlist;
	rel->cheapest_startup_path = NULL;
	rel->cheapest_total_path = NULL;
	rel->cheapest_unique_path = NULL;
	rel->cheapest_parameterized_paths = NIL;
	rel->pathlist = NIL;
	/* CBDB_PARALLEL_FIXME: Need to clear partial_pathlist before we enable OuterQuery locus in paralle mode */
	rel->partial_pathlist = NIL;

	foreach(lc, origpathlist)
	{
		Path	   *origpath = (Path *) lfirst(lc);
		Path	   *path;
		CdbPathLocus outerquery_locus;

		if (CdbPathLocus_IsGeneral(origpath->locus) ||
			CdbPathLocus_IsOuterQuery(origpath->locus))
			path = origpath;
		else
		{
			/*
			 * Cannot pass a param through motion, so if this is a parameterized
			 * path, we can't use it.
			 */
			if (origpath->param_info)
				continue;

			/*
			 * param_info cannot cover the case that an index path's orderbyclauses
			 * See github issue: https://github.com/greenplum-db/gpdb/issues/9733
			 */
			if (IsA(origpath, IndexPath))
			{
				IndexPath *ipath = (IndexPath *) origpath;
				if (contains_outer_params((Node *) ipath->indexorderbys,
										  (void *) root))
					continue;
			}

			CdbPathLocus_MakeOuterQuery(&outerquery_locus);

			path = cdbpath_create_motion_path(root,
											  origpath,
											  NIL, // DESTROY pathkeys
											  false,
											  outerquery_locus);
		}

		if (outer_quals)
			path = (Path *) create_projection_path_with_quals(root,
															  rel,
															  path,
															  path->parent->reltarget,
															  outer_quals,
															  true);
		add_path(rel, path, root);
	}
	set_cheapest(rel);
}

/*
 * The following function "steals" ideas and most of the code from the
 * function bring_to_outer_query.
 *
 * Decorate the Paths of 'rel' with Motions to bring the relation's
 * result to SingleQE locus. The final plan will look something like
 * this:
 *
 *   Result (with quals from 'outer_quals')
 *           \
 *            \_Material
 *                   \
 *                    \_ Gather
 *                           \
 *                            \_SeqScan (with quals from 'baserestrictinfo')
 */
static void
bring_to_singleQE(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *origpathlist;
	ListCell   *lc;

	origpathlist = rel->pathlist;
	rel->cheapest_startup_path = NULL;
	rel->cheapest_total_path = NULL;
	rel->cheapest_unique_path = NULL;
	rel->cheapest_parameterized_paths = NIL;
	rel->pathlist = NIL;

	foreach(lc, origpathlist)
	{
		Path	     *origpath = (Path *) lfirst(lc);
		Path	     *path;
		CdbPathLocus  target_locus;

		if (CdbPathLocus_IsGeneral(origpath->locus) ||
			CdbPathLocus_IsEntry(origpath->locus) ||
			CdbPathLocus_IsSingleQE(origpath->locus) ||
			CdbPathLocus_IsOuterQuery(origpath->locus))
			path = origpath;
		else
		{
			/*
			 * Cannot pass a param through motion, so if this is a parameterized
			 * path, we can't use it.
			 */
			if (origpath->param_info)
				continue;

			/*
			 * param_info cannot cover the case that an index path's orderbyclauses
			 * See github issue: https://github.com/greenplum-db/gpdb/issues/9733
			 */
			if (IsA(origpath, IndexPath))
			{
				IndexPath *ipath = (IndexPath *) origpath;
				if (contains_outer_params((Node *) ipath->indexorderbys,
										  (void *) root))
					continue;
			}

			CdbPathLocus_MakeSingleQE(&target_locus,
									  origpath->locus.numsegments);

			path = cdbpath_create_motion_path(root,
											  origpath,
											  NIL, // DESTROY pathkeys
											  false,
											  target_locus);

			path = (Path *) create_material_path(rel, path);
		}

		add_path(rel, path, root);
	}
	/*
	 * CBDB_PARALLEL_FIXME:
	 * If we need to bring to single QE which commonly seen in lateral
	 * join with group by or limit, we better to set partial pathlist
	 * to NIL in order to make sure single QE locus is satisfied in
	 * upper paths.
	 *
	 * It's not trivial to apply single QE locus constrain to parallel
	 * in current code. We should think about that later.
	 */
	rel->partial_pathlist = NIL;
	set_cheapest(rel);
}

/*
 * handle_gen_seggen_volatile_path
 *
 * Only use for base replicated rel.
 * Change the path in its pathlist if match the pattern
 * (segmentgeneral or general path contains volatile restrictions).
 */
static void
handle_gen_seggen_volatile_path(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *origpathlist;
	ListCell   *lc;

	origpathlist = rel->pathlist;
	rel->cheapest_startup_path = NULL;
	rel->cheapest_total_path = NULL;
	rel->cheapest_unique_path = NULL;
	rel->cheapest_parameterized_paths = NIL;
	rel->pathlist = NIL;

	foreach(lc, origpathlist)
	{
		Path	     *origpath = (Path *) lfirst(lc);
		Path	     *path;

		path = turn_volatile_seggen_to_singleqe(root,
												origpath,
												(Node *) (rel->baserestrictinfo));
		add_path(rel, path, root);
	}

	set_cheapest(rel);
}

/*
 * set_rel_pathlist
 *	  Build access paths for a base relation
 */
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 Index rti, RangeTblEntry *rte)
{
	if (IS_DUMMY_REL(rel))
	{
		/* We already proved the relation empty, so nothing more to do */
	}
	else if (rte->inh)
	{
		/* It's an "append relation", process accordingly */
		set_append_rel_pathlist(root, rel, rti, rte);
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_pathlist(root, rel, rte);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_pathlist(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_pathlist(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
				/* Subquery --- fully handled during set_rel_size */
				break;
			case RTE_FUNCTION:
				/* RangeFunction */
				set_function_pathlist(root, rel, rte);
				break;
			case RTE_TABLEFUNCTION:
				/* RangeFunction --- fully handled during set_rel_size */
				break;
			case RTE_TABLEFUNC:
				/* Table Function */
				set_tablefunc_pathlist(root, rel, rte);
				break;
			case RTE_VALUES:
				/* Values list */
				set_values_pathlist(root, rel, rte);
				break;
			case RTE_CTE:
				/* CTE reference --- fully handled during set_rel_size */
				break;
			case RTE_NAMEDTUPLESTORE:
				/* tuplestore reference --- fully handled during set_rel_size */
				break;
			case RTE_RESULT:
				/* simple Result --- fully handled during set_rel_size */
				break;
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * Cloudberry specific behavior:
	 * Change the path in pathlist if it is a general or segmentgeneral
	 * path that contains volatile restrictions.
	 */
	if (rel->reloptkind == RELOPT_BASEREL)
		handle_gen_seggen_volatile_path(root, rel);

	/*
	 * Allow a plugin to editorialize on the set of Paths for this base
	 * relation.  It could add new paths (such as CustomPaths) by calling
	 * add_path(), or add_partial_path() if parallel aware.  It could also
	 * delete or modify paths added by the core code.
	 */
	if (set_rel_pathlist_hook)
		(*set_rel_pathlist_hook) (root, rel, rti, rte);

	if (rel->upperrestrictinfo)
	{
		bring_to_outer_query(root, rel, rel->upperrestrictinfo);
		/* CBDB_PARALLEL_FIXME: enable parallel outer query? */
	}
	else if (root->config->force_singleQE)
	{
		/*
		 * CDB: we cannot pass parameters across motion,
		 * if this is the inner plan of a lateral join and
		 * it contains limit clause, we will reach here.
		 * Planner will gather all the data into singleQE
		 * and materialize it.
		 */
		bring_to_singleQE(root, rel);
	}

	/*
	 * If this is a baserel, we should normally consider gathering any partial
	 * paths we may have created for it.  We have to do this after calling the
	 * set_rel_pathlist_hook, else it cannot add partial paths to be included
	 * here.
	 *
	 * However, if this is an inheritance child, skip it.  Otherwise, we could
	 * end up with a very large number of gather nodes, each trying to grab
	 * its own pool of workers.  Instead, we'll consider gathering partial
	 * paths for the parent appendrel.
	 *
	 * Also, if this is the topmost scan/join rel (that is, the only baserel),
	 * we postpone gathering until the final scan/join targetlist is available
	 * (see grouping_planner).
	 */
#if 0
	if (rel->reloptkind == RELOPT_BASEREL &&
		bms_membership(root->all_baserels) != BMS_SINGLETON)
		generate_useful_gather_paths(root, rel, false);
#endif

	/* Now find the cheapest of the paths for this rel */
	set_cheapest(rel);

#ifdef OPTIMIZER_DEBUG
	debug_print_rel(root, rel);
#endif
}

/*
 * set_plain_rel_size
 *	  Set size estimates for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/*
 * If this relation could possibly be scanned from within a worker, then set
 * its consider_parallel flag.
 */
static void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte)
{
	/*
	 * The flag has previously been initialized to false, so we can just
	 * return if it becomes clear that we can't safely set it.
	 */
	Assert(!rel->consider_parallel);

	/* Don't call this if parallelism is disallowed for the entire query. */
	Assert(root->glob->parallelModeOK);

	/* This should only be called for baserels and appendrel children. */
	Assert(IS_SIMPLE_REL(rel));

	/* Assorted checks based on rtekind. */
	switch (rte->rtekind)
	{
		case RTE_RELATION:

			/*
			 * Currently, parallel workers can't access the leader's temporary
			 * tables.  We could possibly relax this if we wrote all of its
			 * local buffers at the start of the query and made no changes
			 * thereafter (maybe we could allow hint bit changes), and if we
			 * taught the workers to read them.  Writing a large number of
			 * temporary buffers could be expensive, though, and we don't have
			 * the rest of the necessary infrastructure right now anyway.  So
			 * for now, bail out if we see a temporary table.
			 */
			if (get_rel_persistence(rte->relid) == RELPERSISTENCE_TEMP)
				return;

			/*
			 * Table sampling can be pushed down to workers if the sample
			 * function and its arguments are safe.
			 */
			if (rte->tablesample != NULL)
			{
				char		proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (!is_parallel_safe(root, (Node *) rte->tablesample->args))
					return;
			}

			/*
			 * Ask FDWs whether they can support performing a ForeignScan
			 * within a worker.  Most often, the answer will be no.  For
			 * example, if the nature of the FDW is such that it opens a TCP
			 * connection with a remote server, each parallel worker would end
			 * up with a separate connection, and these connections might not
			 * be appropriately coordinated between workers and the leader.
			 */
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				Assert(rel->fdwroutine);
				if (!rel->fdwroutine->IsForeignScanParallelSafe)
					return;
				if (!rel->fdwroutine->IsForeignScanParallelSafe(root, rel, rte))
					return;
			}

			/*
			 * CBDB_PARALLEL_FIXME: GPDB don't allow parallelism for relations that are system catalogs.
			*/
			if (IsSystemClassByRelid(rte->relid))
				return;

			/*
			 * There are additional considerations for appendrels, which we'll
			 * deal with in set_append_rel_size and set_append_rel_pathlist.
			 * For now, just set consider_parallel based on the rel's own
			 * quals and targetlist.
			 */
			break;

		case RTE_SUBQUERY:

			/*
			 * There's no intrinsic problem with scanning a subquery-in-FROM
			 * (as distinct from a SubPlan or InitPlan) in a parallel worker.
			 * If the subquery doesn't happen to have any parallel-safe paths,
			 * then flagging it as consider_parallel won't change anything,
			 * but that's true for plain tables, too.  We must set
			 * consider_parallel based on the rel's own quals and targetlist,
			 * so that if a subquery path is parallel-safe but the quals and
			 * projection we're sticking onto it are not, we correctly mark
			 * the SubqueryScanPath as not parallel-safe.  (Note that
			 * set_subquery_pathlist() might push some of these quals down
			 * into the subquery itself, but that doesn't change anything.)
			 *
			 * We can't push sub-select containing LIMIT/OFFSET to workers as
			 * there is no guarantee that the row order will be fully
			 * deterministic, and applying LIMIT/OFFSET will lead to
			 * inconsistent results at the top-level.  (In some cases, where
			 * the result is ordered, we could relax this restriction.  But it
			 * doesn't currently seem worth expending extra effort to do so.)
			 */
			{
				Query	   *subquery = castNode(Query, rte->subquery);

				if (limit_needed(subquery))
					return;
			}
			break;

		case RTE_JOIN:
			/* Shouldn't happen; we're only considering baserels here. */
			Assert(false);
			return;

		case RTE_FUNCTION:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->functions))
				return;
			break;

		case RTE_TABLEFUNCTION:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->functions))
				return;

			/* GPDB_96_MERGE_FIXME: other than the function itself, I guess this is like RTE_SUBQUERY... */
			break;

		case RTE_TABLEFUNC:
			/* not parallel safe */
			return;

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->values_lists))
				return;
			break;

		case RTE_CTE:

			/*
			 * CTE tuplestores aren't shared among parallel workers, so we
			 * force all CTE scans to happen in the leader.  Also, populating
			 * the CTE would require executing a subplan that's not available
			 * in the worker, might be parallel-restricted, and must get
			 * executed only once.
			 */
			return;

		case RTE_VOID:

			/*
			 * Not sure if parallelizing a "no-op" void RTE makes sense, but
			 * it's no reason to disable parallelization.
			 */
		case RTE_NAMEDTUPLESTORE:

			/*
			 * tuplestore cannot be shared, at least without more
			 * infrastructure to support that.
			 */
			return;

		case RTE_RESULT:
			/* RESULT RTEs, in themselves, are no problem. */
			break;
	}

	/*
	 * If there's anything in baserestrictinfo that's parallel-restricted, we
	 * give up on parallelizing access to this relation.  We could consider
	 * instead postponing application of the restricted quals until we're
	 * above all the parallelism in the plan tree, but it's not clear that
	 * that would be a win in very many cases, and it might be tricky to make
	 * outer join clauses work correctly.  It would likely break equivalence
	 * classes, too.
	 */
	if (!is_parallel_safe(root, (Node *) rel->baserestrictinfo))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (!is_parallel_safe(root, (Node *) rel->reltarget->exprs))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}

/*
 * set_plain_rel_pathlist
 *	  Build access paths for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	add_path(rel, create_seqscan_path(root, rel, required_outer, 0), root);

	/* If appropriate, consider parallel sequential scan */
	if (rel->consider_parallel && required_outer == NULL)
		create_plain_partial_paths(root, rel);

	/* Consider index and bitmap scans */
	create_index_paths(root, rel);

	create_tidscan_paths(root, rel);
}

/*
 * create_plain_partial_paths
 *	  Build partial access paths for parallel scan of a plain relation
 */
void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int			parallel_workers;

	parallel_workers = compute_parallel_worker(root, rel, rel->pages, -1,
											   max_parallel_workers_per_gather);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	/* GPDB parallel, parallel_workers <= 1 is bogus */
	if (parallel_workers <= 1)
		return;

	/* CBDB_PARALLEL_FIXME: update locus.parallel_workers? */

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));
}

/*
 * set_tablesample_rel_size
 *	  Set size estimates for a sampled relation
 */
static void
set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	TableSampleClause *tsc = rte->tablesample;
	TsmRoutine *tsm;
	BlockNumber pages;
	double		tuples;

	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/*
	 * Call the sampling method's estimation function to estimate the number
	 * of pages it will read and the number of tuples it will return.  (Note:
	 * we assume the function returns sane values.)
	 */
	tsm = GetTsmRoutine(tsc->tsmhandler);
	tsm->SampleScanGetSampleSize(root, rel, tsc->args,
								 &pages, &tuples);

	/*
	 * For the moment, because we will only consider a SampleScan path for the
	 * rel, it's okay to just overwrite the pages and tuples estimates for the
	 * whole relation.  If we ever consider multiple path types for sampled
	 * rels, we'll need more complication.
	 */
	rel->pages = pages;
	rel->tuples = tuples;

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/*
 * set_tablesample_rel_pathlist
 *	  Build access paths for a sampled relation
 */
static void
set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	Path	   *path;

	/*
	 * We don't support pushing join clauses into the quals of a samplescan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in its tlist or TABLESAMPLE arguments.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sampled scan */
	path = create_samplescan_path(root, rel, required_outer);

	/*
	 * If the sampling method does not support repeatable scans, we must avoid
	 * plans that would scan the rel multiple times.  Ideally, we'd simply
	 * avoid putting the rel on the inside of a nestloop join; but adding such
	 * a consideration to the planner seems like a great deal of complication
	 * to support an uncommon usage of second-rate sampling methods.  Instead,
	 * if there is a risk that the query might perform an unsafe join, just
	 * wrap the SampleScan in a Materialize node.  We can check for joins by
	 * counting the membership of all_baserels (note that this correctly
	 * counts inheritance trees as single rels).  If we're inside a subquery,
	 * we can't easily check whether a join might occur in the outer query, so
	 * just assume one is possible.
	 *
	 * GetTsmRoutine is relatively expensive compared to the other tests here,
	 * so check repeatable_across_scans last, even though that's a bit odd.
	 */
	if ((root->query_level > 1 ||
		 bms_membership(root->all_baserels) != BMS_SINGLETON) &&
		!(GetTsmRoutine(rte->tablesample->tsmhandler)->repeatable_across_scans))
	{
		path = (Path *) create_material_path(rel, path);
	}

	add_path(rel, path, root);

	/* For the moment, at least, there are no other paths to consider */
}

/*
 * set_foreign_size
 *		Set size estimates for a foreign table RTE
 */
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Mark rel with estimated output rows, width, etc */
	set_foreign_size_estimates(root, rel);

	/* Let FDW adjust the size estimates, if it can */
	rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

	/* ... but do not let it set the rows estimate to zero */
	rel->rows = clamp_row_est(rel->rows);

	/*
	 * Also, make sure rel->tuples is not insane relative to rel->rows.
	 * Notably, this ensures sanity if pg_class.reltuples contains -1 and the
	 * FDW doesn't do anything to replace that.
	 */
	rel->tuples = Max(rel->tuples, rel->rows);
}

static void
adjust_costs_for_mpp_foreign_scan(List *pathlist)
{
	ListCell *cell;

	foreach(cell, pathlist)
	{
		Path *path;

		path = lfirst(cell);
		if (IsA(path, ForeignPath))
		{
			if (path->locus.locustype == CdbLocusType_Strewn &&
				path->locus.numsegments > 1)
			{
				path->rows *= (path->locus.numsegments * 10);
			}
		}
	}
}

/*
 * set_foreign_pathlist
 *		Build access paths for a foreign table RTE
 */
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Call the FDW's GetForeignPaths function to generate path(s) */
	rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
	adjust_costs_for_mpp_foreign_scan(rel->pathlist);
}

/*
 * set_append_rel_size
 *	  Set size estimates for a simple "append relation"
 *
 * The passed-in rel and RTE represent the entire append relation.  The
 * relation's contents are computed by appending together the output of the
 * individual member relations.  Note that in the non-partitioned inheritance
 * case, the first member relation is actually the same table as is mentioned
 * in the parent RTE ... but it has a different RTE and RelOptInfo.  This is
 * a good thing because their outputs are not the same size.
 */
static void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
					Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	bool		has_live_children;
	double		parent_rows;
	double		parent_size;
	double	   *parent_attrsizes;
	int			nattrs;
	ListCell   *l;

	/* Guard against stack overflow due to overly deep inheritance tree. */
	check_stack_depth();

	Assert(IS_SIMPLE_REL(rel));

	/*
	 * If this is a partitioned baserel, set the consider_partitionwise_join
	 * flag; currently, we only consider partitionwise joins with the baserel
	 * if its targetlist doesn't contain a whole-row Var.
	 */
	if (enable_partitionwise_join &&
		rel->reloptkind == RELOPT_BASEREL &&
		rte->relkind == RELKIND_PARTITIONED_TABLE &&
		rel->attr_needed[InvalidAttrNumber - rel->min_attr] == NULL)
		rel->consider_partitionwise_join = true;

	/*
	 * Initialize to compute size estimates for whole append relation.
	 *
	 * We handle width estimates by weighting the widths of different child
	 * rels proportionally to their number of rows.  This is sensible because
	 * the use of width estimates is mainly to compute the total relation
	 * "footprint" if we have to sort or hash it.  To do this, we sum the
	 * total equivalent size (in "double" arithmetic) and then divide by the
	 * total rowcount estimate.  This is done separately for the total rel
	 * width and each attribute.
	 *
	 * Note: if you consider changing this logic, beware that child rels could
	 * have zero rows and/or width, if they were excluded by constraints.
	 */
	has_live_children = false;
	parent_rows = 0;
	parent_size = 0;
	nattrs = rel->max_attr - rel->min_attr + 1;
	parent_attrsizes = (double *) palloc0(nattrs * sizeof(double));

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;
		ListCell   *parentvars;
		ListCell   *childvars;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];

		/*
		 * The child rel's RelOptInfo was already created during
		 * add_other_rels_to_query.
		 */
		childrel = find_base_rel(root, childRTindex);
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/* We may have already proven the child to be dummy. */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * We have to copy the parent's targetlist and quals to the child,
		 * with appropriate substitution of variables.  However, the
		 * baserestrictinfo quals were already copied/substituted when the
		 * child RelOptInfo was built.  So we don't need any additional setup
		 * before applying constraint exclusion.
		 */
		if (relation_excluded_by_constraints(root, childrel, childRTE))
		{
			/*
			 * This child need not be scanned, so we can omit it from the
			 * appendrel.
			 */
			set_dummy_rel_pathlist(root, childrel);
			continue;
		}

		/*
		 * Constraint exclusion failed, so copy the parent's join quals and
		 * targetlist to the child, with appropriate variable substitutions.
		 *
		 * NB: the resulting childrel->reltarget->exprs may contain arbitrary
		 * expressions, which otherwise would not occur in a rel's targetlist.
		 * Code that might be looking at an appendrel child must cope with
		 * such.  (Normally, a rel's targetlist would only include Vars and
		 * PlaceHolderVars.)  XXX we do not bother to update the cost or width
		 * fields of childrel->reltarget; not clear if that would be useful.
		 */
		childrel->joininfo = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->joininfo,
								   1, &appinfo);
		childrel->reltarget->exprs = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->reltarget->exprs,
								   1, &appinfo);

		/*
		 * We have to make child entries in the EquivalenceClass data
		 * structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings).
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
		childrel->has_eclass_joins = rel->has_eclass_joins;

		/*
		 * Note: we could compute appropriate attr_needed data for the child's
		 * variables, by transforming the parent's attr_needed through the
		 * translated_vars mapping.  However, currently there's no need
		 * because attr_needed is only examined for base relations not
		 * otherrels.  So we just leave the child's attr_needed empty.
		 */

		/*
		 * If we consider partitionwise joins with the parent rel, do the same
		 * for partitioned child rels.
		 *
		 * Note: here we abuse the consider_partitionwise_join flag by setting
		 * it for child rels that are not themselves partitioned.  We do so to
		 * tell try_partitionwise_join() that the child rel is sufficiently
		 * valid to be used as a per-partition input, even if it later gets
		 * proven to be dummy.  (It's not usable until we've set up the
		 * reltarget and EC entries, which we just did.)
		 */
		if (rel->consider_partitionwise_join)
			childrel->consider_partitionwise_join = true;

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this childrel in particular.  But if we've
		 * already decided the appendrel is not parallel-safe as a whole,
		 * there's no point in considering parallelism for this child.  For
		 * consistency, do this before calling set_rel_size() for the child.
		 */
		if (root->glob->parallelModeOK && rel->consider_parallel)
			set_rel_consider_parallel(root, childrel, childRTE);

		/*
		 * Compute the child's size.
		 */
		set_rel_size(root, childrel, childRTindex, childRTE);

		/*
		 * It is possible that constraint exclusion detected a contradiction
		 * within a child subquery, even though we didn't prove one above. If
		 * so, we can skip this child.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* We have at least one live child. */
		has_live_children = true;

		/*
		 * If any live child is not parallel-safe, treat the whole appendrel
		 * as not parallel-safe.  In future we might be able to generate plans
		 * in which some children are farmed out to workers while others are
		 * not; but we don't have that today, so it's a waste to consider
		 * partial paths anywhere in the appendrel unless it's all safe.
		 * (Child rels visited before this one will be unmarked in
		 * set_append_rel_pathlist().)
		 */
		if (!childrel->consider_parallel)
			rel->consider_parallel = false;

		/*
		 * Accumulate size information from each live child.
		 */
		Assert(childrel->rows > 0);

		parent_rows += childrel->rows;
		parent_size += childrel->reltarget->width * childrel->rows;

		/*
		 * Accumulate per-column estimates too.  We need not do anything for
		 * PlaceHolderVars in the parent list.  If child expression isn't a
		 * Var, or we didn't record a width estimate for it, we have to fall
		 * back on a datatype-based estimate.
		 *
		 * By construction, child's targetlist is 1-to-1 with parent's.
		 */
		forboth(parentvars, rel->reltarget->exprs,
				childvars, childrel->reltarget->exprs)
		{
			Var		   *parentvar = (Var *) lfirst(parentvars);
			Node	   *childvar = (Node *) lfirst(childvars);

			if (IsA(parentvar, Var) && parentvar->varno == parentRTindex)
			{
				int			pndx = parentvar->varattno - rel->min_attr;
				int32		child_width = 0;

				if (IsA(childvar, Var) &&
					((Var *) childvar)->varno == childrel->relid)
				{
					int			cndx = ((Var *) childvar)->varattno - childrel->min_attr;

					child_width = childrel->attr_widths[cndx];
				}
				if (child_width <= 0)
					child_width = get_typavgwidth(exprType(childvar),
												  exprTypmod(childvar));
				Assert(child_width > 0);
				parent_attrsizes[pndx] += child_width * childrel->rows;
			}
		}
	}

	if (has_live_children)
	{
		/*
		 * Save the finished size estimates.
		 */
		int			i;

		Assert(parent_rows > 0);
		rel->rows = parent_rows;
		rel->reltarget->width = rint(parent_size / parent_rows);
		for (i = 0; i < nattrs; i++)
			rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);

		/*
		 * Set "raw tuples" count equal to "rows" for the appendrel; needed
		 * because some places assume rel->tuples is valid for any baserel.
		 */
		rel->tuples = parent_rows;

		/*
		 * Note that we leave rel->pages as zero; this is important to avoid
		 * double-counting the appendrel tree in total_table_pages.
		 */
	}
	else
	{
		/*
		 * All children were excluded by constraints, so mark the whole
		 * appendrel dummy.  We must do this in this phase so that the rel's
		 * dummy-ness is visible when we generate paths for other rels.
		 */
		set_dummy_rel_pathlist(root, rel);
	}

	pfree(parent_attrsizes);
}

/*
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 */
static void
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	List	   *live_childrels = NIL;
	ListCell   *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * non-dummy children.
	 */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/*
		 * If set_append_rel_size() decided the parent appendrel was
		 * parallel-unsafe at some point after visiting this child rel, we
		 * need to propagate the unsafety marking down to the child, so that
		 * we don't generate useless partial paths for it.
		 */
		if (!rel->consider_parallel)
			childrel->consider_parallel = false;

		/*
		 * Compute the child's access paths.
		 */
		set_rel_pathlist(root, childrel, childRTindex, childRTE);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);
	}

	/* Add paths to the append relation. */
	add_paths_to_append_rel(root, rel, live_childrels);
}


/*
 * add_paths_to_append_rel
 *		Generate paths for the given append relation given the set of non-dummy
 *		child rels.
 *
 * The function collects all parameterizations and orderings supported by the
 * non-dummy children. For every such parameterization or ordering, it creates
 * an append path collecting one path from each non-dummy child with given
 * parameterization or ordering. Similarly it collects partial paths from
 * non-dummy children to create partial append paths.
 */
void
add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel,
						List *live_childrels)
{
	List	   *subpaths = NIL;
	bool		subpaths_valid = true;
	List	   *partial_subpaths = NIL;
	List	   *pa_partial_subpaths = NIL;
	List	   *pa_nonpartial_subpaths = NIL;
	bool		partial_subpaths_valid = true;
	bool		pa_subpaths_valid;
	List	   *all_child_pathkeys = NIL;
	List	   *all_child_outers = NIL;
	ListCell   *l;
	double		partial_rows = -1;

	/* If appropriate, consider parallel append */
	pa_subpaths_valid = enable_parallel_append && rel->consider_parallel;

	/*
	 * For every non-dummy child, remember the cheapest path.  Also, identify
	 * all pathkeys (orderings) and parameterizations (required_outer sets)
	 * available for the non-dummy member relations.
	 */
	foreach(l, live_childrels)
	{
		RelOptInfo *childrel = lfirst(l);
		ListCell   *lcp;
		Path	   *cheapest_partial_path = NULL;

		/*
		 * If child has an unparameterized cheapest-total path, add that to
		 * the unparameterized Append path we are constructing for the parent.
		 * If not, there's no workable unparameterized path.
		 *
		 * With partitionwise aggregates, the child rel's pathlist may be
		 * empty, so don't assume that a path exists here.
		 */
		if (childrel->pathlist != NIL &&
			childrel->cheapest_total_path->param_info == NULL)
			accumulate_append_subpath(childrel->cheapest_total_path,
									  &subpaths, NULL);
		else
			subpaths_valid = false;

		/* Same idea, but for a partial plan. */
		if (childrel->partial_pathlist != NIL)
		{
			cheapest_partial_path = linitial(childrel->partial_pathlist);
			accumulate_append_subpath(cheapest_partial_path,
									  &partial_subpaths, NULL);
		}
		else
			partial_subpaths_valid = false;

		/*
		 * Same idea, but for a parallel append mixing partial and non-partial
		 * paths.
		 */
		if (pa_subpaths_valid)
		{
			Path	   *nppath = NULL;

			nppath =
				get_cheapest_parallel_safe_total_inner(childrel->pathlist);

			if (cheapest_partial_path == NULL && nppath == NULL)
			{
				/* Neither a partial nor a parallel-safe path?  Forget it. */
				pa_subpaths_valid = false;
			}
			else if (nppath == NULL ||
					 (cheapest_partial_path != NULL &&
					  cheapest_partial_path->total_cost < nppath->total_cost))
			{
				/* Partial path is cheaper or the only option. */
				Assert(cheapest_partial_path != NULL);
				accumulate_append_subpath(cheapest_partial_path,
										  &pa_partial_subpaths,
										  &pa_nonpartial_subpaths);
			}
			else
			{
				/*
				 * Either we've got only a non-partial path, or we think that
				 * a single backend can execute the best non-partial path
				 * faster than all the parallel backends working together can
				 * execute the best partial path.
				 *
				 * It might make sense to be more aggressive here.  Even if
				 * the best non-partial path is more expensive than the best
				 * partial path, it could still be better to choose the
				 * non-partial path if there are several such paths that can
				 * be given to different workers.  For now, we don't try to
				 * figure that out.
				 */
				accumulate_append_subpath(nppath,
										  &pa_nonpartial_subpaths,
										  NULL);
				/*
				 * CBDB_PARALLEL_FIXME: can't use parallel append if subpath
				 * is not parallel safe. 
				 */
				if (!nppath->parallel_safe)
					pa_subpaths_valid = false;
			}
		}

		/*
		 * Collect lists of all the available path orderings and
		 * parameterizations for all the children.  We use these as a
		 * heuristic to indicate which sort orderings and parameterizations we
		 * should build Append and MergeAppend paths for.
		 */
		foreach(lcp, childrel->pathlist)
		{
			Path	   *childpath = (Path *) lfirst(lcp);
			List	   *childkeys = childpath->pathkeys;
			Relids		childouter = PATH_REQ_OUTER(childpath);

			/* Unsorted paths don't contribute to pathkey list */
			if (childkeys != NIL)
			{
				ListCell   *lpk;
				bool		found = false;

				/* Have we already seen this ordering? */
				foreach(lpk, all_child_pathkeys)
				{
					List	   *existing_pathkeys = (List *) lfirst(lpk);

					if (compare_pathkeys(existing_pathkeys,
										 childkeys) == PATHKEYS_EQUAL)
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_pathkeys */
					all_child_pathkeys = lappend(all_child_pathkeys,
												 childkeys);
				}
			}

			/* Unparameterized paths don't contribute to param-set list */
			if (childouter)
			{
				ListCell   *lco;
				bool		found = false;

				/* Have we already seen this param set? */
				foreach(lco, all_child_outers)
				{
					Relids		existing_outers = (Relids) lfirst(lco);

					if (bms_equal(existing_outers, childouter))
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_outers */
					all_child_outers = lappend(all_child_outers,
											   childouter);
				}
			}
		}
	}

	/*
	 * If we found unparameterized paths for all children, build an unordered,
	 * unparameterized Append path for the rel.  (Note: this is correct even
	 * if we have zero or one live subpath due to constraint exclusion.)
	 */
	if (subpaths_valid)
		add_path(rel, (Path *) create_append_path(root, rel, subpaths, NIL,
												  NIL, NULL, 0, false,
												  -1), root);

	/*
	 * Consider an append of unordered, unparameterized partial paths.  Make
	 * it parallel-aware if possible.
	 */
	if (partial_subpaths_valid && partial_subpaths != NIL)
	{
		AppendPath *appendpath;
		ListCell   *lc;
		int			parallel_workers = 0;

		/* Find the highest number of workers requested for any subpath. */
		foreach(lc, partial_subpaths)
		{
			Path	   *path = lfirst(lc);

			parallel_workers = Max(parallel_workers, path->parallel_workers);
		}
		/*
		 * CBDB_PARALLEL_FIXME: it still cannot be opened after we deal with append.
		 * Because we currently allow path with non parallel_workers been added to
		 * partial_path.
		 */
#if 0
		Assert(parallel_workers > 0);
#endif

		/*
		 * If the use of parallel append is permitted, always request at least
		 * log2(# of children) workers.  We assume it can be useful to have
		 * extra workers in this case because they will be spread out across
		 * the children.  The precise formula is just a guess, but we don't
		 * want to end up with a radically different answer for a table with N
		 * partitions vs. an unpartitioned table with the same data, so the
		 * use of some kind of log-scaling here seems to make some sense.
		 */
		if (enable_parallel_append)
		{
			parallel_workers = Max(parallel_workers,
								   fls(list_length(live_childrels)));
			parallel_workers = Min(parallel_workers,
								   max_parallel_workers_per_gather);
		}
		/*
		 * CBDB_PARALLEL_FIXME: it still cannot be opened after we deal with append.
		 * Because we currently allow path with non parallel_workers been added to
		 * partial_path.
		 */
#if 0
		Assert(parallel_workers > 0);
#endif

		if (parallel_workers > 1)
		{
			/* Generate a partial append path. */
			appendpath = create_append_path(root, rel, NIL, partial_subpaths,
											NIL, NULL, parallel_workers,
											enable_parallel_append,
											-1);

			/*
			 * Make sure any subsequent partial paths use the same row count
			 * estimate.
			 */
			partial_rows = appendpath->path.rows;
			/* Add the path if subpath has not Motion.*/
			if (appendpath->path.parallel_safe && appendpath->path.motionHazard == false)
				add_partial_path(rel, (Path *)appendpath);
		}
	}

	/*
	 * Consider a parallel-aware append using a mix of partial and non-partial
	 * paths.  (This only makes sense if there's at least one child which has
	 * a non-partial path that is substantially cheaper than any partial path;
	 * otherwise, we should use the append path added in the previous step.)
	 */
	if (pa_subpaths_valid && pa_nonpartial_subpaths != NIL)
	{
		AppendPath *appendpath;
		ListCell   *lc;
		int			parallel_workers = 0;

		/*
		 * Find the highest number of workers requested for any partial
		 * subpath.
		 */
		foreach(lc, pa_partial_subpaths)
		{
			Path	   *path = lfirst(lc);

			parallel_workers = Max(parallel_workers, path->parallel_workers);
		}

		/*
		 * Same formula here as above.  It's even more important in this
		 * instance because the non-partial paths won't contribute anything to
		 * the planned number of parallel workers.
		 */
		parallel_workers = Max(parallel_workers,
							   fls(list_length(live_childrels)));
		parallel_workers = Min(parallel_workers,
							   max_parallel_workers_per_gather);
		Assert(parallel_workers > 0);

		/* GPDB parallel, parallel_workers <= 1 is bogus */
		if (parallel_workers > 1)
		{
			appendpath = create_append_path(root, rel, pa_nonpartial_subpaths,
											pa_partial_subpaths,
											NIL, NULL, parallel_workers, true,
											partial_rows);
			if (appendpath->path.parallel_safe && appendpath->path.motionHazard == false)
				add_partial_path(rel, (Path *) appendpath);
		}
	}

	/*
	 * Also build unparameterized ordered append paths based on the collected
	 * list of child pathkeys.
	 */
	if (subpaths_valid)
		generate_orderedappend_paths(root, rel, live_childrels,
									 all_child_pathkeys);

	/*
	 * Build Append paths for each parameterization seen among the child rels.
	 * (This may look pretty expensive, but in most cases of practical
	 * interest, the child rels will expose mostly the same parameterizations,
	 * so that not that many cases actually get considered here.)
	 *
	 * The Append node itself cannot enforce quals, so all qual checking must
	 * be done in the child paths.  This means that to have a parameterized
	 * Append path, we must have the exact same parameterization for each
	 * child path; otherwise some children might be failing to check the
	 * moved-down quals.  To make them match up, we can try to increase the
	 * parameterization of lesser-parameterized paths.
	 */
	foreach(l, all_child_outers)
	{
		Relids		required_outer = (Relids) lfirst(l);
		ListCell   *lcr;

		/* Select the child paths for an Append with this parameterization */
		subpaths = NIL;
		subpaths_valid = true;
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *subpath;

			if (childrel->pathlist == NIL)
			{
				/* failed to make a suitable path for this child */
				subpaths_valid = false;
				break;
			}

			subpath = get_cheapest_parameterized_child_path(root,
															childrel,
															required_outer);
			if (subpath == NULL)
			{
				/* failed to make a suitable path for this child */
				subpaths_valid = false;
				break;
			}
			accumulate_append_subpath(subpath, &subpaths, NULL);
		}

		if (subpaths_valid)
			add_path(rel, (Path *)
					 create_append_path(root, rel, subpaths, NIL,
										NIL, required_outer, 0, false,
										-1), root);
	}

	/*
	 * When there is only a single child relation, the Append path can inherit
	 * any ordering available for the child rel's path, so that it's useful to
	 * consider ordered partial paths.  Above we only considered the cheapest
	 * partial path for each child, but let's also make paths using any
	 * partial paths that have pathkeys.
	 */
	if (list_length(live_childrels) == 1)
	{
		RelOptInfo *childrel = (RelOptInfo *) linitial(live_childrels);

		/* skip the cheapest partial path, since we already used that above */
		for_each_from(l, childrel->partial_pathlist, 1)
		{
			Path	   *path = (Path *) lfirst(l);
			AppendPath *appendpath;

			/* skip paths with no pathkeys. */
			if (path->pathkeys == NIL)
				continue;

			appendpath = create_append_path(root, rel, NIL, list_make1(path),
											NIL, NULL,
											path->parallel_workers, true,
											partial_rows);

			if (appendpath->path.parallel_safe && appendpath->path.motionHazard == false)
				add_partial_path(rel, (Path *) appendpath);
		}
	}
}

/*
 * generate_orderedappend_paths
 *		Generate ordered append paths for an append relation
 *
 * Usually we generate MergeAppend paths here, but there are some special
 * cases where we can generate simple Append paths, because the subpaths
 * can provide tuples in the required order already.
 *
 * We generate a path for each ordering (pathkey list) appearing in
 * all_child_pathkeys.
 *
 * We consider both cheapest-startup and cheapest-total cases, ie, for each
 * interesting ordering, collect all the cheapest startup subpaths and all the
 * cheapest total paths, and build a suitable path for each case.
 *
 * We don't currently generate any parameterized ordered paths here.  While
 * it would not take much more code here to do so, it's very unclear that it
 * is worth the planning cycles to investigate such paths: there's little
 * use for an ordered path on the inside of a nestloop.  In fact, it's likely
 * that the current coding of add_path would reject such paths out of hand,
 * because add_path gives no credit for sort ordering of parameterized paths,
 * and a parameterized MergeAppend is going to be more expensive than the
 * corresponding parameterized Append path.  If we ever try harder to support
 * parameterized mergejoin plans, it might be worth adding support for
 * parameterized paths here to feed such joins.  (See notes in
 * optimizer/README for why that might not ever happen, though.)
 */
static void
generate_orderedappend_paths(PlannerInfo *root, RelOptInfo *rel,
							 List *live_childrels,
							 List *all_child_pathkeys)
{
	ListCell   *lcp;
	List	   *partition_pathkeys = NIL;
	List	   *partition_pathkeys_desc = NIL;
	bool		partition_pathkeys_partial = true;
	bool		partition_pathkeys_desc_partial = true;

	/*
	 * Some partitioned table setups may allow us to use an Append node
	 * instead of a MergeAppend.  This is possible in cases such as RANGE
	 * partitioned tables where it's guaranteed that an earlier partition must
	 * contain rows which come earlier in the sort order.  To detect whether
	 * this is relevant, build pathkey descriptions of the partition ordering,
	 * for both forward and reverse scans.
	 */
	if (rel->part_scheme != NULL && IS_SIMPLE_REL(rel) &&
		partitions_are_ordered(rel->boundinfo, rel->nparts))
	{
		partition_pathkeys = build_partition_pathkeys(root, rel,
													  ForwardScanDirection,
													  &partition_pathkeys_partial);

		partition_pathkeys_desc = build_partition_pathkeys(root, rel,
														   BackwardScanDirection,
														   &partition_pathkeys_desc_partial);

		/*
		 * You might think we should truncate_useless_pathkeys here, but
		 * allowing partition keys which are a subset of the query's pathkeys
		 * can often be useful.  For example, consider a table partitioned by
		 * RANGE (a, b), and a query with ORDER BY a, b, c.  If we have child
		 * paths that can produce the a, b, c ordering (perhaps via indexes on
		 * (a, b, c)) then it works to consider the appendrel output as
		 * ordered by a, b, c.
		 */
	}

	/* Now consider each interesting sort ordering */
	foreach(lcp, all_child_pathkeys)
	{
		List	   *pathkeys = (List *) lfirst(lcp);
		List	   *startup_subpaths = NIL;
		List	   *total_subpaths = NIL;
		bool		startup_neq_total = false;
		ListCell   *lcr;
		bool		match_partition_order;
		bool		match_partition_order_desc;

		/*
		 * Determine if this sort ordering matches any partition pathkeys we
		 * have, for both ascending and descending partition order.  If the
		 * partition pathkeys happen to be contained in pathkeys then it still
		 * works, as described above, providing that the partition pathkeys
		 * are complete and not just a prefix of the partition keys.  (In such
		 * cases we'll be relying on the child paths to have sorted the
		 * lower-order columns of the required pathkeys.)
		 */
		match_partition_order =
			pathkeys_contained_in(pathkeys, partition_pathkeys) ||
			(!partition_pathkeys_partial &&
			 pathkeys_contained_in(partition_pathkeys, pathkeys));

		match_partition_order_desc = !match_partition_order &&
			(pathkeys_contained_in(pathkeys, partition_pathkeys_desc) ||
			 (!partition_pathkeys_desc_partial &&
			  pathkeys_contained_in(partition_pathkeys_desc, pathkeys)));

		/* Select the child paths for this ordering... */
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *cheapest_startup,
					   *cheapest_total;

			/* Locate the right paths, if they are available. */
			cheapest_startup =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   STARTUP_COST,
											   false);
			cheapest_total =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   TOTAL_COST,
											   false);

			/*
			 * If we can't find any paths with the right order just use the
			 * cheapest-total path; we'll have to sort it later.
			 */
			if (cheapest_startup == NULL || cheapest_total == NULL)
			{
				cheapest_startup = cheapest_total =
					childrel->cheapest_total_path;
				/* Assert we do have an unparameterized path for this child */
				Assert(cheapest_total->param_info == NULL);
			}

			/*
			 * Notice whether we actually have different paths for the
			 * "cheapest" and "total" cases; frequently there will be no point
			 * in two create_merge_append_path() calls.
			 */
			if (cheapest_startup != cheapest_total)
				startup_neq_total = true;

			/*
			 * Collect the appropriate child paths.  The required logic varies
			 * for the Append and MergeAppend cases.
			 */
			if (match_partition_order)
			{
				/*
				 * We're going to make a plain Append path.  We don't need
				 * most of what accumulate_append_subpath would do, but we do
				 * want to cut out child Appends or MergeAppends if they have
				 * just a single subpath (and hence aren't doing anything
				 * useful).
				 */
				cheapest_startup = get_singleton_append_subpath(cheapest_startup);
				cheapest_total = get_singleton_append_subpath(cheapest_total);

				startup_subpaths = lappend(startup_subpaths, cheapest_startup);
				total_subpaths = lappend(total_subpaths, cheapest_total);
			}
			else if (match_partition_order_desc)
			{
				/*
				 * As above, but we need to reverse the order of the children,
				 * because nodeAppend.c doesn't know anything about reverse
				 * ordering and will scan the children in the order presented.
				 */
				cheapest_startup = get_singleton_append_subpath(cheapest_startup);
				cheapest_total = get_singleton_append_subpath(cheapest_total);

				startup_subpaths = lcons(cheapest_startup, startup_subpaths);
				total_subpaths = lcons(cheapest_total, total_subpaths);
			}
			else
			{
				/*
				 * Otherwise, rely on accumulate_append_subpath to collect the
				 * child paths for the MergeAppend.
				 */
				accumulate_append_subpath(cheapest_startup,
										  &startup_subpaths, NULL);
				accumulate_append_subpath(cheapest_total,
										  &total_subpaths, NULL);
			}
		}

		/* ... and build the Append or MergeAppend paths */
		if (match_partition_order || match_partition_order_desc)
		{
			/* We only need Append */
			add_path(rel, (Path *) create_append_path(root,
													  rel,
													  startup_subpaths,
													  NIL,
													  pathkeys,
													  NULL,
													  0,
													  false,
													  -1),
					root);
			if (startup_neq_total)
				add_path(rel, (Path *) create_append_path(root,
														  rel,
														  total_subpaths,
														  NIL,
														  pathkeys,
														  NULL,
														  0,
														  false,
														  -1),
						root);
		}
		else
		{
			/* We need MergeAppend */
			add_path(rel, (Path *) create_merge_append_path(root,
															rel,
															startup_subpaths,
															pathkeys,
															NULL),
					root);
			if (startup_neq_total)
				add_path(rel, (Path *) create_merge_append_path(root,
																rel,
																total_subpaths,
																pathkeys,
																NULL),
						root);
		}
	}
}

/*
 * get_cheapest_parameterized_child_path
 *		Get cheapest path for this relation that has exactly the requested
 *		parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
static Path *
get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
									  Relids required_outer)
{
	Path	   *cheapest;
	ListCell   *lc;

	/*
	 * Look up the cheapest existing path with no more than the needed
	 * parameterization.  If it has exactly the needed parameterization, we're
	 * done.
	 */
	cheapest = get_cheapest_path_for_pathkeys(rel->pathlist,
											  NIL,
											  required_outer,
											  TOTAL_COST,
											  false);
	Assert(cheapest != NULL);
	if (bms_equal(PATH_REQ_OUTER(cheapest), required_outer))
		return cheapest;

	/*
	 * Otherwise, we can "reparameterize" an existing path to match the given
	 * parameterization, which effectively means pushing down additional
	 * joinquals to be checked within the path's scan.  However, some existing
	 * paths might check the available joinquals already while others don't;
	 * therefore, it's not clear which existing path will be cheapest after
	 * reparameterization.  We have to go through them all and find out.
	 */
	cheapest = NULL;
	foreach(lc, rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		/* Can't use it if it needs more than requested parameterization */
		if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
			continue;

		/*
		 * Reparameterization can only increase the path's cost, so if it's
		 * already more expensive than the current cheapest, forget it.
		 */
		if (cheapest != NULL &&
			compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
			continue;

		/* Reparameterize if needed, then recheck cost */
		if (!bms_equal(PATH_REQ_OUTER(path), required_outer))
		{
			path = reparameterize_path(root, path, required_outer, 1.0);
			if (path == NULL)
				continue;		/* failed to reparameterize this one */
			Assert(bms_equal(PATH_REQ_OUTER(path), required_outer));

			if (cheapest != NULL &&
				compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
				continue;
		}

		/* We have a new best path */
		cheapest = path;
	}

	/* Return the best path, or NULL if we found no suitable candidate */
	return cheapest;
}

/*
 * accumulate_append_subpath
 *		Add a subpath to the list being built for an Append or MergeAppend.
 *
 * It's possible that the child is itself an Append or MergeAppend path, in
 * which case we can "cut out the middleman" and just add its child paths to
 * our own list.  (We don't try to do this earlier because we need to apply
 * both levels of transformation to the quals.)
 *
 * Note that if we omit a child MergeAppend in this way, we are effectively
 * omitting a sort step, which seems fine: if the parent is to be an Append,
 * its result would be unsorted anyway, while if the parent is to be a
 * MergeAppend, there's no point in a separate sort on a child.
 *
 * Normally, either path is a partial path and subpaths is a list of partial
 * paths, or else path is a non-partial plan and subpaths is a list of those.
 * However, if path is a parallel-aware Append, then we add its partial path
 * children to subpaths and the rest to special_subpaths.  If the latter is
 * NULL, we don't flatten the path at all (unless it contains only partial
 * paths).
 */
static void
accumulate_append_subpath(Path *path, List **subpaths, List **special_subpaths)
{
	if (IsA(path, AppendPath))
	{
		AppendPath *apath = (AppendPath *) path;

		if (!apath->path.parallel_aware || apath->first_partial_path == 0)
		{
			*subpaths = list_concat(*subpaths, apath->subpaths);
			return;
		}
		else if (special_subpaths != NULL)
		{
			List	   *new_special_subpaths;

			/* Split Parallel Append into partial and non-partial subpaths */
			*subpaths = list_concat(*subpaths,
									list_copy_tail(apath->subpaths,
												   apath->first_partial_path));
			new_special_subpaths =
				list_truncate(list_copy(apath->subpaths),
							  apath->first_partial_path);
			*special_subpaths = list_concat(*special_subpaths,
											new_special_subpaths);
			return;
		}
	}
	else if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *mpath = (MergeAppendPath *) path;

		*subpaths = list_concat(*subpaths, mpath->subpaths);
		return;
	}

	*subpaths = lappend(*subpaths, path);
}

/*
 * get_singleton_append_subpath
 *		Returns the single subpath of an Append/MergeAppend, or just
 *		return 'path' if it's not a single sub-path Append/MergeAppend.
 *
 * Note: 'path' must not be a parallel-aware path.
 */
static Path *
get_singleton_append_subpath(Path *path)
{
	Assert(!path->parallel_aware);

	if (IsA(path, AppendPath))
	{
		AppendPath *apath = (AppendPath *) path;

		if (list_length(apath->subpaths) == 1)
			return (Path *) linitial(apath->subpaths);
	}
	else if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *mpath = (MergeAppendPath *) path;

		if (list_length(mpath->subpaths) == 1)
			return (Path *) linitial(mpath->subpaths);
	}

	return path;
}

/*
 * set_dummy_rel_pathlist
 *	  Build a dummy path for a relation that's been excluded by constraints
 *
 * Rather than inventing a special "dummy" path type, we represent this as an
 * AppendPath with no members (see also IS_DUMMY_APPEND/IS_DUMMY_REL macros).
 *
 * (See also mark_dummy_rel, which does basically the same thing, but is
 * typically used to change a rel into dummy state after we already made
 * paths for it.)
 */
static void
set_dummy_rel_pathlist(PlannerInfo *root, RelOptInfo *rel)
{
	/* Set dummy size estimates --- we leave attr_widths[] as zeroes */
	rel->rows = 0;
	rel->reltarget->width = 0;

	/* Discard any pre-existing paths; no further need for them */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;

	/* Set up the dummy path */
	add_path(rel, (Path *) create_append_path(root, rel, NIL, NIL,
											  NIL, rel->lateral_relids,
											  0, false, -1),
			 root);

	/*
	 * We set the cheapest-path fields immediately, just in case they were
	 * pointing at some discarded path.  This is redundant when we're called
	 * from set_rel_size(), but not when called from elsewhere, and doing it
	 * twice is harmless anyway.
	 */
	set_cheapest(rel);
}

/* quick-and-dirty test to see if any joining is needed */
static bool
has_multiple_baserels(PlannerInfo *root)
{
	int			num_base_rels = 0;
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		if (brel == NULL)
			continue;

		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind == RELOPT_BASEREL)
			if (++num_base_rels > 1)
				return true;
	}
	return false;
}

/*
 * set_subquery_pathlist
 *		Generate SubqueryScan access paths for a subquery RTE
 *
 * We don't currently support generating parameterized paths for subqueries
 * by pushing join clauses down into them; it seems too expensive to re-plan
 * the subquery multiple times to consider different alternatives.
 * (XXX that could stand to be reconsidered, now that we use Paths.)
 * So the paths made here will be parameterized if the subquery contains
 * LATERAL references, otherwise not.  As long as that's true, there's no need
 * for a separate set_subquery_size phase: just make the paths right away.
 */
static void
set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	Query	   *subquery = rte->subquery;
	Relids		required_outer;
	double		tuple_fraction;
	bool		forceDistRand;
	PlannerConfig *config;
	RelOptInfo *sub_final_rel;
	ListCell   *lc;
	pushdown_safety_info safetyInfo;

	/*
	 * Must copy the Query so that planning doesn't mess up the RTE contents
	 * (really really need to fix the planner to not scribble on its input,
	 * someday ... but see remove_unused_subquery_outputs to start with).
	 */
	subquery = copyObject(subquery);

	/*
	 * If it's a LATERAL subquery, it might contain some Vars of the current
	 * query level, requiring it to be treated as parameterized, even though
	 * we don't support pushing down join quals into subqueries.
	 */
	required_outer = rel->lateral_relids;

	/*
	 * Zero out result area for subquery_is_pushdown_safe, so that it can set
	 * flags as needed while recursing.  In particular, we need a workspace
	 * for keeping track of unsafe-to-reference columns.  unsafeColumns[i]
	 * will be set true if we find that output column i of the subquery is
	 * unsafe to use in a pushed-down qual.
	 */
	memset(&safetyInfo, 0, sizeof(safetyInfo));
	safetyInfo.unsafeColumns = (bool *)
		palloc0((list_length(subquery->targetList) + 1) * sizeof(bool));

	/*
	 * If the subquery has the "security_barrier" flag, it means the subquery
	 * originated from a view that must enforce row-level security.  Then we
	 * must not push down quals that contain leaky functions.  (Ideally this
	 * would be checked inside subquery_is_pushdown_safe, but since we don't
	 * currently pass the RTE to that function, we must do it here.)
	 */
	safetyInfo.unsafeLeaky = rte->security_barrier;

	/* if IS_SINGLENODE then role must be GP_ROLE_UTILITY */
	forceDistRand = rte->forceDistRandom && Gp_role != GP_ROLE_UTILITY;
	/* CDB: Could be a preplanned subquery from window_planner. */
	if (rte->subquery_root == NULL)
	{
		/*
		 * push down quals if possible. Note subquery might be
		 * different pointer from original one.
		 */
		subquery = push_down_restrict(root, rel, rte, rti, subquery);

		/*
		 * The upper query might not use all the subquery's output columns; if
		 * not, we can simplify.
		 */
		remove_unused_subquery_outputs(subquery, rel);

		/*
		 * We can safely pass the outer tuple_fraction down to the subquery if the
		 * outer level has no joining, aggregation, or sorting to do. Otherwise
		 * we'd better tell the subquery to plan for full retrieval. (XXX This
		 * could probably be made more intelligent ...)
		 */
		if (subquery->hasAggs ||
			subquery->groupClause ||
			subquery->groupingSets ||
			subquery->havingQual ||
			subquery->distinctClause ||
			subquery->sortClause ||
			has_multiple_baserels(root))
			tuple_fraction = 0.0;	/* default case */
		else
			tuple_fraction = root->tuple_fraction;

		/* Generate a subroot and Paths for the subquery */
		config = CopyPlannerConfig(root->config);
		config->honor_order_by = false;		/* partial order is enough */

		/*
		 * CDB: if this subquery is the inner plan of a lateral
		 * join and if it contains a limit, we can only gather
		 * it to singleQE and materialize the data because we
		 * cannot pass params across motion.
		 */
		if ((!bms_is_empty(required_outer)) &&
			is_query_contain_limit_groupby(subquery))
			config->force_singleQE = true;

		/*
		 * Cloudberry specific behavior:
		 * config->may_rescan is used to guide if
		 * we should add materialize path over motion
		 * in the left tree of a join.
		 */
		config->may_rescan = config->may_rescan || !bms_is_empty(required_outer);

		/* plan_params should not be in use in current query level */
		Assert(root->plan_params == NIL);

		rel->subroot = subquery_planner(root->glob, subquery,
									root,
									false, tuple_fraction,
									config);
	}
	else
	{
		/* This is a preplanned sub-query RTE. */
		rel->subroot = rte->subquery_root;
	}

	/* Isolate the params needed by this specific subplan */
	rel->subplan_params = root->plan_params;
	root->plan_params = NIL;

	/*
	 * It's possible that constraint exclusion proved the subquery empty. If
	 * so, it's desirable to produce an unadorned dummy path so that we will
	 * recognize appropriate optimizations at this query level.
	 */
	sub_final_rel = fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL);

	if (IS_DUMMY_REL(sub_final_rel))
	{
		set_dummy_rel_pathlist(root, rel);
		return;
	}

	/*
	 * Mark rel with estimated output rows, width, etc.  Note that we have to
	 * do this before generating outer-query paths, else cost_subqueryscan is
	 * not happy.
	 */
	set_subquery_size_estimates(root, rel);

	/*
	 * For each Path that subquery_planner produced, make a SubqueryScanPath
	 * in the outer query.
	 */
	foreach(lc, sub_final_rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		Path       *path;
		List       *l;
		List	   *pathkeys;
		CdbPathLocus locus;

		if (forceDistRand)
			CdbPathLocus_MakeStrewn(&locus, getgpsegmentCount(), subpath->parallel_workers);
		else
			locus = cdbpathlocus_from_subquery(root, rel, subpath);

		/* Convert subpath's pathkeys to outer representation */
		pathkeys = convert_subquery_pathkeys(root,
											 rel,
											 subpath->pathkeys,
											 make_tlist_from_pathtarget(subpath->pathtarget));

		path = (Path *) create_subqueryscan_path(root, rel, subpath,
												 pathkeys, locus, required_outer);

		/*
		 * Cloudberry specific behavior:
		 * If the path is general or segmentgeneral locus and contains
		 * volatile target list of havingQual, we should turn it into
		 * singleQE.
		 */
		l = lappend(list_make1(subquery->havingQual), subpath->pathtarget->exprs);
		path = turn_volatile_seggen_to_singleqe(root,
												path,
												(Node *) l);

		/* Generate outer path using this subpath */
		add_path(rel, path, root);
	}

	/* If outer rel allows parallelism, do same for partial paths. */
	if (rel->consider_parallel && bms_is_empty(required_outer))
	{
		/* If consider_parallel is false, there should be no partial paths. */
		Assert(sub_final_rel->consider_parallel ||
			   sub_final_rel->partial_pathlist == NIL);

		/* Same for partial paths. */
		foreach(lc, sub_final_rel->partial_pathlist)
		{
			Path	   *subpath = (Path *) lfirst(lc);
			Path       *path;
			List	   *pathkeys;
			List       *l;
			CdbPathLocus locus;

			if (forceDistRand)
				CdbPathLocus_MakeStrewn(&locus, getgpsegmentCount(), subpath->parallel_workers);
			else
				locus = cdbpathlocus_from_subquery(root, rel, subpath);

			/* Convert subpath's pathkeys to outer representation */
			pathkeys = convert_subquery_pathkeys(root,
												 rel,
												 subpath->pathkeys,
												 make_tlist_from_pathtarget(subpath->pathtarget));

			/* Generate outer path using this subpath */
			path = (Path *) create_subqueryscan_path(root, rel, subpath,
													  pathkeys,
													  locus,
													  required_outer);
			/* turn into SingleQE if needed */
			l = lappend(list_make1(subquery->havingQual), subpath->pathtarget->exprs);
			path = turn_volatile_seggen_to_singleqe(root, path, (Node *) l);
			add_partial_path(rel, path);
		}
	}
}

/*
 * set_function_pathlist
 *		Build the (single) access path for a function RTE
 */
static void
set_function_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	List	   *pathkeys = NIL;

	/*
	 * We don't support pushing join clauses into the quals of a function
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in the function expression.
	 */
	required_outer = rel->lateral_relids;

	/*
	 * The result is considered unordered unless ORDINALITY was used, in which
	 * case it is ordered by the ordinal column (the last one).  See if we
	 * care, by checking for uses of that Var in equivalence classes.
	 */
	if (rte->funcordinality)
	{
		AttrNumber	ordattno = rel->max_attr;
		Var		   *var = NULL;
		ListCell   *lc;

		/*
		 * Is there a Var for it in rel's targetlist?  If not, the query did
		 * not reference the ordinality column, or at least not in any way
		 * that would be interesting for sorting.
		 */
		foreach(lc, rel->reltarget->exprs)
		{
			Var		   *node = (Var *) lfirst(lc);

			/* checking varno/varlevelsup is just paranoia */
			if (IsA(node, Var) &&
				node->varattno == ordattno &&
				node->varno == rel->relid &&
				node->varlevelsup == 0)
			{
				var = node;
				break;
			}
		}

		/*
		 * Try to build pathkeys for this Var with int8 sorting.  We tell
		 * build_expression_pathkey not to build any new equivalence class; if
		 * the Var isn't already mentioned in some EC, it means that nothing
		 * cares about the ordering.
		 */
		if (var)
			pathkeys = build_expression_pathkey(root,
												(Expr *) var,
												NULL,	/* below outer joins */
												Int8LessOperator,
												rel->relids,
												false);
	}

	/* Generate appropriate path */
	add_path(rel, create_functionscan_path(root, rel, rte,
										   pathkeys, required_outer),
			 root);
}

/*
 * set_tablefunction_pathlist
 *		Build the (single) access path for a table function RTE
 */
static void
set_tablefunction_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	PlannerConfig *config;
	RangeTblFunction *rtfunc;
	FuncExpr   *fexpr;
	ListCell   *arg;
	RelOptInfo *sub_final_rel;
	Relids		required_outer;
	ListCell   *lc;

	/* Cannot be a preplanned subquery from window_planner. */
	Assert(!rte->subquery_root);

	Assert(list_length(rte->functions) == 1);
	rtfunc = (RangeTblFunction *) linitial(rte->functions);
	Assert(rtfunc->funcexpr && IsA(rtfunc->funcexpr, FuncExpr));
	fexpr= (FuncExpr *) rtfunc->funcexpr;

	/*
	 * We don't support pushing join clauses into the quals of a function
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in the function expression.
	 */
	required_outer = rel->lateral_relids;

	config = CopyPlannerConfig(root->config);
	config->honor_order_by = false;		/* partial order is enough */

	/* Plan input subquery */
	rel->subroot = subquery_planner(root->glob, rte->subquery, root,
									false,
									0.0, //tuple_fraction
									config);

	/*
	 * It's possible that constraint exclusion proved the subquery empty. If
	 * so, it's desirable to produce an unadorned dummy path so that we will
	 * recognize appropriate optimizations at this query level.
	 */
	sub_final_rel = fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL);

	/*
	 * With the subquery planned we now need to clear the subquery from the
	 * TableValueExpr nodes, otherwise preprocess_expression will trip over
	 * it.
	 */
	foreach(arg, fexpr->args)
	{
		if (IsA(arg, TableValueExpr))
		{
			TableValueExpr *tve = (TableValueExpr *) arg;

			tve->subquery = NULL;
		}
	}

	/* Mark rel with estimated output rows, width, etc */
	set_table_function_size_estimates(root, rel);

	/*
	 * For each Path that subquery_planner produced, make a SubqueryScanPath
	 * in the outer query.
	 */
	foreach(lc, sub_final_rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		List	   *pathkeys;

		/* Convert subpath's pathkeys to outer representation */
		pathkeys = convert_subquery_pathkeys(root,
											 rel,
											 subpath->pathkeys,
							make_tlist_from_pathtarget(subpath->pathtarget));

		/* Generate appropriate path */
		add_path(rel, (Path *)
				 create_tablefunction_path(root, rel, subpath,
										   pathkeys, required_outer),
				 root);
	}
}

/*
 * set_values_pathlist
 *		Build the (single) access path for a VALUES RTE
 */
static void
set_values_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a values scan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in the values expressions.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_valuesscan_path(root, rel, rte, required_outer), root);
}

/*
 * set_tablefunc_pathlist
 *		Build the (single) access path for a table func RTE
 */
static void
set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a tablefunc
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in the function expression.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_tablefuncscan_path(root, rel,
											required_outer),
			 root);
}

/*
 * set_cte_pathlist
 *		Build the (single) access path for a non-self-reference CTE RTE
 *
 * There's no need for a separate set_cte_size phase, since we don't
 * support join-qual-parameterized paths for CTEs.
 */
static void
set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	PlannerInfo *cteroot;
	Index		levelsup;
	int			ndx;
	ListCell   *lc;
	int			planinfo_id;
	CommonTableExpr *cte = NULL;
	double		tuple_fraction = 0.0;
	CtePlanInfo *cteplaninfo;
	List	   *pathkeys = NULL;
	PlannerInfo *subroot = NULL;
	RelOptInfo *sub_final_rel;
	Relids		required_outer;
	bool		is_shared;
	Query           *subquery = NULL;
	bool            contain_volatile_function = false;

	/*
	 * Find the referenced CTE based on the given range table entry
	 */
	levelsup = rte->ctelevelsup;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}

	ndx = 0;
	foreach(lc, cteroot->parse->cteList)
	{
		cte = (CommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			break;
		ndx++;
	}
	if (lc == NULL)				/* shouldn't happen */
		elog(ERROR, "could not find CTE \"%s\"", rte->ctename);

	Assert(IsA(cte->ctequery, Query));
	/*
	 * Copy query node since subquery_planner may trash it, and we need it
	 * intact in case we need to create another plan for the CTE
	 */
	subquery = (Query *) copyObject(cte->ctequery);
	contain_volatile_function = contain_volatile_functions((Node *) subquery);

	/*
	 * In PostgreSQL, we use the index to look up the plan ID in the
	 * cteroot->cte_plan_ids list. In GPDB, CTE plans work differently, and
	 * we look up the CtePlanInfo struct in the list_cteplaninfo instead.
	 */
	planinfo_id = ndx;

	/*
	 * Determine whether we need to generate a new subplan for this CTE.
	 *
	 * There are the following cases:
	 *   (1) If this subquery can be pulled up as an InitPlan, we will
	 *       generate a new subplan. In InitPlan case, the subplan can
	 *       not be shared with the main query or other InitPlans. We
	 *       do not store this subplan in cteplaninfo.
	 *   (2) If we never generate a subplan for this CTE, then we generate
	 *       one. If the reference count for this CTE is greater than 1
	 *       (excluding ones used in InitPlans), we create multiple subplans,
	 *       each of which has a SharedNode on top. We store these subplans
	 *       in cteplaninfo so that they can be used later.
	 */
	Assert(list_length(cteroot->list_cteplaninfo) > planinfo_id);
	cteplaninfo = list_nth(cteroot->list_cteplaninfo, planinfo_id);

	/*
	 * If there is exactly one reference to this CTE in the query, or plan
	 * sharing is disabled, create a new subplan for this CTE. It will
	 * become simple subquery scan.
	 *
	 * NOTE: The check for "exactly one reference" is a bit fuzzy. The
	 * references are counted in parse analysis phase, and it's possible
	 * that we duplicate a reference during query planning. So the check
	 * for number of references must be treated merely as a hint. If it
	 * turns out that there are in fact multiple references to the same
	 * CTE, even though we thought that there is only one, we might choose
	 * a sub-optimal plan because we missed the opportunity to share the
	 * subplan. That's acceptable for now.
	 *
	 * subquery tree will be modified if any qual is pushed down.
	 * There's risk that it'd be confusing if the tree is used
	 * later. At the moment InitPlan case uses the tree, but it
	 * is called earlier than this pass always, so we don't avoid it.
	 *
	 * Also, we might want to think extracting "common"
	 * qual expressions between multiple references, but
	 * so far we don't support it.
	 */

	switch (cte->ctematerialized)
	{
		case CTEMaterializeNever:
			is_shared = false;
			break;
		case CTEMaterializeAlways:
			is_shared = true;
			break;
		default:
			/* if plan sharing is enabled and contains volatile functions in the CTE query, also generate a shared scan plan */
			is_shared =  root->config->gp_cte_sharing && (cte->cterefcount > 1 || contain_volatile_function);

	}

	/*
	 * since shareinputscan with outer refs is not supported by GPDB, if
	 * contain outer self references, the cte need to be inlined.
	 */
	if (is_shared && contain_outer_selfref(cte->ctequery))
		is_shared = false;

	if (!is_shared)
	{
		PlannerConfig *config = CopyPlannerConfig(root->config);

		/*
		 * Having multiple SharedScans can lead to deadlocks. For now,
		 * disallow sharing of ctes at lower levels.
		 */
		config->gp_cte_sharing = false;

		config->honor_order_by = false;

		if (!cte->cterecursive && subquery->commandType == CMD_SELECT)
		{
			/*
			 * Adjust the subquery so that 'root', i.e. this subquery, is the
			 * parent of the CTE subquery, even though the CTE might've been
			 * higher up syntactically. This is because some of the quals that
			 * we push down might refer to relations between the current level
			 * and the CTE's syntactical level. Such relations are not visible
			 * at the CTE's syntactical level, and SS_finalize_plan() would
			 * throw an error on them.
			 */
			IncrementVarSublevelsUp((Node *) subquery, rte->ctelevelsup, 1);

			/*
			 * Push down quals, like we do in set_subquery_pathlist()
			 *
			 * If the subquery contains volatile functions, like we prevent inlining
			 * when gp_cte_sharing is enables, we don't push down quals when gp_cte_sharing
			 * is disabled either, as push down may cause wrong results.
			 */
			if (!contain_volatile_function)
				subquery = push_down_restrict(root, rel, rte, rel->relid, subquery);

			subroot = subquery_planner(cteroot->glob, subquery, root,
									   cte->cterecursive,
									   tuple_fraction, config);
		}
		else
		{
			subroot = subquery_planner(cteroot->glob, subquery, cteroot,
									   cte->cterecursive,
									   tuple_fraction, config);
		}

		/*
		 * Do not store the subplan in cteplaninfo, since we will not share
		 * this plan.
		 */
	}
	else
	{
		/*
		 * If we haven't created a subplan for this CTE yet, do it now. This
		 * subplan will not be used by InitPlans, so that they can be shared
		 * if this CTE is referenced multiple times (excluding in InitPlans).
		 */
		if (cteplaninfo->subroot == NULL)
		{
			PlannerConfig *config = CopyPlannerConfig(root->config);

			/*
			 * Having multiple SharedScans can lead to deadlocks. For now,
			 * disallow sharing of ctes at lower levels.
			 */
			config->gp_cte_sharing = false;

			config->honor_order_by = false;

			subroot = subquery_planner(cteroot->glob, subquery, cteroot, cte->cterecursive,
									   tuple_fraction, config);

			/* Select best Path and turn it into a Plan */
			sub_final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);

			/*
			 * we cannot use different plans for different instances of this CTE
			 * reference, so keep only the cheapest
			 */
			sub_final_rel->pathlist = list_make1(sub_final_rel->cheapest_total_path);

			cteplaninfo->subroot = subroot;
		}
		else
			subroot = cteplaninfo->subroot;
	}
	rel->subroot = subroot;

	/*
	 * It's possible that constraint exclusion proved the subquery empty. If
	 * so, it's desirable to produce an unadorned dummy path so that we will
	 * recognize appropriate optimizations at this query level.
	 */
	sub_final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);

	if (IS_DUMMY_REL(sub_final_rel))
	{
		set_dummy_rel_pathlist(root, rel);
		return;
	}

	pathkeys = subroot->query_pathkeys;

	/* Mark rel with estimated output rows, width, etc */
	{
		double		numsegments;
		double		sub_total_rows;

		if (CdbPathLocus_IsPartitioned(sub_final_rel->cheapest_total_path->locus))
			numsegments = CdbPathLocus_NumSegments(sub_final_rel->cheapest_total_path->locus);
		else
			numsegments = 1;
		sub_total_rows = sub_final_rel->cheapest_total_path->rows * numsegments;

		set_cte_size_estimates(root, rel, sub_total_rows);
	}

	/*
	 * We don't support pushing join clauses into the quals of a CTE scan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/*
	 * For each Path that subquery_planner produced, make a CteScanPath
	 * in the outer query.
	 */
	foreach(lc, sub_final_rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		List	   *pathkeys;
		CdbPathLocus locus;

		locus = cdbpathlocus_from_subquery(root, rel, subpath);

		/* Convert subquery pathkeys to outer representation */
		pathkeys = convert_subquery_pathkeys(root, rel, subpath->pathkeys,
											 make_tlist_from_pathtarget(subpath->pathtarget));

		/* Generate appropriate path */
		add_path(rel, create_ctescan_path(root,
										  rel,
										  is_shared ? NULL : subpath,
										  locus,
										  pathkeys,
										  required_outer),
				 root);
	}
}

/*
 * set_namedtuplestore_pathlist
 *		Build the (single) access path for a named tuplestore RTE
 *
 * There's no need for a separate set_namedtuplestore_size phase, since we
 * don't support join-qual-parameterized paths for tuplestores.
 */
static void
set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte)
{
	Relids		required_outer;
	Relation	relation = NULL;

	relation = relation_open(rte->relid, NoLock);
	/* Mark rel with estimated output rows, width, etc */
	set_namedtuplestore_size_estimates(root, rel);

	/*
	 * We don't support pushing join clauses into the quals of a tuplestore
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in its tlist.
	 */
	required_outer = rel->lateral_relids;
	/* Use base table or matview's policy */
	if (rel->cdbpolicy == NULL)
		rel->cdbpolicy = relation->rd_cdbpolicy;

	/* Generate appropriate path */
	add_path(rel, create_namedtuplestorescan_path(root, rel, required_outer), root);

	relation_close(relation, NoLock);
	/* Select cheapest path (pretty easy in this case...) */
	set_cheapest(rel);
}

/*
 * set_result_pathlist
 *		Build the (single) access path for an RTE_RESULT RTE
 *
 * There's no need for a separate set_result_size phase, since we
 * don't support join-qual-parameterized paths for these RTEs.
 */
static void
set_result_pathlist(PlannerInfo *root, RelOptInfo *rel,
					RangeTblEntry *rte)
{
	Relids		required_outer;

	/* Mark rel with estimated output rows, width, etc */
	set_result_size_estimates(root, rel);

	/*
	 * We don't support pushing join clauses into the quals of a Result scan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_resultscan_path(root, rel, required_outer), root);

	/* Select cheapest path (pretty easy in this case...) */
	set_cheapest(rel);
}

/*
 * set_worktable_pathlist
 *		Build the (single) access path for a self-reference CTE RTE
 *
 * There's no need for a separate set_worktable_size phase, since we don't
 * support join-qual-parameterized paths for CTEs.
 */
static void
set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Path	   *ctepath;
	PlannerInfo *cteroot;
	Index		levelsup;
	Relids		required_outer;

	/*
	 * We need to find the non-recursive term's path, which is in the plan
	 * level that's processing the recursive UNION, which is one level *below*
	 * where the CTE comes from.
	 */
	levelsup = rte->ctelevelsup;
	if (levelsup == 0)			/* shouldn't happen */
		elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	levelsup--;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	ctepath = cteroot->non_recursive_path;
	if (!ctepath)				/* shouldn't happen */
		elog(ERROR, "could not find path for CTE \"%s\"", rte->ctename);

	/* Mark rel with estimated output rows, width, etc */
	set_cte_size_estimates(root, rel, ctepath->rows);

	/*
	 * We don't support pushing join clauses into the quals of a worktable
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in its tlist.  (I'm not sure this is actually possible given the
	 * restrictions on recursive references, but it's easy enough to support.)
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_worktablescan_path(root, rel, ctepath->locus, required_outer), root);
}

/*
 * generate_gather_paths
 *		Generate parallel access paths for a relation by pushing a Gather or
 *		Gather Merge on top of a partial path.
 *
 * This must not be called until after we're done creating all partial paths
 * for the specified relation.  (Otherwise, add_partial_path might delete a
 * path that some GatherPath or GatherMergePath has a reference to.)
 *
 * If we're generating paths for a scan or join relation, override_rows will
 * be false, and we'll just use the relation's size estimate.  When we're
 * being called for a partially-grouped path, though, we need to override
 * the rowcount estimate.  (It's not clear that the particular value we're
 * using here is actually best, but the underlying rel has no estimate so
 * we must do something.)
 */
void
generate_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
{
	Assert(false);
	Path	   *cheapest_partial_path;
	Path	   *simple_gather_path;
	ListCell   *lc;
	double		rows;
	double	   *rowsp = NULL;

	/* If there are no partial paths, there's nothing to do here. */
	if (rel->partial_pathlist == NIL)
		return;

	/* Should we override the rel's rowcount estimate? */
	if (override_rows)
		rowsp = &rows;

	/*
	 * The output of Gather is always unsorted, so there's only one partial
	 * path of interest: the cheapest one.  That will be the one at the front
	 * of partial_pathlist because of the way add_partial_path works.
	 */
	cheapest_partial_path = linitial(rel->partial_pathlist);

	if (!cheapest_partial_path->parallel_safe)
		return;

	rows =
		cheapest_partial_path->rows * cheapest_partial_path->parallel_workers;
	simple_gather_path = (Path *)
		create_gather_path(root, rel, cheapest_partial_path, rel->reltarget,
						   NULL, rowsp);
	add_path(rel, simple_gather_path, root);

	/*
	 * For each useful ordering, we can consider an order-preserving Gather
	 * Merge.
	 */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		GatherMergePath *path;

		if (subpath->pathkeys == NIL)
			continue;

		rows = subpath->rows * subpath->parallel_workers;
		path = create_gather_merge_path(root, rel, subpath, rel->reltarget,
										subpath->pathkeys, NULL, rowsp);
		add_path(rel, &path->path, root);
	}
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 *
 * This allows us to do incremental sort on top of an index scan under a gather
 * merge node, i.e. parallelized.
 *
 * If the require_parallel_safe is true, we also require the expressions to
 * be parallel safe (which allows pushing the sort below Gather Merge).
 *
 * XXX At the moment this can only ever return a list with a single element,
 * because it looks at query_pathkeys only. So we might return the pathkeys
 * directly, but it seems plausible we'll want to consider other orderings
 * in the future. For example, we might want to consider pathkeys useful for
 * merge joins.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel,
								 bool require_parallel_safe)
{
	List	   *useful_pathkeys_list = NIL;

	/*
	 * Considering query_pathkeys is always worth it, because it might allow
	 * us to avoid a total sort when we have a partially presorted path
	 * available or to push the total sort into the parallel portion of the
	 * query.
	 */
	if (root->query_pathkeys)
	{
		ListCell   *lc;
		int			npathkeys = 0;	/* useful pathkeys */

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;

			/*
			 * We can only build a sort for pathkeys that contain a
			 * safe-to-compute-early EC member computable from the current
			 * relation's reltarget, so ignore the remainder of the list as
			 * soon as we find a pathkey without such a member.
			 *
			 * It's still worthwhile to return any prefix of the pathkeys list
			 * that meets this requirement, as we may be able to do an
			 * incremental sort.
			 *
			 * If requested, ensure the sort expression is parallel-safe too.
			 */
			if (!relation_can_be_sorted_early(root, rel, pathkey_ec,
											  require_parallel_safe))
				break;

			npathkeys++;
		}

		/*
		 * The whole query_pathkeys list matches, so append it directly, to
		 * allow comparing pathkeys easily by comparing list pointer. If we
		 * have to truncate the pathkeys, we gotta do a copy though.
		 */
		if (npathkeys == list_length(root->query_pathkeys))
			useful_pathkeys_list = lappend(useful_pathkeys_list,
										   root->query_pathkeys);
		else if (npathkeys > 0)
			useful_pathkeys_list = lappend(useful_pathkeys_list,
										   list_truncate(list_copy(root->query_pathkeys),
														 npathkeys));
	}

	return useful_pathkeys_list;
}

/*
 * generate_useful_gather_paths
 *		Generate parallel access paths for a relation by pushing a Gather or
 *		Gather Merge on top of a partial path.
 *
 * Unlike plain generate_gather_paths, this looks both at pathkeys of input
 * paths (aiming to preserve the ordering), but also considers ordering that
 * might be useful for nodes above the gather merge node, and tries to add
 * a sort (regular or incremental) to provide that.
 */
void
generate_useful_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
{
	Assert(false);
	ListCell   *lc;
	double		rows;
	double	   *rowsp = NULL;
	List	   *useful_pathkeys_list = NIL;
	Path	   *cheapest_partial_path = NULL;

	/* If there are no partial paths, there's nothing to do here. */
	if (rel->partial_pathlist == NIL)
		return;

	/* Should we override the rel's rowcount estimate? */
	if (override_rows)
		rowsp = &rows;

	/* generate the regular gather (merge) paths */
	generate_gather_paths(root, rel, override_rows);

	/* consider incremental sort for interesting orderings */
	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel, true);

	/* used for explicit (full) sort paths */
	cheapest_partial_path = linitial(rel->partial_pathlist);

	/*
	 * Consider sorted paths for each interesting ordering. We generate both
	 * incremental and full sort.
	 */
	foreach(lc, useful_pathkeys_list)
	{
		List	   *useful_pathkeys = lfirst(lc);
		ListCell   *lc2;
		bool		is_sorted;
		int			presorted_keys;

		foreach(lc2, rel->partial_pathlist)
		{
			Path	   *subpath = (Path *) lfirst(lc2);
			GatherMergePath *path;

			is_sorted = pathkeys_count_contained_in(useful_pathkeys,
													subpath->pathkeys,
													&presorted_keys);

			/*
			 * We don't need to consider the case where a subpath is already
			 * fully sorted because generate_gather_paths already creates a
			 * gather merge path for every subpath that has pathkeys present.
			 *
			 * But since the subpath is already sorted, we know we don't need
			 * to consider adding a sort (other either kind) on top of it, so
			 * we can continue here.
			 */
			if (is_sorted)
				continue;

			/*
			 * Consider regular sort for the cheapest partial path (for each
			 * useful pathkeys). We know the path is not sorted, because we'd
			 * not get here otherwise.
			 *
			 * This is not redundant with the gather paths created in
			 * generate_gather_paths, because that doesn't generate ordered
			 * output. Here we add an explicit sort to match the useful
			 * ordering.
			 */
			if (cheapest_partial_path == subpath)
			{
				Path	   *tmp;

				tmp = (Path *) create_sort_path(root,
												rel,
												subpath,
												useful_pathkeys,
												-1.0);

				rows = tmp->rows * tmp->parallel_workers;

				path = create_gather_merge_path(root, rel,
												tmp,
												rel->reltarget,
												tmp->pathkeys,
												NULL,
												rowsp);

				add_path(rel, &path->path, root);

				/* Fall through */
			}

			/*
			 * Consider incremental sort, but only when the subpath is already
			 * partially sorted on a pathkey prefix.
			 */
			if (enable_incremental_sort && presorted_keys > 0)
			{
				Path	   *tmp;

				/*
				 * We should have already excluded pathkeys of length 1
				 * because then presorted_keys > 0 would imply is_sorted was
				 * true.
				 */
				Assert(list_length(useful_pathkeys) != 1);

				tmp = (Path *) create_incremental_sort_path(root,
															rel,
															subpath,
															useful_pathkeys,
															presorted_keys,
															-1);

				path = create_gather_merge_path(root, rel,
												tmp,
												rel->reltarget,
												tmp->pathkeys,
												NULL,
												rowsp);

				add_path(rel, &path->path, root);
			}
		}
	}
}

/*
 * generate_grouping_paths
 * 		Create partially aggregated paths and add them to grouped relation.
 *
 * "rel_plain" is base or join relation whose paths are not grouped.
 */
void
generate_grouping_paths(PlannerInfo *root, RelOptInfo *rel_grouped,
						RelOptInfo *rel_plain, RelAggInfo *agg_info)
{
	ListCell   *lc;
	Path	   *path;

	if (IS_DUMMY_REL(rel_plain))
	{
		mark_dummy_rel(root, rel_grouped);
		return;
	}

	foreach(lc, rel_plain->pathlist)
	{
		path = (Path *) lfirst(lc);

		/*
		 * Since the path originates from the non-grouped relation which is
		 * not aware of the aggregate push-down, we must ensure that it
		 * provides the correct input for aggregation.
		 */
		path = (Path *) create_projection_path_with_quals(root,
														  rel_grouped,
														  path,
														  agg_info->agg_input,
														  NIL,
														  true);

		/*
		 * add_grouped_path() will check whether the path has suitable
		 * pathkeys.
		 */
		add_grouped_path(root, rel_grouped, path, AGG_SORTED, agg_info,
						 false);

		/*
		 * Repeated creation of hash table (for new parameter values) should
		 * be possible, does not sound like a good idea in terms of efficiency.
		 */
		if (path->param_info == NULL)
			add_grouped_path(root, rel_grouped, path, AGG_HASHED, agg_info,
							 false);
	}

	/* Could not generate any grouped paths? */
	if (rel_grouped->pathlist == NIL)
	{
		mark_dummy_rel(root, rel_grouped);
		return;
	}

	/*
	 * Almost the same for partial paths.
	 *
	 * The difference is that parameterized paths are never created, see
	 * add_partial_path() for explanation.
	 */
	foreach(lc, rel_plain->partial_pathlist)
	{
		path = (Path *) lfirst(lc);

		if (path->param_info != NULL)
			continue;

		path = (Path *) create_projection_path(root, rel_grouped, path,
											   agg_info->agg_input);

		add_grouped_path(root, rel_grouped, path, AGG_SORTED, agg_info,
						 true);
		add_grouped_path(root, rel_grouped, path, AGG_HASHED, agg_info,
						 true);
	}
}

/*
 * Apply partial aggregation to a subpath and add the AggPath to the pathlist.
 */
static void
add_grouped_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				 AggStrategy aggstrategy, RelAggInfo *agg_info,
				 bool partial)
{
	Path	   *agg_path;


	if (aggstrategy == AGG_HASHED)
		agg_path = (Path *) create_agg_hashed_path(root, rel, subpath,
												   agg_info);
	else if (aggstrategy == AGG_SORTED)
		agg_path = (Path *) create_agg_sorted_path(root, rel, subpath,
												   agg_info);
	else
		elog(ERROR, "unexpected strategy %d", aggstrategy);

	/* Add the grouped path to the list of grouped base paths. */
	if (agg_path != NULL)
	{
		if (!partial)
			add_path(rel, (Path *) agg_path, root);
		else
			add_partial_path(rel, (Path *) agg_path);
	}
}

/*
 * make_rel_from_joinlist
 *	  Build access paths using a "joinlist" to guide the join path search.
 *
 * See comments for deconstruct_jointree() for definition of the joinlist
 * data structure.
 */
static RelOptInfo *
make_rel_from_joinlist(PlannerInfo *root, List *joinlist)
{
	int			levels_needed;
	List	   *initial_rels;
	ListCell   *jl;

	/*
	 * Count the number of child joinlist nodes.  This is the depth of the
	 * dynamic-programming algorithm we must employ to consider all ways of
	 * joining the child nodes.
	 */
	levels_needed = list_length(joinlist);

	if (levels_needed <= 0)
		return NULL;			/* nothing to do? */

	/*
	 * Construct a list of rels corresponding to the child joinlist nodes.
	 * This may contain both base rels and rels constructed according to
	 * sub-joinlists.
	 */
	initial_rels = NIL;
	foreach(jl, joinlist)
	{
		Node	   *jlnode = (Node *) lfirst(jl);
		RelOptInfo *thisrel;

		if (IsA(jlnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jlnode)->rtindex;

			thisrel = find_base_rel(root, varno);
		}
		else if (IsA(jlnode, List))
		{
			/* Recurse to handle subproblem */
			thisrel = make_rel_from_joinlist(root, (List *) jlnode);
		}
		else
		{
			elog(ERROR, "unrecognized joinlist node type: %d",
				 (int) nodeTag(jlnode));
			thisrel = NULL;		/* keep compiler quiet */
		}

		initial_rels = lappend(initial_rels, thisrel);
	}

	if (levels_needed == 1)
	{
		/*
		 * Single joinlist node, so we're done.
		 */
		RelOptInfo *rel = (RelOptInfo *) linitial(initial_rels);

		if (bms_equal(rel->relids, root->all_baserels) && root->is_correlated_subplan)
		{
			/*
			 * if the relation had any "outer restrictinfos", we dealt with them
			 * already.
			 */
			bring_to_outer_query(root, rel, NIL);
		 	/* CBDB_PARALLEL_FIXME: enable parallel outer query? */
		}

		return rel;
	}
	else
	{
		/*
		 * Consider the different orders in which we could join the rels,
		 * using a plugin, GEQO, or the regular join search code.
		 *
		 * We put the initial_rels list into a PlannerInfo field because
		 * has_legal_joinclause() needs to look at it (ugly :-().
		 */
		root->initial_rels = initial_rels;

		if (join_search_hook)
			return (*join_search_hook) (root, levels_needed, initial_rels);
		else
			return standard_join_search(root, levels_needed, initial_rels);
	}
}

/*
 * standard_join_search
 *	  Find possible joinpaths for a query by successively finding ways
 *	  to join component relations into join relations.
 *
 * 'levels_needed' is the number of iterations needed, ie, the number of
 *		independent jointree items in the query.  This is > 1.
 *
 * 'initial_rels' is a list of RelOptInfo nodes for each independent
 *		jointree item.  These are the components to be joined together.
 *		Note that levels_needed == list_length(initial_rels).
 *
 * Returns the final level of join relations, i.e., the relation that is
 * the result of joining all the original relations together.
 * At least one implementation path must be provided for this relation and
 * all required sub-relations.
 *
 * To support loadable plugins that modify planner behavior by changing the
 * join searching algorithm, we provide a hook variable that lets a plugin
 * replace or supplement this function.  Any such hook must return the same
 * final join relation as the standard code would, but it might have a
 * different set of implementation paths attached, and only the sub-joinrels
 * needed for these paths need have been instantiated.
 *
 * Note to plugin authors: the functions invoked during standard_join_search()
 * modify root->join_rel_list and root->join_rel_hash.  If you want to do more
 * than one join-order search, you'll probably need to save and restore the
 * original states of those data structures.  See geqo_eval() for an example.
 */
RelOptInfo *
standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	int			lev;
	RelOptInfo *rel;
	RelOptInfo *rel_grouped;

	/*
	 * This function cannot be invoked recursively within any one planning
	 * problem, so join_rel_level[] can't be in use already.
	 */
	Assert(root->join_rel_level == NULL);

	/*
	 * We employ a simple "dynamic programming" algorithm: we first find all
	 * ways to build joins of two jointree items, then all ways to build joins
	 * of three items (from two-item joins and single items), then four-item
	 * joins, and so on until we have considered all ways to join all the
	 * items into one rel.
	 *
	 * root->join_rel_level[j] is a list of all the j-item rels.  Initially we
	 * set root->join_rel_level[1] to represent all the single-jointree-item
	 * relations.
	 */
	root->join_rel_level = (List **) palloc0((levels_needed + 1) * sizeof(List *));

	root->join_rel_level[1] = initial_rels;

	for (lev = 2; lev <= levels_needed; lev++)
	{
		ListCell   *lc;

		/*
		 * Determine all possible pairs of relations to be joined at this
		 * level, and build paths for making each one from every available
		 * pair of lower-level relations.
		 */
		join_search_one_level(root, lev);

		/*
		 * Run generate_partitionwise_join_paths() and
		 * generate_useful_gather_paths() for each just-processed joinrel.  We
		 * could not do this earlier because both regular and partial paths
		 * can get added to a particular joinrel at multiple times within
		 * join_search_one_level.
		 *
		 * After that, we're done creating paths for the joinrel, so run
		 * set_cheapest().
		 */
		foreach(lc, root->join_rel_level[lev])
		{
			rel = (RelOptInfo *) lfirst(lc);

			/* Create paths for partitionwise joins. */
			generate_partitionwise_join_paths(root, rel);

			/*
			 * Except for the topmost scan/join rel, consider gathering
			 * partial paths.  We'll do the same for the topmost scan/join rel
			 * once we know the final targetlist (see grouping_planner).
			 */
#if 0
			if (lev < levels_needed)
				generate_useful_gather_paths(root, rel, false);
#endif

			if (bms_equal(rel->relids, root->all_baserels) && root->is_correlated_subplan)
			{
				bring_to_outer_query(root, rel, NIL);
		 		/* CBDB_PARALLEL_FIXME: enable parallel outer query? */
			}

			/* Find and save the cheapest paths for this rel */
			set_cheapest(rel);

			/* The same for grouped relation */
			rel_grouped = find_grouped_rel(root, rel->relids);
			if (rel_grouped)
				set_cheapest(rel_grouped);

#ifdef OPTIMIZER_DEBUG
			debug_print_rel(root, rel);
			if (rel_grouped)
				debug_print_rel(root, rel_grouped);
#endif
		}
	}

	/*
	 * We should have a single rel at the final level.
	 */
	if (root->join_rel_level[levels_needed] == NIL)
		elog(ERROR, "failed to build any %d-way joins", levels_needed);
	Assert(list_length(root->join_rel_level[levels_needed]) == 1);

	rel = (RelOptInfo *) linitial(root->join_rel_level[levels_needed]);

	root->join_rel_level = NULL;

	return rel;
}

/*****************************************************************************
 *			PUSHING QUALS DOWN INTO SUBQUERIES
 *****************************************************************************/

/*
 * push_down_restrict
 *   push down restrictinfo to subquery if any.
 *
 * If there are any restriction clauses that have been attached to the
 * subquery relation, consider pushing them down to become WHERE or HAVING
 * quals of the subquery itself.  This transformation is useful because it
 * may allow us to generate a better plan for the subquery than evaluating
 * all the subquery output rows and then filtering them.
 *
 * There are several cases where we cannot push down clauses. Restrictions
 * involving the subquery are checked by subquery_is_pushdown_safe().
 * Restrictions on individual clauses are checked by
 * qual_is_pushdown_safe().  Also, we don't want to push down
 * pseudoconstant clauses; better to have the gating node above the
 * subquery.
 *
 * Non-pushed-down clauses will get evaluated as qpquals of the
 * SubqueryScan node.
 *
 * XXX Are there any cases where we want to make a policy decision not to
 * push down a pushable qual, because it'd result in a worse plan?
 */
static Query *
push_down_restrict(PlannerInfo *root, RelOptInfo *rel,
				   RangeTblEntry *rte, Index rti, Query *subquery)
{
	pushdown_safety_info safetyInfo;

	/* Nothing to do here if it doesn't have qual at all */
	if (rel->baserestrictinfo == NIL)
		return subquery;

	/*
	 * Zero out result area for subquery_is_pushdown_safe, so that it can set
	 * flags as needed while recursing.  In particular, we need a workspace
	 * for keeping track of unsafe-to-reference columns.  unsafeColumns[i]
	 * will be set true if we find that output column i of the subquery is
	 * unsafe to use in a pushed-down qual.
	 */
	memset(&safetyInfo, 0, sizeof(safetyInfo));
	safetyInfo.unsafeColumns = (bool *)
		palloc0((list_length(subquery->targetList) + 1) * sizeof(bool));

	/*
	 * If the subquery has the "security_barrier" flag, it means the subquery
	 * originated from a view that must enforce row-level security.  Then we
	 * must not push down quals that contain leaky functions.  (Ideally this
	 * would be checked inside subquery_is_pushdown_safe, but since we don't
	 * currently pass the RTE to that function, we must do it here.)
	 */
	safetyInfo.unsafeLeaky = rte->security_barrier;

	if (subquery_is_pushdown_safe(subquery, subquery, &safetyInfo))
	{
		/* OK to consider pushing down individual quals */
		List	   *upperrestrictlist = NIL;
		ListCell   *l;

		foreach(l, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
			Node	   *clause = (Node *) rinfo->clause;

			if (!rinfo->pseudoconstant &&
				qual_is_pushdown_safe(subquery, rti, rinfo, &safetyInfo))
			{
				/* Push it down */
				subquery_push_qual(subquery, rte, rti, clause);
			}
			else
			{
				/* Keep it in the upper query */
				upperrestrictlist = lappend(upperrestrictlist, rinfo);
			}
		}
		rel->baserestrictinfo = upperrestrictlist;
		/* We don't bother recomputing baserestrict_min_security */
	}

	pfree(safetyInfo.unsafeColumns);

	return subquery;
}

/*
 * subquery_is_pushdown_safe - is a subquery safe for pushing down quals?
 *
 * subquery is the particular component query being checked.  topquery
 * is the top component of a set-operations tree (the same Query if no
 * set-op is involved).
 *
 * Conditions checked here:
 *
 * 1. If the subquery has a LIMIT clause, we must not push down any quals,
 * since that could change the set of rows returned.
 *
 * 2. If the subquery contains EXCEPT or EXCEPT ALL set ops we cannot push
 * quals into it, because that could change the results.
 *
 * 3. If the subquery uses DISTINCT, we cannot push volatile quals into it.
 * This is because upper-level quals should semantically be evaluated only
 * once per distinct row, not once per original row, and if the qual is
 * volatile then extra evaluations could change the results.  (This issue
 * does not apply to other forms of aggregation such as GROUP BY, because
 * when those are present we push into HAVING not WHERE, so that the quals
 * are still applied after aggregation.)
 *
 * 4. If the subquery contains window functions, we cannot push volatile quals
 * into it.  The issue here is a bit different from DISTINCT: a volatile qual
 * might succeed for some rows of a window partition and fail for others,
 * thereby changing the partition contents and thus the window functions'
 * results for rows that remain.
 *
 * 5. If the subquery contains any set-returning functions in its targetlist,
 * we cannot push volatile quals into it.  That would push them below the SRFs
 * and thereby change the number of times they are evaluated.  Also, a
 * volatile qual could succeed for some SRF output rows and fail for others,
 * a behavior that cannot occur if it's evaluated before SRF expansion.
 *
 * 6. If the subquery has nonempty grouping sets, we cannot push down any
 * quals.  The concern here is that a qual referencing a "constant" grouping
 * column could get constant-folded, which would be improper because the value
 * is potentially nullable by grouping-set expansion.  This restriction could
 * be removed if we had a parsetree representation that shows that such
 * grouping columns are not really constant.  (There are other ideas that
 * could be used to relax this restriction, but that's the approach most
 * likely to get taken in the future.  Note that there's not much to be gained
 * so long as subquery_planner can't move HAVING clauses to WHERE within such
 * a subquery.)
 *
 * In addition, we make several checks on the subquery's output columns to see
 * if it is safe to reference them in pushed-down quals.  If output column k
 * is found to be unsafe to reference, we set safetyInfo->unsafeColumns[k]
 * to true, but we don't reject the subquery overall since column k might not
 * be referenced by some/all quals.  The unsafeColumns[] array will be
 * consulted later by qual_is_pushdown_safe().  It's better to do it this way
 * than to make the checks directly in qual_is_pushdown_safe(), because when
 * the subquery involves set operations we have to check the output
 * expressions in each arm of the set op.
 *
 * Note: pushing quals into a DISTINCT subquery is theoretically dubious:
 * we're effectively assuming that the quals cannot distinguish values that
 * the DISTINCT's equality operator sees as equal, yet there are many
 * counterexamples to that assumption.  However use of such a qual with a
 * DISTINCT subquery would be unsafe anyway, since there's no guarantee which
 * "equal" value will be chosen as the output value by the DISTINCT operation.
 * So we don't worry too much about that.  Another objection is that if the
 * qual is expensive to evaluate, running it for each original row might cost
 * more than we save by eliminating rows before the DISTINCT step.  But it
 * would be very hard to estimate that at this stage, and in practice pushdown
 * seldom seems to make things worse, so we ignore that problem too.
 *
 * Note: likewise, pushing quals into a subquery with window functions is a
 * bit dubious: the quals might remove some rows of a window partition while
 * leaving others, causing changes in the window functions' results for the
 * surviving rows.  We insist that such a qual reference only partitioning
 * columns, but again that only protects us if the qual does not distinguish
 * values that the partitioning equality operator sees as equal.  The risks
 * here are perhaps larger than for DISTINCT, since no de-duplication of rows
 * occurs and thus there is no theoretical problem with such a qual.  But
 * we'll do this anyway because the potential performance benefits are very
 * large, and we've seen no field complaints about the longstanding comparable
 * behavior with DISTINCT.
 */
static bool
subquery_is_pushdown_safe(Query *subquery, Query *topquery,
						  pushdown_safety_info *safetyInfo)
{
	SetOperationStmt *topop;

	/* Check point 1 */
	if (subquery->limitOffset != NULL || subquery->limitCount != NULL)
		return false;

	/* Check point 6 */
	if (subquery->groupClause && subquery->groupingSets)
		return false;

	/* Check points 3, 4, and 5 */
	if (subquery->distinctClause ||
		subquery->hasWindowFuncs ||
		subquery->hasTargetSRFs)
		safetyInfo->unsafeVolatile = true;

	/*
	 * If we're at a leaf query, check for unsafe expressions in its target
	 * list, and mark any unsafe ones in unsafeColumns[].  (Non-leaf nodes in
	 * setop trees have only simple Vars in their tlists, so no need to check
	 * them.)
	 */
	if (subquery->setOperations == NULL)
		check_output_expressions(subquery, safetyInfo);

	/* Are we at top level, or looking at a setop component? */
	if (subquery == topquery)
	{
		/* Top level, so check any component queries */
		if (subquery->setOperations != NULL)
			if (!recurse_pushdown_safe(subquery->setOperations, topquery,
									   safetyInfo))
				return false;
	}
	else
	{
		/* Setop component must not have more components (too weird) */
		if (subquery->setOperations != NULL)
			return false;
		/* Check whether setop component output types match top level */
		topop = castNode(SetOperationStmt, topquery->setOperations);
		Assert(topop);
		compare_tlist_datatypes(subquery->targetList,
								topop->colTypes,
								safetyInfo);
	}
	return true;
}

/*
 * Helper routine to recurse through setOperations tree
 */
static bool
recurse_pushdown_safe(Node *setOp, Query *topquery,
					  pushdown_safety_info *safetyInfo)
{
	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *rte = rt_fetch(rtr->rtindex, topquery->rtable);
		Query	   *subquery = rte->subquery;

		Assert(subquery != NULL);
		return subquery_is_pushdown_safe(subquery, topquery, safetyInfo);
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;

		/* EXCEPT is no good (point 2 for subquery_is_pushdown_safe) */
		if (op->op == SETOP_EXCEPT)
			return false;
		/* Else recurse */
		if (!recurse_pushdown_safe(op->larg, topquery, safetyInfo))
			return false;
		if (!recurse_pushdown_safe(op->rarg, topquery, safetyInfo))
			return false;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
	return true;
}

/*
 * check_output_expressions - check subquery's output expressions for safety
 *
 * There are several cases in which it's unsafe to push down an upper-level
 * qual if it references a particular output column of a subquery.  We check
 * each output column of the subquery and set unsafeColumns[k] to true if
 * that column is unsafe for a pushed-down qual to reference.  The conditions
 * checked here are:
 *
 * 1. We must not push down any quals that refer to subselect outputs that
 * return sets, else we'd introduce functions-returning-sets into the
 * subquery's WHERE/HAVING quals.
 *
 * 2. We must not push down any quals that refer to subselect outputs that
 * contain volatile functions, for fear of introducing strange results due
 * to multiple evaluation of a volatile function.
 *
 * 3. If the subquery uses DISTINCT ON, we must not push down any quals that
 * refer to non-DISTINCT output columns, because that could change the set
 * of rows returned.  (This condition is vacuous for DISTINCT, because then
 * there are no non-DISTINCT output columns, so we needn't check.  Note that
 * subquery_is_pushdown_safe already reported that we can't use volatile
 * quals if there's DISTINCT or DISTINCT ON.)
 *
 * 4. If the subquery has any window functions, we must not push down quals
 * that reference any output columns that are not listed in all the subquery's
 * window PARTITION BY clauses.  We can push down quals that use only
 * partitioning columns because they should succeed or fail identically for
 * every row of any one window partition, and totally excluding some
 * partitions will not change a window function's results for remaining
 * partitions.  (Again, this also requires nonvolatile quals, but
 * subquery_is_pushdown_safe handles that.)
 */
static void
check_output_expressions(Query *subquery, pushdown_safety_info *safetyInfo)
{
	ListCell   *lc;

	foreach(lc, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;			/* ignore resjunk columns */

		/* We need not check further if output col is already known unsafe */
		if (safetyInfo->unsafeColumns[tle->resno])
			continue;

		/* Functions returning sets are unsafe (point 1) */
		if (subquery->hasTargetSRFs &&
			expression_returns_set((Node *) tle->expr))
		{
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* Volatile functions are unsafe (point 2) */
		if (contain_volatile_functions((Node *) tle->expr))
		{
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* If subquery uses DISTINCT ON, check point 3 */
		if (subquery->hasDistinctOn &&
			!targetIsInSortList(tle, InvalidOid, subquery->distinctClause))
		{
			/* non-DISTINCT column, so mark it unsafe */
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* If subquery uses window functions, check point 4 */
		if (subquery->hasWindowFuncs &&
			!targetIsInAllPartitionLists(tle, subquery))
		{
			/* not present in all PARTITION BY clauses, so mark it unsafe */
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* Refuse subplans */
		if (contain_subplans((Node *) tle->expr))
		{
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}
	}
}

/*
 * For subqueries using UNION/UNION ALL/INTERSECT/INTERSECT ALL, we can
 * push quals into each component query, but the quals can only reference
 * subquery columns that suffer no type coercions in the set operation.
 * Otherwise there are possible semantic gotchas.  So, we check the
 * component queries to see if any of them have output types different from
 * the top-level setop outputs.  unsafeColumns[k] is set true if column k
 * has different type in any component.
 *
 * We don't have to care about typmods here: the only allowed difference
 * between set-op input and output typmods is input is a specific typmod
 * and output is -1, and that does not require a coercion.
 *
 * tlist is a subquery tlist.
 * colTypes is an OID list of the top-level setop's output column types.
 * safetyInfo->unsafeColumns[] is the result array.
 */
static void
compare_tlist_datatypes(List *tlist, List *colTypes,
						pushdown_safety_info *safetyInfo)
{
	ListCell   *l;
	ListCell   *colType = list_head(colTypes);

	foreach(l, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
			continue;			/* ignore resjunk columns */
		if (colType == NULL)
			elog(ERROR, "wrong number of tlist entries");
		if (exprType((Node *) tle->expr) != lfirst_oid(colType))
			safetyInfo->unsafeColumns[tle->resno] = true;
		colType = lnext(colTypes, colType);
	}
	if (colType != NULL)
		elog(ERROR, "wrong number of tlist entries");
}

/*
 * targetIsInAllPartitionLists
 *		True if the TargetEntry is listed in the PARTITION BY clause
 *		of every window defined in the query.
 *
 * It would be safe to ignore windows not actually used by any window
 * function, but it's not easy to get that info at this stage; and it's
 * unlikely to be useful to spend any extra cycles getting it, since
 * unreferenced window definitions are probably infrequent in practice.
 */
static bool
targetIsInAllPartitionLists(TargetEntry *tle, Query *query)
{
	ListCell   *lc;

	foreach(lc, query->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);

		if (!targetIsInSortList(tle, InvalidOid, wc->partitionClause))
			return false;
	}
	return true;
}

/*
 * qual_is_pushdown_safe - is a particular rinfo safe to push down?
 *
 * rinfo is a restriction clause applying to the given subquery (whose RTE
 * has index rti in the parent query).
 *
 * Conditions checked here:
 *
 * 1. rinfo's clause must not contain any SubPlans (mainly because it's
 * unclear that it will work correctly: SubLinks will already have been
 * transformed into SubPlans in the qual, but not in the subquery).  Note that
 * SubLinks that transform to initplans are safe, and will be accepted here
 * because what we'll see in the qual is just a Param referencing the initplan
 * output.
 *
 * 2. If unsafeVolatile is set, rinfo's clause must not contain any volatile
 * functions.
 *
 * 3. If unsafeLeaky is set, rinfo's clause must not contain any leaky
 * functions that are passed Var nodes, and therefore might reveal values from
 * the subquery as side effects.
 *
 * 4. rinfo's clause must not refer to the whole-row output of the subquery
 * (since there is no easy way to name that within the subquery itself).
 *
 * 5. rinfo's clause must not refer to any subquery output columns that were
 * found to be unsafe to reference by subquery_is_pushdown_safe().
 */
static bool
qual_is_pushdown_safe(Query *subquery, Index rti, RestrictInfo *rinfo,
					  pushdown_safety_info *safetyInfo)
{
	bool		safe = true;
	Node	   *qual = (Node *) rinfo->clause;
	List	   *vars;
	ListCell   *vl;

	/* Refuse subselects (point 1) */
	if (contain_subplans(qual))
		return false;

	/* Refuse volatile quals if we found they'd be unsafe (point 2) */
	if (safetyInfo->unsafeVolatile &&
		contain_volatile_functions((Node *) rinfo))
		return false;

	/* Refuse leaky quals if told to (point 3) */
	if (safetyInfo->unsafeLeaky &&
		contain_leaked_vars(qual))
		return false;

	/*
	 * It would be unsafe to push down window function calls, but at least for
	 * the moment we could never see any in a qual anyhow.  (The same applies
	 * to aggregates, which we check for in pull_var_clause below.)
	 */
	Assert(!contain_window_function(qual));

	/*
	 * Examine all Vars used in clause.  Since it's a restriction clause, all
	 * such Vars must refer to subselect output columns ... unless this is
	 * part of a LATERAL subquery, in which case there could be lateral
	 * references.
	 */
	vars = pull_var_clause(qual, PVC_INCLUDE_PLACEHOLDERS);
	foreach(vl, vars)
	{
		Var		   *var = (Var *) lfirst(vl);

		/*
		 * XXX Punt if we find any PlaceHolderVars in the restriction clause.
		 * It's not clear whether a PHV could safely be pushed down, and even
		 * less clear whether such a situation could arise in any cases of
		 * practical interest anyway.  So for the moment, just refuse to push
		 * down.
		 */
		if (!IsA(var, Var))
		{
			safe = false;
			break;
		}

		/*
		 * Punt if we find any lateral references.  It would be safe to push
		 * these down, but we'd have to convert them into outer references,
		 * which subquery_push_qual lacks the infrastructure to do.  The case
		 * arises so seldom that it doesn't seem worth working hard on.
		 */
		if (var->varno != rti)
		{
			safe = false;
			break;
		}

		/* Subqueries have no system columns */
		Assert(var->varattno >= 0);

		/* Check point 4 */
		if (var->varattno == 0)
		{
			safe = false;
			break;
		}

		/* Check point 5 */
		if (safetyInfo->unsafeColumns[var->varattno])
		{
			safe = false;
			break;
		}
	}

	list_free(vars);

	return safe;
}

/*
 * subquery_push_qual - push down a qual that we have determined is safe
 */
static void
subquery_push_qual(Query *subquery, RangeTblEntry *rte, Index rti, Node *qual)
{
	if (subquery->setOperations != NULL)
	{
		/* Recurse to push it separately to each component query */
		recurse_push_qual(subquery->setOperations, subquery,
						  rte, rti, qual);
	}
	else
	{
		/*
		 * We need to replace Vars in the qual (which must refer to outputs of
		 * the subquery) with copies of the subquery's targetlist expressions.
		 * Note that at this point, any uplevel Vars in the qual should have
		 * been replaced with Params, so they need no work.
		 *
		 * This step also ensures that when we are pushing into a setop tree,
		 * each component query gets its own copy of the qual.
		 */
		qual = ReplaceVarsFromTargetList(qual, rti, 0, rte,
										 subquery->targetList,
										 REPLACEVARS_REPORT_ERROR, 0,
										 &subquery->hasSubLinks);

		/*
		 * Now attach the qual to the proper place: normally WHERE, but if the
		 * subquery uses grouping or aggregation, put it in HAVING (since the
		 * qual really refers to the group-result rows).
		 */
		if (subquery->hasAggs || subquery->groupClause || subquery->groupingSets || subquery->havingQual)
			subquery->havingQual = make_and_qual(subquery->havingQual, qual);
		else
			subquery->jointree->quals =
				make_and_qual(subquery->jointree->quals, qual);

		/*
		 * We need not change the subquery's hasAggs or hasSubLinks flags,
		 * since we can't be pushing down any aggregates that weren't there
		 * before, and we don't push down subselects at all.
		 */
	}
}

/*
 * Helper routine to recurse through setOperations tree
 */
static void
recurse_push_qual(Node *setOp, Query *topquery,
				  RangeTblEntry *rte, Index rti, Node *qual)
{
	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *subrte = rt_fetch(rtr->rtindex, topquery->rtable);
		Query	   *subquery = subrte->subquery;

		Assert(subquery != NULL);
		subquery_push_qual(subquery, rte, rti, qual);
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;

		recurse_push_qual(op->larg, topquery, rte, rti, qual);
		recurse_push_qual(op->rarg, topquery, rte, rti, qual);
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
}

/*****************************************************************************
 *			SIMPLIFYING SUBQUERY TARGETLISTS
 *****************************************************************************/

/*
 * remove_unused_subquery_outputs
 *		Remove subquery targetlist items we don't need
 *
 * It's possible, even likely, that the upper query does not read all the
 * output columns of the subquery.  We can remove any such outputs that are
 * not needed by the subquery itself (e.g., as sort/group columns) and do not
 * affect semantics otherwise (e.g., volatile functions can't be removed).
 * This is useful not only because we might be able to remove expensive-to-
 * compute expressions, but because deletion of output columns might allow
 * optimizations such as join removal to occur within the subquery.
 *
 * To avoid affecting column numbering in the targetlist, we don't physically
 * remove unused tlist entries, but rather replace their expressions with NULL
 * constants.  This is implemented by modifying subquery->targetList.
 */
static void
remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel)
{
	Bitmapset  *attrs_used = NULL;
	ListCell   *lc;

	/*
	 * Do nothing if subquery has UNION/INTERSECT/EXCEPT: in principle we
	 * could update all the child SELECTs' tlists, but it seems not worth the
	 * trouble presently.
	 */
	if (subquery->setOperations)
		return;

	/*
	 * If subquery has regular DISTINCT (not DISTINCT ON), we're wasting our
	 * time: all its output columns must be used in the distinctClause.
	 */
	if (subquery->distinctClause && !subquery->hasDistinctOn)
		return;

	/*
	 * Collect a bitmap of all the output column numbers used by the upper
	 * query.
	 *
	 * Add all the attributes needed for joins or final output.  Note: we must
	 * look at rel's targetlist, not the attr_needed data, because attr_needed
	 * isn't computed for inheritance child rels, cf set_append_rel_size().
	 * (XXX might be worth changing that sometime.)
	 */
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs_used);

	/* Add all the attributes used by un-pushed-down restriction clauses. */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
	}

	/*
	 * If there's a whole-row reference to the subquery, we can't remove
	 * anything.
	 */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
		return;

	/*
	 * Run through the tlist and zap entries we don't need.  It's okay to
	 * modify the tlist items in-place because set_subquery_pathlist made a
	 * copy of the subquery.
	 */
	foreach(lc, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Node	   *texpr = (Node *) tle->expr;

		/*
		 * If it has a sortgroupref number, it's used in some sort/group
		 * clause so we'd better not remove it.  Also, don't remove any
		 * resjunk columns, since their reason for being has nothing to do
		 * with anybody reading the subquery's output.  (It's likely that
		 * resjunk columns in a sub-SELECT would always have ressortgroupref
		 * set, but even if they don't, it seems imprudent to remove them.)
		 */
		if (tle->ressortgroupref || tle->resjunk)
			continue;

		/*
		 * If it's used by the upper query, we can't remove it.
		 */
		if (bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
			continue;

		/*
		 * If it contains a set-returning function, we can't remove it since
		 * that could change the number of rows returned by the subquery.
		 */
		if (subquery->hasTargetSRFs &&
			expression_returns_set(texpr))
			continue;

		/*
		 * If it contains volatile functions, we daren't remove it for fear
		 * that the user is expecting their side-effects to happen.
		 */
		if (contain_volatile_functions(texpr))
			continue;

		/*
		 * OK, we don't need it.  Replace the expression with a NULL constant.
		 * Preserve the exposed type of the expression, in case something
		 * looks at the rowtype of the subquery's result.
		 */
		tle->expr = (Expr *) makeNullConst(exprType(texpr),
										   exprTypmod(texpr),
										   exprCollation(texpr));
	}
}

/*
 * create_partial_bitmap_paths
 *	  Build partial bitmap heap path for the relation
 */
void
create_partial_bitmap_paths(PlannerInfo *root, RelOptInfo *rel,
							Path *bitmapqual)
{
	int			parallel_workers;
	double		pages_fetched;

	/* Compute heap pages for bitmap heap scan */
	pages_fetched = compute_bitmap_pages(root, rel, bitmapqual, 1.0,
										 NULL, NULL);


	/*
	 * Don't support parallel BitmapScan for AO/AOCS.
	 */
	if (rel->reloptkind == RELOPT_BASEREL && (AMHandlerIsAO(rel->amhandler)))
		return;

	parallel_workers = compute_parallel_worker(root, rel, pages_fetched, -1,
											   max_parallel_workers_per_gather);

	if (parallel_workers <= 1)
		return;

	add_partial_path(rel, (Path *) create_bitmap_heap_path(root, rel,
														   bitmapqual, rel->lateral_relids, 1.0, parallel_workers));
}

/*
 * Compute the number of parallel workers that should be used to scan a
 * relation.  We compute the parallel workers based on the size of the heap to
 * be scanned and the size of the index to be scanned, then choose a minimum
 * of those.
 *
 * "heap_pages" is the number of pages from the table that we expect to scan, or
 * -1 if we don't expect to scan any.
 *
 * "index_pages" is the number of pages from the index that we expect to scan, or
 * -1 if we don't expect to scan any.
 *
 * "max_workers" is caller's limit on the number of workers.  This typically
 * comes from a GUC.
 */
int
compute_parallel_worker(PlannerInfo *root, RelOptInfo *rel, double heap_pages, double index_pages,
						int max_workers)
{
	int			parallel_workers = 0;

	/*
	 * If the user has set the parallel_workers reloption, use that; otherwise
	 * select a default number of workers.
	 */
	if (rel->rel_parallel_workers != -1)
		parallel_workers = rel->rel_parallel_workers;
	else
	{
		/*
		 * We need to reconsider parallel workers for AO/AOCO tables
		 * because page number in ao is quite hard to estimate.
		 * The parallel for AO/AOCO tables is based on segment file count in
		 * pg_appendonly which will be updated by analyze/vacuum/truncate processes.
		 */
		if (rel->reloptkind == RELOPT_BASEREL && (AMHandlerIsAO(rel->amhandler)))
		{
			Oid			aorelid = root->simple_rte_array[rel->relid]->relid;
			HeapTuple	aotup;
			Form_pg_appendonly aoform;
			aotup = SearchSysCache1(AORELID, ObjectIdGetDatum(aorelid));
			if (!HeapTupleIsValid(aotup))
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("appendonly table relid %u does not exist in pg_appendonly", aorelid)));

			aoform = (Form_pg_appendonly) GETSTRUCT(aotup);
			Assert(aoform->segfilecount >= 0);
			parallel_workers = Min(aoform->segfilecount, max_workers);
			ReleaseSysCache(aotup);

			/*
			 * Disable parallel for AO/AOCO for:
			 * 1.We don't support parallel/non-parallel IndexScan/IndexOnlyScan.
			 * 2.If parallel_workers is 1, it is pointless in gp parallel mode.
			 */
			if (parallel_workers == 1 || index_pages >= 0)
				parallel_workers = 0;
		}
		else
		{
			/*
			 * If the number of pages being scanned is insufficient to justify a
			 * parallel scan, just return zero ... unless it's an inheritance
			 * child. In that case, we want to generate a parallel path here
			 * anyway.  It might not be worthwhile just for this relation, but
			 * when combined with all of its inheritance siblings it may well pay
			 * off.
			 */
			if (rel->reloptkind == RELOPT_BASEREL &&
				((heap_pages >= 0 && heap_pages < min_parallel_table_scan_size) ||
				 (index_pages >= 0 && index_pages < min_parallel_index_scan_size)))
				return 0;

			if (heap_pages >= 0)
			{
				int			heap_parallel_threshold;
				int			heap_parallel_workers = 1;

				/*
				 * Select the number of workers based on the log of the size of
				 * the relation.  This probably needs to be a good deal more
				 * sophisticated, but we need something here for now.  Note that
				 * the upper limit of the min_parallel_table_scan_size GUC is
				 * chosen to prevent overflow here.
				 */
				heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);
				while (heap_pages >= (BlockNumber) (heap_parallel_threshold * 3))
				{
					heap_parallel_workers++;
					heap_parallel_threshold *= 3;
					if (heap_parallel_threshold > INT_MAX / 3)
						break;		/* avoid overflow */
				}

				parallel_workers = heap_parallel_workers;
			}

			if (index_pages >= 0)
			{
				int			index_parallel_workers = 1;
				int			index_parallel_threshold;

				/* same calculation as for heap_pages above */
				index_parallel_threshold = Max(min_parallel_index_scan_size, 1);
				while (index_pages >= (BlockNumber) (index_parallel_threshold * 3))
				{
					index_parallel_workers++;
					index_parallel_threshold *= 3;
					if (index_parallel_threshold > INT_MAX / 3)
						break;		/* avoid overflow */
				}

				if (parallel_workers > 0)
					parallel_workers = Min(parallel_workers, index_parallel_workers);
				else
					parallel_workers = index_parallel_workers;
			}
		}
	}

	/* In no case use more than caller supplied maximum number of workers */
	parallel_workers = Min(parallel_workers, max_workers);

	/*
	 * GPDB parallel mode don't has a leader process. parallel_workers=1 may cause
	 * CdbLocusType_HashedWorkers locus type. This affect the plan to generate motion node
	 * which is not necessary. So we disable parallel_workers=1 in GPDB parallel mode.
	 */
	if (parallel_workers == 1)
		parallel_workers = 0;

	return parallel_workers;
}

/*
 * generate_partitionwise_join_paths
 * 		Create paths representing partitionwise join for given partitioned
 * 		join relation.
 *
 * This must not be called until after we are done adding paths for all
 * child-joins. Otherwise, add_path might delete a path to which some path
 * generated here has a reference.
 */
void
generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *live_children = NIL;
	int			cnt_parts;
	int			num_parts;
	RelOptInfo **part_rels;

	/* Handle only join relations here. */
	if (!IS_JOIN_REL(rel))
		return;

	/* We've nothing to do if the relation is not partitioned. */
	if (!IS_PARTITIONED_REL(rel))
		return;

	/* The relation should have consider_partitionwise_join set. */
	Assert(rel->consider_partitionwise_join);

	/* Guard against stack overflow due to overly deep partition hierarchy. */
	check_stack_depth();

	num_parts = rel->nparts;
	part_rels = rel->part_rels;

	/* Collect non-dummy child-joins. */
	for (cnt_parts = 0; cnt_parts < num_parts; cnt_parts++)
	{
		RelOptInfo *child_rel = part_rels[cnt_parts];

		/* If it's been pruned entirely, it's certainly dummy. */
		if (child_rel == NULL)
			continue;

		/* Add partitionwise join paths for partitioned child-joins. */
		generate_partitionwise_join_paths(root, child_rel);

		set_cheapest(child_rel);

		/* Dummy children will not be scanned, so ignore those. */
		if (IS_DUMMY_REL(child_rel))
			continue;

#ifdef OPTIMIZER_DEBUG
		debug_print_rel(root, child_rel);
#endif

		live_children = lappend(live_children, child_rel);
	}

	/* If all child-joins are dummy, parent join is also dummy. */
	if (!live_children)
	{
		mark_dummy_rel(root, rel);
		return;
	}

	/* Build additional paths for this rel from child-join paths. */
	add_paths_to_append_rel(root, rel, live_children);
	list_free(live_children);
}

static bool
is_query_contain_limit_groupby(Query *parse)
{
	if (parse->limitCount || parse->limitOffset ||
		parse->groupClause || parse->groupingSets || parse->distinctClause)
		return true;

	if (parse->setOperations)
	{
		SetOperationStmt *sop_stmt = (SetOperationStmt *) (parse->setOperations);
		RangeTblRef   *larg = (RangeTblRef *) sop_stmt->larg;
		RangeTblRef   *rarg = (RangeTblRef *) sop_stmt->rarg;
		RangeTblEntry *lrte = list_nth(parse->rtable, larg->rtindex-1);
		RangeTblEntry *rrte = list_nth(parse->rtable, rarg->rtindex-1);

		if ((lrte->rtekind == RTE_SUBQUERY &&
			 is_query_contain_limit_groupby(lrte->subquery)) ||
			(rrte->rtekind == RTE_SUBQUERY &&
			 is_query_contain_limit_groupby(rrte->subquery)))
			return true;
	}

	return false;
}

/*****************************************************************************
 *			DEBUG SUPPORT
 *****************************************************************************/

#ifdef OPTIMIZER_DEBUG

static void
print_relids(PlannerInfo *root, Relids relids)
{
	int			x;
	bool		first = true;

	x = -1;
	while ((x = bms_next_member(relids, x)) >= 0)
	{
		if (!first)
			printf(" ");
		if (x < root->simple_rel_array_size &&
			root->simple_rte_array[x])
			printf("%s", root->simple_rte_array[x]->eref->aliasname);
		else
			printf("%d", x);
		first = false;
	}
}

static void
print_restrictclauses(PlannerInfo *root, List *clauses)
{
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *c = lfirst(l);

		print_expr((Node *) c->clause, root->parse->rtable);
		if (lnext(clauses, l))
			printf(", ");
	}
}

static void
print_path(PlannerInfo *root, Path *path, int indent)
{
	const char *ptype;
	const char *ltype;
	bool		join = false;
	Path	   *subpath = NULL;
	int			i;

	switch (nodeTag(path))
	{
		case T_Path:
			switch (path->pathtype)
			{
				case T_SeqScan:
					ptype = "SeqScan";
					break;
				case T_SampleScan:
					ptype = "SampleScan";
					break;
				case T_FunctionScan:
					ptype = "FunctionScan";
					break;
				case T_TableFuncScan:
					ptype = "TableFuncScan";
					break;
				case T_ValuesScan:
					ptype = "ValuesScan";
					break;
				case T_CteScan:
					ptype = "CteScan";
					break;
				case T_NamedTuplestoreScan:
					ptype = "NamedTuplestoreScan";
					break;
				case T_Result:
					ptype = "Result";
					break;
				case T_WorkTableScan:
					ptype = "WorkTableScan";
					break;
				default:
					ptype = "???Path";
					break;
			}
			break;
		case T_IndexPath:
			ptype = "IdxScan";
			break;
		case T_BitmapHeapPath:
			ptype = "BitmapHeapScan";
			break;
		case T_BitmapAndPath:
			ptype = "BitmapAndPath";
			break;
		case T_BitmapOrPath:
			ptype = "BitmapOrPath";
			break;
		case T_TidPath:
			ptype = "TidScan";
			break;
		case T_SubqueryScanPath:
			ptype = "SubqueryScan";
			break;
		case T_ForeignPath:
			ptype = "ForeignScan";
			break;
		case T_CustomPath:
			ptype = "CustomScan";
			break;
		case T_NestPath:
			ptype = "NestLoop";
			join = true;
			break;
		case T_MergePath:
			ptype = "MergeJoin";
			join = true;
			break;
		case T_HashPath:
			ptype = "HashJoin";
			join = true;
			break;
		case T_AppendPath:
			ptype = "Append";
			break;
		case T_MergeAppendPath:
			ptype = "MergeAppend";
			break;
		case T_GroupResultPath:
			ptype = "GroupResult";
			break;
		case T_MaterialPath:
			ptype = "Material";
			subpath = ((MaterialPath *) path)->subpath;
			break;
		case T_MemoizePath:
			ptype = "Memoize";
			subpath = ((MemoizePath *) path)->subpath;
			break;
		case T_UniquePath:
			ptype = "Unique";
			subpath = ((UniquePath *) path)->subpath;
			break;
		case T_GatherPath:
			ptype = "Gather";
			subpath = ((GatherPath *) path)->subpath;
			break;
		case T_GatherMergePath:
			ptype = "GatherMerge";
			subpath = ((GatherMergePath *) path)->subpath;
			break;
		case T_ProjectionPath:
			ptype = "Projection";
			subpath = ((ProjectionPath *) path)->subpath;
			break;
		case T_ProjectSetPath:
			ptype = "ProjectSet";
			subpath = ((ProjectSetPath *) path)->subpath;
			break;
		case T_SortPath:
			ptype = "Sort";
			subpath = ((SortPath *) path)->subpath;
			break;
		case T_IncrementalSortPath:
			ptype = "IncrementalSort";
			subpath = ((SortPath *) path)->subpath;
			break;
		case T_GroupPath:
			ptype = "Group";
			subpath = ((GroupPath *) path)->subpath;
			break;
		case T_UpperUniquePath:
			ptype = "UpperUnique";
			subpath = ((UpperUniquePath *) path)->subpath;
			break;
		case T_AggPath:
			ptype = "Agg";
			subpath = ((AggPath *) path)->subpath;
			break;
		case T_GroupingSetsPath:
			ptype = "GroupingSets";
			subpath = ((GroupingSetsPath *) path)->subpath;
			break;
		case T_MinMaxAggPath:
			ptype = "MinMaxAgg";
			break;
		case T_WindowAggPath:
			ptype = "WindowAgg";
			subpath = ((WindowAggPath *) path)->subpath;
			break;
		case T_SetOpPath:
			ptype = "SetOp";
			subpath = ((SetOpPath *) path)->subpath;
			break;
		case T_RecursiveUnionPath:
			ptype = "RecursiveUnion";
			break;
		case T_LockRowsPath:
			ptype = "LockRows";
			subpath = ((LockRowsPath *) path)->subpath;
			break;
		case T_ModifyTablePath:
			ptype = "ModifyTable";
			break;
		case T_RuntimeFilterPath:
			ptype = "RuntimeFilter";
			subpath = ((RuntimeFilterPath *) path)->subpath;
			break;
		case T_LimitPath:
			ptype = "Limit";
			subpath = ((LimitPath *) path)->subpath;
			break;
		case T_CdbMotionPath:
			ptype = "Motion";
			subpath = ((CdbMotionPath *) path)->subpath;
			break;
		default:
			ptype = "???Path";
			break;
	}

	for (i = 0; i < indent; i++)
		printf("\t");
	printf("%s", ptype);

	if (path->parent)
	{
		printf("(");
		print_relids(root, path->parent->relids);
		printf(")");
	}

	switch (path->locus.locustype)
	{
		case CdbLocusType_Entry:
			ltype = "Entry";
			break;
		case CdbLocusType_SingleQE:
			ltype = "SingleQE";
			break;
		case CdbLocusType_General:
			ltype = "General";
			break;
		case CdbLocusType_SegmentGeneral:
			ltype = "SegmentGeneral";
			break;
		case CdbLocusType_SegmentGeneralWorkers:
			ltype = "SegmentGeneralWorkers";
			break;
		case CdbLocusType_Replicated:
			ltype = "Replicated";
			break;
		case CdbLocusType_Hashed:
			ltype = "Hashed";
			break;
		case CdbLocusType_HashedWorkers:
			ltype = "HashedWorkers";
			break;
		case CdbLocusType_HashedOJ:
			ltype = "HashedOJ";
			break;
		case CdbLocusType_Strewn:
			ltype = "Strewn";
			break;
		default:
			ltype = "???CdbLocus";
			break;
	}
	printf(" locus=%s", ltype);
	if (path->param_info)
	{
		printf(" required_outer (");
		print_relids(root, path->param_info->ppi_req_outer);
		printf(")");
	}
	printf(" rows=%.0f cost=%.2f..%.2f\n",
		   path->rows, path->startup_cost, path->total_cost);

	if (path->pathkeys)
	{
		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  pathkeys: ");
		print_pathkeys(path->pathkeys, root->parse->rtable);
	}

	if (join)
	{
		JoinPath   *jp = (JoinPath *) path;

		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  clauses: ");
		print_restrictclauses(root, jp->joinrestrictinfo);
		printf("\n");

		if (IsA(path, MergePath))
		{
			MergePath  *mp = (MergePath *) path;

			for (i = 0; i < indent; i++)
				printf("\t");
			printf("  sortouter=%d sortinner=%d materializeinner=%d\n",
				   ((mp->outersortkeys) ? 1 : 0),
				   ((mp->innersortkeys) ? 1 : 0),
				   ((mp->materialize_inner) ? 1 : 0));
		}

		print_path(root, jp->outerjoinpath, indent + 1);
		print_path(root, jp->innerjoinpath, indent + 1);
	}

	if (subpath)
		print_path(root, subpath, indent + 1);
}

void
debug_print_rel(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *l;

	printf("RELOPTINFO (");
	print_relids(root, rel->relids);
	printf("): rows=%.0f width=%d\n", rel->rows, rel->reltarget->width);

	if (rel->baserestrictinfo)
	{
		printf("\tbaserestrictinfo: ");
		print_restrictclauses(root, rel->baserestrictinfo);
		printf("\n");
	}

	if (rel->joininfo)
	{
		printf("\tjoininfo: ");
		print_restrictclauses(root, rel->joininfo);
		printf("\n");
	}

	printf("\tpath list:\n");
	foreach(l, rel->pathlist)
		print_path(root, lfirst(l), 1);
	if (rel->cheapest_parameterized_paths)
	{
		printf("\n\tcheapest parameterized paths:\n");
		foreach(l, rel->cheapest_parameterized_paths)
			print_path(root, lfirst(l), 1);
	}
	if (rel->cheapest_startup_path)
	{
		printf("\n\tcheapest startup path:\n");
		print_path(root, rel->cheapest_startup_path, 1);
	}
	if (rel->cheapest_total_path)
	{
		printf("\n\tcheapest total path:\n");
		print_path(root, rel->cheapest_total_path, 1);
	}
	printf("\n");
	fflush(stdout);
}

#endif							/* OPTIMIZER_DEBUG */
