/*-------------------------------------------------------------------------
 *
 * relnode.c
 *	  Relation-node lookup/construction routines
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/relnode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_constraint.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_oper.h"
#include "rewrite/rewriteManip.h"
#include "parser/parse_relation.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

#include "access/sysattr.h"
#include "cdb/cdbutil.h"

typedef struct JoinHashEntry
{
	Relids		join_relids;	/* hash key --- MUST BE FIRST */
	RelOptInfo *join_rel;
} JoinHashEntry;

typedef struct GroupedHashEntry
{
	Relids		relids;		/* hash key --- MUST BE FIRST */
	RelAggInfo *agg_info;
} GroupedHashEntry;

static void build_joinrel_tlist(PlannerInfo *root, RelOptInfo *joinrel,
								RelOptInfo *input_rel,
								SpecialJoinInfo *sjinfo,
								List *pushed_down_joins,
								bool can_null);
static List *build_joinrel_restrictlist(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outer_rel,
										RelOptInfo *inner_rel,
										SpecialJoinInfo *sjinfo);
static void build_joinrel_joinlist(RelOptInfo *joinrel,
								   RelOptInfo *outer_rel,
								   RelOptInfo *inner_rel);
static List *subbuild_joinrel_restrictlist(PlannerInfo *root,
										   RelOptInfo *joinrel,
										   RelOptInfo *input_rel,
										   Relids both_input_relids,
										   List *new_restrictlist);
static List *subbuild_joinrel_joinlist(RelOptInfo *joinrel,
									   List *joininfo_list,
									   List *new_joininfo);
static void set_foreign_rel_properties(RelOptInfo *joinrel,
									   RelOptInfo *outer_rel, RelOptInfo *inner_rel,
									   List *restrictlist);
static void add_join_rel(PlannerInfo *root, RelOptInfo *joinrel);
static void build_joinrel_partition_info(PlannerInfo *root,
										 RelOptInfo *joinrel,
										 RelOptInfo *outer_rel, RelOptInfo *inner_rel,
										 SpecialJoinInfo *sjinfo,
										 List *restrictlist);
static int	match_expr_to_partition_keys(Expr *expr, RelOptInfo *rel,
										 bool strict_op);
static void set_joinrel_partition_key_exprs(RelOptInfo *joinrel,
											RelOptInfo *outer_rel, RelOptInfo *inner_rel,
											JoinType jointype);
static void build_child_join_reltarget(PlannerInfo *root,
									   RelOptInfo *parentrel,
									   RelOptInfo *childrel,
									   int nappinfos,
									   AppendRelInfo **appinfos);
static bool init_grouping_targets(PlannerInfo *root, RelOptInfo *rel,
								  PathTarget *target, PathTarget *agg_input,
								  List *gvis, List **group_exprs_extra_p);
static Index get_expression_sortgroupref(Expr *expr, List *gvis);
static bool is_var_in_aggref_only(PlannerInfo *root, Var *var);
static bool is_var_needed_by_join(PlannerInfo *root, Var *var, RelOptInfo *rel);


/*
 * setup_simple_rel_arrays
 *	  Prepare the arrays we use for quickly accessing base relations
 *	  and AppendRelInfos.
 */
void
setup_simple_rel_arrays(PlannerInfo *root)
{
	int			size;
	Index		rti;
	ListCell   *lc;

	/* Arrays are accessed using RT indexes (1..N) */
	size = list_length(root->parse->rtable) + 1;
	root->simple_rel_array_size = size;

	/*
	 * simple_rel_array is initialized to all NULLs, since no RelOptInfos
	 * exist yet.  It'll be filled by later calls to build_simple_rel().
	 */
	root->simple_rel_array = (RelOptInfo **)
		palloc0(size * sizeof(RelOptInfo *));

	/* simple_rte_array is an array equivalent of the rtable list */
	root->simple_rte_array = (RangeTblEntry **)
		palloc0(size * sizeof(RangeTblEntry *));
	rti = 1;
	foreach(lc, root->parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		root->simple_rte_array[rti++] = rte;
	}

	/* append_rel_array is not needed if there are no AppendRelInfos */
	if (root->append_rel_list == NIL)
	{
		root->append_rel_array = NULL;
		return;
	}

	root->append_rel_array = (AppendRelInfo **)
		palloc0(size * sizeof(AppendRelInfo *));

	/*
	 * append_rel_array is filled with any already-existing AppendRelInfos,
	 * which currently could only come from UNION ALL flattening.  We might
	 * add more later during inheritance expansion, but it's the
	 * responsibility of the expansion code to update the array properly.
	 */
	foreach(lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);
		int			child_relid = appinfo->child_relid;

		/* Sanity check */
		Assert(child_relid < size);

		if (root->append_rel_array[child_relid])
			elog(ERROR, "child relation already exists");

		root->append_rel_array[child_relid] = appinfo;
	}
}

/*
 * expand_planner_arrays
 *		Expand the PlannerInfo's per-RTE arrays by add_size members
 *		and initialize the newly added entries to NULLs
 *
 * Note: this causes the append_rel_array to become allocated even if
 * it was not before.  This is okay for current uses, because we only call
 * this when adding child relations, which always have AppendRelInfos.
 */
void
expand_planner_arrays(PlannerInfo *root, int add_size)
{
	int			new_size;

	Assert(add_size > 0);

	new_size = root->simple_rel_array_size + add_size;

	root->simple_rel_array =
		repalloc0_array(root->simple_rel_array, RelOptInfo *, root->simple_rel_array_size, new_size);

	root->simple_rte_array =
		repalloc0_array(root->simple_rte_array, RangeTblEntry *, root->simple_rel_array_size, new_size);

	if (root->append_rel_array)
		root->append_rel_array =
			repalloc0_array(root->append_rel_array, AppendRelInfo *, root->simple_rel_array_size, new_size);
	else
		root->append_rel_array =
			palloc0_array(AppendRelInfo *, new_size);

	root->simple_rel_array_size = new_size;
}

/*
 * build_simple_rel
 *	  Construct a new RelOptInfo for a base relation or 'other' relation.
 */
RelOptInfo *
build_simple_rel(PlannerInfo *root, int relid, RelOptInfo *parent)
{
	RelOptInfo *rel;
	RangeTblEntry *rte;

	/* Rel should not exist already */
	Assert(relid > 0 && relid < root->simple_rel_array_size);
	if (root->simple_rel_array[relid] != NULL)
		elog(ERROR, "rel %d already exists", relid);

	/* Fetch RTE for relation */
	rte = root->simple_rte_array[relid];
	Assert(rte != NULL);

	rel = makeNode(RelOptInfo);
	rel->reloptkind = parent ? RELOPT_OTHER_MEMBER_REL : RELOPT_BASEREL;
	rel->relids = bms_make_singleton(relid);
	rel->rows = 0;
	/* cheap startup cost is interesting iff not all tuples to be retrieved */
	rel->consider_startup = (root->tuple_fraction > 0);
	rel->consider_param_startup = false;	/* might get changed later */
	rel->consider_parallel = false; /* might get changed later */
	rel->reltarget = create_empty_pathtarget();
	rel->pathlist = NIL;
	rel->ppilist = NIL;
	rel->partial_pathlist = NIL;
	rel->cheapest_startup_path = NULL;
	rel->cheapest_total_path = NULL;
	rel->cheapest_unique_path = NULL;
	rel->cheapest_parameterized_paths = NIL;
	rel->relid = relid;
	rel->rtekind = rte->rtekind;
	/* min_attr, max_attr, attr_needed, attr_widths are set below */
	rel->lateral_vars = NIL;
	rel->indexlist = NIL;
	rel->statlist = NIL;
	rel->pages = 0;
	rel->tuples = 0;
	rel->allvisfrac = 0;
	rel->eclass_indexes = NULL;
	rel->subroot = NULL;
	rel->subplan_params = NIL;
	rel->rel_parallel_workers = -1; /* set up in get_relation_info */
	rel->amflags = 0;
	rel->serverid = InvalidOid;
	rel->segSeverids = NIL;

	if (rte->rtekind == RTE_RELATION)
	{
		Assert(parent == NULL ||
			   parent->rtekind == RTE_RELATION ||
			   parent->rtekind == RTE_SUBQUERY);

		/*
		 * For any RELATION rte, we need a userid with which to check
		 * permission access. Baserels simply use their own
		 * RTEPermissionInfo's checkAsUser.
		 *
		 * For otherrels normally there's no RTEPermissionInfo, so we use the
		 * parent's, which normally has one. The exceptional case is that the
		 * parent is a subquery, in which case the otherrel will have its own.
		 */
		if (rel->reloptkind == RELOPT_BASEREL ||
			(rel->reloptkind == RELOPT_OTHER_MEMBER_REL &&
			 parent->rtekind == RTE_SUBQUERY))
		{
			RTEPermissionInfo *perminfo;

			perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
			rel->userid = perminfo->checkAsUser;
		}
		else
			rel->userid = parent->userid;
	}
	else
		rel->userid = InvalidOid;
	rel->useridiscurrent = false;
	rel->exec_location = FTEXECLOCATION_NOT_DEFINED;
	rel->fdwroutine = NULL;
	rel->fdw_private = NULL;
	rel->unique_for_rels = NIL;
	rel->non_unique_for_rels = NIL;
	rel->baserestrictinfo = NIL;
	rel->baserestrictcost.startup = 0;
	rel->baserestrictcost.per_tuple = 0;
	rel->baserestrict_min_security = UINT_MAX;
	rel->joininfo = NIL;
	rel->has_eclass_joins = false;
	rel->consider_partitionwise_join = false;	/* might get changed later */
	rel->part_scheme = NULL;
	rel->nparts = -1;
	rel->boundinfo = NULL;
	rel->partbounds_merged = false;
	rel->partition_qual = NIL;
	rel->part_rels = NULL;
	rel->live_parts = NULL;
	rel->all_partrels = NULL;
	rel->partexprs = NULL;
	rel->nullable_partexprs = NULL;

	/*
	 * Pass assorted information down the inheritance hierarchy.
	 */
	if (parent)
	{
		/* We keep back-links to immediate parent and topmost parent. */
		rel->parent = parent;
		rel->top_parent = parent->top_parent ? parent->top_parent : parent;
		rel->top_parent_relids = rel->top_parent->relids;

		/*
		 * A child rel is below the same outer joins as its parent.  (We
		 * presume this info was already calculated for the parent.)
		 */
		rel->nulling_relids = parent->nulling_relids;

		/*
		 * Also propagate lateral-reference information from appendrel parent
		 * rels to their child rels.  We intentionally give each child rel the
		 * same minimum parameterization, even though it's quite possible that
		 * some don't reference all the lateral rels.  This is because any
		 * append path for the parent will have to have the same
		 * parameterization for every child anyway, and there's no value in
		 * forcing extra reparameterize_path() calls.  Similarly, a lateral
		 * reference to the parent prevents use of otherwise-movable join rels
		 * for each child.
		 *
		 * It's possible for child rels to have their own children, in which
		 * case the topmost parent's lateral info propagates all the way down.
		 */
		rel->direct_lateral_relids = parent->direct_lateral_relids;
		rel->lateral_relids = parent->lateral_relids;
		rel->lateral_referencers = parent->lateral_referencers;
	}
	else
	{
		rel->parent = NULL;
		rel->top_parent = NULL;
		rel->top_parent_relids = NULL;
		rel->nulling_relids = NULL;
		rel->direct_lateral_relids = NULL;
		rel->lateral_relids = NULL;
		rel->lateral_referencers = NULL;
	}

	/* Check type of rtable entry */
	switch (rte->rtekind)
	{
		case RTE_RELATION:
			/* Table --- retrieve statistics from the system catalogs */

			get_relation_info(root, rte->relid, rte->inh, rel);

			/* if we've been asked to, force the dist-policy to be partitioned-randomly. */
			if (rte->forceDistRandom)
			{
				GpPolicy   *origpolicy = GpPolicyFetch(rte->relid);
				int			numsegments;

				if (origpolicy->ptype != POLICYTYPE_ENTRY)
					numsegments = origpolicy->numsegments;
				else
					numsegments = getgpsegmentCount();

				rel->cdbpolicy = createRandomPartitionedPolicy(numsegments);
			}

			if ((root->parse->commandType == CMD_UPDATE ||
				 root->parse->commandType == CMD_DELETE) &&
				root->parse->resultRelation == relid &&
				GpPolicyIsReplicated(rel->cdbpolicy))
			{
				root->upd_del_replicated_table = relid;
			}
			break;
		case RTE_SUBQUERY:
		case RTE_FUNCTION:
		case RTE_TABLEFUNCTION:
		case RTE_TABLEFUNC:
		case RTE_VALUES:
		case RTE_CTE:
		case RTE_NAMEDTUPLESTORE:

			/*
			 * Subquery, function, tablefunc, values list, CTE, or ENR --- set
			 * up attr range and arrays
			 *
			 * Note: 0 is included in range to support whole-row Vars
			 */
            /* CDB: Allow internal use of sysattrs (<0) for subquery dedup. */
        	rel->min_attr = FirstLowInvalidHeapAttributeNumber + 1;     /*CDB*/
			rel->max_attr = list_length(rte->eref->colnames);
			rel->attr_needed = (Relids *)
				palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
			rel->attr_widths = (int32 *)
				palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));
			break;
		case RTE_RESULT:
			/* RTE_RESULT has no columns, nor could it have whole-row Var */
			rel->min_attr = 0;
			rel->max_attr = -1;
			rel->attr_needed = NULL;
			rel->attr_widths = NULL;
			break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) rte->rtekind);
			break;
	}

	/*
	 * Copy the parent's quals to the child, with appropriate substitution of
	 * variables.  If any constant false or NULL clauses turn up, we can mark
	 * the child as dummy right away.  (We must do this immediately so that
	 * pruning works correctly when recursing in expand_partitioned_rtentry.)
	 */
	if (parent)
	{
		AppendRelInfo *appinfo = root->append_rel_array[relid];

		Assert(appinfo != NULL);
		if (!apply_child_basequals(root, parent, rel, rte, appinfo))
		{
			/*
			 * Some restriction clause reduced to constant FALSE or NULL after
			 * substitution, so this child need not be scanned.
			 */
			mark_dummy_rel(root, rel);
		}
	}

	/* Save the finished struct in the query's simple_rel_array */
	root->simple_rel_array[relid] = rel;

	return rel;
}

/*
 * build_simple_grouped_rel
 *	  Construct a new RelOptInfo for a grouped base relation out of an
 *	  existing non-grouped relation. On success, pointer to the corresponding
 *	  RelAggInfo is stored in *agg_info_p in addition to returning the grouped
 *	  relation.
 */
RelOptInfo *
build_base_grouped_rel(PlannerInfo *root, RelOptInfo *rel_plain,
					   RelAggInfo **agg_info_p)
{
	RangeTblEntry *rte;
	RelOptInfo *rel_grouped;
	RelAggInfo *agg_info;

	/* Isn't there any grouping expression to be pushed down? */
	if (root->grouped_var_list == NIL)
		return NULL;

	/* Currently we do not support child relations ("other rels"). */
	if (rel_plain->reloptkind != RELOPT_BASEREL)
		return NULL;

	/* We don't pushdown to empty relation. */
	if (rel_plain->rows == 0)
		return NULL;

	/* Not all RTE kinds are supported when grouping is considered. */
	rte = root->simple_rte_array[rel_plain->relid];
	if (rte->rtekind != RTE_RELATION && rte->rtekind != RTE_JOIN &&
		rte->rtekind != RTE_SUBQUERY && rte->rtekind != RTE_FUNCTION &&
		rte->rtekind != RTE_VALUES)
		return NULL;
	if (rte->tablesample != NULL)
		return NULL;

	/* Grouped append relation is not supported yet. */
	if (rte->inh && rte->relkind != RELKIND_PARTITIONED_TABLE)
		return NULL;

	/* Prepare the information we need for aggregation of the rel contents. */
	agg_info = create_rel_agg_info(root, rel_plain);
	if (agg_info == NULL)
		return NULL;

	/*
	 * Build RelOptInfo from plain rel.
	 *
	 * Note that the plain version and grouped version of the same RelOptInfo
	 * should be identical in most aspects, except for reltarget, paths, and
	 * row estimates. Therefore, to construct the grouped version, we can
	 * simply duplicate the plain version and reset these fields. It's ok for
	 * them to share the same pointer value in other feilds without deep-copy.
	 */
	rel_grouped = makeNode(RelOptInfo);
	memcpy(rel_grouped, rel_plain, sizeof(RelOptInfo));

	/*
	 * Set the appropriate target for grouped paths.
	 *
	 * reltarget should match the target of partially aggregated paths.
	 */
	rel_grouped->reltarget = agg_info->target;

	/* Grouped paths must not be mixed with the plain ones. */
	rel_grouped->pathlist = NIL;
	rel_grouped->partial_pathlist = NIL;
	rel_grouped->cheapest_startup_path = NULL;
	rel_grouped->cheapest_total_path = NULL;
	rel_grouped->cheapest_unique_path = NULL;
	rel_grouped->cheapest_parameterized_paths = NIL;

	/*
	 * The number of aggregation input rows is simply the number of rows of
	 * the non-grouped relation, which should have been estimated by now.
	 */
	agg_info->input_rows = rel_plain->rows;

	/*
	 * The number of output rows is supposedly different (lower) due to
	 * grouping.
	 */
	rel_grouped->rows = estimate_num_groups(root, agg_info->group_exprs,
											agg_info->input_rows, NULL, NULL);

	*agg_info_p = agg_info;
	return rel_grouped;
}

/*
 * find_base_rel
 *	  Find a base or otherrel relation entry, which must already exist.
 */
RelOptInfo *
find_base_rel(PlannerInfo *root, int relid)
{
	RelOptInfo *rel;

	Assert(relid > 0);

	if (relid < root->simple_rel_array_size)
	{
		rel = root->simple_rel_array[relid];
		if (rel)
			return rel;
	}

	elog(ERROR, "no relation entry for relid %d", relid);

	return NULL;				/* keep compiler quiet */
}

/*
 * find_base_rel_ignore_join
 *	  Find a base or otherrel relation entry, which must already exist.
 *
 * Unlike find_base_rel, if relid references an outer join then this
 * will return NULL rather than raising an error.  This is convenient
 * for callers that must deal with relid sets including both base and
 * outer joins.
 */
RelOptInfo *
find_base_rel_ignore_join(PlannerInfo *root, int relid)
{
	Assert(relid > 0);

	if (relid < root->simple_rel_array_size)
	{
		RelOptInfo *rel;
		RangeTblEntry *rte;

		rel = root->simple_rel_array[relid];
		if (rel)
			return rel;

		/*
		 * We could just return NULL here, but for debugging purposes it seems
		 * best to actually verify that the relid is an outer join and not
		 * something weird.
		 */
		rte = root->simple_rte_array[relid];
		if (rte && rte->rtekind == RTE_JOIN && rte->jointype != JOIN_INNER)
			return NULL;
	}

	elog(ERROR, "no relation entry for relid %d", relid);

	return NULL;				/* keep compiler quiet */
}

/*
 * build_join_rel_hash
 *	  Construct the auxiliary hash table for join relations.
 */
static void
build_join_rel_hash(PlannerInfo *root)
{
	HTAB	   *hashtab;
	HASHCTL		hash_ctl;
	ListCell   *l;

	/* Create the hash table */
	hash_ctl.keysize = sizeof(Relids);
	hash_ctl.entrysize = sizeof(JoinHashEntry);
	hash_ctl.hash = bitmap_hash;
	hash_ctl.match = bitmap_match;
	hash_ctl.hcxt = CurrentMemoryContext;
	hashtab = hash_create("JoinRelHashTable",
						  256L,
						  &hash_ctl,
						  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* Insert all the already-existing joinrels */
	foreach(l, root->join_rel_list)
	{
		RelOptInfo *rel = (RelOptInfo *) lfirst(l);
		JoinHashEntry *hentry;
		bool		found;

		hentry = (JoinHashEntry *) hash_search(hashtab,
											   &(rel->relids),
											   HASH_ENTER,
											   &found);
		Assert(!found);
		hentry->join_rel = rel;
	}

	root->join_rel_hash = hashtab;
}

/*
 * find_join_rel
 *	  Returns relation entry corresponding to 'relids' (a set of RT indexes),
 *	  or NULL if none exists.  This is for join relations.
 */
RelOptInfo *
find_join_rel(PlannerInfo *root, Relids relids)
{
	/*
	 * Switch to using hash lookup when list grows "too long".  The threshold
	 * is arbitrary and is known only here.
	 */
	if (!root->join_rel_hash && list_length(root->join_rel_list) > 32)
		build_join_rel_hash(root);

	/*
	 * Use either hashtable lookup or linear search, as appropriate.
	 *
	 * Note: the seemingly redundant hashkey variable is used to avoid taking
	 * the address of relids; unless the compiler is exceedingly smart, doing
	 * so would force relids out of a register and thus probably slow down the
	 * list-search case.
	 */
	if (root->join_rel_hash)
	{
		Relids		hashkey = relids;
		JoinHashEntry *hentry;

		hentry = (JoinHashEntry *) hash_search(root->join_rel_hash,
											   &hashkey,
											   HASH_FIND,
											   NULL);
		if (hentry)
			return hentry->join_rel;
	}
	else
	{
		ListCell   *l;

		foreach(l, root->join_rel_list)
		{
			RelOptInfo *rel = (RelOptInfo *) lfirst(l);

			if (bms_equal(rel->relids, relids))
				return rel;
		}
	}

	return NULL;
}

/*
 * set_foreign_rel_properties
 *		Set up foreign-join fields if outer and inner relation are foreign
 *		tables (or joins) belonging to the same server and assigned to the same
 *		user to check access permissions as.
 *
 * In addition to an exact match of userid, we allow the case where one side
 * has zero userid (implying current user) and the other side has explicit
 * userid that happens to equal the current user; but in that case, pushdown of
 * the join is only valid for the current user.  The useridiscurrent field
 * records whether we had to make such an assumption for this join or any
 * sub-join.
 *
 * Otherwise these fields are left invalid, so GetForeignJoinPaths will not be
 * called for the join relation.
 *
 * GPDB: Also, EXECUTE ON must match. (Perhaps we shouldn't allow EXECUTE
 * ON on individual tables? Then it would be enough to compare server id)
 */
static void
set_foreign_rel_properties(RelOptInfo *joinrel, RelOptInfo *outer_rel,
						   RelOptInfo *inner_rel, List *restrictlist)
{
	if (OidIsValid(outer_rel->serverid) &&
		inner_rel->serverid == outer_rel->serverid &&
		inner_rel->exec_location == outer_rel->exec_location)
	{
		if (inner_rel->exec_location == FTEXECLOCATION_ALL_SEGMENTS)
		{
			ListCell *cell;
			bool mppMatch = false;
			List *l1;
			List *l2;

			l1 = inner_rel->segSeverids;
			l2 = outer_rel->segSeverids;

			if (list_difference_oid(l1, l2))
				return;

			foreach(cell, restrictlist)
			{
				RestrictInfo *info;

				info = lfirst(cell);
				if (IsA(info->clause, OpExpr))
				{
					Expr *larg;
					Expr *rarg;
					OpExpr *opExpr = (OpExpr *) info->clause;

					if (list_length(opExpr->args) != 2)
						continue;

					larg = lfirst(list_head(opExpr->args));
					rarg = lfirst(list_second_cell(opExpr->args));

					if (IsA(larg, Var) && IsA(rarg, Var) &&
						((Var *) larg)->varattno == GpForeignServerAttributeNumber &&
						((Var *) rarg)->varattno == GpForeignServerAttributeNumber)
					{
						mppMatch = true;
						break;
					}
				}
			}

			if (!mppMatch)
				return;
		}

		if (inner_rel->userid == outer_rel->userid)
		{
			joinrel->serverid = outer_rel->serverid;
			joinrel->segSeverids = outer_rel->segSeverids;
			joinrel->userid = outer_rel->userid;
			joinrel->useridiscurrent = outer_rel->useridiscurrent || inner_rel->useridiscurrent;
			joinrel->fdwroutine = outer_rel->fdwroutine;
			joinrel->exec_location = outer_rel->exec_location;
			joinrel->num_segments = outer_rel->num_segments;
		}
		else if (!OidIsValid(inner_rel->userid) &&
				 outer_rel->userid == GetUserId())
		{
			joinrel->serverid = outer_rel->serverid;
			joinrel->segSeverids = outer_rel->segSeverids;
			joinrel->userid = outer_rel->userid;
			joinrel->useridiscurrent = true;
			joinrel->fdwroutine = outer_rel->fdwroutine;
			joinrel->exec_location = outer_rel->exec_location;
			joinrel->num_segments = outer_rel->num_segments;
		}
		else if (!OidIsValid(outer_rel->userid) &&
				 inner_rel->userid == GetUserId())
		{
			joinrel->serverid = outer_rel->serverid;
			joinrel->segSeverids = outer_rel->segSeverids;
			joinrel->userid = inner_rel->userid;
			joinrel->useridiscurrent = true;
			joinrel->fdwroutine = outer_rel->fdwroutine;
			joinrel->exec_location = outer_rel->exec_location;
			joinrel->num_segments = outer_rel->num_segments;
		}
	}
}

/*
 * add_join_rel
 *		Add given join relation to the list of join relations in the given
 *		PlannerInfo. Also add it to the auxiliary hashtable if there is one.
 */
static void
add_join_rel(PlannerInfo *root, RelOptInfo *joinrel)
{
	/* GEQO requires us to append the new joinrel to the end of the list! */
	root->join_rel_list = lappend(root->join_rel_list, joinrel);

	/* store it into the auxiliary hashtable if there is one. */
	if (root->join_rel_hash)
	{
		JoinHashEntry *hentry;
		bool		found;

		hentry = (JoinHashEntry *) hash_search(root->join_rel_hash,
											   &(joinrel->relids),
											   HASH_ENTER,
											   &found);
		Assert(!found);
		hentry->join_rel = joinrel;
	}
}

/*
 * add_grouped_rel_agg_info
 *		Add grouped relation specific info to list.
 *		Also add it to the auxiliary hashtable if there is one.
 */
void
add_grouped_rel_agg_info(PlannerInfo *root, RelOptInfo *rel, RelAggInfo *agginfo)
{
	/* The RelOptInfo must be unique, just attach to RelAggInfo. */
	if (!IS_DUMMY_REL(rel))
		set_cheapest(rel);
	if (agginfo->build_from_plain)
		agginfo->rel_grouped = rel;
	else
		agginfo->rel_grouped_non_plain = rel;

	/* Add to RelAggInfo list if not exist */
	if (!find_grouped_rel_agg_info(root, rel->relids))
	{
		HTAB   *hashtab = root->grouped_rel_info_hash;

		/* Always add to the list. */
		root->grouped_rel_info_list =
			lappend(root->grouped_rel_info_list, agginfo);

		/* Add to hash table if exists */
		if (hashtab)
		{
			GroupedHashEntry   *hentry;
			bool				found;

			hentry = (GroupedHashEntry *) hash_search(hashtab,
													  &(rel->relids),
													  HASH_ENTER,
													  &found);
			Assert(!found);
			hentry->agg_info = agginfo;
		}
	}
}

/*
 * build_grouped_rel_info_hash
 *	  Construct the auxiliary hash table for grouped RelAggInfo.
 */
static void
build_grouped_rel_info_hash(PlannerInfo *root)
{
	HTAB	   *hashtab;
	HASHCTL		hash_ctl;
	ListCell   *l;

	/* Create the hash table */
	hash_ctl.keysize = sizeof(Relids);
	hash_ctl.entrysize = sizeof(GroupedHashEntry);
	hash_ctl.hash = bitmap_hash;
	hash_ctl.match = bitmap_match;
	hash_ctl.hcxt = CurrentMemoryContext;
	hashtab = hash_create("GroupedInfoHashTable",
						  256L,
						  &hash_ctl,
						  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* Insert all the already-existing joinrels */
	foreach(l, root->grouped_rel_info_list)
	{
		RelAggInfo *agginfo = (RelAggInfo *) lfirst(l);
		GroupedHashEntry   *hentry;
		bool				found;

		hentry = (GroupedHashEntry *) hash_search(hashtab,
												  &(agginfo->relids),
												  HASH_ENTER,
												  &found);
		Assert(!found);
		hentry->agg_info = agginfo;
	}

	root->grouped_rel_info_hash = hashtab;
}

/*
 * find_grouped_rel_agg_info
 *	  Returns RelAggInfo corresponding to 'relids' (a set of RT indexes),
 *	  or NULL if none exists.  This is for grouped join relations.
 */
RelAggInfo *
find_grouped_rel_agg_info(PlannerInfo *root, Relids relids)
{
	ListCell   *lc;

	if (!root->grouped_rel_info_hash &&
		list_length(root->grouped_rel_info_list) > 32)
		build_grouped_rel_info_hash(root);

	if (root->grouped_rel_info_hash)
	{
		Relids		hashkey = relids;
		GroupedHashEntry *hentry;

		hentry = (GroupedHashEntry *) hash_search(root->grouped_rel_info_hash,
												  &hashkey,
												  HASH_FIND,
												  NULL);
		if (hentry)
			return hentry->agg_info;
	}
	else
	{
		foreach(lc, root->grouped_rel_info_list)
		{
			RelAggInfo *agg_info = (RelAggInfo *) lfirst(lc);

			if (bms_equal(agg_info->relids, relids))
				return agg_info;
		}
	}
	return NULL;
}

/*
 * get_grouped_rel_agg_info
 *	  Returns RelAggInfo corresponding to 'relids'. Will try to generate new
 *	  RelAggInfo if not exists. Return NULL iff not exists and failed to
 *	  generate.
 */
RelAggInfo *
get_grouped_rel_agg_info(PlannerInfo *root, RelOptInfo *joinrel_plain)
{
	RelAggInfo *agg_info;

	agg_info = find_grouped_rel_agg_info(root, joinrel_plain->relids);
	if (agg_info == NULL)
		agg_info = create_rel_agg_info(root, joinrel_plain);
	return agg_info;
}

static RelOptInfo *
get_cheaper_rel(RelOptInfo *rel1, RelOptInfo *rel2)
{
	int cmp;

	if (rel1 == NULL || IS_DUMMY_REL(rel1))
		return rel2;
	if (rel2 == NULL || IS_DUMMY_REL(rel2))
		return rel1;

	cmp = compare_path_costs(rel1->cheapest_total_path,
							 rel2->cheapest_total_path,
							 TOTAL_COST);
	return cmp < 0 ? rel1 : rel2;
}

/*
 * find_grouped_rel
 *	  Returns grouped relation entry corresponding to 'relids' (a set of RT
 *	  indexes), or NULL if none exists.
 */
RelOptInfo *
find_grouped_rel(PlannerInfo *root, Relids relids)
{
	RelAggInfo *agg_info;

	if (!root->setup_agg_pushdown)
		return NULL;

	agg_info = find_grouped_rel_agg_info(root, relids);
	if (agg_info == NULL)
		return NULL;

	return get_cheaper_rel(agg_info->rel_grouped,
						   agg_info->rel_grouped_non_plain);
}

/*
 * get_grouped_rel
 *	  Get gouped version of given rel.
 *	  We will first look up in the cache, and then try to build a grouped
 *	  version of this rel, if it's a BASE_REL and we have not tried.
 */
RelOptInfo *
get_grouped_rel(PlannerInfo *root, RelOptInfo *rel)
{
	RelOptInfo *rel_grouped;
	RelAggInfo *agg_info;

	/* Try to find exist one. */
	rel_grouped = find_grouped_rel(root, rel->relids);
	if (rel_grouped != NULL)
		return !IS_DUMMY_REL(rel_grouped) ? rel_grouped : NULL;

	/*
	 * Build grouped version iff given rel is BASE_REL, grouped
	 * JOIN_REL will be handled by make_grouped_join_rel().
	 */
	if (rel->reloptkind != RELOPT_BASEREL)
		return NULL;

	rel_grouped = build_base_grouped_rel(root, rel, &agg_info);
	if (rel_grouped != NULL)
	{
		/* Create related paths and remember. */
		generate_grouping_paths(root, rel_grouped, rel, agg_info);
		add_grouped_rel_agg_info(root, rel_grouped, agg_info);
		return !IS_DUMMY_REL(rel_grouped) ? rel_grouped : NULL;
	}
	return NULL;
}

/*
 * build_join_rel
 *	  Returns relation entry corresponding to the union of two given rels,
 *	  creating a new relation entry if none already exists.
 *
 * 'joinrelids' is the Relids set that uniquely identifies the join
 * 'outer_rel' and 'inner_rel' are relation nodes for the relations to be
 *		joined
 * 'sjinfo': join context info
 * 'pushed_down_joins': any pushed-down outer joins that are now completed
 * 'restrictlist_ptr': result variable.  If not NULL, *restrictlist_ptr
 *		receives the list of RestrictInfo nodes that apply to this
 *		particular pair of joinable relations.
 *
 * restrictlist_ptr makes the routine's API a little grotty, but it saves
 * duplicated calculation of the restrictlist...
 */
RelOptInfo *
build_join_rel(PlannerInfo *root,
			   Relids joinrelids,
			   RelOptInfo *outer_rel,
			   RelOptInfo *inner_rel,
			   SpecialJoinInfo *sjinfo,
			   List **restrictlist_ptr,
			   List *pushed_down_joins,
			   RelAggInfo *agg_info)
{
	RelOptInfo *joinrel;
	List	   *restrictlist;
	bool		grouped = agg_info != NULL;

	/* This function should be used only for join between parents. */
	Assert(!IS_OTHER_REL(outer_rel) && !IS_OTHER_REL(inner_rel));

	/*
	 * See if we already have a joinrel for this set of base rels.
	 *
	 * NB: We only call this function to build a grouped relation when it does
	 * not exist, so we won't try to find here.
	 */
	joinrel = !grouped ? find_join_rel(root, joinrelids) : NULL;

	if (joinrel)
	{
		/*
		 * Yes, so we only need to figure the restrictlist for this particular
		 * pair of component relations.
		 */
		if (restrictlist_ptr)
			*restrictlist_ptr = build_joinrel_restrictlist(root,
														   joinrel,
														   outer_rel,
														   inner_rel,
														   sjinfo);
		return joinrel;
	}

	/*
	 * Nope, so make one.
	 */
	joinrel = makeNode(RelOptInfo);
	joinrel->reloptkind = RELOPT_JOINREL;
	joinrel->relids = bms_copy(joinrelids);
	joinrel->rows = 0;
	/* cheap startup cost is interesting iff not all tuples to be retrieved */
	joinrel->consider_startup = (root->tuple_fraction > 0);
	joinrel->consider_param_startup = false;
	joinrel->consider_parallel = false;
	joinrel->reltarget = create_empty_pathtarget();
	joinrel->pathlist = NIL;
	joinrel->ppilist = NIL;
	joinrel->partial_pathlist = NIL;
	joinrel->cheapest_startup_path = NULL;
	joinrel->cheapest_total_path = NULL;
	joinrel->cheapest_unique_path = NULL;
	joinrel->cheapest_parameterized_paths = NIL;
	/* init direct_lateral_relids from children; we'll finish it up below */
	joinrel->direct_lateral_relids =
		bms_union(outer_rel->direct_lateral_relids,
				  inner_rel->direct_lateral_relids);
	joinrel->lateral_relids = min_join_parameterization(root, joinrel->relids,
														outer_rel, inner_rel);
	joinrel->relid = 0;			/* indicates not a baserel */
	joinrel->rtekind = RTE_JOIN;
	joinrel->min_attr = 0;
	joinrel->max_attr = 0;
	joinrel->attr_needed = NULL;
	joinrel->attr_widths = NULL;
	joinrel->nulling_relids = NULL;
	joinrel->lateral_vars = NIL;
	joinrel->lateral_referencers = NULL;
	joinrel->indexlist = NIL;
	joinrel->statlist = NIL;
	joinrel->pages = 0;
	joinrel->tuples = 0;
	joinrel->allvisfrac = 0;
	joinrel->eclass_indexes = NULL;
	joinrel->subroot = NULL;
	joinrel->subplan_params = NIL;
	joinrel->rel_parallel_workers = -1;
	joinrel->amflags = 0;
	joinrel->serverid = InvalidOid;
	joinrel->segSeverids = NIL;
	joinrel->userid = InvalidOid;
	joinrel->useridiscurrent = false;
	joinrel->exec_location = FTEXECLOCATION_NOT_DEFINED;
	joinrel->fdwroutine = NULL;
	joinrel->fdw_private = NULL;
	joinrel->unique_for_rels = NIL;
	joinrel->non_unique_for_rels = NIL;
	joinrel->baserestrictinfo = NIL;
	joinrel->baserestrictcost.startup = 0;
	joinrel->baserestrictcost.per_tuple = 0;
	joinrel->baserestrict_min_security = UINT_MAX;
	joinrel->joininfo = NIL;
	joinrel->has_eclass_joins = false;
	joinrel->consider_partitionwise_join = false;	/* might get changed later */
	joinrel->parent = NULL;
	joinrel->top_parent = NULL;
	joinrel->top_parent_relids = NULL;
	joinrel->part_scheme = NULL;
	joinrel->nparts = -1;
	joinrel->boundinfo = NULL;
	joinrel->partbounds_merged = false;
	joinrel->partition_qual = NIL;
	joinrel->part_rels = NULL;
	joinrel->live_parts = NULL;
	joinrel->all_partrels = NULL;
	joinrel->partexprs = NULL;
	joinrel->nullable_partexprs = NULL;

	/*
	 * Fill the joinrel's tlist with just the Vars and PHVs that need to be
	 * output from this join (ie, are needed for higher joinclauses or final
	 * output).
	 *
	 * NOTE: the tlist order for a join rel will depend on which pair of outer
	 * and inner rels we first try to build it from.  But the contents should
	 * be the same regardless.
	 */
	if (!grouped)
	{
		build_joinrel_tlist(root, joinrel, outer_rel, sjinfo, pushed_down_joins,
							(sjinfo->jointype == JOIN_FULL));
		build_joinrel_tlist(root, joinrel, inner_rel, sjinfo, pushed_down_joins,
							(sjinfo->jointype != JOIN_INNER));
		add_placeholders_to_joinrel(root, joinrel, outer_rel, inner_rel, sjinfo);
	}
	else
	{
		/* Target and costs already created in create_rel_agg_info */
		joinrel->reltarget = agg_info->target;
	}

	/*
	 * add_placeholders_to_joinrel also took care of adding the ph_lateral
	 * sets of any PlaceHolderVars computed here to direct_lateral_relids, so
	 * now we can finish computing that.  This is much like the computation of
	 * the transitively-closed lateral_relids in min_join_parameterization,
	 * except that here we *do* have to consider the added PHVs.
	 */
	joinrel->direct_lateral_relids =
		bms_del_members(joinrel->direct_lateral_relids, joinrel->relids);

	/*
	 * Construct restrict and join clause lists for the new joinrel. (The
	 * caller might or might not need the restrictlist, but I need it anyway
	 * for set_joinrel_size_estimates().)
	 */
	restrictlist = build_joinrel_restrictlist(root, joinrel,
											  outer_rel, inner_rel,
											  sjinfo);

	/* Compute information relevant to the foreign relations. */
	set_foreign_rel_properties(joinrel, outer_rel, inner_rel, restrictlist);

	if (restrictlist_ptr)
		*restrictlist_ptr = restrictlist;
	build_joinrel_joinlist(joinrel, outer_rel, inner_rel);

	/*
	 * This is also the right place to check whether the joinrel has any
	 * pending EquivalenceClass joins.
	 */
	joinrel->has_eclass_joins = has_relevant_eclass_joinclause(root, joinrel);

	/* Store the partition information. */
	build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo,
								 restrictlist);

	if (grouped && agg_info->build_from_plain)
	{
		/*
		 * Grouped version of join rel, and build from 2 plain rels. In this
		 * case we need to join plain rels and then apply partial aggregate,
		 * which will essentially changes the number of rows.
		 */
		joinrel->rows = estimate_num_groups(root, agg_info->group_exprs,
											agg_info->input_rows, NULL, NULL);
	}
	else
	{
		/*
		 * Set estimates of the joinrel's size.
		 *
		 * Row estimate logic for plain rel and grouped rel are the same,
		 * because in this case, grouped rel is simply formed by join grouped
		 * rel and plain rel together.
		 */
		set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel,
								   sjinfo, restrictlist);
	}

	/*
	 * Set the consider_parallel flag if this joinrel could potentially be
	 * scanned within a parallel worker.  If this flag is false for either
	 * inner_rel or outer_rel, then it must be false for the joinrel also.
	 * Even if both are true, there might be parallel-restricted expressions
	 * in the targetlist or quals.
	 *
	 * Note that if there are more than two rels in this relation, they could
	 * be divided between inner_rel and outer_rel in any arbitrary way.  We
	 * assume this doesn't matter, because we should hit all the same baserels
	 * and joinclauses while building up to this joinrel no matter which we
	 * take; therefore, we should make the same decision here however we get
	 * here.
	 */
	if (inner_rel->consider_parallel && outer_rel->consider_parallel &&
		is_parallel_safe(root, (Node *) restrictlist) &&
		is_parallel_safe(root, (Node *) joinrel->reltarget->exprs))
		joinrel->consider_parallel = true;

	/*
	 * Add the joinrel to the PlannerInfo. Ignore grouped rel here, it will
	 * be handled by the caller, and it should not be added to join rel list.
	 */
	if (!grouped)
		add_join_rel(root, joinrel);

	/*
	 * Also, if dynamic-programming join search is active, add the new joinrel
	 * to the appropriate sublist.  Note: you might think the Assert on number
	 * of members should be for equality, but some of the level 1 rels might
	 * have been joinrels already, so we can only assert <=.
	 */
	if (root->join_rel_level && !grouped)
	{
		Assert(root->join_cur_level > 0);
		Assert(root->join_cur_level <= bms_num_members(joinrel->relids));
		root->join_rel_level[root->join_cur_level] =
			lappend(root->join_rel_level[root->join_cur_level], joinrel);
	}

	return joinrel;
}

/*
 * build_child_join_rel
 *	  Builds RelOptInfo representing join between given two child relations.
 *
 * 'outer_rel' and 'inner_rel' are the RelOptInfos of child relations being
 *		joined
 * 'parent_joinrel' is the RelOptInfo representing the join between parent
 *		relations. Some of the members of new RelOptInfo are produced by
 *		translating corresponding members of this RelOptInfo
 * 'restrictlist': list of RestrictInfo nodes that apply to this particular
 *		pair of joinable relations
 * 'sjinfo': child join's join-type details
 */
RelOptInfo *
build_child_join_rel(PlannerInfo *root, RelOptInfo *outer_rel,
					 RelOptInfo *inner_rel, RelOptInfo *parent_joinrel,
					 List *restrictlist, SpecialJoinInfo *sjinfo)
{
	RelOptInfo *joinrel = makeNode(RelOptInfo);
	AppendRelInfo **appinfos;
	int			nappinfos;

	/* Only joins between "other" relations land here. */
	Assert(IS_OTHER_REL(outer_rel) && IS_OTHER_REL(inner_rel));

	/* The parent joinrel should have consider_partitionwise_join set. */
	Assert(parent_joinrel->consider_partitionwise_join);

	/*
	 * Find the AppendRelInfo structures for the child baserels.  We'll need
	 * these for computing the child join's relid set, and later for mapping
	 * Vars to the child rel.
	 */
	appinfos = find_appinfos_by_relids(root,
									   bms_union(outer_rel->relids,
												 inner_rel->relids),
									   &nappinfos);

	joinrel->reloptkind = RELOPT_OTHER_JOINREL;
	joinrel->relids = adjust_child_relids(parent_joinrel->relids,
										  nappinfos, appinfos);
	joinrel->rows = 0;
	/* cheap startup cost is interesting iff not all tuples to be retrieved */
	joinrel->consider_startup = (root->tuple_fraction > 0);
	joinrel->consider_param_startup = false;
	joinrel->consider_parallel = false;
	joinrel->reltarget = create_empty_pathtarget();
	joinrel->pathlist = NIL;
	joinrel->ppilist = NIL;
	joinrel->partial_pathlist = NIL;
	joinrel->cheapest_startup_path = NULL;
	joinrel->cheapest_total_path = NULL;
	joinrel->cheapest_unique_path = NULL;
	joinrel->cheapest_parameterized_paths = NIL;
	joinrel->direct_lateral_relids = NULL;
	joinrel->lateral_relids = NULL;
	joinrel->relid = 0;			/* indicates not a baserel */
	joinrel->rtekind = RTE_JOIN;
	joinrel->min_attr = 0;
	joinrel->max_attr = 0;
	joinrel->attr_needed = NULL;
	joinrel->attr_widths = NULL;
	joinrel->nulling_relids = NULL;
	joinrel->lateral_vars = NIL;
	joinrel->lateral_referencers = NULL;
	joinrel->indexlist = NIL;
	joinrel->pages = 0;
	joinrel->tuples = 0;
	joinrel->allvisfrac = 0;
	joinrel->eclass_indexes = NULL;
	joinrel->subroot = NULL;
	joinrel->subplan_params = NIL;
	joinrel->amflags = 0;
	joinrel->serverid = InvalidOid;
	joinrel->segSeverids = NIL;
	joinrel->userid = InvalidOid;
	joinrel->useridiscurrent = false;
	joinrel->fdwroutine = NULL;
	joinrel->fdw_private = NULL;
	joinrel->baserestrictinfo = NIL;
	joinrel->baserestrictcost.startup = 0;
	joinrel->baserestrictcost.per_tuple = 0;
	joinrel->joininfo = NIL;
	joinrel->has_eclass_joins = false;
	joinrel->consider_partitionwise_join = false;	/* might get changed later */
	joinrel->parent = parent_joinrel;
	joinrel->top_parent = parent_joinrel->top_parent ? parent_joinrel->top_parent : parent_joinrel;
	joinrel->top_parent_relids = joinrel->top_parent->relids;
	joinrel->part_scheme = NULL;
	joinrel->nparts = -1;
	joinrel->boundinfo = NULL;
	joinrel->partbounds_merged = false;
	joinrel->partition_qual = NIL;
	joinrel->part_rels = NULL;
	joinrel->live_parts = NULL;
	joinrel->all_partrels = NULL;
	joinrel->partexprs = NULL;
	joinrel->nullable_partexprs = NULL;

	/* Compute information relevant to foreign relations. */
	set_foreign_rel_properties(joinrel, outer_rel, inner_rel, restrictlist);

	/* Set up reltarget struct */
	build_child_join_reltarget(root, parent_joinrel, joinrel,
							   nappinfos, appinfos);

	/* Construct joininfo list. */
	joinrel->joininfo = (List *) adjust_appendrel_attrs(root,
														(Node *) parent_joinrel->joininfo,
														nappinfos,
														appinfos);

	/*
	 * Lateral relids referred in child join will be same as that referred in
	 * the parent relation.
	 */
	joinrel->direct_lateral_relids = (Relids) bms_copy(parent_joinrel->direct_lateral_relids);
	joinrel->lateral_relids = (Relids) bms_copy(parent_joinrel->lateral_relids);

	/*
	 * If the parent joinrel has pending equivalence classes, so does the
	 * child.
	 */
	joinrel->has_eclass_joins = parent_joinrel->has_eclass_joins;

	/* Is the join between partitions itself partitioned? */
	build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo,
								 restrictlist);

	/* Child joinrel is parallel safe if parent is parallel safe. */
	joinrel->consider_parallel = parent_joinrel->consider_parallel;

	/* Set estimates of the child-joinrel's size. */
	set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel,
							   sjinfo, restrictlist);

	/* We build the join only once. */
	Assert(!find_join_rel(root, joinrel->relids));

	/* Add the relation to the PlannerInfo. */
	add_join_rel(root, joinrel);

	/*
	 * We might need EquivalenceClass members corresponding to the child join,
	 * so that we can represent sort pathkeys for it.  As with children of
	 * baserels, we shouldn't need this unless there are relevant eclass joins
	 * (implying that a merge join might be possible) or pathkeys to sort by.
	 */
	if (joinrel->has_eclass_joins || has_useful_pathkeys(root, parent_joinrel))
		add_child_join_rel_equivalences(root,
										nappinfos, appinfos,
										parent_joinrel, joinrel);

	pfree(appinfos);

	return joinrel;
}

/*
 * min_join_parameterization
 *
 * Determine the minimum possible parameterization of a joinrel, that is, the
 * set of other rels it contains LATERAL references to.  We save this value in
 * the join's RelOptInfo.  This function is split out of build_join_rel()
 * because join_is_legal() needs the value to check a prospective join.
 */
Relids
min_join_parameterization(PlannerInfo *root,
						  Relids joinrelids,
						  RelOptInfo *outer_rel,
						  RelOptInfo *inner_rel)
{
	Relids		result;

	/*
	 * Basically we just need the union of the inputs' lateral_relids, less
	 * whatever is already in the join.
	 *
	 * It's not immediately obvious that this is a valid way to compute the
	 * result, because it might seem that we're ignoring possible lateral refs
	 * of PlaceHolderVars that are due to be computed at the join but not in
	 * either input.  However, because create_lateral_join_info() already
	 * charged all such PHV refs to each member baserel of the join, they'll
	 * be accounted for already in the inputs' lateral_relids.  Likewise, we
	 * do not need to worry about doing transitive closure here, because that
	 * was already accounted for in the original baserel lateral_relids.
	 */
	result = bms_union(outer_rel->lateral_relids, inner_rel->lateral_relids);
	result = bms_del_members(result, joinrelids);
	return result;
}

/*
 * build_joinrel_tlist
 *	  Builds a join relation's target list from an input relation.
 *	  (This is invoked twice to handle the two input relations.)
 *
 * The join's targetlist includes all Vars of its member relations that
 * will still be needed above the join.  This subroutine adds all such
 * Vars from the specified input rel's tlist to the join rel's tlist.
 * Likewise for any PlaceHolderVars emitted by the input rel.
 *
 * We also compute the expected width of the join's output, making use
 * of data that was cached at the baserel level by set_rel_width().
 *
 * Pass can_null as true if the join is an outer join that can null Vars
 * from this input relation.  If so, we will (normally) add the join's relid
 * to the nulling bitmaps of Vars and PHVs bubbled up from the input.
 *
 * When forming an outer join's target list, special handling is needed in
 * case the outer join was commuted with another one per outer join identity 3
 * (see optimizer/README).  We must take steps to ensure that the output Vars
 * have the same nulling bitmaps that they would if the two joins had been
 * done in syntactic order; else they won't match Vars appearing higher in
 * the query tree.  An exception to the match-the-syntactic-order rule is
 * that when an outer join is pushed down into another one's RHS per identity
 * 3, we can't mark its Vars as nulled until the now-upper outer join is also
 * completed.  So we need to do three things:
 *
 * First, we add the outer join's relid to the nulling bitmap only if the
 * outer join has been completely performed and the Var or PHV actually
 * comes from within the syntactically nullable side(s) of the outer join.
 * This takes care of the possibility that we have transformed
 *		(A leftjoin B on (Pab)) leftjoin C on (Pbc)
 * to
 *		A leftjoin (B leftjoin C on (Pbc)) on (Pab)
 * Here the pushed-down B/C join cannot mark C columns as nulled yet,
 * while the now-upper A/B join must not mark C columns as nulled by itself.
 *
 * Second, perform the same operation for each SpecialJoinInfo listed in
 * pushed_down_joins (which, in this example, would be the B/C join when
 * we are at the now-upper A/B join).  This allows the now-upper join to
 * complete the marking of "C" Vars that now have fully valid values.
 *
 * Third, any relid in sjinfo->commute_above_r that is already part of
 * the joinrel is added to the nulling bitmaps of nullable Vars and PHVs.
 * This takes care of the reverse case where we implement
 *		A leftjoin (B leftjoin C on (Pbc)) on (Pab)
 * as
 *		(A leftjoin B on (Pab)) leftjoin C on (Pbc)
 * The C columns emitted by the B/C join need to be shown as nulled by both
 * the B/C and A/B joins, even though they've not physically traversed the
 * A/B join.
 */
static void
build_joinrel_tlist(PlannerInfo *root, RelOptInfo *joinrel,
					RelOptInfo *input_rel,
					SpecialJoinInfo *sjinfo,
					List *pushed_down_joins,
					bool can_null)
{
	Relids		relids = joinrel->relids;
	ListCell   *vars;
	ListCell   *lc;

	foreach(vars, input_rel->reltarget->exprs)
	{
		Var		   *var = (Var *) lfirst(vars);

		/*
		 * For a PlaceHolderVar, we have to look up the PlaceHolderInfo.
		 */
		if (IsA(var, PlaceHolderVar))
		{
			PlaceHolderVar *phv = (PlaceHolderVar *) var;
			PlaceHolderInfo *phinfo = find_placeholder_info(root, phv);

			/* Is it still needed above this joinrel? */
			if (bms_nonempty_difference(phinfo->ph_needed, relids))
			{
				/*
				 * Yup, add it to the output.  If this join potentially nulls
				 * this input, we have to update the PHV's phnullingrels,
				 * which means making a copy.
				 */
				if (can_null)
				{
					phv = copyObject(phv);
					/* See comments above to understand this logic */
					if (sjinfo->ojrelid != 0 &&
						bms_is_member(sjinfo->ojrelid, relids) &&
						(bms_is_subset(phv->phrels, sjinfo->syn_righthand) ||
						 (sjinfo->jointype == JOIN_FULL &&
						  bms_is_subset(phv->phrels, sjinfo->syn_lefthand))))
						phv->phnullingrels = bms_add_member(phv->phnullingrels,
															sjinfo->ojrelid);
					foreach(lc, pushed_down_joins)
					{
						SpecialJoinInfo *othersj = (SpecialJoinInfo *) lfirst(lc);

						Assert(bms_is_member(othersj->ojrelid, relids));
						if (bms_is_subset(phv->phrels, othersj->syn_righthand))
							phv->phnullingrels = bms_add_member(phv->phnullingrels,
																othersj->ojrelid);
					}
					phv->phnullingrels =
						bms_join(phv->phnullingrels,
								 bms_intersect(sjinfo->commute_above_r,
											   relids));
				}

				joinrel->reltarget->exprs = lappend(joinrel->reltarget->exprs,
													phv);
				/* Bubbling up the precomputed result has cost zero */
				joinrel->reltarget->width += phinfo->ph_width;
			}
			continue;
		}

		/*
		 * Otherwise, anything in a baserel or joinrel targetlist ought to be
		 * a Var.  (More general cases can only appear in appendrel child
		 * rels, which will never be seen here.)
		 */
		if (!IsA(var, Var))
			elog(ERROR, "unexpected node type in rel targetlist: %d",
				 (int) nodeTag(var));

		if (var->varno == ROWID_VAR)
		{
			/* UPDATE/DELETE/MERGE row identity vars are always needed */
			RowIdentityVarInfo *ridinfo = (RowIdentityVarInfo *)
				list_nth(root->row_identity_vars, var->varattno - 1);

			/* Update reltarget width estimate from RowIdentityVarInfo */
			joinrel->reltarget->width += ridinfo->rowidwidth;
		}
		else
		{
			RelOptInfo *baserel;
			int			ndx;

			/* Get the Var's original base rel */
			baserel = find_base_rel(root, var->varno);

			/* Is it still needed above this joinrel? */
			ndx = var->varattno - baserel->min_attr;
			if (!bms_nonempty_difference(baserel->attr_needed[ndx], relids))
				continue;		/* nope, skip it */

			/* Update reltarget width estimate from baserel's attr_widths */
			joinrel->reltarget->width += baserel->attr_widths[ndx];
		}

		/*
		 * Add the Var to the output.  If this join potentially nulls this
		 * input, we have to update the Var's varnullingrels, which means
		 * making a copy.  But note that we don't ever add nullingrel bits to
		 * row identity Vars (cf. comments in setrefs.c).
		 */
		if (can_null && var->varno != ROWID_VAR)
		{
			var = copyObject(var);
			/* See comments above to understand this logic */
			if (sjinfo->ojrelid != 0 &&
				bms_is_member(sjinfo->ojrelid, relids) &&
				(bms_is_member(var->varno, sjinfo->syn_righthand) ||
				 (sjinfo->jointype == JOIN_FULL &&
				  bms_is_member(var->varno, sjinfo->syn_lefthand))))
				var->varnullingrels = bms_add_member(var->varnullingrels,
													 sjinfo->ojrelid);
			foreach(lc, pushed_down_joins)
			{
				SpecialJoinInfo *othersj = (SpecialJoinInfo *) lfirst(lc);

				Assert(bms_is_member(othersj->ojrelid, relids));
				if (bms_is_member(var->varno, othersj->syn_righthand))
					var->varnullingrels = bms_add_member(var->varnullingrels,
														 othersj->ojrelid);
			}
			var->varnullingrels =
				bms_join(var->varnullingrels,
						 bms_intersect(sjinfo->commute_above_r,
									   relids));
		}

		joinrel->reltarget->exprs = lappend(joinrel->reltarget->exprs,
											var);

		/* Vars have cost zero, so no need to adjust reltarget->cost */
	}
}

/*
 * build_joinrel_restrictlist
 * build_joinrel_joinlist
 *	  These routines build lists of restriction and join clauses for a
 *	  join relation from the joininfo lists of the relations it joins.
 *
 *	  These routines are separate because the restriction list must be
 *	  built afresh for each pair of input sub-relations we consider, whereas
 *	  the join list need only be computed once for any join RelOptInfo.
 *	  The join list is fully determined by the set of rels making up the
 *	  joinrel, so we should get the same results (up to ordering) from any
 *	  candidate pair of sub-relations.  But the restriction list is whatever
 *	  is not handled in the sub-relations, so it depends on which
 *	  sub-relations are considered.
 *
 *	  If a join clause from an input relation refers to base+OJ rels still not
 *	  present in the joinrel, then it is still a join clause for the joinrel;
 *	  we put it into the joininfo list for the joinrel.  Otherwise,
 *	  the clause is now a restrict clause for the joined relation, and we
 *	  return it to the caller of build_joinrel_restrictlist() to be stored in
 *	  join paths made from this pair of sub-relations.  (It will not need to
 *	  be considered further up the join tree.)
 *
 *	  In many cases we will find the same RestrictInfos in both input
 *	  relations' joinlists, so be careful to eliminate duplicates.
 *	  Pointer equality should be a sufficient test for dups, since all
 *	  the various joinlist entries ultimately refer to RestrictInfos
 *	  pushed into them by distribute_restrictinfo_to_rels().
 *
 * 'joinrel' is a join relation node
 * 'outer_rel' and 'inner_rel' are a pair of relations that can be joined
 *		to form joinrel.
 * 'sjinfo': join context info
 *
 * build_joinrel_restrictlist() returns a list of relevant restrictinfos,
 * whereas build_joinrel_joinlist() stores its results in the joinrel's
 * joininfo list.  One or the other must accept each given clause!
 *
 * NB: Formerly, we made deep(!) copies of each input RestrictInfo to pass
 * up to the join relation.  I believe this is no longer necessary, because
 * RestrictInfo nodes are no longer context-dependent.  Instead, just include
 * the original nodes in the lists made for the join relation.
 */
static List *
build_joinrel_restrictlist(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outer_rel,
						   RelOptInfo *inner_rel,
						   SpecialJoinInfo *sjinfo)
{
	List	   *result;
	Relids		both_input_relids;

	both_input_relids = bms_union(outer_rel->relids, inner_rel->relids);

	/*
	 * Collect all the clauses that syntactically belong at this level,
	 * eliminating any duplicates (important since we will see many of the
	 * same clauses arriving from both input relations).
	 */
	result = subbuild_joinrel_restrictlist(root, joinrel, outer_rel,
										   both_input_relids, NIL);
	result = subbuild_joinrel_restrictlist(root, joinrel, inner_rel,
										   both_input_relids, result);

	/*
	 * Add on any clauses derived from EquivalenceClasses.  These cannot be
	 * redundant with the clauses in the joininfo lists, so don't bother
	 * checking.
	 */
	result = list_concat(result,
						 generate_join_implied_equalities(root,
														  joinrel->relids,
														  outer_rel->relids,
														  inner_rel,
														  sjinfo));

	return result;
}

static void
build_joinrel_joinlist(RelOptInfo *joinrel,
					   RelOptInfo *outer_rel,
					   RelOptInfo *inner_rel)
{
	List	   *result;

	/*
	 * Collect all the clauses that syntactically belong above this level,
	 * eliminating any duplicates (important since we will see many of the
	 * same clauses arriving from both input relations).
	 */
	result = subbuild_joinrel_joinlist(joinrel, outer_rel->joininfo, NIL);
	result = subbuild_joinrel_joinlist(joinrel, inner_rel->joininfo, result);

	joinrel->joininfo = result;
}

static List *
subbuild_joinrel_restrictlist(PlannerInfo *root,
							  RelOptInfo *joinrel,
							  RelOptInfo *input_rel,
							  Relids both_input_relids,
							  List *new_restrictlist)
{
	ListCell   *l;

	foreach(l, input_rel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		if (bms_is_subset(rinfo->required_relids, joinrel->relids))
		{
			/*
			 * This clause should become a restriction clause for the joinrel,
			 * since it refers to no outside rels.  However, if it's a clone
			 * clause then it might be too late to evaluate it, so we have to
			 * check.  (If it is too late, just ignore the clause, taking it
			 * on faith that another clone was or will be selected.)  Clone
			 * clauses should always be outer-join clauses, so we compare
			 * against both_input_relids.
			 */
			if (rinfo->has_clone || rinfo->is_clone)
			{
				Assert(!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids));
				if (!bms_is_subset(rinfo->required_relids, both_input_relids))
					continue;
				if (bms_overlap(rinfo->incompatible_relids, both_input_relids))
					continue;
			}
			else
			{
				/*
				 * For non-clone clauses, we just Assert it's OK.  These might
				 * be either join or filter clauses; if it's a join clause
				 * then it should not refer to the current join's output.
				 * (There is little point in checking incompatible_relids,
				 * because it'll be NULL.)
				 */
				Assert(RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids) ||
					   bms_is_subset(rinfo->required_relids,
									 both_input_relids));
			}

			/*
			 * OK, so add it to the list, being careful to eliminate
			 * duplicates.  (Since RestrictInfo nodes in different joinlists
			 * will have been multiply-linked rather than copied, pointer
			 * equality should be a sufficient test.)
			 */
			new_restrictlist = list_append_unique_ptr(new_restrictlist, rinfo);
		}
		else
		{
			/*
			 * This clause is still a join clause at this level, so we ignore
			 * it in this routine.
			 */
		}
	}

	return new_restrictlist;
}

static List *
subbuild_joinrel_joinlist(RelOptInfo *joinrel,
						  List *joininfo_list,
						  List *new_joininfo)
{
	ListCell   *l;

	/* Expected to be called only for join between parent relations. */
	Assert(joinrel->reloptkind == RELOPT_JOINREL);

	foreach(l, joininfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		if (bms_is_subset(rinfo->required_relids, joinrel->relids))
		{
			/*
			 * This clause becomes a restriction clause for the joinrel, since
			 * it refers to no outside rels.  So we can ignore it in this
			 * routine.
			 */
		}
		else
		{
			/*
			 * This clause is still a join clause at this level, so add it to
			 * the new joininfo list, being careful to eliminate duplicates.
			 * (Since RestrictInfo nodes in different joinlists will have been
			 * multiply-linked rather than copied, pointer equality should be
			 * a sufficient test.)
			 */
			new_joininfo = list_append_unique_ptr(new_joininfo, rinfo);
		}
	}

	return new_joininfo;
}


/*
 * fetch_upper_rel
 *		Build a RelOptInfo describing some post-scan/join query processing,
 *		or return a pre-existing one if somebody already built it.
 *
 * An "upper" relation is identified by an UpperRelationKind and a Relids set.
 * The meaning of the Relids set is not specified here, and very likely will
 * vary for different relation kinds.
 *
 * Most of the fields in an upper-level RelOptInfo are not used and are not
 * set here (though makeNode should ensure they're zeroes).  We basically only
 * care about fields that are of interest to add_path() and set_cheapest().
 */
RelOptInfo *
fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind, Relids relids)
{
	RelOptInfo *upperrel;
	ListCell   *lc;

	/*
	 * For the moment, our indexing data structure is just a List for each
	 * relation kind.  If we ever get so many of one kind that this stops
	 * working well, we can improve it.  No code outside this function should
	 * assume anything about how to find a particular upperrel.
	 */

	/* If we already made this upperrel for the query, return it */
	foreach(lc, root->upper_rels[kind])
	{
		upperrel = (RelOptInfo *) lfirst(lc);

		if (bms_equal(upperrel->relids, relids))
			return upperrel;
	}

	upperrel = makeNode(RelOptInfo);
	upperrel->reloptkind = RELOPT_UPPER_REL;
	upperrel->relids = bms_copy(relids);

	/* cheap startup cost is interesting iff not all tuples to be retrieved */
	upperrel->consider_startup = (root->tuple_fraction > 0);
	upperrel->consider_param_startup = false;
	upperrel->consider_parallel = false;	/* might get changed later */
	upperrel->reltarget = create_empty_pathtarget();
	upperrel->pathlist = NIL;
	upperrel->cheapest_startup_path = NULL;
	upperrel->cheapest_total_path = NULL;
	upperrel->cheapest_unique_path = NULL;
	upperrel->cheapest_parameterized_paths = NIL;

	root->upper_rels[kind] = lappend(root->upper_rels[kind], upperrel);

	return upperrel;
}


/*
 * find_childrel_parents
 *		Compute the set of parent relids of an appendrel child rel.
 *
 * Since appendrels can be nested, a child could have multiple levels of
 * appendrel ancestors.  This function computes a Relids set of all the
 * parent relation IDs.
 */
Relids
find_childrel_parents(PlannerInfo *root, RelOptInfo *rel)
{
	Relids		result = NULL;

	Assert(rel->reloptkind == RELOPT_OTHER_MEMBER_REL);
	Assert(rel->relid > 0 && rel->relid < root->simple_rel_array_size);

	do
	{
		AppendRelInfo *appinfo = root->append_rel_array[rel->relid];
		Index		prelid = appinfo->parent_relid;

		result = bms_add_member(result, prelid);

		/* traverse up to the parent rel, loop if it's also a child rel */
		rel = find_base_rel(root, prelid);
	} while (rel->reloptkind == RELOPT_OTHER_MEMBER_REL);

	Assert(rel->reloptkind == RELOPT_BASEREL);

	return result;
}


/*
 * get_baserel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a base relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 */
ParamPathInfo *
get_baserel_parampathinfo(PlannerInfo *root, RelOptInfo *baserel,
						  Relids required_outer)
{
	ParamPathInfo *ppi;
	Relids		joinrelids;
	List	   *pclauses;
	List	   *eqclauses;
	Bitmapset  *pserials;
	double		rows;
	ListCell   *lc;

	/* If rel has LATERAL refs, every path for it should account for them */
	Assert(bms_is_subset(baserel->lateral_relids, required_outer));

	/* Unparameterized paths have no ParamPathInfo */
	if (bms_is_empty(required_outer))
		return NULL;

	Assert(!bms_overlap(baserel->relids, required_outer));

	/* If we already have a PPI for this parameterization, just return it */
	if ((ppi = find_param_path_info(baserel, required_outer)))
		return ppi;

	/*
	 * Identify all joinclauses that are movable to this base rel given this
	 * parameterization.
	 */
	joinrelids = bms_union(baserel->relids, required_outer);
	pclauses = NIL;
	foreach(lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (join_clause_is_movable_into(rinfo,
										baserel->relids,
										joinrelids))
			pclauses = lappend(pclauses, rinfo);
	}

	/*
	 * Add in joinclauses generated by EquivalenceClasses, too.  (These
	 * necessarily satisfy join_clause_is_movable_into; but in assert-enabled
	 * builds, let's verify that.)
	 */
	eqclauses = generate_join_implied_equalities(root,
												 joinrelids,
												 required_outer,
												 baserel,
												 NULL);
#ifdef USE_ASSERT_CHECKING
	foreach(lc, eqclauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(join_clause_is_movable_into(rinfo,
										   baserel->relids,
										   joinrelids));
	}
#endif
	pclauses = list_concat(pclauses, eqclauses);

	/* Compute set of serial numbers of the enforced clauses */
	pserials = NULL;
	foreach(lc, pclauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pserials = bms_add_member(pserials, rinfo->rinfo_serial);
	}

	/* Estimate the number of rows returned by the parameterized scan */
	rows = get_parameterized_baserel_size(root, baserel, pclauses);

	/* And now we can build the ParamPathInfo */
	ppi = makeNode(ParamPathInfo);
	ppi->ppi_req_outer = required_outer;
	ppi->ppi_rows = rows;
	ppi->ppi_clauses = pclauses;
	ppi->ppi_serials = pserials;
	baserel->ppilist = lappend(baserel->ppilist, ppi);

	return ppi;
}

/*
 * get_joinrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a join relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 *
 * outer_path and inner_path are a pair of input paths that can be used to
 * construct the join, and restrict_clauses is the list of regular join
 * clauses (including clauses derived from EquivalenceClasses) that must be
 * applied at the join node when using these inputs.
 *
 * Unlike the situation for base rels, the set of movable join clauses to be
 * enforced at a join varies with the selected pair of input paths, so we
 * must calculate that and pass it back, even if we already have a matching
 * ParamPathInfo.  We handle this by adding any clauses moved down to this
 * join to *restrict_clauses, which is an in/out parameter.  (The addition
 * is done in such a way as to not modify the passed-in List structure.)
 *
 * Note: when considering a nestloop join, the caller must have removed from
 * restrict_clauses any movable clauses that are themselves scheduled to be
 * pushed into the right-hand path.  We do not do that here since it's
 * unnecessary for other join types.
 */
ParamPathInfo *
get_joinrel_parampathinfo(PlannerInfo *root, RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  SpecialJoinInfo *sjinfo,
						  Relids required_outer,
						  List **restrict_clauses)
{
	ParamPathInfo *ppi;
	Relids		join_and_req;
	Relids		outer_and_req;
	Relids		inner_and_req;
	List	   *pclauses;
	List	   *eclauses;
	List	   *dropped_ecs;
	double		rows;
	ListCell   *lc;

	/* If rel has LATERAL refs, every path for it should account for them */
	Assert(bms_is_subset(joinrel->lateral_relids, required_outer));

	/* Unparameterized paths have no ParamPathInfo or extra join clauses */
	if (bms_is_empty(required_outer))
		return NULL;

	Assert(!bms_overlap(joinrel->relids, required_outer));

	/*
	 * Identify all joinclauses that are movable to this join rel given this
	 * parameterization.  These are the clauses that are movable into this
	 * join, but not movable into either input path.  Treat an unparameterized
	 * input path as not accepting parameterized clauses (because it won't,
	 * per the shortcut exit above), even though the joinclause movement rules
	 * might allow the same clauses to be moved into a parameterized path for
	 * that rel.
	 */
	join_and_req = bms_union(joinrel->relids, required_outer);
	if (outer_path->param_info)
		outer_and_req = bms_union(outer_path->parent->relids,
								  PATH_REQ_OUTER(outer_path));
	else
		outer_and_req = NULL;	/* outer path does not accept parameters */
	if (inner_path->param_info)
		inner_and_req = bms_union(inner_path->parent->relids,
								  PATH_REQ_OUTER(inner_path));
	else
		inner_and_req = NULL;	/* inner path does not accept parameters */

	pclauses = NIL;
	foreach(lc, joinrel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (join_clause_is_movable_into(rinfo,
										joinrel->relids,
										join_and_req) &&
			!join_clause_is_movable_into(rinfo,
										 outer_path->parent->relids,
										 outer_and_req) &&
			!join_clause_is_movable_into(rinfo,
										 inner_path->parent->relids,
										 inner_and_req))
			pclauses = lappend(pclauses, rinfo);
	}

	/* Consider joinclauses generated by EquivalenceClasses, too */
	eclauses = generate_join_implied_equalities(root,
												join_and_req,
												required_outer,
												joinrel,
												NULL);
	/* We only want ones that aren't movable to lower levels */
	dropped_ecs = NIL;
	foreach(lc, eclauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(join_clause_is_movable_into(rinfo,
										   joinrel->relids,
										   join_and_req));
		if (join_clause_is_movable_into(rinfo,
										outer_path->parent->relids,
										outer_and_req))
			continue;			/* drop if movable into LHS */
		if (join_clause_is_movable_into(rinfo,
										inner_path->parent->relids,
										inner_and_req))
		{
			/* drop if movable into RHS, but remember EC for use below */
			Assert(rinfo->left_ec == rinfo->right_ec);
			dropped_ecs = lappend(dropped_ecs, rinfo->left_ec);
			continue;
		}
		pclauses = lappend(pclauses, rinfo);
	}

	/*
	 * EquivalenceClasses are harder to deal with than we could wish, because
	 * of the fact that a given EC can generate different clauses depending on
	 * context.  Suppose we have an EC {X.X, Y.Y, Z.Z} where X and Y are the
	 * LHS and RHS of the current join and Z is in required_outer, and further
	 * suppose that the inner_path is parameterized by both X and Z.  The code
	 * above will have produced either Z.Z = X.X or Z.Z = Y.Y from that EC,
	 * and in the latter case will have discarded it as being movable into the
	 * RHS.  However, the EC machinery might have produced either Y.Y = X.X or
	 * Y.Y = Z.Z as the EC enforcement clause within the inner_path; it will
	 * not have produced both, and we can't readily tell from here which one
	 * it did pick.  If we add no clause to this join, we'll end up with
	 * insufficient enforcement of the EC; either Z.Z or X.X will fail to be
	 * constrained to be equal to the other members of the EC.  (When we come
	 * to join Z to this X/Y path, we will certainly drop whichever EC clause
	 * is generated at that join, so this omission won't get fixed later.)
	 *
	 * To handle this, for each EC we discarded such a clause from, try to
	 * generate a clause connecting the required_outer rels to the join's LHS
	 * ("Z.Z = X.X" in the terms of the above example).  If successful, and if
	 * the clause can't be moved to the LHS, add it to the current join's
	 * restriction clauses.  (If an EC cannot generate such a clause then it
	 * has nothing that needs to be enforced here, while if the clause can be
	 * moved into the LHS then it should have been enforced within that path.)
	 *
	 * Note that we don't need similar processing for ECs whose clause was
	 * considered to be movable into the LHS, because the LHS can't refer to
	 * the RHS so there is no comparable ambiguity about what it might
	 * actually be enforcing internally.
	 */
	if (dropped_ecs)
	{
		Relids		real_outer_and_req;

		real_outer_and_req = bms_union(outer_path->parent->relids,
									   required_outer);
		eclauses =
			generate_join_implied_equalities_for_ecs(root,
													 dropped_ecs,
													 real_outer_and_req,
													 required_outer,
													 outer_path->parent);
		foreach(lc, eclauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			Assert(join_clause_is_movable_into(rinfo,
											   outer_path->parent->relids,
											   real_outer_and_req));
			if (!join_clause_is_movable_into(rinfo,
											 outer_path->parent->relids,
											 outer_and_req))
				pclauses = lappend(pclauses, rinfo);
		}
	}

	/*
	 * Now, attach the identified moved-down clauses to the caller's
	 * restrict_clauses list.  By using list_concat in this order, we leave
	 * the original list structure of restrict_clauses undamaged.
	 */
	*restrict_clauses = list_concat(pclauses, *restrict_clauses);

	/* If we already have a PPI for this parameterization, just return it */
	if ((ppi = find_param_path_info(joinrel, required_outer)))
		return ppi;

	/* Estimate the number of rows returned by the parameterized join */
	rows = get_parameterized_joinrel_size(root, joinrel,
										  outer_path,
										  inner_path,
										  sjinfo,
										  *restrict_clauses);

	/*
	 * And now we can build the ParamPathInfo.  No point in saving the
	 * input-pair-dependent clause list, though.
	 *
	 * Note: in GEQO mode, we'll be called in a temporary memory context, but
	 * the joinrel structure is there too, so no problem.
	 */
	ppi = makeNode(ParamPathInfo);
	ppi->ppi_req_outer = required_outer;
	ppi->ppi_rows = rows;
	ppi->ppi_clauses = NIL;
	ppi->ppi_serials = NULL;
	joinrel->ppilist = lappend(joinrel->ppilist, ppi);

	return ppi;
}

/*
 * get_appendrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for an append relation.
 *
 * For an append relation, the rowcount estimate will just be the sum of
 * the estimates for its children.  However, we still need a ParamPathInfo
 * to flag the fact that the path requires parameters.  So this just creates
 * a suitable struct with zero ppi_rows (and no ppi_clauses either, since
 * the Append node isn't responsible for checking quals).
 */
ParamPathInfo *
get_appendrel_parampathinfo(RelOptInfo *appendrel, Relids required_outer)
{
	ParamPathInfo *ppi;

	/* If rel has LATERAL refs, every path for it should account for them */
	Assert(bms_is_subset(appendrel->lateral_relids, required_outer));

	/* Unparameterized paths have no ParamPathInfo */
	if (bms_is_empty(required_outer))
		return NULL;

	Assert(!bms_overlap(appendrel->relids, required_outer));

	/* If we already have a PPI for this parameterization, just return it */
	if ((ppi = find_param_path_info(appendrel, required_outer)))
		return ppi;

	/* Else build the ParamPathInfo */
	ppi = makeNode(ParamPathInfo);
	ppi->ppi_req_outer = required_outer;
	ppi->ppi_rows = 0;
	ppi->ppi_clauses = NIL;
	ppi->ppi_serials = NULL;
	appendrel->ppilist = lappend(appendrel->ppilist, ppi);

	return ppi;
}

/*
 * Returns a ParamPathInfo for the parameterization given by required_outer, if
 * already available in the given rel. Returns NULL otherwise.
 */
ParamPathInfo *
find_param_path_info(RelOptInfo *rel, Relids required_outer)
{
	ListCell   *lc;

	foreach(lc, rel->ppilist)
	{
		ParamPathInfo *ppi = (ParamPathInfo *) lfirst(lc);

		if (bms_equal(ppi->ppi_req_outer, required_outer))
			return ppi;
	}

	return NULL;
}

/*
 * get_param_path_clause_serials
 *		Given a parameterized Path, return the set of pushed-down clauses
 *		(identified by rinfo_serial numbers) enforced within the Path.
 */
Bitmapset *
get_param_path_clause_serials(Path *path)
{
	if (path->param_info == NULL)
		return NULL;			/* not parameterized */
	if (IsA(path, NestPath) ||
		IsA(path, MergePath) ||
		IsA(path, HashPath))
	{
		/*
		 * For a join path, combine clauses enforced within either input path
		 * with those enforced as joinrestrictinfo in this path.  Note that
		 * joinrestrictinfo may include some non-pushed-down clauses, but for
		 * current purposes it's okay if we include those in the result. (To
		 * be more careful, we could check for clause_relids overlapping the
		 * path parameterization, but it's not worth the cycles for now.)
		 */
		JoinPath   *jpath = (JoinPath *) path;
		Bitmapset  *pserials;
		ListCell   *lc;

		pserials = NULL;
		pserials = bms_add_members(pserials,
								   get_param_path_clause_serials(jpath->outerjoinpath));
		pserials = bms_add_members(pserials,
								   get_param_path_clause_serials(jpath->innerjoinpath));
		foreach(lc, jpath->joinrestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			pserials = bms_add_member(pserials, rinfo->rinfo_serial);
		}
		return pserials;
	}
	else if (IsA(path, AppendPath))
	{
		/*
		 * For an appendrel, take the intersection of the sets of clauses
		 * enforced in each input path.
		 */
		AppendPath *apath = (AppendPath *) path;
		Bitmapset  *pserials;
		ListCell   *lc;

		pserials = NULL;
		foreach(lc, apath->subpaths)
		{
			Path	   *subpath = (Path *) lfirst(lc);
			Bitmapset  *subserials;

			subserials = get_param_path_clause_serials(subpath);
			if (lc == list_head(apath->subpaths))
				pserials = bms_copy(subserials);
			else
				pserials = bms_int_members(pserials, subserials);
		}
		return pserials;
	}
	else if (IsA(path, MergeAppendPath))
	{
		/* Same as AppendPath case */
		MergeAppendPath *apath = (MergeAppendPath *) path;
		Bitmapset  *pserials;
		ListCell   *lc;

		pserials = NULL;
		foreach(lc, apath->subpaths)
		{
			Path	   *subpath = (Path *) lfirst(lc);
			Bitmapset  *subserials;

			subserials = get_param_path_clause_serials(subpath);
			if (lc == list_head(apath->subpaths))
				pserials = bms_copy(subserials);
			else
				pserials = bms_int_members(pserials, subserials);
		}
		return pserials;
	}
	else
	{
		/*
		 * Otherwise, it's a baserel path and we can use the
		 * previously-computed set of serial numbers.
		 */
		return path->param_info->ppi_serials;
	}
}

/*
 * build_joinrel_partition_info
 *		Checks if the two relations being joined can use partitionwise join
 *		and if yes, initialize partitioning information of the resulting
 *		partitioned join relation.
 */
static void
build_joinrel_partition_info(PlannerInfo *root,
							 RelOptInfo *joinrel, RelOptInfo *outer_rel,
							 RelOptInfo *inner_rel, SpecialJoinInfo *sjinfo,
							 List *restrictlist)
{
	PartitionScheme part_scheme;

	/* Nothing to do if partitionwise join technique is disabled. */
	if (!enable_partitionwise_join)
	{
		Assert(!IS_PARTITIONED_REL(joinrel));
		return;
	}

	/*
	 * We can only consider this join as an input to further partitionwise
	 * joins if (a) the input relations are partitioned and have
	 * consider_partitionwise_join=true, (b) the partition schemes match, and
	 * (c) we can identify an equi-join between the partition keys.  Note that
	 * if it were possible for have_partkey_equi_join to return different
	 * answers for the same joinrel depending on which join ordering we try
	 * first, this logic would break.  That shouldn't happen, though, because
	 * of the way the query planner deduces implied equalities and reorders
	 * the joins.  Please see optimizer/README for details.
	 */
	if (outer_rel->part_scheme == NULL || inner_rel->part_scheme == NULL ||
		!outer_rel->consider_partitionwise_join ||
		!inner_rel->consider_partitionwise_join ||
		outer_rel->part_scheme != inner_rel->part_scheme ||
		!have_partkey_equi_join(root, joinrel, outer_rel, inner_rel,
								sjinfo->jointype, restrictlist))
	{
		Assert(!IS_PARTITIONED_REL(joinrel));
		return;
	}

	part_scheme = outer_rel->part_scheme;

	/*
	 * This function will be called only once for each joinrel, hence it
	 * should not have partitioning fields filled yet.
	 */
	Assert(!joinrel->part_scheme && !joinrel->partexprs &&
		   !joinrel->nullable_partexprs && !joinrel->part_rels &&
		   !joinrel->boundinfo);

	/*
	 * If the join relation is partitioned, it uses the same partitioning
	 * scheme as the joining relations.
	 *
	 * Note: we calculate the partition bounds, number of partitions, and
	 * child-join relations of the join relation in try_partitionwise_join().
	 */
	joinrel->part_scheme = part_scheme;
	set_joinrel_partition_key_exprs(joinrel, outer_rel, inner_rel,
									sjinfo->jointype);

	/*
	 * Set the consider_partitionwise_join flag.
	 */
	Assert(outer_rel->consider_partitionwise_join);
	Assert(inner_rel->consider_partitionwise_join);
	joinrel->consider_partitionwise_join = true;
}

/*
 * have_partkey_equi_join
 *
 * Returns true if there exist equi-join conditions involving pairs
 * of matching partition keys of the relations being joined for all
 * partition keys.
 */

bool
have_partkey_equi_join(PlannerInfo *root, RelOptInfo *joinrel,
					   RelOptInfo *rel1, RelOptInfo *rel2,
					   JoinType jointype, List *restrictlist)
{
	PartitionScheme part_scheme = rel1->part_scheme;
	ListCell   *lc;
	int			cnt_pks;
	bool		pk_has_clause[PARTITION_MAX_KEYS];
	bool		strict_op;

	/*
	 * This function must only be called when the joined relations have same
	 * partitioning scheme.
	 */
	Assert(rel1->part_scheme == rel2->part_scheme);
	Assert(part_scheme);

	memset(pk_has_clause, 0, sizeof(pk_has_clause));
	foreach(lc, restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr	   *opexpr;
		Expr	   *expr1;
		Expr	   *expr2;
		int			ipk1;
		int			ipk2;

		/* If processing an outer join, only use its own join clauses. */
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
			continue;

		/* Skip clauses which can not be used for a join. */
		if (!rinfo->can_join)
			continue;

		/* Skip clauses which are not equality conditions. */
		if (!rinfo->mergeopfamilies && !OidIsValid(rinfo->hashjoinoperator))
			continue;

		/* Should be OK to assume it's an OpExpr. */
		opexpr = castNode(OpExpr, rinfo->clause);

		/* Match the operands to the relation. */
		if (bms_is_subset(rinfo->left_relids, rel1->relids) &&
			bms_is_subset(rinfo->right_relids, rel2->relids))
		{
			expr1 = linitial(opexpr->args);
			expr2 = lsecond(opexpr->args);
		}
		else if (bms_is_subset(rinfo->left_relids, rel2->relids) &&
				 bms_is_subset(rinfo->right_relids, rel1->relids))
		{
			expr1 = lsecond(opexpr->args);
			expr2 = linitial(opexpr->args);
		}
		else
			continue;

		/*
		 * Now we need to know whether the join operator is strict; see
		 * comments in pathnodes.h.
		 */
		strict_op = op_strict(opexpr->opno);

		/*
		 * Vars appearing in the relation's partition keys will not have any
		 * varnullingrels, but those in expr1 and expr2 will if we're above
		 * outer joins that could null the respective rels.  It's okay to
		 * match anyway, if the join operator is strict.
		 */
		if (strict_op)
		{
			if (bms_overlap(rel1->relids, root->outer_join_rels))
				expr1 = (Expr *) remove_nulling_relids((Node *) expr1,
													   root->outer_join_rels,
													   NULL);
			if (bms_overlap(rel2->relids, root->outer_join_rels))
				expr2 = (Expr *) remove_nulling_relids((Node *) expr2,
													   root->outer_join_rels,
													   NULL);
		}

		/*
		 * Only clauses referencing the partition keys are useful for
		 * partitionwise join.
		 */
		ipk1 = match_expr_to_partition_keys(expr1, rel1, strict_op);
		if (ipk1 < 0)
			continue;
		ipk2 = match_expr_to_partition_keys(expr2, rel2, strict_op);
		if (ipk2 < 0)
			continue;

		/*
		 * If the clause refers to keys at different ordinal positions, it can
		 * not be used for partitionwise join.
		 */
		if (ipk1 != ipk2)
			continue;

		/* Reject if the partition key collation differs from the clause's. */
		if (rel1->part_scheme->partcollation[ipk1] != opexpr->inputcollid)
			return false;

		/*
		 * The clause allows partitionwise join only if it uses the same
		 * operator family as that specified by the partition key.
		 */
		if (rel1->part_scheme->strategy == PARTITION_STRATEGY_HASH)
		{
			if (!OidIsValid(rinfo->hashjoinoperator) ||
				!op_in_opfamily(rinfo->hashjoinoperator,
								part_scheme->partopfamily[ipk1]))
				continue;
		}
		else if (!list_member_oid(rinfo->mergeopfamilies,
								  part_scheme->partopfamily[ipk1]))
			continue;

		/* Mark the partition key as having an equi-join clause. */
		pk_has_clause[ipk1] = true;
	}

	/* Check whether every partition key has an equi-join condition. */
	for (cnt_pks = 0; cnt_pks < part_scheme->partnatts; cnt_pks++)
	{
		if (!pk_has_clause[cnt_pks])
			return false;
	}

	return true;
}

/*
 * match_expr_to_partition_keys
 *
 * Tries to match an expression to one of the nullable or non-nullable
 * partition keys of "rel".  Returns the matched key's ordinal position,
 * or -1 if the expression could not be matched to any of the keys.
 *
 * strict_op must be true if the expression will be compared with the
 * partition key using a strict operator.  This allows us to consider
 * nullable as well as nonnullable partition keys.
 */
static int
match_expr_to_partition_keys(Expr *expr, RelOptInfo *rel, bool strict_op)
{
	int			cnt;

	/* This function should be called only for partitioned relations. */
	Assert(rel->part_scheme);
	Assert(rel->partexprs);
	Assert(rel->nullable_partexprs);

	/* Remove any relabel decorations. */
	while (IsA(expr, RelabelType))
		expr = (Expr *) (castNode(RelabelType, expr))->arg;

	for (cnt = 0; cnt < rel->part_scheme->partnatts; cnt++)
	{
		ListCell   *lc;

		/* We can always match to the non-nullable partition keys. */
		foreach(lc, rel->partexprs[cnt])
		{
			if (equal(lfirst(lc), expr))
				return cnt;
		}

		if (!strict_op)
			continue;

		/*
		 * If it's a strict join operator then a NULL partition key on one
		 * side will not join to any partition key on the other side, and in
		 * particular such a row can't join to a row from a different
		 * partition on the other side.  So, it's okay to search the nullable
		 * partition keys as well.
		 */
		foreach(lc, rel->nullable_partexprs[cnt])
		{
			if (equal(lfirst(lc), expr))
				return cnt;
		}
	}

	return -1;
}

/*
 * set_joinrel_partition_key_exprs
 *		Initialize partition key expressions for a partitioned joinrel.
 */
static void
set_joinrel_partition_key_exprs(RelOptInfo *joinrel,
								RelOptInfo *outer_rel, RelOptInfo *inner_rel,
								JoinType jointype)
{
	PartitionScheme part_scheme = joinrel->part_scheme;
	int			partnatts = part_scheme->partnatts;

	joinrel->partexprs = (List **) palloc0(sizeof(List *) * partnatts);
	joinrel->nullable_partexprs =
		(List **) palloc0(sizeof(List *) * partnatts);

	/*
	 * The joinrel's partition expressions are the same as those of the input
	 * rels, but we must properly classify them as nullable or not in the
	 * joinrel's output.  (Also, we add some more partition expressions if
	 * it's a FULL JOIN.)
	 */
	for (int cnt = 0; cnt < partnatts; cnt++)
	{
		/* mark these const to enforce that we copy them properly */
		const List *outer_expr = outer_rel->partexprs[cnt];
		const List *outer_null_expr = outer_rel->nullable_partexprs[cnt];
		const List *inner_expr = inner_rel->partexprs[cnt];
		const List *inner_null_expr = inner_rel->nullable_partexprs[cnt];
		List	   *partexpr = NIL;
		List	   *nullable_partexpr = NIL;
		ListCell   *lc;

		switch (jointype)
		{
				/*
				 * A join relation resulting from an INNER join may be
				 * regarded as partitioned by either of the inner and outer
				 * relation keys.  For example, A INNER JOIN B ON A.a = B.b
				 * can be regarded as partitioned on either A.a or B.b.  So we
				 * add both keys to the joinrel's partexpr lists.  However,
				 * anything that was already nullable still has to be treated
				 * as nullable.
				 */
			case JOIN_INNER:
				partexpr = list_concat_copy(outer_expr, inner_expr);
				nullable_partexpr = list_concat_copy(outer_null_expr,
													 inner_null_expr);
				break;

				/*
				 * A join relation resulting from a SEMI or ANTI join may be
				 * regarded as partitioned by the outer relation keys.  The
				 * inner relation's keys are no longer interesting; since they
				 * aren't visible in the join output, nothing could join to
				 * them.
				 */
			case JOIN_SEMI:
			case JOIN_ANTI:
				partexpr = list_copy(outer_expr);
				nullable_partexpr = list_copy(outer_null_expr);
				break;

				/*
				 * A join relation resulting from a LEFT OUTER JOIN likewise
				 * may be regarded as partitioned on the (non-nullable) outer
				 * relation keys.  The inner (nullable) relation keys are okay
				 * as partition keys for further joins as long as they involve
				 * strict join operators.
				 */
			case JOIN_LEFT:
				partexpr = list_copy(outer_expr);
				nullable_partexpr = list_concat_copy(inner_expr,
													 outer_null_expr);
				nullable_partexpr = list_concat(nullable_partexpr,
												inner_null_expr);
				break;

				/*
				 * For FULL OUTER JOINs, both relations are nullable, so the
				 * resulting join relation may be regarded as partitioned on
				 * either of inner and outer relation keys, but only for joins
				 * that involve strict join operators.
				 */
			case JOIN_FULL:
				nullable_partexpr = list_concat_copy(outer_expr,
													 inner_expr);
				nullable_partexpr = list_concat(nullable_partexpr,
												outer_null_expr);
				nullable_partexpr = list_concat(nullable_partexpr,
												inner_null_expr);

				/*
				 * Also add CoalesceExprs corresponding to each possible
				 * full-join output variable (that is, left side coalesced to
				 * right side), so that we can match equijoin expressions
				 * using those variables.  We really only need these for
				 * columns merged by JOIN USING, and only with the pairs of
				 * input items that correspond to the data structures that
				 * parse analysis would build for such variables.  But it's
				 * hard to tell which those are, so just make all the pairs.
				 * Extra items in the nullable_partexprs list won't cause big
				 * problems.  (It's possible that such items will get matched
				 * to user-written COALESCEs, but it should still be valid to
				 * partition on those, since they're going to be either the
				 * partition column or NULL; it's the same argument as for
				 * partitionwise nesting of any outer join.)  We assume no
				 * type coercions are needed to make the coalesce expressions,
				 * since columns of different types won't have gotten
				 * classified as the same PartitionScheme.  Note that we
				 * intentionally leave out the varnullingrels decoration that
				 * would ordinarily appear on the Vars inside these
				 * CoalesceExprs, because have_partkey_equi_join will strip
				 * varnullingrels from the expressions it will compare to the
				 * partexprs.
				 */
				foreach(lc, list_concat_copy(outer_expr, outer_null_expr))
				{
					Node	   *larg = (Node *) lfirst(lc);
					ListCell   *lc2;

					foreach(lc2, list_concat_copy(inner_expr, inner_null_expr))
					{
						Node	   *rarg = (Node *) lfirst(lc2);
						CoalesceExpr *c = makeNode(CoalesceExpr);

						c->coalescetype = exprType(larg);
						c->coalescecollid = exprCollation(larg);
						c->args = list_make2(larg, rarg);
						c->location = -1;
						nullable_partexpr = lappend(nullable_partexpr, c);
					}
				}
				break;

			default:
				elog(ERROR, "unrecognized join type: %d", (int) jointype);
		}

		joinrel->partexprs[cnt] = partexpr;
		joinrel->nullable_partexprs[cnt] = nullable_partexpr;
	}
}

/*
 * build_child_join_reltarget
 *	  Set up a child-join relation's reltarget from a parent-join relation.
 */
static void
build_child_join_reltarget(PlannerInfo *root,
						   RelOptInfo *parentrel,
						   RelOptInfo *childrel,
						   int nappinfos,
						   AppendRelInfo **appinfos)
{
	/* Build the targetlist */
	childrel->reltarget->exprs = (List *)
		adjust_appendrel_attrs(root,
							   (Node *) parentrel->reltarget->exprs,
							   nappinfos, appinfos);

	/* Set the cost and width fields */
	childrel->reltarget->cost.startup = parentrel->reltarget->cost.startup;
	childrel->reltarget->cost.per_tuple = parentrel->reltarget->cost.per_tuple;
	childrel->reltarget->width = parentrel->reltarget->width;
}

/*
 * Check if the relation can produce grouped paths and return the information
 * it'll need for it. The passed relation is the non-grouped one which has the
 * reltarget already constructed.
 */
RelAggInfo *
create_rel_agg_info(PlannerInfo *root, RelOptInfo *rel)
{
	List       *gvis;
	List	   *aggregates = NIL;
	bool		unsolved_agg_found;
	ListCell   *lc;
	RelAggInfo *result;
	PathTarget *agg_input;
	PathTarget *target = NULL;
	List	   *grp_exprs_extra = NIL;
	List	   *group_clauses_final;
	int			i;

	/* Shouldn't been called if there's no opportunity for push-down */
	Assert(root->grouped_var_list != NIL);

	/*
	 * The current implementation of aggregation push-down cannot handle
	 * PlaceHolderVar(PHV).
	 */
	foreach(lc, rel->reltarget->exprs)
	{
		Expr	   *expr = lfirst(lc);

		if (IsA(expr, PlaceHolderVar))
			return NULL;
	}

	/* Caller should only pass base relations or joins. */
	Assert(rel->reloptkind == RELOPT_BASEREL ||
		   rel->reloptkind == RELOPT_JOINREL);

	/*
	 * Use equivalence classes to generate additional grouping expressions for
	 * the current rel. Without these we might not be able to apply
	 * aggregation to the relation result set.
	 *
	 * It's important that create_grouping_expr_grouped_var_infos has
	 * processed the explicit grouping columns by now. If the grouping clause
	 * contains multiple expressions belonging to the same EC, the original
	 * (i.e. not derived) one should be preferred when we build grouping
	 * target for a relation. Otherwise we have a problem when trying to match
	 * target entries to grouping clauses during plan creation, see
	 * get_grouping_expression().
	 *
	 * NB: the correctness of translating Aggerf through EC can't be
	 * guaranteed, so we only translate grouping expressions.
	 */
	gvis = list_copy(root->grouped_var_list);
	foreach(lc, root->grouped_var_list)
	{
		GroupedVarInfo *gvi = lfirst_node(GroupedVarInfo, lc);
		int			relid = -1;

		/* Only interested in grouping expressions. */
		if (IsA(gvi->gvexpr, Aggref))
			continue;

		while ((relid = bms_next_member(rel->relids, relid)) >= 0)
		{
			GroupedVarInfo *gvi_trans;

			gvi_trans = translate_expr_to_rel_vars(root, gvi, relid);
			if (gvi_trans != NULL)
				gvis = lappend(gvis, gvi_trans);
		}
	}

	/*
	 * Check if some aggregates or grouping expressions can be evaluated in
	 * this relation's target, and collect all vars referenced by these
	 * aggregates / grouping expressions;
	 */
	unsolved_agg_found = false;
	foreach(lc, gvis)
	{
		GroupedVarInfo *gvi = lfirst_node(GroupedVarInfo, lc);

		/* Only interested in aggregates. */
		if (!IsA(gvi->gvexpr, Aggref))
			continue;

		/*
		 * The subset includes gv_eval_at uninitialized, which includes
		 * Aggref.aggstar.
		 */
		if (bms_is_subset(gvi->gv_eval_at, rel->relids))
		{
			gvi->agg_partial = (Aggref *) copyObject(gvi->gvexpr);
			mark_partial_aggref(gvi->agg_partial, AGGSPLIT_INITIAL_SERIAL);
			/* Accept the aggregate. */
			aggregates = lappend(aggregates, gvi);
		}
		else
		{
			/* 
			 * Give up if there is at least one aggregate expression that
			 * needs something else than this rel which is not supported.
			 */
			unsolved_agg_found = true;
			break;
		}
	}

	/*
	 * Give up if no avaliable aggregates or some other aggregate(s) need
	 * relations other than the current one (which is not supported yet).
	 */
	if (aggregates == NIL || unsolved_agg_found)
	{
		list_free(gvis);
		return NULL;
	}

	/*
	 * Create target for grouped paths as well as one for the input paths of
	 * the aggregation paths.
	 */
	target = create_empty_pathtarget();
	agg_input = create_empty_pathtarget();

	/* Cannot suitable targets for the aggregation push-down be derived? */
	if (!init_grouping_targets(root, rel, target, agg_input, gvis,
							   &grp_exprs_extra))
	{
		list_free(gvis);
		return NULL;
	}

	list_free(gvis);

	/* Aggregation push-down makes no sense without grouping expressions. */
	if ((list_length(target->exprs) + list_length(grp_exprs_extra)) == 0)
		return NULL;

	group_clauses_final = root->processed_groupClause;

	/*
	 * If the aggregation target should have extra grouping expressions (in
	 * order to emit input vars for join conditions), add them now. This step
	 * includes assignment of tleSortGroupRef's which we can generate now.
	 */
	if (list_length(grp_exprs_extra) > 0)
	{
		Index		sortgroupref;

		/*
		 * We'll have to add some clauses, but query group clause must be
		 * preserved.
		 */
		group_clauses_final = list_copy(root->processed_groupClause);

		/*
		 * Always start at root->max_sortgroupref. The extra grouping
		 * expressions aren't used during the final aggregation, so the
		 * sortgroupref values don't need to be unique across the query. Thus
		 * we don't have to increase root->max_sortgroupref, which makes
		 * recognition of the extra grouping expressions pretty easy.
		 */
		sortgroupref = root->max_sortgroupref;

		/*
		 * Generate the SortGroupClause's and add the expressions to the
		 * target.
		 */
		foreach(lc, grp_exprs_extra)
		{
			Var		   *var = lfirst_node(Var, lc);
			SortGroupClause *cl = makeNode(SortGroupClause);

			/*
			 * Initialize the SortGroupClause.
			 *
			 * As the final aggregation will not use this grouping expression,
			 * we don't care whether sortop is < or >. The value of
			 * nulls_first should not matter for the same reason.
			 */
			cl->tleSortGroupRef = ++sortgroupref;
			get_sort_group_operators(var->vartype,
									 false, true, false,
									 &cl->sortop, &cl->eqop, NULL,
									 &cl->hashable);
			group_clauses_final = lappend(group_clauses_final, cl);
			add_column_to_pathtarget(target, (Expr *) var,
									 cl->tleSortGroupRef);

			/* The aggregation input target must emit this var too. */
			add_column_to_pathtarget(agg_input, (Expr *) var,
									 cl->tleSortGroupRef);
		}
	}

	/* Add aggregates to the grouping target. */
	foreach(lc, aggregates)
	{
		GroupedVarInfo *gvi;

		gvi = lfirst_node(GroupedVarInfo, lc);
		add_column_to_pathtarget(target, (Expr *) gvi->agg_partial,
								 gvi->sortgroupref);
	}

	/*
	 * Build a list of grouping expressions and a list of the corresponding
	 * SortGroupClauses.
	 */
	i = 0;
	result = makeNode(RelAggInfo);
	result->rel_grouped = NULL;
	result->rel_grouped_non_plain = NULL;
	foreach(lc, target->exprs)
	{
		Index		sortgroupref = 0;
		SortGroupClause *cl;
		Expr	   *texpr;

		texpr = (Expr *) lfirst(lc);

		if (IsA(texpr, Aggref))
		{
			/* Once we see Aggref, no grouping expressions should follow. */
			break;
		}

		/* Find the clause by sortgroupref. */
		sortgroupref = target->sortgrouprefs[i++];

		/*
		 * Besides being an aggregate, the target expression should have no
		 * other reason then being a column of a relation functionally
		 * dependent on the GROUP BY clause. So it's not actually a grouping
		 * column.
		 */
		if (sortgroupref == 0)
			continue;

		/*
		 * group_clause_final contains the "local" clauses, so this search
		 * should succeed.
		 */
		cl = get_sortgroupref_clause(sortgroupref, group_clauses_final);

		result->group_clauses = list_append_unique(result->group_clauses, cl);

		/*
		 * Add only unique clauses because of joins (both sides of a join can
		 * point at the same grouping clause). XXX Is it worth adding a bool
		 * argument indicating that we're dealing with join right now?
		 */
		result->group_exprs = list_append_unique(result->group_exprs, texpr);
	}

	/*
	 * Since neither target nor agg_input is supposed to be identical to the
	 * source reltarget, compute the width and cost again.
	 *
	 * target does not yet contain aggregates, but these will be accounted by
	 * AggPath.
	 */
	set_pathtarget_cost_width(root, target);
	set_pathtarget_cost_width(root, agg_input);

	result->relids = bms_copy(rel->relids);
	result->target = target;
	result->agg_input = agg_input;

	/* Finally collect the aggregates. */
	while (lc != NULL)
	{
		Aggref	   *aggref = lfirst_node(Aggref, lc);

		/* Partial aggregation is what the grouped paths should do. */
		result->agg_exprs = lappend(result->agg_exprs, aggref);
		lc = lnext(target->exprs, lc);
	}

	/* The "input_rows" field should be set by caller. */
	return result;
}

/*
 * Initialize target for grouped paths (target) as well as a target for paths
 * that generate input for aggregation (agg_input).
 *
 * group_exprs_extra_p receives a list of Var nodes for which we need to
 * construct SortGroupClause. Those vars will then be used as additional
 * grouping expressions, for the sake of join clauses.
 *
 * gvis a list of GroupedVarInfo's possibly useful for rel.
 *
 * Return true iff the targets could be initialized.
 */
static bool
init_grouping_targets(PlannerInfo *root, RelOptInfo *rel,
					  PathTarget *target, PathTarget *agg_input,
					  List *gvis, List **group_exprs_extra_p)
{
	ListCell   *lc;
	List	   *possibly_dependent = NIL;
	Var		   *tvar;

	foreach(lc, rel->reltarget->exprs)
	{
		Index		sortgroupref;

		/*
		 * We don't support PlaceHolderVar, the source target of the plain
		 * relation must be a simple Var.
		 */
		tvar = lfirst_node(Var, lc);

		sortgroupref = get_expression_sortgroupref((Expr *) tvar, gvis);
		if (sortgroupref > 0)
		{
			/*
			 * If the target expression can be used as the grouping key, we
			 * don't have to worry whether it can be emitted by the AggPath
			 * pushed down to relation / join.
			 */
			add_column_to_pathtarget(target, (Expr *) tvar, sortgroupref);

			/*
			 * As for agg_input, add the original expression but set
			 * sortgroupref in addition.
			 */
			add_column_to_pathtarget(agg_input, (Expr *) tvar, sortgroupref);
		}
		else
		{
			if (is_var_needed_by_join(root, tvar, rel))
			{
				/*
				 * The variable is needed for a join, however it's neither in
				 * the GROUP BY clause nor can it be derived from it using EC.
				 * (Otherwise it would have to be added to the targets above.)
				 * We need to construct special SortGroupClause for that
				 * variable.
				 *
				 * Note that its tleSortGroupRef needs to be unique within
				 * agg_input, so we need to postpone creation of the
				 * SortGroupClause's until we're done with the iteration of
				 * rel->reltarget->exprs. Also it makes sense for the caller
				 * to do some more check before it starts to create those
				 * SortGroupClause's.
				 */
				*group_exprs_extra_p = lappend(*group_exprs_extra_p, tvar);
			}
			else if (is_var_in_aggref_only(root, tvar))
			{
				/*
				 * Another reason we might need this variable is that some
				 * aggregate pushed down to this relation references it. In such a
				 * case, add that var to agg_input, but not to "target". However,
				 * if the aggregate is not the only reason for the var to be in
				 * the target, some more checks need to be performed below.
				 */
				add_new_column_to_pathtarget(agg_input, (Expr *) tvar);
			}
			else
			{
				/*
				 * The Var can be functionally dependent on another expression
				 * of the target, but we cannot check until the other
				 * expressions are in the target. For example:
				 *
				 * SELCT name, SUM(val) FROM tbl GROUP BY id;
				 *
				 * In the case where id is the primary key, even if name is not
				 * in any SortGroupRef and will not be used by any joins or
				 * aggregations, it still needs to be added to the input and
				 * target.
				 */
				possibly_dependent = lappend(possibly_dependent, tvar);
			}
		}
	}

	/*
	 * Now we can check whether the expression is functionally dependent on
	 * another one.
	 */
	foreach(lc, possibly_dependent)
	{
		List	   *deps = NIL;
		RangeTblEntry *rte;

		tvar = lfirst_node(Var, lc);
		rte = root->simple_rte_array[tvar->varno];

		/*
		 * Check if the Var can be in the grouping key even though it's not
		 * mentioned by the GROUP BY clause (and could not be derived using
		 * ECs).
		 */
		if (check_functional_grouping(rte->relid, tvar->varno,
									  tvar->varlevelsup,
									  target->exprs, &deps))
		{
			/*
			 * The var shouldn't be actually used for grouping key evaluation
			 * (instead, the one this depends on will be), so sortgroupref
			 * should not be important.
			 */
			add_new_column_to_pathtarget(target, (Expr *) tvar);
			add_new_column_to_pathtarget(agg_input, (Expr *) tvar);
		}
		else
		{
			/*
			 * As long as the query is semantically correct, arriving here
			 * means that the var is referenced by a generic grouping
			 * expression but not referenced by any join.
			 *
			 * If the aggregate push-down will support generic grouping
			 * expression in the future, create_rel_agg_info() will have to
			 * add this variable to "agg_input" target and also add the whole
			 * generic expression to "target".
			 */
			return false;
		}
	}

	return true;
}

/*
 * Return sortgroupref if expr can be used as the grouping expression in an
 * AggPath at relation or join level, or 0 if it can't.
 *
 * gvis a list of a list of GroupedVarInfo's available for the query,
 * including those derived using equivalence classes.
 */
static Index
get_expression_sortgroupref(Expr *expr, List *gvis)
{
	ListCell   *lc;

	foreach(lc, gvis)
	{
		GroupedVarInfo *gvi = lfirst_node(GroupedVarInfo, lc);

		if (equal(gvi->gvexpr, expr))
		{
			Assert(gvi->sortgroupref > 0);

			return gvi->sortgroupref;
		}
	}

	/* The expression cannot be used as grouping key. */
	return 0;
}

/*
 * Check whether given variable appears in Aggref(s) which we consider usable
 * at relation / join level, and only in the Aggref(s).
 */
static bool
is_var_in_aggref_only(PlannerInfo *root, Var *var)
{
	ListCell   *lc;
	bool		found = false;

	foreach(lc, root->grouped_var_list)
	{
		GroupedVarInfo *gvi = lfirst_node(GroupedVarInfo, lc);
		ListCell   *lc2;
		List	   *vars;

		if (!IsA(gvi->gvexpr, Aggref))
			continue;

		if (!bms_is_member(var->varno, gvi->gv_eval_at))
			continue;
		
		/* FIXME: consider some kind of caching? */
		vars = pull_var_clause((Node *) gvi->gvexpr, PVC_RECURSE_AGGREGATES);
		foreach(lc2, vars)
		{
			Var		   *v = lfirst_node(Var, lc2);

			if (equal(v, var))
			{
				found = true;
				break;
			}

		}
		list_free(vars);

		if (found)
			break;
	}

	/* No aggregate references the Var? */
	if (!found)
		return false;

	/* Does the Var appear in the target outside aggregates? */
	foreach(lc, root->processed_tlist)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (IsA(te->expr, Aggref))
			continue;

		if (equal(te->expr, var))
			return false;
	}

	/* The Var is in aggregate(s) and only there. */
	return true;
}

/*
 * Check if given variable is needed by joins above the current rel?
 *
 * Consider pushing the aggregate avg(b.y) down to relation "b" for the
 * following query:
 *
 *    SELECT a.i, avg(b.y)
 *    FROM a JOIN b ON b.j = a.i
 *    GROUP BY a.i;
 *
 * If we aggregate the "b" relation alone, the column "b.j" needs to be used
 * as the grouping key because otherwise it cannot find its way to the input
 * of the join expression.
 */
static bool
is_var_needed_by_join(PlannerInfo *root, Var *var, RelOptInfo *rel)
{
	Relids		relids_no_top;
	int			idx;
	RelOptInfo *baserel;

	/*
	 * The relids we're not interested in do include 0, which is the top-level
	 * targetlist. The only reason for relids to contain 0 should be that
	 * arg_var is referenced either by aggregate or by grouping expression,
	 * but right now we're interested in the *other* reasons. (As soon
	 * aggregation is pushed down, the aggregates in the query targetlist no
	 * longer need direct reference to arg_var anyway.)
	 */

	relids_no_top = bms_copy(rel->relids);
	bms_add_member(relids_no_top, 0);

	baserel = find_base_rel(root, var->varno);
	idx = var->varattno - baserel->min_attr;

	/* Still needed by other relations? */
	return bms_nonempty_difference(baserel->attr_needed[idx], relids_no_top);
}