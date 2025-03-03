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
 * aqumv.c
 *	  Answer Query Using Materialzed Views.
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/aqumv.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_rewrite.h"
#include "cdb/cdbllize.h"
#include "commands/matview.h"
#include "optimizer/aqumv.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/transform.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_node.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"

typedef struct
{
	int 	varno;
} aqumv_adjust_varno_context;

extern void aqumv_adjust_simple_query(Query *viewQuery);
static bool aqumv_process_from_quals(Node *query_quals, Node *mv_quals, List** post_quals);
static void aqumv_adjust_varno(Query *parse, int delta);
static Node *aqumv_adjust_varno_mutator(Node *node, aqumv_adjust_varno_context *context);

typedef struct
{
	List	*mv_query_tlist;			/* view query's target list. */
	List	*mv_tlist_ordered_index; 	/* Index list by complexity order. */
	TupleDesc mv_tupledesc;				/* mv relation's tuple desc. */
	bool has_unmatched;		/* True if we fail to rewrite an expression. */
} aqumv_equivalent_transformation_context;

static aqumv_equivalent_transformation_context* aqumv_init_context(List *view_tlist, TupleDesc mv_tupledesc);
static bool aqumv_process_targetlist(aqumv_equivalent_transformation_context *context, List *query_tlist, List **mv_final_tlist);
static void aqumv_sort_targetlist(aqumv_equivalent_transformation_context* context);
static Node *aqumv_adjust_sub_matched_expr_mutator(Node *node, aqumv_equivalent_transformation_context *context);
static bool contain_var_or_aggstar_clause_walker(Node *node, void *context);
static bool check_partition(Query *parse, Oid origin_rel_oid);

typedef struct
{
	int	complexity;
} node_complexity_context;

typedef struct
{
	int	tlist_index;	/* Index of tlist, begin from 1 */
	int	count;			/* Count of subnodes in this expression */
} expr_to_sort;

static bool
contain_var_or_aggstar_clause(Node *node)
{
	return contain_var_or_aggstar_clause_walker(node, NULL);
}

/* Copy from contain_var_clause_walker, but return true with aggstar. */
static bool
contain_var_or_aggstar_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref) && ((Aggref *) node)->aggstar)
		return true;

	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		return false;
	}
	if (IsA(node, CurrentOfExpr))
		return true;
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	return expression_tree_walker(node, contain_var_or_aggstar_clause_walker, context);
}

/*
 * Answer Query Using Materialized Views(AQUMV).
 * This function modifies root(parse and etc.), current_rel in-place.
 */
RelOptInfo*
answer_query_using_materialized_views(PlannerInfo *root, AqumvContext aqumv_context)
{
	RelOptInfo *current_rel = aqumv_context->current_rel;
	query_pathkeys_callback qp_callback = aqumv_context->qp_callback;
	void *qp_extra = aqumv_context->qp_extra;
	Node *raw_havingQual = aqumv_context->raw_havingQual;

	Query   		*parse = root->parse; /* Query of origin SQL. */
	Query			*viewQuery; /* Query of view. */
	RelOptInfo 		*mv_final_rel = current_rel; /* Final rel after rewritten. */
	Node    		*jtnode;
	Node			*mvjtnode;
	int				varno;
	RangeTblEntry 	*rte;
	Oid				origin_rel_oid;
	RangeTblEntry 	*mvrte;
	Relation		ruleDesc;
	Relation		matviewRel;
	SysScanDesc		rcscan;
	HeapTuple		tup;
	Form_pg_rewrite	rewrite_tup;
	bool			need_close = false;
	PlannerInfo		*subroot;
	List			*mv_final_tlist = NIL; /* Final target list we want to rewrite to. */
	List 			*post_quals = NIL;
	aqumv_equivalent_transformation_context	*context;
	bool			can_be_partition;
	char			relkind;

	/* Group By without agg could be possible though IMMV doesn't support it yet. */
	bool can_not_use_mv = (parse->commandType != CMD_SELECT) ||
						  (parse->rowMarks != NIL) ||
						  (parse->scatterClause != NIL) ||
						  (parse->cteList != NIL) ||
						  (parse->setOperations != NULL) ||
						  parse->hasWindowFuncs ||
						  parse->hasModifyingCTE ||
						  (parse->parentStmtType == PARENTSTMTTYPE_REFRESH_MATVIEW) ||
						  (parse->parentStmtType == PARENTSTMTTYPE_CTAS) ||
						  (contain_mutable_functions((Node *) parse)) ||
						  parse->hasSubLinks;

	if (can_not_use_mv)
		return mv_final_rel;

	if (list_length(parse->jointree->fromlist) != 1)
		return mv_final_rel;

	jtnode = (Node *) linitial(parse->jointree->fromlist);
	if (!IsA(jtnode, RangeTblRef))
		return mv_final_rel;

	varno = ((RangeTblRef *) jtnode)->rtindex;
	rte = planner_rt_fetch(varno, root);
	if ((rte->rtekind != RTE_RELATION))
		return mv_final_rel;

	/* root's stuff like simple_rte_array may be changed during rewrite, fetch oid here. */
	origin_rel_oid = rte->relid;
	/* excluding catalog tables. */
	if (IsSystemClassByRelid(origin_rel_oid))
		return mv_final_rel;

	relkind = get_rel_relkind(rte->relid);
	if (relkind != RELKIND_RELATION &&
		relkind != RELKIND_PARTITIONED_TABLE &&
		relkind != RELKIND_FOREIGN_TABLE)
		return mv_final_rel;

	/* We don't know what it is. */
	if ((relkind != RELKIND_PARTITIONED_TABLE) &&
		(list_length(parse->rtable) > 1))
		return mv_final_rel;

	/*
	 * excluding inherit tables.
	 */
	can_be_partition = (relkind == RELKIND_PARTITIONED_TABLE) || get_rel_relispartition(rte->relid);
	if (!can_be_partition &&
		(has_superclass(origin_rel_oid) ||
		has_subclass(origin_rel_oid)))
		return mv_final_rel;

	if (relkind == RELKIND_FOREIGN_TABLE && !aqumv_allow_foreign_table)
		return mv_final_rel;

	ruleDesc = table_open(RewriteRelationId, AccessShareLock);

	rcscan = systable_beginscan(ruleDesc, InvalidOid, false,
								NULL, 0, NULL);

	while (HeapTupleIsValid(tup = systable_getnext(rcscan)))
	{
		CHECK_FOR_INTERRUPTS();
		if (need_close)
			table_close(matviewRel, AccessShareLock);

		rewrite_tup = (Form_pg_rewrite) GETSTRUCT(tup);

		matviewRel = table_open(rewrite_tup->ev_class, AccessShareLock);
		need_close = true;

		if (!RelationIsPopulated(matviewRel))
			continue;

		/*
		 * AQUMV
		 * Currently the data of IVM is always up-to-date if there were.
		 * However, we place this future-proof condition to take
		 * care of IVM deferred maintenance/incremental refresh feature (in SERVERLESS mode).
		 * 
		 * Normal materialized views could also be used if its data is up to date.
		 */
		if (!RelationIsIVM(matviewRel) &&
			!MatviewIsGeneralyUpToDate(RelationGetRelid(matviewRel)))
			continue;

		/*
		 * Get a copy of view query to rewrite.
		 */
		viewQuery = copyObject(get_matview_query(matviewRel));
		Assert(IsA(viewQuery, Query));

		/*
		 * AQUMV
		 * We will do some Equivalet Transformation on the viewQuery which
		 * represents the mv's corresponding query.
		 *
		 * AQUMV_FIXME_MVP: viewQuery is a simple query too, like the parse query.
		 * viewQuery->sortClause is ok here, though we can't use the Order by
		 * clause of viewQuery.
		 * The reason is: the Order by clause of materialized view's query is
		 * typically pointless. We can't rely on the order even we wrote the
		 * ordered data into mv, ex: some other access methods except heap.
		 * The Seqscan on a heap-storaged mv seems ordered, but it's a free lunch.
		 * A Parallel Seqscan breaks that hypothesis.
		 */
		if(viewQuery->hasWindowFuncs ||
			viewQuery->hasDistinctOn ||
			viewQuery->hasModifyingCTE ||
			viewQuery->hasSubLinks ||
			(limit_needed(viewQuery)) ||
			/* IVM doesn't support belows now, just in case. */
			(viewQuery->rowMarks != NIL) ||
			(viewQuery->distinctClause != NIL) ||
			(viewQuery->cteList != NIL) ||
			(viewQuery->setOperations != NULL) ||
			(!viewQuery->hasAggs && (viewQuery->groupClause != NIL)) ||
			((viewQuery->havingQual != NULL) && (viewQuery->groupClause == NIL)) ||
			(viewQuery->scatterClause != NIL))
			continue;

		if (list_length(viewQuery->jointree->fromlist) != 1)
			continue;

		mvjtnode = (Node *) linitial(viewQuery->jointree->fromlist);
		if (!IsA(mvjtnode, RangeTblRef))
			continue;

		/*
		 * AQUMV
		 * Require that the relation of viewQuery is a simple query too.
		 * We haven't do sth like: pull up sublinks or subqueries yet.
		 */
		varno = ((RangeTblRef*) mvjtnode)->rtindex;
		mvrte = rt_fetch(varno, viewQuery->rtable);
		Assert(mvrte != NULL);

		if (mvrte->rtekind != RTE_RELATION)
			continue;

		/*
		 * AQUMV_FIXME_MVP
		 * Must be same relation, recursiviely embeded mv is not supported now.
		 */
		if (mvrte->relid != origin_rel_oid)
			continue;

		/*
		 * Check if it actually has children here to match before planning.
		 * Except for Partitioned tables.
		 */
		if (get_rel_relkind(mvrte->relid) == RELKIND_PARTITIONED_TABLE)
			mvrte->inh = false;
		else
			mvrte->inh = has_subclass(mvrte->relid);

		if (mvrte->inh)
			continue;

		if (!check_partition(parse, origin_rel_oid))
			continue;

		subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
		memcpy(subroot, root, sizeof(PlannerInfo));
		subroot->parent_root = root;
		/*
		 * AQUMV_FIXME_MVP:
		 * TODO: keep ECs and adjust varno?
		 */
		subroot->eq_classes = NIL;
		/* Reset subplan-related stuff */
		subroot->plan_params = NIL;
		subroot->outer_params = NULL;
		subroot->init_plans = NIL;
		/* Agg infos would be processed by subroot itself. */
		subroot->agginfos = NIL;
		subroot->aggtransinfos = NIL;
		subroot->parse = viewQuery;

		/*
		 * AQUMV_FIXME:
		 * We copy from root currently, but it's not true
		 * if we support LIMIT node on view query.
		 */
		subroot->tuple_fraction = root->tuple_fraction;
		subroot->limit_tuples = root->limit_tuples;

		/* Adjust to valid query tree and fix varno after rewrite.*/
		aqumv_adjust_simple_query(viewQuery);

		/*
		 * AQUMV_FIXME_MVP
		 * Are stable functions OK?
		 * A STABLE function cannot modify the database and is guaranteed to
		 * return the same results given the same arguments for all rows
		 * within a single statement.
		 * But AQUMV rewrites the query to a new SQL actually, though the same
		 * results is guaranteed.
		 * Its's unclear whether STABLE is OK, let's be conservative for now.
		 */
		if(contain_mutable_functions((Node *)viewQuery))
			continue;

		context = aqumv_init_context(viewQuery->targetList, matviewRel->rd_att);

		if (!parse->hasAggs && viewQuery->hasAggs)
			continue;

		if (parse->hasAggs && viewQuery->hasAggs)
		{
			if (parse->hasDistinctOn ||
				parse->distinctClause != NIL ||
				parse->groupClause != NIL || /* TODO: GROUP BY */
				parse->groupingSets != NIL ||
				parse->groupDistinct)
				continue;

			/* No Group by now. */
			if (viewQuery->hasDistinctOn ||
				viewQuery->distinctClause != NIL ||
				viewQuery->groupClause != NIL ||
				viewQuery->groupingSets != NIL ||
				viewQuery->groupDistinct ||
				viewQuery->havingQual != NULL || /* HAVING clause is not supported on IMMV yet. */
				limit_needed(viewQuery)) /* LIMIT, OFFSET is not supported on IMMV yet. */
				continue;

			if (tlist_has_srf(parse))
				continue;

			/*
			 * There is a trick for ORDER BY for both origin query and view query.
			 * As we has no Groupy By here, the aggregation results would be either one or
			 * zero rows that make the Order By clause pointless, except that there were
			 * SRF.
			 */
			if (parse->sortClause != NIL || viewQuery->sortClause != NIL)
			{
				/* Earse view's sort caluse, it's ok to let alone view's target list. */
				viewQuery->sortClause = NIL;
			}

			/*
			 * Process Limit:
			 * The result would be one row at most.
			 * View may be useful even Limit clause is different, ex:
			 * View:
			 *   create incremental materialized view mv as
			 *   select count(*) as mc1 from t;
			 * Query:
			 *   select count(*) from t limit 1;
			 * Rewrite to:
			 *   select mc1 from mv limit 1;
			 */
			/* Below logic is based on view has no LIMIT/OFFSET. */
			Assert(!limit_needed(viewQuery));
			if (limit_needed(parse))
			{
				Node	   *node;
				/*
				 * AQUMV don't support sublinks now.
				 * Use query's LIMIT/OFFSET if they are const in case.
				 */
				node = parse->limitCount;
				if (node && !IsA(node, Const))
					continue;

				node = parse->limitOffset;
				if (node && !IsA(node, Const))
					continue;

				viewQuery->limitCount = copyObject(parse->limitCount);
				viewQuery->limitOffset = copyObject(parse->limitOffset);
				viewQuery->limitOption = parse->limitOption;
			}

			preprocess_qual_conditions(subroot, (Node *) viewQuery->jointree);

			if(!aqumv_process_from_quals(parse->jointree->quals, viewQuery->jointree->quals, &post_quals))
				continue;

			if (post_quals != NIL)
				continue;

			/* Move HAVING quals to WHERE quals. */
			viewQuery->jointree->quals = aqumv_adjust_sub_matched_expr_mutator(copyObject(raw_havingQual), context);
			if (context->has_unmatched)
				continue;
			subroot->hasHavingQual = false;

			if(!aqumv_process_targetlist(context, aqumv_context->raw_processed_tlist, &mv_final_tlist))
				continue;

			viewQuery->targetList = mv_final_tlist;
			/* SRF is not supported now, but correct the field. */
			viewQuery->hasTargetSRFs = parse->hasTargetSRFs;
			viewQuery->hasAggs = false;
			subroot->agginfos = NIL;
			subroot->aggtransinfos = NIL;
			subroot->hasNonPartialAggs = false;
			subroot->hasNonSerialAggs = false;
			subroot->numOrderedAggs = false;
			/* CBDB specifical */
			subroot->hasNonCombine = false;
			subroot->numPureOrderedAggs = false;
			/*
			 * NB: Update processed_tlist again in case that tlist has been changed. 
			 */
			subroot->processed_tlist = NIL;
			preprocess_targetlist(subroot);

			/* Select from a mv never have that.*/
			subroot->append_rel_list = NIL;
		}
		else
		{
			/*
			 * Process and rewrite target list, return false if failed.
			 */
			if(!aqumv_process_targetlist(context, parse->targetList, &mv_final_tlist))
				continue;

			viewQuery->targetList = mv_final_tlist;

			/*
			 * NB: Update processed_tlist again in case that tlist has been changed. 
			 */
			preprocess_targetlist(subroot);

			/*
			 * We have successfully processed target list, and all columns in Aggrefs
			 * could be computed from viewQuery.
			 */
			viewQuery->hasAggs = parse->hasAggs;
			viewQuery->hasDistinctOn = parse->hasDistinctOn;
			/*
			 * For HAVING quals don't have aggregations, they may be pushed down to
			 * jointree's quals and would be processed in post_quals later.
			 * Set havingQual before we preprocess_aggrefs for that.
			 */
			viewQuery->havingQual = parse->havingQual;
			if (viewQuery->hasAggs)
			{
				preprocess_aggrefs(subroot, (Node *) subroot->processed_tlist);
				preprocess_aggrefs(subroot, viewQuery->havingQual);
			}

			viewQuery->havingQual = aqumv_adjust_sub_matched_expr_mutator(viewQuery->havingQual, context);
			if (context->has_unmatched)
				continue;

			/* SRF is not supported now, but correct the field. */
			viewQuery->hasTargetSRFs = parse->hasTargetSRFs;
			viewQuery->groupClause = parse->groupClause;
			viewQuery->groupingSets = parse->groupingSets;
			viewQuery->sortClause = parse->sortClause;
			viewQuery->distinctClause = parse->distinctClause;
			viewQuery->limitOption = parse->limitOption;
			viewQuery->limitCount = parse->limitCount;
			viewQuery->limitOffset = parse->limitOffset;

			/*
			 * AQUMV
			 * Process all quals to conjunctive normal form.
			 * 
			 * We assume that the selection predicates of view and query expressions
			 * have been converted into conjunctive normal form(CNF) before we process
			 * them.
			 */
			preprocess_qual_conditions(subroot, (Node *) viewQuery->jointree);

			/*
			 * Process quals, return false if failed. 
			 * Else, post_quals are filled if there were. 
			 * Like process target list, post_quals is used later to see if we could
			 * rewrite and apply it to mv relation.
			 */
			if(!aqumv_process_from_quals(parse->jointree->quals, viewQuery->jointree->quals, &post_quals))
				continue;

			/* Rewrite post_quals, return false if failed. */
			post_quals = (List *)aqumv_adjust_sub_matched_expr_mutator((Node *)post_quals, context);
			if (context->has_unmatched)
				continue;

			viewQuery->jointree->quals = (Node *)post_quals;
			/* Select from a mv never have that.*/
			subroot->append_rel_list = NIL;
		}

		/*
		 * AQUMV
		 * NB: Correct the final_locus as we select from another realtion now.
		 */
		PathTarget *newtarget = make_pathtarget_from_tlist(subroot->processed_tlist);
		subroot->final_locus = cdbllize_get_final_locus(subroot, newtarget);

		/* Rewrite with mv's query tree*/
		mvrte->relkind = RELKIND_MATVIEW;
		mvrte->relid = matviewRel->rd_rel->oid;
		viewQuery->rtable = list_make1(mvrte); /* rewrite to SELECT FROM mv itself. */

		/*
		 * Build a plan of new SQL.
		 * AQUMV is cost-based, let planner decide which is better.
		 */
		mv_final_rel = query_planner(subroot, qp_callback, qp_extra);

		/* AQUMV_FIXME_MVP
		 * We don't use STD_FUZZ_FACTOR for cost comparisons like compare_path_costs_fuzzily here.
		 * The STD_FUZZ_FACTOR is used to reduce paths of a rel, and keep the significantly ones.
		 * But in AQUMV, we always have only one best path of rel at the last to compare.
		 */
		if (mv_final_rel->cheapest_total_path->total_cost < current_rel->cheapest_total_path->total_cost)
		{
			root->parse = viewQuery;
			root->processed_tlist = subroot->processed_tlist;
			root->agginfos = subroot->agginfos;
			root->aggtransinfos =  subroot->aggtransinfos;
			root->simple_rte_array = subroot->simple_rte_array;
			root->simple_rel_array = subroot->simple_rel_array;
			root->simple_rel_array_size = subroot->simple_rel_array_size;
			root->hasNonPartialAggs = subroot->hasNonPartialAggs;
			root->hasNonSerialAggs = subroot->hasNonSerialAggs;
			root->numOrderedAggs = subroot->numOrderedAggs;
			/* CBDB specifical */
			root->hasNonCombine = subroot->hasNonCombine;
			root->numPureOrderedAggs = subroot->numPureOrderedAggs;
			root->hasHavingQual = subroot->hasHavingQual;

			/*
			 * Update pathkeys which may be changed by qp_callback.
			 * Set belows if corresponding feature is supported.
			 * window_pathkeys
			 */
			root->group_pathkeys = subroot->group_pathkeys;
			root->sort_pathkeys = subroot->sort_pathkeys;
			root->query_pathkeys = subroot->query_pathkeys;
			root->distinct_pathkeys = subroot->distinct_pathkeys;
			/*
			 * AQUMV_FIXME_MVP
			 * Use new query's ecs.
			 * Equivalence Class is not supported now, we may lost some ECs if the viewQuery has
			 * equal quals or implicit ones.
			 * But keeping them also introduces more complex as we should process them like target list.
			 * Another flaw: the generated Filter expressions by keeping them are pointless as all
			 * rows of mv have matched the filter expressions.
			 * See more in README.cbdb.aqumv
			 */
			root->eq_classes = subroot->eq_classes;
			root->append_rel_list = subroot->append_rel_list;
			current_rel = mv_final_rel;
			table_close(matviewRel, NoLock);
			need_close = false;
		}
	}
	if (need_close)
		table_close(matviewRel, AccessShareLock);
	systable_endscan(rcscan);
	table_close(ruleDesc, AccessShareLock);
	
	return current_rel;
}

/*
 * AQUMV
 * Since tlist and quals rewrite are both based on mv query's tlist,
 * put all stuff into a context.
 */
static aqumv_equivalent_transformation_context*
aqumv_init_context(List *view_tlist, TupleDesc mv_tupledesc)
{
	aqumv_equivalent_transformation_context *context = palloc0(sizeof(aqumv_equivalent_transformation_context));
	ListCell *lc;

	/*
	 * Process viewQuery's tlist to pure-Var and no pure-Var expressions.
	 * See details in README.cbdb.aqumv
	 */
	int i = -1;
	foreach(lc, view_tlist)
	{
		i++;
		TargetEntry* tle = lfirst_node(TargetEntry, lc);
		/* IMMV query couldn't have resjunk column not, just in case. */
		if (tle->resjunk)
			continue;

		/* Avoid expression has no Vars, excpet for count(*). */
		if(!contain_var_or_aggstar_clause((Node*)tle))
			continue;

		/* To be sorted later */
		context->mv_tlist_ordered_index = lappend_int(context->mv_tlist_ordered_index, i);
	}

	context->mv_tupledesc = mv_tupledesc;
	context->mv_query_tlist = view_tlist;
	context->has_unmatched = false;

	/* Sort target list expressions, prepare for Greedy Algorithm. */
	aqumv_sort_targetlist(context);
	return context;
}

/*
 * Compute a node complexity recursively.
 * Complexity of a node is the total times we enter walker function after all
 * subnodes are walked recursively.
 * It's used to sort the expression in mv's tlist.
 */
static bool
compute_node_complexity_walker(Node *node, node_complexity_context *context)
{
	if (node == NULL)
		return false;
	context->complexity++;
	return expression_tree_walker(node, compute_node_complexity_walker, (void *) context);
}

static int
nonpure_vars_expr_compare(const ListCell *a, const ListCell *b)
{
	expr_to_sort	*ets1 = (expr_to_sort *) lfirst(a);
	expr_to_sort	*ets2 = (expr_to_sort *) lfirst(b);
	return (ets1->count < ets2->count) ? 1 : (ets1->count == ets2->count) ? 0 : -1;
}

/*
 * In-place update order of mv_tlist_ordered_index List
 */
static void
aqumv_sort_targetlist(aqumv_equivalent_transformation_context* context)
{
	ListCell* lc;
	List	*expr_to_sort_list = NIL;
	foreach(lc, context->mv_tlist_ordered_index)
	{
		int index = lfirst_int(lc);
		Node *expr = lfirst(list_nth_cell(context->mv_query_tlist, index));
		node_complexity_context *subnode_context = palloc0(sizeof(node_complexity_context));
		(void) compute_node_complexity_walker(expr, subnode_context);
		expr_to_sort *ets = palloc0(sizeof(expr_to_sort));
		ets->tlist_index = index;
		ets->count = subnode_context->complexity;
		expr_to_sort_list = lappend(expr_to_sort_list, ets);
	}

	/* Sort the expr list */
	list_sort(expr_to_sort_list, nonpure_vars_expr_compare);
	/* Reorder */
	context->mv_tlist_ordered_index = NIL;
	foreach(lc, expr_to_sort_list)
	{
		expr_to_sort *ets = (expr_to_sort *) lfirst(lc);
		context->mv_tlist_ordered_index = lappend_int(context->mv_tlist_ordered_index, ets->tlist_index);
	}
	list_free_deep(expr_to_sort_list);
	return;
}

/*
 * Process query and materialized views' quals.
 * Return true if all mv_quals are in query_quals,
 * else return false.
 *
 * If return true, put quals in query_quals but not in mv_quals
 * into post_quals.
 *
 * Ex: create materialized view mv0 as select * from t1 where c1 = 1;
 * query: select * from t1 where c1 = 1 and c2 = 2;
 * post_quals = {c2 = 2}.
 *
 * AQUMV_FIXME_MVP: only support one relation now, so we don't need to
 * compare varno(both are 1 after aqumv_adjust_varno),
 * mv's query tree has been processed into one relation too.
 * 
 * Will return false if varattno in mv->query has different order with query's.
 * Ex: create materialized view mv0 as select c2, c1 from t1 where c1 = 1;
 * 		query: select c1, c2 from t1 where c1 = 1 and c2 = 2;
 * 
 * The returned post_quals may be or may not be used later, it's up to mv's targetList.
 * 
 */
static bool 
aqumv_process_from_quals(Node *query_quals, Node *mv_quals, List **post_quals)
{
	List *qlist = NIL;
	List *mlist = NIL;

	if (query_quals == NULL)
		return mv_quals == NULL;

	if(!IsA(query_quals, List))
		qlist = list_make1(query_quals);
	else
		qlist = (List *)query_quals;

	if (mv_quals == NULL)
	{
		*post_quals = qlist; 
		return true;
	}

	if(!IsA(mv_quals, List))
		mlist = list_make1(mv_quals);
	else
		mlist = (List *)mv_quals;

	if (list_difference(mlist, qlist) != NIL)
		return false;
	*post_quals = list_difference(qlist, mlist);
	return true;
}

/*
 * Adjust query expr's Vars
 * Replace Vars with corresponding attribute in mv relation.
 * Return a new expr after rewrite. 
 */
static Node *aqumv_adjust_sub_matched_expr_mutator(Node *node, aqumv_equivalent_transformation_context *context)
{
	if (!node || context->has_unmatched)
		return node;
	
	bool is_targetEntry = IsA(node, TargetEntry);
	Expr *node_expr = is_targetEntry ? ((TargetEntry *)node)->expr : (Expr *)node;

	/* Don't select Const results form mv, bypass it to upper when projection. */
	if (IsA(node_expr, Const))
		return is_targetEntry ? node : (Node *)node_expr;

	ListCell 	*lc = NULL;
	foreach(lc, context->mv_tlist_ordered_index)
	{
		int index = lfirst_int(lc);
		TargetEntry *tle = list_nth_node(TargetEntry, context->mv_query_tlist, index);
		if(equal(node_expr, tle->expr))
		{
			Form_pg_attribute attr = TupleDescAttr(context->mv_tupledesc, index);
			Var *newVar =  makeVar(1, /* AQUMV_FIXME_MVP: single relation */
								attr->attnum,
								attr->atttypid,
								attr->atttypmod,
								attr->attcollation,
								0 /* AQUMV_FIXME_MVP: consider subquery? */);
			newVar->location = -1;
			if (is_targetEntry)
			{
				((TargetEntry *) node)->expr = (Expr*) newVar;
				return node;
			}
			else
				return (Node *) newVar;
		}
	}

	/*
	 * We didn't find matched nonpure-Var expr.
	 * And if expr doesn't have Vars, return it to upper.
	 * Keep TargetEntry expr no changed in case for count(*).
	 */
	if (!contain_var_or_aggstar_clause((Node *)node_expr))
		return is_targetEntry ? node : (Node *)node_expr;

	/*
	 * Failed to rewrite.
	 * We have walked through all exprs in mv_tlist_ordered_index,
	 * but didn't find a match for Var at leaf nodes.
	 */
	if (IsA(node_expr, Var))
		context->has_unmatched = true;
	
	return expression_tree_mutator(node, aqumv_adjust_sub_matched_expr_mutator, context);
}

/*
 * Process query and materialized views' target list.
 * Return true if all expressions could be computed from view
 * else return false.
 * 
 * If return true, put tlist in mv_quals but not in query_tlist
 * into post_tlist.
 *
 * Ex: create materialized view mv0 as select c1, c2 from t1 where c1 = 1;
 * query: select c2 from t1 where c1 = 1;
 * post_tlist= {1}.
 * 
 * AQUMV_FIXME_MVP: strict match with same resno?
 * MVP0: expression replace 
 *	mv: select c1, c2 from t1 where c1 = 50;
 *	select c1 from t1 where c1 = 50 and abs(t1.c2) = 51;
 *	rewrite: select c1 from mv where abs(mv.c2) = 51; 
 *
 * MVP1: expression eliminate 
 *	mv: select c1, abs(c2) as c2 from t1 where c1 = 50;
 *	select c1 from t1 where c1 = 50 and abs(c2) = 51;
 *	rewrite: select c1 from mv where c2 = 51; 
 * 
 * mv_final_tlist is the final targetList of viewQuery.
 * 
 */
static bool
aqumv_process_targetlist(aqumv_equivalent_transformation_context *context, List *query_tlist, List **mv_final_tlist)
{
	*mv_final_tlist = (List *)aqumv_adjust_sub_matched_expr_mutator((Node *)(copyObject(query_tlist)), context);
	if (context->has_unmatched)
		pfree(*mv_final_tlist);
	
	return !context->has_unmatched;
}

void aqumv_adjust_simple_query(Query *viewQuery)
{
	ListCell *lc;
	/*
	 * AQUMV
	 * We have to rewrite now before we do the real Equivalent
	 * Transformation 'rewrite'.
	 * Because actions stored in rule is not a normal query tree,
	 * it can't be used directly, with exception to new/old relations used to
	 * refresh mv.
	 * Erase unused relations, keep the right one.
	 */
	foreach (lc, viewQuery->rtable)
	{
		RangeTblEntry *rtetmp = lfirst(lc);
		if ((rtetmp->relkind == RELKIND_MATVIEW) &&
			(rtetmp->alias != NULL) &&
			(strcmp(rtetmp->alias->aliasname, "new") == 0 ||
			 strcmp(rtetmp->alias->aliasname, "old") == 0))
		{
			foreach_delete_current(viewQuery->rtable, lc);
		}
	}

	/*
	 * Now we have the right relation, adjust
	 * varnos in its query tree.
	 * AQUMV_FIXME_MVP: Only one single relation
	 * is supported now, we could assign varno
	 * to 1 opportunistically.
	 */
	aqumv_adjust_varno(viewQuery, 1);
}

/*
 * Process varno after we eliminate mv's actions("old" and "new" relation)
 * Correct rindex and all varnos with a delta.
 *
 * MV's actions query tree:
 *		[rtable]
 *				RangeTblEntry [rtekind=RTE_RELATION]
 *						[alias] Alias [aliasname="old"]
 *				RangeTblEntry [rtekind=RTE_RELATION]
 *						[alias] Alias [aliasname="new"]
 *				RangeTblEntry [rtekind=RTE_RELATION]
 *		[jointree]
 *				FromExpr []
 *						[fromlist]
 *								RangeTblRef [rtindex=3]
 *		[targetList]
 *				TargetEntry [resno=1 resname="c1"]
 *						Var [varno=3 varattno=1]
 *				TargetEntry [resno=2 resname="c2"]
 *						Var [varno=3 varattno=2]
 *------------------------------------------------------------------------------------------
 * MV's query tree after rewrite:
 *		[rtable]
 *				RangeTblEntry [rtekind=RTE_RELATION]
 *		[jointree]
 *				FromExpr []
 *						[fromlist]
 *								RangeTblRef [rtindex=3]
 *		[targetList]
 *				TargetEntry [resno=1 resname="c1"]
 *						Var [varno=3 varattno=1]
 *				TargetEntry [resno=2 resname="c2"]
 *						Var [varno=3 varattno=2]
 *------------------------------------------------------------------------------------------
 * MV's query tree after varno adjust:
 *		[rtable]
 *				RangeTblEntry [rtekind=RTE_RELATION]
 *		[jointree]
 *				FromExpr []
 *						[fromlist]
 *								RangeTblRef [rtindex=1]
 *		[targetList]
 *				TargetEntry [resno=1 resname="c1"]
 *						Var [varno=1 varattno=1]
 *				TargetEntry [resno=2 resname="c2"]
 *						Var [varno=1 varattno=2]
 *
 */
static void
aqumv_adjust_varno(Query* parse, int varno)
{
	aqumv_adjust_varno_context context;
	context.varno = varno;
	parse = query_tree_mutator(parse, aqumv_adjust_varno_mutator, &context, QTW_DONT_COPY_QUERY);
}

static Node *aqumv_adjust_varno_mutator(Node *node, aqumv_adjust_varno_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		((Var *)node)->varno = context->varno;
		((Var *)node)->varnosyn = context->varno; /* Keep syntactic with varno. */
	}
	else if (IsA(node, RangeTblRef))
		/* AQUMV_FIXME_MVP: currently we have only one relation */
		((RangeTblRef*) node)->rtindex = context->varno;
	return expression_tree_mutator(node, aqumv_adjust_varno_mutator, context);
}

/*
 * check_partition - Check if the query's range table entries align with the partitioned table structure.
 *
 * This function verifies whether the range table entries in the query (parse->rtable) correspond to
 * the expected structure of a partitioned table. It ensures that all range table entries beyond the
 * first one match the name of the underlying relation (origin_rel_oid).
 * While this behavior is not guaranteed by Postgres, we can rely on it based on our observation of
 * the internal implementation when expanding partitioned tables.
 * This approach is admittedly hacky, but it serves as a practical solution for now, allowing us to move forward.
 *
 * Parameters:
 *   - parse: The query parse tree containing the range table entries to be checked.
 *   - origin_rel_oid: The OID of the original relation (partitioned table) to compare against.
 *
 * Returns:
 *   - true if all range table entries beyond the first match the underlying relation's name.
 *   - false otherwise.
 */
static bool
check_partition(Query *parse, Oid origin_rel_oid)
{
	char *underling_relname;

	if (list_length(parse->rtable) == 1)
		return true;
	underling_relname = get_rel_name(origin_rel_oid);
	for (int i = 2; i <= list_length(parse->rtable); i++)
	{
		RangeTblEntry *other_rte = rt_fetch(i, parse->rtable);
		if (strcmp(underling_relname, other_rte->alias->aliasname) != 0)
			return false;
	}
	return true;
}
