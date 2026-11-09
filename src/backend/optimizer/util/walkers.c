/*
 * walkers.c
 *
 *  Created on: Feb 8, 2011
 *      Author: siva
 * 
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/walkers.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/**
 * Plan node walker related methods.
 */

/* Initialize a plan_tree_base_prefix during planning. */
void planner_init_plan_tree_base(plan_tree_base_prefix *base, PlannerInfo *root)
{
	base->node = (Node*)root;
}

/* Initialize a plan_tree_base_prefix after planning. */
void exec_init_plan_tree_base(plan_tree_base_prefix *base, PlannedStmt *stmt)
{
	base->node = (Node*)stmt;
}

static bool walk_scan_node_fields(Scan *scan, bool (*walker) (Node *, void *), void *context);
static bool walk_join_node_fields(Join *join, bool (*walker) (Node *, void *), void *context);


/* ----------------------------------------------------------------------- *
 * Plan Tree Walker Framework
 * ----------------------------------------------------------------------- *
 */

/* Function walk_plan_node_fields is a subroutine used by plan_tree_walker()
 * to walk the fields of Plan nodes.  Plan is actually an abstract superclass
 * of all plan nodes and this function encapsulates the common structure.
 *
 * Most specific walkers won't need to call this function, but complicated
 * ones may find it a useful utility.
 *
 * Caution: walk_scan_node_fields and walk_join_node_fields call this
 * function.  Use only the most specific function.
 */
bool
walk_plan_node_fields(Plan *plan,
					  bool (*walker) (Node *, void *),
					  void *context)
{
	/* target list to be computed at this node */
	if (walker((Node *) (plan->targetlist), context))
		return true;

	/* implicitly ANDed qual conditions */
	if (walker((Node *) (plan->qual), context))
		return true;

	/* input plan tree(s) */
	if (walker((Node *) (plan->lefttree), context))
		return true;

	/* target list to be computed at this node */
	if (walker((Node *) (plan->righttree), context))
		return true;

	/* Init Plan nodes (uncorrelated expr subselects */
	if (walker((Node *) (plan->initPlan), context))
		return true;

	/* Apache Cloudberry Flow node */
	if (walker((Node *) (plan->flow), context))
		return true;

	return false;
}


/* Function walk_scan_node_fields is a subroutine used by plan_tree_walker()
 * to walk the fields of Scan nodes.  Scan is actually an abstract superclass
 * of all scan nodes and a subclass of Plan.  This function encapsulates the
 * common structure.
 *
 * Most specific walkers won't need to call this function, but complicated
 * ones may find it a useful utility.
 *
 * Caution: This function calls walk_plan_node_fields so callers shouldn't,
 * else they will walk common plan fields twice.
 */
bool
walk_scan_node_fields(Scan *scan,
					  bool (*walker) (Node *, void *),
					  void *context)
{
	/* A Scan node is a kind of Plan node. */
	if (walk_plan_node_fields((Plan *) scan, walker, context))
		return true;

	/* The only additional field is an Index so no extra walking. */
	return false;
}


/* Function walk_join_node_fields is a subroutine used by plan_tree_walker()
 * to walk the fields of Join nodes.  Join is actually an abstract superclass
 * of all join nodes and a subclass of Plan.  This function encapsulates the
 * common structure.
 *
 * Most specific walkers won't need to call this function, but complicated
 * ones may find it a useful utility.
 *
 * Caution: This function calls walk_plan_node_fields so callers shouldn't,
 * else they will walk common plan fields twice.
 */
bool
walk_join_node_fields(Join *join,
					  bool (*walker) (Node *, void *),
					  void *context)
{
	/* A Join node is a kind of Plan node. */
	if (walk_plan_node_fields((Plan *) join, walker, context))
		return true;

	return walker((Node *) (join->joinqual), context);
}


/* Function plan_tree_walker is a general walker for Plan trees.
 *
 * It is based on (and uses) the expression_tree_walker() framework from
 * src/backend/optimizer/util/clauses.c.  See the comments there for more
 * insight into how this function works.
 *
 * The basic idea is that this function (and its helpers) walk plan-specific
 * nodes and delegate other nodes to expression_tree_walker().	The caller
 * may supply a specialized walker
 */
bool
plan_tree_walker(Node *node,
				 bool (*walker) (Node *, void *),
				 void *context,
				 bool recurse_into_subplans)
{
	/*
	 * The walker has already visited the current node, and so we need
	 * only recurse into any sub-nodes it has.
	 *
	 * We assume that the walker is not interested in List nodes per se, so
	 * when we expect a List we just recurse directly to self without
	 * bothering to call the walker.
	 */
	if (node == NULL)
		return false;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
			/*
			 * Plan nodes aren't handled by expression_tree_walker, so we need
			 * to do them here.
			 */
		case T_Plan:
			/* Abstract: really should see only subclasses. */
			return walk_plan_node_fields((Plan *) node, walker, context);

		case T_Result:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker((Node *) ((Result *) node)->resconstantqual, context))
				return true;
			break;

		case T_ProjectSet:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_PartitionSelector:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_Append:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker((Node *) ((Append *) node)->appendplans, context))
				return true;
			break;

		case T_MergeAppend:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker((Node *) ((MergeAppend *) node)->mergeplans, context))
				return true;
			break;

		case T_RecursiveUnion:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_Sequence:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker((Node *) ((Sequence *) node)->subplans, context))
				return true;
			break;

		case T_BitmapAnd:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
            if (walker((Node *) ((BitmapAnd *) node)->bitmapplans, context))
                return true;
            break;
        case T_BitmapOr:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
            if (walker((Node *) ((BitmapOr *) node)->bitmapplans, context))
                return true;
            break;

		case T_Scan:
			/* Abstract: really should see only subclasses. */
			return walk_scan_node_fields((Scan *) node, walker, context);

		case T_SeqScan:
		case T_SampleScan:
		case T_DynamicSeqScan:
		case T_BitmapHeapScan:
		case T_DynamicBitmapHeapScan:
		case T_WorkTableScan:
		case T_NamedTuplestoreScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			break;

		case T_ForeignScan:
		case T_DynamicForeignScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((ForeignScan *) node)->fdw_exprs, context))
				return true;
			break;

		case T_CustomScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
		if (walker((Node *) ((CustomScan *) node)->custom_plans, context))
			return true;
		if (walker((Node *) ((CustomScan *) node)->custom_exprs, context))
			return true;
		if (walker((Node *) ((CustomScan *) node)->custom_scan_tlist, context))
				return true;
			break;

		case T_ValuesScan:
			if (walker((Node *) ((ValuesScan *) node)->values_lists, context))
				return true;
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			break;

		case T_TableFuncScan:
			if (walker((Node *) ((TableFuncScan *) node)->tablefunc, context))
				return true;
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			break;
			
		case T_TableFunctionScan:
			if (walker((Node *) ((TableFunctionScan *) node)->function, context))
				return true;
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			break;

		case T_FunctionScan:
			if (walker((Node *) ((FunctionScan *) node)->functions, context))
				return true;
			if (walker((Node *) ((FunctionScan *) node)->param, context))
				return true;
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			break;

		case T_IndexScan:
		case T_DynamicIndexScan:
		case T_DynamicIndexOnlyScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((IndexScan *) node)->indexqual, context))
				return true;
			/* Other fields are lists of basic items, nothing to walk. */
			break;

		case T_IndexOnlyScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((IndexOnlyScan *) node)->indexqual, context))
				return true;
			break;

		case T_BitmapIndexScan:
		case T_DynamicBitmapIndexScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((BitmapIndexScan *) node)->indexqual, context))
				return true;
			/* Other fields are lists of basic items, nothing to walk. */
			break;

		case T_TidScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((TidScan *) node)->tidquals, context))
				return true;
			break;

		case T_TidRangeScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((TidRangeScan *) node)->tidrangequals, context))
				return true;
			break;

		case T_SubqueryScan:
			if (walk_scan_node_fields((Scan *) node, walker, context))
				return true;
			if (walker((Node *) ((SubqueryScan *) node)->subplan, context))
				return true;
			break;

		case T_Join:
			/* Abstract: really should see only subclasses. */
			return walk_join_node_fields((Join *) node, walker, context);

		case T_NestLoop:
			if (walk_join_node_fields((Join *) node, walker, context))
				return true;
			break;

		case T_MergeJoin:
			if (walk_join_node_fields((Join *) node, walker, context))
				return true;
			if (walker((Node *) ((MergeJoin *) node)->mergeclauses, context))
				return true;
			break;

		case T_HashJoin:
			if (walk_join_node_fields((Join *) node, walker, context))
				return true;
			if (walker((Node *) ((HashJoin *) node)->hashclauses, context))
				return true;
			if (walker((Node *) ((HashJoin *) node)->hashqualclauses, context))
				return true;
			break;

		case T_Material:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_Sort:
		case T_IncrementalSort:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple counts and lists of indexes and oids. */
			break;

		case T_Agg:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_TupleSplit:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (expression_tree_walker((Node *)((TupleSplit *)node)->dqa_expr_lst, walker, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_DQAExpr:
			if (walker((Node *) ((DQAExpr *)node)->agg_filter, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_WindowAgg:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker(((WindowAgg *) node)->startOffset, context))
				return true;
			if (walker(((WindowAgg *) node)->endOffset, context))
				return true;
			break;
		case T_WindowHashAgg:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker(((WindowHashAgg *) node)->startOffset, context))
				return true;
			if (walker(((WindowHashAgg *) node)->endOffset, context))
				return true;
			break;

		case T_Unique:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_Gather:
		case T_GatherMerge:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple items. */
			break;

		case T_Hash:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other info is in parent HashJoin node. */
			break;

		case T_SetOp:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_RuntimeFilter:

			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			/* Other fields are simple items and lists of simple items. */
			break;

		case T_Limit:

			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;

			/* Apache Cloudberry Limit Count */
			if (walker((Node *) (((Limit*) node)->limitCount), context))
					return true;

			/* Apache Cloudberry Limit Offset */
			if (walker((Node *) (((Limit*) node)->limitOffset), context))
					return true;

			/* Other fields are simple items and lists of simple items. */
			break;

		case T_Motion:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;

			if (walker((Node *) ((Motion *)node)->hashExprs, context))
				return true;

			break;

		case T_ShareInputScan:
		case T_CteScan:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_Flow:
			/* Nothing to walk in Flow nodes at present. */
			break;

		case T_SubPlan:

			/*
			 * SubPlan is handled by expression_tree_walker() , but that
			 * walker doesn't recur into the plan itself.  Here, we do so
			 * by using information in the prefix structure defined in
			 * cdbplan.h.
			 *
			 * NB Callers usually don't have to take special account of
			 *    the range table, but should be sure to understand what
			 *    stage of processing they occur in (e.g., early planning,
			 *    late planning, dispatch, or execution) since this affects
			 *    what value are available.
			 */
			{
				SubPlan    *subplan = (SubPlan *) node;
				Plan	   *subplan_plan = plan_tree_base_subplan_get_plan(context, subplan);

				Assert(subplan_plan);

				/* recurse into the exprs list */
				if (expression_tree_walker((Node *) subplan->testexpr,
										   walker, context))
					return true;

				/* recurse into the subplan's plan, a kind of Plan node */
				if (recurse_into_subplans && walker((Node *) subplan_plan, context))
					return true;

				/* also examine args list */
				if (expression_tree_walker((Node *) subplan->args,
										   walker, context))
					return true;
			}
			break;

		case T_IntList:
		case T_OidList:

			/*
			 * Note that expression_tree_walker handles T_List but not these.
			 * No contained stuff, so just succeed.
			 */
			break;

		case T_ModifyTable:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			// if (walker((Node *) ((ModifyTable *) node)->plans, context))
			// 	return true;
			if (walker((Node *) ((ModifyTable *) node)->withCheckOptionLists, context))
				return true;
			if (walker((Node *) ((ModifyTable *) node)->onConflictSet, context))
				return true;
			if (walker((Node *) ((ModifyTable *) node)->onConflictWhere, context))
				return true;
			if (walker((Node *) ((ModifyTable *) node)->returningLists, context))
				return true;
			if (walker((Node *) ((ModifyTable *) node)->mergeActionLists, context))
				return true;

			break;

		case T_LockRows:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_SplitUpdate:
		case T_SplitMerge:
		case T_AssertOp:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			break;

		case T_Memoize:
			if (walk_plan_node_fields((Plan *) node, walker, context))
				return true;
			if (walker((Node *) ((Memoize *) node)->param_exprs, context))
				return true;
			break;

			/*
			 * Query nodes are handled by the Postgres query_tree_walker. We
			 * just use it without setting any ignore flags.  The caller must
			 * handle query nodes specially to get other behavior, e.g. by
			 * calling query_tree_walker directly.
			 */
		case T_Query:
			return query_tree_walker((Query *) node, walker, context, 0);

			/*
			 * The following cases are handled by expression_tree_walker.  In
			 * addition, we let expression_tree_walker handle unrecognized
			 * nodes.
			 *
			 * TODO: Identify node types that should never appear in plan trees
			 * and disallow them here by issuing an error or asserting false.
			 */
		case T_Var:
		case T_Const:
		case T_Param:
		case T_CoerceToDomainValue:
		case T_CaseTestExpr:
		case T_SetToDefault:
		case T_CurrentOfExpr:
		case T_RangeTblRef:
		case T_Aggref:
		case T_SubscriptingRef:
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_ScalarArrayOpExpr:
		case T_BoolExpr:
		case T_SubLink:
		case T_FieldSelect:
		case T_FieldStore:
		case T_RelabelType:
		case T_CaseExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_CoalesceExpr:
		case T_NullIfExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
		case T_TargetEntry:
		case T_List:
		case T_FromExpr:
		case T_JoinExpr:
		case T_SetOperationStmt:
		case T_SpecialJoinInfo:
		case T_TableValueExpr:

		default:
			return expression_tree_walker(node, walker, context);
	}
	return false;
}

/* Return the plan associated with a SubPlan node in a walker.  (This is used by
 * framework, not by users of the framework.)
 */
Plan *plan_tree_base_subplan_get_plan(plan_tree_base_prefix *base, SubPlan *subplan)
{
	if (!base)
		return NULL;
	else if (IsA(base->node, PlannedStmt))
		return exec_subplan_get_plan((PlannedStmt*)base->node, subplan);
	else if (IsA(base->node, PlannerInfo))
		return planner_subplan_get_plan((PlannerInfo*)base->node, subplan);
	else if (IsA(base->node, PlannerGlobal))
	{
		PlannerInfo rootdata;
		rootdata.glob = (PlannerGlobal*)base->node;
		return planner_subplan_get_plan(&rootdata, subplan);
	}
	Assert(false);
	return NULL;
}

/* Rewrite the plan associated with a SubPlan node in a mutator.  (This is used by
 * framework, not by users of the framework.)
 */
void plan_tree_base_subplan_put_plan(plan_tree_base_prefix *base, SubPlan *subplan, Plan *plan)
{
	Assert(base);
	if (IsA(base->node, PlannedStmt))
	{
		exec_subplan_put_plan((PlannedStmt*)base->node, subplan, plan);
		return;
	}
	else if (IsA(base->node, PlannerInfo))
	{
		planner_subplan_put_plan((PlannerInfo*)base->node, subplan, plan);
		return;
	}
	Assert(false && "Must provide relevant base info.");
}

/**
 * These are helpers to retrieve nodes from plans.
 */
typedef struct extract_context
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
 	bool descendIntoSubqueries;
	NodeTag  nodeTag;
	List *nodes;
} extract_context;

static bool extract_nodes_walker(Node *node, extract_context *context);
static bool extract_nodes_expression_walker(Node *node, extract_context *context);

/**
 * Extract nodes with specific tag.
 */
List *extract_nodes(PlannerGlobal *glob, Node *node, int nodeTag)
{
	extract_context context;
	context.base.node = (Node *) glob;
	context.nodes = NULL;
	context.nodeTag = nodeTag;
	context.descendIntoSubqueries = false;
	extract_nodes_walker(node, &context);
	return context.nodes;
}

/**
 * Extract nodes with specific tag.
 * Same as above, but starts off a Plan node rather than a PlannedStmt
 *
 */
List *extract_nodes_plan(Plan *pl, int nodeTag, bool descendIntoSubqueries)
{
	extract_context context;
	Assert(pl);
	context.base.node = NULL;
	context.nodes = NULL;
	context.nodeTag = nodeTag;
	context.descendIntoSubqueries = descendIntoSubqueries;
	extract_nodes_walker((Node *)pl, &context);
	return context.nodes;
}

static bool
extract_nodes_walker(Node *node, extract_context *context);
static bool
extract_nodes_walker_adapter(Node *node, void *context);

static bool
extract_nodes_walker(Node *node, extract_context *context)
{
	if (node == NULL)
		return false;
	if (nodeTag(node) == context->nodeTag)
	{
		context->nodes = lappend(context->nodes, node);
	}
	if (nodeTag(node) == T_SubPlan)
	{
		SubPlan	   *subplan = (SubPlan *) node;

		/*
		 * SubPlan has both of expressions and subquery.  In case the caller wants
		 * non-subquery version, still we need to walk through its expressions.
		 * NB: Since we're not going to descend into SUBPLANs anyway (see below),
		 * look at the SUBPLAN node here, even if descendIntoSubqueries is false
		 * lest we miss some nodes there.
		 */
		if (extract_nodes_walker((Node *) subplan->testexpr,
								 context))
			return true;
		if (expression_tree_walker((Node *) subplan->args,
								   extract_nodes_walker, context))
			return true;

		/*
		 * Do not descend into subplans.
		 * Even if descendIntoSubqueries indicates the caller wants to descend into
		 * subqueries, SubPlan seems special; Some partitioning code assumes this
		 * should return immediately without descending.  See MPP-17168.
		 */
		return false;
	}
	if (nodeTag(node) == T_SubqueryScan
		&& !context->descendIntoSubqueries)
	{
		/* Do not descend into subquery scans. */
		return false;
	}
	if (nodeTag(node) == T_TableFunctionScan
		&& !context->descendIntoSubqueries)
	{
		/* Do not descend into table function subqueries */
		return false;
	}

	return plan_tree_walker(node, extract_nodes_walker_adapter,
							(void *) context,
							true);
}

static bool
extract_nodes_walker_adapter(Node *node, void *context)
{
	return extract_nodes_walker(node, (extract_context *) context);
}

/**
 * Extract nodes with specific tag.
 * Same as above, but starts off a scalar expression node rather than a PlannedStmt
 *
 */
List *extract_nodes_expression(Node *node, int nodeTag, bool descendIntoSubqueries)
{
	extract_context context;
	Assert(node);
	context.base.node = NULL;
	context.nodes = NULL;
	context.nodeTag = nodeTag;
	context.descendIntoSubqueries = descendIntoSubqueries;
	extract_nodes_expression_walker(node, &context);
	
	return context.nodes;
}

static bool
extract_nodes_expression_walker(Node *node, extract_context *context)
{
	if (NULL == node)
	{
		return false;
	}
	
	if (nodeTag(node) == context->nodeTag)
	{
		context->nodes = lappend(context->nodes, node);
	}
	
	if (nodeTag(node) == T_Query && context->descendIntoSubqueries)
	{
		Query *query = (Query *) node;
		if (expression_tree_walker((Node *)query->targetList, extract_nodes_expression_walker, (void *) context))
		{
			return true;
		}

		if (query->jointree != NULL &&
		   expression_tree_walker(query->jointree->quals, extract_nodes_expression_walker, (void *) context))
		{
			return true;
		}

		return expression_tree_walker(query->havingQual, extract_nodes_expression_walker, (void *) context);
	}

	return expression_tree_walker(node, extract_nodes_expression_walker, (void *) context);
}

/**
 * These are helpers to find node in queries
 */
typedef struct find_nodes_context
{
	List *nodeTags;
	int foundNode;
} find_nodes_context;

static bool find_nodes_walker(Node *node, find_nodes_context *context);

/**
 * Looks for nodes that belong to the given list.
 * Returns the index of the first such node that it encounters, or -1 if none
 */
int find_nodes(Node *node, List *nodeTags)
{
	find_nodes_context context;
	Assert(NULL != node);
	context.nodeTags = nodeTags;
	context.foundNode = -1;
	find_nodes_walker(node, &context);

	return context.foundNode;
}

static bool
find_nodes_walker(Node *node, find_nodes_context *context)
{
	if (NULL == node)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node, find_nodes_walker, (void *) context, 0 /* flags */);
	}

	ListCell *lc;
	int i = 0;
	foreach(lc, context->nodeTags)
	{
		NodeTag nodeTag = (NodeTag) lfirst_int(lc);
		if (nodeTag(node) == nodeTag)
		{
			context->foundNode = i;
			return true;
		}

		i++;
	}

	return expression_tree_walker(node, find_nodes_walker, (void *) context);
}

/**
 * GPDB_91_MERGE_FIXME: collation
 * Look for nodes with non-default collation; return 1 if any exist, -1
 * otherwise.
 */
typedef struct check_collation_context
{
	int foundNonDefaultCollation;
} check_collation_context;

static bool check_collation_walker(Node *node, check_collation_context *context);

int check_collation(Node *node)
{
	check_collation_context context;
	Assert(NULL != node);
	context.foundNonDefaultCollation = -1;
	check_collation_walker(node, &context);

	return context.foundNonDefaultCollation;
}


static void
check_collation_in_list(List *colllist, check_collation_context *context)
{
	ListCell *lc;
	foreach (lc, colllist)
	{
		Oid coll = lfirst_oid(lc);
		if (InvalidOid != coll && DEFAULT_COLLATION_OID != coll &&
			C_COLLATION_OID != coll)
		{
			context->foundNonDefaultCollation = 1;
			break;
		}
	}
}

/*
 * ORCA does not keep the collations computed by the parser: when translating
 * its plan back, it derives them again bottom up, from the collations of the
 * columns (see gpdb::ExprCollation() and CTranslatorDXLToScalar). The helpers
 * below mirror those rules, so that queries whose collations would come out
 * differently, e.g. because an explicit COLLATE clause overrides an implicit
 * collation, can be left to the Postgres planner.
 */

/*
 * Do the collation computed by the parser and the one derived by ORCA agree?
 * Only two different valid collations make a difference in the results:
 * whether a collation-insensitive operation gets the default collation or
 * none does not matter, and a collation-sensitive one would raise an error
 * rather than produce a different result.
 */
static bool
orca_same_collation(Oid parser_collation, Oid orca_collation)
{
	if (!OidIsValid(parser_collation))
		parser_collation = DEFAULT_COLLATION_OID;
	if (!OidIsValid(orca_collation))
		orca_collation = DEFAULT_COLLATION_OID;

	return parser_collation == orca_collation;
}

/*
 * Collation ORCA gives to an expression. Nodes that ORCA rebuilds with the
 * collation of their type may differ from the parser's, e.g. a folded
 * current_user constant has no collation in the query but gets the one of
 * type name. For the other nodes, the parser's collation is checked by
 * orca_collations_match().
 */
static Oid
orca_expr_collation(Node *node)
{
	if (node == NULL)
		return InvalidOid;

	switch (nodeTag(node))
	{
		case T_Const:
			return get_typcollation(((Const *) node)->consttype);
		case T_Param:
			if (((Param *) node)->paramkind == PARAM_EXTERN)
				return get_typcollation(((Param *) node)->paramtype);
			break;
		case T_CaseTestExpr:
			return get_typcollation(((CaseTestExpr *) node)->typeId);
		case T_ArrayExpr:
			return get_typcollation(((ArrayExpr *) node)->array_typeid);
		case T_SubscriptingRef:
			return get_typcollation(((SubscriptingRef *) node)->refelemtype);
		case T_CoerceViaIO:
			return get_typcollation(((CoerceViaIO *) node)->resulttype);
		case T_ArrayCoerceExpr:
			return get_typcollation(((ArrayCoerceExpr *) node)->resulttype);
		case T_CoerceToDomain:
			return get_typcollation(((CoerceToDomain *) node)->resulttype);
		case T_RelabelType:
			return orca_expr_collation((Node *) ((RelabelType *) node)->arg);
		default:
			break;
	}

	return exprCollation(node);
}

/*
 * Collation ORCA derives for a list of arguments: like for implicit
 * collations in the parser, a non-default collation beats the default one.
 */
static Oid
orca_merge_collations(List *exprs)
{
	Oid			result = InvalidOid;
	ListCell   *lc;

	foreach(lc, exprs)
	{
		Oid			coll = orca_expr_collation((Node *) lfirst(lc));

		if (OidIsValid(coll) &&
			(!OidIsValid(result) || result == DEFAULT_COLLATION_OID))
			result = coll;
	}

	return result;
}

/* Collation ORCA derives for the result of an operator or function */
static Oid
orca_result_collation(Oid input_collation, Oid result_type)
{
	Oid			type_collation = get_typcollation(result_type);

	if (!OidIsValid(type_collation))
		return InvalidOid;

	return OidIsValid(input_collation) ? input_collation : type_collation;
}

/*
 * ORCA gives constants and parameters the collation of their type, which an
 * explicit COLLATE clause on a constant overrides in the parser. A leaf
 * without a collation (e.g. a folded current_user) or of a type without one
 * (e.g. a pseudo-type) has no collation to lose.
 */
static bool
orca_leaf_collation_matches(Oid collation, Oid type)
{
	Oid			type_collation = get_typcollation(type);

	return !OidIsValid(collation) || !OidIsValid(type_collation) ||
		collation == type_collation;
}

/* Collation ORCA derives for the input of an aggregate */
static Oid
orca_aggref_input_collation(Aggref *aggref)
{
	List	   *args = NIL;
	ListCell   *lc;

	/* the ORDER BY-only arguments do not count, like in the parser */
	foreach(lc, aggref->args)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (!tle->resjunk)
			args = lappend(args, tle->expr);
	}

	return orca_merge_collations(args);
}

/* Collation ORCA derives for the result of a CASE expression */
static Oid
orca_case_collation(CaseExpr *caseexpr)
{
	List	   *results = NIL;
	ListCell   *lc;
	Oid			coll;

	foreach(lc, caseexpr->args)
		results = lappend(results, ((CaseWhen *) lfirst(lc))->result);
	if (caseexpr->defresult)
		results = lappend(results, caseexpr->defresult);

	coll = orca_merge_collations(results);

	return OidIsValid(coll) ? coll : get_typcollation(caseexpr->casetype);
}

/*
 * Does ORCA derive the same collations for this node as the parser did,
 * provided it does so for its arguments?
 */
static bool
orca_collations_match(Node *node)
{
	Oid			input;

	switch (nodeTag(node))
	{
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
			{
				OpExpr	   *op = (OpExpr *) node;

				input = orca_merge_collations(op->args);
				return orca_same_collation(op->inputcollid, input) &&
					orca_same_collation(op->opcollid,
										orca_result_collation(input,
															  op->opresulttype));
			}
		case T_ScalarArrayOpExpr:
			return orca_same_collation(((ScalarArrayOpExpr *) node)->inputcollid,
									   orca_merge_collations(((ScalarArrayOpExpr *) node)->args));
		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;

				input = orca_merge_collations(func->args);
				return orca_same_collation(func->inputcollid, input) &&
					orca_same_collation(func->funccollid,
										orca_result_collation(input,
															  func->funcresulttype));
			}
		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;

				input = orca_aggref_input_collation(aggref);
				return orca_same_collation(aggref->inputcollid, input) &&
					orca_same_collation(aggref->aggcollid,
										orca_result_collation(input,
															  aggref->aggtype));
			}
		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;

				input = orca_merge_collations(wfunc->args);
				return orca_same_collation(wfunc->inputcollid, input) &&
					orca_same_collation(wfunc->wincollid,
										orca_result_collation(input,
															  wfunc->wintype));
			}
		case T_MinMaxExpr:
			{
				MinMaxExpr *minmax = (MinMaxExpr *) node;

				input = orca_merge_collations(minmax->args);
				return orca_same_collation(minmax->inputcollid, input) &&
					orca_same_collation(minmax->minmaxcollid,
										orca_result_collation(input,
															  minmax->minmaxtype));
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;

				input = orca_merge_collations(coalesce->args);
				return orca_same_collation(coalesce->coalescecollid,
										   orca_result_collation(input,
																 coalesce->coalescetype));
			}
		case T_CaseExpr:
			return orca_same_collation(((CaseExpr *) node)->casecollid,
									   orca_case_collation((CaseExpr *) node));
		case T_Const:
			/* e.g. a constant with an explicit COLLATE clause */
			return orca_leaf_collation_matches(((Const *) node)->constcollid,
											   ((Const *) node)->consttype);
		case T_Param:
			if (((Param *) node)->paramkind != PARAM_EXTERN)
				return true;
			return orca_leaf_collation_matches(((Param *) node)->paramcollid,
											   ((Param *) node)->paramtype);
		case T_CaseTestExpr:
			return orca_leaf_collation_matches(((CaseTestExpr *) node)->collation,
											   ((CaseTestExpr *) node)->typeId);
		case T_ArrayExpr:
			return orca_same_collation(((ArrayExpr *) node)->array_collid,
									   orca_expr_collation(node));
		case T_SubscriptingRef:
			return orca_same_collation(((SubscriptingRef *) node)->refcollid,
									   orca_expr_collation(node));
		case T_CoerceViaIO:
			return orca_same_collation(((CoerceViaIO *) node)->resultcollid,
									   orca_expr_collation(node));
		case T_ArrayCoerceExpr:
			return orca_same_collation(((ArrayCoerceExpr *) node)->resultcollid,
									   orca_expr_collation(node));
		case T_CoerceToDomain:
			return orca_same_collation(((CoerceToDomain *) node)->resultcollid,
									   orca_expr_collation(node));
		default:
			return true;
	}
}

static bool
check_collation_walker(Node *node, check_collation_context *context)
{
	Oid collation, inputCollation, type;

	if (NULL == node)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node, check_collation_walker, (void *) context, 0 /* flags */);
	}

	if (!orca_collations_match(node))
	{
		context->foundNonDefaultCollation = 1;
		return true;
	}

	/*
	 * ORCA cannot propagate collation through SubPlan boundaries
	 * (CColRefComputed doesn't carry collation).  When a set-returning
	 * function (e.g. unnest) takes a SubLink argument whose output has
	 * non-default collation (including C), the expanded rows lose their
	 * collation, causing wrong sort order in collation-sensitive operations.
	 * Fall back in this case.
	 *
	 * The SubLink may be wrapped in coercion nodes (RelabelType,
	 * ArrayCoerceExpr, CoerceViaIO, etc.) so we strip those to find it.
	 */
	if (IsA(node, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) node;
		if (func->funcretset)
		{
			ListCell *lc;
			foreach(lc, func->args)
			{
				Node *arg = lfirst(lc);
				/* Strip coercion wrappers to find a buried SubLink */
				for (;;)
				{
					if (arg == NULL)
						break;
					if (IsA(arg, SubLink))
						break;
					if (IsA(arg, RelabelType))
						arg = (Node *) ((RelabelType *) arg)->arg;
					else if (IsA(arg, ArrayCoerceExpr))
						arg = (Node *) ((ArrayCoerceExpr *) arg)->arg;
					else if (IsA(arg, CoerceViaIO))
						arg = (Node *) ((CoerceViaIO *) arg)->arg;
					else if (IsA(arg, ConvertRowtypeExpr))
						arg = (Node *) ((ConvertRowtypeExpr *) arg)->arg;
					else
						break;
				}
				if (arg && IsA(arg, SubLink))
				{
					Oid coll = exprCollation(arg);
					if (OidIsValid(coll) && coll != DEFAULT_COLLATION_OID)
					{
						context->foundNonDefaultCollation = 1;
						break;
					}
				}
			}
		}
	}

	switch (nodeTag(node))
	{
		case T_Var:
		case T_OpExpr:
			type = exprType((node));
			collation = exprCollation(node);
			if (type == NAMEOID || type == NAMEARRAYOID)
			{
				if (collation != C_COLLATION_OID)
					context->foundNonDefaultCollation = 1;
			}
			else if (InvalidOid != collation && DEFAULT_COLLATION_OID != collation &&
				 C_COLLATION_OID != collation)
			{
				context->foundNonDefaultCollation = 1;
			}
			break;
		case T_Const:
		{
			/*
			 * fold_constants() converts CollateExpr on a constant into a
			 * Const with modified constcollid (no RelabelType wrapper).
			 * Normal string constants have constcollid = DEFAULT_COLLATION_OID.
			 * Constants with explicit COLLATE (e.g., 'foo' COLLATE "C")
			 * have a different constcollid that ORCA cannot propagate.
			 *
			 * To distinguish explicit COLLATE from inherent type collation
			 * (e.g., '1505703298'::name where name type has C collation),
			 * compare constcollid with the type's default collation.
			 * A mismatch means an explicit COLLATE override was applied.
			 */
			Const *c = (Const *) node;
			if (OidIsValid(c->constcollid) &&
				c->constcollid != DEFAULT_COLLATION_OID &&
				c->constcollid != get_typcollation(c->consttype))
			{
				context->foundNonDefaultCollation = 1;
			}
			break;
		}
		case T_ScalarArrayOpExpr:
		case T_DistinctExpr:
		case T_BoolExpr:
		case T_BooleanTest:
		case T_CaseExpr:
		case T_CaseTestExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_FuncExpr:
		case T_Aggref:
		case T_WindowFunc:
		case T_NullTest:
		case T_NullIfExpr:
		case T_CoerceToDomain:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_SubLink:
		case T_ArrayExpr:
		case T_SubscriptingRef:
		case T_RowExpr:
		case T_RowCompareExpr:
		case T_FieldSelect:
		case T_FieldStore:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
		case T_NamedArgExpr:
		case T_ConvertRowtypeExpr:
		case T_CollateExpr:
		case T_TableValueExpr:
		case T_XmlExpr:
		case T_SetToDefault:
		case T_PlaceHolderVar:
		case T_Param:
		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_GroupingFunc:
		case T_DMLActionExpr:
			collation = exprCollation(node);
			inputCollation = exprInputCollation(node);
			if ((InvalidOid != collation && DEFAULT_COLLATION_OID != collation &&
				 C_COLLATION_OID != collation) ||
				(InvalidOid != inputCollation && DEFAULT_COLLATION_OID != inputCollation &&
				 C_COLLATION_OID != inputCollation))
			{
				context->foundNonDefaultCollation = 1;
			}
			break;
		case T_RelabelType:
		{
			/*
			 * fold_constants() converts CollateExpr to RelabelType.
			 * Detect expression-level COLLATE override by checking if the
			 * RelabelType's collation differs from its argument's collation.
			 * ORCA does not yet propagate expression-level collation, so
			 * these must fall back to the Postgres planner.
			 *
			 * Skip the check when the argument's collation is InvalidOid.
			 * This happens for CaseTestExpr inside ArrayCoerceExpr, where
			 * the parser leaves the placeholder's collation unset.
			 * Treating that as a mismatch would cause false fallback for
			 * ordinary casts like text[]::varchar[].
			 */
			RelabelType *r = (RelabelType *) node;
			Oid arg_coll = exprCollation((Node *) r->arg);
			if (OidIsValid(r->resultcollid) &&
				OidIsValid(arg_coll) &&
				r->resultcollid != arg_coll)
			{
				context->foundNonDefaultCollation = 1;
				break;
			}
			/* Normal type coercion: check like other expression nodes */
			collation = exprCollation(node);
			inputCollation = exprInputCollation(node);
			if ((InvalidOid != collation && DEFAULT_COLLATION_OID != collation &&
				 C_COLLATION_OID != collation) ||
				(InvalidOid != inputCollation && DEFAULT_COLLATION_OID != inputCollation &&
				 C_COLLATION_OID != inputCollation))
			{
				context->foundNonDefaultCollation = 1;
			}
			break;
		}
		case T_CollateClause:
			/*
			 * CollateClause is a raw parse node without a resolved OID.
			 * Analyzed COLLATE expressions use T_CollateExpr instead,
			 * which is handled above with the C_COLLATION_OID check.
			 * Keep this as unconditional reject for safety.
			 */
			context->foundNonDefaultCollation = 1;
			break;
		case T_RangeTblEntry:
			check_collation_in_list(((RangeTblEntry *) node)->colcollations, context);
			break;
		case T_RangeTblFunction:
			check_collation_in_list(((RangeTblFunction *) node)->funccolcollations, context);
			break;
		case T_CommonTableExpr:
			check_collation_in_list(((CommonTableExpr *) node)->ctecolcollations, context);
			break;
		case T_SetOperationStmt:
			check_collation_in_list(((SetOperationStmt *) node)->colCollations, context);
			break;
		default:
			/* make compiler happy */
			break;
	}

	if (context->foundNonDefaultCollation == 1)
	{
		/* end recursion */
		return true;
	}
	else
	{
		return expression_tree_walker(node, check_collation_walker, (void *) context);
	}
}

/*
 * is_ordering_op
 *
 * Return true if the operator is registered as an ordering operator
 * (amoppurpose = AMOP_ORDER) in any opfamily in pg_amop.
 */
static bool
is_ordering_op(Oid opno)
{
	CatCList   *catlist = SearchSysCacheList1(AMOPOPID,
											  ObjectIdGetDatum(opno));

	for (int i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	tp = &catlist->members[i]->tuple;
		Form_pg_amop amop = (Form_pg_amop) GETSTRUCT(tp);

		if (amop->amoppurpose == AMOP_ORDER)
		{
			ReleaseSysCacheList(catlist);
			return true;
		}
	}
	ReleaseSysCacheList(catlist);
	return false;
}

/*
 * has_plain_var_arg
 *
 * Return true if the OpExpr has at least one direct Var argument
 * (not wrapped in a function or other expression).
 *
 * Implicit coercions such as RelabelType (binary-compatible casts, e.g.
 * varchar -> text) are stripped before the check so that a column
 * reference that was implicitly cast to match the operator's input type
 * is still recognised as a plain Var.
 */
static bool
has_plain_var_arg(OpExpr *op)
{
	ListCell   *arg_lc;

	foreach(arg_lc, op->args)
	{
		Node	   *arg = strip_implicit_coercions(lfirst(arg_lc));

		if (IsA(arg, Var))
			return true;
	}
	return false;
}

/*
 * has_orderby_ordering_op
 *
 * Check if the query's ORDER BY uses ordering operators (amoppurpose =
 * AMOP_ORDER in pg_amop) that the PostgreSQL planner can safely optimize
 * with KNN-GiST index scans but ORCA cannot.
 *
 * Return true only when ALL ordering-operator expressions in ORDER BY
 * have at least one direct Var (column reference) argument.  Expressions
 * like "circle(p,1) <-> point(0,0)" wrap the column in a function,
 * which can cause "lossy distance functions are not supported in
 * index-only scans" errors in the planner.  In such cases we leave the
 * query for ORCA to handle via Seq Scan + Sort.
 */
bool
has_orderby_ordering_op(Query *query)
{
	ListCell   *lc;
	bool		found_ordering_op = false;

	if (query->sortClause == NIL)
		return false;

	foreach(lc, query->sortClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);
		Node	   *expr = (Node *) tle->expr;

		if (!IsA(expr, OpExpr))
			continue;

		OpExpr	   *opexpr = (OpExpr *) expr;

		if (!is_ordering_op(opexpr->opno))
			continue;

		/*
		 * Found an ordering operator.  Check that at least one argument is
		 * a plain Var.  If any ordering operator has only computed arguments
		 * (e.g., function calls wrapping columns), bail out immediately —
		 * falling back to the planner could produce lossy distance errors
		 * in index-only scans.
		 */
		found_ordering_op = true;

		if (!has_plain_var_arg(opexpr))
			return false;
	}

	return found_ordering_op;
}

