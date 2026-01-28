/*-------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 * Note that the primary entry points for planner.c are declared in
 * optimizer/optimizer.h, because they're intended to be called from
 * non-planner code.  Declarations here are meant for use by other
 * planner modules.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/pathnodes.h"
#include "nodes/plannerconfig.h"
#include "nodes/plannodes.h"
#include "optimizer/orcaopt.h"

/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
										   const char *query_string,
										   int cursorOptions,
										   ParamListInfo boundParams,
										   OptimizerOptions *optimizer_options);
extern PGDLLIMPORT planner_hook_type planner_hook;

/* Hook for plugins to get control when grouping_planner() plans upper rels */
typedef void (*create_upper_paths_hook_type) (PlannerInfo *root,
											  UpperRelationKind stage,
											  RelOptInfo *input_rel,
											  RelOptInfo *output_rel,
											  void *extra);
extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;


extern PlannedStmt *standard_planner(Query *parse, const char *query_string,
									 int cursorOptions,
									 ParamListInfo boundParams,
									 OptimizerOptions *optimizer_options);

extern PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse,
									 PlannerInfo *parent_root,
									 bool hasRecursion, double tuple_fraction,
									 PlannerConfig *config);

extern RowMarkType select_rowmark_type(RangeTblEntry *rte,
									   LockClauseStrength strength);

extern bool limit_needed(Query *parse);

extern void mark_partial_aggref(Aggref *agg, AggSplit aggsplit);

extern Path *get_cheapest_fractional_path(RelOptInfo *rel,
										  double tuple_fraction);

extern Expr *preprocess_phv_expression(PlannerInfo *root, Expr *expr);

extern bool optimizer_init;

extern void preprocess_qual_conditions(PlannerInfo *root, Node *jtnode);

#endif							/* PLANNER_H */
