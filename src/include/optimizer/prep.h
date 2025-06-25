/*-------------------------------------------------------------------------
 *
 * prep.h
 *	  prototypes for files in optimizer/prep/
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/prep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREP_H
#define PREP_H

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"


/*
 * prototypes for prepjointree.c
 */
extern void replace_empty_jointree(Query *parse);
extern void pull_up_sublinks(PlannerInfo *root);
extern void preprocess_function_rtes(PlannerInfo *root);
extern void pull_up_subqueries(PlannerInfo *root);
extern void flatten_simple_union_all(PlannerInfo *root);
extern void reduce_outer_joins(PlannerInfo *root);
extern void remove_useless_result_rtes(PlannerInfo *root);
extern Relids get_relids_in_jointree(Node *jtnode, bool include_joins);
extern Relids get_relids_for_join(Query *query, int joinrelid);

extern List *init_list_cteplaninfo(int numCtes);

/*
 * prototypes for preptlist.c
 */
extern void preprocess_targetlist(PlannerInfo *root);

extern List *extract_update_targetlist_colnos(List *tlist, bool reorder_resno);

extern PlanRowMark *get_plan_rowmark(List *rowmarks, Index rtindex);

/*
 * prototypes for prepagg.c
 */
extern void get_agg_clause_costs(PlannerInfo *root, AggSplit aggsplit,
								 AggClauseCosts *agg_costs);
extern void get_agg_clause_costs_multi_stage(PlannerInfo *root, PathTarget* partially_grouped_target, AggSplit aggsplit, AggClauseCosts *costs);
extern void compute_agg_clause_costs(PlannerInfo *root, AggSplit aggsplit,
								 AggClauseCosts *agg_costs);
extern void preprocess_aggrefs(PlannerInfo *root, Node *clause);

extern int find_compatible_agg(List *agginfos, Aggref *newagg, List **same_input_transnos);
extern int find_compatible_trans(List *aggtransinfos, bool shareable,
								  Oid aggtransfn, Oid aggtranstype,
								  int transtypeLen, bool transtypeByVal,
								  Oid aggcombinefn,
								  Oid aggserialfn, Oid aggdeserialfn,
								  Datum initValue, bool initValueIsNull,
								  List *transnos);
/*
 * prototypes for prepunion.c
 */
extern RelOptInfo *plan_set_operations(PlannerInfo *root);


#endif							/* PREP_H */
