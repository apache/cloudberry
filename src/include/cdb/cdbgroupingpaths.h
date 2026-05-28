/*-------------------------------------------------------------------------
 *
 * cdbgroupingpaths.h
 *	  prototypes for cdbgroupingpaths.c.
 *
 *
 * Portions Copyright (c) 2019-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbgroupingpaths.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGROUPINGPATHS_H
#define CDBGROUPINGPATHS_H

#include "nodes/pathnodes.h"

extern void cdb_create_multistage_grouping_paths(PlannerInfo *root,
												 RelOptInfo *input_rel,
												 RelOptInfo *output_rel,
												 PathTarget *target,
												 PathTarget *partial_grouping_target,
												 List *havingQual,
												 double dNumGroupsTotal,
												 const AggClauseCosts *agg_costs,
												 const AggClauseCosts *agg_partial_costs,
												 const AggClauseCosts *agg_final_costs,
												 List *rollups,
												 List *new_rollups,
												 AggStrategy strat,
												 List *partial_pathlist);


extern void cdb_create_twostage_distinct_paths(PlannerInfo *root,
											   RelOptInfo *input_rel,
											   RelOptInfo *output_rel,
											   PathTarget *target,
											   double dNumGroupsTotal);

extern Path *cdb_prepare_path_for_sorted_agg(PlannerInfo *root,
											 bool is_sorted,
											 int presorted_keys,
											 RelOptInfo *rel,
											 Path *subpath,
											 PathTarget *target,
											 List *group_pathkeys,
											 double limit_tuples,
											 List *groupClause,
											 List *rollups);
extern Path *cdb_prepare_path_for_hashed_agg(PlannerInfo *root,
											 Path *subpath,
											 PathTarget *target,
											 List *groupClause,
											 List *rollups);
extern List *get_common_group_tles(PathTarget *target,
								   List *groupClause,
								   List *rollups);
extern CdbPathLocus choose_grouping_locus(PlannerInfo *root, Path *path,
										  List *group_tles,
										  bool *need_redistribute_p);

extern Path *
cdb_create_pre_window_agg_path(PlannerInfo *root,
								bool is_sorted,
								int presorted_keys,
								RelOptInfo *rel,
								Path *subpath,
								PathTarget *target,
								List *group_pathkeys,
								PathTarget *window_target,
								List *window_functions,
								WindowClause *wc);

#endif   /* CDBGROUPINGPATHS_H */
