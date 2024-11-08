/*-------------------------------------------------------------------------
 *
 * aqumv.h
 *	  prototypes for optimizer/plan/aqumv.c.
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 * 
 * Author: Zhang Mingli <avamingli@gmail.com>
 *
 * src/include/optimizer/aqumv.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AQUMV_H
#define AQUMV_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planmain.h"

typedef struct AqumvContextData {
    RelOptInfo      *current_rel;
    List            *raw_processed_tlist;
    Node            *raw_havingQual;
    void            *qp_extra;
    query_pathkeys_callback qp_callback;
} AqumvContextData;

typedef AqumvContextData *AqumvContext;

extern RelOptInfo* answer_query_using_materialized_views(PlannerInfo *root, AqumvContextData *aqumv_context);

/*
 * Adjust parse tree storaged in view's actions.
 * Query should be a simple query, ex:
 * select from a single table.
 */
extern void aqumv_adjust_simple_query(Query *viewQuery);

#endif   /* AQUMV_H */
