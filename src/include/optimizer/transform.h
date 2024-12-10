/*-------------------------------------------------------------------------
 *
 * transform.h
 * 	Query transformation routines
 *
 * Portions Copyright (c) 2011-2013, EMC Corporation
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/optimizer/transform.h
 * Author: Siva
 *-------------------------------------------------------------------------
 */

#ifndef TRANSFORM_H
#define TRANSFORM_H

#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"

extern Query *normalize_query(Query *query);

extern Query *remove_distinct_sort_clause(Query *query);

extern bool query_has_srf(Query *query);

extern bool tlist_has_srf(const Query *query);

#endif /* TRANSFORM_H */
