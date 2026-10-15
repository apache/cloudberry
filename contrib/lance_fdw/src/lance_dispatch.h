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
 * lance_dispatch.h
 *	  What the QD puts in the plan and what each QE takes out of it.
 *
 * IDENTIFICATION
 *	  src/lance_dispatch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_DISPATCH_H
#define LANCE_DISPATCH_H

#include "lance_fdw.h"

#include "nodes/plannodes.h"

/*
 * Fixed positions in the ForeignScan's fdw_private list.  All
 * but the last are written by GetForeignPlan and always present; the units are
 * appended by the QD in BeginForeignScan, so a QE tells "the QD spoke" from
 * "nobody did" by the list length alone.  That is why UNITS stays last and new
 * slots are inserted before it: both length tests read this one constant.
 */
#define LANCE_FDW_PRIVATE_ATTRS		0	/* List of Integer: PostgreSQL attnums */
#define LANCE_FDW_PRIVATE_COLUMNS	1	/* List of String: Lance column names */
#define LANCE_FDW_PRIVATE_FILTER	2	/* String: Lance filter, "" if none */
#define LANCE_FDW_PRIVATE_FILTER_ATTRS	3	/* List of Integer: attnums the filter reads */
#define LANCE_FDW_PRIVATE_FILTER_COLUMNS 4	/* List of String: their Lance names */
#define LANCE_FDW_PRIVATE_TOPK		5	/* NIL, or the vector Top-K below */
#define LANCE_FDW_PRIVATE_UNITS		6	/* List: the scan units below */

/*
 * The TOPK slot: NIL for every scan that is not a
 * Top-K, otherwise these three, written by GetForeignPlan.  The query vector
 * itself is in fdw_exprs, because it may be a parameter.
 */
#define LANCE_TOPK_COLUMN			0	/* String: Lance column name */
#define LANCE_TOPK_METRIC			1	/* Integer: LanceVectorMetric */
#define LANCE_TOPK_K				2	/* String: k, decimal int64 */
#define LANCE_TOPK_NFIELDS			3

typedef enum LanceUnitKind
{
	LANCE_UNIT_FRAGMENT,		/* every QE reads its share of the fragments */
	LANCE_UNIT_NEAREST			/* one QE searches the whole dataset */
} LanceUnitKind;

/*
 * One unit of work, as it travels inside the plan.  Every field is a String
 * node - version and fragment ids because a PostgreSQL Integer node is 32
 * bits while both of these are 64, and the rest so that freeing a
 * cached plan's units only ever has one node type to free.
 */
typedef struct LanceScanUnits
{
	LanceUnitKind kind;
	char	   *uri;
	uint64		version;		/* the exact version the QD pinned */
	int			nsegments;		/* the width the planner built the locus for */

	/* LANCE_UNIT_FRAGMENT */
	uint64	   *ids;
	int			nids;

	/* LANCE_UNIT_NEAREST */
	int			target;			/* the one segindex that searches */
	float4	   *vector;			/* the query vector, bit for bit */
	int			dim;
	int			nprobes;		/* 0: leave it to Lance */
	int			refine_factor;	/* 0: leave it to Lance */
} LanceScanUnits;

/*
 * Hand the whole fragment list down to the QEs.  Writes into the plan node, so
 * that Cloudberry's plan dispatch carries it to every segment; on a re-executed
 * cached plan the previous list is replaced rather than appended to.
 */
extern void lance_dispatch_publish(ForeignScan *fsplan,
								   const char *uri,
								   uint64 version,
								   int nsegments,
								   const uint64 *ids,
								   int nids);

/*
 * Hand a vector Top-K search to one QE.  The query
 * vector travels as the bit patterns of its float4 elements, so the segment
 * searches with exactly the value the QD evaluated.
 */
extern void lance_dispatch_publish_nearest(ForeignScan *fsplan,
										   const char *uri,
										   uint64 version,
										   int nsegments,
										   int target,
										   const float4 *vector,
										   int dim,
										   int nprobes,
										   int refine_factor);

/*
 * The segment that searches: the session id, normalised into [0, nsegments)
 * the way the fragment split does, because C's % keeps the sign of its left
 * operand.
 */
extern int	lance_dispatch_search_target(int nsegments);

/* Did the QD publish units, and if so, what are they? */
extern bool lance_dispatch_read_units(const ForeignScan *fsplan,
									  LanceScanUnits *out);

/*
 * Keep this segment's share of `ids`, filtering in place and
 * returning how many are left.  Every QE of one statement computes the same
 * split from values the statement already shares, so the shares are disjoint
 * and complete without anyone coordinating.
 */
extern int	lance_dispatch_take_share(uint64 *ids, int nids, int nsegments);

/* Fragment ids as a readable list, for DEBUG1 and error detail. */
extern char *lance_dispatch_ids_string(const uint64 *ids, int nids);

#endif							/* LANCE_DISPATCH_H */
