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
 * Fixed positions in the ForeignScan's fdw_private list.  The
 * first two are written by GetForeignPlan and always present; the third is
 * appended by the QD in BeginForeignScan, so a QE tells "the QD spoke" from
 * "nobody did" by the list length alone.
 */
#define LANCE_FDW_PRIVATE_ATTRS		0	/* List of Integer: PostgreSQL attnums */
#define LANCE_FDW_PRIVATE_COLUMNS	1	/* List of String: Lance column names */
#define LANCE_FDW_PRIVATE_UNITS		2	/* List: the scan units below */

/*
 * One unit of work, as it travels inside the plan.  Version and fragment ids
 * are decimal strings because a PostgreSQL Integer node is 32 bits while both
 * of these are 64.
 */
typedef struct LanceScanUnits
{
	char	   *kind;			/* "fragment" in this block */
	char	   *uri;
	uint64		version;		/* the exact version the QD pinned */
	int			nsegments;		/* the width the planner built the locus for */
	uint64	   *ids;
	int			nids;
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
