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
 * qs_types.h
 *		Per-node sample type collected by the pg_query_state plan-tree walker.
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/qs_types.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QS_TYPES_H
#define QS_TYPES_H

#include <stdint.h>
#include <stdbool.h>

#define GPSC_TRACE_ID_LEN 16
#define MAX_RELNAME_LEN 64

/*
 * parent_plan_node_id of a root node.  Not 0: plan_node_id counters start at 0
 * (setrefs.c, and GPORCA's GetNextPlanId), so 0 is always a real node.
 */
#define GPSC_NO_PARENT_PLAN_NODE_ID (-1)

/*
 * Negative segids reported in per-node samples and in the participant list.
 * Real segments report GpIdentity.segindex, which is >= 0.  On the coordinator
 * host it is -1 for the QD and for the entry-db QE alike, so the entry-db is
 * re-stamped: consumers key their dedup and their barrier on this value, and
 * two backends sharing it means one of them is silently dropped.
 */
#define GPSC_SEGID_QD       (-1)
#define GPSC_SEGID_ENTRY_DB (-2)

/*
 * Execution phase of a single plan node as observed at signal time.
 */
typedef enum QsNodeStatus
{
	QS_NODE_STATUS_UNSPECIFIED = 0,
	QS_NODE_STATUS_INITIALIZED = 1,  /* instrumentation allocated but not yet started */
	QS_NODE_STATUS_EXECUTING   = 2,  /* currently inside a tuple-fetch call */
	QS_NODE_STATUS_FINISHED    = 3   /* at least one full loop completed */
} QsNodeStatus;

typedef struct GpscNodeSample
{
	int32_t tmid;                    /* transaction/time id (gp_gettmid) */
	int32_t ssid;                    /* gp_session_id */
	int32_t ccnt;                    /* gp_command_count */
	int32_t plan_node_id;            /* Plan.plan_node_id */
	int32_t parent_plan_node_id;     /* parent's plan_node_id, or
									  * GPSC_NO_PARENT_PLAN_NODE_ID at the root */
	int32_t node_tag;                /* nodeTag(plan) */
	int32_t slice_id;                /* currentSliceId */
	int32_t segindex;                /* GpIdentity.segindex */
	int32_t pid;                     /* MyProcPid of the sampled backend */
	int32_t dbid;					 /* GpIdentity.dbid */
	int32_t relation_oid;            /* OID of scanned relation, or 0 */
	double  plan_rows;               /* optimizer row estimate */
	double  ntuples;                 /* Instrumentation.ntuples */
	double  tuplecount;              /* Instrumentation.tuplecount (in-progress loop) */
	double  nloops;                  /* Instrumentation.nloops */
	double  startup;                 /* Instrumentation.startup (seconds) */
	double  total;                   /* Instrumentation.total (seconds) */
	double  firsttuple;              /* Instrumentation.firsttuple (seconds) */
	uint64_t shared_blks_hit;
	uint64_t shared_blks_read;
	QsNodeStatus node_status;
	bool eof;						 /* Instrumentation.eof: node exhausted for
									  * the current cycle (last fetch returned no
									  * tuple).  Lets consumers tell a finished
									  * node from one still actively producing. */
	/*
	 * Spill, from the GP-specific Instrumentation fields. Reliable once the node
	 * is finalized; a mid-run snapshot is a lower bound (Sort/HashJoin populate
	 * these only at eager-free / explain-end).
	 */
	bool workfile_created;           /* Instrumentation.workfileCreated */
	int64_t workmem_used;            /* Instrumentation.workmemused (bytes) */
	int64_t workmem_wanted;          /* Instrumentation.workmemwanted (bytes); >0 == spilled */
	/*
	 * Derived rate fields, computed in signal_handler from the per-node rolling
	 * state (previous ntuples and sample time) rather than read from
	 * Instrumentation.  Zero on the node's first sample.
	 */
	double ntuples_delta;            /* tuples produced since the previous sample */
	double tuples_per_sec;           /* ntuples_delta divided by the sample interval */
	double time_since_init_sec;      /* seconds since the node's first sample */
	bool stalled;                    /* executing but produced no new tuples and not at eof */
	char relation_name[MAX_RELNAME_LEN];
} GpscNodeSample;

#endif /* QS_TYPES_H */
