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

/*
 * Type of a single plan node, as reported in a per-node sample.
 *
 * These values belong to the wire protocol (yagpcc::PlanNodeType in
 * protos/yagpcc_plan.proto) and are deliberately NOT PostgreSQL NodeTag
 * values.  NodeTag numbering is internal and unstable across major versions:
 * PG16 generates it with gen_node_support.pl (src/include/nodes/nodetags.h)
 * and renumbered every tag relative to PG14 -- T_SeqScan moved from 27 to 395
 * -- so shipping nodeTag(plan) raw made every node resolve to "unknown" on a
 * receiver carrying a table for the other version.  qs_map_node_type()
 * translates.
 *
 * This enum must stay numerically identical on every branch, so that one
 * receiver can serve them all.  SPLIT_MERGE has no PostgreSQL 14 equivalent
 * and is never emitted here, but its value stays reserved.
 *
 * Append new members; never renumber, and keep in step with PlanNodeType and
 * with map_node_type() in PlanNodeEmitter.cpp.
 */
typedef enum QsPlanNodeType
{
	QS_PLAN_NODE_TYPE_UNSPECIFIED               = 0,

	/* control nodes */
	QS_PLAN_NODE_TYPE_RESULT                    = 1,
	QS_PLAN_NODE_TYPE_PROJECT_SET               = 2,
	QS_PLAN_NODE_TYPE_MODIFY_TABLE              = 3,
	QS_PLAN_NODE_TYPE_APPEND                    = 4,
	QS_PLAN_NODE_TYPE_MERGE_APPEND              = 5,
	QS_PLAN_NODE_TYPE_RECURSIVE_UNION           = 6,
	QS_PLAN_NODE_TYPE_BITMAP_AND                = 7,
	QS_PLAN_NODE_TYPE_BITMAP_OR                 = 8,

	/* scans */
	QS_PLAN_NODE_TYPE_SEQ_SCAN                  = 9,
	QS_PLAN_NODE_TYPE_SAMPLE_SCAN               = 10,
	QS_PLAN_NODE_TYPE_INDEX_SCAN                = 11,
	QS_PLAN_NODE_TYPE_INDEX_ONLY_SCAN           = 12,
	QS_PLAN_NODE_TYPE_BITMAP_INDEX_SCAN         = 13,
	QS_PLAN_NODE_TYPE_BITMAP_HEAP_SCAN          = 14,
	QS_PLAN_NODE_TYPE_TID_SCAN                  = 15,
	QS_PLAN_NODE_TYPE_TID_RANGE_SCAN            = 16,
	QS_PLAN_NODE_TYPE_SUBQUERY_SCAN             = 17,
	QS_PLAN_NODE_TYPE_FUNCTION_SCAN             = 18,
	QS_PLAN_NODE_TYPE_TABLE_FUNC_SCAN           = 19,
	QS_PLAN_NODE_TYPE_VALUES_SCAN               = 20,
	QS_PLAN_NODE_TYPE_CTE_SCAN                  = 21,
	QS_PLAN_NODE_TYPE_NAMED_TUPLESTORE_SCAN     = 22,
	QS_PLAN_NODE_TYPE_WORK_TABLE_SCAN           = 23,
	QS_PLAN_NODE_TYPE_FOREIGN_SCAN              = 24,
	QS_PLAN_NODE_TYPE_CUSTOM_SCAN               = 25,

	/* joins */
	QS_PLAN_NODE_TYPE_NEST_LOOP                 = 26,
	QS_PLAN_NODE_TYPE_MERGE_JOIN                = 27,
	QS_PLAN_NODE_TYPE_HASH_JOIN                 = 28,

	/* materialization, ordering, grouping */
	QS_PLAN_NODE_TYPE_MATERIAL                  = 29,
	QS_PLAN_NODE_TYPE_MEMOIZE                   = 30,
	QS_PLAN_NODE_TYPE_SORT                      = 31,
	QS_PLAN_NODE_TYPE_INCREMENTAL_SORT          = 32,
	QS_PLAN_NODE_TYPE_GROUP                     = 33,
	QS_PLAN_NODE_TYPE_AGG                       = 34,
	QS_PLAN_NODE_TYPE_WINDOW_AGG                = 35,
	QS_PLAN_NODE_TYPE_UNIQUE                    = 36,
	QS_PLAN_NODE_TYPE_HASH                      = 37,
	QS_PLAN_NODE_TYPE_SET_OP                    = 38,
	QS_PLAN_NODE_TYPE_LOCK_ROWS                 = 39,
	QS_PLAN_NODE_TYPE_LIMIT                     = 40,

	/* intra-node parallelism */
	QS_PLAN_NODE_TYPE_GATHER                    = 41,
	QS_PLAN_NODE_TYPE_GATHER_MERGE              = 42,

	/* Cloudberry MPP nodes */
	QS_PLAN_NODE_TYPE_MOTION                    = 43,
	QS_PLAN_NODE_TYPE_SEQUENCE                  = 44,
	QS_PLAN_NODE_TYPE_SHARE_INPUT_SCAN          = 45,
	QS_PLAN_NODE_TYPE_SPLIT_UPDATE              = 46,
	QS_PLAN_NODE_TYPE_SPLIT_MERGE               = 47,
	QS_PLAN_NODE_TYPE_ASSERT_OP                 = 48,
	QS_PLAN_NODE_TYPE_PARTITION_SELECTOR        = 49,
	QS_PLAN_NODE_TYPE_RUNTIME_FILTER            = 50,
	QS_PLAN_NODE_TYPE_TUPLE_SPLIT               = 51,
	QS_PLAN_NODE_TYPE_TABLE_FUNCTION_SCAN       = 52,
	QS_PLAN_NODE_TYPE_DYNAMIC_SEQ_SCAN          = 53,
	QS_PLAN_NODE_TYPE_DYNAMIC_INDEX_SCAN        = 54,
	QS_PLAN_NODE_TYPE_DYNAMIC_INDEX_ONLY_SCAN   = 55,
	QS_PLAN_NODE_TYPE_DYNAMIC_BITMAP_INDEX_SCAN = 56,
	QS_PLAN_NODE_TYPE_DYNAMIC_BITMAP_HEAP_SCAN  = 57,
	QS_PLAN_NODE_TYPE_DYNAMIC_FOREIGN_SCAN      = 58
} QsPlanNodeType;

typedef struct GpscNodeSample
{
	int32_t tmid;                    /* transaction/time id (gp_gettmid) */
	int32_t ssid;                    /* gp_session_id */
	int32_t ccnt;                    /* gp_command_count */
	int32_t plan_node_id;            /* Plan.plan_node_id */
	int32_t parent_plan_node_id;     /* parent's plan_node_id, or
									  * GPSC_NO_PARENT_PLAN_NODE_ID at the root */
	QsPlanNodeType node_type;        /* qs_map_node_type(nodeTag(plan)); a
									  * protocol value, not a raw NodeTag */
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
