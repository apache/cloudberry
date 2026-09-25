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
 * pg_query_state.h
 *		Public API for the pg_query_state signal-dispatch layer.
 *
 * This header is included by the C extension entry point (gp_stats_collector.c),
 * which only has to call pg_qs_init(); everything else the module needs it
 * registers itself.  Keep it C-compatible: no C++ types, wrapped in extern "C".
 *
 * Portions derived from pg_query_state
 * (https://github.com/postgrespro/pg_query_state), under the PostgreSQL
 * License:
 *   Portions Copyright (c) 2016-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/pg_query_state.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PG_QUERY_STATE_H__
#define __PG_QUERY_STATE_H__
#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "commands/explain.h"
#include "nodes/pg_list.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "cdb/cdbdispatchresult.h"
#include "qs_types.h"

/* Shared memory queue capacity for passing query-state messages. */
#define QUEUE_SIZE          (64 * 1024)

/* Maximum single chunk size when splitting a message across shm_mq sends. */
#define MSG_MAX_SIZE        (4 * 1024)

/* Delay between shm_mq send retries, in microseconds (100 ms). */
#define WRITING_DELAY       (100 * 1000)

/* Maximum number of send retries before giving up. */
#define NUM_OF_ATTEMPTS     6

/* Bitmask flags for caller-side warnings embedded in shm_mq_msg.warnings. */
#define TIMING_OFF_WARNING 1
#define BUFFERS_OFF_WARNING 2

/* Unique key that identifies our shm_toc segment. */
#define PG_QS_MODULE_KEY    0xCA94B108

/* Table-of-contents slot indices within the shm_toc segment. */
#define PG_QS_RCV_KEY       0
#define PG_QS_SND_KEY       1

/*
 * Timeouts for shm_mq operations.
 * The receive timeout must exceed the send timeout so that waiting workers
 * always give up before the polling process stops listening.
 */
#define MAX_RCV_TIMEOUT     2000  /* ms */
#define MAX_SND_TIMEOUT     1000  /* ms */

/*
 * Sleep between partial-receive retries (SHM_MQ_WOULD_BLOCK case).
 * Must be less than MAX_RCV_TIMEOUT.
 */
#define PART_RCV_DELAY      100   /* ms */

/*
 * Minimum interval between coordinator plan-doc pushes for the same query.
 * SendQueryState() re-sends the ExplainPrintPlan document only after this
 * interval elapses, so repeated polls of a long-running query do not resend
 * the (unchanging) plan on every signal.
 */
#define PLAN_DOC_RESEND_INTERVAL_MS (2 * 60 * 1000)

/*
 * Status codes returned by the signal handler to describe the state of the
 * queried backend.
 */
typedef enum
{
	QUERY_NOT_RUNNING,   /* backend is idle or has no active QueryDesc */
	STAT_DISABLED,       /* pg_query_state.enable = false */
	QS_RETURNED,         /* handler successfully collected and sent stats */
	WRONG_ROLE           /* target is a QE, not the QD: only GP_ROLE_DISPATCH
						  * knows the participant list, so no other backend can
						  * answer BackendInfoPollReason */
} PG_QS_RequestResult;

/*
 * Wire format for a query-state reply message transmitted through shm_mq.
 * The variable-length `stack` field carries sequentially laid out text frames,
 * one per stack depth.
 */
typedef struct
{
	int reqid;
	int length;           /* total message size including flexible array */
	PGPROC *proc;
	PG_QS_RequestResult result_code;
	int warnings;         /* bitmask of TIMING_OFF_WARNING / BUFFERS_OFF_WARNING */
	int stack_depth;
	char stack[FLEXIBLE_ARRAY_MEMBER];
} shm_mq_msg;

#define BASE_SIZEOF_SHM_MQ_MSG (offsetof(shm_mq_msg, stack_depth))

/*
 * Compact identifier for a backend running on a specific segment.
 */
typedef struct
{
	int32 segid;
	int32 pid;
} gp_segment_pid;

/*
 * Wire format for the backend-info (CDB segment PIDs) reply.
 */
typedef struct
{
	int reqid;
	int length;
	PGPROC *proc;
	PG_QS_RequestResult result_code;
	int number;
	gp_segment_pid pids[FLEXIBLE_ARRAY_MEMBER];
} backend_info;

#define BASE_SIZEOF_GP_BACKEND_INFO (offsetof(backend_info, pids))

/*
 * Parameters passed through shared memory from the requestor to the signal
 * handler, controlling what the handler should collect and how.
 */
typedef struct
{
	ProcSignalReason reason;
	int     reqid;
} pg_qs_params;

/*
 * Context threaded through the plan-tree walker.
 * per_node_stats accumulates one GpscNodeSample per visited node.
 */
typedef struct QsWalkerContext
{
	List    *per_node_stats;
	int32_t  parent_plan_node_id;
	int32_t  slice_id;  /* slice owning the node being visited */
	bool 	 finalize; /* true only in pg_qs_executor end */
	TimestampTz ts_now;
	int32_t  tmid;
} QsWalkerContext;

/*
 * Result code for the chunked shm_mq send helper.
 */
typedef enum
{
	MSG_BY_PARTS_SUCCEEDED,
	MSG_BY_PARTS_FAILED
} msg_by_parts_result;

extern bool           pg_qs_enable;
extern bool           pg_qs_timing;
extern bool           pg_qs_buffers;
extern List          *QueryDescStack;
extern pg_qs_params  *params;
extern shm_mq        *mq;
extern uint32        *mq_req_id;

/*
 * Per-backend trace_id slots, indexed by BackendId (1..MaxBackends; slot 0 for
 * InvalidBackendId is unused).  The single shared `params` cannot carry the
 * trace across an asynchronous ProcSignal: two concurrent collections would
 * clobber it and a signaled backend would stamp its batch with the wrong
 * trace.  The dispatcher writes qs_trace_slots[target->backendId] before
 * signalling; the signaled backend reads qs_trace_slots[MyBackendId].  The slot
 * is keyed by backend, not by collection, so two overlapping collections of the
 * same backend still share one slot -- the caller must not poll one pid twice
 * concurrently.
 */
extern char (*qs_trace_slots)[GPSC_TRACE_ID_LEN];

extern ProcSignalReason QueryStatePollReason;
extern ProcSignalReason BackendInfoPollReason;

/*
 * pg_qs_init -- register shared memory, custom signals, GUC variables and the
 * executor hooks.  Must be called from _PG_init() during
 * shared_preload_libraries processing.
 */
extern void pg_qs_init(void);

/* Custom signal handlers registered with RegisterCustomProcSignalHandler. */
extern void SendQueryState(void);
extern void SendCdbComponents(void);

typedef void (*qs_planstate_walker_callback)(PlanState *, QsWalkerContext *);
extern QueryDesc *get_toppest_query(void);
extern void gpsc_reset_node_roll_state(void);

#ifdef __cplusplus
}
#endif
#endif /* __PG_QUERY_STATE_H__ */
