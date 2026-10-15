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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserinit.c
 *	  Module entry point: GUCs and the core hooks Anser hangs off.
 *
 * Anser is a shared_preload_libraries extension.  Everything it needs from the
 * server is reached through an existing extensibility point:
 *
 *	 planner_hook               runtime-filter injection
 *	 RegisterCustomScanMethods  the injected plan nodes
 *	 cdbdisp_notify_hook        parts arriving from segments
 *	 ExecutorRun/Finish_hook    knowing whether a query is still executing
 *	 ExecutorEnd_hook           dropping a query's channels
 *
 * It must be preloaded, because a segment backend deserializing a dispatched
 * plan has no opportunity to load the library first.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserinit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "anser.h"
#include "anserplan.h"
#include "ansersideband.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

bool		gp_anser_enable = false;
bool		gp_anser_runtime_filter = false;
bool		gp_anser_debug = false;
int			gp_anser_max_info_size = 64 * 1024 * 1024 + 1024 * 1024;
int			gp_anser_timeout_ms = 100000;

static void anser_define_gucs(void);
static PlannedStmt *anser_planner(Query *parse, const char *query_string,
								  int cursorOptions, ParamListInfo boundParams,
								  OptimizerOptions *optimizer_options);

static planner_hook_type prev_planner_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static cdbdisp_notify_hook_type prev_cdbdisp_notify_hook = NULL;

/*
 * How many queries are executing right now.  A query's channels may only be
 * dropped when nothing is left running, and the count has to be taken over
 * execution rather than over ExecutorEnd: a statement run through SPI from a
 * function body goes through its whole Start/Run/End inside the *outer*
 * query's Run, so by the time the inner ExecutorEnd fires the outer one has
 * not entered its own End yet.  Counting ends alone makes that inner End look
 * outermost, and dropping channels there takes the running query's filters
 * with it -- the exchange still happens, but every consumer then fails open.
 *
 * Nested execution is nested inside Run (and inside Finish, where AFTER
 * triggers run), which is why those are the two that count.  ExecutorEnd only
 * reads the result: a query's own End always runs after its own Run returned,
 * so an outermost query still sees zero.
 */
static int	anser_exec_nesting = 0;

static bool anser_notify_hook(struct CdbDispatchResult *dispatchResult,
							  struct pgNotify *notify);
static void anser_executor_run(QueryDesc *queryDesc, ScanDirection direction,
							   uint64 count, bool execute_once);
static void anser_executor_finish(QueryDesc *queryDesc);
static void anser_executor_end(QueryDesc *queryDesc);
static void anser_xact_callback(XactEvent event, void *arg);

void
_PG_init(void)
{
	anser_define_gucs();

	/*
	 * Only a preloaded library can be relied on to have installed its hooks in
	 * every backend.  Loaded any other way, Anser stays inert: the GUCs exist
	 * (so a stray setting is not an error) but nothing is wired up.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	prev_planner_hook = planner_hook;
	planner_hook = anser_planner;

	/*
	 * Channels live in backend memory on both ends, so they need a point to be
	 * dropped.  ExecutorEnd covers the normal path; the transaction callback
	 * catches queries that end by erroring.
	 */
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = anser_executor_run;
	prev_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = anser_executor_finish;
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = anser_executor_end;
	RegisterXactCallback(anser_xact_callback, NULL);

	/*
	 * Handle Anser notifies arriving from QEs on the dispatch connections.
	 * Installed unconditionally: it is inert until a segment sends one, and a
	 * QE that never dispatches never calls it.
	 */
	prev_cdbdisp_notify_hook = cdbdisp_notify_hook;
	cdbdisp_notify_hook = anser_notify_hook;

	/*
	 * The producer and consumer nodes travel to the segments inside dispatched
	 * plans, so every backend must be able to resolve their CustomScan methods
	 * by name.  Registering here covers QD and QE alike.
	 */
	AnserRegisterRuntimeFilterMethods();
}

/*
 * The subsystem's GUCs.  All are "anser.*"-qualified because they belong to a
 * loadable module; the C variables keep their gp_anser_ names.
 */
static void
anser_define_gucs(void)
{
	DefineCustomBoolVariable("anser.enable",
							 "Enables the Anser adaptive information sharing subsystem.",
							 "When disabled, the plan pass never injects anything and no filters are exchanged.",
							 &gp_anser_enable,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("anser.runtime_filter",
							 "Enables injection of Anser runtime bloom filters into plans.",
							 "Requires anser.enable; the plan pass is a no-op otherwise.",
							 &gp_anser_runtime_filter,
							 false,
							 PGC_USERSET,
							 GUC_EXPLAIN,
							 NULL, NULL, NULL);

	/*
	 * GUC_GPDB_NEED_SYNC on everything a QE reads.  A custom GUC gets
	 * GUC_GPDB_NO_SYNC by default -- gpdb_assign_sync_flag() says so in as many
	 * words for "the third-part libraries gucs introduced by customer"
	 * (guc_gp.c) -- and a QE created after the SET then never receives the
	 * value.  The QEs alive at the time do get it, because the SET itself is
	 * dispatched to them as a command, so the setting appears to work right up
	 * until a query needs a wider gang and the new processes fall back to the
	 * compiled-in default.  That is not a setting anyone can reason about.
	 */
	DefineCustomBoolVariable("anser.debug",
							 "Logs each step of the Anser filter exchange.",
							 "Traces publish, merge, delivery and receive in the log of the process each happens in.",
							 &gp_anser_debug,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("anser.max_info_size",
							"Sets the maximum byte size of one Anser information record.",
							"Caps the serialized payload a channel may carry, and with it the effective bloom-filter size.  The default holds a full 64 MB bitset plus its serialized-part header.",
							&gp_anser_max_info_size,
							64 * 1024 * 1024 + 1024 * 1024, 1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("anser.timeout_ms",
							"Sets how long an Anser consumer waits for its filter.",
							"On expiry the consumer runs unfiltered.  The deadline matters because a producer that gets squelched never publishes at all.  The default is generous because delivery is serial: the coordinator writes the merged filter to one subscriber at a time, so the last of a wide slice waits for all the writes before it.",
							&gp_anser_timeout_ms,
							100000, 0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS | GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("anser");
}

/*
 * Plan the query as usual, then hand the finished tree to the runtime-filter
 * pass.  Wrapping the hook this way covers both optimizers, because ORCA is
 * dispatched from inside standard_planner().
 */
static PlannedStmt *
anser_planner(Query *parse, const char *query_string, int cursorOptions,
			  ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams, optimizer_options);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams, optimizer_options);

	AnserApplyRuntimeFilters(result);

	return result;
}

/*
 * Dispatch a QE notify to Anser, then to whoever held the hook before us.
 *
 * cdbdisp_notify_hook is a single pointer, so an extension that overwrites it
 * silently swallows every notify the other one was waiting for.  Anser's
 * handler already declines anything that is not addressed to it -- it returns
 * false for any channel other than "anser_rf" -- so all that is missing is the
 * fallthrough.  Returning false from here means nobody claimed the notify and
 * the dispatcher should handle it itself.
 */
static bool
anser_notify_hook(struct CdbDispatchResult *dispatchResult,
				  struct pgNotify *notify)
{
	if (AnserDispatchNotifyHandler(dispatchResult, notify))
		return true;

	if (prev_cdbdisp_notify_hook)
		return prev_cdbdisp_notify_hook(dispatchResult, notify);

	return false;
}

/* Count a query as executing for as long as it is running. */
static void
anser_executor_run(QueryDesc *queryDesc, ScanDirection direction,
				   uint64 count, bool execute_once)
{
	anser_exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorRun_hook)
			prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		anser_exec_nesting--;
	}
	PG_END_TRY();
}

/* The same, for the phase that runs AFTER triggers. */
static void
anser_executor_finish(QueryDesc *queryDesc)
{
	anser_exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorFinish_hook)
			prev_ExecutorFinish_hook(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		anser_exec_nesting--;
	}
	PG_END_TRY();
}

/*
 * Drop the transport's per-query state, once nothing is executing.
 *
 * See anser_exec_nesting: a query ending while another is still running is an
 * inner statement of that query, and its channels are not the ones to drop.
 */
static void
anser_executor_end(QueryDesc *queryDesc)
{
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (anser_exec_nesting == 0)
		AnserSidebandResetAll();
}

static void
anser_xact_callback(XactEvent event, void *arg)
{
	/*
	 * Deliberately does not touch anser_exec_nesting.  PG_FINALLY unwinds it
	 * on the error path, and XACT_EVENT_ABORT is only reached from
	 * AbortTransaction() at a statement boundary, by which point every
	 * executor frame is gone -- so there is nothing here to correct.  Forcing
	 * it to zero would instead risk driving it negative if a frame ever did
	 * outlive the callback, and a negative count never compares equal to zero
	 * again: the resets below would stop happening for the rest of the
	 * session.  auto_explain and pg_stat_statements keep their counters the
	 * same way, on PG_FINALLY alone.
	 */
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		AnserSidebandResetAll();
}
