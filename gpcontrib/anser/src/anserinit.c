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
int			gp_anser_max_info_size = 64 * 1024 * 1024 + 1024 * 1024;
int			gp_anser_timeout_ms = 1000;

static void anser_define_gucs(void);
static PlannedStmt *anser_planner(Query *parse, const char *query_string,
								  int cursorOptions, ParamListInfo boundParams,
								  OptimizerOptions *optimizer_options);

static planner_hook_type prev_planner_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

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
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = anser_executor_end;
	RegisterXactCallback(anser_xact_callback, NULL);

	/*
	 * Handle Anser notifies arriving from QEs on the dispatch connections.
	 * Installed unconditionally: it is inert until a segment sends one, and a
	 * QE that never dispatches never calls it.
	 */
	cdbdisp_notify_hook = AnserDispatchNotifyHandler;

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

	DefineCustomIntVariable("anser.max_info_size",
							"Sets the maximum byte size of one Anser information record.",
							"Per-record DSM payload cap for Anser information.  The default holds a full 64 MB bloom-filter bitset plus its serialized-part header.",
							&gp_anser_max_info_size,
							64 * 1024 * 1024 + 1024 * 1024, 1, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("anser.timeout_ms",
							"Sets how long Anser consumers wait for producer registration.",
							"After producer registration, consumers wait for data without this timeout and rely on query cancellation or channel cancellation.",
							&gp_anser_timeout_ms,
							1000, 0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS,
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
 * Drop the transport's per-query state.
 *
 * Nested executor runs (a function body, say) must not clear state the outer
 * query is still using, so only the outermost end resets.
 */
static void
anser_executor_end(QueryDesc *queryDesc)
{
	static int	nesting_level = 0;

	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorEnd_hook)
			prev_ExecutorEnd_hook(queryDesc);
		else
			standard_ExecutorEnd(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();

	if (nesting_level == 0)
		AnserSidebandResetAll();
}

static void
anser_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		AnserSidebandResetAll();
}
