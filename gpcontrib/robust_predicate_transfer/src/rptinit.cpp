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
 * rptinit.cpp
 *	  Module entry point: GUCs, the planner hook, and the SQL-visible
 *	  transfer-plan dump function.
 *
 * The extension rides on Anser (gpcontrib/anser), which must be preloaded
 * first: the AnserXxx entry points we call are resolved at load time from
 * the global symbol table (dfmgr loads modules RTLD_GLOBAL).
 *
 * The planner hook injects forward and backward filters and logs decisions at
 * DEBUG1 when rpt.debug is on. rpt_transfer_plan() exposes analysis without
 * injecting filters.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/src/rptinit.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "rpt.h"

extern "C"
{
#include "postgres.h"

#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "anserplan.h"
#include "rptlocal.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(rpt_transfer_plan);

void		_PG_init(void);
}


#include <new>

bool		rpt_enable = false;
bool		rpt_debug = false;
bool		rpt_backward = true;
int			rpt_max_backward_consumers = 4;
int			rpt_max_chain_depth = 4;

bool		rpt_forward = true;
/* Measured 2026-11-10 on one host with three primaries. */
double		rpt_forward_ms_per_mib_part = 3.5;
double		rpt_delivery_ms_per_mib = 0.8;
double		rpt_forward_max_exchange_ms = 10000.0;
bool		rpt_forward_total_cost = true;
double		rpt_forward_min_root_rows = 10000.0;
/* The RPT+ paper's lambda (its Algorithm 2 threshold). */
double		rpt_forward_max_selectivity = 0.35;

static planner_hook_type prev_planner_hook = NULL;

/* Set while rpt_transfer_plan() plans its query: the dump must describe the
 * uninjected plan, so the hook leaves it alone. */
static bool rpt_suppress_injection = false;

static void rpt_define_gucs(void);
static bool rpt_inplace_requested(void);
static PlannedStmt *rpt_planner(Query *parse, const char *query_string,
								int cursorOptions, ParamListInfo boundParams,
								OptimizerOptions *optimizer_options);

static void
rpt_append_injection_analysis(PlannedStmt *stmt, StringInfo out)
{
	std::string trace;

	(void) RptInject(stmt, false, &trace);
	RptPgCall([&]() { appendStringInfoString(out, trace.c_str()); });
}

/* Run analysis with C++ unwinding before reporting an error to C callers. */
static void
rpt_analyze(PlannedStmt *stmt, StringInfo out)
{
	bool pg_error = false;
	bool out_of_memory = false;
	char message[256] = "unknown C++ exception";

	try
	{
		RptTransferMgr mgr;

		RptCollectGraph(stmt, &mgr);
		if (mgr.vertices.empty())
			RptPgCall([&]() { appendStringInfoString(out, "no transfer regions\n"); });
		else
		{
			RptBuildClasses(&mgr);
			RptBuildTransferPlan(stmt, &mgr);
			RptDumpTransferPlan(stmt, &mgr, out);

			rpt_append_injection_analysis(stmt, out);
		}
		return;
	}
	catch (const RptPgError &)
	{
		pg_error = true;
	}
	catch (const std::bad_alloc &)
	{
		out_of_memory = true;
	}
	catch (const std::exception &e)
	{
		strlcpy(message, e.what(), sizeof(message));
	}
	catch (...)
	{
	}

	/* No live C++ objects (including exception objects) at these longjmps. */
	if (pg_error)
		PG_RE_THROW();
	if (out_of_memory)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory during RPT analysis")));
	ereport(ERROR, (errmsg("RPT analysis failed: %s", message)));
}

void
_PG_init(void)
{
	rpt_define_gucs();

	/*
	 * The local reducer nodes travel to the segments inside dispatched plans,
	 * so every backend that loads us must be able to resolve them by name.
	 */
	RptRegisterLocalMethods();

	/*
	 * Only a preloaded library can be relied on to have installed its hooks
	 * in every backend.  Loaded any other way the extension stays inert: the
	 * GUCs exist (so a stray setting is not an error) and the dump function
	 * works, but nothing is wired into planning.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	prev_planner_hook = planner_hook;
	planner_hook = rpt_planner;
	AnserSetInplaceRequest(rpt_inplace_requested);

	MarkGUCPrefixReserved("rpt");
}

/*
 * Segments ask this whether to build in-place filters when
 * anser.runtime_filter is off.  The coordinator leaves same-slice joins to
 * them (AnserApplyInplaceRuntimeFilters) whenever backward injection is on,
 * and both settings are synchronized to the segments.
 */
static bool
rpt_inplace_requested(void)
{
	return rpt_enable && rpt_backward;
}

/*
 * The extension's GUCs are "rpt.*"-qualified because they belong to a
 * loadable module; "anser" is already reserved by the extension we ride on.
 */
static void
rpt_define_gucs(void)
{
	DefineCustomBoolVariable("rpt.enable",
							 "Enables RPT+ runtime filters over Anser.",
							 "When disabled the planner hook is a pass-through.",
							 &rpt_enable,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("rpt.debug",
							 "Logs the RPT+ transfer plan of every analyzed query.",
							 "Dumps regions, equivalence classes and the forward/backward transfer plan at DEBUG1.",
							 &rpt_debug,
							 false,
							 PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("rpt.backward",
							 "Developer control for backward runtime-filter injection.",
							 "Only consumers proven safe to wait are injected.  Also read on segments, for in-place filters.",
							 &rpt_backward, true, PGC_USERSET,
							 GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("rpt.max_backward_consumers",
							"Caps backward consumers per equivalence class.",
							"QD-only; prefer consumers with larger estimated benefit.",
							&rpt_max_backward_consumers, 4, 1, INT_MAX,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("rpt.forward",
							 "Developer control for forward reducer injection.",
							 "QD-only. Needs anser.enable=on and anser.runtime_filter=off.",
							 &rpt_forward, true, PGC_USERSET, GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomRealVariable("rpt.forward_ms_per_mib_part",
							 "Estimated collection cost per MiB of bitset per producer.",
							 "Shared by forward and backward exchanges; QD-only.",
							 &rpt_forward_ms_per_mib_part, 3.5, 0.0, 1.0e6,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("rpt.delivery_ms_per_mib",
							 "Estimated delivery cost per consumer process per MiB.",
							 "Shared by forward and backward exchanges; QD-only.",
							 &rpt_delivery_ms_per_mib, 0.8, 0.0, 1.0e6,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("rpt.forward_total_cost",
							 "Rejects weak or unprofitable forward transfers.",
							 "Uses estimated reducer work and downstream savings; QD-only.",
							 &rpt_forward_total_cost, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("rpt.forward_max_exchange_ms",
							 "Skips a forward chain or backward class above this exchange cost.",
							 "-1 disables the limit; QD-only.",
							 &rpt_forward_max_exchange_ms, 10000.0, -1.0, 1.0e9,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("rpt.forward_min_root_rows",
							 "Skips a forward chain whose root scan is estimated smaller.",
							 "Compared with the root scan's plan_rows (per segment); QD-only.",
							 &rpt_forward_min_root_rows, 10000.0, 0.0, 1.0e15,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("rpt.forward_max_selectivity",
							 "Skips transfers expected to keep at least this fraction of target rows.",
							 "Shared selectivity threshold for forward roots and backward members; 1 disables the check; QD-only.",
							 &rpt_forward_max_selectivity, 0.35, 0.0, 1.0,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("rpt.max_chain_depth",
							"Sets the maximum forward-chain length RPT+ may plan.",
							"Global hops require publish/merge/deliver barriers, so chain depth is capped; the default is a placeholder until hop latency is measured.",
							&rpt_max_chain_depth,
							4, 1, INT_MAX,
							PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
							NULL, NULL, NULL);
}

/* The diagnostic hook must not turn an optional analysis failure into a
 * failed user query. No C++ owners live across this PostgreSQL catch: the
 * analysis boundary has already unwound them before rethrowing the error. */
static void
rpt_log_analysis(PlannedStmt *stmt)
{
	MemoryContext saved_context = CurrentMemoryContext;
	MemoryContext volatile analysis_context = NULL;

	PG_TRY();
	{
		analysis_context = AllocSetContextCreate(saved_context,
												"RPT analysis",
												ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(analysis_context);
		StringInfoData buf;

		initStringInfo(&buf);
		rpt_analyze(stmt, &buf);
		elog(DEBUG1, "rpt transfer plan:\n%s", buf.data);
		MemoryContextSwitchTo(saved_context);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		ErrorData *error = CopyErrorData();

		FlushErrorState();
		if (analysis_context != NULL)
		{
			MemoryContextDelete(analysis_context);
			analysis_context = NULL;
		}
		/* An optional pass must still honor the user's cancellation. */
		if (error->sqlerrcode == ERRCODE_QUERY_CANCELED)
			ReThrowError(error);
		ereport(WARNING, (errmsg("RPT analysis skipped: %s", error->message)));
		FreeErrorData(error);
	}
	PG_END_TRY();

	if (analysis_context != NULL)
		MemoryContextDelete(analysis_context);
}

/* Contain C++ exceptions before returning to the PostgreSQL error handler. */
static PlannedStmt *
rpt_build_injection(PlannedStmt *stmt)
{
	bool pg_error = false;
	bool out_of_memory = false;
	char message[256] = "unknown C++ exception";

	try
	{
		std::string trace;
		PlannedStmt *result = RptInject(stmt, true, rpt_debug ? &trace : nullptr);

		if (rpt_debug)
			RptPgCall([&]() { elog(DEBUG1, "rpt injection:\n%s", trace.c_str()); });
		return result;
	}
	catch (const RptPgError &)
	{
		pg_error = true;
	}
	catch (const std::bad_alloc &)
	{
		out_of_memory = true;
	}
	catch (const std::exception &e)
	{
		strlcpy(message, e.what(), sizeof(message));
	}
	catch (...)
	{
	}
	if (pg_error)
		PG_RE_THROW();
	if (out_of_memory)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory building the RPT runtime filters")));
	ereport(ERROR, (errmsg("RPT runtime filters failed: %s", message)));
	return NULL;
}

/* The original plan survives construction failure, reported as a WARNING;
 * only cancellation propagates. */
static PlannedStmt *
rpt_try_injection(PlannedStmt *stmt)
{
	MemoryContext saved_context = CurrentMemoryContext;
	MemoryContext volatile build_context = NULL;
	PlannedStmt *volatile result = stmt;

	PG_TRY();
	{
		build_context = AllocSetContextCreate(saved_context,
											 "RPT runtime filters",
											 ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(build_context);
		result = rpt_build_injection(stmt);
		MemoryContextSwitchTo(saved_context);
		/* Nothing injected: the scratch context holds only garbage. */
		if (result == stmt)
		{
			MemoryContextDelete(build_context);
			build_context = NULL;
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		ErrorData *error = CopyErrorData();

		FlushErrorState();
		if (build_context != NULL)
			MemoryContextDelete(build_context);
		if (error->sqlerrcode == ERRCODE_QUERY_CANCELED)
			ReThrowError(error);
		ereport(WARNING, (errmsg("RPT runtime filters skipped: %s", error->message)));
		FreeErrorData(error);
		result = stmt;
	}
	PG_END_TRY();

	return result;
}

/*
 * Plan as usual, then run the analysis over the finished tree.  Chaining
 * prev first means that when Anser is preloaded before us its injection pass
 * has already run on the tree we analyze, which is what we want: the region
 * discovery sees through Anser's own filter nodes.
 */
static PlannedStmt *
rpt_planner(Query *parse, const char *query_string, int cursorOptions,
			ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams, optimizer_options);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams, optimizer_options);

	if (rpt_enable && rpt_debug &&
		Gp_role == GP_ROLE_DISPATCH &&
		result != NULL && result->commandType == CMD_SELECT &&
		result->planTree != NULL)
		rpt_log_analysis(result);

	if (rpt_enable && (rpt_forward || rpt_backward) && !rpt_suppress_injection &&
		Gp_role == GP_ROLE_DISPATCH &&
		result != NULL && result->commandType == CMD_SELECT &&
		result->planTree != NULL && result->intoClause == NULL &&
		result->copyIntoClause == NULL && result->refreshClause == NULL)
		result = rpt_try_injection(result);

	return result;
}

/*
 * rpt_transfer_plan(query text) -> text
 *
 * Plan the query and dump its RPT+ transfer plan.  Analysis only: the plan
 * is discarded afterwards.  This is the regression-test driver.
 */
static Datum
rpt_transfer_plan_impl(FunctionCallInfo fcinfo)
{
	char	   *query_string = text_to_cstring(PG_GETARG_TEXT_PP(0));
	List	   *raw_parsetree_list;
	List	   *query_list;
	Query	   *query;
	PlannedStmt *stmt;
	StringInfoData buf;

	raw_parsetree_list = pg_parse_query(query_string);
	if (list_length(raw_parsetree_list) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("rpt_transfer_plan expects a single statement")));

	query_list = pg_analyze_and_rewrite_fixedparams(
										 (RawStmt *) linitial(raw_parsetree_list),
										 query_string, NULL, 0, NULL);
	if (list_length(query_list) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("rpt_transfer_plan expects a single query")));

	query = (Query *) linitial(query_list);
	if (query->commandType != CMD_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("rpt_transfer_plan supports only SELECT")));

	/*
	 * pg_plan_query runs the hook chain, so with anser preloaded and its
	 * runtime_filter GUC on the tree we analyze already carries Anser's
	 * injected nodes -- exactly what production planning will hand us.
	 */
	/* The dump describes the uninjected plan; the forward decisions are
	 * appended by a dry run inside rpt_analyze(). */
	rpt_suppress_injection = true;
	PG_TRY();
	{
		stmt = pg_plan_query(query, query_string, 0, NULL);
	}
	PG_FINALLY();
	{
		rpt_suppress_injection = false;
	}
	PG_END_TRY();

	initStringInfo(&buf);
	rpt_analyze(stmt, &buf);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* The SQL entry point also contains exceptions raised by future UDF code. */
Datum
rpt_transfer_plan(PG_FUNCTION_ARGS)
{
	bool pg_error = false;
	bool out_of_memory = false;
	char message[256] = "unknown C++ exception";

	try
	{
		return rpt_transfer_plan_impl(fcinfo);
	}
	catch (const RptPgError &)
	{
		pg_error = true;
	}
	catch (const std::bad_alloc &)
	{
		out_of_memory = true;
	}
	catch (const std::exception &e)
	{
		strlcpy(message, e.what(), sizeof(message));
	}
	catch (...)
	{
	}
	if (pg_error)
		PG_RE_THROW();
	if (out_of_memory)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("out of memory while planning RPT query")));
	ereport(ERROR, (errmsg("RPT query planning failed: %s", message)));
	PG_RETURN_NULL();
}
