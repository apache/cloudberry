/*
 * FIXME: This file will be deleted in the future
 */
#include "hook/hook.h"

#include "libpq-fe.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbdisp_query.h"
#include "catalog/oid_dispatch.h"
#include "access/xact.h"
#include "executor/execUtils.h"
#include "utils/snapmgr.h"
#include "cdb/memquota.h"
#include "cdb/cdbvars.h"
#include "nodes/print.h"
#include "commands/trigger.h"
#include "cdb/cdbmotion.h"
#include "cdb/ml_ipc.h"
#include "utils/metrics_utils.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "foreign/fdwapi.h"
#include "executor/nodeHash.h"
#include "executor/nodeSubplan.h"
#include "cdb/cdbexplain.h"
#include "utils/guc_tables.h"
#include "jit/jit.h"
#include "storage/bufmgr.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbconn.h"
#include <math.h>
#include "utils/ruleutils.h"
#include "utils/json.h"
#include "cdb/cdbdisp.h"
#include "utils/queryjumble.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "nodes/extensible.h"
#include "cdb/cdbendpoint.h"
#include "vecnodes/nodes.h"

#include "vecexecutor/executor.h"


/* Convert bytes into kilobytes */
#define kb(x) (floor((x + 1023.0) / 1024.0))

/* OR-able flags for ExplainXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4

static void VecExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc);
static void VecExplainOpenGroup(const char *objtype, const char *labelname,
								bool labeled, ExplainState *es);
static void VecExplainCloseGroup(const char *objtype, const char *labelname,
								 bool labeled, ExplainState *es);
static void
VecExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es,
				  const char *queryString, ParamListInfo params,
				  QueryEnvironment *queryEnv, const instr_time *planduration,
				  const BufferUsage *bufusage,
				  int cursorOptions);
static void VecExplainNode(PlanState *planstate, List *ancestors,
						   const char *relationship, const char *plan_name,
						   ExplainState *es);
static void VecExplainPrintSettings(ExplainState *es, PlanGenerator planGen);
static void VecExplainSubPlans(List *plans, List *ancestors,
							   const char *relationship, ExplainState *es,
							   SliceTable *sliceTable);
static void ExplainFlushWorkersState(ExplainState *es);
static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainMemberNodes(PlanState **planstates, int nplans,
							   List *ancestors, ExplainState *es);
static void ExplainMissingMembers(int nplans, int nchildren, ExplainState *es);
static void ExplainPrintJIT(ExplainState *es, int jit_flags,
							JitInstrumentation *ji);
static void ExplainOpenWorker(int n, ExplainState *es);
static void ExplainCloseWorker(int n, ExplainState *es);
static void ExplainSaveGroup(ExplainState *es, int depth, int *state_save);
static void
ExplainRestoreGroup(ExplainState *es, int depth, int *state_save);
static void
ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
						 bool labeled, int depth, ExplainState *es);
static void ExplainPropertyStringInfo(const char *qlabel, ExplainState *es,
									  const char *fmt,...)
									  pg_attribute_printf(3, 4);

static void ExplainIndentText(ExplainState *es);
static bool VecExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used);
static ExplainWorkersState * ExplainCreateWorkersState(int num_workers);
static void ExplainYAMLLineStarting(ExplainState *es);
static void ExplainJSONLineEnding(ExplainState *es);
static void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es);


static double elapsed_time(instr_time *starttime);

static void show_plan_tlist(PlanState *planstate, List *ancestors,
							ExplainState *es);
static void show_expression(Node *node, const char *qlabel,
							PlanState *planstate, List *ancestors,
							bool useprefix, ExplainState *es);
static void show_qual(List *qual, const char *qlabel,
					  PlanState *planstate, List *ancestors,
					  bool useprefix, ExplainState *es);
static void show_scan_qual(List *qual, const char *qlabel,
						   PlanState *planstate, List *ancestors,
						   ExplainState *es);
static void show_upper_qual(List *qual, const char *qlabel,
							PlanState *planstate, List *ancestors,
							ExplainState *es);
static void show_sort_keys(SortState *sortstate, List *ancestors,
						   ExplainState *es);
static void show_incremental_sort_keys(IncrementalSortState *incrsortstate,
									   List *ancestors, ExplainState *es);
static void show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
								   ExplainState *es);
static void show_agg_keys(AggState *astate, List *ancestors,
						  ExplainState *es);
static void show_tuple_split_keys(TupleSplitState *tstate, List *ancestors,
								  ExplainState *es);
static void show_grouping_sets(PlanState *planstate, Agg *agg,
							   List *ancestors, ExplainState *es);
static void show_grouping_set_keys(PlanState *planstate,
								   Agg *aggnode, Sort *sortnode,
								   List *context, bool useprefix,
								   List *ancestors, ExplainState *es);
static void show_sort_group_keys(PlanState *planstate, const char *qlabel,
								 int nkeys, int nPresortedKeys, AttrNumber *keycols,
								 Oid *sortOperators, Oid *collations, bool *nullsFirst,
								 List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr,
								   Oid sortOperator, Oid collation, bool nullsFirst);
static void show_tablesample(TableSampleClause *tsc, PlanState *planstate,
							 List *ancestors, ExplainState *es);
static void show_sort_info(SortState *sortstate, ExplainState *es);
static void show_windowagg_keys(WindowAggState *waggstate, List *ancestors, ExplainState *es);
static void show_incremental_sort_info(IncrementalSortState *incrsortstate,
									   ExplainState *es);
static void show_hash_info(HashState *hashstate, ExplainState *es);
static void show_runtime_filter_info(RuntimeFilterState *rfstate,
									 ExplainState *es);
static void show_memoize_info(MemoizeState *mstate, List *ancestors,
							  ExplainState *es);
static void show_hashagg_info(AggState *hashstate, ExplainState *es);
static void show_tidbitmap_info(BitmapHeapScanState *planstate,
								ExplainState *es);
static void show_instrumentation_count(const char *qlabel, int which,
									   PlanState *planstate, ExplainState *es);
static void show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es);
static void show_eval_params(Bitmapset *bms_params, ExplainState *es);
static void show_join_pruning_info(List *join_prune_ids, ExplainState *es);
static const char *explain_get_index_name(Oid indexId);
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage,
							  bool planning);
static void show_wal_usage(ExplainState *es, const WalUsage *usage);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
									ExplainState *es);
static void ExplainScanTarget(Scan *plan, ExplainState *es);
static void ExplainModifyTarget(ModifyTable *plan, ExplainState *es);
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es);
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
								  ExplainState *es);

/* explain_gp.c */
static void cdbexplain_showExecStats(struct PlanState *planstate, ExplainState *es);
static void cdbexplain_formatMemory(char *outbuf, int bufsize, double bytes);
static void cdbexplain_formatSeg(char *outbuf, int bufsize, int segindex, int nInst);
static void cdbexplain_formatSeconds(char *outbuf, int bufsize, double seconds, bool unit);
static void cdbexplain_formatExtraText(StringInfo str, int indent, int segindex, const char *notes, int notelen);
static bool nodeSupportWorkfileCaching(PlanState *planstate);
static void cdbexplain_showExecStatsEnd(struct PlannedStmt *stmt,
										struct CdbExplain_ShowStatCtx *showstatctx,
										struct EState *estate,
										ExplainState *es);
static void
show_dispatch_info(ExecSlice *slice, ExplainState *es, Plan *plan);

static void show_motion_keys(PlanState *planstate, List *hashExpr, int nkeys,
							 AttrNumber *keycols, const char *qlabel,
							 List *ancestors, ExplainState *es);

static void
gpexplain_formatSlicesOutput(struct CdbExplain_ShowStatCtx *showstatctx,
							struct EState *estate,
							ExplainState *es);

/* EXPLAIN ANALYZE statistics for one plan node of a slice */
typedef struct CdbExplain_StatInst
{
	NodeTag		pstype;			/* PlanState node type */

	/* fields from Instrumentation struct */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		ntuples2;
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;
	double		nfiltered2;
	double		execmemused;	/* executor memory used (bytes) */
	double		workmemused;	/* work_mem actually used (bytes) */
	double		workmemwanted;	/* work_mem to avoid workfile i/o (bytes) */
	bool		workfileCreated;	/* workfile created in this node */
	instr_time	firststart;		/* Start time of first iteration of node */
	int			numPartScanned; /* Number of part tables scanned */

	TuplesortInstrumentation sortstats; /* Sort stats, if this is a Sort node */
	HashInstrumentation hashstats; /* Hash stats, if this is a Hash node */
	IncrementalSortGroupInfo fullsortGroupInfo; /* Full sort group info for Incremental Sort node */
	IncrementalSortGroupInfo prefixsortGroupInfo; /* Prefix sort group info for Incremental Sort node */
	int			bnotes;			/* Offset to beginning of node's extra text */
	int			enotes;			/* Offset to end of node's extra text */
	int			nworkers_launched;	/* Number of workers launched for this node */
	WalUsage	walusage;		/* add WAL usage */
	/* fields from Instrumentation struct for one cycle of a node */
	double tuplecount;
	QueryMetricsStatus nodeStatus; /*CDB: stauts*/
} CdbExplain_StatInst;

/* EXPLAIN ANALYZE statistics for one process working on one slice */
typedef struct CdbExplain_SliceWorker
{
	double		peakmemused;	/* bytes alloc in per-query mem context tree */
	double		vmem_reserved;	/* vmem reserved by a QE */
} CdbExplain_SliceWorker;

/* Dispatch status summarized over workers in a slice */
typedef struct CdbExplain_DispatchSummary
{
	int			nResult;
	int			nOk;
	int			nError;
	int			nCanceled;
	int			nNotDispatched;
	int			nIgnorableError;
} CdbExplain_DispatchSummary;


/* One node's EXPLAIN ANALYZE statistics for all the workers of its segworker group */
typedef struct CdbExplain_NodeSummary
{
	/* Summary over all the node's workers */
	CdbExplain_Agg ntuples;
	CdbExplain_Agg runtime_tupleAgg;
	CdbExplain_Agg execmemused;
	CdbExplain_Agg workmemused;
	CdbExplain_Agg workmemwanted;
	CdbExplain_Agg totalWorkfileCreated;
	/* Used for DynamicSeqScan, DynamicIndexScan and DynamicBitmapHeapScan */
	CdbExplain_Agg totalPartTableScanned;
	/* Summary of space used by sort */
	CdbExplain_Agg sortSpaceUsed[NUM_SORT_SPACE_TYPE][NUM_SORT_METHOD];

	/* insts array info */
	int			segindex0;		/* segment id of insts[0] */
	int			ninst;			/* num of StatInst entries in inst array */

	/* Array [0..ninst-1] of StatInst entries is appended starting here */
	CdbExplain_StatInst insts[1];	/* variable size - must be last */
} CdbExplain_NodeSummary;

/* One slice's statistics for all the workers of its segworker group */
typedef struct CdbExplain_SliceSummary
{
	ExecSlice  *slice;

	/* worker array */
	int			nworker;		/* num of SliceWorker slots in worker array */
	int			segindex0;		/* segment id of workers[0] */
	CdbExplain_SliceWorker *workers;	/* -> array [0..nworker-1] of
										 * SliceWorker */
	CdbExplain_Agg peakmemused; /* Summary of SliceWorker stats over all of
								 * the slice's workers */

	CdbExplain_Agg vmem_reserved;	/* vmem reserved by QEs */

	/* Rollup of per-node stats over all of the slice's workers and nodes */
	double		workmemused_max;
	double		workmemwanted_max;

	/* How many workers were dispatched and returned results? (0 if local) */
	CdbExplain_DispatchSummary dispatchSummary;
} CdbExplain_SliceSummary;


/* State for cdbexplain_showExecStats() */
typedef struct CdbExplain_ShowStatCtx
{
	StringInfoData extratextbuf;
	instr_time	querystarttime;

	/* Rollup of per-node stats over the entire query plan */
	double		workmemused_max;
	double		workmemwanted_max;

	bool		stats_gathered;
	/* Per-slice statistics are deposited in this SliceSummary array */
	int			nslice;			/* num of slots in slices array */
	CdbExplain_SliceSummary *slices;	/* -> array[0..nslice-1] of
										 * SliceSummary */
	bool		runtime;
} CdbExplain_ShowStatCtx;

void VecExplainOneQuery(Query *query, int cursorOptions,
						IntoClause *into, ExplainState *es,
						const char *queryString, ParamListInfo params,
						QueryEnvironment *queryEnv)
{

	PlannedStmt *plan;
	instr_time planstart, planduration;
	BufferUsage bufusage_start, bufusage;
	bool vec_type = false;

	if (es->buffers)
		bufusage_start = pgBufferUsage;
	INSTR_TIME_SET_CURRENT(planstart);

	/* plan the query */
	plan = pg_plan_query(query, queryString, cursorOptions, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	if (plan->extensionContext)
		vec_type = find_extension_context(plan->extensionContext);

	if (!vec_type && vec_explain_prev) {
		(*vec_explain_prev) (query, cursorOptions, into, es, queryString, params, queryEnv);
		return;
	}

	/*
		* GPDB_92_MERGE_FIXME: it really should be an optimizer's responsibility
		* to correctly set the into-clause and into-policy of the PlannedStmt.
		*/
	if (into != NULL)
		plan->intoClause = copyObject(into);

	/* calc differences of buffer counters. */
	if (es->buffers)
	{
		memset(&bufusage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
	}

	(vec_type ? VecExplainOnePlan : ExplainOnePlan)
					(plan, into, es, queryString, params, queryEnv, 
									&planduration, (es->buffers ? &bufusage : NULL), cursorOptions);
}

/* ----------------
 * dummy Vec DestReceiver functions
 * ----------------
 */
bool
donothingVecReceive(TupleTableSlot *slot, DestReceiver *self)
{
	return true;
}

void
donothingVecStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
}

void
donothingVecCleanup(DestReceiver *self)
{
	/* this is used for both shutdown and destroy methods */
}

/* ----------------
 * static DestReceiver structs for dest types needing no local state
 * ----------------
 */
DestReceiver donothingVecDR = {
	donothingVecReceive, donothingVecStartup, donothingVecCleanup, donothingVecCleanup,
	DestNone
};

/*
 * VecExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt,
 * in which case executing the query should result in creating that table.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
static void
VecExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es,
				  const char *queryString, ParamListInfo params,
				  QueryEnvironment *queryEnv, const instr_time *planduration,
				  const BufferUsage *bufusage,
				  int cursorOptions)
{
	DestReceiver *dest;
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
	int			eflags;
	int			instrument_option = 0;

	Assert(plannedstmt->commandType != CMD_UTILITY);

	if (es->analyze && es->timing)
		instrument_option |= INSTRUMENT_TIMER;
	else if (es->analyze)
		instrument_option |= INSTRUMENT_ROWS;

	if (es->buffers)
		instrument_option |= INSTRUMENT_BUFFERS;
	if (es->wal)
		instrument_option |= INSTRUMENT_WAL;

	if (es->analyze)
		instrument_option |= INSTRUMENT_CDB;

	if (es->memory_detail)
		instrument_option |= INSTRUMENT_MEMORY_DETAIL;

	/*
	 * We always collect timing for the entire statement, even when node-level
	 * timing is off, so we don't look at es->timing here.  (We could skip
	 * this if !es->summary, but it's hardly worth the complication.)
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/*
	 * Normally we discard the query's output, but if explaining CREATE TABLE
	 * AS, we'd better use the appropriate tuple receiver.
	 */
	if (into)
		dest = CreateIntoRelDestReceiver(into);
	else
		dest = (DestReceiver *)&donothingVecDR;

	// GPDB_14_MERGE_FIXME: fix intoClause in optimizer
	plannedstmt->intoClause = copyObject(into);
	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument_option);

	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(METRICS_QUERY_SUBMIT, queryDesc);

    /* Allocate workarea for summary stats. */
	if (es->analyze)
	{
		/* Attach workarea to QueryDesc so ExecSetParamPlan() can find it. */
		queryDesc->showstatctx = cdbexplain_showExecStatsBegin(queryDesc,
																starttime);
	}
	else
		queryDesc->showstatctx = NULL;

	/* Select execution options */
	if (es->analyze)
		eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;
	if (into)
		eflags |= GetIntoRelEFlags(into);

	queryDesc->plannedstmt->query_mem =
		ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStartWrapper(queryDesc, eflags);

	/* Execute the plan for statistics if asked for */
	if (es->analyze)
	{
		ScanDirection dir;

		/* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
		if (into && into->skipData)
			dir = NoMovementScanDirection;
		else
			dir = ForwardScanDirection;

		/* run the plan */
		ExecutorRunWrapper(queryDesc, dir, 0L, true);

		/* Wait for completion of all qExec processes. */
		if (queryDesc->estate->dispatcherState && queryDesc->estate->dispatcherState->primaryResults)
			cdbdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);

		/* run cleanup too */
		ExecutorFinish(queryDesc);

		/* We can't run ExecutorEnd 'till we're done printing the stats... */
		totaltime += elapsed_time(&starttime);
	}

	VecExplainOpenGroup("Query", NULL, true, es);

	/* Create textual dump of plan tree */
	VecExplainPrintPlan(es, queryDesc);

	if (cursorOptions & CURSOR_OPT_PARALLEL_RETRIEVE)
		ExplainParallelRetrieveCursor(es, queryDesc);

	/*
	 * COMPUTE_QUERY_ID_REGRESS means COMPUTE_QUERY_ID_AUTO, but we don't show
	 * the queryid in any of the EXPLAIN plans to keep stable the results
	 * generated by regression test suites.
	 */
	if (es->verbose && plannedstmt->queryId != UINT64CONST(0) &&
		compute_query_id != COMPUTE_QUERY_ID_REGRESS)
	{
		/*
		 * Output the queryid as an int64 rather than a uint64 so we match
		 * what would be seen in the BIGINT pg_stat_statements.queryid column.
		 */
		ExplainPropertyInteger("Query Identifier", NULL, (int64)
							   plannedstmt->queryId, es);
	}

	/* Show buffer usage in planning */
	if (bufusage)
	{
		VecExplainOpenGroup("Planning", "Planning", true, es);
		show_buffer_usage(es, bufusage, true);
		VecExplainCloseGroup("Planning", "Planning", true, es);
	}

	if (es->summary && planduration)
	{
		double		plantime = INSTR_TIME_GET_DOUBLE(*planduration);

		ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
	}

	/* Print slice table */
	if (es->slicetable)
		ExplainPrintSliceTable(es, queryDesc);

	/* Print info about runtime of triggers */
	if (es->analyze)
		ExplainPrintTriggers(es, queryDesc);

	/*
	 * Display per-slice and whole-query statistics.
	 */
	if (es->analyze)
		cdbexplain_showExecStatsEnd(queryDesc->plannedstmt, queryDesc->showstatctx,
									queryDesc->estate, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		VecExplainOpenGroup("Settings", "Settings", true, es);

		if (queryDesc->plannedstmt->planGen == PLANGEN_PLANNER)
			ExplainPropertyStringInfo("Optimizer", es, "Postgres query optimizer");
#ifdef USE_ORCA
		else
			ExplainPropertyStringInfo("Optimizer", es, "Pivotal Optimizer (GPORCA)");
#endif

		VecExplainCloseGroup("Settings", "Settings", true, es);
	}

	/*
	 * Print info about JITing. Tied to es->costs because we don't want to
	 * display this in regression tests, as it'd cause output differences
	 * depending on build options.  Might want to separate that out from COSTS
	 * at a later stage.
	 */
	if (es->costs)
		ExplainPrintJITSummary(es, queryDesc);

	/*
	 * Close down the query and free resources.  Include time for this in the
	 * total execution time (although it should be pretty minimal).
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	ExecutorEndWrapper(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	/* We need a CCI just in case query expanded to multiple plans */
	if (es->analyze)
		CommandCounterIncrement();

	totaltime += elapsed_time(&starttime);

	/*
	 * We only report execution time if we actually ran the query (that is,
	 * the user specified ANALYZE), and if summary reporting is enabled (the
	 * user can set SUMMARY OFF to not have the timing information included in
	 * the output).  By default, ANALYZE sets SUMMARY to true.
	 */
	if (es->summary && es->analyze)
		ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3,
							 es);

	VecExplainCloseGroup("Query", NULL, true, es);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
static void
VecExplainOpenGroup(const char *objtype, const char *labelname,
					bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_OPENING, es);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			appendStringInfoChar(es->str, labeled ? '{' : '[');

			/*
			 * In JSON format, the grouping_stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level, 1 means we've
			 * emitted something (and so the next item needs a comma). See
			 * ExplainJSONLineEnding().
			 */
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:

			/*
			 * In YAML format, the grouping stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level AND this grouping
			 * level is unlabeled and must be marked with "- ".  See
			 * ExplainYAMLLineStarting().
			 */
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				appendStringInfo(es->str, "%s: ", labelname);
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			}
			else
			{
				appendStringInfoString(es->str, "- ");
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			}
			es->indent++;
			break;
	}
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
static void
VecExplainCloseGroup(const char *objtype, const char *labelname,
					 bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			ExplainXMLTag(objtype, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoChar(es->str, '\n');
			appendStringInfoSpaces(es->str, 2 * es->indent);
			appendStringInfoChar(es->str, labeled ? '}' : ']');
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent--;
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * VecExplainPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Also, output formatting state
 * such as the indent level is assumed valid.  Plan-tree-specific fields
 * in *es are initialized here.
 *
 * NB: will not work on utility statements
 */
static void
VecExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc)
{
	EState     *estate = queryDesc->estate;
	Bitmapset  *rels_used = NULL;
	PlanState  *ps;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;
	es->showstatctx = queryDesc->showstatctx;

	/* CDB: Find slice table entry for the root slice. */
	es->currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));

	/*
	 * Get local stats if root slice was executed here in the qDisp, as long
	 * as we haven't already gathered the statistics. This can happen when an
	 * executor hook generates EXPLAIN output.
	 */
	if (es->analyze && !es->showstatctx->stats_gathered)
	{
		es->showstatctx->runtime = es->runtime;
		if (Gp_role != GP_ROLE_EXECUTE && (!es->currentSlice || sliceRunsOnQD(es->currentSlice)))
			cdbexplain_localExecStats(queryDesc->planstate, es->showstatctx);

        /* Fill in the plan's Instrumentation with stats from qExecs. */
		if (estate->dispatcherState && estate->dispatcherState->primaryResults)
		{
				cdbexplain_recvExecStats(queryDesc->planstate,
										 estate->dispatcherState->primaryResults,
										 LocallyExecutingSliceIndex(estate),
										 es->showstatctx);
		}
	}

	VecExplainPreScanNode(queryDesc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_tree(queryDesc->plannedstmt,
													es->rtable_names);
	es->printed_subplans = NULL;

	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not to be displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 * Such marking is currently only supported on a Gather at the top of the
	 * plan.  We skip that node, and we must also hide per-worker detail data
	 * further down in the plan tree.
	 */
	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) && ((Gather *) ps->plan)->invisible)
	{
		ps = outerPlanState(ps);
		es->hide_workers = true;
	}
	VecExplainNode(ps, NIL, NULL, NULL, es);

	/*
	 * If requested, include information about GUC parameters with values that
	 * don't match the built-in defaults.
	 */
	VecExplainPrintSettings(es, queryDesc->plannedstmt->planGen);
}

/*
 * VecExplainPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
VecExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_ShareInputScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		case T_Append:
			*rels_used = bms_add_members(*rels_used,
										 ((Append *) plan)->apprelids);
			break;
		case T_MergeAppend:
			*rels_used = bms_add_members(*rels_used,
										 ((MergeAppend *) plan)->apprelids);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, VecExplainPreScanNode, rels_used);
}


/*
 * VecExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent Plan and SubPlan nodes, most-closely-nested
 * first.  These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries (but a few subroutines will
 * transiently increment it).  In non-text formats, es->indent corresponds
 * to the nesting depth of logical output groups, and therefore is controlled
 * by VecExplainOpenGroup/VecExplainCloseGroup.
 *
 * es->parentPlanState points to the parent planstate node and can be used by
 * PartitionSelector to deparse its printablePredicate. (This is passed in
 * ExplainState rather than as a normal argument, to avoid changing the
 * function signature from upstream.)
 */
static void
VecExplainNode(PlanState *planstate, List *ancestors,
			   const char *relationship, const char *plan_name,
			   ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	PlanState  *parentplanstate;
	ExecSlice  *save_currentSlice = es->currentSlice;    /* save */
	const char *pname;			/* node type name for text output */
	const char *sname;			/* node type name for non-text output */
	const char *strategy = NULL;
	const char *partialmode = NULL;
	const char *operation = NULL;
	const char *custom_name = NULL;
	ExplainWorkersState *save_workers_state = es->workers_state;
	int			save_indent = es->indent;
	bool		haschildren;
	bool		skip_outer=false;
	char       *skip_outer_msg = NULL;
	int			motion_recv;
	int			motion_snd;
	ExecSlice  *parentSlice = NULL;
	bool vec_type = find_extension_context(es->pstmt->extensionContext);

	/* Remember who called us. */
	parentplanstate = es->parentPlanState;
	es->parentPlanState = planstate;

	/*
	 * If this is a Motion node, we're descending into a new slice.
	 */
	if (IsA(plan, Motion))
	{
		Motion	   *pMotion = (Motion *) plan;
		SliceTable *sliceTable = planstate->state->es_sliceTable;

		if (sliceTable)
		{
			es->currentSlice = &sliceTable->slices[pMotion->motionID];
			parentSlice = es->currentSlice->parentIndex == -1 ? NULL :
						  &sliceTable->slices[es->currentSlice->parentIndex];
		}
	}

	/*
	 * Prepare per-worker output buffers, if needed.  We'll append the data in
	 * these to the main output string further down.
	 */
	if (planstate->worker_instrument && es->analyze && !es->hide_workers)
		es->workers_state = ExplainCreateWorkersState(planstate->worker_instrument->num_workers);
	else
		es->workers_state = NULL;

	/* Identify plan node type, and print generic details */
	switch (nodeTag(plan))
	{
		case T_Result:
			if (vec_type)
			{
				pname = sname = "Vec Result";
			}
			else
			{
				pname = sname = "Result";
			}
			break;
		case T_ProjectSet:
			pname = sname = "ProjectSet";
			break;
		case T_ModifyTable:
			sname = "ModifyTable";
			switch (((ModifyTable *) plan)->operation)
			{
				case CMD_INSERT:
					pname = operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = operation = "Update";
					break;
				case CMD_DELETE:
					pname = operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_Append:
			if (vec_type)
				pname = sname = "Vec Append";
			else
				pname = sname = "Append";
			break;
		case T_MergeAppend:
			pname = sname = "Merge Append";
			break;
		case T_RecursiveUnion:
			pname = sname = "Recursive Union";
			break;
		case T_Sequence:
			if (vec_type)
				pname = sname = "Vec Sequence";
			else
				pname = sname = "Sequence";
			break;
		case T_BitmapAnd:
			pname = sname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = sname = "BitmapOr";
			break;
		case T_NestLoop:
			if (vec_type) 
				pname = sname = "Vec Nested Loop";
			else 
				pname = sname = "Nested Loop";
			if (((NestLoop *)plan)->shared_outer)
			{
				skip_outer = true;
				skip_outer_msg = "See first subplan of Hash Join";
			}
			break;
		case T_MergeJoin:
			pname = "Merge";	/* "Join" gets added by jointype switch */
			sname = "Merge Join";
			break;
		case T_HashJoin:
			if (vec_type) 
			{
				pname = "Vec Hash";		/* "Join" gets added by jointype switch */
				sname = "Vec Hash Join";
			}
			else
			{
				pname = "Hash";		/* "Join" gets added by jointype switch */
				sname = "Hash Join";
			}
			break;
		case T_SeqScan:
			if (vec_type) 
			{
				pname = sname = "Vec Seq Scan";
			}
			else
			{
				pname = sname = "Seq Scan";
			}
			break;
		case T_SampleScan:
			pname = sname = "Sample Scan";
			break;
		case T_Gather:
			pname = sname = "Gather";
			break;
		case T_GatherMerge:
			pname = sname = "Gather Merge";
			break;
		case T_IndexScan:
			pname = sname = "Index Scan";
			break;
		case T_IndexOnlyScan:
			pname = sname = "Index Only Scan";
			break;
		case T_BitmapIndexScan:
			pname = sname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			/*
			 * We print "Bitmap Heap Scan", even for AO tables. It's a bit
			 * confusing, but that's what the plan node is called, regardless
			 * of the table type.
			 */
			pname = sname = "Bitmap Heap Scan";
			break;
		case T_TidScan:
			pname = sname = "Tid Scan";
			break;
		case T_TidRangeScan:
			pname = sname = "Tid Range Scan";
			break;
		case T_SubqueryScan:
			if (vec_type)
				pname = sname = "Vec Subquery Scan";
			else 
				pname = sname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = sname = "Function Scan";
			break;
		case T_TableFuncScan:
			pname = sname = "Table Function Scan";
			break;
		case T_ValuesScan:
			pname = sname = "Values Scan";
			break;
		case T_CteScan:
			pname = sname = "CTE Scan";
			break;
		case T_NamedTuplestoreScan:
			pname = sname = "Named Tuplestore Scan";
			break;
		case T_WorkTableScan:
			pname = sname = "WorkTable Scan";
			break;
		case T_ShareInputScan:
			if (vec_type)
			{
				pname = sname = "Vec Shared Scan";
			}
			else
			{
				pname = sname = "Shared Scan";
			}
			break;
		case T_ForeignScan:
			sname = "Foreign Scan";
			switch (((ForeignScan *) plan)->operation)
			{
				case CMD_SELECT:
					pname = "Foreign Scan";
					operation = "Select";
					break;
				case CMD_INSERT:
					pname = "Foreign Insert";
					operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = "Foreign Update";
					operation = "Update";
					break;
				case CMD_DELETE:
					pname = "Foreign Delete";
					operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_CustomScan:
			sname = "Custom Scan";
			custom_name = ((CustomScan *) plan)->methods->CustomName;
			if (custom_name)
				pname = psprintf("Custom Scan (%s)", custom_name);
			else
				pname = sname;
			break;
		case T_Material:
			if (vec_type)
			{
				pname = sname = "Vec Materialize";
			}
			else
			{
				pname = sname = "Materialize";
			}
			break;
		case T_Memoize:
			pname = sname = "Memoize";
			break;
		case T_Sort:
			if (vec_type) 
			{
				pname = sname = "Vec Sort";
			}
			else 
			{
				pname = sname = "Sort";
			}
			break;
		case T_TupleSplit:
			pname = sname = "TupleSplit";
			break;
		case T_IncrementalSort:
			pname = sname = "Incremental Sort";
			break;
		case T_Group:
			pname = sname = "Group";
			break;
		case T_Agg:
			if (vec_type)
			{
				Agg		   *agg = (Agg *) plan;

				sname = "Aggregate";
				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						pname = "Vec Aggregate";
						strategy = "Plain";
						break;
					case AGG_SORTED:
						pname = "Vec GroupAggregate";
						strategy = "Sorted";
						break;
					case AGG_HASHED:
						pname = "Vec HashAggregate";
						strategy = "Hashed";
						break;
					case AGG_MIXED:
						pname = "Vec MixedAggregate";
						strategy = "Mixed";
						break;
					default:
						pname = "Vec Aggregate ???";
						strategy = "???";
						break;
				}

				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
				{
					partialmode = "Vec Partial";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
				{
					partialmode = "Vec Finalize";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else
					partialmode = "Vec Simple";

				if (agg->streaming)
					pname = psprintf("Vec Streaming %s", pname);
			}
			else
			{
				Agg		   *agg = (Agg *) plan;

				sname = "Aggregate";
				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						pname = "Aggregate";
						strategy = "Plain";
						break;
					case AGG_SORTED:
						pname = "GroupAggregate";
						strategy = "Sorted";
						break;
					case AGG_HASHED:
						pname = "HashAggregate";
						strategy = "Hashed";
						break;
					case AGG_MIXED:
						pname = "MixedAggregate";
						strategy = "Mixed";
						break;
					default:
						pname = "Aggregate ???";
						strategy = "???";
						break;
				}

				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
				{
					partialmode = "Partial";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
				{
					partialmode = "Finalize";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else
					partialmode = "Simple";

				if (agg->streaming)
					pname = psprintf("Streaming %s", pname);
			}
			break;
		case T_WindowAgg:
			if (vec_type)
			{
				pname = sname = "Vec WindowAgg";
			}
			else
			{
				pname = sname = "WindowAgg";
			}

			break;
		case T_TableFunctionScan:
			pname = sname = "Table Function Scan";
			break;
		case T_Unique:
			pname = sname = "Unique";
			break;
		case T_SetOp:
			sname = "SetOp";
			switch (((SetOp *) plan)->strategy)
			{
				case SETOP_SORTED:
					pname = "SetOp";
					strategy = "Sorted";
					break;
				case SETOP_HASHED:
					pname = "HashSetOp";
					strategy = "Hashed";
					break;
				default:
					pname = "SetOp ???";
					strategy = "???";
					break;
			}
			break;
		case T_LockRows:
			pname = sname = "LockRows";
			break;
		case T_RuntimeFilter:
			pname = sname = "RuntimeFilter";
			break;
		case T_Limit:
			if (vec_type)
			{
				pname = sname = "Vec Limit";
			}
			else
			{
				pname = sname = "Limit";
			}
			break;
		case T_Hash:
			if (vec_type)
				pname = sname = "Vec Hash";
			else
				pname = sname = "Hash";
			break;
		case T_Motion:
			{
				Motion		*pMotion = (Motion *) plan;

				Assert(plan->lefttree);

				motion_snd = list_length(es->currentSlice->segments);
				motion_recv = parentSlice == NULL ? 1 : list_length(parentSlice->segments);

				if (vec_type) 
				{
					switch (pMotion->motionType)
					{
						case MOTIONTYPE_GATHER:
							sname = "Vec Gather Motion";
							motion_recv = 1;
							break;
						case MOTIONTYPE_GATHER_SINGLE:
							sname = "Vec Explicit Gather Motion";
							motion_recv = 1;
							break;
						case MOTIONTYPE_HASH:
							sname = "Vec Redistribute Motion";
							break;
						case MOTIONTYPE_BROADCAST:
							sname = "Vec Broadcast Motion";
							break;
						case MOTIONTYPE_EXPLICIT:
							sname = "Vec Explicit Redistribute Motion";
							break;
						default:
							sname = "???";
							motion_recv = -1;
							break;
					}
				}
				else
				{
					switch (pMotion->motionType)
					{
						case MOTIONTYPE_GATHER:
							sname = "Gather Motion";
							motion_recv = 1;
							break;
						case MOTIONTYPE_GATHER_SINGLE:
							sname = "Explicit Gather Motion";
							motion_recv = 1;
							break;
						case MOTIONTYPE_HASH:
							sname = "Redistribute Motion";
							break;
						case MOTIONTYPE_BROADCAST:
							sname = "Broadcast Motion";
							break;
						case MOTIONTYPE_EXPLICIT:
							sname = "Explicit Redistribute Motion";
							break;
						default:
							sname = "???";
							motion_recv = -1;
							break;
					}
				}

				pname = psprintf("%s %d:%d", sname, motion_snd, motion_recv);
			}
			break;
		case T_SplitUpdate:
			pname = sname = "Split";
			break;
		case T_AssertOp:
			if (vec_type)
			{
				pname = sname = "Vec Assert";
			}
			else
			{
				pname = sname = "Assert";
			}
			break;
		case T_PartitionSelector:
			pname = sname = "Partition Selector";
			break;
		default:
			pname = sname = "???";
			break;
		}

	ExplainOpenGroup("Plan",
					 relationship ? NULL : "Plan",
					 true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (plan_name)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "%s", plan_name);

			/*
			 * If this SubPlan is being dispatched separately, show slice
			 * information after the plan name. Currently, we do this for
			 * Init Plans.
			 *
			 * Note: If the top node was a Motion node, we print the slice
			 * *above* the Motion here. We will print the slice below the
			 * Motion, below.
			 */
			if (es->subplanDispatchedSeparately)
				show_dispatch_info(save_currentSlice, es, plan);
			appendStringInfoChar(es->str, '\n');
			es->indent++;
		}
		if (es->indent)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "->  ");
			es->indent += 2;
		}
		if (plan->parallel_aware)
			appendStringInfoString(es->str, "Parallel ");
		if (plan->async_capable)
			appendStringInfoString(es->str, "Async ");
		appendStringInfoString(es->str, pname);

		/*
		 * Print information about the current slice. In order to not make
		 * the output too verbose, only print it at the slice boundaries,
		 * ie. at Motion nodes. (We already switched the "current slice"
		 * to the slice below the Motion.)
		 */
		if (IsA(plan, Motion))
			show_dispatch_info(es->currentSlice, es, plan);

		es->indent++;
	}
	else
	{
		ExplainPropertyText("Node Type", sname, es);
		if (nodeTag(plan) == T_Motion)
		{
			ExplainPropertyInteger("Senders", NULL, motion_snd, es);
			ExplainPropertyInteger("Receivers", NULL, motion_recv, es);
		}
		if (strategy)
			ExplainPropertyText("Strategy", strategy, es);
		if (partialmode)
			ExplainPropertyText("Partial Mode", partialmode, es);
		if (operation)
			ExplainPropertyText("Operation", operation, es);
		if (relationship)
			ExplainPropertyText("Parent Relationship", relationship, es);
		if (plan_name)
			ExplainPropertyText("Subplan Name", plan_name, es);
		if (custom_name)
			ExplainPropertyText("Custom Plan Provider", custom_name, es);

		show_dispatch_info(es->currentSlice, es, plan);
		ExplainPropertyBool("Parallel Aware", plan->parallel_aware, es);
		ExplainPropertyBool("Async Capable", plan->async_capable, es);
	}

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget((Scan *) plan, es);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget((Scan *) plan, es);
			break;
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexscan, es);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexonlyscan, es);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname =
				explain_get_index_name(bitmapindexscan->indexid);

				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " on %s",
									 quote_identifier(indexname));
				else
					ExplainPropertyText("Index Name", indexname, es);
			}
			break;
		case T_ModifyTable:
			ExplainModifyTarget((ModifyTable *) plan, es);
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
					case JOIN_INNER:
						jointype = "Inner";
						break;
					case JOIN_LEFT:
						jointype = "Left";
						break;
					case JOIN_FULL:
						jointype = "Full";
						break;
					case JOIN_RIGHT:
						jointype = "Right";
						break;
					case JOIN_SEMI:
						jointype = "Semi";
						break;
					case JOIN_ANTI:
						jointype = "Anti";
						break;
					case JOIN_LASJ_NOTIN:
						jointype = "Left Anti Semi (Not-In)";
						break;
					default:
						jointype = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					/*
					 * For historical reasons, the join type is interpolated
					 * into the node type name...
					 */
					if (((Join *) plan)->jointype != JOIN_INNER)
						appendStringInfo(es->str, " %s Join", jointype);
					else if (!IsA(plan, NestLoop))
						appendStringInfoString(es->str, " Join");
				}
				else
					ExplainPropertyText("Join Type", jointype, es);
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
					case SETOPCMD_INTERSECT:
						setopcmd = "Intersect";
						break;
					case SETOPCMD_INTERSECT_ALL:
						setopcmd = "Intersect All";
						break;
					case SETOPCMD_EXCEPT:
						setopcmd = "Except";
						break;
					case SETOPCMD_EXCEPT_ALL:
						setopcmd = "Except All";
						break;
					default:
						setopcmd = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " %s", setopcmd);
				else
					ExplainPropertyText("Command", setopcmd, es);
			}
			break;
		case T_ShareInputScan:
			{
				ShareInputScan *sisc = (ShareInputScan *) plan;
				int				slice_id = -1;

				if (es->currentSlice)
					slice_id = es->currentSlice->sliceIndex;

				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " (share slice:id %d:%d)",
									 slice_id, sisc->share_id);
				else
				{
					ExplainPropertyInteger("Share ID", NULL, sisc->share_id, es);
					ExplainPropertyInteger("Slice ID", NULL, slice_id, es);
				}
			}
			break;
		case T_PartitionSelector:
			{
				PartitionSelector *ps = (PartitionSelector *) plan;

				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					appendStringInfo(es->str, " (selector id: $%d)", ps->paramid);
				}
				else
				{
					ExplainPropertyInteger("Selector ID", NULL, ps->paramid, es);
				}
			}
			break;
		default:
			break;
	}

	if (es->costs)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(es->str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
							 plan->startup_cost, plan->total_cost,
							 plan->plan_rows, plan->plan_width);
		}
		else
		{
			ExplainPropertyFloat("Startup Cost", NULL, plan->startup_cost,
								 2, es);
			ExplainPropertyFloat("Total Cost", NULL, plan->total_cost,
								 2, es);
			ExplainPropertyFloat("Plan Rows", NULL, plan->plan_rows,
								 0, es);
			ExplainPropertyInteger("Plan Width", NULL, plan->plan_width,
								   es);
		}
	}

	if (ResManagerPrintOperatorMemoryLimits())
	{
		ExplainPropertyInteger("operatorMem", "kB", PlanStateOperatorMemKB(planstate), es);
	}
	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 *
	 * Note: contrib/auto_explain could cause instrumentation to be set up
	 * even though we didn't ask for it here.  Be careful not to print any
	 * instrumentation results the user didn't ask for.  But we do the
	 * InstrEndLoop call anyway, if possible, to reduce the number of cases
	 * auto_explain has to contend with.
	 */
	if (planstate->instrument && !es->runtime)
		InstrEndLoop(planstate->instrument);

	/* GPDB_90_MERGE_FIXME: In GPDB, these are printed differently. But does that work
	 * with the new XML/YAML EXPLAIN output */
	if (es->analyze &&
		planstate->instrument && planstate->instrument->nloops > 0)
	{
		double		nloops = planstate->instrument->nloops;
		double		startup_ms = 1000.0 * planstate->instrument->startup / nloops;
		double		total_ms = 1000.0 * planstate->instrument->total / nloops;
		double		rows = planstate->instrument->ntuples / nloops;

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->timing)
				appendStringInfo(es->str,
								 " (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
								 startup_ms, total_ms, rows, nloops);
			else
				appendStringInfo(es->str,
								 " (actual rows=%.0f loops=%.0f)",
								 rows, nloops);
		}
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "ms", startup_ms,
									 3, es);
				ExplainPropertyFloat("Actual Total Time", "ms", total_ms,
									 3, es);
			}
			ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);
		}
	}
	else if (es->analyze && !es->runtime)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, " (never executed)");
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "ms", 0.0, 3, es);
				ExplainPropertyFloat("Actual Total Time", "ms", 0.0, 3, es);
			}
			ExplainPropertyFloat("Actual Rows", NULL, 0.0, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, 0.0, 0, es);
		}
	}
/*
	 * Print the progress of node execution at current loop.
	 */
	if (planstate->instrument && es->analyze && es->runtime)
	{
		instr_time	starttimespan;
		double	startup_sec;
		double	total_sec;
		double	rows;
		double	loop_num;
		char 	*status;

		if (!INSTR_TIME_IS_ZERO(planstate->instrument->rt_starttime))
		{
			INSTR_TIME_SET_CURRENT(starttimespan);
			INSTR_TIME_SUBTRACT(starttimespan, planstate->instrument->rt_starttime);
		}
		else
			INSTR_TIME_SET_ZERO(starttimespan);
		startup_sec = 1000.0 * planstate->instrument->rt_firsttuple;
		total_sec = 1000.0 * (INSTR_TIME_GET_DOUBLE(planstate->instrument->rt_counter)
							+ INSTR_TIME_GET_DOUBLE(starttimespan));
		rows = planstate->instrument->rt_tuplecount;
		loop_num = planstate->instrument->nloops + 1;

		switch (planstate->instrument->nodeStatus)
		{
			case METRICS_PLAN_NODE_INITIALIZE:
				status = &("Initialize"[0]);
				break;
			case METRICS_PLAN_NODE_EXECUTING:
				status = &("Executing"[0]);
				break;
			case METRICS_PLAN_NODE_FINISHED:
				status = &("Finished"[0]);
				break;
			default:
				status = &("Unknown"[0]);
				break;
		}
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(es->str,
							 " (node status: %s)", status);
		}
		else
		{
			ExplainPropertyText("Node status", status, es);
		}

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->timing)
			{
				if (planstate->instrument->running)
					appendStringInfo(es->str,
									 " (actual time=%.3f..%.3f rows=%.0f, loops=%.0f)",
									 startup_sec, total_sec, rows, loop_num);
				else
					appendStringInfo(es->str,
									 " (actual time=%.3f rows=0, loops=%.0f)",
									 total_sec, loop_num);
			}
			else
				appendStringInfo(es->str,
								 " (actual rows=%.0f, loops=%.0f)",
								 rows, loop_num);
		}
		else
		{
			if (es->timing)
			{
				if (planstate->instrument->running)
				{
					ExplainPropertyFloat("Actual Startup Time", NULL, startup_sec, 3, es);
					ExplainPropertyFloat("Actual Total Time", NULL, total_sec, 3, es);
				}
				else
					ExplainPropertyFloat("Running Time", NULL, total_sec, 3, es);
			}
			ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, loop_num, 0, es);
		}
	}

	/* in text format, first line ends here */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		appendStringInfoChar(es->str, '\n');

	/* prepare per-worker general execution details */
	if (es->workers_state && es->verbose)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;

		for (int n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;
			double		startup_ms;
			double		total_ms;
			double		rows;

			if (nloops <= 0)
				continue;
			startup_ms = 1000.0 * instrument->startup / nloops;
			total_ms = 1000.0 * instrument->total / nloops;
			rows = instrument->ntuples / nloops;

			ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				if (es->timing)
					appendStringInfo(es->str,
									 "actual time=%.3f..%.3f rows=%.0f loops=%.0f\n",
									 startup_ms, total_ms, rows, nloops);
				else
					appendStringInfo(es->str,
									 "actual rows=%.0f loops=%.0f\n",
									 rows, nloops);
			}
			else
			{
				if (es->timing)
				{
					ExplainPropertyFloat("Actual Startup Time", "ms",
										 startup_ms, 3, es);
					ExplainPropertyFloat("Actual Total Time", "ms",
										 total_ms, 3, es);
				}
				ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
				ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);
			}

			ExplainCloseWorker(n, es);
		}
	}

	/* target list */
	if (es->verbose)
		show_plan_tlist(planstate, ancestors, es);

	/* unique join */
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* try not to be too chatty about this in text mode */
			if (es->format != EXPLAIN_FORMAT_TEXT ||
				(es->verbose && ((Join *) plan)->inner_unique))
				ExplainPropertyBool("Inner Unique",
									((Join *) plan)->inner_unique,
									es);
			break;
		default:
			break;
	}

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexScan *) plan)->indexqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexScan *) plan)->indexorderbyorig,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_IndexOnlyScan:
			show_scan_qual(((IndexOnlyScan *) plan)->indexqual,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexOnlyScan *) plan)->recheckqual)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexOnlyScan *) plan)->indexorderby,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				ExplainPropertyFloat("Heap Fetches", NULL,
									 planstate->instrument->ntuples2, 0, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			break;
		case T_BitmapHeapScan:
		{
			List		*bitmapqualorig;

			bitmapqualorig = ((BitmapHeapScan *) plan)->bitmapqualorig;

			show_scan_qual(bitmapqualorig,
						   "Recheck Cond", planstate, ancestors, es);

			if (bitmapqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				show_tidbitmap_info((BitmapHeapScanState *) planstate, es);
			break;
		}
		case T_SampleScan:
			show_tablesample(((SampleScan *) plan)->tablesample,
							 planstate, ancestors, es);
			/* fall through to print additional fields the same as SeqScan */
			/* FALLTHROUGH */
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Gather:
			{
				Gather	   *gather = (Gather *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gather->num_workers, es);

				/* Show params evaluated at gather node */
				if (gather->initParam)
					show_eval_params(gather->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}

				if (gather->single_copy || es->format != EXPLAIN_FORMAT_TEXT)
					ExplainPropertyBool("Single Copy", gather->single_copy, es);
			}
			break;
		case T_GatherMerge:
			{
				GatherMerge *gm = (GatherMerge *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gm->num_workers, es);

				/* Show params evaluated at gather-merge node */
				if (gm->initParam)
					show_eval_params(gm->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherMergeState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}
			}
			break;
		case T_FunctionScan:
			if (es->verbose)
			{
				List	   *fexprs = NIL;
				ListCell   *lc;

				foreach(lc, ((FunctionScan *) plan)->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

					fexprs = lappend(fexprs, rtfunc->funcexpr);
				}
				/* We rely on show_expression to insert commas as needed */
				show_expression((Node *) fexprs,
								"Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TableFuncScan:
			if (es->verbose)
			{
				TableFunc  *tablefunc = ((TableFuncScan *) plan)->tablefunc;

				show_expression((Node *) tablefunc,
								"Table Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
			}
			break;
		case T_TidRangeScan:
			{
				/*
				 * The tidrangequals list has AND semantics, so be sure to
				 * show it as an AND condition.
				 */
				List	   *tidquals = ((TidRangeScan *) plan)->tidrangequals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_andclause(tidquals));
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
			}
			break;
		case T_ForeignScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			show_foreignscan_info((ForeignScanState *) planstate, es);
			break;
		case T_CustomScan:
			{
				CustomScanState *css = (CustomScanState *) planstate;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				if (css->methods->ExplainCustomScan)
					css->methods->ExplainCustomScan(css, ancestors, es);
			}
			break;
		case T_NestLoop:
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((NestLoop *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_MergeJoin:
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond", planstate, ancestors, es);
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((MergeJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_HashJoin:
		{
			HashJoin *hash_join = (HashJoin *) plan;
			/*
			 * In the case of an "IS NOT DISTINCT" condition, we display
			 * hashqualclauses instead of hashclauses.
			 */
			List *cond_to_show = hash_join->hashclauses;
			if (list_length(hash_join->hashqualclauses) > 0)
				cond_to_show = hash_join->hashqualclauses;

			show_upper_qual(cond_to_show,
							"Hash Cond", planstate, ancestors, es);
			show_upper_qual(((HashJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((HashJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		}
		case T_TupleSplit:
			show_tuple_split_keys((TupleSplitState *)planstate, ancestors, es);
			break;
		case T_Agg:
			show_agg_keys(castNode(AggState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			show_hashagg_info((AggState *) planstate, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
#if 0 /* Group node has been disabled in GPDB */
		case T_Group:
			show_group_keys(castNode(GroupState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
#endif
		case T_WindowAgg:
			show_windowagg_keys((WindowAggState *) planstate, ancestors, es);
			break;
		case T_TableFunctionScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			/* TODO: Partitioning and ordering information */
			break;
		case T_Unique:
			show_motion_keys(planstate,
                             NIL,
						     ((Unique *) plan)->numCols,
						     ((Unique *) plan)->uniqColIdx,
						     "Group Key",
						     ancestors, es);
			break;
		case T_Sort:
			show_sort_keys(castNode(SortState, planstate), ancestors, es);
			show_sort_info(castNode(SortState, planstate), es);
			break;
		case T_IncrementalSort:
			show_incremental_sort_keys(castNode(IncrementalSortState, planstate),
									   ancestors, es);
			show_incremental_sort_info(castNode(IncrementalSortState, planstate),
									   es);
			break;
		case T_MergeAppend:
			show_merge_append_keys(castNode(MergeAppendState, planstate),
								   ancestors, es);
			break;
		case T_Result:
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter", planstate, ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_ModifyTable:
			show_modifytable_info(castNode(ModifyTableState, planstate), ancestors,
								  es);
			break;
		case T_Hash:
			show_hash_info(castNode(HashState, planstate), es);
			break;
		case T_RuntimeFilter:
			show_runtime_filter_info(castNode(RuntimeFilterState, planstate),
									 es);
			break;
		case T_Motion:
			{
				Motion	   *pMotion = (Motion *) plan;

				if (pMotion->sendSorted || pMotion->motionType == MOTIONTYPE_HASH)
					show_motion_keys(planstate,
									 pMotion->hashExprs,
									 pMotion->numSortCols,
									 pMotion->sortColIdx,
									 "Merge Key",
									 ancestors, es);
				if (pMotion->motionType == MOTIONTYPE_HASH &&
					pMotion->numHashSegments != motion_recv)
				{
					Assert(pMotion->numHashSegments < motion_recv);
					appendStringInfoSpaces(es->str, es->indent * 2);
					appendStringInfo(es->str,
									 "Hash Module: %d\n",
									 pMotion->numHashSegments);
				}
			}
			break;
		case T_AssertOp:
			show_upper_qual(plan->qual, "Assert Cond", planstate, ancestors, es);
			break;
		case T_Append:
			show_join_pruning_info(((Append *) plan)->join_prune_paramids, es);
			break;
		case T_Memoize:
			show_memoize_info(castNode(MemoizeState, planstate), ancestors,
							  es);
			break;
		default:
			break;
	}

    /* Show executor statistics */
	if (planstate->instrument && planstate->instrument->need_cdb && !es->runtime)
		cdbexplain_showExecStats(planstate, es);

	/*
	 * Prepare per-worker JIT instrumentation.  As with the overall JIT
	 * summary, this is printed only if printing costs is enabled.
	 */
	if (es->workers_state && es->costs && es->verbose)
	{
		SharedJitInstrumentation *w = planstate->worker_jit_instrument;

		if (w)
		{
			for (int n = 0; n < w->num_workers; n++)
			{
				ExplainOpenWorker(n, es);
				ExplainPrintJIT(es, planstate->state->es_jit_flags,
								&w->jit_instr[n]);
				ExplainCloseWorker(n, es);
			}
		}
	}

	/* Show buffer/WAL usage */
	if (es->buffers && planstate->instrument)
		show_buffer_usage(es, &planstate->instrument->bufusage, false);
	if (es->wal && planstate->instrument)
		show_wal_usage(es, &planstate->instrument->walusage);

	/* Prepare per-worker buffer/WAL usage */
	if (es->workers_state && (es->buffers || es->wal) && es->verbose && !es->runtime)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;

		for (int n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;

			if (nloops <= 0)
				continue;

			ExplainOpenWorker(n, es);
			if (es->buffers)
				show_buffer_usage(es, &instrument->bufusage, false);
			if (es->wal)
				show_wal_usage(es, &instrument->walusage);
			ExplainCloseWorker(n, es);
		}
	}

	/* Show per-worker details for this plan node, then pop that stack */
	if (es->workers_state)
		ExplainFlushWorkersState(es);
	es->workers_state = save_workers_state;

	/*
	 * If partition pruning was done during executor initialization, the
	 * number of child plans we'll display below will be less than the number
	 * of subplans that was specified in the plan.  To make this a bit less
	 * mysterious, emit an indication that this happened.  Note that this
	 * field is emitted now because we want it to be a property of the parent
	 * node; it *cannot* be emitted within the Plans sub-node we'll open next.
	 */
	switch (nodeTag(plan))
	{
		case T_Append:
			ExplainMissingMembers(((AppendState *) planstate)->as_nplans,
								  list_length(((Append *) plan)->appendplans),
								  es);
			break;
		case T_MergeAppend:
			ExplainMissingMembers(((MergeAppendState *) planstate)->ms_nplans,
								  list_length(((MergeAppend *) plan)->mergeplans),
								  es);
			break;
		default:
			break;
	}

	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, Sequence) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;
	if (haschildren)
	{
		ExplainOpenGroup("Plans", "Plans", false, es);
		/* Pass current Plan as head of ancestors list for children */
		ancestors = lcons(plan, ancestors);
	}

	/* initPlan-s */
	if (plan->initPlan)
		VecExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es, planstate->state->es_sliceTable);

	/* lefttree */
	if (outerPlan(plan) && !skip_outer)
	{
		VecExplainNode(outerPlanState(planstate), ancestors,
					"Outer", NULL, es);
	}
    else if (skip_outer)
    {
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "  ->  ");
		appendStringInfoString(es->str, skip_outer_msg);
		appendStringInfo(es->str, "\n");
    }

	/* righttree */
	if (innerPlanState(planstate))
		VecExplainNode(innerPlanState(planstate), ancestors,
					"Inner", NULL, es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			ExplainMemberNodes(((AppendState *) planstate)->appendplans,
							   ((AppendState *) planstate)->as_nplans,
							   ancestors, es);
			break;
		case T_MergeAppend:
			ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
							   ((MergeAppendState *) planstate)->ms_nplans,
							   ancestors, es);
			break;
		case T_Sequence:
			ExplainMemberNodes(((SequenceState *) planstate)->subplans,
							   ((SequenceState *) planstate)->numSubplans,
							   ancestors, es);
			break;
		case T_BitmapAnd:
			ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
							   ((BitmapAndState *) planstate)->nplans,
							   ancestors, es);
			break;
		case T_BitmapOr:
			ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
							   ((BitmapOrState *) planstate)->nplans,
							   ancestors, es);
			break;
		case T_SubqueryScan:
			VecExplainNode(((SubqueryScanState *) planstate)->subplan, ancestors,
						"Subquery", NULL, es);
			break;
		case T_CustomScan:
			ExplainCustomChildren((CustomScanState *) planstate,
								  ancestors, es);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		VecExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es, NULL);

	/* end of child plans */
	if (haschildren)
	{
		ancestors = list_delete_first(ancestors);
		ExplainCloseGroup("Plans", "Plans", false, es);
	}

	/* in text format, undo whatever indentation we added */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		es->indent = save_indent;

	ExplainCloseGroup("Plan",
					  relationship ? NULL : "Plan",
					  true, es);

	es->currentSlice = save_currentSlice;
}


/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlans.
 */
static void
VecExplainSubPlans(List *plans, List *ancestors,
				   const char *relationship, ExplainState *es,
				   SliceTable *sliceTable)
{
	ListCell   *lst;
	ExecSlice  *saved_slice = es->currentSlice;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
        SubPlan    *sp = sps->subplan;
		int			qDispSliceId;

		if (es->pstmt->subplan_sliceIds)
			qDispSliceId = es->pstmt->subplan_sliceIds[sp->plan_id - 1];
		else
			qDispSliceId = -1;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		if (bms_is_member(sp->plan_id, es->printed_subplans))
			continue;
		es->printed_subplans = bms_add_member(es->printed_subplans,
											  sp->plan_id);

		/* Subplan might have its own root slice */
		if (sliceTable && qDispSliceId > 0)
		{
			es->currentSlice = &sliceTable->slices[qDispSliceId];
			es->subplanDispatchedSeparately = true;
		}
		else
			es->subplanDispatchedSeparately = false;

		if (sps->planstate == NULL)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "  ->  ");
			appendStringInfo(es->str, "UNUSED %s", sp->plan_name);
			appendStringInfo(es->str, "\n");
		}
		else
		{
			/*
			 * Treat the SubPlan node as an ancestor of the plan node(s) within
			 * it, so that ruleutils.c can find the referents of subplan
			 * parameters.
			 */
			ancestors = lcons(sp, ancestors);

			VecExplainNode(sps->planstate, ancestors,
						   relationship, sp->plan_name, es);

			ancestors = list_delete_first(ancestors);
		}
	}

	es->currentSlice = saved_slice;
}

/*
 * VecExplainPrintSettings -
 *    Print summary of modified settings affecting query planning.
 */
static void
VecExplainPrintSettings(ExplainState *es, PlanGenerator planGen)
{
	int			num;
	struct config_generic **gucs;

	/* request an array of relevant settings */
	gucs = get_explain_guc_options(&num, es->verbose, es->settings);

	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		VecExplainOpenGroup("Settings", "Settings", true, es);

		if (planGen == PLANGEN_PLANNER)
			ExplainPropertyStringInfo("Optimizer", es, "Postgres query optimizer");
#ifdef USE_ORCA
		else
			ExplainPropertyStringInfo("Optimizer", es, "Pivotal Optimizer (GPORCA)");
#endif

		for (int i = 0; i < num; i++)
		{
			char	   *setting;
			struct config_generic *conf = gucs[i];

			setting = GetConfigOptionByName(conf->name, NULL, true);

			ExplainPropertyText(conf->name, setting, es);
		}

		VecExplainCloseGroup("Settings", "Settings", true, es);
	}
	else
	{
		StringInfoData str;

		if (num <= 0)
			return;

		initStringInfo(&str);

		for (int i = 0; i < num; i++)
		{
			char	   *setting;
			struct config_generic *conf = gucs[i];

			if (i > 0)
				appendStringInfoString(&str, ", ");

			setting = GetConfigOptionByName(conf->name, NULL, true);

			if (setting)
				appendStringInfo(&str, "%s = '%s'", conf->name, setting);
			else
				appendStringInfo(&str, "%s = NULL", conf->name);
		}

		ExplainPropertyText("Settings", str.data, es);
	}
}

/*
 * Print per-worker info for current node, then free the ExplainWorkersState.
 */
static void
ExplainFlushWorkersState(ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	VecExplainOpenGroup("Workers", "Workers", false, es);
	for (int i = 0; i < wstate->num_workers; i++)
	{
		if (wstate->worker_inited[i])
		{
			/* This must match previous ExplainOpenSetAsideGroup call */
			VecExplainOpenGroup("Worker", NULL, true, es);
			appendStringInfoString(es->str, wstate->worker_str[i].data);
			VecExplainCloseGroup("Worker", NULL, true, es);

			pfree(wstate->worker_str[i].data);
		}
	}
	VecExplainCloseGroup("Workers", "Workers", false, es);

	pfree(wstate->worker_inited);
	pfree(wstate->worker_str);
	pfree(wstate->worker_state_save);
	pfree(wstate);
}

/*
 * Explain the constituent plans of an Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 */
static void
ExplainMemberNodes(PlanState **planstates, int nplans,
				   List *ancestors, ExplainState *es)
{
	int			j;

	for (j = 0; j < nplans; j++)
		VecExplainNode(planstates[j], ancestors,
					"Member", NULL, es);
}

/*
 * Report about any pruned subnodes of an Append or MergeAppend node.
 *
 * nplans indicates the number of live subplans.
 * nchildren indicates the original number of subnodes in the Plan;
 * some of these may have been pruned by the run-time pruning code.
 */
static void
ExplainMissingMembers(int nplans, int nchildren, ExplainState *es)
{
	if (nplans < nchildren || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyInteger("Subplans Removed", NULL,
							   nchildren - nplans, es);
}

/*
 * ExplainPrintJIT -
 *	  Append information about JITing to es->str.
 */
static void
ExplainPrintJIT(ExplainState *es, int jit_flags, JitInstrumentation *ji)
{
	instr_time	total_time;

	/* don't print information if no JITing happened */
	if (!ji || ji->created_functions == 0)
		return;

	/* calculate total time */
	INSTR_TIME_SET_ZERO(total_time);
	INSTR_TIME_ADD(total_time, ji->generation_counter);
	INSTR_TIME_ADD(total_time, ji->inlining_counter);
	INSTR_TIME_ADD(total_time, ji->optimization_counter);
	INSTR_TIME_ADD(total_time, ji->emission_counter);

	VecExplainOpenGroup("JIT", "JIT", true, es);

	/* for higher density, open code the text output format */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		ExplainIndentText(es);
		appendStringInfoString(es->str, "JIT:\n");
		es->indent++;

		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		ExplainIndentText(es);
		appendStringInfo(es->str, "Options: %s %s, %s %s, %s %s, %s %s\n",
						 "Inlining", jit_flags & PGJIT_INLINE ? "true" : "false",
						 "Optimization", jit_flags & PGJIT_OPT3 ? "true" : "false",
						 "Expressions", jit_flags & PGJIT_EXPR ? "true" : "false",
						 "Deforming", jit_flags & PGJIT_DEFORM ? "true" : "false");

		if (es->analyze && es->timing)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Timing: %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms\n",
							 "Generation", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
							 "Inlining", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
							 "Optimization", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
							 "Emission", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
							 "Total", 1000.0 * INSTR_TIME_GET_DOUBLE(total_time));
		}

		es->indent--;
	}
	else
	{
		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		VecExplainOpenGroup("Options", "Options", true, es);
		ExplainPropertyBool("Inlining", jit_flags & PGJIT_INLINE, es);
		ExplainPropertyBool("Optimization", jit_flags & PGJIT_OPT3, es);
		ExplainPropertyBool("Expressions", jit_flags & PGJIT_EXPR, es);
		ExplainPropertyBool("Deforming", jit_flags & PGJIT_DEFORM, es);
		VecExplainCloseGroup("Options", "Options", true, es);

		if (es->analyze && es->timing)
		{
			VecExplainOpenGroup("Timing", "Timing", true, es);

			ExplainPropertyFloat("Generation", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
								 3, es);
			ExplainPropertyFloat("Inlining", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
								 3, es);
			ExplainPropertyFloat("Optimization", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
								 3, es);
			ExplainPropertyFloat("Emission", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
								 3, es);
			ExplainPropertyFloat("Total", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(total_time),
								 3, es);

			VecExplainCloseGroup("Timing", "Timing", true, es);
		}
	}

	VecExplainCloseGroup("JIT", "JIT", true, es);
}
/*
 * Begin or resume output into the set-aside group for worker N.
 */
static void
ExplainOpenWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);

	/* Save prior output buffer pointer */
	wstate->prev_str = es->str;

	if (!wstate->worker_inited[n])
	{
		/* First time through, so create the buffer for this worker */
		initStringInfo(&wstate->worker_str[n]);
		es->str = &wstate->worker_str[n];

		/*
		 * Push suitable initial formatting state for this worker's field
		 * group.  We allow one extra logical nesting level, since this group
		 * will eventually be wrapped in an outer "Workers" group.
		 */
		ExplainOpenSetAsideGroup("Worker", NULL, true, 2, es);

		/*
		 * In non-TEXT formats we always emit a "Worker Number" field, even if
		 * there's no other data for this worker.
		 */
		if (es->format != EXPLAIN_FORMAT_TEXT)
			ExplainPropertyInteger("Worker Number", NULL, n, es);

		wstate->worker_inited[n] = true;
	}
	else
	{
		/* Resuming output for a worker we've already emitted some data for */
		es->str = &wstate->worker_str[n];

		/* Restore formatting state saved by last ExplainCloseWorker() */
		ExplainRestoreGroup(es, 2, &wstate->worker_state_save[n]);
	}

	/*
	 * In TEXT format, prefix the first output line for this worker with
	 * "Worker N:".  Then, any additional lines should be indented one more
	 * stop than the "Worker N" line is.
	 */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (es->str->len == 0)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "Worker %d:  ", n);
		}

		es->indent++;
	}
}

/*
 * End output for worker N --- must pair with previous ExplainOpenWorker call
 */
static void
ExplainCloseWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);
	Assert(wstate->worker_inited[n]);

	/*
	 * Save formatting state in case we do another ExplainOpenWorker(), then
	 * pop the formatting stack.
	 */
	ExplainSaveGroup(es, 2, &wstate->worker_state_save[n]);

	/*
	 * In TEXT format, if we didn't actually produce any output line(s) then
	 * truncate off the partial line emitted by ExplainOpenWorker.  (This is
	 * to avoid bogus output if, say, show_buffer_usage chooses not to print
	 * anything for the worker.)  Also fix up the indent level.
	 */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		while (es->str->len > 0 && es->str->data[es->str->len - 1] != '\n')
			es->str->data[--(es->str->len)] = '\0';

		es->indent--;
	}

	/* Restore prior output buffer pointer */
	es->str = wstate->prev_str;
}
/*
 * Pop one level of grouping state, allowing for a re-push later.
 *
 * This is typically used after ExplainOpenSetAsideGroup; pass the
 * same "depth" used for that.
 *
 * This should not emit any output.  If state needs to be saved,
 * save it at *state_save.  Currently, an integer save area is sufficient
 * for all formats, but we might need to revisit that someday.
 */
static void
ExplainSaveGroup(ExplainState *es, int depth, int *state_save)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent -= depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent -= depth;
			*state_save = linitial_int(es->grouping_stack);
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent -= depth;
			*state_save = linitial_int(es->grouping_stack);
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Re-push one level of grouping state, undoing the effects of ExplainSaveGroup.
 */
static void
ExplainRestoreGroup(ExplainState *es, int depth, int *state_save)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->grouping_stack = lcons_int(*state_save, es->grouping_stack);
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = lcons_int(*state_save, es->grouping_stack);
			es->indent += depth;
			break;
	}
}

/*
 * Open a group of related objects, without emitting actual data.
 *
 * Prepare the formatting state as though we were beginning a group with
 * the identified properties, but don't actually emit anything.  Output
 * subsequent to this call can be redirected into a separate output buffer,
 * and then eventually appended to the main output buffer after doing a
 * regular VecExplainOpenGroup call (with the same parameters).
 *
 * The extra "depth" parameter is the new group's depth compared to current.
 * It could be more than one, in case the eventual output will be enclosed
 * in additional nesting group levels.  We assume we don't need to track
 * formatting state for those levels while preparing this group's output.
 *
 * There is no ExplainCloseSetAsideGroup --- in current usage, we always
 * pop this state with ExplainSaveGroup.
 */
static void
ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
						 bool labeled, int depth, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_YAML:
			if (labelname)
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			else
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent += depth;
			break;
	}
}

/*
 * cdbexplain_showExecStats
 *	  Called by qDisp process to format a node's EXPLAIN ANALYZE statistics.
 *
 * 'planstate' is the node whose statistics are to be displayed.
 * 'str' is the output buffer.
 * 'indent' is the root indentation for all the text generated for explain output
 * 'ctx' is a CdbExplain_ShowStatCtx object which was created by a call to
 *		cdbexplain_showExecStatsBegin().
 */
static void
cdbexplain_showExecStats(struct PlanState *planstate, ExplainState *es)
{
	struct CdbExplain_ShowStatCtx *ctx = es->showstatctx;
	Instrumentation *instr = planstate->instrument;
	CdbExplain_NodeSummary *ns = es->runtime? instr->rt_cdbNodeSummary : instr->cdbNodeSummary;
	instr_time	timediff;
	int			i;

	char		totalbuf[50];
	char		avgbuf[50];
	char		maxbuf[50];
	char		segbuf[50];
	char		startbuf[50];

	/* Might not have received stats from qExecs if they hit errors. */
	if (!ns)
		return;

	Assert(instr != NULL);

	/*
	 * Executor memory used by this individual node, if it allocates from a
	 * memory context of its own instead of sharing the per-query context.
	 */
	if (es->analyze && ns->execmemused.vcnt > 0)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Executor Memory: %ldkB  Segments: %d  Max: %ldkB (segment %d)\n",
							 (long) kb(ns->execmemused.vsum),
							 ns->execmemused.vcnt,
							 (long) kb(ns->execmemused.vmax),
							 ns->execmemused.imax);
		}
		else
		{
			ExplainPropertyInteger("Executor Memory", "kB", kb(ns->execmemused.vsum), es);
			ExplainPropertyInteger("Executor Memory Segments", NULL, ns->execmemused.vcnt, es);
			ExplainPropertyInteger("Executor Max Memory", "kB", kb(ns->execmemused.vmax), es);
			ExplainPropertyInteger("Executor Max Memory Segment", NULL, ns->execmemused.imax, es);
		}
	}

	/*
	 * Actual work_mem used and wanted
	 */
	if (es->analyze && es->verbose && ns->workmemused.vcnt > 0)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "work_mem: %ldkB  Segments: %d  Max: %ldkB (segment %d)",
							 (long) kb(ns->workmemused.vsum),
							 ns->workmemused.vcnt,
							 (long) kb(ns->workmemused.vmax),
							 ns->workmemused.imax);

			/*
			 * Total number of segments in which this node reuses cached or
			 * creates workfiles.
			 */
			if (nodeSupportWorkfileCaching(planstate))
				appendStringInfo(es->str, "  Workfile: (%d spilling)",
								 ns->totalWorkfileCreated.vcnt);

			appendStringInfo(es->str, "\n");

			if (ns->workmemwanted.vcnt > 0)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ns->workmemwanted.vmax);
				if (ns->ninst == 1)
				{
					appendStringInfo(es->str,
								 "Work_mem wanted: %s to lessen workfile I/O.",
								 maxbuf);
				}
				else
				{
					cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ns->workmemwanted));
					cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->workmemwanted.imax, ns->ninst);
					appendStringInfo(es->str,
									 "Work_mem wanted: %s avg, %s max%s"
									 " to lessen workfile I/O affecting %d workers.",
									 avgbuf, maxbuf, segbuf, ns->workmemwanted.vcnt);
				}

				appendStringInfo(es->str, "\n");
			}
		}
		else
		{
			VecExplainOpenGroup("work_mem", "work_mem", true, es);
			ExplainPropertyInteger("Used", "kB", kb(ns->workmemused.vsum), es);
			ExplainPropertyInteger("Segments", NULL, ns->workmemused.vcnt, es);
			ExplainPropertyInteger("Max Memory", "kB", kb(ns->workmemused.vmax), es);
			ExplainPropertyInteger("Max Memory Segment", NULL, ns->workmemused.imax, es);

			/*
			 * Total number of segments in which this node reuses cached or
			 * creates workfiles.
			 */
			if (nodeSupportWorkfileCaching(planstate))
				ExplainPropertyInteger("Workfile Spilling", NULL, ns->totalWorkfileCreated.vcnt, es);

			if (ns->workmemwanted.vcnt > 0)
			{
				ExplainPropertyInteger("Max Memory Wanted", "kB", kb(ns->workmemwanted.vmax), es);

				if (ns->ninst > 1)
				{
					ExplainPropertyInteger("Max Memory Wanted Segment", NULL, ns->workmemwanted.imax, es);
					ExplainPropertyInteger("Avg Memory Wanted", "kB", kb(cdbexplain_agg_avg(&ns->workmemwanted)), es);
					ExplainPropertyInteger("Segments Affected", NULL, ns->ninst, es);
				}
			}

			VecExplainCloseGroup("work_mem", "work_mem", true, es);
		}
	}

	bool 			haveExtraText = false;
	StringInfoData	extraData;

	initStringInfo(&extraData);

	for (i = 0; i < ns->ninst; i++)
	{
		CdbExplain_StatInst *nsi = &ns->insts[i];

		if (nsi->bnotes < nsi->enotes)
		{
			if (!haveExtraText)
			{
				VecExplainOpenGroup("Extra Text", "Extra Text", false, es);
				VecExplainOpenGroup("Segment", NULL, true, es);
				haveExtraText = true;
			}
			
			resetStringInfo(&extraData);

			cdbexplain_formatExtraText(&extraData,
									   0,
									   (ns->ninst == 1) ? -1
									   : ns->segindex0 + i,
									   ctx->extratextbuf.data + nsi->bnotes,
									   nsi->enotes - nsi->bnotes);
			ExplainPropertyStringInfo("Extra Text", es, "%s", extraData.data);
		}
	}

	if (haveExtraText)
	{
		VecExplainCloseGroup("Segment", NULL, true, es);
		VecExplainCloseGroup("Extra Text", "Extra Text", false, es);
	}
	pfree(extraData.data);

	/*
	 * Dump stats for all workers.
	 */
	if (gp_enable_explain_allstat && ns->segindex0 >= 0 && ns->ninst > 0)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			/*
			 * create a header for all stats: separate each individual stat by an
			 * underscore, separate the grouped stats for each node by a slash
			 */
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str,
								   "allstat: seg_firststart_total_ntuples");
		}
		else
			VecExplainOpenGroup("Allstat", "Allstat", true, es);

		for (i = 0; i < ns->ninst; i++)
		{
			CdbExplain_StatInst *nsi = &ns->insts[i];

			if (INSTR_TIME_IS_ZERO(nsi->firststart))
				continue;

			/* Time from start of query on qDisp to worker's first result row */
			INSTR_TIME_SET_ZERO(timediff);
			INSTR_TIME_ACCUM_DIFF(timediff, nsi->firststart, ctx->querystarttime);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				cdbexplain_formatSeconds(startbuf, sizeof(startbuf),
										 INSTR_TIME_GET_DOUBLE(timediff), true);
				cdbexplain_formatSeconds(totalbuf, sizeof(totalbuf),
										 nsi->total, true);
				appendStringInfo(es->str,
								 "/seg%d_%s_%s_%.0f",
								 ns->segindex0 + i,
								 startbuf,
								 totalbuf,
								 nsi->ntuples);
			}
			else
			{
				cdbexplain_formatSeconds(startbuf, sizeof(startbuf),
										 INSTR_TIME_GET_DOUBLE(timediff), false);
				cdbexplain_formatSeconds(totalbuf, sizeof(totalbuf),
										 nsi->total, false);

				VecExplainOpenGroup("Segment", NULL, false, es);
				ExplainPropertyInteger("Segment index", NULL, ns->segindex0 + i, es);
				ExplainPropertyText("Time To First Result", startbuf, es);
				ExplainPropertyText("Time To Total Result", totalbuf, es);
				ExplainPropertyFloat("Tuples", NULL, nsi->ntuples, 1, es);
				VecExplainCloseGroup("Segment", NULL, false, es);
			}
		}

		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, "//end\n");
		else
			VecExplainCloseGroup("Allstat", "Allstat", true, es);
	}
}								/* cdbexplain_showExecStats */

/*
 * cdbexplain_formatMemory
 *	  Convert memory size to string from (double) bytes.
 *
 *		outbuf:  [output] pointer to a char buffer to be filled
 *		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 *		bytes:	 [input] a value representing memory size in bytes to be written to outbuf
 */
static void
cdbexplain_formatMemory(char *outbuf, int bufsize, double bytes)
{
	Assert(outbuf != NULL && "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 && "CDBEXPLAIN: size of char buffer is zero");
	/* check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
	int			nchars_written =
#endif							/* USE_ASSERT_CHECKING */
	snprintf(outbuf, bufsize, "%.0fK bytes", kb(bytes));

	Assert(nchars_written < bufsize &&
		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
}								/* cdbexplain_formatMemory */

/*
 * cdbexplain_formatSeg
 *	  Convert segment id to string.
 *
 *		outbuf:  [output] pointer to a char buffer to be filled
 *		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 *		segindex:[input] a value representing segment index to be written to outbuf
 *		nInst:	 [input] no. of stat instances
 */
static void
cdbexplain_formatSeg(char *outbuf, int bufsize, int segindex, int nInst)
{
	Assert(outbuf != NULL && "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 && "CDBEXPLAIN: size of char buffer is zero");

	if (nInst > 1 && segindex >= 0)
	{
		/* check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
		int			nchars_written =
#endif							/* USE_ASSERT_CHECKING */
		snprintf(outbuf, bufsize, " (seg%d)", segindex);

		Assert(nchars_written < bufsize &&
			   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
	}
	else
	{
		outbuf[0] = '\0';
	}
}								/* cdbexplain_formatSeg */


/*
 * cdbexplain_formatSeconds
 *	  Convert time in seconds to readable string
 *
 *		outbuf:  [output] pointer to a char buffer to be filled
 *		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 *		seconds: [input] a value representing no. of seconds to be written to outbuf
 */
static void
cdbexplain_formatSeconds(char *outbuf, int bufsize, double seconds, bool unit)
{
	Assert(outbuf != NULL && "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 && "CDBEXPLAIN: size of char buffer is zero");
	double		ms = seconds * 1000.0;

	/* check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
	int			nchars_written =
#endif							/* USE_ASSERT_CHECKING */
	snprintf(outbuf, bufsize, "%.*f%s",
			 (ms < 10.0 && ms != 0.0 && ms > -10.0) ? 3 : 0,
			 ms, (unit ? " ms" : ""));

	Assert(nchars_written < bufsize &&
		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
}								/* cdbexplain_formatSeconds */

/*
 * cdbexplain_formatExtraText
 *	  Format extra message text into the EXPLAIN output buffer.
 */
static void
cdbexplain_formatExtraText(StringInfo str,
						   int indent,
						   int segindex,
						   const char *notes,
						   int notelen)
{
	const char *cp = notes;
	const char *ep = notes + notelen;

	/* Could be more than one line... */
	while (cp < ep)
	{
		const char *nlp = memchr(cp, '\n', ep - cp);
		const char *dp = nlp ? nlp : ep;

		/* Strip trailing whitespace. */
		while (cp < dp &&
			   isspace(dp[-1]))
			dp--;

		/* Add to output buffer. */
		if (cp < dp)
		{
			appendStringInfoSpaces(str, indent * 2);
			if (segindex >= 0)
			{
				appendStringInfo(str, "(seg%d) ", segindex);
				if (segindex < 10)
					appendStringInfoChar(str, ' ');
				if (segindex < 100)
					appendStringInfoChar(str, ' ');
			}
			appendBinaryStringInfo(str, cp, dp - cp);
			if (nlp)
				appendStringInfoChar(str, '\n');
		}

		if (!nlp)
			break;
		cp = nlp + 1;
	}
}								/* cdbexplain_formatExtraText */

/*
 * nodeSupportWorkfileCaching
 *	 Return true if a given node supports workfile caching.
 */
static bool
nodeSupportWorkfileCaching(PlanState *planstate)
{
	return (IsA(planstate, SortState) ||
			IsA(planstate, HashJoinState) ||
			(IsA(planstate, AggState) &&((Agg *) planstate->plan)->aggstrategy == AGG_HASHED) ||
			IsA(planstate, MaterialState));
}

/*
 * Indent a text-format line.
 *
 * We indent by two spaces per indentation level.  However, when emitting
 * data for a parallel worker there might already be data on the current line
 * (cf. ExplainOpenWorker); in that case, don't indent any more.
 */
static void
ExplainIndentText(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_TEXT);
	if (es->str->len == 0 || es->str->data[es->str->len - 1] == '\n')
		appendStringInfoSpaces(es->str, es->indent * 2);
}


static void
show_dispatch_info(ExecSlice *slice, ExplainState *es, Plan *plan)
{
	int			segments;

	/*
	 * In non-parallel query, there is no slice information.
	 */
	if (!slice)
		return;

	switch (slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
		case GANGTYPE_ENTRYDB_READER:
			segments = 0;
			break;

		case GANGTYPE_PRIMARY_WRITER:
		case GANGTYPE_PRIMARY_READER:
		case GANGTYPE_SINGLETON_READER:
		{
			segments = list_length(slice->segments);
			break;
		}

		default:
			segments = 0;		/* keep compiler happy */
			Assert(false);
			break;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (segments == 0)
			appendStringInfo(es->str, "  (slice%d)", slice->sliceIndex);
		else if (slice->primaryGang && gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
			/*
			 * In gpdb 5 there was a unique gang_id for each gang, this was
			 * retired since gpdb 6, so we use the qe identifier from the first
			 * segment of the gang to identify each gang.
			 */
			appendStringInfo(es->str, "  (slice%d; gang%d; segments: %d)",
							 slice->sliceIndex,
							 slice->primaryGang->db_descriptors[0]->identifier,
							 segments);
		else
			appendStringInfo(es->str, "  (slice%d; segments: %d)",
							 slice->sliceIndex, segments);
	}
	else
	{
		ExplainPropertyInteger("Slice", NULL, slice->sliceIndex, es);
		if (slice->primaryGang && gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
			ExplainPropertyInteger("Gang", NULL, slice->primaryGang->db_descriptors[0]->identifier, es);
		ExplainPropertyInteger("Segments", NULL, segments, es);
		ExplainPropertyText("Gang Type", gangTypeToString(slice->gangType), es);
	}
}

/*
 * Create a per-plan-node workspace for collecting per-worker data.
 *
 * Output related to each worker will be temporarily "set aside" into a
 * separate buffer, which we'll merge into the main output stream once
 * we've processed all data for the plan node.  This makes it feasible to
 * generate a coherent sub-group of fields for each worker, even though the
 * code that produces the fields is in several different places in this file.
 * Formatting of such a set-aside field group is managed by
 * ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup.
 */
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
{
	ExplainWorkersState *wstate;

	wstate = (ExplainWorkersState *) palloc(sizeof(ExplainWorkersState));
	wstate->num_workers = num_workers;
	wstate->worker_inited = (bool *) palloc0(num_workers * sizeof(bool));
	wstate->worker_str = (StringInfoData *)
		palloc0(num_workers * sizeof(StringInfoData));
	wstate->worker_state_save = (int *) palloc(num_workers * sizeof(int));
	return wstate;
}
/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabeled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ExplainYAMLLineStarting(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_YAML);
	if (linitial_int(es->grouping_stack) == 0)
	{
		linitial_int(es->grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
	}
}

static void
ExplainPropertyStringInfo(const char *qlabel, ExplainState *es, const char *fmt,...)
{
	StringInfoData buf;

	initStringInfo(&buf);

	for (;;)
	{
		va_list		args;
		int			needed;

		/* Try to format the data. */
		va_start(args, fmt);
		needed = appendStringInfoVA(&buf, fmt, args);
		va_end(args);

		if (needed == 0)
			break;

		/* Double the buffer size and try again. */
		enlargeStringInfo(&buf, needed);
	}

	ExplainPropertyText(qlabel, buf.data, es);
	pfree(buf.data);
}

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
	const char *s;
	const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(es->str, 2 * es->indent);
	appendStringInfoCharMacro(es->str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(es->str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoChar(es->str, strchr(valid, *s) ? *s : '-');
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(es->str, " /");
	appendStringInfoCharMacro(es->str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(es->str, '\n');
}
/*
 * cdbexplain_showExecStatsEnd
 *	  Called by qDisp process to format the overall statistics for a query
 *	  into the caller's buffer.
 *
 * 'ctx' is the CdbExplain_ShowStatCtx object which was created by a call to
 *		cdbexplain_showExecStatsBegin() and contains statistics which have
 *		been accumulated over a series of calls to cdbexplain_showExecStats().
 *		Invalid on return (it is freed).
 *
 * This doesn't free the CdbExplain_ShowStatCtx object or buffers, because
 * they will be free'd shortly by the end of statement anyway.
 */
static void
cdbexplain_showExecStatsEnd(struct PlannedStmt *stmt,
							struct CdbExplain_ShowStatCtx *showstatctx,
							struct EState *estate,
							ExplainState *es)
{
	if (!es->summary)
		return;

    gpexplain_formatSlicesOutput(showstatctx, estate, es);

	if (!IsResManagerMemoryPolicyNone())
	{
		ExplainOpenGroup("Statement statistics", "Statement statistics", true, es);
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfo(es->str, "Memory used:  %ldkB\n", (long) kb(stmt->query_mem));
		else
			ExplainPropertyInteger("Memory used", "kB", kb(stmt->query_mem), es);

		if (showstatctx->workmemwanted_max > 0)
		{
			long mem_wanted;

			mem_wanted = (long) PolicyAutoStatementMemForNoSpill(stmt,
							(uint64) showstatctx->workmemwanted_max);

			/*
			 * Round up to a kilobyte in case we end up requiring less than
			 * that.
			 */
			if (mem_wanted <= 1024L)
				mem_wanted = 1L;
			else
				mem_wanted = mem_wanted / 1024L;

			if (es->format == EXPLAIN_FORMAT_TEXT)
				appendStringInfo(es->str, "Memory wanted:  %ldkB\n", mem_wanted);
			else
				ExplainPropertyInteger("Memory wanted", "kB", mem_wanted, es);
		}

		ExplainCloseGroup("Statement statistics", "Statement statistics", true, es);
	}
}								/* cdbexplain_showExecStatsEnd */

/*
 * Given a statistics context search for all the slice statistics
 * and format them to the correct layout
 */
static void
gpexplain_formatSlicesOutput(struct CdbExplain_ShowStatCtx *showstatctx,
                             struct EState *estate,
                             ExplainState *es)
{
	ExecSlice  *slice;
	int			sliceIndex;
	int			flag;
	double		total_memory_across_slices = 0;

	char		avgbuf[50];
	char		maxbuf[50];
	char		segbuf[50];

    if (showstatctx->nslice > 0)
        ExplainOpenGroup("Slice statistics", "Slice statistics", false, es);

    for (sliceIndex = 0; sliceIndex < showstatctx->nslice; sliceIndex++)
    {
        CdbExplain_SliceSummary *ss = &showstatctx->slices[sliceIndex];
        CdbExplain_DispatchSummary *ds = &ss->dispatchSummary;
        
        flag = es->str->len;
        if (es->format == EXPLAIN_FORMAT_TEXT)
        {

            appendStringInfo(es->str, "  (slice%d) ", sliceIndex);
            if (sliceIndex < 10)
                appendStringInfoChar(es->str, ' ');

            appendStringInfoString(es->str, "  ");
        }
        else 
        {
            ExplainOpenGroup("Slice", NULL, true, es);
            ExplainPropertyInteger("Slice", NULL, sliceIndex, es);
        }

        /* Worker counts */
        slice = getCurrentSlice(estate, sliceIndex);
        if (slice &&
			list_length(slice->segments) > 0 &&
			list_length(slice->segments) != ss->dispatchSummary.nOk)
        {
			int			nNotDispatched;
			StringInfoData workersInformationText;

			nNotDispatched = list_length(slice->segments) - ds->nResult + ds->nNotDispatched;

			es->str->data[flag] = (ss->dispatchSummary.nError > 0) ? 'X' : '_';

			initStringInfo(&workersInformationText);
			appendStringInfo(&workersInformationText, "Workers:");

            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                if (ds->nError == 1)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d error;",
                                     ds->nError);
                }
                else if (ds->nError > 1)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d errors;",
                                     ds->nError);
                }
            }
            else
            {
                ExplainOpenGroup("Workers", "Workers", true, es);
                if (ds->nError > 0)
                    ExplainPropertyInteger("Errors", NULL, ds->nError, es);
            }

            if (ds->nCanceled > 0)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d canceled;",
                                     ds->nCanceled);
                }
                else
                {
                    ExplainPropertyInteger("Canceled", NULL, ds->nCanceled, es);
                }
            }

            if (nNotDispatched > 0)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d not dispatched;",
                                     nNotDispatched);
                }
                else
                {
                    ExplainPropertyInteger("Not Dispatched", NULL, nNotDispatched, es);
                }
            }

            if (ds->nIgnorableError > 0)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d aborted;",
                                     ds->nIgnorableError);
                }
                else
                {
                    ExplainPropertyInteger("Aborted", NULL, ds->nIgnorableError, es);
                }
            }

            if (ds->nOk > 0)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    appendStringInfo(&workersInformationText,
                                     " %d ok;",
                                     ds->nOk);
                }
                else
                {
                    ExplainPropertyInteger("Ok", NULL, ds->nOk, es);
                }
            }

            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                workersInformationText.len--;
                ExplainPropertyStringInfo("Workers", es, "%s.  ", workersInformationText.data);
            }
            else
            {
                ExplainCloseGroup("Workers", "Workers", true, es);
            }
        }

        /* Executor memory high-water mark */
        cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->peakmemused.vmax);
        if (ss->peakmemused.vcnt == 1)
        {
            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                const char *seg = segbuf;

                if (ss->peakmemused.imax >= 0)
                {
                    cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->peakmemused.imax, 999);
                }
                else if (slice && list_length(slice->segments) > 0)
                {
                    seg = " (entry db)";
                }
                else
                {
                    seg = "";
                }
                appendStringInfo(es->str,
                                 "Executor memory: %s%s.",
                                 maxbuf,
                                 seg);
            }
            else
            {
                ExplainPropertyInteger("Executor Memory", "kB", ss->peakmemused.vmax, es);
            }
        }
        else if (ss->peakmemused.vcnt > 1)
        {
            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ss->peakmemused));
                cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->peakmemused.imax, ss->nworker);
                appendStringInfo(es->str,
                                 "Executor memory: %s avg x %d workers, %s max%s.",
                                 avgbuf,
                                 ss->peakmemused.vcnt,
                                 maxbuf,
                                 segbuf);
            }
            else
            {
                ExplainOpenGroup("Executor Memory", "Executor Memory", true, es);
                ExplainPropertyInteger("Average", "kB", cdbexplain_agg_avg(&ss->peakmemused), es);
                ExplainPropertyInteger("Workers", NULL, ss->peakmemused.vcnt, es);
                ExplainPropertyInteger("Maximum Memory Used", "kB", ss->peakmemused.vmax, es);
                ExplainCloseGroup("Executor Memory", "Executor Memory", true, es);
            }
        }

        if (EXPLAIN_MEMORY_VERBOSITY_SUPPRESS < explain_memory_verbosity)
        {
            /* Vmem reserved by QEs */
            cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->vmem_reserved.vmax);
            if (ss->vmem_reserved.vcnt == 1)
            {

                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    const char *seg = segbuf;

                    if (ss->vmem_reserved.imax >= 0)
                    {
                        cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->vmem_reserved.imax, 999);
                    }
                    else if (slice && list_length(slice->segments) > 0)
                    {
                        seg = " (entry db)";
                    }
                    else
                    {
                        seg = "";
                    }
                    appendStringInfo(es->str,
                                     "  Vmem reserved: %s%s.",
                                     maxbuf,
                                     seg);
                }
                else
                {
                    ExplainPropertyInteger("Virtual Memory", "kB", ss->vmem_reserved.vmax, es);
                }
            }
            else if (ss->vmem_reserved.vcnt > 1)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                {
                    cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ss->vmem_reserved));
                    cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->vmem_reserved.imax, ss->nworker);
                    appendStringInfo(es->str,
                                     "  Vmem reserved: %s avg x %d workers, %s max%s.",
                                     avgbuf,
                                     ss->vmem_reserved.vcnt,
                                     maxbuf,
                                     segbuf);
                }
                else
                {
                    ExplainOpenGroup("Virtual Memory", "Virtual Memory", true, es);
                    ExplainPropertyInteger("Average", "kB", cdbexplain_agg_avg(&ss->vmem_reserved), es);
                    ExplainPropertyInteger("Workers", NULL, ss->vmem_reserved.vcnt, es);
                    ExplainPropertyInteger("Maximum Memory Used", "kB", ss->vmem_reserved.vmax, es);
                    ExplainCloseGroup("Virtual Memory", "Virtual Memory", true, es);
                }

            }
        }

        /* Work_mem used/wanted (max over all nodes and workers of slice) */
        if (ss->workmemused_max + ss->workmemwanted_max > 0)
        {
            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->workmemused_max);
                appendStringInfo(es->str, "  Work_mem: %s max", maxbuf);
                if (ss->workmemwanted_max > 0)
                {
                    es->str->data[flag] = '*';	/* draw attention to this slice */
                    cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->workmemwanted_max);
                    appendStringInfo(es->str, ", %s wanted", maxbuf);
                }
                appendStringInfoChar(es->str, '.');
            }
            else
            {
                ExplainPropertyInteger("Work Maximum Memory", "kB", ss->workmemused_max, es);
            }
        }

        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoChar(es->str, '\n');

        ExplainCloseGroup("Slice", NULL, true, es);
    }

    if (showstatctx->nslice > 0)
        ExplainCloseGroup("Slice statistics", "Slice statistics", false, es);

    if (total_memory_across_slices > 0)
    {
        if (es->format == EXPLAIN_FORMAT_TEXT)
        {
            appendStringInfo(es->str, "Total memory used across slices: %.0fK bytes \n", total_memory_across_slices);
        }
        else
        {
            ExplainPropertyInteger("Total memory used across slices", "bytes", total_memory_across_slices, es);
        }
    }
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ExplainJSONLineEnding(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_JSON);
	if (linitial_int(es->grouping_stack) != 0)
		appendStringInfoChar(es->str, ',');
	else
		linitial_int(es->grouping_stack) = 1;
	appendStringInfoChar(es->str, '\n');
}						

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}

/*
 * Explain a list of children of a CustomScan.
 */
static void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es)
{
	ListCell   *cell;
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		VecExplainNode((PlanState *) lfirst(cell), ancestors, label, NULL, es);
}


/*
 * Show the targetlist of a plan node
 */
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	bool		useprefix;
	ListCell   *lc;

	/* No work if empty tlist (this occurs eg in bitmap indexscans) */
	if (plan->targetlist == NIL)
		return;
	/* The tlist of an Append isn't real helpful, so suppress it */
	if (IsA(plan, Append))
		return;
	/* Likewise for MergeAppend and RecursiveUnion */
	if (IsA(plan, MergeAppend))
		return;
	if (IsA(plan, RecursiveUnion))
		return;

	/*
	 * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
	 *
	 * Note: the tlist for a ForeignScan that executes a direct INSERT/UPDATE
	 * might contain subplan output expressions that are confusing in this
	 * context.  The tlist for a ForeignScan that executes a direct UPDATE/
	 * DELETE always contains "junk" target columns to identify the exact row
	 * to update or delete, which would be confusing in this context.  So, we
	 * suppress it in all the cases.
	 */
	if (IsA(plan, ForeignScan) &&
		((ForeignScan *) plan)->operation != CMD_SELECT)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   plan,
									   ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Deparse each result column (we now include resjunk ones) */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		result = lappend(result,
						 deparse_expression((Node *) tle->expr, context,
											useprefix, false));
	}

	/* Print results */
	ExplainPropertyList("Output", result, es);
}

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es)
{
	Node	   *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void
show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es)
{
	bool		useprefix;

	useprefix = (list_length(es->rtable) > 1 || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show the sort keys for a Sort node.
 */
static void
show_sort_keys(SortState *sortstate, List *ancestors, ExplainState *es)
{
	Sort	   *plan = (Sort *) sortstate->ss.ps.plan;
	const char *SortKeystr;

	if (sortstate->noduplicates)
		SortKeystr = "Sort Key (Distinct)";
	else
		SortKeystr = "Sort Key";

	show_sort_group_keys((PlanState *) sortstate, SortKeystr,
						 plan->numCols, 0, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

static void
show_windowagg_keys(WindowAggState *waggstate, List *ancestors, ExplainState *es)
{
	WindowAgg *window = (WindowAgg *) waggstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(window, ancestors);
	if ( window->partNumCols > 0 )
	{
		show_sort_group_keys((PlanState *) outerPlanState(waggstate), "Partition By",
							 window->partNumCols, 0, window->partColIdx,
							 NULL, NULL, NULL,
							 ancestors, es);
	}

	show_sort_group_keys((PlanState *) outerPlanState(waggstate), "Order By",
						 window->ordNumCols, 0, window->ordColIdx,
						 NULL, NULL, NULL,
						 ancestors, es);
	ancestors = list_delete_first(ancestors);

	/* XXX don't show framing for now */
}



/*
 * Show the sort keys for a IncrementalSort node.
 */
static void
show_incremental_sort_keys(IncrementalSortState *incrsortstate,
						   List *ancestors, ExplainState *es)
{
	IncrementalSort *plan = (IncrementalSort *) incrsortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) incrsortstate, "Sort Key",
						 plan->sort.numCols, plan->nPresortedCols,
						 plan->sort.sortColIdx,
						 plan->sort.sortOperators, plan->sort.collations,
						 plan->sort.nullsFirst,
						 ancestors, es);
}

/*
 * Likewise, for a MergeAppend node.
 */
static void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es)
{
	MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

	show_sort_group_keys((PlanState *) mstate, "Sort Key",
						 plan->numCols, 0, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Show the Split key for an SplitTuple
 */
static void
show_tuple_split_keys(TupleSplitState *tstate, List *ancestors,
					  ExplainState *es)
{
	TupleSplit *plan = (TupleSplit *)tstate->ss.ps.plan;

	ancestors = lcons(tstate, ancestors);

	List	   *context;
	bool		useprefix;
	List	   *result = NIL;
	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   (Plan *) plan,
									   ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	StringInfoData buf;
	initStringInfo(&buf);

	ListCell *lc;
	foreach(lc, plan->dqa_expr_lst)
	{
		DQAExpr *dqa_expr = (DQAExpr *)lfirst(lc);
		result = lappend(result,
		                 deparse_expression((Node *) dqa_expr, context,
		                                    useprefix, true));
	}
	ExplainPropertyList("Split by Col", result, es);

	if (plan->numCols > 0)
		show_sort_group_keys(outerPlanState(tstate), "Group Key",
							 plan->numCols, 0, plan->grpColIdx,
							 NULL, NULL, NULL,
							 ancestors, es);

	ancestors = list_delete_first(ancestors);
}

/*
 * Show the grouping keys for an Agg node.
 */
static void
show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es)
{
	Agg		   *plan = (Agg *) astate->ss.ps.plan;

	if (plan->numCols > 0 || plan->groupingSets)
	{
		/* The key columns refer to the tlist of the child plan */
		ancestors = lcons(plan, ancestors);

		if (plan->groupingSets)
			show_grouping_sets(outerPlanState(astate), plan, ancestors, es);
		else
			show_sort_group_keys(outerPlanState(astate), "Group Key",
								 plan->numCols, 0, plan->grpColIdx,
								 NULL, NULL, NULL,
								 ancestors, es);

		ancestors = list_delete_first(ancestors);
	}
}

static void
show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	ExplainOpenGroup("Grouping Sets", "Grouping Sets", false, es);

	show_grouping_set_keys(planstate, agg, NULL,
						   context, useprefix, ancestors, es);

	foreach(lc, agg->chain)
	{
		Agg		   *aggnode = lfirst(lc);
		Sort	   *sortnode = (Sort *) aggnode->plan.lefttree;

		show_grouping_set_keys(planstate, aggnode, sortnode,
							   context, useprefix, ancestors, es);
	}

	ExplainCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

static void
show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	char	   *exprstr;
	ListCell   *lc;
	List	   *gsets = aggnode->groupingSets;
	AttrNumber *keycols = aggnode->grpColIdx;
	const char *keyname;
	const char *keysetname;

	if (aggnode->aggstrategy == AGG_HASHED || aggnode->aggstrategy == AGG_MIXED)
	{
		keyname = "Hash Key";
		keysetname = "Hash Keys";
	}
	else
	{
		keyname = "Group Key";
		keysetname = "Group Keys";
	}

	ExplainOpenGroup("Grouping Set", NULL, true, es);

	if (sortnode)
	{
		show_sort_group_keys(planstate, "Sort Key",
							 sortnode->numCols, 0, sortnode->sortColIdx,
							 sortnode->sortOperators, sortnode->collations,
							 sortnode->nullsFirst,
							 ancestors, es);
		if (es->format == EXPLAIN_FORMAT_TEXT)
			es->indent++;
	}

	ExplainOpenGroup(keysetname, keysetname, false, es);

	foreach(lc, gsets)
	{
		List	   *result = NIL;
		ListCell   *lc2;

		foreach(lc2, (List *) lfirst(lc))
		{
			Index		i = lfirst_int(lc2);
			AttrNumber	keyresno = keycols[i];
			TargetEntry *target = get_tle_by_resno(plan->targetlist,
												   keyresno);

			if (!target)
				elog(ERROR, "no tlist entry for key %d", keyresno);
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expression((Node *) target->expr, context,
										 useprefix, true);

			result = lappend(result, exprstr);
		}

		if (!result && es->format == EXPLAIN_FORMAT_TEXT)
			ExplainPropertyText(keyname, "()", es);
		else
			ExplainPropertyListNested(keyname, result, es);
	}

	ExplainCloseGroup(keysetname, keysetname, false, es);

	if (sortnode && es->format == EXPLAIN_FORMAT_TEXT)
		es->indent--;

	ExplainCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
#if 0
static void
show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es)
{
	Group	   *plan = (Group *) gstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(plan, ancestors);
	show_sort_group_keys(outerPlanState(gstate), "Group Key",
						 plan->numCols, 0, plan->grpColIdx,
						 NULL, NULL, NULL,
						 ancestors, es);
	ancestors = list_delete_first(ancestors);
}
#endif

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, int nPresortedKeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	List	   *resultPresorted = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   plan,
									   ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context,
									 useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
		if (keyno < nPresortedKeys)
			resultPresorted = lappend(resultPresorted, exprstr);
	}

	ExplainPropertyList(qlabel, result, es);

	/*
	 * GPDB_90_MERGE_FIXME: handle rollup times printing
	 * if (rollup_gs_times > 1)
	 *	appendStringInfo(es->str, " (%d times)", rollup_gs_times);
	 */
	if (nPresortedKeys > 0)
		ExplainPropertyList("Presorted Key", resultPresorted, es);
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default for the column's type.  There are
	 * some cases where this is redundant, eg if expression is a column whose
	 * declared collation is that collation, but it's hard to distinguish that
	 * here (and arguably, printing COLLATE explicitly is a good idea anyway
	 * in such cases).
	 */
	if (OidIsValid(collation) && collation != get_typcollation(sortcoltype))
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Show TABLESAMPLE properties
 */
static void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	char	   *method_name;
	List	   *params = NIL;
	char	   *repeatable;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Get the tablesample method name */
	method_name = get_func_name(tsc->tsmhandler);

	/* Deparse parameter expressions */
	foreach(lc, tsc->args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		params = lappend(params,
						 deparse_expression(arg, context,
											useprefix, false));
	}
	if (tsc->repeatable)
		repeatable = deparse_expression((Node *) tsc->repeatable, context,
										useprefix, false);
	else
		repeatable = NULL;

	/* Print results */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		first = true;

		ExplainIndentText(es);
		appendStringInfo(es->str, "Sampling: %s (", method_name);
		foreach(lc, params)
		{
			if (!first)
				appendStringInfoString(es->str, ", ");
			appendStringInfoString(es->str, (const char *) lfirst(lc));
			first = false;
		}
		appendStringInfoChar(es->str, ')');
		if (repeatable)
			appendStringInfo(es->str, " REPEATABLE (%s)", repeatable);
		appendStringInfoChar(es->str, '\n');
	}
	else
	{
		ExplainPropertyText("Sampling Method", method_name, es);
		ExplainPropertyList("Sampling Parameters", params, es);
		if (repeatable)
			ExplainPropertyText("Repeatable Seed", repeatable, es);
	}
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 *
 * GPDB_90_MERGE_FIXME: The sort statistics are stored quite differently from
 * upstream, it would be nice to rewrite this to avoid looping over all the
 * sort methods and instead have a _get_stats() function as in upstream.
 */
static void
show_sort_info(SortState *sortstate, ExplainState *es)
{
	CdbExplain_NodeSummary *ns;
	int			i;

	if (!es->analyze)
		return;

	ns = es->runtime ? ((PlanState *)sortstate)->instrument->rt_cdbNodeSummary : ((PlanState *)sortstate)->instrument->cdbNodeSummary;
	if (!ns)
		return;

	for (i = 0; i < NUM_SORT_METHOD; i++)
	{
		CdbExplain_Agg	*agg;
		const char *sortMethod;
		const char *spaceType;
		int			j;

		/*
		 * Memory and disk usage statistics are saved separately in GPDB so
		 * need to pull out the one in question first
		 */
		for (j = 0; j < NUM_SORT_SPACE_TYPE; j++)
		{
			agg = &ns->sortSpaceUsed[j][i];

			if (agg->vcnt > 0)
				break;
		}
		/*
		 * If the current sort method in question hasn't been used, skip to
		 * next one
		 */
		if (j >= NUM_SORT_SPACE_TYPE)
			continue;

		sortMethod = tuplesort_method_name(i);
		spaceType = tuplesort_space_type_name(j);

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Sort Method:  %s  %s: " INT64_FORMAT "kB\n",
				sortMethod, spaceType, (long) agg->vsum);
			if (es->verbose)
			{
				appendStringInfo(es->str, "  Max Memory: " INT64_FORMAT "kB  Avg Memory: " INT64_FORMAT "kb (%d segments)\n",
								 (long) agg->vmax,
								 (long) (agg->vsum / agg->vcnt),
								 agg->vcnt);
			}
		}
		else
		{
			ExplainPropertyText("Sort Method", sortMethod, es);
			ExplainPropertyInteger("Sort Space Used", "kB", agg->vsum, es);
			ExplainPropertyText("Sort Space Type", spaceType, es);
			if (es->verbose)
			{
				ExplainPropertyInteger("Sort Max Segment Memory", "kB", agg->vmax, es);
				ExplainPropertyInteger("Sort Avg Segment Memory", "kB", (agg->vsum / agg->vcnt), es);
				ExplainPropertyInteger("Sort Segments", NULL, agg->vcnt, es);
			}
		}
	}

	/*
	 * You might think we should just skip this stanza entirely when
	 * es->hide_workers is true, but then we'd get no sort-method output at
	 * all.  We have to make it look like worker 0's data is top-level data.
	 * This is easily done by just skipping the OpenWorker/CloseWorker calls.
	 * Currently, we don't worry about the possibility that there are multiple
	 * workers in such a case; if there are, duplicate output fields will be
	 * emitted.
	 */
	if (sortstate->shared_info != NULL)
	{
		int			n;

		for (n = 0; n < sortstate->shared_info->num_workers; n++)
		{
			TuplesortInstrumentation *sinstrument;
			const char *sortMethod;
			const char *spaceType;
			int64		spaceUsed;

			sinstrument = &sortstate->shared_info->sinstrument[n];
			if (sinstrument->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
				continue;		/* ignore any unfilled slots */
			sortMethod = tuplesort_method_name(sinstrument->sortMethod);
			spaceType = tuplesort_space_type_name(sinstrument->spaceType);
			spaceUsed = sinstrument->spaceUsed;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				appendStringInfo(es->str,
								 "Sort Method: %s  %s: " INT64_FORMAT "kB\n",
								 sortMethod, spaceType, spaceUsed);
			}
			else
			{
				ExplainPropertyText("Sort Method", sortMethod, es);
				ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
				ExplainPropertyText("Sort Space Type", spaceType, es);
			}

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * Incremental sort nodes sort in (a potentially very large number of) batches,
 * so EXPLAIN ANALYZE needs to roll up the tuplesort stats from each batch into
 * an intelligible summary.
 *
 * This function is used for both a non-parallel node and each worker in a
 * parallel incremental sort node.
 */
static void
show_incremental_sort_group_info(IncrementalSortGroupInfo *groupInfo,
								 const char *groupLabel, bool indent, ExplainState *es)
{
	ListCell   *methodCell;
	List	   *methodNames = NIL;

	/* Generate a list of sort methods used across all groups. */
	for (int bit = 0; bit < NUM_TUPLESORTMETHODS; bit++)
	{
		TuplesortMethod sortMethod = (1 << bit);

		if (groupInfo->sortMethods & sortMethod)
		{
			const char *methodName = tuplesort_method_name(sortMethod);

			methodNames = lappend(methodNames, unconstify(char *, methodName));
		}
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (indent)
			appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "%s Groups: " INT64_FORMAT "  Sort Method", groupLabel,
						 groupInfo->groupCount);
		/* plural/singular based on methodNames size */
		if (list_length(methodNames) > 1)
			appendStringInfoString(es->str, "s: ");
		else
			appendStringInfoString(es->str, ": ");
		foreach(methodCell, methodNames)
		{
			appendStringInfoString(es->str, (char *) methodCell->ptr_value);
			if (foreach_current_index(methodCell) < list_length(methodNames) - 1)
				appendStringInfoString(es->str, ", ");
		}

		if (groupInfo->maxMemorySpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
			appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
							 spaceTypeName, avgSpace,
							 spaceTypeName, groupInfo->maxMemorySpaceUsed);
		}

		if (groupInfo->maxDiskSpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;

			const char *spaceTypeName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
			appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
							 spaceTypeName, avgSpace,
							 spaceTypeName, groupInfo->maxDiskSpaceUsed);
		}
	}
	else
	{
		StringInfoData groupName;

		initStringInfo(&groupName);
		appendStringInfo(&groupName, "%s Groups", groupLabel);
		ExplainOpenGroup("Incremental Sort Groups", groupName.data, true, es);
		ExplainPropertyInteger("Group Count", NULL, groupInfo->groupCount, es);

		ExplainPropertyList("Sort Methods Used", methodNames, es);

		if (groupInfo->maxMemorySpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;
			StringInfoData memoryName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
			initStringInfo(&memoryName);
			appendStringInfo(&memoryName, "Sort Space %s", spaceTypeName);
			ExplainOpenGroup("Sort Space", memoryName.data, true, es);

			ExplainPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
			ExplainPropertyInteger("Peak Sort Space Used", "kB",
								   groupInfo->maxMemorySpaceUsed, es);

			ExplainCloseGroup("Sort Space", memoryName.data, true, es);
		}
		if (groupInfo->maxDiskSpaceUsed > 0)
		{
			int64		avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;
			const char *spaceTypeName;
			StringInfoData diskName;

			spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
			initStringInfo(&diskName);
			appendStringInfo(&diskName, "Sort Space %s", spaceTypeName);
			ExplainOpenGroup("Sort Space", diskName.data, true, es);

			ExplainPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
			ExplainPropertyInteger("Peak Sort Space Used", "kB",
								   groupInfo->maxDiskSpaceUsed, es);

			ExplainCloseGroup("Sort Space", diskName.data, true, es);
		}

		ExplainCloseGroup("Incremental Sort Groups", groupName.data, true, es);
	}
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for an incremental sort node
 */
static void
show_incremental_sort_info(IncrementalSortState *incrsortstate,
						   ExplainState *es)
{
	IncrementalSortGroupInfo *fullsortGroupInfo;
	IncrementalSortGroupInfo *prefixsortGroupInfo;

	fullsortGroupInfo = &incrsortstate->incsort_info.fullsortGroupInfo;

	if (!es->analyze)
		return;

	/*
	 * Since we never have any prefix groups unless we've first sorted a full
	 * groups and transitioned modes (copying the tuples into a prefix group),
	 * we don't need to do anything if there were 0 full groups.
	 *
	 * We still have to continue after this block if there are no full groups,
	 * though, since it's possible that we have workers that did real work
	 * even if the leader didn't participate.
	 */
	if (fullsortGroupInfo->groupCount > 0)
	{
		show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort", true, es);
		prefixsortGroupInfo = &incrsortstate->incsort_info.prefixsortGroupInfo;
		if (prefixsortGroupInfo->groupCount > 0)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
				appendStringInfoChar(es->str, '\n');
			show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
		}
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoChar(es->str, '\n');
	}

	if (incrsortstate->shared_info != NULL)
	{
		int			n;
		bool		indent_first_line;

		for (n = 0; n < incrsortstate->shared_info->num_workers; n++)
		{
			IncrementalSortInfo *incsort_info =
			&incrsortstate->shared_info->sinfo[n];

			/*
			 * If a worker hasn't processed any sort groups at all, then
			 * exclude it from output since it either didn't launch or didn't
			 * contribute anything meaningful.
			 */
			fullsortGroupInfo = &incsort_info->fullsortGroupInfo;

			/*
			 * Since we never have any prefix groups unless we've first sorted
			 * a full groups and transitioned modes (copying the tuples into a
			 * prefix group), we don't need to do anything if there were 0
			 * full groups.
			 */
			if (fullsortGroupInfo->groupCount == 0)
				continue;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			indent_first_line = es->workers_state == NULL || es->verbose;
			show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort",
											 indent_first_line, es);
			prefixsortGroupInfo = &incsort_info->prefixsortGroupInfo;
			if (prefixsortGroupInfo->groupCount > 0)
			{
				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfoChar(es->str, '\n');
				show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
			}
			if (es->format == EXPLAIN_FORMAT_TEXT)
				appendStringInfoChar(es->str, '\n');

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * Show information on hash buckets/batches.
 */
static void
show_hash_info(HashState *hashstate, ExplainState *es)
{
	HashInstrumentation hinstrument = {0};

	/*
	 * Collect stats from the local process, even when it's a parallel query.
	 * In a parallel query, the leader process may or may not have run the
	 * hash join, and even if it did it may not have built a hash table due to
	 * timing (if it started late it might have seen no tuples in the outer
	 * relation and skipped building the hash table).  Therefore we have to be
	 * prepared to get instrumentation data from all participants.
	 */
	if (hashstate->hinstrument)
		memcpy(&hinstrument, hashstate->hinstrument,
			   sizeof(HashInstrumentation));
	/*
	 * Merge results from workers.  In the parallel-oblivious case, the
	 * results from all participants should be identical, except where
	 * participants didn't run the join at all so have no data.  In the
	 * parallel-aware case, we need to consider all the results.  Each worker
	 * may have seen a different subset of batches and we want to report the
	 * highest memory usage across all batches.  We take the maxima of other
	 * values too, for the same reasons as in ExecHashAccumInstrumentation.
	 */
	if (hashstate->shared_info)
	{
		SharedHashInfo *shared_info = hashstate->shared_info;
		int			i;

		for (i = 0; i < shared_info->num_workers; ++i)
		{
			HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

			hinstrument.nbuckets = Max(hinstrument.nbuckets,
									   worker_hi->nbuckets);
			hinstrument.nbuckets_original = Max(hinstrument.nbuckets_original,
												worker_hi->nbuckets_original);
			hinstrument.nbatch = Max(hinstrument.nbatch,
									 worker_hi->nbatch);
			hinstrument.nbatch_original = Max(hinstrument.nbatch_original,
											  worker_hi->nbatch_original);
			hinstrument.space_peak = Max(hinstrument.space_peak,
										 worker_hi->space_peak);
		}
	}

	if (hinstrument.nbatch > 0)
	{
		long		spacePeakKb = (hinstrument.space_peak + 1023) / 1024;

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			ExplainPropertyInteger("Hash Buckets", NULL,
								   hinstrument.nbuckets, es);
			ExplainPropertyInteger("Original Hash Buckets", NULL,
								   hinstrument.nbuckets_original, es);
			ExplainPropertyInteger("Hash Batches", NULL,
								   hinstrument.nbatch, es);
			ExplainPropertyInteger("Original Hash Batches", NULL,
								   hinstrument.nbatch_original, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB",
								   spacePeakKb, es);
		}
		else if (hinstrument.nbatch_original != hinstrument.nbatch ||
				 hinstrument.nbuckets_original != hinstrument.nbuckets)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets,
							 hinstrument.nbuckets_original,
							 hinstrument.nbatch,
							 hinstrument.nbatch_original,
							 spacePeakKb);
		}
		else
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Buckets: %d  Batches: %d  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets, hinstrument.nbatch,
							 spacePeakKb);
		}
	}
}

static void
show_runtime_filter_info(RuntimeFilterState *rfstate, ExplainState *es)
{
	if (es->analyze)
	{
		if (rfstate->bf != NULL)
			ExplainPropertyUInteger("Bloom Bits", NULL,
									bloom_total_bits(rfstate->bf), es);
	}
}

/*
 * Show information on memoize hits/misses/evictions and memory usage.
 */
static void
show_memoize_info(MemoizeState *mstate, List *ancestors, ExplainState *es)
{
	Plan	   *plan = ((PlanState *) mstate)->plan;
	ListCell   *lc;
	List	   *context;
	StringInfoData keystr;
	char	   *seperator = "";
	bool		useprefix;
	int64		memPeakKb;

	initStringInfo(&keystr);

	/*
	 * It's hard to imagine having a memoize node with fewer than 2 RTEs, but
	 * let's just keep the same useprefix logic as elsewhere in this file.
	 */
	useprefix = list_length(es->rtable) > 1 || es->verbose;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   plan,
									   ancestors);

	foreach(lc, ((Memoize *) plan)->param_exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);

		appendStringInfoString(&keystr, seperator);

		appendStringInfoString(&keystr, deparse_expression(expr, context,
														   useprefix, false));
		seperator = ", ";
	}

	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyText("Cache Key", keystr.data, es);
		ExplainPropertyText("Cache Mode", mstate->binary_mode ? "binary" : "logical", es);
	}
	else
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "Cache Key: %s\n", keystr.data);
		ExplainIndentText(es);
		appendStringInfo(es->str, "Cache Mode: %s\n", mstate->binary_mode ? "binary" : "logical");
	}

	pfree(keystr.data);

	if (!es->analyze)
		return;

	if (mstate->stats.cache_misses > 0)
	{
		/*
		 * mem_peak is only set when we freed memory, so we must use mem_used
		 * when mem_peak is 0.
		 */
		if (mstate->stats.mem_peak > 0)
			memPeakKb = (mstate->stats.mem_peak + 1023) / 1024;
		else
			memPeakKb = (mstate->mem_used + 1023) / 1024;

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			ExplainPropertyInteger("Cache Hits", NULL, mstate->stats.cache_hits, es);
			ExplainPropertyInteger("Cache Misses", NULL, mstate->stats.cache_misses, es);
			ExplainPropertyInteger("Cache Evictions", NULL, mstate->stats.cache_evictions, es);
			ExplainPropertyInteger("Cache Overflows", NULL, mstate->stats.cache_overflows, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
		}
		else
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Hits: " UINT64_FORMAT "  Misses: " UINT64_FORMAT "  Evictions: " UINT64_FORMAT "  Overflows: " UINT64_FORMAT "  Memory Usage: " INT64_FORMAT "kB\n",
							 mstate->stats.cache_hits,
							 mstate->stats.cache_misses,
							 mstate->stats.cache_evictions,
							 mstate->stats.cache_overflows,
							 memPeakKb);
		}
	}

	if (mstate->shared_info == NULL)
		return;

	/* Show details from parallel workers */
	for (int n = 0; n < mstate->shared_info->num_workers; n++)
	{
		MemoizeInstrumentation *si;

		si = &mstate->shared_info->sinstrument[n];

		/*
		 * Skip workers that didn't do any work.  We needn't bother checking
		 * for cache hits as a miss will always occur before a cache hit.
		 */
		if (si->cache_misses == 0)
			continue;

		if (es->workers_state)
			ExplainOpenWorker(n, es);

		/*
		 * Since the worker's MemoizeState.mem_used field is unavailable to
		 * us, ExecEndMemoize will have set the
		 * MemoizeInstrumentation.mem_peak field for us.  No need to do the
		 * zero checks like we did for the serial case above.
		 */
		memPeakKb = (si->mem_peak + 1023) / 1024;

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Hits: " UINT64_FORMAT "  Misses: " UINT64_FORMAT "  Evictions: " UINT64_FORMAT "  Overflows: " UINT64_FORMAT "  Memory Usage: " INT64_FORMAT "kB\n",
							 si->cache_hits, si->cache_misses,
							 si->cache_evictions, si->cache_overflows,
							 memPeakKb);
		}
		else
		{
			ExplainPropertyInteger("Cache Hits", NULL,
								   si->cache_hits, es);
			ExplainPropertyInteger("Cache Misses", NULL,
								   si->cache_misses, es);
			ExplainPropertyInteger("Cache Evictions", NULL,
								   si->cache_evictions, es);
			ExplainPropertyInteger("Cache Overflows", NULL,
								   si->cache_overflows, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb,
								   es);
		}

		if (es->workers_state)
			ExplainCloseWorker(n, es);
	}
}

/*
 * Show information on hash aggregate memory usage and batches.
 */
static void
show_hashagg_info(AggState *aggstate, ExplainState *es)
{
	Agg		   *agg = (Agg *) aggstate->ss.ps.plan;
	int64		memPeakKb = (aggstate->hash_mem_peak + 1023) / 1024;

	if (agg->aggstrategy != AGG_HASHED &&
		agg->aggstrategy != AGG_MIXED)
		return;

	if (es->format != EXPLAIN_FORMAT_TEXT)
	{

		if (es->costs)
			ExplainPropertyInteger("Planned Partitions", NULL,
								   aggstate->hash_planned_partitions, es);

		/*
		 * During parallel query the leader may have not helped out.  We
		 * detect this by checking how much memory it used.  If we find it
		 * didn't do any work then we don't show its properties.
		 */
		if (es->analyze && aggstate->hash_mem_peak > 0)
		{
			ExplainPropertyInteger("HashAgg Batches", NULL,
								   aggstate->hash_batches_used, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
			ExplainPropertyInteger("Disk Usage", "kB",
								   aggstate->hash_disk_used, es);
		}
	}
	else
	{
		bool		gotone = false;

		if (es->costs && aggstate->hash_planned_partitions > 0)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "Planned Partitions: %d",
							 aggstate->hash_planned_partitions);
			gotone = true;
		}

		/*
		 * During parallel query the leader may have not helped out.  We
		 * detect this by checking how much memory it used.  If we find it
		 * didn't do any work then we don't show its properties.
		 */
		if (es->analyze && aggstate->hash_mem_peak > 0)
		{
			if (!gotone)
				ExplainIndentText(es);
			else
				appendStringInfoString(es->str, "  ");

			appendStringInfo(es->str, "Batches: %d  Memory Usage: " INT64_FORMAT "kB",
							 aggstate->hash_batches_used, memPeakKb);
			gotone = true;

			/* Only display disk usage if we spilled to disk */
			if (aggstate->hash_batches_used > 1)
			{
				appendStringInfo(es->str, "  Disk Usage: " UINT64_FORMAT "kB",
								 aggstate->hash_disk_used);
			}
		}

		if (gotone)
			appendStringInfoChar(es->str, '\n');
	}

	/* Display stats for each parallel worker */
	if (es->analyze && aggstate->shared_info != NULL)
	{
		for (int n = 0; n < aggstate->shared_info->num_workers; n++)
		{
			AggregateInstrumentation *sinstrument;
			uint64		hash_disk_used;
			int			hash_batches_used;

			sinstrument = &aggstate->shared_info->sinstrument[n];
			/* Skip workers that didn't do anything */
			if (sinstrument->hash_mem_peak == 0)
				continue;
			hash_disk_used = sinstrument->hash_disk_used;
			hash_batches_used = sinstrument->hash_batches_used;
			memPeakKb = (sinstrument->hash_mem_peak + 1023) / 1024;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);

				appendStringInfo(es->str, "Batches: %d  Memory Usage: " INT64_FORMAT "kB",
								 hash_batches_used, memPeakKb);

				/* Only display disk usage if we spilled to disk */
				if (hash_batches_used > 1)
					appendStringInfo(es->str, "  Disk Usage: " UINT64_FORMAT "kB",
									 hash_disk_used);
				appendStringInfoChar(es->str, '\n');
			}
			else
			{
				ExplainPropertyInteger("HashAgg Batches", NULL,
									   hash_batches_used, es);
				ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb,
									   es);
				ExplainPropertyInteger("Disk Usage", "kB", hash_disk_used, es);
			}

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show exact/lossy pages for a BitmapHeapScan node
 */
static void
show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es)
{
	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyInteger("Exact Heap Blocks", NULL,
							   planstate->exact_pages, es);
		ExplainPropertyInteger("Lossy Heap Blocks", NULL,
							   planstate->lossy_pages, es);
	}
	else
	{
		if (planstate->exact_pages > 0 || planstate->lossy_pages > 0)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Heap Blocks:");
			if (planstate->exact_pages > 0)
				appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
			if (planstate->lossy_pages > 0)
				appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
			appendStringInfoChar(es->str, '\n');
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
static void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;
	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (nloops > 0)
			ExplainPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
		else
			ExplainPropertyFloat(qlabel, NULL, 0.0, 0, es);
	}
}

/*
 * Show extra information for a ForeignScan node.
 */
static void
show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es)
{
	FdwRoutine *fdwroutine = fsstate->fdwroutine;

	/* Let the FDW emit whatever fields it wants */
	if (((ForeignScan *) fsstate->ss.ps.plan)->operation != CMD_SELECT)
	{
		if (fdwroutine->ExplainDirectModify != NULL)
			fdwroutine->ExplainDirectModify(fsstate, es);
	}
	else
	{
		if (fdwroutine->ExplainForeignScan != NULL)
			fdwroutine->ExplainForeignScan(fsstate, es);
	}
}

/*
 * Show initplan params evaluated at Gather or Gather Merge node.
 */
static void
show_eval_params(Bitmapset *bms_params, ExplainState *es)
{
	int			paramid = -1;
	List	   *params = NIL;

	Assert(bms_params);

	while ((paramid = bms_next_member(bms_params, paramid)) >= 0)
	{
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	if (params)
		ExplainPropertyList("Params Evaluated", params, es);
}

static void
show_join_pruning_info(List *join_prune_ids, ExplainState *es)
{
	List	   *params = NIL;
	ListCell   *lc;

	if (!join_prune_ids)
		return;

	foreach(lc, join_prune_ids)
	{
		int			paramid = lfirst_int(lc);
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	ExplainPropertyList("Partition Selectors", params, es);
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 *
 * Note: names returned by this function should be "raw"; the caller will
 * apply quoting if needed.  Formerly the convention was to do quoting here,
 * but we don't want that in non-text output formats.
 */
static const char *
explain_get_index_name(Oid indexId)
{
	const char *result;

	if (explain_get_index_name_hook)
		result = (*explain_get_index_name_hook) (indexId);
	else
		result = NULL;
	if (result == NULL)
	{
		/* default behavior: look it up in the catalogs */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
	}
	return result;
}

/*
 * Show buffer usage details.
 */
static void
show_buffer_usage(ExplainState *es, const BufferUsage *usage, bool planning)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		has_shared = (usage->shared_blks_hit > 0 ||
								  usage->shared_blks_read > 0 ||
								  usage->shared_blks_dirtied > 0 ||
								  usage->shared_blks_written > 0);
		bool		has_local = (usage->local_blks_hit > 0 ||
								 usage->local_blks_read > 0 ||
								 usage->local_blks_dirtied > 0 ||
								 usage->local_blks_written > 0);
		bool		has_temp = (usage->temp_blks_read > 0 ||
								usage->temp_blks_written > 0);
		bool		has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) ||
								  !INSTR_TIME_IS_ZERO(usage->blk_write_time));
		bool		show_planning = (planning && (has_shared ||
												  has_local || has_temp || has_timing));

		if (show_planning)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Planning:\n");
			es->indent++;
		}

		/* Show only positive counter values. */
		if (has_shared || has_local || has_temp)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Buffers:");

			if (has_shared)
			{
				appendStringInfoString(es->str, " shared");
				if (usage->shared_blks_hit > 0)
					appendStringInfo(es->str, " hit=%lld",
									 (long long) usage->shared_blks_hit);
				if (usage->shared_blks_read > 0)
					appendStringInfo(es->str, " read=%lld",
									 (long long) usage->shared_blks_read);
				if (usage->shared_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%lld",
									 (long long) usage->shared_blks_dirtied);
				if (usage->shared_blks_written > 0)
					appendStringInfo(es->str, " written=%lld",
									 (long long) usage->shared_blks_written);
				if (has_local || has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_local)
			{
				appendStringInfoString(es->str, " local");
				if (usage->local_blks_hit > 0)
					appendStringInfo(es->str, " hit=%lld",
									 (long long) usage->local_blks_hit);
				if (usage->local_blks_read > 0)
					appendStringInfo(es->str, " read=%lld",
									 (long long) usage->local_blks_read);
				if (usage->local_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%lld",
									 (long long) usage->local_blks_dirtied);
				if (usage->local_blks_written > 0)
					appendStringInfo(es->str, " written=%lld",
									 (long long) usage->local_blks_written);
				if (has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_temp)
			{
				appendStringInfoString(es->str, " temp");
				if (usage->temp_blks_read > 0)
					appendStringInfo(es->str, " read=%lld",
									 (long long) usage->temp_blks_read);
				if (usage->temp_blks_written > 0)
					appendStringInfo(es->str, " written=%lld",
									 (long long) usage->temp_blks_written);
			}
			appendStringInfoChar(es->str, '\n');
		}

		/* As above, show only positive counter values. */
		if (has_timing)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "I/O Timings:");
			if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
				appendStringInfo(es->str, " read=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
			if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
				appendStringInfo(es->str, " write=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
			appendStringInfoChar(es->str, '\n');
		}

		if (show_planning)
			es->indent--;
	}
	else
	{
		ExplainPropertyInteger("Shared Hit Blocks", NULL,
							   usage->shared_blks_hit, es);
		ExplainPropertyInteger("Shared Read Blocks", NULL,
							   usage->shared_blks_read, es);
		ExplainPropertyInteger("Shared Dirtied Blocks", NULL,
							   usage->shared_blks_dirtied, es);
		ExplainPropertyInteger("Shared Written Blocks", NULL,
							   usage->shared_blks_written, es);
		ExplainPropertyInteger("Local Hit Blocks", NULL,
							   usage->local_blks_hit, es);
		ExplainPropertyInteger("Local Read Blocks", NULL,
							   usage->local_blks_read, es);
		ExplainPropertyInteger("Local Dirtied Blocks", NULL,
							   usage->local_blks_dirtied, es);
		ExplainPropertyInteger("Local Written Blocks", NULL,
							   usage->local_blks_written, es);
		ExplainPropertyInteger("Temp Read Blocks", NULL,
							   usage->temp_blks_read, es);
		ExplainPropertyInteger("Temp Written Blocks", NULL,
							   usage->temp_blks_written, es);
		if (track_io_timing)
		{
			ExplainPropertyFloat("I/O Read Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time),
								 3, es);
			ExplainPropertyFloat("I/O Write Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time),
								 3, es);
		}
	}
}

/*
 * Show WAL usage details.
 */
static void
show_wal_usage(ExplainState *es, const WalUsage *usage)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		/* Show only positive counter values. */
		if ((usage->wal_records >= 0) || (usage->wal_fpi >= 0) ||
			(usage->wal_bytes >= 0))
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "WAL:");

			if (usage->wal_records > 0)
				appendStringInfo(es->str, " records=%lld",
								 (long long) usage->wal_records);
			if (usage->wal_fpi > 0)
				appendStringInfo(es->str, " fpi=%lld",
								 (long long) usage->wal_fpi);
			if (usage->wal_bytes > 0)
				appendStringInfo(es->str, " bytes=" UINT64_FORMAT,
								 usage->wal_bytes);
			appendStringInfoChar(es->str, '\n');
		}
	}
	else
	{
		ExplainPropertyInteger("WAL Records", NULL,
							   usage->wal_records, es);
		ExplainPropertyInteger("WAL FPI", NULL,
							   usage->wal_fpi, es);
		ExplainPropertyUInteger("WAL Bytes", NULL,
								usage->wal_bytes, es);
	}
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es)
{
	const char *indexname = explain_get_index_name(indexid);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (ScanDirectionIsBackward(indexorderdir))
			appendStringInfoString(es->str, " Backward");
		appendStringInfo(es->str, " using %s", quote_identifier(indexname));
	}
	else
	{
		const char *scandir;

		switch (indexorderdir)
		{
			case BackwardScanDirection:
				scandir = "Backward";
				break;
			case NoMovementScanDirection:
				scandir = "NoMovement";
				break;
			case ForwardScanDirection:
				scandir = "Forward";
				break;
			default:
				scandir = "???";
				break;
		}
		ExplainPropertyText("Scan Direction", scandir, es);
		ExplainPropertyText("Index Name", indexname, es);
	}
}

/*
 * Show the target of a Scan node
 */
static void
ExplainScanTarget(Scan *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
static void
ExplainModifyTarget(ModifyTable *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->nominalRelation, es);
}

/*
 * Show the target relation of a scan or modify node
 */
static void
ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte;
	char	   *refname;
	int			dynamicScanId = 0;

	rte = rt_fetch(rti, es->rtable);
	refname = (char *) list_nth(es->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			if (es->verbose)
				namespace = get_namespace_name(get_rel_namespace(rte->relid));
			objecttag = "Relation Name";

			break;
		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) plan;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);

				/*
				 * If the expression is still a function call of a single
				 * function, we can get the real name of the function.
				 * Otherwise, punt.  (Even if it was a single function call
				 * originally, the optimizer could have simplified it away.)
				 */
				if (list_length(fscan->functions) == 1)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

						objectname = get_func_name(funcid);
						if (es->verbose)
							namespace =
								get_namespace_name(get_func_namespace(funcid));
					}
				}
				objecttag = "Function Name";
			}
			break;
		case T_TableFunctionScan:
			{
				TableFunctionScan *fscan = (TableFunctionScan *) plan;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_TABLEFUNCTION);

				/*
				 * Unlike in a FunctionScan, in a TableFunctionScan the call
				 * should always be a function call of a single function.
				 * Get the real name of the function.
				 */
				{
					RangeTblFunction *rtfunc = fscan->function;

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

						objectname = get_func_name(funcid);
						if (es->verbose)
							namespace =
								get_namespace_name(get_func_namespace(funcid));
					}
				}
				objecttag = "Function Name";

				/* might be nice to add order by and scatter by info, if it's a TableFunctionScan */
			}
			break;
		case T_TableFuncScan:
			Assert(rte->rtekind == RTE_TABLEFUNC);
			objectname = "xmltable";
			objecttag = "Table Function Name";
			break;
		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
		case T_CteScan:
			/* Assert it's on a non-self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(!rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		case T_NamedTuplestoreScan:
			Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
			objectname = rte->enrname;
			objecttag = "Tuplestore Name";
			break;
		case T_WorkTableScan:
			/* Assert it's on a self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		default:
			break;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoString(es->str, " on");
		if (namespace != NULL)
			appendStringInfo(es->str, " %s.%s", quote_identifier(namespace),
							 quote_identifier(objectname));
		else if (objectname != NULL)
			appendStringInfo(es->str, " %s", quote_identifier(objectname));
		if (objectname == NULL || strcmp(refname, objectname) != 0)
			appendStringInfo(es->str, " %s", quote_identifier(refname));

		if (dynamicScanId != 0)
			appendStringInfo(es->str, " (dynamic scan id: %d)",
							 dynamicScanId);
	}
	else
	{
		if (objecttag != NULL && objectname != NULL)
			ExplainPropertyText(objecttag, objectname, es);
		if (namespace != NULL)
			ExplainPropertyText("Schema", namespace, es);
		ExplainPropertyText("Alias", refname, es);

		if (dynamicScanId != 0)
			ExplainPropertyInteger("Dynamic Scan Id", NULL, dynamicScanId, es);
	}
}

/*
 * Show extra information for a ModifyTable node
 *
 * We have three objectives here.  First, if there's more than one target
 * table or it's different from the nominal target, identify the actual
 * target(s).  Second, give FDWs a chance to display extra info about foreign
 * targets.  Third, show information about ON CONFLICT.
 */
static void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ExplainState *es)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	const char *operation;
	const char *foperation;
	bool		labeltargets;
	int			j;
	List	   *idxNames = NIL;
	ListCell   *lst;

	switch (node->operation)
	{
		case CMD_INSERT:
			operation = "Insert";
			foperation = "Foreign Insert";
			break;
		case CMD_UPDATE:
			operation = "Update";
			foperation = "Foreign Update";
			break;
		case CMD_DELETE:
			operation = "Delete";
			foperation = "Foreign Delete";
			break;
		default:
			operation = "???";
			foperation = "Foreign ???";
			break;
	}

	/* Should we explicitly label target relations? */
	labeltargets = (mtstate->mt_nrels > 1 ||
					(mtstate->mt_nrels == 1 &&
					 mtstate->resultRelInfo[0].ri_RangeTableIndex != node->nominalRelation));

	if (labeltargets)
		ExplainOpenGroup("Target Tables", "Target Tables", false, es);

	for (j = 0; j < mtstate->mt_nrels; j++)
	{
		ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
		FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

		if (labeltargets)
		{
			/* Open a group for this target */
			ExplainOpenGroup("Target Table", NULL, true, es);

			/*
			 * In text mode, decorate each target with operation type, so that
			 * ExplainTargetRel's output of " on foo" will read nicely.
			 */
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				appendStringInfoString(es->str,
									   fdwroutine ? foperation : operation);
			}

			/* Identify target */
			ExplainTargetRel((Plan *) node,
							 resultRelInfo->ri_RangeTableIndex,
							 es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoChar(es->str, '\n');
				es->indent++;
			}
		}

		/* Give FDW a chance if needed */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			fdwroutine != NULL &&
			fdwroutine->ExplainForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, j);

			fdwroutine->ExplainForeignModify(mtstate,
											 resultRelInfo,
											 fdw_private,
											 j,
											 es);
		}

		if (labeltargets)
		{
			/* Undo the indentation we added in text format */
			if (es->format == EXPLAIN_FORMAT_TEXT)
				es->indent--;

			/* Close the group */
			ExplainCloseGroup("Target Table", NULL, true, es);
		}
	}

	/* Gather names of ON CONFLICT arbiter indexes */
	foreach(lst, node->arbiterIndexes)
	{
		char	   *indexname = get_rel_name(lfirst_oid(lst));

		idxNames = lappend(idxNames, indexname);
	}

	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		ExplainPropertyText("Conflict Resolution",
							node->onConflictAction == ONCONFLICT_NOTHING ?
							"NOTHING" : "UPDATE",
							es);

		/*
		 * Don't display arbiter indexes at all when DO NOTHING variant
		 * implicitly ignores all conflicts
		 */
		if (idxNames)
			ExplainPropertyList("Conflict Arbiter Indexes", idxNames, es);

		/* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
		if (node->onConflictWhere)
		{
			show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
							&mtstate->ps, ancestors, es);
			show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
		}

		/* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
		if (es->analyze && mtstate->ps.instrument)
		{
			double		total;
			double		insert_path;
			double		other_path;

			if (!es->runtime)
				InstrEndLoop(outerPlanState(mtstate)->instrument);

			/* count the number of source rows */
			other_path = mtstate->ps.instrument->nfiltered2;

			/*
			 * Insert occurs after extracting row from subplan and in runtime mode
			 * we can appear between these two operations - situation when
			 * total > insert_path + other_path. Therefore we don't know exactly
			 * whether last row from subplan is inserted.
			 * We don't print inserted tuples in runtime mode in order to not print
			 * inconsistent data
			 */
			if (!es->runtime)
			{
				total = outerPlanState(mtstate)->instrument->ntuples;
				insert_path = total - other_path;
				ExplainPropertyFloat("Tuples Inserted", NULL, insert_path, 0, es);
			}
			ExplainPropertyFloat("Conflicting Tuples", NULL,
								 other_path, 0, es);
		}
	}

	if (labeltargets)
		ExplainCloseGroup("Target Tables", "Target Tables", false, es);
}

/*
 * Show the hash and merge keys for a Motion node.
 */
static void
show_motion_keys(PlanState *planstate, List *hashExpr, int nkeys, AttrNumber *keycols,
			     const char *qlabel, List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	char	   *exprstr;
	bool		useprefix = list_length(es->rtable) > 1;
	int			keyno;
	List	   *result = NIL;

	if (!nkeys && !hashExpr)
		return;

	/* Set up deparse context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   plan,
									   ancestors);

    /* Merge Receive ordering key */
    for (keyno = 0; keyno < nkeys; keyno++)
    {
	    /* find key expression in tlist */
	    AttrNumber	keyresno = keycols[keyno];
	    TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);

	    /* Deparse the expression, showing any top-level cast */
	    if (target)
	        exprstr = deparse_expression((Node *) target->expr, context,
								         useprefix, true);
        else
        {
            elog(WARNING, "Gather Motion %s error: no tlist item %d",
                 qlabel, keyresno);
            exprstr = "*BOGUS*";
        }

		result = lappend(result, exprstr);
    }

	if (list_length(result) > 0)
		ExplainPropertyList(qlabel, result, es);

    /* Hashed repartitioning key */
    if (hashExpr)
    {
	    /* Deparse the expression */
	    exprstr = deparse_expression((Node *)hashExpr, context, useprefix, true);
		ExplainPropertyText("Hash Key", exprstr, es);
    }
}

/*
 * Explain a parallel retrieve cursor,
 * indicate the endpoints exist on entry DB, or on some segments,
 * or on all segments.
 */
void ExplainParallelRetrieveCursor(ExplainState *es, QueryDesc* queryDesc)
{
	PlannedStmt *plan = queryDesc->plannedstmt;
	SliceTable *sliceTable = queryDesc->estate->es_sliceTable;
	StringInfoData            endpointInfoStr;
	enum EndPointExecPosition endPointExecPosition;

	initStringInfo(&endpointInfoStr);

	endPointExecPosition = GetParallelCursorEndpointPosition(plan);
	ExplainOpenGroup("Cursor", "Cursor", true, es);
	switch(endPointExecPosition)
	{
		case ENDPOINT_ON_ENTRY_DB:
		{
			appendStringInfo(&endpointInfoStr, "\"on coordinator\"");
			break;
		}
		case ENDPOINT_ON_SINGLE_QE:
		{
			appendStringInfo(
							 &endpointInfoStr, "\"on segment: contentid [%d]\"",
							 gp_session_id % plan->planTree->flow->numsegments);
			break;
		}
		case ENDPOINT_ON_SOME_QE:
		{
			ListCell * cell;
			bool isFirst = true;
			appendStringInfo(&endpointInfoStr, "on segments: contentid [");
			ExecSlice *slice = &sliceTable->slices[0];
			foreach(cell, slice->segments)
			{
				int contentid = lfirst_int(cell);
				appendStringInfo(&endpointInfoStr, (isFirst)?"%d":", %d", contentid);
				isFirst = false;
			}
			appendStringInfo(&endpointInfoStr, "]");
			break;
		}
		case ENDPOINT_ON_ALL_QE:
		{
			appendStringInfo(&endpointInfoStr, "on all %d segments", getgpsegmentCount());
			break;
		}
		default:
		{
			elog(ERROR, "invalid endpoint position : %d", endPointExecPosition);
			break;
		}
	}
	ExplainPropertyText("Endpoint", endpointInfoStr.data, es);
	ExplainCloseGroup("Cursor", "Cursor", true, es);
}
