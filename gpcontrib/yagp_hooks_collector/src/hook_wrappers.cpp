#define typeid __typeid
extern "C" {
#include "postgres.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "executor/execUtils.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/metrics_utils.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#include "tcop/utility.h"
#include "stat_statements_parser/pg_stat_statements_ya_parser.h"
}
#undef typeid

#include "Config.h"
#include "YagpStat.h"
#include "EventSender.h"
#include "hook_wrappers.h"
#include "memory/gpdbwrappers.h"

static ExecutorStart_hook_type previous_ExecutorStart_hook = nullptr;
static ExecutorRun_hook_type previous_ExecutorRun_hook = nullptr;
static ExecutorFinish_hook_type previous_ExecutorFinish_hook = nullptr;
static ExecutorEnd_hook_type previous_ExecutorEnd_hook = nullptr;
static query_info_collect_hook_type previous_query_info_collect_hook = nullptr;
#ifdef ANALYZE_STATS_COLLECT_HOOK
static analyze_stats_collect_hook_type previous_analyze_stats_collect_hook =
    nullptr;
#endif
#ifdef IC_TEARDOWN_HOOK
static ic_teardown_hook_type previous_ic_teardown_hook = nullptr;
#endif
static ProcessUtility_hook_type previous_ProcessUtility_hook = nullptr;

static void ya_ExecutorStart_hook(QueryDesc *query_desc, int eflags);
static void ya_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
                                uint64 count, bool execute_once);
static void ya_ExecutorFinish_hook(QueryDesc *query_desc);
static void ya_ExecutorEnd_hook(QueryDesc *query_desc);
static void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg);
static void ya_ic_teardown_hook(ChunkTransportState *transportStates,
                                bool hasErrors);
#ifdef ANALYZE_STATS_COLLECT_HOOK
static void ya_analyze_stats_collect_hook(QueryDesc *query_desc);
#endif
static void ya_process_utility_hook(PlannedStmt *pstmt, const char *queryString,
                                    bool readOnlyTree,
                                    ProcessUtilityContext context,
                                    ParamListInfo params,
                                    QueryEnvironment *queryEnv,
                                    DestReceiver *dest, QueryCompletion *qc);

static EventSender *sender = nullptr;

static inline EventSender *get_sender() {
  if (!sender) {
    sender = new EventSender();
  }
  return sender;
}

template <typename T, typename R, typename... Args>
R cpp_call(T *obj, R (T::*func)(Args...), Args... args) {
  try {
    return (obj->*func)(args...);
  } catch (const std::exception &e) {
    ereport(FATAL, (errmsg("Unexpected exception in yagpcc %s", e.what())));
  }
}

void hooks_init() {
  Config::init();
  YagpStat::init();
  previous_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = ya_ExecutorStart_hook;
  previous_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = ya_ExecutorRun_hook;
  previous_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = ya_ExecutorFinish_hook;
  previous_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = ya_ExecutorEnd_hook;
  previous_query_info_collect_hook = query_info_collect_hook;
  query_info_collect_hook = ya_query_info_collect_hook;
#ifdef IC_TEARDOWN_HOOK
  previous_ic_teardown_hook = ic_teardown_hook;
  ic_teardown_hook = ya_ic_teardown_hook;
#endif
#ifdef ANALYZE_STATS_COLLECT_HOOK
  previous_analyze_stats_collect_hook = analyze_stats_collect_hook;
  analyze_stats_collect_hook = ya_analyze_stats_collect_hook;
#endif
  stat_statements_parser_init();
  previous_ProcessUtility_hook = ProcessUtility_hook;
  ProcessUtility_hook = ya_process_utility_hook;
}

void hooks_deinit() {
  ExecutorStart_hook = previous_ExecutorStart_hook;
  ExecutorEnd_hook = previous_ExecutorEnd_hook;
  ExecutorRun_hook = previous_ExecutorRun_hook;
  ExecutorFinish_hook = previous_ExecutorFinish_hook;
  query_info_collect_hook = previous_query_info_collect_hook;
#ifdef IC_TEARDOWN_HOOK
  ic_teardown_hook = previous_ic_teardown_hook;
#endif
#ifdef ANALYZE_STATS_COLLECT_HOOK
  analyze_stats_collect_hook = previous_analyze_stats_collect_hook;
#endif
  stat_statements_parser_deinit();
  if (sender) {
    delete sender;
  }
  YagpStat::deinit();
  ProcessUtility_hook = previous_ProcessUtility_hook;
}

void ya_ExecutorStart_hook(QueryDesc *query_desc, int eflags) {
  cpp_call(get_sender(), &EventSender::executor_before_start, query_desc,
           eflags);
  if (previous_ExecutorStart_hook) {
    (*previous_ExecutorStart_hook)(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }
  cpp_call(get_sender(), &EventSender::executor_after_start, query_desc,
           eflags);
}

void ya_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
                         uint64 count, bool execute_once) {
  get_sender()->incr_depth();
  PG_TRY();
  {
    if (previous_ExecutorRun_hook)
      previous_ExecutorRun_hook(query_desc, direction, count, execute_once);
    else
      standard_ExecutorRun(query_desc, direction, count, execute_once);
    get_sender()->decr_depth();
  }
  PG_CATCH();
  {
    get_sender()->decr_depth();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void ya_ExecutorFinish_hook(QueryDesc *query_desc) {
  get_sender()->incr_depth();
  PG_TRY();
  {
    if (previous_ExecutorFinish_hook)
      previous_ExecutorFinish_hook(query_desc);
    else
      standard_ExecutorFinish(query_desc);
    get_sender()->decr_depth();
  }
  PG_CATCH();
  {
    get_sender()->decr_depth();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void ya_ExecutorEnd_hook(QueryDesc *query_desc) {
  cpp_call(get_sender(), &EventSender::executor_end, query_desc);
  if (previous_ExecutorEnd_hook) {
    (*previous_ExecutorEnd_hook)(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg) {
  cpp_call(get_sender(), &EventSender::query_metrics_collect, status,
           arg /* queryDesc */, false /* utility */, (ErrorData *)NULL);
  if (previous_query_info_collect_hook) {
    (*previous_query_info_collect_hook)(status, arg);
  }
}

void ya_ic_teardown_hook(ChunkTransportState *transportStates, bool hasErrors) {
  cpp_call(get_sender(), &EventSender::ic_metrics_collect);
#ifdef IC_TEARDOWN_HOOK
  if (previous_ic_teardown_hook) {
    (*previous_ic_teardown_hook)(transportStates, hasErrors);
  }
#endif
}

#ifdef ANALYZE_STATS_COLLECT_HOOK
void ya_analyze_stats_collect_hook(QueryDesc *query_desc) {
  cpp_call(get_sender(), &EventSender::analyze_stats_collect, query_desc);
  if (previous_analyze_stats_collect_hook) {
    (*previous_analyze_stats_collect_hook)(query_desc);
  }
}
#endif

static void ya_process_utility_hook(PlannedStmt *pstmt, const char *queryString,
                                    bool readOnlyTree,
                                    ProcessUtilityContext context,
                                    ParamListInfo params,
                                    QueryEnvironment *queryEnv,
                                    DestReceiver *dest, QueryCompletion *qc) {
  /* Project utility data on QueryDesc to use existing logic */
  QueryDesc *query_desc = (QueryDesc *)palloc0(sizeof(QueryDesc));
  query_desc->sourceText = queryString;

  cpp_call(get_sender(), &EventSender::query_metrics_collect,
           METRICS_QUERY_SUBMIT, (void *)query_desc, true /* utility */,
           (ErrorData *)NULL);

  get_sender()->incr_depth();
  PG_TRY();
  {
    if (previous_ProcessUtility_hook) {
      (*previous_ProcessUtility_hook)(pstmt, queryString, readOnlyTree, context,
                                      params, queryEnv, dest, qc);
    } else {
      standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                              queryEnv, dest, qc);
    }

    get_sender()->decr_depth();
    cpp_call(get_sender(), &EventSender::query_metrics_collect, METRICS_QUERY_DONE,
         (void *)query_desc, true /* utility */, (ErrorData *)NULL);

    pfree(query_desc);
  }
  PG_CATCH();
  {
    ErrorData *edata;
    MemoryContext oldctx;

    oldctx = MemoryContextSwitchTo(TopMemoryContext);
    edata = CopyErrorData();
    FlushErrorState();
    MemoryContextSwitchTo(oldctx);

    get_sender()->decr_depth();
    cpp_call(get_sender(), &EventSender::query_metrics_collect, METRICS_QUERY_ERROR,
         (void *)query_desc, true /* utility */, edata);

    pfree(query_desc);
    ReThrowError(edata);
  }
  PG_END_TRY();
}

static void check_stats_loaded() {
  if (!YagpStat::loaded()) {
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("yagp_hooks_collector must be loaded via "
                           "shared_preload_libraries")));
  }
}

void yagp_functions_reset() {
  check_stats_loaded();
  YagpStat::reset();
}

Datum yagp_functions_get(FunctionCallInfo fcinfo) {
  const int ATTNUM = 6;
  check_stats_loaded();
  auto stats = YagpStat::get_stats();
  TupleDesc tupdesc = CreateTemplateTupleDesc(ATTNUM);
  TupleDescInitEntry(tupdesc, (AttrNumber)1, "segid", INT4OID, -1 /* typmod */,
                     0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)2, "total_messages", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)3, "send_failures", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)4, "connection_failures", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)5, "other_errors", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)6, "max_message_size", INT4OID,
                     -1 /* typmod */, 0 /* attdim */);
  tupdesc = BlessTupleDesc(tupdesc);
  Datum values[ATTNUM];
  bool nulls[ATTNUM];
  MemSet(nulls, 0, sizeof(nulls));
  values[0] = Int32GetDatum(GpIdentity.segindex);
  values[1] = Int64GetDatum(stats.total);
  values[2] = Int64GetDatum(stats.failed_sends);
  values[3] = Int64GetDatum(stats.failed_connects);
  values[4] = Int64GetDatum(stats.failed_other);
  values[5] = Int32GetDatum(stats.max_message_size);
  HeapTuple tuple = ya_gpdb::heap_form_tuple(tupdesc, values, nulls);
  Datum result = HeapTupleGetDatum(tuple);
  PG_RETURN_DATUM(result);
}