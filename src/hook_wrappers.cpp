extern "C" {
#include "postgres.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/metrics_utils.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "tcop/utility.h"
}

#include "Config.h"
#include "YagpStat.h"
#include "EventSender.h"
#include "hook_wrappers.h"
#include "stat_statements_parser/pg_stat_statements_ya_parser.h"

static ExecutorStart_hook_type previous_ExecutorStart_hook = nullptr;
static ExecutorRun_hook_type previous_ExecutorRun_hook = nullptr;
static ExecutorFinish_hook_type previous_ExecutorFinish_hook = nullptr;
static ExecutorEnd_hook_type previous_ExecutorEnd_hook = nullptr;
static query_info_collect_hook_type previous_query_info_collect_hook = nullptr;

static void ya_ExecutorStart_hook(QueryDesc *query_desc, int eflags);
static void ya_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
                                long count);
static void ya_ExecutorFinish_hook(QueryDesc *query_desc);
static void ya_ExecutorEnd_hook(QueryDesc *query_desc);
static void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg);

static EventSender *sender = nullptr;

static inline EventSender *get_sender() {
  if (!sender) {
    sender = new EventSender();
  }
  return sender;
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
  stat_statements_parser_init();
}

void hooks_deinit() {
  ExecutorStart_hook = previous_ExecutorStart_hook;
  ExecutorRun_hook = previous_ExecutorRun_hook;
  ExecutorFinish_hook = previous_ExecutorFinish_hook;
  ExecutorEnd_hook = previous_ExecutorEnd_hook;
  query_info_collect_hook = previous_query_info_collect_hook;
  stat_statements_parser_deinit();
  if (sender) {
    delete sender;
  }
  YagpStat::deinit();
}

void ya_ExecutorStart_hook(QueryDesc *query_desc, int eflags) {
  PG_TRY();
  { get_sender()->executor_before_start(query_desc, eflags); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_ExecutorBeforeStart_hook")));
  }
  PG_END_TRY();
  if (previous_ExecutorStart_hook) {
    (*previous_ExecutorStart_hook)(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }
  PG_TRY();
  { get_sender()->executor_after_start(query_desc, eflags); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_ExecutorAfterStart_hook")));
  }
  PG_END_TRY();
}

void ya_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
                         long count) {
  get_sender()->incr_depth();
  PG_TRY();
  {
    if (previous_ExecutorRun_hook)
      previous_ExecutorRun_hook(query_desc, direction, count);
    else
      standard_ExecutorRun(query_desc, direction, count);
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
  PG_TRY();
  { get_sender()->executor_end(query_desc); }
  PG_CATCH();
  { ereport(WARNING, (errmsg("EventSender failed in ya_ExecutorEnd_hook"))); }
  PG_END_TRY();
  if (previous_ExecutorEnd_hook) {
    (*previous_ExecutorEnd_hook)(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg) {
  PG_TRY();
  { get_sender()->query_metrics_collect(status, arg); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_query_info_collect_hook")));
  }
  PG_END_TRY();
  if (previous_query_info_collect_hook) {
    (*previous_query_info_collect_hook)(status, arg);
  }
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
  TupleDesc tupdesc = CreateTemplateTupleDesc(ATTNUM, false);
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
  HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
  Datum result = HeapTupleGetDatum(tuple);
  PG_RETURN_DATUM(result);
}