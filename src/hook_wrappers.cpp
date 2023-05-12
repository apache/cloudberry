extern "C" {
#include "postgres.h"
#include "utils/metrics_utils.h"
#include "utils/elog.h"
#include "executor/executor.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbexplain.h"

#include "tcop/utility.h"
}

#include "stat_statements_parser/pg_stat_statements_ya_parser.h"
#include "hook_wrappers.h"
#include "EventSender.h"

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

void hooks_init() {
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
  ExecutorEnd_hook = previous_ExecutorEnd_hook;
  query_info_collect_hook = previous_query_info_collect_hook;
  stat_statements_parser_deinit();
}

void ya_ExecutorStart_hook(QueryDesc *query_desc, int eflags) {
  PG_TRY();
  { EventSender::instance()->executor_before_start(query_desc, eflags); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_ExecutorBeforeStart_hook")));
    PG_RE_THROW();
  }
  PG_END_TRY();
  if (previous_ExecutorStart_hook) {
    (*previous_ExecutorStart_hook)(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }
  PG_TRY();
  { EventSender::instance()->executor_after_start(query_desc, eflags); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_ExecutorAfterStart_hook")));
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void ya_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
                         long count) {
  EventSender::instance()->incr_depth();
  PG_TRY();
  {
    if (previous_ExecutorRun_hook)
      previous_ExecutorRun_hook(query_desc, direction, count);
    else
      standard_ExecutorRun(query_desc, direction, count);
    EventSender::instance()->decr_depth();
  }
  PG_CATCH();
  {
    EventSender::instance()->decr_depth();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void ya_ExecutorFinish_hook(QueryDesc *query_desc) {
  EventSender::instance()->incr_depth();
  PG_TRY();
  {
    if (previous_ExecutorFinish_hook)
      previous_ExecutorFinish_hook(query_desc);
    else
      standard_ExecutorFinish(query_desc);
    EventSender::instance()->decr_depth();
  }
  PG_CATCH();
  {
    EventSender::instance()->decr_depth();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void ya_ExecutorEnd_hook(QueryDesc *query_desc) {
  PG_TRY();
  { EventSender::instance()->executor_end(query_desc); }
  PG_CATCH();
  {
    ereport(WARNING, (errmsg("EventSender failed in ya_ExecutorEnd_hook")));
    PG_RE_THROW();
  }
  PG_END_TRY();
  if (previous_ExecutorEnd_hook) {
    (*previous_ExecutorEnd_hook)(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg) {
  PG_TRY();
  { EventSender::instance()->query_metrics_collect(status, arg); }
  PG_CATCH();
  {
    ereport(WARNING,
            (errmsg("EventSender failed in ya_query_info_collect_hook")));
    PG_RE_THROW();
  }
  PG_END_TRY();
  if (previous_query_info_collect_hook) {
    (*previous_query_info_collect_hook)(status, arg);
  }
}