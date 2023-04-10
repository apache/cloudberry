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
static query_info_collect_hook_type previous_query_info_collect_hook = nullptr;

static void ya_ExecutorAfterStart_hook(QueryDesc *query_desc, int eflags);
static void ya_query_info_collect_hook(QueryMetricsStatus status, void *arg);

void hooks_init() {
  previous_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = ya_ExecutorAfterStart_hook;
  previous_query_info_collect_hook = query_info_collect_hook;
  query_info_collect_hook = ya_query_info_collect_hook;
  stat_statements_parser_init();
}

void hooks_deinit() {
  ExecutorStart_hook = previous_ExecutorStart_hook;
  query_info_collect_hook = previous_query_info_collect_hook;
  stat_statements_parser_deinit();
}

void ya_ExecutorAfterStart_hook(QueryDesc *query_desc, int eflags) {
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