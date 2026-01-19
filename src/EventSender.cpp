#include "Config.h"
#include "UDSConnector.h"
#include "memory/gpdbwrappers.h"
#include "log/LogOps.h"

#define typeid __typeid
extern "C" {
#include "postgres.h"

#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
}
#undef typeid

#include "EventSender.h"
#include "PgUtils.h"
#include "ProtoUtils.h"

#define need_collect_analyze()                                                 \
  (Gp_role == GP_ROLE_DISPATCH && Config::min_analyze_time() >= 0 &&           \
   Config::enable_analyze())

static bool enable_utility = Config::enable_utility();

bool EventSender::verify_query(QueryDesc *query_desc, QueryState state,
                               bool utility) {
  if (!proto_verified) {
    return false;
  }
  if (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_EXECUTE) {
    return false;
  }

  switch (state) {
  case QueryState::SUBMIT:
    // Cache enable_utility at SUBMIT to ensure consistent behavior at DONE.
    // Without caching, a query that sets enable_utility to false from true
    // would be accepted at SUBMIT (guc is true) but rejected at DONE (guc
    // is false), causing a leak.
    enable_utility = Config::enable_utility();
    if (utility && enable_utility == false) {
      return false;
    }
    // Sync config in case current query changes it.
    Config::sync();
    // Register qkey for a nested query we won't report,
    // so we can detect nesting_level > 0 and skip reporting at end/done.
    if (!need_report_nested_query() && nesting_level > 0) {
      QueryKey::register_qkey(query_desc, nesting_level);
      return false;
    }
    if (is_top_level_query(query_desc, nesting_level)) {
      nested_timing = 0;
      nested_calls = 0;
    }
    break;
  case QueryState::START:
    if (!qdesc_submitted(query_desc)) {
      collect_query_submit(query_desc, false /* utility */);
    }
    break;
  case QueryState::DONE:
    if (utility && enable_utility == false) {
      return false;
    }
  default:
    break;
  }

  if (filter_query(query_desc)) {
    return false;
  }
  if (!nesting_is_valid(query_desc, nesting_level)) {
    return false;
  }

  return true;
}

bool EventSender::log_query_req(const yagpcc::SetQueryReq &req,
                                const std::string &event, bool utility) {
  bool clear_big_fields = false;
  switch (Config::logging_mode()) {
  case LOG_MODE_UDS:
    clear_big_fields = UDSConnector::report_query(req, event);
    break;
  case LOG_MODE_TBL:
    ya_gpdb::insert_log(req, utility);
    clear_big_fields = false;
    break;
  default:
    Assert(false);
  }
  return clear_big_fields;
}

void EventSender::query_metrics_collect(QueryMetricsStatus status, void *arg,
                                        bool utility, ErrorData *edata) {
  auto *query_desc = reinterpret_cast<QueryDesc *>(arg);
  switch (status) {
  case METRICS_PLAN_NODE_INITIALIZE:
  case METRICS_PLAN_NODE_EXECUTING:
  case METRICS_PLAN_NODE_FINISHED:
    // TODO
    break;
  case METRICS_QUERY_SUBMIT:
    collect_query_submit(query_desc, utility);
    break;
  case METRICS_QUERY_START:
    // no-op: executor_after_start is enough
    break;
  case METRICS_QUERY_CANCELING:
    // it appears we're only interested in the actual CANCELED event.
    // for now we will ignore CANCELING state unless otherwise requested from
    // end users
    break;
  case METRICS_QUERY_DONE:
  case METRICS_QUERY_ERROR:
  case METRICS_QUERY_CANCELED:
  case METRICS_INNER_QUERY_DONE:
    collect_query_done(query_desc, utility, status, edata);
    break;
  default:
    ereport(FATAL, (errmsg("Unknown query status: %d", status)));
  }
}

void EventSender::executor_before_start(QueryDesc *query_desc, int eflags) {
  if (!verify_query(query_desc, QueryState::START, false /* utility*/)) {
    return;
  }

  if (Gp_role == GP_ROLE_DISPATCH && Config::enable_analyze() &&
      (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0) {
    query_desc->instrument_options |= INSTRUMENT_BUFFERS;
    query_desc->instrument_options |= INSTRUMENT_ROWS;
    query_desc->instrument_options |= INSTRUMENT_TIMER;
    if (Config::enable_cdbstats()) {
      query_desc->instrument_options |= INSTRUMENT_CDB;
      if (!query_desc->showstatctx) {
        instr_time starttime;
        INSTR_TIME_SET_CURRENT(starttime);
        query_desc->showstatctx =
            ya_gpdb::cdbexplain_showExecStatsBegin(query_desc, starttime);
      }
    }
  }
}

void EventSender::executor_after_start(QueryDesc *query_desc, int /* eflags*/) {
  if (!verify_query(query_desc, QueryState::START, false /* utility */)) {
    return;
  }

  auto &query = get_query(query_desc);
  auto query_msg = query.message.get();
  *query_msg->mutable_start_time() = current_ts();
  update_query_state(query, QueryState::START, false /* utility */);
  set_query_plan(query_msg, query_desc);
  if (need_collect_analyze()) {
    // Set up to track total elapsed time during query run.
    // Make sure the space is allocated in the per-query
    // context so it will go away at executor_end.
    if (query_desc->totaltime == NULL) {
      MemoryContext oldcxt =
          ya_gpdb::mem_ctx_switch_to(query_desc->estate->es_query_cxt);
      query_desc->totaltime = ya_gpdb::instr_alloc(1, INSTRUMENT_ALL, false);
      ya_gpdb::mem_ctx_switch_to(oldcxt);
    }
  }
  yagpcc::GPMetrics stats;
  std::swap(stats, *query_msg->mutable_query_metrics());
  if (log_query_req(*query_msg, "started", false /* utility */)) {
    clear_big_fields(query_msg);
  }
  std::swap(stats, *query_msg->mutable_query_metrics());
}

void EventSender::executor_end(QueryDesc *query_desc) {
  if (!verify_query(query_desc, QueryState::END, false /* utility */)) {
    return;
  }

  auto &query = get_query(query_desc);
  auto *query_msg = query.message.get();
  *query_msg->mutable_end_time() = current_ts();
  update_query_state(query, QueryState::END, false /* utility */);
  if (is_top_level_query(query_desc, nesting_level)) {
    set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, nested_calls,
                   nested_timing);
  } else {
    set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, 0, 0);
  }
  if (log_query_req(*query_msg, "ended", false /* utility */)) {
    clear_big_fields(query_msg);
  }
}

void EventSender::collect_query_submit(QueryDesc *query_desc, bool utility) {
  if (!verify_query(query_desc, QueryState::SUBMIT, utility)) {
    return;
  }

  submit_query(query_desc);
  auto &query = get_query(query_desc);
  auto *query_msg = query.message.get();
  *query_msg = create_query_req(yagpcc::QueryStatus::QUERY_STATUS_SUBMIT);
  *query_msg->mutable_submit_time() = current_ts();
  set_query_info(query_msg);
  set_qi_nesting_level(query_msg, nesting_level);
  set_qi_slice_id(query_msg);
  set_query_text(query_msg, query_desc);
  if (log_query_req(*query_msg, "submit", utility)) {
    clear_big_fields(query_msg);
  }
  // take initial metrics snapshot so that we can safely take diff afterwards
  // in END or DONE events.
  set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, 0, 0);
#ifdef IC_TEARDOWN_HOOK
  // same for interconnect statistics
  ic_metrics_collect();
  set_ic_stats(query_msg->mutable_query_metrics()->mutable_instrumentation(),
               &ic_statistics);
#endif
}

void EventSender::report_query_done(QueryDesc *query_desc, QueryItem &query,
                                    QueryMetricsStatus status, bool utility,
                                    ErrorData *edata) {
  yagpcc::QueryStatus query_status;
  std::string msg;
  switch (status) {
  case METRICS_QUERY_DONE:
  case METRICS_INNER_QUERY_DONE:
    query_status = yagpcc::QueryStatus::QUERY_STATUS_DONE;
    msg = "done";
    break;
  case METRICS_QUERY_ERROR:
    query_status = yagpcc::QueryStatus::QUERY_STATUS_ERROR;
    msg = "error";
    break;
  case METRICS_QUERY_CANCELING:
    // at the moment we don't track this event, but I`ll leave this code
    // here just in case
    Assert(false);
    query_status = yagpcc::QueryStatus::QUERY_STATUS_CANCELLING;
    msg = "cancelling";
    break;
  case METRICS_QUERY_CANCELED:
    query_status = yagpcc::QueryStatus::QUERY_STATUS_CANCELED;
    msg = "cancelled";
    break;
  default:
    ereport(FATAL,
            (errmsg("Unexpected query status in query_done hook: %d", status)));
  }
  auto prev_state = query.state;
  update_query_state(query, QueryState::DONE, utility,
                     query_status == yagpcc::QueryStatus::QUERY_STATUS_DONE);
  auto query_msg = query.message.get();
  query_msg->set_query_status(query_status);
  if (status == METRICS_QUERY_ERROR) {
    bool error_flushed = elog_message() == NULL;
    if (error_flushed && edata->message == NULL) {
      ereport(WARNING, (errmsg("YAGPCC missing error message")));
      ereport(DEBUG3,
              (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
    } else {
      set_qi_error_message(query_msg,
                           error_flushed ? edata->message : elog_message());
    }
  }
  if (prev_state == START) {
    // We've missed ExecutorEnd call due to query cancel or error. It's
    // fine, but now we need to collect and report execution stats
    *query_msg->mutable_end_time() = current_ts();
    set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, nested_calls,
                   nested_timing);
  }
#ifdef IC_TEARDOWN_HOOK
  ic_metrics_collect();
  set_ic_stats(query_msg->mutable_query_metrics()->mutable_instrumentation(),
               &ic_statistics);
#endif
  (void)log_query_req(*query_msg, msg, utility);
}

void EventSender::collect_query_done(QueryDesc *query_desc, bool utility,
                                     QueryMetricsStatus status,
                                     ErrorData *edata) {
  if (!verify_query(query_desc, QueryState::DONE, utility)) {
    return;
  }

  // Skip sending done message if query errored before submit.
  if (!qdesc_submitted(query_desc)) {
    if (status != METRICS_QUERY_ERROR) {
      ereport(WARNING, (errmsg("YAGPCC trying to process DONE hook for "
                               "unsubmitted and unerrored query")));
      ereport(DEBUG3,
              (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
    }
    return;
  }

  if (queries.empty()) {
    ereport(WARNING, (errmsg("YAGPCC cannot find query to process DONE hook")));
    ereport(DEBUG3,
            (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
    return;
  }
  auto &query = get_query(query_desc);

  report_query_done(query_desc, query, status, utility, edata);

  if (need_report_nested_query())
    update_nested_counters(query_desc);

  queries.erase(QueryKey::from_qdesc(query_desc));
  pfree(query_desc->yagp_query_key);
  query_desc->yagp_query_key = NULL;
}

void EventSender::ic_metrics_collect() {
#ifdef IC_TEARDOWN_HOOK
  if (Gp_interconnect_type != INTERCONNECT_TYPE_UDPIFC) {
    return;
  }
  if (!proto_verified || gp_command_count == 0 || !Config::enable_collector() ||
      Config::filter_user(get_user_name())) {
    return;
  }
  // we also would like to know nesting level here and filter queries BUT we
  // don't have this kind of information from this callback. Will have to
  // collect stats anyways and throw it away later, if necessary
  auto metrics = UDPIFCGetICStats();
  ic_statistics.totalRecvQueueSize += metrics.totalRecvQueueSize;
  ic_statistics.recvQueueSizeCountingTime += metrics.recvQueueSizeCountingTime;
  ic_statistics.totalCapacity += metrics.totalCapacity;
  ic_statistics.capacityCountingTime += metrics.capacityCountingTime;
  ic_statistics.totalBuffers += metrics.totalBuffers;
  ic_statistics.bufferCountingTime += metrics.bufferCountingTime;
  ic_statistics.activeConnectionsNum += metrics.activeConnectionsNum;
  ic_statistics.retransmits += metrics.retransmits;
  ic_statistics.startupCachedPktNum += metrics.startupCachedPktNum;
  ic_statistics.mismatchNum += metrics.mismatchNum;
  ic_statistics.crcErrors += metrics.crcErrors;
  ic_statistics.sndPktNum += metrics.sndPktNum;
  ic_statistics.recvPktNum += metrics.recvPktNum;
  ic_statistics.disorderedPktNum += metrics.disorderedPktNum;
  ic_statistics.duplicatedPktNum += metrics.duplicatedPktNum;
  ic_statistics.recvAckNum += metrics.recvAckNum;
  ic_statistics.statusQueryMsgNum += metrics.statusQueryMsgNum;
#endif
}

void EventSender::analyze_stats_collect(QueryDesc *query_desc) {
  if (!verify_query(query_desc, QueryState::END, false /* utility */)) {
    return;
  }
  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }
  if (!query_desc->totaltime || !need_collect_analyze()) {
    return;
  }
  // Make sure stats accumulation is done.
  // (Note: it's okay if several levels of hook all do this.)
  ya_gpdb::instr_end_loop(query_desc->totaltime);

  double ms = query_desc->totaltime->total * 1000.0;
  if (ms >= Config::min_analyze_time()) {
    auto &query = get_query(query_desc);
    auto *query_msg = query.message.get();
    set_analyze_plan_text(query_desc, query_msg);
  }
}

EventSender::EventSender() {
  if (Config::enable_collector()) {
    try {
      GOOGLE_PROTOBUF_VERIFY_VERSION;
      proto_verified = true;
    } catch (const std::exception &e) {
      ereport(INFO, (errmsg("Unable to start query tracing %s", e.what())));
    }
  }
#ifdef IC_TEARDOWN_HOOK
  memset(&ic_statistics, 0, sizeof(ICStatistics));
#endif
}

EventSender::~EventSender() {
  for (const auto &[qkey, _] : queries) {
    ereport(LOG, (errmsg("YAGPCC query with missing done event: "
                         "tmid=%d ssid=%d ccnt=%d nlvl=%d",
                         qkey.tmid, qkey.ssid, qkey.ccnt, qkey.nesting_level)));
  }
}

// That's basically a very simplistic state machine to fix or highlight any bugs
// coming from GP
void EventSender::update_query_state(QueryItem &query, QueryState new_state,
                                     bool utility, bool success) {
  switch (new_state) {
  case QueryState::SUBMIT:
    Assert(false);
    break;
  case QueryState::START:
    if (query.state == QueryState::SUBMIT) {
      query.message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    } else {
      Assert(false);
    }
    break;
  case QueryState::END:
    // Example of below assert triggering: CURSOR closes before ever being
    // executed Assert(query->state == QueryState::START ||
    // IsAbortInProgress());
    query.message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_END);
    break;
  case QueryState::DONE:
    Assert(query.state == QueryState::END || !success || utility);
    query.message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_DONE);
    break;
  default:
    Assert(false);
  }
  query.state = new_state;
}

EventSender::QueryItem &EventSender::get_query(QueryDesc *query_desc) {
  if (!qdesc_submitted(query_desc)) {
    ereport(WARNING,
            (errmsg("YAGPCC attempting to get query that was not submitted")));
    ereport(DEBUG3,
            (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
    throw std::runtime_error("Attempting to get query that was not submitted");
  }
  return queries.find(QueryKey::from_qdesc(query_desc))->second;
}

void EventSender::submit_query(QueryDesc *query_desc) {
  if (query_desc->yagp_query_key) {
    ereport(WARNING,
            (errmsg("YAGPCC trying to submit already submitted query")));
    ereport(DEBUG3,
            (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
  }
  QueryKey::register_qkey(query_desc, nesting_level);
  auto key = QueryKey::from_qdesc(query_desc);
  auto [_, inserted] = queries.emplace(key, QueryItem(QueryState::SUBMIT));
  if (!inserted) {
    ereport(WARNING, (errmsg("YAGPCC duplicate query submit detected")));
    ereport(DEBUG3,
            (errmsg("YAGPCC query sourceText: %s", query_desc->sourceText)));
  }
}

void EventSender::update_nested_counters(QueryDesc *query_desc) {
  if (!is_top_level_query(query_desc, nesting_level)) {
    auto &query = get_query(query_desc);
    nested_calls++;
    double end_time = protots_to_double(query.message->end_time());
    double start_time = protots_to_double(query.message->start_time());
    if (end_time >= start_time) {
      nested_timing += end_time - start_time;
    } else {
      ereport(WARNING, (errmsg("YAGPCC query start_time > end_time (%f > %f)",
                               start_time, end_time)));
      ereport(DEBUG3,
              (errmsg("YAGPCC nested query text %s", query_desc->sourceText)));
    }
  }
}

bool EventSender::qdesc_submitted(QueryDesc *query_desc) {
  if (query_desc->yagp_query_key == NULL) {
    return false;
  }
  return queries.find(QueryKey::from_qdesc(query_desc)) != queries.end();
}

EventSender::QueryItem::QueryItem(QueryState st)
    : message(std::make_unique<yagpcc::SetQueryReq>()), state(st) {}
