#include "Config.h"
#include "ProcStats.h"
#include "UDSConnector.h"
#include <chrono>
#include <ctime>

#define typeid __typeid
#define operator __operator
extern "C" {
#include "postgres.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "commands/resgroupcmds.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/workfile_mgr.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbvars.h"

#include "stat_statements_parser/pg_stat_statements_ya_parser.h"
#include "tcop/utility.h"
}
#undef typeid
#undef operator

#include "EventSender.h"

#define need_collect()                                                         \
  (nesting_level == 0 && gp_command_count != 0 &&                              \
   query_desc->sourceText != nullptr && Config::enable_collector() &&          \
   !Config::filter_user(get_user_name()))

namespace {

std::string *get_user_name() {
  const char *username = GetConfigOption("session_authorization", false, false);
  return username ? new std::string(username) : nullptr;
}

std::string *get_db_name() {
  char *dbname = get_database_name(MyDatabaseId);
  std::string *result = dbname ? new std::string(dbname) : nullptr;
  return result;
}

std::string *get_rg_name() {
  auto userId = GetUserId();
  if (!OidIsValid(userId))
    return nullptr;
  auto groupId = GetResGroupIdForRole(userId);
  if (!OidIsValid(groupId))
    return nullptr;
  char *rgname = GetResGroupNameForId(groupId);
  if (rgname == nullptr)
    return nullptr;
  auto result = new std::string(rgname);
  return result;
}

google::protobuf::Timestamp current_ts() {
  google::protobuf::Timestamp current_ts;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  current_ts.set_seconds(tv.tv_sec);
  current_ts.set_nanos(static_cast<int32_t>(tv.tv_usec * 1000));
  return current_ts;
}

void set_query_key(yagpcc::QueryKey *key, QueryDesc *query_desc) {
  key->set_ccnt(gp_command_count);
  key->set_ssid(gp_session_id);
  int32 tmid = 0;
  gpmon_gettmid(&tmid);
  key->set_tmid(tmid);
}

void set_segment_key(yagpcc::SegmentKey *key, QueryDesc *query_desc) {
  key->set_dbid(GpIdentity.dbid);
  key->set_segindex(GpIdentity.segindex);
}

ExplainState get_explain_state(QueryDesc *query_desc, bool costs) {
  ExplainState es;
  ExplainInitState(&es);
  es.costs = costs;
  es.verbose = true;
  es.format = EXPLAIN_FORMAT_TEXT;
  ExplainBeginOutput(&es);
  ExplainPrintPlan(&es, query_desc);
  ExplainEndOutput(&es);
  return es;
}

void set_plan_text(std::string *plan_text, QueryDesc *query_desc) {
  auto es = get_explain_state(query_desc, true);
  *plan_text = std::string(es.str->data, es.str->len);
}

void set_query_plan(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->plannedstmt) {
    auto qi = req->mutable_query_info();
    qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                          ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                          : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
    set_plan_text(qi->mutable_plan_text(), query_desc);
    StringInfo norm_plan = gen_normplan(qi->plan_text().c_str());
    *qi->mutable_template_plan_text() = std::string(norm_plan->data);
    qi->set_plan_id(hash_any((unsigned char *)norm_plan->data, norm_plan->len));
    // TODO: For now assume queryid equal to planid, which is wrong. The
    // reason for doing so this bug
    // https://github.com/greenplum-db/gpdb/pull/15385 (ORCA loses
    // pg_stat_statements` queryid during planning phase). Need to fix it
    // upstream, cherry-pick and bump gp
    // qi->set_query_id(query_desc->plannedstmt->queryId);
    qi->set_query_id(qi->plan_id());
  }
}

void set_query_text(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->sourceText) {
    auto qi = req->mutable_query_info();
    *qi->mutable_query_text() = query_desc->sourceText;
    char *norm_query = gen_normquery(query_desc->sourceText);
    *qi->mutable_template_query_text() = std::string(norm_query);
  }
}

void clear_big_fields(yagpcc::SetQueryReq *req) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    auto qi = req->mutable_query_info();
    qi->clear_plan_text();
    qi->clear_query_text();
  }
}

void set_query_info(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    auto qi = req->mutable_query_info();
    qi->set_allocated_username(get_user_name());
    qi->set_allocated_databasename(get_db_name());
    qi->set_allocated_rsgname(get_rg_name());
  }
}

void set_metric_instrumentation(yagpcc::MetricInstrumentation *metrics,
                                QueryDesc *query_desc) {
  auto instrument = query_desc->planstate->instrument;
  if (instrument) {
    metrics->set_ntuples(instrument->ntuples);
    metrics->set_nloops(instrument->nloops);
    metrics->set_tuplecount(instrument->tuplecount);
    metrics->set_firsttuple(instrument->firsttuple);
    metrics->set_startup(instrument->startup);
    metrics->set_total(instrument->total);
    auto &buffusage = instrument->bufusage;
    metrics->set_shared_blks_hit(buffusage.shared_blks_hit);
    metrics->set_shared_blks_read(buffusage.shared_blks_read);
    metrics->set_shared_blks_dirtied(buffusage.shared_blks_dirtied);
    metrics->set_shared_blks_written(buffusage.shared_blks_written);
    metrics->set_local_blks_hit(buffusage.local_blks_hit);
    metrics->set_local_blks_read(buffusage.local_blks_read);
    metrics->set_local_blks_dirtied(buffusage.local_blks_dirtied);
    metrics->set_local_blks_written(buffusage.local_blks_written);
    metrics->set_temp_blks_read(buffusage.temp_blks_read);
    metrics->set_temp_blks_written(buffusage.temp_blks_written);
    metrics->set_blk_read_time(INSTR_TIME_GET_DOUBLE(buffusage.blk_read_time));
    metrics->set_blk_write_time(
        INSTR_TIME_GET_DOUBLE(buffusage.blk_write_time));
  }
  if (query_desc->estate && query_desc->estate->motionlayer_context) {
    MotionLayerState *mlstate =
        (MotionLayerState *)query_desc->estate->motionlayer_context;
    metrics->mutable_sent()->set_total_bytes(mlstate->stat_total_bytes_sent);
    metrics->mutable_sent()->set_tuple_bytes(mlstate->stat_tuple_bytes_sent);
    metrics->mutable_sent()->set_chunks(mlstate->stat_total_chunks_sent);
    metrics->mutable_received()->set_total_bytes(
        mlstate->stat_total_bytes_recvd);
    metrics->mutable_received()->set_tuple_bytes(
        mlstate->stat_tuple_bytes_recvd);
    metrics->mutable_received()->set_chunks(mlstate->stat_total_chunks_recvd);
  }
}

decltype(std::chrono::high_resolution_clock::now()) query_start_time;

void set_gp_metrics(yagpcc::GPMetrics *metrics, QueryDesc *query_desc) {
  if (query_desc->planstate && query_desc->planstate->instrument) {
    set_metric_instrumentation(metrics->mutable_instrumentation(), query_desc);
  }
  fill_self_stats(metrics->mutable_systemstat());
  std::chrono::duration<double> elapsed_seconds =
      std::chrono::high_resolution_clock::now() - query_start_time;
  metrics->mutable_systemstat()->set_runningtimeseconds(
      elapsed_seconds.count());
  metrics->mutable_spill()->set_filecount(WorkfileTotalFilesCreated());
  metrics->mutable_spill()->set_totalbytes(WorkfileTotalBytesWritten());
}

yagpcc::SetQueryReq create_query_req(QueryDesc *query_desc,
                                     yagpcc::QueryStatus status) {
  yagpcc::SetQueryReq req;
  req.set_query_status(status);
  *req.mutable_datetime() = current_ts();
  set_query_key(req.mutable_query_key(), query_desc);
  set_segment_key(req.mutable_segment_key(), query_desc);
  return req;
}

} // namespace

void EventSender::query_metrics_collect(QueryMetricsStatus status, void *arg) {
  if (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_EXECUTE) {
    return;
  }
  switch (status) {
  case METRICS_PLAN_NODE_INITIALIZE:
  case METRICS_PLAN_NODE_EXECUTING:
  case METRICS_PLAN_NODE_FINISHED:
    // TODO
    break;
  case METRICS_QUERY_SUBMIT:
    collect_query_submit(reinterpret_cast<QueryDesc *>(arg));
    break;
  case METRICS_QUERY_START:
    // no-op: executor_after_start is enough
    break;
  case METRICS_QUERY_DONE:
  case METRICS_QUERY_ERROR:
  case METRICS_QUERY_CANCELING:
  case METRICS_QUERY_CANCELED:
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), status);
    break;
  case METRICS_INNER_QUERY_DONE:
    // TODO
    break;
  default:
    ereport(FATAL, (errmsg("Unknown query status: %d", status)));
  }
}

void EventSender::executor_before_start(QueryDesc *query_desc,
                                        int /* eflags*/) {
  if (!connector) {
    return;
  }
  if (!need_collect()) {
    return;
  }
  if (query_msg->has_query_key()) {
    connector->report_query(*query_msg, "previous query");
    query_msg->Clear();
  }
  query_start_time = std::chrono::high_resolution_clock::now();
  WorkfileResetBackendStats();
  if (Gp_role == GP_ROLE_DISPATCH && Config::enable_analyze()) {
    query_desc->instrument_options |= INSTRUMENT_BUFFERS;
    query_desc->instrument_options |= INSTRUMENT_ROWS;
    query_desc->instrument_options |= INSTRUMENT_TIMER;
    if (Config::enable_cdbstats()) {
      query_desc->instrument_options |= INSTRUMENT_CDB;

      instr_time starttime;
      INSTR_TIME_SET_CURRENT(starttime);
      query_desc->showstatctx =
          cdbexplain_showExecStatsBegin(query_desc, starttime);
    }
  }
}

void EventSender::executor_after_start(QueryDesc *query_desc, int /* eflags*/) {
  if (!connector) {
    return;
  }
  if ((Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) &&
      need_collect()) {
    query_msg->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    *query_msg->mutable_start_time() = current_ts();
    set_query_plan(query_msg, query_desc);
    if (connector->report_query(*query_msg, "started")) {
      clear_big_fields(query_msg);
    }
  }
}

void EventSender::executor_end(QueryDesc *query_desc) {
  if (!connector) {
    return;
  }
  if (!need_collect() ||
      (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_EXECUTE)) {
    return;
  }
  /* TODO: when querying via CURSOR this call freezes. Need to investigate.
     To reproduce - uncomment it and run installchecks. It will freeze around
  join test. Needs investigation

    if (Gp_role == GP_ROLE_DISPATCH && Config::enable_analyze() &&
      Config::enable_cdbstats() && query_desc->estate->dispatcherState &&
      query_desc->estate->dispatcherState->primaryResults) {
    cdbdisp_checkDispatchResult(query_desc->estate->dispatcherState,
                                DISPATCH_WAIT_NONE);
  }*/
  query_msg->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_END);
  *query_msg->mutable_end_time() = current_ts();
  set_gp_metrics(query_msg->mutable_query_metrics(), query_desc);
  if (connector->report_query(*query_msg, "ended")) {
    query_msg->Clear();
  }
}

void EventSender::collect_query_submit(QueryDesc *query_desc) {
  if (connector && need_collect()) {
    *query_msg =
        create_query_req(query_desc, yagpcc::QueryStatus::QUERY_STATUS_SUBMIT);
    *query_msg->mutable_submit_time() = current_ts();
    set_query_info(query_msg, query_desc);
    set_query_text(query_msg, query_desc);
    if (connector->report_query(*query_msg, "submit")) {
      clear_big_fields(query_msg);
    }
  }
}

void EventSender::collect_query_done(QueryDesc *query_desc,
                                     QueryMetricsStatus status) {
  if (connector && need_collect()) {
    yagpcc::QueryStatus query_status;
    std::string msg;
    switch (status) {
    case METRICS_QUERY_DONE:
      query_status = yagpcc::QueryStatus::QUERY_STATUS_DONE;
      msg = "done";
      break;
    case METRICS_QUERY_ERROR:
      query_status = yagpcc::QueryStatus::QUERY_STATUS_ERROR;
      msg = "error";
      break;
    case METRICS_QUERY_CANCELING:
      query_status = yagpcc::QueryStatus::QUERY_STATUS_CANCELLING;
      msg = "cancelling";
      break;
    case METRICS_QUERY_CANCELED:
      query_status = yagpcc::QueryStatus::QUERY_STATUS_CANCELED;
      msg = "cancelled";
      break;
    default:
      ereport(FATAL, (errmsg("Unexpected query status in query_done hook: %d",
                             status)));
    }
    query_msg->set_query_status(query_status);
    if (connector->report_query(*query_msg, msg)) {
      clear_big_fields(query_msg);
    }
  }
}

EventSender::EventSender() {
  if (Config::enable_collector() && !Config::filter_user(get_user_name())) {
    query_msg = new yagpcc::SetQueryReq();
    try {
      connector = new UDSConnector();
    } catch (const std::exception &e) {
      ereport(INFO, (errmsg("Unable to start query tracing %s", e.what())));
    }
  }
}

EventSender::~EventSender() {
  delete query_msg;
  delete connector;
}