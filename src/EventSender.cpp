#include "GrpcConnector.h"
#include "ProcStats.h"
#include <ctime>

extern "C" {
#include "postgres.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "commands/resgroupcmds.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/metrics_utils.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"

#include "stat_statements_parser/pg_stat_statements_ya_parser.h"
#include "tcop/utility.h"

void get_spill_info(int ssid, int ccid, int32_t *file_count,
                    int64_t *total_bytes);
}

#include "EventSender.h"

namespace {

std::string *get_user_name() {
  const char *username = GetConfigOption("session_authorization", false, false);
  return username ? new std::string(username) : nullptr;
}

std::string *get_db_name() {
  char *dbname = get_database_name(MyDatabaseId);
  std::string *result = dbname ? new std::string(dbname) : nullptr;
  pfree(dbname);
  return result;
}

int get_cur_slice_id(QueryDesc *desc) {
  if (!desc->estate) {
    return 0;
  }
  return LocallyExecutingSliceIndex(desc->estate);
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

void set_query_plan(yagpcc::QueryInfo *qi, QueryDesc *query_desc) {
  qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                        ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                        : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
  set_plan_text(qi->mutable_plan_text(), query_desc);
  StringInfo norm_plan = gen_normplan(qi->plan_text().c_str());
  *qi->mutable_temlate_plan_text() = std::string(norm_plan->data);
  qi->set_plan_id(hash_any((unsigned char *)norm_plan->data, norm_plan->len));
  // TODO: free stringinfo?
}

void set_query_text(yagpcc::QueryInfo *qi, QueryDesc *query_desc) {
  *qi->mutable_query_text() = query_desc->sourceText;
  char *norm_query = gen_normquery(query_desc->sourceText);
  *qi->mutable_temlate_query_text() = std::string(norm_query);
  pfree(norm_query);
}

void set_query_info(yagpcc::QueryInfo *qi, QueryDesc *query_desc,
                    bool with_text, bool with_plan) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    if (query_desc->sourceText && with_text) {
      set_query_text(qi, query_desc);
    }
    if (query_desc->plannedstmt && with_plan) {
      set_query_plan(qi, query_desc);
      qi->set_query_id(query_desc->plannedstmt->queryId);
    }
    qi->set_allocated_username(get_user_name());
    qi->set_allocated_databasename(get_db_name());
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
}

void set_gp_metrics(yagpcc::GPMetrics *metrics, QueryDesc *query_desc,
                    bool need_spillinfo) {
  if (need_spillinfo) {
    int32_t n_spill_files = 0;
    int64_t n_spill_bytes = 0;
    get_spill_info(gp_session_id, gp_command_count, &n_spill_files,
                   &n_spill_bytes);
    metrics->mutable_spill()->set_filecount(n_spill_files);
    metrics->mutable_spill()->set_totalbytes(n_spill_bytes);
  }
  if (query_desc->planstate && query_desc->planstate->instrument) {
    set_metric_instrumentation(metrics->mutable_instrumentation(), query_desc);
  }
  fill_self_stats(metrics->mutable_systemstat());
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
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), "done");
    break;
  case METRICS_QUERY_ERROR:
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), "error");
    break;
  case METRICS_QUERY_CANCELING:
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), "calcelling");
    break;
  case METRICS_QUERY_CANCELED:
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), "cancelled");
    break;
  case METRICS_INNER_QUERY_DONE:
    // TODO
    break;
  default:
    elog(FATAL, "Unknown query status: %d", status);
  }
}

void EventSender::executor_after_start(QueryDesc *query_desc, int /* eflags*/) {
  if (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_EXECUTE) {
    return;
  }
  auto req =
      create_query_req(query_desc, yagpcc::QueryStatus::QUERY_STATUS_START);
  set_query_info(req.mutable_query_info(), query_desc, false, true);
  send_query_info(&req, "started");
}

void EventSender::collect_query_submit(QueryDesc *query_desc) {
  query_desc->instrument_options |= INSTRUMENT_BUFFERS;
  query_desc->instrument_options |= INSTRUMENT_ROWS;
  query_desc->instrument_options |= INSTRUMENT_TIMER;

  auto req =
      create_query_req(query_desc, yagpcc::QueryStatus::QUERY_STATUS_SUBMIT);
  set_query_info(req.mutable_query_info(), query_desc, true, false);
  send_query_info(&req, "submit");
}

void EventSender::collect_query_done(QueryDesc *query_desc,
                                     const std::string &status) {
  auto req =
      create_query_req(query_desc, yagpcc::QueryStatus::QUERY_STATUS_DONE);
  set_query_info(req.mutable_query_info(), query_desc, false, false);
  // NOTE: there are no cummulative spillinfo stats AFAIU, so no need to gather
  // it here. It only makes sense when doing regular stat checks.
  set_gp_metrics(req.mutable_query_metrics(), query_desc,
                 /*need_spillinfo*/ false);
  send_query_info(&req, status);
}

void EventSender::send_query_info(yagpcc::SetQueryReq *req,
                                  const std::string &event) {
  auto result = connector->set_metric_query(*req);
  if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR) {
    elog(WARNING, "Query {%d-%d-%d} %s reporting failed with an error %s",
         req->query_key().tmid(), req->query_key().ssid(),
         req->query_key().ccnt(), event.c_str(), result.error_text().c_str());
  }
}

EventSender *EventSender::instance() {
  static EventSender sender;
  return &sender;
}

EventSender::EventSender() { connector = std::make_unique<GrpcConnector>(); }