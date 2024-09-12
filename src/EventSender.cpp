#include "Config.h"
#include "ProcStats.h"
#include "UDSConnector.h"
#include <ctime>

#define typeid __typeid
#define operator __operator
extern "C" {
#include "postgres.h"

#include "access/hash.h"
#include "access/xact.h"
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

namespace {

std::string *get_user_name() {
  const char *username = GetConfigOption("session_authorization", false, false);
  // username is not to be freed
  return username ? new std::string(username) : nullptr;
}

std::string *get_db_name() {
  char *dbname = get_database_name(MyDatabaseId);
  std::string *result = nullptr;
  if (dbname) {
    result = new std::string(dbname);
    pfree(dbname);
  }
  return result;
}

std::string *get_rg_name() {
  auto groupId = ResGroupGetGroupIdBySessionId(MySessionState->sessionId);
  if (!OidIsValid(groupId))
    return nullptr;
  char *rgname = GetResGroupNameForId(groupId);
  if (rgname == nullptr)
    return nullptr;
  return new std::string(rgname);
}

/**
 * Things get tricky with nested queries.
 * a) A nested query on master is a real query optimized and executed from
 * master. An example would be `select some_insert_function();`, where
 * some_insert_function does something like `insert into tbl values (1)`. Master
 * will create two statements. Outer select statement and inner insert statement
 * with nesting level 1.
 * For segments both statements are top-level statements with nesting level 0.
 * b) A nested query on segment is something executed as sub-statement on
 * segment. An example would be `select a from tbl where is_good_value(b);`. In
 * this case master will issue one top-level statement, but segments will change
 * contexts for UDF execution and execute  is_good_value(b) once for each tuple
 * as a nested query. Creating massive load on gpcc agent.
 *
 * Hence, here is a decision:
 * 1) ignore all queries that are nested on segments
 * 2) record (if enabled) all queries that are nested on master
 * NODE: The truth is, we can't really ignore nested master queries, because
 * segment sees those as top-level.
 */

inline bool is_top_level_query(QueryDesc *query_desc, int nesting_level) {
  return (query_desc->gpmon_pkt &&
          query_desc->gpmon_pkt->u.qexec.key.tmid == 0) ||
         nesting_level == 0;
}

inline bool nesting_is_valid(QueryDesc *query_desc, int nesting_level) {
  return (Gp_session_role == GP_ROLE_DISPATCH &&
          Config::report_nested_queries()) ||
         is_top_level_query(query_desc, nesting_level);
}

bool need_report_nested_query() {
  return Config::report_nested_queries() && Gp_session_role == GP_ROLE_DISPATCH;
}

inline bool filter_query(QueryDesc *query_desc) {
  return gp_command_count == 0 || query_desc->sourceText == nullptr ||
         !Config::enable_collector() || Config::filter_user(get_user_name());
}

inline bool need_collect(QueryDesc *query_desc, int nesting_level) {
  return !filter_query(query_desc) &&
         nesting_is_valid(query_desc, nesting_level);
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

inline std::string char_to_trimmed_str(const char *str, size_t len) {
  return std::string(str, std::min(len, Config::max_text_size()));
}

void set_query_plan(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->plannedstmt) {
    auto qi = req->mutable_query_info();
    qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                          ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                          : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
    MemoryContext oldcxt =
        MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
    auto es = get_explain_state(query_desc, true);
    MemoryContextSwitchTo(oldcxt);
    *qi->mutable_plan_text() = char_to_trimmed_str(es.str->data, es.str->len);
    StringInfo norm_plan = gen_normplan(es.str->data);
    *qi->mutable_template_plan_text() =
        char_to_trimmed_str(norm_plan->data, norm_plan->len);
    qi->set_plan_id(hash_any((unsigned char *)norm_plan->data, norm_plan->len));
    qi->set_query_id(query_desc->plannedstmt->queryId);
    pfree(es.str->data);
    pfree(norm_plan->data);
  }
}

void set_query_text(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->sourceText) {
    auto qi = req->mutable_query_info();
    *qi->mutable_query_text() = char_to_trimmed_str(
        query_desc->sourceText, strlen(query_desc->sourceText));
    char *norm_query = gen_normquery(query_desc->sourceText);
    *qi->mutable_template_query_text() =
        char_to_trimmed_str(norm_query, strlen(norm_query));
  }
}

void clear_big_fields(yagpcc::SetQueryReq *req) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    auto qi = req->mutable_query_info();
    qi->clear_plan_text();
    qi->clear_template_plan_text();
    qi->clear_query_text();
    qi->clear_template_query_text();
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

void set_qi_nesting_level(yagpcc::SetQueryReq *req, int nesting_level) {
  auto aqi = req->mutable_add_info();
  aqi->set_nested_level(nesting_level);
}

void set_qi_slice_id(yagpcc::SetQueryReq *req) {
  auto aqi = req->mutable_add_info();
  aqi->set_slice_id(currentSliceId);
}

void set_qi_error_message(yagpcc::SetQueryReq *req) {
  auto aqi = req->mutable_add_info();
  auto error = elog_message();
  *aqi->mutable_error_message() = char_to_trimmed_str(error, strlen(error));
}

void set_metric_instrumentation(yagpcc::MetricInstrumentation *metrics,
                                QueryDesc *query_desc, int nested_calls,
                                double nested_time) {
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
  metrics->set_inherited_calls(nested_calls);
  metrics->set_inherited_time(nested_time);
}

void set_gp_metrics(yagpcc::GPMetrics *metrics, QueryDesc *query_desc,
                    int nested_calls, double nested_time) {
  if (query_desc->planstate && query_desc->planstate->instrument) {
    set_metric_instrumentation(metrics->mutable_instrumentation(), query_desc,
                               nested_calls, nested_time);
  }
  fill_self_stats(metrics->mutable_systemstat());
  metrics->mutable_systemstat()->set_runningtimeseconds(
      time(NULL) - metrics->mutable_systemstat()->runningtimeseconds());
  metrics->mutable_spill()->set_filecount(
      WorkfileTotalFilesCreated() - metrics->mutable_spill()->filecount());
  metrics->mutable_spill()->set_totalbytes(
      WorkfileTotalBytesWritten() - metrics->mutable_spill()->totalbytes());
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

double protots_to_double(const google::protobuf::Timestamp &ts) {
  return double(ts.seconds()) + double(ts.nanos()) / 1000000000.0;
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
    // don't collect anything here. We will fake this call in ExecutorStart as
    // it really makes no difference. Just complicates things
    break;
  case METRICS_QUERY_START:
    // no-op: executor_after_start is enough
    break;
  case METRICS_QUERY_CANCELING:
    // it appears we're unly interested in the actual CANCELED event.
    // for now we will ignore CANCELING state unless otherwise requested from
    // end users
    break;
  case METRICS_QUERY_DONE:
  case METRICS_QUERY_ERROR:
  case METRICS_QUERY_CANCELED:
  case METRICS_INNER_QUERY_DONE:
    collect_query_done(reinterpret_cast<QueryDesc *>(arg), status);
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
  if (is_top_level_query(query_desc, nesting_level)) {
    nested_timing = 0;
    nested_calls = 0;
  }
  if (!need_collect(query_desc, nesting_level)) {
    return;
  }
  collect_query_submit(query_desc);
  if (Gp_role == GP_ROLE_DISPATCH && Config::enable_analyze()) {
    query_desc->instrument_options |= INSTRUMENT_BUFFERS;
    query_desc->instrument_options |= INSTRUMENT_ROWS;
    query_desc->instrument_options |= INSTRUMENT_TIMER;
    if (Config::enable_cdbstats()) {
      query_desc->instrument_options |= INSTRUMENT_CDB;
      if (!query_desc->showstatctx) {
        instr_time starttime;
        INSTR_TIME_SET_CURRENT(starttime);
        query_desc->showstatctx =
            cdbexplain_showExecStatsBegin(query_desc, starttime);
      }
    }
  }
}

void EventSender::executor_after_start(QueryDesc *query_desc, int /* eflags*/) {
  if (!connector) {
    return;
  }
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    if (!filter_query(query_desc)) {
      auto *query = get_query_message(query_desc);
      auto query_msg = query->message;
      *query_msg->mutable_start_time() = current_ts();
      if (!nesting_is_valid(query_desc, nesting_level)) {
        return;
      }
      update_query_state(query_desc, query, QueryState::START);
      set_query_plan(query_msg, query_desc);
      yagpcc::GPMetrics stats;
      std::swap(stats, *query_msg->mutable_query_metrics());
      if (connector->report_query(*query_msg, "started")) {
        clear_big_fields(query_msg);
      }
      std::swap(stats, *query_msg->mutable_query_metrics());
    }
  }
}

void EventSender::executor_end(QueryDesc *query_desc) {
  if (!connector ||
      (Gp_role != GP_ROLE_DISPATCH && Gp_role != GP_ROLE_EXECUTE)) {
    return;
  }
  if (!filter_query(query_desc)) {
    auto *query = get_query_message(query_desc);
    auto query_msg = query->message;
    *query_msg->mutable_end_time() = current_ts();
    if (nesting_is_valid(query_desc, nesting_level)) {
      if (query->state == UNKNOWN &&
          // Yet another greenplum weirdness: thats actually a nested query
          // which is being committed/rollbacked. Treat it accordingly.
          !need_report_nested_query()) {
        return;
      }
      update_query_state(query_desc, query, QueryState::END);
      if (is_top_level_query(query_desc, nesting_level)) {
        set_gp_metrics(query_msg->mutable_query_metrics(), query_desc,
                       nested_calls, nested_timing);
      } else {
        set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, 0, 0);
      }
      if (connector->report_query(*query_msg, "ended")) {
        clear_big_fields(query_msg);
      }
    }
  }
}

void EventSender::collect_query_submit(QueryDesc *query_desc) {
  if (connector && need_collect(query_desc, nesting_level)) {
    auto *query = get_query_message(query_desc);
    query->state = QueryState::SUBMIT;
    auto query_msg = query->message;
    *query_msg =
        create_query_req(query_desc, yagpcc::QueryStatus::QUERY_STATUS_SUBMIT);
    *query_msg->mutable_submit_time() = current_ts();
    set_query_info(query_msg, query_desc);
    set_qi_nesting_level(query_msg, query_desc->gpmon_pkt->u.qexec.key.tmid);
    set_qi_slice_id(query_msg);
    set_query_text(query_msg, query_desc);
    if (connector->report_query(*query_msg, "submit")) {
      clear_big_fields(query_msg);
    }
    // take initial metrics snapshot so that we can safely take diff afterwards
    // in END or DONE events.
    set_gp_metrics(query_msg->mutable_query_metrics(), query_desc, 0, 0);
  }
}

void EventSender::collect_query_done(QueryDesc *query_desc,
                                     QueryMetricsStatus status) {
  if (connector && !filter_query(query_desc)) {
    auto *query = get_query_message(query_desc);
    if (query->state != UNKNOWN || need_report_nested_query()) {
      if (nesting_is_valid(query_desc, nesting_level)) {
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
                  (errmsg("Unexpected query status in query_done hook: %d",
                          status)));
        }
        auto prev_state = query->state;
        update_query_state(query_desc, query, QueryState::DONE,
                           query_status ==
                               yagpcc::QueryStatus::QUERY_STATUS_DONE);
        auto query_msg = query->message;
        query_msg->set_query_status(query_status);
        if (status == METRICS_QUERY_ERROR) {
          set_qi_error_message(query_msg);
        }
        if (prev_state == START) {
          // We've missed ExecutorEnd call due to query cancel or error. It's
          // fine, but now we need to collect and report execution stats
          *query_msg->mutable_end_time() = current_ts();
          set_gp_metrics(query_msg->mutable_query_metrics(), query_desc,
                         nested_calls, nested_timing);
        }
        connector->report_query(*query_msg, msg);
      }
      update_nested_counters(query_desc);
    }
    query_msgs.erase({query_desc->gpmon_pkt->u.qexec.key.ccnt,
                      query_desc->gpmon_pkt->u.qexec.key.tmid});
    pfree(query_desc->gpmon_pkt);
  }
}

EventSender::EventSender() {
  if (Config::enable_collector() && !Config::filter_user(get_user_name())) {
    try {
      connector = new UDSConnector();
    } catch (const std::exception &e) {
      ereport(INFO, (errmsg("Unable to start query tracing %s", e.what())));
    }
  }
}

EventSender::~EventSender() {
  delete connector;
  for (auto iter = query_msgs.begin(); iter != query_msgs.end(); ++iter) {
    delete iter->second.message;
  }
}

// That's basically a very simplistic state machine to fix or highlight any bugs
// coming from GP
void EventSender::update_query_state(QueryDesc *query_desc, QueryItem *query,
                                     QueryState new_state, bool success) {
  if (query->state == UNKNOWN) {
    collect_query_submit(query_desc);
  }
  switch (new_state) {
  case QueryState::SUBMIT:
    Assert(false);
    break;
  case QueryState::START:
    if (query->state == QueryState::SUBMIT) {
      query->message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    } else {
      Assert(false);
    }
    break;
  case QueryState::END:
    // Example of below assert triggering: CURSOR closes before ever being
    // executed Assert(query->state == QueryState::START ||
    // IsAbortInProgress());
    query->message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_END);
    break;
  case QueryState::DONE:
    Assert(query->state == QueryState::END || !success);
    query->message->set_query_status(yagpcc::QueryStatus::QUERY_STATUS_DONE);
    break;
  default:
    Assert(false);
  }
  query->state = new_state;
}

EventSender::QueryItem *EventSender::get_query_message(QueryDesc *query_desc) {
  if (query_desc->gpmon_pkt == nullptr ||
      query_msgs.find({query_desc->gpmon_pkt->u.qexec.key.ccnt,
                       query_desc->gpmon_pkt->u.qexec.key.tmid}) ==
          query_msgs.end()) {
    query_desc->gpmon_pkt = (gpmon_packet_t *)palloc0(sizeof(gpmon_packet_t));
    query_desc->gpmon_pkt->u.qexec.key.ccnt = gp_command_count;
    query_desc->gpmon_pkt->u.qexec.key.tmid = nesting_level;
    query_msgs.insert({{gp_command_count, nesting_level},
                       QueryItem(UNKNOWN, new yagpcc::SetQueryReq())});
  }
  return &query_msgs.at({query_desc->gpmon_pkt->u.qexec.key.ccnt,
                         query_desc->gpmon_pkt->u.qexec.key.tmid});
}

void EventSender::update_nested_counters(QueryDesc *query_desc) {
  if (!is_top_level_query(query_desc, nesting_level)) {
    auto query_msg = get_query_message(query_desc);
    nested_calls++;
    double end_time = protots_to_double(query_msg->message->end_time());
    double start_time = protots_to_double(query_msg->message->start_time());
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

EventSender::QueryItem::QueryItem(EventSender::QueryState st,
                                  yagpcc::SetQueryReq *msg)
    : state(st), message(msg) {}