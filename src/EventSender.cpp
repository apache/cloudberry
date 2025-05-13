#include "Config.h"
#include "UDSConnector.h"

#define typeid __typeid
extern "C" {
#include "postgres.h"

#include "access/hash.h"
#include "executor/executor.h"
#include "utils/elog.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
}
#undef typeid

#include "EventSender.h"
#include "PgUtils.h"
#include "ProtoUtils.h"

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
    // it appears we're only interested in the actual CANCELED event.
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
    *query_msg = create_query_req(yagpcc::QueryStatus::QUERY_STATUS_SUBMIT);
    *query_msg->mutable_submit_time() = current_ts();
    set_query_info(query_msg);
    set_qi_nesting_level(query_msg, query_desc->gpmon_pkt->u.qexec.key.tmid);
    set_qi_slice_id(query_msg);
    set_query_text(query_msg, query_desc);
    if (connector->report_query(*query_msg, "submit")) {
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
#ifdef IC_TEARDOWN_HOOK
        ic_metrics_collect();
        set_ic_stats(
            query_msg->mutable_query_metrics()->mutable_instrumentation(),
            &ic_statistics);
#endif
        connector->report_query(*query_msg, msg);
      }
      update_nested_counters(query_desc);
    }
    query_msgs.erase({query_desc->gpmon_pkt->u.qexec.key.ccnt,
                      query_desc->gpmon_pkt->u.qexec.key.tmid});
    pfree(query_desc->gpmon_pkt);
  }
}

void EventSender::ic_metrics_collect() {
#ifdef IC_TEARDOWN_HOOK
  if (Gp_interconnect_type != INTERCONNECT_TYPE_UDPIFC) {
    return;
  }
  if (!connector || gp_command_count == 0 || !Config::enable_collector() ||
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

EventSender::EventSender() {
  if (Config::enable_collector() && !Config::filter_user(get_user_name())) {
    try {
      connector = new UDSConnector();
    } catch (const std::exception &e) {
      ereport(INFO, (errmsg("Unable to start query tracing %s", e.what())));
    }
  }
#ifdef IC_TEARDOWN_HOOK
  memset(&ic_statistics, 0, sizeof(ICStatistics));
#endif
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