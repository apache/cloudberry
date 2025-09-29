#include "ProtoUtils.h"
#include "PgUtils.h"
#include "ProcStats.h"
#include "Config.h"
#include "memory/gpdbwrappers.h"

#define typeid __typeid
#define operator __operator
extern "C" {
#include "postgres.h"
#include "access/hash.h"
#include "access/xact.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#ifdef IC_TEARDOWN_HOOK
#include "cdb/ic_udpifc.h"
#endif
#include "utils/workfile_mgr.h"
}
#undef typeid
#undef operator

#include <ctime>
#include <string>

google::protobuf::Timestamp current_ts() {
  google::protobuf::Timestamp current_ts;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  current_ts.set_seconds(tv.tv_sec);
  current_ts.set_nanos(static_cast<int32_t>(tv.tv_usec * 1000));
  return current_ts;
}

void set_query_key(yagpcc::QueryKey *key) {
  key->set_ccnt(gp_command_count);
  key->set_ssid(gp_session_id);
  int32 tmid = 0;
  gpmon_gettmid(&tmid);
  key->set_tmid(tmid);
}

void set_segment_key(yagpcc::SegmentKey *key) {
  key->set_dbid(GpIdentity.dbid);
  key->set_segindex(GpIdentity.segindex);
}

inline std::string char_to_trimmed_str(const char *str, size_t len,
                                       size_t lim) {
  return std::string(str, std::min(len, lim));
}

void set_query_plan(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->plannedstmt) {
    auto qi = req->mutable_query_info();
    qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                          ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                          : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
    MemoryContext oldcxt =
        ya_gpdb::mem_ctx_switch_to(query_desc->estate->es_query_cxt);
    ExplainState es = ya_gpdb::get_explain_state(query_desc, true);
    if (es.str) {
      *qi->mutable_plan_text() = char_to_trimmed_str(es.str->data, es.str->len,
                                                     Config::max_plan_size());
      StringInfo norm_plan = ya_gpdb::gen_normplan(es.str->data);
      *qi->mutable_template_plan_text() = char_to_trimmed_str(
          norm_plan->data, norm_plan->len, Config::max_plan_size());
      qi->set_plan_id(
          hash_any((unsigned char *)norm_plan->data, norm_plan->len));
      qi->set_query_id(query_desc->plannedstmt->queryId);
      ya_gpdb::pfree(es.str->data);
      ya_gpdb::pfree(norm_plan->data);
    }
    ya_gpdb::mem_ctx_switch_to(oldcxt);
  }
}

void set_query_text(yagpcc::SetQueryReq *req, QueryDesc *query_desc) {
  if (Gp_session_role == GP_ROLE_DISPATCH && query_desc->sourceText) {
    auto qi = req->mutable_query_info();
    *qi->mutable_query_text() = char_to_trimmed_str(
        query_desc->sourceText, strlen(query_desc->sourceText),
        Config::max_text_size());
    char *norm_query = ya_gpdb::gen_normquery(query_desc->sourceText);
    *qi->mutable_template_query_text() = char_to_trimmed_str(
        norm_query, strlen(norm_query), Config::max_text_size());
  }
}

void clear_big_fields(yagpcc::SetQueryReq *req) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    auto qi = req->mutable_query_info();
    qi->clear_plan_text();
    qi->clear_template_plan_text();
    qi->clear_query_text();
    qi->clear_template_query_text();
    qi->clear_analyze_text();
  }
}

void set_query_info(yagpcc::SetQueryReq *req) {
  if (Gp_session_role == GP_ROLE_DISPATCH) {
    auto qi = req->mutable_query_info();
    qi->set_username(get_user_name());
    if (IsTransactionState())
      qi->set_databasename(get_db_name());
    qi->set_rsgname(get_rg_name());
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

void set_qi_error_message(yagpcc::SetQueryReq *req, const char *err_msg) {
  auto aqi = req->mutable_add_info();
  *aqi->mutable_error_message() =
      char_to_trimmed_str(err_msg, strlen(err_msg), Config::max_text_size());
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

#define UPDATE_IC_STATS(proto_name, stat_name)                                 \
  metrics->mutable_interconnect()->set_##proto_name(                           \
      ic_statistics->stat_name -                                               \
      metrics->mutable_interconnect()->proto_name());                          \
  Assert(metrics->mutable_interconnect()->proto_name() >= 0 &&                 \
         metrics->mutable_interconnect()->proto_name() <=                      \
             ic_statistics->stat_name)

void set_ic_stats(yagpcc::MetricInstrumentation *metrics,
                  const ICStatistics *ic_statistics) {
#ifdef IC_TEARDOWN_HOOK
  UPDATE_IC_STATS(total_recv_queue_size, totalRecvQueueSize);
  UPDATE_IC_STATS(recv_queue_size_counting_time, recvQueueSizeCountingTime);
  UPDATE_IC_STATS(total_capacity, totalCapacity);
  UPDATE_IC_STATS(capacity_counting_time, capacityCountingTime);
  UPDATE_IC_STATS(total_buffers, totalBuffers);
  UPDATE_IC_STATS(buffer_counting_time, bufferCountingTime);
  UPDATE_IC_STATS(active_connections_num, activeConnectionsNum);
  UPDATE_IC_STATS(retransmits, retransmits);
  UPDATE_IC_STATS(startup_cached_pkt_num, startupCachedPktNum);
  UPDATE_IC_STATS(mismatch_num, mismatchNum);
  UPDATE_IC_STATS(crc_errors, crcErrors);
  UPDATE_IC_STATS(snd_pkt_num, sndPktNum);
  UPDATE_IC_STATS(recv_pkt_num, recvPktNum);
  UPDATE_IC_STATS(disordered_pkt_num, disorderedPktNum);
  UPDATE_IC_STATS(duplicated_pkt_num, duplicatedPktNum);
  UPDATE_IC_STATS(recv_ack_num, recvAckNum);
  UPDATE_IC_STATS(status_query_msg_num, statusQueryMsgNum);
#endif
}

yagpcc::SetQueryReq create_query_req(yagpcc::QueryStatus status) {
  yagpcc::SetQueryReq req;
  req.set_query_status(status);
  *req.mutable_datetime() = current_ts();
  set_query_key(req.mutable_query_key());
  set_segment_key(req.mutable_segment_key());
  return req;
}

double protots_to_double(const google::protobuf::Timestamp &ts) {
  return double(ts.seconds()) + double(ts.nanos()) / 1000000000.0;
}

void set_analyze_plan_text(QueryDesc *query_desc, yagpcc::SetQueryReq *req) {
  // Make sure it is a valid txn and it is not an utility
  // statement for ExplainPrintPlan() later.
  if (!IsTransactionState() || !query_desc->plannedstmt) {
    return;
  }
  MemoryContext oldcxt =
      ya_gpdb::mem_ctx_switch_to(query_desc->estate->es_query_cxt);
  ExplainState es = ya_gpdb::get_analyze_state(
      query_desc, query_desc->instrument_options && Config::enable_analyze());
  ya_gpdb::mem_ctx_switch_to(oldcxt);
  if (es.str) {
    // Remove last line break.
    if (es.str->len > 0 && es.str->data[es.str->len - 1] == '\n') {
      es.str->data[--es.str->len] = '\0';
    }
    auto trimmed_analyze =
        char_to_trimmed_str(es.str->data, es.str->len, Config::max_plan_size());
    req->mutable_query_info()->set_analyze_text(trimmed_analyze);
    ya_gpdb::pfree(es.str->data);
  }
}