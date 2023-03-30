#include "EventSender.h"
#include "GrpcConnector.h"
#include "protos/yagpcc_set_service.pb.h"
#include <ctime>

extern "C"
{
#include "postgres.h"
#include "access/hash.h"
#include "utils/metrics_utils.h"
#include "utils/elog.h"
#include "executor/executor.h"
#include "commands/explain.h"
#include "commands/dbcommands.h"
#include "commands/resgroupcmds.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbexplain.h"

#include "tcop/utility.h"
#include "pg_stat_statements_ya_parser.h"

void get_spill_info(int ssid, int ccid, int32_t* file_count, int64_t* total_bytes);
}

namespace
{

std::string* get_user_name()
{
    const char *username = GetConfigOption("session_authorization", false, false);
    return username ? new std::string(username) : nullptr;
}

std::string* get_db_name()
{
    char *dbname = get_database_name(MyDatabaseId);
    std::string* result = dbname ? new std::string(dbname) : nullptr;
    pfree(dbname);
    return result;
}

int get_cur_slice_id(QueryDesc *desc)
{
    if (!desc->estate)
    {
        return 0;
    }
    return LocallyExecutingSliceIndex(desc->estate);
}

google::protobuf::Timestamp current_ts()
{
    google::protobuf::Timestamp current_ts;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    current_ts.set_seconds(tv.tv_sec);
    current_ts.set_nanos(static_cast<int32_t>(tv.tv_usec * 1000));
    return current_ts;
}

void set_query_key(yagpcc::QueryKey *key, QueryDesc *query_desc)
{
    key->set_ccnt(gp_command_count);
    key->set_ssid(gp_session_id);
    int32 tmid = 0;
    gpmon_gettmid(&tmid);
    key->set_tmid(tmid);
}

void set_segment_key(yagpcc::SegmentKey *key, QueryDesc *query_desc)
{
    key->set_dbid(GpIdentity.dbid);
    key->set_segindex(GpIdentity.segindex);
}

ExplainState get_explain_state(QueryDesc *query_desc, bool costs)
{
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

void set_plan_text(std::string *plan_text, QueryDesc *query_desc)
{
    auto es = get_explain_state(query_desc, true);
    *plan_text = std::string(es.str->data, es.str->len);
}

void set_query_plan(yagpcc::QueryInfo *qi, QueryDesc *query_desc)
{
    qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                                ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                                : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
    set_plan_text(qi->mutable_plan_text(), query_desc);
    StringInfo norm_plan = gen_normplan(qi->plan_text().c_str());
    *qi->mutable_temlate_plan_text() = std::string(norm_plan->data);
    qi->set_plan_id(hash_any((unsigned char *)norm_plan->data, norm_plan->len));
    //TODO: free stringinfo?
}

void set_query_text(yagpcc::QueryInfo *qi, QueryDesc *query_desc)
{
    *qi->mutable_query_text() = query_desc->sourceText;
    char* norm_query = gen_normquery(query_desc->sourceText);
    *qi->mutable_temlate_query_text() = std::string(norm_query);
    pfree(norm_query);
}

void set_query_info(yagpcc::QueryInfo *qi, QueryDesc *query_desc)
{
    if (query_desc->sourceText)
        set_query_text(qi, query_desc);
    if (query_desc->plannedstmt)
    {
        set_query_plan(qi, query_desc);
        qi->set_query_id(query_desc->plannedstmt->queryId);
    }
    qi->set_allocated_username(get_user_name());
    qi->set_allocated_databasename(get_db_name());
}

void set_gp_metrics(yagpcc::GPMetrics* metrics, QueryDesc *query_desc)
{
    int32_t n_spill_files = 0;
    int64_t n_spill_bytes = 0;
    get_spill_info(gp_session_id, gp_command_count, &n_spill_files, &n_spill_bytes);
    metrics->mutable_spill()->set_filecount(n_spill_files);
    metrics->mutable_spill()->set_totalbytes(n_spill_bytes);
}
} // namespace

void EventSender::ExecutorStart(QueryDesc *query_desc, int /* eflags*/)
{
    elog(DEBUG1, "Query %s start recording", query_desc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    *req.mutable_datetime() = current_ts();
    set_query_key(req.mutable_query_key(), query_desc);
    auto result = connector->set_metric_query(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR)
    {
        elog(WARNING, "Query %s start reporting failed with an error %s",
             query_desc->sourceText, result.error_text().c_str());
    }
    else
    {
        elog(DEBUG1, "Query %s start successful", query_desc->sourceText);
    }
}

void EventSender::ExecutorFinish(QueryDesc *query_desc)
{
    elog(DEBUG1, "Query %s finish recording", query_desc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_DONE);
    *req.mutable_datetime() = current_ts();
    set_query_key(req.mutable_query_key(), query_desc);
    set_query_info(req.mutable_query_info(), query_desc);
    set_gp_metrics(req.mutable_query_metrics(), query_desc);
    auto result = connector->set_metric_query(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR)
    {
        elog(WARNING, "Query %s finish reporting failed with an error %s",
             query_desc->sourceText, result.error_text().c_str());
    }
    else
    {
        elog(DEBUG1, "Query %s finish successful", query_desc->sourceText);
    }
}

EventSender *EventSender::instance()
{
    static EventSender sender;
    return &sender;
}

EventSender::EventSender()
{
    connector = std::make_unique<GrpcConnector>();
}