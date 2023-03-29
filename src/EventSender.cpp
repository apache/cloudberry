#include "EventSender.h"
#include "GrpcConnector.h"
#include "protos/yagpcc_set_service.pb.h"
#include <ctime>

extern "C"
{
#include "postgres.h"
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

std::string* get_rg_name()
{
    auto user_id = GetUserId();
    if (!OidIsValid(user_id))
        return nullptr;
    auto group_id = GetResGroupIdForRole(user_id);
    if (!OidIsValid(group_id))
        return nullptr;
    char *rgname = GetResGroupNameForId(group_id);
    if (rgname == nullptr)
        return nullptr;
    pfree(rgname);
    return new std::string(rgname);
}

std::string* get_app_name()
{
    return application_name ? new std::string(application_name) : nullptr;
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

void set_header(yagpcc::QueryInfoHeader *header, QueryDesc *query_desc)
{
    header->set_pid(MyProcPid);
    auto gpid = header->mutable_gpidentity();
    gpid->set_dbid(GpIdentity.dbid);
    gpid->set_segindex(GpIdentity.segindex);
    gpid->set_gp_role(static_cast<yagpcc::GpRole>(Gp_role));
    gpid->set_gp_session_role(static_cast<yagpcc::GpRole>(Gp_session_role));
    header->set_ssid(gp_session_id);
    header->set_ccnt(gp_command_count);
    header->set_sliceid(get_cur_slice_id(query_desc));
    int32 tmid = 0;
    gpmon_gettmid(&tmid);
    header->set_tmid(tmid);
}

void set_session_info(yagpcc::SessionInfo *si, QueryDesc *query_desc)
{
    if (query_desc->sourceText)
        *si->mutable_sql() = std::string(query_desc->sourceText);
    si->set_allocated_applicationname(get_app_name());
    si->set_allocated_databasename(get_db_name());
    si->set_allocated_resourcegroup(get_rg_name());
    si->set_allocated_username(get_user_name());
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

void set_query_info(yagpcc::QueryInfo *qi, QueryDesc *query_desc)
{
    set_session_info(qi->mutable_sessioninfo(), query_desc);
    if (query_desc->sourceText)
        *qi->mutable_querytext() = query_desc->sourceText;
    if (query_desc->plannedstmt)
    {
        qi->set_generator(query_desc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                                ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                                : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
        if (query_desc->planstate)
        {
            set_plan_text(qi->mutable_plantext(), query_desc);
            qi->set_plan_id(get_plan_id(query_desc));
        }
    }
    qi->set_query_id(query_desc->plannedstmt->queryId);
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
    set_header(req.mutable_header(), query_desc);
    set_query_info(req.mutable_query_info(), query_desc);
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
    set_header(req.mutable_header(), query_desc);
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