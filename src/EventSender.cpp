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
    auto userId = GetUserId();
    if (!OidIsValid(userId))
        return nullptr;
    auto groupId = GetResGroupIdForRole(userId);
    if (!OidIsValid(groupId))
        return nullptr;
    char *rgname = GetResGroupNameForId(groupId);
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

void set_header(yagpcc::QueryInfoHeader *header, QueryDesc *queryDesc)
{
    header->set_pid(MyProcPid);
    auto gpId = header->mutable_gpidentity();
    gpId->set_dbid(GpIdentity.dbid);
    gpId->set_segindex(GpIdentity.segindex);
    gpId->set_gp_role(static_cast<yagpcc::GpRole>(Gp_role));
    gpId->set_gp_session_role(static_cast<yagpcc::GpRole>(Gp_session_role));
    header->set_ssid(gp_session_id);
    header->set_ccnt(gp_command_count);
    header->set_sliceid(get_cur_slice_id(queryDesc));
    int32 tmid = 0;
    gpmon_gettmid(&tmid);
    header->set_tmid(tmid);
}

void set_session_info(yagpcc::SessionInfo *si, QueryDesc *queryDesc)
{
    if (queryDesc->sourceText)
        *si->mutable_sql() = std::string(queryDesc->sourceText);
    si->set_allocated_applicationname(get_app_name());
    si->set_allocated_databasename(get_db_name());
    si->set_allocated_resourcegroup(get_rg_name());
    si->set_allocated_username(get_user_name());
}

ExplainState get_explain_state(QueryDesc *queryDesc, bool costs)
{
    ExplainState es;
    ExplainInitState(&es);
    es.costs = costs;
    es.verbose = true;
    es.format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(&es);
    ExplainPrintPlan(&es, queryDesc);
    ExplainEndOutput(&es);
    return es;
}

void set_plan_text(std::string *plan_text, QueryDesc *queryDesc)
{
    auto es = get_explain_state(queryDesc, true);
    *plan_text = std::string(es.str->data, es.str->len);
}

void set_query_info(yagpcc::QueryInfo *qi, QueryDesc *queryDesc)
{
    set_session_info(qi->mutable_sessioninfo(), queryDesc);
    if (queryDesc->sourceText)
        *qi->mutable_querytext() = queryDesc->sourceText;
    if (queryDesc->plannedstmt)
    {
        qi->set_generator(queryDesc->plannedstmt->planGen == PLANGEN_OPTIMIZER
                                ? yagpcc::PlanGenerator::PLAN_GENERATOR_OPTIMIZER
                                : yagpcc::PlanGenerator::PLAN_GENERATOR_PLANNER);
        set_plan_text(qi->mutable_plantext(), queryDesc);
        qi->set_plan_id(get_plan_id(queryDesc));
        qi->set_query_id(queryDesc->plannedstmt->queryId);
    }
}
} // namespace

void EventSender::ExecutorStart(QueryDesc *queryDesc, int /* eflags*/)
{
    elog(DEBUG1, "Query %s start recording", queryDesc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    *req.mutable_datetime() = current_ts();
    set_header(req.mutable_header(), queryDesc);
    set_query_info(req.mutable_query_info(), queryDesc);
    auto result = connector->set_metric_query(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR)
    {
        elog(WARNING, "Query %s start reporting failed with an error %s",
             queryDesc->sourceText, result.error_text().c_str());
    }
    else
    {
        elog(DEBUG1, "Query %s start successful", queryDesc->sourceText);
    }
}

void EventSender::ExecutorFinish(QueryDesc *queryDesc)
{
    elog(DEBUG1, "Query %s finish recording", queryDesc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_DONE);
    *req.mutable_datetime() = current_ts();
    set_header(req.mutable_header(), queryDesc);
    set_query_info(req.mutable_query_info(), queryDesc);
    auto result = connector->set_metric_query(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR)
    {
        elog(WARNING, "Query %s finish reporting failed with an error %s",
             queryDesc->sourceText, result.error_text().c_str());
    }
    else
    {
        elog(DEBUG1, "Query %s finish successful", queryDesc->sourceText);
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