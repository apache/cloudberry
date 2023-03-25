#include "EventSender.h"
#include "GrpcConnector.h"
#include "protos/yagpcc_set_service.pb.h"
#include <ctime>

extern "C" {
#include "postgres.h"
#include "utils/metrics_utils.h"
#include "utils/elog.h"
#include "executor/executor.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbexplain.h"

#include "tcop/utility.h"
}

static google::protobuf::Timestamp current_ts() {
    google::protobuf::Timestamp current_ts;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    current_ts.set_seconds(tv.tv_sec);
    current_ts.set_nanos(static_cast<int32_t>(tv.tv_usec * 1000));
    return current_ts;
}

static yagpcc::QueryInfoHeader create_header() {
    yagpcc::QueryInfoHeader header;
    header.set_pid(MyProcPid);
    auto gpId = header.mutable_gpidentity();
    gpId->set_dbid(GpIdentity.dbid);
    gpId->set_segindex(GpIdentity.segindex);
    gpId->set_gp_role(static_cast<yagpcc::GpRole>(Gp_role));
    gpId->set_gp_session_role(static_cast<yagpcc::GpRole>(Gp_session_role));
    header.set_ssid(gp_session_id);
    header.set_ccnt(gp_command_count);
    header.set_sliceid(0);
    int32 tmid = 0;
    gpmon_gettmid(&tmid);
    header.set_tmid(tmid);
    return header;
}

static yagpcc::QueryInfo create_query_info(QueryDesc *queryDesc) {
     yagpcc::QueryInfo qi;
     // TODO
     return qi;
}

void EventSender::ExecutorStart(QueryDesc *queryDesc, int/* eflags*/) {
    elog(DEBUG1, "Query %s start recording", queryDesc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    *req.mutable_datetime() = current_ts();
    *req.mutable_header() = create_header();
    *req.mutable_query_info() = create_query_info(queryDesc);
    auto result = connector->setMetricQuery(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR) {
        elog(WARNING, "Query %s start reporting failed with an error %s",
            queryDesc->sourceText, result.error_text().c_str());
    } else {
        elog(DEBUG1, "Query %s start successful", queryDesc->sourceText);
    }
}

void EventSender::ExecutorFinish(QueryDesc *queryDesc) {
    elog(DEBUG1, "Query %s finish recording", queryDesc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_DONE);
    *req.mutable_datetime() = current_ts();
    *req.mutable_header() = create_header();
    *req.mutable_query_info() = create_query_info(queryDesc);
    auto result = connector->setMetricQuery(req);
    if (result.error_code() == yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR) {
        elog(WARNING, "Query %s finish reporting failed with an error %s",
            queryDesc->sourceText, result.error_text().c_str());
    } else {
        elog(DEBUG1, "Query %s finish successful", queryDesc->sourceText);
    }
}

EventSender* EventSender::instance() {
    static EventSender sender;
    return &sender;
}

EventSender::EventSender() {
    connector = std::make_unique<GrpcConnector>();
}