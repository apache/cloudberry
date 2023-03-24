#include "EventSender.h"
#include "GrpcConnector.h"
#include "protos/yagpcc_set_service.pb.h"

extern "C" {
#include "postgres.h"
#include "utils/metrics_utils.h"
#include "utils/elog.h"
#include "executor/executor.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbexplain.h"

#include "tcop/utility.h"
}

void EventSender::ExecutorStart(QueryDesc *queryDesc, int/* eflags*/) {
    elog(DEBUG1, "Query %s start recording", queryDesc->sourceText);
    yagpcc::SetQueryReq req;
    req.set_query_status(yagpcc::QueryStatus::QUERY_STATUS_START);
    google::protobuf::Timestamp ts;
    req.set_allocated_datetime(ts.New());
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
    google::protobuf::Timestamp ts;
    req.set_allocated_datetime(ts.New());
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