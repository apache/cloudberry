#pragma once

#include "yagpcc_set_service.pb.h"

class GrpcConnector {
public:
    GrpcConnector();
    ~GrpcConnector();
    yagpcc::MetricResponse setMetricQuery(yagpcc::SetQueryReq req);

private:
    class Impl;
    Impl* impl;
};