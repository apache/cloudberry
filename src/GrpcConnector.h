#pragma once

#include "yagpcc_set_service.pb.h"

class GrpcConnector
{
public:
    GrpcConnector();
    ~GrpcConnector();
    yagpcc::MetricResponse set_metric_query(yagpcc::SetQueryReq req);

private:
    class Impl;
    Impl *impl;
};