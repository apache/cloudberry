#pragma once

#include "protos/yagpcc_set_service.pb.h"

class GrpcConnector {
public:
  GrpcConnector();
  ~GrpcConnector();
  yagpcc::MetricResponse report_query(const yagpcc::SetQueryReq &req,
                                      const std::string &event);

private:
  class Impl;
  Impl *impl;
};