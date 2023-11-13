#pragma once

#include "protos/yagpcc_set_service.pb.h"
#include <queue>

class UDSConnector {
public:
  UDSConnector();
  bool report_query(const yagpcc::SetQueryReq &req, const std::string &event);
};