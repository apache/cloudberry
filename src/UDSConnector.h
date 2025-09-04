#pragma once

#include "protos/yagpcc_set_service.pb.h"

class UDSConnector {
public:
  bool static report_query(const yagpcc::SetQueryReq &req,
                           const std::string &event);
};