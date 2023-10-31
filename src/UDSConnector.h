#pragma once

#include "protos/yagpcc_set_service.pb.h"
#include <queue>

class UDSConnector {
public:
  UDSConnector();
  ~UDSConnector();
  void report_query(std::queue<yagpcc::SetQueryReq> &reqs,
                    const std::string &event);

private:
  class Impl;
  Impl *impl;
};