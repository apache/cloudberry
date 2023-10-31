#pragma once

#include "protos/yagpcc_set_service.pb.h"
#include <queue>

class UDSConnector {
public:
  UDSConnector();
  void report_query(std::queue<yagpcc::SetQueryReq> &reqs,
                    const std::string &event);

private:
  bool report_query(const yagpcc::SetQueryReq &req, const std::string &event);
  const std::string uds_path;
};