#include "UDSConnector.h"
#include "Config.h"

#include <string>

extern "C" {
#include "postgres.h"
#include "cdb/cdbvars.h"
}

class UDSConnector::Impl {
public:
  Impl() : SOCKET_FILE("unix://" + Config::uds_path()) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
  }

  bool report_query(const yagpcc::SetQueryReq &req, const std::string &event) {
    return true;
  }

private:
  const std::string SOCKET_FILE;
};

UDSConnector::UDSConnector() { impl = new Impl(); }

UDSConnector::~UDSConnector() { delete impl; }

void UDSConnector::report_query(std::queue<yagpcc::SetQueryReq> &reqs,
                                const std::string &event) {
  while (!reqs.empty()) {
    const auto &req = reqs.front();
    if (impl->report_query(req, event)) {
      reqs.pop();
    } else {
      break;
    }
  }
}