#include "GrpcConnector.h"
#include "yagpcc_set_service.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/channel.h>
#include <string>

class GrpcConnector::Impl {
public:
  Impl() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    this->stub = yagpcc::SetQueryInfo::NewStub(
        grpc::CreateChannel(SOCKET_FILE, grpc::InsecureChannelCredentials()));
  }

  yagpcc::MetricResponse set_metric_query(yagpcc::SetQueryReq req) {
    yagpcc::MetricResponse response;
    grpc::ClientContext context;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(50);
    context.set_deadline(deadline);

    grpc::Status status = (stub->SetMetricQuery)(&context, req, &response);

    if (!status.ok()) {
      response.set_error_text("Connection lost: " + status.error_message() +
                              "; " + status.error_details());
      response.set_error_code(yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR);
    }

    return response;
  }

private:
  const std::string SOCKET_FILE = "unix:///tmp/yagpcc_agent.sock";
  const std::string TCP_ADDRESS = "127.0.0.1:1432";
  std::unique_ptr<yagpcc::SetQueryInfo::Stub> stub;
};

GrpcConnector::GrpcConnector() { impl = new Impl(); }

GrpcConnector::~GrpcConnector() { delete impl; }

yagpcc::MetricResponse
GrpcConnector::set_metric_query(yagpcc::SetQueryReq req) {
  return impl->set_metric_query(req);
}