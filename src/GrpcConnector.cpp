#include "GrpcConnector.h"
#include "Config.h"
#include "yagpcc_set_service.grpc.pb.h"

#include <atomic>
#include <condition_variable>
#include <grpc++/channel.h>
#include <grpc++/grpc++.h>
#include <mutex>
#include <string>
#include <thread>

extern "C"
{
#include "postgres.h"
#include "cdb/cdbvars.h"
}

class GrpcConnector::Impl
{
public:
  Impl() : SOCKET_FILE("unix://" + Config::uds_path())
  {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    channel =
        grpc::CreateChannel(SOCKET_FILE, grpc::InsecureChannelCredentials());
    stub = yagpcc::SetQueryInfo::NewStub(channel);
    connected = true;
    done = false;
    reconnect_thread = std::thread(&Impl::reconnect, this);
  }

  ~Impl()
  {
    done = true;
    cv.notify_one();
    reconnect_thread.join();
  }

  yagpcc::MetricResponse set_metric_query(yagpcc::SetQueryReq req)
  {
    yagpcc::MetricResponse response;
    if (!connected)
    {
      response.set_error_code(yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR);
      response.set_error_text(
          "Not tracing this query connection to agent has been lost");
      return response;
    }
    grpc::ClientContext context;
    int timeout = Gp_role == GP_ROLE_DISPATCH ? 500 : 250;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    context.set_deadline(deadline);
    grpc::Status status = (stub->SetMetricQuery)(&context, req, &response);
    if (!status.ok())
    {
      response.set_error_text("Connection lost: " + status.error_message() +
                              "; " + status.error_details());
      response.set_error_code(yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR);
      connected = false;
      cv.notify_one();
    }

    return response;
  }

private:
  const std::string SOCKET_FILE;
  std::unique_ptr<yagpcc::SetQueryInfo::Stub> stub;
  std::shared_ptr<grpc::Channel> channel;
  std::atomic_bool connected;
  std::thread reconnect_thread;
  std::condition_variable cv;
  std::mutex mtx;
  bool done;

  void reconnect()
  {
    while (!done)
    {
      {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock);
      }
      while (!connected && !done)
      {
        auto deadline =
            std::chrono::system_clock::now() + std::chrono::milliseconds(100);
        connected = channel->WaitForConnected(deadline);
      }
    }
  }
};

GrpcConnector::GrpcConnector() { impl = new Impl(); }

GrpcConnector::~GrpcConnector() { delete impl; }

yagpcc::MetricResponse
GrpcConnector::set_metric_query(yagpcc::SetQueryReq req)
{
  return impl->set_metric_query(req);
}