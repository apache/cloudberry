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
#include <signal.h>
#include <pthread.h>

extern "C" {
#include "postgres.h"
#include "cdb/cdbvars.h"
}

/*
 * Set up the thread signal mask, we don't want to run our signal handlers
 * in downloading and uploading threads.
 */
static void MaskThreadSignals() {
  sigset_t sigs;

  if (pthread_equal(main_tid, pthread_self())) {
    ereport(ERROR, (errmsg("thread_mask is called from main thread!")));
    return;
  }

  sigemptyset(&sigs);

  /* make our thread to ignore these signals (which should allow that they be
   * delivered to the main thread) */
  sigaddset(&sigs, SIGHUP);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGTERM);
  sigaddset(&sigs, SIGALRM);
  sigaddset(&sigs, SIGUSR1);
  sigaddset(&sigs, SIGUSR2);

  pthread_sigmask(SIG_BLOCK, &sigs, NULL);
}

class GrpcConnector::Impl {
public:
  Impl() : SOCKET_FILE("unix://" + Config::uds_path()) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    channel =
        grpc::CreateChannel(SOCKET_FILE, grpc::InsecureChannelCredentials());
    stub = yagpcc::SetQueryInfo::NewStub(channel);
    connected = true;
    reconnected = false;
    done = false;
    reconnect_thread = std::thread(&Impl::reconnect, this);
  }

  ~Impl() {
    done = true;
    cv.notify_one();
    reconnect_thread.join();
  }

  yagpcc::MetricResponse set_metric_query(const yagpcc::SetQueryReq &req,
                                          const std::string &event) {
    yagpcc::MetricResponse response;
    if (!connected) {
      response.set_error_code(yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR);
      response.set_error_text(
          "Not tracing this query because grpc connection has been lost");
      return response;
    } else if (reconnected) {
      reconnected = false;
      ereport(LOG, (errmsg("GRPC connection is restored")));
    }
    grpc::ClientContext context;
    int timeout = Gp_role == GP_ROLE_DISPATCH ? 500 : 250;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
    context.set_deadline(deadline);
    grpc::Status status = (stub->SetMetricQuery)(&context, req, &response);
    if (!status.ok()) {
      response.set_error_text("GRPC error: " + status.error_message() + "; " +
                              status.error_details());
      response.set_error_code(yagpcc::METRIC_RESPONSE_STATUS_CODE_ERROR);
      ereport(LOG, (errmsg("Query {%d-%d-%d} %s tracing failed with error %s",
                           req.query_key().tmid(), req.query_key().ssid(),
                           req.query_key().ccnt(), event.c_str(),
                           response.error_text().c_str())));
      connected = false;
      reconnected = false;
      cv.notify_one();
    }

    return response;
  }

private:
  const std::string SOCKET_FILE;
  std::unique_ptr<yagpcc::SetQueryInfo::Stub> stub;
  std::shared_ptr<grpc::Channel> channel;
  std::atomic_bool connected, reconnected, done;
  std::thread reconnect_thread;
  std::condition_variable cv;
  std::mutex mtx;

  void reconnect() {
    MaskThreadSignals();
    while (!done) {
      {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock);
      }
      while (!connected && !done) {
        auto deadline =
            std::chrono::system_clock::now() + std::chrono::milliseconds(100);
        connected = channel->WaitForConnected(deadline);
        reconnected = true;
      }
    }
  }
};

GrpcConnector::GrpcConnector() { impl = new Impl(); }

GrpcConnector::~GrpcConnector() { delete impl; }

yagpcc::MetricResponse
GrpcConnector::set_metric_query(const yagpcc::SetQueryReq &req,
                                const std::string &event) {
  return impl->set_metric_query(req, event);
}