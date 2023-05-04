#pragma once

#include <memory>
#include <string>

class GrpcConnector;
struct QueryDesc;
namespace yagpcc {
class SetQueryReq;
}

class EventSender {
public:
  void executor_after_start(QueryDesc *query_desc, int eflags);
  void executor_end(QueryDesc *query_desc);
  void query_metrics_collect(QueryMetricsStatus status, void *arg);
  static EventSender *instance();

private:
  void collect_query_submit(QueryDesc *query_desc);
  void collect_query_done(QueryDesc *query_desc, const std::string &status);

  EventSender();
  void send_query_info(yagpcc::SetQueryReq *req, const std::string &event);
  std::unique_ptr<GrpcConnector> connector;
};