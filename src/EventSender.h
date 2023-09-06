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
  void executor_before_start(QueryDesc *query_desc, int eflags);
  void executor_after_start(QueryDesc *query_desc, int eflags);
  void executor_end(QueryDesc *query_desc);
  void query_metrics_collect(QueryMetricsStatus status, void *arg);
  void incr_depth() { nesting_level++; }
  void decr_depth() { nesting_level--; }
  EventSender();
  ~EventSender();

private:
  void collect_query_submit(QueryDesc *query_desc);
  void collect_query_done(QueryDesc *query_desc, const std::string &status);
  void send_query_info(yagpcc::SetQueryReq *req, const std::string &event);
  GrpcConnector *connector;
  int nesting_level = 0;
};