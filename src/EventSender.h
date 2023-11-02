#pragma once

#include <memory>
#include <queue>
#include <string>

extern "C" {
#include "utils/metrics_utils.h"
}

class UDSConnector;
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
  void collect_query_done(QueryDesc *query_desc, QueryMetricsStatus status);
  UDSConnector *connector = nullptr;
  int nesting_level = 0;
  yagpcc::SetQueryReq *query_msg;
};