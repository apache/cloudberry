#pragma once

#include <memory>
#include <unordered_map>
#include <string>

#define typeid __typeid
extern "C" {
#include "utils/metrics_utils.h"
#include "cdb/ml_ipc.h"
#ifdef IC_TEARDOWN_HOOK
#include "cdb/ic_udpifc.h"
#endif
}
#undef typeid

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
  void ic_metrics_collect();
  void incr_depth() { nesting_level++; }
  void decr_depth() { nesting_level--; }
  EventSender();
  ~EventSender();

private:
  enum QueryState { UNKNOWN, SUBMIT, START, END, DONE };

  struct QueryItem {
    QueryState state = QueryState::UNKNOWN;
    yagpcc::SetQueryReq *message = nullptr;

    QueryItem(QueryState st, yagpcc::SetQueryReq *msg);
  };

  struct pair_hash {
    std::size_t operator()(const std::pair<int, int> &p) const {
      auto h1 = std::hash<int>{}(p.first);
      auto h2 = std::hash<int>{}(p.second);
      return h1 ^ h2;
    }
  };

  void update_query_state(QueryDesc *query_desc, QueryItem *query,
                          QueryState new_state, bool success = true);
  QueryItem *get_query_message(QueryDesc *query_desc);
  void collect_query_submit(QueryDesc *query_desc);
  void collect_query_done(QueryDesc *query_desc, QueryMetricsStatus status);
  void cleanup_messages();
  void update_nested_counters(QueryDesc *query_desc);
  void update_ignored_users(const char *new_guc_ignored_users);

  UDSConnector *connector = nullptr;
  int nesting_level = 0;
  int64_t nested_calls = 0;
  double nested_timing = 0;
#ifdef IC_TEARDOWN_HOOK
  ICStatistics ic_statistics;
#endif
  std::unordered_map<std::pair<int, int>, QueryItem, pair_hash> query_msgs;
};