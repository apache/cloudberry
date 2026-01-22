/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * EventSender.h
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/EventSender.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>
#include <unordered_map>
#include <tuple>

#define typeid __typeid
extern "C" {
#include "utils/metrics_utils.h"
#ifdef IC_TEARDOWN_HOOK
#include "cdb/ic_udpifc.h"
#endif
}
#undef typeid

#include "memory/gpdbwrappers.h"
#include "Config.h"

class UDSConnector;
struct QueryDesc;
namespace yagpcc {
class SetQueryReq;
}

#include <cstdint>

extern void gp_gettmid(int32 *);

struct QueryKey {
  int tmid;
  int ssid;
  int ccnt;
  int nesting_level;
  uintptr_t query_desc_addr;

  bool operator==(const QueryKey &other) const {
    return std::tie(tmid, ssid, ccnt, nesting_level, query_desc_addr) ==
           std::tie(other.tmid, other.ssid, other.ccnt, other.nesting_level,
                    other.query_desc_addr);
  }

  static void register_qkey(QueryDesc *query_desc, size_t nesting_level) {
    query_desc->yagp_query_key =
        (YagpQueryKey *)ya_gpdb::palloc0(sizeof(YagpQueryKey));
    int32 tmid;
    gp_gettmid(&tmid);
    query_desc->yagp_query_key->tmid = tmid;
    query_desc->yagp_query_key->ssid = gp_session_id;
    query_desc->yagp_query_key->ccnt = gp_command_count;
    query_desc->yagp_query_key->nesting_level = nesting_level;
    query_desc->yagp_query_key->query_desc_addr = (uintptr_t)query_desc;
  }

  static QueryKey from_qdesc(QueryDesc *query_desc) {
    return {
        .tmid = query_desc->yagp_query_key->tmid,
        .ssid = query_desc->yagp_query_key->ssid,
        .ccnt = query_desc->yagp_query_key->ccnt,
        .nesting_level = query_desc->yagp_query_key->nesting_level,
        .query_desc_addr = query_desc->yagp_query_key->query_desc_addr,
    };
  }
};

// https://www.boost.org/doc/libs/1_35_0/doc/html/boost/hash_combine_id241013.html
template <class T> inline void hash_combine(std::size_t &seed, const T &v) {
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

namespace std {
template <> struct hash<QueryKey> {
  size_t operator()(const QueryKey &k) const noexcept {
    size_t seed = hash<uint32_t>{}(k.tmid);
    hash_combine(seed, k.ssid);
    hash_combine(seed, k.ccnt);
    hash_combine(seed, k.nesting_level);
    uintptr_t addr = k.query_desc_addr;
    if constexpr (SIZE_MAX < UINTPTR_MAX) {
      addr %= SIZE_MAX;
    }
    hash_combine(seed, addr);
    return seed;
  }
};
} // namespace std

class EventSender {
public:
  void executor_before_start(QueryDesc *query_desc, int eflags);
  void executor_after_start(QueryDesc *query_desc, int eflags);
  void executor_end(QueryDesc *query_desc);
  void query_metrics_collect(QueryMetricsStatus status, void *arg, bool utility,
                             ErrorData *edata = NULL);
  void ic_metrics_collect();
  void analyze_stats_collect(QueryDesc *query_desc);
  void incr_depth() { nesting_level++; }
  void decr_depth() { nesting_level--; }
  EventSender();
  ~EventSender();

private:
  enum QueryState { SUBMIT, START, END, DONE };

  struct QueryItem {
    std::unique_ptr<yagpcc::SetQueryReq> message;
    QueryState state;

    explicit QueryItem(QueryState st);
  };

  bool log_query_req(const yagpcc::SetQueryReq &req, const std::string &event,
                     bool utility);
  bool verify_query(QueryDesc *query_desc, QueryState state, bool utility);
  void update_query_state(QueryItem &query, QueryState new_state, bool utility,
                          bool success = true);
  QueryItem &get_query(QueryDesc *query_desc);
  void submit_query(QueryDesc *query_desc);
  void collect_query_submit(QueryDesc *query_desc, bool utility);
  void report_query_done(QueryDesc *query_desc, QueryItem &query,
                         QueryMetricsStatus status, bool utility,
                         ErrorData *edata = NULL);
  void collect_query_done(QueryDesc *query_desc, bool utility,
                          QueryMetricsStatus status, ErrorData *edata = NULL);
  void update_nested_counters(QueryDesc *query_desc);
  bool qdesc_submitted(QueryDesc *query_desc);
  bool nesting_is_valid(QueryDesc *query_desc, int nesting_level);
  bool need_report_nested_query();
  bool filter_query(QueryDesc *query_desc);

  bool proto_verified = false;
  int nesting_level = 0;
  int64_t nested_calls = 0;
  double nested_timing = 0;
#ifdef IC_TEARDOWN_HOOK
  ICStatistics ic_statistics;
#endif
  std::unordered_map<QueryKey, QueryItem> queries;

  Config config;
};