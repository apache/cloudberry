#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#define LOG_MODE_UDS 0
#define LOG_MODE_TBL 1

using IgnoredUsers = std::unordered_set<std::string>;

class Config {
public:
  static void init_gucs();

  void sync();

  const std::string &uds_path() const { return uds_path_; }
  bool enable_analyze() const { return enable_analyze_; }
  bool enable_cdbstats() const { return enable_cdbstats_; }
  bool enable_collector() const { return enable_collector_; }
  bool enable_utility() const { return enable_utility_; }
  bool report_nested_queries() const { return report_nested_queries_; }
  int max_text_size() const { return max_text_size_; }
  int max_plan_size() const { return max_plan_size_ * 1024; }
  int min_analyze_time() const { return min_analyze_time_; }
  int logging_mode() const { return logging_mode_; }
  bool filter_user(const std::string &username) const;

private:
  void update_ignored_users(const char *new_guc_ignored_users);

  std::unique_ptr<IgnoredUsers> ignored_users_;
  std::string uds_path_;
  bool enable_analyze_;
  bool enable_cdbstats_;
  bool enable_collector_;
  bool enable_utility_;
  bool report_nested_queries_;
  int max_text_size_;
  int max_plan_size_;
  int min_analyze_time_;
  int logging_mode_;
};
