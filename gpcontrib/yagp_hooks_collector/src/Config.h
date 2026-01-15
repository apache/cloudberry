#pragma once

#include <string>

#define LOG_MODE_UDS 0
#define LOG_MODE_TBL 1

class Config {
public:
  static void init();
  static std::string uds_path();
  static bool enable_analyze();
  static bool enable_cdbstats();
  static bool enable_collector();
  static bool enable_utility();
  static bool filter_user(std::string username);
  static bool report_nested_queries();
  static size_t max_text_size();
  static size_t max_plan_size();
  static int min_analyze_time();
  static int logging_mode();
  static void sync();
};