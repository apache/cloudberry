#pragma once

#include <string>

class Config {
public:
  static void init();
  static std::string uds_path();
  static bool enable_analyze();
  static bool enable_cdbstats();
  static bool enable_collector();
  static bool filter_user(const std::string *username);
  static bool report_nested_queries();
  static const char *ignored_users();
  static size_t max_text_size();
};