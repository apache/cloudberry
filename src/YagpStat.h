#pragma once

#include <cstdint>

class YagpStat {
public:
  struct Data {
    int64_t total, failed_sends, failed_connects, failed_other;
    int32_t max_message_size;
  };

  static void init();
  static void deinit();
  static void reset();
  static void report_send(int32_t msg_size);
  static void report_bad_connection();
  static void report_bad_send(int32_t msg_size);
  static void report_error();
  static Data get_stats();
  static bool loaded();
};