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
 * YagpStat.h
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/YagpStat.h
 *
 *-------------------------------------------------------------------------
 */

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