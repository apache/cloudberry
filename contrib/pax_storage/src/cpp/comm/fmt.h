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
 * fmt.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/fmt.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstdarg>
#include <cstring>
#include <memory>
#include <string>

namespace pax {

#define MAX_LEN_OF_FMT_STR 2048
#define BOOL_TOSTRING(is) ((is) ? "true" : "false")

static char format_str[MAX_LEN_OF_FMT_STR] = {0};

// Do not use the (Args...) to forward args
// need use the __format__ to prevent potential type problems
static inline __attribute__((__format__(__printf__, 1, 2))) std::string fmt(
    const char *format, ...) {
  va_list args;
  va_start(args, format);
  vsnprintf(format_str, MAX_LEN_OF_FMT_STR - 1, format, args);
  va_end(args);

  return std::string(format_str);
}

}  // namespace pax