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
 *-------------------------------------------------------------------------
 */

#pragma once
#include "paxpy_config_d.h"
#define FORMAT_CODE_PY_SSIZE_T "%" PY_FORMAT_SIZE_T "d"

/* GCC 4.0 and later have support for specifying symbol visibility */
#if __GNUC__ >= 4
#define HIDDEN __attribute__((visibility("hidden")))
#else
#define HIDDEN
#endif

/* debug printf-like function */
#ifdef PAXPY_DEBUG
extern HIDDEN int paxpy_debug_enabled;
#endif

#if defined(__GNUC__) && !defined(__APPLE__)
#ifdef PAXPY_DEBUG
#define PAXPY_PRINT(fmt, args...) \
  if (!paxpy_debug_enabled)       \
    ;                             \
  else                            \
    fprintf(stderr, "[%d] " fmt "\n", (int)getpid(), ##args)
#else
#define PAXPY_PRINT(fmt, args...)
#endif
#else /* !__GNUC__ or __APPLE__ */
#ifdef PAXPY_DEBUG
#include <stdarg.h>
static void PAXPY_PRINT(const char *fmt, ...) {
  va_list ap;

  if (!paxpy_debug_enabled) return;
  printf("[%d] ", (int)getpid());
  va_start(ap, fmt);
  vprintf(fmt, ap);
  va_end(ap);
  printf("\n");
}
#else
static void PAXPY_PRINT(const char *fmt, ...) {}
#endif
#endif
