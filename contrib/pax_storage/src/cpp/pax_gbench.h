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
 * pax_gbench.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/pax_gbench.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <functional>
#include <benchmark/benchmark.h>

namespace pax {

namespace bench {

// Generic initialization and cleanup function types
using InitFunction = std::function<void()>;
using CleanupFunction = std::function<void()>;

// Create memory context for benchmark
extern void CreateMemoryContext();

// Forward declaration
class BenchmarkRegistry;

// Global registry access function
BenchmarkRegistry &GetBenchmarkRegistry();

// Global initialization and cleanup functions
void GlobalBenchmarkInit();
void GlobalBenchmarkCleanup();

// Registration functions (implemented in pax_gbench.cc)
void RegisterBenchmarkInit(InitFunction func);
void RegisterBenchmarkCleanup(CleanupFunction func);

}  // namespace benchmark
}  // namespace pax

// Convenient registration macros
#define REGISTER_BENCHMARK_INIT(func)               \
  static bool BENCHMARK_INIT_##__COUNTER__ = []() { \
    pax::bench::RegisterBenchmarkInit(func);    \
    return true;                                    \
  }()

#define REGISTER_BENCHMARK_CLEANUP(func)               \
  static bool BENCHMARK_CLEANUP_##__COUNTER__ = []() { \
    pax::bench::RegisterBenchmarkCleanup(func);    \
    return true;                                       \
  }()
