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
 * pax_gbench.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/pax_gbench.cc
 *
 *-------------------------------------------------------------------------
 */

#include "pax_gbench.h"

#include "comm/cbdb_api.h"

#include <benchmark/benchmark.h>

#include <functional>
#include <memory>
#include <vector>

#include "access/paxc_rel_options.h"
#include "comm/cbdb_wrappers.h"
#include "cpp-stub/src/stub.h"
#include "storage/micro_partition_iterator.h"
#include "storage/pax.h"
#include "storage/strategy.h"

namespace pax::bench {

// Create memory context for benchmark
void CreateMemoryContext() {
  MemoryContext test_memory_context = AllocSetContextCreate(
      (MemoryContext)NULL, "TestMemoryContext", 80 * 1024 * 1024,
      80 * 1024 * 1024, 80 * 1024 * 1024);
  MemoryContextSwitchTo(test_memory_context);
}

// Global registry
class BenchmarkRegistry {
 private:
  std::vector<InitFunction> init_functions_;
  std::vector<CleanupFunction> cleanup_functions_;
  bool initialized_ = false;

 public:
  void RegisterInitFunction(InitFunction func) {
    init_functions_.push_back(func);
  }

  void RegisterCleanupFunction(CleanupFunction func) {
    cleanup_functions_.push_back(func);
  }

  void RunAllInitFunctions() {
    if (initialized_) return;

    printf("Running PAX Benchmark Suite...\n");
    printf("Initializing all benchmark modules...\n\n");

    for (const auto &func : init_functions_) {
      func();
    }
    initialized_ = true;
  }

  void RunAllCleanupFunctions() {
    if (!initialized_) return;

    printf("\nCleaning up all benchmark modules...\n");

    // Cleanup functions executed in reverse order
    for (auto it = cleanup_functions_.rbegin(); it != cleanup_functions_.rend();
         ++it) {
      (*it)();
    }
    initialized_ = false;
  }
};

// Global registry access function
BenchmarkRegistry &GetBenchmarkRegistry() {
  static BenchmarkRegistry instance;
  return instance;
}

// Registration functions
void RegisterBenchmarkInit(InitFunction func) {
  GetBenchmarkRegistry().RegisterInitFunction(func);
}

void RegisterBenchmarkCleanup(CleanupFunction func) {
  GetBenchmarkRegistry().RegisterCleanupFunction(func);
}

// Global Mock functions for benchmark framework
bool MockMinMaxGetStrategyProcinfo(Oid, Oid, Oid *, FmgrInfo *,
                                   StrategyNumber) {
  return false;
}

int32 MockGetFastSequences(Oid) {
  static int32 mock_id = 0;
  return mock_id++;
}

void MockInsertMicroPartitionPlaceHolder(Oid, int) {}
void MockDeleteMicroPartitionEntry(Oid, Snapshot, int) {}
void MockExecStoreVirtualTuple(TupleTableSlot *) {}

std::string MockBuildPaxDirectoryPath(RelFileNode rnode, BackendId backend_id) {
  // Create a simple file path for benchmarks
  return std::string("./bench_data");
}

std::vector<int> MockGetMinMaxColumnIndexes(Relation) {
  return std::vector<int>();
}

std::vector<int> MockBloomFilterColumnIndexes(Relation) {
  return std::vector<int>();
}

std::vector<std::tuple<ColumnEncoding_Kind, int>> MockGetRelEncodingOptions(
    Relation relation) {
  std::vector<std::tuple<ColumnEncoding_Kind, int>> encoding_opts;

  // Get number of columns from relation
  int num_columns = 10;  // default for benchmark
  if (relation && relation->rd_att) {
    num_columns = relation->rd_att->natts;
  }

  // Create encoding options for each column (NO_ENCODED, 0)
  for (int i = 0; i < num_columns; i++) {
    encoding_opts.emplace_back(
        std::make_tuple(ColumnEncoding_Kind_NO_ENCODED, 0));
  }

  return encoding_opts;
}

// Mock TupleDescInitEntry that doesn't rely on SYSCACHE
void MockTupleDescInitEntry(TupleDesc desc, AttrNumber attributeNumber,
                            const char *attributeName, Oid oidtypeid,
                            int32 typmod, int attdim) {
  // Basic validation
  if (attributeNumber < 1 || attributeNumber > desc->natts) {
    return;
  }

  Form_pg_attribute att = TupleDescAttr(desc, attributeNumber - 1);

  // Set basic attribute properties
  namestrcpy(&(att->attname), attributeName);
  att->atttypid = oidtypeid;
  att->atttypmod = typmod;
  att->attndims = attdim;
  att->attnum = attributeNumber;
  att->attnotnull = false;
  att->atthasdef = false;
  att->attidentity = '\0';
  att->attgenerated = '\0';
  att->attisdropped = false;
  att->attislocal = true;
  att->attinhcount = 0;
  att->attcollation = InvalidOid;

  // Set type-specific properties based on OID (hardcoded for common types)
  switch (oidtypeid) {
    case INT2OID:  // smallint
      att->attlen = 2;
      att->attalign = 's';
      att->attstorage = 'p';
      att->attbyval = true;
      break;
    case INT4OID:  // integer
      att->attlen = 4;
      att->attalign = 'i';
      att->attstorage = TYPSTORAGE_PLAIN;
      att->attbyval = true;
      break;
    case INT8OID:  // bigint
      att->attlen = 8;
      att->attalign = 'd';
      att->attstorage = TYPSTORAGE_PLAIN;
      att->attbyval = FLOAT8PASSBYVAL;
      break;
    case FLOAT8OID:  // double precision
      att->attlen = 8;
      att->attalign = 'd';
      att->attstorage = 'p';
      att->attbyval = FLOAT8PASSBYVAL;
      break;
    case BOOLOID:  // boolean
      att->attlen = 1;
      att->attalign = 'c';
      att->attstorage = 'p';
      att->attbyval = true;
      break;
    case TEXTOID:  // text
      att->attlen = -1;
      att->attalign = 'i';
      att->attstorage = TYPSTORAGE_PLAIN;
      att->attbyval = false;
      att->attcollation = DEFAULT_COLLATION_OID;
      break;
    case NUMERICOID:  // numeric
      att->attlen = -1;
      att->attalign = TYPALIGN_INT;
      att->attstorage = TYPSTORAGE_PLAIN;
      att->attbyval = false;
      break;
    case TIMESTAMPOID:  // timestamp
      att->attlen = 8;
      att->attalign = 'd';
      att->attstorage = TYPSTORAGE_PLAIN;
      att->attbyval = FLOAT8PASSBYVAL;
      break;
    default:
      // Default values for unknown types
      att->attlen = -1;
      att->attalign = 'i';
      att->attstorage = 'p';
      att->attbyval = false;
      break;
  }
}

// Global initialization function for general benchmark framework
void GlobalBenchmarkInit() {
  static bool global_initialized = false;
  if (global_initialized) return;

  printf("Initializing PAX benchmark framework...\n");

  // Initialize memory context
  MemoryContextInit();

  // Setup global Mock functions
  static std::unique_ptr<Stub> stub_global = std::make_unique<Stub>();

  stub_global->set(MinMaxGetPgStrategyProcinfo, MockMinMaxGetStrategyProcinfo);
  stub_global->set(CPaxGetFastSequences, MockGetFastSequences);
  stub_global->set(cbdb::BuildPaxDirectoryPath, MockBuildPaxDirectoryPath);
  stub_global->set(cbdb::InsertMicroPartitionPlaceHolder,
                   MockInsertMicroPartitionPlaceHolder);
  stub_global->set(cbdb::DeleteMicroPartitionEntry,
                   MockDeleteMicroPartitionEntry);
  stub_global->set(cbdb::GetMinMaxColumnIndexes, MockGetMinMaxColumnIndexes);
  stub_global->set(cbdb::GetBloomFilterColumnIndexes,
                   MockBloomFilterColumnIndexes);
  stub_global->set(cbdb::GetRelEncodingOptions, MockGetRelEncodingOptions);
  stub_global->set(ExecStoreVirtualTuple, MockExecStoreVirtualTuple);
  stub_global->set(TupleDescInitEntry, MockTupleDescInitEntry);

  // Create basic test directory
  system("mkdir -p ./bench_data");

  global_initialized = true;
  printf("PAX benchmark framework initialized.\n");
}

// Global cleanup function for general benchmark framework
void GlobalBenchmarkCleanup() {
  printf("Cleaning up PAX benchmark framework...\n");

  // Clean up test directory
  // system("rm -rf ./bench_data");

  // Reset memory context
  if (TopMemoryContext) {
    MemoryContextReset(TopMemoryContext);
  }

  printf("PAX benchmark framework cleaned up.\n");
}

// Example benchmark test
static void example_benchmark(::benchmark::State &state) {
  for (auto _ : state) {
    // Empty example test
  }
}
BENCHMARK(example_benchmark);

}  // namespace pax::benchmark

// Global cleanup function (C-style for atexit)
static void cleanup_all() {
  pax::bench::GetBenchmarkRegistry().RunAllCleanupFunctions();
  pax::bench::GlobalBenchmarkCleanup();
}

// Main entry function
int main(int argc, char **argv) {
  // Register global cleanup function
  std::atexit(cleanup_all);

  // Global initialization
  pax::bench::GlobalBenchmarkInit();

  // Run all registered initialization functions
  pax::bench::GetBenchmarkRegistry().RunAllInitFunctions();

  // Initialize benchmark framework
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

  printf("\n=== Starting PAX Benchmark Suite ===\n");
  printf("Use --benchmark_filter=<pattern> to run specific tests\n");
  printf("Use --benchmark_list_tests to see all available tests\n\n");

  // Run benchmark
  ::benchmark::RunSpecifiedBenchmarks();

  return 0;
}