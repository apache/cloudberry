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
 * pax_filter_test.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_filter_test.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_filter.h"

#include <sstream>
#include <thread>

#include "comm/gtest_wrappers.h"
#include "storage/filter/pax_row_filter.h"
#include "storage/filter/pax_sparse_filter.h"
#include "storage/filter/pax_sparse_filter_tree.h"

namespace pax::tests {

class PaxFilterTest : public ::testing::Test {};

TEST_F(PaxFilterTest, TestPrint) {
  PaxSparseFilter sf(nullptr, false);
  std::shared_ptr<AndNode> and_node;
  std::shared_ptr<OrNode> or_node1, or_node2, or_node3;
  std::shared_ptr<ConstNode> const_node1, const_node2, const_node3;
  std::shared_ptr<ConstNode> const_node4, const_node5, const_node6, const_node7,
      const_node8;

  and_node = std::make_shared<AndNode>();
  or_node1 = std::make_shared<OrNode>();
  or_node2 = std::make_shared<OrNode>();
  or_node3 = std::make_shared<OrNode>();

  const_node1 = std::make_shared<ConstNode>();
  const_node2 = std::make_shared<ConstNode>();
  const_node3 = std::make_shared<ConstNode>();

  const_node4 = std::make_shared<ConstNode>();
  const_node5 = std::make_shared<ConstNode>();
  const_node6 = std::make_shared<ConstNode>();
  const_node7 = std::make_shared<ConstNode>();
  const_node8 = std::make_shared<ConstNode>();

  or_node1->parent = and_node;
  const_node1->parent = and_node;
  and_node->sub_nodes.emplace_back(or_node1);
  and_node->sub_nodes.emplace_back(const_node1);

  const_node2->parent = or_node1;
  const_node3->parent = or_node1;
  or_node1->sub_nodes.emplace_back(const_node2);
  or_node1->sub_nodes.emplace_back(const_node3);

  or_node2->parent = and_node;
  const_node4->parent = and_node;
  and_node->sub_nodes.emplace_back(or_node2);
  and_node->sub_nodes.emplace_back(const_node4);

  const_node5->parent = or_node2;
  or_node3->parent = or_node2;
  or_node2->sub_nodes.emplace_back(const_node5);
  or_node2->sub_nodes.emplace_back(or_node3);

  const_node7->parent = or_node3;
  const_node8->parent = or_node3;
  or_node3->sub_nodes.emplace_back(const_node7);
  or_node3->sub_nodes.emplace_back(const_node8);

  sf.filter_tree_ = and_node;
  std::cout << sf.DebugString() << std::endl;

  sf.filter_tree_ = nullptr;
}

TEST_F(PaxFilterTest, OrdersBloomFiltersBeforeMoreSelectivePredicates) {
  ExecutionFilterContext context{};
  using Kind = ExecutionFilterContext::FilterKind;
  context.filter_nodes = {
      {Kind::kBloom, 0, 100, 90},
      {Kind::kExpr, 0, 100, 5},
      {Kind::kBloom, 1, 100, 20},
  };

  context.FinishSampling();

  ASSERT_FALSE(context.sampling);
  ASSERT_EQ(context.filter_nodes[0].kind, Kind::kBloom);
  ASSERT_EQ(context.filter_nodes[0].index, 1);
  ASSERT_EQ(context.filter_nodes[1].kind, Kind::kBloom);
  ASSERT_EQ(context.filter_nodes[1].index, 0);
  ASSERT_EQ(context.filter_nodes[2].kind, Kind::kExpr);
}

TEST_F(PaxFilterTest, KeepsExactlyTheRowsPassingBloomAndSqlFilters) {
  ExecutionFilterContext context{};
  using Kind = ExecutionFilterContext::FilterKind;
  using Result = ExecutionFilterContext::FilterResult;
  context.filter_nodes = {{Kind::kBloom, 0}, {Kind::kBloom, 1},
                          {Kind::kExpr, 0}, {Kind::kExpr, 1}};

  struct Row {
    bool bloom[2];
    bool sql[2];
  };
  const Row rows[] = {
      {{true, true}, {true, true}},
      {{false, false}, {true, true}},
      {{true, false}, {false, false}},
      {{false, true}, {true, true}},
      {{true, true}, {true, false}},
      {{true, true}, {false, true}},
      {{true, true}, {true, true}},
  };
  context.sample_target = sizeof(rows) / sizeof(rows[0]);
  const std::vector<size_t> expected_kept{0, 6};
  const std::vector<size_t> expected_bloom_removed{1, 2, 3};
  const std::vector<size_t> expected_sql_removed{4, 5};

  for (bool sampling : {true, false}) {
    ASSERT_EQ(context.sampling, sampling);
    std::vector<size_t> kept, bloom_removed, sql_removed;
    size_t sql_calls_on_bloom_rejection = 0;
    size_t sql_calls_on_sql_rejection = 0;
    for (size_t i = 0; i < context.sample_target; ++i) {
      const auto &row = rows[i];
      bool sql_rejected = false;
      auto result = context.ApplyFilters([&](const auto &node) {
        if (node.kind == Kind::kBloom) return row.bloom[node.index];
        if (!row.bloom[0] || !row.bloom[1]) ++sql_calls_on_bloom_rejection;
        if (sql_rejected) ++sql_calls_on_sql_rejection;
        sql_rejected = !row.sql[node.index];
        return row.sql[node.index];
      });
      if (result == Result::kPass) kept.push_back(i);
      if (result == Result::kBloomRejected) bloom_removed.push_back(i);
      if (result == Result::kExprRejected) sql_removed.push_back(i);
    }
    EXPECT_EQ(kept, expected_kept);
    EXPECT_EQ(bloom_removed, expected_bloom_removed);
    EXPECT_EQ(sql_removed, expected_sql_removed);
    EXPECT_EQ(sql_calls_on_bloom_rejection, 0U);
    // A SQL condition never sees a row an earlier one rejected, sampling or
    // not, so the second condition is sampled on fewer rows.
    EXPECT_EQ(sql_calls_on_sql_rejection, 0U);

    for (const auto &node : context.filter_nodes) {
      if (node.kind == Kind::kBloom) {
        EXPECT_EQ(node.tested, 7U);
        EXPECT_EQ(node.passed, 5U);
      } else {
        EXPECT_EQ(node.tested, node.index == 0 ? 4U : 3U);
        EXPECT_EQ(node.passed, node.index == 0 ? 3U : 2U);
      }
    }
  }
}

void thread_task(std::array<std::atomic<int>, 2> &array,
                 const int loop_counts) {
  for (int i = 0; i < loop_counts; i++) {
    array[1].fetch_add(1);
  }
}

TEST_F(PaxFilterTest, TestAtotic) {
  std::array<std::atomic<int>, 2> array = {0};
  const int thread_nums = 10;
  const int loop_counts = 500000;
  std::thread threads[thread_nums];

  for (int i = 0; i < thread_nums; i++) {
    threads[i] = std::thread(thread_task, std::ref(array), loop_counts);
  }

  for (int i = 0; i < thread_nums; i++) {
    threads[i].join();
  }

  ASSERT_EQ(array[1].load(), loop_counts * thread_nums);
}

};  // namespace pax::tests