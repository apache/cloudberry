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