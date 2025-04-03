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
 * pax_sparse_filter.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_sparse_filter.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/filter/pax_column_stats.h"
#include "storage/filter/pax_sparse_filter_tree.h"

#ifdef VEC_BUILD
#include "storage/vec/arrow_wrapper.h"
#endif

namespace pax {
namespace stats {
class MicroPartitionStatisticsInfo;
class ColumnBasicInfo;
class ColumnDataStats;
}  // namespace stats
struct PaxSparseExecContext;  // internal object

class PaxSparseFilter final {
 public:
  // filter kind
  enum StatisticsKind {
    // The value will be index at `filter_kind_desc`
    kFile = 0,
    kGroup,
    kEnd,
  };

  PaxSparseFilter(Relation relation, bool allow_fallback_to_pg);

  ~PaxSparseFilter();

  bool ExistsFilterPath() const;

  void Initialize(List *quals);

#ifdef VEC_BUILD
  void Initialize(
      const arrow::compute::Expression &expr,
      const std::vector<std::pair<const char *, size_t>> &table_names);
#endif

  bool ExecFilter(const ColumnStatsProvider &provider, const TupleDesc desc,
                  int kind);

  std::string DebugString() const;

  void LogStatistics() const;
#ifndef RUN_GTEST
 private:
#endif

  // Used to build the filter tree with the PG quals
  std::shared_ptr<PFTNode> ExprWalker(Expr *expr);
  Expr *ExprFlatVar(Expr *expr);
  std::shared_ptr<PFTNode> ProcessVarExpr(Expr *expr);
  std::shared_ptr<PFTNode> ProcessConstExpr(Expr *expr);
  std::shared_ptr<PFTNode> ProcessOpExpr(Expr *expr);
  std::shared_ptr<PFTNode> ProcessScalarArrayOpExpr(Expr *expr);
  std::shared_ptr<PFTNode> ProcessNullTest(Expr *expr);
  std::shared_ptr<PFTNode> ProcessCastExpr(Expr *expr);
  std::shared_ptr<PFTNode> ProcessFuncExpr(Expr *expr);

  // Make the `quals` to the `AndNode` if length of `quals` > 1
  // quals may create/append by below cases:
  //  - Then key in `where` when we do the `seq scan`
  //  - The key in the `where` when we do the `join`
  //  - The key in `where` same as `on` in the `join`
  //  - The key in `where` which contain the `or` same as `on` in the `join`
  //
  // After call the function, the `filter_tree` will be generate
  //
  void BuildPFTRoot(const std::vector<std::shared_ptr<PFTNode>> &fl_nodes);

  // Used to build the filter tree with the arrow Expression
#ifdef VEC_BUILD
  std::pair<Form_pg_attribute, AttrNumber> VecExprFlatVar(
      const arrow::FieldRef *ref);
  std::shared_ptr<PFTNode> VecExprWalker(
      const arrow::compute::Expression &expr);
  std::shared_ptr<PFTNode> ProcessVecVarExpr(
      const arrow::compute::Expression &expr);
  std::shared_ptr<PFTNode> ProcessVecConstExpr(
      const arrow::compute::Expression &expr);
  std::shared_ptr<PFTNode> ProcessVecOpExpr(
      const arrow::compute::Expression &expr);
  std::shared_ptr<PFTNode> ProcessVecScalarArrayOpExpr(
      const arrow::compute::Expression &expr);
  std::shared_ptr<PFTNode> ProcessVecNullTest(
      const arrow::compute::Expression &expr);
#endif

  // Destroy the single node and its sub-nodes
  // There is currently a bidirectional reference to `filter_tree_`.
  // So we still need to provide a method to dereference
  //
  // if an exception currently occurs, we can still trigger ~PaxSparseFilter()
  // correctly which will call the `DestroyNode(filter_tree_)` to free all
  // memory When an exception occurs when we build the filter_tree_, the orphan
  // nodes can also be freed normally.
  void DestroyNode(const std::shared_ptr<PFTNode> &node);

  // Simplify the filter tree
  // Currently there are two cases that have been handled:
  // 1. An UnsupportedNode is not the subnodes of
  // `IsNodeAcceptUnsupportedNode()`, then parent node will change to the
  // UnsupportedNode
  // 2. An NullTestNode in the subnodes of NotNode, then NotNode will change to
  // the NullTestNode, also the flags in NullTestNode will be invert
  //
  // Current function can be called after filter_tree_ generated.
  bool SimplifyFilterTree(std::shared_ptr<PFTNode> &cur_node);

  // Used to execute the filter tree
  //
  // When we generate filter tree using pg quals or arrow expression,
  // then we will recursively execute the filter tree. The execution logic is
  // consistent. When we build the filter tree, we have ensured that we used the
  // same pg struct to generate filter tree .
  //
  // The following functions will be called from `ExecFilter`, which is the
  // entry point for executing sparese filter.
  bool ExecFilterTree(PaxSparseExecContext *exec_ctx,
                      const std::shared_ptr<PFTNode> &node);
  bool ExecAndNode(PaxSparseExecContext *exec_ctx,
                   const std::shared_ptr<PFTNode> &node);
  bool ExecOrNode(PaxSparseExecContext *exec_ctx,
                  const std::shared_ptr<PFTNode> &node);
  bool ExecOpNode(PaxSparseExecContext *exec_ctx,
                  const std::shared_ptr<PFTNode> &node);
  bool ExecArithmeticOpNode(PaxSparseExecContext *exec_ctx,
                            const std::shared_ptr<PFTNode> &node);
  bool ExecCastNode(PaxSparseExecContext *exec_ctx,
                    const std::shared_ptr<PFTNode> &node);
  bool ExecConstNode(PaxSparseExecContext *exec_ctx,
                     const std::shared_ptr<PFTNode> &node);
  bool ExecVarNode(PaxSparseExecContext *exec_ctx,
                   const std::shared_ptr<PFTNode> &node);
  bool ExecNullTests(PaxSparseExecContext *exec_ctx,
                     const std::shared_ptr<PFTNode> &node);
  bool ExecInNode(PaxSparseExecContext *exec_ctx,
                  const std::shared_ptr<PFTNode> &node);

#ifndef RUN_GTEST
 private:
#endif
  Relation rel_;
#ifdef VEC_BUILD
  std::vector<std::pair<const char *, size_t>> table_names_;
#endif
  std::shared_ptr<PFTNode> filter_tree_;

  // used to log the filter rate
  std::array<std::atomic<int>, StatisticsKind::kEnd> hits_;
  std::array<std::atomic<int>, StatisticsKind::kEnd> totals_;

  bool allow_fallback_to_pg_;
};  // class PaxSparseFilter

}  // namespace pax