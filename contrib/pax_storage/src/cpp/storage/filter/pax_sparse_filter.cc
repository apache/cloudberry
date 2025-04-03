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
 * pax_sparse_filter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_sparse_filter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_sparse_filter.h"

#include "comm/cbdb_api.h"

#include "comm/bloomfilter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "comm/paxc_wrappers.h"
#include "storage/filter/pax_sparse_filter_tree.h"
#include "storage/micro_partition_stats.h"
#include "storage/oper/pax_stats.h"
#include "storage/proto/proto_wrappers.h"

namespace pax {

#define SWAP(T, A, B) \
  do {                \
    (T) = (A);        \
    (A) = (B);        \
    (B) = (T);        \
  } while (0)

#define IsNode(n, t) ((n)->type == (t))
#define IsNodeAcceptUnsupportedNode(n) ((n)->type == AndType)

struct PaxSparseExecContext final {
  // the `from_node` have three types of return results
  // - ConstType: the const expr, the `const_value` will be fill
  // - VarType: the var expr, the `var_value` will be fill
  // - ResultType: the expr return with the bool
  PFTNodeType from_node = UnsupportedType;
  const ColumnStatsProvider &provider;
  const TupleDesc desc;

  // the const expr return value
  struct {
    // the const value
    Datum value;
    // the const type id
    Oid const_type;

    // SK_ISNULL or 0
    int sk_flags;
  } const_value;

  // the var expr return value
  struct {
    // the attribute number
    AttrNumber attrno;

    // the var type id
    Oid typid;

    // the min/max stats part
    Datum min;
    Datum max;
    bool exist_min_max;

    // the bloom filter part
    pax::stats::BloomFilterBasicInfo bf_info;
    std::string bf;
    bool exist_bf;
  } var_value;

  PaxSparseExecContext(const ColumnStatsProvider &p, const TupleDesc d)
      : provider(p), desc(d) {}
  PaxSparseExecContext(const PaxSparseExecContext &ctx)
      : from_node(ctx.from_node),
        provider(ctx.provider),
        desc(ctx.desc),
        const_value(ctx.const_value),
        var_value(ctx.var_value) {}
};

PaxSparseFilter::PaxSparseFilter(Relation relation, bool allow_fallback_to_pg)
    : rel_(relation),
      filter_tree_(nullptr),
      hits_{0},
      totals_{0},
      allow_fallback_to_pg_(allow_fallback_to_pg) {}

static void ConvertFilterTreeToDebugString(
    std::stringstream &ss, const std::string &prefix,
    const std::shared_ptr<PFTNode> &node) {
  if (node->sub_nodes.empty()) return;

  ss << prefix;
  size_t n_subnode = node->sub_nodes.size();
  ss << (n_subnode > 1 ? "├── " : "");

  for (size_t i = 0; i < n_subnode; ++i) {
    auto sub_node = node->sub_nodes[i];
    if (i < n_subnode - 1) {
      if (i > 0) {
        ss << prefix << "├── ";
      }

      ss << sub_node->DebugString() << std::endl;
      ConvertFilterTreeToDebugString(
          ss,
          prefix +
              (n_subnode > 1 && !sub_node->sub_nodes.empty() ? "│  " : "  "),
          sub_node);
    } else {
      ss << (n_subnode > 1 ? prefix : "") << "└── ";
      ss << sub_node->DebugString() << std::endl;
      ConvertFilterTreeToDebugString(ss, prefix + "  ", sub_node);
    }
  }
}

std::string PaxSparseFilter::DebugString() const {
  std::stringstream ss;
  if (!filter_tree_) {
    return "no filter tree build.";
  }

  ss << filter_tree_->DebugString() << std::endl;
  ConvertFilterTreeToDebugString(ss, "  ", filter_tree_);
  return ss.str();
}

void PaxSparseFilter::LogStatistics() const {
  static const char *filter_kind_desc[] = {"file", "group"};
  std::stringstream ss;

  if (pax_enable_debug) {
    auto N = sizeof(filter_kind_desc) / sizeof(char *);
    for (size_t i = 0; i < N; i++) {
      if (totals_[i] == 0) {
        ss << pax::fmt("kind %s, no filter. ", filter_kind_desc[i])
           << std::endl;
      } else {
        ss << pax::fmt("kind %s, filter rate: %d / %d", filter_kind_desc[i],
                       hits_[i].load(), totals_[i].load())
           << std::endl;
      }
    }

    PAX_LOG("%s", ss.str().c_str());
  }
}

bool PaxSparseFilter::ExistsFilterPath() const {
  return filter_tree_ && !IsNode(filter_tree_, UnsupportedType);
}

void PaxSparseFilter::DestroyNode(const std::shared_ptr<PFTNode> &node) {
  if (!node) {
    return;
  }

  for (const auto &sub_node : node->sub_nodes) {
    DestroyNode(sub_node);
  }

  node->sub_nodes.clear();
  node->parent = nullptr;
}

bool PaxSparseFilter::SimplifyFilterTree(std::shared_ptr<PFTNode> &cur_node) {
  bool tainted = false;
  size_t tainted_idx = 0;
  std::shared_ptr<PFTNode> node;

  node = cur_node;
  while (tainted_idx < node->sub_nodes.size() && !tainted) {
    tainted = SimplifyFilterTree(node->sub_nodes[tainted_idx]);
    tainted_idx++;
  }

  if (tainted) {
    auto parent = node->parent;
    auto subidx = node->subidx;
    Assert(tainted_idx > 0 && (tainted_idx - 1) < node->sub_nodes.size());
    auto sub_node = node->sub_nodes[tainted_idx - 1];
    AssertImply(parent, subidx >= 0);

    // deal the case1
    if (!IsNodeAcceptUnsupportedNode(node) &&
        IsNode(sub_node, UnsupportedType)) {
      // cut the true node and bring it into the parent
      sub_node->parent = parent;
      sub_node->subidx = subidx;
      if (parent) parent->sub_nodes[subidx] = sub_node;

      // Don't worry about subidx, as the current node will be removed
      node->sub_nodes.erase(node->sub_nodes.begin() + tainted_idx - 1);

      DestroyNode(node);
      cur_node = sub_node;
      node = sub_node;
      // deal the case2
    } else if (IsNode(node, NotType) && IsNode(sub_node, NullTestType)) {
      // not single node below NOT
      if (node->sub_nodes.size() != 1) {
        DestroyNode(node);
        node = std::make_shared<UnsupportedNode>(
            "Unknown NOT clause which has more than 1 sub nodes");
      } else {  // single node below NOT

        auto nt_node = std::dynamic_pointer_cast<NullTestNode>(sub_node);
        if (nt_node->sk_flags & SK_SEARCHNULL) {
          nt_node->sk_flags &= ~SK_SEARCHNULL;
          nt_node->sk_flags |= SK_SEARCHNOTNULL;
        } else if (nt_node->sk_flags & SK_SEARCHNOTNULL) {
          nt_node->sk_flags &= ~SK_SEARCHNOTNULL;
          nt_node->sk_flags |= SK_SEARCHNULL;
        } else {
          // never happend
          Assert(false);
          return false;
        }

        node->sub_nodes.erase(node->sub_nodes.begin() + tainted_idx - 1);
        DestroyNode(node);
        node = sub_node;
      }

      node->parent = parent;
      node->subidx = subidx;
      if (parent) parent->sub_nodes[subidx] = node;
      cur_node = node;
      // deal the case3 in parent node
    } else {
      // never happend
      Assert(false);
    }
  }

  // case1: current node is TRUE and parent node is unAcceptUnsupportedNode
  if (IsNode(node, UnsupportedType) && node->parent &&
      !IsNodeAcceptUnsupportedNode(node->parent)) {
    return true;
  }

  // case2: current node is NULLTEST and parent node is NOT
  if (IsNode(node, NullTestType) && node->parent &&
      IsNode(node->parent, NotType)) {
    return true;
  }

  return false;
}

PaxSparseFilter::~PaxSparseFilter() { DestroyNode(filter_tree_); }

bool PaxSparseFilter::ExecFilter(const ColumnStatsProvider &provider,
                                 const TupleDesc desc, int kind) {
  PaxSparseExecContext exec_ctx(provider, desc);
  bool filter_failed = true;
  if (!filter_tree_) {
    goto no_filter;
  }

#ifdef ENABLE_DEBUG
  // When we execute the sparse filter,  we require each `PFTNode` to fill the
  // return part of `PaxSparseExecContext`. This is why we do not fill the
  // default value. Filling the default value is more likely to cause that the
  // logic errors be ignored.
  //
  // In the test version of PAX , we use `0x7f` to make logic errors more easily
  // exposed.
  exec_ctx.const_value.value = 0x7f;
  exec_ctx.const_value.const_type = 0;
  exec_ctx.const_value.sk_flags = 0x7f;

  exec_ctx.var_value.attrno = -1;
  exec_ctx.var_value.typid = 0;
  exec_ctx.var_value.min = 0x7f;
  exec_ctx.var_value.max = 0x7f;
  exec_ctx.var_value.exist_min_max = false;
  exec_ctx.var_value.exist_bf = false;
#endif

  filter_failed = ExecFilterTree(&exec_ctx, filter_tree_);
  if (exec_ctx.from_node != ResultType) {
    // The result only can from the result type
    filter_failed = true;
  }

no_filter:
  if (pax_enable_debug) {
    if (!filter_failed) {
      hits_[kind].fetch_add(1);
    }

    totals_[kind].fetch_add(1);
  }
  return filter_failed;
}

static Form_pg_attribute CheckAndGetAttr(AttrNumber column_index,
                                         const ColumnStatsProvider &provider,
                                         const TupleDesc desc, Oid coll_in_op,
                                         bool verify_coll_in_op) {
  Form_pg_attribute attr = nullptr;

  Assert(column_index >= 0 && column_index < desc->natts);

  attr = &desc->attrs[column_index];
  // scan key should never contain dropped column
  Assert(!attr->attisdropped);

  // no any stats for current column
  if (column_index >= provider.ColumnSize()) {
    return nullptr;
  }

  // the collation in catalog/scan key/stats should be consistent
  // FIXME: in scan key/stats be consistent is ok?
  if (attr->attcollation != provider.ColumnInfo(column_index).collation())
    return nullptr;

  if (verify_coll_in_op && OidIsValid(coll_in_op) &&
      attr->attcollation != coll_in_op)
    return nullptr;

  return attr;
}

bool PaxSparseFilter::ExecArithmeticOpNode(
    PaxSparseExecContext *exec_ctx, const std::shared_ptr<PFTNode> &node) {
  bool no_filter = false;
  PaxSparseExecContext lctx_copy(*exec_ctx), rctx_copy(*exec_ctx);
  std::shared_ptr<ArithmeticOpNode> aop_node;
  FmgrInfo finfo;
  FmgrInfo min_finfo, max_finfo;
  Oid coll;
  Assert(node && IsNode(node, ArithmeticOpType) && node->sub_nodes.size() == 2);

  aop_node = std::dynamic_pointer_cast<ArithmeticOpNode>(node);
  no_filter |= ExecFilterTree(&lctx_copy, node->sub_nodes[0]);
  no_filter |= ExecFilterTree(&rctx_copy, node->sub_nodes[1]);
  if (no_filter) {
    return no_filter;
  }

  auto ok = cbdb::PGGetProc(aop_node->opfuncid, &finfo);
  if (!ok) {
    return true;
  }

  coll = aop_node->collation;

  auto init_min_max_funcs = [&](Oid typid) -> bool {
    [[maybe_unused]] FmgrInfo finfo_dummy;
    bool exist_final_finfo = false;
    bool agginitval_isnull = false;
    bool ok;
    Oid prorettype, transtype;

    ok = cbdb::PGGetAggInfo("min", lctx_copy.var_value.typid, &prorettype,
                            &transtype, &min_finfo, &finfo_dummy,
                            &exist_final_finfo, &agginitval_isnull);

    if (!ok || exist_final_finfo || !agginitval_isnull || typid != prorettype) {
      return false;
    }

    ok = cbdb::PGGetAggInfo("max", lctx_copy.var_value.typid, &prorettype,
                            &transtype, &max_finfo, &finfo_dummy,
                            &exist_final_finfo, &agginitval_isnull);

    if (!ok || exist_final_finfo || !agginitval_isnull || typid != prorettype) {
      return false;
    }

    return true;
  };

  // FIXME(jiaqizho): support the "/" opname
  if (lctx_copy.from_node == VarType && rctx_copy.from_node == ConstType) {
    // check left min/max exist
    if (!lctx_copy.var_value.exist_min_max) {
      return true;
    }

    if (aop_node->op_name == ArithmeticAddStr ||
        aop_node->op_name == ArithmeticSubStr) {
      // min = lmin +- rconst
      // max = lmax +- rconst
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.min, rctx_copy.const_value.value);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.max, rctx_copy.const_value.value);
    } else if (aop_node->op_name == ArithmeticMulStr) {
      // 1. left = [pos, pos], right = [pos]
      //    min = lmin * right, max = lmax * right
      // 2. left = [pos, pos], right = [neg]
      //    min = lmax * right, max = lmin * right
      // 3. left = [neg, pos], right = [pos]
      //    min = lmin * right, max = lmax * right
      // 4. left = [neg, pos], right = [neg]
      //    min = lmax * right, max = lmin * right
      // 5. left = [neg, neg], right = [pos]
      //    min = lmax * right, max = lmin * right
      // 6. left = [neg, neg], right = [neg]
      //    min = lmin * right, max = lmax * right
      // so the result is:
      //  min = min(lmin * right, lmax * right)
      //  max = max(lmin * right, lmax * right)
      Datum datum1;
      Datum datum2;
      if (!init_min_max_funcs(lctx_copy.var_value.typid)) {
        return true;
      }

      datum1 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.min,
                                       rctx_copy.const_value.value);
      datum2 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.max,
                                       rctx_copy.const_value.value);
      exec_ctx->var_value.min =
          cbdb::FunctionCall2Coll(&min_finfo, InvalidOid, datum1, datum2);
      exec_ctx->var_value.max =
          cbdb::FunctionCall2Coll(&max_finfo, InvalidOid, datum1, datum2);
    } else {
      Assert(false);
    }

    exec_ctx->var_value.attrno = lctx_copy.var_value.attrno;
    exec_ctx->var_value.typid = lctx_copy.var_value.typid;
    exec_ctx->var_value.exist_min_max = true;
    exec_ctx->var_value.exist_bf = false;
  } else if (lctx_copy.from_node == ConstType &&
             rctx_copy.from_node == VarType) {
    // check right min/max exist
    if (!rctx_copy.var_value.exist_min_max) {
      return true;
    }

    if (aop_node->op_name == ArithmeticAddStr) {
      // min = lconst + rmin
      // max = lconst + rmax
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.min);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.max);
    } else if (aop_node->op_name == ArithmeticSubStr) {
      // min = lconst - rmax
      // max = lconst - rmin
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.max);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.min);
    } else if (aop_node->op_name == ArithmeticMulStr) {
      // same as the case in (VarType + ConstType && ArithmeticMulStr)
      // This is because multiplication is interchangeable
      // so the result is:
      //  min = min(lmin * right, lmax * right)
      //  max = max(lmin * right, lmax * right)
      Datum datum1;
      Datum datum2;
      if (!init_min_max_funcs(rctx_copy.var_value.typid)) {
        return true;
      }

      datum1 = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.min);
      datum2 = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.const_value.value, rctx_copy.var_value.max);
      exec_ctx->var_value.min =
          cbdb::FunctionCall2Coll(&min_finfo, InvalidOid, datum1, datum2);
      exec_ctx->var_value.max =
          cbdb::FunctionCall2Coll(&max_finfo, InvalidOid, datum1, datum2);
    } else {
      Assert(false);
    }

    exec_ctx->var_value.attrno = rctx_copy.var_value.attrno;
    exec_ctx->var_value.typid = rctx_copy.var_value.typid;
    exec_ctx->var_value.exist_min_max = true;
    exec_ctx->var_value.exist_bf = false;
  } else if (lctx_copy.from_node == VarType && rctx_copy.from_node == VarType) {
    // check the left and right both exist min/max
    if (!lctx_copy.var_value.exist_min_max ||
        !rctx_copy.var_value.exist_min_max) {
      return true;
    }

    // must match
    if (lctx_copy.var_value.typid != rctx_copy.var_value.typid) {
      return true;
    }

    if (aop_node->op_name == ArithmeticAddStr) {
      // min = lmin + rmin
      // max = lmax + rmax
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.min, rctx_copy.var_value.min);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.max, rctx_copy.var_value.max);
    } else if (aop_node->op_name == ArithmeticSubStr) {
      // min = lmin - rmax
      // max = lmax - rmin
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.min, rctx_copy.var_value.max);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &finfo, coll, lctx_copy.var_value.max, rctx_copy.var_value.min);
    } else if (aop_node->op_name == ArithmeticMulStr) {
      // 1. left = [pos, pos] right = [pos, pos]
      //   min = lmin * rmin, max = lmax * rmax
      // 2. left = [pos, pos], right = [neg, pos]
      //   min = lmax * rmin, max = lmax * rmax
      // 3. left = [pos, pos], right = [neg, neg]
      //   min = lmax * rmin, max = lmin * rmax
      // 4. left = [neg, pos], right = [pos, pos]
      //   min = lmin * rmax, max = lmax * rmax
      // 5. left = [neg, pos], right = [neg, pos]
      //   min = min(lmin * rmax, lmax * rmin), max = max(lmax * rmax, lmin *
      //   rmin)
      // 6. left = [neg, pos], right = [neg, neg]
      //   min = lmax * rmin, max = lmin * rmin
      // 7. left = [neg, neg], right = [pos, pos]
      //   min = min(lmin * rmax, lmax * rmin), max = lmax * lmin
      // 8. left = [neg, neg], right = [neg, pos]
      //   min = lmin * rmax, max = lmin * rmin
      // 9. left = [neg, neg], right = [neg, neg]
      //   min = lmax * rmax, max = lmin * rmin
      // so the result is:
      //  min = min(lmin * rmin, lmin * rmax, lmax * rmin, lmax * rmax)
      //  max = max(lmin * rmin, lmin * rmax, lmax * rmin, lmax * rmax)
      Datum datum1;  // lmin * rmin
      Datum datum2;  // lmin * rmax
      Datum datum3;  // lmax * rmin
      Datum datum4;  // lmax * rmax

      if (!init_min_max_funcs(lctx_copy.var_value.typid)) {
        return true;
      }

      datum1 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.min,
                                       rctx_copy.var_value.min);
      datum2 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.min,
                                       rctx_copy.var_value.max);
      datum3 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.max,
                                       rctx_copy.var_value.min);
      datum4 = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.max,
                                       rctx_copy.var_value.max);

      exec_ctx->var_value.min =
          cbdb::FunctionCall2Coll(&min_finfo, InvalidOid, datum1, datum2);
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &min_finfo, InvalidOid, exec_ctx->var_value.min, datum3);
      exec_ctx->var_value.min = cbdb::FunctionCall2Coll(
          &min_finfo, InvalidOid, exec_ctx->var_value.min, datum4);

      exec_ctx->var_value.max =
          cbdb::FunctionCall2Coll(&max_finfo, InvalidOid, datum1, datum2);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &max_finfo, InvalidOid, exec_ctx->var_value.max, datum3);
      exec_ctx->var_value.max = cbdb::FunctionCall2Coll(
          &max_finfo, InvalidOid, exec_ctx->var_value.max, datum4);
    } else {
      Assert(false);
    }

    exec_ctx->var_value.attrno = lctx_copy.var_value.attrno;
    exec_ctx->var_value.typid = lctx_copy.var_value.typid;
    exec_ctx->var_value.exist_min_max = true;
    exec_ctx->var_value.exist_bf = false;
  }

  return no_filter;
}

bool PaxSparseFilter::ExecOpNode(PaxSparseExecContext *exec_ctx,
                                 const std::shared_ptr<PFTNode> &node) {
  bool no_filter = false;
  PaxSparseExecContext lctx_copy(*exec_ctx), rctx_copy(*exec_ctx);
  std::shared_ptr<OpNode> op_node;
  OperMinMaxFunc lfunc1, lfunc2;
  FmgrInfo finfo;

  Assert(node && IsNode(node, OpType) && node->sub_nodes.size() == 2);
  op_node = std::dynamic_pointer_cast<OpNode>(node);

  no_filter = no_filter || ExecFilterTree(&lctx_copy, node->sub_nodes[0]);
  no_filter = no_filter || ExecFilterTree(&rctx_copy, node->sub_nodes[1]);
  if (no_filter) {
    return no_filter;
  }

  // deal the op(var, var) case
  if (lctx_copy.from_node == VarType && rctx_copy.from_node == VarType) {
    AttrNumber lidx, ridx;
    Form_pg_attribute lattr, rattr;
    Datum matches = true;

    Oid ltypid, rtypid, coll;
    bool ok;

    if (!lctx_copy.var_value.exist_min_max ||
        !rctx_copy.var_value.exist_min_max) {
      // no min max exist
      return true;
    }

    lidx = lctx_copy.var_value.attrno - 1;
    ridx = rctx_copy.var_value.attrno - 1;

    // Bypass when lattrno is same with rattrno
    if (lidx == ridx) {
      switch (op_node->strategy) {
        case BTLessStrategyNumber:
        case BTGreaterStrategyNumber: {
          return false;
        }
        case BTLessEqualStrategyNumber:
        case BTEqualStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        default:
          return true;
      }
    }

    lattr = CheckAndGetAttr(lidx, lctx_copy.provider, lctx_copy.desc,
                            op_node->collation, true);
    rattr = CheckAndGetAttr(ridx, rctx_copy.provider, rctx_copy.desc,
                            op_node->collation, true);

    // no stats or collation not match
    if (!lattr || !rattr) {
      return true;
    }

    coll = OidIsValid(op_node->collation) ? op_node->collation
                                          : lattr->attcollation;
    ltypid = op_node->left_typid;
    rtypid = op_node->right_typid;
    // no need check the l/r.var_value.typid
    // because in PG, some of type can direct do the cast without cast node
    // used l/r.var_value.typid or typid in `op_node` both ok in this case

    auto left_min_with_right_max = [&](StrategyNumber strategynum) {
      Datum m = true;
      if (MinMaxGetStrategyProcinfo(ltypid, rtypid, coll, lfunc1,
                                    strategynum)) {
        Assert(lfunc1);
        m = lfunc1(&lctx_copy.var_value.min, &rctx_copy.var_value.max, coll);
      } else if (allow_fallback_to_pg_) {
        ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo, strategynum);
        if (!ok) return m;
        m = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.min,
                                    rctx_copy.var_value.max);
      }
      return m;
    };

    auto left_max_with_right_min = [&](StrategyNumber strategynum) {
      Datum m = true;
      if (MinMaxGetStrategyProcinfo(ltypid, rtypid, coll, lfunc1,
                                    strategynum)) {
        Assert(lfunc1);
        m = lfunc1(&lctx_copy.var_value.max, &rctx_copy.var_value.min, coll);
      } else if (allow_fallback_to_pg_) {
        ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo, strategynum);
        if (!ok) return m;
        m = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.max,
                                    rctx_copy.var_value.min);
      }
      return m;
    };

    // v1 < v2: min(v1) >= max(v2) -> return false
    // v1 <= v2: min(v1) > max(v2) -> return false
    // v1 = v2: max(v1) <  min(v2) || min(v1) > max(v2) -> return false
    // v1 >= v2: max(v1) < min(v2) -> return false
    // v1 > v2: max(v1) <= min(v2) -> return false
    switch (op_node->strategy) {
      case BTLessStrategyNumber:
        matches = !left_min_with_right_max(BTGreaterEqualStrategyNumber);
        break;
      case BTLessEqualStrategyNumber: {
        matches = !left_min_with_right_max(BTGreaterStrategyNumber);
        break;
      }
      case BTEqualStrategyNumber: {
        matches &= !left_min_with_right_max(BTGreaterStrategyNumber);
        matches &= !left_max_with_right_min(BTLessStrategyNumber);
        break;
      }
      case BTGreaterEqualStrategyNumber: {
        matches = !left_max_with_right_min(BTLessStrategyNumber);
        break;
      }
      case BTGreaterStrategyNumber: {
        matches = !left_max_with_right_min(BTLessEqualStrategyNumber);
        break;
      }
      default:
        // not support others `sk_strategy`
        matches = BoolGetDatum(true);
        break;
    }

    return DatumGetBool(matches);
    // deal the op(var, const)/op(const, var) case
  } else if ((lctx_copy.from_node == VarType &&
              rctx_copy.from_node == ConstType) ||
             (lctx_copy.from_node == ConstType &&
              rctx_copy.from_node == VarType)) {
    Datum matches = true;
    Oid collation, ltypid, rtypid;
    bool ok;
    AttrNumber column_index;
    Form_pg_attribute attr;
    StrategyNumber strategy;
    Datum lmin, lmax, const_val;

    if (lctx_copy.from_node == VarType && rctx_copy.from_node == ConstType) {
      if (!lctx_copy.var_value.exist_min_max) {
        // no min_max stats
        return true;
      }

      lmin = lctx_copy.var_value.min;
      lmax = lctx_copy.var_value.max;
      const_val = rctx_copy.const_value.value;
      column_index = lctx_copy.var_value.attrno - 1;
      attr = CheckAndGetAttr(column_index, lctx_copy.provider, lctx_copy.desc,
                             op_node->collation, true);
      ltypid = op_node->left_typid;
      rtypid = op_node->right_typid;
      strategy = op_node->strategy;
    } else {
      if (!rctx_copy.var_value.exist_min_max) {
        // no min_max stats
        return true;
      }
      lmin = rctx_copy.var_value.min;
      lmax = rctx_copy.var_value.max;
      const_val = lctx_copy.const_value.value;
      column_index = rctx_copy.var_value.attrno - 1;
      attr = CheckAndGetAttr(column_index, rctx_copy.provider, rctx_copy.desc,
                             op_node->collation, true);
      ltypid = op_node->right_typid;
      rtypid = op_node->left_typid;
      strategy = InvertStrategy(op_node->strategy);
    }

    // no stats or collation not match
    if (!attr) return true;

    collation = OidIsValid(op_node->collation) ? op_node->collation
                                               : attr->attcollation;

    // no need check the l/r.var_value.typid
    // because in PG, some of type can direct do the cast without cast node
    // used l/r.var_value.typid or typid in `op_node` both ok in this case

    switch (strategy) {
      case BTLessStrategyNumber:
      case BTLessEqualStrategyNumber: {
        if (MinMaxGetStrategyProcinfo(ltypid, rtypid, collation, lfunc1,
                                      strategy)) {
          Assert(lfunc1);
          matches = lfunc1(&lmin, &const_val, collation);
        } else if (allow_fallback_to_pg_) {
          ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo, strategy);
          if (!ok) break;
          matches = cbdb::FunctionCall2Coll(&finfo, collation, lmin, const_val);
        }
        break;
      }
      case BTEqualStrategyNumber: {
        if (MinMaxGetStrategyProcinfo(ltypid, rtypid, collation, lfunc1,
                                      BTLessEqualStrategyNumber) &&
            MinMaxGetStrategyProcinfo(ltypid, rtypid, collation, lfunc2,
                                      BTGreaterEqualStrategyNumber)) {
          Assert(lfunc1 && lfunc2);
          matches = lfunc1(&lmin, &const_val, collation);
          if (!DatumGetBool(matches))
            // not (min <= value) --> min > value
            break;
          matches = lfunc2(&lmax, &const_val, collation);
        } else if (allow_fallback_to_pg_) {
          ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo,
                                           BTLessEqualStrategyNumber);
          if (!ok) break;
          matches = cbdb::FunctionCall2Coll(&finfo, collation, lmin, const_val);

          if (!DatumGetBool(matches))
            // not (min <= value) --> min > value
            break;
          ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo,
                                           BTGreaterEqualStrategyNumber);
          if (!ok) break;
          matches = cbdb::FunctionCall2Coll(&finfo, collation, lmin, const_val);
        }
        break;
      }
      case BTGreaterEqualStrategyNumber:
      case BTGreaterStrategyNumber: {
        if (MinMaxGetStrategyProcinfo(ltypid, rtypid, collation, lfunc1,
                                      strategy)) {
          Assert(lfunc1);
          matches = lfunc1(&lmax, &const_val, collation);
        } else if (allow_fallback_to_pg_) {
          ok = MinMaxGetPgStrategyProcinfo(ltypid, rtypid, &finfo, strategy);
          if (!ok) break;
          matches = cbdb::FunctionCall2Coll(&finfo, collation, lmax, const_val);
        }

        break;
      }
      default:
        // not support others `sk_strategy`
        matches = BoolGetDatum(true);
        break;
    }

    return DatumGetBool(matches);

    // deal the op(const, const) case
  } else if ((lctx_copy.from_node == ConstType &&
              rctx_copy.from_node == ConstType)) {
    Datum matches = true;
    bool ok;

    if (op_node->strategy == InvalidStrategy) {
      return true;
    }

    // no need check the typid in op_node
    // because in PG, some of type can direct do the cast without cast node
    // used l/r.const_value.const_typid or typid in `op_node` both ok in this
    // case

    if (MinMaxGetStrategyProcinfo(
            lctx_copy.const_value.const_type, rctx_copy.const_value.const_type,
            op_node->collation, lfunc1, op_node->strategy)) {
      Assert(lfunc1);
      matches = lfunc1(&lctx_copy.const_value.value,
                       &rctx_copy.const_value.value, op_node->collation);
      return matches;
    } else if (allow_fallback_to_pg_) {
      ok = MinMaxGetPgStrategyProcinfo(lctx_copy.const_value.const_type,
                                       rctx_copy.const_value.const_type, &finfo,
                                       op_node->strategy);
      if (!ok) return true;
      matches = cbdb::FunctionCall2Coll(&finfo, op_node->collation,
                                        lctx_copy.const_value.const_type,
                                        rctx_copy.const_value.const_type);
    }

    // if not match (ex. 11 > 42), current should return true
    // if match (ex. 11 < 42), current should return true
    return DatumGetBool(matches);
  } else {  // should not more case
    Assert(false);
  }

  // should not be here
  return true;
}

bool PaxSparseFilter::ExecConstNode(PaxSparseExecContext *exec_ctx,
                                    const std::shared_ptr<PFTNode> &node) {
  std::shared_ptr<ConstNode> const_node;

  Assert(node && IsNode(node, ConstType));

  const_node = std::dynamic_pointer_cast<ConstNode>(node);

  exec_ctx->const_value.value = const_node->const_val;
  exec_ctx->const_value.const_type = const_node->const_type;
  exec_ctx->const_value.sk_flags = const_node->sk_flags;

  return false;
}

bool PaxSparseFilter::ExecCastNode(PaxSparseExecContext *exec_ctx,
                                   const std::shared_ptr<PFTNode> &node) {
  std::shared_ptr<CastNode> cast_node;
  FmgrInfo finfo;

  Assert(node && IsNode(node, CastType));
  Assert(node->sub_nodes.size() == 1);

  if (ExecFilterTree(exec_ctx, node->sub_nodes[0])) {
    return true;
  }

  // Only can cast the var type and need with min/max
  // if current node only have bloom filter, the bloom filter will not work.
  if (exec_ctx->from_node != VarType || !exec_ctx->var_value.exist_min_max) {
    return true;
  }

  cast_node = std::dynamic_pointer_cast<CastNode>(node);
  auto ok = cbdb::PGGetProc(cast_node->opno, &finfo);
  if (!ok) {
    return true;
  }

  exec_ctx->var_value.min =
      cbdb::FunctionCall1Coll(&finfo, cast_node->coll, exec_ctx->var_value.min);
  exec_ctx->var_value.max =
      cbdb::FunctionCall1Coll(&finfo, cast_node->coll, exec_ctx->var_value.max);
  return false;
}

bool PaxSparseFilter::ExecVarNode(PaxSparseExecContext *exec_ctx,
                                  const std::shared_ptr<PFTNode> &node) {
  Form_pg_attribute attr;
  std::shared_ptr<VarNode> var_node;
  int column_index;
  Assert(node && IsNode(node, VarType));

  var_node = std::dynamic_pointer_cast<VarNode>(node);

  column_index = var_node->attrno - 1;
  // no need check the coll match, but opnode will do that
  attr = CheckAndGetAttr(column_index, exec_ctx->provider, exec_ctx->desc,
                         InvalidOid, false);
  // the index out of range, invalid attr
  if (!attr) {
    return true;
  }

  const auto &data_stats = exec_ctx->provider.DataStats(column_index);
  Assert(data_stats.has_minimal() == data_stats.has_maximum());
  if (data_stats.has_minimal()) {
    exec_ctx->var_value.min = pax::MicroPartitionStats::FromValue(
        data_stats.minimal(), attr->attlen, attr->attbyval, column_index);
    exec_ctx->var_value.max = pax::MicroPartitionStats::FromValue(
        data_stats.maximum(), attr->attlen, attr->attbyval, column_index);
    exec_ctx->var_value.exist_min_max = true;
  } else {
    exec_ctx->var_value.exist_min_max = false;
  }

  if (exec_ctx->provider.HasBloomFilter(column_index)) {
    exec_ctx->var_value.bf_info =
        exec_ctx->provider.BloomFilterBasicInfo(column_index);
    exec_ctx->var_value.bf = exec_ctx->provider.GetBloomFilter(column_index);
    exec_ctx->var_value.exist_bf = true;
  } else {
    exec_ctx->var_value.exist_bf = false;
  }

  exec_ctx->var_value.typid = attr->atttypid;
  exec_ctx->var_value.attrno = var_node->attrno;
  return false;
}

bool PaxSparseFilter::ExecNullTests(PaxSparseExecContext *exec_ctx,
                                    const std::shared_ptr<PFTNode> &node) {
  std::shared_ptr<NullTestNode> nt_node;

  Assert(node && IsNode(node, NullTestType));
  nt_node = std::dynamic_pointer_cast<NullTestNode>(node);

  auto exec_null_test = [](const int flags, const bool allnull,
                           const bool hasnull) {
    // handle null test
    // SK_SEARCHNULL and SK_SEARCHNOTNULL must not co-exist with each other
    Assert(flags & SK_ISNULL);
    Assert((flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) !=
           (SK_SEARCHNULL | SK_SEARCHNOTNULL));

    if (flags & SK_SEARCHNULL) {
      // test: IS NULL
      if (!hasnull) return false;
    } else if (flags & SK_SEARCHNOTNULL) {
      // test: IS NOT NULL
      if (allnull) return false;
    } else {
      // Neither IS NULL nor IS NOT NULL was used; assume all indexable
      // operators are strict and thus return false with NULL value in
      // the scan key.
      return false;
    }

    return true;
  };

  Assert(nt_node->sub_nodes.size() == 1);
  bool no_filter = ExecFilterTree(exec_ctx, nt_node->sub_nodes[0]);
  if (no_filter || exec_ctx->from_node != VarType) {
    return true;
  }

  auto attno = exec_ctx->var_value.attrno;
  if (attno > 0) {
    Assert(!TupleDescAttr(exec_ctx->desc, attno - 1)->attisdropped);
    // current column missing values
    // caused by add new column
    if ((attno - 1) >= exec_ctx->provider.ColumnSize()) {
      return true;
    }
    return exec_null_test(nt_node->sk_flags,
                          exec_ctx->provider.AllNull(attno - 1),
                          exec_ctx->provider.HasNull(attno - 1));
  }

  // check all columns, see ExecEvalRowNullInt()
  Assert(attno == 0);

  // missing values in the columns, can't filter
  if (exec_ctx->desc->natts != exec_ctx->provider.ColumnSize()) return true;

  for (int i = 0; i < exec_ctx->desc->natts; i++) {
    if (TupleDescAttr(exec_ctx->desc, i)->attisdropped) continue;
    if (!exec_null_test(nt_node->sk_flags, exec_ctx->provider.AllNull(i),
                        exec_ctx->provider.HasNull(i)))
      return false;
  }
  return true;
}

bool PaxSparseFilter::ExecInNode(PaxSparseExecContext *exec_ctx,
                                 const std::shared_ptr<PFTNode> &node) {
  bool no_filter = false;
  PaxSparseExecContext lctx_copy(*exec_ctx), rctx_copy(*exec_ctx);
  BloomFilter bf;
  ArrayType *arr;
  ArrayIterator arr_it;
  std::shared_ptr<InNode> in_node;
  AttrNumber column_index;
  Form_pg_attribute attr;
  Datum value;
  Oid typid, coll;
  int16 typlen;
  bool typbyval;
  bool is_null;
  bool can_run_bf;

  Assert(node && IsNode(node, InType));
  Assert(node->sub_nodes.size() == 2);
  in_node = std::dynamic_pointer_cast<InNode>(node);

  // The stats in PAX has no effect in `NOT IN` case
  if (!in_node->in) {
    return true;
  }

  no_filter = no_filter || ExecFilterTree(&lctx_copy, node->sub_nodes[0]);
  no_filter = no_filter || ExecFilterTree(&rctx_copy, node->sub_nodes[1]);
  if (no_filter) {
    return no_filter;
  }

  Assert(lctx_copy.from_node == VarType && rctx_copy.from_node == ConstType);

  column_index = lctx_copy.var_value.attrno - 1;
  attr = CheckAndGetAttr(column_index, lctx_copy.provider, lctx_copy.desc,
                         InvalidType, false);
  typid = attr->atttypid;
  typbyval = attr->attbyval;
  typlen = attr->attlen;
  coll = attr->attcollation;

  // Mounting nodes other than VarNode + ConstNode under the IN node
  // will cause the bloomfilter in statistis to become invalid.
  //
  // for example:
  //   select count(*) from t_bf where v1 in (3, 9, -1, '3'::float);
  // v1 CAST to the float, then bloom filter will be different
  can_run_bf = IsNode(in_node->sub_nodes[0], VarType) &&
               IsNode(in_node->sub_nodes[1], ConstType);

  // check the bf part(if set) before we check the min/max
  //
  // If current clause is `NOT IN (a,b)` case then we cannot use bloom filter to
  // filter. This is because bloom filter has false positives. We can only
  // expect that the element is not in bloom filter, it is only possible to
  // filter the `IN (a,b)` case.
  if (can_run_bf && lctx_copy.var_value.exist_bf) {
    bool not_in_bf = true;

    // Bloom filter not support the numeric
    // Because there is `n_header` field in numeric type,
    // In PG, different `n_header` will be considered to be the same numeric
    // type. for example:
    //  1. create table t(v1 numeric(20, 10)) using pax
    //  with(bloomfilter_columns='v1');
    //  2. insert into t values(...);
    //  3. select * from in_test_t where v1 in (2.0, 4.0);
    // then the value `2.0` won't be numeric(20,10), but it will be the
    // numeric(1,0) which have the different `n_header` with numeric(20,10), so
    // we can't use the bloom filter in this case.
    if (typid == NUMERICOID) {
      return true;
    }

    Assert(lctx_copy.var_value.bf_info.bf_hash_funcs() != 0);
    Assert(lctx_copy.var_value.bf_info.bf_m() != 0);
    Assert(lctx_copy.var_value.bf.length() != 0);
    bf.Create(lctx_copy.var_value.bf.c_str(),
              lctx_copy.var_value.bf_info.bf_m(),
              lctx_copy.var_value.bf_info.bf_seed(),
              lctx_copy.var_value.bf_info.bf_hash_funcs());

    arr = cbdb::DatumToArrayTypeP(rctx_copy.const_value.value);
    // invalid arr datum
    if (!arr || cbdb::ArrayGetN(ARR_NDIM(arr), ARR_DIMS(arr)) <= 0) {
      return true;
    }

    arr_it = cbdb::ArrayCreateIterator(arr, 0, NULL);
    while (cbdb::ArrayIterate(arr_it, &value, &is_null)) {
      if (is_null) {  // IN (..., NULL, ...)
        not_in_bf = !exec_ctx->provider.HasNull(column_index);
      } else {
        if (typbyval) {
          switch (typlen) {
            case 1: {
              auto val_no_ptr = cbdb::DatumToInt8(value);
              not_in_bf = bf.Test((unsigned char *)&val_no_ptr, typlen);
              break;
            }
            case 2: {
              auto val_no_ptr = cbdb::DatumToInt16(value);
              not_in_bf = bf.Test((unsigned char *)&val_no_ptr, typlen);
              break;
            }
            case 4: {
              auto val_no_ptr = cbdb::DatumToInt32(value);
              not_in_bf = bf.Test((unsigned char *)&val_no_ptr, typlen);
              break;
            }
            case 8: {
              auto val_no_ptr = cbdb::DatumToInt64(value);
              not_in_bf = bf.Test((unsigned char *)&val_no_ptr, typlen);
              break;
            }
            default:
              Assert(false);
          }
        } else if (typlen == -1) {
          auto val_ptr =
              reinterpret_cast<struct varlena *>(cbdb::DatumToPointer(value));
          char *val_data = VARDATA_ANY(val_ptr);
          auto val_len = VARSIZE_ANY_EXHDR(val_ptr);

          // ignore trailing spaces so it can filter text/varchar correctly
          if (typid == BPCHAROID) val_len = bpchartruelen(val_data, val_len);

          // safe to direct call, cause no toast here
          not_in_bf = bf.Test((unsigned char *)val_data, val_len);
        } else {
          Assert(typlen > 0);
          auto val_ptr = (unsigned char *)cbdb::DatumToPointer(value);
          Size real_size;
          // Pass by reference, but not varlena, so not toasted
          real_size = datumGetSize(value, typbyval, typlen);
          not_in_bf = bf.Test(val_ptr, real_size);
        }

      }  // if (!is_null);

      if (!not_in_bf) {
        // in bloom filter, can't filter
        break;
      }
    }

    cbdb::ArrayFreeIterator(arr_it);
    if (not_in_bf) return false;
  } else {
    // No bloom filter or `NOT IN` case
    // Then we try using the MIN/MAX to filter it
    bool not_in_range = true;
    OperMinMaxFunc lfunc1, lfunc2;

    // no stats or collation not match
    if (!lctx_copy.var_value.exist_min_max ||
        (coll != exec_ctx->provider.ColumnInfo(column_index).collation())) {
      return true;
    }

    arr = cbdb::DatumToArrayTypeP(rctx_copy.const_value.value);
    // invalid arr datum
    if (!arr || cbdb::ArrayGetN(ARR_NDIM(arr), ARR_DIMS(arr)) <= 0) {
      return true;
    }

    arr_it = cbdb::ArrayCreateIterator(arr, 0, NULL);

    auto in_range = [&](Datum _value) {
      Datum m = true;
      FmgrInfo finfo;
      bool ok;

      if (MinMaxGetStrategyProcinfo(typid, typid, coll, lfunc1,
                                    BTLessEqualStrategyNumber) &&
          MinMaxGetStrategyProcinfo(typid, typid, coll, lfunc2,
                                    BTGreaterEqualStrategyNumber)) {
        Assert(lfunc1 && lfunc2);
        m = lfunc1(&lctx_copy.var_value.min, &_value, coll);
        if (!DatumGetBool(m))  // not (min <= value) --> min > value
          goto min_max_final;
        m = lfunc2(&lctx_copy.var_value.max, &_value,
                   coll);  // max >= value or not
      } else if (allow_fallback_to_pg_) {
        ok = MinMaxGetPgStrategyProcinfo(typid, typid, &finfo,
                                         BTLessEqualStrategyNumber);
        if (!ok) goto min_max_final;
        m = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.min,
                                    _value);
        if (!DatumGetBool(m))  // not (min <= value) --> min > value
          goto min_max_final;

        ok = MinMaxGetPgStrategyProcinfo(typid, typid, &finfo,
                                         BTGreaterEqualStrategyNumber);
        if (!ok) goto min_max_final;
        m = cbdb::FunctionCall2Coll(&finfo, coll, lctx_copy.var_value.max,
                                    _value);
      }

    min_max_final:
      return DatumGetBool(m);
    };  // function in_range

    while (cbdb::ArrayIterate(arr_it, &value, &is_null)) {
      if (is_null) {  // IN (..., NULL, ...)
        not_in_range &= !exec_ctx->provider.HasNull(column_index);
      } else {
        not_in_range &= !in_range(value);
      }

      if (!not_in_range) {
        // in range, can't filter
        break;
      }
    }

    if (not_in_range) return false;
  }

  return true;
}

bool PaxSparseFilter::ExecAndNode(PaxSparseExecContext *exec_ctx,
                                  const std::shared_ptr<PFTNode> &node) {
  bool no_filter = true;

  Assert(IsNode(node, AndType));
  Assert(!node->sub_nodes.empty());

  for (const auto &sub : node->sub_nodes) {
    no_filter = ExecFilterTree(exec_ctx, sub);
    if (exec_ctx->from_node != ResultType) {
      // ignore the case which from not result node
      no_filter = true;
    }
    if (!no_filter) {
      break;
    }
  }
  return no_filter;
}

bool PaxSparseFilter::ExecOrNode(PaxSparseExecContext *exec_ctx,
                                 const std::shared_ptr<PFTNode> &node) {
  bool no_filter = true;
  Assert(IsNode(node, OrType));
  Assert(!node->sub_nodes.empty());
  for (const auto &sub : node->sub_nodes) {
    no_filter = ExecFilterTree(exec_ctx, sub);
    if (no_filter || exec_ctx->from_node != ResultType) {
      // once `no_filter == true && exec_ctx.from_node != ResultType`
      // which means current filter not work
      no_filter = true;
      break;
    }
  }
  exec_ctx->from_node = ResultType;
  return no_filter;
}

bool PaxSparseFilter::ExecFilterTree(PaxSparseExecContext *exec_ctx,
                                     const std::shared_ptr<PFTNode> &node) {
  bool no_filter = true;
  switch (node->type) {
    case AndType: {
      no_filter = ExecAndNode(exec_ctx, node);
      exec_ctx->from_node = ResultType;
      break;
    }
    case OrType: {
      no_filter = ExecOrNode(exec_ctx, node);
      exec_ctx->from_node = ResultType;
      break;
    }
    case NotType: {
      // I'm not sure which case is NOT case besides NULLTEST
      // just let us ignore it
      // Then NotType + NULLTEST will be flat in `SimplifyFilterTree`
      no_filter = true;
      exec_ctx->from_node = ResultType;
      break;
    }
    case CastType: {
      no_filter = ExecCastNode(exec_ctx, node);
      exec_ctx->from_node = VarType;
      break;
    }
    case VarType: {
      no_filter = ExecVarNode(exec_ctx, node);
      exec_ctx->from_node = VarType;
      break;
    }
    case ConstType: {
      no_filter = ExecConstNode(exec_ctx, node);
      exec_ctx->from_node = ConstType;
      break;
    }
    case ArithmeticOpType: {
      no_filter = ExecArithmeticOpNode(exec_ctx, node);
      exec_ctx->from_node = VarType;
      break;
    }
    case OpType: {
      no_filter = ExecOpNode(exec_ctx, node);
      exec_ctx->from_node = ResultType;
      break;
    }
    case NullTestType: {
      no_filter = ExecNullTests(exec_ctx, node);
      exec_ctx->from_node = ResultType;
      break;
    }
    case InType: {
      no_filter = ExecInNode(exec_ctx, node);
      exec_ctx->from_node = ResultType;
      break;
    }
    case UnsupportedType: {
      no_filter = true;
      exec_ctx->from_node = InvalidType;
      break;
    }
    default: {
      no_filter = true;
      exec_ctx->from_node = InvalidType;
      Assert(false);
      break;
    }
  }

  return no_filter;
}

}  // namespace pax