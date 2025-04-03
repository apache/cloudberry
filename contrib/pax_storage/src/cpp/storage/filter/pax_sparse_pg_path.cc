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
 * pax_sparse_pg_path.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_sparse_pg_path.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/guc.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "comm/paxc_wrappers.h"
#include "storage/filter/pax_sparse_filter.h"
#include "storage/oper/pax_stats.h"

namespace pax {

void PaxSparseFilter::Initialize(List *quals) {
  ListCell *qual_cell;
  std::vector<std::shared_ptr<PFTNode>> fl_nodes; /* first level nodes */
  std::string origin_tree_str;

  // no inited
  Assert(!filter_tree_);

  if (!quals) {
    return;
  }

  foreach (qual_cell, quals) {
    Expr *fl_clause = (Expr *)lfirst(qual_cell);
    std::shared_ptr<PFTNode> fl_node = ExprWalker(fl_clause);

    Assert(fl_node);

    fl_nodes.emplace_back(std::move(fl_node));
  }

  // build the root of `filter_tree_`
  BuildPFTRoot(fl_nodes);
  if (pax_log_filter_tree) origin_tree_str = DebugString();

  SimplifyFilterTree(filter_tree_);
  PAX_LOG_IF(pax_log_filter_tree,
             "Origin filter tree: \n%s\nFinal filter tree: \n%s\n",
             origin_tree_str.c_str(), DebugString().c_str());
}

Expr *PaxSparseFilter::ExprFlatVar(Expr *clause) {
  Expr *flat_clause = clause;
  if (unlikely(!clause)) {
    return flat_clause;
  }

  if (IsA(clause, RelabelType)) {
    // Checks if the arg is of type Var
    // and if it is uses the Var as the left operator
    RelabelType *relabelType = (RelabelType *)clause;
    Expr *var_clause = relabelType->arg;

    if (IsA(var_clause, Var)) {
      flat_clause = var_clause;
    }
  }
  return flat_clause;
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessVarExpr(Expr *clause) {
  std::string ignore_reason;
  AttrNumber varattno;
  std::shared_ptr<VarNode> var_node = nullptr;

  Assert(IsA(clause, Var) || IsA(clause, RelabelType));
  clause = ExprFlatVar(clause);

  varattno = ((Var *)clause)->varattno;
  if (varattno < 1 || varattno > RelationGetNumberOfAttributes(rel_)) {
    ignore_reason = pax::fmt(
        "Invalid left Var in OpExpr, [varattno=%d, number_of_attrs=%d]",
        varattno, RelationGetNumberOfAttributes(rel_));
    goto ignore_clause;
  }

  var_node = std::make_shared<VarNode>();
  var_node->attrno = varattno;

  return var_node;

ignore_clause:
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessConstExpr(Expr *clause) {
  std::shared_ptr<ConstNode> const_node;
  Const *const_clause;

  const_clause = castNode(Const, clause);

  const_node = std::make_shared<ConstNode>();
  const_node->const_val = const_clause->constvalue;
  const_node->const_type = const_clause->consttype;
  if (const_clause->constisnull) {
    const_node->sk_flags |= SK_ISNULL;
  }

  return const_node;
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessOpExpr(Expr *clause) {
  Oid opno; /* operator's OID */
  Oid collid = InvalidOid;
  NameData op_name;              /* operator's name */
  StrategyNumber op_strategy;    /* operator's strategy number */
  Oid op_lefttype, op_righttype; /* operator's declared input types */
  Expr *leftop, *rightop;        /* clause on lhs,rhs of operator */
  bool ok = false;
  std::string ignore_reason;
  std::shared_ptr<PFTNode> cur_node = nullptr;
  std::shared_ptr<PFTNode> left_node = nullptr;
  std::shared_ptr<PFTNode> right_node = nullptr;
  OpExpr *op_expr;
  bool is_arithmetic;

  op_expr = castNode(OpExpr, clause);

  // Get the basic info from the OpExpr
  {
    opno = op_expr->opno;
    collid = op_expr->inputcollid;

    ok = cbdb::PGGetOperatorNo(opno, &op_name, &op_lefttype, &op_righttype,
                               NULL);

    if (!ok) {
      ignore_reason = pax::fmt("Invalid proc in OpExpr, [opno=%d]", opno);
      goto ignore_clause;
    }

    op_strategy = OpernameToStrategy(NameStr(op_name));
    is_arithmetic = SupportedArithmeticOpername(NameStr(op_name));
    // not the comparison operator or arithmetic operator
    if (op_strategy == InvalidStrategy && !is_arithmetic) {
      ignore_reason = pax::fmt(
          "Invalid strategy in OpExpr, [opno=%d, name=%s, ltype=%d, rtype=%d]",
          opno, NameStr(op_name) ? NameStr(op_name) : "empty name", op_lefttype,
          op_righttype);
      goto ignore_clause;
    }
  }

  // flat the RelabelType
  leftop = (Expr *)get_leftop(clause);
  rightop = (Expr *)get_rightop(clause);
  if (!leftop || !rightop) {
    ignore_reason = pax::fmt("Empty OpExpr in OpExpr, [laddr=%s, raddr=%s]",
                             BOOL_TOSTRING(leftop), BOOL_TOSTRING(rightop));
    goto ignore_clause;
  }

  if (op_strategy != InvalidStrategy) {
    auto op_node = std::make_shared<OpNode>();
    op_node->opno = opno;
    op_node->strategy = op_strategy;
    op_node->collation = collid;
    op_node->left_typid = op_lefttype;
    op_node->right_typid = op_righttype;

    cur_node = op_node;
  } else {
    Assert(is_arithmetic);
    auto aop_node = std::make_shared<ArithmeticOpNode>();
    aop_node->opfuncid = op_expr->opfuncid;
    aop_node->op_name = std::string(NameStr(op_name));
    aop_node->collation = collid;
    aop_node->left_typid = op_lefttype;
    aop_node->right_typid = op_righttype;

    cur_node = aop_node;
  }

  Assert(cur_node);

  // Support these cases:
  //
  //  - (var, const)
  //  - (const, var)
  //  - (var, var)
  left_node = ExprWalker(leftop);
  right_node = ExprWalker(rightop);

  PFTNode::AppendSubNode(cur_node, std::move(left_node));
  PFTNode::AppendSubNode(cur_node, std::move(right_node));

  return cur_node;
ignore_clause:
  Assert(!left_node && !right_node && !cur_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessCastExpr(Expr *clause) {
  std::shared_ptr<CastNode> cast_node = nullptr;
  std::shared_ptr<PFTNode> sub_node = nullptr;
  FuncExpr *func_clause;
  std::string ignore_reason;
  Expr *sub_expr;

  func_clause = castNode(FuncExpr, clause);
  Assert(func_clause->funcformat == COERCE_EXPLICIT_CAST ||
         func_clause->funcformat == COERCE_IMPLICIT_CAST);

  if (list_length(func_clause->args) != 1) {
    ignore_reason = pax::fmt("Unexpected CAST args [len=%d]",
                             list_length(func_clause->args));
    goto ignore_clause;
  }

  sub_expr = (Expr *)linitial(func_clause->args);
  if (!sub_expr) {
    ignore_reason = pax::fmt("Empty sub expr in CAST expr");
    goto ignore_clause;
  }

  sub_node = ExprWalker(sub_expr);
  Assert(sub_node);

  cast_node = std::make_shared<CastNode>();
  cast_node->opno = func_clause->funcid;
  cast_node->result_typid = func_clause->funcresulttype;
  cast_node->coll = func_clause->funccollid;
  PFTNode::AppendSubNode(cast_node, std::move(sub_node));

  return cast_node;
ignore_clause:
  Assert(!cast_node && !sub_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessFuncExpr(Expr *clause) {
  FuncExpr *func_clause;
  std::string ignore_reason;

  func_clause = castNode(FuncExpr, clause);
  switch (func_clause->funcformat) {
    case COERCE_EXPLICIT_CAST:  // fallthrough
    case COERCE_IMPLICIT_CAST: {
      return ProcessCastExpr(clause);
    }
    default: {
      ignore_reason =
          pax::fmt("Not support funcformat in FuncExpr, [funcformat=%d]",
                   func_clause->funcformat);
      goto ignore_clause;
    }
  }

  Assert(false);
ignore_clause:
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessScalarArrayOpExpr(
    Expr *clause) {
  ScalarArrayOpExpr *sa_clause;
  Expr *leftop;  /* clause on lhs of operator */
  Expr *rightop; /* clause on rhs of operator */
  std::string ignore_reason;
  std::shared_ptr<InNode> array_node = nullptr;
  std::shared_ptr<PFTNode> var_node = nullptr, const_node = nullptr;

  sa_clause = castNode(ScalarArrayOpExpr, clause);

  leftop = (Expr *)linitial(sa_clause->args);
  rightop = (Expr *)lsecond(sa_clause->args);

  var_node = ExprWalker(leftop);
  const_node = ExprWalker(rightop);

  Assert(var_node && const_node);
  array_node = std::make_shared<InNode>();

  array_node->in = ((ScalarArrayOpExpr *)clause)->useOr;  // not in  `!=/<>`
  PFTNode::AppendSubNode(array_node, std::move(var_node));
  PFTNode::AppendSubNode(array_node, std::move(const_node));

  return array_node;
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessNullTest(Expr *clause) {
  NullTest *nt_clause;
  std::shared_ptr<NullTestNode> nt_node = nullptr;
  std::shared_ptr<PFTNode> sub_node = nullptr;
  Expr *leftop; /* expr on lhs of operator */
  std::string ignore_reason;
  int sk_flags;

  nt_clause = castNode(NullTest, clause);
  leftop = ExprFlatVar(nt_clause->arg);

  // Var always be left in the nulltest
  sub_node = ExprWalker(leftop);
  Assert(sub_node);

  if (nt_clause->nulltesttype == IS_NULL) {
    sk_flags = SK_ISNULL | SK_SEARCHNULL;
  } else if (nt_clause->nulltesttype == IS_NOT_NULL) {
    sk_flags = SK_ISNULL | SK_SEARCHNOTNULL;
  } else {
    ignore_reason = pax::fmt("Invalid NullTest type, [nulltesttype=%d]",
                             nt_clause->nulltesttype);
    goto ignore_clause;
  }

  nt_node = std::make_shared<NullTestNode>();
  nt_node->sk_flags = sk_flags;
  PFTNode::AppendSubNode(nt_node, std::move(sub_node));
  return nt_node;

ignore_clause:
  Assert(!nt_node);
  DestroyNode(sub_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ExprWalker(Expr *clause) {
  std::shared_ptr<PFTNode> node = nullptr;

  Assert(clause);

  switch (clause->type) {
    case T_RelabelType:
    case T_Var:
      node = ProcessVarExpr(clause);
      break;
    case T_Const:
      node = ProcessConstExpr(clause);
      break;
    case T_FuncExpr:
      node = ProcessFuncExpr(clause);
      break;
    case T_OpExpr:
      node = ProcessOpExpr(clause);
      break;
    case T_ScalarArrayOpExpr:
      node = ProcessScalarArrayOpExpr(clause);
      break;
    case T_NullTest:
      node = ProcessNullTest(clause);
      break;
    case T_BoolExpr: {
      ListCell *lc;
      BoolExpr *bool_clause = ((BoolExpr *)clause);
      if (is_andclause(clause)) {
        node = std::make_shared<AndNode>();
      } else if (is_orclause(clause)) {
        node = std::make_shared<OrNode>();
      } else if (is_notclause(clause)) {
        node = std::make_shared<NotNode>();
      } else {
        node = std::make_shared<UnsupportedNode>("Unknown And Expr");
        break;
      }

      foreach (lc, bool_clause->args) {
        Expr *sub_clause = (Expr *)lfirst(lc);
        std::shared_ptr<PFTNode> sub_node = nullptr;

        sub_node = ExprWalker(sub_clause);

        PFTNode::AppendSubNode(node, std::move(sub_node));
      }

      break;
    }
    default:
      // can't call the nodetostring in this time
      node = std::make_shared<UnsupportedNode>("Unknown Expr");
      break;
  }

  Assert(node);
  return node;
}

void PaxSparseFilter::BuildPFTRoot(
    const std::vector<std::shared_ptr<PFTNode>> &fl_nodes) {
  Assert(!fl_nodes.empty());
  if (fl_nodes.size() == 1) {
    filter_tree_ = fl_nodes[0];
    return;
  }

  filter_tree_ = std::make_shared<AndNode>();
  for (auto node : fl_nodes) {
    PFTNode::AppendSubNode(filter_tree_, std::move(node));
  }
}

}  // namespace pax