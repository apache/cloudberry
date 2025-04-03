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
 * pax_sparse_vec_path.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_sparse_vec_path.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_sparse_filter.h"

#ifdef VEC_BUILD
#include "comm/cbdb_api.h"

#include "comm/bloomfilter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "comm/log.h"
#include "comm/pax_memory.h"
#include "comm/paxc_wrappers.h"
#include "comm/vec_numeric.h"
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

static ArrayType *InitializeArray(
    Form_pg_attribute attr, const std::shared_ptr<arrow::ArrayData> &array) {
  Datum *datums;
  bool *nulls;
  int len = array->length;

  if (array->buffers.size() < 2) return nullptr;

  auto &null_array = array->buffers[0];
  auto &b1_array = array->buffers[1];
  auto typid PG_USED_FOR_ASSERTS_ONLY = array->type->id();
  auto data = b1_array->data();  // data for fixed-length types

  datums = (Datum *)cbdb::Palloc(sizeof(Datum) * len);
  nulls = (bool *)cbdb::Palloc(sizeof(bool) * len);

  if (null_array) {
    const uint8_t *bitmap = null_array->data();

    for (int64_t i = 0; i < array->length; ++i) {
      // null 0, non-null 1
      nulls[i] = !arrow::bit_util::GetBit(bitmap, i);
    }
  } else {
    // if no null_bitmap_data, there is no null value
    std::memset(nulls, false, len);
  }

  switch (attr->atttypid) {
    case BOOLOID: {
      Assert(typid == arrow::Type::BOOL);
      auto bitmap = reinterpret_cast<const uint8_t *>(data);

      for (int i = 0; i < len; i++) {
        datums[i] = BoolGetDatum(arrow::bit_util::GetBit(bitmap, i));
      }
      break;
    }
    case INT2OID: {
      Assert(typid == arrow::Type::INT16);
      const int16_t *p = reinterpret_cast<const int16_t *>(data);
      for (int i = 0; i < len; i++) {
        datums[i] = Int16GetDatum(p[i]);
      }
      break;
    }

    case DATEOID:  // fallthrough
    case INT4OID: {
      Assert(attr->attlen == 4);
      const int32_t *p = reinterpret_cast<const int32_t *>(data);
      for (int i = 0; i < len; i++) {
        datums[i] = Int32GetDatum(p[i]);
      }
      break;
    }

    case TIMEOID:         // fallthrough
    case TIMESTAMPOID:    // fallthrough
    case TIMESTAMPTZOID:  // fallthrough
    case INT8OID: {
      Assert(attr->attlen == 8);
      static_assert(sizeof(Datum) == sizeof(int64));
      memcpy(&datums[0], data, sizeof(Datum) * len);
      break;
    }
    case FLOAT4OID: {
      Assert(typid == arrow::Type::FLOAT);
      const float *p = reinterpret_cast<const float *>(data);
      for (int i = 0; i < len; i++) {
        datums[i] = Float4GetDatum(p[i]);
      }
      break;
    }
    case FLOAT8OID: {
      Assert(typid == arrow::Type::DOUBLE);
      auto p = reinterpret_cast<const double *>(data);
      for (int i = 0; i < len; i++) {
        datums[i] = Float8GetDatum(p[i]);
      }
      break;
    }
    case NUMERICOID: {
      Assert(typid == arrow::Type::NUMERIC128);
      auto p = reinterpret_cast<const int64_t *>(data);
      for (int i = 0, k = 0; i < len; i++, k += 2) {
        if (nulls[i]) continue;

        datums[i] = vec_short_numeric_to_datum(&p[k], &p[k + 1]);
      }
      break;
    }
    case TEXTOID: {
      data = array->buffers[2]->data();
      auto p = reinterpret_cast<const char *>(data);
      auto offsets = reinterpret_cast<const int32_t *>(b1_array->data());
      Assert(offsets);

      int32_t offset = offsets[0];
      for (int i = 0; i < len; i++) {
        int32_t offset2 = offsets[i + 1];
        if (!nulls[i]) {
          int32_t value_len = offset2 - offset;
          Assert(value_len >= 0);

          auto ptr = cbdb::CstringToText(p, static_cast<size_t>(value_len));
          datums[i] = PointerGetDatum(ptr);
          p += value_len;
        }
        offset = offset2;
      }
      break;
    }
    case VARCHAROID: {
      data = array->buffers[2]->data();
      auto p = reinterpret_cast<const char *>(data);
      auto offsets = reinterpret_cast<const int32_t *>(b1_array->data());
      Assert(offsets);

      int32_t offset = offsets[0];
      int typmod = attr->atttypmod;
      for (int i = 0; i < len; i++) {
        int32_t offset2 = offsets[i + 1];
        if (!nulls[i]) {
          int32_t value_len = offset2 - offset;
          Assert(value_len >= 0);

          auto ptr =
              cbdb::VarcharInput(p, static_cast<size_t>(value_len), typmod);
          datums[i] = PointerGetDatum(ptr);
          p += value_len;
        }
        offset = offset2;
      }
      break;
    }
    case BPCHAROID: {
      data = array->buffers[2]->data();
      auto p = reinterpret_cast<const char *>(data);
      auto offsets = reinterpret_cast<const int32_t *>(b1_array->data());
      Assert(offsets);

      int32_t offset = offsets[0];
      int typmod = attr->atttypmod;
      for (int i = 0; i < len; i++) {
        int32_t offset2 = offsets[i + 1];
        if (!nulls[i]) {
          int32_t value_len = offset2 - offset;
          Assert(value_len >= 0);

          auto ptr =
              cbdb::BpcharInput(p, static_cast<size_t>(value_len), typmod);
          datums[i] = PointerGetDatum(ptr);
          p += value_len;
        }
        offset = offset2;
      }
      break;
    }
    default:
      return nullptr;
  }

  return cbdb::ConstructMdArrayType(datums, nulls, len, attr->atttypid,
                                    attr->attlen, attr->attbyval,
                                    attr->attalign);
}

inline StrategyNumber GetStrategyByVecFuncName(const std::string &func_name) {
  if (func_name == "greater") {
    return BTGreaterStrategyNumber;
  } else if (func_name == "greater_equal") {
    return BTGreaterEqualStrategyNumber;
  } else if (func_name == "less") {
    return BTLessStrategyNumber;
  } else if (func_name == "less_equal") {
    return BTLessEqualStrategyNumber;
  } else if (func_name == "equal") {
    return BTEqualStrategyNumber;
  } else {
    // not support operator
    return InvalidStrategy;
  }
}
template <typename T>
inline auto ToScalarValue(const arrow::Scalar *scalar) {
  auto derived = dynamic_cast<const T *>(scalar);
  Assert(derived);
  return derived->value;
}

static std::pair<Datum, bool> ArrowScalarToDatum(
    const std::shared_ptr<arrow::Scalar> &p_scalar, Form_pg_attribute attr) {
  auto scalar = p_scalar.get();

  switch (scalar->type->id()) {
    case arrow::Type::BOOL: {
      auto v = ToScalarValue<arrow::BooleanScalar>(scalar);
      return {BoolGetDatum(v), true};
    }
    case arrow::Type::INT8: {
      auto v = ToScalarValue<arrow::Int8Scalar>(scalar);
      return {Int8GetDatum(v), true};
    }
    case arrow::Type::INT16: {
      auto v = ToScalarValue<arrow::Int16Scalar>(scalar);
      return {Int16GetDatum(v), true};
    }
    case arrow::Type::INT32: {
      // candicate: int4, xid, oid
      auto v = ToScalarValue<arrow::Int32Scalar>(scalar);
      return {Int32GetDatum(v), true};
    }
    case arrow::Type::INT64: {
      auto v = ToScalarValue<arrow::Int64Scalar>(scalar);
      return {Int64GetDatum(v), true};
    }
    case arrow::Type::FLOAT: {
      auto v = ToScalarValue<arrow::FloatScalar>(scalar);
      return {Float4GetDatum(v), true};
    }
    case arrow::Type::DOUBLE: {
      auto v = ToScalarValue<arrow::DoubleScalar>(scalar);
      return {Float8GetDatum(v), true};
    }
    case arrow::Type::DATE32: {
      auto v = ToScalarValue<arrow::Date32Scalar>(scalar);
      return {Int32GetDatum(v), true};
    }
    case arrow::Type::NUMERIC128: {
      auto const v = ToScalarValue<arrow::Numeric128Scalar>(scalar);
      auto high = static_cast<int64>(v.high_bits());
      auto low = static_cast<int64>(v.low_bits());
      auto datum = vec_short_numeric_to_datum(&low, &high);
      return {datum, true};
    }
    case arrow::Type::TIMESTAMP: {
      auto v = ToScalarValue<arrow::TimestampScalar>(scalar);
      switch (attr->atttypid) {
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
          return {Int64GetDatum(v), true};
        default:
          return {0, false};
      }
      break;
    }
    case arrow::Type::STRING: {
      auto v = ToScalarValue<arrow::StringScalar>(scalar);
      const char *s = reinterpret_cast<const char *>(v->data());
      auto len = static_cast<size_t>(v->size());
      switch (attr->atttypid) {
        case TEXTOID:
          return {PointerGetDatum(cbdb::CstringToText(s, len)), true};
        case VARCHAROID:
          // trailing spaces will be removed
          return {PointerGetDatum(cbdb::VarcharInput(s, len, attr->atttypmod)),
                  true};
        case BPCHAROID:
          // stripped spaces will be added
          return {PointerGetDatum(cbdb::BpcharInput(s, len, attr->atttypmod)),
                  true};
        default:
          break;
      }
      return {0, false};
    }
    default:
      break;
  }
  return {0, false};
}

std::pair<Form_pg_attribute, AttrNumber> PaxSparseFilter::VecExprFlatVar(
    const arrow::FieldRef *ref) {
  Assert(rel_);

  if (!ref || !ref->IsName()) {
    return std::make_pair(nullptr, -1);
  }

  auto pname = arrow::ExtractFieldName(*ref->name());
  auto attno = arrow::FindFieldIndex(table_names_, pname) + 1;

  if (attno <= 0 || attno > RelationGetNumberOfAttributes(rel_)) {
    return std::make_pair(nullptr, -1);
  }

  return std::make_pair(TupleDescAttr(RelationGetDescr(rel_), attno - 1),
                        attno);
}

void PaxSparseFilter::Initialize(
    const arrow::compute::Expression &expr,
    const std::vector<std::pair<const char *, size_t>> &table_names) {
  std::string origin_tree_str;

  // no inited
  Assert(!filter_tree_);
  if (!expr.call()) {
    // empty expr
    return;
  }

  table_names_ = table_names;

  filter_tree_ = VecExprWalker(expr);
  if (pax_log_filter_tree) origin_tree_str = DebugString();
  SimplifyFilterTree(filter_tree_);
  PAX_LOG_IF(pax_log_filter_tree,
             "Origin filter tree: \n%s\nFinal filter tree: \n%s\n",
             origin_tree_str.c_str(), DebugString().c_str());
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessVecOpExpr(
    const arrow::compute::Expression &expr) {
  const arrow::compute::Expression::Call *call;
  arrow::compute::Expression leftop;
  arrow::compute::Expression rightop;
  const arrow::FieldRef *left_ref, *right_ref;
  const arrow::Datum *right_datum;
  AttrNumber lvarattno, rvarattno;
  Form_pg_attribute lattr, rattr;
  StrategyNumber strategy;
  std::string ignore_reason;
  std::shared_ptr<OpNode> op_node = nullptr;

  call = expr.call();

  Assert(call && (call->function_name == "greater" ||
                  call->function_name == "greater_equal" ||
                  call->function_name == "less" ||
                  call->function_name == "less_equal" ||
                  call->function_name == "equal"));

  if (call->arguments.size() != 2) {
    ignore_reason = pax::fmt(
        "Unexpected size of arguments in VecOpExpr [arguments=%ld], which must "
        "be 2.",
        call->arguments.size());
    goto ignore_clause;
  }

  leftop = call->arguments[0];
  rightop = call->arguments[1];

  strategy = GetStrategyByVecFuncName(call->function_name);
  if (strategy == InvalidStrategy) {
    ignore_reason = pax::fmt("Invalid strategy in VecOpExpr. [func_name=%s] ",
                             call->function_name.c_str());
    goto ignore_clause;
  }

  op_node = std::make_shared<OpNode>();
  op_node->strategy = strategy;
  op_node->collation = InvalidOid;  // place holder
  op_node->opno = InvalidOid;       // no need

  if ((leftop.field_ref() && rightop.literal()) ||
      (rightop.field_ref() && leftop.literal())) {
    std::shared_ptr<VarNode> var_node = nullptr;
    std::shared_ptr<ConstNode> const_node = nullptr;
    Assert(op_node);

    if (rightop.field_ref()) {
      arrow::compute::Expression tmp_clause;
      SWAP(tmp_clause, leftop, rightop);
    }

    left_ref = leftop.field_ref();
    right_datum = rightop.literal();

    // get the left lvarattno && lattr
    std::tie(lattr, lvarattno) = VecExprFlatVar(left_ref);

    if (!lattr || lvarattno <= 0 ||
        lvarattno > RelationGetNumberOfAttributes(rel_)) {
      ignore_reason = pax::fmt(
          "Invalid left Var in OpExpr, [varattno=%d, number_of_attrs=%d]",
          lvarattno, RelationGetNumberOfAttributes(rel_));
      goto ignore_clause;
    }

    // get the right datum
    Datum value;
    {
      if (!right_datum || !right_datum->is_scalar()) {
        ignore_reason = pax::fmt(
            "Unexpected right expr [desc=%s] in VecOpExpr which must be scalar "
            "datum.",
            right_datum->ToString().c_str());
        goto ignore_clause;
      }

      auto value_pair = ArrowScalarToDatum(right_datum->scalar(), lattr);
      if (!value_pair.second) {
        ignore_reason = pax::fmt("Unknown right expr [desc=%s] in VecOpExpr.",
                                 right_datum->ToString().c_str());
        goto ignore_clause;
      }
      value = value_pair.first;
    }

    op_node->collation = lattr->attcollation;
    op_node->left_typid = lattr->atttypid;
    // same as the left
    // If current case left have not same typid with the right
    // Arrow will add a cast node in left. which is different logic with pg
    // ex. v(date) < const(timestamp)
    // - pg won't add cast node
    // - arrow will add it
    //
    // So it's ok we direct use the left atttypid
    // because we will not do the cast in vec path
    op_node->right_typid = lattr->atttypid;

    var_node = std::make_shared<VarNode>();
    var_node->attrno = lvarattno;

    const_node = std::make_shared<ConstNode>();
    const_node->const_val = value;
    if (rightop.IsNullLiteral()) const_node->sk_flags |= SK_ISNULL;

    PFTNode::AppendSubNode(op_node, std::move(var_node));
    PFTNode::AppendSubNode(op_node, std::move(const_node));
  } else if ((leftop.field_ref() && rightop.field_ref())) {
    std::shared_ptr<VarNode> lvar_node = nullptr;
    std::shared_ptr<VarNode> rvar_node = nullptr;
    Assert(op_node);

    left_ref = leftop.field_ref();
    right_ref = rightop.field_ref();

    // get the left lvarattno && lattr
    std::tie(lattr, lvarattno) = VecExprFlatVar(left_ref);

    if (!lattr || lvarattno <= 0 ||
        lvarattno > RelationGetNumberOfAttributes(rel_)) {
      ignore_reason = pax::fmt(
          "Invalid left Var in OpExpr, [varattno=%d, number_of_attrs=%d]",
          lvarattno, RelationGetNumberOfAttributes(rel_));
      goto ignore_clause;
    }

    std::tie(rattr, rvarattno) = VecExprFlatVar(right_ref);

    if (!rattr || rvarattno <= 0 ||
        rvarattno > RelationGetNumberOfAttributes(rel_)) {
      ignore_reason = pax::fmt(
          "Invalid right Var in OpExpr, [varattno=%d, number_of_attrs=%d]",
          rvarattno, RelationGetNumberOfAttributes(rel_));
      goto ignore_clause;
    }

    if (lattr->attcollation != rattr->attcollation) {
      ignore_reason = pax::fmt(
          "left && right expr should use the same collation in VecOpExpr. "
          "[lcoll=%d, rcoll=%d]",
          lattr->attcollation, rattr->attcollation);
      goto ignore_clause;
    }

    op_node->left_typid = lattr->atttypid;
    op_node->right_typid = rattr->atttypid;
    op_node->collation = lattr->attcollation;

    lvar_node = std::make_shared<VarNode>();
    lvar_node->attrno = lvarattno;

    rvar_node = std::make_shared<VarNode>();
    rvar_node->attrno = rvarattno;

    PFTNode::AppendSubNode(op_node, std::move(lvar_node));
    PFTNode::AppendSubNode(op_node, std::move(rvar_node));
  } else {
    ignore_reason = "Unsupport OpExpr(maybe func(Var))";
    goto ignore_clause;
  }

  return op_node;

ignore_clause:
  DestroyNode(op_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessVecScalarArrayOpExpr(
    const arrow::compute::Expression &expr) {
  std::shared_ptr<InNode> in_node = nullptr;
  std::shared_ptr<VarNode> var_node = nullptr;
  std::shared_ptr<ConstNode> const_node = nullptr;
  std::string ignore_reason;
  size_t nargs;
  Form_pg_attribute attr = nullptr;
  std::shared_ptr<arrow::ArrayData> arrow_array;
  ArrayType *pg_array;
  AttrNumber varattno;
  auto call = expr.call();
  auto options = call->options;
  auto lookup_options =
      std::dynamic_pointer_cast<arrow::compute::SetLookupOptions>(options);
  if (!lookup_options) {
    ignore_reason =
        "Invalid options in VecScalarArray, can't cast it to the "
        "SetLookupOptions";
    goto ignore_clause;
  }

  nargs = call->arguments.size();
  if (nargs != 1) {
    ignore_reason = pax::fmt("Unexperted number of args [nargs= %ld]", nargs);
    goto ignore_clause;
  }

  // get the attr and index
  std::tie(attr, varattno) = VecExprFlatVar(call->arguments[0].field_ref());

  if (!attr || varattno <= 0 ||
      varattno > RelationGetNumberOfAttributes(rel_)) {
    ignore_reason = pax::fmt(
        "Invalid left Var in VecScalarArray, [varattno=%d, number_of_attrs=%d]",
        varattno, RelationGetNumberOfAttributes(rel_));
    goto ignore_clause;
  }

  // only filter if value_set is an array and skip nulls
  if (!lookup_options->value_set.is_array() || !lookup_options->skip_nulls ||
      lookup_options->value_set.length() < 1) {
    ignore_reason = "Invalid lookup_options in VecScalarArray Expression.";
    goto ignore_clause;
  }

  arrow_array = lookup_options->value_set.array();
  pg_array = InitializeArray(attr, arrow_array);
  if (!pg_array) {
    ignore_reason = "Failed to convert the arrow array to the PG array";
    goto ignore_clause;
  }

  var_node = std::make_shared<VarNode>();
  var_node->attrno = varattno;

  const_node = std::make_shared<ConstNode>();
  const_node->const_val = PointerGetDatum(pg_array);
  const_node->sk_flags = SK_SEARCHARRAY;
  const_node->const_type = InvalidOid;

  in_node = std::make_shared<InNode>();
  in_node->in = true;  // invert + in will be ignored
  PFTNode::AppendSubNode(in_node, std::move(var_node));
  PFTNode::AppendSubNode(in_node, std::move(const_node));

  return in_node;
ignore_clause:
  Assert(!var_node && !const_node && !in_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::ProcessVecNullTest(
    const arrow::compute::Expression &expr) {
  const arrow::compute::Expression::Call *call;
  std::shared_ptr<NullTestNode> nt_node = nullptr;
  std::shared_ptr<VarNode> var_node = nullptr;
  arrow::compute::Expression leftop;
  [[maybe_unused]] Form_pg_attribute attr;
  AttrNumber varattno; /* att number used in scan */

  std::string ignore_reason;

  call = expr.call();
  Assert(call && call->function_name == "is_null");

  if (call->arguments.size() != 1) {
    ignore_reason = pax::fmt(
        "Unexpected size of arguments [arguments=%ld], which must be 1.",
        call->arguments.size());
    goto ignore_clause;
  }

  leftop = call->arguments[0];

  if (!leftop.field_ref()) {
    ignore_reason = "Unexpected empty leftop in NullTest Expression";
    goto ignore_clause;
  }

  std::tie(attr, varattno) = VecExprFlatVar(leftop.field_ref());

  if (!attr || varattno <= 0 ||
      varattno > RelationGetNumberOfAttributes(rel_)) {
    ignore_reason = pax::fmt(
        "Invalid left Var in OpExpr, [varattno=%d, number_of_attrs=%d]",
        varattno, RelationGetNumberOfAttributes(rel_));
    goto ignore_clause;
  }

  var_node = std::make_shared<VarNode>();
  var_node->attrno = varattno;

  nt_node = std::make_shared<NullTestNode>();

  // not null will with a invert
  // it's ok we not deal it in this NullTestNode
  nt_node->sk_flags = SK_ISNULL | SK_SEARCHNULL;
  PFTNode::AppendSubNode(nt_node, std::move(var_node));

  return nt_node;

ignore_clause:
  Assert(!var_node && !nt_node);
  Assert(!ignore_reason.empty());
  return std::make_shared<UnsupportedNode>(ignore_reason);
}

std::shared_ptr<PFTNode> PaxSparseFilter::VecExprWalker(
    const arrow::compute::Expression &expr) {
  auto call = expr.call();
  // must be a call, or direct call the `Process*`
  if (!call) {
    goto ignore_clause;
  }

  // same as opexpr, but vectorization not support the `!=/<>` expr
  if (call->function_name == "greater" ||
      call->function_name == "greater_equal" || call->function_name == "less" ||
      call->function_name == "less_equal" || call->function_name == "equal") {
    return ProcessVecOpExpr(expr);
  } else if (call->function_name == "is_in") {
    return ProcessVecScalarArrayOpExpr(expr);
  } else if (call->function_name == "is_null") {
    return ProcessVecNullTest(expr);
  }

  // Below need call the `VecExprWalker`
  if (call->function_name == "and_kleene" ||
      call->function_name == "or_kleene" || call->function_name == "invert") {
    std::shared_ptr<PFTNode> bool_node = nullptr;
    if (call->function_name == "and_kleene") {
      bool_node = std::make_shared<AndNode>();
    } else if (call->function_name == "or_kleene") {
      bool_node = std::make_shared<OrNode>();
    } else if (call->function_name == "invert") {
      bool_node = std::make_shared<NotNode>();
    } else {
      Assert(false);
    }

    for (const auto &sub_expr : call->arguments) {
      std::shared_ptr<PFTNode> sub_node = nullptr;

      sub_node = VecExprWalker(sub_expr);
      PFTNode::AppendSubNode(bool_node, std::move(sub_node));
    }
    return bool_node;
  } else {
    goto ignore_clause;
  }

  return nullptr;

ignore_clause:
  // can't call the nodetostring in this time
  return std::make_shared<UnsupportedNode>("Unknown Expr");
}

}  // namespace pax
#endif
