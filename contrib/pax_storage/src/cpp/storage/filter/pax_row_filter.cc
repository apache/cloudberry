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
 * pax_row_filter.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/filter/pax_row_filter.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/filter/pax_row_filter.h"
#include "comm/cbdb_wrappers.h"

namespace paxc {

static inline void FindAttrsInQual(Node *qual, bool *proj, int ncol,
                                   int *proj_atts, int *num_proj_atts) {
  int i, k;
  /* get attrs in qual */
  extractcolumns_from_node(qual, proj, ncol);

  /* collect the number of proj attr and attr_no from proj[] */
  k = 0;
  for (i = 0; i < ncol; i++) {
    if (proj[i]) proj_atts[k++] = i;
  }
  *num_proj_atts = k;
}

static bool BuildExecutionFilterForColumns(Relation rel, PlanState *ps,
                                    pax::ExecutionFilterContext *ctx) {
  List *qual = ps->plan->qual;
  List **qual_list;
  ListCell *lc;
  bool *proj;
  int *qual_atts;
  int natts = RelationGetNumberOfAttributes(rel);

  if (!qual || !IsA(qual, List)) return false;

  if (list_length(qual) == 1 && IsA(linitial(qual), BoolExpr)) {
    auto boolexpr = (BoolExpr *)linitial(qual);
    if (boolexpr->boolop != AND_EXPR) return false;
    qual = boolexpr->args;
  }
  Assert(IsA(qual, List));

  proj = (bool *)palloc(sizeof(bool) * natts);
  qual_atts = (int *)palloc(sizeof(int) * natts);
  qual_list = (List **)palloc0(sizeof(List *) * (natts + 1));

  ctx->econtext = ps->ps_ExprContext;
  ctx->estate_final = nullptr;
  ctx->estates = nullptr;
  ctx->attnos = nullptr;
  ctx->size = 0;

  foreach (lc, qual) {
    Expr *subexpr = (Expr *)lfirst(lc);
    int num_qual_atts = 0;
    int attno;

    Assert(subexpr);
    memset(proj, 0, sizeof(bool) * natts);
    FindAttrsInQual((Node *)subexpr, proj, natts, qual_atts, &num_qual_atts);
    if (num_qual_atts == 0 || num_qual_atts > 1) {
      qual_list[0] = lappend(qual_list[0], subexpr);
      continue;
    }
    attno = qual_atts[0] + 1;
    Assert(num_qual_atts == 1 && attno > 0 && attno <= natts);
    if (!qual_list[attno]) ctx->size++;
    qual_list[attno] = lappend(qual_list[attno], subexpr);
  }

  if (ctx->size > 0) {
    int k = 0;
    ctx->estates = (ExprState **)palloc(sizeof(ExprState *) * ctx->size);
    ctx->attnos = (AttrNumber *)palloc(sizeof(AttrNumber) * ctx->size);
    for (AttrNumber i = 1; i <= (AttrNumber)natts; i++) {
      if (!qual_list[i]) continue;
      ctx->estates[k] = ExecInitQual(qual_list[i], ps);
      ctx->attnos[k] = i;
      list_free(qual_list[i]);
      k++;
    }
    Assert(ctx->size == k);
  }
  if (qual_list[0]) {
    ctx->estate_final = ExecInitQual(qual_list[0], ps);
    list_free(qual_list[0]);
  }

  Assert(ctx->size > 0 || ctx->estate_final);
  ps->qual = nullptr;

  pfree(proj);
  pfree(qual_atts);
  pfree(qual_list);
  return true;
}

} // namespace paxc


namespace pax {

PaxRowFilter::PaxRowFilter() {}

bool PaxRowFilter::Initialize(Relation rel, PlanState *ps, const std::vector<bool> &projection) {
  bool ok = false;
  
  CBDB_WRAP_START;
  { 
    ok = paxc::BuildExecutionFilterForColumns(rel, ps, &efctx_); 
  }
  CBDB_WRAP_END;

  if (ok) {
    FillRemainingColumns(rel, projection);
  }

  return ok;
}

void PaxRowFilter::FillRemainingColumns(Relation rel, const std::vector<bool> &projection) {
  int natts = RelationGetNumberOfAttributes(rel);
  auto proj_len = projection.size();
  std::vector<bool> atts(natts);
  if (proj_len > 0) {
    Assert(natts >= 0 && static_cast<size_t>(natts) >= proj_len);
    for (size_t i = 0; i < proj_len; i++) atts[i] = projection[i];
    for (auto i = static_cast<int>(proj_len); i < natts; i++) atts[i] = false;
  } else {
    for (int i = 0; i < natts; i++) atts[i] = true;
  }
  // minus attnos in efctx_.attnos
  for (int i = 0; i < efctx_.size; i++) {
    auto attno = efctx_.attnos[i];
    Assert(attno > 0 && attno <= natts);
    atts[attno - 1] = false;
  }
  for (AttrNumber attno = 1; attno <= (AttrNumber)natts; attno++) {
    if (atts[attno - 1]) remaining_attnos_.emplace_back(attno);
  }
}


}  // namespace pax