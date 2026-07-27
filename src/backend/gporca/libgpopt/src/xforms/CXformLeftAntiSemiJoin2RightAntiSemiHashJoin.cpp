//---------------------------------------------------------------------------
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CXformLeftAntiSemiJoin2RightAntiSemiHashJoin.cpp
//
//	@doc:
//		Implementation of transform: logical left anti semi join ->
//		physical right anti semi hash join (build = outer/left).
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2RightAntiSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalRightAntiSemiHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::
	CXformLeftAntiSemiJoin2RightAntiSemiHashJoin(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


CXform::EXformPromise
CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::Exfp(
	CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


void
CXformLeftAntiSemiJoin2RightAntiSemiHashJoin::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	// Do not build a right-anti-semi *hash* join for null-aware
	// (IS NOT DISTINCT FROM) join predicates.  Such predicates arise from
	// EXCEPT / NOT IN over all columns and can reference types that have a
	// hash operator family but no usable hash function (e.g. money ->
	// "could not identify a hash function for type money").  Forcing a hash
	// join then fails at plan time, whereas the regular left-anti path falls
	// back to a nested-loop anti join.  Let that safe fallback win here.
	CMemoryPool *mp = pxfctxt->Pmp();
	CExpressionArray *pdrgpexprConj =
		CPredicateUtils::PdrgpexprConjuncts(mp, (*pexpr)[2]);
	BOOL fNullAware = false;
	for (ULONG ul = 0; ul < pdrgpexprConj->Size(); ul++)
	{
		if (CPredicateUtils::FINDF((*pdrgpexprConj)[ul]))
		{
			fNullAware = true;
			break;
		}
	}
	pdrgpexprConj->Release();
	if (fNullAware)
	{
		return;
	}

	CXformUtils::ImplementHashJoin<CPhysicalRightAntiSemiHashJoin>(
		pxfctxt, pxfres, pexpr);

	// NB: deliberately do NOT retry via FProcessGPDBAntiSemiHashJoin (unlike
	// CXformLeftAntiSemiJoin2HashJoin).  That retry strips the GPDB-style
	// IsNotFalse wrapper so a hash join can be built for anti joins whose
	// predicate is "NOT (a IS DISTINCT FROM b)" (e.g. EXCEPT / NOT IN).  For
	// those, building a right-anti *hash* join forces hashing the join keys,
	// which fails at plan time for types without a hash opclass (e.g. money:
	// "could not identify a hash function for type money").  The Postgres
	// planner and ORCA's existing left-anti path fall back to a nested-loop
	// anti join for such predicates, so right-anti simply declines here and
	// lets that safe fallback win; it still applies to anti joins whose
	// predicate yields hashable keys directly (the common NOT EXISTS case).
}


// EOF
