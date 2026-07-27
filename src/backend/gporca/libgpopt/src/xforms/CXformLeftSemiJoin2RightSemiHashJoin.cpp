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
//		CXformLeftSemiJoin2RightSemiHashJoin.cpp
//
//	@doc:
//		Implementation of transform: logical left semi join -> physical
//		right semi hash join (build = outer/left, probe = inner/right).
//		Generates the RIGHT candidate; cost model (M3) picks vs LEFT.
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiJoin2RightSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalRightSemiHashJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::CXformLeftSemiJoin2RightSemiHashJoin
//
//	@doc:
//		ctor -- pattern identical to LeftSemi xform (same logical pattern)
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2RightSemiHashJoin::CXformLeftSemiJoin2RightSemiHashJoin(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::Exfp
//
//	@doc:
//		Compute xform promise. Generate RIGHT_SEMI candidate whenever the
//		expression can be implemented as a hash join. Cost model decides
//		LEFT vs RIGHT -- no GUC, no heuristic in xform layer.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2RightSemiHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2RightSemiHashJoin::Transform
//
//	@doc:
//		Actual transformation: instantiate CPhysicalRightSemiHashJoin with
//		children in original [outer, inner] order. The RIGHT operator's
//		Eopid encodes the build/probe flip.
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2RightSemiHashJoin::Transform(CXformContext *pxfctxt,
												CXformResult *pxfres,
												CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	// Do not build a right-semi *hash* join for null-aware (IS NOT DISTINCT
	// FROM) join predicates; such predicates can reference types that have a
	// hash operator family but no usable hash function (e.g. money), which
	// fails at plan time.  The regular left-semi path falls back safely.
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

	CXformUtils::ImplementHashJoin<CPhysicalRightSemiHashJoin>(pxfctxt, pxfres,
															   pexpr);
}


// EOF
