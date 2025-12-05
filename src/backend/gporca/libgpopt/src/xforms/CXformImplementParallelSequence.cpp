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
 * CXformImplementParallelSequence.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementParallelSequence.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementParallelSequence.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalSequence.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPhysicalParallelSequence.h"

using namespace gpopt;
namespace gpdb {
bool IsParallelModeOK(void);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequence::CXformImplementParallelSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementParallelSequence::CXformImplementParallelSequence(CMemoryPool *mp)
	: CXformImplementation(
	// pattern
	GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalSequence(mp),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEProducer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementParallelSequence::Exfp(CExpressionHandle &  // exprhdl
) const
{
	// Check if parallel plans are enabled in context and parallel processing in safe
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelSequence::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementParallelSequence::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpressionArray *pdrgpexpr = pexpr->PdrgPexpr();
	pdrgpexpr->AddRef();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CPhysicalParallelSequence(mp, 2), pdrgpexpr);
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


//	EOF
