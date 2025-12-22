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
 * CXformImplementParallelCTEConsumer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementParallelCTEConsumer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementParallelCTEConsumer.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalParallelCTEConsumer.h"

using namespace gpopt;
namespace gpdb {
bool IsParallelModeOK(void);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEConsumer::CXformImplementParallelCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementParallelCTEConsumer::CXformImplementParallelCTEConsumer(CMemoryPool *mp)
	: CXformImplementation(
	// pattern
	GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp)))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementParallelCTEConsumer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementParallelCTEConsumer::Exfp(CExpressionHandle &  // exprhdl
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
//		CXformImplementParallelCTEConsumer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementParallelCTEConsumer::Transform(CXformContext *pxfctxt,
											  CXformResult *pxfres,
											  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalCTEConsumer *popCTEConsumer =
		CLogicalCTEConsumer::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components for alternative
	ULONG id = popCTEConsumer->UlCTEId();

	CColRefArray *colref_array = popCTEConsumer->Pdrgpcr();
	colref_array->AddRef();

	UlongToColRefMap *colref_mapping = popCTEConsumer->Phmulcr();
	GPOS_ASSERT(nullptr != colref_mapping);
	colref_mapping->AddRef();

	// create physical CTE Consumer
	CExpression *pexprAlt =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalParallelCTEConsumer(
			mp, id, colref_array, colref_mapping, 2));

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF