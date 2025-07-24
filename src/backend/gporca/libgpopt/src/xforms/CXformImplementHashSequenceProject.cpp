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
 * CXformImplementHashSequenceProject.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformImplementHashSequenceProject.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformImplementHashSequenceProject.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalHashSequenceProject.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementHashSequenceProject::CXformImplementHashSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementHashSequenceProject::CXformImplementHashSequenceProject(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSequenceProject(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // scalar child
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementHashSequenceProject::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementHashSequenceProject::Transform(CXformContext *pxfctxt,
											  CXformResult *pxfres,
											  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// extract members of logical sequence project operator
	CLogicalSequenceProject *popLogicalSequenceProject =
		CLogicalSequenceProject::PopConvert(pexpr->Pop());
	COperator::ESPType sptype = popLogicalSequenceProject->Pspt();
	CDistributionSpec *pds = popLogicalSequenceProject->Pds();
	COrderSpecArray *pdrgpos = popLogicalSequenceProject->Pdrgpos();
	CWindowFrameArray *pdrgpwf = popLogicalSequenceProject->Pdrgpwf();
	pds->AddRef();
	pdrgpos->AddRef();
	pdrgpwf->AddRef();

	// assemble physical operator
	CExpression *pexprSequenceProject = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CPhysicalHashSequenceProject(mp, sptype, pds, pdrgpos, pdrgpwf),
		pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprSequenceProject);
}


// EOF
