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
 *  @filename:
 * 		CXformSplitWindowFunc.cpp
 * 
 *  @doc:
 * 		Implementation of the splitting of window function
 * 
 *-------------------------------------------------------------------------
 */
#include "gpopt/xforms/CXformSplitWindowFunc.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarWindowFunc.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitWindowFunc::CXformSplitWindowFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitWindowFunc::CXformSplitWindowFunc(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalSelect(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalSequenceProject(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
				  GPOS_NEW(mp) CExpression(
					  mp,
					  GPOS_NEW(mp) CPatternTree(mp))),	// scalar project list
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))))	// scalar project list
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitWindowFunc::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitWindowFunc::Exfp(CExpressionHandle & /*exprhdl*/) const
{
	// Current promise can't be measured in `Exfp`
	// Cause current root operator is the `CLogicalSelect`
	// But we can measure it in the `Transform`
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitWindowFunc::PopulateLocalGlobalProjectList
//
//	@doc:
//		Populate the local or global project list from the input project list
//
//---------------------------------------------------------------------------
void
CXformSplitWindowFunc::PopulateLocalGlobalProjectList(
	CMemoryPool *mp, CExpression *pexprProjList,
	CExpression **ppexprProjListLocal, CExpression **ppexprProjListGlobal)
{
	// list of project elements for the new local and global windows agg
	CExpressionArray *pdrgpexprProjElemLocal =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprProjElemGlobal =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprProjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprProgElem = (*pexprProjList)[ul];
		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprProgElem->Pop());
		GPOS_ASSERT(popScPrEl);

		// get the scalar window func
		CExpression *pexprWinFunc = (*pexprProgElem)[0];
		CScalarWindowFunc *popScWinFunc =
			CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());
		GPOS_ASSERT(popScWinFunc);

		// add ref for the pop local window function
		popScWinFunc->FuncMdId()->AddRef();
		popScWinFunc->MdidType()->AddRef();

		CScalarWindowFunc *popScWinFuncLocal = GPOS_NEW(mp) CScalarWindowFunc(
			mp, popScWinFunc->FuncMdId(), popScWinFunc->MdidType(),
			GPOS_NEW(mp)
				CWStringConst(mp, popScWinFunc->PstrFunc()->GetBuffer()),
			popScWinFunc->Ews(), popScWinFunc->IsDistinct(),
			popScWinFunc->IsStarArg(), popScWinFunc->IsSimpleAgg());

		// add ref for the pop global window function
		popScWinFunc->FuncMdId()->AddRef();
		popScWinFunc->MdidType()->AddRef();
		CScalarWindowFunc *popScWinFuncGlobal = GPOS_NEW(mp) CScalarWindowFunc(
			mp, popScWinFunc->FuncMdId(), popScWinFunc->MdidType(),
			GPOS_NEW(mp)
				CWStringConst(mp, popScWinFunc->PstrFunc()->GetBuffer()),
			popScWinFunc->Ews(), popScWinFunc->IsDistinct(),
			popScWinFunc->IsStarArg(), popScWinFunc->IsSimpleAgg());


		CExpressionArray *pdrgpexprWin = pexprWinFunc->PdrgPexpr();

		// create a new local window function
		// create array of arguments for the window function
		pdrgpexprWin->AddRef();
		CExpression *pexprWinFuncLocal =
			GPOS_NEW(mp) CExpression(mp, popScWinFuncLocal, pdrgpexprWin);

		// create a new global window function
		pdrgpexprWin->AddRef();
		CExpression *pexprWinFuncGlobal =
			GPOS_NEW(mp) CExpression(mp, popScWinFuncGlobal, pdrgpexprWin);

		// create new project elements for the window functions
		CExpression *pexprProjElemLocal = CUtils::PexprScalarProjectElement(
			mp, popScPrEl->Pcr(), pexprWinFuncLocal);

		CExpression *pexprProjElemGlobal = CUtils::PexprScalarProjectElement(
			mp, popScPrEl->Pcr(), pexprWinFuncGlobal);

		pdrgpexprProjElemLocal->Append(pexprProjElemLocal);
		pdrgpexprProjElemGlobal->Append(pexprProjElemGlobal);
	}

	// create new project lists
	*ppexprProjListLocal = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprProjElemLocal);

	*ppexprProjListGlobal = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprProjElemGlobal);
}

void
CXformSplitWindowFunc::PopulateSelect(CMemoryPool *mp, CExpression *pexpr,
									  COperator **ppSelectCopy)
{
	CLogicalSelect *pexprSelect = CLogicalSelect::PopConvert(pexpr->Pop());
	CTableDescriptor *tblDesc = pexprSelect->Ptabdesc();
	if (tblDesc)
		tblDesc->AddRef();

	*ppSelectCopy = GPOS_NEW(mp) CLogicalSelect(mp, tblDesc);
}

bool
CXformSplitWindowFunc::CheckFilterAndProjectList(CExpression *pexprScalarCmp,
												 CExpression *pexprProjList)
{
	CScalarCmp *pexprCmp;
	CScalarIdent *pexprId;
	IMDType::ECmpType pexprCmpType;

	if (pexprScalarCmp->Pop()->Eopid() != CScalarCmp::EopScalarCmp)
	{
		return false;
	}

	pexprCmp = CScalarCmp::PopConvert(pexprScalarCmp->Pop());
	pexprCmpType = pexprCmp->ParseCmpType();

	if (!(pexprCmpType == IMDType::EcmptL || pexprCmpType == IMDType::EcmptLEq))
	{
		return false;
	}

	if ((*pexprScalarCmp)[0]->Pop()->Eopid() != CScalarIdent::EopScalarIdent)
	{
		return false;
	}

	pexprId = CScalarIdent::PopConvert((*pexprScalarCmp)[0]->Pop());

	auto windowOids =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetWindowOids();

	const ULONG arity = pexprProjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprProgElem = (*pexprProjList)[ul];
		if (pexprProgElem->Pop()->Eopid() !=
			CScalarProjectElement::EopScalarProjectElement)
		{
			return false;
		}

		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprProgElem->Pop());

		if (popScPrEl->Pcr() != pexprId->Pcr())
		{
			return false;
		}

		CExpression *pexprWinFunc = (*pexprProgElem)[0];
		if (pexprWinFunc->Pop()->Eopid() !=
			CScalarWindowFunc::EopScalarWindowFunc)
		{
			return false;
		}

		CScalarWindowFunc *popScWinFunc =
			CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());

		if (!(IMDId::MDIdCompare(popScWinFunc->FuncMdId(),
								 windowOids->MDIdRowNumber()) ||
			  IMDId::MDIdCompare(popScWinFunc->FuncMdId(),
								 windowOids->MDIdRank()) ||
			  IMDId::MDIdCompare(popScWinFunc->FuncMdId(),
								 windowOids->MDIdDenseRank())))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitWindowFunc::Transform
//
//	@doc:
//		Actual transformation to expand a global window agg into a pair of
//		local and global window agg
//
//		Input:
//		 +--CLogicalSelect (Global(one-step))
//		 |--CLogicalSequenceProject
//		 |  |-- ANY(*)
//		 |  +--CScalarProjectList
//		 +--CScalarCmp
//
//		Output:
//		 +--CLogicalSelect
//		 |--CLogicalSequenceProject (Global(two-step))
//		 |  |--CLogicalSelect
//		 |  |  |--CLogicalSequenceProject (Local)
//		 |  |  |  |-- ANY(*)
//		 |  |  |  +--CScalarProjectList
//		 |  |  +--CScalarCmp
//		 |  +--CScalarProjectList
//		 +--CScalarCmp
//
//---------------------------------------------------------------------------
void
CXformSplitWindowFunc::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(nullptr != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprSequenceProject = (*pexpr)[0];
	CExpression *pexprScalarCmp = (*pexpr)[1];	// scalar filter

	// extend logic from `Exfp()`
	{
		// not the SequenceProject below Select
		if (pexprSequenceProject->Pop()->Eopid() !=
				COperator::EopLogicalSequenceProject ||
			pexprScalarCmp->Pop()->Eopid() != COperator::EopScalarCmp ||
			pexprSequenceProject->Arity() != 2)
		{
			return;
		}


		// make sure `SequenceProject` can be Transform
		CExpressionHandle exprhdl(mp);
		exprhdl.Attach(pexprSequenceProject);

		if (CLogicalSequenceProject::PopConvert(pexprSequenceProject->Pop())
					->Pspt() ==
				COperator::ESPType::EsptypeLocal || /* split global only */
			0 < exprhdl.DeriveOuterReferences()->Size() ||
			nullptr == exprhdl.PexprScalarExactChild(1) ||
			CXformUtils::FHasAmbiguousType(
				exprhdl.PexprScalarExactChild(1 /*child_index*/),
				COptCtxt::PoctxtFromTLS()->Pmda()))
		{
			return;
		}
	}

	COperator *pSelectCopy = nullptr;

	CLogicalSequenceProject *popWinFunc =
		CLogicalSequenceProject::PopConvert(pexprSequenceProject->Pop());

	// extract components
	CExpression *pexprRelational = (*pexprSequenceProject)[0];
	CExpression *pexprProjectList = (*pexprSequenceProject)[1];

	CExpression *pexprProjectListLocal = nullptr;
	CExpression *pexprProjectListGlobal = nullptr;

	if (!CheckFilterAndProjectList(pexprScalarCmp, pexprProjectList))
	{
		return;
	}

	// copy the LogicalSelect
	(void) PopulateSelect(mp, pexpr, &pSelectCopy);
	GPOS_ASSERT(pSelectCopy);

	(void) PopulateLocalGlobalProjectList(
		mp, pexprProjectList, &pexprProjectListLocal, &pexprProjectListGlobal);

	GPOS_ASSERT(nullptr != pexprProjectListLocal &&
				nullptr != pexprProjectListLocal);

	CDistributionSpec *pds = popWinFunc->Pds();
	pds->AddRef();

	COrderSpecArray *pdrgpos = popWinFunc->Pdrgpos();
	pdrgpos->AddRef();

	CWindowFrameArray *pdrgpwf = popWinFunc->Pdrgpwf();
	pdrgpwf->AddRef();

	pexprRelational->AddRef();

	CExpression *pexprLocal = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalSequenceProject(
			mp, COperator::ESPType::EsptypeLocal, pds, pdrgpos, pdrgpwf),
		pexprRelational, pexprProjectListLocal);

	pexprScalarCmp->AddRef();

	CExpression *pexprLocalSelect =
		GPOS_NEW(mp) CExpression(mp, pSelectCopy, pexprLocal, pexprScalarCmp);

	CExpression *pexprGlobal = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CLogicalSequenceProject(
						mp, COperator::ESPType::EsptypeGlobalTwoStep, pds,
						pdrgpos, pdrgpwf),
					pexprLocalSelect, pexprProjectListGlobal);

	CExpression *pexprGlobalSelect =
		GPOS_NEW(mp) CExpression(mp, pexpr->Pop(), pexprGlobal, pexprScalarCmp);

	pxfres->Add(pexprGlobalSelect);

	return;
}

// EOF
