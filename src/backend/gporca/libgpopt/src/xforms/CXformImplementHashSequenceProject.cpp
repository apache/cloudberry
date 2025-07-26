//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementHashSequenceProject.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

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
CXformImplementHashSequenceProject::CXformImplementHashSequenceProject(CMemoryPool *mp)
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
