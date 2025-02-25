//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformFullJoinCommutativity.cpp
//
//	@doc:
//		Implementation of full join commutativity transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformFullJoinCommutativity.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalFullOuterJoin.h"
#include "gpopt/operators/CPatternLeaf.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformFullJoinCommutativity::CXformFullJoinCommutativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFullJoinCommutativity::CXformFullJoinCommutativity(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalFullOuterJoin(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))  // predicate
	  )
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFullJoinCommutativity::FCompatible
//
//	@doc:
//		Compatibility function for join commutativity
//
//---------------------------------------------------------------------------
BOOL
CXformFullJoinCommutativity::FCompatible(CXform::EXformId exfid)
{
	BOOL fCompatible = true;

	switch (exfid)
	{
		case CXform::ExfFullJoinCommutativity:
			fCompatible = false;
			break;
		default:
			fCompatible = true;
	}

	return fCompatible;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFullJoinCommutativity::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformFullJoinCommutativity::Transform(CXformContext *pxfctxt,
									   CXformResult *pxfres,
									   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// addref children
	pexprLeft->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();

	// assemble transformed expression
	CExpression *pexprAlt = CUtils::PexprLogicalJoin<CLogicalFullOuterJoin>(
		mp, pexprRight, pexprLeft, pexprScalar);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
