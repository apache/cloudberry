//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformInnerJoinCommutativity.cpp
//
//	@doc:
//		Implementation of inner join commutativity transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerJoinCommutativity.h"

#include "gpos/base.h"

#include "gpopt/hints/CPlanHint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::CXformInnerJoinCommutativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoinCommutativity::CXformInnerJoinCommutativity(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
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
//		CXformInnerJoinCommutativity::FCompatible
//
//	@doc:
//		Compatibility function for join commutativity
//
//---------------------------------------------------------------------------
BOOL
CXformInnerJoinCommutativity::FCompatible(CXform::EXformId exfid)
{
	BOOL fCompatible = true;

	switch (exfid)
	{
		case CXform::ExfInnerJoinCommutativity:
			fCompatible = false;
			break;
		default:
			fCompatible = true;
	}

	return fCompatible;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoinCommutativity::Transform(CXformContext *pxfctxt,
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
	CExpression *pexprAlt = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprRight, pexprLeft, pexprScalar);

	// join order hints may specify that a specific relation be on the inner or
	// the outer side of the join. if such a hint exists, then skip adding join
	// commutativity alternatives.
	CPlanHint *planhint =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetPlanHint();
	if (nullptr != planhint && planhint->WasCreatedViaDirectedHint(pexprAlt))
	{
		pexprAlt->Release();
		return;
	}

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
