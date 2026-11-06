//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApply2LeftSemiJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiApply2LeftSemiJoin.h"

#include "gpos/base.h"

#include "gpopt/search/CGroupProxy.h"


using namespace gpopt;

namespace
{
//---------------------------------------------------------------------------
//	@function:
//		FGroupHasWindowFunc
//
//	@doc:
//		Check if any group expression reachable from the given Memo group
//		contains a window function; each group is visited only once
//
//---------------------------------------------------------------------------
BOOL
FGroupHasWindowFunc(CGroup *pgroup, CBitSet *pbsVisited)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != pgroup);

	// ExchangeSet returns the previous value of the bit
	if (pbsVisited->ExchangeSet(pgroup->Id()))
		return false;

	CGroupProxy gp(pgroup);
	for (CGroupExpression *pgexpr = gp.PgexprFirst(); nullptr != pgexpr;
		 pgexpr = gp.PgexprNext(pgexpr))
	{
		if (COperator::EopScalarWindowFunc == pgexpr->Pop()->Eopid())
			return true;

		for (ULONG ul = 0; ul < pgexpr->Arity(); ul++)
			if (FGroupHasWindowFunc((*pgexpr)[ul], pbsVisited))
				return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		FInnerHasWindowFunc
//
//	@doc:
//		Check if the inner (subquery) child of an Apply contains a window
//		function
//
//---------------------------------------------------------------------------
BOOL
FInnerHasWindowFunc(CExpressionHandle &exprhdl)
{
	CGroupExpression *pgexpr = exprhdl.Pgexpr();
	if (nullptr == pgexpr)
		return false;

	CMemoryPool *mp = exprhdl.Pmp();
	CBitSet *pbsVisited = GPOS_NEW(mp) CBitSet(mp);
	BOOL fHasWindow = FGroupHasWindowFunc((*pgexpr)[1], pbsVisited);
	pbsVisited->Release();

	return fHasWindow;
}
}  // namespace


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApply2LeftSemiJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		outer references are not allowed
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiApply2LeftSemiJoin::Exfp(CExpressionHandle &exprhdl) const
{
	// if there are outer refs that include columns from the immediate outer child, the
	// transformation is applicable
	if (exprhdl.HasOuterRefs(1 /*child_index*/) &&
		!CUtils::FInnerUsesExternalColsOnly(exprhdl) &&
		!FInnerHasWindowFunc(exprhdl))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiApply2LeftSemiJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformLeftSemiApply2LeftSemiJoin::Transform(CXformContext *pxfctxt,
											CXformResult *pxfres,
											CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	Decorrelate(pxfctxt, pxfres, pexpr);
}


// EOF
