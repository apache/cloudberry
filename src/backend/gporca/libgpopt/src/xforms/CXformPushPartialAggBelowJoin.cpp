//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushPartialAggBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformPushPartialAggBelowJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

// ctor
CXformPushPartialAggBelowJoin::CXformPushPartialAggBelowJoin(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),  // global-stage agg
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),  // local-stage agg
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join outer child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join inner child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
				  ),
			  GPOS_NEW(mp) CExpression(
				  mp,
				  GPOS_NEW(mp) CPatternTree(mp))  // local scalar project list
			  ),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternTree(mp))  // global scalar project list
		  ))
{
}

// ctor
CXformPushPartialAggBelowJoin::CXformPushPartialAggBelowJoin(
	CExpression *pexprPattern)
	: CXformExploration(pexprPattern)
{
}


BOOL
CXformPushPartialAggBelowJoin::FLocalGbAggAlreadyPushed(
	CExpression *pexprGlobalGb)
{
	CExpression *pexprLocalGb = (*pexprGlobalGb)[0];
	CExpression *pexprJoin = (*pexprLocalGb)[0];
	CExpression *pexprOuter = (*pexprJoin)[0];
	return pexprOuter->Pop()->Eopid() == COperator::EopLogicalGbAgg &&
		   CLogicalGbAgg::PopConvert(pexprOuter->Pop())->Egbaggtype() ==
			   COperator::EgbaggtypeLocal;
}


// compute xform promise for a given expression handle
CXform::EXformPromise
CXformPushPartialAggBelowJoin::Exfp(CExpressionHandle &exprhdl) const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popGbAgg->FGlobal())  // multi-stage-but-local-agg
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

CColRefSet *
CXformPushPartialAggBelowJoin::PexprGetGbAggkey(CMemoryPool *mp,
												CExpression *pexprGbAgg)
{
	// get the group by key set
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGbAgg->Pop());

	CColRefSet *pcrsGbKey = GPOS_NEW(mp) CColRefSet(mp);
	CColRefArray *colref_array = popGbAgg->PdrgpcrMinimal();
	if (nullptr == colref_array)
	{
		colref_array = popGbAgg->Pdrgpcr();
	}
	pcrsGbKey->Include(colref_array);
	return pcrsGbKey;
}

// always get the outer join key
CColRefSet *
CXformPushPartialAggBelowJoin::PcrsJoinKey(CMemoryPool *mp,
										   CExpression *pexprOuter,
										   CExpression *pexprInner,
										   CExpression *pexprScalar)
{
	CColRefSet *prcsOuterOutput = pexprOuter->DeriveOutputColumns();
	CColRefSet *prcsInnerOutput = pexprInner->DeriveOutputColumns();
	CColRefSet *pcrsFKey = GPOS_NEW(mp) CColRefSet(mp);

	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	const ULONG ulConjuncts = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		CExpression *pexprConjunct = (*pdrgpexpr)[ul];
		if (!CPredicateUtils::FPlainEquality(pexprConjunct))
		{
			continue;
		}

		CColRef *pcrFst = const_cast<CColRef *>(
			CScalarIdent::PopConvert((*pexprConjunct)[0]->Pop())->Pcr());
		CColRef *pcrSnd = const_cast<CColRef *>(
			CScalarIdent::PopConvert((*pexprConjunct)[1]->Pop())->Pcr());
		if (prcsOuterOutput->FMember(pcrFst) &&
			prcsInnerOutput->FMember(pcrSnd))
		{
			pcrsFKey->Include(pcrFst);
		}
		else if (prcsOuterOutput->FMember(pcrSnd) &&
				 prcsInnerOutput->FMember(pcrFst))
		{
			pcrsFKey->Include(pcrSnd);
		}
	}

	pdrgpexpr->Release();
	if (pcrsFKey->Size() == 0)
	{
		pcrsFKey->Release();
		pcrsFKey = nullptr;
	}

	return pcrsFKey;
}


// Different with CXformUtils::PexprPushGbBelowJoin(will pushdown the one-stage agg)
// Push the partial agg only follow the three rules:
//  1. The keyset of group by in(contains all) the keyset of join
//  2. The output from join outer child contains the scalar from groupby
//    - the scalar from groupby is not the groupby key, but it's the output of agg
//  3. The output of group by contains(all) the join scalar used from outer child
//
// Partial agg cannot be pushed through join because
// (1) no group by keysets in(contains all) join keysets, or
// (3) Gb uses columns from both join children, or
// (4) Gb hides columns required for the join scalar child
//
// not like the CXformUtils::PexprPushGbBelowJoin, pushdown a partial agg **NO REQUIRED**
// the unique key in the key of join inner child.
//
// Also we should **NEVER** pushdown the multi-stage agg from `CXformSplitDQA`(the DISTINCT case).
//
std::pair<BOOL, BOOL>
CXformPushPartialAggBelowJoin::FCanPushLocalGbAggBelowJoin(
	CMemoryPool *mp, CExpression *pexprGlobalGb)
{
	// safe to direct extract the child expressions
	CExpression *pexprLocalGb = (*pexprGlobalGb)[0];

	CExpression *pexprJoin = (*pexprLocalGb)[0];
	CExpression *pexprLocalPrjList = (*pexprLocalGb)[1];

	CExpression *pexprOuter = (*pexprJoin)[0];
	CExpression *pexprInner = (*pexprJoin)[1];
	CExpression *pexprScalar = (*pexprJoin)[2];

	// prepare the args for the `FCanPushGbAggBelowJoin`
	CColRefSet *pcrsJoinOuterChildOutput = pexprOuter->DeriveOutputColumns();
	CColRefSet *pcrsGrpByUsed = pexprLocalPrjList->DeriveUsedColumns();
	CColRefSet *pcrsGrpByOutput = pexprLocalGb->DeriveOutputColumns();

	BOOL fCanPush;

	// current agg form DQA, can't be pushed, otherwise the result may be wrong
	if (!pexprLocalPrjList->PdrgPexpr() ||
		pexprLocalPrjList->PdrgPexpr()->Size() == 0 ||
		CLogicalGbAgg::PopConvert(pexprLocalGb->Pop())->PdrgpcrArgDQA() !=
			nullptr)
	{
		return std::make_pair(false, true);
	}

	// get the group by key set
	CColRefSet *pcrsGbKey = PexprGetGbAggkey(mp, pexprLocalGb);

	// get the keyset group by key in join key
	CColRefSet *pcrsGbInJoinKey =
		PcrsJoinKey(mp, pexprOuter, pexprInner, pexprScalar);

	// get the join scalar used from outer child
	CColRefSet *pcrsJoinScalarUsedFromOuter =
		GPOS_NEW(mp) CColRefSet(mp, *(pexprScalar->DeriveUsedColumns()));
	pcrsJoinScalarUsedFromOuter->Intersection(pcrsJoinOuterChildOutput);

	// check can we pushdown the local agg
	fCanPush = CXformUtils::FCanPushGbAggBelowJoin(
		pcrsGbKey, pcrsJoinOuterChildOutput, pcrsJoinScalarUsedFromOuter,
		pcrsGrpByOutput, pcrsGrpByUsed, pcrsGbInJoinKey);

	pcrsJoinScalarUsedFromOuter->Release();
	pcrsGbKey->Release();
	CRefCount::SafeRelease(pcrsGbInJoinKey);
	return std::make_pair(fCanPush, false);
}

CExpression *
CXformPushPartialAggBelowJoin::PushLocalGbAggBelowJoin(
	CMemoryPool *mp, CExpression *pexprGlobalGb)
{
	CExpression *pexprLocalGb = (*pexprGlobalGb)[0];
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprLocalGb->Pop());
	CExpression *pexprGlobalPrjList = (*pexprGlobalGb)[1];

	CExpression *pexprJoin = (*pexprLocalGb)[0];
	CExpression *pexprLocalPrjList = (*pexprLocalGb)[1];

	CExpression *pexprOuter = (*pexprJoin)[0];
	CExpression *pexprInner = (*pexprJoin)[1];
	CExpression *pexprScalar = (*pexprJoin)[2];

	// get the group by key set
	CColRefSet *pcrsGbKey = PexprGetGbAggkey(mp, pexprLocalGb);

	CColRefSet *pcrsJoinOuterChildOutput = pexprOuter->DeriveOutputColumns();
	CLogicalGbAgg *popGbAggNew;

	if (!pcrsJoinOuterChildOutput->ContainsAll(pcrsGbKey))
	{
		CColRefSet *pcrsGrpColsNew = GPOS_NEW(mp) CColRefSet(mp);
		pcrsGrpColsNew->Include(pcrsGbKey);
		pcrsGrpColsNew->Intersection(pcrsJoinOuterChildOutput);

		popGbAggNew = GPOS_NEW(mp)
			CLogicalGbAgg(mp, pcrsGrpColsNew->Pdrgpcr(mp),
						  popGbAgg->Egbaggtype(), popGbAgg->AggStage());

		pcrsGrpColsNew->Release();
	}
	else
	{
		popGbAgg->AddRef();
		popGbAggNew = popGbAgg;
	}

	pcrsGbKey->Release();

	// recreate a local agg below join, above the join outer expression
	pexprOuter->AddRef();
	pexprLocalPrjList->AddRef();

	popGbAggNew->MarkAggPushdown();
	CExpression *pexprNewGb = GPOS_NEW(mp)
		CExpression(mp, popGbAggNew, pexprOuter, pexprLocalPrjList);

	// recreate the join which outer is the local agg
	COperator *popJoin = pexprJoin->Pop();
	popJoin->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprNewJoin = GPOS_NEW(mp)
		CExpression(mp, popJoin, pexprNewGb, pexprInner, pexprScalar);

	// recreate the global agg below the join
	CLogicalGbAgg *popGlobalGb =
		CLogicalGbAgg::PopConvert(pexprGlobalGb->Pop());
	popGlobalGb->Pdrgpcr()->AddRef();
	CLogicalGbAgg *popGlobalGbNew = GPOS_NEW(mp)
		CLogicalGbAgg(mp, popGlobalGb->Pdrgpcr(), popGlobalGb->Egbaggtype(),
					  popGlobalGb->AggStage());
	popGlobalGbNew->MarkAggPushdown();

	pexprGlobalPrjList->AddRef();
	return GPOS_NEW(mp)
		CExpression(mp, popGlobalGbNew, pexprNewJoin, pexprGlobalPrjList);
}


// Used to exchange the group by key with join key
// Current groupby should not equal with outer join key but equal with inner join key.
// Also should not be the multi groupby key.
// Then exchange the local agg group by key to the outer join key
CExpression *
CXformPushPartialAggBelowJoin::ExchangeGbkeyFromJoinKey(
	CMemoryPool *mp, CExpression *pexprGlobalGb)
{
	CExpression *pexprLocalGb = (*pexprGlobalGb)[0];
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprLocalGb->Pop());
	CExpression *pexprGlobalPrjList = (*pexprGlobalGb)[1];

	CExpression *pexprJoin = (*pexprLocalGb)[0];
	CExpression *pexprLocalPrjList = (*pexprLocalGb)[1];

	CExpression *pexprOuter = (*pexprJoin)[0];
	CExpression *pexprInner = (*pexprJoin)[1];
	CExpression *pexprScalar = (*pexprJoin)[2];

	CExpression *pexprNew = nullptr;

	// get the group by key set
	CColRefSet *pcrsGbKey = PexprGetGbAggkey(mp, pexprLocalGb);
	CColRefSet *pcrsGbInJoinKey =
		PcrsJoinKey(mp, pexprOuter, pexprInner, pexprScalar);

	if (pcrsGbKey->Size() == 1 && pcrsGbInJoinKey &&
		pcrsGbInJoinKey->Size() == 1)
	{
		CColRefSet *pcrsJoinInnerKey =
			PcrsJoinKey(mp, pexprInner, pexprOuter, pexprScalar);

		// current group by not in outer and inner join key
		if (!pcrsJoinInnerKey || !pcrsGbKey->ContainsAll(pcrsJoinInnerKey))
		{
			pcrsGbKey->Release();
			CRefCount::SafeRelease(pcrsJoinInnerKey);
			CRefCount::SafeRelease(pcrsGbInJoinKey);
			return nullptr;
		}
		else
		{
			CRefCount::SafeRelease(pcrsJoinInnerKey);
		}

		CColRefArray *pcraGbInJoinKey = pcrsGbInJoinKey->Pdrgpcr(mp);

		// change group by to the join key(another side)mak
		CLogicalGbAgg *popGbAggNew = GPOS_NEW(mp) CLogicalGbAgg(
			mp, pcraGbInJoinKey, popGbAgg->Egbaggtype(), popGbAgg->AggStage());

		pexprJoin->AddRef();
		pexprLocalPrjList->AddRef();

		CExpression *pexprNewLocalGb = GPOS_NEW(mp)
			CExpression(mp, popGbAggNew, pexprJoin, pexprLocalPrjList);

		// recreate the global agg also exchange the group by key
		CLogicalGbAgg *popGlobalGb =
			CLogicalGbAgg::PopConvert(pexprGlobalGb->Pop());
		popGlobalGb->Pdrgpcr()->AddRef();
		COperator *popGlobalGbNew = GPOS_NEW(mp)
			CLogicalGbAgg(mp, popGlobalGb->Pdrgpcr(), popGlobalGb->Egbaggtype(),
						  popGlobalGb->AggStage());

		pexprGlobalPrjList->AddRef();
		pexprNew = GPOS_NEW(mp) CExpression(mp, popGlobalGbNew, pexprNewLocalGb,
											pexprGlobalPrjList);
	}

	pcrsGbKey->Release();
	CRefCount::SafeRelease(pcrsGbInJoinKey);

	return pexprNew;
}

// actual transform
void
CXformPushPartialAggBelowJoin::Transform(CXformContext *pxfctxt,
										 CXformResult *pxfres,
										 CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	BOOL fCanPush;
	BOOL fFailDueDQA;
	CMemoryPool *mp = pxfctxt->Pmp();

	// check current agg already been pushdowned
	if (FLocalGbAggAlreadyPushed(pexpr))
	{
		return;
	}

	CExpression *pexprResult = nullptr;
	std::tie(fCanPush, fFailDueDQA) = FCanPushLocalGbAggBelowJoin(mp, pexpr);

	// check the expression can do the pushdown.
	if (fCanPush)
	{
		// do the pushdown
		pexprResult = PushLocalGbAggBelowJoin(mp, pexpr);
		GPOS_ASSERT(pexprResult);
	}
	else if (!fFailDueDQA)	// no DQA, we can exchange the gb key and try again
	{
		// exchange the groupby by key
		CExpression *pexprNew = ExchangeGbkeyFromJoinKey(mp, pexpr);
		if (pexprNew)
		{
			std::tie(fCanPush, fFailDueDQA) =
				FCanPushLocalGbAggBelowJoin(mp, pexprNew);

			// should not fail due the DQA
			GPOS_ASSERT(!fFailDueDQA);
			if (fCanPush)
			{
				pexprResult = PushLocalGbAggBelowJoin(mp, pexprNew);
				GPOS_ASSERT(pexprResult);
			}
		}

		// release the expression
		CRefCount::SafeRelease(pexprNew);
	}

	if (pexprResult)
		pxfres->Add(pexprResult);
}