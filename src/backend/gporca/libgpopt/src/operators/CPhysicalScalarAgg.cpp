//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalScalarAgg.cpp
//
//	@doc:
//		Implementation of scalar aggregation operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalScalarAgg.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarSortGroupClause.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::CPhysicalScalarAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalScalarAgg::CPhysicalScalarAgg(
	CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal,
	COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates,
	CColRefArray *pdrgpcrArgDQA, BOOL fMultiStage, BOOL isAggFromSplitDQA,
	CLogicalGbAgg::EAggStage aggStage, BOOL isAggPushdown,
	BOOL should_enforce_distribution)
	: CPhysicalAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype,
				   fGeneratesDuplicates, pdrgpcrArgDQA, fMultiStage,
				   isAggFromSplitDQA, aggStage, isAggPushdown,
				   should_enforce_distribution)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::~CPhysicalScalarAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalScalarAgg::~CPhysicalScalarAgg() = default;


//---------------------------------------------------------------------------
//	@function:
//		PosFromAggOrder
//
//	@doc:
//		Return the order spec of the ORDER BY of an aggregate function, or
//		null if it orders by something other than plain columns produced by
//		the child of the aggregate, e.g. an outer reference
//
//---------------------------------------------------------------------------
static COrderSpec *
PosFromAggOrder(CMemoryPool *mp, CExpression *pexprAggFunc,
				CColRefSet *pcrsChildOutput)
{
	CExpression *pexprArgs = (*pexprAggFunc)[EaggfuncIndexArgs];
	CExpression *pexprOrder = (*pexprAggFunc)[EaggfuncIndexOrder];

	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	for (ULONG ul = 0; ul < pexprOrder->Arity(); ul++)
	{
		CScalarSortGroupClause *popSortGroupClause =
			CScalarSortGroupClause::PopConvert((*pexprOrder)[ul]->Pop());
		INT index = popSortGroupClause->Index();
		if (0 > index || pexprArgs->Arity() <= (ULONG) index ||
			!CUtils::FScalarIdent((*pexprArgs)[index]))
		{
			pos->Release();
			return nullptr;
		}

		const CColRef *colref =
			CScalarIdent::PopConvert((*pexprArgs)[index]->Pop())->Pcr();
		if (!pcrsChildOutput->FMember(colref))
		{
			pos->Release();
			return nullptr;
		}
		COrderSpec::ENullTreatment ent = popSortGroupClause->NullsFirst()
											 ? COrderSpec::EntFirst
											 : COrderSpec::EntLast;
		pos->Append(GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral,
										   popSortGroupClause->SortOp()),
					colref, ent);
	}

	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::PosPresorted
//
//	@doc:
//		Return the order of the input that lets the aggregates with ORDER BY
//		in the project list skip their own sort, or an empty order spec.
//
//		An aggregate with ORDER BY sorts its input itself, on the
//		coordinator for a scalar aggregate that is not split. When the input
//		of an order sensitive aggregate such as string_agg() is sorted below
//		the aggregate instead, the segments sort their own rows in parallel,
//		and a merging Gather Motion produces the ordered input.
//
//		This is only done when all the aggregates with ORDER BY order by the
//		same plain columns of the input, at least one of them is order
//		sensitive, and none of the aggregates has DISTINCT.
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalScalarAgg::PosPresorted(CMemoryPool *mp, CExpression *pexprPrL,
								 CColRefSet *pcrsChildOutput) const
{
	if (GPOS_FTRACE(EopttraceDisablePresortedAgg) ||
		COperator::EgbaggtypeGlobal != Egbaggtype() || nullptr == pexprPrL)
	{
		return GPOS_NEW(mp) COrderSpec(mp);
	}

	COrderSpec *pos = nullptr;
	BOOL has_order_sensitive_agg = false;
	for (ULONG ul = 0; ul < pexprPrL->Arity(); ul++)
	{
		CExpression *pexprAggFunc = (*(*pexprPrL)[ul])[0];
		if (COperator::EopScalarAggFunc != pexprAggFunc->Pop()->Eopid())
		{
			continue;
		}

		CScalarAggFunc *popAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
		if (popAggFunc->IsDistinct())
		{
			CRefCount::SafeRelease(pos);
			return GPOS_NEW(mp) COrderSpec(mp);
		}

		if (!CUtils::FAggHasOrderBy(pexprAggFunc))
		{
			continue;
		}

		COrderSpec *pos_agg = nullptr;
		if (EaggfunckindNormal == popAggFunc->AggKind())
		{
			pos_agg = PosFromAggOrder(mp, pexprAggFunc, pcrsChildOutput);
		}

		if (nullptr == pos_agg ||
			(nullptr != pos && !pos->Matches(pos_agg)))
		{
			CRefCount::SafeRelease(pos_agg);
			CRefCount::SafeRelease(pos);
			return GPOS_NEW(mp) COrderSpec(mp);
		}

		if (nullptr == pos)
		{
			pos = pos_agg;
		}
		else
		{
			pos_agg->Release();
		}

		has_order_sensitive_agg = has_order_sensitive_agg ||
								  CUtils::FOrderSensitiveAgg(popAggFunc->MDId());
	}

	if (nullptr == pos || !has_order_sensitive_agg)
	{
		CRefCount::SafeRelease(pos);
		return GPOS_NEW(mp) COrderSpec(mp);
	}

	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalScalarAgg::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								COrderSpec *,  // posRequired
								ULONG
#ifdef GPOS_DEBUG
									child_index
#endif	// GPOS_DEBUG
								,
								CDrvdPropArray *,  // pdrgpdpCtxt
								ULONG			   // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// require the order of the aggregates with ORDER BY, if they can use it
	return PosPresorted(mp, exprhdl.PexprScalarRepChild(1),
						exprhdl.DeriveOutputColumns(0 /*child_index*/));
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalScalarAgg::PosDerive(CMemoryPool *mp,
							  CExpressionHandle &  // exprhdl
) const
{
	// return empty sort order
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScalarAgg::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalScalarAgg::EpetOrder(CExpressionHandle &,	// exprhdl
							  const CEnfdOrder *
#ifdef GPOS_DEBUG
								  peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	// TODO: , 06/20/2012: scalar agg produces one row, and hence it should satisfy any order;
	// a problem happens if we have a NLJ(R,S) where R is Salar Agg, and we require sorting on the
	// agg on top of NLJ, in this case we should satisfy this requirement without introducing a Sort
	// even though the NLJ's max card may be > 1
	return CEnfdProp::EpetRequired;
}

// EOF
