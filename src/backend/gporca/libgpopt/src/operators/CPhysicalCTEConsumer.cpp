//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalCTEConsumer.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::CPhysicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::CPhysicalCTEConsumer(CMemoryPool *mp, ULONG id,
										   CColRefArray *colref_array,
										   UlongToColRefMap *colref_mapping)
	: CPhysical(mp),
	  m_id(id),
	  m_pdrgpcr(nullptr),
	  m_phmulcr(colref_mapping),
	  m_pidxmap(nullptr)
{
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(nullptr != colref_mapping);

	ULONG colref_size = colref_array->Size();
	m_pdrgpcr = GPOS_NEW(mp) CColRefArray(mp);
	m_pidxmap = GPOS_NEW(mp) ULongPtrArray(mp);

	for (ULONG index = 0; index < colref_size; index++) {
		CColRef *col_ref = (*colref_array)[index];
		if (col_ref->GetUsage(true, true) == CColRef::EUsed) {
			m_pdrgpcr->Append(col_ref);
			m_pidxmap->Append(GPOS_NEW(m_mp) ULONG(index));
		} 
	}

	if (m_pidxmap->Size() == colref_size) {
		m_pidxmap->Release();
		m_pidxmap = nullptr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::~CPhysicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalCTEConsumer::~CPhysicalCTEConsumer()
{
	m_pdrgpcr->Release();
	m_phmulcr->Release();
	CRefCount::SafeRelease(m_pidxmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalCTEConsumer::PcrsRequired(CMemoryPool *,		 // mp,
								   CExpressionHandle &,	 // exprhdl,
								   CColRefSet *,		 // pcrsRequired,
								   ULONG,				 // child_index,
								   CDrvdPropArray *,	 // pdrgpdpCtxt
								   ULONG				 // ulOptReq
)
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalCTEConsumer::PosRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  COrderSpec *,			// posRequired,
								  ULONG,				// child_index,
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalCTEConsumer::PdsRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  CDistributionSpec *,	// pdsRequired,
								  ULONG,				//child_index
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalCTEConsumer::PrsRequired(CMemoryPool *,		 // mp,
								  CExpressionHandle &,	 // exprhdl,
								  CRewindabilitySpec *,	 // prsRequired,
								  ULONG,				 // child_index,
								  CDrvdPropArray *,		 // pdrgpdpCtxt
								  ULONG					 // ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalCTEConsumer::PcteRequired(CMemoryPool *,		 //mp,
								   CExpressionHandle &,	 //exprhdl,
								   CCTEReq *,			 //pcter,
								   ULONG,				 //child_index,
								   CDrvdPropArray *,	 //pdrgpdpCtxt,
								   ULONG				 //ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalCTEConsumer::PosDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(!"Unexpected call to CTE consumer order property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalCTEConsumer::PdsDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to CTE consumer distribution property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalCTEConsumer::PrsDerive(CMemoryPool *,		 //mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to CTE consumer rewindability property derivation");

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalCTEConsumer::PcmDerive(CMemoryPool *mp, CExpressionHandle &
#ifdef GPOS_DEBUG
													 exprhdl
#endif
) const
{
	GPOS_ASSERT(0 == exprhdl.Arity());

	CCTEMap *pcmConsumer = GPOS_NEW(mp) CCTEMap(mp);
	pcmConsumer->Insert(m_id, CCTEMap::EctConsumer, nullptr /*pdpplan*/);

	return pcmConsumer;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEConsumer::FProvidesReqdCols(CExpressionHandle &exprhdl,
										CColRefSet *pcrsRequired,
										ULONG  // ulOptReq
) const
{
	GPOS_ASSERT(nullptr != pcrsRequired);

	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();
	return pcrsOutput->ContainsAll(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEConsumer::EpetOrder(CExpressionHandle &exprhdl,
								const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEConsumer::EpetRewindability(CExpressionHandle &exprhdl,
										const CEnfdRewindability *per) const
{
	GPOS_ASSERT(nullptr != per);

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEConsumer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalCTEConsumer *popCTEConsumer =
		CPhysicalCTEConsumer::PopConvert(pop);

	return m_id == popCTEConsumer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalCTEConsumer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalCTEConsumer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}

// EOF
