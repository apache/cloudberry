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
 * CPhysicalParallelCTEConsumer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelCTEConsumer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelCTEConsumer.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::CPhysicalParallelCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelCTEConsumer::CPhysicalParallelCTEConsumer(CMemoryPool *mp, ULONG id,
										   CColRefArray *colref_array,
										   UlongToColRefMap *colref_mapping,
										   ULONG ulParallelWorkers)
	: CPhysical(mp),
	  m_id(id),
	  m_pdrgpcr(nullptr),
	  m_phmulcr(colref_mapping),
	  m_pidxmap(nullptr),
	  m_ulParallelWorkers(ulParallelWorkers)
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
//		CPhysicalParallelCTEConsumer::~CPhysicalParallelCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelCTEConsumer::~CPhysicalParallelCTEConsumer()
{
	m_pdrgpcr->Release();
	m_phmulcr->Release();
	CRefCount::SafeRelease(m_pidxmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalParallelCTEConsumer::PcrsRequired(CMemoryPool *,		 // mp,
								   CExpressionHandle &,	 // exprhdl,
								   CColRefSet *,		 // pcrsRequired,
								   ULONG,				 // child_index,
								   CDrvdPropArray *,	 // pdrgpdpCtxt
								   ULONG				 // ulOptReq
)
{
	GPOS_ASSERT(!"CPhysicalParallelCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelCTEConsumer::PosRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  COrderSpec *,			// posRequired,
								  ULONG,				// child_index,
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalParallelCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelCTEConsumer::PdsRequired(CMemoryPool *,		// mp,
								  CExpressionHandle &,	// exprhdl,
								  CDistributionSpec *,	// pdsRequired,
								  ULONG,				//child_index
								  CDrvdPropArray *,		// pdrgpdpCtxt
								  ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalParallelCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelCTEConsumer::PrsRequired(CMemoryPool *,		 // mp,
								  CExpressionHandle &,	 // exprhdl,
								  CRewindabilitySpec *,	 // prsRequired,
								  ULONG,				 // child_index,
								  CDrvdPropArray *,		 // pdrgpdpCtxt
								  ULONG					 // ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalParallelCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalParallelCTEConsumer::PcteRequired(CMemoryPool *,		 //mp,
								   CExpressionHandle &,	 //exprhdl,
								   CCTEReq *,			 //pcter,
								   ULONG,				 //child_index,
								   CDrvdPropArray *,	 //pdrgpdpCtxt,
								   ULONG				 //ulOptReq
) const
{
	GPOS_ASSERT(!"CPhysicalParallelCTEConsumer has no relational children");
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelCTEConsumer::PosDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(!"Unexpected call to parallel CTE consumer order property derivation");

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelCTEConsumer::PdsDerive(CMemoryPool *,		 // mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to parallel CTE consumer distribution property derivation");

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelCTEConsumer::PrsDerive(CMemoryPool *,		 //mp
								CExpressionHandle &	 //exprhdl
) const
{
	GPOS_ASSERT(
		!"Unexpected call to parallel CTE consumer rewindability property derivation");

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalParallelCTEConsumer::PcmDerive(CMemoryPool *mp, CExpressionHandle &
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
//		CPhysicalParallelCTEConsumer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelCTEConsumer::FProvidesReqdCols(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelCTEConsumer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelCTEConsumer::EpetOrder(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelCTEConsumer::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelCTEConsumer::EpetRewindability(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelCTEConsumer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalParallelCTEConsumer *popCTEConsumer =
		CPhysicalParallelCTEConsumer::PopConvert(pop);

	return m_id == popCTEConsumer->UlCTEId() &&
		m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelCTEConsumer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalParallelCTEConsumer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}

// EOF