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
 * CPhysicalParallelCTEProducer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelCTEProducer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalParallelCTEProducer.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSpool.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::CPhysicalParallelCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelCTEProducer::CPhysicalParallelCTEProducer(CMemoryPool *mp,
														   ULONG id,
														   CColRefArray *colref_array,
														   BOOL *umask,
														   ULONG ulParallelWorkers)
	: CPhysical(mp), m_id(id), m_pdrgpcr(nullptr), m_pcrs(nullptr), m_pdrgpcr_unused(nullptr), m_pidxmap(nullptr), m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(nullptr != colref_array);

#ifdef GPOS_DEBUG
	if (umask) {
		ULONG colref_size = colref_array->Size();
		for (ULONG ul = 0; ul < colref_size; ul++) {
			CColRef *col_ref = (*colref_array)[ul];
			if (col_ref->GetUsage(true, true) != CColRef::EUsed) {
				GPOS_ASSERT(!umask[ul]);
			}
		}
	}
#endif

	if (umask) {
		ULONG colref_size = colref_array->Size();
		ULONG unused_inc = 0;
		m_pcrs = GPOS_NEW(mp) CColRefSet(mp);
		m_pdrgpcr = GPOS_NEW(mp) CColRefArray(mp);
		m_pdrgpcr_unused = GPOS_NEW(mp) ULongPtrArray(mp, colref_size);
		m_pidxmap = GPOS_NEW(mp) ULongPtrArray(mp, colref_size);

		for (ULONG ul = 0; ul < colref_size; ul++) {
			CColRef *col_ref = (*colref_array)[ul];
			m_pdrgpcr_unused->Append(GPOS_NEW(m_mp) ULONG(unused_inc));
			if (umask[ul]) {
				m_pidxmap->Append(GPOS_NEW(m_mp) ULONG(m_pdrgpcr->Size()));
				m_pdrgpcr->Append(col_ref);
				m_pcrs->Include(col_ref);
			} else {
				m_pidxmap->Append(GPOS_NEW(m_mp) ULONG(gpos::ulong_max));
				unused_inc++;
			}
		}

	} else {
		m_pdrgpcr = colref_array;
		m_pcrs = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::~CPhysicalParallelCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelCTEProducer::~CPhysicalParallelCTEProducer()
{
	m_pdrgpcr->Release();
	m_pcrs->Release();
	CRefCount::SafeRelease(m_pdrgpcr_unused);
	CRefCount::SafeRelease(m_pidxmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalParallelCTEProducer::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired, ULONG child_index,
								   CDrvdPropArray *,  // pdrgpdpCtxt
								   ULONG			  // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(0 == pcrsRequired->Size());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	ULONG ccr_size = m_pdrgpcr->Size();
	for (ULONG index = 0; index < ccr_size; index++) {
		CColRef *col_ref = (*m_pdrgpcr)[index];
		GPOS_ASSERT(col_ref->GetUsage() != CColRef::EUnknown);
		if (col_ref->GetUsage() == CColRef::EUsed) {
			pcrs->Include(col_ref);
		}
	}

	pcrs->Union(pcrsRequired);
	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);

	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelCTEProducer::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  COrderSpec *posRequired, ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelCTEProducer::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CDistributionSpec *pdsRequired,
								  ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelCTEProducer::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CRewindabilitySpec *prsRequired,
								  ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalParallelCTEProducer::PcteRequired(CMemoryPool *,		 //mp,
								   CExpressionHandle &,	 //exprhdl,
								   CCTEReq *pcter,
								   ULONG
#ifdef GPOS_DEBUG
child_index
#endif
	,
								   CDrvdPropArray *,  //pdrgpdpCtxt,
								   ULONG			  //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalParallelCTEProducer::PosDerive(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelCTEProducer::PdsDerive(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalParallelCTEProducer::PrsDerive(CMemoryPool *mp,
								CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalParallelCTEProducer::PcmDerive(CMemoryPool *mp,
								CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(1 == exprhdl.Arity());

	CCTEMap *pcmChild = exprhdl.Pdpplan(0)->GetCostModel();

	CCTEMap *pcmProducer = GPOS_NEW(mp) CCTEMap(mp);
	// store plan properties of the child in producer's CTE map
	pcmProducer->Insert(m_id, CCTEMap::EctProducer, exprhdl.Pdpplan(0));

	CCTEMap *pcmCombined = CCTEMap::PcmCombine(mp, *pcmProducer, *pcmChild);
	pcmProducer->Release();

	return pcmCombined;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelCTEProducer::FProvidesReqdCols(CExpressionHandle &exprhdl,
										CColRefSet *pcrsRequired,
										ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelCTEProducer::EpetOrder(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelCTEProducer::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelCTEProducer::EpetRewindability(CExpressionHandle &exprhdl,
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
//		CPhysicalParallelCTEProducer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelCTEProducer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalParallelCTEProducer *popCTEProducer =
		CPhysicalParallelCTEProducer::PopConvert(pop);

	return m_id == popCTEProducer->UlCTEId() &&
		m_pdrgpcr->Equals(popCTEProducer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalParallelCTEProducer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelCTEProducer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalParallelCTEProducer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}

// EOF