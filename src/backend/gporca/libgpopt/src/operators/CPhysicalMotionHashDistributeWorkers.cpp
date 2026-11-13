/*
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
 */

//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionHashDistributeWorkers.cpp
//
//	@doc:
//		Implementation of worker-level hash distribute motion operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalMotionHashDistributeWorkers.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::CPhysicalMotionHashDistributeWorkers
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistributeWorkers::CPhysicalMotionHashDistributeWorkers(
	CMemoryPool *mp, CDistributionSpecWorkerRandom *pdsWorkerRandom)
	: CPhysicalMotion(mp), m_pdsWorkerRandom(pdsWorkerRandom)
{
	GPOS_ASSERT(nullptr != pdsWorkerRandom);
	GPOS_ASSERT(nullptr != pdsWorkerRandom->PdsSegmentBase());
	GPOS_ASSERT(CDistributionSpec::EdtHashed ==
				pdsWorkerRandom->PdsSegmentBase()->Edt());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::~CPhysicalMotionHashDistributeWorkers
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistributeWorkers::~CPhysicalMotionHashDistributeWorkers()
{
	m_pdsWorkerRandom->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistributeWorkers::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalMotionHashDistributeWorkers *popWorkers =
		CPhysicalMotionHashDistributeWorkers::PopConvert(pop);

	return m_pdsWorkerRandom->Equals(popWorkers->m_pdsWorkerRandom);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionHashDistributeWorkers::PcrsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
	ULONG child_index,
	CDrvdPropArray *,  // pdrgpdpCtxt
	ULONG			   // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	// Get hash key columns from the base segment distribution
	CDistributionSpec *pdsBase = m_pdsWorkerRandom->PdsSegmentBase();
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);

	if (nullptr != pdsBase && CDistributionSpec::EdtHashed == pdsBase->Edt())
	{
		CDistributionSpecHashed *pdsHashed =
			CDistributionSpecHashed::PdsConvert(pdsBase);
		CColRefSet *pcrsHashKeys = pdsHashed->PcrsUsed(mp);
		pcrs->Union(pcrsHashKeys);
		pcrsHashKeys->Release();
	}

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

CPartitionPropagationSpec *
CPhysicalMotionHashDistributeWorkers::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &,  // exprhdl
	CPartitionPropagationSpec *,		   // pppsRequired
	ULONG,								   // child_index
	CDrvdPropArray *,					   // pdrgpdpCtxt
	ULONG								   // ulOptReq
) const
{
	// A motion is a hard barrier for partition propagation since it executes
	// in a different slice; and thus it cannot require this property from its child
	return GPOS_NEW(mp) CPartitionPropagationSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistributeWorkers::FProvidesReqdCols(
	CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
	ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionHashDistributeWorkers::EpetOrder(
	CExpressionHandle &,  // exprhdl
	const CEnfdOrder *
#ifdef GPOS_DEBUG
		peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistributeWorkers::PosRequired(
	CMemoryPool *mp,
	CExpressionHandle &,  //exprhdl,
	COrderSpec *,		  //posInput,
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

	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistributeWorkers::PosDerive(
	CMemoryPool *mp,
	CExpressionHandle &	 // exprhdl
) const
{
	return GPOS_NEW(mp) COrderSpec(mp);
}

CPartitionPropagationSpec *
CPhysicalMotionHashDistributeWorkers::PppsDerive(
	CMemoryPool *mp, CExpressionHandle &  // exprhdl
) const
{
	// A Motion cannot pass propagation spec
	return GPOS_NEW(mp) CPartitionPropagationSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionHashDistributeWorkers::OsPrint(IOstream &os) const
{
	os << SzId() << " (workers: " << NumWorkers() << ")";

	// choose a prefix big enough to avoid overlapping at least the simpler
	// expression trees
	CDistributionSpec *pdsBase = m_pdsWorkerRandom->PdsSegmentBase();

	if (nullptr != pdsBase && CDistributionSpec::EdtHashed == pdsBase->Edt())
	{
		CDistributionSpecHashed *pdsHashed =
			CDistributionSpecHashed::PdsConvert(pdsBase);
		return pdsHashed->OsPrintWithPrefix(
		os, "                                        ");
	}
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistributeWorkers::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistributeWorkers *
CPhysicalMotionHashDistributeWorkers::PopConvert(COperator *pop)
{
	GPOS_ASSERT(nullptr != pop);
	GPOS_ASSERT(EopPhysicalMotionHashDistributeWorkers == pop->Eopid());

	return dynamic_cast<CPhysicalMotionHashDistributeWorkers *>(pop);
}

// EOF
