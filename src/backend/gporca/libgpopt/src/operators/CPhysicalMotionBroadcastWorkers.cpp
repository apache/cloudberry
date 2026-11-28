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

#include "gpopt/operators/CPhysicalMotionBroadcastWorkers.h"

#include "gpos/base.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::CPhysicalMotionBroadcastWorkers
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcastWorkers::CPhysicalMotionBroadcastWorkers(
	CMemoryPool *mp, CDistributionSpecReplicatedWorkers *pds)
	: CPhysicalMotion(mp), m_pdsReplicatedWorkers(pds)
{
	GPOS_ASSERT(nullptr != pds);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::~CPhysicalMotionBroadcastWorkers
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcastWorkers::~CPhysicalMotionBroadcastWorkers()
{
	m_pdsReplicatedWorkers->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcastWorkers::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalMotionBroadcastWorkers *popBroadcastWorkers =
		CPhysicalMotionBroadcastWorkers::PopConvert(pop);

	return m_pdsReplicatedWorkers->Matches(popBroadcastWorkers->m_pdsReplicatedWorkers);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::HashValue
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalMotionBroadcastWorkers::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_pdsReplicatedWorkers->HashValue());

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionBroadcastWorkers::PcrsRequired(CMemoryPool *mp,
											   CExpressionHandle &exprhdl,
											   CColRefSet *pcrsRequired,
											   ULONG child_index,
											   CDrvdPropArray *,  // pdrgpdpCtxt
											   ULONG			  // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcastWorkers::FProvidesReqdCols(CExpressionHandle &exprhdl,
													CColRefSet *pcrsRequired,
													ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionBroadcastWorkers::EpetOrder(CExpressionHandle &,  // exprhdl
										   const CEnfdOrder *	 // peo
) const
{
	// broadcast workers motion is not order-preserving
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcastWorkers::PosRequired(CMemoryPool *mp,
											  CExpressionHandle &,	// exprhdl
											  COrderSpec *,			// posInput
											  ULONG
#ifdef GPOS_DEBUG
												  child_index
#endif	// GPOS_DEBUG
											  ,
											  CDrvdPropArray *,	 // pdrgpdpCtxt
											  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// no order required from child expression
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcastWorkers::PosDerive(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
) const
{
	// broadcast workers motion is not order-preserving
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcastWorkers::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionBroadcastWorkers::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	m_pdsReplicatedWorkers->OsPrint(os);
	return os;
}

// EOF
