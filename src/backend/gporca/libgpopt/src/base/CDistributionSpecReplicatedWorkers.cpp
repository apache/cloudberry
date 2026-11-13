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

#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionBroadcastWorkers.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers::CDistributionSpecReplicatedWorkers(
	ULONG ulWorkers)
	: m_ulWorkers(ulWorkers)
{
	GPOS_ASSERT(ulWorkers > 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::~CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers::~CDistributionSpecReplicatedWorkers()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::PdsCreate
//
//	@doc:
//		Factory method
//
//---------------------------------------------------------------------------
CDistributionSpecReplicatedWorkers *
CDistributionSpecReplicatedWorkers::PdsCreate(CMemoryPool *mp, ULONG ulWorkers)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(ulWorkers > 0);

	return GPOS_NEW(mp) CDistributionSpecReplicatedWorkers(ulWorkers);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecReplicatedWorkers::Matches(const CDistributionSpec *pds) const
{
	if (EdtReplicatedWorkers != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecReplicatedWorkers *pdsReplicatedWorkers =
		CDistributionSpecReplicatedWorkers::PdsConvert(pds);

	return m_ulWorkers == pdsReplicatedWorkers->m_ulWorkers;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::FSatisfies
//
//	@doc:
//		Check if this distribution satisfies the given requirement
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecReplicatedWorkers::FSatisfies(const CDistributionSpec *pds) const
{
	// Direct match
	if (Matches(pds))
	{
		return true;
	}

	// ReplicatedWorkers satisfies universal requirements
	if (EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt())
	{
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::AppendEnforcers
//
//	@doc:
//		Add required motion enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecReplicatedWorkers::AppendEnforcers(
	CMemoryPool *mp, CExpressionHandle &
	/*exprhdl*/
	,
	CReqdPropPlan *prpp GPOS_ASSERTS_ONLY, CExpressionArray *pdrgpexpr, CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
				"required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionBroadcastWorkers))
	{
		// broadcast-workers Motion is disabled
		return;
	}

	// Add reference to input expression
	pexpr->AddRef();

	// Create Broadcast Workers Motion
	CDistributionSpecReplicatedWorkers *pdsReplicatedWorkers =
		PdsCreate(mp, m_ulWorkers);

	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalMotionBroadcastWorkers(mp, pdsReplicatedWorkers),
		pexpr);

	pdrgpexpr->Append(pexprMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecReplicatedWorkers::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecReplicatedWorkers::OsPrint(IOstream &os) const
{
	os << "REPLICATED_WORKERS(workers=" << m_ulWorkers << ")";
	return os;
}

// EOF
