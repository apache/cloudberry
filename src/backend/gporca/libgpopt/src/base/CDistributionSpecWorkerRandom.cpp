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
 * CDistributionSpecWorkerRandom.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/base/CDistributionSpecWorkerRandom.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/base/CDistributionSpecWorkerRandom.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecStrictRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::CDistributionSpecWorkerRandom
//
//	@doc:
//		Ctor
//		Note: This constructor should only be called from PdsCreateWorkerRandom
//		factory method, which ensures pdsSegmentBase is properly initialized
//
//---------------------------------------------------------------------------
CDistributionSpecWorkerRandom::CDistributionSpecWorkerRandom(ULONG ulWorkers, CDistributionSpec *pdsSegmentBase)
	: m_ulWorkers(ulWorkers), m_pdsSegmentBase(pdsSegmentBase)
{
	GPOS_ASSERT(ulWorkers > 0);
	GPOS_ASSERT(nullptr != pdsSegmentBase &&
				"pdsSegmentBase must be non-null. Use PdsCreateWorkerRandom factory method.");

	m_pdsSegmentBase->AddRef();

	if (COptCtxt::PoctxtFromTLS()->FDMLQuery())
	{
		// set duplicate sensitive flag to enforce Hash-Distribution of
		// Const Tables in DML queries
		MarkDuplicateSensitive();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::~CDistributionSpecWorkerRandom
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecWorkerRandom::~CDistributionSpecWorkerRandom()
{
	CRefCount::SafeRelease(m_pdsSegmentBase);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::PdsCreateWorkerRandom
//
//	@doc:
//		Factory method for creating worker-level random distribution
//
//---------------------------------------------------------------------------
CDistributionSpecWorkerRandom *
CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(CMemoryPool *mp, ULONG ulWorkers, CDistributionSpec *pdsBase)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(ulWorkers > 0);

	// If no base distribution provided, create a default random distribution
	// using the provided memory pool (not TLS pool)
	CDistributionSpec *pdsSegmentBase = pdsBase;
	if (nullptr == pdsSegmentBase)
	{
		pdsSegmentBase = GPOS_NEW(mp) CDistributionSpecRandom();
	}

	return GPOS_NEW(mp) CDistributionSpecWorkerRandom(ulWorkers, pdsSegmentBase);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecWorkerRandom::Matches(const CDistributionSpec *pds) const
{
	if (pds->Edt() == CDistributionSpec::EdtWorkerRandom)
	{
		const CDistributionSpecWorkerRandom *pdsWorkerRandom =
			CDistributionSpecWorkerRandom::PdsConvert(pds);

		// Check if worker counts match and base distributions are compatible
		return (m_ulWorkers == pdsWorkerRandom->m_ulWorkers &&
				IsDuplicateSensitive() == pdsWorkerRandom->IsDuplicateSensitive() &&
				((nullptr == m_pdsSegmentBase && nullptr == pdsWorkerRandom->m_pdsSegmentBase) ||
				 (nullptr != m_pdsSegmentBase && nullptr != pdsWorkerRandom->m_pdsSegmentBase &&
				  m_pdsSegmentBase->Matches(pdsWorkerRandom->m_pdsSegmentBase))));
	}
	else if (pds->Edt() == CDistributionSpec::EdtRandom)
	{
		// Worker random can match regular random if base distribution matches
		const CDistributionSpecRandom *pdsRandom =
			CDistributionSpecRandom::PdsConvert(pds);

		return (nullptr != m_pdsSegmentBase &&
				m_pdsSegmentBase->Matches(pds) &&
				IsDuplicateSensitive() == pdsRandom->IsDuplicateSensitive());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecWorkerRandom::FSatisfies(const CDistributionSpec *pds) const
{
	if (Matches(pds))
	{
		return true;
	}

	// Handle different distribution types
	if (EdtWorkerRandom == pds->Edt())
	{
		const CDistributionSpecWorkerRandom *pdsWorkerRandom =
			CDistributionSpecWorkerRandom::PdsConvert(pds);

		// Worker-level can satisfy another worker-level if it has the same number of workers
		// and the base segment distribution is compatible
		return (m_ulWorkers == pdsWorkerRandom->m_ulWorkers &&
				(nullptr == m_pdsSegmentBase || nullptr == pdsWorkerRandom->m_pdsSegmentBase ||
				 m_pdsSegmentBase->FSatisfies(pdsWorkerRandom->m_pdsSegmentBase)) &&
				(IsDuplicateSensitive() || !pdsWorkerRandom->IsDuplicateSensitive()));
	}
	else if (EdtRandom == pds->Edt())
	{
		const CDistributionSpecRandom *pdsRandom = CDistributionSpecRandom::PdsConvert(pds);

		// Worker-level can satisfy segment-level requirement
		// if the base segment distribution satisfies it
		return (nullptr != m_pdsSegmentBase &&
				m_pdsSegmentBase->FSatisfies(pds) &&
				(IsDuplicateSensitive() || !pdsRandom->IsDuplicateSensitive()));
	}

	// Standard satisfaction logic for other distribution types
	return EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt() ||
		   EdtNonReplicated == pds->Edt();
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecWorkerRandom::AppendEnforcers(CMemoryPool *mp,
											   CExpressionHandle &exprhdl,
											   CReqdPropPlan *prpp,
											   CExpressionArray *pdrgpexpr,
											   CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(
		this == prpp->Ped()->PdsRequired() &&
		"required plan properties don't match enforced distribution spec");

	// Get the actually required distribution specification
	CDistributionSpec *pdsRequired = prpp->Ped()->PdsRequired();
	GPOS_ASSERT(nullptr != pdsRequired);

	// Get child's distribution for duplicate hazard checking
	CDistributionSpec *expr_dist_spec =
		CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	BOOL fDuplicateHazard = CUtils::FDuplicateHazardDistributionSpec(expr_dist_spec);

	pexpr->AddRef();
	CExpression *pexprMotion = nullptr;

	// Generate appropriate motion based on required distribution type
	switch (pdsRequired->Edt())
	{
		case CDistributionSpec::EdtHashed:
		{
			// Required: Hashed distribution -> Generate HashDistribute Motion
			if (GPOS_FTRACE(EopttraceDisableMotionHashDistribute))
			{
				// Hash redistribute Motion is disabled, cannot satisfy requirement
				pexpr->Release();
				return;
			}

			CDistributionSpecHashed *pdsHashedRequired =
				CDistributionSpecHashed::PdsConvert(pdsRequired);
			pdsHashedRequired->AddRef();

			if (fDuplicateHazard)
			{
				pdsHashedRequired->MarkDuplicateSensitive();
			}

			pexprMotion = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CPhysicalMotionHashDistribute(mp, pdsHashedRequired), pexpr);
			break;
		}

		case CDistributionSpec::EdtRandom:
		case CDistributionSpec::EdtWorkerRandom:
		{
			// Required: Random/WorkerRandom distribution -> Generate Random Motion
			if (GPOS_FTRACE(EopttraceDisableMotionRandom))
			{
				// Random Motion is disabled
				pexpr->Release();
				return;
			}

			// Use factory method to ensure proper memory pool usage
			CDistributionSpecWorkerRandom *random_dist_spec =
				PdsCreateWorkerRandom(mp, m_ulWorkers, m_pdsSegmentBase);

			if (fDuplicateHazard)
			{
				random_dist_spec->MarkDuplicateSensitive();
			}

			pexprMotion = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CPhysicalMotionRandom(mp, random_dist_spec), pexpr);
			break;
		}

		default:
		{
			// Fallback: cannot generate appropriate motion
			pexpr->Release();
			return;
		}
	}

	// Add the generated motion to the enforcer array
	if (nullptr != pexprMotion)
	{
		pdrgpexpr->Append(pexprMotion);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecWorkerRandom::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecWorkerRandom::OsPrint(IOstream &os) const
{
	os << SzId() << "[workers:" << m_ulWorkers << "]";
	if (nullptr != m_pdsSegmentBase)
	{
		os << " base:";
		m_pdsSegmentBase->OsPrint(os);
	}
	if (IsDuplicateSensitive())
	{
		os << " (duplicate sensitive)";
	}
	return os;
}

// EOF