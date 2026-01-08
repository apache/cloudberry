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
 * CPhysicalAppendTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalAppendTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalAppendTableScan.h"
#include "gpopt/operators/CPhysicalParallelAppendTableScan.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::CPhysicalParallelAppendTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelAppendTableScan::CPhysicalParallelAppendTableScan(CMemoryPool *mp,
																   const CName *pnameAlias,
																   CTableDescriptor *ptabdesc,
																   ULONG ulOriginOpId,
																   ULONG scan_id,
																   CColRefArray *pdrgpcrOutput,
																   CColRef2dArray *pdrgpdrgpcrParts,
																   IMdIdArray *partition_mdids,
																   ColRefToUlongMapArray *root_col_mapping_per_part,
																   ULONG ulParallelWorkers)
	: CPhysicalDynamicScan(mp, ptabdesc, ulOriginOpId, pnameAlias, scan_id,
						   pdrgpcrOutput, pdrgpdrgpcrParts, partition_mdids,
						   root_col_mapping_per_part),
	  m_ulParallelWorkers(ulParallelWorkers),
	  m_pdsWorkerDistribution(nullptr)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
	GPOS_ASSERT(nullptr != m_pds);
	// Create worker-level distribution based on table's segment distribution
	if (ulParallelWorkers > 0 && nullptr != m_pds)
	{
		// Create worker-level random distribution using the table's distribution as base
		// The base CPhysicalScan already sets up m_pds from the table descriptor
		m_pdsWorkerDistribution = CDistributionSpecWorkerRandom::PdsCreateWorkerRandom(mp, ulParallelWorkers, m_pds);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::~CPhysicalParallelAppendTableScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelAppendTableScan::~CPhysicalParallelAppendTableScan()
{
	CRefCount::SafeRelease(m_pdsWorkerDistribution);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelAppendTableScan::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalParallelAppendTableScan *popParallelAppendTableScan = 
		CPhysicalParallelAppendTableScan::PopConvert(pop);

	return m_ptabdesc->MDId()->Equals(popParallelAppendTableScan->Ptabdesc()->MDId()) &&
		   m_pdrgpcrOutput->Equals(popParallelAppendTableScan->PdrgpcrOutput()) &&
		   m_ulParallelWorkers == popParallelAppendTableScan->UlParallelWorkers();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::PstatsDerive
//
//	@doc:
//		Statistics derivation during costing
//
//---------------------------------------------------------------------------
IStatistics *
CPhysicalParallelAppendTableScan::PstatsDerive(CMemoryPool *mp,
											   CExpressionHandle &exprhdl,
											   CReqdPropPlan *prpplan,
											   IStatisticsArray *	// stats_ctxt
) const
{
	GPOS_ASSERT(nullptr != prpplan);

	return CStatisticsUtils::DeriveStatsForDynamicScan(
		mp, exprhdl, ScanId(), prpplan->Pepp()->PppsRequired());
}

CPartitionPropagationSpec *
CPhysicalParallelAppendTableScan::PppsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
	CPartitionPropagationSpec *pps = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	pps->Insert(ScanId(), CPartitionPropagationSpec::EpptConsumer,
				Ptabdesc()->MDId(), nullptr, nullptr);

	return pps;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::PdsDerive
//
//	@doc:
//		Derive distribution for parallel append table scan
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelAppendTableScan::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	// If we have a pre-computed worker distribution, use it
	if (nullptr != m_pdsWorkerDistribution)
	{
		m_pdsWorkerDistribution->AddRef();
		return m_pdsWorkerDistribution;
	}

	// Otherwise, derive from the base physical scan
	// This uses the m_pds member from CPhysicalScan
	return CPhysicalScan::PdsDerive(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelAppendTableScan::EpetDistribution(CExpressionHandle & /*exprhdl*/,
												   const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(nullptr != ped);

	// First check if worker-level distribution can satisfy the requirement
	// This is the primary distribution for parallel scans
	if (nullptr != m_pdsWorkerDistribution && ped->FCompatible(m_pdsWorkerDistribution))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	// Neither distribution satisfies the requirement
	// Motion enforcement will be needed on the output
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalParallelAppendTableScan::EpetRewindability(CExpressionHandle &exprhdl,
													const CEnfdRewindability *per) const
{
	GPOS_ASSERT(nullptr != per);

	// Get derived rewindability from this operator
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	// Check if our derived rewindability satisfies the requirement
	if (per->FCompatible(prs))
	{
		// Our derived rewindability (ErtNone) satisfies the requirement
		return CEnfdProp::EpetUnnecessary;
	}

	// Cannot satisfy the rewindability requirement
	// GPORCA will need to add an enforcer (e.g., Spool)
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelAppendTableScan::FValidContext
//
//	@doc:
//		Check if optimization contexts is valid;
//		Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
//		because ParallelAppendTableScan derives NONE (not rewindable)
//
//---------------------------------------------------------------------------
BOOL
CPhysicalParallelAppendTableScan::FValidContext(CMemoryPool *,
												COptimizationContext *poc,
												COptimizationContextArray *) const
{
	GPOS_ASSERT(nullptr != poc);

	CReqdPropPlan *prpp = poc->Prpp();
	CRewindabilitySpec *prsRequired = prpp->Per()->PrsRequired();

	// If parent requires REWINDABLE or higher, reject
	// ParallelTableScan can only provide ErtNone
	if (prsRequired->IsOriginNLJoin())
	{
		// Parent requires rewindability (e.g., NL Join inner child)
		// but ParallelTableScan cannot provide it
		// Reject this plan to avoid the assertion failure later
		return false;
	}

	return true;
}

//	EOF
