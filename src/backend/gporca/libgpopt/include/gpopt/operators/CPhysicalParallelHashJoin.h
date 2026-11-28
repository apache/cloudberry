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
//		CPhysicalParallelHashJoin.h
//
//	@doc:
//		Base parallel hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalParallelHashJoin_H
#define GPOPT_CPhysicalParallelHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelHashJoin
//
//	@doc:
//		Base parallel hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelHashJoin : public CPhysicalHashJoin
{
private:
	// per-side parallel degrees (extracted in FValidContext from child groups)
	mutable ULONG m_ulProbeWorkers;
	mutable ULONG m_ulBuildWorkers;

	// Extract worker count from a child group by scanning for parallel operators
	// This handles cases where Motion nodes hide WorkerRandom distributions
	ULONG UlExtractWorkersFromGroup(CGroup *pgroup) const;

	// Internal recursive function with visited set to avoid infinite loops
	ULONG UlExtractWorkersFromGroupInternal(CGroup *pgroup, CBitSet *visited_groups) const;

	// Extract requested worker count for distribution requirement
	// Used when requesting WorkerRandom for first child before optimization
	ULONG UlExtractRequestedWorkers(CExpressionHandle &exprhdl, ULONG child_index) const;

	// Extract workers from child distributions (lazy initialization)
	// Called by PdsDerive on first invocation
	void ExtractWorkersIfNeeded(CExpressionHandle &exprhdl) const;

	// Compute required redistribute distribution spec for the n-th child
	// Wraps base class redistribution logic with WorkerRandom
	CDistributionSpec *PdsRequiredRedistributeParallel(CMemoryPool *mp,
											   CExpressionHandle &exprhdl,
											   CDistributionSpec *pdsInput,
											   ULONG child_index,
											   CDrvdPropArray *pdrgpdpCtxt,
											   ULONG ulOptReq) const;

	// Compute required replicate workers distribution spec for the n-th child
	// Similar to PdsRequiredReplicate but uses ReplicatedWorkers
	CDistributionSpec *PdsRequiredReplicateWorkers(CMemoryPool *mp,
												   CExpressionHandle &exprhdl,
												   CDistributionSpec *pdsInput,
												   ULONG child_index,
												   CDrvdPropArray *pdrgpdpCtxt,
												   ULONG ulOptReq) const;

public:
	CPhysicalParallelHashJoin(const CPhysicalParallelHashJoin &) = delete;

	// ctor
	CPhysicalParallelHashJoin(CMemoryPool *mp,
							  CExpressionArray *pdrgpexprOuterKeys,
							  CExpressionArray *pdrgpexprInnerKeys,
							  IMdIdArray *hash_opfamilies, BOOL is_null_aware,
							  CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalParallelHashJoin() override;

	//void CreateOptRequests(CMemoryPool *mp) override;

	ULONG UlProbeWorkers(CExpressionHandle &exprhdl) {
		ExtractWorkersIfNeeded(exprhdl);
		return UlProbeWorkers();
	}

	// parallel workers accessors
	ULONG UlProbeWorkers() const {
		//GPOS_ASSERT(m_ulProbeWorkers > 0 && "Probe workers not extracted - check ExtractWorkersIfNeeded");
		return m_ulProbeWorkers;
	}
	ULONG UlBuildWorkers() const {
		//GPOS_ASSERT(m_ulBuildWorkers > 0 && "Build workers not extracted - check ExtractWorkersIfNeeded");
		return m_ulBuildWorkers;
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required distribution of the n-th child
	CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CDistributionSpec *pdsRequired,
								   ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulOptReq) const override;

	// compute required rewindability of the n-th child
	CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CRewindabilitySpec *prsRequired,
									ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive distribution
	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

	// derive rewindability
	CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// compute required distribution of the n-th child - overridden to preserve WorkerRandom
	CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CReqdPropPlan *prppInput, ULONG child_index,
						   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override;

	// return distribution property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetDistribution(
		CExpressionHandle &exprhdl,
		const CEnfdDistribution *ped) const override;

	// check if optimization context is valid
	BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
					   COptimizationContextArray *pdrgpocChild) const override;

	// conversion function
	static CPhysicalParallelHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(CUtils::FParallelHashJoin(pop));

		return dynamic_cast<CPhysicalParallelHashJoin *>(pop);
	}

};	// class CPhysicalParallelHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelHashJoin_H

// EOF
