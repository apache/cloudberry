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
//		CPhysicalParallelLeftOuterHashJoin.cpp
//
//	@doc:
//		Implementation of parallel left outer hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalParallelLeftOuterHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

// GUC variable from PostgreSQL
extern int max_parallel_workers_per_gather;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelLeftOuterHashJoin::CPhysicalParallelLeftOuterHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelLeftOuterHashJoin::CPhysicalParallelLeftOuterHashJoin(
	CMemoryPool *mp,
	CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys,
	IMdIdArray *hash_opfamilies,
	BOOL is_null_aware,
	CXform::EXformId origin_xform
)
	: CPhysicalParallelHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
								hash_opfamilies, is_null_aware, origin_xform)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelLeftOuterHashJoin::~CPhysicalParallelLeftOuterHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelLeftOuterHashJoin::~CPhysicalParallelLeftOuterHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelLeftOuterHashJoin::PdsDerive
//
//	@doc:
//		Derive distribution
//
//		For Left Outer Join, output distribution MUST be based on the left
//		(outer) child's distribution because all left table rows must be
//		preserved in the output.
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalParallelLeftOuterHashJoin::PdsDerive(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl
) const
{
	// Get distributions from children
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0)->Pds();
	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1)->Pds();

	// ========== Priority 1: Handle Parallel-specific Distributions ==========

	// Case 1: Inner child is ReplicatedWorkers (BroadcastWorkers scenario)
	// This means the inner table is small and replicated to all workers
	// Join output distribution follows the outer child's distribution
	if (CDistributionSpec::EdtReplicatedWorkers == pdsInner->Edt())
	{
		// Inner is replicated to workers, outer's distribution is preserved
		// Left Outer Join result follows outer's distribution
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// Case 2: Both children are WorkerRandom (HashDistributeWorkers scenario)
	if (CDistributionSpec::EdtWorkerRandom == pdsOuter->Edt() &&
		CDistributionSpec::EdtWorkerRandom == pdsInner->Edt())
	{
		// For Left Outer Join, output distribution ALWAYS follows outer (probe) child
		// This ensures all left table rows are preserved in the output
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// Case 3: Only outer is WorkerRandom
	if (CDistributionSpec::EdtWorkerRandom == pdsOuter->Edt())
	{
		// Return outer's WorkerRandom distribution
		// Inner will be redistributed to match (handled by PdsRequired)
		pdsOuter->AddRef();
		return pdsOuter;
	}

	// ========== Priority 2: Traditional Distributions ==========
	// For non-WorkerRandom cases (e.g., from Motion nodes), use PdsDeriveForOuterJoin
	return PdsDeriveForOuterJoin(mp, exprhdl);
}

// EOF
