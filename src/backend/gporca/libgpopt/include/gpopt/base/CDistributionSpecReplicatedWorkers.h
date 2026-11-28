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

#ifndef GPOPT_CDistributionSpecReplicatedWorkers_H
#define GPOPT_CDistributionSpecReplicatedWorkers_H

#include "gpos/base.h"
#include "gpopt/base/CDistributionSpec.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecReplicatedWorkers
//
//	@doc:
//		Distribution specification for worker-level replication
//		Used for Broadcast Workers Motion
//
//		Semantics:
//		- Data is replicated across all workers WITHIN each segment
//		- Does NOT cross segment boundaries
//		- Each worker in a segment has a complete copy of that segment's data
//
//		Example with 6 segments, 2 workers per segment:
//		  Before: Seg0.W0 has rows 0-50, Seg0.W1 has rows 51-100
//		  After:  Seg0.W0 has rows 0-100, Seg0.W1 has rows 0-100
//		          (Seg0.W0 and W1 exchange data locally, no network transfer)
//
//---------------------------------------------------------------------------
class CDistributionSpecReplicatedWorkers : public CDistributionSpec
{
private:
	// Number of parallel workers per segment
	ULONG m_ulWorkers;

	// Private copy ctor
	CDistributionSpecReplicatedWorkers(const CDistributionSpecReplicatedWorkers &);

public:
	// Ctor
	CDistributionSpecReplicatedWorkers(ULONG ulWorkers);

	// Dtor
	~CDistributionSpecReplicatedWorkers() override;

	// Accessor: distribution type
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtReplicatedWorkers;
	}

	// Accessor: number of workers
	ULONG
	UlWorkers() const
	{
		return m_ulWorkers;
	}

	// Does this distribution match the given one?
	BOOL Matches(const CDistributionSpec *pds) const override;

	// Does this distribution satisfy the given one?
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// Append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// Return distribution partitioning type
	EDistributionPartitioningType
	Edpt() const override
	{
		return EdptNonPartitioned;
	}

	// Print function
	IOstream &OsPrint(IOstream &os) const override;

	// Factory method
	static CDistributionSpecReplicatedWorkers *PdsCreate(CMemoryPool *mp,
														  ULONG ulWorkers);

	// Conversion function
	static CDistributionSpecReplicatedWorkers *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtReplicatedWorkers == pds->Edt());
		return dynamic_cast<CDistributionSpecReplicatedWorkers *>(pds);
	}

	// Conversion function: const argument
	static const CDistributionSpecReplicatedWorkers *
	PdsConvert(const CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtReplicatedWorkers == pds->Edt());
		return dynamic_cast<const CDistributionSpecReplicatedWorkers *>(pds);
	}

};	// class CDistributionSpecReplicatedWorkers

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecReplicatedWorkers_H

// EOF
