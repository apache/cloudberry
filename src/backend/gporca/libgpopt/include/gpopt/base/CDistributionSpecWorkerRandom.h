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
 * CDistributionSpecWorkerRandom.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/base/CDistributionSpecWorkerRandom.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CDistributionSpecWorkerRandom_H
#define GPOPT_CDistributionSpecWorkerRandom_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecRandom.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecWorkerRandom
//
//	@doc:
//		Class for representing worker-level random distribution.
//		This class provides a specialized implementation for parallel
//		worker execution with explicit worker count management.
//
//---------------------------------------------------------------------------
class CDistributionSpecWorkerRandom : public CDistributionSpecRandom
{
private:
	// Number of workers for parallel execution
	ULONG m_ulWorkers;

	// Base segment distribution (usually segment-level random)
	CDistributionSpec *m_pdsSegmentBase;

	// private copy ctor
	CDistributionSpecWorkerRandom(const CDistributionSpecWorkerRandom &);

public:
	// ctor
	CDistributionSpecWorkerRandom(ULONG ulWorkers, CDistributionSpec *pdsSegmentBase = nullptr);

	// dtor
	~CDistributionSpecWorkerRandom() override;

	// distribution type accessor
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtWorkerRandom;
	}

	// distribution identifier
	const CHAR *
	SzId() const override
	{
		return "WORKER_RANDOM";
	}

	// Get worker count
	ULONG
	UlWorkers() const
	{
		return m_ulWorkers;
	}

	// Get base segment distribution
	CDistributionSpec *
	PdsSegmentBase() const
	{
		return m_pdsSegmentBase;
	}

	// does this distribution match the given one
	BOOL Matches(const CDistributionSpec *pds) const override;

	// does this distribution satisfy the given one
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// Factory method for creating worker-level random distribution
	static CDistributionSpecWorkerRandom *PdsCreateWorkerRandom(
		CMemoryPool *mp, ULONG ulWorkers, CDistributionSpec *pdsBase = nullptr);

	// conversion function
	static CDistributionSpecWorkerRandom *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtWorkerRandom == pds->Edt());

		return dynamic_cast<CDistributionSpecWorkerRandom *>(pds);
	}

	// conversion function: const argument
	static const CDistributionSpecWorkerRandom *
	PdsConvert(const CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtWorkerRandom == pds->Edt());

		return dynamic_cast<const CDistributionSpecWorkerRandom *>(pds);
	}

};	// class CDistributionSpecWorkerRandom

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecWorkerRandom_H

// EOF