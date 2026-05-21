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
 * CPhysicalParallelTableScan.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalParallelTableScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CPhysicalParallelTableScan_H
#define GPOPT_CPhysicalParallelTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalTableScan.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelTableScan
//
//	@doc:
//		Parallel table scan operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelTableScan : public CPhysicalTableScan
{
private:
	// number of parallel workers
	ULONG m_ulParallelWorkers;

	// worker-level distribution spec
	CDistributionSpec *m_pdsWorkerDistribution;

	// private copy ctor
	CPhysicalParallelTableScan(const CPhysicalParallelTableScan &);

public:
	// ctors
	explicit CPhysicalParallelTableScan(CMemoryPool *mp);
	CPhysicalParallelTableScan(CMemoryPool *mp, const CName *pnameAlias, 
							   CTableDescriptor *ptabdesc,
							   CColRefArray *pdrgpcrOutput,
							   ULONG ulParallelWorkers);

	// dtor
	~CPhysicalParallelTableScan() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelTableScan;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelTableScan";
	}

	// number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(COperator *) const override;

	// debug print
	IOstream &OsPrint(IOstream &) const override;

	// conversion function
	static CPhysicalParallelTableScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelTableScan == pop->Eopid());

		return dynamic_cast<CPhysicalParallelTableScan *>(pop);
	}

	CRewindabilitySpec *
	PrsDerive(CMemoryPool *mp,
			  CExpressionHandle &  // exprhdl
	) const override
	{
		return GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtNone,
							   CRewindabilitySpec::EmhtNoMotion);
	}

	// derive distribution
	CDistributionSpec *PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// return distribution property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetDistribution(
		CExpressionHandle &exprhdl,
		const CEnfdDistribution *ped) const override;

	// return rewindability property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &exprhdl,
		const CEnfdRewindability *per) const override;

	// check if optimization contexts is valid
	// Reject if parent requires REWINDABLE (e.g., for NL Join inner child)
	BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
					   COptimizationContextArray *pdrgpocChild) const override;

};	// class CPhysicalParallelTableScan

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelTableScan_H

// EOF