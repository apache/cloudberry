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
 * CPhysicalParallelAppendTableScan.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalParallelAppendTableScan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPOPT_CPhysicalParallelAppendTableScan_H
#define GPOPT_CPhysicalParallelAppendTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelAppendTableScan
//
//	@doc:
//		Parallel Append Table scan operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelAppendTableScan : public CPhysicalDynamicScan
{
private:
	// number of parallel workers
	ULONG m_ulParallelWorkers;

	// worker-level distribution spec
	CDistributionSpec *m_pdsWorkerDistribution;
public:
	CPhysicalParallelAppendTableScan(const CPhysicalParallelAppendTableScan &) = delete;

	// ctors
	CPhysicalParallelAppendTableScan(CMemoryPool *mp, const CName *pnameAlias,
								     CTableDescriptor *ptabdesc, ULONG ulOriginOpId,
								     ULONG scan_id, CColRefArray *pdrgpcrOutput,
								     CColRef2dArray *pdrgpdrgpcrParts,
								     IMdIdArray *partition_mdids,
								     ColRefToUlongMapArray *root_col_mapping_per_part,
								     ULONG ulParallelWorkers);

	// dtor
	~CPhysicalParallelAppendTableScan() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelAppendTableScan;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelAppendTableScan";
	}

	// match function
	BOOL Matches(COperator *) const override;

	// statistics derivation during costing
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  CReqdPropPlan *prpplan,
							  IStatisticsArray *stats_ctxt) const override;

	// number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// conversion function
	static CPhysicalParallelAppendTableScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelAppendTableScan == pop->Eopid());

		return dynamic_cast<CPhysicalParallelAppendTableScan *>(pop);
	}

	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

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

};	// class CPhysicalParallelAppendTableScan

}	// namespace gpopt

#endif	// !GPOPT_CPhysicalParallelAppendTableScan_H

//	EOF
