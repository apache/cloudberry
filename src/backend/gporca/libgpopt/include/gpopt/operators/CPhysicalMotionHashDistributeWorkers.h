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
//		CPhysicalMotionHashDistributeWorkers.h
//
//	@doc:
//		Physical worker-level hash distribute motion operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotionHashDistributeWorkers_H
#define GPOPT_CPhysicalMotionHashDistributeWorkers_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecWorkerRandom.h"
#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionHashDistributeWorkers
//
//	@doc:
//		Worker-level hash distribute motion operator for parallel execution
//
//---------------------------------------------------------------------------
class CPhysicalMotionHashDistributeWorkers : public CPhysicalMotion
{
private:
	// worker-level random distribution spec containing base hash distribution
	CDistributionSpecWorkerRandom *m_pdsWorkerRandom;

public:
	CPhysicalMotionHashDistributeWorkers(
		const CPhysicalMotionHashDistributeWorkers &) = delete;

	// ctor
	CPhysicalMotionHashDistributeWorkers(
		CMemoryPool *mp, CDistributionSpecWorkerRandom *pdsWorkerRandom);

	// dtor
	~CPhysicalMotionHashDistributeWorkers() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalMotionHashDistributeWorkers;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalMotionHashDistributeWorkers";
	}

	// output distribution accessor
	CDistributionSpec *
	Pds() const override
	{
		return m_pdsWorkerRandom;
	}

	// Get hash expressions from base distribution
	CExpressionArray *
	GetHashExpressions() const
	{
		CDistributionSpec *pdsBase = m_pdsWorkerRandom->PdsSegmentBase();
		if (nullptr != pdsBase &&
			CDistributionSpec::EdtHashed == pdsBase->Edt())
		{
			CDistributionSpecHashed *pdsHashed =
				CDistributionSpecHashed::PdsConvert(pdsBase);
			return pdsHashed->Pdrgpexpr();
		}
		return nullptr;
	}

	// Get hash opfamilies from base distribution
	IMdIdArray *
	GetHashOpfamilies() const
	{
		CDistributionSpec *pdsBase = m_pdsWorkerRandom->PdsSegmentBase();
		if (nullptr != pdsBase &&
			CDistributionSpec::EdtHashed == pdsBase->Edt())
		{
			CDistributionSpecHashed *pdsHashed =
				CDistributionSpecHashed::PdsConvert(pdsBase);
			return pdsHashed->Opfamilies();
		}
		return nullptr;
	}

	// Get number of workers
	ULONG
	NumWorkers() const
	{
		return m_pdsWorkerRandom->UlWorkers();
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CColRefSet *pcrsInput, ULONG child_index,
							 CDrvdPropArray *pdrgpdpCtxt,
							 ULONG ulOptReq) override;

	// compute required sort order of the n-th child
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posInput, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

	// compute required partition propagation spec of the n-th child
	CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	COrderSpec *PosDerive(CMemoryPool *mp,
						  CExpressionHandle &exprhdl) const override;

	// derived properties: derive partition propagation spec
	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// print
	IOstream &OsPrint(IOstream &) const override;

	// conversion function
	static CPhysicalMotionHashDistributeWorkers *PopConvert(COperator *pop);

};	// class CPhysicalMotionHashDistributeWorkers

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalMotionHashDistributeWorkers_H

// EOF
