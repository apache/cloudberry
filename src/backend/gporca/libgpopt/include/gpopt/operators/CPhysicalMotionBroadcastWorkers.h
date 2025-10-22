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

#ifndef GPOPT_CPhysicalMotionBroadcastWorkers_H
#define GPOPT_CPhysicalMotionBroadcastWorkers_H

#include "gpos/base.h"
#include "gpopt/base/CDistributionSpecReplicatedWorkers.h"
#include "gpopt/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionBroadcastWorkers
//
//	@doc:
//		Physical Broadcast Workers Motion operator
//
//		Broadcasts data across workers within each segment (not across segments)
//		This is used for parallel hash joins when one side (typically the build
//		side) needs to be replicated to all workers in each segment.
//
//---------------------------------------------------------------------------
class CPhysicalMotionBroadcastWorkers : public CPhysicalMotion
{
private:
	// Distribution spec for replicated workers
	CDistributionSpecReplicatedWorkers *m_pdsReplicatedWorkers;

	// Private copy ctor
	CPhysicalMotionBroadcastWorkers(const CPhysicalMotionBroadcastWorkers &);

public:
	// Ctor
	CPhysicalMotionBroadcastWorkers(CMemoryPool *mp,
									CDistributionSpecReplicatedWorkers *pds);

	// Dtor
	~CPhysicalMotionBroadcastWorkers() override;

	// Operator identifier
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalMotionBroadcastWorkers;
	}

	// Operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalMotionBroadcastWorkers";
	}

	// Output distribution accessor
	CDistributionSpec *
	Pds() const override
	{
		return m_pdsReplicatedWorkers;
	}

	// Match function
	BOOL Matches(COperator *pop) const override;

	// Hash function
	ULONG HashValue() const override;

	// Sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return false;
	}

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

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	COrderSpec *PosDerive(CMemoryPool *mp,
						  CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	//-------------------------------------------------------------------------------------
	// Print
	//-------------------------------------------------------------------------------------

	IOstream &OsPrint(IOstream &os) const override;

	// Conversion function
	static CPhysicalMotionBroadcastWorkers *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalMotionBroadcastWorkers == pop->Eopid());

		return dynamic_cast<CPhysicalMotionBroadcastWorkers *>(pop);
	}

};	// class CPhysicalMotionBroadcastWorkers

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalMotionBroadcastWorkers_H

// EOF
