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
 * CPhysicalParallelSequence.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalParallelSequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CPhysicalParallelSequence_H
#define GPOPT_CPhysicalParallelSequence_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelSequence
//
//	@doc:
//		Physical parallel sequence operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelSequence : public CPhysical
{
private:
	//empty column set to be requested from all children except last child
	CColRefSet *m_pcrsEmpty;

	// number of parallel workers
	ULONG m_ulParallelWorkers;
public:
	CPhysicalParallelSequence(const CPhysicalParallelSequence &) = delete;

	// ctor
	explicit CPhysicalParallelSequence(CMemoryPool *mp, ULONG ulParallelWorkers);

	// dtor
	~CPhysicalParallelSequence() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelSequence;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelSequence";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	// conversion function
	static CPhysicalParallelSequence *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelSequence == pop->Eopid());

		return dynamic_cast<CPhysicalParallelSequence *>(pop);
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						     CColRefSet *pcrsRequired, ULONG child_index,
						     CDrvdPropArray *pdrgpdpCtxt,
						     ULONG ulOptReq) override;

	// compute required ctes of the n-th child
	CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
					      CCTEReq *pcter, ULONG child_index,
					      CDrvdPropArray *pdrgpdpCtxt,
					      ULONG ulOptReq) const override;

	// compute required sort columns of the n-th child
	COrderSpec *PosRequired(CMemoryPool *,		  // mp
							CExpressionHandle &,  // exprhdl
							COrderSpec *,		  // posRequired
							ULONG,				  // child_index
							CDrvdPropArray *,	  // pdrgpdpCtxt
							ULONG				  // ulOptReq
	) const override;

	// compute required distribution of the n-th child
	CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CDistributionSpec *pdsRequired,
								   ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulOptReq) const override;

	// compute required rewindability of the n-th child
	CRewindabilitySpec *PrsRequired(CMemoryPool *,		   //mp
									CExpressionHandle &,   //exprhdl
									CRewindabilitySpec *,  //prsRequired
									ULONG,				   // child_index
									CDrvdPropArray *,	   // pdrgpdpCtxt
									ULONG ulOptReq) const override;

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order from the last child
	COrderSpec *PosDerive(CMemoryPool *mp,
						  CExpressionHandle &exprhdl) const override;

	// derive distribution
	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

	// derive rewindability
	CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const override;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	// return rewindability property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &exprhdl,
		const CEnfdRewindability *per) const override;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	BOOL
	FPassThruStats() const override
	{
		return false;
	}


};	// class CPhysicalParallelSequence

}	// namespace gpopt

#endif	// !GPOPT_CPhysicalParallelSequence_H

//	EOF
