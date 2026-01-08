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
 * CPhysicalParallelCTEConsumer.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalParallelCTEConsumer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CPhysicalParallelCTEConsumer_H
#define GPOPT_CPhysicalParallelCTEConsumer_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelCTEConsumer
//
//	@doc:
//		parallel CTE consumer operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelCTEConsumer : public CPhysical
{
private:
	// cte identifier
	ULONG m_id;

	// cte columns
	CColRefArray *m_pdrgpcr;

	// hashmap for all the columns in the CTE expression
	UlongToColRefMap *m_phmulcr;

	// mapping index of colref to origin idx
	// if current colref not used, then set -1 in the position
	// if all used, then m_pidxmap will be nullptr
	ULongPtrArray *m_pidxmap;

	// number of parallel workers
	ULONG m_ulParallelWorkers;
public:
	CPhysicalParallelCTEConsumer(const CPhysicalParallelCTEConsumer &) = delete;

	// ctor
	CPhysicalParallelCTEConsumer(CMemoryPool *mp, ULONG id, CColRefArray *colref_array,
						 UlongToColRefMap *colref_mapping, ULONG ulParallelWorkers);

	// dtor
	~CPhysicalParallelCTEConsumer() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelCTEConsumer;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelCTEConsumer";
	}

	// cte identifier
	ULONG
	UlCTEId() const
	{
		return m_id;
	}

	// cte columns
	CColRefArray *
	Pdrgpcr() const
	{
		return m_pdrgpcr;
	}

	// column mapping
	UlongToColRefMap *
	Phmulcr() const
	{
		return m_phmulcr;
	}

	ULongPtrArray *
	PCTEIdxMap() const
	{
		return m_pidxmap;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// sensitivity to order of inputs
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
							 CColRefSet *pcrsRequired, ULONG child_index,
							 CDrvdPropArray *pdrgpdpCtxt,
							 ULONG ulOptReq) override;

	// compute required ctes of the n-th child
	CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  CCTEReq *pcter, ULONG child_index,
						  CDrvdPropArray *pdrgpdpCtxt,
						  ULONG ulOptReq) const override;

	// compute required sort order of the n-th child
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posRequired, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

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

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	COrderSpec *PosDerive(CMemoryPool *mp,
						  CExpressionHandle &exprhdl) const override;

	// derive distribution
	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

	// derive rewindability
	CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const override;

	// derive cte map
	CCTEMap *PcmDerive(CMemoryPool *mp,
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

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CPhysicalParallelCTEConsumer *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelCTEConsumer == pop->Eopid());

		return dynamic_cast<CPhysicalParallelCTEConsumer *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CPhysicalParallelCTEConsumer

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelCTEConsumer_H

//	EOF