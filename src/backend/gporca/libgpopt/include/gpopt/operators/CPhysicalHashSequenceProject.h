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
 * CPhysicalHashSequenceProject.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalHashSequenceProject.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPOPT_CPhysicalHashSequenceProject_H
#define GPOPT_CPhysicalHashSequenceProject_H

#include "gpos/base.h"

#include "gpopt/base/CWindowFrame.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"

namespace gpopt
{
// fwd declarations
class CDistributionSpec;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalHashSequenceProject
//
//	@doc:
//		Physical Hash Sequence Project operator
//
//---------------------------------------------------------------------------
class CPhysicalHashSequenceProject : public CPhysicalSequenceProject
{
public:
	CPhysicalHashSequenceProject(const CPhysicalHashSequenceProject &) = delete;

	// ctor
	CPhysicalHashSequenceProject(CMemoryPool *mp, ESPType m_sptype,
								 CDistributionSpec *pds,
								 COrderSpecArray *pdrgpos,
								 CWindowFrameArray *pdrgpwf);

	// dtor
	~CPhysicalHashSequenceProject() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalHashSequenceProject;
	}

	// operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalHashSequenceProject";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// hashing function
	ULONG HashValue() const override;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const override;

	// compute required sort order of the n-th child
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posRequired, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	BOOL
	FPassThruStats() const override
	{
		return true;
	}

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// conversion function
	static CPhysicalHashSequenceProject *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalHashSequenceProject == pop->Eopid());

		return dynamic_cast<CPhysicalHashSequenceProject *>(pop);
	}

};	// class CPhysicalHashSequenceProject

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalHashSequenceProject_H

// EOF
