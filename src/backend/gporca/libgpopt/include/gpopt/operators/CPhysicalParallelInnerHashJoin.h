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
//		CPhysicalParallelInnerHashJoin.h
//
//	@doc:
//		Parallel inner hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalParallelInnerHashJoin_H
#define GPOPT_CPhysicalParallelInnerHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalParallelHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelInnerHashJoin
//
//	@doc:
//		Parallel inner hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelInnerHashJoin : public CPhysicalParallelHashJoin
{
public:
	CPhysicalParallelInnerHashJoin(const CPhysicalParallelInnerHashJoin &) =
		delete;

	// ctor
	CPhysicalParallelInnerHashJoin(CMemoryPool *mp,
								   CExpressionArray *pdrgpexprOuterKeys,
								   CExpressionArray *pdrgpexprInnerKeys,
								   IMdIdArray *hash_opfamilies,
								   BOOL is_null_aware,
								   CXform::EXformId origin_xform =
									   CXform::ExfSentinel);

	// dtor
	~CPhysicalParallelInnerHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalParallelInnerHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalParallelInnerHashJoin";
	}

	// partition propagation
	CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// conversion function
	static CPhysicalParallelInnerHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelInnerHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalParallelInnerHashJoin *>(pop);
	}

};	// class CPhysicalParallelInnerHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelInnerHashJoin_H

// EOF
