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
//		CPhysicalParallelLeftOuterHashJoin.h
//
//	@doc:
//		Parallel left outer hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalParallelLeftOuterHashJoin_H
#define GPOPT_CPhysicalParallelLeftOuterHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalParallelHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalParallelLeftOuterHashJoin
//
//	@doc:
//		Parallel left outer hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalParallelLeftOuterHashJoin : public CPhysicalParallelHashJoin
{
public:
	CPhysicalParallelLeftOuterHashJoin(const CPhysicalParallelLeftOuterHashJoin &) = delete;

	// ctor
	CPhysicalParallelLeftOuterHashJoin(
		CMemoryPool *mp,
		CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys,
		IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel
	);

	// dtor
	~CPhysicalParallelLeftOuterHashJoin() override;

	// ident accessors
	EOperatorId Eopid() const override
	{
		return EopPhysicalParallelLeftOuterHashJoin;
	}

	// return a string for operator name
	const CHAR *SzId() const override
	{
		return "CPhysicalParallelLeftOuterHashJoin";
	}

	// derive distribution - MUST be based on left (outer) child
	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

	// conversion function
	static CPhysicalParallelLeftOuterHashJoin *PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalParallelLeftOuterHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalParallelLeftOuterHashJoin *>(pop);
	}

};	// class CPhysicalParallelLeftOuterHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalParallelLeftOuterHashJoin_H

// EOF
