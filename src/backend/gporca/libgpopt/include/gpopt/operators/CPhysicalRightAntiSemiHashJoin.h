//---------------------------------------------------------------------------
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
//	Greenplum Database
//	Portions Copyright (c) 2023-2026, HashData Technology Limited.
//
//	@filename:
//		CPhysicalRightAntiSemiHashJoin.h
//
//	@doc:
//		Right anti semi hash join operator (PG-style: build = outer/left,
//		probe = inner/right, finalize emits left rows that were not visited (unmatched).
//		Mirror of CPhysicalLeftAntiSemiHashJoin with build/probe roles flipped.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightAntiSemiHashJoin_H
#define GPOPT_CPhysicalRightAntiSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Right anti semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalRightAntiSemiHashJoin : public CPhysicalHashJoin
{
private:
public:
	CPhysicalRightAntiSemiHashJoin(const CPhysicalRightAntiSemiHashJoin &) =
		delete;

	// ctor
	CPhysicalRightAntiSemiHashJoin(
		CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalRightAntiSemiHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalRightAntiSemiHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalRightAntiSemiHashJoin";
	}

	// check if required columns are included in output columns
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	// conversion function
	static CPhysicalRightAntiSemiHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalRightAntiSemiHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalRightAntiSemiHashJoin *>(pop);
	}


};	// class CPhysicalRightAntiSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalRightAntiSemiHashJoin_H

// EOF
