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
//		CPhysicalRightSemiHashJoin.h
//
//	@doc:
//		Right semi hash join operator (PG-style: build = outer/left,
//		probe = inner/right, finalize emits left rows that were visited (matched).
//		Mirror of CPhysicalLeftSemiHashJoin with build/probe roles flipped.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightSemiHashJoin_H
#define GPOPT_CPhysicalRightSemiHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalRightSemiHashJoin
//
//	@doc:
//		Right semi hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalRightSemiHashJoin : public CPhysicalHashJoin
{
private:
public:
	CPhysicalRightSemiHashJoin(const CPhysicalRightSemiHashJoin &) = delete;

	// ctor
	CPhysicalRightSemiHashJoin(
		CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalRightSemiHashJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalRightSemiHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalRightSemiHashJoin";
	}

	// NB: deliberately does NOT override PppsRequired/PppsDerive.  The GPDB
	// executor builds this join on the outer (LHS) child (see the build-side
	// swap in CTranslatorDXLToPlStmt), which is the opposite of what
	// PppsRequiredForJoins assumes (selector on the inner/build child, consumer
	// on the outer/probe child).  Hosting a join-driven Partition Selector here
	// would place it on the wrong execution side and crash ExecInitPartitionSelector.
	// Inheriting CPhysical's base behaviour keeps right-semi out of join-driven
	// dynamic partition elimination (matching CPhysicalRightAntiSemiHashJoin),
	// so a partitioned semijoin falls back to the regular Hash Semi Join that
	// carries the selector correctly.

	// check if required columns are included in output columns.
	// Right semi join semantically preserves left-side rows (same as Left
	// semi join), so output columns come from the outer (left) child.
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
						   ULONG ulOptReq) const override;

	// conversion function
	static CPhysicalRightSemiHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalRightSemiHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalRightSemiHashJoin *>(pop);
	}


};	// class CPhysicalRightSemiHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalRightSemiHashJoin_H

// EOF
