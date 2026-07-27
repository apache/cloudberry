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
//		CPhysicalRightSemiHashJoin.cpp
//
//	@doc:
//		Implementation of right semi hash join operator.
//		Mirror of CPhysicalLeftSemiHashJoin with build/probe roles flipped:
//		  - LeftSemi:  build = inner (right child), probe = outer (left child)
//		  - RightSemi: build = outer (left child),  probe = inner (right child)
//		Output schema and FProvidesReqdCols semantics are identical to LeftSemi
//		(both preserve left-side rows). The flip is encoded in the operator
//		EOperatorId, queried by M3 cost model and M4 DXL translation.
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::CPhysicalRightSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightSemiHashJoin::CPhysicalRightSemiHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::~CPhysicalRightSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightSemiHashJoin::~CPhysicalRightSemiHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalRightSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl,
											  CColRefSet *pcrsRequired,
											  ULONG	 // ulOptReq
) const
{
	// Right semi join semantically preserves left-side rows (same as Left
	// semi). Physical build side is the left child, finalize phase scans the
	// build hash table and emits visited left rows -- so output columns come
	// from the outer (left) child.
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

// NB: PppsRequired/PppsDerive are intentionally NOT overridden here; this
// operator inherits CPhysical's base behaviour so it does not host a
// join-driven Partition Selector.  Because the GPDB executor builds this join
// on the outer (LHS) child (build-side swap in CTranslatorDXLToPlStmt), a
// selector placed by PppsRequiredForJoins (which assumes selector-on-inner)
// would land on the probe side and crash ExecInitPartitionSelector.  A
// partitioned semijoin therefore falls back to the regular Hash Semi Join,
// which carries the selector correctly (matches CPhysicalRightAntiSemiHashJoin).
// EOF
