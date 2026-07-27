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
//		CPhysicalRightAntiSemiHashJoin.cpp
//
//	@doc:
//		Implementation of right anti semi hash join operator.
//		Mirror of CPhysicalLeftAntiSemiHashJoin with build/probe roles flipped:
//		  - LeftAntiSemi:  build = inner (right child), probe = outer (left)
//		  - RightAntiSemi: build = outer (left child),  probe = inner (right)
//		Finalize emits left-side rows that have no match. Output column
//		semantics identical to LeftAntiSemi.
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightAntiSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightAntiSemiHashJoin::CPhysicalRightAntiSemiHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::~CPhysicalRightAntiSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightAntiSemiHashJoin::~CPhysicalRightAntiSemiHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightAntiSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalRightAntiSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl,
												  CColRefSet *pcrsRequired,
												  ULONG	 // ulOptReq
) const
{
	// Right anti semi join semantically preserves left-side rows (same as
	// Left anti semi). Output columns come from the outer (left) child.
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

// EOF
