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
//		CPhysicalParallelInnerHashJoin.cpp
//
//	@doc:
//		Implementation of parallel inner hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalParallelInnerHashJoin.h"

#include "gpos/base.h"

using namespace gpopt;

CPartitionPropagationSpec *
CPhysicalParallelInnerHashJoin::PppsRequired(CMemoryPool *mp,
												 CExpressionHandle &exprhdl,
												 CPartitionPropagationSpec *pppsRequired,
												 ULONG child_index,
												 CDrvdPropArray *pdrgpdpCtxt,
												 ULONG ulOptReq) const
{
	return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index,
								 pdrgpdpCtxt, ulOptReq);
}

CPartitionPropagationSpec *
CPhysicalParallelInnerHashJoin::PppsDerive(CMemoryPool *mp,
												 CExpressionHandle &exprhdl) const
{
	return PppsDeriveForJoins(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelInnerHashJoin::CPhysicalParallelInnerHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalParallelInnerHashJoin::CPhysicalParallelInnerHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalParallelHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
								hash_opfamilies, is_null_aware, origin_xform)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalParallelInnerHashJoin::~CPhysicalParallelInnerHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalParallelInnerHashJoin::~CPhysicalParallelInnerHashJoin() = default;

// EOF
