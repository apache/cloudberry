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
 * CDedupSupersetPreprocessor.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CDedupSupersetPreprocessor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CDedupSupersetPreprocessor_H
#define GPOPT_CDedupSupersetPreprocessor_H

#include "gpos/base.h"
#include "gpos/memory/set.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"

namespace gpopt
{

class TableDescIdent final
{
public:
	IMDId *pmdid;
	ULONG subid;
	ULONG ocolid;

	TableDescIdent(IMDId *mdid, ULONG sid, ULONG output_colid);

	~TableDescIdent();

	// hash function
	static ULONG HashFunc(const TableDescIdent *ptdi);

	// equality function
	static BOOL EqualFunc(const TableDescIdent *pltdi,
						  const TableDescIdent *prtdi);
};

// desc to ulong map
using TDIToUlongPtrMap =
	CHashMap<TableDescIdent, ULONG, TableDescIdent::HashFunc,
			 TableDescIdent::EqualFunc, CleanupDelete<TableDescIdent>,
			 CleanupDelete<ULONG>>;

// iterator
using TDIToUlongPtrMapIter =
	CHashMapIter<TableDescIdent, ULONG, TableDescIdent::HashFunc,
				 TableDescIdent::EqualFunc, CleanupDelete<TableDescIdent>,
				 CleanupDelete<ULONG>>;

class CDedupSupersetPreprocessor
{
private:
	// Expression is CLogicalGet,CLogicalDynamicGet or CLogicalCTEConsumer?
	static BOOL PexprIsLogicalOutput(CExpression *pexpr);

	// Get the meta id from expression
	static IMDId *PexprGetIMDid(CMemoryPool *mp, CExpression *pexpr);

	// full the maps with superset expression
	static void ChildExprFullSuperset(CMemoryPool *mp, CExpression *pexpr,
									  TDIToUlongPtrMap *eqcrmaps,
									  BOOL *dedupulmasks, ULONG ul);

	// full the dedups array with subset(simply logicalget)
	static void ChildExprFullSimplySubset(CMemoryPool *mp, CExpression *pexpr,
										  TDIToUlongPtrMap *eqcrmaps,
										  BOOL *dedupulmasks);

	// Extract the sub child from the join(inner join) expr
	static CExpression *PexprExtractSubChildJoinExpr(CExpression *pexpr,
													 const CColRef *subany_col);

	// query the join subset
	static ULONG *DriveFullJoinSubset(CMemoryPool *mp,
									  TDIToUlongPtrMap *eqcrmaps,
									  CExpression *pexpr_join,
									  const CColRef *pcr_sbany,
									  const CColRef *pcr_ident);

	// full the dedups array with subset(inner join)
	static void ChildExprFullJoinSubset(CMemoryPool *mp, CExpression *pexpr,
										TDIToUlongPtrMap *eqcrmaps,
										BOOL *dedupulmasks);

	// child expr in CScalarBoolOp(ANDOP) is the superset
	static BOOL ChildExprIsSuperset(CExpression *pexpr);

	// child expr in CScalarBoolOp(ANDOP) is the simply subset
	static BOOL ChildExprIsSimplySubset(CExpression *pexpr);

	// child expr in CScalarBoolOp(ANDOP) is the inner-join subset
	// left-semi-join won't occur in the pre-process
	static BOOL ChildExprIsJoinSubset(CExpression *pexpr);


public:
	CDedupSupersetPreprocessor(const CDedupSupersetPreprocessor &) = delete;

	// main driver
	static CExpression *PexprPreprocess(CMemoryPool *mp, CExpression *pexpr);
};

}  // namespace gpopt


#endif	// GPOPT_CDedupSupersetPreprocessor_H
