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
 * CPhysicalAppendTableScan.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/operators/CPhysicalAppendTableScan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPOPT_CPhysicalAppendTableScan_H
#define GPOPT_CPhysicalAppendTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalAppendTableScan
//
//	@doc:
//		Append Table scan operator
//
//---------------------------------------------------------------------------
class CPhysicalAppendTableScan : public CPhysicalDynamicScan
{
private:
public:
	CPhysicalAppendTableScan(const CPhysicalAppendTableScan &) = delete;

	// ctors
	CPhysicalAppendTableScan(CMemoryPool *mp, const CName *pnameAlias,
						  	 CTableDescriptor *ptabdesc, ULONG ulOriginOpId,
						  	 ULONG scan_id, CColRefArray *pdrgpcrOutput,
						  	 CColRef2dArray *pdrgpdrgpcrParts,
						  	 IMdIdArray *partition_mdids,
						  	 ColRefToUlongMapArray *root_col_mapping_per_part);

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalAppendTableScan;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalAppendTableScan";
	}

	// match function
	BOOL Matches(COperator *) const override;

	// statistics derivation during costing
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
						      CReqdPropPlan *prpplan,
						      IStatisticsArray *stats_ctxt) const override;

	// conversion function
	static CPhysicalAppendTableScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalAppendTableScan == pop->Eopid());

		return dynamic_cast<CPhysicalAppendTableScan *>(pop);
	}

	CPartitionPropagationSpec *PppsDerive(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
};	// class CPhysicalAppendTableScan

}	// namespace gpopt

#endif	// !GPOPT_CPhysicalAppendTableScan_H

//	EOF
