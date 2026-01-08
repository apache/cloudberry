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
 * CPhysicalAppendTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalAppendTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/operators/CPhysicalAppendTableScan.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAppendTableScan::CPhysicalAppendTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalAppendTableScan::CPhysicalAppendTableScan(CMemoryPool *mp,
												   const CName *pnameAlias,
												   CTableDescriptor *ptabdesc,
												   ULONG ulOriginOpId,
												   ULONG scan_id,
												   CColRefArray *pdrgpcrOutput,
												   CColRef2dArray *pdrgpdrgpcrParts,
												   IMdIdArray *partition_mdids,
												   ColRefToUlongMapArray *root_col_mapping_per_part)
	: CPhysicalDynamicScan(mp, ptabdesc, ulOriginOpId, pnameAlias, scan_id,
							   pdrgpcrOutput, pdrgpdrgpcrParts, partition_mdids,
							   root_col_mapping_per_part)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAppendTableScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalAppendTableScan::Matches(COperator *pop) const
{
	return CUtils::FMatchDynamicScan(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAppendTableScan::PstatsDerive
//
//	@doc:
//		Statistics derivation during costing
//
//---------------------------------------------------------------------------
IStatistics *
CPhysicalAppendTableScan::PstatsDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   CReqdPropPlan *prpplan,
									   IStatisticsArray *	// stats_ctxt
) const
{
	GPOS_ASSERT(nullptr != prpplan);

	return CStatisticsUtils::DeriveStatsForDynamicScan(
		mp, exprhdl, ScanId(), prpplan->Pepp()->PppsRequired());
}

CPartitionPropagationSpec *
CPhysicalAppendTableScan::PppsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
	CPartitionPropagationSpec *pps = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	pps->Insert(ScanId(), CPartitionPropagationSpec::EpptConsumer,
			    Ptabdesc()->MDId(), nullptr, nullptr);

	return pps;
}

//	EOF
