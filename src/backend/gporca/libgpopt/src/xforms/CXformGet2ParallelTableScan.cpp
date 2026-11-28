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
 * CXformGet2ParallelTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformGet2ParallelTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformGet2ParallelTableScan.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/hints/CHintUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CPhysicalParallelTableScan.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CMemo.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDRelation.h"


// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

// Forward declarations for gpdbwrappers functions
namespace gpdb {
	bool IsParallelModeOK(void);
}

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2ParallelTableScan::CXformGet2ParallelTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGet2ParallelTableScan::CXformGet2ParallelTableScan(CMemoryPool *mp)
	: CXformImplementation(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalGet(mp)))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2ParallelTableScan::Exfp
//
//	@doc:
//		Compute promise of xform based on GUC enable_parallel
//		Uses unified parallel degree from max_parallel_workers_per_gather
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGet2ParallelTableScan::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel plans are enabled in context and parallel processing is safe
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	// Check for parallel-incompatible operations that would conflict with parallel scans
	if (CXformUtils::FHasParallelIncompatibleOps(exprhdl))
	{
		return CXform::ExfpNone;
	}

	CLogicalGet *popGet = CLogicalGet::PopConvert(exprhdl.Pop());
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();

	// Don't use parallel scan for replicated tables
	if (ptabdesc->GetRelDistribution() == IMDRelation::EreldistrReplicated ||
		ptabdesc->GetRelDistribution() == IMDRelation::EreldistrMasterOnly ||
		COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		//FIXME: Should we consider replicated tables.
		return CXform::ExfpNone;
	}

	// High promise for parallel scan when enabled
	// All tables will use the same parallel degree from max_parallel_workers_per_gather
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2ParallelTableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGet2ParallelTableScan::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
									   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalGet *popGet = CLogicalGet::PopConvert(pexpr->Pop());

	CMemoryPool *mp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(mp) CName(mp, popGet->Name());

	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popGet->PdrgpcrOutput();
	GPOS_ASSERT(nullptr != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	// Determine parallel workers degree
	// Priority: table-level parallel_workers setting > GUC max_parallel_workers_per_gather > default
	ULONG ulParallelWorkers = 2;

	// Check if table has a specific parallel_workers setting
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	INT table_parallel_workers = pmdrel->ParallelWorkers();

	if (table_parallel_workers > 0)
	{
		// Use table-level setting if explicitly configured
		ulParallelWorkers = (ULONG)table_parallel_workers;
	}
	else if (max_parallel_workers_per_gather > 0)
	{
		// Fall back to GUC setting
		ulParallelWorkers = (ULONG)max_parallel_workers_per_gather;
	}

	// Mark that we have parallel operators in the query
	COptCtxt::PoctxtFromTLS()->SetHasParallelOperators();

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalParallelTableScan(mp, pname, ptabdesc, pdrgpcrOutput, ulParallelWorkers));

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF