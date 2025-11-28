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
 * CXformDynamicGet2ParallelAppendTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/xforms/CXformDynamicGet2ParallelAppendTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gpopt/xforms/CXformDynamicGet2ParallelAppendTableScan.h"

#include "gpos/base.h"

#include "gpopt/hints/CHintUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CPhysicalParallelAppendTableScan.h"
#include "gpopt/optimizer/COptimizerConfig.h"

// Forward declarations for gpdbwrappers functions
namespace gpdb {
bool IsParallelModeOK(void);
}

using namespace gpopt;

// Use gpdbwrappers for parallel checks
extern int max_parallel_workers_per_gather;

//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicGet2ParallelAppendTableScan::CXformDynamicGet2ParallelAppendTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformDynamicGet2ParallelAppendTableScan::CXformDynamicGet2ParallelAppendTableScan(
	CMemoryPool *mp)
	: CXformImplementation(
	// pattern
	GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalDynamicGet(mp)))
{
}

// compute xform promise for a given expression handle
CXform::EXformPromise
CXformDynamicGet2ParallelAppendTableScan::Exfp(CExpressionHandle &exprhdl) const
{
	// Check if parallel plans are enabled in context and parallel processing in safe
	if (!gpdb::IsParallelModeOK())
	{
		return CXform::ExfpNone;
	}

	if (!GPOS_FTRACE(EopttraceEnableParallelAppendScan))
		return CXform::ExfpNone;

	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(exprhdl.Pop());
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();

	// Don't use parallel append for replicated tables
	if (ptabdesc->GetRelDistribution() == IMDRelation::EreldistrReplicated ||
		ptabdesc->GetRelDistribution() == IMDRelation::EreldistrMasterOnly ||
		COptCtxt::PoctxtFromTLS()->HasReplicatedTables())
	{
		return CXform::ExfpNone;
	}

	// For AO/AOCO tables, check segfilecount early to avoid useless transformation
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdesc->MDId());
	IMDRelation::Erelstoragetype storage_type = pmdrel->RetrieveRelStorageType();

	// Check if this is an AO/AOCO table
	if (storage_type == IMDRelation::ErelstorageAppendOnlyRows ||
		storage_type == IMDRelation::ErelstorageAppendOnlyCols)
	{
		INT seg_file_count = pmdrel->SegFileCount();
		// Only reject if segfilecount is explicitly known to be 0 or 1
		// -1 means unknown (e.g., from DXL deserialization), so allow parallel in that case
		if (seg_file_count >= 0 && seg_file_count <= 1)
		{
			// If segfilecount is 0 or 1, parallel execution is pointless
			// Reject parallel scan early in promise phase
			GPOS_TRACE_FORMAT("CXformGet2ParallelAppendTableScan rejected for table %ls: AO/AOCO table has segfilecount=%d (needs >1 for parallel scan)",
							  ptabdesc->Name().Pstr()->GetBuffer(), seg_file_count);
			return CXform::ExfpNone;
		}
	}

	// Do not run if contains foreign partitions, instead run CXformExpandDynamicGetWithForeignPartitions
	if (popGet->ContainsForeignParts())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformDynamicGet2ParallelAppendTableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformDynamicGet2ParallelAppendTableScan::Transform(CXformContext *pxfctxt,
													CXformResult *pxfres,
													CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(pexpr->Pop());

	if (!CHintUtils::SatisfiesPlanHints(
		popGet,
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetPlanHint()))
	{
		return;
	}

	CMemoryPool *mp = pxfctxt->Pmp();

	// create/extract components for alternative
	CName *pname = GPOS_NEW(mp) CName(mp, popGet->Name());

	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = popGet->PdrgpcrOutput();
	GPOS_ASSERT(nullptr != pdrgpcrOutput);

	pdrgpcrOutput->AddRef();

	CColRef2dArray *pdrgpdrgpcrPart = popGet->PdrgpdrgpcrPart();
	pdrgpdrgpcrPart->AddRef();

	popGet->GetPartitionMdids()->AddRef();
	popGet->GetRootColMappingPerPart()->AddRef();

	// Determine parallel workers degree
	// Priority: table-level parallel_workers setting > GUC max_parallel_workers_per_gather > default
	ULONG ulParallelWorkers = 2;  // default

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

	// create alternative expression
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalParallelAppendTableScan(
			mp, pname, ptabdesc, popGet->UlOpId(), popGet->ScanId(),
			pdrgpcrOutput, pdrgpdrgpcrPart, popGet->GetPartitionMdids(),
			popGet->GetRootColMappingPerPart(), ulParallelWorkers));
	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF