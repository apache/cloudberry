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
 * CDXLPhysicalParallelAppend.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/include/naucrates/dxl/operators/CDXLPhysicalParallelAppend.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPDXL_CDXLPhysicalParallelAppend_H
#define GPDXL_CDXLPhysicalParallelAppend_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalAppend.h"

namespace gpdxl
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelAppend
//
//	@doc:
//		Class for representing DXL parallel append operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalParallelAppend : public CDXLPhysical
{
private:
	// is the append node used in an update/delete statement
	BOOL m_used_in_upd_del = false;

	// TODO:  - Apr 12, 2011; find a better name (and comments) for this variable
	BOOL m_is_zapped = false;

	// scan id from the CPhysicalDynamicTableScan (a.k.a part_index_id)
	// when m_scan_id != gpos::ulong_max
	ULONG m_scan_id = gpos::ulong_max;

	// table descr of the root partitioned table (when translated from a CPhysicalDynamicTableScan)
	CDXLTableDescr *m_dxl_table_descr = nullptr;

	ULongPtrArray *m_selector_ids = nullptr;

	// number of parallel workers
	ULONG m_ulParallelWorkers;

public:
	CDXLPhysicalParallelAppend(const CDXLPhysicalParallelAppend &) = delete;

	//ctor
	CDXLPhysicalParallelAppend(CMemoryPool *mp, BOOL fIsTarget, BOOL fIsZapped, ULONG ulParallelWorkers);

	// ctor for paritioned table scan
	CDXLPhysicalParallelAppend(CMemoryPool *mp, BOOL fIsTarget, BOOL fIsZapped,
							   ULONG scan_id, CDXLTableDescr *dxl_table_desc,
							   ULongPtrArray *selector_ids, ULONG ulParallelWorkers);

	// dtor
	~CDXLPhysicalParallelAppend() override;

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	BOOL IsUsedInUpdDel() const;
	BOOL IsZapped() const;

	CDXLTableDescr *
	GetDXLTableDesc() const
	{
		return m_dxl_table_descr;
	}

	void
	SetDXLTableDesc(CDXLTableDescr *dxl_table_desc)
	{
		m_dxl_table_descr = dxl_table_desc;
	}

	ULONG
	GetScanId() const
	{
		return m_scan_id;
	}

	const ULongPtrArray *
	GetSelectorIds() const
	{
		return m_selector_ids;
	}

	// get number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalParallelAppend *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalParallelAppend == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalParallelAppend *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};	// class CDXLPhysicalParallelAppend

}	// namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalParallelAppend_H

//	EOF