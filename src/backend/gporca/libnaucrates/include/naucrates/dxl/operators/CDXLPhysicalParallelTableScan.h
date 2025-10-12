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
 * CDXLPhysicalParallelTableScan.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/include/naucrates/dxl/operators/CDXLPhysicalParallelTableScan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPDXL_CDXLPhysicalParallelTableScan_H
#define GPDXL_CDXLPhysicalParallelTableScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"

namespace gpdxl
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelTableScan
//
//	@doc:
//		Class for representing DXL parallel table scan operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalParallelTableScan : public CDXLPhysicalTableScan
{
private:
	// number of parallel workers
	ULONG m_ulParallelWorkers;

public:
	CDXLPhysicalParallelTableScan(const CDXLPhysicalParallelTableScan &) = delete;

	// ctor
	CDXLPhysicalParallelTableScan(CMemoryPool *mp, CDXLTableDescr *table_descr,
								  ULONG ulParallelWorkers);

	// ctor with uninitialized table descriptor
	CDXLPhysicalParallelTableScan(CMemoryPool *mp, ULONG ulParallelWorkers);

	// dtor
	~CDXLPhysicalParallelTableScan() override = default;

	// get operator type
	Edxlopid GetDXLOperator() const override;

	// get operator name
	const CWStringConst *GetOpNameStr() const override;

	// get number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalParallelTableScan *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalParallelTableScan == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalParallelTableScan *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode, BOOL validate_children) const override;
#endif	// GPOS_DEBUG

};	// class CDXLPhysicalParallelTableScan

}  // namespace gpdxl

#endif	// !GPDXL_CDXLPhysicalParallelTableScan_H

// EOF