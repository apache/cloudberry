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
 * CDXLPhysicalParallelTableScan.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/src/operators/CDXLPhysicalParallelTableScan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "naucrates/dxl/operators/CDXLPhysicalParallelTableScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::CDXLPhysicalParallelTableScan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelTableScan::CDXLPhysicalParallelTableScan(CMemoryPool *mp,
															 CDXLTableDescr *table_descr,
															 ULONG ulParallelWorkers)
	: CDXLPhysicalTableScan(mp, table_descr),
	  m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::CDXLPhysicalParallelTableScan
//
//	@doc:
//		Constructor with uninitialized table descriptor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelTableScan::CDXLPhysicalParallelTableScan(CMemoryPool *mp,
															 ULONG ulParallelWorkers)
	: CDXLPhysicalTableScan(mp),
	  m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(ulParallelWorkers > 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelTableScan::GetDXLOperator() const
{
	return EdxlopPhysicalParallelTableScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelTableScan::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelTableScan::SerializeToDXL(CXMLSerializer *xml_serializer,
											   const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();
	xml_serializer->OpenElement(CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
								element_name);

	// serialize parallel workers attribute
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenParallelWorkers),
								 m_ulParallelWorkers);

	// serialize properties
	dxlnode->SerializePropertiesToDXL(xml_serializer);

	// serialize projection list and filter
	dxlnode->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
								 element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelTableScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelTableScan::AssertValid(const CDXLNode *dxlnode,
											BOOL validate_children) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(dxlnode, validate_children);

	// parallel table scan has only 2 children (proj list and filter)
	GPOS_ASSERT(2 == dxlnode->Arity());

	CDXLNode *proj_list_dxlnode = (*dxlnode)[0];  // First child is projection list
	CDXLNode *filter_dxlnode = (*dxlnode)[1];     // Second child is filter

	GPOS_ASSERT(EdxlopScalarProjectList ==
				proj_list_dxlnode->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(EdxlopScalarFilter ==
				filter_dxlnode->GetOperator()->GetDXLOperator());

	if (validate_children)
	{
		proj_list_dxlnode->GetOperator()->AssertValid(proj_list_dxlnode, validate_children);
		filter_dxlnode->GetOperator()->AssertValid(filter_dxlnode, validate_children);
	}
}
#endif	// GPOS_DEBUG

// EOF