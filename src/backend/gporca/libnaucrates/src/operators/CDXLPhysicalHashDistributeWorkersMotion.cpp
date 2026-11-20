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
//		CDXLPhysicalHashDistributeWorkersMotion.cpp
//
//	@doc:
//		Implementation of DXL physical worker-level hash distribute motion operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalHashDistributeWorkersMotion.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashDistributeWorkersMotion::CDXLPhysicalHashDistributeWorkersMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalHashDistributeWorkersMotion::
	CDXLPhysicalHashDistributeWorkersMotion(CMemoryPool *mp, ULONG num_workers)
	: CDXLPhysicalMotion(mp), m_num_workers(num_workers)
{
	GPOS_ASSERT(num_workers > 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashDistributeWorkersMotion::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalHashDistributeWorkersMotion::GetDXLOperator() const
{
	return EdxlopPhysicalMotionHashDistributeWorkers;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashDistributeWorkersMotion::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalHashDistributeWorkersMotion::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalHashDistributeWorkersMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashDistributeWorkersMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashDistributeWorkersMotion::SerializeToDXL(
	CXMLSerializer *xml_serializer, const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);

	SerializeSegmentInfoToDXL(xml_serializer);

	// Serialize worker count
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenParallelWorkers), m_num_workers);

	// serialize properties
	dxlnode->SerializePropertiesToDXL(xml_serializer);

	// serialize children
	dxlnode->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashDistributeWorkersMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashDistributeWorkersMotion::AssertValid(
	const CDXLNode *dxlnode, BOOL validate_children) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(dxlnode, validate_children);

	GPOS_ASSERT(m_input_segids_array != nullptr);
	GPOS_ASSERT(0 < m_input_segids_array->Size());
	GPOS_ASSERT(m_output_segids_array != nullptr);
	GPOS_ASSERT(0 < m_output_segids_array->Size());
	GPOS_ASSERT(m_num_workers > 0);

	// Hash expr list is REQUIRED for worker-level hash distribute
	ULONG arity = dxlnode->Arity();
	GPOS_ASSERT(arity == EdxlhashworkersIndexSentinel);

	// Verify hash expression list exists
	CDXLNode *hash_expr_list_dxlnode =
		(*dxlnode)[EdxlhashworkersIndexHashExprList];
	GPOS_ASSERT(nullptr != hash_expr_list_dxlnode);
	GPOS_ASSERT(EdxlopScalarHashExprList ==
				hash_expr_list_dxlnode->GetOperator()->GetDXLOperator());

	CDXLNode *child_dxlnode = (*dxlnode)[EdxlhashworkersIndexChild];
	GPOS_ASSERT(EdxloptypePhysical ==
				child_dxlnode->GetOperator()->GetDXLOperatorType());

	if (validate_children)
	{
		child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
												  validate_children);
	}
}
#endif	// GPOS_DEBUG

// EOF
