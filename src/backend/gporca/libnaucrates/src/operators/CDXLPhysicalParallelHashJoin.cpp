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
//		CDXLPhysicalParallelHashJoin.cpp
//
//	@doc:
//		Implementation of DXL physical parallel hash join operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalParallelHashJoin.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelHashJoin::CDXLPhysicalParallelHashJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelHashJoin::CDXLPhysicalParallelHashJoin(
	CMemoryPool *mp, EdxlJoinType join_type, ULONG probe_workers, ULONG build_workers)
	: CDXLPhysicalHashJoin(mp, join_type),
	  m_probe_workers(probe_workers),
	  m_build_workers(build_workers)
{
	GPOS_ASSERT(probe_workers > 0 && "Probe workers must be greater than 0");
	GPOS_ASSERT(build_workers > 0 && "Build workers must be greater than 0");
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelHashJoin::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelHashJoin::GetDXLOperator() const
{
	return EdxlopPhysicalParallelHashJoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelHashJoin::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelHashJoin::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelHashJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelHashJoin::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelHashJoin::SerializeToDXL(CXMLSerializer *xml_serializer,
											  const CDXLNode *node) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);

	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenJoinType),
								 GetJoinTypeNameStr());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenParallelProbeWorkers),
		m_probe_workers);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenParallelBuildWorkers),
		m_build_workers);

	// serialize properties
	node->SerializePropertiesToDXL(xml_serializer);

	// serialize children
	node->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelHashJoin::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelHashJoin::AssertValid(const CDXLNode *node,
										  BOOL validate_children) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(node, validate_children);

	GPOS_ASSERT(EdxlhjIndexSentinel == node->Arity());
	GPOS_ASSERT(EdxljtSentinel > GetJoinType());
	GPOS_ASSERT(m_probe_workers > 0 && m_build_workers > 0);

	CDXLNode *join_filter = (*node)[EdxlhjIndexJoinFilter];
	CDXLNode *hash_clauses = (*node)[EdxlhjIndexHashCondList];
	CDXLNode *left = (*node)[EdxlhjIndexHashLeft];
	CDXLNode *right = (*node)[EdxlhjIndexHashRight];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarJoinFilter ==
				join_filter->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(EdxlopScalarHashCondList ==
				hash_clauses->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(EdxloptypePhysical ==
				left->GetOperator()->GetDXLOperatorType());
	GPOS_ASSERT(EdxloptypePhysical ==
				right->GetOperator()->GetDXLOperatorType());

	if (validate_children)
	{
		join_filter->GetOperator()->AssertValid(join_filter, validate_children);
		hash_clauses->GetOperator()->AssertValid(hash_clauses,
												 validate_children);
		left->GetOperator()->AssertValid(left, validate_children);
		right->GetOperator()->AssertValid(right, validate_children);
	}
}
#endif	// GPOS_DEBUG

// EOF
