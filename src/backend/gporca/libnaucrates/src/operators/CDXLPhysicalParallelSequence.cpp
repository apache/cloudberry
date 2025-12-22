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
 * CDXLPhysicalParallelSequence.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/src/operators/CDXLPhysicalParallelSequence.cpp
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPOPT_CPhysicalParallelSequence_H
#define GPOPT_CPhysicalParallelSequence_H

#include "naucrates/dxl/operators/CDXLPhysicalParallelSequence.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelSequence::CDXLPhysicalParallelSequence
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelSequence::CDXLPhysicalParallelSequence(CMemoryPool *mp, ULONG ulParallelWorkers)
	: CDXLPhysical(mp), m_ulParallelWorkers(ulParallelWorkers)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelSequence::~CDXLPhysicalParallelSequence
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelSequence::~CDXLPhysicalParallelSequence() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelSequence::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelSequence::GetDXLOperator() const
{
	return EdxlopPhysicalParallelSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelSequence::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelSequence::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelSequence);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelSequence::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelSequence::SerializeToDXL(CXMLSerializer *xml_serializer,
											 const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelSequence), element_name);

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
//		CDXLPhysicalParallelSequence::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelSequence::AssertValid(const CDXLNode *dxlnode,
								  BOOL validate_children) const
{
	const ULONG arity = dxlnode->Arity();
	GPOS_ASSERT(1 < arity);

	for (ULONG ul = 1; ul < arity; ul++)
	{
		CDXLNode *child_dxlnode = (*dxlnode)[ul];
		GPOS_ASSERT(EdxloptypePhysical ==
					child_dxlnode->GetOperator()->GetDXLOperatorType());

		if (validate_children)
		{
			child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
													  validate_children);
		}
	}
}
#endif	// GPOS_DEBUG

#endif	// !GPOPT_CPhysicalParallelSequence_H

//	EOF