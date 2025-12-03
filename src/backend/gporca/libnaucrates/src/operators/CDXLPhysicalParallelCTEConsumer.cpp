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
 * CDXLPhysicalParallelCTEConsumer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/src/operators/CDXLPhysicalParallelCTEConsumer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "naucrates/dxl/operators/CDXLPhysicalParallelCTEConsumer.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::CDXLPhysicalParallelCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelCTEConsumer::CDXLPhysicalParallelCTEConsumer(
	CMemoryPool *mp, ULONG id, ULongPtrArray *output_colids_array, ULongPtrArray *output_colidx_map, ULONG ulParallelWorkers)
	: CDXLPhysical(mp), m_id(id), m_output_colids_array(output_colids_array), m_output_colidx_map(output_colidx_map), m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(nullptr != output_colids_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::~CDXLPhysicalParallelCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelCTEConsumer::~CDXLPhysicalParallelCTEConsumer()
{
	m_output_colids_array->Release();
	CRefCount::SafeRelease(m_output_colidx_map);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelCTEConsumer::GetDXLOperator() const
{
	return EdxlopPhysicalParallelCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelCTEConsumer::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelCTEConsumer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelCTEConsumer::SerializeToDXL(CXMLSerializer *xml_serializer,
												const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenCTEId),
							     Id());

	CWStringDynamic *str_colids =
		CDXLUtils::Serialize(m_mp, m_output_colids_array);
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenColumns),
							     str_colids);
	GPOS_DELETE(str_colids);

	if (m_output_colidx_map) {
		CWStringDynamic *str_colidx_map =
			CDXLUtils::Serialize(m_mp, m_output_colidx_map);

		xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenColIdxmap),
									 str_colidx_map);
		GPOS_DELETE(str_colidx_map);
	}

	// serialize properties
	dxlnode->SerializePropertiesToDXL(xml_serializer);

	dxlnode->SerializeChildrenToDXL(xml_serializer);
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEConsumer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelCTEConsumer::AssertValid(const CDXLNode *dxlnode,
									 BOOL validate_children) const
{
	GPOS_ASSERT(1 == dxlnode->Arity());

	CDXLNode *dxlnode_proj_list = (*dxlnode)[0];
	GPOS_ASSERT(EdxlopScalarProjectList ==
				dxlnode_proj_list->GetOperator()->GetDXLOperator());

	if (validate_children)
	{
		dxlnode_proj_list->GetOperator()->AssertValid(dxlnode_proj_list,
													  validate_children);
	}
}
#endif	// GPOS_DEBUG

//	EOF
