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
 * CDXLPhysicalParallelCTEProducer.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/src/operators/CDXLPhysicalParallelCTEProducer.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "naucrates/dxl/operators/CDXLPhysicalParallelCTEProducer.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEProducer::CDXLPhysicalParallelCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelCTEProducer::CDXLPhysicalParallelCTEProducer(
	CMemoryPool *mp, ULONG id, ULongPtrArray *output_colids_array, ULongPtrArray *output_colidx_map, ULONG ulParallelWorkers)
	: CDXLPhysical(mp), m_id(id), m_output_colids_array(output_colids_array), m_output_colidx_map(output_colidx_map), m_ulParallelWorkers(ulParallelWorkers)
{
	GPOS_ASSERT(nullptr != output_colids_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEProducer::~CDXLPhysicalParallelCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelCTEProducer::~CDXLPhysicalParallelCTEProducer()
{
	m_output_colids_array->Release();
	CRefCount::SafeRelease(m_output_colidx_map);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEProducer::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelCTEProducer::GetDXLOperator() const
{
	return EdxlopPhysicalParallelCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEProducer::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelCTEProducer::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelCTEProducer);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelCTEProducer::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelCTEProducer::SerializeToDXL(CXMLSerializer *xml_serializer,
										const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenCTEId),
								 Id());

	CWStringDynamic *pstrColIds =
		CDXLUtils::Serialize(m_mp, m_output_colids_array);
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenColumns),
								 pstrColIds);
	GPOS_DELETE(pstrColIds);

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
//		CDXLPhysicalParallelCTEProducer::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelCTEProducer::AssertValid(const CDXLNode *dxlnode,
									 BOOL validate_children) const
{
	GPOS_ASSERT(2 == dxlnode->Arity());

	CDXLNode *pdxlnPrL = (*dxlnode)[0];
	CDXLNode *child_dxlnode = (*dxlnode)[1];

	GPOS_ASSERT(EdxlopScalarProjectList ==
				pdxlnPrL->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(EdxloptypePhysical ==
				child_dxlnode->GetOperator()->GetDXLOperatorType());

	if (validate_children)
	{
		pdxlnPrL->GetOperator()->AssertValid(pdxlnPrL, validate_children);
		child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
												  validate_children);
	}
}
#endif	// GPOS_DEBUG

// EOF