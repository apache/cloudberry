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
 * CPhysicalParallelAppend.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CPhysicalParallelAppend.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "naucrates/dxl/operators/CDXLPhysicalParallelAppend.h"

#include "gpos/common/CBitSetIter.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelAppend::CDXLPhysicalParallelAppend
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalParallelAppend::CDXLPhysicalParallelAppend(CMemoryPool *mp, BOOL fIsTarget,
													   BOOL fIsZapped, ULONG ulParallelWorkers)
	: CDXLPhysical(mp), m_used_in_upd_del(fIsTarget), m_is_zapped(fIsZapped), m_ulParallelWorkers(ulParallelWorkers)
{
}

CDXLPhysicalParallelAppend::CDXLPhysicalParallelAppend(CMemoryPool *mp,
													   BOOL fIsTarget,
													   BOOL fIsZapped,
													   ULONG scan_id,
													   CDXLTableDescr *dxl_table_desc,
													   ULongPtrArray *selector_ids,
													   ULONG ulParallelWorkers)
	: CDXLPhysical(mp),
	  m_used_in_upd_del(fIsTarget),
	  m_is_zapped(fIsZapped),
	  m_scan_id(scan_id),
	  m_dxl_table_descr(dxl_table_desc),
	  m_selector_ids(selector_ids),
	  m_ulParallelWorkers(ulParallelWorkers)
{
}

CDXLPhysicalParallelAppend::~CDXLPhysicalParallelAppend() 
{
	CRefCount::SafeRelease(m_dxl_table_descr);
	CRefCount::SafeRelease(m_selector_ids);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelAppend::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalParallelAppend::GetDXLOperator() const
{
	return EdxlopPhysicalParallelAppend;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelAppend::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalParallelAppend::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalParallelAppend);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalParallelAppend::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelAppend::SerializeToDXL(CXMLSerializer *xml_serializer, const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);

	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenAppendIsTarget), m_used_in_upd_del);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenAppendIsZapped), m_is_zapped);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenParallelWorkers), m_ulParallelWorkers);

	if (m_scan_id != gpos::ulong_max)
	{
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenPartIndexId), m_scan_id);

		CWStringDynamic *serialized_selector_ids =
			CDXLUtils::Serialize(m_mp, m_selector_ids);
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenSelectorIds),
			serialized_selector_ids);
		GPOS_DELETE(serialized_selector_ids);
	}
	// serialize properties
	dxlnode->SerializePropertiesToDXL(xml_serializer);

	if (m_dxl_table_descr != nullptr)
	{
		GPOS_ASSERT(m_scan_id != gpos::ulong_max);
		m_dxl_table_descr->SerializeToDXL(xml_serializer);
	}

	// serialize children
	dxlnode->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalParallelAppend::AssertValid(const CDXLNode *dxlnode,
								BOOL validate_children) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(dxlnode, validate_children);

	const ULONG ulChildren = dxlnode->Arity();
	for (ULONG ul = EdxlappendIndexFirstChild; ul < ulChildren; ul++)
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

//	EOF
