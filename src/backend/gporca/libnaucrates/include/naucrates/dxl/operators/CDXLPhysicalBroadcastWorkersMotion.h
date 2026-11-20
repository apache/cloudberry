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
//		CDXLPhysicalBroadcastWorkersMotion.h
//
//	@doc:
//		Class for representing DXL broadcast workers motion operators
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalBroadcastWorkersMotion_H
#define GPDXL_CDXLPhysicalBroadcastWorkersMotion_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
// indices of broadcast workers motion elements in the children array
enum Edxlbwm
{
	EdxlbwmIndexProjList = 0,
	EdxlbwmIndexFilter,
	EdxlbwmIndexSortColList,
	EdxlbwmIndexChild,
	EdxlbwmIndexSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalBroadcastWorkersMotion
//
//	@doc:
//		Class for representing DXL broadcast workers motion operators
//		This motion broadcasts data across workers within each segment
//		(not across segments like regular broadcast)
//
//---------------------------------------------------------------------------
class CDXLPhysicalBroadcastWorkersMotion : public CDXLPhysicalMotion
{
private:
public:
	CDXLPhysicalBroadcastWorkersMotion(const CDXLPhysicalBroadcastWorkersMotion &) = delete;

	// ctor/dtor
	explicit CDXLPhysicalBroadcastWorkersMotion(CMemoryPool *mp);

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// index of relational child node in the children array
	ULONG
	GetRelationChildIdx() const override
	{
		return EdxlbwmIndexChild;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalBroadcastWorkersMotion *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalMotionBroadcastWorkers == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLPhysicalBroadcastWorkersMotion *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalBroadcastWorkersMotion_H

// EOF
