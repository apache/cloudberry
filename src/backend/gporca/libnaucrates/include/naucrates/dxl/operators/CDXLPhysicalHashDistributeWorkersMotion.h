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
//		CDXLPhysicalHashDistributeWorkersMotion.h
//
//	@doc:
//		Class for representing DXL worker-level hash distribute motion operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalHashDistributeWorkersMotion_H
#define GPDXL_CDXLPhysicalHashDistributeWorkersMotion_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"

namespace gpdxl
{
// indexes of hash distribute workers motion elements in the children array
enum Edxlhashworkers
{
	EdxlhashworkersIndexProjList = 0,
	EdxlhashworkersIndexFilter,
	EdxlhashworkersIndexSortColList,
	EdxlhashworkersIndexHashExprList,  // Hash expressions (REQUIRED)
	EdxlhashworkersIndexChild,
	EdxlhashworkersIndexSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalHashDistributeWorkersMotion
//
//	@doc:
//		Class for representing DXL worker-level hash distribute motion operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalHashDistributeWorkersMotion : public CDXLPhysicalMotion
{
private:
	// number of workers
	ULONG m_num_workers;

public:
	CDXLPhysicalHashDistributeWorkersMotion(
		const CDXLPhysicalHashDistributeWorkersMotion &) = delete;

	// ctor
	CDXLPhysicalHashDistributeWorkersMotion(CMemoryPool *mp, ULONG num_workers);

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// number of workers
	ULONG
	NumWorkers() const
	{
		return m_num_workers;
	}

	// index of relational child node in the children array
	ULONG
	GetRelationChildIdx() const override
	{
		return EdxlhashworkersIndexChild;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalHashDistributeWorkersMotion *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalMotionHashDistributeWorkers ==
					dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLPhysicalHashDistributeWorkersMotion *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalHashDistributeWorkersMotion_H

// EOF
