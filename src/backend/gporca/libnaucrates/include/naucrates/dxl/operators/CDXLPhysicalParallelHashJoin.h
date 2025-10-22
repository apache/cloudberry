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
//		CDXLPhysicalParallelHashJoin.h
//
//	@doc:
//		Class for representing DXL parallel hash join operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalParallelHashJoin_H
#define GPDXL_CDXLPhysicalParallelHashJoin_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalHashJoin.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelHashJoin
//
//	@doc:
//		Class for representing DXL parallel hash join operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalParallelHashJoin : public CDXLPhysicalHashJoin
{
private:
	// per-side workers
	ULONG m_probe_workers;
	ULONG m_build_workers;

public:
	CDXLPhysicalParallelHashJoin(const CDXLPhysicalParallelHashJoin &) = delete;

	// ctor
	CDXLPhysicalParallelHashJoin(CMemoryPool *mp, EdxlJoinType join_type,
								 ULONG probe_workers, ULONG build_workers);

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// per-side workers accessors
	ULONG ProbeWorkers() const {
		GPOS_ASSERT(m_probe_workers > 0);
		return m_probe_workers;
	}
	ULONG BuildWorkers() const {
		GPOS_ASSERT(m_build_workers > 0);
		return m_build_workers;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalParallelHashJoin *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalParallelHashJoin == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLPhysicalParallelHashJoin *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLPhysicalParallelHashJoin_H

// EOF
