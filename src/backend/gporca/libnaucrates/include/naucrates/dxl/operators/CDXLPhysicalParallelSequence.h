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
 * CDXLPhysicalParallelSequence.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/include/naucrates/dxl/operators/CDXLPhysicalParallelSequence.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPDXL_CDXLPhysicalParallelSequence_H
#define GPDXL_CDXLPhysicalParallelSequence_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelSequence
//
//	@doc:
//		Class for representing DXL physical parallel sequence operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalParallelSequence : public CDXLPhysical
{
private:
	// number of parallel workers
	ULONG m_ulParallelWorkers;

public:
	CDXLPhysicalParallelSequence(CDXLPhysicalParallelSequence &) = delete;

	// ctor
	CDXLPhysicalParallelSequence(CMemoryPool *mp, ULONG ulParallelWorkers);

	// dtor
	~CDXLPhysicalParallelSequence() override;

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// get number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
					    const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalParallelSequence *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalParallelSequence == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalParallelSequence *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}	// namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalParallelSequence_H

//	EOF