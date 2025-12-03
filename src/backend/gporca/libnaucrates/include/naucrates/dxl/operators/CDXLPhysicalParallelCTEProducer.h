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
 * CDXLPhysicalParallelCTEProducer.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/include/naucrates/dxl/operators/CDXLPhysicalParallelCTEProducer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPDXL_CDXLPhysicalParallelCTEProducer_H
#define GPDXL_CDXLPhysicalParallelCTEProducer_H
#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalParallelCTEProducer
//
//	@doc:
//		Class for representing DXL physical parallel CTE producers
//
//---------------------------------------------------------------------------
class CDXLPhysicalParallelCTEProducer : public CDXLPhysical
{
private:
	// cte id
	ULONG m_id;

	// output column ids
	ULongPtrArray *m_output_colids_array;

	// output column index mapping
	ULongPtrArray *m_output_colidx_map;

	// number of parallel workers
	ULONG m_ulParallelWorkers;

public:
	CDXLPhysicalParallelCTEProducer(CDXLPhysicalParallelCTEProducer &) = delete;

	// ctor
	CDXLPhysicalParallelCTEProducer(CMemoryPool *mp, ULONG id,
								    ULongPtrArray *output_colids_array,
								    ULongPtrArray *output_colidx_map,
								    ULONG ulParallelWorkers);

	// dtor
	~CDXLPhysicalParallelCTEProducer() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// cte identifier
	ULONG
	Id() const
	{
		return m_id;
	}

	ULongPtrArray *
	GetOutputColIdsArray() const
	{
		return m_output_colids_array;
	}


	ULongPtrArray *
	GetOutputColIdxMap() const
	{
		return m_output_colidx_map;
	}

	// get number of parallel workers
	ULONG UlParallelWorkers() const
	{
		return m_ulParallelWorkers;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// conversion function
	static CDXLPhysicalParallelCTEProducer *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalParallelCTEProducer == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLPhysicalParallelCTEProducer *>(dxl_op);
	}
};
}
#endif	// !GPDXL_CDXLPhysicalParallelCTEProducer_H