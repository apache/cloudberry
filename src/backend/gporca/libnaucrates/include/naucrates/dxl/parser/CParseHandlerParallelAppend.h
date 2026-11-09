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
 * CParseHandlerParallelAppend.h
 *	  SAX parse handler class for parsing ParallelAppend operator nodes.
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libnaucrates/include/naucrates/dxl/parser/CParseHandlerParallelAppend.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPDXL_CParseHandlerParallelAppend_H
#define GPDXL_CParseHandlerParallelAppend_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalParallelAppend.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerParallelAppend
//
//	@doc:
//		Parse handler for ParallelAppend operator
//
//---------------------------------------------------------------------------
class CParseHandlerParallelAppend : public CParseHandlerPhysicalOp
{
private:
	CDXLPhysicalParallelAppend *m_dxl_op;

	// set up initial handlers
	void SetupInitialHandlers(const Attributes &attrs);

	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attrs					// element's attributes
		) override;

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
		) override;

public:
	CParseHandlerParallelAppend(const CParseHandlerParallelAppend &) = delete;

	// ctor/dtor
	CParseHandlerParallelAppend(CMemoryPool *mp,
						CParseHandlerManager *parse_handler_mgr,
						CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl
#endif	// GPDXL_CParseHandlerParallelAppend_H

// EOF
