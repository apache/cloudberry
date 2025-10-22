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
//		CParseHandlerHashDistributeWorkersMotion.h
//
//	@doc:
//		SAX parse handler class for parsing worker-level hash distribute motion operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerHashDistributeWorkersMotion_H
#define GPDXL_CParseHandlerHashDistributeWorkersMotion_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalHashDistributeWorkersMotion.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerHashDistributeWorkersMotion
//
//	@doc:
//		Parse handler for worker-level hash distribute motion operators
//
//---------------------------------------------------------------------------
class CParseHandlerHashDistributeWorkersMotion : public CParseHandlerPhysicalOp
{
private:
	// motion operator
	CDXLPhysicalHashDistributeWorkersMotion *m_dxl_op;

	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
		) override;

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
		) override;

public:
	CParseHandlerHashDistributeWorkersMotion(
		const CParseHandlerHashDistributeWorkersMotion &) = delete;

	// ctor
	CParseHandlerHashDistributeWorkersMotion(
		CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
		CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerHashDistributeWorkersMotion_H

// EOF
