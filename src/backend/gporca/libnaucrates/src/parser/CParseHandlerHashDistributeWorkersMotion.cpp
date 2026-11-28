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
//		CParseHandlerHashDistributeWorkersMotion.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing worker-level
//		hash distribute motion operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerHashDistributeWorkersMotion.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerHashExprList.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerSortColList.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHashDistributeWorkersMotion::CParseHandlerHashDistributeWorkersMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerHashDistributeWorkersMotion::
	CParseHandlerHashDistributeWorkersMotion(
		CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
		CParseHandlerBase *parse_handler_root)
	: CParseHandlerPhysicalOp(mp, parse_handler_mgr, parse_handler_root),
	  m_dxl_op(nullptr)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHashDistributeWorkersMotion::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHashDistributeWorkersMotion::StartElement(
	const XMLCh *const,	 // element_uri,
	const XMLCh *const element_local_name,
	const XMLCh *const,	 // element_qname
	const Attributes &attrs)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(
					 EdxltokenPhysicalHashDistributeWorkersMotion),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	// parse and create worker-level hash distribute motion operator
	m_dxl_op = (CDXLPhysicalHashDistributeWorkersMotion *)
		CDXLOperatorFactory::MakeDXLHashDistributeWorkersMotion(
			m_parse_handler_mgr->GetDXLMemoryManager(), attrs);

	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance

	// parse handler for child node
	CParseHandlerBase *child_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenPhysical),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(child_parse_handler);

	// parse handler for hash expr list (REQUIRED for worker-level hash distribute)
	CParseHandlerBase *hash_expr_list_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarHashExprList),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(hash_expr_list_parse_handler);

	// parse handler for the sorting column list
	CParseHandlerBase *sort_col_list_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarSortColList),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(sort_col_list_parse_handler);

	// parse handler for the filter
	CParseHandlerBase *filter_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(filter_parse_handler);

	// parse handler for the proj list
	CParseHandlerBase *proj_list_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(proj_list_parse_handler);

	//parse handler for the properties of the operator
	CParseHandlerBase *prop_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenProperties),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(prop_parse_handler);

	// store parse handlers (must match child index order in CDXLPhysicalHashDistributeWorkersMotion.h)
	this->Append(prop_parse_handler);
	this->Append(proj_list_parse_handler);
	this->Append(filter_parse_handler);
	this->Append(sort_col_list_parse_handler);
	this->Append(hash_expr_list_parse_handler);
	this->Append(child_parse_handler);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHashDistributeWorkersMotion::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHashDistributeWorkersMotion::EndElement(
	const XMLCh *const,	 // element_uri,
	const XMLCh *const element_local_name,
	const XMLCh *const	// element_qname
)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(
					 EdxltokenPhysicalHashDistributeWorkersMotion),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	// construct node from the created child nodes

	CParseHandlerProperties *prop_parse_handler =
		dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *proj_list_parse_handler =
		dynamic_cast<CParseHandlerProjList *>((*this)[1]);
	CParseHandlerFilter *filter_parse_handler =
		dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerSortColList *sort_col_list_parse_handler =
		dynamic_cast<CParseHandlerSortColList *>((*this)[3]);
	CParseHandlerHashExprList *hash_expr_list_parse_handler =
		dynamic_cast<CParseHandlerHashExprList *>((*this)[4]);
	CParseHandlerPhysicalOp *child_parse_handler =
		dynamic_cast<CParseHandlerPhysicalOp *>((*this)[5]);

	m_dxl_node = GPOS_NEW(m_mp) CDXLNode(m_mp, m_dxl_op);
	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_dxl_node, prop_parse_handler);

	// add children (must match child index order in CDXLPhysicalHashDistributeWorkersMotion.h)
	AddChildFromParseHandler(proj_list_parse_handler);
	AddChildFromParseHandler(filter_parse_handler);
	AddChildFromParseHandler(sort_col_list_parse_handler);
	AddChildFromParseHandler(hash_expr_list_parse_handler);
	AddChildFromParseHandler(child_parse_handler);

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF
