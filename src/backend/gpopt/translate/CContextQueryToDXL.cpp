//---------------------------------------------------------------------------
//  Greenplum Database
//	Copyright (C) 2018-Present VMware, Inc. or its affiliates.
//
//	@filename:
//		CContextQueryToDXL.cpp
//
//	@doc:
//		Implementation of the methods used to hold information about
//		the whole query, when translate a query into DXL tree. All
//		translator methods allocate memory in the provided memory pool,
//		and the caller is responsible for freeing it
//
//---------------------------------------------------------------------------
extern "C" {
#include "postgres.h"
}

#include "gpopt/translate/CContextQueryToDXL.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"

using namespace gpdxl;

CContextQueryToDXL::CContextQueryToDXL(CMemoryPool *mp)
	: m_mp(mp),
	  m_has_distributed_tables(false),
	  m_distribution_hashops(DistrHashOpsNotDeterminedYet),
	  m_security_quals(NIL),
	  m_security_quals_relids(NIL)
{
	// map that stores gpdb att to optimizer col mapping
	m_colid_counter = GPOS_NEW(mp) CIdGenerator(GPDXL_COL_ID_START);
	m_queryid_counter = GPOS_NEW(mp) CIdGenerator(GPDXL_QUERY_ID_START);
	m_cte_id_counter = GPOS_NEW(mp) CIdGenerator(GPDXL_CTE_ID_START);
}

CContextQueryToDXL::~CContextQueryToDXL()
{
	GPOS_DELETE(m_queryid_counter);
	GPOS_DELETE(m_colid_counter);
	GPOS_DELETE(m_cte_id_counter);
}

ULONG
CContextQueryToDXL::GetNextQueryId()
{
	return m_queryid_counter->next_id();
}

ULONG
CContextQueryToDXL::AddSecurityQuals(OID relid, List *security_quals)
{
	GPOS_ASSERT(NIL != security_quals);

	// The translation may work on a mutated copy of the query. Copy the
	// quals, so that they outlive it until the plan is translated.
	m_security_quals = gpdb::LAppend(
		m_security_quals, gpdb::CopyObject(security_quals));
	m_security_quals_relids = gpdb::LAppendOid(m_security_quals_relids, relid);

	return gpdb::ListLength(m_security_quals);
}
