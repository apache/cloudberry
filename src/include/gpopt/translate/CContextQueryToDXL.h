//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextQueryToDXL.h
//
//	@doc:
//		Class to hold information about a whole top-level query, while
//		recursively translating a Query tree to DXL tree.
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CContextQueryToDXL_H
#define GPDXL_CContextQueryToDXL_H

#include "gpos/base.h"

#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#define GPDXL_CTE_ID_START 1
#define GPDXL_COL_ID_START 1
#define GPDXL_QUERY_ID_START 1

namespace gpdxl
{
// fwd declarations
class CTranslatorQueryToDXL;
class CTranslatorScalarToDXL;

//---------------------------------------------------------------------------
//	@class:
//		CContextQueryToDXL
//
//	@doc:
//		Class to hold information about a whole top-level query, while
//		recursively translating a Query tree to DXL tree.
//
//---------------------------------------------------------------------------
class CContextQueryToDXL
{
	friend class CTranslatorQueryToDXL;
	friend class CTranslatorScalarToDXL;

private:
	// memory pool
	CMemoryPool *m_mp;

	// counter for generating unique column ids
	CIdGenerator *m_colid_counter;

	// counter for generating unique CTE ids
	CIdGenerator *m_cte_id_counter;

	// counter for upper-level query and its subqueries
	CIdGenerator *m_queryid_counter;

	// does the query have any distributed tables?
	BOOL m_has_distributed_tables;

	// What operator classes are used in the distribution keys?
	DistributionHashOpsKind m_distribution_hashops;

	// security quals of the range table entries that have some, and the
	// oids of their relations, in the order the entries were translated.
	// The position in these lists, starting at 1, is the security quals id
	// of the table descriptors created for the entries.
	List *m_security_quals;
	List *m_security_quals_relids;

public:
	// ctor
	CContextQueryToDXL(CMemoryPool *mp);

	// dtor
	~CContextQueryToDXL();

	ULONG GetNextQueryId();

	// register the security quals of a range table entry of relid, and
	// return their id
	ULONG AddSecurityQuals(OID relid, List *security_quals);
};
}  // namespace gpdxl
#endif	// GPDXL_CContextQueryToDXL_H

//EOF
