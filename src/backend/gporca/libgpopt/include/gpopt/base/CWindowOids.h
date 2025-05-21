//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CWindowOids.h
//
//	@doc:
//		System specific oids for window operations
//---------------------------------------------------------------------------
#ifndef GPOPT_CWindowOids_H
#define GPOPT_CWindowOids_H

#include "gpos/common/CRefCount.h"

#include "naucrates/dxl/gpdb_types.h"

#define DUMMY_ROW_NUMBER_OID OID(7000)
#define DUMMY_WIN_RANK OID(7001)
#define DUMMY_WIN_DENSE_RANK OID(7002)

namespace gpmd
{
class IMDId;
}

namespace gpopt
{
using namespace gpmd;
//---------------------------------------------------------------------------
//	@class:
//		CWindowOids
//
//	@doc:
//		GPDB specific oids
//
//---------------------------------------------------------------------------
class CWindowOids : public CRefCount
{
private:
	// oid of window operation "row_number" function
	OID m_oidRowNumber;

	// metadata id of window operation "row_number" function
	IMDId *m_MDIdRowNumber;

	// oid of window operation "rank" function
	OID m_oidRank;

	// metadata id of window operation "rank" function
	IMDId *m_MDIdRank;

	// oid of window operation "dense_rank" function
	OID m_oidDenseRank;

	// metadata id of window operation "dense_rank" function
	IMDId *m_MDDenseRank;

public:
	CWindowOids(CMemoryPool *mp, OID row_number_oid, OID rank_oid,
				OID dense_rank_oid);

	// accessor of oid value of "row_number" function
	OID OidRowNumber() const;
	IMDId *MDIdRowNumber() const;

	// accessor of oid value of "rank" function
	OID OidRank() const;
	IMDId *MDIdRank() const;

	// accessor of oid value of "dense_rank" function
	OID OidDenseRank() const;
	IMDId *MDIdDenseRank() const;

	// generate default window oids
	static CWindowOids *GetWindowOids(CMemoryPool *mp);

};	// class CWindowOids
}  // namespace gpopt

#endif	// !GPOPT_CWindowOids_H

// EOF
