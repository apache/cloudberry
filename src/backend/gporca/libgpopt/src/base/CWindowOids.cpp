//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.

#include "gpopt/base/CWindowOids.h"

#include "naucrates/md/CMDIdGPDB.h"
using namespace gpopt;

CWindowOids::CWindowOids(CMemoryPool *mp, OID row_number_oid, OID rank_oid,
						 OID dense_rank_oid)
{
	m_oidRowNumber = row_number_oid;
	m_MDIdRowNumber =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, m_oidRowNumber);
	m_oidRank = rank_oid;
	m_MDIdRank = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, m_oidRank);
	m_oidDenseRank = dense_rank_oid;
	m_MDDenseRank = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, m_oidDenseRank);
}

OID
CWindowOids::OidRowNumber() const
{
	return m_oidRowNumber;
}

IMDId *
CWindowOids::MDIdRowNumber() const
{
	return m_MDIdRowNumber;
}

OID
CWindowOids::OidRank() const
{
	return m_oidRank;
}

IMDId *
CWindowOids::MDIdRank() const
{
	return m_MDIdRank;
}

OID
CWindowOids::OidDenseRank() const
{
	return m_oidDenseRank;
}

IMDId *
CWindowOids::MDIdDenseRank() const
{
	return m_MDDenseRank;
}

CWindowOids *
CWindowOids::GetWindowOids(CMemoryPool *mp)
{
	return GPOS_NEW(mp) CWindowOids(mp, DUMMY_ROW_NUMBER_OID, DUMMY_WIN_RANK,
									DUMMY_WIN_DENSE_RANK);
}
