//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashSequenceProject.cpp
//
//	@doc:
//		Implementation of physical sequence project operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalHashSequenceProject.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalSequenceProject.h"
#include "gpopt/operators/CScalarIdent.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashSequenceProject::CPhysicalHashSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashSequenceProject::CPhysicalHashSequenceProject(CMemoryPool *mp,
												   ESPType sptype,
												   CDistributionSpec *pds,
												   COrderSpecArray *pdrgpos,
												   CWindowFrameArray *pdrgpwf)
	: CPhysicalSequenceProject(mp, sptype, pds, pdrgpos, pdrgpwf) 
{}

CPhysicalHashSequenceProject::~CPhysicalHashSequenceProject() {}

COrderSpec *
CPhysicalHashSequenceProject::PosRequired(CMemoryPool *mp,
									  CExpressionHandle &,	// exprhdl
									  COrderSpec *,			// posRequired
									  ULONG
#ifdef GPOS_DEBUG
										  child_index
#endif	// GPOS_DEBUG
									  ,
									  CDrvdPropArray *,	 // pdrgpdpCtxt
									  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return GPOS_NEW(mp) COrderSpec(mp);
}

// match function
BOOL 
CPhysicalHashSequenceProject::Matches(COperator *pop) const
{
	GPOS_ASSERT(nullptr != pop);
	if (Eopid() == pop->Eopid())
	{
		CPhysicalHashSequenceProject *popHashSequenceProject =
			CPhysicalHashSequenceProject::PopConvert(pop);
		return m_sptype == popHashSequenceProject->Pspt() &&
			   m_pds->Matches(popHashSequenceProject->Pds()) &&
			   CWindowFrame::Equals(m_pdrgpwf,
									popHashSequenceProject->Pdrgpwf()) &&
			   COrderSpec::Equals(m_pdrgpos,
								  popHashSequenceProject->Pdrgpos());
	}

	return false;
}

// hashing function

ULONG
CPhysicalHashSequenceProject::HashValue() const
{
	BOOL ltrue = true;
	ULONG ulHash = CPhysicalSequenceProject::HashValue();
	
	ulHash = gpos::CombineHashes(ulHash, m_pds->HashValue());
	// combine a true hash value
	ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<BOOL>(&ltrue));

	return ulHash;
}

BOOL
CPhysicalHashSequenceProject::FInputOrderSensitive() const {
	return false;
}

CEnfdProp::EPropEnforcingType
CPhysicalHashSequenceProject::EpetOrder(CExpressionHandle &exprhdl,
									const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}


IOstream &CPhysicalHashSequenceProject::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	CLogicalSequenceProject::OsPrintWindowType(os, m_sptype);
	os << ", hashed) (";
	(void) m_pds->OsPrint(os);
	os << ", ";
	(void) COrderSpec::OsPrint(os, m_pdrgpos);
	os << ", ";
	(void) CWindowFrame::OsPrint(os, m_pdrgpwf);

	return os << ")";
}


// EOF
