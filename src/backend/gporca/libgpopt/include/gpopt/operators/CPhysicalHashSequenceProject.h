//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashSequenceProject.h
//
//	@doc:
//		Physical Sequence Project operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalHashSequenceProject_H
#define GPOPT_CPhysicalHashSequenceProject_H

#include "gpos/base.h"

#include "gpopt/base/CWindowFrame.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"

namespace gpopt
{
// fwd declarations
class CDistributionSpec;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalHashSequenceProject
//
//	@doc:
//		Physical Hash Sequence Project operator
//
//---------------------------------------------------------------------------
class CPhysicalHashSequenceProject : public CPhysicalSequenceProject
{

public:
	CPhysicalHashSequenceProject(const CPhysicalHashSequenceProject &) = delete;

	// ctor
	CPhysicalHashSequenceProject(CMemoryPool *mp, ESPType m_sptype,
							 CDistributionSpec *pds, COrderSpecArray *pdrgpos,
							 CWindowFrameArray *pdrgpwf);

	// dtor
	~CPhysicalHashSequenceProject() override;

		// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalHashSequenceProject;
	}

	// operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalHashSequenceProject";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// hashing function
	ULONG HashValue() const override;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override;

	// compute required sort order of the n-th child
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posRequired, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	BOOL
	FPassThruStats() const override
	{
		return true;
	}

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// conversion function
	static CPhysicalHashSequenceProject *PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalHashSequenceProject == pop->Eopid());

		return dynamic_cast<CPhysicalHashSequenceProject *>(pop);
	}

};	// class CPhysicalHashSequenceProject

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalHashSequenceProject_H

// EOF
