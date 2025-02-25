//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom.
//
//	@filename:
//		CPhysicalFullHashJoin.h
//
//	@doc:
//		Full outer hash join operator
//
//      We don't support DPE in full hash join, since both sides are fully
//      output with or without a match in the other. Therefore, we should
//      not skip scanning certain partitions.

//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalFullHashJoin_H
#define GPOPT_CPhysicalFullHashJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalFullHashJoin
//
//	@doc:
//		Full outer hash join operator
//
//---------------------------------------------------------------------------
class CPhysicalFullHashJoin : public CPhysicalHashJoin
{
public:
	CPhysicalFullHashJoin(const CPhysicalFullHashJoin &) = delete;

	// ctor
	CPhysicalFullHashJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
						  CExpressionArray *pdrgpexprInnerKeys,
						  IMdIdArray *hash_opfamilies,
						  BOOL is_null_aware = true,
						  CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	~CPhysicalFullHashJoin() override = default;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalFullHashJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalFullHashJoin";
	}

	// compute required distribution of the n-th child
	CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CReqdPropPlan *prppInput, ULONG child_index,
						   CDrvdPropArray *pdrgpdpCtxt,
						   ULONG ulOptReq) override;
	// derive distribution
	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

	// conversion function
	static CPhysicalFullHashJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalFullHashJoin == pop->Eopid());

		return dynamic_cast<CPhysicalFullHashJoin *>(pop);
	}


};	// class CPhysicalFullHashJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalFullHashJoin_H

// EOF
