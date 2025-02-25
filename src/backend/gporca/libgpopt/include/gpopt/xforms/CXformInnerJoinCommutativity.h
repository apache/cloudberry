//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformInnerJoinCommutativity.h
//
//	@doc:
//		Transform inner join by commutativity
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoinCommutativity_H
#define GPOPT_CXformInnerJoinCommutativity_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinCommutativity
//
//	@doc:
//		Commutative transformation of join
//
//---------------------------------------------------------------------------
class CXformInnerJoinCommutativity : public CXformExploration
{
private:
public:
	CXformInnerJoinCommutativity(const CXformInnerJoinCommutativity &) = delete;

	// ctor
	explicit CXformInnerJoinCommutativity(CMemoryPool *mp);

	// dtor
	~CXformInnerJoinCommutativity() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfInnerJoinCommutativity;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformInnerJoinCommutativity";
	}

	// compatibility function
	BOOL FCompatible(CXform::EXformId exfid) override;

	// compute xform promise for a given expression handle
	EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const override
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformInnerJoinCommutativity

}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoinCommutativity_H

// EOF
