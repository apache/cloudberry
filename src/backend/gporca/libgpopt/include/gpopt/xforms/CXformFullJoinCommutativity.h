//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformFullJoinCommutativity.h
//
//	@doc:
//		Transform full join by commutativity
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFullJoinCommutativity_H
#define GPOPT_CXformFullJoinCommutativity_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformFullJoinCommutativity
//
//	@doc:
//		Commutative transformation of join
//
//---------------------------------------------------------------------------
class CXformFullJoinCommutativity : public CXformExploration
{
private:
public:
	CXformFullJoinCommutativity(const CXformFullJoinCommutativity &) = delete;

	// ctor
	explicit CXformFullJoinCommutativity(CMemoryPool *mp);

	// dtor
	~CXformFullJoinCommutativity() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfFullJoinCommutativity;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformFullJoinCommutativity";
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

};	// class CXformFullJoinCommutativity

}  // namespace gpopt


#endif	// !GPOPT_CXformFullJoinCommutativity_H

// EOF
