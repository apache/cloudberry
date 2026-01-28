//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformPushPartialAggBelowJoin.h
//
//	@doc:
//		Push group by below join transform
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushPartialAggBelowJoin_H
#define GPOPT_CXformPushPartialAggBelowJoin_H

#include <tuple>

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"
namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushPartialAggBelowJoin
//
//	@doc:
//		Push group by below join transform
//
//---------------------------------------------------------------------------
class CXformPushPartialAggBelowJoin : public CXformExploration
{
private:
	static BOOL FLocalGbAggAlreadyPushed(CExpression *pexprGlobalGb);

	static std::pair<BOOL, BOOL> FCanPushLocalGbAggBelowJoin(
		CMemoryPool *mp, CExpression *pexpr);

	static CExpression *ExchangeGbkeyFromJoinKey(CMemoryPool *mp,
												 CExpression *pexpr);
	static CColRefSet *PcrsJoinKey(CMemoryPool *mp, CExpression *pexprOuter,
								   CExpression *pexprInner,
								   CExpression *pexprScalar);

	static CExpression *PushLocalGbAggBelowJoin(CMemoryPool *mp,
												CExpression *pexprGlobalGb);

	static CColRefSet *PexprGetGbAggkey(CMemoryPool *mp,
										CExpression *pexprGbAgg);

public:
	CXformPushPartialAggBelowJoin(const CXformPushPartialAggBelowJoin &) =
		delete;

	// ctor
	explicit CXformPushPartialAggBelowJoin(CMemoryPool *mp);

	// ctor
	explicit CXformPushPartialAggBelowJoin(CExpression *pexprPattern);

	// dtor
	~CXformPushPartialAggBelowJoin() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfPushPartialAggBelowJoin;
	}

	const CHAR *
	SzId() const override
	{
		return "CXformPushPartialAggBelowJoin";
	}

	// Compatibility function
	BOOL
	FCompatible(CXform::EXformId exfid) override
	{
		return (CXform::ExfPushPartialAggBelowJoin != exfid);
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformPushPartialAggBelowJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformPushPartialAggBelowJoin_H

// EOF
