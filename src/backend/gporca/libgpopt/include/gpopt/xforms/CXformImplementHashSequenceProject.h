//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementHashSequenceProject.h
//
//	@doc:
//		Transform Logical Sequence Project to Physical Sequence Project
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementHashSequenceProject_H
#define GPOPT_CXformImplementHashSequenceProject_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementHashSequenceProject
//
//	@doc:
//		Transform Project to ComputeScalar
//
//---------------------------------------------------------------------------
class CXformImplementHashSequenceProject : public CXformImplementation
{
private:
public:
	CXformImplementHashSequenceProject(const CXformImplementHashSequenceProject &) =
		delete;

	// ctor
	explicit CXformImplementHashSequenceProject(CMemoryPool *mp);

	// dtor
	~CXformImplementHashSequenceProject() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfImplementHashSequenceProject;
	}

	const CHAR *
	SzId() const override
	{
		return "CXformImplementHashSequenceProject";
	}

	// compute xform promise for a given expression handle
	EXformPromise
	Exfp(CExpressionHandle &exprhdl) const override
	{
		if (exprhdl.DeriveHasSubquery(1))
		{
			return CXform::ExfpNone;
		}

		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *, CXformResult *,
				   CExpression *) const override;

};	// class CXformImplementHashSequenceProject

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementHashSequenceProject_H

// EOF
