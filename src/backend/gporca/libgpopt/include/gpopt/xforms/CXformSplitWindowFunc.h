/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 *  @filename:
 * 		CXformSplitWindowFunc.h
 * 
 *  @doc:
 * 		Split a window function into pair of local and global window function
 * 
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CXformSplitWindowFunc_H
#define GPOPT_CXformSplitWindowFunc_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitWindowFunc
//
//	@doc:
//		Split a window function into pair of local and global window function
//
//---------------------------------------------------------------------------
class CXformSplitWindowFunc : public CXformExploration
{
private:
	// generate a project lists for the local and global window function
	// from the original window function
	static CExpression *PexprWindowFunc(
		CMemoryPool *mp,  // memory pool
		CExpression *
			pexprProjListOrig,	// project list of the original global window function
		CExpression *
			ppexprProjListLocal,  // project list of the new local window function
		CExpression *
			ppexprProjListGlobal  // project list of the new global window function
	);

	static void PopulateLocalGlobalProjectList(
		CMemoryPool *mp, CExpression *pexprProjList,
		CExpression **ppexprProjListLocal, CExpression **ppexprProjListGlobal);

	static bool CheckFilterAndProjectList(CExpression *pexprSelect,
										  CExpression *pexprProjList);

	static void PopulateSelect(CMemoryPool *mp, CExpression *pexprSelect,
							   COperator **ppSelectCopy);

public:
	CXformSplitWindowFunc(const CXformSplitWindowFunc &) = delete;

	// ctor
	explicit CXformSplitWindowFunc(CMemoryPool *mp);

	// dtor
	~CXformSplitWindowFunc() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfSplitWindowFunc;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformSplitWindowFunc";
	}

	// Compatibility function for splitting limit
	BOOL
	FCompatible(CXform::EXformId exfid) override
	{
		return (CXform::ExfSplitWindowFunc != exfid);
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformSplitWindowFunc

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitWindowFunc_H

// EOF
