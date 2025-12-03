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
 * CXformImplementParallelSequence.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/xforms/CXformImplementParallelSequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CXformImplementParallelSequence_H
#define GPOPT_CXformImplementParallelSequence_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementParallelSequence
//
//	@doc:
//		Transform logical to physical Parallel Sequence
//
//---------------------------------------------------------------------------
class CXformImplementParallelSequence : public CXformImplementation
{
private:
public:
	CXformImplementParallelSequence(const CXformImplementParallelSequence &) = delete;

	// ctor
	explicit CXformImplementParallelSequence(CMemoryPool *);

	// dtor
	~CXformImplementParallelSequence() override = default;

	//	ident accessors
	EXformId
	Exfid() const override
	{
		return ExfImplementParallelSequence;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformImplementParallelSequence";
	}

	// compute xform promise for a given expresion handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;
};	// class CXformImplementParallelSequence

}	// namespace gpopt

#endif	//	!GPOPT_CXformImplementParallelSequence_H

//	EOF
