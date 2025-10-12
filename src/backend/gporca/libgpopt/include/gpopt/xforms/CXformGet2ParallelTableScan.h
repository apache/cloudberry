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
 * CXformGet2ParallelTableScan.h
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/include/gpopt/xforms/CXformGet2ParallelTableScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GPOPT_CXformGet2ParallelTableScan_H
#define GPOPT_CXformGet2ParallelTableScan_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2ParallelTableScan
//
//	@doc:
//		Transform Get to Parallel TableScan using GUC enable_parallel
//
//---------------------------------------------------------------------------
class CXformGet2ParallelTableScan : public CXformImplementation
{
private:
	// check if memo contains logical operators that are incompatible with parallel execution
	static BOOL FHasParallelIncompatibleOps(CExpressionHandle &exprhdl);

public:
	CXformGet2ParallelTableScan(const CXformGet2ParallelTableScan &) = delete;

	// ctor
	explicit CXformGet2ParallelTableScan(CMemoryPool *);

	// dtor
	~CXformGet2ParallelTableScan() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfGet2ParallelTableScan;
	}

	// return a string for xform name
	const CHAR *
	SzId() const override
	{
		return "CXformGet2ParallelTableScan";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformGet2ParallelTableScan

}  // namespace gpopt

#endif	// !GPOPT_CXformGet2ParallelTableScan_H

// EOF