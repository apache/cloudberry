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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * rptqual.h
 *	  Trusted built-in functions for eager reducer quals.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/include/rptqual.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPTQUAL_H
#define RPTQUAL_H

#include "utils/fmgroids.h"

/* OIDs of built-in comparisons with no argument-dependent or unconditional
 * evaluation errors. LEAKPROOF alone does not promise this for user functions.
 * Keep arithmetic, coercions and arbitrary extension functions out. */
static inline bool
RptQualFunctionSafe(Oid function)
{
	switch (function)
	{
		case F_BOOLEQ:
		case F_BOOLNE:
		case F_BOOLLT:
		case F_BOOLLE:
		case F_BOOLGT:
		case F_BOOLGE:
		case F_INT2EQ:
		case F_INT2NE:
		case F_INT2LT:
		case F_INT2LE:
		case F_INT2GT:
		case F_INT2GE:
		case F_INT4EQ:
		case F_INT4NE:
		case F_INT4LT:
		case F_INT4LE:
		case F_INT4GT:
		case F_INT4GE:
		case F_INT8EQ:
		case F_INT8NE:
		case F_INT8LT:
		case F_INT8LE:
		case F_INT8GT:
		case F_INT8GE:
		case F_INT24EQ:
		case F_INT24NE:
		case F_INT24LT:
		case F_INT24LE:
		case F_INT24GT:
		case F_INT24GE:
		case F_INT42EQ:
		case F_INT42NE:
		case F_INT42LT:
		case F_INT42LE:
		case F_INT42GT:
		case F_INT42GE:
		case F_INT28EQ:
		case F_INT28NE:
		case F_INT28LT:
		case F_INT28LE:
		case F_INT28GT:
		case F_INT28GE:
		case F_INT82EQ:
		case F_INT82NE:
		case F_INT82LT:
		case F_INT82LE:
		case F_INT82GT:
		case F_INT82GE:
		case F_INT48EQ:
		case F_INT48NE:
		case F_INT48LT:
		case F_INT48LE:
		case F_INT48GT:
		case F_INT48GE:
		case F_INT84EQ:
		case F_INT84NE:
		case F_INT84LT:
		case F_INT84LE:
		case F_INT84GT:
		case F_INT84GE:
		case F_OIDEQ:
		case F_OIDNE:
		case F_OIDLT:
		case F_OIDLE:
		case F_OIDGT:
		case F_OIDGE:
		case F_NUMERIC_GE:
		case F_NUMERIC_LE:
			return true;
		default:
			return false;
	}
}

/* ScalarArrayOpExpr can call these in addition to its comparison function. */
static inline bool
RptQualHashFunctionSafe(Oid function)
{
	switch (function)
	{
		case F_HASHINT2:
		case F_HASHINT4:
		case F_HASHINT8:
		case F_HASHOID:
		case F_HASHCHAR:
			return true;
		default:
			return false;
	}
}

#endif /* RPTQUAL_H */
