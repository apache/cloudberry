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
 * CDedupSupersetPreprocessor.cpp
 *
 * IDENTIFICATION
 *	  src/backend/gporca/libgpopt/src/operators/CDedupSupersetPreprocessor.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "gpopt/operators/CDedupSupersetPreprocessor.h"

#include <limits>

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CLogicalCTEAnchor.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarSubqueryAny.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

#define CTEID_TO_OID_START 60000

TableDescIdent::TableDescIdent(IMDId *mdid, ULONG sid, ULONG output_colid)
	: pmdid(mdid), subid(sid), ocolid(output_colid)
{
	GPOS_ASSERT(nullptr != mdid);
}

TableDescIdent::~TableDescIdent()
{
	pmdid->Release();
}

ULONG
TableDescIdent::HashFunc(const TableDescIdent *ptdi)
{
	GPOS_ASSERT(nullptr != ptdi);

	return gpos::CombineHashes(
		ptdi->pmdid->HashValue(),
		gpos::CombineHashes(gpos::HashValue(&ptdi->subid),
							gpos::HashValue(&ptdi->ocolid)));
}

// equality function
BOOL
TableDescIdent::EqualFunc(const TableDescIdent *pltdi,
						  const TableDescIdent *prtdi)
{
	GPOS_ASSERT(nullptr != pltdi && nullptr != prtdi);
	return pltdi->pmdid->Equals(prtdi->pmdid) && pltdi->subid == prtdi->subid &&
		   pltdi->ocolid == prtdi->ocolid;
}


// expr is CLogicalGet,CLogicalDynamicGet or CLogicalCTEConsumer ?
BOOL
CDedupSupersetPreprocessor::PexprIsLogicalOutput(CExpression *pexpr)
{
	GPOS_ASSERT(pexpr);
	COperator *pop = pexpr->Pop();
	return pop->Eopid() == COperator::EopLogicalGet ||
		   pop->Eopid() == COperator::EopLogicalDynamicGet ||
		   pop->Eopid() == COperator::EopLogicalCTEConsumer;
}

// Get the meta id from expression
// The expression must be CLogicalGet/CLogicalDynamicGet/CLogicalCTEConsumer
// for the CLogicalGet/CLogicalDynamicGet, we can direct get the meta id by table desc.
// But for the CLogicalCTEConsumer, we will create a meta id from CTEid
IMDId *
CDedupSupersetPreprocessor::PexprGetIMDid(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(pexpr);
	COperator *poper = pexpr->Pop();
	IMDId *pmdid = nullptr;
	switch (poper->Eopid())
	{
		case COperator::EopLogicalGet:
		{
			CLogicalGet *pop_get = CLogicalGet::PopConvert(poper);
			if (pop_get->Ptabdesc())
			{
				pmdid = pop_get->Ptabdesc()->MDId()->Copy(mp);
			}
			break;
		}
		case COperator::EopLogicalDynamicGet:
		{
			CLogicalDynamicGet *pop_dynamicget =
				CLogicalDynamicGet::PopConvert(poper);
			if (pop_dynamicget->Ptabdesc())
			{
				pmdid = pop_dynamicget->Ptabdesc()->MDId()->Copy(mp);
			}
			break;
		}
		case COperator::EopLogicalCTEConsumer:
		{
			CLogicalCTEConsumer *pop_cc =
				CLogicalCTEConsumer::PopConvert(poper);
			pmdid = GPOS_NEW(mp) CMDIdGPDB(
				IMDId::EmdidGeneral, pop_cc->UlCTEId() + CTEID_TO_OID_START);
			break;
		}
		default:
			break;
	}

	return pmdid;
}

// full the maps with superset expression
void
CDedupSupersetPreprocessor::ChildExprFullSuperset(CMemoryPool *mp,
												  CExpression *pexpr,
												  TDIToUlongPtrMap *eqcrmaps,
												  BOOL *dedupulmasks, ULONG ul)
{
	CScalarSubqueryAny *pop_subany =
		CScalarSubqueryAny::PopConvert(pexpr->Pop());
	CExpression *pexpr_child0 = (*pexpr)[0];
	IMDId *tdimid = PexprGetIMDid(mp, pexpr_child0);
	CScalarIdent *pop_ident = CScalarIdent::PopConvert((*pexpr)[1]->Pop());
	ULONG ulidx_subany;

	if (nullptr == tdimid)
	{
		// skip
		return;
	}
	CColRefSet *colrefsets = pexpr_child0->DeriveOutputColumns();

	// The colref(in CScalarSubqueryAny) must in the output
	(void) colrefsets->ExtractIndex(pop_subany->Pcr(), &ulidx_subany);
	GPOS_ASSERT(ulidx_subany != gpos::ulong_max);

	TableDescIdent tdikey =
		TableDescIdent(tdimid, ulidx_subany, pop_ident->Pcr()->Id());
	ULONG *pul = eqcrmaps->Find(&tdikey);
	if (!pul)
	{
		// new find superset, register it
		eqcrmaps->Insert(GPOS_NEW(mp) TableDescIdent(tdimid, ulidx_subany,
													 pop_ident->Pcr()->Id()),
						 GPOS_NEW(mp) ULONG(ul));
	}
	else
	{
		// already register the same superset expr, direct remove it
		//
		// ex.
		//   select * from t1,t2 where
		//   t1.v1 in (select v3 from t2)  <- colref(v3)
		//   and
		//   t1.v1 in (select v3 from t2); <- same colref(v3) but id is different
		dedupulmasks[ul] = false;
	}
}

// full the dedups array with subset(simply logicalget)
void
CDedupSupersetPreprocessor::ChildExprFullSimplySubset(
	CMemoryPool *mp, CExpression *pexpr, TDIToUlongPtrMap *eqcrmaps,
	BOOL *dedupulmasks)
{
	CScalarSubqueryAny *pop_subany =
		CScalarSubqueryAny::PopConvert(pexpr->Pop());
	CExpression *pexpr_subchild0 = (*(*pexpr)[0])[0];
	IMDId *tdimid = PexprGetIMDid(mp, pexpr_subchild0);
	CScalarIdent *pop_ident = CScalarIdent::PopConvert((*pexpr)[1]->Pop());
	ULONG ulidx_subany;
	GPOS_ASSERT(PexprIsLogicalOutput(pexpr_subchild0));

	if (nullptr == tdimid)
	{
		// skip current subset
		return;
	}

	CColRefSet *colrefsets = pexpr_subchild0->DeriveOutputColumns();
	// The colref(in CScalarSubqueryAny) must in the output
	(void) colrefsets->ExtractIndex(pop_subany->Pcr(), &ulidx_subany);
	GPOS_ASSERT(ulidx_subany != gpos::ulong_max);

	TableDescIdent tdikey =
		TableDescIdent(tdimid, ulidx_subany, pop_ident->Pcr()->Id());
	ULONG *pul = eqcrmaps->Find(&tdikey);
	if (pul)
	{
		// remove the expression in ul
		dedupulmasks[*pul] = false;
	}
}

// Extract the sub child from the join(inner join) expr
//
// 	|--CLogicalNAryJoin
//     |  |--CLogicalGet "t2" || CLogicalDynamicGet "t2" || CLogicalCTEConsumer (0) <- extract the expression if colref belong child0
//     |  |--CLogicalGet "t3" || CLogicalDynamicGet "t3" || CLogicalCTEConsumer (1) <- extract the expression if colref belong child1
//     |  +--Scalar
CExpression *
CDedupSupersetPreprocessor::PexprExtractSubChildJoinExpr(
	CExpression *pexpr, const CColRef *subany_col)
{
	GPOS_ASSERT(pexpr);
	COperator *poper = pexpr->Pop();
	CExpression *child_pexpr = nullptr;

	if (poper->Eopid() == COperator::EopLogicalNAryJoin)
	{
		GPOS_ASSERT(pexpr->Arity() >= 2);
		CExpression *pexpr_child0 = (*pexpr)[0];
		CExpression *pexpr_child1 = (*pexpr)[1];

		if (PexprIsLogicalOutput(pexpr_child0) &&
			pexpr_child0->DeriveOutputColumns()->FMember(subany_col))
		{
			child_pexpr = pexpr_child0;
		}
		else if (PexprIsLogicalOutput(pexpr_child1) &&
				 pexpr_child1->DeriveOutputColumns()->FMember(subany_col))
		{
			child_pexpr = pexpr_child1;
		}
	}

	return child_pexpr;
}

ULONG *
CDedupSupersetPreprocessor::DriveFullJoinSubset(CMemoryPool *mp,
												TDIToUlongPtrMap *eqcrmaps,
												CExpression *pexpr_join,
												const CColRef *pcr_sbany,
												const CColRef *pcr_ident)
{
	CExpression *pexpr_join_child;
	IMDId *tdimid = nullptr;
	ULONG ulidx_subany;

	pexpr_join_child = PexprExtractSubChildJoinExpr(pexpr_join, pcr_sbany);

	if (nullptr == pexpr_join_child)
	{
		return nullptr;
	}

	GPOS_ASSERT(PexprIsLogicalOutput(pexpr_join_child));
	tdimid = PexprGetIMDid(mp, pexpr_join_child);
	if (nullptr == tdimid)
	{
		return nullptr;
	}

	CColRefSet *colrefsets = pexpr_join_child->DeriveOutputColumns();
	// The colref(in CScalarSubqueryAny) must in the output
	(void) colrefsets->ExtractIndex(pcr_sbany, &ulidx_subany);
	GPOS_ASSERT(ulidx_subany != gpos::ulong_max);

	TableDescIdent tdikey =
		TableDescIdent(tdimid, ulidx_subany, pcr_ident->Id());

	return eqcrmaps->Find(&tdikey);
}

// full the dedups array with subset(inner join)
void
CDedupSupersetPreprocessor::ChildExprFullJoinSubset(CMemoryPool *mp,
													CExpression *pexpr,
													TDIToUlongPtrMap *eqcrmaps,
													BOOL *dedupulmasks)
{
	CScalarSubqueryAny *pop_subany =
		CScalarSubqueryAny::PopConvert(pexpr->Pop());
	CExpression *pexpr_join = (*pexpr)[0];
	CScalarIdent *pop_ident = CScalarIdent::PopConvert((*pexpr)[1]->Pop());

	ULONG *pul = DriveFullJoinSubset(mp, eqcrmaps, pexpr_join,
									 pop_subany->Pcr(), pop_ident->Pcr());
	if (pul)
	{
		// remove the expression in ul
		dedupulmasks[*pul] = false;
		return;
	}

	// not match, we can try the join key.
	// ex.
	//   t1(v1 int, v2 int)
	//   t2(v1 int, v2 int)
	//   t3(v1 int, v2 int)
	//
	//   select v1 from t1 where v1 in (select t2.v1 from t2) and v1 in (select t3.v1 from t2,t3 where t3.v1 = t2.v1);
	//
	// In this case, we still can use the v3.v1 as the subset key
	if (pexpr_join->Arity() == 3 &&
		(*pexpr_join)[2]->Pop()->Eopid() == COperator::EopScalarCmp)
	{
		CExpression *pexpr_cmp = (*pexpr_join)[2];
		CScalarCmp *pop_cmp = CScalarCmp::PopConvert(pexpr_cmp->Pop());
		if (pop_cmp->ParseCmpType() != IMDType::EcmptEq ||
			pexpr_cmp->Arity() != 2 ||
			(*pexpr_cmp)[0]->Pop()->Eopid() != COperator::EopScalarIdent ||
			(*pexpr_cmp)[1]->Pop()->Eopid() != COperator::EopScalarIdent)
		{
			// do nothing
			return;
		}
		CScalarIdent *pop_id1 =
			CScalarIdent::PopConvert((*pexpr_cmp)[0]->Pop());
		CScalarIdent *pop_id2 =
			CScalarIdent::PopConvert((*pexpr_cmp)[1]->Pop());
		ULONG *pul = nullptr;

		if (pop_id1->Pcr() == pop_subany->Pcr())
		{
			pul = DriveFullJoinSubset(mp, eqcrmaps, pexpr_join, pop_id2->Pcr(),
									  pop_ident->Pcr());
		}
		else if (pop_id2->Pcr() == pop_subany->Pcr())
		{
			pul = DriveFullJoinSubset(mp, eqcrmaps, pexpr_join, pop_id1->Pcr(),
									  pop_ident->Pcr());
		}

		if (pul)
		{
			// remove the expression in ul
			dedupulmasks[*pul] = false;
		}
	}
}

// child expr in CScalarBoolOp(ANDOP) is the superset
//
// |--CScalarSubqueryAny(=)["v3" (20)] ... <- it's a superset
//    |--CLogicalGet "t2"/CLogicalDynamicGet "t2"/CLogicalCTEConsumer (0) <- all fine
//    +--CScalarIdent "v1" ... <- get the output
BOOL
CDedupSupersetPreprocessor::ChildExprIsSuperset(CExpression *pexpr)
{
	return pexpr->Pop()->Eopid() == COperator::EopScalarSubqueryAny &&
		   pexpr->Arity() == 2 && PexprIsLogicalOutput((*pexpr)[0]) &&
		   (*pexpr)[1]->Pop()->Eopid() == COperator::EopScalarIdent;
}

// child expr in CScalarBoolOp(ANDOP) is the simply subset
//
// --CScalarSubqueryAny(=)["v3" (30)] <- it's a subset
// |--CLogicalSelect
// |  |--CLogicalGet "t2" || CLogicalDynamicGet "t2" || CLogicalCTEConsumer (0) <- all fine
// |  +--CScalar           <- ANY scalar
// +--CScalarIdent "v1" (0)
BOOL
CDedupSupersetPreprocessor::ChildExprIsSimplySubset(CExpression *pexpr)
{
	GPOS_ASSERT(pexpr);
	return pexpr->Pop()->Eopid() == COperator::EopScalarSubqueryAny &&
		   pexpr->Arity() == 2 &&
		   (*pexpr)[0]->Pop()->Eopid() == COperator::EopLogicalSelect &&
		   (*pexpr)[1]->Pop()->Eopid() == COperator::EopScalarIdent &&
		   (*pexpr)[0]->Arity() > 0 && PexprIsLogicalOutput((*(*pexpr)[0])[0]);
}

// child expr in CScalarBoolOp(ANDOP) is the inner-join subset
// left-semi-join won't occur in the pre-process
//
// +--CScalarSubqueryAny(=)["v3" (20)] <- it's a subset
// 	|--CLogicalNAryJoin <- inner or smei join. Although semi join(EopLogicalLeftSemiJoin) is supported, but no semi join will occur at pre-process.
//     |  |--CLogicalGet "t2" || CLogicalDynamicGet "t2" || CLogicalCTEConsumer (0) <- all fine
//     |  |--CLogicalGet "t3" ("t3")
//     |  +--CScalarCmp (=) 			<- ANY scalar(won't be full/cross join condition)
//     |     |--CScalarIdent "v4" (21)
//     |     +--CScalarIdent "v6" (31)
//     +--CScalarIdent "v1" (0)
BOOL
CDedupSupersetPreprocessor::ChildExprIsJoinSubset(CExpression *pexpr)
{
	GPOS_ASSERT(pexpr);
	return pexpr->Pop()->Eopid() == COperator::EopScalarSubqueryAny &&
		   pexpr->Arity() == 2 &&
		   (*pexpr)[0]->Pop()->Eopid() ==
			   COperator::EopLogicalNAryJoin &&	 // inner join
		   (*pexpr)[1]->Pop()->Eopid() == COperator::EopScalarIdent &&
		   (*pexpr)[0]->Arity() >= 2 &&	 // at least left and right
		   (PexprIsLogicalOutput((*(*pexpr)[0])[0]) ||
			PexprIsLogicalOutput((*(*pexpr)[0])[1]));
}

// mian driver
// If the same key in the current scalar has the following conditions:
// - select a superset in {table A}
// - select a subset in {table A}
//	 - the subset can be the get/dynamicget/cteconsumer or a inner join
//
// Then the scalar(superset in {table A}) can be removed.
//
//	Example, for the schema: t1(v1, v2), t2(v3, v4)
//
//  The query:
//    select * from t1,t2 where t1.v1 in (select v3 from t2) and t1.v1 in (select v3 from t2 where v3 < 100);
//  equivalent to:
//    select * from t1,t2 where t1.v1 t1.v1 in (select v3 from t2 where v3 < 100);
//
//  Similarly, CTE also can removed duplicate scalar
//    with cte_t as (...)
//    select * from t1 where v1 in (select v2 from cte_1) and v1 in (select v2 from cte_1 where {any condition});
//  equivalent to:
//    select * from t where a in (select count(i) from s group by j);
//
//  Also it also satisfies the cross join condition.
//    select * from t1,t2 where t1.v1 in (select v2 from t2) and t1.v1 in (select v2 from t2 where v2 < 100);
//  equivalent to:
//    select * from t1,t2 where t1.v1 in (select v2 from t2 where v2 < 100);
//
//  And the inner join can be the subset.
//    select * from t1 where v1 in (select v3 from t2) and v1 in (select v3 from t2,t3 where v4=v6);
//  equivalent to:
//    select * from t1 where v1 v1 in (select v3 from t2,t3 where v4=v6);
CExpression *
CDedupSupersetPreprocessor::PexprPreprocess(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexpr);

	// operator, possibly altered below if we need to change the operator
	COperator *pop = pexpr->Pop();

	if (pop->Eopid() == COperator::EopScalarBoolOp)
	{
		const ULONG arity = pexpr->Arity();
		BOOL *ulmasks = GPOS_NEW_ARRAY(mp, BOOL, arity);
		BOOL *dedupulmasks = GPOS_NEW_ARRAY(mp, BOOL, arity);
		clib::Memset(ulmasks, true, arity * sizeof(BOOL));
		clib::Memset(dedupulmasks, true, arity * sizeof(BOOL));

		// TableDescIdent(table desc mdid, subany index, ident colid) -> ULONG(index of current )
		TDIToUlongPtrMap *eqcrmaps = GPOS_NEW(mp) TDIToUlongPtrMap(mp);

		if (arity < 2 || CScalarBoolOp::PopConvert(pop)->Eboolop() !=
							 CScalarBoolOp::EboolopAnd)
		{
			goto not_match;
		}

		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *child_pexpr = (*pexpr)[ul];
			if (ChildExprIsSuperset(child_pexpr))
			{
				ChildExprFullSuperset(mp, child_pexpr, eqcrmaps, dedupulmasks,
									  ul);
				// no need check this expression in next loop
				ulmasks[ul] = false;
			}
		}

		// not found any superset expression
		if (eqcrmaps->Size() == 0)
		{
			goto not_match;
		}

		for (ULONG ul = 0; ul < arity; ul++)
		{
			// current is superset
			if (!ulmasks[ul])
			{
				continue;
			}

			CExpression *child_pexpr = (*pexpr)[ul];

			if (ChildExprIsSimplySubset(child_pexpr))
			{
				ChildExprFullSimplySubset(mp, child_pexpr, eqcrmaps,
										  dedupulmasks);
				continue;
			}

			if (ChildExprIsJoinSubset(child_pexpr))
			{
				ChildExprFullJoinSubset(mp, child_pexpr, eqcrmaps,
										dedupulmasks);
			}
		}

	not_match:
		CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

		for (ULONG ul = 0; ul < arity; ul++)
		{
			if (!dedupulmasks[ul])
			{
				continue;
			}

			(*pexpr)[ul]->AddRef();
			pdrgpexprChildren->Append((*pexpr)[ul]);
		}

		pop->AddRef();

		GPOS_DELETE_ARRAY(ulmasks);
		GPOS_DELETE_ARRAY(dedupulmasks);
		eqcrmaps->Release();

		return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
	}
	// FIXME: should we consider remove the CTE consumer from CTEINFO?

	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprPreprocess(mp, (*pexpr)[ul]);

		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}