//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSubqueryHandler.cpp
//
//	@doc:
//		Implementation of transforming subquery expressions to Apply
//		expressions;
//		Given a SELECT/PROJECT/Gb-Agg/SEQUENCE-PROJECT expression whose scalar
//		child involves subqueries, this class is used for unnesting subqueries by
//		creating a new expression whose logical child is an Apply tree and whose
//		scalar child is subquery-free
//
//		The default behavior is to generate regular apply expressions which are then
//		turned into regular (inner/outer/semi/anti-semi) joins,
//		only when a regular apply expression cannot be generated, a correlated apply
//		expression is generated instead, which results in a correlated join (subplan in GPDB)
//
//		The handler can also be used to always enforce generating correlated apply
//		expressions, this behavior can be enforced by setting the flag
//		fEnforceCorrelatedApply to true
//---------------------------------------------------------------------------

#include "gpopt/xforms/CSubqueryHandler.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CLogicalConstTableGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalInnerApply.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApplyNotIn.h"
#include "gpopt/operators/CLogicalLeftOuterApply.h"
#include "gpopt/operators/CLogicalLeftOuterCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApplyIn.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarBooleanTest.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarCoalesce.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarIf.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarSubquery.h"
#include "gpopt/operators/CScalarSubqueryAll.h"
#include "gpopt/operators/CScalarSubqueryAny.h"
#include "gpopt/operators/CScalarSubqueryQuantified.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt8.h"

using namespace gpopt;

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::AssertValidArguments
//
//	@doc:
//		Validity checks of arguments
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::AssertValidArguments(CMemoryPool *mp, CExpression *pexprOuter,
									   CExpression *pexprScalar,
									   CExpression **ppexprNewOuter,
									   CExpression **ppexprResidualScalar)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pexprOuter);
	GPOS_ASSERT(pexprOuter->Pop()->FLogical());
	GPOS_ASSERT(nullptr != pexprScalar);
	GPOS_ASSERT(pexprScalar->Pop()->FScalar());
	GPOS_ASSERT(nullptr != ppexprNewOuter);
	GPOS_ASSERT(nullptr != ppexprResidualScalar);
}
#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprReplace
//
//	@doc:
//		Given an input expression, replace all occurrences of given column
//		with the given scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprReplace(CMemoryPool *mp, CExpression *pexprInput,
							   CColRef *colref, CExpression *pexprScalar)
{
	GPOS_ASSERT(nullptr != pexprInput);
	GPOS_ASSERT(nullptr != colref);

	COperator *pop = pexprInput->Pop();
	if (pop->Eopid() == CScalar::EopScalarIdent &&
		CScalarIdent::PopConvert(pop)->Pcr() == colref)
	{
		GPOS_ASSERT(nullptr != pexprScalar);
		GPOS_ASSERT(pexprScalar->Pop()->FScalar());

		pexprScalar->AddRef();
		return pexprScalar;
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pexprInput->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild =
			PexprReplace(mp, (*pexprInput)[ul], colref, pexprScalar);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprSubqueryPred
//
//	@doc:
//		Build a predicate expression for the quantified comparison of the
//		subquery.
//
//		This method first attempts to un-nest any subquery that may be present in
//		the Scalar part of the quantified subquery. Then, it proceeds to create a
//		scalar predicate comparison between the new Scalar and Logical children of
//		the Subquery. It returns the new Logical child via ppexprResult.
//
//		For example, for the query :
//		select * from foo where
//		    (select a from foo limit 1) in (select b from bar);
//
//		+--CLogicalSelect
//		   |--CLogicalGet "foo"
//		   +--CScalarSubqueryAny(=)["b" (10)]
//		      |--CLogicalGet "bar"
//		      +--CScalarSubquery["a" (18)]
//		         +--CLogicalLimit <empty> global
//		            |--CLogicalGet "foo"
//		            |--CScalarConst (0)
//		            +--CScalarCast
//		               +--CScalarConst (1)
//
//		will return ..
//		+--CScalarCmp (=)
//		   |--CScalarIdent "a" (18)
//		   +--CScalarIdent "b" (10)
//
//		with pexprResult as ..
//		+--CLogicalInnerApply
//		   |--CLogicalGet "bar"
//		   |--CLogicalMaxOneRow
//		   |  +--CLogicalLimit
//		   |     |--CLogicalGet "foo"
//		   |     |--CScalarConst (0)   origin: [Grp:3, GrpExpr:0]
//		   |     +--CScalarCast   origin: [Grp:5, GrpExpr:0]
//		   |        +--CScalarConst (1)   origin: [Grp:4, GrpExpr:0]
//		   +--CScalarConst (1)
//
//		If there is no such subquery, it returns a comparison expression using
//		the original Scalar expression and sets ppexprResult to the passed in
//		pexprOuter.
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprSubqueryPred(CExpression *pexprOuter,
									CExpression *pexprSubquery,
									CExpression **ppexprResult,
									CSubqueryHandler::ESubqueryCtxt esqctxt)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));

	CExpression *pexprNewScalar = nullptr;
	CExpression *pexprNewLogical = nullptr;

	CExpression *pexprScalarChild = (*pexprSubquery)[1];

	// If pexprScalarChild is a non-scalar subquery such as follows,
	// EXPLAIN SELECT * FROM t3 WHERE (c = ANY(SELECT c FROM t2)) IN (SELECT b from t1);
	// EXPLAIN SELECT * FROM t3 WHERE (NOT EXISTS(SELECT c FROM t2)) IN (SELECT b from t1);
	// then it must be treated in "Value" context such that the
	// corresponding subquery unnesting routines correctly return a column
	// identifier for the subquery outcome, which can then be referenced
	// correctly inside the quantified comparison expression.

	// This is not needed if pexprScalarChild is a scalar subquery such as
	// follows, since FRemoveScalarSubquery() always produces a column
	// identifier for the subquery projection.
	// EXPLAIN SELECT * FROM t3 WHERE (SELECT c FROM t2) IN (SELECT b from t1);
	if (CUtils::FQuantifiedSubquery(pexprScalarChild->Pop()) ||
		CUtils::FExistentialSubquery(pexprScalarChild->Pop()) ||
		CPredicateUtils::FAnd(pexprScalarChild))
	{
		esqctxt = EsqctxtValue;
	}

	if (!FProcess(pexprOuter, pexprScalarChild, esqctxt, &pexprNewLogical,
				  &pexprNewScalar))
	{
		// subquery unnesting failed; attempt to create a predicate directly
		*ppexprResult = pexprOuter;
		pexprNewScalar = pexprScalarChild;
	}

	if (nullptr != pexprNewLogical)
	{
		*ppexprResult = pexprNewLogical;
	}
	else
	{
		*ppexprResult = pexprOuter;
	}

	GPOS_ASSERT(nullptr != pexprNewScalar);

	CScalarSubqueryQuantified *popSqQuantified =
		CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop());

	const CColRef *colref = popSqQuantified->Pcr();
	IMDId *mdid_op = popSqQuantified->MdIdOp();
	const CWStringConst *str = popSqQuantified->PstrOp();

	mdid_op->AddRef();
	CExpression *pexprPredicate =
		CUtils::PexprScalarCmp(m_mp, pexprNewScalar, colref, *str, mdid_op);

	return pexprPredicate;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProjectCountSubquery
//
//	@doc:
//		Detect subqueries with expressions over count aggregate similar
//		to (SELECT 'abc' || (SELECT count(*) from X))
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProjectCountSubquery(CExpression *pexprSubquery,
										CColRef *pcrCount)
{
	GPOS_ASSERT(nullptr != pexprSubquery);
	GPOS_ASSERT(COperator::EopScalarSubquery == pexprSubquery->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopLogicalProject ==
				(*pexprSubquery)[0]->Pop()->Eopid());
	GPOS_ASSERT(nullptr != pcrCount);
#ifdef GPOS_DEBUG
	CColRef *colref = nullptr;
	GPOS_ASSERT(CUtils::FHasCountAgg((*pexprSubquery)[0], &colref));
	GPOS_ASSERT(colref == pcrCount);
#endif	// GPOS_DEBUG

	CScalarSubquery *popScalarSubquery =
		CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcrSubquery = popScalarSubquery->Pcr();

	if (pcrCount == pcrSubquery)
	{
		// fail if subquery does not use compute a new expression using count column
		return false;
	}

	CExpression *pexprPrj = (*pexprSubquery)[0];
	CExpression *pexprPrjChild = (*pexprPrj)[0];
	CExpression *pexprPrjList = (*pexprPrj)[1];

	if (COperator::EopLogicalGbAgg != pexprPrjChild->Pop()->Eopid() ||
		pexprPrjList->DeriveHasNonScalarFunction() ||
		IMDFunction::EfsVolatile ==
			pexprPrjList->DeriveScalarFunctionProperties()->Efs())
	{
		// fail if Project child is not GbAgg, or there are non-scalar/volatile functions in project list
		return false;
	}

	CColRefSet *pcrsOutput = pexprPrjChild->DeriveOutputColumns();
	if (1 < pcrsOutput->Size())
	{
		// fail if GbAgg has more than one output column
		return false;
	}

	CColRefSet *pcrsUsed = pexprPrjList->DeriveUsedColumns();
	BOOL fPrjUsesCount = (0 == pcrsUsed->Size()) ||
						 (1 == pcrsUsed->Size() && pcrsUsed->FMember(pcrCount));
	if (!fPrjUsesCount)
	{
		// fail if Project does not use count column
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		FContainsEmptyGbAgg
//
//	@doc:
//		Return true if pexpr contains a GbAgg with empty grouping columns
//		(i.e., GROUP BY ())
//
//---------------------------------------------------------------------------
static BOOL
FContainsEmptyGbAgg(CExpression *pexpr)
{
	if (COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid())
	{
		return 0 == CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr()->Size();
	}
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FLogical() && FContainsEmptyGbAgg(pexprChild))
		{
			return true;
		}
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		FHasCorrelatedSelectAboveGbAgg
//
//	@doc:
//		Return true if pexpr has a CLogicalSelect with outer references in its
//		filter predicate that sits above a GROUP BY () aggregate.  This pattern
//		arises when a correlated scalar subquery has a correlated HAVING clause,
//		e.g. "SELECT count(*) FROM t GROUP BY () HAVING outer_col".
//
//		When such a pattern exists the scalar subquery must NOT be decorrelated
//		with COALESCE(count,0) semantics: if the HAVING condition is false the
//		subquery should return NULL (no rows), not 0.
//
//---------------------------------------------------------------------------
static BOOL
FHasCorrelatedSelectAboveGbAgg(CExpression *pexpr)
{
	// Stop recursion at a GbAgg boundary: we are looking for a Select
	// that sits *above* a GbAgg, so once we reach the GbAgg there is
	// nothing more to check in this branch.
	if (COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid())
	{
		return false;
	}

	if (COperator::EopLogicalSelect == pexpr->Pop()->Eopid() &&
		pexpr->HasOuterRefs())
	{
		// The Select has outer references somewhere in its subtree.
		// Check whether they originate from the filter (child 1) rather
		// than from the logical child (child 0).  If the logical child has
		// no outer refs but the Select as a whole does, the outer refs must
		// come from the filter predicate — exactly the correlated-HAVING
		// pattern we want to detect.
		CExpression *pexprLogicalChild = (*pexpr)[0];
		if (!pexprLogicalChild->HasOuterRefs() &&
			FContainsEmptyGbAgg(pexprLogicalChild))
		{
			return true;
		}
	}

	// Recurse into logical children only.
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FLogical() &&
			FHasCorrelatedSelectAboveGbAgg(pexprChild))
		{
			return true;
		}
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		FAggFuncsEmptyInputNull
//
//	@doc:
//		Return true if every aggregate the scalar expression computes is known
//		to return NULL on empty input, like sum() or avg(), unlike count()
//
//---------------------------------------------------------------------------
static BOOL
FAggFuncsEmptyInputNull(CExpression *pexprScalar)
{
	GPOS_CHECK_STACK_SIZE;

	if (COperator::EopScalarAggFunc == pexprScalar->Pop()->Eopid())
	{
		CScalarAggFunc *popAggFunc =
			CScalarAggFunc::PopConvert(pexprScalar->Pop());
		if (popAggFunc->FCountStar() || popAggFunc->FCountAny())
		{
			return false;
		}

		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		return md_accessor->RetrieveAgg(popAggFunc->MDId())
			->IsAggEmptyInputNull();
	}

	const ULONG arity = pexprScalar->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexprScalar)[ul];
		if (pexprChild->Pop()->FScalar() &&
			!FAggFuncsEmptyInputNull(pexprChild))
		{
			return false;
		}
	}
	return true;
}


// Check whether evaluating empty-input aggregates ahead of the outer rows
// can call a user-defined final function.  Such a function may throw even
// though every outer row has matching input, so retain correlated execution.
static BOOL
FAggFuncsEmptyInputSafe(CExpression *pexprScalar)
{
	GPOS_CHECK_STACK_SIZE;

	if (COperator::EopScalarAggFunc == pexprScalar->Pop()->Eopid())
	{
		CScalarAggFunc *popAggFunc =
			CScalarAggFunc::PopConvert(pexprScalar->Pop());
		return COptCtxt::PoctxtFromTLS()->Pmda()->RetrieveAgg(popAggFunc->MDId())
			->IsAggEmptyInputSafe();
	}

	for (ULONG ul = 0; ul < pexprScalar->Arity(); ul++)
	{
		if ((*pexprScalar)[ul]->Pop()->FScalar() &&
			!FAggFuncsEmptyInputSafe((*pexprScalar)[ul]))
		{
			return false;
		}
	}
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		FHasCorrelatedScalarAggNotNullOnEmpty
//
//	@doc:
//		Return true if pexpr has a GROUP BY () aggregate over correlated
//		input that computes an aggregate not known to return NULL on empty
//		input, like count() or regr_count().
//
//		Decorrelation groups such an aggregate by the correlated columns and
//		joins it to the outer side, so an outer row with no matching inner
//		rows gets no group at all instead of the aggregate's value on empty
//		input (the "count bug").
//
//---------------------------------------------------------------------------
static BOOL
FHasCorrelatedScalarAggNotNullOnEmpty(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalGbAgg == pop->Eopid() &&
		0 == CLogicalGbAgg::PopConvert(pop)->Pdrgpcr()->Size() &&
		(*pexpr)[0]->HasOuterRefs() && !FAggFuncsEmptyInputNull((*pexpr)[1]))
	{
		return true;
	}

	// Recurse into logical children only.
	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FLogical() &&
			FHasCorrelatedScalarAggNotNullOnEmpty(pexprChild))
		{
			return true;
		}
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		PexprSkipProjects
//
//	@doc:
//		Return the first descendant of pexpr that is not a projection
//
//---------------------------------------------------------------------------
static CExpression *
PexprSkipProjects(CExpression *pexpr)
{
	while (COperator::EopLogicalProject == pexpr->Pop()->Eopid())
	{
		pexpr = (*pexpr)[0];
	}
	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		FIsEmptyGbAggCountOutput
//
//	@doc:
//		Return true if pexpr is a GROUP BY () aggregate, possibly under
//		projections, that computes colref as count(*)/count(Any). Such a
//		subquery produces exactly one row, and colref is 0 when the aggregate
//		sees no input rows.
//
//---------------------------------------------------------------------------
static BOOL
FIsEmptyGbAggCountOutput(CExpression *pexpr, const CColRef *colref)
{
	pexpr = PexprSkipProjects(pexpr);

	if (COperator::EopLogicalGbAgg != pexpr->Pop()->Eopid() ||
		0 != CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr()->Size())
	{
		return false;
	}

	CExpression *pexprPrjList = (*pexpr)[1];
	const ULONG arity = pexprPrjList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrjElem = (*pexprPrjList)[ul];
		CColRef *pcrCount = nullptr;
		if (COperator::EopScalarAggFunc == (*pexprPrjElem)[0]->Pop()->Eopid() &&
			CUtils::FCountAggProjElem(pexprPrjElem, &pcrCount) &&
			colref == pcrCount)
		{
			return true;
		}
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		PexprCorrelatedScalarAgg
//
//	@doc:
//		If the relational child of a subquery is a GROUP BY () aggregate over
//		correlated input, possibly under projections and HAVING filters,
//		return the aggregate. It produces one row before applying HAVING.
//
//---------------------------------------------------------------------------
static CExpression *
PexprCorrelatedScalarAgg(CExpression *pexprInner)
{
	CExpression *pexpr = pexprInner;
	while (COperator::EopLogicalProject == pexpr->Pop()->Eopid() ||
		   COperator::EopLogicalSelect == pexpr->Pop()->Eopid())
	{
		pexpr = (*pexpr)[0];
	}

	if (COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid() &&
		0 == CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr()->Size() &&
		(*pexpr)[0]->HasOuterRefs())
	{
		return pexpr;
	}
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		FNeedsEmptyInputValue
//
//	@doc:
//		Return true if the subquery with relational child pexprInner may
//		return a non-NULL value for an outer row without matching inner rows,
//		which decorrelation into an outer join would turn into NULL. See
//		PexprScalarAggApply().
//
//---------------------------------------------------------------------------
static BOOL
FNeedsEmptyInputValue(CExpression *pexprInner)
{
	CExpression *pexprGbAgg = PexprCorrelatedScalarAgg(pexprInner);

	// projections can compute a non-NULL value from NULL aggregates, like
	// coalesce(sum(x), 0)
	return nullptr != pexprGbAgg &&
		   (pexprGbAgg != pexprInner ||
			!FAggFuncsEmptyInputNull((*pexprGbAgg)[1]));
}


//---------------------------------------------------------------------------
//	@function:
//		PexprSubstituteColRefs
//
//	@doc:
//		Return a copy of the scalar expression with the columns in colrefs
//		replaced by the matching expressions in pdrgpexpr
//
//---------------------------------------------------------------------------
static CExpression *
PexprSubstituteColRefs(CMemoryPool *mp, CExpression *pexpr,
					   CColRefArray *colrefs, CExpressionArray *pdrgpexpr)
{
	GPOS_CHECK_STACK_SIZE;

	COperator *pop = pexpr->Pop();
	if (COperator::EopScalarIdent == pop->Eopid())
	{
		const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
		for (ULONG ul = 0; ul < colrefs->Size(); ul++)
		{
			if ((*colrefs)[ul] == colref)
			{
				(*pdrgpexpr)[ul]->AddRef();
				return (*pdrgpexpr)[ul];
			}
		}
	}

	const ULONG arity = pexpr->Arity();
	if (0 == arity)
	{
		pexpr->AddRef();
		return pexpr;
	}

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			pdrgpexprChildren->Append(
				PexprSubstituteColRefs(mp, pexprChild, colrefs, pdrgpexpr));
		}
		else
		{
			pexprChild->AddRef();
			pdrgpexprChildren->Append(pexprChild);
		}
	}
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}


// Is this the built-in hypothetical rank with no direct arguments to evaluate?
static BOOL
FRankWithConstDirectArgs(CExpression *pexprAgg)
{
	if (COperator::EopScalarAggFunc != pexprAgg->Pop()->Eopid())
	{
		return false;
	}

	CScalarAggFunc *popAgg = CScalarAggFunc::PopConvert(pexprAgg->Pop());
	if (IMDId::EmdidGeneral != popAgg->MDId()->MdidType() ||
		GPDB_RANK_ANY != CMDIdGPDB::CastMdid(popAgg->MDId())->Oid())
	{
		return false;
	}

	// Direct arguments are evaluated even on empty input. Only constants
	// can be discarded without losing an error or a side effect.
	CExpression *pexprDirectArgs = (*pexprAgg)[EaggfuncIndexDirectArgs];
	for (ULONG ul = 0; ul < pexprDirectArgs->Arity(); ul++)
	{
		if (COperator::EopScalarConst != (*pexprDirectArgs)[ul]->Pop()->Eopid())
		{
			return false;
		}
	}
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		PexprScalarAggApply
//
//	@doc:
//		Unnest a subquery whose relational child pexprInner is a GROUP BY ()
//		aggregate over correlated input, possibly under projections and HAVING,
//		and that returns colref. Return the new outer expression, and in
//		*ppexprValue the subquery's value for every outer row. Takes over the
//		references to pexprOuter and pexprInner.
//
//		Before HAVING, the subquery returns one row for every outer row.
//		Decorrelation groups the aggregate by the correlated columns and joins
//		it to the outer side, so a row without matching inner rows gets no group,
//		and an outer join gives it NULL instead of the subquery's value over
//		empty input: 0 for count(), 1 for rank() of a hypothetical row, the
//		initial state of an aggregate without a final function, and whatever
//		the projections compute from such values.
//
//		So build a LEFT outer apply with a match flag on the inner side, and
//		use
//			CASE WHEN flag THEN <colref over matched aggregates>
//			               ELSE <colref over empty input> END
//		The value over empty input is the projections' expression for colref
//		with every aggregate replaced by its own value over empty input: a
//		constant for count(), rank() with constant direct arguments, and
//		aggregates known to return NULL; otherwise the aggregate computed over
//		an empty table, joined to the outer side. The expression around the
//		aggregates is evaluated per row, and only for the outer rows without
//		a match, so "10 / count(*)"
//		raises "division by zero" only if there is such a row, as the
//		correlated subquery does. Likewise, pull the projections off the
//		inner side and rebuild them in the matched branch. Otherwise a hash
//		join can evaluate "100 / coalesce(sum(x), 0)" for an unused all-NULL
//		group, even when the outer relation is empty.
//
//		Evaluate HAVING above the join as well, using the matched or empty
//		aggregate values. A rejected row yields NULL for a scalar subquery.
//		For IN, return the condition separately in ppexprHaving so the caller
//		can return FALSE for an empty subquery, even when its left operand is
//		NULL, without evaluating the subquery's target expression.
//
//---------------------------------------------------------------------------
static CExpression *
PexprScalarAggApply(CMemoryPool *mp, CExpression *pexprOuter,
					CExpression *pexprInner, const CColRef *colref,
					CExpression **ppexprValue, CExpression **ppexprHaving = nullptr)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CExpression *pexprGbAgg = PexprCorrelatedScalarAgg(pexprInner);
	GPOS_ASSERT(nullptr != pexprGbAgg);

	// columns computed by the aggregate and the projections, and their
	// values over empty input and matched aggregate output
	CColRefArray *pdrgpcrDefined = GPOS_NEW(mp) CColRefArray(mp);
	CExpressionArray *pdrgpexprEmpty = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprMatched = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprEmptyHaving = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprMatchedHaving = GPOS_NEW(mp) CExpressionArray(mp);

	// the aggregates to compute over an empty table
	CExpressionArray *pdrgpexprEmptyAggs = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray *pdrgpcrEmptyTable = GPOS_NEW(mp) CColRefArray(mp);
	UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);

	CExpression *pexprAggPrjList = (*pexprGbAgg)[1];
	for (ULONG ul = 0; ul < pexprAggPrjList->Arity(); ul++)
	{
		CExpression *pexprPrjElem = (*pexprAggPrjList)[ul];
		CColRef *pcrAgg =
			CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr();
		CExpression *pexprAgg = (*pexprPrjElem)[0];
		CExpression *pexprEmpty = nullptr;
		CColRef *pcrCount = nullptr;

		if (CUtils::FCountAggProjElem(pexprPrjElem, &pcrCount) &&
			COperator::EopScalarAggFunc == pexprAgg->Pop()->Eopid())
		{
			pexprEmpty = CUtils::PexprScalarConstInt8(mp, 0 /*val*/);
		}
		else if (FRankWithConstDirectArgs(pexprAgg))
		{
			pexprEmpty = CUtils::PexprScalarConstInt8(mp, 1 /*val*/);
		}
		else if (FAggFuncsEmptyInputNull(pexprAgg))
		{
			pexprEmpty = CUtils::PexprScalarConstNull(
				mp, pcrAgg->RetrieveType(), pcrAgg->TypeModifier());
		}
		else
		{
			// compute the aggregate over an empty table with fresh columns
			CColRefSetIter crsi(*pexprAgg->DeriveUsedColumns());
			while (crsi.Advance())
			{
				CColRef *pcr = crsi.Pcr();
				ULONG id = pcr->Id();
				if (nullptr == colref_mapping->Find(&id))
				{
					CColRef *pcrNew = col_factory->PcrCreate(pcr);
					colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrNew);
					pdrgpcrEmptyTable->Append(pcrNew);
				}
			}
			CColRef *pcrEmpty = col_factory->PcrCreate(pcrAgg);
			pdrgpexprEmptyAggs->Append(CUtils::PexprScalarProjectElement(
				mp, pcrEmpty,
				pexprAgg->PexprCopyWithRemappedColumns(mp, colref_mapping,
													   true /*must_exist*/)));
			pexprEmpty = CUtils::PexprScalarIdent(mp, pcrEmpty);
		}

		pdrgpcrDefined->Append(pcrAgg);
		pdrgpexprEmpty->Append(pexprEmpty);
		pdrgpexprMatched->Append(CUtils::PexprScalarIdent(mp, pcrAgg));
	}

	// Projections and HAVING filters, from the aggregate up.
	CExpressionArray *pdrgpexprAboveAgg = GPOS_NEW(mp) CExpressionArray(mp);
	for (CExpression *pexpr = pexprInner; pexpr != pexprGbAgg;
		 pexpr = (*pexpr)[0])
	{
		pexpr->AddRef();
		pdrgpexprAboveAgg->Append(pexpr);
	}
	for (ULONG ul = pdrgpexprAboveAgg->Size(); 0 < ul; ul--)
	{
		CExpression *pexpr = (*pdrgpexprAboveAgg)[ul - 1];
		if (COperator::EopLogicalSelect == pexpr->Pop()->Eopid())
		{
			pdrgpexprEmptyHaving->Append(PexprSubstituteColRefs(
				mp, (*pexpr)[1], pdrgpcrDefined, pdrgpexprEmpty));
			pdrgpexprMatchedHaving->Append(PexprSubstituteColRefs(
				mp, (*pexpr)[1], pdrgpcrDefined, pdrgpexprMatched));
			continue;
		}
		CExpression *pexprPrjList = (*pexpr)[1];
		for (ULONG ulElem = 0; ulElem < pexprPrjList->Arity(); ulElem++)
		{
			CExpression *pexprPrjElem = (*pexprPrjList)[ulElem];
			pdrgpcrDefined->Append(
				CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr());
			pdrgpexprEmpty->Append(PexprSubstituteColRefs(
				mp, (*pexprPrjElem)[0], pdrgpcrDefined, pdrgpexprEmpty));
			pdrgpexprMatched->Append(PexprSubstituteColRefs(
				mp, (*pexprPrjElem)[0], pdrgpcrDefined, pdrgpexprMatched));
		}
	}
	pdrgpexprAboveAgg->Release();

	CExpression *pexprEmptyValue = nullptr;
	CExpression *pexprMatchedValue = nullptr;
	for (ULONG ul = pdrgpcrDefined->Size();
		 0 < ul && nullptr == pexprEmptyValue; ul--)
	{
		if ((*pdrgpcrDefined)[ul - 1] == colref)
		{
			pexprEmptyValue = (*pdrgpexprEmpty)[ul - 1];
			pexprEmptyValue->AddRef();
			pexprMatchedValue = (*pdrgpexprMatched)[ul - 1];
			pexprMatchedValue->AddRef();
		}
	}
	GPOS_ASSERT(nullptr != pexprEmptyValue);

	// Keep only the aggregates and a match flag on the inner side.
	pexprGbAgg->AddRef();
	CExpression *pexprInnerWithFlag = CUtils::PexprAddProjection(
		mp, pexprGbAgg, CUtils::PexprScalarConstBool(mp, true /*value*/));
	pexprInner->Release();
	const CColRef *pcrFlag =
		CScalarProjectElement::PopConvert((*(*pexprInnerWithFlag)[1])[0]->Pop())
			->Pcr();
	// This aggregate yields a scalar even for IN. If decorrelation fails,
	// correlated execution needs every aggregate column and the match flag.
	CExpression *pexprNewOuter =
		CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
			mp, pexprOuter, pexprInnerWithFlag,
			pexprInnerWithFlag->DeriveOutputColumns()->Pdrgpcr(mp),
			COperator::EopScalarSubquery);

	if (0 < pdrgpexprEmptyAggs->Size())
	{
		if (0 == pdrgpcrEmptyTable->Size())
		{
			// the aggregates use no columns, the table still needs one
			pdrgpcrEmptyTable->Append(col_factory->PcrCreate(
				md_accessor->PtMDType<IMDTypeBool>(), default_type_modifier));
		}
		CExpression *pexprEmptyTable = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CLogicalConstTableGet(
								mp, pdrgpcrEmptyTable,
								GPOS_NEW(mp) IDatum2dArray(mp)));
		CExpression *pexprEmptyAgg = CUtils::PexprLogicalGbAggGlobal(
			mp, GPOS_NEW(mp) CColRefArray(mp), pexprEmptyTable,
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
									 pdrgpexprEmptyAggs));

		// the aggregate returns one row, so this keeps every outer row
		pexprNewOuter = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			mp, pexprNewOuter, pexprEmptyAgg,
			CUtils::PexprScalarConstBool(mp, true /*value*/));
	}
	else
	{
		pdrgpexprEmptyAggs->Release();
		pdrgpcrEmptyTable->Release();
	}

	IMDId *mdid_type = colref->RetrieveType()->MDId();
	mdid_type->AddRef();
	*ppexprValue = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarIf(mp, mdid_type),
					CUtils::PexprScalarIdent(mp, pcrFlag),
					pexprMatchedValue, pexprEmptyValue);

	if (nullptr != ppexprHaving)
	{
		*ppexprHaving = nullptr;
	}
	if (0 < pdrgpexprMatchedHaving->Size())
	{
		IMDId *mdid_bool = md_accessor->PtMDType<IMDTypeBool>()->MDId();
		mdid_bool->AddRef();
		CExpression *pexprHaving = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarIf(mp, mdid_bool),
			CUtils::PexprScalarIdent(mp, pcrFlag),
			CPredicateUtils::PexprConjunction(mp, pdrgpexprMatchedHaving),
			CPredicateUtils::PexprConjunction(mp, pdrgpexprEmptyHaving));
		if (nullptr != ppexprHaving)
		{
			*ppexprHaving = pexprHaving;
		}
		else
		{
			mdid_type->AddRef();
			*ppexprValue = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarIf(mp, mdid_type), pexprHaving,
				*ppexprValue, CUtils::PexprScalarConstNull(
					mp, colref->RetrieveType(), colref->TypeModifier()));
		}
	}
	else
	{
		pdrgpexprMatchedHaving->Release();
		pdrgpexprEmptyHaving->Release();
	}

	colref_mapping->Release();
	pdrgpcrDefined->Release();
	pdrgpexprEmpty->Release();
	pdrgpexprMatched->Release();

	return pexprNewOuter;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::SSubqueryDesc::SetCorrelatedExecution
//
//	@doc:
//		Set correlated execution flag
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::SSubqueryDesc::SetCorrelatedExecution()
{
	// check conditions of correlated execution
	m_fCorrelatedExecution =
		m_returns_set ||  // subquery produces > 1 rows, we need correlated execution to check for cardinality at runtime
		m_fHasVolatileFunctions ||	// volatile functions cannot be decorrelated
		(m_fHasCountAgg &&
		 m_fHasSkipLevelCorrelations);	// count() with skip-level correlations cannot be decorrelated due to their NULL semantics
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::Psd
//
//	@doc:
//		Create subquery descriptor
//
//---------------------------------------------------------------------------
CSubqueryHandler::SSubqueryDesc *
CSubqueryHandler::Psd(CMemoryPool *mp, CExpression *pexprSubquery,
					  CExpression *pexprOuter, const CColRef *pcrSubquery,
					  ESubqueryCtxt esqctxt)
{
	GPOS_ASSERT(nullptr != pexprSubquery);
	GPOS_ASSERT(CUtils::FSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(nullptr != pexprOuter);

	CExpression *pexprInner = (*pexprSubquery)[0];
	CColRefSet *subqueryOutputCols = (*pexprSubquery)[0]->DeriveOutputColumns();
	CColRefSet *outer_refs = (*pexprSubquery)[0]->DeriveOuterReferences();
	CColRefSet *pcrsOuterOutput = pexprOuter->DeriveOutputColumns();

	SSubqueryDesc *psd = GPOS_NEW(mp) SSubqueryDesc();
	psd->m_returns_set = (1 < pexprInner->DeriveMaxCard().Ull());
	psd->m_fReturnedPcrIsOuterRef = (!subqueryOutputCols->FMember(pcrSubquery));
	psd->m_fHasOuterRefs =
		pexprInner->HasOuterRefs() || psd->m_fReturnedPcrIsOuterRef;
	psd->m_fHasVolatileFunctions =
		(IMDFunction::EfsVolatile ==
		 pexprSubquery->DeriveScalarFunctionProperties()->Efs());
	// We have skip-level outer refs if there are outer refs at all, and at least one of the following is true:
	// - the outer refs below the subquery node don't all come from the outer table (the level right above us)
	// - the ColRef returned by the subquery is an outer ref that does not come from the outer table
	psd->m_fHasSkipLevelCorrelations =
		psd->m_fHasOuterRefs && (!pcrsOuterOutput->ContainsAll(outer_refs) ||
								 (psd->m_fReturnedPcrIsOuterRef &&
								  !pcrsOuterOutput->FMember(pcrSubquery)));
	psd->m_fHasCountAgg =
		CUtils::FHasCountAgg((*pexprSubquery)[0], &psd->m_pcrCountAgg);

	if (psd->m_fHasCountAgg &&
		COperator::EopLogicalProject == (*pexprSubquery)[0]->Pop()->Eopid())
	{
		psd->m_fProjectCount =
			FProjectCountSubquery(pexprSubquery, psd->m_pcrCountAgg);
	}

	// set flag for using subquery in a value context
	// a subquery generated from a quantified one has its own handling of
	// empty input, see FCreateOuterApplyForScalarSubquery()
	BOOL fGeneratedByQuantified =
		COperator::EopScalarSubquery == pexprSubquery->Pop()->Eopid() &&
		CScalarSubquery::PopConvert(pexprSubquery->Pop())
			->FGeneratedByQuantified();

	psd->m_fValueSubquery =
		EsqctxtValue == esqctxt ||
		(psd->m_fHasCountAgg && psd->m_fHasOuterRefs) ||
		(psd->m_fHasOuterRefs && !fGeneratedByQuantified &&
		 FNeedsEmptyInputValue(pexprInner));

	// set flag of correlated execution
	psd->SetCorrelatedExecution();

	// A correlated scalar subquery of the form
	//   SELECT count(*) FROM t GROUP BY () HAVING <outer_ref_condition>
	// must execute as a correlated SubPlan.  After NormalizeHaving() the HAVING
	// clause becomes a CLogicalSelect with outer refs sitting above the GbAgg.
	// If we decorrelate such a subquery the join filter replaces the HAVING
	// condition, but a LEFT JOIN returns 0 (not NULL) for count(*) when no
	// rows match — which is semantically wrong.  Forcing correlated execution
	// preserves the correct NULL-when-no-rows semantics.
	if (!psd->m_fCorrelatedExecution && psd->m_fHasCountAgg &&
		psd->m_fHasOuterRefs &&
		FHasCorrelatedSelectAboveGbAgg(pexprInner))
	{
		psd->m_fCorrelatedExecution = true;
	}

	// A GROUP BY () aggregate over correlated input returns its value on
	// empty input for an outer row without matching inner rows, e.g. 0 for
	// count(). Decorrelation groups it by the correlated columns and joins it
	// to the outer side, so such a row gets no group. At the top of the
	// subquery, under projections and HAVING, PexprScalarAggApply() computes
	// the value for such rows; deeper down, e.g. under another aggregate,
	// it cannot, so keep the correlated execution unless all the
	// aggregates there return NULL on empty input anyway.
	if (!psd->m_fCorrelatedExecution && psd->m_fHasOuterRefs &&
		!fGeneratedByQuantified)
	{
		CExpression *pexprGbAgg = PexprCorrelatedScalarAgg(pexprInner);
		CExpression *pexprToCheck =
			(nullptr != pexprGbAgg) ? (*pexprGbAgg)[0] : pexprInner;

		if (FHasCorrelatedScalarAggNotNullOnEmpty(pexprToCheck) ||
			(nullptr != pexprGbAgg &&
			 !FAggFuncsEmptyInputSafe((*pexprGbAgg)[1])))
		{
			psd->m_fCorrelatedExecution = true;
		}
	}

	return psd;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveScalarSubquery
//
//	@doc:
//		Replace a scalar subquery node with a column identifier, and create
//		a new Apply expression;
//
//		when subquery is defined on top of a Project node, the function simplifies
//		subquery expression by pulling-up project above subquery to facilitate
//		detecting special subquery types such as count(*) subqueries
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveScalarSubquery(CExpression *pexprOuter,
										CExpression *pexprSubquery,
										ESubqueryCtxt esqctxt,
										CExpression **ppexprNewOuter,
										CExpression **ppexprResidualScalar)
{
	CMemoryPool *pmp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(m_mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	CScalarSubquery *popScalarSubquery =
		CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *pcrSubquery = popScalarSubquery->Pcr();

	SSubqueryDesc *psd =
		Psd(pmp, pexprSubquery, pexprOuter, pcrSubquery, esqctxt);

	if (psd->m_fReturnedPcrIsOuterRef)
	{
		// The subquery returns an outer reference. We can't simply replace the subquery with that
		// expression, because we would miss the case where the subquery is an empty table and we
		// would have to substitute the outer ref with a NULL.
		// We could use a dummy expression from the subquery to perform a check, but for now we'll
		// just give up.
		// Example: select * from foo where foo.a = (select foo.b from bar);
		GPOS_DELETE(psd);
		return false;
	}

	BOOL fSuccess = false;
	if (psd->m_fProjectCount && !psd->m_fCorrelatedExecution)
	{
		// count(*)/count(Any) have special semantics: they produce '0' if their input is empty,
		// all other agg functions produce 'NULL' if their input is empty

		// for subqueries of the form (SELECT 'abc' || count(*) from X where x.i=outer.i),
		// we first create a LeftOuterApply expression to compute 'count' value and replace NULL
		// count values with '0' in the output of LOA expression,
		// we then pull the Project node below subquery to be above the LOA expression

		// create a new subquery to compute count(*) agg
		CExpression *pexprPrj = (*pexprSubquery)[0];
		CExpression *pexprPrjList = (*pexprPrj)[1];
		CExpression *pexprGbAgg = (*pexprPrj)[0];
		GPOS_ASSERT(COperator::EopLogicalGbAgg == pexprGbAgg->Pop()->Eopid());

		CScalarSubquery *popInnerSubq = GPOS_NEW(m_mp) CScalarSubquery(
			m_mp, psd->m_pcrCountAgg, false /*fGeneratedByExist*/,
			false /*fGeneratedByQuantified*/);
		pexprGbAgg->AddRef();
		CExpression *pexprNewSubq =
			GPOS_NEW(m_mp) CExpression(m_mp, popInnerSubq, pexprGbAgg);

		// unnest new subquery
		GPOS_DELETE(psd);
		CExpression *pexprNewOuter = nullptr;
		CExpression *pexprResidualScalar = nullptr;
		psd = Psd(m_mp, pexprNewSubq, pexprOuter, popInnerSubq->Pcr(), esqctxt);
		fSuccess = FRemoveScalarSubqueryInternal(
			m_mp, pexprOuter, pexprNewSubq, EsqctxtValue, psd,
			m_fEnforceCorrelatedApply, &pexprNewOuter, &pexprResidualScalar);

		if (fSuccess)
		{
			// unnesting succeeded -- replace all occurrences of count(*) column in project list with residual expression
			pexprPrj->Pop()->AddRef();
			CExpression *pexprPrjListNew = PexprReplace(
				m_mp, pexprPrjList, psd->m_pcrCountAgg, pexprResidualScalar);
			*ppexprNewOuter = GPOS_NEW(m_mp) CExpression(
				m_mp, pexprPrj->Pop(), pexprNewOuter, pexprPrjListNew);
			*ppexprResidualScalar = CUtils::PexprScalarIdent(m_mp, pcrSubquery);
		}

		CRefCount::SafeRelease(pexprNewSubq);
		CRefCount::SafeRelease(pexprResidualScalar);
	}
	else
	{
		fSuccess = FRemoveScalarSubqueryInternal(
			m_mp, pexprOuter, pexprSubquery, esqctxt, psd,
			m_fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
	}

	GPOS_DELETE(psd);
	return fSuccess;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FGenerateCorrelatedApplyForScalarSubquery
//
//	@doc:
//		Helper to generate a correlated apply expression when needed
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FGenerateCorrelatedApplyForScalarSubquery(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprSubquery,
	ESubqueryCtxt
#ifdef GPOS_DEBUG
		esqctxt
#endif	// GPOS_DEBUG
	,
	CSubqueryHandler::SSubqueryDesc *psd, BOOL fEnforceCorrelatedApply,
	CExpression **ppexprNewOuter, CExpression **ppexprResidualScalar)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG
	GPOS_ASSERT(nullptr != psd);
	GPOS_ASSERT(psd->m_fCorrelatedExecution || fEnforceCorrelatedApply);

	CScalarSubquery *popScalarSubquery =
		CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popScalarSubquery->Pcr();
	COperator::EOperatorId eopidSubq = popScalarSubquery->Eopid();

	CExpression *pexprInner = (*pexprSubquery)[0];
	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression
	pexprInner->AddRef();

	// in order to check cardinality of subquery output at run time, we can
	// use MaxOneRow expression, only if
	// (1) correlated execution is not enforced,
	// (2) there are no outer references below, and
	// (3) transformation converting MaxOneRow to Assert is enabled
	BOOL fUseMaxOneRow = !fEnforceCorrelatedApply && !psd->m_fHasOuterRefs &&
						 GPOPT_FENABLED_XFORM(CXform::ExfMaxOneRow2Assert);

	if (psd->m_fValueSubquery)
	{
		if (fUseMaxOneRow)
		{
			// we need correlated execution here to check if more than one row are generated
			// by inner expression during execution, we generate a MaxOneRow expression to handle this case
			CExpression *pexprMaxOneRow = GPOS_NEW(mp)
				CExpression(mp, GPOS_NEW(mp) CLogicalMaxOneRow(mp), pexprInner);
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
				mp, pexprOuter, pexprMaxOneRow, colref, eopidSubq);
		}
		else
		{
			// correlated inner expression requires correlated execution
			*ppexprNewOuter =
				CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
					mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

		return true;
	}

	GPOS_ASSERT(EsqctxtFilter == esqctxt);

	if (fUseMaxOneRow)
	{
		// we need correlated execution here to check if more than one row are generated
		// by inner expression during execution, we generate a MaxOneRow expression to handle this case
		CExpression *pexprMaxOneRow = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CLogicalMaxOneRow(mp), pexprInner);
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(
			mp, pexprOuter, pexprMaxOneRow, colref, eopidSubq);
	}
	else
	{
		// correlated inner expression requires correlated execution
		*ppexprNewOuter =
			CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(
				mp, pexprOuter, pexprInner, colref, eopidSubq);
	}
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveScalarSubqueryInternal
//
//	@doc:
//		Replace a scalar subquery node with a column identifier, and create
//		a new Apply expression;
//
//		Example:
//
//			SELECT											SELECT
//			/		|									/			|
//			R		=			==>				INNER-APPLY			=
//				/	|							/		|		/	|
//				a	GrpBy						R		GrpBy	a	 x
//					/		|							/	|
//					S	x:=sum(b)						S	x:=sum(b)
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveScalarSubqueryInternal(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt, CSubqueryHandler::SSubqueryDesc *psd,
	BOOL fEnforceCorrelatedApply, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG
	GPOS_ASSERT(nullptr != psd);

	if (psd->m_fCorrelatedExecution || fEnforceCorrelatedApply)
	{
		return FGenerateCorrelatedApplyForScalarSubquery(
			mp, pexprOuter, pexprSubquery, esqctxt, psd,
			fEnforceCorrelatedApply, ppexprNewOuter, ppexprResidualScalar);
	}

	CScalarSubquery *popScalarSubquery =
		CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popScalarSubquery->Pcr();

	CExpression *pexprInner = (*pexprSubquery)[0];
	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression
	pexprInner->AddRef();
	BOOL fSuccess = true;
	if (psd->m_fValueSubquery)
	{
		fSuccess = FCreateOuterApply(
			mp, pexprOuter, pexprInner, pexprSubquery,
			nullptr /* pexprPredicate */, psd->m_fHasOuterRefs, ppexprNewOuter,
			ppexprResidualScalar, false /* not null opt for quant*/);
		if (!fSuccess)
		{
			pexprInner->Release();
		}
		return fSuccess;
	}

	GPOS_ASSERT_FIXME(EsqctxtFilter == esqctxt);

	*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalInnerApply>(
		mp, pexprOuter, pexprInner, colref, pexprSubquery->Pop()->Eopid());
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprInnerSelect
//
//	@doc:
//		Helper for creating an inner select expression when creating
//		outer apply;
//		create a select node with a <pexprPredicate> IS NOT FALSE
//		predicate to allow rows where the comparision evaluates to
//		true or NULL.
//		We apply one optimization: If we are dealing with a not nullable
//		column on the inner table and if the comparison operator is "very
//		strict" (is strict and never returns NULL on non-NULL inputs), then
//		we can use a regular comparison. The only way such a comparison can
//		evaluate to NULL is for the outer column to be NULL.
//		If we use this optimization, then we need to add another condition
//		that handles the outer column in CSubqueryHandler::PexprScalarIf.
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprInnerSelect(CMemoryPool *mp, const CColRef *pcrInner,
								   CExpression *pexprInner,
								   CExpression *pexprPredicate,
								   BOOL *useNotNullableInnerOpt)
{
	CExpression *predToUse = nullptr;
	CScalarCmp *pscalarCmp = CScalarCmp::PopConvert(pexprPredicate->Pop());
	BOOL innerIsNullable =
		!pexprInner->DeriveNotNullColumns()->FMember(pcrInner);

	GPOS_ASSERT(nullptr != pscalarCmp);

	*useNotNullableInnerOpt = false;
	pexprPredicate->AddRef();

	if (innerIsNullable ||
		!CPredicateUtils::FBuiltInComparisonIsVeryStrict(pscalarCmp->MdIdOp()))
	{
		predToUse = GPOS_NEW(mp) CExpression(
			mp,
			GPOS_NEW(mp)
				CScalarBooleanTest(mp, CScalarBooleanTest::EbtIsNotFalse),
			pexprPredicate);
	}
	else
	{
		predToUse = pexprPredicate;
		*useNotNullableInnerOpt = true;
	}

	pexprInner->AddRef();

	return CUtils::PexprLogicalSelect(mp, pexprInner, predToUse);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApplyForScalarSubquery
//
//	@doc:
//		Helper for creating an outer apply expression for scalar subqueries
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApplyForScalarSubquery(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpression *pexprSubquery,
	BOOL,  // fOuterRefsUnderInner
	CExpression **ppexprNewOuter, CExpression **ppexprResidualScalar)
{
	CScalarSubquery *popSubquery =
		CScalarSubquery::PopConvert(pexprSubquery->Pop());
	const CColRef *colref = popSubquery->Pcr();
	BOOL fSuccess = true;

	// a GROUP BY () aggregate over correlated input: keep the subquery's value
	// over empty input for the outer rows without a match; a bare count() is
	// fixed up with COALESCE(count, 0) below
	if (!popSubquery->FGeneratedByQuantified() &&
		!FIsEmptyGbAggCountOutput(pexprInner, colref) &&
		FNeedsEmptyInputValue(pexprInner))
	{
		*ppexprNewOuter =
			PexprScalarAggApply(mp, pexprOuter, pexprInner, colref,
								ppexprResidualScalar);
		return fSuccess;
	}

	// generate an outer apply between outer expression and the relational child of scalar subquery
	CExpression *pexprLeftOuterApply =
		CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
			mp, pexprOuter, pexprInner, colref, popSubquery->Eopid());

	const CLogicalGbAgg *pgbAgg = nullptr;
	BOOL fHasCountAggMatchingColumn = CUtils::FHasCountAggMatchingColumn(
		(*pexprSubquery)[0], colref, &pgbAgg);

	if (!fHasCountAggMatchingColumn)
	{
		// residual scalar uses the scalar subquery column
		*ppexprNewOuter = pexprLeftOuterApply;
		*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, colref);
		return fSuccess;
	}

	// add projection for subquery column
	CExpression *pexprPrj = CUtils::PexprAddProjection(
		mp, pexprLeftOuterApply, CUtils::PexprScalarIdent(mp, colref));
	const CColRef *pcrComputed =
		CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
	*ppexprNewOuter = pexprPrj;

	BOOL fGeneratedByQuantified = popSubquery->FGeneratedByQuantified();

	// When GROUP BY () has a correlated HAVING clause (now represented as a
	// CLogicalSelect with outer refs sitting above the GbAgg), the subquery
	// must return NULL — not 0 — when the HAVING condition is false.
	// Applying COALESCE(count,0) would incorrectly convert that NULL to 0,
	// so we skip the special count(*) semantics in that case.
	BOOL fCorrelatedHavingAboveEmptyGby =
		(fHasCountAggMatchingColumn && 0 == pgbAgg->Pdrgpcr()->Size() &&
		 FHasCorrelatedSelectAboveGbAgg((*pexprSubquery)[0]));

	if (fGeneratedByQuantified ||
		(fHasCountAggMatchingColumn && 0 == pgbAgg->Pdrgpcr()->Size() &&
		 !fCorrelatedHavingAboveEmptyGby))
	{
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDTypeInt8 *pmdtypeint8 = md_accessor->PtMDType<IMDTypeInt8>();
		IMDId *pmdidInt8 = pmdtypeint8->MDId();
		pmdidInt8->AddRef();
		CExpression *pexprCoalesce = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarCoalesce(mp, pmdidInt8),
						CUtils::PexprScalarIdent(mp, pcrComputed),
						CUtils::PexprScalarConstInt8(mp, 0 /*val*/));

		if (fGeneratedByQuantified)
		{
			// we produce Null if count(*) value is -1,
			// this case can only occur when transforming quantified subquery to
			// count(*) subquery using CXformSimplifySubquery
			pmdidInt8->AddRef();
			*ppexprResidualScalar = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarIf(mp, pmdidInt8),
				CUtils::PexprScalarEqCmp(
					mp, pcrComputed,
					CUtils::PexprScalarConstInt8(mp, -1 /*value*/)),
				CUtils::PexprScalarConstInt8(mp, 0 /*value*/, true /*is_null*/),
				pexprCoalesce);
		}
		else
		{
			// count(*) value can either be NULL (if produced by a lower outer join), or some value >= 0,
			// we return coalesce(count(*), 0) in this case

			*ppexprResidualScalar = pexprCoalesce;
		}

		return fSuccess;
	}

	// residual scalar uses the computed subquery column
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrComputed);
	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateGrpCols
//
//	@doc:
//		Helper for creating an grouping columns for outer apply expression,
//		the function returns True if creation of grouping columns succeeded
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateGrpCols(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	BOOL fExistential, BOOL fOuterRefsUnderInner,
	CColRefArray **ppdrgpcr,  // output: constructed grouping columns
	BOOL *pfGbOnInner		  // output: is Gb created on inner expression
)
{
	GPOS_ASSERT(nullptr != pexprOuter);
	GPOS_ASSERT(nullptr != pexprInner);
	GPOS_ASSERT(nullptr != ppdrgpcr);
	GPOS_ASSERT(nullptr != pfGbOnInner);

	CColRefSet *pcrsOuterOutput = pexprOuter->DeriveOutputColumns();
	CColRefSet *pcrsInnerOutput = pexprInner->DeriveOutputColumns();
	CColRefSet *pcrsUsedOuter = GPOS_NEW(mp) CColRefSet(mp);

	BOOL fGbOnInner = false;
	CExpression *pexprScalar = nullptr;

	// remove any columns that are not referenced in the query from pcrsOuterOutput
	CColRefSetIter it(*pcrsOuterOutput);

	while (it.Advance())
	{
		CColRef *pcr = it.Pcr();

		if (CColRef::EUsed == pcr->GetUsage())
		{
			pcrsUsedOuter->Include(pcr);
		}
	}
	if (!fExistential && !fOuterRefsUnderInner)
	{
		GPOS_ASSERT(COperator::EopLogicalSelect == pexprInner->Pop()->Eopid() &&
					"expecting Select expression");

		pexprScalar = (*pexprInner)[1];
		fGbOnInner = CPredicateUtils::FSimpleEqualityUsingCols(mp, pexprScalar,
															   pcrsInnerOutput);
	}

	CColRefArray *colref_array = nullptr;
	if (fGbOnInner)
	{
		CColRefSet *pcrsUsed = pexprScalar->DeriveUsedColumns();
		CColRefSet *pcrsGb = GPOS_NEW(mp) CColRefSet(mp);
		pcrsGb->Include(pcrsUsed);
		pcrsGb->Difference(pcrsUsedOuter);
		GPOS_ASSERT(0 < pcrsGb->Size());

		colref_array = pcrsGb->Pdrgpcr(mp);
		pcrsGb->Release();
	}
	else
	{
		if (nullptr == pexprOuter->DeriveKeyCollection())
		{
			pcrsUsedOuter->Release();
			// outer expression must have a key
			return false;
		}

		CColRefArray *pdrgpcrSystemCols =
			COptCtxt::PoctxtFromTLS()->PdrgpcrSystemCols();
		if (nullptr != pdrgpcrSystemCols && 0 < pdrgpcrSystemCols->Size())
		{
			CColRefSet *pcrsSystemCols =
				GPOS_NEW(mp) CColRefSet(mp, pdrgpcrSystemCols);
			BOOL fOuterSystemColsReqd =
				!(pcrsSystemCols->IsDisjoint(pcrsUsedOuter));
			pcrsSystemCols->Release();
			if (fOuterSystemColsReqd)
			{
				// bail out if system columns from outer's output are required since we
				// may introduce grouping on unsupported types
				pcrsUsedOuter->Release();
				return false;
			}
		}

		// generate a group by on outer columns
		CColRefArray *pdrgpcrKey = nullptr;
		colref_array = CUtils::PdrgpcrGroupingKey(mp, pexprOuter, &pdrgpcrKey);
		pdrgpcrKey->Release();	// key is not used here
	}

	*ppdrgpcr = colref_array;
	*pfGbOnInner = fGbOnInner;
	pcrsUsedOuter->Release();
	return true;
}
//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::CreateGroupByNode
//
//	@doc:
//		Given a <child>, a <colref>, an initial list of grouping columns
//		<colref_array> and a <predicate>, generate the following
//		CExpression tree:
//		- For exists subqueries:
//
//						GbAgg [<colref_array>, <colref>]
//						/			|
//					<child>			<no aggregates>
//
//		- For quantified subqueries:
//
//						GbAgg [<colref_array>]
//						/			|
//					  Proj			 count(<CR1>)
//					 /	 \			 sum(<CR2>)
//					/	  |
//				<child>	  CR2: case when <predicate> is null then 1 else 0 end
//
//		For quantified subqueries, return the newly generated colrefs for
//		the count and sum aggregates.
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::CreateGroupByNode(CMemoryPool *mp, CExpression *pexprChild,
									CColRefArray *colref_array,
									BOOL fExistential, CColRef *colref,
									CExpression *pexprPredicate,
									CColRef **pcrCount, CColRef **pcrSum)
{
	GPOS_ASSERT(nullptr == *pcrCount);
	GPOS_ASSERT(nullptr == *pcrSum);
	GPOS_ASSERT(nullptr != colref);
	// create project list of group by expression
	CExpression *pexprPrjList = nullptr;
	CExpression *pexprNewChild = pexprChild;
	if (fExistential)
	{
		// add the new column introduced by project node as a groupby column,
		// this will be "true" if the subquery returns rows and NULL if it doesn't
		colref_array->Append(colref);
		// for existential queries, we don't compute any aggregates
		pexprPrjList =
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp));
	}
	else
	{
		// quantified subqueries -- generate count(true) and sum(null indicator) expressions
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

		CExpression *pexprCount =
			CUtils::PexprCount(mp, colref, false /* is_distinct */,
			false /*is_agg_star*/);
		CScalarAggFunc *popCount =
			CScalarAggFunc::PopConvert(pexprCount->Pop());
		const IMDType *pmdtypeCount =
			md_accessor->RetrieveType(popCount->MdidType());
		*pcrCount =
			col_factory->PcrCreate(pmdtypeCount, popCount->TypeModifier());
		CExpression *pexprPrjElemCount =
			CUtils::PexprScalarProjectElement(mp, *pcrCount, pexprCount);

		pexprPredicate->AddRef();
		CExpression *sumExpr =
			CXformUtils::PexprNullIndicator(mp, pexprPredicate);
		// now make a project for sumExpr so we get a colref
		pexprNewChild = CUtils::PexprAddProjection(mp, pexprChild, sumExpr);
		CExpression *pexprPrjListForSum = (*pexprNewChild)[1];
		CColRef *colrefForSum =
			CScalarProjectElement::PopConvert((*pexprPrjListForSum)[0]->Pop())
				->Pcr();
		CExpression *pexprSum = CUtils::PexprSum(mp, colrefForSum);
		CScalarAggFunc *popSum = CScalarAggFunc::PopConvert(pexprSum->Pop());
		const IMDType *pmdtypeSum =
			md_accessor->RetrieveType(popSum->MdidType());
		*pcrSum = col_factory->PcrCreate(pmdtypeSum, popSum->TypeModifier());
		CExpression *pexprPrjElemSum =
			CUtils::PexprScalarProjectElement(mp, *pcrSum, pexprSum);

		pexprPrjList =
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
									 pexprPrjElemCount, pexprPrjElemSum);
	}

	CExpression *pexprGb = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalGbAgg(mp, colref_array,
								   COperator::EgbaggtypeGlobal /*egbaggtype*/),
		pexprNewChild, pexprPrjList);
	return pexprGb;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApplyForExistOrQuant
//
//	@doc:
//		Helper for creating an outer apply expression for existential/quantified
//		subqueries; see below for an example:
//
//		For queries similar to
//			select (select T.i <op> ANY|ALL (select R.i from R)) from T;
//
//		the expectation is to return for each row in T one of three possible values
//		{TRUE, FALSE, NULL) based on the semantics of the subquery. This depends on
//		the comparison result of T.i with all the qualifying rows of R:
//
//		set of                                               (see below)
//		comparison results  ANY subquery  ALL subquery  C3: count  C4: sum
//		T.i <op> R.i        result        result        aggregate  aggregate
//		------------------  ------------  ------------  ---------  ---------
//		{}                     false         true           0        null
//		{false}                false         false          0        null
//		{null}                 null          false          n         n
//		{true}                 true          true           n         0
//		{null, false}          null          false          n         n
//		{null, true}           true          null           n        <n
//		{false, true}          true          false          n         0
//		{null, false, true}    true          false          n        <n
//
//		For example, for an = ANY subquery:
//			- if T.i=1 and R.i = {1,2,3}, return TRUE
//			- if T.i=1 and R.i = {2,3}, return FALSE
//			- if T.i=1 and R.i = {2,3, NULL}, return NULL
//			- if T.i=1 and R.i = {}, return FALSE
//			- if T.i=1 and R.i = {NULL}, return NULL
//
//		To implement these semantics (without the need for correlated execution), the
//		optimizer can generate a special plan alternative during subquery decorrelation
//		that can be simplified as follows:
//
//		+-Select
//			+--Gb[T.i, T.ctid, T.gp_segment_id], C3 = count(C0), C4 = sum(C1)
//			|	+---Project
//			|		|---LOApply
//			|		|	|---T
//			|		|	+---Select ((T.i=R.i) IS NOT FALSE)
//			|		|		+---Project
//			|		|			|---R
//			|		|			+--- (C0: constant(true))
//			|		+---- (C1: case when (T.i=R.i) is null then 1 else 0 end)
//			+--If (C3 > 0)
//				|-- if (C3 > C4)
//				|	 +---{return TRUE}
//				|	 +---else {return NULL}
//				+---{return FALSE}
//
//		Note: The picture shows the groupby above the LOApply, but the groupby can
//			  also appear as the inner child of the LOApply
//
//		The reasoning behind this alternative is the following:
//
//		- Meaning of the two aggregate columns, C2, C3:
//			C3: The number of rows in the subquery that evaluate to NULL or true
//				(NULL means 0 such rows)
//			C4: Number of rows in the subquery that evaluate to NULL
//
//		- LOApply will return T.i values along with their matching R.i values, along
//		  with a boolean "true" (C0) that will be turned into a NULL when the subquery
//		  is empty.
//		  The LOApply will also join a T tuple with any R tuple if R.i <op> T.i is NULL,
//		  but it will eliminate all the rows where T.i <op> R.i is FALSE. The table
//		  above shows that this is ok for ANY subqueries. For ALL subqueries, those
//		  get transformed into the equivalent of an ANY subquery, and it is also ok
//		  to eliminate the FALSE rows:
//
//			x <op> ALL (SQ)		<==>	NOT(x <inv-op> ANY (SQ))
//
//		- The Gb on top of LOJ groups join results based on T.i values, and for each
//		  group, it computes the size of the group  (count(C0)), as well as the number
//		  of NULL values in R.i. The count is zero when there is no match for T.i
//		  in the LOApply.
//
//		- The Gb may appear on top of the LOJ or as its right child. If it is on top,
//		  the grouping columns will include a unique key of the left child and T.i.
//		  In this case, the count(C0) aggregate will return 0 for an empty result
//		  set from the subquery, because the outer join returns a NULL for C0.
//		  If the groupby is the right child of the LOJ, then we only group on T.i
//		  and the result of count(C0) for an empty subquery result will be a NULL,
//		  as seen from above the apply.
//
//		- After the Gb, the optimizer generates an If operator that checks the values
//		  of the two computed aggregates (C3 and C4) and determines what value
//		  (TRUE/FALSE/NULL) should be generated for each T tuple based on the IN subquery
//		  semantics described above.
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApplyForExistOrQuant(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpression *pexprSubquery, CExpression *pexprPredicate,
	BOOL fOuterRefsUnderInner, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar, BOOL useNotNullableInnerOpt)
{
	BOOL fExistential = CUtils::FExistentialSubquery(pexprSubquery->Pop());

	CColRefArray *colref_array = nullptr;
	BOOL fGbOnInner = false;
	if (!FCreateGrpCols(mp, pexprOuter, pexprInner, fExistential,
						fOuterRefsUnderInner, &colref_array, &fGbOnInner))
	{
		// creating outer apply expression has failed
		return false;
	}
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	// add a project node on top of inner expression
	CExpression *pexprPrjInner = nullptr;

	AddProjectNode(mp, pexprInner, &pexprPrjInner);
	CExpression *pexprPrjList = (*pexprPrjInner)[1];
	CColRef *colref =
		CScalarProjectElement::PopConvert((*pexprPrjList)[0]->Pop())->Pcr();
	const CColRef *pcrSubquery = colref;


	if (!fExistential)
	{
		// for quantified subqueries, we are going to use <outer col> <op> <inner col> for
		// the actual comparison, but we'll pass just <inner col> to the apply as the inner
		// colref
		pcrSubquery =
			CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop())->Pcr();
	}

	CColRef *pcrCount = nullptr;
	CColRef *pcrSum = nullptr;

	if (fGbOnInner)
	{
		CExpression *pexprGb = CreateGroupByNode(
			mp,
			pexprPrjInner,	// child on which to create the project
			colref_array,	// group by cols
			fExistential,
			colref,			 // "true"
			pexprPredicate,	 // comparison op for quantified SQ
			&pcrCount,		 // out: count aggregate colref
			&pcrSum);		 // out: sum aggregate colref

		// generate an outer apply between outer expression and a group by on inner expression
		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
			mp, pexprOuter, pexprGb, pcrSubquery, COperator::EopScalarSubquery);
	}
	else
	{
		// generate an outer apply between outer expression and the new project expression
		CExpression *result = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
			mp, pexprOuter, pexprPrjInner, pcrSubquery,
			COperator::EopScalarSubquery);

		*ppexprNewOuter =
			CreateGroupByNode(mp, result, colref_array, fExistential, colref,
							  pexprPredicate, &pcrCount, &pcrSum);
	}

	// residual scalar examines introduced columns
	*ppexprResidualScalar = PexprScalarIf(
		mp, colref, pcrSum, pcrCount, pexprSubquery, useNotNullableInnerOpt);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateOuterApply
//
//	@doc:
//		Helper for creating an outer apply expression
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateOuterApply(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprInner,
	CExpression *pexprSubquery, CExpression *pexprPredicate,
	BOOL fOuterRefsUnderInner, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar, BOOL useNotNullableInnerOpt)
{
	COperator *popSubquery = pexprSubquery->Pop();
	BOOL fExistential = CUtils::FExistentialSubquery(popSubquery);
	BOOL fQuantified = CUtils::FQuantifiedSubquery(popSubquery);

	if (fExistential || fQuantified)
	{
		return FCreateOuterApplyForExistOrQuant(
			mp, pexprOuter, pexprInner, pexprSubquery, pexprPredicate,
			fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar,
			useNotNullableInnerOpt);
	}

	return FCreateOuterApplyForScalarSubquery(
		mp, pexprOuter, pexprInner, pexprSubquery, fOuterRefsUnderInner,
		ppexprNewOuter, ppexprResidualScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForQuantifiedSubquery
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		quantified subquery
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForQuantifiedSubquery(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar)
{
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopidSubq ||
				COperator::EopScalarSubqueryAll == eopidSubq);

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];
	CScalarSubqueryQuantified *popSubquery =
		CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop());
	CColRef *colref = const_cast<CColRef *>(popSubquery->Pcr());

	// If subq is SubqueryAll and scalar child is a subquery then it must be
	// treated in "Value" context. For example:
	//
	//   SELECT * FROM foo WHERE (SELECT a FROM foo limit 1) = ALL(SELECT b FROM bar);
	//
	//   +--CScalarSubqueryAll(=)["b" (8)]
	//      |--CLogicalGet "bar" ("bar"),
	//      +--CScalarSubquery["a" (16)]
	//         +--CLogicalLimit <empty> global
	//            |--CLogicalGet "foo" ("foo"),
	//            |--CScalarConst (0)
	//            +--CScalarCast
	//               +--CScalarConst (1)
	//
	// "Value" context signals FGenerateCorrelatedApplyForScalarSubquery() to
	// generate a left outer apply as opposed to an inner apply. This is
	// necessary in order to avoid incorrectly filtering out non-matching rows
	// which are required to correctly determine the ALL_SUBLINK result.
	if ((*pexprSubquery)[1]->Pop()->Eopid() == COperator::EopScalarSubquery &&
		eopidSubq == COperator::EopScalarSubqueryAll)
	{
		esqctxt = EsqctxtValue;
	}

	// build subquery quantified comparison
	CExpression *pexprResult = nullptr;
	CSubqueryHandler sh(mp, true /* fEnforceCorrelatedApply */);
	CExpression *pexprPredicate =
		sh.PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult, esqctxt);

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi-IN/anti-semi-NOT-IN apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryAny == eopidSubq)
		{
			*ppexprNewOuter =
				CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApplyIn>(
					mp, pexprOuter, pexprResult, colref, eopidSubq,
					pexprPredicate);
		}
		else
		{
			*ppexprNewOuter = CUtils::PexprLogicalApply<
				CLogicalLeftAntiSemiCorrelatedApplyNotIn>(
				mp, pexprOuter, pexprResult, colref, eopidSubq, pexprPredicate);
		}
		*ppexprResidualScalar =
			CUtils::PexprScalarConstBool(mp, true /*value*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue = CUtils::PexprAddProjection(
		mp, pexprResult, CUtils::PexprScalarConstBool(mp, true /*value*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert(
						   (*(*pexprProjectConstTrue)[1])[0]->Pop())
						   ->Pcr();

	// add the created column and subquery column to required inner columns
	CColRefArray *pdrgpcrInner = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(colref);

	*ppexprNewOuter =
		CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
			mp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq,
			pexprPredicate);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrBool);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForExistentialSubquery
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		existential subquery
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForExistentialSubquery(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar)
{
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == eopidSubq ||
				COperator::EopScalarSubqueryNotExists == eopidSubq);

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];

	// for existential subqueries, any column produced by inner expression
	// can be used to check for empty answers; we use first column for that
	CColRef *colref = pexprInner->DeriveOutputColumns()->PcrFirst();

	pexprInner->AddRef();
	if (EsqctxtFilter == esqctxt)
	{
		// we can use correlated semi/anti-semi apply here since the subquery is used in filtering context
		if (COperator::EopScalarSubqueryExists == eopidSubq)
		{
			CColRefSet *outer_refs = pexprInner->DeriveOuterReferences();
			if (0 == outer_refs->Size())
			{
				// add a limit operator on top of the inner child if the subquery does not have
				// any outer references. Adding Limit for the correlated case hinders pulling up
				// predicates into an EXISTS join
				pexprInner = AddOrReplaceLimitOne(mp, pexprInner);
			}

			*ppexprNewOuter =
				CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(
					mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		else
		{
			*ppexprNewOuter =
				CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApply>(
					mp, pexprOuter, pexprInner, colref, eopidSubq);
		}
		*ppexprResidualScalar =
			CUtils::PexprScalarConstBool(mp, true /*value*/);

		return true;
	}

	// subquery occurs in a value context or disjunction, we need to create an outer apply expression
	// add a project node with constant true to be used as subplan place holder
	CExpression *pexprProjectConstTrue = CUtils::PexprAddProjection(
		mp, pexprInner, CUtils::PexprScalarConstBool(mp, true /*value*/));
	CColRef *pcrBool = CScalarProjectElement::PopConvert(
						   (*(*pexprProjectConstTrue)[1])[0]->Pop())
						   ->Pcr();

	// add the created column and subquery column to required inner columns
	CColRefArray *pdrgpcrInner = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrInner->Append(pcrBool);
	pdrgpcrInner->Append(colref);

	*ppexprNewOuter =
		CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
			mp, pexprOuter, pexprProjectConstTrue, pdrgpcrInner, eopidSubq);

	// we replace existential subquery with the boolean column we created,
	// Expr2DXL translator replaces this column with a subplan node
	*ppexprResidualScalar = CUtils::PexprScalarIdent(mp, pcrBool);

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FCreateCorrelatedApplyForExistOrQuant
//
//	@doc:
//		Helper for creating a correlated apply expression for
//		quantified/existential subqueries
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FCreateCorrelatedApplyForExistOrQuant(
	CMemoryPool *mp, CExpression *pexprOuter, CExpression *pexprSubquery,
	ESubqueryCtxt esqctxt, CExpression **ppexprNewOuter,
	CExpression **ppexprResidualScalar)
{
	BOOL fExistential = CUtils::FExistentialSubquery(pexprSubquery->Pop());
#ifdef GPOS_DEBUG
	BOOL fQuantified = CUtils::FQuantifiedSubquery(pexprSubquery->Pop());
#endif	// GPOS_DEBUG
	GPOS_ASSERT(fExistential || fQuantified);

	if (fExistential)
	{
		return FCreateCorrelatedApplyForExistentialSubquery(
			mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

	return FCreateCorrelatedApplyForQuantifiedSubquery(
		mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
		ppexprResidualScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveAnySubquery
//
//	@doc:
//		Replace a subquery ANY node <pexprSubquery> with a new predicate
//		<*ppexprResidualScalar> and put the relational part of the subquery
//		on top of an existing relational node <pexprOuter>:
//
//		Example, for filter context:
//
//			SELECT									SELECT
//			/		|								/		|
//			R		=			==>		LEFT-SEMI-APPLY		true
//				/	|					/		|
//				R.a	ANY					R		SELECT
//					/	|						/	 |
//					S	S.b						S	  =
//													/	|
//													R.a	S.b
//
//		For a picture of the tree in a value context, see method
//		CSubqueryHandler::FCreateOuterApplyForExistOrQuant().
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveAnySubquery(CExpression *pexprOuter,
									 CExpression *pexprSubquery,
									 ESubqueryCtxt esqctxt,
									 CExpression **ppexprNewOuter,
									 CExpression **ppexprResidualScalar)
{
	CMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	CScalarSubqueryAny *pScalarSubqAny =
		CScalarSubqueryAny::PopConvert(pexprSubquery->Pop());

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(
			mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

	// get the logical child of subquery
	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();
	const CColRef *colref =
		CScalarSubqueryAny::PopConvert(pexprSubquery->Pop())->Pcr();
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();

	// build subquery quantified comparison
	CExpression *pexprResult = nullptr;
	CExpression *pexprPredicate =
		PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult, esqctxt);

	// generate a select for the quantified predicate
	pexprInner->AddRef();
	CExpression *pexprSelect =
		CUtils::PexprLogicalSelect(mp, pexprResult, pexprPredicate);
	BOOL fSuccess = true;
	BOOL fUseCorrelated = false;
	BOOL fUseNotNullableInnerOpt = false;

	// "a IN (SELECT count(*) ... WHERE y = a)": the subquery is a GROUP BY ()
	// aggregate over correlated input, so before HAVING it returns exactly
	// one row, and IN is equivalent to "a = (SELECT count(*) ...)". A semi-join
	// would drop outer rows without matching inner rows, for which the subquery
	// returns the aggregate's empty-input value, losing the count() = 0 match (the
	// "count bug"). Instead unnest it like a scalar subquery, which keeps
	// every outer row, and compare against its value, see
	// PexprScalarAggApply(). This is needed in both contexts.
	//
	// An uncorrelated aggregate always yields exactly one row, so the
	// semi-join stays correct there and costs far less.
	if (fOuterRefsUnderInner && FNeedsEmptyInputValue(pexprInner) &&
		FAggFuncsEmptyInputSafe((*PexprCorrelatedScalarAgg(pexprInner))[1]) &&
		!FHasCorrelatedScalarAggNotNullOnEmpty(
			(*PexprCorrelatedScalarAgg(pexprInner))[0]))
	{
		pexprSelect->Release();

		pexprInner->AddRef();
		CExpression *pexprValue = nullptr;
		CExpression *pexprHaving = nullptr;
		*ppexprNewOuter = PexprScalarAggApply(mp, pexprOuter, pexprInner,
											  colref, &pexprValue, &pexprHaving);

		// residual: outer_expr <op> subquery value
		IMDId *mdid_op = pScalarSubqAny->MdIdOp();
		mdid_op->AddRef();
		CExpression *pexprOuterScalar = (*pexprSubquery)[1];
		pexprOuterScalar->AddRef();
		*ppexprResidualScalar =
			CUtils::PexprScalarCmp(mp, pexprOuterScalar, pexprValue,
								   *pScalarSubqAny->PstrOp(), mdid_op);
		if (nullptr != pexprHaving)
		{
			IMDId *mdid_bool =
				COptCtxt::PoctxtFromTLS()->Pmda()->PtMDType<IMDTypeBool>()->MDId();
			mdid_bool->AddRef();
			*ppexprResidualScalar = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarIf(mp, mdid_bool), pexprHaving,
				*ppexprResidualScalar, CUtils::PexprScalarConstBool(mp, false));
		}

		return true;
	}

	// The same aggregate deeper down in the subquery, e.g. under another
	// aggregate, has no such fix-up, so keep the correlated execution there.
	if (fOuterRefsUnderInner)
	{
		CExpression *pexprGbAgg = PexprCorrelatedScalarAgg(pexprInner);
		CExpression *pexprToCheck =
			(nullptr != pexprGbAgg) ? (*pexprGbAgg)[0] : pexprInner;

		if (FHasCorrelatedScalarAggNotNullOnEmpty(pexprToCheck) ||
			(nullptr != pexprGbAgg &&
			 !FAggFuncsEmptyInputSafe((*pexprGbAgg)[1])))
		{
			pexprSelect->Release();
			return FCreateCorrelatedApplyForExistOrQuant(
				mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
				ppexprResidualScalar);
		}
	}

	if (EsqctxtValue == esqctxt)
	{
		CExpression *pexprNewSelect = PexprInnerSelect(
			mp, colref, pexprResult, pexprPredicate, &fUseNotNullableInnerOpt);
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDScalarOp *pmdOp =
			md_accessor->RetrieveScOp(pScalarSubqAny->MdIdOp());
		// function attributes of the comparison operator itself
		// TODO: Synthesize the function attibutes of general operators, like
		//       CScalarSubqueryAny/All, CScalarCmp, CScalarOp by providing a
		//       DeriveFunctionProperties() method in these classes.
		//       Once we do that, we can remove the line below and related code.
		const IMDFunction *pmdFunc =
			md_accessor->RetrieveFunc(pmdOp->FuncMdId());

		if (IMDFunction::EfsVolatile == pmdFunc->GetFuncStability() ||
			IMDFunction::EfsVolatile ==
				pexprSubquery->DeriveScalarFunctionProperties()->Efs())
		{
			// the non-correlated plan would evaluate the comparison operation twice
			// per outer row, that is not a good idea when the operation is volatile
			fUseCorrelated = true;
		}

		pexprSelect->Release();
		pexprSelect = pexprNewSelect;

		if (!fUseCorrelated)
		{
			fSuccess = FCreateOuterApply(
				mp, pexprOuter, pexprSelect, pexprSubquery, pexprPredicate,
				fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar,
				fUseNotNullableInnerOpt);
		}
		if (!fSuccess || fUseCorrelated)
		{
			pexprSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(
				mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
				ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApplyIn>(
			mp, pexprOuter, pexprSelect, colref, eopidSubq);
		*ppexprResidualScalar =
			CUtils::PexprScalarConstBool(mp, true /*value*/);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprIsNotNull
//
//	@doc:
//		Helper for adding nullness check, only if needed, to the given
//		scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprIsNotNull(
	CMemoryPool *mp, CExpression *pexprOuter,
	CExpression *pexprLogical,	// the logical parent of scalar expression
	CExpression *pexprScalar)
{
	pexprScalar->AddRef();
	BOOL fUsesInnerNullable =
		CUtils::FUsesNullableCol(mp, pexprScalar, pexprLogical);

	if (fUsesInnerNullable)
	{
		CColRefSet *pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
		pcrsUsed->Include(pexprScalar->DeriveUsedColumns());
		pcrsUsed->Intersection(pexprOuter->DeriveOutputColumns());
		BOOL fHasOuterRefs = (0 < pcrsUsed->Size());
		pcrsUsed->Release();

		if (fHasOuterRefs)
		{
			return CUtils::PexprIsNotNull(mp, pexprScalar);
		}
	}

	return pexprScalar;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveAllSubquery
//
//	@doc:
//		Replace a subquery ALL node <pexprSubquery> with a new predicate
//		<*ppexprResidualScalar> and put the relational part of the subquery
//		on top of an existing relational node <pexprOuter>:
//
//		Example, for filter context:
//
//		 SELECT									SELECT
//		/		|								/		|
//		R		<>			==>			LAS-APPLY		true
//				/	|				 /			  |
//			   R.a	ALL			SELECT			  |
//			  		/	|		/	|				SELECT
//					S	S.b		R	IS_NOT_NULL		/	|
//											|		S	IS_NOT_FALSE
//											R.a				  |
//															  =
//															/	|
//															R.a	S.b
//
//		For a picture of the tree in a value context, see method
//		CSubqueryHandler::FCreateOuterApplyForExistOrQuant().
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveAllSubquery(CExpression *pexprOuter,
									 CExpression *pexprSubquery,
									 ESubqueryCtxt esqctxt,
									 CExpression **ppexprNewOuter,
									 CExpression **ppexprResidualScalar)
{
	CMemoryPool *mp = m_mp;
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(
			mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

	BOOL fSuccess = true;
	BOOL fUseCorrelated = false;
	CExpression *pexprPredicate = nullptr;
	CExpression *pexprInner = (*pexprSubquery)[0];
	COperator::EOperatorId eopidSubq = pexprSubquery->Pop()->Eopid();
	const CColRef *colref =
		CScalarSubqueryAll::PopConvert(pexprSubquery->Pop())->Pcr();

	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();
	BOOL fUseNotNullOptimization = false;
	pexprInner->AddRef();

	if (fOuterRefsUnderInner)
	{
		// outer references in SubqueryAll necessitate correlated execution for correctness
		// Only in the filter context, do we want to create a CLogicalLeftAntiSemiCorrelatedApplyNotIn
		if (EsqctxtFilter == esqctxt)
		{
			// build subquery quantified comparison
			CExpression *pexprResult = nullptr;
			CExpression *pexprPredicate = PexprSubqueryPred(
				pexprInner, pexprSubquery, &pexprResult, esqctxt);

			*ppexprResidualScalar =
				CUtils::PexprScalarConstBool(mp, true /*value*/);
			*ppexprNewOuter = CUtils::PexprLogicalApply<
				CLogicalLeftAntiSemiCorrelatedApplyNotIn>(
				mp, pexprOuter, pexprResult, colref, eopidSubq, pexprPredicate);

			return fSuccess;
		}
		else
		{
			fUseCorrelated = true;
		}
	}

	// generate a select with the inverse predicate as the selection predicate
	// TODO: Handle the case where pexprInversePred == NULL
	CExpression *pexprInversePred =
		CXformUtils::PexprInversePred(mp, pexprSubquery);
	pexprPredicate = pexprInversePred;

	if (EsqctxtValue == esqctxt)
	{
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		CScalarCmp *scalarComp = CScalarCmp::PopConvert(pexprPredicate->Pop());

		if (nullptr != scalarComp)
		{
			const IMDScalarOp *pmdOp =
				md_accessor->RetrieveScOp(scalarComp->MdIdOp());
			const IMDFunction *pmdFunc =
				md_accessor->RetrieveFunc(pmdOp->FuncMdId());
			if (IMDFunction::EfsVolatile == pmdFunc->GetFuncStability())
			{
				// the non-correlated plan would evaluate the comparison operation twice
				// per outer row, that is not a good idea when the operation is volatile
				fUseCorrelated = true;
			}
		}

		CExpression *pexprInnerSelect = PexprInnerSelect(
			mp, colref, pexprInner, pexprPredicate, &fUseNotNullOptimization);

		if (!fUseCorrelated)
		{
			fSuccess = FCreateOuterApply(
				mp, pexprOuter, pexprInnerSelect, pexprSubquery, pexprPredicate,
				fOuterRefsUnderInner, ppexprNewOuter, ppexprResidualScalar,
				fUseNotNullOptimization);
		}
		if (!fSuccess || fUseCorrelated)
		{
			pexprInnerSelect->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(
				mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
				ppexprResidualScalar);
		}

		// cleanup
		pexprInner->Release();
		pexprPredicate->Release();
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		*ppexprResidualScalar = CUtils::PexprScalarConstBool(mp, true);
		*ppexprNewOuter =
			CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApplyNotIn>(
				mp, pexprOuter, pexprInner, colref, eopidSubq, pexprPredicate);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::AddProjectNode
//
//	@doc:
//		Helper for adding a Project node with a const TRUE on top of
//		the given expression;
//		For EXISTS subqueries, this acts as the select list. For
//		quantified subqueries, this is used as the argument of the
//		count aggregate to count the number of rows in the subquery.
//
//---------------------------------------------------------------------------
void
CSubqueryHandler::AddProjectNode(CMemoryPool *mp, CExpression *pexpr,
								 CExpression **ppexprResult)
{
	GPOS_ASSERT(nullptr != ppexprResult);

	CExpression *pexprProjected =
		CUtils::PexprScalarConstBool(mp, true /*value*/);

	*ppexprResult = CUtils::PexprAddProjection(mp, pexpr, pexprProjected);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::PexprScalarIf
//
//	@doc:
//		Helper for creating a scalar if expression used when generating
//		an outer apply
//
//---------------------------------------------------------------------------
CExpression *
CSubqueryHandler::PexprScalarIf(CMemoryPool *mp, CColRef *pcrBool,
								CColRef *pcrSum, CColRef *pcrCount,
								CExpression *pexprSubquery,
								BOOL useNotNullableInnerOpt)
{
	COperator *popSubquery = pexprSubquery->Pop();
	BOOL fExistential = CUtils::FExistentialSubquery(popSubquery);
#ifdef GPOS_DEBUG
	BOOL fQuantified = CUtils::FQuantifiedSubquery(popSubquery);
#endif	// GPOS_DEBUG
	COperator::EOperatorId op_id = popSubquery->Eopid();

	GPOS_ASSERT(fExistential || fQuantified);
	GPOS_ASSERT_IMP(fExistential, nullptr != pcrBool);
	GPOS_ASSERT_IMP(fQuantified, nullptr != pcrSum && nullptr != pcrCount);
	GPOS_ASSERT_IMP(useNotNullableInnerOpt, !fExistential);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDTypeBool *pmdtypebool = md_accessor->PtMDType<IMDTypeBool>();
	IMDId *mdid = pmdtypebool->MDId();

	BOOL value = true;
	if (COperator::EopScalarSubqueryNotExists == op_id ||
		COperator::EopScalarSubqueryAll == op_id)
	{
		value = false;
	}

	if (fExistential)
	{
		CExpression *pexprIsNotNull =
			CUtils::PexprIsNotNull(mp, CUtils::PexprScalarIdent(mp, pcrBool));
		mdid->AddRef();
		return GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarIf(mp, mdid), pexprIsNotNull,
						CUtils::PexprScalarConstBool(mp, value),
						CUtils::PexprScalarConstBool(mp, !value));
	}

	// quantified subquery

	// sum = count
	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrSum, pcrCount);

	CExpression *pexprRowCountGreaterZero = CUtils::PexprScalarCmp(
		mp, pcrCount, CUtils::PexprScalarConstInt8(mp, 0), IMDType::EcmptG);

	mdid->AddRef();

	// If sum(null indicators) = count(*), that means that the subquery comparison op returned some
	// NULL values and maybe some "false" values, but no "true" values. In that case, the subquery
	// evaluates to NULL. Otherwise, the count must be greater than sum(null indicators). Since we
	// are eliminating "false" values before we reach the groupby, that means that some of the rows
	// in the subquery evaluated to "true" in the comparison op. That means the subquery evaluates
	// to "value".

	CExpression *pexprScalarIf = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarIf(mp, mdid), pexprEquality,
		CUtils::PexprScalarConstBool(mp, false /*value*/, true /*is_null*/),
		CUtils::PexprScalarConstBool(mp, value));

	// If count(row indicator) = 0, or count(row indicator) is NULL that means that the subquery
	// returned no rows (except maybe some rows that evaluated to "false"). The subquery evaluates
	// to "!value".
	// Otherwise, the result is determined by the expression in pexprScalarIf that we built above.
	// create the outer if: if (count(...) > 0) then <inner if> else <!value>

	mdid->AddRef();
	CExpression *result = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarIf(mp, mdid), pexprRowCountGreaterZero,
		pexprScalarIf, CUtils::PexprScalarConstBool(mp, !value));

	if (useNotNullableInnerOpt)
	{
		// add another case/if statement that returns NULL if the outer column is null,
		// we do this if the inner column is not nullable and if we use a well-known
		// comparison operator
		CExpression *pexprScalar = (*pexprSubquery)[1];
		pexprScalar->AddRef();
		mdid->AddRef();
		result = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarIf(mp, mdid),
			CUtils::PexprIsNull(mp, pexprScalar),
			CUtils::PexprScalarConstBool(mp, false /*value*/, true /*is_null*/),
			result);
	}

	return result;
}

// add a limit 1 expression over given expression,
// removing any existing limits
CExpression *
CSubqueryHandler::AddOrReplaceLimitOne(CMemoryPool *mp, CExpression *pexpr)
{
	if (COperator::EopLogicalLimit == pexpr->Pop()->Eopid() &&
		CUtils::FHasZeroOffset(pexpr))
	{
		// If the expression is LIMIT expression with zero OFFSET
		//  then remove existing LIMIT before adding a new LIMIT with COUNT = 1
		CExpression *old_limit_expr = pexpr;
		pexpr = (*pexpr)[0];
		pexpr->AddRef();
		old_limit_expr->Release();
	}
	return CUtils::PexprLimit(mp, pexpr, 0, 1);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveExistentialSubquery
//
//	@doc:
//		Replace a subquery EXISTS/NOT EXISTS node with a constant True, and
//		create a new Apply expression
//
//
//		Example:
//
//				SELECT										SELECT
//				/		|									/			|
//				R		EXISTS		==>				LEFT-SEMI-APPLY		true
//							|						/		|
//							S						R		S
//
//
//			SELECT											SELECT
//			/		|										/			|
//			R		NOT EXISTS		==>			LEFT-ANTI-SEMI-APPLY	true
//						|						/			|
//						S						R			S
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveExistentialSubquery(
	CMemoryPool *mp, COperator::EOperatorId op_id, CExpression *pexprOuter,
	CExpression *pexprSubquery, ESubqueryCtxt esqctxt,
	CExpression **ppexprNewOuter, CExpression **ppexprResidualScalar)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprSubquery, ppexprNewOuter,
						 ppexprResidualScalar);
	GPOS_ASSERT(COperator::EopScalarSubqueryExists == op_id ||
				COperator::EopScalarSubqueryNotExists == op_id);
	GPOS_ASSERT(op_id == pexprSubquery->Pop()->Eopid());
#endif	// GPOS_DEBUG

	CExpression *pexprInner = (*pexprSubquery)[0];
	BOOL fOuterRefsUnderInner = pexprInner->HasOuterRefs();

	// we always add-ref Apply's inner child since it is reused from subquery
	// inner expression

	pexprInner->AddRef();

	BOOL fSuccess = true;
	if (EsqctxtValue == esqctxt)
	{
		fSuccess = FCreateOuterApply(mp, pexprOuter, pexprInner, pexprSubquery,
									 nullptr /* pexprPredicate */,
									 fOuterRefsUnderInner, ppexprNewOuter,
									 ppexprResidualScalar, false);
		if (!fSuccess)
		{
			pexprInner->Release();
			fSuccess = FCreateCorrelatedApplyForExistOrQuant(
				mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
				ppexprResidualScalar);
		}
	}
	else
	{
		GPOS_ASSERT(EsqctxtFilter == esqctxt);

		// for existential subqueries, any column produced by inner expression
		// can be used to check for empty answers; we use first column for that
		CColRef *colref = pexprInner->DeriveOutputColumns()->PcrFirst();

		if (COperator::EopScalarSubqueryExists == op_id)
		{
			CColRefSet *outer_refs = pexprInner->DeriveOuterReferences();

			if (0 == outer_refs->Size())
			{
				// add a limit operator on top of the inner child if the subquery does not have
				// any outer references.
				pexprInner = AddOrReplaceLimitOne(mp, pexprInner);
			}
			*ppexprNewOuter = CUtils::PexprLogicalApply<CLogicalLeftSemiApply>(
				mp, pexprOuter, pexprInner, colref, op_id);
		}
		else
		{
			*ppexprNewOuter =
				CUtils::PexprLogicalApply<CLogicalLeftAntiSemiApply>(
					mp, pexprOuter, pexprInner, colref, op_id);
		}
		*ppexprResidualScalar =
			CUtils::PexprScalarConstBool(mp, true /*value*/);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveExistsSubquery
//
//	@doc:
//		Replace a subquery EXISTS node with a constant True, and
//		create a new Apply expression

//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveExistsSubquery(CExpression *pexprOuter,
										CExpression *pexprSubquery,
										ESubqueryCtxt esqctxt,
										CExpression **ppexprNewOuter,
										CExpression **ppexprResidualScalar)
{
	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(
			m_mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery(m_mp, COperator::EopScalarSubqueryExists,
									  pexprOuter, pexprSubquery, esqctxt,
									  ppexprNewOuter, ppexprResidualScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRemoveNotExistsSubquery
//
//	@doc:
//		Replace a subquery NOT EXISTS node with a constant True, and
//		create a new Apply expression
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRemoveNotExistsSubquery(CExpression *pexprOuter,
										   CExpression *pexprSubquery,
										   ESubqueryCtxt esqctxt,
										   CExpression **ppexprNewOuter,
										   CExpression **ppexprResidualScalar)
{
	if (m_fEnforceCorrelatedApply)
	{
		return FCreateCorrelatedApplyForExistOrQuant(
			m_mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

	return FRemoveExistentialSubquery(
		m_mp, COperator::EopScalarSubqueryNotExists, pexprOuter, pexprSubquery,
		esqctxt, ppexprNewOuter, ppexprResidualScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FRecursiveHandler
//
//	@doc:
//		Handle subqueries in scalar tree recursively
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FRecursiveHandler(CExpression *pexprOuter,
									CExpression *pexprScalar,
									ESubqueryCtxt esqctxt,
									CExpression **ppexprNewOuter,
									CExpression **ppexprResidualScalar)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;

	CMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprScalar, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	COperator *popScalar = pexprScalar->Pop();

	if (CPredicateUtils::FOr(pexprScalar) ||
		CPredicateUtils::FNot(pexprScalar) ||
		COperator::EopScalarProjectElement == popScalar->Eopid())
	{
		// set subquery context to Value
		esqctxt = EsqctxtValue;
	}

	// save the current logical expression
	CExpression *pexprCurrentOuter = pexprOuter;
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprScalar->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprScalarChild = (*pexprScalar)[ul];
		COperator *popScalarChild = pexprScalarChild->Pop();
		CExpression *pexprNewLogical = nullptr;
		CExpression *pexprNewScalar = nullptr;

		// Set the subquery context to Value for a non-scalar subquery nested in a
		// scalar expression such that the corresponding subquery unnesting routines
		// will return a column identifier. The identifier is then used to replace the
		// subquery node in the scalar expression.
		//
		// Note that conjunction is special cased here. Multiple non-scalar subqueries
		// will produce nested Apply expressions which are implicitly AND-ed. Thus,
		// unlike disjuctions or negations, it is not neccesary to use the Value
		// context for disjunctions.  This enables us to generate optimal plans with
		// nested joins for queries like:
		// SELECT * FROM t3 WHERE EXISTS(SELECT c FROM t2) AND NOT EXISTS (SELECT c from t3);
		//
		// However, if the AND is nested inside another scalar expression then it must
		// be treated in Value context from this point onwards because an implicit AND
		// does not apply within a scalar expression:
		// SELECT * FROM t1 WHERE b = (EXISTS(SELECT c FROM t2) AND NOT EXISTS (SELECT c from t3));
		//
		// Also note that at this stage we would never have a AND expression whose
		// immediate child is another AND since the conjuncts are always unnested
		// during preprocessing - but this case is handled in anyway.
		if (!CPredicateUtils::FAnd(pexprScalar) &&
			(CPredicateUtils::FAnd(pexprScalarChild) ||
			 CUtils::FExistentialSubquery(popScalarChild) ||
			 CUtils::FQuantifiedSubquery(popScalarChild)))
		{
			esqctxt = EsqctxtValue;
		}

		if (!FProcess(pexprCurrentOuter, pexprScalarChild, esqctxt,
					  &pexprNewLogical, &pexprNewScalar))
		{
			// subquery unnesting failed, cleanup created expressions
			*ppexprNewOuter = pexprCurrentOuter;
			pdrgpexpr->Release();

			return false;
		}

		GPOS_ASSERT(nullptr != pexprNewScalar);

		if (pexprScalarChild->DeriveHasSubquery())
		{
			// the logical expression must have been updated during recursion
			GPOS_ASSERT(nullptr != pexprNewLogical);

			// update current logical expression based on recursion results
			pexprCurrentOuter = pexprNewLogical;
		}
		pdrgpexpr->Append(pexprNewScalar);
	}

	*ppexprNewOuter = pexprCurrentOuter;
	COperator *pop = pexprScalar->Pop();
	pop->AddRef();
	*ppexprResidualScalar = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProcessScalarOperator
//
//	@doc:
//		Handle subqueries on a case-by-case basis
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProcessScalarOperator(CExpression *pexprOuter,
										 CExpression *pexprScalar,
										 ESubqueryCtxt esqctxt,
										 CExpression **ppexprNewOuter,
										 CExpression **ppexprResidualScalar)
{
	CMemoryPool *mp = m_mp;

#ifdef GPOS_DEBUG
	AssertValidArguments(mp, pexprOuter, pexprScalar, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	BOOL fSuccess = false;
	COperator::EOperatorId op_id = pexprScalar->Pop()->Eopid();
	switch (op_id)
	{
		case COperator::EopScalarSubquery:
			fSuccess =
				FRemoveScalarSubquery(pexprOuter, pexprScalar, esqctxt,
									  ppexprNewOuter, ppexprResidualScalar);
			break;
		case COperator::EopScalarSubqueryAny:
			fSuccess = FRemoveAnySubquery(pexprOuter, pexprScalar, esqctxt,
										  ppexprNewOuter, ppexprResidualScalar);
			break;
		case COperator::EopScalarSubqueryAll:
			fSuccess = FRemoveAllSubquery(pexprOuter, pexprScalar, esqctxt,
										  ppexprNewOuter, ppexprResidualScalar);
			break;
		case COperator::EopScalarSubqueryExists:
			fSuccess =
				FRemoveExistsSubquery(pexprOuter, pexprScalar, esqctxt,
									  ppexprNewOuter, ppexprResidualScalar);
			break;
		case COperator::EopScalarSubqueryNotExists:
			fSuccess =
				FRemoveNotExistsSubquery(pexprOuter, pexprScalar, esqctxt,
										 ppexprNewOuter, ppexprResidualScalar);
			break;
		case COperator::EopScalarBoolOp:
		case COperator::EopScalarProjectList:
		case COperator::EopScalarProjectElement:
		case COperator::EopScalarCmp:
		case COperator::EopScalarOp:
		case COperator::EopScalarIsDistinctFrom:
		case COperator::EopScalarNullTest:
		case COperator::EopScalarBooleanTest:
		case COperator::EopScalarIf:
		case COperator::EopScalarFunc:
		case COperator::EopScalarCast:
		case COperator::EopScalarCoerceToDomain:
		case COperator::EopScalarCoerceViaIO:
		case COperator::EopScalarArrayCoerceExpr:
		case COperator::EopScalarAggFunc:
		case COperator::EopScalarWindowFunc:
		case COperator::EopScalarArray:
		case COperator::EopScalarArrayRef:
		case COperator::EopScalarArrayRefIndexList:
		case COperator::EopScalarArrayCmp:
		case COperator::EopScalarCoalesce:
		case COperator::EopScalarCaseTest:
		case COperator::EopScalarNullIf:
		case COperator::EopScalarSwitch:
		case COperator::EopScalarSwitchCase:
		case COperator::EopScalarValuesList:
		case COperator::EopScalarMinMax:
			fSuccess = FRecursiveHandler(pexprOuter, pexprScalar, esqctxt,
										 ppexprNewOuter, ppexprResidualScalar);
			break;
		default:
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnexpectedOp,
					   GPOS_WSZ_LIT("Subquery in unexpected context"));
	}

	if (fSuccess)
	{
		// clean-up unnecessary equality operations
		CExpression *pexprPruned =
			CPredicateUtils::PexprPruneSuperfluosEquality(
				mp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar = pexprPruned;

		// cleanup unncessary conjuncts
		CExpressionArray *pdrgpexpr =
			CPredicateUtils::PdrgpexprConjuncts(mp, *ppexprResidualScalar);
		(*ppexprResidualScalar)->Release();
		*ppexprResidualScalar =
			CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
	}

	return fSuccess;
}


//---------------------------------------------------------------------------
//	@function:
//		CSubqueryHandler::FProcess
//
//	@doc:
//		Main driver;
//
//
//---------------------------------------------------------------------------
BOOL
CSubqueryHandler::FProcess(
	CExpression *pexprOuter,   // logical child of a SELECT node
	CExpression *pexprScalar,  // scalar child of a SELECT node
	ESubqueryCtxt esqctxt,	   // context in which subquery occurs
	CExpression *
		*ppexprNewOuter,  // an Apply logical expression produced as output
	CExpression *
		*ppexprResidualScalar  // residual scalar expression produced as output
)
{
#ifdef GPOS_DEBUG
	AssertValidArguments(m_mp, pexprOuter, pexprScalar, ppexprNewOuter,
						 ppexprResidualScalar);
#endif	// GPOS_DEBUG

	if (!pexprScalar->DeriveHasSubquery())
	{
		// no subqueries, add-ref root node and return immediately
		pexprScalar->AddRef();
		*ppexprResidualScalar = pexprScalar;

		return true;
	}

	return FProcessScalarOperator(pexprOuter, pexprScalar, esqctxt,
								  ppexprNewOuter, ppexprResidualScalar);
}


// EOF
