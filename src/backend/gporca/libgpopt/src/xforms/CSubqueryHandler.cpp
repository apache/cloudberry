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
#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApplyNotIn.h"
#include "gpopt/operators/CLogicalLeftOuterApply.h"
#include "gpopt/operators/CLogicalLeftOuterCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApply.h"
#include "gpopt/operators/CLogicalLeftSemiCorrelatedApplyIn.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"
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


// Return a scalar aggregate under scalar projections and, when requested,
// HAVING filters. Quantified subqueries need exactly one row before they can
// become scalar comparisons, so they must not skip filters here.
static CExpression *
PexprScalarGbAgg(CExpression *pexpr, BOOL fSkipHaving = false)
{
	while (COperator::EopLogicalProject == pexpr->Pop()->Eopid() ||
		   (fSkipHaving &&
			COperator::EopLogicalSelect == pexpr->Pop()->Eopid()))
	{
		pexpr = (*pexpr)[0];
	}
	if (COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid() &&
		0 == CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr()->Size())
	{
		return pexpr;
	}
	return nullptr;
}


// A scalar aggregate returns one row even for an empty input. Decorrelation
// adds the correlation keys to GROUP BY and can lose that row. Handle only a
// single aggregation level here; other shapes retain correlated execution.
static BOOL
FCanDecorrelateScalarAgg(CExpression *pexprInner, CExpression *pexprGbAgg)
{
	if (nullptr == pexprGbAgg || CUtils::FHasScalarAgg((*pexprGbAgg)[0]) ||
		CUtils::FHasSubqueryOrApply((*pexprGbAgg)[0]) ||
		(*pexprGbAgg)[1]->DeriveHasSubquery())
	{
		return false;
	}
	if (!(*pexprGbAgg)[0]->DeriveOutputColumns()->ContainsAll(
			(*pexprGbAgg)[1]->DeriveUsedColumns()))
	{
		// Aggregate arguments, including ordered-set direct arguments, can
		// depend on the outer row and must retain their evaluation order.
		return false;
	}
	CExpression *agg_list = (*pexprGbAgg)[1];
	for (ULONG ul = 0; ul < agg_list->Arity(); ul++)
	{
		CExpression *agg = (*(*agg_list)[ul])[0];
		if (COperator::EopScalarAggFunc == agg->Pop()->Eopid())
		{
			CExpression *direct_args = (*agg)[EaggfuncIndexDirectArgs];
			for (ULONG arg = 0; arg < direct_args->Arity(); arg++)
			{
				if (COperator::EopScalarConst !=
					(*direct_args)[arg]->Pop()->Eopid())
				{
					return false;
				}
			}
		}
	}
	for (CExpression *pexpr = pexprInner; pexpr != pexprGbAgg;
		 pexpr = (*pexpr)[0])
	{
		if ((*pexpr)[1]->DeriveHasSubquery() ||
			(*pexpr)[1]->DeriveHasNonScalarFunction())
		{
			return false;
		}
	}
	return true;
}


// Substitute already computed projection expressions without copying a
// relational subtree. FCanDecorrelateScalarAgg excludes nested subqueries.
static CExpression *
PexprSubstituteAggColumns(CMemoryPool *mp, CExpression *pexpr,
						  CColRefArray *colrefs, CExpressionArray *values)
{
	GPOS_CHECK_STACK_SIZE;
	if (COperator::EopScalarIdent == pexpr->Pop()->Eopid())
	{
		const CColRef *colref = CScalarIdent::PopConvert(pexpr->Pop())->Pcr();
		for (ULONG ul = 0; ul < colrefs->Size(); ul++)
		{
			if ((*colrefs)[ul] == colref)
			{
				(*values)[ul]->AddRef();
				return (*values)[ul];
			}
		}
	}
	CExpressionArray *children = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
	{
		children->Append(
			PexprSubstituteAggColumns(mp, (*pexpr)[ul], colrefs, values));
	}
	pexpr->Pop()->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pexpr->Pop(), children);
}


// Type metadata identifies the built-in MIN/MAX aggregates; their empty-input
// result is NULL. Do not infer this for an arbitrary aggregate of the same type.
static BOOL
FMinMaxAgg(CExpression *project_elem)
{
	CExpression *agg = (*project_elem)[0];
	if (COperator::EopScalarAggFunc != agg->Pop()->Eopid())
	{
		return false;
	}
	const CColRef *colref =
		CScalarProjectElement::PopConvert(project_elem->Pop())->Pcr();
	CScalarAggFunc *agg_func = CScalarAggFunc::PopConvert(agg->Pop());
	return agg_func->IsMinMax(colref->RetrieveType());
}


// A bare built-in NULL-on-empty aggregate needs no missing-row repair.
// Keep its existing decorrelation path, including subqueries in its input.
// Nested scalar aggregates can themselves supply a row on empty input and
// still require the general safety checks.
static BOOL
FScalarAggReturnsNullOnEmpty(CExpression *pexprInner, const CColRef *colref)
{
	CExpression *pexprGbAgg = PexprScalarGbAgg(pexprInner);
	if (nullptr == pexprGbAgg || pexprGbAgg != pexprInner ||
		1 != (*pexprGbAgg)[1]->Arity() ||
		CUtils::FHasScalarAgg((*pexprGbAgg)[0]) ||
		!(*pexprGbAgg)[0]->DeriveOutputColumns()->ContainsAll(
			(*pexprGbAgg)[1]->DeriveUsedColumns()))
	{
		return false;
	}
	CExpression *elem = (*(*pexprGbAgg)[1])[0];
	CExpression *agg = (*elem)[0];
	if (CScalarProjectElement::PopConvert(elem->Pop())->Pcr() != colref ||
		COperator::EopScalarAggFunc != agg->Pop()->Eopid())
	{
		return false;
	}
	if (FMinMaxAgg(elem))
	{
		return true;
	}
	CExpression *args = (*agg)[EaggfuncIndexArgs];
	if (1 != args->Arity())
	{
		return false;
	}
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *arg_type = md_accessor->RetrieveType(
		CScalar::PopConvert((*args)[0]->Pop())->MdidType());
	IMDId *agg_mdid = CScalarAggFunc::PopConvert(agg->Pop())->MDId();
	return agg_mdid->Equals(arg_type->GetMdidForAggType(IMDType::EaggSum)) ||
		   agg_mdid->Equals(arg_type->GetMdidForAggType(IMDType::EaggAvg));
}


// Preserve the scalar aggregate's empty-input row and evaluate its projections
// only for the requested outer rows. The real input can still be aggregated
// once and joined by correlation key. When its empty result is not known, a
// guarded SubPlan computes the original aggregate and projections over an empty
// table when no group matches. HAVING is evaluated after restoring that row.
// This uses the aggregate's actual semantics, including user-defined final
// functions, rather than assuming that every non-count aggregate returns NULL.
// Takes ownership of pexprOuter and pexprInner.
static CExpression *
PexprScalarAggApply(CMemoryPool *mp, CExpression *pexprOuter,
					CExpression *pexprInner, const CColRef *colref,
					CExpression **ppexprValue)
{
	CExpression *pexprGbAgg = PexprScalarGbAgg(pexprInner, true);
	GPOS_ASSERT(FCanDecorrelateScalarAgg(pexprInner, pexprGbAgg));
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	UlongToColRefMap *mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
	CColRefArray *empty_cols = GPOS_NEW(mp) CColRefArray(mp);
	CColRefSetIter input_cols(*(*pexprGbAgg)[0]->DeriveOutputColumns());
	while (input_cols.Advance())
	{
		CColRef *pcr = input_cols.Pcr();
		CColRef *copy = col_factory->PcrCreate(pcr);
		mapping->Insert(GPOS_NEW(mp) ULONG(pcr->Id()), copy);
		empty_cols->Append(copy);
	}
	if (0 == empty_cols->Size())
	{
		empty_cols->Append(col_factory->PcrCreate(
			COptCtxt::PoctxtFromTLS()->Pmda()->PtMDType<IMDTypeBool>(),
			default_type_modifier));
	}
	CExpression *pexprEmpty = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalConstTableGet(
							mp, empty_cols, GPOS_NEW(mp) IDatum2dArray(mp)));

	// Rebuild the empty aggregate, projections and filters from the bottom up,
	// with fresh local columns. Keep outer references bound to the outer row.
	CExpressionArray *chain = GPOS_NEW(mp) CExpressionArray(mp);
	for (CExpression *pexpr = pexprInner;; pexpr = (*pexpr)[0])
	{
		pexpr->AddRef();
		chain->Append(pexpr);
		if (pexpr == pexprGbAgg)
		{
			break;
		}
	}
	CColRefArray *defined = GPOS_NEW(mp) CColRefArray(mp);
	CExpressionArray *matched = GPOS_NEW(mp) CExpressionArray(mp);
	CExpression *having = nullptr;
	for (ULONG pos = chain->Size(); 0 < pos; pos--)
	{
		CExpression *pexpr = (*chain)[pos - 1];
		CExpression *project_list = (*pexpr)[1];
		if (COperator::EopLogicalSelect == pexpr->Pop()->Eopid())
		{
			CExpression *predicate = PexprSubstituteAggColumns(
				mp, project_list, defined, matched);
			if (nullptr == having)
			{
				having = predicate;
			}
			else
			{
				// A later filter is evaluated only if the earlier filters
				// retained the row, including when one returned NULL.
				CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
				IMDId *bool_type = md_accessor->PtMDType<IMDTypeBool>()->MDId();
				bool_type->AddRef();
				having = GPOS_NEW(mp) CExpression(
					mp, GPOS_NEW(mp) CScalarIf(mp, bool_type), having,
					predicate, CUtils::PexprScalarConstBool(mp, false));
			}
		}
		else
		{
			for (ULONG ul = 0; ul < project_list->Arity(); ul++)
			{
				CExpression *elem = (*project_list)[ul];
				CColRef *pcr =
					CScalarProjectElement::PopConvert(elem->Pop())->Pcr();
				mapping->Insert(GPOS_NEW(mp) ULONG(pcr->Id()),
								col_factory->PcrCreate(pcr));
				CExpression *value = pexpr == pexprGbAgg
										 ? CUtils::PexprScalarIdent(mp, pcr)
										 : PexprSubstituteAggColumns(
											   mp, (*elem)[0], defined, matched);
				defined->Append(pcr);
				matched->Append(value);
			}
		}
		pexprEmpty = GPOS_NEW(mp) CExpression(
			mp, pexpr->Pop()->PopCopyWithRemappedColumns(mp, mapping, false),
			pexprEmpty,
			project_list->PexprCopyWithRemappedColumns(mp, mapping, false));
	}
	const CColRef *empty_value = CUtils::PcrRemap(colref, mapping, true);
	CExpression *matched_value = nullptr;
	for (ULONG ul = 0; ul < defined->Size(); ul++)
	{
		if ((*defined)[ul] == colref)
		{
			matched_value = (*matched)[ul];
			matched_value->AddRef();
			break;
		}
	}
	GPOS_ASSERT(nullptr != matched_value);
	if (nullptr != having)
	{
		IMDId *type = colref->RetrieveType()->MDId();
		type->AddRef();
		matched_value = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarIf(mp, type), having, matched_value,
			CUtils::PexprScalarConstNull(mp, colref->RetrieveType(),
									   colref->TypeModifier()));
	}

	// COUNT and MIN/MAX have known empty-input values. Substitute every
	// aggregate output, including duplicate COUNTs used by SELECT and HAVING,
	// before evaluating projections and filters for the missing group.
	CExpression *known_empty_value = nullptr;
	CColRefArray *agg_cols = GPOS_NEW(mp) CColRefArray(mp);
	CExpressionArray *empty_values = GPOS_NEW(mp) CExpressionArray(mp);
	CExpression *agg_list = (*pexprGbAgg)[1];
	BOOL all_known = true;
	for (ULONG ul = 0; ul < agg_list->Arity(); ul++)
	{
		CExpression *elem = (*agg_list)[ul];
		CColRef *agg_col =
			CScalarProjectElement::PopConvert(elem->Pop())->Pcr();
		CColRef *count_col = nullptr;
		if (CUtils::FCountAggProjElem(elem, &count_col))
		{
			empty_values->Append(CUtils::PexprScalarConstInt8(mp, 0));
		}
		else if (FMinMaxAgg(elem))
		{
			empty_values->Append(CUtils::PexprScalarConstNull(
				mp, agg_col->RetrieveType(), agg_col->TypeModifier()));
		}
		else
		{
			all_known = false;
			break;
		}
		agg_cols->Append(agg_col);
	}
	if (all_known)
	{
		known_empty_value = PexprSubstituteAggColumns(
			mp, matched_value, agg_cols, empty_values);
	}
	agg_cols->Release();
	empty_values->Release();
	chain->Release();
	mapping->Release();
	defined->Release();
	matched->Release();

	pexprGbAgg->AddRef();
	CExpression *with_flag = CUtils::PexprAddProjection(
		mp, pexprGbAgg, CUtils::PexprScalarConstBool(mp, true));
	const CColRef *flag =
		CScalarProjectElement::PopConvert((*(*with_flag)[1])[0]->Pop())->Pcr();
	pexprInner->Release();
	CExpression *result = CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
		mp, pexprOuter, with_flag,
		with_flag->DeriveOutputColumns()->Pdrgpcr(mp),
		COperator::EopScalarSubquery);

	CExpression *unmatched_value = known_empty_value;
	if (nullptr == unmatched_value)
	{
		// Gate above the aggregate: filtering its input would still finalize
		// an empty aggregate even when a real group exists.
		pexprEmpty = CUtils::PexprLogicalSelect(
			mp, pexprEmpty,
			CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, flag)));
		result = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(
			mp, result, pexprEmpty, empty_value, COperator::EopScalarSubquery);
		unmatched_value = CUtils::PexprScalarIdent(mp, empty_value);
	}
	else
	{
		pexprEmpty->Release();
	}
	IMDId *type = colref->RetrieveType()->MDId();
	type->AddRef();
	*ppexprValue = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarIf(mp, type),
					CUtils::PexprScalarIdent(mp, flag), matched_value,
					unmatched_value);
	return result;
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
			CUtils::FHasScalarAgg(pexprLogicalChild))
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
//		PexprReplaceScalarSubqueryWithNull
//
//	@doc:
//		Return a copy with the selected scalar subquery replaced by a NULL
//		of its type, leaving all other subqueries unknown
//
//---------------------------------------------------------------------------
static CExpression *
PexprReplaceScalarSubqueryWithNull(CMemoryPool *mp, CExpression *pexpr,
								   CExpression *pexprSubquery)
{
	GPOS_CHECK_STACK_SIZE;

	COperator *pop = pexpr->Pop();
	if (pexpr == pexprSubquery)
	{
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		CScalarSubquery *popSubquery = CScalarSubquery::PopConvert(pop);
		return CUtils::PexprScalarConstNull(
			mp, md_accessor->RetrieveType(popSubquery->MdidType()),
			popSubquery->Pcr()->TypeModifier());
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
				PexprReplaceScalarSubqueryWithNull(mp, pexprChild,
												  pexprSubquery));
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


// Reject NULL from each subquery individually.  Replacing all subqueries
// at once is insufficient: COALESCE(sq1, sq2) can be non-NULL when either
// one is NULL, even though it is NULL when both are NULL. A subquery known
// to be nonempty needs no NULL-rejection check: an inner apply cannot drop
// an outer row for it.
static BOOL
FNullRejectingOnEachSubquery(CMemoryPool *mp, CExpression *pexprScalar,
							   CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;

	if (COperator::EopScalarSubquery == pexpr->Pop()->Eopid())
	{
		// A constant table with one row, e.g. (SELECT 1), cannot lose an
		// outer row through an empty result. Keep its existing context;
		// changing it to Value would unnecessarily introduce an outer join.
		CExpression *pexprInner = (*pexpr)[0];
		while (COperator::EopLogicalProject == pexprInner->Pop()->Eopid() &&
			   !(*pexprInner)[1]->DeriveHasNonScalarFunction())
		{
			pexprInner = (*pexprInner)[0];
		}
		if (COperator::EopLogicalConstTableGet == pexprInner->Pop()->Eopid() &&
			1 == CLogicalConstTableGet::PopConvert(pexprInner->Pop())
					 ->Pdrgpdrgpdatum()->Size())
		{
			return true;
		}

		CExpression *pexprNull =
			PexprReplaceScalarSubqueryWithNull(mp, pexprScalar, pexpr);
		CScalar::EBoolEvalResult eber = CScalar::EberEvaluate(mp, pexprNull);
		pexprNull->Release();
		return CScalar::EberNull == eber || CScalar::EberFalse == eber ||
			   CScalar::EberNotTrue == eber;
	}

	for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
	{
		if ((*pexpr)[ul]->Pop()->FScalar() &&
			!FNullRejectingOnEachSubquery(mp, pexprScalar, (*pexpr)[ul]))
		{
			return false;
		}
	}
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		FNullRejectingOnSubqueries
//
//	@doc:
//		Return true if the boolean scalar expression cannot be true when any
//		one of its scalar subqueries returns NULL, e.g. "a > (SELECT ...)", but not
//		"(SELECT ...) IS NULL" or "coalesce((SELECT ...), 0) = 0". Subqueries
//		known to be nonempty are also safe to retain in the filter context.
//
//---------------------------------------------------------------------------
static BOOL
FNullRejectingOnSubqueries(CMemoryPool *mp, CExpression *pexprScalar)
{
	COperator::EOperatorId eopid = pexprScalar->Pop()->Eopid();
	if (COperator::EopScalarProjectList == eopid ||
		COperator::EopScalarProjectElement == eopid ||
		COperator::EopScalarNAryJoinPredList == eopid)
	{
		return false;
	}

	// These nodes group operands and have no result type of their own.
	// Their enclosing predicate has already been checked; do not call
	// MdidType(), which asserts for both operators.
	if (COperator::EopScalarArrayRefIndexList == eopid ||
		COperator::EopScalarSwitchCase == eopid)
	{
		return true;
	}

	// only a boolean expression can be the predicate, its operands are
	// covered by the check on the predicate itself
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *mdid_type = CScalar::PopConvert(pexprScalar->Pop())->MdidType();
	if (nullptr == mdid_type ||
		!mdid_type->Equals(md_accessor->PtMDType<IMDTypeBool>()->MDId()))
	{
		return true;
	}

	return FNullRejectingOnEachSubquery(mp, pexprScalar, pexprScalar);
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

	// QuantifiedToAgg has its own empty-input handling. Its generated
	// aggregates and bare NULL-on-empty built-ins retain their existing paths.
	BOOL fNeedsEmptyInputFix =
		!CScalarSubquery::PopConvert(pexprSubquery->Pop())
			 ->FGeneratedByQuantified() &&
		psd->m_fHasOuterRefs && CUtils::FHasScalarAgg(pexprInner) &&
		!FScalarAggReturnsNullOnEmpty(pexprInner, pcrSubquery);

	// set flag for using subquery in a value context
	psd->m_fValueSubquery =
		EsqctxtValue == esqctxt ||
		(psd->m_fHasCountAgg && psd->m_fHasOuterRefs) || fNeedsEmptyInputFix;

	// set flag of correlated execution
	psd->SetCorrelatedExecution();

	if ((psd->m_fHasCountAgg && psd->m_fHasOuterRefs &&
		 FHasCorrelatedSelectAboveGbAgg(pexprInner)) ||
		(fNeedsEmptyInputFix &&
		 !FCanDecorrelateScalarAgg(pexprInner,
								 PexprScalarGbAgg(pexprInner, true))))
	{
		// A correlated HAVING above an uncorrelated COUNT must still be
		// able to remove its row. Nested aggregates and subqueries also
		// need their original evaluation and cardinality checks.
		psd->m_fCorrelatedExecution = true;
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
		// count(*)/count(Any) produce '0' if their input is empty.

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

	CExpression *pexprGbAgg = PexprScalarGbAgg(pexprInner, true);
	const CLogicalGbAgg *count_gb = nullptr;
	BOOL fSingleCount =
		pexprGbAgg == pexprInner && 1 == (*pexprGbAgg)[1]->Arity() &&
		CUtils::FHasCountAggMatchingColumn(pexprInner, colref, &count_gb);
	BOOL fNullOnEmpty = FScalarAggReturnsNullOnEmpty(pexprInner, colref);
	// Restore a missing group only when the aggregate input is correlated.
	// Outer references confined to HAVING or projections need no such repair.
	if (!popSubquery->FGeneratedByQuantified() &&
		FCanDecorrelateScalarAgg(pexprInner, pexprGbAgg) &&
		(*pexprGbAgg)[0]->HasOuterRefs() && !fSingleCount && !fNullOnEmpty)
	{
		*ppexprNewOuter = PexprScalarAggApply(mp, pexprOuter, pexprInner,
											  colref, ppexprResidualScalar);
		return true;
	}

	// generate an outer apply between outer expression and the relational child of scalar subquery
	CExpression *pexprLeftOuterApply =
		CUtils::PexprLogicalApply<CLogicalLeftOuterApply>(
			mp, pexprOuter, pexprInner, colref, popSubquery->Eopid());

	const CLogicalGbAgg *pgbAgg = nullptr;
	BOOL fHasCountAggMatchingColumn = CUtils::FHasCountAggMatchingColumn(
		(*pexprSubquery)[0], colref, &pgbAgg);

	if (!fHasCountAggMatchingColumn ||
		(!popSubquery->FGeneratedByQuantified() &&
		 (!fSingleCount || !pexprInner->HasOuterRefs())))
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

	if (fOuterRefsUnderInner && CUtils::FHasScalarAgg(pexprInner))
	{
		// A scalar aggregate under scalar projections returns exactly one
		// row, including on empty input. Evaluate that row before comparing
		// it, preserving NULL versus false in value context as well.
		if (FCanDecorrelateScalarAgg(pexprInner, PexprScalarGbAgg(pexprInner)))
		{
			pexprInner->AddRef();
			CExpression *pexprScalarSubquery = GPOS_NEW(mp) CExpression(
				mp,
				GPOS_NEW(mp)
					CScalarSubquery(mp, colref, false /*fGeneratedByExist*/,
									false /*fGeneratedByQuantified*/),
				pexprInner);
			CExpression *pexprOuterScalar = (*pexprSubquery)[1];
			pexprOuterScalar->AddRef();
			IMDId *mdid_op = pScalarSubqAny->MdIdOp();
			mdid_op->AddRef();
			CExpression *pexprComparison = CUtils::PexprScalarCmp(
				mp, pexprOuterScalar, pexprScalarSubquery,
				*pScalarSubqAny->PstrOp(), mdid_op);
			BOOL fSuccess = FProcess(pexprOuter, pexprComparison, EsqctxtValue,
									 ppexprNewOuter, ppexprResidualScalar);
			pexprComparison->Release();
			return fSuccess;
		}

		// HAVING, grouping and LIMIT can remove the aggregate row. ANY over
		// no rows is false, whereas a scalar comparison would yield NULL.
		// Nested aggregates/subqueries also keep their original execution.
		return FCreateCorrelatedApplyForExistOrQuant(
			mp, pexprOuter, pexprSubquery, esqctxt, ppexprNewOuter,
			ppexprResidualScalar);
	}

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

	// In a filter a scalar subquery is unnested into an inner apply, which
	// drops the outer rows the subquery returns no row for. That is right
	// only if the predicate cannot be true when the subquery is NULL, like
	// "a > (SELECT ...)"; for "(SELECT ...) IS NULL" or
	// "coalesce((SELECT ...), 0) = 0" those rows must be kept.
	if (EsqctxtFilter == esqctxt && !CPredicateUtils::FAnd(pexprScalar) &&
		!FNullRejectingOnSubqueries(mp, pexprScalar))
	{
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
