/*-------------------------------------------------------------------------
 *
 * prepqual.c
 *	  Routines for preprocessing qualification expressions
 *
 *
 * While the parser will produce flattened (N-argument) AND/OR trees from
 * simple sequences of AND'ed or OR'ed clauses, there might be an AND clause
 * directly underneath another AND, or OR underneath OR, if the input was
 * oddly parenthesized.  Also, rule expansion and subquery flattening could
 * produce such parsetrees.  The planner wants to flatten all such cases
 * to ensure consistent optimization behavior.
 *
 * Formerly, this module was responsible for doing the initial flattening,
 * but now we leave it to eval_const_expressions to do that since it has to
 * make a complete pass over the expression tree anyway.  Instead, we just
 * have to ensure that our manipulations preserve AND/OR flatness.
 * pull_ands() and pull_ors() are used to maintain flatness of the AND/OR
 * tree after local transformations that might introduce nested AND/ORs.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/prep/prepqual.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/prep.h"
#include "utils/lsyscache.h"


static List *pull_ands(List *andlist);
static List *pull_ors(List *orlist);
static Expr *find_duplicate_ors(Expr *qual, bool is_check);
static Expr *process_duplicate_ors(List *orlist);


/*
 * negate_clause
 *	  Negate a Boolean expression.
 *
 * Input is a clause to be negated (e.g., the argument of a NOT clause).
 * Returns a new clause equivalent to the negation of the given clause.
 *
 * Although this can be invoked on its own, it's mainly intended as a helper
 * for eval_const_expressions(), and that context drives several design
 * decisions.  In particular, if the input is already AND/OR flat, we must
 * preserve that property.  We also don't bother to recurse in situations
 * where we can assume that lower-level executions of eval_const_expressions
 * would already have simplified sub-clauses of the input.
 *
 * The difference between this and a simple make_notclause() is that this
 * tries to get rid of the NOT node by logical simplification.  It's clearly
 * always a win if the NOT node can be eliminated altogether.  However, our
 * use of DeMorgan's laws could result in having more NOT nodes rather than
 * fewer.  We do that unconditionally anyway, because in WHERE clauses it's
 * important to expose as much top-level AND/OR structure as possible.
 * Also, eliminating an intermediate NOT may allow us to flatten two levels
 * of AND or OR together that we couldn't have otherwise.  Finally, one of
 * the motivations for doing this is to ensure that logically equivalent
 * expressions will be seen as physically equal(), so we should always apply
 * the same transformations.
 */
Node *
negate_clause(Node *node)
{
	if (node == NULL)			/* should not happen */
		elog(ERROR, "can't negate an empty subexpression");
	switch (nodeTag(node))
	{
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/* NOT NULL is still NULL */
				if (c->constisnull)
					return makeBoolConst(false, true);
				/* otherwise pretty easy */
				return makeBoolConst(!DatumGetBool(c->constvalue), false);
			}
			break;
		case T_OpExpr:
			{
				/*
				 * Negate operator if possible: (NOT (< A B)) => (>= A B)
				 */
				OpExpr	   *opexpr = (OpExpr *) node;
				Oid			negator = get_negator(opexpr->opno);

				if (negator)
				{
					OpExpr	   *newopexpr = makeNode(OpExpr);

					newopexpr->opno = negator;
					newopexpr->opfuncid = InvalidOid;
					newopexpr->opresulttype = opexpr->opresulttype;
					newopexpr->opretset = opexpr->opretset;
					newopexpr->opcollid = opexpr->opcollid;
					newopexpr->inputcollid = opexpr->inputcollid;
					newopexpr->args = opexpr->args;
					newopexpr->location = opexpr->location;
					return (Node *) newopexpr;
				}
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				/*
				 * Negate a ScalarArrayOpExpr if its operator has a negator;
				 * for example x = ANY (list) becomes x <> ALL (list)
				 */
				ScalarArrayOpExpr *saopexpr = (ScalarArrayOpExpr *) node;
				Oid			negator = get_negator(saopexpr->opno);

				if (negator)
				{
					ScalarArrayOpExpr *newopexpr = makeNode(ScalarArrayOpExpr);

					newopexpr->opno = negator;
					newopexpr->opfuncid = InvalidOid;
					newopexpr->hashfuncid = InvalidOid;
					newopexpr->useOr = !saopexpr->useOr;
					newopexpr->inputcollid = saopexpr->inputcollid;
					newopexpr->args = saopexpr->args;
					newopexpr->location = saopexpr->location;
					return (Node *) newopexpr;
				}
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				switch (expr->boolop)
				{
						/*--------------------
						 * Apply DeMorgan's Laws:
						 *		(NOT (AND A B)) => (OR (NOT A) (NOT B))
						 *		(NOT (OR A B))	=> (AND (NOT A) (NOT B))
						 * i.e., swap AND for OR and negate each subclause.
						 *
						 * If the input is already AND/OR flat and has no NOT
						 * directly above AND or OR, this transformation preserves
						 * those properties.  For example, if no direct child of
						 * the given AND clause is an AND or a NOT-above-OR, then
						 * the recursive calls of negate_clause() can't return any
						 * OR clauses.  So we needn't call pull_ors() before
						 * building a new OR clause.  Similarly for the OR case.
						 *--------------------
						 */
					case AND_EXPR:
						{
							List	   *nargs = NIL;
							ListCell   *lc;

							foreach(lc, expr->args)
							{
								nargs = lappend(nargs,
												negate_clause(lfirst(lc)));
							}
							return (Node *) make_orclause(nargs);
						}
						break;
					case OR_EXPR:
						{
							List	   *nargs = NIL;
							ListCell   *lc;

							foreach(lc, expr->args)
							{
								nargs = lappend(nargs,
												negate_clause(lfirst(lc)));
							}
							return (Node *) make_andclause(nargs);
						}
						break;
					case NOT_EXPR:

						/*
						 * NOT underneath NOT: they cancel.  We assume the
						 * input is already simplified, so no need to recurse.
						 */
						return (Node *) linitial(expr->args);
					default:
						elog(ERROR, "unrecognized boolop: %d",
							 (int) expr->boolop);
						break;
				}
			}
			break;
		case T_NullTest:
			{
				NullTest   *expr = (NullTest *) node;

				/*
				 * In the rowtype case, the two flavors of NullTest are *not*
				 * logical inverses, so we can't simplify.  But it does work
				 * for scalar datatypes.
				 */
				if (!expr->argisrow)
				{
					NullTest   *newexpr = makeNode(NullTest);

					newexpr->arg = expr->arg;
					newexpr->nulltesttype = (expr->nulltesttype == IS_NULL ?
											 IS_NOT_NULL : IS_NULL);
					newexpr->argisrow = expr->argisrow;
					newexpr->location = expr->location;
					return (Node *) newexpr;
				}
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *expr = (BooleanTest *) node;
				BooleanTest *newexpr = makeNode(BooleanTest);

				newexpr->arg = expr->arg;
				switch (expr->booltesttype)
				{
					case IS_TRUE:
						newexpr->booltesttype = IS_NOT_TRUE;
						break;
					case IS_NOT_TRUE:
						newexpr->booltesttype = IS_TRUE;
						break;
					case IS_FALSE:
						newexpr->booltesttype = IS_NOT_FALSE;
						break;
					case IS_NOT_FALSE:
						newexpr->booltesttype = IS_FALSE;
						break;
					case IS_UNKNOWN:
						newexpr->booltesttype = IS_NOT_UNKNOWN;
						break;
					case IS_NOT_UNKNOWN:
						newexpr->booltesttype = IS_UNKNOWN;
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) expr->booltesttype);
						break;
				}
				newexpr->location = expr->location;
				return (Node *) newexpr;
			}
			break;
		default:
			/* else fall through */
			break;
	}

	/*
	 * Otherwise we don't know how to simplify this, so just tack on an
	 * explicit NOT node.
	 */
	return (Node *) make_notclause((Expr *) node);
}


/*
 * canonicalize_qual
 *	  Convert a qualification expression to the most useful form.
 *
 * This is primarily intended to be used on top-level WHERE (or JOIN/ON)
 * clauses.  It can also be used on top-level CHECK constraints, for which
 * pass is_check = true.  DO NOT call it on any expression that is not known
 * to be one or the other, as it might apply inappropriate simplifications.
 *
 * The name of this routine is a holdover from a time when it would try to
 * force the expression into canonical AND-of-ORs or OR-of-ANDs form.
 * Eventually, we recognized that that had more theoretical purity than
 * actual usefulness, and so now the transformation doesn't involve any
 * notion of reaching a canonical form.
 *
 * NOTE: we assume the input has already been through eval_const_expressions
 * and therefore possesses AND/OR flatness.  Formerly this function included
 * its own flattening logic, but that requires a useless extra pass over the
 * tree.
 *
 * Returns the modified qualification.
 */
Expr *
canonicalize_qual(Expr *qual, bool is_check)
{
	Expr	   *newqual;

	/* Quick exit for empty qual */
	if (qual == NULL)
		return NULL;

	/* This should not be invoked on quals in implicit-AND format */
	Assert(!IsA(qual, List));

	/*
	 * Pull up redundant subclauses in OR-of-AND trees.  We do this only
	 * within the top-level AND/OR structure; there's no point in looking
	 * deeper.  Also remove any NULL constants in the top-level structure.
	 */
	newqual = find_duplicate_ors(qual, is_check);

	return newqual;
}


/*
 * pull_ands
 *	  Recursively flatten nested AND clauses into a single and-clause list.
 *
 * Input is the arglist of an AND clause.
 * Returns the rebuilt arglist (note original list structure is not touched).
 */
static List *
pull_ands(List *andlist)
{
	List	   *out_list = NIL;
	ListCell   *arg;

	foreach(arg, andlist)
	{
		Node	   *subexpr = (Node *) lfirst(arg);

		if (is_andclause(subexpr))
			out_list = list_concat(out_list,
								   pull_ands(((BoolExpr *) subexpr)->args));
		else
			out_list = lappend(out_list, subexpr);
	}
	return out_list;
}

/*
 * pull_ors
 *	  Recursively flatten nested OR clauses into a single or-clause list.
 *
 * Input is the arglist of an OR clause.
 * Returns the rebuilt arglist (note original list structure is not touched).
 */
static List *
pull_ors(List *orlist)
{
	List	   *out_list = NIL;
	ListCell   *arg;

	foreach(arg, orlist)
	{
		Node	   *subexpr = (Node *) lfirst(arg);

		if (is_orclause(subexpr))
			out_list = list_concat(out_list,
								   pull_ors(((BoolExpr *) subexpr)->args));
		else
			out_list = lappend(out_list, subexpr);
	}
	return out_list;
}


/*--------------------
 * The following code attempts to apply the inverse OR distributive law:
 *		((A AND B) OR (A AND C))  =>  (A AND (B OR C))
 * That is, locate OR clauses in which every subclause contains an
 * identical term, and pull out the duplicated terms.
 *
 * This may seem like a fairly useless activity, but it turns out to be
 * applicable to many machine-generated queries, and there are also queries
 * in some of the TPC benchmarks that need it.  This was in fact almost the
 * sole useful side-effect of the old prepqual code that tried to force
 * the query into canonical AND-of-ORs form: the canonical equivalent of
 *		((A AND B) OR (A AND C))
 * is
 *		((A OR A) AND (A OR C) AND (B OR A) AND (B OR C))
 * which the code was able to simplify to
 *		(A AND (A OR C) AND (B OR A) AND (B OR C))
 * thus successfully extracting the common condition A --- but at the cost
 * of cluttering the qual with many redundant clauses.
 *--------------------
 */

/*
 * find_duplicate_ors
 *	  Given a qualification tree with the NOTs pushed down, search for
 *	  OR clauses to which the inverse OR distributive law might apply.
 *	  Only the top-level AND/OR structure is searched.
 *
 * While at it, we remove any NULL constants within the top-level AND/OR
 * structure, eg in a WHERE clause, "x OR NULL::boolean" is reduced to "x".
 * In general that would change the result, so eval_const_expressions can't
 * do it; but at top level of WHERE, we don't need to distinguish between
 * FALSE and NULL results, so it's valid to treat NULL::boolean the same
 * as FALSE and then simplify AND/OR accordingly.  Conversely, in a top-level
 * CHECK constraint, we may treat a NULL the same as TRUE.
 *
 * Returns the modified qualification.  AND/OR flatness is preserved.
 */
static Expr *
find_duplicate_ors(Expr *qual, bool is_check)
{
	if (is_orclause(qual))
	{
		List	   *orlist = NIL;
		ListCell   *temp;

		/* Recurse */
		foreach(temp, ((BoolExpr *) qual)->args)
		{
			Expr	   *arg = (Expr *) lfirst(temp);

			arg = find_duplicate_ors(arg, is_check);

			/* Get rid of any constant inputs */
			if (arg && IsA(arg, Const))
			{
				Const	   *carg = (Const *) arg;

				if (is_check)
				{
					/* Within OR in CHECK, drop constant FALSE */
					if (!carg->constisnull && !DatumGetBool(carg->constvalue))
						continue;
					/* Constant TRUE or NULL, so OR reduces to TRUE */
					return (Expr *) makeBoolConst(true, false);
				}
				else
				{
					/* Within OR in WHERE, drop constant FALSE or NULL */
					if (carg->constisnull || !DatumGetBool(carg->constvalue))
						continue;
					/* Constant TRUE, so OR reduces to TRUE */
					return arg;
				}
			}

			orlist = lappend(orlist, arg);
		}

		/* Flatten any ORs pulled up to just below here */
		orlist = pull_ors(orlist);

		/* Now we can look for duplicate ORs */
		return process_duplicate_ors(orlist);
	}
	else if (is_andclause(qual))
	{
		List	   *andlist = NIL;
		ListCell   *temp;

		/* Recurse */
		foreach(temp, ((BoolExpr *) qual)->args)
		{
			Expr	   *arg = (Expr *) lfirst(temp);

			arg = find_duplicate_ors(arg, is_check);

			/* Get rid of any constant inputs */
			if (arg && IsA(arg, Const))
			{
				Const	   *carg = (Const *) arg;

				if (is_check)
				{
					/* Within AND in CHECK, drop constant TRUE or NULL */
					if (carg->constisnull || DatumGetBool(carg->constvalue))
						continue;
					/* Constant FALSE, so AND reduces to FALSE */
					return arg;
				}
				else
				{
					/* Within AND in WHERE, drop constant TRUE */
					if (!carg->constisnull && DatumGetBool(carg->constvalue))
						continue;
					/* Constant FALSE or NULL, so AND reduces to FALSE */
					return (Expr *) makeBoolConst(false, false);
				}
			}

			andlist = lappend(andlist, arg);
		}

		/* Flatten any ANDs introduced just below here */
		andlist = pull_ands(andlist);

		/* AND of no inputs reduces to TRUE */
		if (andlist == NIL)
			return (Expr *) makeBoolConst(true, false);

		/* Single-expression AND just reduces to that expression */
		if (list_length(andlist) == 1)
			return (Expr *) linitial(andlist);

		/* Else we still need an AND node */
		return make_andclause(andlist);
	}
	else
		return qual;
}

/*
 * process_duplicate_ors
 *	  Given a list of exprs which are ORed together, try to apply
 *	  the inverse OR distributive law.
 *
 * Returns the resulting expression (could be an AND clause, an OR
 * clause, or maybe even a single subexpression).
 */
static Expr *
process_duplicate_ors(List *orlist)
{
	List	   *reference = NIL;
	int			num_subclauses = 0;
	List	   *winners;
	List	   *neworlist;
	ListCell   *temp;

	/* OR of no inputs reduces to FALSE */
	if (orlist == NIL)
		return (Expr *) makeBoolConst(false, false);

	/* Single-expression OR just reduces to that expression */
	if (list_length(orlist) == 1)
		return (Expr *) linitial(orlist);

	/*
	 * Choose the shortest AND clause as the reference list --- obviously, any
	 * subclause not in this clause isn't in all the clauses. If we find a
	 * clause that's not an AND, we can treat it as a one-element AND clause,
	 * which necessarily wins as shortest.
	 */
	foreach(temp, orlist)
	{
		Expr	   *clause = (Expr *) lfirst(temp);

		if (is_andclause(clause))
		{
			List	   *subclauses = ((BoolExpr *) clause)->args;
			int			nclauses = list_length(subclauses);

			if (reference == NIL || nclauses < num_subclauses)
			{
				reference = subclauses;
				num_subclauses = nclauses;
			}
		}
		else
		{
			reference = list_make1(clause);
			break;
		}
	}

	/*
	 * Just in case, eliminate any duplicates in the reference list.
	 */
	reference = list_union(NIL, reference);

	/*
	 * Check each element of the reference list to see if it's in all the OR
	 * clauses.  Build a new list of winning clauses.
	 */
	winners = NIL;
	foreach(temp, reference)
	{
		Expr	   *refclause = (Expr *) lfirst(temp);
		bool		win = true;
		ListCell   *temp2;

		foreach(temp2, orlist)
		{
			Expr	   *clause = (Expr *) lfirst(temp2);

			if (is_andclause(clause))
			{
				if (!list_member(((BoolExpr *) clause)->args, refclause))
				{
					win = false;
					break;
				}
			}
			else
			{
				if (!equal(refclause, clause))
				{
					win = false;
					break;
				}
			}
		}

		if (win)
			winners = lappend(winners, refclause);
	}

	/*
	 * If no winners, we can't transform the OR
	 */
	if (winners == NIL)
		return make_orclause(orlist);

	/*
	 * Generate new OR list consisting of the remaining sub-clauses.
	 *
	 * If any clause degenerates to empty, then we have a situation like (A
	 * AND B) OR (A), which can be reduced to just A --- that is, the
	 * additional conditions in other arms of the OR are irrelevant.
	 *
	 * Note that because we use list_difference, any multiple occurrences of a
	 * winning clause in an AND sub-clause will be removed automatically.
	 */
	neworlist = NIL;
	foreach(temp, orlist)
	{
		Expr	   *clause = (Expr *) lfirst(temp);

		if (is_andclause(clause))
		{
			List	   *subclauses = ((BoolExpr *) clause)->args;

			subclauses = list_difference(subclauses, winners);
			if (subclauses != NIL)
			{
				if (list_length(subclauses) == 1)
					neworlist = lappend(neworlist, linitial(subclauses));
				else
					neworlist = lappend(neworlist, make_andclause(subclauses));
			}
			else
			{
				neworlist = NIL;	/* degenerate case, see above */
				break;
			}
		}
		else
		{
			if (!list_member(winners, clause))
				neworlist = lappend(neworlist, clause);
			else
			{
				neworlist = NIL;	/* degenerate case, see above */
				break;
			}
		}
	}

	/*
	 * Append reduced OR to the winners list, if it's not degenerate, handling
	 * the special case of one element correctly (can that really happen?).
	 * Also be careful to maintain AND/OR flatness in case we pulled up a
	 * sub-sub-OR-clause.
	 */
	if (neworlist != NIL)
	{
		if (list_length(neworlist) == 1)
			winners = lappend(winners, linitial(neworlist));
		else
			winners = lappend(winners, make_orclause(pull_ors(neworlist)));
	}

	/*
	 * And return the constructed AND clause, again being wary of a single
	 * element and AND/OR flatness.
	 */
	if (list_length(winners) == 1)
		return (Expr *) linitial(winners);
	else
		return make_andclause(pull_ands(winners));
}

static Expr *
convert_or_to_cnf_complete(Expr *expr);

static Expr *
convert_and_to_cnf_complete(Expr *expr);
static Expr *
distribute_or_over_ands_complete(List *non_ands, List *and_clauses);
static List *
flatten_or_args_complete(List *args);
static List *
flatten_and_args_complete(List *args);
static Expr *
combine_cnf_clauses_complete(List *clauses);
static List *
remove_duplicates_in_list(List *clauses);
static List *
remove_duplicate_and_subsumed_clauses(List *clauses);
static bool
or_clause_subsumes(Expr *or_clause1, Expr *or_clause2);
static Expr *
deduplicate_cnf_result(Expr *expr);

/*
 * CNF Conversion for CTE Predicate Pushdown
 *
 * MOTIVATION:
 * When a CTE is referenced multiple times with different filter predicates,
 * we want to push down the combined predicates to reduce materialization.
 * For example:
 *
 *   WITH cte AS (SELECT ... FROM large_table)
 *   SELECT * FROM cte WHERE store_id = 10
 *   UNION ALL
 *   SELECT * FROM cte WHERE store_id = 20
 *
 * We collect predicates from all consumers and combine them:
 *   (store_id = 10) OR (store_id = 20)
 *
 * For more complex cases with AND predicates:
 *   WHERE (store_id = 10 AND year = 2001)
 *   WHERE (store_id = 20 AND year = 2001)
 *
 * Combined: (store_id = 10 AND year = 2001) OR (store_id = 20 AND year = 2001)
 *
 * WHY CNF CONVERSION:
 * CNF (Conjunctive Normal Form) is required because:
 * 1. The planner expects filter predicates in AND-of-ORs form
 * 2. CNF enables individual clauses to be pushed down independently
 * 3. After CNF conversion, (year = 2001) can be extracted as a separate
 *    conjunct and pushed down even if other parts cannot be
 *
 * ALGORITHM:
 * We use the distributive law to convert OR-of-ANDs to AND-of-ORs:
 *
 *   (A AND B) OR (A AND C)
 *   = (A OR A) AND (A OR C) AND (B OR A) AND (B OR C)  [distribute]
 *   = A AND (A OR C) AND (B OR A) AND (B OR C)         [simplify A OR A = A]
 *   = A AND (A OR C) AND (A OR B) AND (B OR C)         [reorder]
 *
 * With subsumption detection, we can further simplify:
 *   - (A OR C) subsumes any clause containing all its terms plus more
 *   - So A AND (A OR C) simplifies to A (since A subsumes A OR C? No...)
 *   
 * Actually: In CNF context, (A) subsumes (A OR B) because:
 *   - If A is true, both A and (A OR B) are true
 *   - (A) is more restrictive, so (A OR B) is redundant
 *
 * EXAMPLE WALKTHROUGH:
 * Input: (s='s' AND year=2001) OR (s='s' AND year=2002)
 *
 * Step 1: Identify AND clauses to distribute
 *   - First AND: (s='s' AND year=2001)
 *   - Remaining: (s='s' AND year=2002)
 *
 * Step 2: Distribute first AND over remaining
 *   - (s='s' OR (s='s' AND year=2002))  → recurse
 *   - (year=2001 OR (s='s' AND year=2002))  → recurse
 *
 * Step 3: Recursively convert each:
 *   - (s='s' OR (s='s' AND year=2002))
 *     = (s='s' OR s='s') AND (s='s' OR year=2002)
 *     = s='s' AND (s='s' OR year=2002)
 *   
 *   - (year=2001 OR (s='s' AND year=2002))
 *     = (year=2001 OR s='s') AND (year=2001 OR year=2002)
 *
 * Step 4: Combine with AND:
 *   = s='s' AND (s='s' OR year=2002) AND (year=2001 OR s='s') AND (year=2001 OR year=2002)
 *
 * Step 5: Deduplicate and remove subsumed clauses:
 *   - s='s' subsumes (s='s' OR year=2002) and (year=2001 OR s='s')
 *   - Final: s='s' AND (year=2001 OR year=2002)
 *
 * DEDUPLICATION STRATEGY:
 * 1. Exact duplicate removal: (A OR B) appears twice → keep one
 * 2. Subsumption removal: (A OR B) AND (A OR B OR C) → keep only (A OR B)
 *    because (A OR B) being true implies (A OR B OR C) is true
 *
 * COMPLEXITY NOTE:
 * CNF conversion can cause exponential blowup in the worst case.
 * For n AND-clauses each with m terms: O(m^n) output clauses.
 * The deduplication helps mitigate this for practical queries.
 */
Expr *
convert_expr_to_cnf_complete(Expr *expr)
{
	if (expr == NULL)
		return NULL;

	/* Base case: non-Boolean expressions */
	if (!is_orclause(expr) && !is_andclause(expr))
		return expr;

	if (is_orclause(expr))
	{
		return convert_or_to_cnf_complete(expr);
	}
	else if (is_andclause(expr))
	{
		return convert_and_to_cnf_complete(expr);
	}

	return expr;
}

/*
 * convert_or_to_cnf_complete
 *    Complete OR to CNF conversion with deduplication
 */
static Expr *
convert_or_to_cnf_complete(Expr *expr)
{
	List *or_args = NIL;
	ListCell *lc;

	/* Step 1: Recursively convert all arguments */
	foreach (lc, ((BoolExpr *)expr)->args)
	{
		Expr *arg = convert_expr_to_cnf_complete((Expr *)lfirst(lc));
		or_args = lappend(or_args, arg);
	}

	/* Step 2: Flatten nested ORs */
	or_args = flatten_or_args_complete(or_args);

	/* Step 3: Remove duplicate arguments within this OR */
	or_args = remove_duplicates_in_list(or_args);

	/* Step 4: Check for AND clauses that need distribution */
	List *and_clauses = NIL;
	List *non_and_clauses = NIL;
	bool has_and = false;

	foreach (lc, or_args)
	{
		Expr *arg = (Expr *)lfirst(lc);
		if (is_andclause(arg))
		{
			and_clauses = lappend(and_clauses, arg);
			has_and = true;
		}
		else
		{
			non_and_clauses = lappend(non_and_clauses, arg);
		}
	}

	/* Step 5: If no AND clauses, return simplified OR */
	if (!has_and)
	{
		if (list_length(or_args) == 0)
			return (Expr *)makeBoolConst(false, false);
		else if (list_length(or_args) == 1)
			return (Expr *)linitial(or_args);
		else
			return make_orclause(or_args);
	}

	/* Step 6: Apply distributive law */
	Expr *result = distribute_or_over_ands_complete(non_and_clauses, and_clauses);

	/* Step 7: Final deduplication of the resulting CNF */
	return deduplicate_cnf_result(result);
}

/*
 * convert_and_to_cnf_complete
 *    Complete AND to CNF conversion with deduplication
 */
static Expr *
convert_and_to_cnf_complete(Expr *expr)
{
	List *and_args = NIL;
	ListCell *lc;

	/* Step 1: Recursively convert all arguments */
	foreach (lc, ((BoolExpr *)expr)->args)
	{
		Expr *arg = convert_expr_to_cnf_complete((Expr *)lfirst(lc));
		and_args = lappend(and_args, arg);
	}

	/* Step 2: Flatten nested ANDs */
	and_args = flatten_and_args_complete(and_args);

	/* Step 3: Remove duplicates */
	and_args = remove_duplicates_in_list(and_args);

	/* Step 4: Return simplified AND */
	if (list_length(and_args) == 0)
		return (Expr *)makeBoolConst(true, false);
	else if (list_length(and_args) == 1)
		return (Expr *)linitial(and_args);
	else
		return make_andclause(and_args);
}

/*
 * distribute_or_over_ands_complete
 *    Enhanced distribution that handles multiple AND clauses properly
 */
static Expr *
distribute_or_over_ands_complete(List *non_ands, List *and_clauses)
{
	/* Use the first AND clause for initial distribution */
	Expr *first_and = (Expr *)linitial(and_clauses);
	List *first_and_args = ((BoolExpr *)first_and)->args;

	/* Remove duplicates from first AND arguments */
	first_and_args = remove_duplicates_in_list(first_and_args);

	/* Remaining AND clauses */
	List *remaining_ands = list_delete_first(list_copy(and_clauses));

	/* Base arguments for distribution: non-ANDs + remaining ANDs */
	List *base_args = list_concat(remove_duplicates_in_list(non_ands),
								  remaining_ands);

	/* Apply distribution */
	List *distributed_clauses = NIL;
	ListCell *lc;

	foreach (lc, first_and_args)
	{
		Expr *subclause = (Expr *)lfirst(lc);

		/* Create new OR: (base_args OR subclause) */
		List *new_or_args = list_copy(base_args);
		new_or_args = lappend(new_or_args, subclause);

		/* Remove duplicates in the new OR arguments */
		new_or_args = remove_duplicates_in_list(new_or_args);

		/* Convert recursively to CNF */
		Expr *new_or = make_orclause(new_or_args);
		Expr *cnf_or = convert_expr_to_cnf_complete(new_or);

		distributed_clauses = lappend(distributed_clauses, cnf_or);
	}

	/* Combine all distributed clauses */
	return combine_cnf_clauses_complete(distributed_clauses);
}

/*
 * flatten_or_args_complete
 *    Flatten nested OR clauses with deduplication
 */
static List *
flatten_or_args_complete(List *args)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, args)
	{
		Expr *arg = (Expr *)lfirst(lc);

		if (is_orclause(arg))
		{
			List *sub_args = flatten_or_args_complete(((BoolExpr *)arg)->args);
			result = list_concat(result, sub_args);
		}
		else
		{
			result = lappend(result, arg);
		}
	}

	/* Remove duplicates after flattening */
	return remove_duplicates_in_list(result);
}

/*
 * flatten_and_args_complete
 *    Flatten nested AND clauses with deduplication
 */
static List *
flatten_and_args_complete(List *args)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, args)
	{
		Expr *arg = (Expr *)lfirst(lc);

		if (is_andclause(arg))
		{
			List *sub_args = flatten_and_args_complete(((BoolExpr *)arg)->args);
			result = list_concat(result, sub_args);
		}
		else
		{
			result = lappend(result, arg);
		}
	}

	/* Remove duplicates after flattening */
	return remove_duplicates_in_list(result);
}

/*
 * combine_cnf_clauses_complete
 *    Combine CNF clauses with advanced deduplication
 */
static Expr *
combine_cnf_clauses_complete(List *clauses)
{
	if (list_length(clauses) == 0)
		return (Expr *)makeBoolConst(true, false);

	if (list_length(clauses) == 1)
		return (Expr *)linitial(clauses);

	/* Extract all subclauses, handling nested ANDs */
	List *all_clauses = NIL;
	ListCell *lc;

	foreach (lc, clauses)
	{
		Expr *clause = (Expr *)lfirst(lc);

		if (is_andclause(clause))
		{
			all_clauses = list_concat(all_clauses,
									  list_copy(((BoolExpr *)clause)->args));
		}
		else
		{
			all_clauses = lappend(all_clauses, clause);
		}
	}

	/* Remove duplicates and subsumed clauses */
	all_clauses = remove_duplicate_and_subsumed_clauses(all_clauses);

	if (list_length(all_clauses) == 0)
		return (Expr *)makeBoolConst(true, false);
	else if (list_length(all_clauses) == 1)
		return (Expr *)linitial(all_clauses);
	else
		return make_andclause(all_clauses);
}

/*
 * remove_duplicates_in_list
 *    Remove duplicate expressions from a list
 */
static List *
remove_duplicates_in_list(List *clauses)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, clauses)
	{
		Expr *clause = (Expr *)lfirst(lc);
		bool found = false;
		ListCell *lc2;

		foreach (lc2, result)
		{
			if (equal(clause, (Expr *)lfirst(lc2)))
			{
				found = true;
				break;
			}
		}

		if (!found)
			result = lappend(result, clause);
	}

	return result;
}

/*
 * remove_duplicate_and_subsumed_clauses
 *    Remove duplicates and logically redundant clauses from a CNF conjunction.
 *
 * In a CNF conjunction (AND of clauses), a clause is redundant if it is
 * logically implied by another clause already in the list. We detect three
 * cases of redundancy:
 *
 * 1. Exact duplicates: (A OR B) AND (A OR B) → keep one
 *
 * 2. OR-vs-OR subsumption: (A OR B) AND (A OR B OR C) → keep (A OR B)
 *    A shorter OR-clause with all its terms present in a longer one
 *    makes the longer one always-true when the shorter one is true.
 *
 * 3. Literal-vs-OR subsumption: A AND (A OR B) → keep A
 *    A bare literal makes any OR-clause containing it redundant,
 *    because if the literal is true, the OR-clause is trivially true.
 */
static List *
remove_duplicate_and_subsumed_clauses(List *clauses)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, clauses)
	{
		Expr *clause = (Expr *)lfirst(lc);
		bool keep = true;
		List *to_remove = NIL;

		/* Check against all existing clauses */
		ListCell *lc_exist;
		foreach (lc_exist, result)
		{
			Expr *existing = (Expr *)lfirst(lc_exist);

			/* Exact duplicate */
			if (equal(clause, existing))
			{
				keep = false;
				break;
			}

			/* Check for OR clause subsumption */
			if (is_orclause(clause) && is_orclause(existing))
			{
				if (or_clause_subsumes(existing, clause))
				{
					/* Existing subsumes current, skip current */
					keep = false;
					break;
				}
				else if (or_clause_subsumes(clause, existing))
				{
					/*
					 * Current subsumes existing, mark for removal.
					 * Continue checking other clauses since the current
					 * clause may subsume multiple existing clauses.
					 */
					to_remove = lappend(to_remove, existing);
				}
			}
			else if (!is_orclause(clause) && is_orclause(existing))
			{
				/* A AND (A OR B) could be simplied to A */
				if (list_member(((BoolExpr *)existing)->args, clause))
				{
					to_remove = lappend(to_remove, existing);
				}
			}
			else if (is_orclause(clause) && !is_orclause(existing))
			{
				if (list_member(((BoolExpr *)clause)->args, existing))
				{
					keep = false;
					break;
				}
			}
		}

		/* Remove all clauses that current clause subsumes */
		if (to_remove != NIL)
		{
			ListCell *lc_rm;
			foreach (lc_rm, to_remove)
			{
				result = list_delete_ptr(result, lfirst(lc_rm));
			}
			list_free(to_remove);
		}

		if (keep)
			result = lappend(result, clause);
	}

	return result;
}

/*
 * or_clause_subsumes
 *    Check if or_clause1 subsumes or_clause2
 *    (A OR B) subsumes (B OR C OR A) means we can remove (B OR C OR A)
 */
static bool
or_clause_subsumes(Expr *or_clause1, Expr *or_clause2)
{
	if (!is_orclause(or_clause1) || !is_orclause(or_clause2))
		return false;

	List *args1 = ((BoolExpr *)or_clause1)->args;
	List *args2 = ((BoolExpr *)or_clause2)->args;

	/* If all elements of clause1 are in clause2, clause1 subsumes clause2 */
	ListCell *lc1;
	foreach (lc1, args1)
	{
		Expr *arg1 = (Expr *)lfirst(lc1);
		bool found = false;
		ListCell *lc2;

		foreach (lc2, args2)
		{
			if (equal(arg1, (Expr *)lfirst(lc2)))
			{
				found = true;
				break;
			}
		}

		if (!found)
			return false;
	}

	return true;
}

/*
 * deduplicate_cnf_result
 *    Final deduplication pass for the CNF result
 */
static Expr *
deduplicate_cnf_result(Expr *expr)
{
	if (!is_andclause(expr))
		return expr;

	List *and_args = ((BoolExpr *)expr)->args;
	List *unique_clauses = remove_duplicate_and_subsumed_clauses(and_args);

	if (list_length(unique_clauses) == 0)
		return (Expr *)makeBoolConst(true, false);
	else if (list_length(unique_clauses) == 1)
		return (Expr *)linitial(unique_clauses);
	else
		return make_andclause(unique_clauses);
}
