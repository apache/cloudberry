/*-------------------------------------------------------------------------
 *
 * predtest.c
 *	  Routines to attempt to prove logical implications between predicate
 *	  expressions.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/predtest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/predtest.h"
#include "utils/array.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "nodes/makefuncs.h"

#include "catalog/pg_operator.h"
#include "optimizer/paths.h"

static const bool kUseFnEvaluationForPredicates = true;

/*
 * Proof attempts involving large arrays in ScalarArrayOpExpr nodes are
 * likely to require O(N^2) time, and more often than not fail anyway.
 * So we set an arbitrary limit on the number of array elements that
 * we will allow to be treated as an AND or OR clause.
 * XXX is it worth exposing this as a GUC knob?
 */
#define MAX_SAOP_ARRAY_SIZE		100

/*
 * To avoid redundant coding in predicate_implied_by_recurse and
 * predicate_refuted_by_recurse, we need to abstract out the notion of
 * iterating over the components of an expression that is logically an AND
 * or OR structure.  There are multiple sorts of expression nodes that can
 * be treated as ANDs or ORs, and we don't want to code each one separately.
 * Hence, these types and support routines.
 */
typedef enum
{
	CLASS_ATOM,					/* expression that's not AND or OR */
	CLASS_AND,					/* expression with AND semantics */
	CLASS_OR					/* expression with OR semantics */
} PredClass;

typedef struct PredIterInfoData *PredIterInfo;

typedef struct PredIterInfoData
{
	/* node-type-specific iteration state */
	void	   *state;
	/* initialize to do the iteration */
	void		(*startup_fn) (Node *clause, PredIterInfo info);
	/* next-component iteration function */
	Node	   *(*next_fn) (PredIterInfo info);
	/* release resources when done with iteration */
	void		(*cleanup_fn) (PredIterInfo info);
} PredIterInfoData;

#define iterate_begin(item, clause, info)	\
	do { \
		Node   *item; \
		(info).startup_fn((clause), &(info)); \
		while ((item = (info).next_fn(&(info))) != NULL)

#define iterate_end(info)	\
		(info).cleanup_fn(&(info)); \
	} while (0)


static bool predicate_implied_by_internal(List *predicate_list, List *clause_list,
							 bool weak);
static bool predicate_implied_by_recurse(Node *clause, Node *predicate,
							 bool weak);
static bool predicate_refuted_by_internal(List *predicate_list, List *clause_list,
							 bool weak);
static bool predicate_refuted_by_recurse(Node *clause, Node *predicate,
							 bool weak);
static PredClass predicate_classify(Node *clause, PredIterInfo info);
static void list_startup_fn(Node *clause, PredIterInfo info);
static Node *list_next_fn(PredIterInfo info);
static void list_cleanup_fn(PredIterInfo info);
static void boolexpr_startup_fn(Node *clause, PredIterInfo info);
static void arrayconst_startup_fn(Node *clause, PredIterInfo info);
static Node *arrayconst_next_fn(PredIterInfo info);
static void arrayconst_cleanup_fn(PredIterInfo info);
static void arrayexpr_startup_fn(Node *clause, PredIterInfo info);
static Node *arrayexpr_next_fn(PredIterInfo info);
static void arrayexpr_cleanup_fn(PredIterInfo info);
static bool predicate_implied_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak);
static bool predicate_refuted_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak);
static Node *extract_not_arg(Node *clause);
static Node *extract_strong_not_arg(Node *clause);
static bool btree_predicate_proof(Expr *predicate, Node *clause,
					  bool refute_it, bool weak);
static bool clause_is_strict_for(Node *clause, Node *subexpr);
static Oid	get_btree_test_op(Oid pred_op, Oid clause_op, bool refute_it);
static void InvalidateOprProofCacheCallBack(Datum arg, int cacheid, uint32 hashvalue);


static bool simple_equality_predicate_refuted(Node *clause, Node *predicate);

/*
 * predicate_implied_by_internal
 *	  Recursively checks whether the clauses in clause_list imply that the
 *	  given predicate is true.
 *
 * We support two definitions of implication:
 *
 * "Strong" implication: A implies B means that truth of A implies truth of B.
 * We use this to prove that a row satisfying one WHERE clause or index
 * predicate must satisfy another one.
 *
 * "Weak" implication: A implies B means that non-falsity of A implies
 * non-falsity of B ("non-false" means "either true or NULL").  We use this to
 * prove that a row satisfying one CHECK constraint must satisfy another one.
 *
 * Strong implication can also be used to prove that a WHERE clause implies a
 * CHECK constraint, although it will fail to prove a few cases where we could
 * safely conclude that the implication holds.  There's no support for proving
 * the converse case, since only a few kinds of CHECK constraint would allow
 * deducing anything.
 *
 * The top-level List structure of each list corresponds to an AND list.
 * We assume that eval_const_expressions() has been applied and so there
 * are no un-flattened ANDs or ORs (e.g., no AND immediately within an AND,
 * including AND just below the top-level List structure).
 * If this is not true we might fail to prove an implication that is
 * valid, but no worse consequences will ensue.
 *
 * We assume the predicate has already been checked to contain only
 * immutable functions and operators.  (In many current uses this is known
 * true because the predicate is part of an index predicate that has passed
 * CheckPredicate(); otherwise, the caller must check it.)  We dare not make
 * deductions based on non-immutable functions, because they might change
 * answers between the time we make the plan and the time we execute the plan.
 * Immutability of functions in the clause_list is checked here, if necessary.
 */
static bool
predicate_implied_by_internal(List *predicate_list, List *clause_list,
					 bool weak)
{
	Node	   *p,
			   *c;

	if (predicate_list == NIL)
		return true;			/* no predicate: implication is vacuous */
	if (clause_list == NIL)
		return false;			/* no restriction: implication must fail */

	/*
	 * If either input is a single-element list, replace it with its lone
	 * member; this avoids one useless level of AND-recursion.  We only need
	 * to worry about this at top level, since eval_const_expressions should
	 * have gotten rid of any trivial ANDs or ORs below that.
	 */
	if (list_length(predicate_list) == 1)
		p = (Node *) linitial(predicate_list);
	else
		p = (Node *) predicate_list;
	if (list_length(clause_list) == 1)
		c = (Node *) linitial(clause_list);
	else
		c = (Node *) clause_list;

	/* And away we go ... */
	return predicate_implied_by_recurse(c, p, weak);
}

bool
predicate_implied_by(List *predicate_list, List *clause_list)
{
	return predicate_implied_by_internal(predicate_list, clause_list, false);
}

bool
predicate_implied_by_weak(List *predicate_list, List *clause_list)
{
	return predicate_implied_by_internal(predicate_list, clause_list, true);
}

/*
 * predicate_refuted_by_internal
 *	  Recursively checks whether the clauses in clause_list refute the given
 *	  predicate (that is, prove it false).
 *
 * This is NOT the same as !(predicate_implied_by_internal), though it is similar
 * in the technique and structure of the code.
 *
 * We support two definitions of refutation:
 *
 * "Strong" refutation: A refutes B means truth of A implies falsity of B.
 * We use this to disprove a CHECK constraint given a WHERE clause, i.e.,
 * prove that any row satisfying the WHERE clause would violate the CHECK
 * constraint.  (Observe we must prove B yields false, not just not-true.)
 *
 * "Weak" refutation: A refutes B means truth of A implies non-truth of B
 * (i.e., B must yield false or NULL).  We use this to detect mutually
 * contradictory WHERE clauses.
 *
 * Weak refutation can be proven in some cases where strong refutation doesn't
 * hold, so it's useful to use it when possible.  We don't currently have
 * support for disproving one CHECK constraint based on another one, nor for
 * disproving WHERE based on CHECK.  (As with implication, the last case
 * doesn't seem very practical.  CHECK-vs-CHECK might be useful, but isn't
 * currently needed anywhere.)
 *
 * The top-level List structure of each list corresponds to an AND list.
 * We assume that eval_const_expressions() has been applied and so there
 * are no un-flattened ANDs or ORs (e.g., no AND immediately within an AND,
 * including AND just below the top-level List structure).
 * If this is not true we might fail to prove an implication that is
 * valid, but no worse consequences will ensue.
 *
 * We assume the predicate has already been checked to contain only
 * immutable functions and operators.  We dare not make deductions based on
 * non-immutable functions, because they might change answers between the
 * time we make the plan and the time we execute the plan.
 * Immutability of functions in the clause_list is checked here, if necessary.
 */
static bool
predicate_refuted_by_internal(List *predicate_list, List *clause_list,
					 bool weak)
{
	Node	   *p,
			   *c;

	if (predicate_list == NIL)
		return false;			/* no predicate: no refutation is possible */
	if (clause_list == NIL)
		return false;			/* no restriction: refutation must fail */

	/*
	 * If either input is a single-element list, replace it with its lone
	 * member; this avoids one useless level of AND-recursion.  We only need
	 * to worry about this at top level, since eval_const_expressions should
	 * have gotten rid of any trivial ANDs or ORs below that.
	 */
	if (list_length(predicate_list) == 1)
		p = (Node *) linitial(predicate_list);
	else
		p = (Node *) predicate_list;
	if (list_length(clause_list) == 1)
		c = (Node *) linitial(clause_list);
	else
		c = (Node *) clause_list;

	/* And away we go ... */
	if (predicate_refuted_by_recurse(c, p, weak))
        return true;

    if ( ! kUseFnEvaluationForPredicates )
        return false;
    return simple_equality_predicate_refuted((Node*)clause_list, (Node*)predicate_list);
}

bool
predicate_refuted_by(List *predicate_list, List *clause_list)
{
	return predicate_refuted_by_internal(predicate_list, clause_list, false);
}

bool
predicate_refuted_by_weak(List *predicate_list, List *clause_list)
{
	return predicate_refuted_by_internal(predicate_list, clause_list, true);
}

/*----------
 * predicate_implied_by_recurse
 *	  Does the predicate implication test for non-NULL restriction and
 *	  predicate clauses.
 *
 * The logic followed here is ("=>" means "implies"):
 *	atom A => atom B iff:			predicate_implied_by_simple_clause says so
 *	atom A => AND-expr B iff:		A => each of B's components
 *	atom A => OR-expr B iff:		A => any of B's components
 *	AND-expr A => atom B iff:		any of A's components => B
 *	AND-expr A => AND-expr B iff:	A => each of B's components
 *	AND-expr A => OR-expr B iff:	A => any of B's components,
 *									*or* any of A's components => B
 *	OR-expr A => atom B iff:		each of A's components => B
 *	OR-expr A => AND-expr B iff:	A => each of B's components
 *	OR-expr A => OR-expr B iff:		each of A's components => any of B's
 *
 * An "atom" is anything other than an AND or OR node.  Notice that we don't
 * have any special logic to handle NOT nodes; these should have been pushed
 * down or eliminated where feasible during eval_const_expressions().
 *
 * All of these rules apply equally to strong or weak implication.
 *
 * We can't recursively expand either side first, but have to interleave
 * the expansions per the above rules, to be sure we handle all of these
 * examples:
 *		(x OR y) => (x OR y OR z)
 *		(x AND y AND z) => (x AND y)
 *		(x AND y) => ((x AND y) OR z)
 *		((x OR y) AND z) => (x OR y)
 * This is still not an exhaustive test, but it handles most normal cases
 * under the assumption that both inputs have been AND/OR flattened.
 *
 * We have to be prepared to handle RestrictInfo nodes in the restrictinfo
 * tree, though not in the predicate tree.
 *----------
 */
static bool
predicate_implied_by_recurse(Node *clause, Node *predicate,
							 bool weak)
{
	PredIterInfoData clause_info;
	PredIterInfoData pred_info;
	PredClass	pclass;
	bool		result;

	/* skip through RestrictInfo */
	Assert(clause != NULL);
	if (IsA(clause, RestrictInfo))
		clause = (Node *) ((RestrictInfo *) clause)->clause;

	pclass = predicate_classify(predicate, &pred_info);

	switch (predicate_classify(clause, &clause_info))
	{
		case CLASS_AND:
			switch (pclass)
			{
				case CLASS_AND:

					/*
					 * AND-clause => AND-clause if A implies each of B's items
					 */
					result = true;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (!predicate_implied_by_recurse(clause, pitem,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_OR:

					/*
					 * AND-clause => OR-clause if A implies any of B's items
					 *
					 * Needed to handle (x AND y) => ((x AND y) OR z)
					 */
					result = false;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (predicate_implied_by_recurse(clause, pitem,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(pred_info);
					if (result)
						return result;

					/*
					 * Also check if any of A's items implies B
					 *
					 * Needed to handle ((x OR y) AND z) => (x OR y)
					 */
					iterate_begin(citem, clause, clause_info)
					{
						if (predicate_implied_by_recurse(citem, predicate,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(clause_info);
					return result;

				case CLASS_ATOM:

					/*
					 * AND-clause => atom if any of A's items implies B
					 */
					result = false;
					iterate_begin(citem, clause, clause_info)
					{
						if (predicate_implied_by_recurse(citem, predicate,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(clause_info);
					return result;
			}
			break;

		case CLASS_OR:
			switch (pclass)
			{
				case CLASS_OR:

					/*
					 * OR-clause => OR-clause if each of A's items implies any
					 * of B's items.  Messy but can't do it any more simply.
					 */
					result = true;
					iterate_begin(citem, clause, clause_info)
					{
						bool		presult = false;

						iterate_begin(pitem, predicate, pred_info)
						{
							if (predicate_implied_by_recurse(citem, pitem,
															 weak))
							{
								presult = true;
								break;
							}
						}
						iterate_end(pred_info);
						if (!presult)
						{
							result = false;		/* doesn't imply any of B's */
							break;
						}
					}
					iterate_end(clause_info);
					return result;

				case CLASS_AND:
				case CLASS_ATOM:

					/*
					 * OR-clause => AND-clause if each of A's items implies B
					 *
					 * OR-clause => atom if each of A's items implies B
					 */
					result = true;
					iterate_begin(citem, clause, clause_info)
					{
						if (!predicate_implied_by_recurse(citem, predicate,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(clause_info);
					return result;
			}
			break;

		case CLASS_ATOM:
			switch (pclass)
			{
				case CLASS_AND:

					/*
					 * atom => AND-clause if A implies each of B's items
					 */
					result = true;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (!predicate_implied_by_recurse(clause, pitem,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_OR:

					/*
					 * atom => OR-clause if A implies any of B's items
					 */
					result = false;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (predicate_implied_by_recurse(clause, pitem,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_ATOM:

					/*
					 * atom => atom is the base case
					 */
					return
						predicate_implied_by_simple_clause((Expr *) predicate,
														   clause,
														   weak);
			}
			break;
	}

	/* can't get here */
	elog(ERROR, "predicate_classify returned a bogus value");
	return false;
}

/*----------
 * predicate_refuted_by_recurse
 *	  Does the predicate refutation test for non-NULL restriction and
 *	  predicate clauses.
 *
 * The logic followed here is ("R=>" means "refutes"):
 *	atom A R=> atom B iff:			predicate_refuted_by_simple_clause says so
 *	atom A R=> AND-expr B iff:		A R=> any of B's components
 *	atom A R=> OR-expr B iff:		A R=> each of B's components
 *	AND-expr A R=> atom B iff:		any of A's components R=> B
 *	AND-expr A R=> AND-expr B iff:	A R=> any of B's components,
 *									*or* any of A's components R=> B
 *	AND-expr A R=> OR-expr B iff:	A R=> each of B's components
 *	OR-expr A R=> atom B iff:		each of A's components R=> B
 *	OR-expr A R=> AND-expr B iff:	each of A's components R=> any of B's
 *	OR-expr A R=> OR-expr B iff:	A R=> each of B's components
 *
 * All of the above rules apply equally to strong or weak refutation.
 *
 * In addition, if the predicate is a NOT-clause then we can use
 *	A R=> NOT B if:					A => B
 * This works for several different SQL constructs that assert the non-truth
 * of their argument, ie NOT, IS FALSE, IS NOT TRUE, IS UNKNOWN, although some
 * of them require that we prove strong implication.  Likewise, we can use
 *	NOT A R=> B if:					B => A
 * but here we must be careful about strong vs. weak refutation and make
 * the appropriate type of implication proof (weak or strong respectively).
 *
 * Other comments are as for predicate_implied_by_recurse().
 *----------
 */
static bool
predicate_refuted_by_recurse(Node *clause, Node *predicate,
							 bool weak)
{
	PredIterInfoData clause_info;
	PredIterInfoData pred_info;
	PredClass	pclass;
	Node	   *not_arg;
	bool		result;

	CHECK_FOR_INTERRUPTS();

	/* skip through RestrictInfo */
	Assert(clause != NULL);
	if (IsA(clause, RestrictInfo))
		clause = (Node *) ((RestrictInfo *) clause)->clause;

	pclass = predicate_classify(predicate, &pred_info);

	switch (predicate_classify(clause, &clause_info))
	{
		case CLASS_AND:
			switch (pclass)
			{
				case CLASS_AND:

					/*
					 * AND-clause R=> AND-clause if A refutes any of B's items
					 *
					 * Needed to handle (x AND y) R=> ((!x OR !y) AND z)
					 */
					result = false;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (predicate_refuted_by_recurse(clause, pitem,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(pred_info);
					if (result)
						return result;

					/*
					 * Also check if any of A's items refutes B
					 *
					 * Needed to handle ((x OR y) AND z) R=> (!x AND !y)
					 */
					iterate_begin(citem, clause, clause_info)
					{
						if (predicate_refuted_by_recurse(citem, predicate,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(clause_info);
					return result;

				case CLASS_OR:

					/*
					 * AND-clause R=> OR-clause if A refutes each of B's items
					 */
					result = true;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (!predicate_refuted_by_recurse(clause, pitem,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_ATOM:

					/*
					 * If B is a NOT-type clause, A R=> B if A => B's arg
					 *
					 * Since, for either type of refutation, we are starting
					 * with the premise that A is true, we can use a strong
					 * implication test in all cases.  That proves B's arg is
					 * true, which is more than we need for weak refutation if
					 * B is a simple NOT, but it allows not worrying about
					 * exactly which kind of negation clause we have.
					 */
					not_arg = extract_not_arg(predicate);
					if (not_arg &&
						predicate_implied_by_recurse(clause, not_arg,
													 false))
						return true;

					/*
					 * AND-clause R=> atom if any of A's items refutes B
					 */
					result = false;
					iterate_begin(citem, clause, clause_info)
					{
						if (predicate_refuted_by_recurse(citem, predicate,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(clause_info);
					return result;
			}
			break;

		case CLASS_OR:
			switch (pclass)
			{
				case CLASS_OR:

					/*
					 * OR-clause R=> OR-clause if A refutes each of B's items
					 */
					result = true;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (!predicate_refuted_by_recurse(clause, pitem,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_AND:

					/*
					 * OR-clause R=> AND-clause if each of A's items refutes
					 * any of B's items.
					 */
					result = true;
					iterate_begin(citem, clause, clause_info)
					{
						bool		presult = false;

						iterate_begin(pitem, predicate, pred_info)
						{
							if (predicate_refuted_by_recurse(citem, pitem,
															 weak))
							{
								presult = true;
								break;
							}
						}
						iterate_end(pred_info);
						if (!presult)
						{
							result = false;		/* citem refutes nothing */
							break;
						}
					}
					iterate_end(clause_info);
					return result;

				case CLASS_ATOM:

					/*
					 * If B is a NOT-type clause, A R=> B if A => B's arg
					 *
					 * Same logic as for the AND-clause case above.
					 */
					not_arg = extract_not_arg(predicate);
					if (not_arg &&
						predicate_implied_by_recurse(clause, not_arg,
													 false))
						return true;

					/*
					 * OR-clause R=> atom if each of A's items refutes B
					 */
					result = true;
					iterate_begin(citem, clause, clause_info)
					{
						if (!predicate_refuted_by_recurse(citem, predicate,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(clause_info);
					return result;
			}
			break;

		case CLASS_ATOM:

			/*
			 * If A is a strong NOT-clause, A R=> B if B => A's arg
			 *
			 * Since A is strong, we may assume A's arg is false (not just
			 * not-true).  If B weakly implies A's arg, then B can be neither
			 * true nor null, so that strong refutation is proven.  If B
			 * strongly implies A's arg, then B cannot be true, so that weak
			 * refutation is proven.
			 */
			not_arg = extract_strong_not_arg(clause);
			if (not_arg &&
				predicate_implied_by_recurse(predicate, not_arg,
											 !weak))
				return true;

			switch (pclass)
			{
				case CLASS_AND:

					/*
					 * atom R=> AND-clause if A refutes any of B's items
					 */
					result = false;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (predicate_refuted_by_recurse(clause, pitem,
														 weak))
						{
							result = true;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_OR:

					/*
					 * atom R=> OR-clause if A refutes each of B's items
					 */
					result = true;
					iterate_begin(pitem, predicate, pred_info)
					{
						if (!predicate_refuted_by_recurse(clause, pitem,
														  weak))
						{
							result = false;
							break;
						}
					}
					iterate_end(pred_info);
					return result;

				case CLASS_ATOM:

					/*
					 * If B is a NOT-type clause, A R=> B if A => B's arg
					 *
					 * Same logic as for the AND-clause case above.
					 */
					not_arg = extract_not_arg(predicate);
					if (not_arg &&
						predicate_implied_by_recurse(clause, not_arg,
													 false))
						return true;

					/*
					 * atom R=> atom is the base case
					 */
					return
						predicate_refuted_by_simple_clause((Expr *) predicate,
														   clause,
														   weak);
			}
			break;
	}

	/* can't get here */
	elog(ERROR, "predicate_classify returned a bogus value");
	return false;
}


/*
 * predicate_classify
 *	  Classify an expression node as AND-type, OR-type, or neither (an atom).
 *
 * If the expression is classified as AND- or OR-type, then *info is filled
 * in with the functions needed to iterate over its components.
 *
 * This function also implements enforcement of MAX_SAOP_ARRAY_SIZE: if a
 * ScalarArrayOpExpr's array has too many elements, we just classify it as an
 * atom.  (This will result in its being passed as-is to the simple_clause
 * functions, which will fail to prove anything about it.)	Note that we
 * cannot just stop after considering MAX_SAOP_ARRAY_SIZE elements; in general
 * that would result in wrong proofs, rather than failing to prove anything.
 */
static PredClass
predicate_classify(Node *clause, PredIterInfo info)
{
	/* Caller should not pass us NULL, nor a RestrictInfo clause */
	Assert(clause != NULL);
	Assert(!IsA(clause, RestrictInfo));

	/*
	 * If we see a List, assume it's an implicit-AND list; this is the correct
	 * semantics for lists of RestrictInfo nodes.
	 */
	if (IsA(clause, List))
	{
		info->startup_fn = list_startup_fn;
		info->next_fn = list_next_fn;
		info->cleanup_fn = list_cleanup_fn;
		return CLASS_AND;
	}

	/* Handle normal AND and OR boolean clauses */
	if (and_clause(clause))
	{
		info->startup_fn = boolexpr_startup_fn;
		info->next_fn = list_next_fn;
		info->cleanup_fn = list_cleanup_fn;
		return CLASS_AND;
	}
	if (or_clause(clause))
	{
		info->startup_fn = boolexpr_startup_fn;
		info->next_fn = list_next_fn;
		info->cleanup_fn = list_cleanup_fn;
		return CLASS_OR;
	}

	/* Handle ScalarArrayOpExpr */
	if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		Node	   *arraynode = (Node *) lsecond(saop->args);

		/*
		 * We can break this down into an AND or OR structure, but only if we
		 * know how to iterate through expressions for the array's elements.
		 * We can do that if the array operand is a non-null constant or a
		 * simple ArrayExpr.
		 */
		if (arraynode && IsA(arraynode, Const) &&
			!((Const *) arraynode)->constisnull)
		{
			ArrayType  *arrayval;
			int			nelems;

			arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
			nelems = ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
			if (nelems <= MAX_SAOP_ARRAY_SIZE)
			{
				info->startup_fn = arrayconst_startup_fn;
				info->next_fn = arrayconst_next_fn;
				info->cleanup_fn = arrayconst_cleanup_fn;
				return saop->useOr ? CLASS_OR : CLASS_AND;
			}
		}
		else if (arraynode && IsA(arraynode, ArrayExpr) &&
				 !((ArrayExpr *) arraynode)->multidims &&
				 list_length(((ArrayExpr *) arraynode)->elements) <= MAX_SAOP_ARRAY_SIZE)
		{
			info->startup_fn = arrayexpr_startup_fn;
			info->next_fn = arrayexpr_next_fn;
			info->cleanup_fn = arrayexpr_cleanup_fn;
			return saop->useOr ? CLASS_OR : CLASS_AND;
		}
	}

	/* None of the above, so it's an atom */
	return CLASS_ATOM;
}

/*
 * PredIterInfo routines for iterating over regular Lists.  The iteration
 * state variable is the next ListCell to visit.
 */
static void
list_startup_fn(Node *clause, PredIterInfo info)
{
	info->state = (void *) list_head((List *) clause);
}

static Node *
list_next_fn(PredIterInfo info)
{
	ListCell   *l = (ListCell *) info->state;
	Node	   *n;

	if (l == NULL)
		return NULL;
	n = lfirst(l);
	info->state = (void *) lnext(l);
	return n;
}

static void
list_cleanup_fn(PredIterInfo info)
{
	/* Nothing to clean up */
}

/*
 * BoolExpr needs its own startup function, but can use list_next_fn and
 * list_cleanup_fn.
 */
static void
boolexpr_startup_fn(Node *clause, PredIterInfo info)
{
	info->state = (void *) list_head(((BoolExpr *) clause)->args);
}

/*
 * PredIterInfo routines for iterating over a ScalarArrayOpExpr with a
 * constant array operand.
 */
typedef struct
{
	OpExpr		opexpr;
	Const		constexpr;
	int			next_elem;
	int			num_elems;
	Datum	   *elem_values;
	bool	   *elem_nulls;
} ArrayConstIterState;

static void
arrayconst_startup_fn(Node *clause, PredIterInfo info)
{
	ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
	ArrayConstIterState *state;
	Const	   *arrayconst;
	ArrayType  *arrayval;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;

	/* Create working state struct */
	state = (ArrayConstIterState *) palloc(sizeof(ArrayConstIterState));
	info->state = (void *) state;

	/* Deconstruct the array literal */
	arrayconst = (Const *) lsecond(saop->args);
	arrayval = DatumGetArrayTypeP(arrayconst->constvalue);
	get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
						 &elmlen, &elmbyval, &elmalign);
	deconstruct_array(arrayval,
					  ARR_ELEMTYPE(arrayval),
					  elmlen, elmbyval, elmalign,
					  &state->elem_values, &state->elem_nulls,
					  &state->num_elems);

	/* Set up a dummy OpExpr to return as the per-item node */
	state->opexpr.xpr.type = T_OpExpr;
	state->opexpr.opno = saop->opno;
	state->opexpr.opfuncid = saop->opfuncid;
	state->opexpr.opresulttype = BOOLOID;
	state->opexpr.opretset = false;
	state->opexpr.opcollid = InvalidOid;
	state->opexpr.inputcollid = saop->inputcollid;
	state->opexpr.args = list_copy(saop->args);

	/* Set up a dummy Const node to hold the per-element values */
	state->constexpr.xpr.type = T_Const;
	state->constexpr.consttype = ARR_ELEMTYPE(arrayval);
	state->constexpr.consttypmod = -1;
	state->constexpr.constcollid = arrayconst->constcollid;
	state->constexpr.constlen = elmlen;
	state->constexpr.constbyval = elmbyval;
	lsecond(state->opexpr.args) = &state->constexpr;

	/* Initialize iteration state */
	state->next_elem = 0;
}

static Node *
arrayconst_next_fn(PredIterInfo info)
{
	ArrayConstIterState *state = (ArrayConstIterState *) info->state;

	if (state->next_elem >= state->num_elems)
		return NULL;
	state->constexpr.constvalue = state->elem_values[state->next_elem];
	state->constexpr.constisnull = state->elem_nulls[state->next_elem];
	state->next_elem++;
	return (Node *) &(state->opexpr);
}

static void
arrayconst_cleanup_fn(PredIterInfo info)
{
	ArrayConstIterState *state = (ArrayConstIterState *) info->state;

	pfree(state->elem_values);
	pfree(state->elem_nulls);
	list_free(state->opexpr.args);
	pfree(state);
}

/*
 * PredIterInfo routines for iterating over a ScalarArrayOpExpr with a
 * one-dimensional ArrayExpr array operand.
 */
typedef struct
{
	OpExpr		opexpr;
	ListCell   *next;
} ArrayExprIterState;

static void
arrayexpr_startup_fn(Node *clause, PredIterInfo info)
{
	ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
	ArrayExprIterState *state;
	ArrayExpr  *arrayexpr;

	/* Create working state struct */
	state = (ArrayExprIterState *) palloc(sizeof(ArrayExprIterState));
	info->state = (void *) state;

	/* Set up a dummy OpExpr to return as the per-item node */
	state->opexpr.xpr.type = T_OpExpr;
	state->opexpr.opno = saop->opno;
	state->opexpr.opfuncid = saop->opfuncid;
	state->opexpr.opresulttype = BOOLOID;
	state->opexpr.opretset = false;
	state->opexpr.opcollid = InvalidOid;
	state->opexpr.inputcollid = saop->inputcollid;
	state->opexpr.args = list_copy(saop->args);

	/* Initialize iteration variable to first member of ArrayExpr */
	arrayexpr = (ArrayExpr *) lsecond(saop->args);
	state->next = list_head(arrayexpr->elements);
}

static Node *
arrayexpr_next_fn(PredIterInfo info)
{
	ArrayExprIterState *state = (ArrayExprIterState *) info->state;

	if (state->next == NULL)
		return NULL;
	lsecond(state->opexpr.args) = lfirst(state->next);
	state->next = lnext(state->next);
	return (Node *) &(state->opexpr);
}

static void
arrayexpr_cleanup_fn(PredIterInfo info)
{
	ArrayExprIterState *state = (ArrayExprIterState *) info->state;

	list_free(state->opexpr.args);
	pfree(state);
}


/*----------
 * predicate_implied_by_simple_clause
 *	  Does the predicate implication test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 *
 * We return TRUE if able to prove the implication, FALSE if not.
 *
 * We have three strategies for determining whether one simple clause
 * implies another:
 *
 * A simple and general way is to see if they are equal(); this works for any
 * kind of expression, and for either implication definition.  (Actually,
 * there is an implied assumption that the functions in the expression are
 * immutable --- but this was checked for the predicate by the caller.)
 *
 * If the predicate is of the form "foo IS NOT NULL", and we are considering
 * strong implication, we can conclude that the predicate is implied if the
 * clause is strict for "foo", i.e., it must yield NULL when "foo" is NULL.
 * In that case truth of the clause requires that "foo" isn't NULL.
 * (Again, this is a safe conclusion because "foo" must be immutable.)
 * This doesn't work for weak implication, though.
 *
 * Finally, we may be able to deduce something using knowledge about btree
 * operator families; this is encapsulated in btree_predicate_proof().
 *----------
 */
static bool
predicate_implied_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak)
{
	/* Allow interrupting long proof attempts */
	CHECK_FOR_INTERRUPTS();

	/* First try the equal() test */
	if (equal((Node *) predicate, clause))
		return true;

	/* Next try the IS NOT NULL case */
	if (!weak &&
		predicate && IsA(predicate, NullTest))
	{
		NullTest   *ntest = (NullTest *) predicate;

		/* row IS NOT NULL does not act in the simple way we have in mind */
		if (ntest->nulltesttype == IS_NOT_NULL &&
			!ntest->argisrow)
		{
			/* strictness of clause for foo implies foo IS NOT NULL */
			if (clause_is_strict_for(clause, (Node *) ntest->arg))
				return true;
		}
		return false;			/* we can't succeed below... */
	}

	/* Else try btree operator knowledge */
	return btree_predicate_proof(predicate, clause, false, weak);
}

/*----------
 * predicate_refuted_by_simple_clause
 *	  Does the predicate refutation test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 *
 * We return TRUE if able to prove the refutation, FALSE if not.
 *
 * Unlike the implication case, checking for equal() clauses isn't helpful.
 * But relation_excluded_by_constraints() checks for self-contradictions in a
 * list of clauses, so that we may get here with predicate and clause being
 * actually pointer-equal, and that is worth eliminating quickly.
 *
 * When the predicate is of the form "foo IS NULL", we can conclude that
 * the predicate is refuted if the clause is strict for "foo" (see notes for
 * implication case), or is "foo IS NOT NULL".  That works for either strong
 * or weak refutation.
 *
 * A clause "foo IS NULL" refutes a predicate "foo IS NOT NULL" in all cases.
 * If we are considering weak refutation, it also refutes a predicate that
 * is strict for "foo", since then the predicate must yield NULL (and since
 * "foo" appears in the predicate, it's known immutable).
 *
 * (The main motivation for covering these IS [NOT] NULL cases is to support
 * using IS NULL/IS NOT NULL as partition-defining constraints.)
 *
 * Finally, we may be able to deduce something using knowledge about btree
 * operator families; this is encapsulated in btree_predicate_proof().
 *----------
 */
static bool
predicate_refuted_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak)
{
	/* Allow interrupting long proof attempts */
	CHECK_FOR_INTERRUPTS();

	/* A simple clause can't refute itself */
	/* Worth checking because of relation_excluded_by_constraints() */
	if ((Node *) predicate == clause)
		return false;

	/* Try the predicate-IS-NULL case */
	if (predicate && IsA(predicate, NullTest) &&
		((NullTest *) predicate)->nulltesttype == IS_NULL)
	{
		Expr	   *isnullarg = ((NullTest *) predicate)->arg;

		/* row IS NULL does not act in the simple way we have in mind */
		if (((NullTest *) predicate)->argisrow)
			return false;

		/* strictness of clause for foo refutes foo IS NULL */
		if (clause_is_strict_for(clause, (Node *) isnullarg))
			return true;

		/* foo IS NOT NULL refutes foo IS NULL */
		if (clause && IsA(clause, NullTest) &&
			((NullTest *) clause)->nulltesttype == IS_NOT_NULL &&
			!((NullTest *) clause)->argisrow &&
			equal(((NullTest *) clause)->arg, isnullarg))
			return true;

		return false;			/* we can't succeed below... */
	}

	/* Try the clause-IS-NULL case */
	if (clause && IsA(clause, NullTest) &&
		((NullTest *) clause)->nulltesttype == IS_NULL)
	{
		Expr	   *isnullarg = ((NullTest *) clause)->arg;

		/* row IS NULL does not act in the simple way we have in mind */
		if (((NullTest *) clause)->argisrow)
			return false;

		/* foo IS NULL refutes foo IS NOT NULL */
		if (predicate && IsA(predicate, NullTest) &&
			((NullTest *) predicate)->nulltesttype == IS_NOT_NULL &&
			!((NullTest *) predicate)->argisrow &&
			equal(((NullTest *) predicate)->arg, isnullarg))
			return true;

		/* foo IS NULL weakly refutes any predicate that is strict for foo */
		if (weak &&
			clause_is_strict_for((Node *) predicate, (Node *) isnullarg))
			return true;

		return false;			/* we can't succeed below... */
	}

	/* Else try btree operator knowledge */
	return btree_predicate_proof(predicate, clause, true, weak);
}

/**
 * If n is a List, then return an AND tree of the nodes of the list
 * Otherwise return n.
 */
static Node *
convertToExplicitAndsShallowly( Node *n)
{
    if ( IsA(n, List))
    {
        ListCell *cell;
        List *list = (List*)n;
        Node *result = NULL;

        Assert(list_length(list) != 0 );

        foreach( cell, list )
        {
            Node *value = (Node*) lfirst(cell);
            if ( result == NULL)
            {
                result = value;
            }
            else
            {
                result = (Node *) makeBoolExpr(AND_EXPR, list_make2(result, value), -1 /* parse location */);
            }
        }
        return result;
    }
    else return n;
}

/**
 * Check to see if the predicate is expr=constant or constant=expr. In that case, try to evaluate the clause
 *   by replacing every occurrence of expr with the constant.  If the clause can then be reduced to FALSE, we
 *   conclude that the expression is refuted
 *
 * Returns true only if evaluation is possible AND expression is refuted based on evaluation results
 *
 * MPP-18979:
 * This mechanism cannot be used to prove implication. One example expression is
 * "F(x)=1 and x=2", where F(x) is an immutable function that returns 1 for any input x.
 * In this case, replacing x with 2 produces F(2)=1 and 2=2. Although evaluating the resulting
 * expression gives TRUE, we cannot conclude that (x=2) is implied by the whole expression.
 *
 */
static bool
simple_equality_predicate_refuted(Node *clause, Node *predicate)
{
	OpExpr *predicateExpr;
	Node *leftPredicateOp, *rightPredicateOp;
    Node *constExprInPredicate, *varExprInPredicate;
	List *list;

    /* BEGIN inspecting the predicate: this only works for a simple equality predicate */
    if ( nodeTag(predicate) != T_List )
        return false;

    if ( clause == predicate )
        return false; /* don't both doing for self-refutation ... let normal behavior handle that */

    list = (List *) predicate;
    if ( list_length(list) != 1 )
        return false;

    predicate = linitial(list);
	if ( ! is_opclause(predicate))
		return false;

	predicateExpr = (OpExpr*) predicate;
	leftPredicateOp = get_leftop((Expr*)predicate);
	rightPredicateOp = get_rightop((Expr*)predicate);
	if (!leftPredicateOp || !rightPredicateOp)
		return false;

	/* check if it's equality operation */
	if ( ! is_builtin_true_equality_between_same_type(predicateExpr->opno))
		return false;

	/* check if one operand is a constant */
	if ( IsA(rightPredicateOp, Const))
	{
		varExprInPredicate = leftPredicateOp;
		constExprInPredicate = rightPredicateOp;
	}
	else if ( IsA(leftPredicateOp, Const))
	{
		constExprInPredicate = leftPredicateOp;
		varExprInPredicate = rightPredicateOp;
	}
	else
	{
	    return false;
	}

    if ( IsA(varExprInPredicate, RelabelType))
    {
        RelabelType *rt = (RelabelType*) varExprInPredicate;
        varExprInPredicate = (Node*) rt->arg;
    }

    if ( ! IsA(varExprInPredicate, Var))
    {
        /* for now, this code is targeting predicates used in value partitions ...
         *   so don't apply it for other expressions.  This check can probably
         *   simply be removed and some test cases built. */
        return false;
    }

    /* DONE inspecting the predicate */

	/* clause may have non-immutable functions...don't eval if that's the case:
	 *
	 * Note that since we are replacing elements of the clause that match
	 *   varExprInPredicate, there is no need to also check varExprInPredicate
	 *   for mutable functions (note that this is only relevant when the
	 *   earlier check for varExprInPredicate being a Var is removed.
	 */
	if ( contain_mutable_functions(clause))
		return false;

	/* now do the evaluation */
	{
		Node *newClause, *reducedExpression;
		ReplaceExpressionMutatorReplacement replacement;
		bool				result = false;
		MemoryContext 		old_context;
		MemoryContext		tmp_context;

		replacement.replaceThis = varExprInPredicate;
		replacement.withThis = constExprInPredicate;
        replacement.numReplacementsDone = 0;

		tmp_context = AllocSetContextCreate(CurrentMemoryContext,
											"Predtest",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

		old_context = MemoryContextSwitchTo(tmp_context);

		newClause = replace_expression_mutator(clause, &replacement);

        if ( replacement.numReplacementsDone > 0)
        {
            newClause = convertToExplicitAndsShallowly(newClause);
            reducedExpression = eval_const_expressions(NULL, newClause);

            if ( IsA(reducedExpression, Const ))
            {
                Const *c = (Const *) reducedExpression;
                if ( c->consttype == BOOLOID &&
                     ! c->constisnull )
                {
                	result = (DatumGetBool(c->constvalue) == false);
                }
            }
        }

		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(tmp_context);
        return result;
	}
}

/*
 * If clause asserts the non-truth of a subclause, return that subclause;
 * otherwise return NULL.
 */
static Node *
extract_not_arg(Node *clause)
{
	if (clause == NULL)
		return NULL;
	if (IsA(clause, BoolExpr))
	{
		BoolExpr   *bexpr = (BoolExpr *) clause;

		if (bexpr->boolop == NOT_EXPR)
			return (Node *) linitial(bexpr->args);
	}
	else if (IsA(clause, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) clause;

		if (btest->booltesttype == IS_NOT_TRUE ||
			btest->booltesttype == IS_FALSE ||
			btest->booltesttype == IS_UNKNOWN)
			return (Node *) btest->arg;
	}
	return NULL;
}

/*
 * If clause asserts the falsity of a subclause, return that subclause;
 * otherwise return NULL.
 */
static Node *
extract_strong_not_arg(Node *clause)
{
	if (clause == NULL)
		return NULL;
	if (IsA(clause, BoolExpr))
	{
		BoolExpr   *bexpr = (BoolExpr *) clause;

		if (bexpr->boolop == NOT_EXPR)
			return (Node *) linitial(bexpr->args);
	}
	else if (IsA(clause, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) clause;

		if (btest->booltesttype == IS_FALSE)
			return (Node *) btest->arg;
	}
	return NULL;
}


/*
 * Can we prove that "clause" returns NULL if "subexpr" does?
 *
 * The base case is that clause and subexpr are equal().  (We assume that
 * the caller knows at least one of the input expressions is immutable,
 * as this wouldn't hold for volatile expressions.)
 *
 * We can also report success if the subexpr appears as a subexpression
 * of "clause" in a place where it'd force nullness of the overall result.
 */
static bool
clause_is_strict_for(Node *clause, Node *subexpr)
{
	ListCell   *lc;

	/* safety checks */
	if (clause == NULL || subexpr == NULL)
		return false;

	/*
	 * Look through any RelabelType nodes, so that we can match, say,
	 * varcharcol with lower(varcharcol::text).  (In general we could recurse
	 * through any nullness-preserving, immutable operation.)  We should not
	 * see stacked RelabelTypes here.
	 */
	if (IsA(clause, RelabelType))
		clause = (Node *) ((RelabelType *) clause)->arg;
	if (IsA(subexpr, RelabelType))
		subexpr = (Node *) ((RelabelType *) subexpr)->arg;

	/* Base case */
	if (equal(clause, subexpr))
		return true;

	/*
	 * If we have a strict operator or function, a NULL result is guaranteed
	 * if any input is forced NULL by subexpr.  This is OK even if the op or
	 * func isn't immutable, since it won't even be called on NULL input.
	 */
	if (is_opclause(clause) &&
		op_strict(((OpExpr *) clause)->opno))
	{
		foreach(lc, ((OpExpr *) clause)->args)
		{
			if (clause_is_strict_for((Node *) lfirst(lc), subexpr))
				return true;
		}
		return false;
	}
	if (is_funcclause(clause) &&
		func_strict(((FuncExpr *) clause)->funcid))
	{
		foreach(lc, ((FuncExpr *) clause)->args)
		{
			if (clause_is_strict_for((Node *) lfirst(lc), subexpr))
				return true;
		}
		return false;
	}

	return false;
}


/*
 * Define an "operator implication table" for btree operators ("strategies"),
 * and a similar table for refutation.
 *
 * The strategy numbers defined by btree indexes (see access/skey.h) are:
 *		(1) <	(2) <=	 (3) =	 (4) >=   (5) >
 * and in addition we use (6) to represent <>.  <> is not a btree-indexable
 * operator, but we assume here that if an equality operator of a btree
 * opfamily has a negator operator, the negator behaves as <> for the opfamily.
 * (This convention is also known to get_op_btree_interpretation().)
 *
 * The interpretation of:
 *
 *		test_op = BT_implic_table[given_op-1][target_op-1]
 *
 * where test_op, given_op and target_op are strategy numbers (from 1 to 6)
 * of btree operators, is as follows:
 *
 *	 If you know, for some ATTR, that "ATTR given_op CONST1" is true, and you
 *	 want to determine whether "ATTR target_op CONST2" must also be true, then
 *	 you can use "CONST2 test_op CONST1" as a test.  If this test returns true,
 *	 then the target expression must be true; if the test returns false, then
 *	 the target expression may be false.
 *
 * For example, if clause is "Quantity > 10" and pred is "Quantity > 5"
 * then we test "5 <= 10" which evals to true, so clause implies pred.
 *
 * Similarly, the interpretation of a BT_refute_table entry is:
 *
 *	 If you know, for some ATTR, that "ATTR given_op CONST1" is true, and you
 *	 want to determine whether "ATTR target_op CONST2" must be false, then
 *	 you can use "CONST2 test_op CONST1" as a test.  If this test returns true,
 *	 then the target expression must be false; if the test returns false, then
 *	 the target expression may be true.
 *
 * For example, if clause is "Quantity > 10" and pred is "Quantity < 5"
 * then we test "5 <= 10" which evals to true, so clause refutes pred.
 *
 * An entry where test_op == 0 means the implication cannot be determined.
 */

#define BTLT BTLessStrategyNumber
#define BTLE BTLessEqualStrategyNumber
#define BTEQ BTEqualStrategyNumber
#define BTGE BTGreaterEqualStrategyNumber
#define BTGT BTGreaterStrategyNumber
#define BTNE ROWCOMPARE_NE

static const StrategyNumber BT_implic_table[6][6] = {
/*
 *			The target operator:
 *
 *	 LT    LE	 EQ    GE	 GT    NE
 */
	{BTGE, BTGE, 0, 0, 0, BTGE},	/* LT */
	{BTGT, BTGE, 0, 0, 0, BTGT},	/* LE */
	{BTGT, BTGE, BTEQ, BTLE, BTLT, BTNE},		/* EQ */
	{0, 0, 0, BTLE, BTLT, BTLT},	/* GE */
	{0, 0, 0, BTLE, BTLE, BTLE},	/* GT */
	{0, 0, 0, 0, 0, BTEQ}		/* NE */
};

static const StrategyNumber BT_refute_table[6][6] = {
/*
 *			The target operator:
 *
 *	 LT    LE	 EQ    GE	 GT    NE
 */
	{0, 0, BTGE, BTGE, BTGE, 0},	/* LT */
	{0, 0, BTGT, BTGT, BTGE, 0},	/* LE */
	{BTLE, BTLT, BTNE, BTGT, BTGE, BTEQ},		/* EQ */
	{BTLE, BTLT, BTLT, 0, 0, 0},	/* GE */
	{BTLE, BTLE, BTLE, 0, 0, 0},	/* GT */
	{0, 0, BTEQ, 0, 0, 0}		/* NE */
};


/*
 * btree_predicate_proof
 *	  Does the predicate implication or refutation test for a "simple clause"
 *	  predicate and a "simple clause" restriction, when both are simple
 *	  operator clauses using related btree operators.
 *
 * When refute_it == false, we want to prove the predicate true;
 * when refute_it == true, we want to prove the predicate false.
 * (There is enough common code to justify handling these two cases
 * in one routine.)  We return TRUE if able to make the proof, FALSE
 * if not able to prove it.
 *
 * We mostly need not distinguish strong vs. weak implication/refutation here.
 * This depends on the assumption that a pair of related operators (i.e.,
 * commutators, negators, or btree opfamily siblings) will not return one NULL
 * and one non-NULL result for the same inputs.  Then, for the proof types
 * where we start with an assumption of truth of the clause, the predicate
 * operator could not return NULL either, so it doesn't matter whether we are
 * trying to make a strong or weak proof.  For weak implication, it could be
 * that the clause operator returned NULL, but then the predicate operator
 * would as well, so that the weak implication still holds.  This argument
 * doesn't apply in the case where we are considering two different constant
 * values, since then the operators aren't being given identical inputs.  But
 * we only support that for btree operators, for which we can assume that all
 * non-null inputs result in non-null outputs, so that it doesn't matter which
 * two non-null constants we consider.  Currently the code below just reports
 * "proof failed" if either constant is NULL, but in some cases we could be
 * smarter (and that likely would require checking strong vs. weak proofs).
 *
 */
static bool
btree_predicate_proof(Expr *predicate, Node *clause, bool refute_it, bool weak)
{
	Node	   *leftop,
			   *rightop;
	Node	   *pred_var,
			   *clause_var;
	Const	   *pred_const,
			   *clause_const;
	bool		pred_var_on_left,
				clause_var_on_left;
	Oid			pred_collation,
				clause_collation;
	Oid			pred_op,
				clause_op,
				test_op;
	Expr	   *test_expr;
	ExprState  *test_exprstate;
	Datum		test_result;
	bool		isNull;
	EState	   *estate;
	MemoryContext oldcontext;

	/*
	 * Both expressions must be binary opclauses with a Const on one side, and
	 * identical subexpressions on the other sides. Note we don't have to
	 * think about binary relabeling of the Const node, since that would have
	 * been folded right into the Const.
	 *
	 * If either Const is null, we also fail right away; this assumes that the
	 * test operator will always be strict.
	 */
	if (!is_opclause(predicate))
		return false;
	leftop = get_leftop(predicate);
	rightop = get_rightop(predicate);
	if (rightop == NULL)
		return false;			/* not a binary opclause */
	if (IsA(rightop, Const))
	{
		pred_var = leftop;
		pred_const = (Const *) rightop;
		pred_var_on_left = true;
	}
	else if (IsA(leftop, Const))
	{
		pred_var = rightop;
		pred_const = (Const *) leftop;
		pred_var_on_left = false;
	}
	else
		return false;			/* no Const to be found */

	if (!is_opclause(clause))
		return false;
	leftop = get_leftop((Expr *) clause);
	rightop = get_rightop((Expr *) clause);
	if (rightop == NULL)
		return false;			/* not a binary opclause */
	if (IsA(rightop, Const))
	{
		clause_var = leftop;
		clause_const = (Const *) rightop;
		clause_var_on_left = true;
	}
	else if (IsA(leftop, Const))
	{
		clause_var = rightop;
		clause_const = (Const *) leftop;
		clause_var_on_left = false;
	}
	else
		return false;			/* no Const to be found */

	/*
	 * Check for matching subexpressions on the non-Const sides.  We used to
	 * only allow a simple Var, but it's about as easy to allow any
	 * expression.  Remember we already know that the pred expression does not
	 * contain any non-immutable functions, so identical expressions should
	 * yield identical results.
	 */
	if (!equal(pred_var, clause_var))
		return false;

	/*
	 * They'd better have the same collation, too.
	 */
	pred_collation = ((OpExpr *) predicate)->inputcollid;
	clause_collation = ((OpExpr *) clause)->inputcollid;
	if (pred_collation != clause_collation)
		return false;

	/*
	 * Okay, get the operators in the two clauses we're comparing. Commute
	 * them if needed so that we can assume the variables are on the left.
	 */
	pred_op = ((OpExpr *) predicate)->opno;
	if (!pred_var_on_left)
	{
		pred_op = get_commutator(pred_op);
		if (!OidIsValid(pred_op))
			return false;
	}

	clause_op = ((OpExpr *) clause)->opno;
	if (!clause_var_on_left)
	{
		clause_op = get_commutator(clause_op);
		if (!OidIsValid(clause_op))
			return false;
	}

	if (!equal(pred_const, clause_const) &&
		clause_const->constisnull)
	{
		/* If clause_op isn't strict, we can't prove anything */
		if (!op_strict(clause_op))
			return false;

		/*
		 * At this point we know that the clause returns NULL.  For proof
		 * types that assume truth of the clause, this means the proof is
		 * vacuously true (a/k/a "false implies anything").  That's all proof
		 * types except weak implication.
		 */
		if (!(weak && !refute_it))
			return true;

		/*
		 * For weak implication, it's still possible for the proof to succeed,
		 * if the predicate can also be proven NULL.  In that case we've got
		 * NULL => NULL which is valid for this proof type.
		 */
		if (pred_const->constisnull && op_strict(pred_op))
			return true;
		/* Else the proof fails */
		return false;
	}

	if (!equal(pred_const, clause_const) &&
		pred_const->constisnull)
	{
		/*
		 * If the pred_op is strict, we know the predicate yields NULL, which
		 * means the proof succeeds for either weak implication or weak
		 * refutation.
		 */
		if (weak && op_strict(pred_op))
			return true;
		/* Else the proof fails */
		return false;
	}

	/*
	 * Lookup the comparison operator using the system catalogs and the
	 * operator implication tables.
	 */
	test_op = get_btree_test_op(pred_op, clause_op, refute_it);

	if (!OidIsValid(test_op))
	{
		/* couldn't find a suitable comparison operator */
		return false;
	}

	/*
	 * Evaluate the test.  For this we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Build expression tree */
	test_expr = make_opclause(test_op,
							  BOOLOID,
							  false,
							  (Expr *) pred_const,
							  (Expr *) clause_const,
							  InvalidOid,
							  pred_collation);

	/* Fill in opfuncids */
	fix_opfuncids((Node *) test_expr);

	/* Prepare it for execution */
	test_exprstate = ExecInitExpr(test_expr, NULL);

	/* And execute it. */
	test_result = ExecEvalExprSwitchContext(test_exprstate,
											GetPerTupleExprContext(estate),
											&isNull, NULL);

	/* Get back to outer memory context */
	MemoryContextSwitchTo(oldcontext);

	/* Release all the junk we just created */
	FreeExecutorState(estate);

	if (isNull)
	{
		/* Treat a null result as non-proof ... but it's a tad fishy ... */
		elog(DEBUG2, "null predicate test result");
		return false;
	}
	return DatumGetBool(test_result);
}


/*
 * We use a lookaside table to cache the result of btree proof operator
 * lookups, since the actual lookup is pretty expensive and doesn't change
 * for any given pair of operators (at least as long as pg_amop doesn't
 * change).  A single hash entry stores both positive and negative results
 * for a given pair of operators.
 */
typedef struct OprProofCacheKey
{
	Oid			pred_op;		/* predicate operator */
	Oid			clause_op;		/* clause operator */
} OprProofCacheKey;

typedef struct OprProofCacheEntry
{
	/* the hash lookup key MUST BE FIRST */
	OprProofCacheKey key;

	bool		have_implic;	/* do we know the implication result? */
	bool		have_refute;	/* do we know the refutation result? */
	Oid			implic_test_op; /* OID of the operator, or 0 if none */
	Oid			refute_test_op; /* OID of the operator, or 0 if none */
} OprProofCacheEntry;

static HTAB *OprProofCacheHash = NULL;


/*
 * get_btree_test_op
 *	  Identify the comparison operator needed for a btree-operator
 *	  proof or refutation.
 *
 * Given the truth of a predicate "var pred_op const1", we are attempting to
 * prove or refute a clause "var clause_op const2".  The identities of the two
 * operators are sufficient to determine the operator (if any) to compare
 * const2 to const1 with.
 *
 * Returns the OID of the operator to use, or InvalidOid if no proof is
 * possible.
 */
static Oid
get_btree_test_op(Oid pred_op, Oid clause_op, bool refute_it)
{
	OprProofCacheKey key;
	OprProofCacheEntry *cache_entry;
	bool		cfound;
	Oid			test_op = InvalidOid;
	bool		found = false;
	List	   *pred_op_infos,
			   *clause_op_infos;
	ListCell   *lcp,
			   *lcc;

	/*
	 * Find or make a cache entry for this pair of operators.
	 */
	if (OprProofCacheHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(OprProofCacheKey);
		ctl.entrysize = sizeof(OprProofCacheEntry);
		ctl.hash = tag_hash;
		OprProofCacheHash = hash_create("Btree proof lookup cache", 256,
										&ctl, HASH_ELEM | HASH_FUNCTION);

		/* Arrange to flush cache on pg_amop changes */
		CacheRegisterSyscacheCallback(AMOPOPID,
									  InvalidateOprProofCacheCallBack,
									  (Datum) 0);
	}

	key.pred_op = pred_op;
	key.clause_op = clause_op;
	cache_entry = (OprProofCacheEntry *) hash_search(OprProofCacheHash,
													 (void *) &key,
													 HASH_ENTER, &cfound);
	if (!cfound)
	{
		/* new cache entry, set it invalid */
		cache_entry->have_implic = false;
		cache_entry->have_refute = false;
	}
	else
	{
		/* pre-existing cache entry, see if we know the answer */
		if (refute_it)
		{
			if (cache_entry->have_refute)
				return cache_entry->refute_test_op;
		}
		else
		{
			if (cache_entry->have_implic)
				return cache_entry->implic_test_op;
		}
	}

	/*
	 * Try to find a btree opfamily containing the given operators.
	 *
	 * We must find a btree opfamily that contains both operators, else the
	 * implication can't be determined.  Also, the opfamily must contain a
	 * suitable test operator taking the operators' righthand datatypes.
	 *
	 * If there are multiple matching opfamilies, assume we can use any one to
	 * determine the logical relationship of the two operators and the correct
	 * corresponding test operator.  This should work for any logically
	 * consistent opfamilies.
	 */
	clause_op_infos = get_op_btree_interpretation(clause_op);
	if (clause_op_infos)
		pred_op_infos = get_op_btree_interpretation(pred_op);
	else	/* no point in looking */
		pred_op_infos = NIL;

	foreach(lcp, pred_op_infos)
	{
		OpBtreeInterpretation *pred_op_info = lfirst(lcp);
		Oid			opfamily_id = pred_op_info->opfamily_id;

		foreach(lcc, clause_op_infos)
		{
			OpBtreeInterpretation *clause_op_info = lfirst(lcc);
			StrategyNumber pred_strategy,
						clause_strategy,
						test_strategy;

			/* Must find them in same opfamily */
			if (opfamily_id != clause_op_info->opfamily_id)
				continue;
			/* Lefttypes should match */
			Assert(clause_op_info->oplefttype == pred_op_info->oplefttype);

			pred_strategy = pred_op_info->strategy;
			clause_strategy = clause_op_info->strategy;

			/*
			 * Look up the "test" strategy number in the implication table
			 */
			if (refute_it)
				test_strategy = BT_refute_table[clause_strategy - 1][pred_strategy - 1];
			else
				test_strategy = BT_implic_table[clause_strategy - 1][pred_strategy - 1];

			if (test_strategy == 0)
			{
				/* Can't determine implication using this interpretation */
				continue;
			}

			/*
			 * See if opfamily has an operator for the test strategy and the
			 * datatypes.
			 */
			if (test_strategy == BTNE)
			{
				test_op = get_opfamily_member(opfamily_id,
											  pred_op_info->oprighttype,
											  clause_op_info->oprighttype,
											  BTEqualStrategyNumber);
				if (OidIsValid(test_op))
					test_op = get_negator(test_op);
			}
			else
			{
				test_op = get_opfamily_member(opfamily_id,
											  pred_op_info->oprighttype,
											  clause_op_info->oprighttype,
											  test_strategy);
			}

			if (!OidIsValid(test_op))
				continue;

			/*
			 * Last check: test_op must be immutable.
			 *
			 * Note that we require only the test_op to be immutable, not the
			 * original clause_op.  (pred_op is assumed to have been checked
			 * immutable by the caller.)  Essentially we are assuming that the
			 * opfamily is consistent even if it contains operators that are
			 * merely stable.
			 */
			if (op_volatile(test_op) == PROVOLATILE_IMMUTABLE)
			{
				found = true;
				break;
			}
		}

		if (found)
			break;
	}

	list_free_deep(pred_op_infos);
	list_free_deep(clause_op_infos);

	if (!found)
	{
		/* couldn't find a suitable comparison operator */
		test_op = InvalidOid;
	}

	/* Cache the result, whether positive or negative */
	if (refute_it)
	{
		cache_entry->refute_test_op = test_op;
		cache_entry->have_refute = true;
	}
	else
	{
		cache_entry->implic_test_op = test_op;
		cache_entry->have_implic = true;
	}

	return test_op;
}


/*
 * Callback for pg_amop inval events
 */
static void
InvalidateOprProofCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	OprProofCacheEntry *hentry;

	Assert(OprProofCacheHash != NULL);

	/* Currently we just reset all entries; hard to be smarter ... */
	hash_seq_init(&status, OprProofCacheHash);

	while ((hentry = (OprProofCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		hentry->have_implic = false;
		hentry->have_refute = false;
	}
}

/**
 * Process an AND clause -- this can do a INTERSECTION between sets learned from child clauses
 */
static PossibleValueSet
ProcessAndClauseForPossibleValues( PredIterInfoData *clauseInfo, Node *clause, Node *variable, Oid opfamily)
{
	PossibleValueSet result;

	InitPossibleValueSetData(&result);

	iterate_begin(child, clause, *clauseInfo)
	{
		PossibleValueSet childPossible = DeterminePossibleValueSet( child, variable, opfamily );
		if ( childPossible.isAnyValuePossible)
		{
			/* any value possible, this AND member does not add any information */
			DeletePossibleValueSetData( &childPossible);
		}
		else
		{
			/* a particular set so this AND member can refine our estimate */
			if ( result.isAnyValuePossible )
			{
				/* current result was not informative so just take the child */
				result = childPossible;
			}
			else
			{
				/* result.set AND childPossible.set: do intersection inside result */
				RemoveUnmatchingValues( &result, &childPossible );
				DeletePossibleValueSetData( &childPossible);
			}
		}
	}
	iterate_end(*clauseInfo);

	return result;
}

/**
 * Process an OR clause -- this can do a UNION between sets learned from child clauses
 */
static PossibleValueSet
ProcessOrClauseForPossibleValues( PredIterInfoData *clauseInfo, Node *clause, Node *variable, Oid opfamily)
{
	PossibleValueSet result;
	InitPossibleValueSetData(&result);

	iterate_begin(child, clause, *clauseInfo)
	{
		PossibleValueSet childPossible = DeterminePossibleValueSet( child, variable, opfamily );
		if ( childPossible.isAnyValuePossible)
		{
			/* any value is possible for the entire AND */
			DeletePossibleValueSetData( &childPossible );
			DeletePossibleValueSetData( &result );

			/* it can't improve once a part of the OR accepts all, so just quit */
			result.isAnyValuePossible = true;
			break;
		}

		if ( result.isAnyValuePossible )
		{
			/* first one in loop so just take it */
			result = childPossible;
		}
		else
		{
			/* result.set OR childPossible.set --> do union into result */
			AddUnmatchingValues( &result, &childPossible );
			DeletePossibleValueSetData( &childPossible);
		}
	}
	iterate_end(*clauseInfo);

	return result;
}


/**
 *
 * Get the possible values of variable, as determined by the given qualification clause
 *
 * Note that only variables whose type is greengageDbHashtable will return an actual finite set of values.  All others
 *    will go to the default behavior -- return that any value is possible
 *
 * Note that if there are two variables to check, you must call this twice.  This then means that
 *    if the two variables are dependent you won't learn of that -- you only know that the set of
 *    possible values is within the cross-product of the two variables' sets
 */
PossibleValueSet
DeterminePossibleValueSet(Node *clause, Node *variable, Oid opfamily)
{
	PredIterInfoData clauseInfo;
	PossibleValueSet result;

	if ( clause == NULL )
	{
		InitPossibleValueSetData(&result);
		return result;
	}

	switch (predicate_classify(clause, &clauseInfo))
	{
		case CLASS_AND:
			return ProcessAndClauseForPossibleValues(&clauseInfo, clause, variable, opfamily);
		case CLASS_OR:
			return ProcessOrClauseForPossibleValues(&clauseInfo, clause, variable, opfamily);
		case CLASS_ATOM:
			if (TryProcessExprForPossibleValues(clause, variable, opfamily, &result))
			{
				return result;
			}
			/* can't infer anything, so return that any value is possible */
			InitPossibleValueSetData(&result);
			return result;
	}


	/* can't get here */
	elog(ERROR, "predicate_classify returned a bad value");
	return result;
}
