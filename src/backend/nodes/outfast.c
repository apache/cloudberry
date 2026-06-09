/*-------------------------------------------------------------------------
 *
 * outfast.c
 *	  Fast serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * NOTES
 *	  Every node type that can appear in an Greenplum Database serialized query or plan
 *    tree must have an output function defined here.
 *
 * 	  There *MUST* be a one-to-one correspondence between this routine
 *    and readfast.c.  If not, you will likely crash the system.
 *
 *     By design, the only user of these routines is the function
 *     serializeNode in cdbsrlz.c.  Other callers beware.
 *
 *    Like readfast.c, this file borrows the definitions of most functions
 *    from outfuncs.c.
 *
 * 	  Rather than serialize to a (somewhat human-readable) string, these
 *    routines create a binary serialization via a simple depth-first walk
 *    of the tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "cdb/cdbgang.h"
#include "utils/workfile_mgr.h"
#include "parser/parsetree.h"

/*
 * Macros to simplify output of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/*
 * Write the label for the node type.  nodelabel is accepted for
 * compatibility with outfuncs.c, but is ignored
 */
#define WRITE_NODE_TYPE(nodelabel) \
	{ int16 nt =nodeTag(node); appendBinaryStringInfo(str, (const char *)&nt, sizeof(int16)); }

/* Write an integer field  */
#define WRITE_INT_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/* Write an integer field  */
#define WRITE_INT8_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int8)); }

/* Write an integer field  */
#define WRITE_INT16_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int16)); }

/* Write an unsigned integer field */
#define WRITE_UINT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int))

/* Write an uint64 field */
#define WRITE_UINT64_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(uint64))

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(Oid))

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(long))

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendBinaryStringInfo(str, &node->fldname, 1)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	{ int16 en=node->fldname; appendBinaryStringInfo(str, (const char *)&en, sizeof(int16)); }

/* Write a float field --- the format is accepted but ignored (for compat with outfuncs.c)  */
#define WRITE_FLOAT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(double))

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	{ \
		char b = node->fldname ? 1 : 0; \
		appendBinaryStringInfo(str, (const char *)&b, 1); }

/* Write a character-string (possibly NULL) varable */
#define WRITE_STRING_VAR(var) \
	{ int slen = var != NULL ? strlen(var) : 0; \
		appendBinaryStringInfo(str, (const char *)&slen, sizeof(int)); \
		if (slen>0) appendBinaryStringInfo(str, var, slen);}

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname)  WRITE_STRING_VAR(node->fldname)

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/*
 * Write a Node field
 *
 * If compiled with GP_SERIALIZATION_DEBUG, write the field name in the
 * serialized form, and check that it matches in the read function
 * (READ_NODE_FIELD in readfast.c). That makes it much easier to debug bugs
 * where the out and read functions are not in sync, as you get an error
 * much earlier, and it can print the field name where the mismatch occurred.
 * It makes the serialized plans much larger, though, so we don't want to do
 * it production.
 */
#ifdef GP_SERIALIZATION_DEBUG
#define WRITE_NODE_FIELD(fldname) \
	do { \
		const char *xx = CppAsString(fldname); \
		appendBinaryStringInfo(str, xx, strlen(xx) + 1); \
		(_outNode(str, node->fldname)); \
	} while (0)
#else
#define WRITE_NODE_FIELD(fldname) \
	(_outNode(str, node->fldname))
#endif


/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	 _outBitmapset(str, node->fldname)

/* Write a binary field */
#define WRITE_BINARY_FIELD(fldname, sz) \
{ appendBinaryStringInfo(str, (const char *) &node->fldname, (sz)); }

/* Write a bytea field */
#define WRITE_BYTEA_FIELD(fldname) \
	(_outDatum(str, PointerGetDatum(node->fldname), -1, false))

/* Write a dummy field -- value not displayable or copyable */
#define WRITE_DUMMY_FIELD(fldname) \
	{ /*int * dummy = 0; appendBinaryStringInfo(str,(const char *)&dummy, sizeof(int *)) ;*/ }

	/* Read an integer array */
#define WRITE_INT_ARRAY(fldname, count) \
	StaticAssertStmt(sizeof(node->fldname[0]) == sizeof(int32), \
					 "WRITE_INT_ARRAY() used on wrong array type"); \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(int32)); \
		} \
	}

/* Write a boolean array  */
#define WRITE_BOOL_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			char b = node->fldname[i] ? 1 : 0;								\
			appendBinaryStringInfo(str, (const char *)&b, 1); \
		} \
	}

/* Write an Trasnaction ID array  */
#define WRITE_XID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(TransactionId)); \
		} \
	}

/* Write an AttrNumber array  */
#define WRITE_ATTRNUMBER_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(AttrNumber)); \
		} \
	}

/* Write an Oid array  */
#define WRITE_OID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Oid)); \
		} \
	}

/* Write an Oid array  */
#define WRITE_INDEX_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Index)); \
		} \
	}

static void _outNode(StringInfo str, void *obj);


static void
_outList(StringInfo str, List *node)
{
	ListCell   *lc;

	if (node == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
		return;
	}

	WRITE_NODE_TYPE("");
    WRITE_INT_FIELD(length);

	foreach(lc, node)
	{

		if (IsA(node, List))
		{
			_outNode(str, lfirst(lc));
		}
		else if (IsA(node, IntList))
		{
			int n = lfirst_int(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(int));
		}
		else if (IsA(node, OidList))
		{
			Oid n = lfirst_oid(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(Oid));
		}
	}
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Currently bitmapsets do not appear in any node type that is stored in
 * rules, so there is no support in readfast.c for reading this format.
 */
static void
_outBitmapset(StringInfo str, const Bitmapset *bms)
{
	int			i;
	int			nwords = 0;

	if (bms)
		nwords = bms->nwords;

	appendBinaryStringInfo(str, (char *) &nwords, sizeof(int));
	for (i = 0; i < nwords; i++)
	{
		appendBinaryStringInfo(str, (char *) &bms->words[i], sizeof(bitmapword));
	}

}

/*
 * Print the value of a Datum given its type.
 */
static void
_outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length;
	char	   *s;

	if (typbyval)
	{
		s = (char *) (&value);
		appendBinaryStringInfo(str, s, sizeof(Datum));
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
		{
			length = 0;
			appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
		}
		else
		{
			if (typlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(s))
			{
				ExpandedObjectHeader *eoh = DatumGetEOHP(value);
				Size		resultsize;
				char		*resultptr;

				resultsize = EOH_get_flat_size(eoh);
				resultptr = (char *) palloc(resultsize);
				EOH_flatten_into(eoh, (void *) resultptr, resultsize);
				appendBinaryStringInfo(str, (char *)&resultsize, sizeof(Size));
				appendBinaryStringInfo(str, resultptr, resultsize);
			}
			else
			{
				length = datumGetSize(value, typbyval, typlen);
				appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
				appendBinaryStringInfo(str, s, length);
			}
		}
	}
}

static void
_outCdbPathLocus(StringInfo str, const CdbPathLocus *node)
{
	WRITE_ENUM_FIELD(locustype, CdbLocusType);
	WRITE_NODE_FIELD(distkey);
	WRITE_INT_FIELD(numsegments);
}


static void
_outInteger(StringInfo str, const Integer *node)
{
	int16 vt = T_Integer;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	appendBinaryStringInfo(str, (const char *)&node->ival, sizeof(int));
}


static void
_outFloat(StringInfo str, const Float *node)
{
	int16 vt = T_Float;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->fval != NULL ? strlen(node->fval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->fval, slen);
}

static void
_outBoolean(StringInfo str, const Boolean *node)
{
	int16 vt = T_Boolean;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	appendBinaryStringInfo(str, (const char *)&node->boolval, sizeof(bool));
}


static void
_outString(StringInfo str, const String *node)
{
	int16 vt = T_String;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->sval != NULL ? strlen(node->sval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->sval, slen);
}

static void
_outBitString(StringInfo str, const BitString *node)
{
	int16 vt = T_BitString;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->bsval != NULL ? strlen(node->bsval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->bsval, slen);
}


static void
_outA_Const(StringInfo str, A_Const *node)
{
	WRITE_NODE_TYPE("A_CONST");
	WRITE_BOOL_FIELD(isnull);
	if (!node->isnull)
		_outNode(str, &node->val);
	WRITE_LOCATION_FIELD(location);
}


static void
_outConst(StringInfo str, Const *node)
{
	WRITE_NODE_TYPE("CONST");

	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	if (!node->constisnull)
		_outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outBoolExpr(StringInfo str, BoolExpr *node)
{
	WRITE_NODE_TYPE("BOOLEXPR");
	WRITE_ENUM_FIELD(boolop, BoolExprType);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}


static void
_outA_Expr(StringInfo str, A_Expr *node)
{
	WRITE_NODE_TYPE("AEXPR");
	WRITE_ENUM_FIELD(kind, A_Expr_Kind);

	switch (node->kind)
	{
		case AEXPR_OP:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			WRITE_NODE_FIELD(name);
			break;

		case AEXPR_IN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;

		default:

			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}


static void
_outColumnDef(StringInfo str, const ColumnDef *node)
{
	WRITE_NODE_TYPE("COLUMNDEF");

	WRITE_STRING_FIELD(colname);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(compression);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_local);
	WRITE_BOOL_FIELD(is_not_null);
	WRITE_BOOL_FIELD(is_from_type);
	WRITE_INT_FIELD(attnum);
	WRITE_INT_FIELD(storage);
	WRITE_STRING_FIELD(storage_name);
	WRITE_NODE_FIELD(raw_default);
	WRITE_NODE_FIELD(cooked_default);

	WRITE_BOOL_FIELD(hasCookedMissingVal);
	WRITE_BOOL_FIELD(missingIsNull);
	if (node->hasCookedMissingVal && !node->missingIsNull)
		outDatum(str, node->missingVal, -1, false);

	WRITE_CHAR_FIELD(identity);
	WRITE_NODE_FIELD(identitySequence);
	WRITE_CHAR_FIELD(generated);
	WRITE_NODE_FIELD(collClause);
	WRITE_OID_FIELD(collOid);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(fdwoptions);
	WRITE_LOCATION_FIELD(location);
}


static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
	WRITE_NODE_TYPE("RTE");

	/* put alias + eref first to make dump more legible */
	WRITE_NODE_FIELD(alias);
	WRITE_NODE_FIELD(eref);
	WRITE_ENUM_FIELD(rtekind, RTEKind);
	WRITE_BOOL_FIELD(relisivm);

	switch (node->rtekind)
	{
		case RTE_RELATION:
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			WRITE_NODE_FIELD(tablesample);
			WRITE_UINT_FIELD(perminfoindex);
			break;
		case RTE_SUBQUERY:
			WRITE_NODE_FIELD(subquery);
			WRITE_BOOL_FIELD(security_barrier);
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			WRITE_UINT_FIELD(perminfoindex);
			break;
		case RTE_JOIN:
			WRITE_ENUM_FIELD(jointype, JoinType);
			WRITE_INT_FIELD(joinmergedcols);
			WRITE_NODE_FIELD(joinaliasvars);
			WRITE_NODE_FIELD(joinleftcols);
			WRITE_NODE_FIELD(joinrightcols);
			WRITE_NODE_FIELD(join_using_alias);
			break;
		case RTE_FUNCTION:
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNCTION:
			WRITE_NODE_FIELD(subquery);
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			WRITE_NODE_FIELD(tablefunc);
			break;
		case RTE_VALUES:
			WRITE_NODE_FIELD(values_lists);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			WRITE_STRING_FIELD(ctename);
			WRITE_UINT_FIELD(ctelevelsup);
			WRITE_BOOL_FIELD(self_reference);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			WRITE_STRING_FIELD(enrname);
			WRITE_FLOAT_FIELD(enrtuples);
			WRITE_OID_FIELD(relid);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_RESULT:
			/* no extra fields */
			break;
        case RTE_VOID:                                                  /*CDB*/
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) node->rtekind);
			break;
	}

	WRITE_BOOL_FIELD(lateral);
	WRITE_BOOL_FIELD(inh);
	WRITE_BOOL_FIELD(inFromCl);
	WRITE_NODE_FIELD(securityQuals);

	WRITE_BOOL_FIELD(forceDistRandom);
}


static void
_outConstraint(StringInfo str, const Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_ENUM_FIELD(contype, ConstrType);
	/* name, or NULL if unnamed */
	WRITE_STRING_FIELD(conname);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	WRITE_BOOL_FIELD(is_no_inherit);
	WRITE_NODE_FIELD(raw_expr);
	WRITE_STRING_FIELD(cooked_expr);
	WRITE_CHAR_FIELD(generated_when);
	WRITE_BOOL_FIELD(nulls_not_distinct);

	WRITE_NODE_FIELD(keys);
	WRITE_NODE_FIELD(including);

	WRITE_NODE_FIELD(exclusions);

	WRITE_NODE_FIELD(options);
	WRITE_STRING_FIELD(indexname);
	WRITE_STRING_FIELD(indexspace);
	WRITE_BOOL_FIELD(reset_default_tblspc);

	WRITE_STRING_FIELD(access_method);
	WRITE_NODE_FIELD(where_clause);

	WRITE_NODE_FIELD(pktable);
	WRITE_NODE_FIELD(fk_attrs);
	WRITE_NODE_FIELD(pk_attrs);
	WRITE_CHAR_FIELD(fk_matchtype);
	WRITE_CHAR_FIELD(fk_upd_action);
	WRITE_CHAR_FIELD(fk_del_action);
	WRITE_NODE_FIELD(old_conpfeqop);
	WRITE_OID_FIELD(old_pktable_oid);

	WRITE_BOOL_FIELD(skip_validation);
	WRITE_BOOL_FIELD(initially_valid);
}


static void
_outPlannerGlobal(StringInfo str, const PlannerGlobal *node)
{
	WRITE_NODE_TYPE("PLANNERGLOBAL");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(finalrtable);
	WRITE_NODE_FIELD(finalrowmarks);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(appendRelations);
	WRITE_NODE_FIELD(relationOids);
	WRITE_NODE_FIELD(invalItems);
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_UINT_FIELD(lastPHId);
	WRITE_UINT_FIELD(lastRowMarkId);
	WRITE_INT_FIELD(lastPlanNodeId);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(oneoffPlan);
	WRITE_NODE_FIELD(share.motStack);
	WRITE_BITMAPSET_FIELD(share.qdShares);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeOK);
	WRITE_BOOL_FIELD(parallelModeNeeded);
	WRITE_CHAR_FIELD(maxParallelHazard);
}


static void
_outPlannerInfo(StringInfo str, const PlannerInfo *node)
{
	WRITE_NODE_TYPE("PLANNERINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(parse);
	WRITE_NODE_FIELD(glob);
	WRITE_UINT_FIELD(query_level);
	WRITE_NODE_FIELD(plan_params);
	WRITE_BITMAPSET_FIELD(outer_params);
	WRITE_INT_FIELD(simple_rel_array_size);
	WRITE_BITMAPSET_FIELD(all_baserels);
	WRITE_BITMAPSET_FIELD(outer_join_rels);
	WRITE_NODE_FIELD(join_rel_list);
	WRITE_NODE_FIELD(join_rel_list);
	WRITE_BOOL_FIELD(setup_agg_pushdown);
	WRITE_NODE_FIELD(grouped_rel_info_list);
	WRITE_INT_FIELD(join_cur_level);
	WRITE_NODE_FIELD(init_plans);
	WRITE_NODE_FIELD(cte_plan_ids);
	WRITE_NODE_FIELD(multiexpr_params);
	WRITE_NODE_FIELD(join_domains);
	WRITE_NODE_FIELD(eq_classes);
	WRITE_BOOL_FIELD(ec_merging_done);
	WRITE_NODE_FIELD(canon_pathkeys);
	WRITE_NODE_FIELD(left_join_clauses);
	WRITE_NODE_FIELD(right_join_clauses);
	WRITE_NODE_FIELD(full_join_clauses);
	WRITE_NODE_FIELD(join_info_list);
	WRITE_INT_FIELD(last_rinfo_serial);
	WRITE_BITMAPSET_FIELD(all_result_relids);
	WRITE_BITMAPSET_FIELD(leaf_result_relids);
	WRITE_NODE_FIELD(append_rel_list);
	WRITE_NODE_FIELD(row_identity_vars);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(placeholder_list);
	WRITE_NODE_FIELD(grouped_var_list);
	WRITE_NODE_FIELD(fkey_list);
	WRITE_NODE_FIELD(query_pathkeys);
	WRITE_NODE_FIELD(group_pathkeys);
	WRITE_INT_FIELD(num_groupby_pathkeys);
	WRITE_NODE_FIELD(window_pathkeys);
	WRITE_NODE_FIELD(distinct_pathkeys);
	WRITE_NODE_FIELD(sort_pathkeys);
	WRITE_NODE_FIELD(processed_groupClause);
	WRITE_NODE_FIELD(processed_distinctClause);
	WRITE_NODE_FIELD(processed_tlist);
	WRITE_INT_FIELD(max_sortgroupref);
	WRITE_NODE_FIELD(update_colnos);
	WRITE_NODE_FIELD(minmax_aggs);
	WRITE_FLOAT_FIELD(total_table_pages);
	WRITE_FLOAT_FIELD(tuple_fraction);
	WRITE_FLOAT_FIELD(limit_tuples);
	WRITE_UINT_FIELD(qual_security_level);
	WRITE_BOOL_FIELD(hasJoinRTEs);
	WRITE_BOOL_FIELD(hasLateralRTEs);
	WRITE_BOOL_FIELD(hasHavingQual);
	WRITE_BOOL_FIELD(hasPseudoConstantQuals);
	WRITE_BOOL_FIELD(hasAlternativeSubPlans);
	WRITE_BOOL_FIELD(placeholdersFrozen);
	WRITE_BOOL_FIELD(hasRecursion);
	WRITE_NODE_FIELD(agginfos);
	WRITE_NODE_FIELD(aggtransinfos);
	WRITE_INT_FIELD(numOrderedAggs);
	WRITE_BOOL_FIELD(hasNonPartialAggs);
	WRITE_BOOL_FIELD(hasNonSerialAggs);
	WRITE_INT_FIELD(wt_param_id);
	WRITE_NODE_FIELD(non_recursive_path);
	WRITE_BITMAPSET_FIELD(curOuterRels);
	WRITE_NODE_FIELD(curOuterParams);
	WRITE_BOOL_FIELD(partColsUpdated);
}


static void
_outForeignKeyOptInfo(StringInfo str, const ForeignKeyOptInfo *node)
{
	WRITE_NODE_TYPE("FOREIGNKEYOPTINFO");

	WRITE_UINT_FIELD(con_relid);
	WRITE_UINT_FIELD(ref_relid);
	WRITE_INT_FIELD(nkeys);
	WRITE_ATTRNUMBER_ARRAY(conkey, node->nkeys);
	WRITE_ATTRNUMBER_ARRAY(confkey, node->nkeys);
	WRITE_OID_ARRAY(conpfeqop, node->nkeys);
	WRITE_INT_FIELD(nmatched_ec);
	WRITE_INT_FIELD(nconst_ec);
	WRITE_INT_FIELD(nmatched_rcols);
	WRITE_INT_FIELD(nmatched_ri);
}


static void
_outEquivalenceClass(StringInfo str, const EquivalenceClass *node)
{
	/*
	 * To simplify reading, we just chase up to the topmost merged EC and
	 * print that, without bothering to show the merge-ees separately.
	 */
	while (node->ec_merged)
		node = node->ec_merged;

	WRITE_NODE_TYPE("EQUIVALENCECLASS");

	WRITE_NODE_FIELD(ec_opfamilies);
	WRITE_OID_FIELD(ec_collation);
	WRITE_NODE_FIELD(ec_members);
	WRITE_NODE_FIELD(ec_sources);
	WRITE_NODE_FIELD(ec_derives);
	WRITE_BITMAPSET_FIELD(ec_relids);
	WRITE_BOOL_FIELD(ec_has_const);
	WRITE_BOOL_FIELD(ec_has_volatile);
	WRITE_BOOL_FIELD(ec_broken);
	WRITE_UINT_FIELD(ec_sortref);
	WRITE_UINT_FIELD(ec_min_security);
	WRITE_UINT_FIELD(ec_max_security);
}

static void
_outExtensibleNode(StringInfo str, const ExtensibleNode *node)
{
	const ExtensibleNodeMethods *methods;

	methods = GetExtensibleNodeMethods(node->extnodename, false);

	WRITE_NODE_TYPE("EXTENSIBLENODE");

	WRITE_STRING_FIELD(extnodename);

	/* serialize the private fields */
	methods->nodeOut(str, node);
}

#define outDatum(str, value, typlen, typbyval) _outDatum(str, value, typlen, typbyval)

#include "outfast.funcs.c"

/*
 * _outNode -
 *	  converts a Node into binary string and append it to 'str'
 */
static void
_outNode(StringInfo str, void *obj)
{
	if (obj == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
	}
	else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
		_outList(str, obj);
	else if (IsA(obj, Integer))
		_outInteger(str, (Integer *) obj);
	else if (IsA(obj, Float))
		_outFloat(str, (Float *) obj);
	else if (IsA(obj, Boolean))
		_outBoolean(str, (Boolean *) obj);
	else if (IsA(obj, String))
		_outString(str, (String *) obj);
	else if (IsA(obj, BitString))
		_outBitString(str, (BitString *) obj);
	else if (IsA(obj, Bitmapset))
		outBitmapset(str, (Bitmapset *) obj);
	else
	{
		switch (nodeTag(obj))
		{
#include "outfast.switch.c"

			default:
				elog(ERROR, "could not serialize unrecognized node type: %d",
						 (int) nodeTag(obj));
				break;
		}
	}
}

/*
 * nodeToBinaryStringFast -
 *	   returns a binary representation of the Node as a palloc'd string
 */
char *
nodeToBinaryStringFast(void *obj, int *length)
{
	StringInfoData str;
	int16 tg = (int16) 0xDEAD;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfoOfSize(&str, 4096);

	_outNode(&str, obj);

	/* Add something special at the end that we can check in readfast.c */
	appendBinaryStringInfo(&str, (const char *)&tg, sizeof(int16));

	*length = str.len;
	return str.data;
}
