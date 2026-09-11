/*-------------------------------------------------------------------------
 *
 * outfuncs.c
 *	  Output functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/outfuncs.c
 *
 * NOTES
 *	  Every node type that can appear in stored rules' parsetrees *must*
 *	  have an output function defined here (as well as an input function
 *	  in readfuncs.c).  In addition, plan nodes should have input and
 *	  output functions so that they can be sent to parallel workers.
 *
 *	  For use in debugging, we also provide output functions for nodes
 *	  that appear in raw parsetrees and planner Paths.  These node types
 *	  need not have input functions.  Output support for raw parsetrees
 *	  is somewhat incomplete, too; in particular, utility statements are
 *	  almost entirely unsupported.  We try to support everything that can
 *	  appear in a raw SELECT, though.
 *
 *    N.B. Faster variants of these functions (producing illegible output)
 *         are supplied in outfast.c for use in Greenplum Database serialization.  The
 *         function in this file are intended to produce legible output.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "access/attnum.h"
#include "common/shortest_dec.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/altertablenodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/datum.h"

#include "cdb/cdbpathlocus.h"

static void outChar(StringInfo str, char c);
static void outDouble(StringInfo str, double d);


/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, nodelabel)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an unsigned integer field (anything written with UINT64_FORMAT) */
#define WRITE_UINT64_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " " UINT64_FORMAT, \
					 node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outChar(str, node->fldname))

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field (actually, they're double) */
#define WRITE_FLOAT_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outDouble(str, node->fldname))

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outBitmapset(str, node->fldname))

/* Write a variable-length array (not a List) of Node pointers */
#define WRITE_NODE_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeNodeArray(str, (const Node * const *) node->fldname, len))

/* Write a variable-length array of AttrNumber */
#define WRITE_ATTRNUMBER_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeAttrNumberCols(str, node->fldname, len))

/* Write a variable-length array of Oid */
#define WRITE_OID_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeOidCols(str, node->fldname, len))

/* Write a variable-length array of Index */
#define WRITE_INDEX_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeIndexCols(str, node->fldname, len))

/* Write a variable-length array of int */
#define WRITE_INT_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeIntCols(str, node->fldname, len))

/* Write a variable-length array of bool */
#define WRITE_BOOL_ARRAY(fldname, len) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 writeBoolCols(str, node->fldname, len))

/* Write a bytea field */
#define WRITE_BYTEA_FIELD(fldname) \
	(_outDatum(str, PointerGetDatum(node->fldname), -1, false))

/* Write a dummy field -- value not displayable or copyable */
#define WRITE_DUMMY_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outToken(str, NULL))


static void
_outCdbPathLocus(StringInfo str, const CdbPathLocus *node)
{
	WRITE_ENUM_FIELD(locustype, CdbLocusType);
	WRITE_NODE_FIELD(distkey);
	WRITE_INT_FIELD(numsegments);
}

#define booltostr(x)  ((x) ? "true" : "false")


/*
 * outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null string pointer is given, it is encoded as '<>'.
 *	  An empty string is encoded as '""'.  To avoid ambiguity, input
 *	  strings beginning with '<' or '"' receive a leading backslash.
 */
void
outToken(StringInfo str, const char *s)
{
	if (s == NULL)
	{
		appendStringInfoString(str, "<>");
		return;
	}
	if (*s == '\0')
	{
		appendStringInfoString(str, "\"\"");
		return;
	}

	/*
	 * Look for characters or patterns that are treated specially by read.c
	 * (either in pg_strtok() or in nodeRead()), and therefore need a
	 * protective backslash.
	 */
	/* These characters only need to be quoted at the start of the string */
	if (*s == '<' ||
		*s == '"' ||
		isdigit((unsigned char) *s) ||
		((*s == '+' || *s == '-') &&
		 (isdigit((unsigned char) s[1]) || s[1] == '.')))
		appendStringInfoChar(str, '\\');
	while (*s)
	{
		/* These chars must be backslashed anywhere in the string */
		if (*s == ' ' || *s == '\n' || *s == '\t' ||
			*s == '(' || *s == ')' || *s == '{' || *s == '}' ||
			*s == '\\')
			appendStringInfoChar(str, '\\');
		appendStringInfoChar(str, *s++);
	}
}

/*
 * Convert one char.  Goes through outToken() so that special characters are
 * escaped.
 */
static void
outChar(StringInfo str, char c)
{
	char		in[2];

	/* Traditionally, we've represented \0 as <>, so keep doing that */
	if (c == '\0')
	{
		appendStringInfoString(str, "<>");
		return;
	}

	in[0] = c;
	in[1] = '\0';

	outToken(str, in);
}

/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
static void
outDouble(StringInfo str, double d)
{
	char		buf[DOUBLE_SHORTEST_DECIMAL_LEN];

	double_to_shortest_decimal_buf(d, buf);
	appendStringInfoString(str, buf);
}

/*
 * common implementation for scalar-array-writing functions
 *
 * The data format is either "<>" for a NULL pointer or "(item item item)".
 * fmtstr must include a leading space, and the rest of it must produce
 * something that will be seen as a single simple token by pg_strtok().
 * convfunc can be empty, or the name of a conversion macro or function.
 */
#define WRITE_SCALAR_ARRAY(fnname, datatype, fmtstr, convfunc) \
static void \
fnname(StringInfo str, const datatype *arr, int len) \
{ \
	if (arr != NULL) \
	{ \
		appendStringInfoChar(str, '('); \
		for (int i = 0; i < len; i++) \
			appendStringInfo(str, fmtstr, convfunc(arr[i])); \
		appendStringInfoChar(str, ')'); \
	} \
	else \
		appendStringInfoString(str, "<>"); \
}

WRITE_SCALAR_ARRAY(writeAttrNumberCols, AttrNumber, " %d",)
WRITE_SCALAR_ARRAY(writeOidCols, Oid, " %u",)
WRITE_SCALAR_ARRAY(writeIndexCols, Index, " %u",)
WRITE_SCALAR_ARRAY(writeIntCols, int, " %d",)
WRITE_SCALAR_ARRAY(writeBoolCols, bool, " %s", booltostr)

/*
 * Print an array (not a List) of Node pointers.
 *
 * The decoration is identical to that of scalar arrays, but we can't
 * quite use appendStringInfo() in the loop.
 */
static void
writeNodeArray(StringInfo str, const Node *const *arr, int len)
{
	if (arr != NULL)
	{
		appendStringInfoChar(str, '(');
		for (int i = 0; i < len; i++)
		{
			appendStringInfoChar(str, ' ');
			outNode(str, arr[i]);
		}
		appendStringInfoChar(str, ')');
	}
	else
		appendStringInfoString(str, "<>");
}

/*
 * Print a List.
 */
static void
_outList(StringInfo str, const List *node)
{
	const ListCell *lc;

	appendStringInfoChar(str, '(');

	if (IsA(node, IntList))
		appendStringInfoChar(str, 'i');
	else if (IsA(node, OidList))
		appendStringInfoChar(str, 'o');
	else if (IsA(node, XidList))
		appendStringInfoChar(str, 'x');

	foreach(lc, node)
	{
		/*
		 * For the sake of backward compatibility, we emit a slightly
		 * different whitespace format for lists of nodes vs. other types of
		 * lists. XXX: is this necessary?
		 */
		if (IsA(node, List))
		{
			outNode(str, lfirst(lc));
			if (lnext(node, lc))
				appendStringInfoChar(str, ' ');
		}
		else if (IsA(node, IntList))
			appendStringInfo(str, " %d", lfirst_int(lc));
		else if (IsA(node, OidList))
			appendStringInfo(str, " %u", lfirst_oid(lc));
		else if (IsA(node, XidList))
			appendStringInfo(str, " %u", lfirst_xid(lc));
		else
			elog(ERROR, "unrecognized list node type: %d",
				 (int) node->type);
	}

	appendStringInfoChar(str, ')');
}

/*
 * outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Note: the output format is "(b int int ...)", similar to an integer List.
 *
 * We export this function for use by extensions that define extensible nodes.
 * That's somewhat historical, though, because calling outNode() will work.
 */
void
outBitmapset(StringInfo str, const Bitmapset *bms)
{
	int			x;

	appendStringInfoChar(str, '(');
	appendStringInfoChar(str, 'b');
	x = -1;
	while ((x = bms_next_member(bms, x)) >= 0)
		appendStringInfo(str, " %d", x);
	appendStringInfoChar(str, ')');
}

/*
 * Print the value of a Datum given its type.
 */
void
outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length,
				i;
	char	   *s;

	length = datumGetSize(value, typbyval, typlen);

	if (typbyval)
	{
		s = (char *) (&value);
		appendStringInfo(str, "%u [ ", (unsigned int) length);
		for (i = 0; i < (Size) sizeof(Datum); i++)
			appendStringInfo(str, "%d ", (int) (s[i]));
		appendStringInfoChar(str, ']');
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
			appendStringInfoString(str, "0 [ ]");
		else
		{
			appendStringInfo(str, "%u [ ", (unsigned int) length);
			for (i = 0; i < length; i++)
				appendStringInfo(str, "%d ", (int) (s[i]));
			appendStringInfoChar(str, ']');
		}
	}
}


#include "outfuncs.funcs.c"


/*
 * Support functions for nodes with custom_read_write attribute or
 * special_read_write attribute
 */

static void
_outConst(StringInfo str, const Const *node)
{
	WRITE_NODE_TYPE("CONST");

	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	appendStringInfoString(str, " :constvalue ");
	if (node->constisnull)
		appendStringInfoString(str, "<>");
	else
		outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outBoolExpr(StringInfo str, const BoolExpr *node)
{
	char	   *opstr = NULL;

	WRITE_NODE_TYPE("BOOLEXPR");

	/* do-it-yourself enum representation */
	switch (node->boolop)
	{
		case AND_EXPR:
			opstr = "and";
			break;
		case OR_EXPR:
			opstr = "or";
			break;
		case NOT_EXPR:
			opstr = "not";
			break;
	}
	appendStringInfoString(str, " :boolop ");
	outToken(str, opstr);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outForeignKeyOptInfo(StringInfo str, const ForeignKeyOptInfo *node)
{
	int			i;

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
	/* for compactness, just print the number of matches per column: */
	appendStringInfoString(str, " :eclass");
	for (i = 0; i < node->nkeys; i++)
		appendStringInfo(str, " %d", (node->eclass[i] != NULL));
	appendStringInfoString(str, " :rinfos");
	for (i = 0; i < node->nkeys; i++)
		appendStringInfo(str, " %d", list_length(node->rinfos[i]));
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

static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
	WRITE_NODE_TYPE("RANGETBLENTRY");

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
			/* we re-use these RELATION fields, too: */
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
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			/* we re-use these RELATION fields, too: */
			WRITE_OID_FIELD(relid);
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
_outSerializedParams(StringInfo str, const SerializedParams *node)
{
	WRITE_NODE_TYPE("SERIALIZEDPARAMS");

	WRITE_INT_FIELD(nExternParams);
	for (int i = 0; i < node->nExternParams; i++)
	{
		WRITE_BOOL_FIELD(externParams[i].isnull);
		WRITE_INT_FIELD(externParams[i].pflags);
		WRITE_OID_FIELD(externParams[i].ptype);
		WRITE_INT_FIELD(externParams[i].plen);
		WRITE_BOOL_FIELD(externParams[i].pbyval);

		if (!node->externParams[i].isnull)
			outDatum(str,
					 node->externParams[i].value,
					 node->externParams[i].plen,
					 node->externParams[i].pbyval);
	}

	WRITE_INT_FIELD(nExecParams);
	for (int i = 0; i < node->nExecParams; i++)
	{
		WRITE_BOOL_FIELD(execParams[i].isnull);
		WRITE_BOOL_FIELD(execParams[i].isvalid);
		WRITE_INT_FIELD(execParams[i].plen);
		WRITE_BOOL_FIELD(execParams[i].pbyval);

		if (node->execParams[i].isvalid && !node->execParams[i].isnull)
			outDatum(str,
					 node->execParams[i].value,
					 node->execParams[i].plen,
					 node->execParams[i].pbyval);
		WRITE_BOOL_FIELD(execParams[i].pbyval);
	}
}

static void
_outSliceTable(StringInfo str, const SliceTable *node)
{
	WRITE_NODE_TYPE("SLICETABLE");

	WRITE_INT_FIELD(localSlice);
	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].rootIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].planNumSegments);
		WRITE_NODE_FIELD(slices[i].children); /* List of int index */
		WRITE_ENUM_FIELD(slices[i].gangType, GangType);
		WRITE_NODE_FIELD(slices[i].segments); /* List of int */
		WRITE_BOOL_FIELD(slices[i].useMppParallelMode);
		WRITE_INT_FIELD(slices[i].parallel_workers);
		WRITE_DUMMY_FIELD(slices[i].primaryGang);
		WRITE_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		WRITE_BITMAPSET_FIELD(slices[i].processesMap);
	}
	WRITE_BOOL_FIELD(hasMotions);
	WRITE_INT_FIELD(instrument_options);
	WRITE_INT_FIELD(ic_instance_id);
}

static void
_outA_Expr(StringInfo str, const A_Expr *node)
{
	WRITE_NODE_TYPE("A_EXPR");

	switch (node->kind)
	{
		case AEXPR_OP:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			appendStringInfoString(str, " ANY ");
			break;
		case AEXPR_OP_ALL:
			appendStringInfoChar(str, ' ');
			WRITE_NODE_FIELD(name);
			appendStringInfoString(str, " ALL ");
			break;
		case AEXPR_DISTINCT:
			appendStringInfoString(str, " DISTINCT ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:
			appendStringInfoString(str, " NOT_DISTINCT ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:
			appendStringInfoString(str, " NULLIF ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_IN:
			appendStringInfoString(str, " IN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:
			appendStringInfoString(str, " LIKE ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:
			appendStringInfoString(str, " ILIKE ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:
			appendStringInfoString(str, " SIMILAR ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:
			appendStringInfoString(str, " BETWEEN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:
			appendStringInfoString(str, " NOT_BETWEEN ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:
			appendStringInfoString(str, " BETWEEN_SYM ");
			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:
			appendStringInfoString(str, " NOT_BETWEEN_SYM ");
			WRITE_NODE_FIELD(name);
			break;
		default:
			appendStringInfoString(str, " ??");
			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outInteger(StringInfo str, const Integer *node)
{
	appendStringInfo(str, "%d", node->ival);
}

static void
_outFloat(StringInfo str, const Float *node)
{
	/*
	 * We assume the value is a valid numeric literal and so does not need
	 * quoting.
	 */
	appendStringInfoString(str, node->fval);
}

static void
_outBoolean(StringInfo str, const Boolean *node)
{
	appendStringInfoString(str, node->boolval ? "true" : "false");
}

static void
_outString(StringInfo str, const String *node)
{
	/*
	 * We use outToken to provide escaping of the string's content, but we
	 * don't want it to convert an empty string to '""', because we're putting
	 * double quotes around the string already.
	 */
	appendStringInfoChar(str, '"');
	if (node->sval[0] != '\0')
		outToken(str, node->sval);
	appendStringInfoChar(str, '"');
}

static void
_outBitString(StringInfo str, const BitString *node)
{
	/*
	 * The lexer will always produce a string starting with 'b' or 'x'.  There
	 * might be characters following that that need escaping, but outToken
	 * won't escape the 'b' or 'x'.  This is relied on by nodeTokenType.
	 */
	Assert(node->bsval[0] == 'b' || node->bsval[0] == 'x');
	outToken(str, node->bsval);
}

static void
_outA_Const(StringInfo str, const A_Const *node)
{
	WRITE_NODE_TYPE("A_CONST");

	if (node->isnull)
		appendStringInfoString(str, " NULL");
	else
	{
		appendStringInfoString(str, " :val ");
		outNode(str, &node->val);
	}
	WRITE_LOCATION_FIELD(location);
}

static void
_outConstraint(StringInfo str, const Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_STRING_FIELD(conname);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	appendStringInfoString(str, " :contype ");
	switch (node->contype)
	{
		case CONSTR_NULL:
			appendStringInfoString(str, "NULL");
			break;

		case CONSTR_NOTNULL:
			appendStringInfoString(str, "NOT_NULL");
			break;

		case CONSTR_DEFAULT:
			appendStringInfoString(str, "DEFAULT");
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			break;

		case CONSTR_IDENTITY:
			appendStringInfoString(str, "IDENTITY");
			WRITE_NODE_FIELD(options);
			WRITE_CHAR_FIELD(generated_when);
			break;

		case CONSTR_GENERATED:
			appendStringInfoString(str, "GENERATED");
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			WRITE_CHAR_FIELD(generated_when);
			break;

		case CONSTR_CHECK:
			appendStringInfoString(str, "CHECK");
			WRITE_BOOL_FIELD(is_no_inherit);
			WRITE_NODE_FIELD(raw_expr);
			WRITE_STRING_FIELD(cooked_expr);
			WRITE_BOOL_FIELD(skip_validation);
			WRITE_BOOL_FIELD(initially_valid);
			break;

		case CONSTR_PRIMARY:
			appendStringInfoString(str, "PRIMARY_KEY");
			WRITE_NODE_FIELD(keys);
			WRITE_NODE_FIELD(including);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_UNIQUE:
			appendStringInfoString(str, "UNIQUE");
			WRITE_BOOL_FIELD(nulls_not_distinct);
			WRITE_NODE_FIELD(keys);
			WRITE_NODE_FIELD(including);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_EXCLUSION:
			appendStringInfoString(str, "EXCLUSION");
			WRITE_NODE_FIELD(exclusions);
			WRITE_NODE_FIELD(including);
			WRITE_NODE_FIELD(options);
			WRITE_STRING_FIELD(indexname);
			WRITE_STRING_FIELD(indexspace);
			WRITE_BOOL_FIELD(reset_default_tblspc);
			WRITE_STRING_FIELD(access_method);
			WRITE_NODE_FIELD(where_clause);
			break;

		case CONSTR_FOREIGN:
			appendStringInfoString(str, "FOREIGN_KEY");
			WRITE_NODE_FIELD(pktable);
			WRITE_NODE_FIELD(fk_attrs);
			WRITE_NODE_FIELD(pk_attrs);
			WRITE_CHAR_FIELD(fk_matchtype);
			WRITE_CHAR_FIELD(fk_upd_action);
			WRITE_CHAR_FIELD(fk_del_action);
			WRITE_NODE_FIELD(fk_del_set_cols);
			WRITE_NODE_FIELD(old_conpfeqop);
			WRITE_OID_FIELD(old_pktable_oid);
			WRITE_BOOL_FIELD(skip_validation);
			WRITE_BOOL_FIELD(initially_valid);
			break;

		case CONSTR_ATTR_DEFERRABLE:
			appendStringInfoString(str, "ATTR_DEFERRABLE");
			break;

		case CONSTR_ATTR_NOT_DEFERRABLE:
			appendStringInfoString(str, "ATTR_NOT_DEFERRABLE");
			break;

		case CONSTR_ATTR_DEFERRED:
			appendStringInfoString(str, "ATTR_DEFERRED");
			break;

		case CONSTR_ATTR_IMMEDIATE:
			appendStringInfoString(str, "ATTR_IMMEDIATE");
			break;

		default:
			elog(ERROR, "unrecognized ConstrType: %d", (int) node->contype);
			break;
	}
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
	WRITE_NODE_ARRAY(simple_rel_array, node->simple_rel_array_size);
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
_outTupleDescNode(StringInfo str, const TupleDescNode *node)
{
	Assert(node->tuple->tdtypeid == RECORDOID);

	WRITE_NODE_TYPE("TUPLEDESCNODE");
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
}


static void
wrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		char	   *str = (char *) lfirst(lc);

		lfirst(lc) = makeString(str);
	}
}

static void
unwrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		String	   *val = lfirst(lc);

		lfirst(lc) = strVal(val);
		pfree(val);
	}
}

static void
_outAlteredTableInfo(StringInfo str, const AlteredTableInfo *node)
{
	ListCell   *lc;

	WRITE_NODE_TYPE("ALTEREDTABLEINFO");

	WRITE_OID_FIELD(relid);
	WRITE_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
	{
		WRITE_NODE_FIELD(subcmds[i]);
	}

	/*
	 * These aren't Nodes in upstream, so make sure the node tags
	 * are set correctly before trying to serialize them.
	 */
	foreach(lc, node->constraints)
	{
		NewConstraint *e = (NewConstraint *) lfirst(lc);
		e->type = T_NewConstraint;
	}
	foreach(lc, node->newvals)
	{
		NewColumnValue *e = (NewColumnValue *) lfirst(lc);
		e->type = T_NewColumnValue;
	}

	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(afterStmts);
	WRITE_BOOL_FIELD(verify_new_notnull);
	WRITE_INT_FIELD(rewrite);
	WRITE_OID_FIELD(newAccessMethod);
	WRITE_BOOL_FIELD(dist_opfamily_changed);
	WRITE_OID_FIELD(new_opclass);
	/*
	 * NB: newTableSpace is excluded, it will be assigned in phase 1 of AlterTable.
	 * If newTableSpace is required, refer to the name in its corresponding cmd.
	 * If newTableSpace is strongly required in serialization, please add it
	 * and update `ATPrepSetTableSpace()` to avoid error.
	 */
	WRITE_BOOL_FIELD(chgPersistence);
	WRITE_CHAR_FIELD(newrelpersistence);
	WRITE_NODE_FIELD(partition_constraint);
	WRITE_BOOL_FIELD(validate_default);
	WRITE_NODE_FIELD(changedConstraintOids);

	/* node->changedConstraintDefs is a list of naked strings, so
	 * we can't use WRITE_NODE_FIELD on it. Temporarily wrap them in Values.
	 */
	wrapStringList(node->changedConstraintDefs);
	WRITE_NODE_FIELD(changedConstraintDefs);
	/* unwrap them again */
	unwrapStringList(node->changedConstraintDefs);

	WRITE_NODE_FIELD(changedIndexOids);
	wrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(changedIndexDefs);
	unwrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(beforeStmtLists);
	WRITE_NODE_FIELD(constraintLists);
}

static void
_outNewConstraint(StringInfo str, const NewConstraint *node)
{
	WRITE_NODE_TYPE("NEWCONSTRAINT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(contype, ConstrType);
	WRITE_OID_FIELD(refrelid);
	WRITE_OID_FIELD(refindid);
	WRITE_OID_FIELD(conid);
	WRITE_NODE_FIELD(qual);
	/* can't serialize qualstate */
}

static void
_outPlannedStmt(StringInfo str, const PlannedStmt *node)
{
	WRITE_NODE_TYPE("PLANNEDSTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(planGen, PlanGenerator);
	WRITE_UINT64_FIELD(queryId);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(oneoffPlan);
	WRITE_OID_FIELD(simplyUpdatableRel);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeNeeded);
	WRITE_INT_FIELD(jitFlags);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(permInfos);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(appendRelations);
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(relationOids);
	WRITE_NODE_FIELD(invalItems);
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_INT_FIELD(stmt_len);

	WRITE_INT_ARRAY(subplan_sliceIds, list_length(node->subplans));

	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].gangType);
		WRITE_INT_FIELD(slices[i].numsegments);
		WRITE_INT_FIELD(slices[i].parallel_workers);
		WRITE_INT_FIELD(slices[i].segindex);
		WRITE_BOOL_FIELD(slices[i].directDispatch.isDirectDispatch);
		WRITE_NODE_FIELD(slices[i].directDispatch.contentIds);
	}

	WRITE_BITMAPSET_FIELD(rewindPlanIDs);

	WRITE_NODE_FIELD(intoPolicy);

	WRITE_UINT64_FIELD(query_mem);

	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(copyIntoClause);
	WRITE_NODE_FIELD(refreshClause);
	WRITE_INT_FIELD(metricsQueryType);
	WRITE_NODE_FIELD(extensionContext);

}

static void
_outMotion(StringInfo str, const Motion *node)
{
	WRITE_NODE_TYPE("MOTION");

	WRITE_FLOAT_FIELD(plan.startup_cost);
	WRITE_FLOAT_FIELD(plan.total_cost);
	WRITE_FLOAT_FIELD(plan.plan_rows);
	WRITE_INT_FIELD(plan.plan_width);
	WRITE_BOOL_FIELD(plan.parallel_aware);
	WRITE_BOOL_FIELD(plan.parallel_safe);
	WRITE_BOOL_FIELD(plan.async_capable);
	WRITE_INT_FIELD(plan.plan_node_id);
	WRITE_NODE_FIELD(plan.targetlist);
	WRITE_NODE_FIELD(plan.qual);
	WRITE_NODE_FIELD(plan.lefttree);
	WRITE_NODE_FIELD(plan.righttree);
	WRITE_NODE_FIELD(plan.initPlan);
	WRITE_BITMAPSET_FIELD(plan.extParam);
	WRITE_BITMAPSET_FIELD(plan.allParam);
	WRITE_NODE_FIELD(plan.flow);
	WRITE_UINT_FIELD(plan.locustype);
	WRITE_INT_FIELD(plan.parallel);
	WRITE_UINT64_FIELD(plan.operatorMemKB);
	
	WRITE_INT_FIELD(motionID);
	WRITE_ENUM_FIELD(motionType, MotionType);

	WRITE_BOOL_FIELD(sendSorted);

	WRITE_NODE_FIELD(hashExprs);
	WRITE_OID_ARRAY(hashFuncs, list_length(node->hashExprs));
	WRITE_INT_FIELD(numSortCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numSortCols);
	WRITE_OID_ARRAY(sortOperators, node->numSortCols);
	WRITE_OID_ARRAY(collations, node->numSortCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numSortCols);
	WRITE_INT_FIELD(segidColIdx);

	WRITE_INT_FIELD(numHashSegments);
}

static void
_outOidAssignment(StringInfo str, const OidAssignment *node)
{
	WRITE_NODE_TYPE("OIDASSIGNMENT");

	WRITE_OID_FIELD(catalog);
	WRITE_STRING_FIELD(objname);
	WRITE_OID_FIELD(namespaceOid);
	WRITE_OID_FIELD(keyOid1);
	WRITE_OID_FIELD(keyOid2);
	WRITE_OID_FIELD(oid);
}

/*
 * outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
void
outNode(StringInfo str, const void *obj)
{
	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	if (obj == NULL)
		appendStringInfoString(str, "<>");
	else if (IsA(obj, List) || IsA(obj, IntList) || IsA(obj, OidList) ||
			 IsA(obj, XidList))
		_outList(str, obj);
	/* nodeRead does not want to see { } around these! */
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
		appendStringInfoChar(str, '{');
		switch (nodeTag(obj))
		{
#include "outfuncs.switch.c"

			default:

				/*
				 * This should be an ERROR, but it's too useful to be able to
				 * dump structures that outNode only understands part of.
				 */
				elog(WARNING, "could not dump unrecognized node type: %d",
					 (int) nodeTag(obj));
				break;
		}
		appendStringInfoChar(str, '}');
	}
}

/*
 * nodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char *
nodeToString(const void *obj)
{
	StringInfoData str;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfo(&str);
	outNode(&str, obj);
	return str.data;
}

/*
 * bmsToString -
 *	   returns the ascii representation of the Bitmapset as a palloc'd string
 */
char *
bmsToString(const Bitmapset *bms)
{
	StringInfoData str;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfo(&str);
	outBitmapset(&str, bms);
	return str.data;
}
