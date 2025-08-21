/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *	  Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/readfuncs.c
 *
 * NOTES
 *	  Path nodes do not have any readfuncs support, because we never
 *	  have occasion to read them in.  (There was once code here that
 *	  claimed to read them, but it was broken as well as unused.)  We
 *	  never read executor state trees, either.
 *
 *    But due to the use of this routine in older version of CDB/MPP/GPDB,
 *    there are routines that do read those types of nodes (unlike PostgreSQL)
 *    Those routines never actually get called.
 *
 *    We could go back and remove them, but they don't hurt anything.
 *
 *    The purpose of these routines is to read serialized trees that were stored
 *    in the catalog, and reconstruct the trees.
 *
 *	  Parse location fields are written out by outfuncs.c, but only for
 *	  debugging use.  When reading a location field, we normally discard
 *	  the stored value and set the location field to -1 (ie, "unknown").
 *	  This is because nodes coming from a stored rule should not be thought
 *	  to have a known location in the current query's text.
 *	  However, if restore_location_fields is true, we do restore location
 *	  fields from the string.  This is currently intended only for use by the
 *	  WRITE_READ_PARSE_PLAN_TREES test code, which doesn't want to cause
 *	  any change in the node contents.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "utils/builtins.h"

#include "cdb/cdbgang.h"
#include "nodes/altertablenodes.h"

/*
 * readfuncs.c is compiled normally into readfuncs.o, but it's also
 * #included from readfast.c. When #included, readfuncs.c defines
 * COMPILING_BINARY_FUNCS, and provides replacements READ_* macros. See
 * comments at top of readfast.c.
 */
#ifndef COMPILING_BINARY_FUNCS

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	const char *token;		\
	int			length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)			\
	READ_LOCALS_NO_FIELDS(nodeTypeName);	\
	READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoui(token)

/* Read an unsigned integer field (anything written using UINT64_FORMAT) */
#define READ_UINT64_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtou64(token, NULL, 10)

/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atol(token)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atooid(token)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	/* avoid overhead of calling debackslash() for one char */ \
	local_node->fldname = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and possibly throw away the value) */
#ifdef WRITE_READ_PARSE_PLAN_TREES
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = restore_location_fields ? atoi(token) : -1
#else
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = -1	/* set field to "unknown" */
#endif

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = nodeRead(NULL, 0)

/* Read a bytea field */
#define READ_BYTEA_FIELD(fldname) \
	local_node->fldname = (bytea *) DatumGetPointer(readDatum(false))

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = _readBitmapset()

/* Read an attribute number array */
#define READ_ATTRNUMBER_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readAttrNumberCols(len)

/* Read an oid array */
#define READ_OID_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readOidCols(len)

/* Read an int array */
#define READ_INT_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readIntCols(len)

/* Read a bool array */
#define READ_BOOL_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readBoolCols(len)

/* Routine exit */
#define READ_DONE() \
	return local_node


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))


#endif /* COMPILING_BINARY_FUNCS */

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readBitmapset
 */
static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *result = NULL;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = pg_strtok(&length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
		result = bms_add_member(result, val);
	}

	return result;
}

/*
 * for use by extensions which define extensible nodes
 */
Bitmapset *
readBitmapset(void)
{
	return _readBitmapset();
}

/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(querySource, QuerySource);
	local_node->queryId = UINT64CONST(0);	/* not saved in output format */
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindowFuncs);
	READ_BOOL_FIELD(hasTargetSRFs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_BOOL_FIELD(hasDynamicFunctions);
	READ_BOOL_FIELD(hasFuncsWithExecRestrictions);
	READ_BOOL_FIELD(hasDistinctOn);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(hasForUpdate);
	READ_BOOL_FIELD(hasRowSecurity);
	READ_BOOL_FIELD(canOptSelectLockingClause);
	READ_BOOL_FIELD(isReturn);
	READ_NODE_FIELD(cteList);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(rteperminfos);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(mergeActionList);
	READ_BOOL_FIELD(mergeUseOuterJoin);
	READ_NODE_FIELD(targetList);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(onConflict);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_BOOL_FIELD(groupDistinct);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(scatterClause);
	READ_BOOL_FIELD(isTableValueSelect);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_ENUM_FIELD(limitOption, LimitOption);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(constraintDeps);
	READ_NODE_FIELD(withCheckOptions);
    local_node->intoPolicy = NULL;
    READ_BOOL_FIELD(parentStmtType);
	READ_LOCATION_FIELD(stmt_location);
	READ_INT_FIELD(stmt_len);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(void)
{
	READ_LOCALS(NotifyStmt);

	READ_STRING_FIELD(conditionname);
	READ_STRING_FIELD(payload);

	READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(void)
{
	READ_LOCALS(DeclareCursorStmt);

	READ_STRING_FIELD(portalname);
	READ_INT_FIELD(options);
	READ_NODE_FIELD(query);

	READ_DONE();
}

/*
 * _readWithCheckOption
 */
static WithCheckOption *
_readWithCheckOption(void)
{
	READ_LOCALS(WithCheckOption);

	READ_ENUM_FIELD(kind, WCOKind);
	READ_STRING_FIELD(relname);
	READ_STRING_FIELD(polname);
	READ_NODE_FIELD(qual);
	READ_BOOL_FIELD(cascaded);

	READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause *
_readSortGroupClause(void)
{
	READ_LOCALS(SortGroupClause);

	READ_UINT_FIELD(tleSortGroupRef);
	READ_OID_FIELD(eqop);
	READ_OID_FIELD(sortop);
	READ_BOOL_FIELD(nulls_first);
	READ_BOOL_FIELD(hashable);

	READ_DONE();
}

/*
 * _readGroupingSet
 */
static GroupingSet *
_readGroupingSet(void)
{
	READ_LOCALS(GroupingSet);

	READ_ENUM_FIELD(kind, GroupingSetKind);
	READ_NODE_FIELD(content);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause *
_readWindowClause(void)
{
	READ_LOCALS(WindowClause);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_OID_FIELD(startInRangeFunc);
	READ_OID_FIELD(endInRangeFunc);
	READ_OID_FIELD(inRangeColl);
	READ_BOOL_FIELD(inRangeAsc);
	READ_BOOL_FIELD(inRangeNullsFirst);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(copiedOrder);

	READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(void)
{
	READ_LOCALS(RowMarkClause);

	READ_UINT_FIELD(rti);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	READ_BOOL_FIELD(pushedDown);

	READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr *
_readCommonTableExpr(void)
{
	READ_LOCALS(CommonTableExpr);

	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	READ_ENUM_FIELD(ctematerialized, CTEMaterialize);
	READ_NODE_FIELD(ctequery);
	READ_NODE_FIELD(search_clause);
	READ_NODE_FIELD(cycle_clause);
	READ_LOCATION_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);
	READ_NODE_FIELD(ctecolcollations);

	READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(void)
{
	READ_LOCALS(SetOperationStmt);

	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(colTypes);
	READ_NODE_FIELD(colTypmods);
	READ_NODE_FIELD(colCollations);
	READ_NODE_FIELD(groupClauses);

	READ_DONE();
}


/*
 *	Stuff from primnodes.h.
 */

static Alias *
_readAlias(void)
{
	READ_LOCALS(Alias);

	READ_STRING_FIELD(aliasname);
	READ_NODE_FIELD(colnames);

	READ_DONE();
}

static RangeVar *
_readRangeVar(void)
{
	READ_LOCALS(RangeVar);

	local_node->catalogname = NULL; /* not currently saved in output format */

	READ_STRING_FIELD(catalogname);
	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(relname);
	READ_BOOL_FIELD(inh);
	READ_CHAR_FIELD(relpersistence);
	READ_NODE_FIELD(alias);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readTableFunc
 */
static TableFunc *
_readTableFunc(void)
{
	READ_LOCALS(TableFunc);

	READ_NODE_FIELD(ns_uris);
	READ_NODE_FIELD(ns_names);
	READ_NODE_FIELD(docexpr);
	READ_NODE_FIELD(rowexpr);
	READ_NODE_FIELD(colnames);
	READ_NODE_FIELD(coltypes);
	READ_NODE_FIELD(coltypmods);
	READ_NODE_FIELD(colcollations);
	READ_NODE_FIELD(colexprs);
	READ_NODE_FIELD(coldefexprs);
	READ_BITMAPSET_FIELD(notnulls);
	READ_INT_FIELD(ordinalitycol);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static IntoClause *
_readIntoClause(void)
{
	READ_LOCALS(IntoClause);

	READ_NODE_FIELD(rel);
	READ_NODE_FIELD(colNames);
	READ_STRING_FIELD(accessMethod);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(onCommit, OnCommitAction);
	READ_STRING_FIELD(tableSpaceName);
	READ_NODE_FIELD(viewQuery);
	READ_BOOL_FIELD(skipData);
	READ_NODE_FIELD(distributedBy);
	READ_BOOL_FIELD(ivm);
	READ_OID_FIELD(matviewOid);
	READ_STRING_FIELD(enrname);
	READ_BOOL_FIELD(dynamicTbl);
	READ_STRING_FIELD(schedule);

	READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(void)
{
	READ_LOCALS(Var);

	READ_UINT_FIELD(varno);
	READ_INT_FIELD(varattno);
	READ_OID_FIELD(vartype);
	READ_INT_FIELD(vartypmod);
	READ_OID_FIELD(varcollid);
	READ_BITMAPSET_FIELD(varnullingrels);
	READ_UINT_FIELD(varlevelsup);
	READ_UINT_FIELD(varnosyn);
	READ_INT_FIELD(varattnosyn);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readConst
 */
static Const *
_readConst(void)
{
	READ_LOCALS(Const);

	READ_OID_FIELD(consttype);
	READ_INT_FIELD(consttypmod);
	READ_OID_FIELD(constcollid);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);
	READ_LOCATION_FIELD(location);

	token = pg_strtok(&length); /* skip :constvalue */
	if (local_node->constisnull)
		token = pg_strtok(&length); /* skip "<>" */
	else
		local_node->constvalue = readDatum(local_node->constbyval);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readParam
 */
static Param *
_readParam(void)
{
	READ_LOCALS(Param);

	READ_ENUM_FIELD(paramkind, ParamKind);
	READ_INT_FIELD(paramid);
	READ_OID_FIELD(paramtype);
	READ_INT_FIELD(paramtypmod);
	READ_OID_FIELD(paramcollid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(void)
{
	READ_LOCALS(Aggref);

	READ_OID_FIELD(aggfnoid);
	READ_OID_FIELD(aggtype);
	READ_OID_FIELD(aggcollid);
	READ_OID_FIELD(inputcollid);
	READ_OID_FIELD(aggtranstype);
	READ_NODE_FIELD(aggargtypes);
	READ_NODE_FIELD(aggdirectargs);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggorder);
	READ_NODE_FIELD(aggdistinct);
	READ_NODE_FIELD(aggfilter);
	READ_BOOL_FIELD(aggstar);
	READ_BOOL_FIELD(aggvariadic);
	READ_CHAR_FIELD(aggkind);
	READ_UINT_FIELD(agglevelsup);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_INT_FIELD(aggno);
	READ_INT_FIELD(aggtransno);
	READ_LOCATION_FIELD(location);
	READ_INT_FIELD(agg_expr_id);

	READ_DONE();
}

/*
 * _readGroupingFunc
 */
static GroupingFunc *
_readGroupingFunc(void)
{
	READ_LOCALS(GroupingFunc);

	READ_NODE_FIELD(args);
	READ_NODE_FIELD(refs);
	READ_NODE_FIELD(cols);
	READ_UINT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowFunc
 */
static WindowFunc *
_readWindowFunc(void)
{
	READ_LOCALS(WindowFunc);

	READ_OID_FIELD(winfnoid);
	READ_OID_FIELD(wintype);
	READ_OID_FIELD(wincollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggfilter);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(winstar);
	READ_BOOL_FIELD(winagg);
	READ_BOOL_FIELD(windistinct);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSubscriptingRef
 */
static SubscriptingRef *
_readSubscriptingRef(void)
{
	READ_LOCALS(SubscriptingRef);

	READ_OID_FIELD(refcontainertype);
	READ_OID_FIELD(refelemtype);
	READ_OID_FIELD(refrestype);
	READ_INT_FIELD(reftypmod);
	READ_OID_FIELD(refcollid);
	READ_NODE_FIELD(refupperindexpr);
	READ_NODE_FIELD(reflowerindexpr);
	READ_NODE_FIELD(refexpr);
	READ_NODE_FIELD(refassgnexpr);

	READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(void)
{
	READ_LOCALS(FuncExpr);

	READ_OID_FIELD(funcid);
	READ_OID_FIELD(funcresulttype);
	READ_BOOL_FIELD(funcretset);
	READ_BOOL_FIELD(funcvariadic);
	READ_ENUM_FIELD(funcformat, CoercionForm);
	READ_OID_FIELD(funccollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(is_tablefunc);  /* GPDB */
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr *
_readNamedArgExpr(void)
{
	READ_LOCALS(NamedArgExpr);

	READ_NODE_FIELD(arg);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(argnumber);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{
	READ_LOCALS(OpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(void)
{
	READ_LOCALS(DistinctExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(void)
{
	READ_LOCALS(NullIfExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(void)
{
	READ_LOCALS(ScalarArrayOpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(hashfuncid);
	READ_BOOL_FIELD(useOr);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	/* do-it-yourself enum representation */
	token = pg_strtok(&length); /* skip :boolop */
	token = pg_strtok(&length); /* get field value */
	if (strncmp(token, "and", 3) == 0)
		local_node->boolop = AND_EXPR;
	else if (strncmp(token, "or", 2) == 0)
		local_node->boolop = OR_EXPR;
	else if (strncmp(token, "not", 3) == 0)
		local_node->boolop = NOT_EXPR;
	else
		elog(ERROR, "unrecognized boolop \"%.*s\"", length, token);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readSubLink
 */
static SubLink *
_readSubLink(void)
{
	READ_LOCALS(SubLink);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_INT_FIELD(subLinkId);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(operName);
	READ_NODE_FIELD(subselect);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(void)
{
	READ_LOCALS(FieldSelect);

	READ_NODE_FIELD(arg);
	READ_INT_FIELD(fieldnum);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);

	READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(void)
{
	READ_LOCALS(FieldStore);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(fieldnums);
	READ_OID_FIELD(resulttype);

	READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(void)
{
	READ_LOCALS(RelabelType);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(relabelformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO *
_readCoerceViaIO(void)
{
	READ_LOCALS(CoerceViaIO);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr *
_readArrayCoerceExpr(void)
{
	READ_LOCALS(ArrayCoerceExpr);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(elemexpr);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(void)
{
	READ_LOCALS(ConvertRowtypeExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_ENUM_FIELD(convertformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr *
_readCollateExpr(void)
{
	READ_LOCALS(CollateExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(collOid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(void)
{
	READ_LOCALS(CaseExpr);

	READ_OID_FIELD(casetype);
	READ_OID_FIELD(casecollid);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(defresult);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(void)
{
	READ_LOCALS(CaseWhen);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(result);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(void)
{
	READ_LOCALS(CaseTestExpr);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);

	READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(void)
{
	READ_LOCALS(ArrayExpr);

	READ_OID_FIELD(array_typeid);
	READ_OID_FIELD(array_collid);
	READ_OID_FIELD(element_typeid);
	READ_NODE_FIELD(elements);
	READ_BOOL_FIELD(multidims);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(void)
{
	READ_LOCALS(RowExpr);

	READ_NODE_FIELD(args);
	READ_OID_FIELD(row_typeid);
	READ_ENUM_FIELD(row_format, CoercionForm);
	READ_NODE_FIELD(colnames);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(void)
{
	READ_LOCALS(RowCompareExpr);

	READ_ENUM_FIELD(rctype, RowCompareType);
	READ_NODE_FIELD(opnos);
	READ_NODE_FIELD(opfamilies);
	READ_NODE_FIELD(inputcollids);
	READ_NODE_FIELD(largs);
	READ_NODE_FIELD(rargs);

	READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(void)
{
	READ_LOCALS(CoalesceExpr);

	READ_OID_FIELD(coalescetype);
	READ_OID_FIELD(coalescecollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(void)
{
	READ_LOCALS(MinMaxExpr);

	READ_OID_FIELD(minmaxtype);
	READ_OID_FIELD(minmaxcollid);
	READ_OID_FIELD(inputcollid);
	READ_ENUM_FIELD(op, MinMaxOp);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSQLValueFunction
 */
static SQLValueFunction *
_readSQLValueFunction(void)
{
	READ_LOCALS(SQLValueFunction);

	READ_ENUM_FIELD(op, SQLValueFunctionOp);
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr *
_readXmlExpr(void)
{
	READ_LOCALS(XmlExpr);

	READ_ENUM_FIELD(op, XmlExprOp);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(named_args);
	READ_NODE_FIELD(arg_names);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(xmloption, XmlOptionType);
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(void)
{
	READ_LOCALS(NullTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(nulltesttype, NullTestType);
	READ_BOOL_FIELD(argisrow);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(void)
{
	READ_LOCALS(BooleanTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(booltesttype, BoolTestType);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(void)
{
	READ_LOCALS(CoerceToDomain);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coercionformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(void)
{
	READ_LOCALS(CoerceToDomainValue);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(void)
{
	READ_LOCALS(SetToDefault);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(void)
{
	READ_LOCALS(CurrentOfExpr);

	READ_UINT_FIELD(cvarno);
	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(cursor_param);
	READ_OID_FIELD(target_relid);

	READ_DONE();
}

/*
 * _readNextValueExpr
 */
static NextValueExpr *
_readNextValueExpr(void)
{
	READ_LOCALS(NextValueExpr);

	READ_OID_FIELD(seqid);
	READ_OID_FIELD(typeId);

	READ_DONE();
}

/*
 * _readInferenceElem
 */
static InferenceElem *
_readInferenceElem(void)
{
	READ_LOCALS(InferenceElem);

	READ_NODE_FIELD(expr);
	READ_OID_FIELD(infercollid);
	READ_OID_FIELD(inferopclass);

	READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(void)
{
	READ_LOCALS(TargetEntry);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(resno);
	READ_STRING_FIELD(resname);
	READ_UINT_FIELD(ressortgroupref);
	READ_OID_FIELD(resorigtbl);
	READ_INT_FIELD(resorigcol);
	READ_BOOL_FIELD(resjunk);

	READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(void)
{
	READ_LOCALS(RangeTblRef);

	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(void)
{
	READ_LOCALS(JoinExpr);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(isNatural);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(usingClause);
	READ_NODE_FIELD(join_using_alias);
	READ_NODE_FIELD(quals);
	READ_NODE_FIELD(alias);
	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(void)
{
	READ_LOCALS(FromExpr);

	READ_NODE_FIELD(fromlist);
	READ_NODE_FIELD(quals);

	READ_DONE();
}

/*
 * _readOnConflictExpr
 */
static OnConflictExpr *
_readOnConflictExpr(void)
{
	READ_LOCALS(OnConflictExpr);

	READ_ENUM_FIELD(action, OnConflictAction);
	READ_NODE_FIELD(arbiterElems);
	READ_NODE_FIELD(arbiterWhere);
	READ_OID_FIELD(constraint);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictWhere);
	READ_INT_FIELD(exclRelIndex);
	READ_NODE_FIELD(exclRelTlist);

	READ_DONE();
}

/*
 *	Stuff from pathnodes.h.
 *
 * Mostly we don't need to read planner nodes back in again, but some
 * of these also end up in plan trees.
 */

/*
 * _readAppendRelInfo
 */
static AppendRelInfo *
_readAppendRelInfo(void)
{
	READ_LOCALS(AppendRelInfo);

	READ_UINT_FIELD(parent_relid);
	READ_UINT_FIELD(child_relid);
	READ_OID_FIELD(parent_reltype);
	READ_OID_FIELD(child_reltype);
	READ_NODE_FIELD(translated_vars);
	READ_INT_FIELD(num_child_cols);
	READ_ATTRNUMBER_ARRAY(parent_colnos, local_node->num_child_cols);
	READ_OID_FIELD(parent_reloid);

	READ_DONE();
}

/*
 *	Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(void)
{
	READ_LOCALS(RangeTblEntry);

	/* put alias + eref first to make dump more legible */
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(eref);
	READ_ENUM_FIELD(rtekind, RTEKind);
	READ_BOOL_FIELD(relisivm);

	switch (local_node->rtekind)
	{
		case RTE_RELATION:
			READ_OID_FIELD(relid);
			READ_CHAR_FIELD(relkind);
			READ_INT_FIELD(rellockmode);
			READ_NODE_FIELD(tablesample);
			READ_UINT_FIELD(perminfoindex);
			break;
		case RTE_SUBQUERY:
			READ_NODE_FIELD(subquery);
			READ_BOOL_FIELD(security_barrier);
			READ_OID_FIELD(relid);
			READ_CHAR_FIELD(relkind);
			READ_INT_FIELD(rellockmode);
			READ_UINT_FIELD(perminfoindex);
			break;
		case RTE_JOIN:
			READ_ENUM_FIELD(jointype, JoinType);
			READ_INT_FIELD(joinmergedcols);
			READ_NODE_FIELD(joinaliasvars);
			READ_NODE_FIELD(joinleftcols);
			READ_NODE_FIELD(joinrightcols);
			READ_NODE_FIELD(join_using_alias);
			break;
		case RTE_FUNCTION:
			READ_NODE_FIELD(functions);
			READ_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNCTION:
			READ_NODE_FIELD(subquery);
			READ_NODE_FIELD(functions);
			READ_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			READ_NODE_FIELD(tablefunc);
			/* The RTE must have a copy of the column type info, if any */
			if (local_node->tablefunc)
			{
				TableFunc  *tf = local_node->tablefunc;

				local_node->coltypes = tf->coltypes;
				local_node->coltypmods = tf->coltypmods;
				local_node->colcollations = tf->colcollations;
			}
			break;
		case RTE_VALUES:
			READ_NODE_FIELD(values_lists);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			READ_STRING_FIELD(ctename);
			READ_UINT_FIELD(ctelevelsup);
			READ_BOOL_FIELD(self_reference);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			READ_STRING_FIELD(enrname);
			READ_FLOAT_FIELD(enrtuples);
			READ_OID_FIELD(relid);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_RESULT:
			/* no extra fields */
			break;
        case RTE_VOID:                                                  /*CDB*/
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) local_node->rtekind);
			break;
	}

	READ_BOOL_FIELD(lateral);
	READ_BOOL_FIELD(inh);
	READ_BOOL_FIELD(inFromCl);
	READ_NODE_FIELD(securityQuals);

	READ_BOOL_FIELD(forceDistRandom);

	READ_DONE();
}

/*
 * _readRangeTblFunction
 */
static RangeTblFunction *
_readRangeTblFunction(void)
{
	READ_LOCALS(RangeTblFunction);

	READ_NODE_FIELD(funcexpr);
	READ_INT_FIELD(funccolcount);
	READ_NODE_FIELD(funccolnames);
	READ_NODE_FIELD(funccoltypes);
	READ_NODE_FIELD(funccoltypmods);
	READ_NODE_FIELD(funccolcollations);
	/* funcuserdata is only serialized in binary out/read functions */
#ifdef COMPILING_BINARY_FUNCS
	READ_BYTEA_FIELD(funcuserdata);
#endif
	READ_BITMAPSET_FIELD(funcparams);

	READ_DONE();
}

/*
 * Apache Cloudberry additions for serialization support
 * These are currently not used (see outfastc ad readfast.c)
 */
#include "nodes/plannodes.h"

/*
 * _readTableSampleClause
 */
static TableSampleClause *
_readTableSampleClause(void)
{
	READ_LOCALS(TableSampleClause);

	READ_OID_FIELD(tsmhandler);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(repeatable);

	READ_DONE();
}

/*
 * _readDefElem
 */
static DefElem *
_readDefElem(void)
{
	READ_LOCALS(DefElem);

	READ_STRING_FIELD(defnamespace);
	READ_STRING_FIELD(defname);
	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(defaction, DefElemAction);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 *	Stuff from plannodes.h.
 */

/*
 * _readPlannedStmt
 */
static PlannedStmt *
_readPlannedStmt(void)
{
	READ_LOCALS(PlannedStmt);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(planGen, PlanGenerator);
	READ_UINT64_FIELD(queryId);
	READ_BOOL_FIELD(hasReturning);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(canSetTag);
	READ_BOOL_FIELD(transientPlan);
	READ_BOOL_FIELD(oneoffPlan);
	READ_OID_FIELD(simplyUpdatableRel);
	READ_BOOL_FIELD(dependsOnRole);
	READ_BOOL_FIELD(parallelModeNeeded);
	READ_INT_FIELD(jitFlags);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(permInfos);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(appendRelations);
	READ_NODE_FIELD(subplans);
	READ_BITMAPSET_FIELD(rewindPlanIDs);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(relationOids);
	/* invalItems not serialized in binary mode */
#ifndef COMPILING_BINARY_FUNCS
	READ_NODE_FIELD(invalItems);
#endif /* COMPILING_BINARY_FUNCS */
	READ_NODE_FIELD(paramExecTypes);
	READ_NODE_FIELD(utilityStmt);
	READ_LOCATION_FIELD(stmt_location);
	READ_INT_FIELD(stmt_len);

	READ_INT_ARRAY(subplan_sliceIds, list_length(local_node->subplans));

	READ_INT_FIELD(numSlices);
	local_node->slices = palloc(local_node->numSlices * sizeof(PlanSlice));
	for (int i = 0; i < local_node->numSlices; i++)
	{
		READ_INT_FIELD(slices[i].sliceIndex);
		READ_INT_FIELD(slices[i].parentIndex);
		READ_INT_FIELD(slices[i].gangType);
		READ_INT_FIELD(slices[i].numsegments);
		READ_INT_FIELD(slices[i].parallel_workers);
		READ_INT_FIELD(slices[i].segindex);
		READ_BOOL_FIELD(slices[i].directDispatch.isDirectDispatch);
		READ_NODE_FIELD(slices[i].directDispatch.contentIds);
	}

	READ_BITMAPSET_FIELD(rewindPlanIDs);

	READ_NODE_FIELD(intoPolicy);

	READ_UINT64_FIELD(query_mem);

	READ_NODE_FIELD(intoClause);
	READ_NODE_FIELD(copyIntoClause);
	READ_NODE_FIELD(refreshClause);
	READ_INT_FIELD(metricsQueryType);
	READ_NODE_FIELD(extensionContext);

	READ_DONE();
}

/*
 * ReadCommonPlan
 *	Assign the basic stuff of all nodes that inherit from Plan
 */
static void
ReadCommonPlan(Plan *local_node)
{
	READ_TEMP_LOCALS();

	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(total_cost);
	READ_FLOAT_FIELD(plan_rows);
	READ_INT_FIELD(plan_width);
	READ_BOOL_FIELD(parallel_aware);
	READ_BOOL_FIELD(parallel_safe);
	READ_BOOL_FIELD(async_capable);
	READ_INT_FIELD(plan_node_id);
	READ_NODE_FIELD(targetlist);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(lefttree);
	READ_NODE_FIELD(righttree);
	READ_NODE_FIELD(initPlan);
	READ_BITMAPSET_FIELD(extParam);
	READ_BITMAPSET_FIELD(allParam);

#ifndef COMPILING_BINARY_FUNCS
	READ_NODE_FIELD(flow);
#endif /* COMPILING_BINARY_FUNCS */

	READ_UINT64_FIELD(operatorMemKB);
}

/*
 * _readPlan
 */
static Plan *
_readPlan(void)
{
	READ_LOCALS_NO_FIELDS(Plan);

	ReadCommonPlan(local_node);

	READ_DONE();
}

/*
 * _readResult
 */
static Result *
_readResult(void)
{
	READ_LOCALS(Result);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(resconstantqual);

	READ_INT_FIELD(numHashFilterCols);
	READ_ATTRNUMBER_ARRAY(hashFilterColIdx, local_node->numHashFilterCols);
	READ_OID_ARRAY(hashFilterFuncs, local_node->numHashFilterCols);

	READ_DONE();
}

/*
 * _readProjectSet
 */
static ProjectSet *
_readProjectSet(void)
{
	READ_LOCALS_NO_FIELDS(ProjectSet);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readModifyTable
 */
static ModifyTable *
_readModifyTable(void)
{
	READ_LOCALS(ModifyTable);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_BOOL_FIELD(canSetTag);
	READ_UINT_FIELD(nominalRelation);
	READ_UINT_FIELD(rootRelation);
	READ_BOOL_FIELD(partColsUpdated);
	READ_BOOL_FIELD(splitUpdate);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(updateColnosLists);
	READ_NODE_FIELD(withCheckOptionLists);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(fdwPrivLists);
	READ_BITMAPSET_FIELD(fdwDirectModifyPlans);
	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);
	READ_ENUM_FIELD(onConflictAction, OnConflictAction);
	READ_NODE_FIELD(arbiterIndexes);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictCols);
	READ_NODE_FIELD(onConflictWhere);
	READ_UINT_FIELD(exclRelRTI);
	READ_NODE_FIELD(exclRelTlist);
	READ_BOOL_FIELD(forceTupleRouting);
	READ_NODE_FIELD(mergeActionLists);

	READ_DONE();
}

/*
 * _readAppend
 */
static Append *
_readAppend(void)
{
	READ_LOCALS(Append);

	ReadCommonPlan(&local_node->plan);

	READ_BITMAPSET_FIELD(apprelids);
	READ_NODE_FIELD(appendplans);
	READ_INT_FIELD(nasyncplans);
	READ_INT_FIELD(first_partial_plan);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readMergeAppend
 */
static MergeAppend *
_readMergeAppend(void)
{
	READ_LOCALS(MergeAppend);

	ReadCommonPlan(&local_node->plan);

	READ_BITMAPSET_FIELD(apprelids);
	READ_NODE_FIELD(mergeplans);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readRecursiveUnion
 */
static RecursiveUnion *
_readRecursiveUnion(void)
{
	READ_LOCALS(RecursiveUnion);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(wtParam);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
	READ_OID_ARRAY(dupOperators, local_node->numCols);
	READ_OID_ARRAY(dupCollations, local_node->numCols);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readBitmapAnd
 */
static BitmapAnd *
_readBitmapAnd(void)
{
	READ_LOCALS(BitmapAnd);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}

/*
 * _readBitmapOr
 */
static BitmapOr *
_readBitmapOr(void)
{
	READ_LOCALS(BitmapOr);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}

/*
 * ReadCommonScan
 *	Assign the basic stuff of all nodes that inherit from Scan
 */
static void
ReadCommonScan(Scan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(scanrelid);
}

/*
 * _readScan
 */
static Scan *
_readScan(void)
{
	READ_LOCALS_NO_FIELDS(Scan);

	ReadCommonScan(local_node);

	READ_DONE();
}

/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(void)
{
	READ_LOCALS_NO_FIELDS(SeqScan);

	ReadCommonScan(&local_node->scan);

	READ_DONE();
}

/*
 * _readSampleScan
 */
static SampleScan *
_readSampleScan(void)
{
	READ_LOCALS(SampleScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablesample);

	READ_DONE();
}

/*
 * _readIndexScan
 */

static void readIndexScanFields(IndexScan *local_node);

static IndexScan *
_readIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(IndexScan);

	readIndexScanFields(local_node);

	READ_DONE();
}

static DynamicIndexScan *
_readDynamicIndexScan(void)
{
	READ_LOCALS(DynamicIndexScan);
	/* DynamicIndexScan has some content from IndexScan. */
	readIndexScanFields(&local_node->indexscan);
	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);
	READ_DONE();
}
static void
readIndexScanFields(IndexScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indexorderbyorig);
	READ_NODE_FIELD(indexorderbyops);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);
}

/*
 * _readIndexOnlyScan
 */
static void readIndexOnlyScanFields(IndexOnlyScan *local_node);

static IndexOnlyScan *
_readIndexOnlyScan(void)
{
	READ_LOCALS_NO_FIELDS(IndexOnlyScan);
	readIndexOnlyScanFields(local_node);
	READ_DONE();
}

static void
readIndexOnlyScanFields(IndexOnlyScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(recheckqual);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indextlist);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);
}

static DynamicIndexOnlyScan *
_readDynamicIndexOnlyScan(void)
{
	READ_LOCALS(DynamicIndexOnlyScan);

	/* DynamicIndexScan has some content from IndexScan. */
	readIndexOnlyScanFields(&local_node->indexscan);
	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);
	READ_DONE();
}

static void
readBitmapIndexScanFields(BitmapIndexScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
}

/*
 * _readBitmapIndexScan
 */
static BitmapIndexScan *
_readBitmapIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(BitmapIndexScan);

	readBitmapIndexScanFields(local_node);

	READ_DONE();
}

static DynamicBitmapIndexScan *
_readDynamicBitmapIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(DynamicBitmapIndexScan);

	/* DynamicBitmapIndexScan has some content from BitmapIndexScan. */
	readBitmapIndexScanFields(&local_node->biscan);

	READ_DONE();
}

static void
readBitmapHeapScanFields(BitmapHeapScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(bitmapqualorig);
}

/*
 * _readBitmapHeapScan
 */
static BitmapHeapScan *
_readBitmapHeapScan(void)
{
	READ_LOCALS_NO_FIELDS(BitmapHeapScan);

	readBitmapHeapScanFields(local_node);

	READ_DONE();
}


static DynamicBitmapHeapScan *
_readDynamicBitmapHeapScan(void)
{
	READ_LOCALS(DynamicBitmapHeapScan);

	/* DynamicBitmapHeapScan has some content from BitmapHeapScan. */
	readBitmapHeapScanFields(&local_node->bitmapheapscan);

	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readTidScan
 */
static TidScan *
_readTidScan(void)
{
	READ_LOCALS(TidScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tidquals);

	READ_DONE();
}

/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(void)
{
	READ_LOCALS(SubqueryScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(subplan);

	READ_DONE();
}

/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(void)
{
	READ_LOCALS(FunctionScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(functions);
	READ_BOOL_FIELD(funcordinality);
	READ_NODE_FIELD(param);
	READ_BOOL_FIELD(resultInTupleStore);
	READ_INT_FIELD(initplanId);

	READ_DONE();
}

/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(void)
{
	READ_LOCALS(ValuesScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(values_lists);

	READ_DONE();
}

/*
 * _readTableFuncScan
 */
static TableFuncScan *
_readTableFuncScan(void)
{
	READ_LOCALS(TableFuncScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablefunc);

	READ_DONE();
}

/*
 * _readCteScan
 */
static CteScan *
_readCteScan(void)
{
	READ_LOCALS(CteScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(ctePlanId);
	READ_INT_FIELD(cteParam);

	READ_DONE();
}

/*
 * _readNamedTuplestoreScan
 */
static NamedTuplestoreScan *
_readNamedTuplestoreScan(void)
{
	READ_LOCALS(NamedTuplestoreScan);

	ReadCommonScan(&local_node->scan);

	READ_STRING_FIELD(enrname);

	READ_DONE();
}

/*
 * _readWorkTableScan
 */
static WorkTableScan *
_readWorkTableScan(void)
{
	READ_LOCALS(WorkTableScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(wtParam);

	READ_DONE();
}


static void readForeignScanFields(ForeignScan *local_node);

/*
 * _readForeignScan
 */
static ForeignScan *
_readForeignScan(void)
{
	READ_LOCALS_NO_FIELDS(ForeignScan);
	readForeignScanFields(local_node);
	READ_DONE();
}

static DynamicForeignScan *
_readDynamicForeignScan(void)
{
	READ_LOCALS(DynamicForeignScan);
	/* DynamicForeignScan has some content from ForeignScan. */
	readForeignScanFields(&local_node->foreignscan);
	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);
	READ_NODE_FIELD(fdw_private_list);
	READ_DONE();
}

static void
readForeignScanFields(ForeignScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_UINT_FIELD(resultRelation);
	READ_OID_FIELD(fs_server);
	READ_NODE_FIELD(fdw_exprs);
	READ_NODE_FIELD(fdw_private);
	READ_NODE_FIELD(fdw_scan_tlist);
	READ_NODE_FIELD(fdw_recheck_quals);
	READ_BITMAPSET_FIELD(fs_relids);
	READ_BOOL_FIELD(fsSystemCol);
}


#ifndef COMPILING_BINARY_FUNCS
/*
 * _readCustomScan
 */
static CustomScan *
_readCustomScan(void)
{
	READ_LOCALS(CustomScan);
	char	   *custom_name;
	const CustomScanMethods *methods;

	ReadCommonScan(&local_node->scan);

	READ_UINT_FIELD(flags);
	READ_NODE_FIELD(custom_plans);
	READ_NODE_FIELD(custom_exprs);
	READ_NODE_FIELD(custom_private);
	READ_NODE_FIELD(custom_scan_tlist);
	READ_BITMAPSET_FIELD(custom_relids);

	/* Lookup CustomScanMethods by CustomName */
	token = pg_strtok(&length); /* skip methods: */
	token = pg_strtok(&length); /* CustomName */
	custom_name = nullable_string(token, length);
	methods = GetCustomScanMethods(custom_name, false);
	local_node->methods = methods;

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * ReadCommonJoin
 *	Assign the basic stuff of all nodes that inherit from Join
 */
static void
ReadCommonJoin(Join *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(prefetch_inner);
	READ_BOOL_FIELD(prefetch_joinqual);
	READ_BOOL_FIELD(prefetch_qual);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(inner_unique);
	READ_NODE_FIELD(joinqual);
}

/*
 * _readJoin
 */
static Join *
_readJoin(void)
{
	READ_LOCALS_NO_FIELDS(Join);

	ReadCommonJoin(local_node);

	READ_DONE();
}

/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(void)
{
	READ_LOCALS(NestLoop);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(nestParams);

	READ_BOOL_FIELD(shared_outer);
	READ_BOOL_FIELD(singleton_outer); /*CDB-OLAP*/

	READ_DONE();
}

/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(void)
{
	int			numCols;

	READ_LOCALS(MergeJoin);

	ReadCommonJoin(&local_node->join);

	READ_BOOL_FIELD(skip_mark_restore);
	READ_NODE_FIELD(mergeclauses);

	numCols = list_length(local_node->mergeclauses);

	READ_OID_ARRAY(mergeFamilies, numCols);
	READ_OID_ARRAY(mergeCollations, numCols);
	READ_INT_ARRAY(mergeStrategies, numCols);
	READ_BOOL_ARRAY(mergeNullsFirst, numCols);
	READ_BOOL_FIELD(unique_outer);

	READ_DONE();
}

/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(void)
{
	READ_LOCALS(HashJoin);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(hashclauses);
	READ_NODE_FIELD(hashoperators);
	READ_NODE_FIELD(hashcollations);
	READ_NODE_FIELD(hashkeys);
	READ_NODE_FIELD(hashqualclauses);
	READ_BOOL_FIELD(batch0_barrier);
	READ_BOOL_FIELD(outer_motionhazard);

	READ_DONE();
}

/*
 * _readMaterial
 */
static Material *
_readMaterial(void)
{
	READ_LOCALS(Material);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(cdb_strict);
	READ_BOOL_FIELD(cdb_shield_child_from_rescans);

	READ_DONE();
}

/*
 * ReadCommonSort
 *	Assign the basic stuff of all nodes that inherit from Sort
 */
static void
ReadCommonSort(Sort *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
}

/*
 * _readSort
 */
static Sort *
_readSort(void)
{
	READ_LOCALS_NO_FIELDS(Sort);

	ReadCommonSort(local_node);

	READ_DONE();
}

/*
 * _readIncrementalSort
 */
static IncrementalSort *
_readIncrementalSort(void)
{
	READ_LOCALS(IncrementalSort);

	ReadCommonSort(&local_node->sort);

	READ_INT_FIELD(nPresortedCols);

	READ_DONE();
}

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readGroup
 */
static Group *
_readGroup(void)
{
	READ_LOCALS(Group);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);
	READ_OID_ARRAY(grpOperators, local_node->numCols);
	READ_OID_ARRAY(grpCollations, local_node->numCols);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readAgg
 */
static Agg *
_readAgg(void)
{
	READ_LOCALS(Agg);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(aggstrategy, AggStrategy);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);
	READ_OID_ARRAY(grpOperators, local_node->numCols);
	READ_OID_ARRAY(grpCollations, local_node->numCols);
	READ_LONG_FIELD(numGroups);
	READ_UINT64_FIELD(transitionSpace);
	READ_BITMAPSET_FIELD(aggParams);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(chain);
	READ_BOOL_FIELD(streaming);
	READ_UINT_FIELD(agg_expr_id);

	READ_DONE();
}

/*
 * _readWindowAgg
 */
static WindowAgg *
_readWindowAgg(void)
{
	READ_LOCALS(WindowAgg);

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(winref);
	READ_INT_FIELD(partNumCols);
	READ_ATTRNUMBER_ARRAY(partColIdx, local_node->partNumCols);
	READ_OID_ARRAY(partOperators, local_node->partNumCols);
	READ_OID_ARRAY(partCollations, local_node->partNumCols);
	READ_INT_FIELD(ordNumCols);
	READ_ATTRNUMBER_ARRAY(ordColIdx, local_node->ordNumCols);
	READ_OID_ARRAY(ordOperators, local_node->ordNumCols);
	READ_OID_ARRAY(ordCollations, local_node->ordNumCols);
	READ_INT_FIELD(firstOrderCol);
	READ_OID_FIELD(firstOrderCmpOperator);
	READ_BOOL_FIELD(firstOrderNullsFirst);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_NODE_FIELD(runCondition);
	READ_NODE_FIELD(runConditionOrig);
	READ_OID_FIELD(startInRangeFunc);
	READ_OID_FIELD(endInRangeFunc);
	READ_OID_FIELD(inRangeColl);
	READ_BOOL_FIELD(inRangeAsc);
	READ_BOOL_FIELD(inRangeNullsFirst);
	READ_BOOL_FIELD(topWindow);

	READ_DONE();
}

/*
 * _readUnique
 */
static Unique *
_readUnique(void)
{
	READ_LOCALS(Unique);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->numCols);
	READ_OID_ARRAY(uniqOperators, local_node->numCols);
	READ_OID_ARRAY(uniqCollations, local_node->numCols);

	READ_DONE();
}

/*
 * _readGather
 */
static Gather *
_readGather(void)
{
	READ_LOCALS(Gather);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_BOOL_FIELD(single_copy);
	READ_BOOL_FIELD(invisible);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readGatherMerge
 */
static GatherMerge *
_readGatherMerge(void)
{
	READ_LOCALS(GatherMerge);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readHash
 */
static Hash *
_readHash(void)
{
	READ_LOCALS(Hash);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(hashkeys);
	READ_OID_FIELD(skewTable);
	READ_INT_FIELD(skewColumn);
	READ_BOOL_FIELD(skewInherit);
	READ_FLOAT_FIELD(rows_total);
    READ_BOOL_FIELD(rescannable);           /*CDB*/
    READ_BOOL_FIELD(sync_barrier); 

	READ_DONE();
}

/*
 * _readSetOp
 */
static SetOp *
_readSetOp(void)
{
	READ_LOCALS(SetOp);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(cmd, SetOpCmd);
	READ_ENUM_FIELD(strategy, SetOpStrategy);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
	READ_OID_ARRAY(dupOperators, local_node->numCols);
	READ_OID_ARRAY(dupCollations, local_node->numCols);
	READ_INT_FIELD(flagColIdx);
	READ_INT_FIELD(firstFlag);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readLockRows
 */
static LockRows *
_readLockRows(void)
{
	READ_LOCALS(LockRows);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);

	READ_DONE();
}

static RuntimeFilter *
_readRuntimeFilter(void)
{
	READ_LOCALS_NO_FIELDS(RuntimeFilter);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readLimit
 */
static Limit *
_readLimit(void)
{
	READ_LOCALS(Limit);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_ENUM_FIELD(limitOption, LimitOption);
	READ_INT_FIELD(uniqNumCols);
	READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->uniqNumCols);
	READ_OID_ARRAY(uniqOperators, local_node->uniqNumCols);
	READ_OID_ARRAY(uniqCollations, local_node->uniqNumCols);

	READ_DONE();
}

/*
 * _readNestLoopParam
 */
static NestLoopParam *
_readNestLoopParam(void)
{
	READ_LOCALS(NestLoopParam);

	READ_INT_FIELD(paramno);
	READ_NODE_FIELD(paramval);

	READ_DONE();
}

/*
 * _readPlanRowMark
 */
static PlanRowMark *
_readPlanRowMark(void)
{
	READ_LOCALS(PlanRowMark);

	READ_UINT_FIELD(rti);
	READ_UINT_FIELD(prti);
	READ_UINT_FIELD(rowmarkId);
	READ_ENUM_FIELD(markType, RowMarkType);
	READ_INT_FIELD(allMarkTypes);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	READ_BOOL_FIELD(isParent);

	READ_DONE();
}

static PartitionPruneInfo *
_readPartitionPruneInfo(void)
{
	READ_LOCALS(PartitionPruneInfo);

	READ_NODE_FIELD(prune_infos);
	READ_BITMAPSET_FIELD(other_subplans);

	READ_DONE();
}

static PartitionedRelPruneInfo *
_readPartitionedRelPruneInfo(void)
{
	READ_LOCALS(PartitionedRelPruneInfo);

	READ_UINT_FIELD(rtindex);
	READ_BITMAPSET_FIELD(present_parts);
	READ_INT_FIELD(nparts);
	READ_INT_ARRAY(subplan_map, local_node->nparts);
	READ_INT_ARRAY(subpart_map, local_node->nparts);
	READ_OID_ARRAY(relid_map, local_node->nparts);
	READ_NODE_FIELD(initial_pruning_steps);
	READ_NODE_FIELD(exec_pruning_steps);
	READ_BITMAPSET_FIELD(execparamids);

	READ_DONE();
}

static PartitionPruneStepOp *
_readPartitionPruneStepOp(void)
{
	READ_LOCALS(PartitionPruneStepOp);

	READ_INT_FIELD(step.step_id);
	READ_INT_FIELD(opstrategy);
	READ_NODE_FIELD(exprs);
	READ_NODE_FIELD(cmpfns);
	READ_BITMAPSET_FIELD(nullkeys);

	READ_DONE();
}

static PartitionPruneStepCombine *
_readPartitionPruneStepCombine(void)
{
	READ_LOCALS(PartitionPruneStepCombine);

	READ_INT_FIELD(step.step_id);
	READ_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
	READ_NODE_FIELD(source_stepids);

	READ_DONE();
}

/*
 * _readPlanInvalItem
 */
static PlanInvalItem *
_readPlanInvalItem(void)
{
	READ_LOCALS(PlanInvalItem);

	READ_INT_FIELD(cacheId);
	READ_UINT_FIELD(hashValue);

	READ_DONE();
}

/*
 * _readSubPlan
 */
static SubPlan *
_readSubPlan(void)
{
	READ_LOCALS(SubPlan);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(paramIds);
	READ_INT_FIELD(plan_id);
	READ_STRING_FIELD(plan_name);
	READ_OID_FIELD(firstColType);
	READ_INT_FIELD(firstColTypmod);
	READ_OID_FIELD(firstColCollation);
	READ_BOOL_FIELD(useHashTable);
	READ_BOOL_FIELD(unknownEqFalse);
	READ_BOOL_FIELD(parallel_safe);
	READ_BOOL_FIELD(is_initplan); /*CDB*/
	READ_BOOL_FIELD(is_multirow); /*CDB*/
	READ_NODE_FIELD(setParam);
	READ_NODE_FIELD(parParam);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(extParam);
	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(per_call_cost);

	READ_DONE();
}

/*
 * _readAlternativeSubPlan
 */
static AlternativeSubPlan *
_readAlternativeSubPlan(void)
{
	READ_LOCALS(AlternativeSubPlan);

	READ_NODE_FIELD(subplans);

	READ_DONE();
}

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readExtensibleNode
 */
static ExtensibleNode *
_readExtensibleNode(void)
{
	const ExtensibleNodeMethods *methods;
	ExtensibleNode *local_node;
	const char *extnodename;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length); /* skip :extnodename */
	token = pg_strtok(&length); /* get extnodename */

	extnodename = nullable_string(token, length);
	if (!extnodename)
		elog(ERROR, "extnodename has to be supplied");
	methods = GetExtensibleNodeMethods(extnodename, false);

	local_node = (ExtensibleNode *) newNode(methods->node_size,
											T_ExtensibleNode);
	local_node->extnodename = extnodename;

	/* deserialize the private fields */
	methods->nodeRead(local_node);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */

/*
 * _readPartitionBoundSpec
 */
static PartitionBoundSpec *
_readPartitionBoundSpec(void)
{
	READ_LOCALS(PartitionBoundSpec);

	READ_CHAR_FIELD(strategy);
	READ_BOOL_FIELD(is_default);
	READ_INT_FIELD(modulus);
	READ_INT_FIELD(remainder);
	READ_NODE_FIELD(listdatums);
	READ_NODE_FIELD(lowerdatums);
	READ_NODE_FIELD(upperdatums);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readPartitionRangeDatum
 */
static PartitionRangeDatum *
_readPartitionRangeDatum(void)
{
	READ_LOCALS(PartitionRangeDatum);

	READ_ENUM_FIELD(kind, PartitionRangeDatumKind);
	READ_NODE_FIELD(value);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static RTEPermissionInfo *
_readRTEPermissionInfo(void)
{
	READ_LOCALS(RTEPermissionInfo);

	READ_OID_FIELD(relid);
	READ_BOOL_FIELD(inh);
	READ_UINT64_FIELD(requiredPerms);
	READ_OID_FIELD(checkAsUser);
	READ_BITMAPSET_FIELD(selectedCols);
	READ_BITMAPSET_FIELD(insertedCols);
	READ_BITMAPSET_FIELD(updatedCols);

	READ_DONE();
}

/*
 * _readGpPolicy
 */
static GpPolicy *
_readGpPolicy(void)
{
	READ_LOCALS(GpPolicy);

	READ_ENUM_FIELD(ptype, GpPolicyType);

	READ_INT_FIELD(numsegments);

	READ_INT_FIELD(nattrs);
	READ_ATTRNUMBER_ARRAY(attrs, local_node->nattrs);
	READ_OID_ARRAY(opclasses, local_node->nattrs);

	READ_DONE();
}

static MergeAction *
_readMergeAction(void)
{
	READ_LOCALS(MergeAction);

	READ_BOOL_FIELD(matched);
	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(updateColnos);

	READ_DONE();
}

static PublicationObjSpec *
_readPublicationObjSpec(void)
{
	READ_LOCALS(PublicationObjSpec);

	READ_ENUM_FIELD(pubobjtype, PublicationObjSpecType);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(pubtable);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static PublicationTable *
_readPublicationTable(void)
{
	READ_LOCALS(PublicationTable);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(columns);

	READ_DONE();
}

static WindowDef *
_readWindowDef(void)
{
	READ_LOCALS(WindowDef);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static JsonConstructorExpr *
_readJsonConstructorExpr(void)
{
	READ_LOCALS(JsonConstructorExpr);

	READ_ENUM_FIELD(type, JsonConstructorType);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(func);
	READ_NODE_FIELD(coercion);
	READ_NODE_FIELD(returning);
	READ_BOOL_FIELD(absent_on_null);
	READ_BOOL_FIELD(unique);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static JsonIsPredicate *
_readJsonIsPredicate(void)
{
	READ_LOCALS(JsonIsPredicate);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(format);
	READ_ENUM_FIELD(item_type, JsonValueType);
	READ_BOOL_FIELD(unique_keys);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static JsonReturning *
_readJsonReturning(void)
{
	READ_LOCALS(JsonReturning);

	READ_NODE_FIELD(format);
	READ_OID_FIELD(typid);
	READ_INT_FIELD(typmod);

	READ_DONE();
}

static JsonValueExpr *
_readJsonValueExpr(void)
{
	READ_LOCALS(JsonValueExpr);

	READ_NODE_FIELD(raw_expr);
	READ_NODE_FIELD(formatted_expr);
	READ_NODE_FIELD(format);

	READ_DONE();
}

static JsonFormat *
_readJsonFormat(void)
{
	READ_LOCALS(JsonFormat);

	READ_ENUM_FIELD(format_type, JsonFormatType);
	READ_ENUM_FIELD(encoding, JsonEncoding);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#include "readfuncs_common.c"
#ifndef COMPILING_BINARY_FUNCS
/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node *
parseNodeString(void)
{
	void	   *return_value;

	READ_TEMP_LOCALS();

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	token = pg_strtok(&length);

#define MATCH(tokname, namelen) \
	(length == namelen && memcmp(token, tokname, namelen) == 0)

	/*
	 * Same as MATCH, but we make our life a bit easier by relying on the
	 * compiler to be smart, and evaluate the strlen("<constant>") at
	 * compilation time for us.
	 */
#define MATCHX(tokname) \
	(length == strlen(tokname) && strncmp(token, tokname, strlen(tokname)) == 0)

	if (MATCH("QUERY", 5))
		return_value = _readQuery();
	else if (MATCH("WITHCHECKOPTION", 15))
		return_value = _readWithCheckOption();
	else if (MATCH("SORTGROUPCLAUSE", 15))
		return_value = _readSortGroupClause();
	else if (MATCH("GROUPINGSET", 11))
		return_value = _readGroupingSet();
	else if (MATCH("WINDOWCLAUSE", 12))
		return_value = _readWindowClause();
	else if (MATCH("ROWMARKCLAUSE", 13))
		return_value = _readRowMarkClause();
	else if (MATCH("CTESEARCHCLAUSE", 15))
		return_value = _readCTESearchClause();
	else if (MATCH("CTECYCLECLAUSE", 14))
		return_value = _readCTECycleClause();
	else if (MATCH("COMMONTABLEEXPR", 15))
		return_value = _readCommonTableExpr();
	else if (MATCH("SETOPERATIONSTMT", 16))
		return_value = _readSetOperationStmt();
	else if (MATCH("ALIAS", 5))
		return_value = _readAlias();
	else if (MATCH("RANGEVAR", 8))
		return_value = _readRangeVar();
	else if (MATCH("INTOCLAUSE", 10))
		return_value = _readIntoClause();
	else if (MATCH("COPYINTOCLAUSE", 14))
		return_value = _readCopyIntoClause();
	else if (MATCH("REFRESHCLAUSE", 13))
		return_value = _readRefreshClause();
	else if (MATCH("TABLEFUNC", 9))
		return_value = _readTableFunc();
	else if (MATCH("VAR", 3))
		return_value = _readVar();
	else if (MATCH("CONST", 5))
		return_value = _readConst();
	else if (MATCH("PARAM", 5))
		return_value = _readParam();
	else if (MATCH("AGGREF", 6))
		return_value = _readAggref();
	else if (MATCH("GROUPINGFUNC", 12))
		return_value = _readGroupingFunc();
	else if (MATCH("GROUPID", 7))
		return_value = _readGroupId();
	else if (MATCH("GROUPINGSETID", 13))
		return_value = _readGroupingSetId();
	else if (MATCH("WINDOWFUNC", 10))
		return_value = _readWindowFunc();
	else if (MATCH("SUBSCRIPTINGREF", 15))
		return_value = _readSubscriptingRef();
	else if (MATCH("FUNCEXPR", 8))
		return_value = _readFuncExpr();
	else if (MATCH("NAMEDARGEXPR", 12))
		return_value = _readNamedArgExpr();
	else if (MATCH("OPEXPR", 6))
		return_value = _readOpExpr();
	else if (MATCH("DISTINCTEXPR", 12))
		return_value = _readDistinctExpr();
	else if (MATCH("NULLIFEXPR", 10))
		return_value = _readNullIfExpr();
	else if (MATCH("SCALARARRAYOPEXPR", 17))
		return_value = _readScalarArrayOpExpr();
	else if (MATCH("BOOLEXPR", 8))
		return_value = _readBoolExpr();
	else if (MATCH("SUBLINK", 7))
		return_value = _readSubLink();
	else if (MATCH("FIELDSELECT", 11))
		return_value = _readFieldSelect();
	else if (MATCH("FIELDSTORE", 10))
		return_value = _readFieldStore();
	else if (MATCH("RELABELTYPE", 11))
		return_value = _readRelabelType();
	else if (MATCH("COERCEVIAIO", 11))
		return_value = _readCoerceViaIO();
	else if (MATCH("ARRAYCOERCEEXPR", 15))
		return_value = _readArrayCoerceExpr();
	else if (MATCH("CONVERTROWTYPEEXPR", 18))
		return_value = _readConvertRowtypeExpr();
	else if (MATCH("COLLATE", 7))
		return_value = _readCollateExpr();
	else if (MATCH("CASE", 4))
		return_value = _readCaseExpr();
	else if (MATCH("WHEN", 4))
		return_value = _readCaseWhen();
	else if (MATCH("CASETESTEXPR", 12))
		return_value = _readCaseTestExpr();
	else if (MATCH("ARRAY", 5))
		return_value = _readArrayExpr();
	else if (MATCH("ROW", 3))
		return_value = _readRowExpr();
	else if (MATCH("ROWCOMPARE", 10))
		return_value = _readRowCompareExpr();
	else if (MATCH("COALESCE", 8))
		return_value = _readCoalesceExpr();
	else if (MATCH("MINMAX", 6))
		return_value = _readMinMaxExpr();
	else if (MATCH("SQLVALUEFUNCTION", 16))
		return_value = _readSQLValueFunction();
	else if (MATCH("XMLEXPR", 7))
		return_value = _readXmlExpr();
	else if (MATCH("NULLTEST", 8))
		return_value = _readNullTest();
	else if (MATCH("BOOLEANTEST", 11))
		return_value = _readBooleanTest();
	else if (MATCH("COERCETODOMAIN", 14))
		return_value = _readCoerceToDomain();
	else if (MATCH("COERCETODOMAINVALUE", 19))
		return_value = _readCoerceToDomainValue();
	else if (MATCH("SETTODEFAULT", 12))
		return_value = _readSetToDefault();
	else if (MATCH("CURRENTOFEXPR", 13))
		return_value = _readCurrentOfExpr();
	else if (MATCH("NEXTVALUEEXPR", 13))
		return_value = _readNextValueExpr();
	else if (MATCH("INFERENCEELEM", 13))
		return_value = _readInferenceElem();
	else if (MATCH("TARGETENTRY", 11))
		return_value = _readTargetEntry();
	else if (MATCH("RANGETBLREF", 11))
		return_value = _readRangeTblRef();
	else if (MATCH("JOINEXPR", 8))
		return_value = _readJoinExpr();
	else if (MATCH("FROMEXPR", 8))
		return_value = _readFromExpr();
	else if (MATCH("ONCONFLICTEXPR", 14))
		return_value = _readOnConflictExpr();
	else if (MATCH("APPENDRELINFO", 13))
		return_value = _readAppendRelInfo();
	else if (MATCH("RTE", 3))
		return_value = _readRangeTblEntry();
	else if (MATCH("RANGETBLFUNCTION", 16))
		return_value = _readRangeTblFunction();
	else if (MATCH("TABLESAMPLECLAUSE", 17))
		return_value = _readTableSampleClause();
	else if (MATCH("NOTIFY", 6))
		return_value = _readNotifyStmt();
	else if (MATCH("DEFELEM", 7))
		return_value = _readDefElem();
	else if (MATCH("DECLARECURSOR", 13))
		return_value = _readDeclareCursorStmt();
	else if (MATCH("PLANNEDSTMT", 11))
		return_value = _readPlannedStmt();
	else if (MATCH("PLAN", 4))
		return_value = _readPlan();
	else if (MATCH("RESULT", 6))
		return_value = _readResult();
	else if (MATCH("PROJECTSET", 10))
		return_value = _readProjectSet();
	else if (MATCH("MODIFYTABLE", 11))
		return_value = _readModifyTable();
	else if (MATCH("APPEND", 6))
		return_value = _readAppend();
	else if (MATCH("MERGEAPPEND", 11))
		return_value = _readMergeAppend();
	else if (MATCH("RECURSIVEUNION", 14))
		return_value = _readRecursiveUnion();
	else if (MATCH("BITMAPAND", 9))
		return_value = _readBitmapAnd();
	else if (MATCH("BITMAPOR", 8))
		return_value = _readBitmapOr();
	else if (MATCH("SCAN", 4))
		return_value = _readScan();
	else if (MATCH("SEQSCAN", 7))
		return_value = _readSeqScan();
	else if (MATCH("SAMPLESCAN", 10))
		return_value = _readSampleScan();
	else if (MATCH("INDEXSCAN", 9))
		return_value = _readIndexScan();
	else if (MATCH("DYNAMICINDEXSCAN", 16))
		return_value = _readDynamicIndexScan();
	else if (MATCH("DYNAMICINDEXONLYSCAN", 20))
		return_value = _readDynamicIndexOnlyScan();
	else if (MATCH("INDEXONLYSCAN", 13))
		return_value = _readIndexOnlyScan();
	else if (MATCH("BITMAPINDEXSCAN", 15))
		return_value = _readBitmapIndexScan();
	else if (MATCH("DYNAMICBITMAPINDEXSCAN", 23))
		return_value = _readDynamicBitmapIndexScan();
	else if (MATCH("BITMAPHEAPSCAN", 14))
		return_value = _readBitmapHeapScan();
	else if (MATCH("DYNAMICBITMAPHEAPSCAN", 21))
		return_value = _readDynamicBitmapHeapScan();
	else if (MATCH("TIDSCAN", 7))
		return_value = _readTidScan();
	else if (MATCH("TIDRANGESCAN", 12))
		return_value = _readTidRangeScan();
	else if (MATCH("SUBQUERYSCAN", 12))
		return_value = _readSubqueryScan();
	else if (MATCH("TABLEFUNCTIONSCAN", 17))
		return_value = _readTableFunctionScan();
	else if (MATCH("FUNCTIONSCAN", 12))
		return_value = _readFunctionScan();
	else if (MATCH("VALUESSCAN", 10))
		return_value = _readValuesScan();
	else if (MATCH("TABLEFUNCSCAN", 13))
		return_value = _readTableFuncScan();
	else if (MATCH("CTESCAN", 7))
		return_value = _readCteScan();
	else if (MATCH("NAMEDTUPLESTORESCAN", 19))
		return_value = _readNamedTuplestoreScan();
	else if (MATCH("WORKTABLESCAN", 13))
		return_value = _readWorkTableScan();
	else if (MATCH("FOREIGNSCAN", 11))
		return_value = _readForeignScan();
	else if (MATCH("DYNAMICFOREIGNSCAN", 18))
		return_value = _readDynamicForeignScan();
	else if (MATCH("CUSTOMSCAN", 10))
		return_value = _readCustomScan();
	else if (MATCH("JOIN", 4))
		return_value = _readJoin();
	else if (MATCH("NESTLOOP", 8))
		return_value = _readNestLoop();
	else if (MATCH("MERGEJOIN", 9))
		return_value = _readMergeJoin();
	else if (MATCH("HASHJOIN", 8))
		return_value = _readHashJoin();
	else if (MATCH("MATERIAL", 8))
		return_value = _readMaterial();
	else if (MATCH("MEMOIZE", 7))
		return_value = _readMemoize();
	else if (MATCH("SORT", 4))
		return_value = _readSort();
	else if (MATCH("INCREMENTALSORT", 15))
		return_value = _readIncrementalSort();
	else if (MATCH("GROUP", 5))
		return_value = _readGroup();
	else if (MATCH("AGG", 3))
		return_value = _readAgg();
	else if (MATCH("TupleSplit", 10))
		return_value = _readTupleSplit();
	else if (MATCH("DQAExpr", 7))
		return_value = _readDQAExpr();
	else if (MATCH("WINDOWAGG", 9))
		return_value = _readWindowAgg();
	else if (MATCH("UNIQUE", 6))
		return_value = _readUnique();
	else if (MATCH("GATHER", 6))
		return_value = _readGather();
	else if (MATCH("GATHERMERGE", 11))
		return_value = _readGatherMerge();
	else if (MATCH("HASH", 4))
		return_value = _readHash();
	else if (MATCH("SETOP", 5))
		return_value = _readSetOp();
	else if (MATCH("LOCKROWS", 8))
		return_value = _readLockRows();
	else if (MATCH("RUNTIMEFILTER", 13))
		return_value = _readRuntimeFilter();
	else if (MATCH("LIMIT", 5))
		return_value = _readLimit();
	else if (MATCH("NESTLOOPPARAM", 13))
		return_value = _readNestLoopParam();
	else if (MATCH("PLANROWMARK", 11))
		return_value = _readPlanRowMark();
	else if (MATCH("PARTITIONPRUNEINFO", 18))
		return_value = _readPartitionPruneInfo();
	else if (MATCH("PARTITIONEDRELPRUNEINFO", 23))
		return_value = _readPartitionedRelPruneInfo();
	else if (MATCH("PARTITIONPRUNESTEPOP", 20))
		return_value = _readPartitionPruneStepOp();
	else if (MATCH("PARTITIONPRUNESTEPCOMBINE", 25))
		return_value = _readPartitionPruneStepCombine();
	else if (MATCH("PLANINVALITEM", 13))
		return_value = _readPlanInvalItem();
	else if (MATCH("SUBPLAN", 7))
		return_value = _readSubPlan();
	else if (MATCH("ALTERNATIVESUBPLAN", 18))
		return_value = _readAlternativeSubPlan();
	else if (MATCH("RESTRICTINFO", 12))
		return_value = _readRestrictInfo();
	else if (MATCH("EXTENSIBLENODE", 14))
		return_value = _readExtensibleNode();
	else if (MATCH("PARTITIONBOUNDSPEC", 18))
		return_value = _readPartitionBoundSpec();
	else if (MATCH("PARTITIONRANGEDATUM", 19))
		return_value = _readPartitionRangeDatum();
	else if (MATCH("PARTITIONSPEC", 13))
		return_value = _readPartitionSpec();
	else if (MATCH("PARTITIONELEM", 13))
		return_value = _readPartitionElem();
	else if (MATCHX("PARTITIONCMD"))
		return_value = _readPartitionCmd();

	/* GPDB additions */
	else if (MATCHX("A_ARRAYEXPR"))
		return_value = _readA_ArrayExpr();
	else if (MATCHX("A_CONST"))
		return_value = _readAConst();
	else if (MATCHX("AEXPR"))
		return_value = _readAExpr();
	else if (MATCHX("ALTERDOMAINSTMT"))
		return_value = _readAlterDomainStmt();
	else if (MATCHX("ALTERFUNCTIONSTMT"))
		return_value = _readAlterFunctionStmt();
	else if (MATCHX("ALTEROBJECTSCHEMASTMT"))
		return_value = _readAlterObjectSchemaStmt();
	else if (MATCHX("ALTEROWNERSTMT"))
		return_value = _readAlterOwnerStmt();
	else if (MATCHX("ALTEROPFAMILYSTMT"))
		return_value = _readAlterOpFamilyStmt();
	else if (MATCHX("ALTERPOLICYSTMT"))
		return_value = _readAlterPolicyStmt();
	else if (MATCHX("ALTERROLESETSTMT"))
		return_value = _readAlterRoleSetStmt();
	else if (MATCHX("ALTERSYSTEMSTMT"))
		return_value = _readAlterSystemStmt();
	else if (MATCHX("ALTERROLESTMT"))
		return_value = _readAlterRoleStmt();
	else if (MATCHX("ALTERPROFILESTMT"))
		return_value = _readAlterProfileStmt();
	else if (MATCHX("ALTERSEQSTMT"))
		return_value = _readAlterSeqStmt();
	else if (MATCHX("ALTERTABLECMD"))
		return_value = _readAlterTableCmd();
	else if (MATCHX("ALTEREDTABLEINFO"))
		return_value = _readAlteredTableInfo();
	else if (MATCHX("NEWCONSTRAINT"))
		return_value = _readNewConstraint();
	else if (MATCHX("NEWCOLUMNVALUE"))
		return_value = _readNewColumnValue();
	else if (MATCHX("ALTERTABLESTMT"))
		return_value = _readAlterTableStmt();
	else if (MATCHX("ALTERTYPESTMT"))
		return_value = _readAlterTypeStmt();
	else if (MATCHX("CDBPROCESS"))
		return_value = _readCdbProcess();
	else if (MATCHX("CLUSTERSTMT"))
		return_value = _readClusterStmt();
	else if (MATCHX("COLUMNDEF"))
		return_value = _readColumnDef();
	else if (MATCHX("COLUMNREF"))
		return_value = _readColumnRef();
	else if (MATCHX("PARAMREF"))
		return_value = _readParamRef();
	else if (MATCHX("COMMONTABLEEXPR"))
		return_value = _readCommonTableExpr();
	else if (MATCHX("COMPTYPESTMT"))
		return_value = _readCompositeTypeStmt();
	else if (MATCHX("CONSTRAINT"))
		return_value = _readConstraint();
	else if (MATCHX("CONSTRAINTSSETSTMT"))
		return_value = _readConstraintsSetStmt();
	else if (MATCHX("CREATECAST"))
		return_value = _readCreateCastStmt();
	else if (MATCHX("CREATECONVERSION"))
		return_value = _readCreateConversionStmt();
	else if (MATCHX("CREATEDBSTMT"))
		return_value = _readCreatedbStmt();
	else if (MATCHX("CREATEDOMAINSTMT"))
		return_value = _readCreateDomainStmt();
	else if (MATCHX("CREATEENUMSTMT"))
		return_value = _readCreateEnumStmt();
	else if (MATCHX("CREATEEXTERNALSTMT"))
		return_value = _readCreateExternalStmt();
	else if (MATCHX("CREATEFUNCSTMT"))
		return_value = _readCreateFunctionStmt();
	else if (MATCHX("CREATEOPCLASS"))
		return_value = _readCreateOpClassStmt();
	else if (MATCHX("CREATEOPCLASSITEM"))
		return_value = _readCreateOpClassItem();
	else if (MATCHX("CREATEOPFAMILYSTMT"))
		return_value = _readCreateOpFamilyStmt();
	else if (MATCHX("CREATEPLANGSTMT"))
		return_value = _readCreatePLangStmt();
	else if (MATCHX("CREATEPUBLICATIONSTMT"))
		return_value = _readCreatePublicationStmt();
	else if (MATCHX("ALTERPUBLICATIONSTMT"))
		return_value = _readAlterPublicationStmt();
	else if (MATCHX("CREATESUBSCRIPTIONSTMT"))
		return_value = _readCreateSubscriptionStmt();
	else if (MATCHX("DROPSUBSCRIPTIONSTMT"))
		return_value = _readDropSubscriptionStmt();
	else if (MATCHX("ALTERSUBSCRIPTIONSTMT"))
		return_value = _readAlterSubscriptionStmt();
	else if (MATCHX("CREATEPOLICYSTMT"))
		return_value = _readCreatePolicyStmt();
	else if (MATCHX("CREATEROLESTMT"))
		return_value = _readCreateRoleStmt();
	else if (MATCHX("CREATEPROFILESTMT"))
		return_value = _readCreateProfileStmt();
	else if (MATCHX("CREATESCHEMASTMT"))
		return_value = _readCreateSchemaStmt();
	else if (MATCHX("ALTERSCHEMASTMT"))
		return_value = _readAlterSchemaStmt();
	else if (MATCHX("CREATETAGSTMT"))
		return_value = _readCreateTagStmt();
	else if (MATCHX("ALTERTAGSTMT"))
		return_value = _readAlterTagStmt();
	else if (MATCHX("DROPTAGSTMT"))
		return_value = _readDropTagStmt();
	else if (MATCHX("CREATESEQSTMT"))
		return_value = _readCreateSeqStmt();
	else if (MATCHX("CREATETRANSFORMSTMT"))
		return_value = _readCreateTransformStmt();
	else if (MATCHX("CURSORPOSINFO"))
		return_value = _readCursorPosInfo();
	else if (MATCHX("DEFELEM"))
		return_value = _readDefElem();
	else if (MATCHX("DEFINESTMT"))
		return_value = _readDefineStmt();
	else if (MATCHX("DENYLOGININTERVAL"))
		return_value = _readDenyLoginInterval();
	else if (MATCHX("DENYLOGINPOINT"))
		return_value = _readDenyLoginPoint();
	else if (MATCHX("DROPDBSTMT"))
		return_value = _readDropdbStmt();
	else if (MATCHX("DROPROLESTMT"))
		return_value = _readDropRoleStmt();
	else if (MATCHX("DROPPROFILESTMT"))
		return_value = _readDropProfileStmt();
	else if (MATCHX("DROPSTMT"))
		return_value = _readDropStmt();
	else if (MATCHX("DISTRIBUTIONKEYELEM"))
		return_value = _readDistributionKeyElem();
	else if (MATCHX("EXTTABLETYPEDESC"))
		return_value = _readExtTableTypeDesc();
	else if (MATCHX("FUNCCALL"))
		return_value = _readFuncCall();
	else if (MATCHX("FUNCTIONPARAMETER"))
		return_value = _readFunctionParameter();
	else if (MATCHX("OBJECTWITHARGS"))
		return_value = _readObjectWithArgs();
	else if (MATCHX("GRANTROLESTMT"))
		return_value = _readGrantRoleStmt();
	else if (MATCHX("GRANTSTMT"))
		return_value = _readGrantStmt();
	else if (MATCHX("INDEXELEM"))
		return_value = _readIndexElem();
	else if (MATCHX("INDEXSTMT"))
		return_value = _readIndexStmt();
	else if (MATCHX("LOCKSTMT"))
		return_value = _readLockStmt();
	else if (MATCHX("REINDEXSTMT"))
		return_value = _readReindexStmt();
	else if (MATCHX("REINDEXINDEXINFO"))
		return_value = _readReindexIndexInfo();
	else if (MATCHX("RENAMESTMT"))
		return_value = _readRenameStmt();
	else if (MATCHX("REPLICAIDENTITYSTMT"))
		return_value = _readReplicaIdentityStmt();
	else if (MATCHX("RULESTMT"))
		return_value = _readRuleStmt();
	else if (MATCHX("SEGFILEMAPNODE"))
		return_value = _readSegfileMapNode();
	else if (MATCHX("SINGLEROWERRORDESC"))
		return_value = _readSingleRowErrorDesc();
	else if (MATCHX("SLICETABLE"))
		return_value = _readSliceTable();
	else if (MATCHX("SORTBY"))
		return_value = _readSortBy();
	else if (MATCHX("TABLEVALUEEXPR"))
		return_value = _readTableValueExpr();
	else if (MATCHX("TRUNCATESTMT"))
		return_value = _readTruncateStmt();
	else if (MATCHX("TYPECAST"))
		return_value = _readTypeCast();
	else if (MATCHX("TYPENAME"))
		return_value = _readTypeName();
	else if (MATCHX("VACUUMSTMT"))
		return_value = _readVacuumStmt();
	else if (MATCHX("VACUUMRELATION"))
		return_value = _readVacuumRelation();
	else if (MATCHX("VARIABLESETSTMT"))
		return_value = _readVariableSetStmt();
	else if (MATCHX("VIEWSTMT"))
		return_value = _readViewStmt();
	else if (MATCHX("WITHCLAUSE"))
		return_value = _readWithClause();
	else if (MATCHX("GPPARTITIONDEFINITION"))
		return_value = _readGpPartitionDefinition();
	else if (MATCHX("GPPARTDEFELEM"))
		return_value = _readGpPartDefElem();
	else if (MATCHX("GPPARTITIONRANGEITEM"))
		return_value = _readGpPartitionRangeItem();
	else if (MATCHX("GPPARTITIONRANGESPEC"))
		return_value = _readGpPartitionRangeSpec();
	else if (MATCHX("GPPARTITIONLISTSPEC"))
		return_value = _readGpPartitionListSpec();
	else if (MATCHX("COLUMNREFERENCESTORAGEDIRECTIVE"))
		return_value = _readColumnReferenceStorageDirective();
	else if (MATCHX("RETURN"))
		return_value = _readReturnStmt();
	else if (MATCHX("DROPDIRECTORYTABLESTMT"))
		return_value = _readDropDirectoryTableStmt();
	else if (MATCHX("RTEPERMISSIONINFO"))
		return_value = _readRTEPermissionInfo();
	else if (MATCHX("GPPOLICY"))
		return_value = _readGpPolicy();
	else if (MATCHX("MERGEACTION"))
		return_value = _readMergeAction();
	else if (MATCHX("PUBLICATIONOBJSPEC"))
		return_value = _readPublicationObjSpec();
	else if (MATCHX("PUBLICATIONTABLE"))
		return_value = _readPublicationTable();
	else if (MATCHX("WINDOWDEF"))
		return_value = _readWindowDef();
	else if (MATCHX("JSONCONSTRUCTOREXPR"))
		return_value = _readJsonConstructorExpr();
	else if (MATCHX("JSONISPREDICATE"))
		return_value = _readJsonIsPredicate();
	else if (MATCHX("JSONRETURNING"))
		return_value = _readJsonReturning();
	else if (MATCHX("JSONVALUEEXPR"))
		return_value = _readJsonValueExpr();
	else if (MATCHX("JSONFORMAT"))
		return_value = _readJsonFormat();
	else
	{
        ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("This operation involves an internal data item "
						"of a type called \"%.*s\" which is not "
						"supported in this version of %s.",
						length, token, PACKAGE_NAME)));
		return_value = NULL;	/* keep compiler quiet */
	}

	return (Node *) return_value;
}


/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
Datum
readDatum(bool typbyval)
{
	Size		length,
				i;
	int			tokenLength;
	const char *token;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = pg_strtok(&tokenLength);
	length = atoui(token);

	token = pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
			 token ? token : "[NULL]", length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
			 token ? token : "[NULL]", length);

	return res;
}

/*
 * readAttrNumberCols
 */
AttrNumber *
readAttrNumberCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	AttrNumber *attr_vals;

	if (numCols <= 0)
		return NULL;

	attr_vals = (AttrNumber *) palloc(numCols * sizeof(AttrNumber));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		attr_vals[i] = atoi(token);
	}

	return attr_vals;
}

/*
 * readOidCols
 */
Oid *
readOidCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	Oid		   *oid_vals;

	if (numCols <= 0)
		return NULL;

	oid_vals = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		oid_vals[i] = atooid(token);
	}

	return oid_vals;
}

/*
 * readIntCols
 */
int *
readIntCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	int		   *int_vals;

	if (numCols <= 0)
		return NULL;

	int_vals = (int *) palloc(numCols * sizeof(int));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		int_vals[i] = atoi(token);
	}

	return int_vals;
}

/*
 * readBoolCols
 */
bool *
readBoolCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	bool	   *bool_vals;

	if (numCols <= 0)
		return NULL;

	bool_vals = (bool *) palloc(numCols * sizeof(bool));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		bool_vals[i] = strtobool(token);
	}

	return bool_vals;
}
#endif
