/*-------------------------------------------------------------------------
 *
 * readfast.c
 *	  Binary Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * These routines must be exactly the inverse of the routines in
 * outfast.c.
 *
 * For most node types, these routines are identical to the text reader
 * functions, in readfuncs.c. To avoid code duplication and merge hazards
 * (readfast.c is a Cloudberry addon), most read routines borrow the source
 * definition from readfuncs.c, we just compile it with different READ_*
 * macros.
 *
 * The way that works is that readfast.c defines all the necessary macros,
 * as well as COMPILING_BINARY_FUNCS, and then #includes readfuncs.c. For
 * those node types where the binary and text functions are different,
 * the function in readfuncs.c is put in a #ifndef COMPILING_BINARY_FUNCS
 * block, and readfast.c provides the binary version of the function.
 * outfast.c and outfuncs.c have a similar relationship.
 *
 * By this, CDB could link only readfast.o (#includes readfuncs.c) to get all
 * the fast version deserializing functions, outfast.o likewise.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "catalog/aocatalog.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"
#include "cdb/cdbgang.h"
#include "nodes/altertablenodes.h"

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.
 */

#define READ_TEMP_LOCALS()

#define READ_LOCALS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/*
 * no difference between READ_LOCALS and READ_LOCALS_NO_FIELDS in readfast.c,
 * but define both for compatibility.
 */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	READ_LOCALS(nodeTypeName)

/* Allocate non-node variable */
#define ALLOCATE_LOCAL(local, typeName, size) \
	local = (typeName *)palloc0(sizeof(typeName) * size)

/* Read an integer field  */
#define READ_INT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int));  read_str_ptr+=sizeof(int)

/* Read an int8 field  */
#define READ_INT8_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int8));  read_str_ptr+=sizeof(int8)

/* Read an int16 field  */
#define READ_INT16_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int16));  read_str_ptr+=sizeof(int16)

/* Read an unsigned integer field) */
#define READ_UINT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int)); read_str_ptr+=sizeof(int)

/* Read an uint64 field) */
#define READ_UINT64_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(uint64)); read_str_ptr+=sizeof(uint64)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(Oid)); read_str_ptr+=sizeof(Oid)

/* Read a long-integer field  */
#define READ_LONG_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(long)); read_str_ptr+=sizeof(long)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, 1); read_str_ptr++

/* Read an enumerated-type field that was written as a short integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	{ int16 ent; memcpy(&ent, read_str_ptr, sizeof(int16));  read_str_ptr+=sizeof(int16);local_node->fldname = (enumtype) ent; }

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(double)); read_str_ptr+=sizeof(double)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	local_node->fldname = read_str_ptr[0] != 0;  Assert(read_str_ptr[0]==1 || read_str_ptr[0]==0); read_str_ptr++

/* Read a character-string variable */
#define READ_STRING_VAR(var) \
	{ int slen; char * nn = NULL; \
		memcpy(&slen, read_str_ptr, sizeof(int)); \
		read_str_ptr+=sizeof(int); \
		if (slen>0) { \
		    nn = palloc(slen+1); \
		    memcpy(nn,read_str_ptr,slen); \
		    read_str_ptr+=(slen); nn[slen]='\0'; \
		} \
		var = nn;  }

#define READ_STRING_VAR_NULL(var) \
	{ int slen; char * nn = NULL; \
		memcpy(&slen, read_str_ptr, sizeof(int)); \
		read_str_ptr+=sizeof(int); \
		if (slen>0) { \
		    nn = palloc(slen+1); \
		    memcpy(nn,read_str_ptr,slen); \
		    read_str_ptr+=(slen); nn[slen]='\0'; \
		} \
		if (slen==0) { \
			nn = palloc(1); \
			nn[0] = '\0'; \
		} \
		var = nn;  }

/* Read a character-string field */
#define READ_STRING_FIELD(fldname)  READ_STRING_VAR(local_node->fldname)

#define READ_STRING_FIELD_NULL(fldname)  READ_STRING_VAR_NULL(local_node->fldname)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) READ_INT_FIELD(fldname)

/* Read a Node field */
#ifdef GP_SERIALIZATION_DEBUG
#define READ_NODE_FIELD(fldname) \
	do { \
		char *xexpected = CppAsString(fldname); \
		char got[100]; \
		\
		memcpy(got, read_str_ptr, strlen(xexpected) + 1); \
		read_str_ptr += strlen(xexpected) + 1; \
		if (strcmp(xexpected, got) != 0) \
			elog(ERROR, "deserialization lost sync: %s vs %02x%02x%02x", xexpected, (unsigned char) got[0], (unsigned char) got[1], (unsigned char) got[2]); \
		\
		local_node->fldname = readNodeBinary(); \
	} while(0)
#else
#define READ_NODE_FIELD(fldname) \
	local_node->fldname = readNodeBinary()
#endif

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	 local_node->fldname = _readBitmapset()

/* Read in a binary field */
#define READ_BINARY_FIELD(fldname, sz) \
	memcpy(&local_node->fldname, read_str_ptr, (sz)); read_str_ptr += (sz)

/* Read a bytea field */
#define READ_BYTEA_FIELD(fldname) \
	local_node->fldname = (bytea *) DatumGetPointer(readDatumBinary(false))

/* Read a dummy field */
#define READ_DUMMY_FIELD(fldname,fldvalue) \
	{ local_node->fldname = (0); /*read_str_ptr += sizeof(int*);*/ }

/* Routine exit */
#define READ_DONE() \
	return local_node

/* Read an integer array  */
#define READ_ARRAY_OF_TYPE(fldname, count, Type) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		local_node->fldname = (Type *)palloc((count) * sizeof(Type)); \
		for(i = 0; i < (count); i++) \
		{ \
			memcpy(&local_node->fldname[i], read_str_ptr, sizeof(Type)); read_str_ptr+=sizeof(Type); \
		} \
	}

/* Read a bool array  */
#define READ_BOOL_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		local_node->fldname = (bool *) palloc((count) * sizeof(bool)); \
		for(i = 0; i < (count); i++) \
		{ \
			local_node->fldname[i] = *read_str_ptr ? true : false; \
			read_str_ptr++; \
		} \
	}

/* Read an attribute number array  */
#define READ_ATTRNUMBER_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, AttrNumber)

/* Read an Oid array  */
#define READ_OID_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, Oid)

/* Read an int array  */
#define READ_INT_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, int)


static void *readNodeBinary(void);

static Datum readDatumBinary(bool typbyval);
#define readDatum(x) readDatumBinary(x)

static Bitmapset *_readBitmapset(void);

/*
 * Current position in the message that we are processing. We can keep
 * this in a global variable because readNodeFromBinaryString() is not
 * re-entrant. This is similar to the current position that pg_strtok()
 * keeps, used by the normal stringToNode() function.
 */
static const char *read_str_ptr;

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
	if (local_node->constisnull)
		local_node->constvalue = 0;
	else
		local_node->constvalue = readDatumBinary(local_node->constbyval);

	READ_DONE();
}

static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	READ_ENUM_FIELD(boolop, BoolExprType);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}


static A_Expr *
_readA_Expr(void)
{
	READ_LOCALS(A_Expr);

	READ_ENUM_FIELD(kind, A_Expr_Kind);

	switch (local_node->kind)
	{
		case AEXPR_OP:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			READ_NODE_FIELD(name);
			break;

		case AEXPR_IN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:

			READ_NODE_FIELD(name);
			break;

		default:
			elog(ERROR,"Unable to understand A_Expr node ");
			break;
	}

	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static A_Const *
_readA_Const(void)
{
	READ_LOCALS(A_Const);

	READ_BOOL_FIELD(isnull);

	if (!local_node->isnull)
	{
		union ValUnion *tmp = readNodeBinary();

		switch (nodeTag(tmp))
		{
			case T_Integer:
				memcpy(&local_node->val, tmp, sizeof(Integer));
				break;
			case T_Float:
				memcpy(&local_node->val, tmp, sizeof(Float));
				break;
			case T_Boolean:
				memcpy(&local_node->val, tmp, sizeof(Boolean));
				break;
			case T_String:
				memcpy(&local_node->val, tmp, sizeof(String));
				break;
			case T_BitString:
				memcpy(&local_node->val, tmp, sizeof(BitString));
				break;
			default:
				break;
		}
	}

	READ_LOCATION_FIELD(location);   /*CDB*/
	READ_DONE();
}


static ColumnDef *
_readColumnDef(void)
{
	READ_LOCALS(ColumnDef);

	READ_STRING_FIELD(colname);
	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(compression);
	READ_INT_FIELD(inhcount);
	READ_BOOL_FIELD(is_local);
	READ_BOOL_FIELD(is_not_null);
	READ_BOOL_FIELD(is_from_type);
	READ_INT_FIELD(attnum);
	READ_INT_FIELD(storage);
	READ_STRING_FIELD(storage_name);
	READ_NODE_FIELD(raw_default);
	READ_NODE_FIELD(cooked_default);

	READ_BOOL_FIELD(hasCookedMissingVal);
	READ_BOOL_FIELD(missingIsNull);
	if (local_node->hasCookedMissingVal && !local_node->missingIsNull)
		local_node->missingVal = readDatum(false);

	READ_CHAR_FIELD(identity);
	READ_NODE_FIELD(identitySequence);
	READ_CHAR_FIELD(generated);
	READ_NODE_FIELD(collClause);
	READ_OID_FIELD(collOid);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(fdwoptions);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

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

static Constraint *
_readConstraint(void)
{
	READ_LOCALS(Constraint);

	READ_ENUM_FIELD(contype, ConstrType);
	READ_STRING_FIELD(conname);			/* name, or NULL if unnamed */
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_LOCATION_FIELD(location);

	READ_BOOL_FIELD(is_no_inherit);
	READ_NODE_FIELD(raw_expr);
	READ_STRING_FIELD(cooked_expr);
	READ_CHAR_FIELD(generated_when);
	READ_BOOL_FIELD(nulls_not_distinct);

	READ_NODE_FIELD(keys);
	READ_NODE_FIELD(including);

	READ_NODE_FIELD(exclusions);

	READ_NODE_FIELD(options);
	READ_STRING_FIELD(indexname);
	READ_STRING_FIELD(indexspace);
	READ_BOOL_FIELD(reset_default_tblspc);

	READ_STRING_FIELD(access_method);
	READ_NODE_FIELD(where_clause);

	READ_NODE_FIELD(pktable);
	READ_NODE_FIELD(fk_attrs);
	READ_NODE_FIELD(pk_attrs);
	READ_CHAR_FIELD(fk_matchtype);
	READ_CHAR_FIELD(fk_upd_action);
	READ_CHAR_FIELD(fk_del_action);
	READ_NODE_FIELD(old_conpfeqop);
	READ_OID_FIELD(old_pktable_oid);

	READ_BOOL_FIELD(skip_validation);
	READ_BOOL_FIELD(initially_valid);

	READ_DONE();
}


static ExtensibleNode *
_readExtensibleNode(void)
{
	const ExtensibleNodeMethods *methods;
	ExtensibleNode *local_node;
	const char *extnodename;

	char *str;
	const char *save_strtok = NULL;
	const char *save_begin = NULL;
	const char ** save_strtok_ptr = &save_strtok;
	const char ** save_begin_ptr = &save_begin;

	READ_STRING_VAR(extnodename);
	if (!extnodename)
		elog(ERROR, "extnodename has to be supplied");
	methods = GetExtensibleNodeMethods(extnodename, false);

	local_node = (ExtensibleNode *) newNode(methods->node_size,
											T_ExtensibleNode);
	local_node->extnodename = extnodename;

	READ_STRING_VAR(str);

	/*
	 * deserialize the private fields
	 */

	/* set the states for pg_strtok(), let methods->nodeRead() to process str */
	save_strtok_states(save_strtok_ptr, save_begin_ptr);
	set_strtok_states(str, str);

	/* do reading */
	methods->nodeRead(local_node);

	/* set the states for pg_strtok() back */
	set_strtok_states(save_strtok, save_begin);

	READ_DONE();
}

static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *bms = NULL;
	int			nwords;
	int			i;

	memcpy(&nwords, read_str_ptr, sizeof(int)); read_str_ptr+=sizeof(int);
	if (nwords==0)
		return bms;

	bms = palloc(offsetof(Bitmapset, words)+nwords*sizeof(bitmapword));
	bms->nwords = nwords;
	for (i = 0; i < nwords; i++)
	{
		memcpy(&bms->words[i], read_str_ptr, sizeof(bitmapword)); read_str_ptr+=sizeof(bitmapword);
	}

	return bms;
}

static SerializedParams *
_readSerializedParams(void)
{
	READ_LOCALS(SerializedParams);

	READ_INT_FIELD(nExternParams);
	local_node->externParams = palloc0(local_node->nExternParams * sizeof(SerializedParamExternData));
	for (int i = 0; i < local_node->nExternParams; i++)
	{
		READ_BOOL_FIELD(externParams[i].isnull);
		READ_INT_FIELD(externParams[i].pflags);
		READ_OID_FIELD(externParams[i].ptype);
		READ_INT_FIELD(externParams[i].plen);
		READ_BOOL_FIELD(externParams[i].pbyval);

		if (!local_node->externParams[i].isnull)
			local_node->externParams[i].value = readDatum(local_node->externParams[i].pbyval);
	}

	READ_INT_FIELD(nExecParams);
	local_node->execParams = palloc0(local_node->nExecParams * sizeof(SerializedParamExecData));
	for (int i = 0; i < local_node->nExecParams; i++)
	{
		READ_BOOL_FIELD(execParams[i].isnull);
		READ_BOOL_FIELD(execParams[i].isvalid);
		READ_INT_FIELD(execParams[i].plen);
		READ_BOOL_FIELD(execParams[i].pbyval);

		if (local_node->execParams[i].isvalid && !local_node->execParams[i].isnull)
			local_node->execParams[i].value = readDatum(local_node->execParams[i].pbyval);
		READ_BOOL_FIELD(execParams[i].pbyval);
	}

	READ_NODE_FIELD(transientTypes);

	READ_DONE();
}

static SliceTable *
_readSliceTable(void)
{
	READ_LOCALS(SliceTable);

	READ_INT_FIELD(localSlice);
	READ_INT_FIELD(numSlices);
	local_node->slices = palloc0(local_node->numSlices * sizeof(ExecSlice));
	for (int i = 0; i < local_node->numSlices; i++)
	{
		READ_INT_FIELD(slices[i].sliceIndex);
		READ_INT_FIELD(slices[i].rootIndex);
		READ_INT_FIELD(slices[i].parentIndex);
		READ_INT_FIELD(slices[i].planNumSegments);
		READ_NODE_FIELD(slices[i].children); /* List of int index */
		READ_ENUM_FIELD(slices[i].gangType, GangType);
		READ_NODE_FIELD(slices[i].segments); /* List of int index */
		READ_BOOL_FIELD(slices[i].useMppParallelMode);
		READ_INT_FIELD(slices[i].parallel_workers);
		local_node->slices[i].primaryGang = NULL;
		READ_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		READ_BITMAPSET_FIELD(slices[i].processesMap);
	}
	READ_BOOL_FIELD(hasMotions);

	READ_INT_FIELD(instrument_options);
	READ_INT_FIELD(ic_instance_id);

	READ_DONE();
}


static Integer *
_readInteger(void)
{
	READ_LOCALS(Integer);

	memcpy(&local_node->ival, read_str_ptr, sizeof(int));
	read_str_ptr += sizeof(int);

	READ_DONE();
}

static Boolean *
_readBoolean(void)
{
	READ_LOCALS(Boolean);

	memcpy(&local_node->boolval, read_str_ptr, sizeof(bool));
	read_str_ptr += sizeof(bool);

	READ_DONE();
}

static Float *
_readFloat(void)
{
	READ_LOCALS(Float);

	int slen;
	char *nn;
	memcpy(&slen, read_str_ptr, sizeof(int));
	read_str_ptr += sizeof(int);
	nn = palloc(slen + 1);
	memcpy(nn, read_str_ptr, slen);
	nn[slen] = '\0';
	local_node->fval = nn;
	read_str_ptr += slen;

	READ_DONE();
}

static String *
_readString(void)
{
	int slen;
	char *nn;

	READ_LOCALS(String);

	memcpy(&slen, read_str_ptr, sizeof(int));
	read_str_ptr += sizeof(int);
	nn = palloc(slen + 1);
	memcpy(nn, read_str_ptr, slen);
	nn[slen] = '\0';
	local_node->sval = nn;
	read_str_ptr += slen;

	READ_DONE();
}

static BitString *
_readBitString(void)
{
	READ_LOCALS(BitString);

	int slen;
	char *nn;
	memcpy(&slen, read_str_ptr, sizeof(int));
	read_str_ptr += sizeof(int);
	nn = palloc(slen + 1);
	memcpy(nn, read_str_ptr, slen);
	nn[slen] = '\0';
	local_node->bsval = nn;
	read_str_ptr += slen;

	READ_DONE();
}

static TupleDescNode *
_readTupleDescNode(void)
{
	READ_LOCALS(TupleDescNode);

	READ_INT_FIELD(natts);

	local_node->tuple = CreateTemplateTupleDesc(local_node->natts);

	READ_INT_FIELD(tuple->natts);
	if (local_node->tuple->natts > 0)
	{
		int i = 0;
		for (; i < local_node->tuple->natts; i++)
		{
			memcpy(&local_node->tuple->attrs[i], read_str_ptr, ATTRIBUTE_FIXED_PART_SIZE);
			read_str_ptr+=ATTRIBUTE_FIXED_PART_SIZE;
		}
	}

	READ_OID_FIELD(tuple->tdtypeid);
	READ_INT_FIELD(tuple->tdtypmod);
	READ_INT_FIELD(tuple->tdrefcount);

	// Transient type don't have constraint.
	local_node->tuple->constr = NULL;

	Assert(local_node->tuple->tdtypeid == RECORDOID);

	READ_DONE();
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

static AlteredTableInfo *
_readAlteredTableInfo(void)
{
	READ_LOCALS(AlteredTableInfo);

	READ_OID_FIELD(relid);
	READ_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
	{
		READ_NODE_FIELD(subcmds[i]);
	}

	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(afterStmts);
	READ_BOOL_FIELD(verify_new_notnull);
	READ_INT_FIELD(rewrite);
	READ_OID_FIELD(newAccessMethod);
	READ_BOOL_FIELD(dist_opfamily_changed);
	READ_OID_FIELD(new_opclass);
	READ_BOOL_FIELD(chgPersistence);
	READ_CHAR_FIELD(newrelpersistence);
	READ_NODE_FIELD(partition_constraint);
	READ_BOOL_FIELD(validate_default);
	READ_NODE_FIELD(changedConstraintOids);
	READ_NODE_FIELD(changedConstraintDefs);
	/* The QD sends changedConstraintDefs wrapped in Values. Unwrap them. */
	unwrapStringList(local_node->changedConstraintDefs);
	READ_NODE_FIELD(changedIndexOids);
	READ_NODE_FIELD(changedIndexDefs);
	unwrapStringList(local_node->changedIndexDefs);
	READ_NODE_FIELD(beforeStmtLists);
	READ_NODE_FIELD(constraintLists);

	READ_DONE();
}

static NewConstraint *
_readNewConstraint(void)
{
	READ_LOCALS(NewConstraint);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(contype, ConstrType);
	READ_OID_FIELD(refrelid);
	READ_OID_FIELD(refindid);
	READ_OID_FIELD(conid);
	READ_NODE_FIELD(qual);
	/* can't serialize qualstate */

	READ_DONE();
}

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

static Motion *
_readMotion(void)
{
	READ_LOCALS(Motion);

	READ_FLOAT_FIELD(plan.startup_cost);
	READ_FLOAT_FIELD(plan.total_cost);
	READ_FLOAT_FIELD(plan.plan_rows);
	READ_INT_FIELD(plan.plan_width);
	READ_BOOL_FIELD(plan.parallel_aware);
	READ_BOOL_FIELD(plan.parallel_safe);
	READ_BOOL_FIELD(plan.async_capable);
	READ_INT_FIELD(plan.plan_node_id);
	READ_NODE_FIELD(plan.targetlist);
	READ_NODE_FIELD(plan.qual);
	READ_NODE_FIELD(plan.lefttree);
	READ_NODE_FIELD(plan.righttree);
	READ_NODE_FIELD(plan.initPlan);
	READ_BITMAPSET_FIELD(plan.extParam);
	READ_BITMAPSET_FIELD(plan.allParam);
	READ_NODE_FIELD(plan.flow);
	READ_UINT_FIELD(plan.locustype);
	READ_INT_FIELD(plan.parallel);
	READ_UINT64_FIELD(plan.operatorMemKB);
	
	READ_INT_FIELD(motionID);
	READ_ENUM_FIELD(motionType, MotionType);

	Assert(local_node->motionType == MOTIONTYPE_GATHER ||
		   local_node->motionType == MOTIONTYPE_GATHER_SINGLE ||
		   local_node->motionType == MOTIONTYPE_HASH ||
		   local_node->motionType == MOTIONTYPE_BROADCAST ||
		   local_node->motionType == MOTIONTYPE_BROADCAST_WORKERS ||
		   local_node->motionType == MOTIONTYPE_EXPLICIT);

	READ_BOOL_FIELD(sendSorted);

	READ_NODE_FIELD(hashExprs);
	READ_OID_ARRAY(hashFuncs, list_length(local_node->hashExprs));

	READ_INT_FIELD(numSortCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numSortCols);
	READ_OID_ARRAY(sortOperators, local_node->numSortCols);
	READ_OID_ARRAY(collations, local_node->numSortCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numSortCols);

	READ_INT_FIELD(segidColIdx);
	READ_INT_FIELD(numHashSegments);
	
	READ_DONE();
}

static OidAssignment *
_readOidAssignment(void)
{
	READ_LOCALS(OidAssignment);

	READ_OID_FIELD(catalog);
	READ_STRING_FIELD_NULL(objname);
	READ_OID_FIELD(namespaceOid);
	READ_OID_FIELD(keyOid1);
	READ_OID_FIELD(keyOid2);
	READ_OID_FIELD(oid);
	READ_DONE();
}

#include "readfast.funcs.c"

/*
 * For some structs, we have to provide a read functions because it differs
 * from the text version (or the text version doesn't exist at all).
 */

static void *
readNodeBinary(void)
{
	void	   *return_value;
	NodeTag 	nt;
	int16       ntt;

	memcpy(&ntt, read_str_ptr,sizeof(int16));
	read_str_ptr+=sizeof(int16);
	nt = (NodeTag) ntt;

	if (nt==0)
		return NULL;

	if (nt == T_List || nt == T_IntList || nt == T_OidList)
	{
		List	   *l = NIL;
		int listsize = 0;
		int i;

		memcpy(&listsize,read_str_ptr,sizeof(int));
		read_str_ptr+=sizeof(int);

		if (nt == T_IntList)
		{
			int val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,read_str_ptr,sizeof(int));
				read_str_ptr+=sizeof(int);
				l = lappend_int(l, val);
			}
		}
		else if (nt == T_OidList)
		{
			Oid val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,read_str_ptr,sizeof(Oid));
				read_str_ptr+=sizeof(Oid);
				l = lappend_oid(l, val);
			}
		}
		else
		{

			for (i = 0; i < listsize; i++)
			{
				l = lappend(l, readNodeBinary());
			}
		}
		Assert(l->length==listsize);

		return l;
	}

	switch(nt)
	{
		case T_Integer:
			return_value = _readInteger();
			break;
		case T_Boolean:
			return_value = _readBoolean();
			break;
		case T_Float:
			return_value = _readFloat();
			break;
		case T_String:
			return_value = _readString();
			break;
		case T_BitString:
			return_value = _readBitString();
			break;
			
#include "readfast.switch.c"

		default:
			return_value = NULL; /* keep the compiler silent */
			elog(ERROR, "could not deserialize unrecognized node type: %d",
					 (int) nt);
			break;
	}

	return (Node *)return_value;
}

Node *
readNodeFromBinaryString(const char *str_arg, int len pg_attribute_unused())
{
	Node	   *node;
	int16		tg;

	read_str_ptr = str_arg;

	node = readNodeBinary();

	memcpy(&tg, read_str_ptr, sizeof(int16));
	if (tg != (int16)0xDEAD)
		elog(ERROR,"Deserialization lost sync.");

	return node;

}
/*
 * readDatumBinary
 *
 * Like readDatum() in readfuncs.c.
 */
static Datum
readDatumBinary(bool typbyval)
{
	Size		length;

	Datum		res;
	char	   *s;

	if (typbyval)
	{
		memcpy(&res, read_str_ptr, sizeof(Datum)); read_str_ptr+=sizeof(Datum);
	}
	else
	{
		memcpy(&length, read_str_ptr, sizeof(Size)); read_str_ptr+=sizeof(Size);
	  	if (length <= 0)
			res = 0;
		else
		{
			s = (char *) palloc(length+1);
			memcpy(s, read_str_ptr, length); read_str_ptr+=length;
			s[length]='\0';
			res = PointerGetDatum(s);
		}

	}

	return res;
}
