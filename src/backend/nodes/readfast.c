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

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "catalog/aocatalog.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"
#include "cdb/cdbgang.h"

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

/*
 * For most structs, we reuse the definitions from readfuncs.c. See comment
 * in readfuncs.c.
 */
#define COMPILING_BINARY_FUNCS
#include "readfuncs.c"

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
			case T_PlannedStmt:
				return_value = _readPlannedStmt();
				break;
			case T_QueryDispatchDesc:
				return_value = _readQueryDispatchDesc();
				break;
			case T_OidAssignment:
				return_value = _readOidAssignment();
				break;
			case T_Plan:
				return_value = _readPlan();
				break;
			case T_Result:
				return_value = _readResult();
				break;
			case T_ProjectSet:
				return_value = _readProjectSet();
				break;
			case T_Append:
				return_value = _readAppend();
				break;
			case T_MergeAppend:
				return_value = _readMergeAppend();
				break;
			case T_Sequence:
				return_value = _readSequence();
				break;
			case T_RecursiveUnion:
				return_value = _readRecursiveUnion();
				break;
			case T_BitmapAnd:
				return_value = _readBitmapAnd();
				break;
			case T_BitmapOr:
				return_value = _readBitmapOr();
				break;
			case T_Gather:
				return_value = _readGather();
				break;
			case T_GatherMerge:
				return_value = _readGatherMerge();
				break;
			case T_Scan:
				return_value = _readScan();
				break;
			case T_SeqScan:
				return_value = _readSeqScan();
				break;
			case T_DynamicSeqScan:
				return_value = _readDynamicSeqScan();
				break;
			case T_ExternalScanInfo:
				return_value = _readExternalScanInfo();
				break;
			case T_IndexScan:
				return_value = _readIndexScan();
				break;
			case T_IndexOnlyScan:
				return_value = _readIndexOnlyScan();
				break;
			case T_DynamicIndexScan:
				return_value = _readDynamicIndexScan();
				break;
			case T_DynamicIndexOnlyScan:
				return_value = _readDynamicIndexOnlyScan();
				break;
			case T_BitmapIndexScan:
				return_value = _readBitmapIndexScan();
				break;
			case T_DynamicBitmapIndexScan:
				return_value = _readDynamicBitmapIndexScan();
				break;
			case T_BitmapHeapScan:
				return_value = _readBitmapHeapScan();
				break;
			case T_DynamicBitmapHeapScan:
				return_value = _readDynamicBitmapHeapScan();
				break;
			case T_CteScan:
				return_value = _readCteScan();
				break;
			case T_NamedTuplestoreScan:
				return_value = _readNamedTuplestoreScan();
				break;
			case T_WorkTableScan:
				return_value = _readWorkTableScan();
				break;
			case T_TidScan:
				return_value = _readTidScan();
				break;
			case T_TidRangeScan:
				return_value = _readTidRangeScan();
				break;
			case T_SubqueryScan:
				return_value = _readSubqueryScan();
				break;
			case T_FunctionScan:
				return_value = _readFunctionScan();
				break;
			case T_TableFuncScan:
				return_value = _readTableFuncScan();
				break;
			case T_ValuesScan:
				return_value = _readValuesScan();
				break;
			case T_ForeignScan:
				return_value = _readForeignScan();
				break;
			case T_DynamicForeignScan:
				return_value = _readDynamicForeignScan();
				break;
			case T_CustomScan:
				return_value = _readCustomScan();
				break;
			case T_SampleScan:
				return_value = _readSampleScan();
				break;
			case T_Join:
				return_value = _readJoin();
				break;
			case T_NestLoop:
				return_value = _readNestLoop();
				break;
			case T_MergeJoin:
				return_value = _readMergeJoin();
				break;
			case T_HashJoin:
				return_value = _readHashJoin();
				break;
			case T_Agg:
				return_value = _readAgg();
				break;
			case T_TupleSplit:
				return_value = _readTupleSplit();
				break;
			case T_DQAExpr:
				return_value = _readDQAExpr();
				break;
			case T_WindowAgg:
				return_value = _readWindowAgg();
				break;
			case T_WindowHashAgg:
				return_value = _readWindowHashAgg();
				break;
			case T_TableFunctionScan:
				return_value = _readTableFunctionScan();
				break;
			case T_Material:
				return_value = _readMaterial();
				break;
			case T_Memoize:
				return_value = _readMemoize();
				break;
			case T_ShareInputScan:
				return_value = _readShareInputScan();
				break;
			case T_Sort:
				return_value = _readSort();
				break;
			case T_IncrementalSort:
				return_value = _readIncrementalSort();
				break;
			case T_Unique:
				return_value = _readUnique();
				break;
			case T_SetOp:
				return_value = _readSetOp();
				break;
			case T_RuntimeFilter:
				return_value = _readRuntimeFilter();
				break;
			case T_Limit:
				return_value = _readLimit();
				break;
			case T_NestLoopParam:
				return_value = _readNestLoopParam();
				break;
			case T_PlanRowMark:
				return_value = _readPlanRowMark();
				break;
			case T_PartitionPruneInfo:
				return_value = _readPartitionPruneInfo();
				break;
			case T_PartitionedRelPruneInfo:
				return_value = _readPartitionedRelPruneInfo();
				break;
			case T_PartitionPruneStepOp:
				return_value = _readPartitionPruneStepOp();
				break;
			case T_PartitionPruneStepCombine:
				return_value = _readPartitionPruneStepCombine();
				break;
			case T_PlanInvalItem:
				return_value = _readPlanInvalItem();
				break;
			case T_Hash:
				return_value = _readHash();
				break;
			case T_Motion:
				return_value = _readMotion();
				break;
			case T_SplitUpdate:
				return_value = _readSplitUpdate();
				break;
			case T_SplitMerge:
				return_value = _readSplitMerge();
				break;
			case T_AssertOp:
				return_value = _readAssertOp();
				break;
			case T_PartitionSelector:
				return_value = _readPartitionSelector();
				break;
			case T_GpPartDefElem:
				return_value = _readGpPartDefElem();
				break;
			case T_Alias:
				return_value = _readAlias();
				break;
			case T_RangeVar:
				return_value = _readRangeVar();
				break;
			case T_TableFunc:
				return_value = _readTableFunc();
				break;
			case T_IntoClause:
				return_value = _readIntoClause();
				break;
			case T_CopyIntoClause:
				return_value = _readCopyIntoClause();
				break;
			case T_RefreshClause:
				return_value = _readRefreshClause();
				break;
			case T_Var:
				return_value = _readVar();
				break;
			case T_Const:
				return_value = _readConst();
				break;
			case T_Param:
				return_value = _readParam();
				break;
			case T_Aggref:
				return_value = _readAggref();
				break;
			case T_GroupingFunc:
				return_value = _readGroupingFunc();
				break;
			case T_GroupId:
				return_value = _readGroupId();
				break;
			case T_GroupingSetId:
				return_value = _readGroupingSetId();
				break;
			case T_WindowFunc:
				return_value = _readWindowFunc();
				break;
			case T_SubscriptingRef:
				return_value = _readSubscriptingRef();
				break;
			case T_FuncExpr:
				return_value = _readFuncExpr();
				break;
			case T_NamedArgExpr:
				return_value = _readNamedArgExpr();
				break;
			case T_OpExpr:
				return_value = _readOpExpr();
				break;
			case T_DistinctExpr:
				return_value = _readDistinctExpr();
				break;
			case T_ScalarArrayOpExpr:
				return_value = _readScalarArrayOpExpr();
				break;
			case T_BoolExpr:
				return_value = _readBoolExpr();
				break;
			case T_SubLink:
				return_value = _readSubLink();
				break;
			case T_SubPlan:
				return_value = _readSubPlan();
				break;
			case T_AlternativeSubPlan:
				return_value = _readAlternativeSubPlan();
				break;
			case T_FieldSelect:
				return_value = _readFieldSelect();
				break;
			case T_FieldStore:
				return_value = _readFieldStore();
				break;
			case T_RelabelType:
				return_value = _readRelabelType();
				break;
			case T_CoerceViaIO:
				return_value = _readCoerceViaIO();
				break;
			case T_ArrayCoerceExpr:
				return_value = _readArrayCoerceExpr();
				break;
			case T_ConvertRowtypeExpr:
				return_value = _readConvertRowtypeExpr();
				break;
			case T_CollateExpr:
				return_value = _readCollateExpr();
				break;
			case T_CaseExpr:
				return_value = _readCaseExpr();
				break;
			case T_CaseWhen:
				return_value = _readCaseWhen();
				break;
			case T_CaseTestExpr:
				return_value = _readCaseTestExpr();
				break;
			case T_ArrayExpr:
				return_value = _readArrayExpr();
				break;
			case T_A_ArrayExpr:
				return_value = _readA_ArrayExpr();
				break;
			case T_RowExpr:
				return_value = _readRowExpr();
				break;
			case T_RowCompareExpr:
				return_value = _readRowCompareExpr();
				break;
			case T_CoalesceExpr:
				return_value = _readCoalesceExpr();
				break;
			case T_MinMaxExpr:
				return_value = _readMinMaxExpr();
				break;
			case T_NullIfExpr:
				return_value = _readNullIfExpr();
				break;
			case T_NullTest:
				return_value = _readNullTest();
				break;
			case T_BooleanTest:
				return_value = _readBooleanTest();
				break;
			case T_SQLValueFunction:
				return_value = _readSQLValueFunction();
				break;
			case T_XmlExpr:
				return_value = _readXmlExpr();
				break;
			case T_CoerceToDomain:
				return_value = _readCoerceToDomain();
				break;
			case T_CoerceToDomainValue:
				return_value = _readCoerceToDomainValue();
				break;
			case T_SetToDefault:
				return_value = _readSetToDefault();
				break;
			case T_CurrentOfExpr:
				return_value = _readCurrentOfExpr();
				break;
			case T_NextValueExpr:
				return_value = _readNextValueExpr();
				break;
			case T_InferenceElem:
				return_value = _readInferenceElem();
				break;
			case T_TargetEntry:
				return_value = _readTargetEntry();
				break;
			case T_RangeTblRef:
				return_value = _readRangeTblRef();
				break;
			case T_RangeTblFunction:
				return_value = _readRangeTblFunction();
				break;
			case T_TableSampleClause:
				return_value = _readTableSampleClause();
				break;
			case T_JoinExpr:
				return_value = _readJoinExpr();
				break;
			case T_FromExpr:
				return_value = _readFromExpr();
				break;
			case T_OnConflictExpr:
				return_value = _readOnConflictExpr();
				break;
			case T_AppendRelInfo:
				return_value = _readAppendRelInfo();
				break;
			case T_GrantStmt:
				return_value = _readGrantStmt();
				break;
			case T_AccessPriv:
				return_value = _readAccessPriv();
				break;
			case T_ObjectWithArgs:
				return_value = _readObjectWithArgs();
				break;
			case T_GrantRoleStmt:
				return_value = _readGrantRoleStmt();
				break;
			case T_LockStmt:
				return_value = _readLockStmt();
				break;

			case T_PartitionSpec:
				return_value = _readPartitionSpec();
				break;
			case T_PartitionElem:
				return_value = _readPartitionElem();
				break;
			case T_PartitionRangeDatum:
				return_value = _readPartitionRangeDatum();
				break;
			case T_PartitionCmd:
				return_value = _readPartitionCmd();
				break;
			case T_GpAlterPartitionId:
				return_value = _readGpAlterPartitionId();
				break;
			case T_DistributionKeyElem:
				return_value = _readDistributionKeyElem();
				break;
			case T_PartitionBoundSpec:
				return_value = _readPartitionBoundSpec();
				break;
			case T_RestrictInfo:
				return_value = _readRestrictInfo();
				break;
			case T_ExtensibleNode:
				return_value = _readExtensibleNode();
				break;
			case T_CreateStmt:
				return_value = _readCreateStmt();
				break;
			case T_CreateForeignTableStmt:
				return_value = _readCreateForeignTableStmt();
				break;
			case T_ColumnReferenceStorageDirective:
				return_value = _readColumnReferenceStorageDirective();
				break;
			case T_SegfileMapNode:
				return_value = _readSegfileMapNode();
				break;
			case T_ExtTableTypeDesc:
				return_value = _readExtTableTypeDesc();
				break;
			case T_CreateExternalStmt:
				return_value = _readCreateExternalStmt();
				break;
			case T_CreateExtensionStmt:
				return_value = _readCreateExtensionStmt();
				break;
			case T_IndexStmt:
				return_value = _readIndexStmt();
				break;
			case T_ReindexStmt:
				return_value = _readReindexStmt();
				break;
			case T_ReindexIndexInfo:
				return_value = _readReindexIndexInfo();
				break;

			case T_ConstraintsSetStmt:
				return_value = _readConstraintsSetStmt();
				break;

			case T_CreateFunctionStmt:
				return_value = _readCreateFunctionStmt();
				break;
			case T_FunctionParameter:
				return_value = _readFunctionParameter();
				break;
			case T_AlterFunctionStmt:
				return_value = _readAlterFunctionStmt();
				break;

			case T_DefineStmt:
				return_value = _readDefineStmt();
				break;

			case T_CompositeTypeStmt:
				return_value = _readCompositeTypeStmt();
				break;
			case T_CreateEnumStmt:
				return_value = _readCreateEnumStmt();
				break;
			case T_CreateRangeStmt:
				return_value = _readCreateRangeStmt();
				break;
			case T_AlterEnumStmt:
				return_value = _readAlterEnumStmt();
				break;
			case T_CreateCastStmt:
				return_value = _readCreateCastStmt();
				break;
			case T_CreateOpClassStmt:
				return_value = _readCreateOpClassStmt();
				break;
			case T_CreateOpClassItem:
				return_value = _readCreateOpClassItem();
				break;
			case T_CreateOpFamilyStmt:
				return_value = _readCreateOpFamilyStmt();
				break;
			case T_CreateStatsStmt:
				return_value = _readCreateStatsStmt();
				break;
			case T_AlterOpFamilyStmt:
				return_value = _readAlterOpFamilyStmt();
				break;
			case T_CreateConversionStmt:
				return_value = _readCreateConversionStmt();
				break;
			case T_ViewStmt:
				return_value = _readViewStmt();
				break;
			case T_RuleStmt:
				return_value = _readRuleStmt();
				break;
			case T_DropStmt:
				return_value = _readDropStmt();
				break;

			case T_DropOwnedStmt:
				return_value = _readDropOwnedStmt();
				break;
			case T_ReassignOwnedStmt:
				return_value = _readReassignOwnedStmt();
				break;

			case T_TruncateStmt:
				return_value = _readTruncateStmt();
				break;

			case T_ReplicaIdentityStmt:
				return_value = _readReplicaIdentityStmt();
				break;
			case T_AlterTableStmt:
				return_value = _readAlterTableStmt();
				break;
			case T_AlterTableCmd:
				return_value = _readAlterTableCmd();
				break;
			case T_AlteredTableInfo:
				return_value = _readAlteredTableInfo();
				break;
			case T_NewConstraint:
				return_value = _readNewConstraint();
				break;
			case T_NewColumnValue:
				return_value = _readNewColumnValue();
				break;

			case T_CreateRoleStmt:
				return_value = _readCreateRoleStmt();
				break;
			case T_DropRoleStmt:
				return_value = _readDropRoleStmt();
				break;
			case T_AlterRoleStmt:
				return_value = _readAlterRoleStmt();
				break;
			case T_AlterRoleSetStmt:
				return_value = _readAlterRoleSetStmt();
				break;

			case T_CreateProfileStmt:
				return_value = _readCreateProfileStmt();
				break;
			case T_AlterProfileStmt:
				return_value = _readAlterProfileStmt();
				break;
			case T_DropProfileStmt:
				return_value = _readDropProfileStmt();
				break;

			case T_AlterObjectDependsStmt:
				return_value = _readAlterObjectDependsStmt();
				break;

			case T_AlterSystemStmt:
				return_value = _readAlterSystemStmt();
				break;

			case T_AlterObjectSchemaStmt:
				return_value = _readAlterObjectSchemaStmt();
				break;

			case T_AlterOwnerStmt:
				return_value = _readAlterOwnerStmt();
				break;

			case T_RenameStmt:
				return_value = _readRenameStmt();
				break;

			case T_CreateSeqStmt:
				return_value = _readCreateSeqStmt();
				break;
			case T_AlterSeqStmt:
				return_value = _readAlterSeqStmt();
				break;
			case T_ClusterStmt:
				return_value = _readClusterStmt();
				break;
			case T_CreatedbStmt:
				return_value = _readCreatedbStmt();
				break;
			case T_DropdbStmt:
				return_value = _readDropdbStmt();
				break;
			case T_CreateDomainStmt:
				return_value = _readCreateDomainStmt();
				break;
			case T_AlterDomainStmt:
				return_value = _readAlterDomainStmt();
				break;
			case T_AlterDefaultPrivilegesStmt:
				return_value = _readAlterDefaultPrivilegesStmt();
				break;

			case T_NotifyStmt:
				return_value = _readNotifyStmt();
				break;
			case T_DeclareCursorStmt:
				return_value = _readDeclareCursorStmt();
				break;

			case T_SingleRowErrorDesc:
				return_value = _readSingleRowErrorDesc();
				break;
			case T_CopyStmt:
				return_value = _readCopyStmt();
				break;
			case T_SelectStmt:
				return_value = _readSelectStmt();
				break;
			case T_InsertStmt:
				return_value = _readInsertStmt();
				break;
			case T_DeleteStmt:
				return_value = _readDeleteStmt();
				break;
			case T_UpdateStmt:
				return_value = _readUpdateStmt();
				break;
			case T_ColumnDef:
				return_value = _readColumnDef();
				break;
			case T_TypeName:
				return_value = _readTypeName();
				break;
			case T_SortBy:
				return_value = _readSortBy();
				break;
			case T_TypeCast:
				return_value = _readTypeCast();
				break;
			case T_CollateClause:
				return_value = _readCollateClause();
				break;
			case T_IndexElem:
				return_value = _readIndexElem();
				break;
			case T_Query:
				return_value = _readQuery();
				break;
			case T_WithCheckOption:
				return_value = _readWithCheckOption();
				break;
			case T_SortGroupClause:
				return_value = _readSortGroupClause();
				break;
			case T_DMLActionExpr:
				return_value = _readDMLActionExpr();
				break;
			case T_GroupingSet:
				return_value = _readGroupingSet();
				break;
			case T_WindowClause:
				return_value = _readWindowClause();
				break;
			case T_RowMarkClause:
				return_value = _readRowMarkClause();
				break;
			case T_CTESearchClause:
				return_value = _readCTESearchClause();
				break;
			case T_CTECycleClause:
				return_value = _readCTECycleClause();
				break;
			case T_WithClause:
				return_value = _readWithClause();
				break;
			case T_CommonTableExpr:
				return_value = _readCommonTableExpr();
				break;
			case T_RoleSpec:
				return_value = _readRoleSpec();
				break;
			case T_SetOperationStmt:
				return_value = _readSetOperationStmt();
				break;
			case T_RangeTblEntry:
				return_value = _readRangeTblEntry();
				break;
			case T_A_Expr:
				return_value = _readAExpr();
				break;
			case T_ColumnRef:
				return_value = _readColumnRef();
				break;
			case T_ParamRef:
				return_value = _readParamRef();
				break;
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
			case T_A_Const:
				return_value = _readAConst();
				break;
			case T_A_Star:
				return_value = _readA_Star();
				break;
			case T_A_Indices:
				return_value = _readA_Indices();
				break;
			case T_A_Indirection:
				return_value = _readA_Indirection();
				break;
			case T_ResTarget:
				return_value = _readResTarget();
				break;
			case T_MultiAssignRef:
				return_value = _readMultiAssignRef();
				break;
			case T_Constraint:
				return_value = _readConstraint();
				break;
			case T_FuncCall:
				return_value = _readFuncCall();
				break;
			case T_DefElem:
				return_value = _readDefElem();
				break;
			case T_CreateSchemaStmt:
				return_value = _readCreateSchemaStmt();
				break;
			case T_AlterSchemaStmt:
				return_value = _readAlterSchemaStmt();
				break;
			case T_CreateTagStmt:
				return_value = _readCreateTagStmt();
				break;
			case T_AlterTagStmt:
				return_value = _readAlterTagStmt();
				break;
			case T_DropTagStmt:
				return_value = _readDropTagStmt();
				break;
			case T_CreatePLangStmt:
				return_value = _readCreatePLangStmt();
				break;
			case T_VacuumStmt:
				return_value = _readVacuumStmt();
				break;
			case T_VacuumRelation:
				return_value = _readVacuumRelation();
				break;
			case T_CdbProcess:
				return_value = _readCdbProcess();
				break;
			case T_SliceTable:
				return_value = _readSliceTable();
				break;
			case T_CursorPosInfo:
				return_value = _readCursorPosInfo();
				break;
			case T_VariableSetStmt:
				return_value = _readVariableSetStmt();
				break;
			case T_CreateTrigStmt:
				return_value = _readCreateTrigStmt();
				break;
			case T_TriggerTransition:
				return_value = _readTriggerTransition();
				break;

			case T_CreateTableSpaceStmt:
				return_value = _readCreateTableSpaceStmt();
				break;
			case T_AlterTableSpaceOptionsStmt:
				return_value = _readAlterTableSpaceOptionsStmt();
				break;
			case T_DropTableSpaceStmt:
				return_value = _readDropTableSpaceStmt();
				break;

			case T_CreateQueueStmt:
				return_value = _readCreateQueueStmt();
				break;
			case T_AlterQueueStmt:
				return_value = _readAlterQueueStmt();
				break;
			case T_DropQueueStmt:
				return_value = _readDropQueueStmt();
				break;

			case T_CreateResourceGroupStmt:
				return_value = _readCreateResourceGroupStmt();
				break;
			case T_DropResourceGroupStmt:
				return_value = _readDropResourceGroupStmt();
				break;
			case T_AlterResourceGroupStmt:
				return_value = _readAlterResourceGroupStmt();
				break;

			case T_CommentStmt:
				return_value = _readCommentStmt();
				break;
			case T_DenyLoginInterval:
				return_value = _readDenyLoginInterval();
				break;
			case T_DenyLoginPoint:
				return_value = _readDenyLoginPoint();
				break;

			case T_TableValueExpr:
				return_value = _readTableValueExpr();
				break;

			case T_AlterTypeStmt:
				return_value = _readAlterTypeStmt();
				break;
			case T_AlterExtensionStmt:
				return_value = _readAlterExtensionStmt();
				break;
			case T_AlterExtensionContentsStmt:
				return_value = _readAlterExtensionContentsStmt();
				break;

			case T_TupleDescNode:
				return_value = _readTupleDescNode();
				break;
			case T_SerializedParams:
				return_value = _readSerializedParams();
				break;

			case T_AlterTSConfigurationStmt:
				return_value = _readAlterTSConfigurationStmt();
				break;
			case T_AlterTSDictionaryStmt:
				return_value = _readAlterTSDictionaryStmt();
				break;

			case T_CookedConstraint:
				return_value = _readCookedConstraint();
				break;

			case T_DropUserMappingStmt:
				return_value = _readDropUserMappingStmt();
				break;
			case T_AlterUserMappingStmt:
				return_value = _readAlterUserMappingStmt();
				break;
			case T_CreateUserMappingStmt:
				return_value = _readCreateUserMappingStmt();
				break;
			case T_CreateStorageUserMappingStmt:
				return_value = _readCreateStorageUserMappingStmt();
				break;
			case T_AlterStorageUserMappingStmt:
				return_value = _readAlterStorageUserMappingStmt();
				break;
			case T_DropStorageUserMappingStmt:
				return_value = _readDropStorageUserMappingStmt();
				break;
			case T_AlterForeignServerStmt:
				return_value = _readAlterForeignServerStmt();
				break;
			case T_CreateForeignServerStmt:
				return_value = _readCreateForeignServerStmt();
				break;
			case T_AddForeignSegStmt:
				return_value = _readAddForeignSegStmt();
				break;
			case T_AlterFdwStmt:
				return_value = _readAlterFdwStmt();
				break;
			case T_CreateStorageServerStmt:
				return_value = _readCreateStorageServerStmt();
				break;
			case T_AlterStorageServerStmt:
				return_value = _readAlterStorageServerStmt();
				break;
			case T_DropStorageServerStmt:
				return_value = _readDropStorageServerStmt();
				break;
			case T_CreateFdwStmt:
				return_value = _readCreateFdwStmt();
				break;
			case T_ModifyTable:
				return_value = _readModifyTable();
				break;
			case T_LockRows:
				return_value = _readLockRows();
				break;
			case T_GpPolicy:
				return_value = _readGpPolicy();
				break;
			case T_DistributedBy:
				return_value = _readDistributedBy();
				break;
			case T_ImportForeignSchemaStmt:
				return_value = _readImportForeignSchemaStmt();
				break;
			case T_AlterTableMoveAllStmt:
				return_value = _readAlterTableMoveAllStmt();
				break;

			case T_CreatePublicationStmt:
				return_value = _readCreatePublicationStmt();
				break;
			case T_AlterPublicationStmt:
				return_value = _readAlterPublicationStmt();
				break;
			case T_CreateSubscriptionStmt:
				return_value = _readCreateSubscriptionStmt();
				break;
			case T_DropSubscriptionStmt:
				return_value = _readDropSubscriptionStmt();
				break;
			case T_AlterSubscriptionStmt:
				return_value = _readAlterSubscriptionStmt();
				break;

			case T_CreatePolicyStmt:
				return_value = _readCreatePolicyStmt();
				break;
			case T_AlterPolicyStmt:
				return_value = _readAlterPolicyStmt();
				break;
			case T_CreateTransformStmt:
				return_value = _readCreateTransformStmt();
				break;
			case T_CreateAmStmt:
				return_value = _readCreateAmStmt();
				break;
			case T_LockingClause:
				return_value = _readLockingClause();
				break;
			case T_AggExprId:
				return_value = _readAggExprId();
				break;
			case T_RowIdExpr:
				return_value = _readRowIdExpr();
				break;
			case T_GpDropPartitionCmd:
				return_value = _readGpDropPartitionCmd();
				break;
			case T_GpPartitionRangeSpec:
				return_value = _readGpPartitionRangeSpec();
				break;
			case T_GpPartitionRangeItem:
				return_value = _readGpPartitionRangeItem();
				break;
			case T_GpPartitionListSpec:
				return_value = _readGpPartitionListSpec();
				break;
			case T_GpAlterPartitionCmd:
				return_value = _readGpAlterPartitionCmd();
				break;
			case T_GpPartitionDefinition:
				return_value = _readGpPartitionDefinition();
				break;
			case T_GpSplitPartitionCmd:
				return_value = _readGpSplitPartitionCmd();
				break;
			case T_ReturnStmt:
				return_value = _readReturnStmt();
				break;
			case T_StatsElem:
				return_value = _readStatsElem();
				break;
			case T_EphemeralNamedRelationInfo:
				return_value = _readEphemeralNamedRelationInfo();
				break;
			case T_AlterDatabaseStmt:
				return_value = _readAlterDatabaseStmt();
				break;
			case T_CreateDirectoryTableStmt:
				return_value = _readCreateDirectoryTableStmt();
				break;
			case T_AlterDirectoryTableStmt:
				return_value = _readAlterDirectoryTableStmt();
				break;
			case T_DropDirectoryTableStmt:
				return_value = _readDropDirectoryTableStmt();
				break;
			case T_CreateTaskStmt:
				return_value = _readCreateTaskStmt();
				break;
			case T_AlterTaskStmt:
				return_value = _readAlterTaskStmt();
				break;
			case T_DropTaskStmt:
				return_value = _readDropTaskStmt();
				break;
			case T_RTEPermissionInfo:
				return_value = _readRTEPermissionInfo();
				break;
			case T_MergeAction:
				return_value = _readMergeAction();
				break;
			case T_PublicationObjSpec:
				return_value = _readPublicationObjSpec();
				break;
			case T_PublicationTable:
				return_value = _readPublicationTable();
				break;
			case T_WindowDef:
				return_value = _readWindowDef();
				break;
			case T_JsonConstructorExpr:
				return_value = _readJsonConstructorExpr();
				break;
			case T_JsonIsPredicate:
				return_value = _readJsonIsPredicate();
				break;
			case T_JsonReturning:
				return_value = _readJsonReturning();
				break;
			case T_JsonValueExpr:
				return_value = _readJsonValueExpr();
				break;
			case T_JsonFormat:
				return_value = _readJsonFormat();
				break;
			case T_PlaceHolderVar:
				return_value = _readPlaceHolderVar();
				break;
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
