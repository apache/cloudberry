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
 * cbdb_wrappers.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/cbdb_wrappers.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/cbdb_wrappers.h"

#include <algorithm>

#include "access/paxc_rel_options.h"
#include "comm/paxc_wrappers.h"
extern "C" {
const char *progname;
}

namespace cbdb {

MemoryContext pax_memory_context = nullptr;

CAutoExceptionStack::CAutoExceptionStack(void **global_exception_stack,
                                         void **global_error_context_stack)
    : m_global_exception_stack_(global_exception_stack),
      m_global_error_context_stack_(global_error_context_stack),
      m_exception_stack_(*global_exception_stack),
      m_error_context_stack_(*global_error_context_stack) {}

CAutoExceptionStack::~CAutoExceptionStack() {
  *m_global_exception_stack_ = m_exception_stack_;
  *m_global_error_context_stack_ = m_error_context_stack_;
}

// set the exception stack to the given address
void CAutoExceptionStack::SetLocalJmp(void *local_jump) {
  *m_global_exception_stack_ = local_jump;
}

void *MemCtxAlloc(MemoryContext ctx, size_t size) {
  CBDB_WRAP_START;
  {
    { return MemoryContextAlloc(ctx, (Size)size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc(size_t size) {
  CBDB_WRAP_START;
  {
    { return palloc(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc0(size_t size) {
  CBDB_WRAP_START;
  {
    { return palloc0(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *RePalloc(void *ptr, size_t size) {
  CBDB_WRAP_START;
  { return repalloc(ptr, size); }
  CBDB_WRAP_END;
  return nullptr;
}

void Pfree(void *ptr) {
  CBDB_WRAP_START;
  { pfree(ptr); }
  CBDB_WRAP_END;
}

}  // namespace cbdb

MemoryContext cbdb::AllocSetCtxCreate(MemoryContext parent, const char *name,
                                      Size min_context_size,
                                      Size init_block_size,
                                      Size max_block_size) {
  CBDB_WRAP_START;
  {
    return AllocSetContextCreateInternal(parent, name, min_context_size,
                                         init_block_size, max_block_size);
  }
  CBDB_WRAP_END;
  return nullptr;
}

void cbdb::MemoryCtxDelete(MemoryContext memory_context) {
  CBDB_WRAP_START;
  { MemoryContextDelete(memory_context); }
  CBDB_WRAP_END;
}

void cbdb::MemoryCtxRegisterResetCallback(MemoryContext context,
                                          MemoryContextCallback *cb) {
  CBDB_WRAP_START;
  { MemoryContextRegisterResetCallback(context, cb); }
  CBDB_WRAP_END;
}

#ifdef RUN_GTEST
Datum cbdb::DatumFromCString(const char *src, size_t length) {
  CBDB_WRAP_START;
  {
    text *result = reinterpret_cast<text *>(palloc(length + VARHDRSZ));
    SET_VARSIZE(result, length + VARHDRSZ);
    memcpy(VARDATA(result), src, length);
    return PointerGetDatum(result);
  }
  CBDB_WRAP_END;
  return 0;
}

Datum cbdb::DatumFromPointer(const void *p, int16 typlen) {
  CBDB_WRAP_START;
  {
    char *resultptr;
    resultptr = reinterpret_cast<char *>(palloc(typlen));
    memcpy(resultptr, p, typlen);
    return PointerGetDatum(resultptr);
  }
  CBDB_WRAP_END;
  return 0;
}
#endif

BpChar *cbdb::BpcharInput(const char *s, size_t len, int32 atttypmod) {
  CBDB_WRAP_START;
  { return bpchar_input(s, len, atttypmod); }
  CBDB_WRAP_END;
  return nullptr;
}

VarChar *cbdb::VarcharInput(const char *s, size_t len, int32 atttypmod) {
  CBDB_WRAP_START;
  { return varchar_input(s, len, atttypmod); }
  CBDB_WRAP_END;
  return nullptr;
}

text *cbdb::CstringToText(const char *s, size_t len) {
  CBDB_WRAP_START;
  { return cstring_to_text_with_len(s, len); }
  CBDB_WRAP_END;
  return nullptr;
}

Numeric cbdb::DatumToNumeric(Datum d) {
  CBDB_WRAP_START;
  { return DatumGetNumeric(d); }
  CBDB_WRAP_END;
  return nullptr;
}

ArrayType *cbdb::DatumToArrayTypeP(Datum d) {
  CBDB_WRAP_START;
  { return DatumGetArrayTypeP(d); }
  CBDB_WRAP_END;
}

int cbdb::ArrayGetN(int ndim, const int *dims) {
  CBDB_WRAP_START;
  { return ArrayGetNItems(ndim, dims); }
  CBDB_WRAP_END;
  return 0;
}

ArrayIterator cbdb::ArrayCreateIterator(ArrayType *arr, int slice_ndim,
                                        ArrayMetaState *mstate) {
  CBDB_WRAP_START;
  { return array_create_iterator(arr, slice_ndim, mstate); }
  CBDB_WRAP_END;
  return nullptr;
}

bool cbdb::ArrayIterate(ArrayIterator iterator, Datum *value, bool *isnull) {
  CBDB_WRAP_START;
  { return array_iterate(iterator, value, isnull); }
  CBDB_WRAP_END;
  return false;
}

void cbdb::ArrayFreeIterator(ArrayIterator iterator) {
  CBDB_WRAP_START;
  { return array_free_iterator(iterator); }
  CBDB_WRAP_END;
}

ArrayType *cbdb::ConstructMdArrayType(Datum *datums, bool *nulls, int len,
                                      Oid atttypid, int attlen, bool attbyval,
                                      char attalign) {
  CBDB_WRAP_START;
  {
    int dims[1];
    int lbs[1];

    dims[0] = len;
    lbs[0] = 1;
    return construct_md_array(datums, nulls, 1, dims, lbs, atttypid, attlen,
                              attbyval, attalign);
  }
  CBDB_WRAP_END;
  return nullptr;
}

struct varlena *cbdb::PgDeToastDatum(struct varlena *datum) {
  CBDB_WRAP_START;
  { return detoast_attr(datum); }
  CBDB_WRAP_END;
  return nullptr;
}

struct varlena *cbdb::PgDeToastDatumPacked(struct varlena *datum) {
  CBDB_WRAP_START;
  { return pg_detoast_datum_packed(datum); }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::PointerAndLenFromDatum(Datum d, int *len) {
  struct varlena *vl = nullptr;
  CBDB_WRAP_START;
  {
    vl = (struct varlena *)DatumGetPointer(d);
    *len = VARSIZE_ANY(vl);
    return static_cast<void *>(vl);
  }
  CBDB_WRAP_END;
}

void cbdb::SlotGetMissingAttrs(TupleTableSlot *slot, int start_attno,
                               int last_attno) {
  CBDB_WRAP_START;
  { slot_getmissingattrs(slot, start_attno, last_attno); }
  CBDB_WRAP_END;
}

int cbdb::PathNameCreateDir(const char *path) {
  // no need to wrap, it calls only posix API.
  { return MakePGDirectory(path); }
}

HeapTuple cbdb::SearchSysCache(Relation rel, SysCacheIdentifier id) {
  CBDB_WRAP_START;
  { return SearchSysCache1(id, RelationGetRelid(rel)); }
  CBDB_WRAP_END;
}

bool cbdb::TupleIsValid(HeapTuple tupcache) {
  CBDB_WRAP_START;
  { return HeapTupleIsValid(tupcache); }
  CBDB_WRAP_END;
}

void cbdb::ReleaseTupleCache(HeapTuple tupcache) {
  CBDB_WRAP_START;
  { ReleaseSysCache(tupcache); }
  CBDB_WRAP_END;
}

void cbdb::PathNameDeleteDir(const char *path, bool delete_topleveldir) {
  CBDB_WRAP_START;
  { paxc::DeletePaxDirectoryPath(path, delete_topleveldir); }
  CBDB_WRAP_END;
}

void cbdb::MakedirRecursive(const char *path) {
  CBDB_WRAP_START;
  { paxc::MakedirRecursive(path); }
  CBDB_WRAP_END;
}

std::string cbdb::BuildPaxDirectoryPath(RelFileNode rd_node,
                                        BackendId rd_backend) {
  CBDB_WRAP_START;
  {
    char *tmp_str =
        paxc::BuildPaxDirectoryPath(rd_node, rd_backend);
    std::string ret_str(tmp_str);
    pfree(tmp_str);
    return ret_str;
  }
  CBDB_WRAP_END;
}

std::string cbdb::BuildPaxFilePath(const char *rel_path, const char *block_id) {
  char path[4096];
  Assert(rel_path && block_id);

  snprintf(path, sizeof(path), "%s/%s", rel_path, block_id);
  return std::string(path);
}

int cbdb::RelationGetAttributesNumber(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetNumberOfAttributes(rel); }
  CBDB_WRAP_END;
}

StdRdOptions **cbdb::RelGetAttributeOptions(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetAttributeOptions(rel); }
  CBDB_WRAP_END;
}

TupleDesc cbdb::RelationGetTupleDesc(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetDescr(rel); }
  CBDB_WRAP_END;
}

bool cbdb::IsSystemAttrNumExist(struct PaxcExtractcolumnContext *context,
                                AttrNumber number) {
  Assert(number < 0 && number > FirstLowInvalidHeapAttributeNumber && context);
  return context->system_attr_number_mask[~number];
}

bool cbdb::NeedWAL(Relation rel) {
  CBDB_WRAP_START;
  { return paxc::NeedWAL(rel); }
  CBDB_WRAP_END;
}

extern "C" {

static bool paxc_extractcolumns_walker(  // NOLINT
    Node *node, struct PaxcExtractcolumnContext *ec_ctx) {
  if (node == NULL) {
    return false;
  }

  if (IsA(node, Var)) {
    Var *var = (Var *)node;

    if (IS_SPECIAL_VARNO(var->varno)) return false;

    if (var->varattno < 0) {
      Assert(var->varattno > FirstLowInvalidHeapAttributeNumber);
      ec_ctx->system_attr_number_mask[~var->varattno] = true;
    } else if (!ec_ctx->col_bits.empty()) {
      auto natts = static_cast<int>(ec_ctx->col_bits.size());
      if (var->varattno == 0) {
        // If all attributes are included,
        // set all entries in mask to true.
        for (int attno = 0; attno < natts; attno++)
          ec_ctx->col_bits[attno] = true;
        ec_ctx->found = true;
      } else if (var->varattno <= natts) {
        ec_ctx->col_bits[var->varattno - 1] = true;
        ec_ctx->found = true;
      }
      // Still need fill `system_attr_number_mask`
      // Let this case return false
    }

    return false;
  }

  return expression_tree_walker(
      node,
      (bool (*)())(void (*)() /* to make -Wcast-function-type happy*/)(
          paxc_extractcolumns_walker),
      (void *)ec_ctx);
}

};  // extern "C"

bool cbdb::ExtractcolumnsFromNode(Node *expr,
                                  struct PaxcExtractcolumnContext *ec_ctx) {
  CBDB_WRAP_START;
  {
    paxc_extractcolumns_walker(expr, ec_ctx);
    return ec_ctx->found;
  }
  CBDB_WRAP_END;
}

bool cbdb::ExtractcolumnsFromNode(Node *expr, std::vector<bool> &col_bits) {
  CBDB_WRAP_START;
  {
    PaxcExtractcolumnContext ec_ctx(col_bits);
    paxc_extractcolumns_walker(expr, &ec_ctx);
    return ec_ctx.found;
  }
  CBDB_WRAP_END;
}

bool cbdb::PGGetOperator(const char *operatorName, Oid operatorNamespace,
                         Oid leftObjectId, Oid rightObjectId, Oid *opno,
                         FmgrInfo *finfo) {
  CBDB_WRAP_START;
  {
    return paxc::PGGetOperator(operatorName, operatorNamespace, leftObjectId,
                               rightObjectId, opno, finfo);
  }
  CBDB_WRAP_END;
}

bool cbdb::PGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft,
                           Oid *oprright, FmgrInfo *finfo) {
  CBDB_WRAP_START;
  { return paxc::PGGetOperatorNo(opno, oprname, oprleft, oprright, finfo); }
  CBDB_WRAP_END;
}

bool cbdb::PGGetAddOperator(Oid atttypid, Oid subtype, Oid namespc,
                            Oid *resulttype, FmgrInfo *finfo) {
  CBDB_WRAP_START;
  {
    return paxc::PGGetAddOperator(atttypid, subtype, namespc, resulttype,
                                  finfo);
  }
  CBDB_WRAP_END;
}

bool cbdb::PGGetProc(Oid procoid, FmgrInfo *finfo) {
  CBDB_WRAP_START;
  { return paxc::PGGetProc(procoid, finfo); }
  CBDB_WRAP_END;
}

bool cbdb::SumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                             FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                             bool *final_func_exist, FmgrInfo *add_finfo) {
  CBDB_WRAP_START;
  {
    return paxc::SumAGGGetProcinfo(atttypid, prorettype, transtype, trans_finfo,
                                   final_finfo, final_func_exist, add_finfo);
  }
  CBDB_WRAP_END;
}

bool cbdb::PGGetAggInfo(const char *procedure, Oid atttypid, Oid *prorettype,
                        Oid *transtype, FmgrInfo *trans_finfo,
                        FmgrInfo *final_finfo, bool *final_func_exist,
                        bool *agginitval_isnull) {
  CBDB_WRAP_START;
  {
    return paxc::PGGetAggInfo(procedure, atttypid, prorettype, transtype,
                              trans_finfo, final_finfo, final_func_exist,
                              agginitval_isnull);
  }
  CBDB_WRAP_END;
}

Datum cbdb::SumFuncCall(FmgrInfo *flinfo, AggState *state, Datum arg1,
                        Datum arg2) {
  CBDB_WRAP_START;
  { return paxc::SumFuncCall(flinfo, state, arg1, arg2); }
  CBDB_WRAP_END;
}

Datum cbdb::datumCopy(Datum value, bool typByVal, int typLen) {
  CBDB_WRAP_START;
  { return ::datumCopy(value, typByVal, typLen); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1) {
  CBDB_WRAP_START;
  { return ::FunctionCall1Coll(flinfo, collation, arg1); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2) {
  CBDB_WRAP_START;
  { return ::FunctionCall2Coll(flinfo, collation, arg1, arg2); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall3Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2, Datum arg3) {
  CBDB_WRAP_START;
  { return ::FunctionCall3Coll(flinfo, collation, arg1, arg2, arg3); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall4Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2, Datum arg3, Datum arg4) {
  CBDB_WRAP_START;
  { return ::FunctionCall4Coll(flinfo, collation, arg1, arg2, arg3, arg4); }
  CBDB_WRAP_END;
}

int16 cbdb::GetTyplen(Oid typid) {
  CBDB_WRAP_START;
  { return get_typlen(typid); }
  CBDB_WRAP_END;
}

bool cbdb::GetTypbyval(Oid typid) {
  CBDB_WRAP_START;
  { return get_typbyval(typid); }
  CBDB_WRAP_END;
}

SysScanDesc cbdb::SystableBeginScan(Relation rel, Oid index_id, bool index_ok,
                                    Snapshot snapshot, int n_keys,
                                    ScanKey keys) {
  CBDB_WRAP_START;
  {
    return systable_beginscan(rel, index_id, index_ok, snapshot, n_keys, keys);
  }
  CBDB_WRAP_END;
}

HeapTuple cbdb::SystableGetNext(SysScanDesc desc) {
  CBDB_WRAP_START;
  { return systable_getnext(desc); }
  CBDB_WRAP_END;
}

void cbdb::SystableEndScan(SysScanDesc desc) {
  CBDB_WRAP_START;
  { return systable_endscan(desc); }
  CBDB_WRAP_END;
}

Datum cbdb::HeapGetAttr(HeapTuple tup, int attnum, TupleDesc tuple_desc,
                        bool *isnull) {
  CBDB_WRAP_START;
  { return heap_getattr(tup, attnum, tuple_desc, isnull); }
  CBDB_WRAP_END;
}

Relation cbdb::TableOpen(Oid relid, LOCKMODE lockmode) {
  CBDB_WRAP_START;
  { return table_open(relid, lockmode); }
  CBDB_WRAP_END;
}

void cbdb::TableClose(Relation rel, LOCKMODE lockmode) {
  CBDB_WRAP_START;
  { return table_close(rel, lockmode); }
  CBDB_WRAP_END;
}

void cbdb::RelOpenSmgr(Relation rel) {
  CBDB_WRAP_START;
  {
    Assert(RelationIsPAX(rel));
    if ((rel)->rd_smgr == NULL)
      smgrsetowner(&((rel)->rd_smgr),
                   smgropen((rel)->rd_node, (rel)->rd_backend, SMGR_PAX, rel));
  }
  CBDB_WRAP_END;
}

void cbdb::RelCloseSmgr(Relation rel) {
  CBDB_WRAP_START;
  { RelationCloseSmgr(rel); }
  CBDB_WRAP_END;
}

void cbdb::RelDropStorage(Relation rel) {
  CBDB_WRAP_START;
  { RelationDropStorage(rel); }
  CBDB_WRAP_END;
}

void cbdb::PaxRelationCreateStorage(RelFileNode rnode, Relation rel) {
  CBDB_WRAP_START;
  { paxc::PaxRelationCreateStorage(rnode, rel); }
  CBDB_WRAP_END;
}

char *cbdb::GetGUCConfigOptionByName(const char *name, const char **varname,
                                     bool missing_ok) {
  CBDB_WRAP_START;
  { return GetConfigOptionByName(name, varname, missing_ok); }
  CBDB_WRAP_END;
}

void cbdb::ExecDropSingleTupleTableSlot(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { ::ExecDropSingleTupleTableSlot(slot); }
  CBDB_WRAP_END;
}

TupleTableSlot *cbdb::MakeSingleTupleTableSlot(
    TupleDesc tupdesc, const TupleTableSlotOps *tts_ops) {
  CBDB_WRAP_START;
  { return ::MakeSingleTupleTableSlot(tupdesc, tts_ops); }
  CBDB_WRAP_END;
}

void cbdb::SlotGetAllAttrs(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { ::slot_getallattrs(slot); }
  CBDB_WRAP_END;
}

void cbdb::ExecClearTuple(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { ::ExecClearTuple(slot); }
  CBDB_WRAP_END;
}

void cbdb::ExecStoreVirtualTuple(TupleTableSlot *slot) {
  CBDB_WRAP_START;
  { ::ExecStoreVirtualTuple(slot); }
  CBDB_WRAP_END;
}
