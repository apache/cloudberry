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
 * cbdb_wrappers.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/cbdb_wrappers.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include <cstddef>
#include <string>
#include <vector>

#include "exceptions/CException.h"

struct PaxcExtractcolumnContext {
  // If cols set and call ExtractcolumnsFromNode with
  // `target list`. Then the cols will fill with projection mask.
  PaxcExtractcolumnContext(std::vector<bool> &col_bitmap)
      : col_bits(col_bitmap) {}
  std::vector<bool> &col_bits;
  bool found = false;

  // This mask use to filter system attribute number.
  // (~AttrNumber) will be index, mapping the [0,
  // FirstLowInvalidHeapAttributeNumber) call `IsSystemAttrNumExist` to check
  // system-defined attributes set
  bool system_attr_number_mask[~FirstLowInvalidHeapAttributeNumber] = {
      0};  // NOLINT
};

namespace cbdb {

#define PAX_ALLOCSET_DEFAULT_MINSIZE ALLOCSET_DEFAULT_MINSIZE
#define PAX_ALLOCSET_DEFAULT_INITSIZE (8 * 1024)
#define PAX_ALLOCSET_DEFAULT_MAXSIZE (3 * 64 * 1024 * 1024)

#define PAX_ALLOCSET_DEFAULT_SIZES                             \
  PAX_ALLOCSET_DEFAULT_MINSIZE, PAX_ALLOCSET_DEFAULT_INITSIZE, \
      PAX_ALLOCSET_DEFAULT_MAXSIZE
extern MemoryContext pax_memory_context;

//---------------------------------------------------------------------------
//  @class:
//      CAutoExceptionStack
//
//  @doc:
//      Auto object for saving and restoring exception stack
//
//---------------------------------------------------------------------------
class CAutoExceptionStack final {
 public:
  CAutoExceptionStack(const CAutoExceptionStack &) = delete;
  CAutoExceptionStack(CAutoExceptionStack &&) = delete;
  void *operator new(std::size_t count, ...) = delete;
  void *operator new[](std::size_t count, ...) = delete;

  // ctor
  CAutoExceptionStack(void **global_exception_stack,
                      void **global_error_context_stack);

  // dtor
  ~CAutoExceptionStack();

  // set the exception stack to the given address
  void SetLocalJmp(void *local_jump);

 private:
  // address of the global exception stack value
  void **m_global_exception_stack_;

  // address of the global error context stack value
  void **m_global_error_context_stack_;

  // value of exception stack when object is created
  void *m_exception_stack_;

  // value of error context stack when object is created
  void *m_error_context_stack_;
};  // class CAutoExceptionStack

void *MemCtxAlloc(MemoryContext ctx, size_t size);
void *Palloc(size_t size);
void *Palloc0(size_t size);
void *RePalloc(void *ptr, size_t size);
void Pfree(void *ptr);

MemoryContext AllocSetCtxCreate(MemoryContext parent, const char *name,
                                Size min_context_size, Size init_block_size,
                                Size max_block_size);
void MemoryCtxDelete(MemoryContext memory_context);
void MemoryCtxRegisterResetCallback(MemoryContext context,
                                    MemoryContextCallback *cb);

static inline void *DatumToPointer(Datum d) noexcept {
  return DatumGetPointer(d);
}

static inline Datum PointerToDatum(void *p) noexcept {
  return PointerGetDatum(p);
}

static inline int8 DatumToInt8(Datum d) noexcept { return DatumGetInt8(d); }

static inline int16 DatumToInt16(Datum d) noexcept { return DatumGetInt16(d); }

static inline int32 DatumToInt32(Datum d) noexcept { return DatumGetInt32(d); }

static inline int64 DatumToInt64(Datum d) noexcept { return DatumGetInt64(d); }

static inline Datum Int8ToDatum(int8 d) noexcept { return Int8GetDatum(d); }

static inline Datum Int16ToDatum(int16 d) noexcept { return Int16GetDatum(d); }

static inline Datum Int32ToDatum(int32 d) noexcept { return Int32GetDatum(d); }

static inline Datum Int64ToDatum(int64 d) noexcept { return Int64GetDatum(d); }

static inline bool DatumToBool(Datum d) noexcept { return DatumGetBool(d); }

static inline Datum BoolToDatum(bool d) noexcept { return BoolGetDatum(d); }

static inline Datum NumericToDatum(Numeric n) noexcept {
  return NumericGetDatum(n);
}

static inline Datum Float4ToDutum(float4 d) noexcept {
  return Float4GetDatum(d);
}

static inline float4 DatumToFloat4(Datum d) noexcept {
  return DatumGetFloat4(d);
}

static inline Datum Float8ToDutum(float8 d) noexcept {
  return Float8GetDatum(d);
}

static inline float8 DatumToFloat8(Datum d) noexcept {
  return DatumGetFloat8(d);
}

static pg_attribute_always_inline Oid RelationGetRelationId(Relation rel) noexcept {
  return RelationGetRelid(rel);
}

BpChar *BpcharInput(const char *s, size_t len, int32 atttypmod);
VarChar *VarcharInput(const char *s, size_t len, int32 atttypmod);
text *CstringToText(const char *s, size_t len);

Numeric DatumToNumeric(Datum d);

ArrayType *DatumToArrayTypeP(Datum d);
int ArrayGetN(int ndim, const int *dims);
ArrayIterator ArrayCreateIterator(ArrayType *arr, int slice_ndim,
                                  ArrayMetaState *mstate);
bool ArrayIterate(ArrayIterator iterator, Datum *value, bool *isnull);
void ArrayFreeIterator(ArrayIterator iterator);
ArrayType *ConstructMdArrayType(Datum *datums, bool *nulls, int len,
                                Oid atttypid, int attlen, bool attbyval,
                                char attalign);

void *PointerAndLenFromDatum(Datum d, int *len);

void SlotGetMissingAttrs(TupleTableSlot *slot, int start_attno, int last_attno);

#ifdef RUN_GTEST
Datum DatumFromCString(const char *src, size_t length);

Datum DatumFromPointer(const void *p, int16 typlen);
#endif

struct varlena *PgDeToastDatum(struct varlena *datum);

struct varlena *PgDeToastDatumPacked(struct varlena *datum);

bool TupleIsValid(HeapTuple tupcache);

void ReleaseTupleCache(HeapTuple tupcache);

int PathNameCreateDir(const char *path);

HeapTuple SearchSysCache(Relation rel, SysCacheIdentifier id);

void PathNameDeleteDir(const char *path, bool delete_topleveldir);

void MakedirRecursive(const char *path);

// gopher file path must start with '/', so we need to add '/' to the path
std::string BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);

std::string BuildPaxFilePath(const char *rel_path, const char *block_id);
static inline std::string BuildPaxFilePath(const std::string &rel_path,
                                           const std::string &block_id) {
  return BuildPaxFilePath(rel_path.c_str(), block_id.c_str());
}

int RelationGetAttributesNumber(Relation rel);

StdRdOptions **RelGetAttributeOptions(Relation rel);
TupleDesc RelationGetTupleDesc(Relation rel);

bool ExtractcolumnsFromNode(Node *expr,
                            struct PaxcExtractcolumnContext *ec_ctx);

bool IsSystemAttrNumExist(struct PaxcExtractcolumnContext *context,
                          AttrNumber number);

bool ExtractcolumnsFromNode(Node *expr, std::vector<bool> &col_bits);
bool PGGetOperator(const char *operatorName, Oid operatorNamespace,
                   Oid leftObjectId, Oid rightObjectId, Oid *opno,
                   FmgrInfo *finfo);
bool PGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft, Oid *oprright,
                     FmgrInfo *finfo);
bool PGGetAddOperator(Oid atttypid, Oid subtype, Oid namespc, Oid *resulttype,
                      FmgrInfo *finfo);
bool PGGetProc(Oid procoid, FmgrInfo *finfo);
bool PGGetAggInfo(const char *procedure, Oid atttypid, Oid *prorettype,
                  Oid *transtype, FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                  bool *final_func_exist, bool *agginitval_isnull);
bool SumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                       FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                       bool *final_func_exist, FmgrInfo *add_finfo);
Datum datumCopy(Datum value, bool typByVal, int typLen);
Datum SumFuncCall(FmgrInfo *flinfo, AggState *state, Datum arg1, Datum arg2);
Datum FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1);
Datum FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                        Datum arg2);
Datum FunctionCall3Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2,
                        Datum arg3);
Datum FunctionCall4Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2,
                        Datum arg3, Datum arg4);
int16 GetTyplen(Oid typid);
bool GetTypbyval(Oid typid);

SysScanDesc SystableBeginScan(Relation rel, Oid index_id, bool index_ok,
                              Snapshot snapshot, int n_keys, ScanKey keys);

HeapTuple SystableGetNext(SysScanDesc desc);

void SystableEndScan(SysScanDesc desc);

Datum HeapGetAttr(HeapTuple tup, int attnum, TupleDesc tuple_desc,
                  bool *isnull);

Relation TableOpen(Oid relid, LOCKMODE lockmode);

void TableClose(Relation rel, LOCKMODE lockmode);

void RelOpenSmgr(Relation rel);

void RelCloseSmgr(Relation rel);

void PaxRelationCreateStorage(RelFileNode rnode, Relation rel);

void RelDropStorage(Relation rel);

char *GetGUCConfigOptionByName(const char *name, const char **varname,
                               bool missing_ok);

bool NeedWAL(Relation rel);

void ExecDropSingleTupleTableSlot(TupleTableSlot *slot);
TupleTableSlot *MakeSingleTupleTableSlot(TupleDesc tupdesc,
                                         const TupleTableSlotOps *tts_ops);

void SlotGetAllAttrs(TupleTableSlot *slot);

void ExecClearTuple(TupleTableSlot *slot);

void ExecStoreVirtualTuple(TupleTableSlot *slot);

}  // namespace cbdb

// clang-format off
#define CBDB_WRAP_START                                           \
  sigjmp_buf local_sigjmp_buf;                                    \
  {                                                               \
      cbdb::CAutoExceptionStack aes(                              \
          reinterpret_cast<void **>(&PG_exception_stack),         \
          reinterpret_cast<void **>(&error_context_stack));       \
      if (0 == sigsetjmp(local_sigjmp_buf, 0))                    \
      {                                                           \
          aes.SetLocalJmp(&local_sigjmp_buf)

#define CBDB_WRAP_END                                             \
  }                                                               \
  else                                                            \
  {                                                               \
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCError);         \
  }                                                               \
  }

#define CBDB_WRAP_FUNCTION(func, ...) do { \
    CBDB_WRAP_START; \
    func(__VA_ARGS__); \
    CBDB_WRAP_END; \
} while(0)
// clang-format on
