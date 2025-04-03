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
 * pax_manifest.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_manifest.cc
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/pax_catalog.h"

#include "access/pax_visimap.h"
#include "comm/cbdb_wrappers.h"
#include "exceptions/CException.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_metadata.h"
#include "storage/micro_partition_stats.h"
#include "storage/pax_itemptr.h"
#include "storage/wal/paxc_wal.h"
#include "storage/wal/pax_wal.h"

namespace paxc {
static inline bool TestVisimap(Relation rel, const char *visimap_name,
                               int offset) {
  CBDB_TRY();
  { return pax::TestVisimap(rel, visimap_name, offset); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
  pg_unreachable();
}
void CPaxCopyAllTuples(Relation old_rel, Relation new_rel, Snapshot snapshot) {
  Assert(RelationIsPAX(old_rel));
  Assert(RelationIsPAX(new_rel));
#ifdef USE_ASSERT_CHECKING
#define CMP_ATTR(a1, a2, field) Assert(a1->field == a2->field)
  {
    auto desc1 = RelationGetDescr(old_rel);
    auto desc2 = RelationGetDescr(new_rel);
    Assert(desc1->natts == desc2->natts);
    for (int i = 0; i < desc1->natts; i++) {
      auto attr1 = TupleDescAttr(desc1, i);
      auto attr2 = TupleDescAttr(desc2, i);

      CMP_ATTR(attr1, attr2, atttypid);
      CMP_ATTR(attr1, attr2, attlen);
      CMP_ATTR(attr1, attr2, attnum);
      CMP_ATTR(attr1, attr2, atttypmod);
      CMP_ATTR(attr1, attr2, attbyval);
      CMP_ATTR(attr1, attr2, attisdropped);
      CMP_ATTR(attr1, attr2, attnotnull);
      Assert(strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) == 0);
    }
  }
#undef CMP_ATTR
#endif
  TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(old_rel),
                                                  table_slot_callbacks(old_rel));

  auto scan = table_beginscan(old_rel, snapshot, 0, nullptr);
  CommandId mycid = GetCurrentCommandId(true);
  while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
    table_tuple_insert(new_rel, slot, mycid, 0, nullptr);
    CHECK_FOR_INTERRUPTS();
  }
  table_endscan(scan);
  ExecDropSingleTupleTableSlot(slot);
}
} // namespace paxc

namespace pax {
void PaxCopyAllDataFiles(Relation rel, const RelFileNode *newrnode,
                         bool createnewpath) {
  std::string src_path;
  std::string dst_path;
  std::vector<std::string> filelist;

  Assert(rel && newrnode);

  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  src_path = cbdb::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
  Assert(!src_path.empty());

  dst_path =
      cbdb::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
  Assert(!dst_path.empty());

  CBDB_CHECK(!src_path.empty() && !dst_path.empty(),
             cbdb::CException::ExType::kExTypeFileOperationError,
             fmt("Fail to build directory path. "
                 "src [spcNode=%u, dbNode=%u, relNode=%u, backend=%d]"
                 "dst [spcNode=%u, dbNode=%u, relNode=%u, backend=%d]",
                 rel->rd_node.spcNode, rel->rd_node.dbNode,
                 rel->rd_node.relNode, rel->rd_backend, newrnode->spcNode,
                 newrnode->dbNode, newrnode->relNode, rel->rd_backend));

  // createnewpath is used to indicate if creating destination micropartition
  // file directory and storage file for copying or not.
  // 1. For RelationCopyData case, createnewpath should be set as true to
  // explicitly create a new destination directory under
  //    new tablespace path pg_tblspc/.
  // 2. For RelationCopyDataForCluster case, createnewpath should be set as
  // false cause the destination directory was already
  //    created with a new temp table by previously calling
  //    PaxAuxRelationSetNewFilenode.
  if (createnewpath) {
    // create micropartition file destination folder for copying.
    CBDB_CHECK((fs->CreateDirectory(dst_path) == 0),
               cbdb::CException::ExType::kExTypeIOError,
               fmt("Fail to create directory [path=%s]", dst_path.c_str()));
  }

  // Get micropatition file source folder filename list for copying, if file
  // list is empty then skip copying file directly.
  filelist = fs->ListDirectory(src_path);
  if (filelist.empty()) return;

  const size_t buffer_size = 1024 * 1024;
  void *buffer = cbdb::Palloc(buffer_size);

  auto need_wal = cbdb::NeedWAL(rel);
  for (auto &iter : filelist) {
    Assert(!iter.empty());
    std::string src_file = src_path;
    std::string dst_file = dst_path;
    src_file.append("/");
    src_file.append(iter);
    dst_file.append("/");
    dst_file.append(iter);
    auto file1 = fs->Open(src_file, pax::fs::kReadMode);
    auto file2 = fs->Open(dst_file, pax::fs::kWriteMode);

    auto size = file1->FileLength();
    int64 offset = 0;
    while (size > 0) {
      auto batch_len = Min(size, buffer_size);
      file1->ReadN(buffer, batch_len);
      file2->WriteN(buffer, batch_len);
      if (need_wal)
        cbdb::XLogPaxInsert(*newrnode, iter.c_str(), offset, buffer, batch_len);
      offset += batch_len;
      size -= batch_len;
    }

    file1->Close();
    file2->Close();
  }

  cbdb::Pfree(buffer);
}

}

extern "C" {
extern Datum MicroPartitionStatsCombineResult(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(MicroPartitionStatsCombineResult);
}


#ifdef USE_MANIFEST_API
extern "C" {
PG_FUNCTION_INFO_V1(pax_get_catalog_rows);
struct fetch_catalog_rows_context {
  Relation relation;
  ManifestRelation mrel;
  ManifestScan mscan;
  TupleDesc tupdesc;
};

Datum pax_get_catalog_rows(PG_FUNCTION_ARGS) {
  FuncCallContext *fctx;
  struct fetch_catalog_rows_context *ctx;
  ManifestTuple mtuple;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldctx;
    TupleDesc tupdesc;
    Oid relid;

    relid = PG_GETARG_OID(0);

    fctx = SRF_FIRSTCALL_INIT();
    oldctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

    tupdesc = CreateTemplateTupleDesc(8);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segment_id",
                      INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2,
                      PAX_AUX_PTBLOCKNAME, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3,
                      PAX_AUX_PTTUPCOUNT, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4,
                      PAX_AUX_PTBLOCKSIZE, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5,
                      PAX_AUX_PTSTATISITICS, PAX_AUX_STATS_TYPE_OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6,
                      PAX_AUX_PTVISIMAPNAME, NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7,
                      PAX_AUX_PTEXISTEXTTOAST, BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8,
                      PAX_AUX_PTISCLUSTERED, BOOLOID, -1, 0);

    ctx = (struct fetch_catalog_rows_context *)palloc(sizeof(*ctx));
    ctx->relation = table_open(relid, AccessShareLock);
    ctx->mrel = manifest_open(ctx->relation);
    ctx->mscan = manifest_beginscan(ctx->mrel, NULL);

    fctx->user_fctx = ctx;
    fctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  } else {
    fctx = SRF_PERCALL_SETUP();
    ctx = (struct fetch_catalog_rows_context *)fctx->user_fctx;
  }

  mtuple = manifest_getnext(ctx->mscan, nullptr);
  if (mtuple) {
    ManifestRelation mrel;
    HeapTuple tuple;
    NameData visimap;
    Datum values[8];
    bool isnull[8];

    mrel = ctx->mrel;
    // segement_id
    values[0] = Int32GetDatum(GpIdentity.segindex);
    isnull[0] = false;

    values[1] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTBLOCKNAME, &isnull[1]);
    values[2] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTTUPCOUNT, &isnull[2]);
    values[3] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTBLOCKSIZE, &isnull[3]);
    values[4] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTSTATISITICS, &isnull[4]);
    values[5] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTVISIMAPNAME, &isnull[5]);
    if (!isnull[5]) {
      namestrcpy(&visimap, DatumGetCString(values[5]));
      values[5] = NameGetDatum(&visimap);
    }
    values[6] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTEXISTEXTTOAST, &isnull[6]);
    values[7] = get_manifesttuple_value(mtuple, mrel, PAX_AUX_PTISCLUSTERED, &isnull[7]);

    tuple = heap_form_tuple(fctx->tuple_desc, values, isnull);
    SRF_RETURN_NEXT(fctx, HeapTupleGetDatum(tuple));
  }

  manifest_endscan(ctx->mscan);
  manifest_close(ctx->mrel);
  table_close(ctx->relation, AccessShareLock);

  ctx->relation = nullptr;
  ctx->mrel = nullptr;
  ctx->mscan = nullptr;
  SRF_RETURN_DONE(fctx);
}
}

namespace pax {
MicroPartitionMetadata ManifestTupleToValue(
    const std::string &rel_path, ManifestRelation mrel, ManifestTuple tuple);
}
namespace paxc {
bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool * /*all_dead*/) {
  int block_id;
  ManifestRelation mrel;
  ManifestTuple tuple;
  bool exists;

  block_id = pax::GetBlockNumber(*tid);
  mrel = manifest_open(rel);
  tuple = manifest_find(mrel, snapshot, block_id);
  exists = tuple != nullptr;
  if (exists) {
    Datum datum;
    bool isnull;
    datum = get_manifesttuple_value(tuple, mrel, PAX_AUX_PTVISIMAPNAME, &isnull);
    if (!isnull) {
      exists = TestVisimap(rel, NameStr(*DatumGetName(datum)),
                           pax::GetTupleOffset(*tid));
    }
    manifest_free_tuple(tuple);
  }
  manifest_close(mrel);

  return exists;
}

namespace internal {
void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary,
      ::pax::stats::MicroPartitionStatisticsInfo *dummy_stats,
      int block_id) {
  void *stats_output;

  {
    auto stats = summary.mp_stats ? summary.mp_stats : dummy_stats;
    int stats_length = stats->ByteSizeLong();
    uint32 len = VARHDRSZ + stats_length;

    stats_output = palloc(len);
    SET_VARSIZE(stats_output, len);
    auto ok = stats->SerializeToArray(VARDATA(stats_output), stats_length);
    if (!ok)
      elog(ERROR, "corrupted pb stats, serialize failed");
  }

  MetaValue values[] = {
    {
      PAX_AUX_PTBLOCKNAME,
      Int32GetDatum(block_id),
    },
    {
      PAX_AUX_PTTUPCOUNT,
      Int32GetDatum(summary.num_tuples),
    },
    {
      PAX_AUX_PTBLOCKSIZE,
      Int64GetDatum(summary.file_size),
    },
    {
      PAX_AUX_PTSTATISITICS,
      PointerGetDatum(stats_output),
    },
    {
      PAX_AUX_PTEXISTEXTTOAST,
      BoolGetDatum(summary.exist_ext_toast),
    },
    {
      PAX_AUX_PTISCLUSTERED,
      BoolGetDatum(summary.is_clustered),
    },
  };

  auto rel = table_open(summary.rel_oid, AccessShareLock);
  auto mrel = manifest_open(rel);
  manifest_update(mrel, block_id, values, lengthof(values));
  manifest_close(mrel);
  table_close(rel, AccessShareLock);

  pfree(stats_output);
}

void InsertMicroPartitionPlaceHolder(Oid pax_relid, int block_id) {
  MetaValue values[] = {
    {
      PAX_AUX_PTBLOCKNAME,
      Int32GetDatum(block_id),
    },
  };
  Relation pax_rel = table_open(pax_relid, AccessShareLock);
  auto rel = manifest_open(pax_rel);
  manifest_insert(rel, values, lengthof(values));
  manifest_close(rel);
  table_close(pax_rel, AccessShareLock);
}

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id) {
  Relation pax_rel = table_open(pax_relid, AccessShareLock);
  auto aux_rel = manifest_open(pax_rel);
  manifest_delete(aux_rel, block_id);
  manifest_close(aux_rel);
  table_close(pax_rel, AccessShareLock);
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot) {
  auto aux_rel = manifest_open(pax_rel);
  auto tuple = manifest_find(aux_rel, snapshot, block);
  auto visible = tuple != nullptr;
  manifest_close(aux_rel);

  if (tuple) manifest_free_tuple(tuple);
  return visible;
}

void GetMicroPartitionMetadata(Relation rel, Snapshot snapshot, int block_id,
                               pax::MicroPartitionMetadata &info) {
  auto mrel = manifest_open(rel);
  auto tuple = manifest_find(mrel, snapshot, block_id);

  CBDB_TRY();
  {
    auto rel_path = cbdb::BuildPaxDirectoryPath(rel->rd_node,
                                               rel->rd_backend);
    info = pax::ManifestTupleToValue(rel_path, mrel, tuple);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  manifest_free_tuple(tuple);
  manifest_close(mrel);
}

} // namespace internal
} // namespace paxc

namespace cbdb {
void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary) {
  ::pax::stats::MicroPartitionStatisticsInfo dummy_stats;
  int block_id;

  block_id = std::stol(summary.block_id);
  CBDB_WRAP_START;
  {
    paxc::internal::InsertOrUpdateMicroPartitionEntry(summary, &dummy_stats, block_id);
  }
  CBDB_WRAP_END;
}

void InsertMicroPartitionPlaceHolder(Oid pax_relid, int block_id) {
  CBDB_WRAP_START;
  {
    paxc::internal::InsertMicroPartitionPlaceHolder(pax_relid, block_id);
  }
  CBDB_WRAP_END;
}
void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id) {
  CBDB_WRAP_START;
  {
    paxc::internal::DeleteMicroPartitionEntry(pax_relid, snapshot, block_id);
  }
  CBDB_WRAP_END;
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot) {
  CBDB_WRAP_START;
  {
    return paxc::internal::IsMicroPartitionVisible(pax_rel, block, snapshot);
  }
  CBDB_WRAP_END;
}

pax::MicroPartitionMetadata GetMicroPartitionMetadata(Relation rel,
                                                      Snapshot snapshot,
                                                      int block_id) {
  pax::MicroPartitionMetadata info;

  CBDB_WRAP_START;
  {
    paxc::internal::GetMicroPartitionMetadata(rel, snapshot, block_id, info);
  }
  CBDB_WRAP_END;
  return info;
}

} // namespace cbdb

namespace pax {
PaxCatalogUpdater::PaxCatalogUpdater(PaxCatalogUpdater &&other)
  : pax_rel_(other.pax_rel_)
  , mrel_(other.mrel_)
  {}

PaxCatalogUpdater &PaxCatalogUpdater::operator=(PaxCatalogUpdater &&other) {
  if (this != &other) {
    if (this->mrel_) End();

    this->pax_rel_ = other.pax_rel_;
    this->mrel_ = other.mrel_;
    other.mrel_ = nullptr;
  }
  return *this;
}

PaxCatalogUpdater PaxCatalogUpdater::Begin(Relation pax_rel) {
  PaxCatalogUpdater up(pax_rel);
  CBDB_WRAP_START;
  { up.mrel_ = manifest_open(pax_rel); }
  CBDB_WRAP_END;
  return up;
}

void PaxCatalogUpdater::End() {
  Assert(mrel_);
  CBDB_WRAP_START;
  { manifest_close(mrel_); }
  CBDB_WRAP_END;
  mrel_ = nullptr;
}

void PaxCatalogUpdater::UpdateVisimap(int block_id, const char *visimap_filename) {
  Assert(block_id >= 0 && "block is is negative");
  Assert(visimap_filename && "visimap file name is null");

  MetaValue values[] = {
    {
      PAX_AUX_PTVISIMAPNAME,
      CStringGetDatum(visimap_filename),
    },
  };

  CBDB_WRAP_START;
  {
    manifest_update(mrel_, block_id, values, lengthof(values));
  }
  CBDB_WRAP_END;
}

void PaxCatalogUpdater::UpdateStatistics(int block_id,
                        pax::stats::MicroPartitionStatisticsInfo *mp_stats) {
  Assert(block_id >= 0 && "block is is negative");
  Assert(mp_stats);

  uint32 stats_len;
  void *stats_out;

  CBDB_WRAP_START;
  {
    stats_len = VARHDRSZ + mp_stats->ByteSizeLong();
    stats_out = palloc(stats_len);
    SET_VARSIZE(stats_out, stats_len);
    auto ok = mp_stats->SerializeToArray(VARDATA(stats_out),
                                        stats_len - VARHDRSZ);
    if (!ok) elog(ERROR, "failed to serialize stats");


    MetaValue values[] = {
      {
        PAX_AUX_PTSTATISITICS,
        PointerGetDatum(stats_out),
      },
    };
    manifest_update(mrel_, block_id, values, lengthof(values));
    pfree(stats_out);
  }
  CBDB_WRAP_END;
}

} // namespace pax

// CREATE OR REPLACE FUNCTION MicroPartitionStatsCombineResult(relid Oid)
// RETURNS text
//      AS '$libdir/pax', 'MicroPartitionStatsCombineResult' LANGUAGE C
//      IMMUTABLE;
Datum MicroPartitionStatsCombineResult(PG_FUNCTION_ARGS) {
  Oid relid;
  Relation rel;
  ManifestRelation mrel;
  ManifestScan mscan;
  ManifestTuple tuple;
  TupleDesc rel_desc;

  bool pg_attribute_unused() isnull;
  bool pg_attribute_unused() ok;
  bool got_first = false;

  pax::stats::MicroPartitionStatisticsInfo result;
  StringInfoData str;

  relid = PG_GETARG_OID(0);

  // get the tuple desc
  rel = table_open(relid, AccessShareLock);
  if (!RelationIsPAX(rel))
    elog(ERROR, "non-pax table");

  rel_desc = CreateTupleDescCopy(RelationGetDescr(rel));

  mrel = manifest_open(rel);
  mscan = manifest_beginscan(mrel, nullptr);
  while ((tuple = manifest_getnext(mscan, nullptr))) {
    Datum datum;
    datum = get_manifesttuple_value(tuple, mrel, PAX_AUX_PTSTATISITICS, &isnull);
    Assert(!isnull);

    auto flat_stats = reinterpret_cast<struct varlena*>(DatumGetPointer(datum));
    if (got_first) {
      ok = result.ParseFromArray(VARDATA_ANY(flat_stats),
                                 VARSIZE_ANY_EXHDR(flat_stats));
      Assert(ok);
    } else {
      pax::stats::MicroPartitionStatisticsInfo temp;
      ok = temp.ParseFromArray(VARDATA_ANY(flat_stats),
                               VARSIZE_ANY_EXHDR(flat_stats));
      Assert(ok);
      CBDB_TRY();
      {
        ok = pax::MicroPartitionStats::MicroPartitionStatisticsInfoCombine(
            &result, &temp, rel_desc, false);
        Assert(ok);
      }
      CBDB_CATCH_DEFAULT();
      CBDB_END_TRY();
    }
  }
  manifest_endscan(mscan);
  manifest_close(mrel);
  table_close(rel, AccessShareLock);

  if (!got_first) {
    PG_RETURN_TEXT_P(cstring_to_text("EMPTY"));
  }

  paxc::MicroPartitionStatsToString(&result, &str);
  PG_RETURN_TEXT_P(cstring_to_text(str.data));
}
#else
extern "C" {

PG_FUNCTION_INFO_V1(pax_get_catalog_rows);
Datum pax_get_catalog_rows(PG_FUNCTION_ARGS) {
  FuncCallContext *fctx;
  paxc::ScanAuxContext *sctx;
  HeapTuple tuple;
 
  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldctx;
    TupleDesc tupdesc;
    Oid relid;
    Oid aux_relid;
    paxc::ScanAuxContext scan_context;

    relid = PG_GETARG_OID(0);
    fctx = SRF_FIRSTCALL_INIT();
    oldctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

    aux_relid = paxc::GetPaxAuxRelid(relid);
    scan_context.BeginSearchMicroPartition(aux_relid, nullptr, AccessShareLock);

    tupdesc = CreateTemplateTupleDesc(8);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segment_id",
						   INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2,
                      PAX_AUX_PTBLOCKNAME, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3,
                      PAX_AUX_PTTUPCOUNT, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4,
                      PAX_AUX_PTBLOCKSIZE, INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5,
                      PAX_AUX_PTSTATISITICS, PAX_AUX_STATS_TYPE_OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6,
                      PAX_AUX_PTVISIMAPNAME, NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7,
                      PAX_AUX_PTEXISTEXTTOAST, BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)8,
                      PAX_AUX_PTISCLUSTERED, BOOLOID, -1, 0);

    sctx = (paxc::ScanAuxContext *)palloc(sizeof(*sctx));
    *sctx = scan_context;
    fctx->user_fctx = sctx;
    fctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  } else {
    fctx = SRF_PERCALL_SETUP();
    sctx = (paxc::ScanAuxContext *)fctx->user_fctx;
  }

  tuple = sctx->SearchMicroPartitionEntry();
  if (HeapTupleIsValid(tuple)) {
    Datum values[8];
    bool isnull[8];
    Relation rel = sctx->GetRelation();
    TupleDesc desc = RelationGetDescr(rel);

    // segement_id
    values[0] = Int32GetDatum(GpIdentity.segindex);
    isnull[0] = false;

    values[1] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, desc, &isnull[1]);
    values[2] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT, desc, &isnull[2]);
    values[3] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE, desc, &isnull[3]);
    values[4] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS, desc, &isnull[4]);
    values[5] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME, desc, &isnull[5]);
    values[6] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST, desc, &isnull[6]);
    values[7] = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED, desc, &isnull[7]);
    tuple = heap_form_tuple(fctx->tuple_desc, values, isnull);
    SRF_RETURN_NEXT(fctx, HeapTupleGetDatum(tuple));
  }

  sctx->EndSearchMicroPartition(AccessShareLock);
  SRF_RETURN_DONE(fctx);
}
} // extern "C"

namespace paxc {

bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                      bool * /*all_dead*/) {
  paxc::ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;
  bool exists;
  int block_id;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(rel));
  block_id = pax::GetBlockNumber(*tid);
  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot,
                                    AccessShareLock, block_id);
  tuple = context.SearchMicroPartitionEntry();
  exists = HeapTupleIsValid(tuple);
  if (exists) {
    bool isnull;
    auto desc = RelationGetDescr(context.GetRelation());
    auto visimap = heap_getattr(tuple, ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME,
                                desc, &isnull);
    if (!isnull) {
      exists = TestVisimap(rel, NameStr(*DatumGetName(visimap)),
                           pax::GetTupleOffset(*tid));
    }
  }

  context.EndSearchMicroPartition(AccessShareLock);
  return exists;
}
} // namespace paxc

namespace cbdb {
void InsertMicroPartitionPlaceHolder(Oid pax_relid, int block_id) {
  CBDB_WRAP_START;
  {
    Oid aux_relid;

    aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);
    paxc::InsertMicroPartitionPlaceHolder(aux_relid, block_id);
  }
  CBDB_WRAP_END;
}

void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary) {
  CBDB_WRAP_START;
  {
    Oid aux_relid;

    aux_relid = ::paxc::GetPaxAuxRelid(summary.rel_oid);

    paxc::InsertOrUpdateMicroPartitionPlaceHolder(
        aux_relid, std::stol(summary.block_id), summary.num_tuples,
        summary.file_size,
        summary.mp_stats ? *summary.mp_stats
                         : ::pax::stats::MicroPartitionStatisticsInfo(),
        summary.exist_ext_toast, summary.is_clustered);
  }
  CBDB_WRAP_END;
}

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id) {
  CBDB_WRAP_START;
  { paxc::DeleteMicroPartitionEntry(pax_relid, snapshot, block_id); }
  CBDB_WRAP_END;
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot) {
  CBDB_WRAP_START;
  { return paxc::IsMicroPartitionVisible(pax_rel, block, snapshot); }
  CBDB_WRAP_END;
}

pax::MicroPartitionMetadata GetMicroPartitionMetadata(Relation rel,
                                                      Snapshot snapshot,
                                                      int block_id) {
  return cbdb::PaxGetMicroPartitionMetadata(rel, snapshot, block_id);
}

} // namespace cbdb

namespace pax {
PaxCatalogUpdater::PaxCatalogUpdater(PaxCatalogUpdater &&other)
  : pax_rel_(other.pax_rel_)
  , aux_relid_(other.aux_relid_)
  {}

PaxCatalogUpdater &PaxCatalogUpdater::operator=(PaxCatalogUpdater &&other) {
  if (this != &other) {
    this->pax_rel_ = other.pax_rel_;
    this->aux_relid_ = other.aux_relid_;
  }
  return *this;
}

PaxCatalogUpdater PaxCatalogUpdater::Begin(Relation pax_rel) {
  auto aux_oid = cbdb::GetPaxAuxRelid(RelationGetRelid(pax_rel));
  PaxCatalogUpdater up(pax_rel);
  up.aux_relid_ = aux_oid;
  return up;
}

void PaxCatalogUpdater::End() { }

void PaxCatalogUpdater::UpdateVisimap(int block_id, const char *visimap_filename) {
  cbdb::UpdateVisimap(this->aux_relid_, block_id, visimap_filename);
}

void PaxCatalogUpdater::UpdateStatistics(int block_id,
                        pax::stats::MicroPartitionStatisticsInfo *mp_stats) {
  cbdb::UpdateStatistics(this->aux_relid_, block_id, mp_stats);
}

} // namespace pax

// CREATE OR REPLACE FUNCTION MicroPartitionStatsCombineResult(relid Oid)
// RETURNS text
//      AS '$libdir/pax', 'MicroPartitionStatsCombineResult' LANGUAGE C
//      IMMUTABLE;
Datum MicroPartitionStatsCombineResult(PG_FUNCTION_ARGS) {
  Oid relid, auxrelid;
  Relation auxrel, rel;
  TupleDesc rel_desc;

  HeapTuple tup;
  SysScanDesc auxscan;
  bool pg_attribute_unused() isnull;
  bool pg_attribute_unused() ok;
  bool got_first = false;

  pax::stats::MicroPartitionStatisticsInfo result, temp;
  StringInfoData str;

  relid = PG_GETARG_OID(0);

  // get the tuple desc
  rel = table_open(relid, AccessShareLock);
  rel_desc = CreateTupleDescCopy(RelationGetDescr(rel));
  table_close(rel, AccessShareLock);

  auxrelid = ::paxc::GetPaxAuxRelid(relid);
  auxrel = table_open(auxrelid, AccessShareLock);
  auxscan = systable_beginscan(auxrel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid(tup = systable_getnext(auxscan))) {
    Datum tup_datum = heap_getattr(tup, ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                                   RelationGetDescr(auxrel), &isnull);
    Assert(!isnull);
    auto stats_vl =
        pg_detoast_datum_packed((struct varlena *)(DatumGetPointer(tup_datum)));

    if (!got_first) {
      ok = result.ParseFromArray(VARDATA_ANY(stats_vl),
                                 VARSIZE_ANY_EXHDR(stats_vl));
      Assert(ok);
      got_first = true;
    } else {
      ok = temp.ParseFromArray(VARDATA_ANY(stats_vl),
                               VARSIZE_ANY_EXHDR(stats_vl));
      Assert(ok);

      CBDB_TRY();
      {
        ok = pax::MicroPartitionStats::MicroPartitionStatisticsInfoCombine(
            &result, &temp, rel_desc, false);
        Assert(ok);
      }
      CBDB_CATCH_DEFAULT();
      CBDB_END_TRY();
    }
  }

  systable_endscan(auxscan);
  table_close(auxrel, AccessShareLock);

  if (!got_first) {
    PG_RETURN_TEXT_P(cstring_to_text("EMPTY"));
  }

  paxc::MicroPartitionStatsToString(&result, &str);
  PG_RETURN_TEXT_P(cstring_to_text(str.data));
}
#endif

namespace paxc {
#if !defined(USE_MANIFEST_API) || defined(USE_PAX_CATALOG)
void CPaxAuxSwapRelationFiles(Oid relid1, Oid relid2,
                              TransactionId frozen_xid,
                              MultiXactId cutoff_multi) {
  HeapTuple old_tuple1;
  HeapTuple old_tuple2;
  Relation pax_rel;
  TupleDesc desc;
  ScanKeyData key[1];
  SysScanDesc scan;

  Oid aux_relid1;
  Oid aux_relid2;

  pax_rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  desc = RelationGetDescr(pax_rel);

  // save ctid, auxrelid and partition-spec for the first pax relation
  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid1));

  scan = systable_beginscan(pax_rel, PAX_TABLES_RELID_INDEX_ID, true, nullptr,
                            1, key);
  old_tuple1 = systable_getnext(scan);
  if (!HeapTupleIsValid(old_tuple1))
    ereport(ERROR, (errmsg("relid=%u is not a pax relation", relid1)));

  old_tuple1 = heap_copytuple(old_tuple1);
  systable_endscan(scan);

  // save ctid, auxrelid and partition-spec for the second pax relation
  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid2));
  scan = systable_beginscan(pax_rel, PAX_TABLES_RELID_INDEX_ID, true, nullptr,
                            1, key);
  old_tuple2 = systable_getnext(scan);
  if (!HeapTupleIsValid(old_tuple2))
    ereport(ERROR, (errmsg("relid=%u is not a pax relation", relid2)));

  old_tuple2 = heap_copytuple(old_tuple2);
  systable_endscan(scan);

  // swap the entries
  {
    HeapTuple tuple1;
    HeapTuple tuple2;
    Datum values[NATTS_PG_PAX_TABLES];
    bool nulls[NATTS_PG_PAX_TABLES];
    Datum datum;
    bool isnull;

    datum =
        heap_getattr(old_tuple1, ANUM_PG_PAX_TABLES_AUXRELID, desc, &isnull);
    Assert(!isnull);
    aux_relid1 = DatumGetObjectId(datum);

    values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid1);
    values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = datum;
    nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
    nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

    tuple1 = heap_form_tuple(desc, values, nulls);
    tuple1->t_data->t_ctid = old_tuple1->t_data->t_ctid;
    tuple1->t_self = old_tuple1->t_self;
    tuple1->t_tableOid = old_tuple1->t_tableOid;

    datum =
        heap_getattr(old_tuple2, ANUM_PG_PAX_TABLES_AUXRELID, desc, &isnull);
    Assert(!isnull);
    aux_relid2 = DatumGetObjectId(datum);

    values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid2);
    values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = datum;
    nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
    nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

    tuple2 = heap_form_tuple(desc, values, nulls);
    tuple2->t_data->t_ctid = old_tuple2->t_data->t_ctid;
    tuple2->t_self = old_tuple2->t_self;
    tuple2->t_tableOid = old_tuple2->t_tableOid;

    CatalogIndexState indstate;

    indstate = CatalogOpenIndexes(pax_rel);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple1->t_self, tuple1, indstate);
    CatalogTupleUpdateWithInfo(pax_rel, &tuple2->t_self, tuple2, indstate);
    CatalogCloseIndexes(indstate);
  }

  table_close(pax_rel, NoLock);

  /* swap fast seq */
  {
    int32 seqno1, seqno2;

    seqno1 = CPaxGetFastSequences(relid1, false);
    seqno2 = CPaxGetFastSequences(relid2, false);

    CPaxInitializeFastSequenceEntry(relid1, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                    seqno2);
    CPaxInitializeFastSequenceEntry(relid2, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                    seqno1);
  }
  SIMPLE_FAULT_INJECTOR("pax_finish_swap_fast_fastsequence");

  /* swap relation files for aux table */
  {
    Relation aux_rel1;
    Relation aux_rel2;
    ReindexParams reindex_params = {0};
    Relation toast_rel1 = nullptr;
    Relation toast_rel2 = nullptr;

    aux_rel1 = relation_open(aux_relid1, AccessExclusiveLock);
    aux_rel2 = relation_open(aux_relid2, AccessExclusiveLock);

    if (OidIsValid(aux_rel1->rd_rel->reltoastrelid))
      toast_rel1 =
          relation_open(aux_rel1->rd_rel->reltoastrelid, AccessExclusiveLock);
    if (OidIsValid(aux_rel2->rd_rel->reltoastrelid))
      toast_rel2 =
          relation_open(aux_rel2->rd_rel->reltoastrelid, AccessExclusiveLock);

    swap_relation_files(aux_relid1, aux_relid2, false, /* target_is_pg_class */
                        true, /* swap_toast_by_content */
                        true, /*swap_stats */
                        true, /* is_internal */
                        frozen_xid, cutoff_multi, NULL);

    if (toast_rel1) relation_close(toast_rel1, NoLock);
    if (toast_rel2) relation_close(toast_rel2, NoLock);
    relation_close(aux_rel1, NoLock);
    relation_close(aux_rel2, NoLock);

    reindex_relation(aux_relid1, 0, &reindex_params);
    reindex_relation(aux_relid2, 0, &reindex_params);
  }
}

#endif

} // namespace paxc
