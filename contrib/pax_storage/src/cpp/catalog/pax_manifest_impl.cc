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
 * pax_manifest_impl.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_manifest_impl.cc
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/pax_catalog.h"
#include "catalog/pax_aux_table.h"
#include "catalog/pax_fastsequence.h"
#include "catalog/pg_pax_tables.h"

#include "comm/cbdb_api.h"

#include "comm/paxc_wrappers.h"
#include "comm/singleton.h"
#include "storage/local_file_system.h"
#include "storage/file_system.h"
#include "storage/wal/paxc_wal.h"

static inline void InsertTuple(Relation rel, Datum *values, bool *nulls) {
  HeapTuple tuple;
  tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
  CatalogTupleInsert(rel, tuple);
}

extern "C" {
struct ManifestRelationData {
  Relation aux_rel;
  MemoryContext parent_ctx;
  MemoryContext mctx; // memory context for per tuple allocation.
};

struct ManifestScanData {
  ManifestRelation mrel;
  SysScanDesc desc;
};
}

static void manifest_update_internal(ManifestRelation mrel,
                                     ManifestTuple oldtuple,
                                     const MetaValue data[],
                                     int count);

static inline AttrNumber get_aux_name_attrno(const char *colname) {
  if (strcmp(colname, PAX_AUX_PTBLOCKNAME) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME;
  if (strcmp(colname, PAX_AUX_PTTUPCOUNT) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT;
  if (strcmp(colname, PAX_AUX_PTBLOCKSIZE) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE;
  if (strcmp(colname, PAX_AUX_PTSTATISITICS) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS;
  if (strcmp(colname, PAX_AUX_PTVISIMAPNAME) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME;
  if (strcmp(colname, PAX_AUX_PTEXISTEXTTOAST) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST;
  if (strcmp(colname, PAX_AUX_PTISCLUSTERED) == 0)
    return ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED;

  elog(ERROR, "unknown column name '%s'", colname);
  return 0;
}

static void manifest_init_attribute(ManifestDesc desc, int index, const char *name, MetaFieldType ftype, Datum default_value) {
  auto attr = &desc->attrs[index];
  attr->field_name = name;
  attr->field_type = ftype;
  attr->deflt = default_value;
}

ManifestDesc manifest_init() {
  auto nattrs = NATTS_PG_PAX_BLOCK_TABLES;
  size_t size = offsetof(ManifestDescData, attrs) + 
                  sizeof(MetaAttribute) * nattrs;
  ManifestDesc desc = (ManifestDesc)malloc(size);
  if (!desc) return nullptr;

  desc->numattrs = nattrs;
  manifest_init_attribute(desc,
                          ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1,
                          PAX_AUX_PTBLOCKNAME,
                          Meta_Field_Type_Int,
                          0);
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1,
                          PAX_AUX_PTTUPCOUNT,
                          Meta_Field_Type_Int,
                          0);
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1,
                          PAX_AUX_PTBLOCKSIZE,
                          Meta_Field_Type_Int,
                          0);                            
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1,
                          PAX_AUX_PTSTATISITICS,
                          Meta_Field_Type_Bytes,
                          0);                            
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1,
                          PAX_AUX_PTVISIMAPNAME,
                          Meta_Field_Type_String,
                          0);                            
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1,
                          PAX_AUX_PTEXISTEXTTOAST,
                          Meta_Field_Type_Bool,
                          0);                            
  manifest_init_attribute(desc, 
                          ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1,
                          PAX_AUX_PTISCLUSTERED,
                          Meta_Field_Type_Bool,
                          0);                            

  return desc;
}

static bool manifest_new_filenode(Relation rel,
                                               RelFileNode newrnode,
                                               char persistence) {
  Relation pax_tables_rel;
  ScanKeyData scan_key[1];
  SysScanDesc scan;
  HeapTuple tuple;
  Oid pax_relid;
  bool exists;

  pax_tables_rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  pax_relid = RelationGetRelid(rel);

  ScanKeyInit(&scan_key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(pax_relid));
  scan = systable_beginscan(pax_tables_rel, PAX_TABLES_RELID_INDEX_ID, true,
                            NULL, 1, scan_key);
  tuple = systable_getnext(scan);
  exists = HeapTupleIsValid(tuple);
  if (exists) {
    Oid aux_relid;

    // set new filenode, not create new table
    //
    // 1. truncate aux table by new relfilenode
    aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);
    Assert(OidIsValid(aux_relid));
    paxc::PaxAuxRelationSetNewFilenode(aux_relid);
  } else {
    // create new table
    //
    // 1. create aux table
    // 2. initialize fast sequence in pg_pax_fastsequence
    // 3. setup dependency
    paxc::CPaxCreateMicroPartitionTable(rel);
  }

  // initialize or reset the fast sequence number
  CPaxInitializeFastSequenceEntry(
      pax_relid,
      exists ? FASTSEQUENCE_INIT_TYPE_UPDATE : FASTSEQUENCE_INIT_TYPE_CREATE,
      0);

  systable_endscan(scan);
  table_close(pax_tables_rel, NoLock);

  return true;
}

static void manifest_create_data_dir(Relation rel, const RelFileNode &newrnode) {
  // create relfilenode file for pax table
  auto srel = paxc::PaxRelationCreateStorage(newrnode, rel);
  smgrclose(srel);

  char *path = paxc::BuildPaxDirectoryPath(newrnode, rel->rd_backend);
  Assert(path);

  int rc;
  CBDB_TRY();
  {
    pax::FileSystem *fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();
    rc = fs->CreateDirectory(path);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();

  if (rc != 0)
    elog(ERROR, "create data dir failed for %u/%u/%lu",
                newrnode.dbNode, newrnode.spcNode, newrnode.relNode);
}

void manifest_create(Relation rel, RelFileNode relnode) {
  auto should_create_dir = manifest_new_filenode(rel, relnode, rel->rd_rel->relpersistence);
  if (should_create_dir) {
    manifest_create_data_dir(rel, relnode);
  }
}

void manifest_truncate(Relation rel) {
  CBDB_TRY();
  {
    pax::CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(rel);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_END_TRY();
}

ManifestRelation manifest_open(Relation rel) {
  Oid aux_relid;
  ManifestRelation mrel;

  mrel = (ManifestRelation) palloc(sizeof(*mrel));

  aux_relid = paxc::GetPaxAuxRelid(RelationGetRelid(rel));
  mrel->aux_rel = table_open(aux_relid, AccessShareLock);
  mrel->parent_ctx = CurrentMemoryContext;
  mrel->mctx = nullptr;

  return mrel;
}

static inline MemoryContext manifest_memory_context(ManifestRelation mrel) {
  if (mrel->mctx) {
    MemoryContextReset(mrel->mctx);
  } else {
    mrel->mctx = AllocSetContextCreate(mrel->parent_ctx, "manifest-relation",
                                       ALLOCSET_DEFAULT_SIZES);
  }
  return mrel->mctx;
}

void manifest_close(ManifestRelation mrel) {
  table_close(mrel->aux_rel, NoLock);
  if (mrel->mctx)
    MemoryContextDelete(mrel->mctx);
  pfree(mrel);
}

ManifestTuple manifest_find(ManifestRelation mrel, Snapshot snapshot, int block) {
  paxc::ScanAuxContext context;
  Relation aux_rel;
  HeapTuple tuple;

  aux_rel = mrel->aux_rel;
  context.BeginSearchMicroPartition(RelationGetRelid(aux_rel), InvalidOid, snapshot,
                                    AccessShareLock, DatumGetInt32(block));
  tuple = context.SearchMicroPartitionEntry();
  if (HeapTupleIsValid(tuple))
    tuple = heap_copytuple(tuple);
  context.EndSearchMicroPartition(NoLock);

  return tuple;
}

void manifest_free_tuple(ManifestTuple tuple) {
  heap_freetuple(tuple);
}

#define AUX_CMP_NAME(col, name) strcmp((col).field_name, name) == 0

void manifest_insert(ManifestRelation mrel, const MetaValue data[], int count) {
  Relation aux_rel;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool isnull[NATTS_PG_PAX_BLOCK_TABLES];
  NameData visimap;

  aux_rel = mrel->aux_rel;
  memset(isnull, true, sizeof(isnull));
  for (int i = 0; i < count; i++) {
    const MetaValue &col = data[i];
    Assert(col.field_name);

    if (AUX_CMP_NAME(col, PAX_AUX_PTBLOCKNAME)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTTUPCOUNT)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTBLOCKSIZE)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTSTATISITICS)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTVISIMAPNAME)) {
      namestrcpy(&visimap, DatumGetCString(col.value));
      values[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = NameGetDatum(&visimap);
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTEXISTEXTTOAST)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = false;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTISCLUSTERED)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = false;
    } else {
      elog(ERROR, "unknown column name '%s'", col.field_name);
    }
  }

  InsertTuple(aux_rel, values, isnull);
  CommandCounterIncrement();
}

void manifest_update(ManifestRelation mrel, int block, const MetaValue data[],
                     int count) {
  paxc::ScanAuxContext context;
  Relation aux_rel;
  HeapTuple tuple;
  MemoryContext oldctx;

  aux_rel = mrel->aux_rel;
  oldctx = MemoryContextSwitchTo(manifest_memory_context(mrel));
  context.BeginSearchMicroPartition(RelationGetRelid(aux_rel), InvalidOid, NULL,
                                    RowExclusiveLock, block);
  tuple = context.SearchMicroPartitionEntry();
  if (HeapTupleIsValid(tuple)) {
    manifest_update_internal(mrel, tuple, data, count);
  }
  context.EndSearchMicroPartition(NoLock);
  MemoryContextSwitchTo(oldctx);
}

static void manifest_update_internal(ManifestRelation mrel,
                                     ManifestTuple oldtuple,
                                     const MetaValue data[],
                                     int count) {
  Relation aux_rel;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool isnull[NATTS_PG_PAX_BLOCK_TABLES];
  bool repl[NATTS_PG_PAX_BLOCK_TABLES];
  NameData visimap;

  aux_rel = mrel->aux_rel;
  memset(repl, false, sizeof(repl));
  for (int i = 0; i < count; i++) {
    const auto &col = data[i];
    Assert(col.field_name);

    if (AUX_CMP_NAME(col, PAX_AUX_PTBLOCKNAME)) {
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTTUPCOUNT)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = false;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = true;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTBLOCKSIZE)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = false;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = true;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTSTATISITICS)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = col.value == 0;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = true;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTVISIMAPNAME)) {
      namestrcpy(&visimap, DatumGetCString(col.value));
      values[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = NameGetDatum(&visimap);
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = false;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = true;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTEXISTEXTTOAST)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = false;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = true;
    } else if (AUX_CMP_NAME(col, PAX_AUX_PTISCLUSTERED)) {
      values[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = col.value;
      isnull[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = false;
      repl[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = true;
    } else {
      elog(ERROR, "unknown column name '%s'", col.field_name);
    }
  }

  auto newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(aux_rel),
                                    values, isnull, repl);

  CatalogTupleUpdate(aux_rel, &newtuple->t_self, newtuple);
  heap_freetuple(newtuple);

  CommandCounterIncrement();
}

void manifest_delete(ManifestRelation mrel, int block) {
  paxc::ScanAuxContext context;
  Relation aux_rel;
  HeapTuple tuple;
  MemoryContext oldctx;

  aux_rel = mrel->aux_rel;
  oldctx = MemoryContextSwitchTo(manifest_memory_context(mrel));
  context.BeginSearchMicroPartition(RelationGetRelid(aux_rel), InvalidOid, NULL,
                                    RowExclusiveLock, block);
  tuple = context.SearchMicroPartitionEntry();
  if (HeapTupleIsValid(tuple)) {
    CatalogTupleDelete(aux_rel, &tuple->t_self);
  }
  context.EndSearchMicroPartition(NoLock);
  MemoryContextSwitchTo(oldctx);
}

Datum get_manifesttuple_value(ManifestTuple tuple, ManifestRelation mrel,
                              const char *field_name, bool *isnull) {
  Relation aux_rel;
  HeapTuple htuple;
  AttrNumber attno = -1;
  Datum datum;

  aux_rel = mrel->aux_rel;
  htuple = tuple;
  auto desc = RelationGetDescr(aux_rel);

  attno = get_aux_name_attrno(field_name);
  Assert(attno > 0);
  datum = heap_getattr(htuple, attno, desc, isnull);
  switch (attno) {
    case ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS: {
      MemoryContext old = MemoryContextSwitchTo(manifest_memory_context(mrel));
      auto stats = reinterpret_cast<struct varlena*>(DatumGetPointer(datum));
      auto flat_stats = pg_detoast_datum_packed(stats);
      MemoryContextSwitchTo(old);
      return PointerGetDatum(flat_stats);
    }
    case ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME:
      return CStringGetDatum(NameStr(*DatumGetName(datum)));
    default:
      return datum;
  }
}

ManifestScan manifest_beginscan(ManifestRelation mrel, Snapshot snapshot) {
  ManifestScan scan;
  scan = (ManifestScan) palloc(sizeof(*scan));
  scan->mrel = mrel;
  scan->desc = systable_beginscan(mrel->aux_rel, InvalidOid, false, snapshot, 0, nullptr);
  return scan;
}

void manifest_endscan(ManifestScan scan) {
  SysScanDesc desc = scan->desc;
  systable_endscan(desc);
  pfree(scan);
}

ManifestTuple manifest_getnext(ManifestScan scan, void *context) {
  if (scan->mrel->mctx)
    MemoryContextReset(scan->mrel->mctx);
  return systable_getnext(scan->desc);
}

void manifest_swap_table(Oid relid1, Oid relid2,
                         TransactionId frozen_xid,
                         MultiXactId cutoff_multi) {
  paxc::CPaxAuxSwapRelationFiles(relid1, relid2, frozen_xid, cutoff_multi);
}
