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
 * pax_aux_table.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_aux_table.cc
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/pax_aux_table.h"

#include "comm/cbdb_api.h"

#include <uuid/uuid.h>

#include <utility>

#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "comm/fmt.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_metadata.h"
#include "storage/wal/pax_wal.h"
namespace paxc {

static inline void InsertTuple(Relation rel, Datum *values, bool *nulls) {
  HeapTuple tuple;
  tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
  CatalogTupleInsert(rel, tuple);
}

static inline void InsertTuple(Oid relid, Datum *values, bool *nulls) {
  Relation rel;

  rel = table_open(relid, RowExclusiveLock);
  InsertTuple(rel, values, nulls);
  table_close(rel, NoLock);
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
static void CPaxNontransactionalTruncateTable(Relation rel) {
  Relation aux_rel;
  Oid aux_relid;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(rel));
  Assert(OidIsValid(aux_relid));

  aux_rel = relation_open(aux_relid, AccessExclusiveLock);
  heap_truncate_one_rel(aux_rel);
  relation_close(aux_rel, NoLock);

  CPaxInitializeFastSequenceEntry(RelationGetRelid(rel),
                                  FASTSEQUENCE_INIT_TYPE_INPLACE, 0);
}

void CPaxCreateMicroPartitionTable(Relation rel) {
  Relation pg_class_desc;
  char aux_relname[32];
  Oid relid;
  Oid aux_relid;
  Oid aux_namespace_id;
  Oid pax_relid;
  TupleDesc tupdesc;

  pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);
  pax_relid = RelationGetRelid(rel);

  // 1. create blocks table.
  snprintf(aux_relname, sizeof(aux_relname), "pg_pax_blocks_%u", pax_relid);
  aux_namespace_id = PG_EXTAUX_NAMESPACE;
  aux_relid = GetNewOidForRelation(pg_class_desc, ClassOidIndexId,
                                   Anum_pg_class_oid,  // new line
                                   aux_relname, aux_namespace_id);
  tupdesc = CreateTemplateTupleDesc(NATTS_PG_PAX_BLOCK_TABLES);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                     PAX_AUX_PTBLOCKNAME, INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                     PAX_AUX_PTTUPCOUNT, INT4OID, -1, 0);
  // TODO(chenhongjie): uncompressed and compressed ptblocksize are needed.
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE,
                     PAX_AUX_PTBLOCKSIZE, INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                     PAX_AUX_PTSTATISITICS, PAX_AUX_STATS_TYPE_OID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME,
                     PAX_AUX_PTVISIMAPNAME, NAMEOID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST,
                     PAX_AUX_PTEXISTEXTTOAST, BOOLOID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED,
                     PAX_AUX_PTISCLUSTERED, BOOLOID, -1, 0);
  {
    // Add constraints for the aux table
    auto attr =
        TupleDescAttr(tupdesc, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1);
    attr->attnotnull = true;
  }

  // FIXME: temporary table in aux namespace  is not supported yet.
  relid = heap_create_with_catalog(
      aux_relname, aux_namespace_id, InvalidOid, aux_relid, InvalidOid,
      InvalidOid, rel->rd_rel->relowner, HEAP_TABLE_AM_OID, tupdesc, NIL,
      RELKIND_RELATION,
      rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED
          ? RELPERSISTENCE_UNLOGGED
          : RELPERSISTENCE_PERMANENT,
      rel->rd_rel->relisshared, RelationIsMapped(rel), ONCOMMIT_NOOP,
      NULL,                         /* GP Policy */
      (Datum)0, false,              /* use _user_acl */
      true, true, InvalidOid, NULL, /* typeaddress */
      false /* valid_opts */);
  Assert(relid == aux_relid);
  table_close(pg_class_desc, NoLock);

  NewRelationCreateToastTable(relid, (Datum)0);

  // 2. insert entry into pg_pax_tables.
  ::paxc::InsertPaxTablesEntry(pax_relid, aux_relid);

  // 3. record pg_depend, pg_pax_blocks_<xxx> depends relation.
  {
    ObjectAddress base;
    ObjectAddress aux;
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = RelationRelationId;
    aux.objectId = aux_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);

    // pg_pax_tables single row depend
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = PAX_TABLES_RELATION_ID;
    aux.objectId = pax_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
  }
  CommandCounterIncrement();

  // 4. create index on ptblockname dynamically, the index name should be
  // pg_paxaux.pg_pax_blocks_index_xxx.
  {
    char aux_index_name[NAMEDATALEN];
    IndexInfo *indexInfo;
    List *indexColNames;
    Relation aux_rel;
    int16 coloptions[1];
    Oid classObjectId[1];
    Oid collationObjectId[1];

    snprintf(aux_index_name, sizeof(aux_index_name), "%s_idx", aux_relname);

    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 1;
    indexInfo->ii_NumIndexKeyAttrs = 1;
    indexInfo->ii_IndexAttrNumbers[0] = ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_Am = BTREE_AM_OID;
    indexInfo->ii_Context = CurrentMemoryContext;

    collationObjectId[0] = 0;
    classObjectId[0] = GetDefaultOpClass(INT4OID, BTREE_AM_OID);
    coloptions[0] = 0;

    auto attr =
        TupleDescAttr(tupdesc, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1);
    indexColNames = list_make1(NameStr(attr->attname));

    // ShareLock is not really needed here, but take it anyway.
    aux_rel = table_open(aux_relid, ShareLock);

    index_create(aux_rel, aux_index_name, InvalidOid, InvalidOid, InvalidOid,
                 InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                 // The tablespace in aux index should follow aux table
                 aux_rel->rd_rel->reltablespace, collationObjectId,
                 classObjectId, coloptions, (Datum)0, INDEX_CREATE_IS_PRIMARY,
                 0, true, true, NULL);

    // Unlock target table -- no one can see it
    table_close(aux_rel, ShareLock);

    // Unlock the index -- no one can see it anyway
    // UnlockRelationOid(paxauxiliary_idxid, AccessExclusiveLock);

    CommandCounterIncrement();
  }
}

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id) {
  ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;

  aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot,
                                    RowExclusiveLock, block_id);
  tuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "delete micro partition \"%d\" failed for relation(%u)",
         block_id, pax_relid);

  Assert(context.GetRelation());
  CatalogTupleDelete(context.GetRelation(), &tuple->t_self);

  context.EndSearchMicroPartition(NoLock);
}

void InsertMicroPartitionPlaceHolder(Oid aux_relid, int block_id) {
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];

  memset(nulls, true, sizeof(nulls));

  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = Int32GetDatum(block_id);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;

  InsertTuple(aux_relid, values, nulls);
  CommandCounterIncrement();
}

void UpdateVisimap(Oid aux_relid, int block_id, const char *visimap_filename) {
  NameData pt_visimap_name;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];
  bool repls[NATTS_PG_PAX_BLOCK_TABLES];
  ScanAuxContext context;
  HeapTuple newtuple;

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, NULL,
                                    RowExclusiveLock, block_id);
  auto aux_rel = context.GetRelation();
  auto oldtuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(oldtuple))
    elog(ERROR,
         "The micro partition does not exist when we updating visible map.");

  memset(repls, false, sizeof(repls));
  repls[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = true;
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = !visimap_filename;
  if (visimap_filename) {
    namestrcpy(&pt_visimap_name, visimap_filename);
    values[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] =
        NameGetDatum(&pt_visimap_name);
  }

  newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(aux_rel), values,
                               nulls, repls);

  CatalogTupleUpdate(aux_rel, &oldtuple->t_self, newtuple);
  heap_freetuple(newtuple);
  CommandCounterIncrement();

  context.EndSearchMicroPartition(NoLock);
}

void UpdateStatistics(Oid aux_relid, int block_id,
                      pax::stats::MicroPartitionStatisticsInfo *mp_stats) {
  // NameData pt_visimap_name;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];
  bool repls[NATTS_PG_PAX_BLOCK_TABLES];
  ScanAuxContext context;
  HeapTuple newtuple;
  uint32 stats_len;
  void *stats_out;

  Assert(mp_stats);
  stats_len = VARHDRSZ + mp_stats->ByteSizeLong();
  stats_out = palloc(stats_len);
  SET_VARSIZE(stats_out, stats_len);
  mp_stats->SerializeToArray(VARDATA(stats_out), stats_len - VARHDRSZ);

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, NULL,
                                    RowExclusiveLock, block_id);
  auto aux_rel = context.GetRelation();
  auto oldtuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(oldtuple))
    elog(ERROR,
         "The micro partition does not exist when we updating statistics.");

  memset(repls, false, sizeof(repls));
  repls[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = true;
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = false;
  values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] =
      PointerGetDatum(stats_out);
  newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(aux_rel), values,
                               nulls, repls);

  CatalogTupleUpdate(aux_rel, &oldtuple->t_self, newtuple);
  heap_freetuple(newtuple);
  context.EndSearchMicroPartition(NoLock);

  pfree(stats_out);

  CommandCounterIncrement();
}

// We won't update the visimap in here
// but will update visimap info in the `UpdateVisimap`
// so we can no pass the `visimap_filename`
void InsertOrUpdateMicroPartitionPlaceHolder(
    Oid aux_relid, int block_id, int num_tuples, int file_size,
    const ::pax::stats::MicroPartitionStatisticsInfo &mp_stats,
    /* const char *visimap_filename, */
    bool exist_ext_toast, bool is_clustered) {
  int stats_length = mp_stats.ByteSizeLong();
  uint32 len = VARHDRSZ + stats_length;
  void *output;

  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];

  output = palloc(len);
  SET_VARSIZE(output, len);
  mp_stats.SerializeToArray(VARDATA(output), stats_length);

  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = Int32GetDatum(block_id);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;

  values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = Int32GetDatum(num_tuples);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = false;
  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = Int32GetDatum(file_size);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = false;

  // Serialize catalog statitics information into PG bytea format and saved in
  // aux table ptstatitics column.
  values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = PointerGetDatum(output);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = false;

  // The visimap will be updated in the `UpdateVisimap`
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1] = true;

  values[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] =
      BoolGetDatum(exist_ext_toast);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1] = false;

  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] = false;
  values[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1] =
      BoolGetDatum(is_clustered);

  ScanAuxContext context;
  context.BeginSearchMicroPartition(aux_relid, InvalidOid, NULL,
                                    RowExclusiveLock, block_id);
  auto aux_rel = context.GetRelation();
  auto oldtuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(oldtuple))
    elog(ERROR,
         "micro partition doesn't exist before inserting tuples, relid:%d, "
         "block_id:%d",
         aux_relid, block_id);

  if (num_tuples > 0) {
    auto newtuple = heap_form_tuple(RelationGetDescr(aux_rel), values, nulls);

    newtuple->t_data->t_ctid = oldtuple->t_data->t_ctid;
    newtuple->t_self = oldtuple->t_self;
    newtuple->t_tableOid = oldtuple->t_tableOid;
    CatalogTupleUpdate(aux_rel, &newtuple->t_self, newtuple);
    heap_freetuple(newtuple);
  } else {
    CatalogTupleDelete(aux_rel, &oldtuple->t_self);
  }
  context.EndSearchMicroPartition(NoLock);

  pfree(output);

  CommandCounterIncrement();
}

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot) {
  ScanKeyData scankey[1];
  Relation indrel;
  SysScanDesc scan;
  HeapTuple tuple;
  Oid index_oid;
  int index_count = 0;

  ScanKeyInit(&scankey[0], Anum_pg_index_indrelid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(aux_relid));
  indrel = table_open(IndexRelationId, AccessShareLock);
  scan = systable_beginscan(indrel, IndexIndrelidIndexId, true, snapshot, 1,
                            scankey);

  index_oid = InvalidOid;
  while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
    auto index = (Form_pg_index)GETSTRUCT(tuple);
    index_count++;
    if (!index->indislive || !index->indisvalid) continue;
    index_oid = index->indexrelid;
  }
  systable_endscan(scan);
  table_close(indrel, NoLock);

  if (index_count != 1 || !OidIsValid(index_oid))
    elog(ERROR, "unexpected number of index of aux table: %d", index_count);

  return index_oid;
}

static inline Oid GetAuxIndexOid(Oid aux_relid, Oid *aux_index_relid,
                                 Snapshot snapshot) {
  if (aux_index_relid) {
    if (OidIsValid(*aux_index_relid))
      return *aux_index_relid;
    else
      return *aux_index_relid = FindAuxIndexOid(aux_relid, snapshot);
  } else {
    return FindAuxIndexOid(aux_relid, snapshot);
  }
}

void ScanAuxContext::BeginSearchMicroPartition(Oid aux_relid,
                                               Oid aux_index_relid,
                                               Snapshot snapshot,
                                               LOCKMODE lockmode,
                                               int block_id) {
  Assert(aux_relid);
  Assert(block_id >= 0 || block_id == PAX_SCAN_ALL_BLOCKS);

  if (!OidIsValid(aux_index_relid))
    aux_index_relid = FindAuxIndexOid(aux_relid, snapshot);

  aux_rel_ = table_open(aux_relid, lockmode);
  if (block_id >= 0) {
    ScanKeyData scankey[1];

    ScanKeyInit(&scankey[0], ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                BTEqualStrategyNumber, F_INT4EQ, block_id);
    scan_ = systable_beginscan(aux_rel_, aux_index_relid, true, snapshot, 1,
                               scankey);
  } else if (block_id == PAX_SCAN_ALL_BLOCKS) {
    scan_ = systable_beginscan(aux_rel_, aux_index_relid, false, snapshot, 0,
                               nullptr);
  } else {
    CBDB_RAISE(cbdb::CException::kExTypeInvalid,
               "Invalid block id for micro partition scan");
  }
}

HeapTuple ScanAuxContext::SearchMicroPartitionEntry() {
  Assert(aux_rel_ && scan_);
  return systable_getnext(scan_);
}

void ScanAuxContext::EndSearchMicroPartition(LOCKMODE lockmode) {
  Assert(aux_rel_ && scan_);

  systable_endscan(scan_);
  table_close(aux_rel_, lockmode);
  scan_ = nullptr;
  aux_rel_ = nullptr;
}

void PaxAuxRelationSetNewFilenode(Oid aux_relid) {
  Relation aux_rel;
  Oid toastrelid;
  ReindexParams reindex_params = {0};

  aux_rel = relation_open(aux_relid, AccessExclusiveLock);
  RelationSetNewRelfilenode(aux_rel, aux_rel->rd_rel->relpersistence);
  toastrelid = aux_rel->rd_rel->reltoastrelid;

  if (OidIsValid(toastrelid)) {
    Relation toast_rel;
    toast_rel = relation_open(toastrelid, AccessExclusiveLock);
    RelationSetNewRelfilenode(toast_rel, toast_rel->rd_rel->relpersistence);
    relation_close(toast_rel, NoLock);
  }

  if (aux_rel->rd_rel->relhasindex)
    reindex_relation(aux_relid, REINDEX_REL_PROCESS_TOAST, &reindex_params);

  pgstat_count_truncate(aux_rel);
  relation_close(aux_rel, NoLock);
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot) {
  struct ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;
  bool ok;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(pax_rel));

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot,
                                    AccessShareLock, DatumGetInt32(block));
  tuple = context.SearchMicroPartitionEntry();
  ok = HeapTupleIsValid(tuple);
  context.EndSearchMicroPartition(NoLock);

  return ok;
}

static void CPaxCopyPaxBlockEntry(Relation old_relation,
                                  Relation new_relation) {
  HeapTuple tuple;
  SysScanDesc pax_scan;
  Relation old_aux_rel, new_aux_rel;
  Oid old_aux_relid = 0, new_aux_relid = 0;

  old_aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(old_relation));
  new_aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(new_relation));
  old_aux_rel = table_open(old_aux_relid, RowExclusiveLock);
  new_aux_rel = table_open(new_aux_relid, RowExclusiveLock);

  pax_scan = systable_beginscan(old_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while ((tuple = systable_getnext(pax_scan)) != NULL) {
    // Do not direct insert tuple into new_aux_rel,
    // ex. `CatalogTupleInsert(new_aux_rel, tuple);`
    // Because it will change the old tuple xmin/xmax/tableoid
    HeapTuple copy_one = heap_copytuple(tuple);
    CatalogTupleInsert(new_aux_rel, copy_one);
    heap_freetuple(copy_one);
  }
  systable_endscan(pax_scan);
  table_close(old_aux_rel, RowExclusiveLock);
  table_close(new_aux_rel, RowExclusiveLock);

  // also need update the fast seq
  {
    int32 seqno1;

    seqno1 = CPaxGetFastSequences(old_relation->rd_id, false);
    CPaxInitializeFastSequenceEntry(new_relation->rd_id,
                                    FASTSEQUENCE_INIT_TYPE_UPDATE, seqno1);
  }
}

void FetchMicroPartitionAuxRow(Relation rel, Snapshot snapshot, int block_id,
                               void (*callback)(Datum *values, bool *isnull,
                                                void *arg),
                               void *arg) {
  ::paxc::ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool isnull[NATTS_PG_PAX_BLOCK_TABLES];

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(rel));

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot,
                                    AccessShareLock, block_id);
  tuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "get micro partition \"%d\" failed for relation(%u)", block_id,
         RelationGetRelid(rel));

  bool should_free_stats = false;
  struct varlena *flat_stats = nullptr;
  auto tup_desc = RelationGetDescr(context.GetRelation());

  for (int i = 0; i < NATTS_PG_PAX_BLOCK_TABLES; i++)
    values[i] = heap_getattr(tuple, i + 1, tup_desc, &isnull[i]);

  if (!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1]) {
    auto pstat =
        DatumGetPointer(values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1]);
    auto stats = reinterpret_cast<struct varlena *>(pstat);
    flat_stats = pg_detoast_datum_packed(stats);
    should_free_stats = flat_stats != stats;
    values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] =
        PointerGetDatum(flat_stats);
  }

  if (callback) callback(values, isnull, arg);

  AssertImply(should_free_stats, flat_stats);
  if (should_free_stats) pfree(flat_stats);

  context.EndSearchMicroPartition(NoLock);
}
}  // namespace paxc

namespace cbdb {
Oid GetPaxAuxRelid(Oid relid) {
  CBDB_WRAP_START;
  { return ::paxc::GetPaxAuxRelid(relid); }
  CBDB_WRAP_END;
}

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot) {
  CBDB_WRAP_START;
  { return ::paxc::FindAuxIndexOid(aux_relid, snapshot); }
  CBDB_WRAP_END;
}

struct FetchMicroPartitionAuxRowContext {
  pax::MicroPartitionMetadata info;
  Relation rel;
};

static void FetchMicroPartitionAuxRowCallback(Datum *values, bool *isnull,
                                              void *arg) {
  auto ctx = reinterpret_cast<struct FetchMicroPartitionAuxRowContext *>(arg);
  auto rel = ctx->rel;
  auto rel_path = cbdb::BuildPaxDirectoryPath(
      rel->rd_node, rel->rd_backend);

  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME]);
  {
    auto datum = values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1];
    std::string block_name = std::to_string(DatumGetInt32(datum));
    auto file_name = cbdb::BuildPaxFilePath(rel_path, block_name);
    ctx->info.SetMicroPartitionId(DatumGetInt32(datum));
    ctx->info.SetFileName(std::move(file_name));
  }

  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1]);
  ctx->info.SetTupleCount(
      cbdb::DatumToInt32(values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1]));

  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1]);
  ctx->info.SetTupleCount(
      cbdb::DatumToInt32(values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1]));

  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1]);
  {
    ::pax::stats::MicroPartitionStatisticsInfo stats_info;
    auto pstats = cbdb::DatumToPointer(
        values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1]);
    auto flat_stats = reinterpret_cast<struct varlena *>(pstats);
    auto ok = stats_info.ParseFromArray(VARDATA_ANY(flat_stats),
                                        VARSIZE_ANY_EXHDR(flat_stats));

    CBDB_CHECK(ok, cbdb::CException::kExTypeLogicError,
               ::pax::fmt("Invalid pb structure in the aux table [rd_id=%d]",
                          rel->rd_id));
    ctx->info.SetStats(std::move(stats_info));
  }

  if (!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1]) {
    auto datum = values[ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME - 1];
    auto name = NameStr(*DatumGetName(datum));
    auto vname = cbdb::BuildPaxFilePath(rel_path, name);
    ctx->info.SetVisibilityBitmapFile(std::move(vname));
  }

  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1]);
  {
    auto datum = values[ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST - 1];
    ctx->info.SetExistToast(DatumGetBool(datum));
  }
  Assert(!isnull[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1]);
  ctx->info.SetClustered(
      cbdb::DatumToBool(values[ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED - 1]));
}

static void FetchMicroPartitionAuxRowCallbackWrapper(Datum *values,
                                                     bool *isnull, void *arg) {
  CBDB_TRY();
  { FetchMicroPartitionAuxRowCallback(values, isnull, arg); }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

pax::MicroPartitionMetadata PaxGetMicroPartitionMetadata(Relation rel,
                                                         Snapshot snapshot,
                                                         int block_id) {
  CBDB_WRAP_START;
  {
    FetchMicroPartitionAuxRowContext ctx;
    ctx.rel = rel;
    paxc::FetchMicroPartitionAuxRow(rel, snapshot, block_id,
                                    FetchMicroPartitionAuxRowCallbackWrapper,
                                    &ctx);
    return std::move(ctx.info);
  }
  CBDB_WRAP_END;
}

void UpdateVisimap(Oid aux_relid, int block_id, const char *visimap_filename) {
  CBDB_WRAP_START;
  { paxc::UpdateVisimap(aux_relid, block_id, visimap_filename); }
  CBDB_WRAP_END;
}

void UpdateStatistics(Oid aux_relid, int block_id,
                      pax::stats::MicroPartitionStatisticsInfo *mp_stats) {
  CBDB_WRAP_START;
  { paxc::UpdateStatistics(aux_relid, block_id, mp_stats); }
  CBDB_WRAP_END;
}

static void PaxNontransactionalTruncateTable(Relation rel) {
  CBDB_WRAP_START;
  { paxc::CPaxNontransactionalTruncateTable(rel); }
  CBDB_WRAP_END;
}

static void PaxCopyPaxBlockEntry(Relation old_relation, Relation new_relation) {
  CBDB_WRAP_START;
  { paxc::CPaxCopyPaxBlockEntry(old_relation, new_relation); }
  CBDB_WRAP_END;
}
}  // namespace cbdb

namespace pax {
void CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(Relation rel) {
  cbdb::PaxNontransactionalTruncateTable(rel);

  // Delete all micro partition file on non-transactional truncate  but reserve
  // top level PAX file directory.
  PaxAuxRelationFileUnlink(
      rel->rd_node, rel->rd_backend, false,
      rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT);
}

void CCPaxAuxTable::PaxAuxRelationCopyData(Relation rel,
                                           const RelFileNode *newrnode,
                                           bool createnewpath) {
  PaxCopyAllDataFiles(rel, newrnode, createnewpath);
}

void CCPaxAuxTable::PaxAuxRelationCopyDataForCluster(Relation old_rel,
                                                     Relation new_rel) {
  PaxAuxRelationCopyData(old_rel, &new_rel->rd_node, false);
  cbdb::PaxCopyPaxBlockEntry(old_rel, new_rel);
  // TODO(Tony) : here need to implement PAX re-organize semantics logic.
}

void CCPaxAuxTable::PaxAuxRelationFileUnlink(RelFileNode node,
                                             BackendId backend,
                                             bool delete_topleveldir,
                                             bool need_wal) {
  std::string relpath;
  FileSystem *fs;

  relpath = cbdb::BuildPaxDirectoryPath(node, backend);

  fs = pax::Singleton<LocalFileSystem>::GetInstance();
  fs->DeleteDirectory(relpath, delete_topleveldir);
  // delete directory wal log
  if (need_wal) {
    cbdb::XLogPaxTruncate(node);
  }
}

}  // namespace pax
