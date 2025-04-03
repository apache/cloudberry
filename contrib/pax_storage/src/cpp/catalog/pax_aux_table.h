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
 * pax_aux_table.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_aux_table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <string>

#include "storage/micro_partition_metadata.h"

#define PAX_SCAN_ALL_BLOCKS -1

#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME 1
#define ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT 2
#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE 3
#define ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS 4
#define ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME 5
#define ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST 6
#define ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED 7
#define NATTS_PG_PAX_BLOCK_TABLES 7

namespace paxc {
void CPaxCreateMicroPartitionTable(Relation rel);

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot);

void InsertMicroPartitionPlaceHolder(Oid aux_relid, int block_id);
void InsertOrUpdateMicroPartitionPlaceHolder(
    Oid aux_relid, int block_id, int num_tuples, int file_size,
    const ::pax::stats::MicroPartitionStatisticsInfo &mp_stats,
    bool exist_ext_toast, bool is_clustered);
void UpdateVisimap(Oid aux_relid, int block_id, const char *visimap_filename);
void UpdateStatistics(Oid aux_relid, int block_id,
                      pax::stats::MicroPartitionStatisticsInfo *mp_stats);
void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id);
// Scan aux table
// seqscan: MicroPartitionInfoIterator
// index scan
struct ScanAuxContext {
 public:
  void BeginSearchMicroPartition(Oid aux_relid, Oid aux_index_relid,
                                 Snapshot snapshot, LOCKMODE lockmode,
                                 int block_id);
  void BeginSearchMicroPartition(Oid aux_relid, Snapshot snapshot,
                                 LOCKMODE lockmode) {
    BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot, lockmode,
                              PAX_SCAN_ALL_BLOCKS);
  }
  HeapTuple SearchMicroPartitionEntry();
  void EndSearchMicroPartition(LOCKMODE lockmode);

  Relation GetRelation() { return aux_rel_; }

 private:
  Relation aux_rel_ = nullptr;
  SysScanDesc scan_ = nullptr;
};

void PaxAuxRelationSetNewFilenode(Oid aux_relid);
bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot);
void FetchMicroPartitionAuxRow(Relation rel, Snapshot snapshot, int block_id,
                               void (*callback)(Datum *values, bool *isnull,
                                                void *arg),
                               void *arg);
}  // namespace paxc

namespace pax {
class CCPaxAuxTable final {
 public:
  CCPaxAuxTable() = delete;
  ~CCPaxAuxTable() = delete;

  static void PaxAuxRelationSetNewFilenode(Relation rel,
                                           const RelFileNode *newrnode,
                                           char persistence);

  static void PaxAuxRelationNontransactionalTruncate(Relation rel);

  static void PaxAuxRelationCopyData(Relation rel, const RelFileNode *newrnode,
                                     bool createnewpath = true);

  static void PaxAuxRelationCopyDataForCluster(Relation old_rel,
                                               Relation new_rel);

  static void PaxAuxRelationFileUnlink(RelFileNode node, BackendId backend,
                                       bool delete_topleveldir, bool need_wal);
};

}  // namespace pax

namespace cbdb {

Oid GetPaxAuxRelid(Oid relid);

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot);

void UpdateVisimap(Oid aux_relid, int block_id, const char *visimap_filename);

void UpdateStatistics(Oid aux_relid, int block_id,
                      pax::stats::MicroPartitionStatisticsInfo *mp_stats);

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot);

pax::MicroPartitionMetadata PaxGetMicroPartitionMetadata(Relation rel,
                                                         Snapshot snapshot,
                                                         int block_id);
}  // namespace cbdb
