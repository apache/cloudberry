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
 * pax_catalog.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_catalog.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include "catalog/pax_catalog_columns.h"
#include "catalog/pax_fastsequence.h"
#include "storage/micro_partition_metadata.h"

#ifdef USE_MANIFEST_API
extern "C" {
#ifdef USE_PAX_CATALOG
#define USE_OWN_MANIFEST_TUPLE
typedef struct HeapTupleData *ManifestTuple;

#endif // end USE_PAX_CATALOG

#include "catalog/manifest_api.h"
}
#endif

#if !defined(USE_MANIFEST_API) || defined(USE_PAX_CATALOG)
// not USE_MANIFEST_API or defined USE_PAX_CATALOG
#include "catalog/pax_aux_table.h"
#include "catalog/pg_pax_tables.h"
#endif

namespace pax {
class PaxCatalogUpdater {
 public:
  static PaxCatalogUpdater Begin(Relation pax_rel);
  void End();
  // set new visimap name for block
  void UpdateVisimap(int block_id, const char *visimap_filename);
  // update stats
  void UpdateStatistics(int block_id,
                        pax::stats::MicroPartitionStatisticsInfo *mp_stats);

 private:
  PaxCatalogUpdater(Relation pax_rel) : pax_rel_(pax_rel) {}
  // disallow to allocate from heap
  void* operator new(size_t) = delete;
  void operator delete(void*) = delete;
  PaxCatalogUpdater(const PaxCatalogUpdater &) = delete;
  PaxCatalogUpdater(PaxCatalogUpdater &&);
  PaxCatalogUpdater &operator=(const PaxCatalogUpdater &) = delete;
  PaxCatalogUpdater &operator=(PaxCatalogUpdater &&other);


 private:
  Relation pax_rel_;
#ifdef USE_MANIFEST_API
  ManifestRelation mrel_ = nullptr;
#else
  Oid aux_relid_ = InvalidOid;
#endif
};

void PaxCopyAllDataFiles(Relation rel, const RelFileNode *newrnode,
                         bool createnewpath);
} // namespace pax

namespace cbdb {
void InsertMicroPartitionPlaceHolder(Oid pax_relid, int block_id);
void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary);
void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot, int block_id);
bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot);
pax::MicroPartitionMetadata GetMicroPartitionMetadata(Relation rel,
                                                      Snapshot snapshot,
                                                      int block_id);
}

namespace paxc {
#if !defined(USE_MANIFEST_API) || defined(USE_PAX_CATALOG)
void CPaxAuxSwapRelationFiles(Oid relid1, Oid relid2,
                              TransactionId frozen_xid,
                              MultiXactId cutoff_multi);
#endif

void CPaxCopyAllTuples(Relation old_rel, Relation new_rel, Snapshot snapshot);

} // namespace paxc