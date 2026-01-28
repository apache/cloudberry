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
 * pax_access_handle.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/pax_access_handle.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

namespace paxc {

class PaxAccessMethod final {
 private:
  PaxAccessMethod() = default;

 public:
  static const TupleTableSlotOps *SlotCallbacks(Relation rel) noexcept;
  static uint32 ScanFlags(Relation relation);

  static void ScanSetTidrange(TableScanDesc scan, ItemPointer mintid,
                              ItemPointer maxtid);
  static void ScanGetnextslotTidrange(TableScanDesc scan,
                                      ScanDirection direction,
                                      TupleTableSlot *slot);

  /* Parallel table scan related functions. */
  static Size ParallelscanEstimate(Relation rel);
  static Size ParallelscanInitialize(Relation rel, ParallelTableScanDesc pscan);
  static void ParallelscanReinitialize(Relation rel,
                                       ParallelTableScanDesc pscan);

  /* Callbacks for non-modifying operations on individual tuples */
  static bool TupleFetchRowVersion(Relation relation, ItemPointer tid,
                                   Snapshot snapshot, TupleTableSlot *slot);
  static bool TupleTidValid(TableScanDesc scan, ItemPointer tid);
  static void TupleGetLatestTid(TableScanDesc sscan, ItemPointer tid);
  static bool TupleSatisfiesSnapshot(Relation rel, TupleTableSlot *slot,
                                     Snapshot snapshot);
  static TransactionId IndexDeleteTuples(Relation rel,
                                         TM_IndexDeleteOp *delstate);

  static BlockSequence *RelationGetBlockSequences(Relation rel,
                                                  int *numSequences);

  static void RelationGetBlockSequence(Relation rel, BlockNumber blkNum,
                                       BlockSequence *sequence);
  static bool RelationNeedsToastTable(Relation rel);
  static uint64 RelationSize(Relation rel, ForkNumber fork_number);
  static void EstimateRelSize(Relation rel, int32 *attr_widths,
                              BlockNumber *pages, double *tuples,
                              double *allvisfrac);

  /* unsupported DML now, may move to CCPaxAccessMethod */
  static void TupleInsertSpeculative(Relation relation, TupleTableSlot *slot,
                                     CommandId cid, int options,
                                     BulkInsertState bistate,
                                     uint32 spec_token);
  static void TupleCompleteSpeculative(Relation relation, TupleTableSlot *slot,
                                       uint32 spec_token, bool succeeded);
  static TM_Result TupleLock(Relation relation, ItemPointer tid,
                             Snapshot snapshot, TupleTableSlot *slot,
                             CommandId cid, LockTupleMode mode,
                             LockWaitPolicy wait_policy, uint8 flags,
                             TM_FailureData *tmfd);

  static void RelationVacuum(Relation onerel, VacuumParams *params,
                             BufferAccessStrategy bstrategy);
  static double IndexBuildRangeScan(
      Relation heap_relation, Relation index_relation, IndexInfo *index_info,
      bool allow_sync, bool anyvisible, bool progress,
      BlockNumber start_blockno, BlockNumber numblocks,
      IndexBuildCallback callback, void *callback_state, TableScanDesc scan);
  static void IndexValidateScan(Relation heap_relation, Relation index_relation,
                                IndexInfo *index_info, Snapshot snapshot,
                                ValidateIndexState *state);
  static void SwapRelationFiles(Oid relid1, Oid relid2,
                                TransactionId frozen_xid,
                                MultiXactId cutoff_multi);

  static bytea *AmOptions(Datum reloptions, char relkind, bool validate);
  static void ValidateColumnEncodingClauses(List *encoding_opts);
  static List *TransformColumnEncodingClauses(Relation rel, List *encoding_opts,
                                              bool validate, bool from_type);
};

void register_custom_object_classes();

}  // namespace paxc

namespace pax {
class CCPaxAccessMethod final {
 private:
  CCPaxAccessMethod() = default;

 public:
  static TableScanDesc ScanBegin(Relation rel, Snapshot snapshot, int nkeys,
                                 struct ScanKeyData *key,
                                 ParallelTableScanDesc pscan, uint32 flags);
  static void ScanEnd(TableScanDesc scan);
  static void ScanRescan(TableScanDesc scan, struct ScanKeyData *key,
                         bool set_params, bool allow_strat, bool allow_sync,
                         bool allow_pagemode);
  static bool ScanGetNextSlot(TableScanDesc scan, ScanDirection direction,
                              TupleTableSlot *slot);

  static TableScanDesc ScanExtractColumns(Relation rel, Snapshot snapshot,
                                          int nkeys, struct ScanKeyData *key,
                                          ParallelTableScanDesc parallel_scan,
                                          struct PlanState *ps, uint32 flags);

  static bool IndexUniqueCheck(Relation rel, ItemPointer tid, Snapshot snapshot,
                               bool *all_dead);

  /* Index Scan Callbacks */
  static struct IndexFetchTableData *IndexFetchBegin(Relation rel);
  static void IndexFetchEnd(struct IndexFetchTableData *scan);
  static void IndexFetchReset(struct IndexFetchTableData *scan);
  static bool IndexFetchTuple(struct IndexFetchTableData *scan, ItemPointer tid,
                              Snapshot snapshot, TupleTableSlot *slot,
                              bool *call_again, bool *all_dead);

  /* Manipulations of physical tuples. */
  static void TupleInsert(Relation relation, TupleTableSlot *slot,
                          CommandId cid, int options, BulkInsertState bistate);
  static TM_Result TupleDelete(Relation relation, ItemPointer tid,
                               CommandId cid, Snapshot snapshot,
                               Snapshot crosscheck, bool wait,
                               TM_FailureData *tmfd, bool changing_part);
  static TM_Result TupleUpdate(Relation relation, ItemPointer otid,
                               TupleTableSlot *slot, CommandId cid,
                               Snapshot snapshot, Snapshot crosscheck,
                               bool wait, TM_FailureData *tmfd,
                               LockTupleMode *lockmode, bool *update_indexes);

  static void RelationCopyData(Relation rel, const RelFileNode *newrnode);

  static void RelationCopyForCluster(Relation old_heap, Relation new_heap,
                                     Relation old_index, bool use_sort,
                                     TransactionId oldest_xmin,
                                     TransactionId *xid_cutoff,
                                     MultiXactId *multi_cutoff,
                                     double *num_tuples, double *tups_vacuumed,
                                     double *tups_recently_dead);

  static void RelationSetNewFilenode(Relation rel, const RelFileNode *newrnode,
                                     char persistence,
                                     TransactionId *freeze_xid,
                                     MultiXactId *minmulti);

  static void RelationNontransactionalTruncate(Relation rel);

  static bool ScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno,
                                   BufferAccessStrategy bstrategy);
  static bool ScanAnalyzeNextTuple(TableScanDesc scan,
                                   TransactionId oldest_xmin, double *liverows,
                                   double *deadrows, TupleTableSlot *slot);
  static bool ScanBitmapNextBlock(TableScanDesc scan, TBMIterateResult *tbmres);
  static bool ScanBitmapNextTuple(TableScanDesc scan, TBMIterateResult *tbmres,
                                  TupleTableSlot *slot);
  static bool ScanSampleNextBlock(TableScanDesc scan,
                                  SampleScanState *scanstate);
  static bool ScanSampleNextTuple(TableScanDesc scan,
                                  SampleScanState *scanstate,
                                  TupleTableSlot *slot);

  static void MultiInsert(Relation relation, TupleTableSlot **slots,
                          int ntuples, CommandId cid, int options,
                          BulkInsertState bistate);

  static void FinishBulkInsert(Relation relation, int options);

  // DML init/fini hooks
  static void ExtDmlInit(Relation rel, CmdType operation);
  static void ExtDmlFini(Relation rel, CmdType operation);
};

}  // namespace pax
