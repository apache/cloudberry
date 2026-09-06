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
 * pg_iceberg_am_handler.c
 *	  Table access method callbacks for Iceberg tables.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_am_handler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/tableam.h"
#include "am_iceberg/pg_iceberg_options.h"
#include "am_iceberg/pg_iceberg_reject.h"
#include "fmgr.h"

PG_FUNCTION_INFO_V1(iceberg_am_handler);

static const TupleTableSlotOps *
pg_iceberg_slot_callbacks(Relation rel pg_attribute_unused())
{
	return &TTSOpsVirtual;
}

static TableScanDesc
pg_iceberg_scan_begin(Relation rel pg_attribute_unused(),
					  Snapshot snapshot pg_attribute_unused(),
					  int nkeys pg_attribute_unused(),
					  struct ScanKeyData *key pg_attribute_unused(),
					  ParallelTableScanDesc pscan pg_attribute_unused(),
					  uint32 flags pg_attribute_unused())
{
	pg_iceberg_not_supported("SELECT");
}

static void
pg_iceberg_scan_end(TableScanDesc scan pg_attribute_unused())
{
	pg_iceberg_not_supported("SELECT");
}

static void
pg_iceberg_scan_rescan(TableScanDesc scan pg_attribute_unused(),
					   struct ScanKeyData *key pg_attribute_unused(),
					   bool set_params pg_attribute_unused(),
					   bool allow_strat pg_attribute_unused(),
					   bool allow_sync pg_attribute_unused(),
					   bool allow_pagemode pg_attribute_unused())
{
	pg_iceberg_not_supported("SELECT");
}

static bool
pg_iceberg_scan_getnextslot(TableScanDesc scan pg_attribute_unused(),
							ScanDirection direction pg_attribute_unused(),
							TupleTableSlot *slot pg_attribute_unused())
{
	pg_iceberg_not_supported("SELECT");
}

static Size
pg_iceberg_parallelscan_estimate(Relation rel pg_attribute_unused())
{
	pg_iceberg_not_supported("parallel scan");
}

static Size
pg_iceberg_parallelscan_initialize(Relation rel pg_attribute_unused(),
								   ParallelTableScanDesc pscan pg_attribute_unused())
{
	pg_iceberg_not_supported("parallel scan");
}

static void
pg_iceberg_parallelscan_reinitialize(Relation rel pg_attribute_unused(),
									 ParallelTableScanDesc pscan pg_attribute_unused())
{
	pg_iceberg_not_supported("parallel scan");
}

static struct IndexFetchTableData *
pg_iceberg_index_fetch_begin(Relation rel pg_attribute_unused())
{
	pg_iceberg_not_supported("index access");
}

static void
pg_iceberg_index_fetch_reset(struct IndexFetchTableData *data pg_attribute_unused())
{
	pg_iceberg_not_supported("index access");
}

static void
pg_iceberg_index_fetch_end(struct IndexFetchTableData *data pg_attribute_unused())
{
	pg_iceberg_not_supported("index access");
}

static bool
pg_iceberg_index_fetch_tuple(struct IndexFetchTableData *scan pg_attribute_unused(),
							 ItemPointer tid pg_attribute_unused(),
							 Snapshot snapshot pg_attribute_unused(),
							 TupleTableSlot *slot pg_attribute_unused(),
							 bool *call_again pg_attribute_unused(),
							 bool *all_dead pg_attribute_unused())
{
	pg_iceberg_not_supported("index access");
}

static bool
pg_iceberg_tuple_fetch_row_version(Relation rel pg_attribute_unused(),
								   ItemPointer tid pg_attribute_unused(),
								   Snapshot snapshot pg_attribute_unused(),
								   TupleTableSlot *slot pg_attribute_unused())
{
	pg_iceberg_not_supported("tuple fetch by TID");
}

static bool
pg_iceberg_tuple_tid_valid(TableScanDesc scan pg_attribute_unused(),
						   ItemPointer tid pg_attribute_unused())
{
	pg_iceberg_not_supported("tuple fetch by TID");
}

static void
pg_iceberg_tuple_get_latest_tid(TableScanDesc scan pg_attribute_unused(),
								ItemPointer tid pg_attribute_unused())
{
	pg_iceberg_not_supported("tuple fetch by TID");
}

static bool
pg_iceberg_tuple_satisfies_snapshot(Relation rel pg_attribute_unused(),
									TupleTableSlot *slot pg_attribute_unused(),
									Snapshot snapshot pg_attribute_unused())
{
	pg_iceberg_not_supported("tuple fetch by TID");
}

static TransactionId
pg_iceberg_index_delete_tuples(Relation rel pg_attribute_unused(),
							   TM_IndexDeleteOp *delstate pg_attribute_unused())
{
	pg_iceberg_not_supported("index maintenance");
}

static void
pg_iceberg_tuple_insert(Relation rel pg_attribute_unused(),
						TupleTableSlot *slot pg_attribute_unused(),
						CommandId cid pg_attribute_unused(),
						int options pg_attribute_unused(),
						struct BulkInsertStateData *bistate pg_attribute_unused())
{
	pg_iceberg_not_supported("INSERT");
}

static void
pg_iceberg_tuple_insert_speculative(Relation rel pg_attribute_unused(),
									TupleTableSlot *slot pg_attribute_unused(),
									CommandId cid pg_attribute_unused(),
									int options pg_attribute_unused(),
									struct BulkInsertStateData *bistate pg_attribute_unused(),
									uint32 specToken pg_attribute_unused())
{
	pg_iceberg_not_supported("INSERT ... ON CONFLICT");
}

static void
pg_iceberg_tuple_complete_speculative(Relation rel pg_attribute_unused(),
									  TupleTableSlot *slot pg_attribute_unused(),
									  uint32 specToken pg_attribute_unused(),
									  bool succeeded pg_attribute_unused())
{
	pg_iceberg_not_supported("INSERT ... ON CONFLICT");
}

static void
pg_iceberg_multi_insert(Relation rel pg_attribute_unused(),
						TupleTableSlot **slots pg_attribute_unused(),
						int nslots pg_attribute_unused(),
						CommandId cid pg_attribute_unused(),
						int options pg_attribute_unused(),
						struct BulkInsertStateData *bistate pg_attribute_unused())
{
	pg_iceberg_not_supported("INSERT");
}

static TM_Result
pg_iceberg_tuple_delete(Relation rel pg_attribute_unused(),
						ItemPointer tid pg_attribute_unused(),
						CommandId cid pg_attribute_unused(),
						Snapshot snapshot pg_attribute_unused(),
						Snapshot crosscheck pg_attribute_unused(),
						bool wait pg_attribute_unused(),
						TM_FailureData *tmfd pg_attribute_unused(),
						bool changingPart pg_attribute_unused())
{
	pg_iceberg_not_supported("DELETE");
}

static TM_Result
pg_iceberg_tuple_update(Relation rel pg_attribute_unused(),
						ItemPointer otid pg_attribute_unused(),
						TupleTableSlot *slot pg_attribute_unused(),
						CommandId cid pg_attribute_unused(),
						Snapshot snapshot pg_attribute_unused(),
						Snapshot crosscheck pg_attribute_unused(),
						bool wait pg_attribute_unused(),
						TM_FailureData *tmfd pg_attribute_unused(),
						LockTupleMode *lockmode pg_attribute_unused(),
						TU_UpdateIndexes *update_indexes pg_attribute_unused())
{
	pg_iceberg_not_supported("UPDATE");
}

static TM_Result
pg_iceberg_tuple_lock(Relation rel pg_attribute_unused(),
					  ItemPointer tid pg_attribute_unused(),
					  Snapshot snapshot pg_attribute_unused(),
					  TupleTableSlot *slot pg_attribute_unused(),
					  CommandId cid pg_attribute_unused(),
					  LockTupleMode mode pg_attribute_unused(),
					  LockWaitPolicy wait_policy pg_attribute_unused(),
					  uint8 flags pg_attribute_unused(),
					  TM_FailureData *tmfd pg_attribute_unused())
{
	pg_iceberg_not_supported("row locking (SELECT ... FOR UPDATE)");
}

static void
pg_iceberg_relation_set_new_filelocator(Relation rel pg_attribute_unused(),
										const RelFileLocator *newrlocator pg_attribute_unused(),
										char persistence pg_attribute_unused(),
										TransactionId *freezeXid,
										MultiXactId *minmulti)
{
	/*
	 * Iceberg data lives in external object storage, so creating local smgr
	 * storage here would be both unnecessary and misleading.
	 */
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;
	return;
}

static void
pg_iceberg_relation_nontransactional_truncate(Relation rel pg_attribute_unused())
{
	pg_iceberg_not_supported("TRUNCATE");
}

static void
pg_iceberg_relation_copy_data(Relation rel pg_attribute_unused(),
							  const RelFileLocator *newrlocator pg_attribute_unused())
{
	pg_iceberg_not_supported("ALTER TABLE ... SET TABLESPACE");
}

static void
pg_iceberg_relation_copy_for_cluster(Relation OldTable pg_attribute_unused(),
									 Relation NewTable pg_attribute_unused(),
									 Relation OldIndex pg_attribute_unused(),
									 bool use_sort pg_attribute_unused(),
									 TransactionId OldestXmin pg_attribute_unused(),
									 TransactionId *xid_cutoff pg_attribute_unused(),
									 MultiXactId *multi_cutoff pg_attribute_unused(),
									 double *num_tuples pg_attribute_unused(),
									 double *tups_vacuumed pg_attribute_unused(),
									 double *tups_recently_dead pg_attribute_unused())
{
	pg_iceberg_not_supported("CLUSTER / VACUUM FULL");
}

static void
pg_iceberg_relation_vacuum(Relation rel pg_attribute_unused(),
						   struct VacuumParams *params pg_attribute_unused(),
						   BufferAccessStrategy bstrategy pg_attribute_unused())
{
	/*
	 * A database-wide VACUUM or autovacuum must not fail merely because it
	 * encounters an Iceberg table. There is no local storage to vacuum.
	 */
	return;
}

static bool
pg_iceberg_scan_analyze_next_block(TableScanDesc scan pg_attribute_unused(),
								   BlockNumber blockno pg_attribute_unused(),
								   BufferAccessStrategy bstrategy pg_attribute_unused())
{
	/* Defensive fallback; relation_acquire_sample_rows bypasses this path. */
	return false;
}

static bool
pg_iceberg_scan_analyze_next_tuple(TableScanDesc scan pg_attribute_unused(),
								   TransactionId OldestXmin pg_attribute_unused(),
								   double *liverows pg_attribute_unused(),
								   double *deadrows pg_attribute_unused(),
								   TupleTableSlot *slot pg_attribute_unused())
{
	/* Defensive fallback; relation_acquire_sample_rows bypasses this path. */
	return false;
}

static int
pg_iceberg_relation_acquire_sample_rows(Relation onerel pg_attribute_unused(),
										int elevel pg_attribute_unused(),
										HeapTuple *rows pg_attribute_unused(),
										int targrows pg_attribute_unused(),
										double *totalrows,
										double *totaldeadrows)
{
	/*
	 * analyze.c uses this callback directly when present and therefore never
	 * starts a table_beginscan_analyze() scan. This lets ANALYZE succeed as a
	 * zero-sample no-op while ordinary scans remain unsupported.
	 */
	*totalrows = 0;
	*totaldeadrows = 0;
	return 0;
}

static double
pg_iceberg_index_build_range_scan(Relation table_rel pg_attribute_unused(),
								  Relation index_rel pg_attribute_unused(),
								  struct IndexInfo *index_info pg_attribute_unused(),
								  bool allow_sync pg_attribute_unused(),
								  bool anyvisible pg_attribute_unused(),
								  bool progress pg_attribute_unused(),
								  BlockNumber start_blockno pg_attribute_unused(),
								  BlockNumber numblocks pg_attribute_unused(),
								  IndexBuildCallback callback pg_attribute_unused(),
								  void *callback_state pg_attribute_unused(),
								  TableScanDesc scan pg_attribute_unused())
{
	pg_iceberg_not_supported("CREATE INDEX");
}

static void
pg_iceberg_index_validate_scan(Relation table_rel pg_attribute_unused(),
							   Relation index_rel pg_attribute_unused(),
							   struct IndexInfo *index_info pg_attribute_unused(),
							   Snapshot snapshot pg_attribute_unused(),
							   struct ValidateIndexState *state pg_attribute_unused())
{
	pg_iceberg_not_supported("CREATE INDEX");
}

static uint64
pg_iceberg_relation_size(Relation rel pg_attribute_unused(),
						 ForkNumber forkNumber pg_attribute_unused())
{
	return 0;
}

static BlockSequence *
pg_iceberg_relation_get_block_sequences(Relation rel pg_attribute_unused(),
										int *numSequences)
{
	*numSequences = 0;
	return palloc0(sizeof(BlockSequence));
}

static void
pg_iceberg_relation_get_block_sequence(Relation rel pg_attribute_unused(),
									   BlockNumber blkNum pg_attribute_unused(),
									   BlockSequence *sequence pg_attribute_unused())
{
	pg_iceberg_not_supported("block sequence access");
}

static bool
pg_iceberg_relation_needs_toast_table(Relation rel pg_attribute_unused())
{
	return false;
}

static void
pg_iceberg_relation_estimate_size(Relation rel pg_attribute_unused(),
								  int32 *attr_widths pg_attribute_unused(),
								  BlockNumber *pages,
								  double *tuples,
								  double *allvisfrac)
{
	*pages = 0;
	*tuples = 0;
	*allvisfrac = 0;
}

static bool
pg_iceberg_scan_sample_next_block(TableScanDesc scan pg_attribute_unused(),
								  struct SampleScanState *scanstate pg_attribute_unused())
{
	pg_iceberg_not_supported("TABLESAMPLE");
}

static bool
pg_iceberg_scan_sample_next_tuple(TableScanDesc scan pg_attribute_unused(),
								  struct SampleScanState *scanstate pg_attribute_unused(),
								  TupleTableSlot *slot pg_attribute_unused())
{
	pg_iceberg_not_supported("TABLESAMPLE");
}

/*
 * Optional callbacks below remain NULL. Iceberg currently has no local tuple
 * scanning, index, bulk-insert, TOAST, bitmap-scan, DML-state, file-swap, or
 * column-encoding implementation.
 */
static const TableAmRoutine pg_iceberg_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = pg_iceberg_slot_callbacks,

	.scan_begin = pg_iceberg_scan_begin,
	.scan_begin_extractcolumns = NULL,
	.scan_begin_extractcolumns_bm = NULL,
	.scan_end = pg_iceberg_scan_end,
	.scan_rescan = pg_iceberg_scan_rescan,
	.scan_getnextslot = pg_iceberg_scan_getnextslot,
	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,
	.scan_flags = NULL,

	.parallelscan_estimate = pg_iceberg_parallelscan_estimate,
	.parallelscan_initialize = pg_iceberg_parallelscan_initialize,
	.parallelscan_reinitialize = pg_iceberg_parallelscan_reinitialize,

	.index_fetch_begin = pg_iceberg_index_fetch_begin,
	.index_fetch_reset = pg_iceberg_index_fetch_reset,
	.index_fetch_end = pg_iceberg_index_fetch_end,
	.index_fetch_tuple = pg_iceberg_index_fetch_tuple,
	.index_unique_check = NULL,

	.tuple_fetch_row_version = pg_iceberg_tuple_fetch_row_version,
	.tuple_tid_valid = pg_iceberg_tuple_tid_valid,
	.tuple_get_latest_tid = pg_iceberg_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = pg_iceberg_tuple_satisfies_snapshot,
	.index_delete_tuples = pg_iceberg_index_delete_tuples,

	.tuple_insert = pg_iceberg_tuple_insert,
	.tuple_insert_speculative = pg_iceberg_tuple_insert_speculative,
	.tuple_complete_speculative = pg_iceberg_tuple_complete_speculative,
	.multi_insert = pg_iceberg_multi_insert,
	.tuple_delete = pg_iceberg_tuple_delete,
	.tuple_update = pg_iceberg_tuple_update,
	.tuple_lock = pg_iceberg_tuple_lock,
	.finish_bulk_insert = NULL,

	.relation_set_new_filelocator = pg_iceberg_relation_set_new_filelocator,
	.relation_nontransactional_truncate = pg_iceberg_relation_nontransactional_truncate,
	.relation_copy_data = pg_iceberg_relation_copy_data,
	.relation_copy_for_cluster = pg_iceberg_relation_copy_for_cluster,
	.relation_vacuum = pg_iceberg_relation_vacuum,
	.scan_analyze_next_block = pg_iceberg_scan_analyze_next_block,
	.scan_analyze_next_tuple = pg_iceberg_scan_analyze_next_tuple,
	.relation_acquire_sample_rows = pg_iceberg_relation_acquire_sample_rows,
	.index_build_range_scan = pg_iceberg_index_build_range_scan,
	.index_validate_scan = pg_iceberg_index_validate_scan,

	.relation_size = pg_iceberg_relation_size,
	.relation_get_block_sequences = pg_iceberg_relation_get_block_sequences,
	.relation_get_block_sequence = pg_iceberg_relation_get_block_sequence,
	.relation_needs_toast_table = pg_iceberg_relation_needs_toast_table,
	.relation_toast_am = NULL,
	.relation_fetch_toast_slice = NULL,

	.relation_estimate_size = pg_iceberg_relation_estimate_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = pg_iceberg_scan_sample_next_block,
	.scan_sample_next_tuple = pg_iceberg_scan_sample_next_tuple,

	.dml_init = NULL,
	.dml_fini = NULL,
	.amoptions = pg_iceberg_amoptions,
	.swap_relation_files = NULL,
	.validate_column_encoding_clauses = NULL,
	.transform_column_encoding_clauses = NULL,
};

Datum
iceberg_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&pg_iceberg_methods);
}
