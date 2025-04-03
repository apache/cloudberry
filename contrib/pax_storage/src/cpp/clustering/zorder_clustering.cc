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
 * zorder_clustering.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/zorder_clustering.cc
 *
 *-------------------------------------------------------------------------
 */

#include "clustering/zorder_clustering.h"

#include "comm/cbdb_api.h"

#include "clustering/sorter_tuple.h"
#include "clustering/zorder_utils.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"

#define MAX_ZORDER_NKEYS 32

namespace pax {
namespace clustering {

ZOrderClustering::ZOrderClustering() {}

ZOrderClustering::~ZOrderClustering() {}

void ZOrderClustering::Clustering(ClusteringDataReader *reader,
                                  ClusteringDataWriter *writer,
                                  const DataClusteringOptions *options) {
  const ZOrderClusteringOptions *zorder_options =
      (ZOrderClusteringOptions *)options;
  CheckOptions(zorder_options);

  TupleDesc tup_desc;
  TupleTableSlot *origin_slot;
  TupleTableSlot *zorder_slot;
  TupleTableSlot *zorder_sorted_slot;
  TupleSorter::HeapTupleSorterOptions sorter_options;
  AttrNumber attr_number[1];
  Oid zorder_default_collation = DEFAULT_COLLATION_OID;
  Oid zorder_default_operator = ByteaLessOperator;

  int buffer_len = zorder_options->nkeys * N_BYTES;
  char column_datum_buffer[buffer_len];
  memset(column_datum_buffer, 0, buffer_len);

  int zorder_nattrs = zorder_options->tup_desc->natts + 1;

  CBDB_WRAP_START;
  {
    tup_desc = CreateTemplateTupleDesc(zorder_nattrs);

    for (int i = 1; i <= zorder_options->tup_desc->natts; i++) {
      TupleDescCopyEntry(tup_desc, i, zorder_options->tup_desc, i);
    }
    TupleDescInitEntry(tup_desc, (AttrNumber)zorder_nattrs, "zorder_value",
                       BYTEAOID, -1, 0);
  }
  CBDB_WRAP_END;

  sorter_options.tup_desc = tup_desc;
  sorter_options.nulls_first_flags = zorder_options->nulls_first_flags;
  sorter_options.work_mem = zorder_options->work_mem;
  sorter_options.nkeys = 1;
  sorter_options.attr = &attr_number[0];
  // zorder-value is the last column
  sorter_options.attr[0] = tup_desc->natts;

  sorter_options.sortCollations = &zorder_default_collation;
  sorter_options.sortOperators = &zorder_default_operator;

  // TODO(gongxun): use dependency injection to support different sorter
  // implementations
  TupleSorter sorter(sorter_options);

  origin_slot =
      cbdb::MakeSingleTupleTableSlot(zorder_options->tup_desc, &TTSOpsVirtual);
  zorder_slot = cbdb::MakeSingleTupleTableSlot(tup_desc, &TTSOpsVirtual);

  while (reader->GetNextTuple(origin_slot)) {
    MakeZOrderTupleSlot(zorder_slot, origin_slot, zorder_options,
                        column_datum_buffer, buffer_len);
    sorter.AppendSortData(zorder_slot);

    // release zorder value
    cbdb::Pfree(
        cbdb::DatumToPointer(zorder_slot->tts_values[zorder_nattrs - 1]));
  }

  cbdb::ExecDropSingleTupleTableSlot(origin_slot);

  sorter.Sort();

  zorder_sorted_slot =
      cbdb::MakeSingleTupleTableSlot(tup_desc, &TTSOpsMinimalTuple);
  while (sorter.GetSortedData(zorder_sorted_slot)) {
    cbdb::SlotGetAllAttrs(zorder_sorted_slot);
    writer->WriteTuple(zorder_sorted_slot);
  }
  cbdb::ExecDropSingleTupleTableSlot(zorder_sorted_slot);
}

void ZOrderClustering::CheckOptions(
    const ZOrderClusteringOptions *zorder_options) {
  CBDB_CHECK(zorder_options != NULL, cbdb::CException::kExTypeInvalid,
             "zorder_options should not be NULL");
  CBDB_CHECK(
      (zorder_options->nkeys >= 0 && zorder_options->nkeys <= MAX_ZORDER_NKEYS),
      cbdb::CException::kExTypeInvalid,
      fmt("nkeys should be less than or equal to %d", MAX_ZORDER_NKEYS));
  CBDB_CHECK(zorder_options->tup_desc != NULL, cbdb::CException::kExTypeInvalid,
             "tup_desc should not be NULL");

  for (int i = 0; i < zorder_options->nkeys; i++) {
    int col_index = zorder_options->attr[i];
    CBDB_CHECK(paxc::support_zorder_type(
                   zorder_options->tup_desc->attrs[col_index - 1].atttypid),
               cbdb::CException::kExTypeInvalid,
               fmt("type %d is not supported in zorder clustering",
                   zorder_options->tup_desc->attrs[col_index - 1].atttypid));
  }
}

void ZOrderClustering::MakeZOrderTupleSlot(
    TupleTableSlot *zorder_slot, const TupleTableSlot *slot,
    const ZOrderClusteringOptions *zorder_options, char *column_datum_buffer,
    int buffer_len) {
  Datum zorder_value;
  int zorder_nattr = zorder_slot->tts_tupleDescriptor->natts - 1;

  for (int i = 0; i < zorder_options->nkeys; i++) {
    int col_index = zorder_options->attr[i];
    Oid type = slot->tts_tupleDescriptor->attrs[col_index - 1].atttypid;
    Datum value = slot->tts_values[col_index - 1];
    pax::datum_to_bytes(value, type, slot->tts_isnull[col_index - 1],
                        column_datum_buffer + i * N_BYTES);
  }

  zorder_value =
      pax::bytes_to_zorder_datum(column_datum_buffer, zorder_options->nkeys);

  cbdb::ExecClearTuple(zorder_slot);
  // value copy is ok, pg sort will perform a deep copy
  memcpy(zorder_slot->tts_values, slot->tts_values,
         sizeof(Datum) * (zorder_options->tup_desc->natts));

  memcpy(zorder_slot->tts_isnull, slot->tts_isnull,
         sizeof(bool) * (zorder_options->tup_desc->natts));

  zorder_slot->tts_values[zorder_nattr] = zorder_value;
  zorder_slot->tts_isnull[zorder_nattr] = false;

  cbdb::ExecStoreVirtualTuple(zorder_slot);
}
}  // namespace clustering

}  // namespace pax
