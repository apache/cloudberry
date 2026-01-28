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
 * pax.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/pax.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/pax.h"

#include <map>
#include <utility>

#include "access/pax_visimap.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_catalog.h"
#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "comm/paxc_wrappers.h"
#include "storage/columns/pax_encoding.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/micro_partition_metadata.h"
#include "storage/micro_partition_stats.h"
#include "storage/micro_partition_stats_updater.h"
#include "storage/orc/orc_dump_reader.h"
#include "storage/wal/pax_wal.h"
#include "storage/wal/paxc_wal.h"
#ifdef VEC_BUILD
#include "storage/vec/pax_vec_reader.h"
#endif

#define PAX_SPLIT_STRATEGY_CHECK_INTERVAL (16)

namespace paxc {
class IndexUpdaterInternal {
 public:
  void Begin(Relation rel) {
    Assert(rel);

    rel_ = rel;
    slot_ = MakeTupleTableSlot(rel->rd_att, &TTSOpsVirtual);

    if (HasIndex()) {
      estate_ = CreateExecutorState();

      relinfo_ = makeNode(ResultRelInfo);
      relinfo_->ri_RelationDesc = rel;
      ExecOpenIndices(relinfo_, false);
    }
  }

  void UpdateIndex(TupleTableSlot *slot) {
    Assert(slot == slot_);
    Assert(HasIndex());
    auto recheck_index =
        ExecInsertIndexTuples(relinfo_, slot_, estate_, true, false, NULL, NIL);
    list_free(recheck_index);
  }

  void End() {
    if (HasIndex()) {
      Assert(relinfo_ && estate_);

      ExecCloseIndices(relinfo_);
      pfree(relinfo_);
      relinfo_ = nullptr;

      FreeExecutorState(estate_);
      estate_ = nullptr;
    }
    Assert(relinfo_ == nullptr && estate_ == nullptr);

    ExecDropSingleTupleTableSlot(slot_);
    slot_ = nullptr;

    rel_ = nullptr;
  }

  inline TupleTableSlot *GetSlot() { return slot_; }
  inline bool HasIndex() const { return rel_->rd_rel->relhasindex; }

 private:
  Relation rel_ = nullptr;
  TupleTableSlot *slot_ = nullptr;
  EState *estate_ = nullptr;
  ResultRelInfo *relinfo_ = nullptr;
};
}  // namespace paxc

namespace pax {
class IndexUpdater final {
 public:
  void Begin(Relation rel) {
    CBDB_WRAP_START;
    { stub_.Begin(rel); }
    CBDB_WRAP_END;
  }
  void UpdateIndex(TupleTableSlot *slot) {
    CBDB_WRAP_START;
    { stub_.UpdateIndex(slot); }
    CBDB_WRAP_END;
  }
  void End() {
    CBDB_WRAP_START;
    { stub_.End(); }
    CBDB_WRAP_END;
  }
  inline TupleTableSlot *GetSlot() { return stub_.GetSlot(); }
  inline bool HasIndex() const { return stub_.HasIndex(); }

 private:
  paxc::IndexUpdaterInternal stub_;
};
}  // namespace pax

namespace pax {

TableWriter::TableWriter(Relation relation)
    : relation_(relation), summary_callback_(nullptr), options_cached_(false) {
  Assert(relation);

  file_system_ = Singleton<LocalFileSystem>::GetInstance();
}

TableWriter *TableWriter::SetWriteSummaryCallback(
    WriteSummaryCallback callback) {
  Assert(!summary_callback_);
  summary_callback_ = callback;
  return this;
}

TableWriter *TableWriter::SetFileSplitStrategy(
    std::unique_ptr<FileSplitStrategy> &&strategy) {
  Assert(!strategy_);
  strategy_ = std::move(strategy);
  return this;
}

TableWriter::~TableWriter() {}

const FileSplitStrategy *TableWriter::GetFileSplitStrategy() const {
  return strategy_.get();
}

std::string TableWriter::GenFilePath(const std::string &block_id) {
  return cbdb::BuildPaxFilePath(rel_path_, block_id);
}

std::string TableWriter::GenToastFilePath(const std::string &file_path) {
  return file_path + TOAST_FILE_SUFFIX;
}

std::unique_ptr<MicroPartitionWriter> TableWriter::CreateMicroPartitionWriter(
    std::shared_ptr<MicroPartitionStats> mp_stats, bool write_only) {
  MicroPartitionWriter::WriterOptions options;
  std::string file_path;
  std::string toast_file_path;
  std::string block_id;
  std::unique_ptr<File> file;
  std::unique_ptr<File> toast_file;
  int open_flags;
  int block_number;

  Assert(relation_);
  Assert(strategy_);
  Assert(summary_callback_);

  block_number = cbdb::CPaxGetFastSequences(RelationGetRelid(relation_));
  Assert(block_number >= 0);
  current_blockno_ = block_number;
  block_id = std::to_string(block_number);
  file_path = GenFilePath(block_id);
  toast_file_path = GenToastFilePath(file_path);

  options.rel_oid = relation_->rd_id;
  options.node = relation_->rd_node;
  // only permanent table should write wal
  options.need_wal =
      relation_->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT;
  options.rel_tuple_desc = relation_->rd_att;
  options.block_id = std::move(block_id);
  options.file_name = std::move(file_path);
  options.encoding_opts = GetRelEncodingOptions();
  options.storage_format = GetStorageFormat();
  options.enable_min_max_col_idxs = GetMinMaxColumnIndexes();
  options.enable_bf_col_idxs = GetBloomFilterColumnIndexes();

  // FIXME(gongxun): Non-partition writers do not need to read and merge, so we
  // open them in write-only mode. Write-only writer can work on object storage.
  // For partitioned writes, we need to reconstruct this part of the logic to
  // support partitioned writers on object storage.
  if (write_only) {
    // FIXME: In a transaction, after obtaining a block_id from
    // pax_fastsequence,if the next two file_system->Open() calls are
    // successful, the block_id file and block_id.toast file will be created.
    // when getting the incremented block_id from the pax_fastsequence table, we
    // use heap_inplace_update which will mark the heap buffer as dirty and
    // insert an xlog, but it does not guarantee that the page buffer or xlog
    // has been flushed to disk. If the transation is abort, the buffer and
    // xlog are not flushed to disk, and next insert transaction will start from
    // the current block_id. but the data file and toast file created earlier
    // will not be deleted. Therefore, O_CREAT | O_TRUNC is specified: if the
    // file does not exist, it will be created; if it exists, its previous
    // content will be truncated.
    open_flags = fs::kWriteWithTruncMode;
  } else {
    open_flags = fs::kReadWriteMode;
  }

  // should be kReadWriteMode here
  // cause PAX may do read after write in partition logic
  file =
      file_system_->Open(options.file_name, open_flags, file_system_options_);
  Assert(file);

  if (pax_enable_toast) {
    toast_file =
        file_system_->Open(toast_file_path, open_flags, file_system_options_);
  }

  auto mp_writer = MicroPartitionFileFactory::CreateMicroPartitionWriter(
      std::move(options), std::move(file), std::move(toast_file));

  Assert(mp_writer);
  mp_writer->SetWriteSummaryCallback(summary_callback_)
      ->SetStatsCollector(mp_stats);
  return mp_writer;
}

void TableWriter::InitOptionsCaches() {
  if (!options_cached_) {
    storage_format_ = StorageFormatKeyToPaxStorageFormat(RelationGetOptions(
        relation_, storage_format,
        pax_default_storage_format ? pax_default_storage_format
                                   : STORAGE_FORMAT_TYPE_DEFAULT));
    min_max_col_idx_ = cbdb::GetMinMaxColumnIndexes(relation_);
    bf_col_idx_ = cbdb::GetBloomFilterColumnIndexes(relation_);
    encoding_opts_ = cbdb::GetRelEncodingOptions(relation_);
    options_cached_ = true;
  }
}

void TableWriter::Open() {
  rel_path_ =
      cbdb::BuildPaxDirectoryPath(relation_->rd_node, relation_->rd_backend);

  InitOptionsCaches();

  // Exception may be thrown causing writer_ to be nullptr

  if (!mp_stats_) {
    mp_stats_ =
        std::make_shared<MicroPartitionStats>(RelationGetDescr(relation_));
    mp_stats_->Initialize(GetMinMaxColumnIndexes(),
                          GetBloomFilterColumnIndexes());
  } else {
    mp_stats_->Reset();
  }

  writer_ = CreateMicroPartitionWriter(mp_stats_);
  num_tuples_ = 0;
  // insert tuple into the aux table before inserting any tuples.
  cbdb::InsertMicroPartitionPlaceHolder(RelationGetRelid(relation_),
                                        current_blockno_);
  cur_physical_size_ = 0;
}

void TableWriter::WriteTuple(TupleTableSlot *slot) {
  Assert(writer_);
  Assert(strategy_);
  // Because of the CTID constraint, we have to strictly enforce the accuracy of
  // the tuple count and make sure it doesn't exceed
  // PAX_MAX_NUM_TUPLES_PER_FILE. That's why we kept this precise check here.

  // On the other hand,the biggest performance hit here is the PhysicalSize()
  // function.So to reduce the overhead of calling it so often,
  // we only update the file size every PAX_SPLIT_STRATEGY_CHECK_INTERVAL
  // tuples.
  if ((num_tuples_ % PAX_SPLIT_STRATEGY_CHECK_INTERVAL) == 0) {
    cur_physical_size_ = writer_->PhysicalSize();
  }

  if (strategy_->ShouldSplit(cur_physical_size_, num_tuples_)) {
    writer_->Close();
    writer_ = nullptr;
    Open();
  }

  writer_->WriteTuple(slot);
  SetBlockNumber(&slot->tts_tid, current_blockno_);
  ++num_tuples_;
}

void TableWriter::Close() {
  if (writer_) {
    writer_->Close();
    writer_ = nullptr;
  }
  num_tuples_ = 0;
}

TableReader::TableReader(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    ReaderOptions options)
    : iterator_(std::move(iterator)),
      reader_(nullptr),
      is_empty_(false),
      reader_options_(options) {
  file_system_ = Singleton<LocalFileSystem>::GetInstance();
}

TableReader::~TableReader() {}

void TableReader::Open() {
  if (!iterator_->HasNext()) {
    is_empty_ = true;
    return;
  }
  OpenFile();
  is_empty_ = false;
}

void TableReader::ReOpen() {
  Close();
  iterator_->Rewind();
  Open();
}

void TableReader::Close() {
  if (reader_) {
    reader_->Close();
    reader_ = nullptr;
  }
  iterator_->Release();
}

bool TableReader::ReadTuple(TupleTableSlot *slot) {
  if (unlikely(!reader_)) {
    Open();
    if (is_empty_) return false;
    Assert(reader_);
  }

  ExecClearTuple(slot);
  SetBlockNumber(&slot->tts_tid, current_block_number_);
  while (!reader_->ReadTuple(slot)) {
    reader_->Close();
    reader_ = nullptr;
    Open();
    if (is_empty_) return false;
    Assert(reader_);
    SetBlockNumber(&slot->tts_tid, current_block_number_);
  }

  Assert(TTS_EMPTY(slot));
  ExecStoreVirtualTuple(slot);

  return true;
}

bool TableReader::GetTuple(TupleTableSlot *slot, ScanDirection direction,
                           size_t offset) {
  MicroPartitionReader::ReaderOptions options;
  ReaderFlags reader_flags = FLAGS_EMPTY;
  size_t row_index = current_block_row_index_;
  size_t max_row_index;
  size_t remaining_offset = offset;
  std::unique_ptr<File> toast_file;
  bool ok;

  if (!reader_) {
    Open();
    if (is_empty_) return false;
  }

  Assert(direction == ForwardScanDirection);
  Assert(current_block_metadata_.GetTupleCount() >= 1);
  max_row_index = current_block_metadata_.GetTupleCount() - 1;

  // The number of remaining rows in the current block is enough to satisfy the
  // offset
  if ((max_row_index - row_index) >= remaining_offset) {
    current_block_row_index_ = row_index + remaining_offset;
    SetBlockNumber(&slot->tts_tid,
                   current_block_metadata_.GetMicroPartitionId());
    ok = reader_->GetTuple(slot, current_block_row_index_);
    return ok;
  }

  // There are not enough lines remaining in the current block and we need to
  // jump to the next block
  remaining_offset -= (max_row_index - row_index);

  // iterator_->HasNext() must be true here.
  // When analyzing, pax read the number of tuples in the auxiliary table
  // through relation_estimate_size. The number of tuples is a correct number.
  // The target_tuple_id (the return blocknumber of BlockSampler_Next ) sampled
  // by analyze will not exceed the number of tuples.
  CBDB_CHECK(
      iterator_->HasNext(), cbdb::CException::kExTypeOutOfRange,
      fmt("No more tuples to read. Invalid [target offsets=%lu, remain "
          "offsets=%lu]. ",
          offset, remaining_offset - current_block_metadata_.GetTupleCount()));

  while (iterator_->HasNext()) {
    current_block_metadata_ = iterator_->Next();
    if (current_block_metadata_.GetTupleCount() >= remaining_offset) {
      break;
    }
    remaining_offset -= current_block_metadata_.GetTupleCount();
  }

  // remain_offset must point to an existing tuple (whether it is valid or
  // invalid)
  CBDB_CHECK(current_block_metadata_.GetTupleCount() >= remaining_offset,
             cbdb::CException::kExTypeLogicError,
             fmt("Invalid tuple counts in current block [block tuple counts= "
                 "%ul, remain offsets=%lu]\n"
                 "Meta data [name=%s, id=%d]. ",
                 current_block_metadata_.GetTupleCount(), remaining_offset,
                 current_block_metadata_.GetFileName().c_str(),
                 current_block_metadata_.GetMicroPartitionId()));

  // close old reader
  if (reader_) {
    reader_->Close();
  }

  options.filter = reader_options_.filter;
  options.reused_buffer = reader_options_.reused_buffer;

  // load visibility map
  // TODO(jiaqizho): PAX should not do read/pread in table layer
  std::string visibility_bitmap_file =
      current_block_metadata_.GetVisibilityBitmapFile();
  if (!visibility_bitmap_file.empty()) {
    auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode);
    auto file_length = file->FileLength();
    auto bm = std::make_shared<Bitmap8>(file_length * 8);
    file->ReadN(bm->Raw().bitmap, file_length);
    options.visibility_bitmap = bm;
    file->Close();
  }

  if (current_block_metadata_.GetExistToast()) {
    toast_file = file_system_->Open(
        current_block_metadata_.GetFileName() + TOAST_FILE_SUFFIX,
        fs::kReadMode);
  }

  reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
      std::move(options), reader_flags,
      file_system_->Open(current_block_metadata_.GetFileName(), fs::kReadMode),
      std::move(toast_file));

  // row_index start from 0, so row_index = offset -1
  current_block_row_index_ = remaining_offset - 1;
  SetBlockNumber(&slot->tts_tid, current_block_metadata_.GetMicroPartitionId());

  ok = reader_->GetTuple(slot, current_block_row_index_);
  return ok;
}

void TableReader::OpenFile() {
  Assert(iterator_->HasNext());

  auto it = iterator_->Next();
  current_block_metadata_ = it;
  MicroPartitionReader::ReaderOptions options;
  std::unique_ptr<File> toast_file;
  int32 reader_flags = FLAGS_EMPTY;

  micro_partition_id_ = it.GetMicroPartitionId();
  current_block_number_ = micro_partition_id_;

  options.filter = reader_options_.filter;
  options.reused_buffer = reader_options_.reused_buffer;

  // load visibility map
  std::string visibility_bitmap_file = it.GetVisibilityBitmapFile();
  if (!visibility_bitmap_file.empty()) {
    auto file = file_system_->Open(visibility_bitmap_file, fs::kReadMode);
    auto file_length = file->FileLength();
    auto bm = std::make_shared<Bitmap8>(file_length * 8);
    file->ReadN(bm->Raw().bitmap, file_length);
    options.visibility_bitmap = bm;
    file->Close();
  }

#ifdef VEC_BUILD
  options.tuple_desc = reader_options_.tuple_desc;
  if (reader_options_.is_vec) {
    Assert(options.tuple_desc);
    READER_FLAG_SET_VECTOR_PATH(reader_flags);
  }

  if (reader_options_.vec_build_ctid)
    READER_FLAG_SET_SCAN_WITH_CTID(reader_flags);
#endif

  if (it.GetExistToast()) {
    // must exist the file in disk
    toast_file =
        file_system_->Open(it.GetFileName() + TOAST_FILE_SUFFIX, fs::kReadMode);
  }

  reader_ = MicroPartitionFileFactory::CreateMicroPartitionReader(
      std::move(options), reader_flags,
      file_system_->Open(it.GetFileName(), fs::kReadMode),
      std::move(toast_file));
}

TableDeleter::TableDeleter(
    Relation rel, std::map<int, std::shared_ptr<Bitmap8>> delete_bitmap,
    Snapshot snapshot)
    : rel_(rel), snapshot_(snapshot), delete_bitmap_(delete_bitmap) {
  need_wal_ = cbdb::NeedWAL(rel);
  file_system_ = Singleton<LocalFileSystem>::GetInstance();
}

void TableDeleter::UpdateStatsInAuxTable(
    pax::PaxCatalogUpdater &catalog_update,
    const pax::MicroPartitionMetadata &meta,
    std::shared_ptr<Bitmap8> visi_bitmap,
    const std::vector<int> &min_max_col_idxs,
    const std::vector<int> &bf_col_idxs, std::shared_ptr<PaxFilter> filter) {
  MicroPartitionReader::ReaderOptions options;
  std::unique_ptr<File> toast_file;
  int32 reader_flags = FLAGS_EMPTY;
  TupleTableSlot *slot;

  options.filter = filter;
  options.reused_buffer =
      nullptr;  // TODO(jiaqizho): let us reuse the read buffer
  options.visibility_bitmap = visi_bitmap;

  if (meta.GetExistToast()) {
    // must exist the file in disk
    toast_file = file_system_->Open(meta.GetFileName() + TOAST_FILE_SUFFIX,
                                    fs::kReadMode, file_system_options_);
  }

  auto mp_reader = MicroPartitionFileFactory::CreateMicroPartitionReader(
      std::move(options), reader_flags,
      file_system_->Open(meta.GetFileName(), fs::kReadMode,
                         file_system_options_),
      std::move(toast_file));

  slot = MakeTupleTableSlot(rel_->rd_att, &TTSOpsVirtual);
  auto updated_stats = MicroPartitionStatsUpdater(mp_reader.get(), visi_bitmap)
                           .Update(slot, min_max_col_idxs, bf_col_idxs);

  // update the statistics in aux table
  catalog_update.UpdateStatistics(meta.GetMicroPartitionId(),
                                  updated_stats->Serialize());

  mp_reader->Close();

  cbdb::ExecDropSingleTupleTableSlot(slot);
}

// The pattern of file name of the visimap file is:
// <blocknum>_<generation>_<tag>.visimap
// blocknum: the corresponding micro partition that the visimap is for
// generation: every delete operation will increase the generation
// tag: used to make the file name unique in reality. Only the first two
//      parts is enough to be unique in normal. If the next generation
//      visimap file is generated, but was canceled later, the visimap
//      file may be not deleted in time. Then, the next delete will
//      use the same file name for visimap file.
void TableDeleter::DeleteWithVisibilityMap(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator,
    TransactionId delete_xid) {
  if (!iterator->HasNext()) {
    return;
  }
  std::vector<int> min_max_col_idxs;
  auto stats_updater_projection = std::make_shared<PaxFilter>();

  std::unique_ptr<Bitmap8> visi_bitmap;
  auto catalog_update = pax::PaxCatalogUpdater::Begin(rel_);
  auto rel_path = cbdb::BuildPaxDirectoryPath(rel_->rd_node, rel_->rd_backend);

  min_max_col_idxs = cbdb::GetMinMaxColumnIndexes(rel_);
  stats_updater_projection->SetColumnProjection(min_max_col_idxs,
                                                rel_->rd_att->natts);
  do {
    auto it = iterator->Next();

    auto block_id = it.GetMicroPartitionId();
    auto micro_partition_metadata =
        cbdb::GetMicroPartitionMetadata(rel_, snapshot_, block_id);
    int generate = 0;
    char visimap_file_name[128];

    // union bitmap
    auto delete_visi_bitmap = delete_bitmap_[block_id]->Clone();
    // read visibility map
    {
      int rc pg_attribute_unused();
      auto visibility_map_filename =
          micro_partition_metadata.GetVisibilityBitmapFile();

      if (!visibility_map_filename.empty()) {
        auto buffer = LoadVisimap(file_system_, file_system_options_,
                                  visibility_map_filename);
        auto visibility_file_bitmap =
            Bitmap8(BitmapRaw<uint8>(buffer->data(), buffer->size()));
        visi_bitmap =
            Bitmap8::Union(&visibility_file_bitmap, delete_visi_bitmap.get());

        {
          int blocknum;
          TransactionId xid;

          auto visi_name = strrchr(visibility_map_filename.c_str(), '/');
          CBDB_CHECK(visi_name != nullptr, cbdb::CException::kExTypeLogicError);
          visi_name++;
          rc =
              sscanf(visi_name, "%d_%x_%x.visimap", &blocknum, &generate, &xid);
          Assert(blocknum >= 0 && block_id == blocknum);
          (void)xid;
          CBDB_CHECK(rc == 3, cbdb::CException::kExTypeLogicError,
                     fmt("Fail to sscanf [rc=%d, filename=%s, rel_path=%s]", rc,
                         visibility_map_filename.c_str(), rel_path.c_str()));
        }
      } else {
        visi_bitmap = std::move(delete_visi_bitmap);
      }

      // generate new file name for visimap
      rc = snprintf(visimap_file_name, sizeof(visimap_file_name),
                    "%d_%x_%x.visimap", block_id, generate + 1, delete_xid);
      Assert(rc <= NAMEDATALEN);
    }

    Assert(visi_bitmap != nullptr);
    std::string visimap_file_path =
        cbdb::BuildPaxFilePath(rel_path.c_str(), visimap_file_name);
    {
      auto &raw = visi_bitmap->Raw();
      auto visimap_file = file_system_->Open(visimap_file_path, fs::kWriteMode,
                                             file_system_options_);
      visimap_file->WriteN(raw.bitmap, raw.size);
      if (need_wal_) {
        cbdb::XLogPaxInsert(rel_->rd_node, visimap_file_name, 0, raw.bitmap,
                            raw.size);
      }
      visimap_file->Close();
    }

    // TODO: update stats and visimap all in one catalog update
    // Update the stats in pax aux table
    // Notice that: PAX won't update the stats in group
    UpdateStatsInAuxTable(
        catalog_update, micro_partition_metadata,
        std::make_shared<Bitmap8>(visi_bitmap->Raw()), min_max_col_idxs,
        cbdb::GetBloomFilterColumnIndexes(rel_), stats_updater_projection);

    // write pg_pax_blocks_oid
    catalog_update.UpdateVisimap(block_id, visimap_file_name);
  } while (iterator->HasNext());
  catalog_update.End();
}

void TableDeleter::Delete(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator) {
  if (!iterator->HasNext()) {
    return;
  }
  auto reader = OpenReader(std::move(iterator));
  auto writer = OpenWriter();
  pax::IndexUpdater index_updater;

  index_updater.Begin(rel_);
  Assert(rel_->rd_rel->relhasindex == index_updater.HasIndex());
  auto slot = index_updater.GetSlot();

  slot->tts_tableOid = RelationGetRelid(rel_);
  // TODO(gongxun): because bulk insert as AO/HEAP does with tuples iteration
  // not implemented. we should implement bulk insert firstly. and then we can
  // use ReadTupleN and WriteTupleN to delete tuples in batch.
  while (reader->ReadTuple(slot)) {
    auto block_id = reader->GetCurrentMicroPartitionId();
    auto it = delete_bitmap_.find(block_id);
    Assert(it != delete_bitmap_.end());

    auto bitmap = it->second.get();
    if (bitmap->Test(pax::GetTupleOffset(slot->tts_tid))) continue;

    writer->WriteTuple(slot);
    if (index_updater.HasIndex()) {
      // Already store the ctid after WriteTuple
      Assert(!TTS_EMPTY(slot));
      Assert(ItemPointerIsValid(&slot->tts_tid));
      index_updater.UpdateIndex(slot);
    }
  }
  index_updater.End();

  writer->Close();
  reader->Close();
}

std::unique_ptr<TableWriter> TableDeleter::OpenWriter() {
  auto writer = std::make_unique<TableWriter>(rel_);
  writer->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(std::make_unique<PaxDefaultSplitStrategy>())
      ->Open();
  return writer;
}

std::unique_ptr<TableReader> TableDeleter::OpenReader(
    std::unique_ptr<IteratorBase<MicroPartitionMetadata>> &&iterator) {
  TableReader::ReaderOptions reader_options{};

  reader_options.table_space_id = rel_->rd_rel->reltablespace;
  auto reader =
      std::make_unique<TableReader>(std::move(iterator), reader_options);
  reader->Open();
  return reader;
}

}  // namespace pax
