#pragma once

#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <string>

#include "comm/bitmap.h"
#include "comm/pax_memory.h"
#include "storage/pax.h"

namespace pax {
class CPaxDeleter {
 public:
  explicit CPaxDeleter(Relation rel, Snapshot snapshot);
  ~CPaxDeleter() = default;
  static TM_Result DeleteTuple(Relation relation, ItemPointer tid,
                               CommandId cid, Snapshot snapshot,
                               TM_FailureData *tmfd);

  TM_Result MarkDelete(ItemPointer tid);
  bool IsMarked(ItemPointerData tid) const;
  void MarkDelete(BlockNumber pax_block_id);
  void ExecDelete();

 private:
  std::unique_ptr<IteratorBase<MicroPartitionMetadata>> BuildDeleteIterator();
  std::map<std::string, std::shared_ptr<Bitmap8>> block_bitmap_map_;
  Relation rel_;
  Snapshot snapshot_;
  TransactionId delete_xid_;
  bool use_visimap_;
};  // class CPaxDeleter
}  // namespace pax