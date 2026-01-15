#include "YagpStat.h"

#include <algorithm>

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
}

namespace {
struct ProtectedData {
  slock_t mutex;
  YagpStat::Data data;
};
shmem_startup_hook_type prev_shmem_startup_hook = NULL;
ProtectedData *data = nullptr;

void yagp_shmem_startup() {
  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  bool found;
  data = reinterpret_cast<ProtectedData *>(
      ShmemInitStruct("yagp_stat_messages", sizeof(ProtectedData), &found));
  if (!found) {
    SpinLockInit(&data->mutex);
    data->data = YagpStat::Data();
  }
  LWLockRelease(AddinShmemInitLock);
}

class LockGuard {
public:
  LockGuard(slock_t *mutex) : mutex_(mutex) { SpinLockAcquire(mutex_); }
  ~LockGuard() { SpinLockRelease(mutex_); }

private:
  slock_t *mutex_;
};
} // namespace

void YagpStat::init() {
  if (!process_shared_preload_libraries_in_progress)
    return;
  RequestAddinShmemSpace(sizeof(ProtectedData));
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = yagp_shmem_startup;
}

void YagpStat::deinit() { shmem_startup_hook = prev_shmem_startup_hook; }

void YagpStat::reset() {
  LockGuard lg(&data->mutex);
  data->data = YagpStat::Data();
}

void YagpStat::report_send(int32_t msg_size) {
  LockGuard lg(&data->mutex);
  data->data.total++;
  data->data.max_message_size = std::max(msg_size, data->data.max_message_size);
}

void YagpStat::report_bad_connection() {
  LockGuard lg(&data->mutex);
  data->data.total++;
  data->data.failed_connects++;
}

void YagpStat::report_bad_send(int32_t msg_size) {
  LockGuard lg(&data->mutex);
  data->data.total++;
  data->data.failed_sends++;
  data->data.max_message_size = std::max(msg_size, data->data.max_message_size);
}

void YagpStat::report_error() {
  LockGuard lg(&data->mutex);
  data->data.total++;
  data->data.failed_other++;
}

YagpStat::Data YagpStat::get_stats() {
  LockGuard lg(&data->mutex);
  return data->data;
}

bool YagpStat::loaded() { return data != nullptr; }
