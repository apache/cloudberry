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
 * GpscStat.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/GpscStat.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "GpscStat.h"

#include <algorithm>

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
}

namespace
{
struct ProtectedData
{
	slock_t mutex;
	GpscStat::Data data;
};
shmem_startup_hook_type prev_shmem_startup_hook = NULL;
ProtectedData *data = nullptr;

void
gpsc_shmem_startup()
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	bool found;
	data = reinterpret_cast<ProtectedData *>(
		ShmemInitStruct("gpsc_stat_messages", sizeof(ProtectedData), &found));
	if (!found)
	{
		SpinLockInit(&data->mutex);
		data->data = GpscStat::Data();
	}
	LWLockRelease(AddinShmemInitLock);
}

class LockGuard
{
public:
	LockGuard(slock_t *mutex) : mutex_(mutex)
	{
		SpinLockAcquire(mutex_);
	}
	~LockGuard()
	{
		SpinLockRelease(mutex_);
	}

private:
	slock_t *mutex_;
};
}  // namespace

void
GpscStat::init()
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	RequestAddinShmemSpace(sizeof(ProtectedData));
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = gpsc_shmem_startup;
}

void
GpscStat::deinit()
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

void
GpscStat::reset()
{
	LockGuard lg(&data->mutex);
	data->data = GpscStat::Data();
}

void
GpscStat::report_send(int32_t msg_size)
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.max_message_size =
		std::max(msg_size, data->data.max_message_size);
}

void
GpscStat::report_bad_connection()
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_connects++;
}

void
GpscStat::report_bad_send(int32_t msg_size)
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_sends++;
	data->data.max_message_size =
		std::max(msg_size, data->data.max_message_size);
}

void
GpscStat::report_error()
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_other++;
}

GpscStat::Data
GpscStat::get_stats()
{
	LockGuard lg(&data->mutex);
	return data->data;
}

bool
GpscStat::loaded()
{
	return data != nullptr;
}
