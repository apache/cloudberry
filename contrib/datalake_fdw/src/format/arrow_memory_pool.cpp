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
 * arrow_memory_pool.cpp
 *	  An Arrow memory pool whose bytes the vmem tracker knows about.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_memory_pool.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <string>

#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/util/config.h>

#include "format/arrow_memory_pool.h"

extern "C"
{
#include "postgres.h"

#include "miscadmin.h"
#include "utils/vmem_tracker.h"
}

namespace
{

const char *
dl_vmem_failure(MemoryAllocationStatus status)
{
	switch (status)
	{
		case MemoryFailure_VmemExhausted:
			return "the segment's memory limit (gp_vmem_protect_limit) is exhausted";
		case MemoryFailure_QueryMemoryExhausted:
			return "the query's memory limit is exhausted";
		case MemoryFailure_ResourceGroupMemoryExhausted:
			return "the resource group's memory limit is exhausted";
		case MemoryFailure_SystemMemoryExhausted:
			return "the system is out of memory";
		default:
			return "the memory could not be reserved";
	}
}

/*
 * Reserves with the tracker before allocating and releases after freeing, so
 * that what the tracker counts is never less than what Arrow holds.
 *
 * ProxyMemoryPool rather than MemoryPool as the base: the statistics methods
 * are pure virtual in some Arrow versions and absent in others, and the proxy
 * implements whichever set its version has.  Only the three that move memory
 * are overridden, and those changed signature at Arrow 11, which is what the
 * version guard is about.
 */
class DlTrackedMemoryPool : public arrow::ProxyMemoryPool
{
public:
	explicit DlTrackedMemoryPool(arrow::MemoryPool *pool)
		: arrow::ProxyMemoryPool(pool)
	{
	}

#if ARROW_VERSION_MAJOR >= 11
	using arrow::MemoryPool::Allocate;
	using arrow::MemoryPool::Reallocate;
	using arrow::MemoryPool::Free;

	arrow::Status Allocate(int64_t size, int64_t alignment,
						   uint8_t **out) override
	{
		return TrackedAllocate(size, [&]() {
			return arrow::ProxyMemoryPool::Allocate(size, alignment, out);
		});
	}

	arrow::Status Reallocate(int64_t old_size, int64_t new_size,
							 int64_t alignment, uint8_t **ptr) override
	{
		return TrackedReallocate(old_size, new_size, [&]() {
			return arrow::ProxyMemoryPool::Reallocate(old_size, new_size,
													  alignment, ptr);
		});
	}

	void Free(uint8_t *buffer, int64_t size, int64_t alignment) override
	{
		arrow::ProxyMemoryPool::Free(buffer, size, alignment);
		Release(size);
	}
#else
	arrow::Status Allocate(int64_t size, uint8_t **out) override
	{
		return TrackedAllocate(size, [&]() {
			return arrow::ProxyMemoryPool::Allocate(size, out);
		});
	}

	arrow::Status Reallocate(int64_t old_size, int64_t new_size,
							 uint8_t **ptr) override
	{
		return TrackedReallocate(old_size, new_size, [&]() {
			return arrow::ProxyMemoryPool::Reallocate(old_size, new_size, ptr);
		});
	}

	void Free(uint8_t *buffer, int64_t size) override
	{
		arrow::ProxyMemoryPool::Free(buffer, size);
		Release(size);
	}
#endif

	std::string backend_name() const override
	{
		return "vmem-tracked " + arrow::ProxyMemoryPool::backend_name();
	}

private:
	/*
	 * The tracker may decide, on the way to saying no, that this session is
	 * the one to cancel, or that a pending interrupt should be serviced now;
	 * either is an elog(ERROR), and a longjmp from here would go through
	 * Arrow's C++ frames, which is undefined behaviour.  Holding interrupts
	 * turns both into "not now": the tracker then only returns a status, and
	 * the interrupt is taken at the next CHECK_FOR_INTERRUPTS() in C code,
	 * which every loop that calls into Arrow has.
	 */
	static arrow::Status
	Reserve(int64_t bytes)
	{
		MemoryAllocationStatus status;

		if (bytes <= 0)
			return arrow::Status::OK();

		HOLD_INTERRUPTS();
		status = VmemTracker_ReserveVmem(bytes);
		RESUME_INTERRUPTS();

		if (status != MemoryAllocation_Success)
			return arrow::Status::OutOfMemory("could not reserve ", bytes,
											  " bytes for Arrow: ",
											  dl_vmem_failure(status));

		return arrow::Status::OK();
	}

	static void
	Release(int64_t bytes)
	{
		if (bytes > 0)
			VmemTracker_ReleaseVmem(bytes);
	}

	template <typename Allocate>
	static arrow::Status
	TrackedAllocate(int64_t size, Allocate allocate)
	{
		arrow::Status status = Reserve(size);

		if (!status.ok())
			return status;

		status = allocate();
		if (!status.ok())
			Release(size);

		return status;
	}

	/*
	 * Growth is reserved before the move and shrinkage released after it, the
	 * same order gp_realloc() uses: a failed realloc then leaves the tracker
	 * where it was on both paths.
	 */
	template <typename Reallocate>
	static arrow::Status
	TrackedReallocate(int64_t old_size, int64_t new_size, Reallocate reallocate)
	{
		int64_t		growth = new_size - old_size;
		arrow::Status status = Reserve(growth);

		if (!status.ok())
			return status;

		status = reallocate();
		if (!status.ok())
		{
			Release(growth);
			return status;
		}

		Release(-growth);
		return status;
	}
};

}								/* namespace */

arrow::MemoryPool *
DlArrowMemoryPool(void)
{
	static DlTrackedMemoryPool pool(arrow::default_memory_pool());

	return &pool;
}
