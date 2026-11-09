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
 * arrow_memory_pool.h
 *	  The allocator every Arrow allocation in this module goes through.
 *
 * Arrow allocates from its own pool, not from palloc, so nothing it holds is
 * visible to the server's memory accounting on its own: a writer buffering a
 * row group, or a reader decoding one, could push a segment past its limit
 * while looking small to the resource manager.  This pool reserves every byte
 * with the vmem tracker before handing it out, so statement_mem, the resource
 * group and gp_vmem_protect_limit see Arrow's memory as they see palloc's.
 *
 * Every place that would name arrow::default_memory_pool() names this instead
 * -- including the two that would otherwise get it by default, the Parquet
 * ReaderProperties and the WriterProperties builder.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_memory_pool.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ARROW_MEMORY_POOL_H
#define DL_ARROW_MEMORY_POOL_H

#include <arrow/memory_pool.h>

/*
 * The one pool.  It wraps Arrow's default and only adds the accounting, so
 * what it hands out is aligned and freed exactly as the default's would be.
 * Only for the backend's own thread: the vmem tracker is not thread-safe, which
 * is one more reason the readers here never let Arrow use a thread pool.
 */
extern arrow::MemoryPool *DlArrowMemoryPool(void);

#endif							/* DL_ARROW_MEMORY_POOL_H */
