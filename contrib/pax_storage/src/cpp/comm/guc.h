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
 * guc.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/guc.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace pax {
extern bool pax_enable_debug;
extern bool pax_enable_sparse_filter;
extern bool pax_enable_row_filter;
extern int pax_scan_reuse_buffer_size;
extern int pax_max_tuples_per_group;

extern int pax_max_tuples_per_file;
extern int pax_max_size_per_file;

extern bool pax_enable_toast;
extern int pax_min_size_of_compress_toast;
extern int pax_min_size_of_external_toast;

extern char *pax_default_storage_format;
extern int pax_bloom_filter_work_memory_bytes;
extern bool pax_log_filter_tree;
}  // namespace pax

namespace paxc {
extern void DefineGUCs();
}
