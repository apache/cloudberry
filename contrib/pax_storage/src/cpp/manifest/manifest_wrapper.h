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
 * manifest_wrapper.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/manifest/manifest_wrapper.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include "postgres.h"
#include "utils/rel.h"

extern bool paxc_need_wal(Relation rel);
extern char *paxc_get_pax_dir(RelFileNode rnode, BackendId backend);
extern void paxc_create_pax_directory(Relation rel, RelFileNode newrnode);
extern void paxc_store_file(const char *filename, const void *data, size_t size);

extern void paxc_read_all(const char *filename, void (*func)(const void *ptr, size_t size, void *opaque), void *opaque);

extern void paxc_wal_insert_if_required(Relation rel, const char *filename, const void *data, size_t size, int64 offset);
extern void paxc_wal_create_directory(RelFileNode node);
extern void paxc_wal_truncate_directory(RelFileNode node);

#ifdef __cplusplus
}
#endif
