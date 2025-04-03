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
 * pg_pax_tables.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pg_pax_tables.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#define NATTS_PG_PAX_TABLES 2
#define ANUM_PG_PAX_TABLES_RELID 1
#define ANUM_PG_PAX_TABLES_AUXRELID 2

namespace paxc {

void InsertPaxTablesEntry(Oid relid, Oid blocksrelid);

void GetPaxTablesEntryAttributes(Oid relid, Oid *blocksrelid);

static inline Oid GetPaxAuxRelid(Oid pax_relid) {
  Oid aux_relid;
  GetPaxTablesEntryAttributes(pax_relid, &aux_relid);
  return aux_relid;
}

}  // namespace paxc
