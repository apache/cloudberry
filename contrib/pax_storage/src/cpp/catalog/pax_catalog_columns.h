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
 * pax_catalog_columns.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_catalog_columns.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#define PAX_AUX_PTBLOCKNAME       "ptblockname"
#define PAX_AUX_PTTUPCOUNT        "pttupcount"
#define PAX_AUX_PTBLOCKSIZE       "ptblocksize"
#define PAX_AUX_PTSTATISITICS     "ptstatistics"
#define PAX_AUX_PTVISIMAPNAME     "ptvisimapname"
#define PAX_AUX_PTEXISTEXTTOAST   "ptexistexttoast"
#define PAX_AUX_PTISCLUSTERED     "ptisclustered"


#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME 1
#define ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT 2
#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE 3
#define ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS 4
#define ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME 5
#define ANUM_PG_PAX_BLOCK_TABLES_PTEXISTEXTTOAST 6
#define ANUM_PG_PAX_BLOCK_TABLES_PTISCLUSTERED 7
#define NATTS_PG_PAX_BLOCK_TABLES 7