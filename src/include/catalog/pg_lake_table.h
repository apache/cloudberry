/*-------------------------------------------------------------------------
 *
 * pg_lake_table.h
 *	  definition of the "lake table" system catalog (pg_lake_table)
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
 * src/include/catalog/pg_lake_table.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LAKE_TABLE_H
#define PG_LAKE_TABLE_H

#include "catalog/genbki.h"
#include "catalog/pg_lake_table_d.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_lake_table definition.  cpp turns this into
 *		typedef struct FormData_pg_lake_table
 * ----------------
 */
CATALOG(pg_lake_table,9901,LakeTableRelationId)
{
	Oid		ltrelid BKI_LOOKUP(pg_class);				/* OID of the lake table relation */
	Oid		ltforeign_catalog BKI_LOOKUP_OPT(pg_foreign_catalog);	/* OID of foreign catalog */
	Oid		ltforeign_volume BKI_LOOKUP_OPT(pg_foreign_volume);		/* OID of foreign volume */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text	lttable_type;		/* table type: ICEBERG, etc. */
	text	ltoptions[1];		/* lake table options */
#endif
} FormData_pg_lake_table;

/* ----------------
 *		Form_pg_lake_table corresponds to a pointer to a tuple with
 *		the format of pg_lake_table relation.
 * ----------------
 */
typedef FormData_pg_lake_table *Form_pg_lake_table;

DECLARE_TOAST(pg_lake_table, 9903, 9904);

DECLARE_UNIQUE_INDEX_PKEY(pg_lake_table_relid_index, 9902, LakeTableRelidIndexId, on pg_lake_table using btree(ltrelid oid_ops));

/* ----------------
 *		Lake table structure for caching
 * ----------------
 */
typedef struct LakeTable
{
	Oid			relid;				/* OID of the lake table relation */
	char	   *table_type;			/* table type: ICEBERG, etc. */
	char	   *foreign_catalog;	/* foreign catalog name */
	char	   *foreign_volume;		/* foreign volume name */
	List	   *options;			/* lake table options */
} LakeTable;

#endif							/* PG_LAKE_TABLE_H */
