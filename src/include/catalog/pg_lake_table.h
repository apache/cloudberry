/*-------------------------------------------------------------------------
 *
 * pg_lake_table.h
 *	  definition of the "lake table" system catalog (pg_lake_table)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
