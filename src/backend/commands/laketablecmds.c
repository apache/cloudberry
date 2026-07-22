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
 * laketablecmds.c
 *	  lake table creation/manipulation commands
 *
 * IDENTIFICATION
 *	  src/backend/commands/laketablecmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_foreign_catalog.h"
#include "catalog/pg_foreign_volume.h"
#include "catalog/pg_lake_table.h"
#include "commands/defrem.h"
#include "commands/laketablecmds.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

/* GUC variables for default Iceberg catalog and volume */
char	   *iceberg_default_catalog = NULL;
char	   *iceberg_default_volume = NULL;

/*
 * check_iceberg_default_catalog: validate new iceberg_default_catalog GUC value
 */
bool
check_iceberg_default_catalog(char **newval, void **extra, GucSource source)
{
	/*
	 * If we aren't inside a transaction, or connected to a database, we
	 * cannot do the catalog accesses necessary to verify the name.  Must
	 * accept the value on faith.
	 */
	if (IsTransactionState() && MyDatabaseId != InvalidOid)
	{
		if (**newval != '\0')
		{
			Oid			catalog_oid = get_foreign_catalog_oid(*newval, true);

			if (!OidIsValid(catalog_oid))
			{
				/*
				 * When source == PGC_S_TEST, don't throw a hard error for a
				 * nonexistent catalog, only a NOTICE.  See comments in guc.h.
				 */
				if (source == PGC_S_TEST)
				{
					ereport(NOTICE,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("foreign catalog \"%s\" does not exist",
									*newval)));
				}
				else
				{
					GUC_check_errdetail("Foreign catalog \"%s\" does not exist.",
										*newval);
					return false;
				}
			}
		}
	}

	return true;
}

/*
 * check_iceberg_default_volume: validate new iceberg_default_volume GUC value
 */
bool
check_iceberg_default_volume(char **newval, void **extra, GucSource source)
{
	/*
	 * If we aren't inside a transaction, or connected to a database, we
	 * cannot do the catalog accesses necessary to verify the name.  Must
	 * accept the value on faith.
	 */
	if (IsTransactionState() && MyDatabaseId != InvalidOid)
	{
		if (**newval != '\0')
		{
			Oid			volume_oid = get_foreign_volume_oid(*newval, true);

			if (!OidIsValid(volume_oid))
			{
				/*
				 * When source == PGC_S_TEST, don't throw a hard error for a
				 * nonexistent volume, only a NOTICE.  See comments in guc.h.
				 */
				if (source == PGC_S_TEST)
				{
					ereport(NOTICE,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("foreign volume \"%s\" does not exist",
									*newval)));
				}
				else
				{
					GUC_check_errdetail("Foreign volume \"%s\" does not exist.",
										*newval);
					return false;
				}
			}
		}
	}

	return true;
}

/*
 * GetDefaultIcebergCatalog -- get the name of the current default Iceberg catalog
 *
 * Returns NULL if no default catalog is set.
 * This function hides the iceberg_default_catalog GUC variable.
 */
const char *
GetDefaultIcebergCatalog(void)
{
	if (iceberg_default_catalog == NULL || iceberg_default_catalog[0] == '\0')
		return NULL;

	/*
	 * Verify that the catalog still exists.  We don't cache this because
	 * the catalog could be dropped after the GUC was set.
	 */
	if (!OidIsValid(get_foreign_catalog_oid(iceberg_default_catalog, true)))
		return NULL;

	return iceberg_default_catalog;
}

/*
 * GetDefaultIcebergVolume -- get the name of the current default Iceberg volume
 *
 * Returns NULL if no default volume is set.
 * This function hides the iceberg_default_volume GUC variable.
 */
const char *
GetDefaultIcebergVolume(void)
{
	if (iceberg_default_volume == NULL || iceberg_default_volume[0] == '\0')
		return NULL;

	/*
	 * Verify that the volume still exists.  We don't cache this because
	 * the volume could be dropped after the GUC was set.
	 */
	if (!OidIsValid(get_foreign_volume_oid(iceberg_default_volume, true)))
		return NULL;

	return iceberg_default_volume;
}

/*
 * GetIcebergTableAmOid
 *
 * Look up the OID of the iceberg table access method, which is provided by
 * a datalake extension rather than the kernel.  Returns InvalidOid if the
 * access method is not installed and missing_ok is true.
 */
Oid
GetIcebergTableAmOid(bool missing_ok)
{
	return get_table_am_oid(ICEBERG_TABLE_AM_NAME, missing_ok);
}

/*
 * RelationIsLakeTable
 *
 * True iff the relation has a pg_lake_table entry, i.e. it was created by
 * CREATE LAKE TABLE.  Lake tables are told apart from ordinary relations by
 * this catalog membership rather than by their access method.
 */
bool
RelationIsLakeTable(Relation rel)
{
	Relation	ltRel;
	ScanKeyData skey;
	SysScanDesc scan;
	bool		found;

	ltRel = table_open(LakeTableRelationId, AccessShareLock);
	ScanKeyInit(&skey,
				Anum_pg_lake_table_ltrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));
	scan = systable_beginscan(ltRel, LakeTableRelidIndexId, true, NULL, 1, &skey);
	found = HeapTupleIsValid(systable_getnext(scan));
	systable_endscan(scan);
	table_close(ltRel, AccessShareLock);

	return found;
}

/*
 * Validate table type
 */
static void
validate_table_type(const char *table_type)
{
	if (!table_type)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lake table format cannot be NULL")));

	if (strcmp(table_type, ICEBERG_TABLE_AM_NAME) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported lake table format \"%s\"", table_type),
				 errhint("The only supported format is ICEBERG (USING ICEBERG).")));
}

/*
 * Validate foreign catalog exists
 */
static Oid
validate_foreign_catalog(const char *catalog_name)
{
	if (!catalog_name || catalog_name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no foreign catalog specified"),
				 errhint("Specify CATALOG in CREATE LAKE TABLE or set iceberg_default_catalog.")));

	return get_foreign_catalog_oid(catalog_name, false);
}

/*
 * Validate foreign volume exists
 */
static Oid
validate_foreign_volume(const char *volume_name)
{
	if (!volume_name || volume_name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no foreign volume specified"),
				 errhint("Specify VOLUME in CREATE LAKE TABLE or set iceberg_default_volume.")));

	return get_foreign_volume_oid(volume_name, false);
}

/*
 * ResolveLakeTableOptions
 *
 * Resolve and validate the table type, catalog and volume of a
 * CreateLakeTableStmt, returning the catalog/volume OIDs.
 *
 * Also exposed (via ValidateLakeTableStmt) so ProcessUtilitySlow can run
 * the validation on the QD before DefineRelation: DefineRelation dispatches
 * the statement to the QEs, so a validation failure raised only inside
 * CreateLakeTable() would surface as a confusing QE-annotated error.
 */
static void
ResolveLakeTableOptions(CreateLakeTableStmt *stmt,
						Oid *catalog_oid_out, Oid *volume_oid_out)
{
	const char *catalog_name;
	const char *volume_name;

	/* Validate the table format named in the USING clause first, so an
	 * unsupported format is reported before anything else. */
	validate_table_type(stmt->table_type);

	/*
	 * The format is implemented by a like-named table access method that a
	 * datalake extension provides; a lake table is unusable without it, so
	 * check it here (after the format) so the install hint takes precedence
	 * over catalog/volume resolution errors.
	 */
	if (!OidIsValid(get_table_am_oid(stmt->table_type, true)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table access method \"%s\" does not exist",
						stmt->table_type),
				 errhint("CREATE LAKE TABLE ... USING ICEBERG requires an extension that provides the \"%s\" table access method.",
						 stmt->table_type)));

	/*
	 * Determine catalog name: use explicit value if provided, otherwise
	 * fall back to the iceberg_default_catalog GUC.  When the GUC is set
	 * but its catalog has been dropped, say so instead of the generic
	 * "no foreign catalog specified".
	 */
	catalog_name = stmt->foreign_catalog;
	if (catalog_name == NULL || catalog_name[0] == '\0')
	{
		catalog_name = GetDefaultIcebergCatalog();
		if (catalog_name == NULL &&
			iceberg_default_catalog != NULL && iceberg_default_catalog[0] != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("default iceberg catalog \"%s\" does not exist",
							iceberg_default_catalog),
					 errhint("Set iceberg_default_catalog to an existing foreign catalog.")));
	}

	/*
	 * Determine volume name: use explicit value if provided, otherwise
	 * fall back to the iceberg_default_volume GUC.
	 */
	volume_name = stmt->foreign_volume;
	if (volume_name == NULL || volume_name[0] == '\0')
	{
		volume_name = GetDefaultIcebergVolume();
		if (volume_name == NULL &&
			iceberg_default_volume != NULL && iceberg_default_volume[0] != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("default iceberg volume \"%s\" does not exist",
							iceberg_default_volume),
					 errhint("Set iceberg_default_volume to an existing foreign volume.")));
	}

	*catalog_oid_out = validate_foreign_catalog(catalog_name);

	/*
	 * A volume is required for every lake table, even when the catalog
	 * vends the table's physical location: the QEs still read and write
	 * the data files through the volume's storage endpoint and
	 * credentials.  Without this check the missing volume only surfaces
	 * later, deep in the access method's create path.
	 */
	*volume_oid_out = validate_foreign_volume(volume_name);
}

/*
 * ValidateLakeTableStmt
 *
 * QD-side pre-DefineRelation validation wrapper; see ResolveLakeTableOptions.
 */
void
ValidateLakeTableStmt(CreateLakeTableStmt *stmt)
{
	Oid			catalog_oid;
	Oid			volume_oid;

	ResolveLakeTableOptions(stmt, &catalog_oid, &volume_oid);
}

/*
 * CreateLakeTable
 *
 * Create a lake table entry in pg_lake_table after the base table has been
 * created.
 */
void
CreateLakeTable(CreateLakeTableStmt *stmt, Oid relId)
{
	Relation	lake_rel;
	Datum		values[Natts_pg_lake_table];
	bool		nulls[Natts_pg_lake_table];
	HeapTuple	tuple;
	Oid			catalog_oid;
	Oid			volume_oid;
	ObjectAddress myself;
	ObjectAddress referenced;

	ResolveLakeTableOptions(stmt, &catalog_oid, &volume_oid);

	/*
	 * Make the just-created base relation (from DefineRelation) visible to
	 * this command before we record dependencies on it.
	 */
	CommandCounterIncrement();

	lake_rel = table_open(LakeTableRelationId, RowExclusiveLock);

	/*
	 * Insert tuple into pg_lake_table.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_lake_table_ltrelid - 1] = ObjectIdGetDatum(relId);
	values[Anum_pg_lake_table_ltforeign_catalog - 1] = ObjectIdGetDatum(catalog_oid);
	values[Anum_pg_lake_table_ltforeign_volume - 1] = ObjectIdGetDatum(volume_oid);

	tuple = heap_form_tuple(lake_rel->rd_att, values, nulls);

	CatalogTupleInsert(lake_rel, tuple);

	/* Record dependencies on the foreign catalog and volume */
	myself.classId = RelationRelationId;
	myself.objectId = relId;
	myself.objectSubId = 0;

	referenced.classId = ForeignCatalogRelationId;
	referenced.objectId = catalog_oid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	referenced.classId = ForeignVolumeRelationId;
	referenced.objectId = volume_oid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	heap_freetuple(tuple);
	table_close(lake_rel, RowExclusiveLock);

	CommandCounterIncrement();
	InvokeObjectPostCreateHook(LakeTableRelationId, relId, 0);
}

/*
 * RemoveLakeTableEntry
 *
 * Remove the pg_lake_table entry for the given relation.
 */
void
RemoveLakeTableEntry(Oid relid)
{
	Relation	ltRel;
	HeapTuple	tup;
	ScanKeyData skey;
	SysScanDesc scan;

	ltRel = table_open(LakeTableRelationId, RowExclusiveLock);
	ScanKeyInit(&skey,
				Anum_pg_lake_table_ltrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(ltRel, LakeTableRelidIndexId, true, NULL, 1, &skey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
		CatalogTupleDelete(ltRel, &tup->t_self);
	systable_endscan(scan);
	table_close(ltRel, RowExclusiveLock);
}
