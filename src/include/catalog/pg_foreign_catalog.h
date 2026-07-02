/*-------------------------------------------------------------------------
 *
 * pg_foreign_catalog.h
 *	  definition of the "foreign catalog" system catalog (pg_foreign_catalog)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_foreign_catalog.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FOREIGN_CATALOG_H
#define PG_FOREIGN_CATALOG_H

#include "catalog/genbki.h"
#include "catalog/pg_foreign_catalog_d.h"

/* ----------------
 *		pg_foreign_catalog definition.  cpp turns this into
 *		typedef struct FormData_pg_foreign_catalog
 * ----------------
 */
CATALOG(pg_foreign_catalog,8549,ForeignCatalogRelationId)
{
	Oid			oid;			/* oid */

	NameData	fcname;			/* foreign catalog name */

	Oid			fcowner BKI_LOOKUP(pg_authid);	/* owner of the foreign catalog */

	Oid			fcserver BKI_LOOKUP(pg_foreign_server);	/* foreign server this catalog belongs to */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		fcoptions[1];	/* foreign catalog options */
#endif
} FormData_pg_foreign_catalog;

/* ----------------
 *		Form_pg_foreign_catalog corresponds to a pointer to a tuple with
 *		the format of pg_foreign_catalog relation.
 * ----------------
 */
typedef FormData_pg_foreign_catalog *Form_pg_foreign_catalog;

DECLARE_TOAST(pg_foreign_catalog, 8550, 8551);

DECLARE_UNIQUE_INDEX_PKEY(pg_foreign_catalog_oid_index, 8552, ForeignCatalogOidIndexId, on pg_foreign_catalog using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_foreign_catalog_name_index, 8553, ForeignCatalogNameIndexId, on pg_foreign_catalog using btree(fcname name_ops));

#endif							/* PG_FOREIGN_CATALOG_H */
