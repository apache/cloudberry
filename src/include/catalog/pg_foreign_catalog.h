/*-------------------------------------------------------------------------
 *
 * pg_foreign_catalog.h
 *	  definition of the "foreign catalog" system catalog (pg_foreign_catalog)
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
