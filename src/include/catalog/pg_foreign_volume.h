/*-------------------------------------------------------------------------
 *
 * pg_foreign_volume.h
 *	  definition of the "foreign volume" system catalog (pg_foreign_volume)
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
 * src/include/catalog/pg_foreign_volume.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FOREIGN_VOLUME_H
#define PG_FOREIGN_VOLUME_H

#include "catalog/genbki.h"
#include "catalog/pg_foreign_volume_d.h"

/* ----------------
 *		pg_foreign_volume definition.  cpp turns this into
 *		typedef struct FormData_pg_foreign_volume
 * ----------------
 */
CATALOG(pg_foreign_volume,8554,ForeignVolumeRelationId)
{
	Oid			oid;			/* oid */

	NameData	fvname;			/* foreign volume name */

	Oid			fvowner BKI_LOOKUP(pg_authid);	/* owner of the foreign volume */

	Oid			fvserver BKI_LOOKUP(pg_foreign_server);	/* foreign server this volume belongs to */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		fvoptions[1];	/* foreign volume options */
#endif
} FormData_pg_foreign_volume;

/* ----------------
 *		Form_pg_foreign_volume corresponds to a pointer to a tuple with
 *		the format of pg_foreign_volume relation.
 * ----------------
 */
typedef FormData_pg_foreign_volume *Form_pg_foreign_volume;

DECLARE_TOAST(pg_foreign_volume, 8555, 8556);

DECLARE_UNIQUE_INDEX_PKEY(pg_foreign_volume_oid_index, 8557, ForeignVolumeOidIndexId, on pg_foreign_volume using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_foreign_volume_name_index, 8558, ForeignVolumeNameIndexId, on pg_foreign_volume using btree(fvname name_ops));

#endif							/* PG_FOREIGN_VOLUME_H */
