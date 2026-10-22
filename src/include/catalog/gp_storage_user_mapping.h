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
 * gp_storage_user_mapping.h
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_storage_user_mapping.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_STORAGE_USER_MAPPING_H
#define GP_STORAGE_USER_MAPPING_H

#include "catalog/genbki.h"
#include "catalog/gp_storage_user_mapping_d.h"

/* ----------------
 *		gp_storage_user_mapping definition.  cpp turns this into
 *		typedef struct FormData_gp_storage_user_mapping
 * ----------------
 */
CATALOG(gp_storage_user_mapping,6131,StorageUserMappingRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */

	Oid			umuser BKI_LOOKUP_OPT(pg_authid);	/* Id of the user,
													 * InvalidOid if PUBLIC is
													 * wanted */
	Oid			umserver BKI_LOOKUP(gp_storage_server); /* server of this
														 * mapping */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		umoptions[1];	/* user mapping options */
#endif
} FormData_gp_storage_user_mapping;

/* ----------------
 *		Form_gp_storage_user_mapping corresponds to a pointer to a tuple with
 *		the format of gp_storage_user_mapping relation.
 * ----------------
 */
typedef FormData_gp_storage_user_mapping *Form_gp_storage_user_mapping;

DECLARE_TOAST(gp_storage_user_mapping, 6132, 6133);
#define GpStorageUserMappingToastTable	6132
#define GpStorageUserMappingToastIndex	6133

DECLARE_UNIQUE_INDEX_PKEY(gp_storage_user_mapping_oid_index, 6134, StorageUserMappingOidIndexId, on gp_storage_user_mapping using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(gp_storage_user_mapping_server_index, 6135, StorageUserMappingServerIndexId, on gp_storage_user_mapping using btree(umuser oid_ops, umserver oid_ops));

#endif //GP_STORAGE_USER_MAPPING_H
