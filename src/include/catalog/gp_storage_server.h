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
 * example.c (option)
 *	  use for one example of how to add a licenser header (option)
 *
 * IDENTIFICATION (option)
 *	  src/timezone/zic.c (option)
 *
 * gp_storage_server.h
 *
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_storage_server.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_STORAGE_SERVER_H
#define GP_STORAGE_SERVER_H

#include "catalog/genbki.h"
#include "catalog/gp_storage_server_d.h"

/* ----------------
 *		gp_storage_server definition.  cpp turns this into
 *		typedef struct FormData_gp_storage_server
 * ----------------
 */
CATALOG(gp_storage_server,6015,StorageServerRelationId) BKI_SHARED_RELATION
{
	Oid		oid;			/* oid */
	NameData	srvname;		/* storage server name */
	Oid		srvowner BKI_LOOKUP(pg_authid); /* server owner */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	aclitem		srvacl[1];		/* access permissions */
	text		srvoptions[1];	/* FDW-specific options */
#endif
} FormData_gp_storage_server;

/* ----------------
 *		Form_gp_storage_server corresponds to a pointer to a tuple with
 *		the format of gp_storage_server relation.
 * ----------------
 */
typedef FormData_gp_storage_server *Form_gp_storage_server;

DECLARE_TOAST(gp_storage_server, 6016, 6017);
#define GpStorageServerToastTable	6016
#define GpStorageServerToastIndex	6017

DECLARE_UNIQUE_INDEX_PKEY(gp_storage_server_oid_index, 6018, StorageServerOidIndexId, on gp_storage_server using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(gp_storage_server_name_index, 6019, StorageServerNameIndexId, on gp_storage_server using btree(srvname name_ops));

#endif //GP_STORAGE_SERVER_H
