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
 * storagecmds.h
 *	  storage server/user_mapping creation/manipulation commands
 *
 * IDENTIFICATION
 *	  src/include/commands/storagecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGECMDS_H
#define STORAGECMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"


/* Flags for GetStorageServerExtended */
#define SSV_MISSING_OK	0x01

/* Helper for obtaining username for user mapping */
#define StorageMappingUserName(userid) \
	(OidIsValid(userid) ? GetUserNameFromId(userid, false) : "public")

typedef struct StorageServer
{
	Oid 		serverid;		/* storage server Oid */
	Oid 		owner;			/* storage server owner user Oid */
	char 		*servername;	/* name of the storage server */
	List 		*options;		/* srvoptions as DefElem list */
} StorageServer;

typedef struct StorageUserMapping
{
	Oid		umid;			/* Oid of storage user mapping */
	Oid		userid;			/* local user Oid */
	Oid		serverid;		/* storage server Oid */
	List	   *options;		/* useoptions as DefElem list */
} StorageUserMapping;

extern Oid get_storage_server_oid(const char *servername, bool missing_ok);
extern StorageServer *GetStorageServerExtended(Oid serverid, bits16 flags);
extern StorageServer *GetStorageServer(Oid serverid);
extern StorageServer *GetStorageServerByName(const char *srvname, bool missing_ok);
extern StorageUserMapping *GetStorageUserMapping(Oid userid, Oid serverid);
extern Datum transformStorageGenericOptions(Oid catalogId, Datum oldOptions, List *options);

#endif //STORAGECMDS_H
