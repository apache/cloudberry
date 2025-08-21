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
 * storagecmds.c
 *	  storage server/user_mapping creation/manipulation commands
 *
 * IDENTIFICATION
 *	  src/backend/commands/storagecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/gp_storage_server.h"
#include "catalog/gp_storage_user_mapping.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/storagecmds.h"
#include "executor/execdesc.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

Oid
get_storage_server_oid(const char *servername, bool missing_ok)
{
	Oid 		oid;

	oid = GetSysCacheOid1(STORAGESERVERNAME, Anum_gp_storage_server_oid,
					   		CStringGetDatum(servername));

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("server \"%s\" does not exist", servername)));
	return oid;
}

/*
 * GetStorageServerExtended - look up the storage server definition. If
 * flags uses FSV_MISSING_OK, return NULL if the object cannot be found
 * instead of raising an error.
 */
StorageServer *
GetStorageServerExtended(Oid serverid, bits16 flags)
{
	Form_gp_storage_server serverform;
	StorageServer *server;
	HeapTuple	tp;
	Datum		datum;
	bool		isnull;

	tp = SearchSysCache1(STORAGESERVEROID, ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		if ((flags & SSV_MISSING_OK) == 0)
			elog(ERROR, "cache lookup failed for storage server %u", serverid);
		return NULL;
	}

	serverform = (Form_gp_storage_server) GETSTRUCT(tp);

	server = (StorageServer *) palloc(sizeof(StorageServer));
	server->serverid = serverid;
	server->servername = pstrdup(NameStr(serverform->srvname));
	server->owner = serverform->srvowner;

	/* Extract the srvoptions */
	datum = SysCacheGetAttr(STORAGESERVEROID,
							tp,
							Anum_gp_storage_server_srvoptions,
							&isnull);
	if (isnull)
		server->options = NIL;
	else
		server->options = untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return server;
}

/*
 * GetStorageServer - look up the storage server definition.
 */
StorageServer *
GetStorageServer(Oid serverid)
{
	return GetStorageServerExtended(serverid, 0);
}

/*
 * GetStorageServerByName - look up the storage server definition by name.
 */
StorageServer *
GetStorageServerByName(const char *srvname, bool missing_ok)
{
	Oid			serverid = get_storage_server_oid(srvname, missing_ok);

	if (!OidIsValid(serverid))
		return NULL;

	return GetStorageServer(serverid);
}

/*
 * GetStorgeUserMapping	- look up the storage user mapping.
 *
 * If no mapping is found for the supplied user, we also look for
 * PUBLIC mappings (userid == InvalidOid).
 */
StorageUserMapping *
GetStorageUserMapping(Oid userid, Oid serverid)
{
	Datum		datum;
	HeapTuple	tp;
	bool		isnull;
	StorageUserMapping *um;

	tp = SearchSysCache2(STORAGEUSERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		/* Not found for the specific user -- try PUBLIC */
		tp = SearchSysCache2(STORAGEUSERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	}

	if (!HeapTupleIsValid(tp))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("storage user mapping not found for \"%s\"",
					    StorageMappingUserName(userid))));

	um = (StorageUserMapping *) palloc(sizeof(StorageUserMapping));
	um->umid = ((Form_gp_storage_user_mapping) GETSTRUCT(tp))->oid;
	um->userid = userid;
	um->serverid = serverid;

	/* Extract the umoptions */
	datum = SysCacheGetAttr(STORAGEUSERMAPPINGUSERSERVER,
							tp,
							Anum_gp_storage_user_mapping_umoptions,
							&isnull);
	if (isnull)
		um->options = NIL;
	else
		um->options = untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return um;
}


/*
 * Convert a DefElem list to the text array format that is used in
 * gp_storage_server, gp_storage_user_mapping.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * Note: The array is usually stored to database without further
 * processing, hence any validation should be done before this
 * conversion.
 */
static Datum
optionListToArray(List *options)
{
	ArrayBuildState *astate = NULL;
	ListCell   *cell;

	foreach(cell, options)
	{
		DefElem    *def = lfirst(cell);
		const char *value;
		Size		len;
		text	   *t;

		value = defGetString(def);
		len = VARHDRSZ + strlen(def->defname) + 1 + strlen(value);
		t = palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s=%s", def->defname, value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
								  false, TEXTOID,
								  CurrentMemoryContext);
	}

	if (astate)
		return makeArrayResult(astate, CurrentMemoryContext);

	return PointerGetDatum(NULL);
}

/*
 * Transform a list of DefElem into text array format.  This is substantially
 * the same thing as optionListToArray(), except we recognize SET/ADD/DROP
 * actions for modifying an existing list of options, which is passed in
 * Datum form as oldOptions.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * This is used by CREATE/ALTER of STORAGE SERVER/USER MAPPING
 */
Datum
transformStorageGenericOptions(Oid catalogId,
						Datum oldOptions,
						List *options)
{
	List	   *resultOptions = untransformRelOptions(oldOptions);
	ListCell   *optcell;
	Datum		result;

	foreach(optcell, options)
	{
		DefElem    *od = lfirst(optcell);
		ListCell   *cell;

		/*
		 * Find the element in resultOptions.  We need this for validation in
		 * all cases.
		 */
		foreach(cell, resultOptions)
		{
			DefElem    *def = lfirst(cell);

			if (strcmp(def->defname, od->defname) == 0)
				break;
		}

		/*
		 * It is possible to perform multiple SET/DROP actions on the same
		 * option.  The standard permits this, as long as the options to be
		 * added are unique.  Note that an unspecified action is taken to be
		 * ADD.
		 */
		switch (od->defaction)
		{
			case DEFELEM_DROP:
				if (!cell)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("option \"%s\" not found",
									   od->defname)));
				resultOptions = list_delete_cell(resultOptions, cell);
				break;

			case DEFELEM_SET:
				if (!cell)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("option \"%s\" not found",
									   od->defname)));
				lfirst(cell) = od;
				break;

			case DEFELEM_ADD:
			case DEFELEM_UNSPEC:
				if (cell)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("option \"%s\" provided more than once",
									   od->defname)));
				resultOptions = lappend(resultOptions, od);
				break;

			default:
				elog(ERROR, "unrecognized action %d on option \"%s\"",
					 (int) od->defaction, od->defname);
				break;
		}
	}

	result = optionListToArray(resultOptions);

	return result;
}

/*
 * Common routine to check permission for storage-user-mapping-related DDL
 * commands.  We allow server owners to operate on any mapping, and
 * users to operate on their own mapping.
 */
static void
storage_user_mapping_ddl_aclcheck(Oid umuserid, Oid serverid, const char *servername)
{
	Oid			curuserid = GetUserId();

	if (!gp_storage_server_ownercheck(serverid, curuserid))
	{
		if (umuserid == curuserid)
		{
			AclResult	aclresult;

			aclresult = object_aclcheck(StorageServerRelationId, serverid, curuserid, ACL_USAGE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_STORAGE_SERVER, servername);
		}
		else
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_STORAGE_SERVER,
						   servername);
	}
}

/*
 * Create a storage server
 */
ObjectAddress
CreateStorageServer(CreateStorageServerStmt *stmt)
{
	Relation 	rel;
	Datum 		srvoptions;
	Datum 		values[Natts_gp_storage_server];
	bool 		nulls[Natts_gp_storage_server];
	HeapTuple 	tuple;
	Oid 		srvId;
	Oid 		ownerId;
	ObjectAddress	myself = {0};

	rel = table_open(StorageServerRelationId, RowExclusiveLock);

	/* For now the owner cannot be specified on create. Use effective user ID. */
	ownerId = GetUserId();

	/*
	 * Check that there is no other storage server by this name. Do nothing if
	 * IF NOT EXISTS was enforced.
	 */
	if (GetStorageServerByName(stmt->servername, true) != NULL)
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("storage server \"%s\" already exists, skipping",
							stmt->servername)));
			table_close(rel, RowExclusiveLock);
			return InvalidObjectAddress;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("storage server \"%s\" already exists",
			 				stmt->servername)));
		}
	}

	/*
	 * Insert tuple into gp_storage_server.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	srvId = GetNewOidForStorageServer(rel, StorageServerOidIndexId,
								      Anum_gp_storage_server_oid,
								      stmt->servername);
	values[Anum_gp_storage_server_oid - 1] = ObjectIdGetDatum(srvId);
	values[Anum_gp_storage_server_srvname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->servername));
	values[Anum_gp_storage_server_srvowner -1] = ObjectIdGetDatum(ownerId);
	/* Start with a blank acl */
	nulls[Anum_gp_storage_server_srvacl - 1] = true;

	/* Add storage server options */
	srvoptions = transformStorageGenericOptions(StorageServerRelationId,
									  	 		PointerGetDatum(NULL),
									  	 		stmt->options);

	if (PointerIsValid(DatumGetPointer(srvoptions)))
		values[Anum_gp_storage_server_srvoptions - 1] = srvoptions;
	else
		nulls[Anum_gp_storage_server_srvoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	/* Post creation hook for new storage server */
	InvokeObjectPostCreateHook(StorageServerRelationId, srvId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
							  		DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
							  		GetAssignedOidsForDispatch(),
							  		NULL);
	}

	table_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Alter Storage Server
 */
ObjectAddress
AlterStorageServer(AlterStorageServerStmt *stmt)
{
	Relation	rel;
	HeapTuple 	tp;
	Datum 		repl_val[Natts_gp_storage_server];
	bool 		repl_null[Natts_gp_storage_server];
	bool 		repl_repl[Natts_gp_storage_server];
	Oid			srvId;
	Form_gp_storage_server srvForm;
	ObjectAddress	address = {0};

	rel = table_open(StorageServerRelationId, RowExclusiveLock);

	tp = SearchSysCacheCopy1(STORAGESERVERNAME,
						  		CStringGetDatum(stmt->servername));

	if (!HeapTupleIsValid(tp))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("storage server \"%s\" does not exist", stmt->servername)));

	srvForm = (Form_gp_storage_server) GETSTRUCT(tp);
	srvId = srvForm->oid;

	/*
	 * Only owner or a superuser can ALTER a STORAGE SERVER.
	 */
	if (!gp_storage_server_ownercheck(srvId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_STORAGE_SERVER,
				 	   stmt->servername);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (stmt->options)
	{
		Datum		datum;
		bool 		isnull;

		/* Extract the current srvoptions */
		datum = SysCacheGetAttr(STORAGESERVEROID,
						  		tp,
						  		Anum_gp_storage_server_srvoptions,
						  		&isnull);
		if (isnull)
			datum = PointerGetDatum(NULL);

		/* Prepare the options array */
		datum = transformStorageGenericOptions(StorageServerRelationId,
										 	   datum,
										 	   stmt->options);
		if (PointerIsValid(DatumGetPointer(datum)))
			repl_val[Anum_gp_storage_server_srvoptions - 1] = datum;
		else
			repl_null[Anum_gp_storage_server_srvoptions - 1] = true;

		repl_repl[Anum_gp_storage_server_srvoptions - 1] = true;
	}
	/* Everything looks good - update the tuple */
	tp = heap_modify_tuple(tp, RelationGetDescr(rel),
						   repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(rel, &tp->t_self, tp);

	InvokeObjectPostAlterHook(StorageServerRelationId, srvId, 0);

	ObjectAddressSet(address, StorageServerRelationId, srvId);

	heap_freetuple(tp);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	table_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Remove Storage Server
 */
Oid
RemoveStorageServer(DropStorageServerStmt *stmt)
{
	Relation	rel;
	Oid 		serverId;
	Oid 		srvOwnerId;
	Oid			curuserid;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple 	tuple;
	char		*detail;
	char		*detail_log;

	curuserid = GetUserId();

	rel = table_open(StorageServerRelationId, RowExclusiveLock);
	/*
	 * Check that if the storage server exists. Do nothing if IF NOT
	 * EXISTS was enforced.
	 */
	serverId = GetSysCacheOid1(STORAGESERVERNAME, Anum_gp_storage_server_oid,
						  CStringGetDatum(stmt->servername));

	if (!OidIsValid(serverId))
	{
		if (stmt->missing_ok)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("storage server \"%s\" not exists, skipping",
							stmt->servername)));
			table_close(rel, RowExclusiveLock);
			return InvalidOid;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("storage server \"%s\" not exists",
							stmt->servername)));
		}
	}

	srvOwnerId = GetSysCacheOid1(STORAGESERVERNAME, Anum_gp_storage_server_srvowner,
								 CStringGetDatum(stmt->servername));

	if (!gp_storage_server_ownercheck(serverId, curuserid))
	{
		if (srvOwnerId == curuserid)
		{
			AclResult	aclresult;

			aclresult = object_aclcheck(StorageServerRelationId, serverId, curuserid, ACL_USAGE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_STORAGE_SERVER, stmt->servername);
		}
		else
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_STORAGE_SERVER,
						   stmt->servername);
	}

	ScanKeyInit(&scankey,
			 	Anum_gp_storage_server_oid,
			 	BTEqualStrategyNumber, F_OIDEQ,
			 	ObjectIdGetDatum(serverId));
	sscan = systable_beginscan(rel, StorageServerOidIndexId,
							   true, NULL, 1, &scankey);

	tuple = systable_getnext(sscan);

	/*
	 * Lock the storage server, so nobody can add dependencies to her while we drop
	 * her. We keep the lock until the end of transaction.
	 */
	LockSharedObject(StorageServerRelationId, serverId, 0, AccessExclusiveLock);

	/* Check for pg_shdepend entries depending on this profile */
	if (checkSharedDependencies(StorageServerRelationId, serverId,
								&detail, &detail_log))
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					errmsg("storage server \"%s\" cannot be dropped because some objects depend on it",
						   stmt->servername),
					errdetail_internal("%s", detail),
					errdetail_log("%s", detail_log)));

	CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(sscan);
	table_close(rel, RowExclusiveLock);

	/*
	 * Drop hook for the role being removed
	 */
	InvokeObjectDropHook(StorageServerRelationId, serverId, 0);

	/*
	 * Delete shared dependency references related to this server object.
	 */
	deleteSharedDependencyRecordsFor(StorageServerRelationId, serverId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	return serverId;
}

/*
 * Create Storage User Mapping
 */
ObjectAddress
CreateStorageUserMapping(CreateStorageUserMappingStmt *stmt)
{
	Relation 	rel;
	Datum 		useoptions;
	Datum 		values[Natts_gp_storage_user_mapping];
	bool 		nulls[Natts_gp_storage_user_mapping];
	HeapTuple 	tuple;
	Oid			useId;
	Oid 		umId;
	StorageServer *srv;
	ObjectAddress myself;
	RoleSpec	*role = (RoleSpec *) stmt->user;

	rel = table_open(StorageUserMappingRelationId, RowExclusiveLock);

	if (role->roletype == ROLESPEC_PUBLIC)
		useId = ACL_ID_PUBLIC;
	else
		useId = get_rolespec_oid(stmt->user, false);

	/* Check that the server exists. */
	srv = GetStorageServerByName(stmt->servername, false);

	storage_user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

	/*
	 * Check that the user mapping is unique within server.
	 */
	umId = GetSysCacheOid2(STORAGEUSERMAPPINGUSERSERVER, Anum_gp_storage_user_mapping_oid,
						   ObjectIdGetDatum(useId),
						   ObjectIdGetDatum(srv->serverid));

	if (OidIsValid(umId))
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("storage user mapping for \"%s\" already exists for storage server \"%s\", skipping",
			 		 StorageMappingUserName(useId),
			 		 stmt->servername)));

			table_close(rel, RowExclusiveLock);
			return InvalidObjectAddress;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("storage user mapping for \"%s\" already exists for storage server \"%s\"",
			 		 StorageMappingUserName(useId),
			 		 stmt->servername)));
	}

	/*
	 * Insert tuple into gp_storage_user_mapping.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	umId = GetNewOidForStorageUserMapping(rel, StorageUserMappingOidIndexId,
									   	  Anum_gp_storage_user_mapping_oid,
									   	  useId, srv->serverid);
	values[Anum_gp_storage_user_mapping_oid -1] = ObjectIdGetDatum(umId);
	values[Anum_gp_storage_user_mapping_umuser -1] = ObjectIdGetDatum(useId);
	values[Anum_gp_storage_user_mapping_umserver - 1] = ObjectIdGetDatum(srv->serverid);

	/* Add user options */
	useoptions = transformStorageGenericOptions(StorageUserMappingRelationId,
											 	PointerGetDatum(NULL),
											 	stmt->options);

	if (PointerIsValid(DatumGetPointer(useoptions)))
		values[Anum_gp_storage_user_mapping_umoptions - 1] = useoptions;
	else
		nulls[Anum_gp_storage_user_mapping_umoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	myself.classId = StorageUserMappingRelationId;
	myself.objectId = umId;
	myself.objectSubId = 0;

	recordStorageServerDependency(StorageUserMappingRelationId, umId, srv->serverid);

	if (OidIsValid(useId))
	{
		/* Record the mapped user dependency */
		recordDependencyOnOwner(StorageUserMappingRelationId, umId, useId);
	}

	/*
	 * Perhaps someday there should be a recordDependencyOnCurrentExtension
	 * call here; but since roles aren't members of extensions, it seems like
	 * storage user mappings shouldn't be either.  Note that the grammar and pg_dump
	 * would need to be extended too if we change this.
	 */

	/* Post creation hook for new storage user mapping */
	InvokeObjectPostCreateHook(StorageUserMappingRelationId, umId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	table_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Alter storage user mapping
 */
ObjectAddress
AlterStorageUserMapping(AlterStorageUserMappingStmt *stmt)
{
	Relation 	rel;
	HeapTuple 	tp;
	Datum 		repl_val[Natts_gp_storage_user_mapping];
	bool		repl_null[Natts_gp_storage_user_mapping];
	bool		repl_repl[Natts_gp_storage_user_mapping];
	Oid 		useId;
	Oid			umId;
	StorageServer *srv;
	ObjectAddress address;
	RoleSpec	*role = (RoleSpec *) stmt->user;

	rel = table_open(StorageUserMappingRelationId, RowExclusiveLock);

	if (role->roletype == ROLESPEC_PUBLIC)
		useId = ACL_ID_PUBLIC;
	else
		useId = get_rolespec_oid(stmt->user, false);

	srv = GetStorageServerByName(stmt->servername, false);

	umId = GetSysCacheOid2(STORAGEUSERMAPPINGUSERSERVER, Anum_gp_storage_user_mapping_oid,
						   ObjectIdGetDatum(useId),
						   ObjectIdGetDatum(srv->serverid));

	if (!OidIsValid(umId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("storage user mapping for \"%s\" does not exist for server \"%s\"",
						StorageMappingUserName(useId), stmt->servername)));

	storage_user_mapping_ddl_aclcheck(useId, srv->serverid, stmt->servername);

	tp = SearchSysCacheCopy1(STORAGEUSERMAPPINGOID, ObjectIdGetDatum(umId));

	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for storage user mapping %u", umId);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	if (stmt->options)
	{
		Datum 	datum;
		bool	isnull;

		/*
		 * Process the options.
		 */
		datum = SysCacheGetAttr(STORAGEUSERMAPPINGUSERSERVER,
						  		tp,
						  		Anum_gp_storage_user_mapping_umoptions,
						  		&isnull);

		if (isnull)
			datum = PointerGetDatum(NULL);

		/* Prepare the options array */
		datum = transformStorageGenericOptions(StorageUserMappingRelationId,
										 	   datum,
										 	   stmt->options);

		if (PointerIsValid(DatumGetPointer(datum)))
			repl_val[Anum_gp_storage_user_mapping_umoptions - 1] = datum;
		else
			repl_null[Anum_gp_storage_user_mapping_umoptions - 1] = true;

		repl_repl[Anum_gp_storage_user_mapping_umoptions - 1] = true;
	}

	/* Everything looks good - update the tuple */
	tp = heap_modify_tuple(tp, RelationGetDescr(rel),
						   repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(rel, &tp->t_self, tp);

	InvokeObjectPostAlterHook(StorageUserMappingRelationId,
						   	  umId, 0);

	ObjectAddressSet(address, StorageUserMappingRelationId, umId);

	heap_freetuple(tp);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	table_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Drop storage user mapping
 */
Oid
RemoveStorageUserMapping(DropStorageUserMappingStmt *stmt)
{
	Oid 		useId;
	Oid 		umId;
	StorageServer *srv;
	RoleSpec	*role = (RoleSpec *) stmt->user;
	Relation	gp_storage_user_mapping_rel;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple 	tuple;

	if (role->roletype == ROLESPEC_PUBLIC)
		useId = ACL_ID_PUBLIC;
	else
	{
		useId = get_rolespec_oid(stmt->user, stmt->missing_ok);
		if (!OidIsValid(useId))
		{
			/*
			 * IF EXISTS specified, role not found and not public. Notice this
			 * and leave.
			 */
			elog(NOTICE, "role \"%s\" does not exist, skipping",
				 role->rolename);
			return InvalidOid;
		}
	}

	srv = GetStorageServerByName(stmt->servername, true);

	if (!srv)
	{
		if (!stmt->missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("storage server \"%s\" does not exist",
			 				stmt->servername)));
		/* IF EXISTS, just note it */
		ereport(NOTICE,
				(errmsg("storage server \"%s\" does not exist, skipping",
						stmt->servername)));
		return InvalidOid;
	}

	umId = GetSysCacheOid2(STORAGEUSERMAPPINGUSERSERVER, Anum_gp_storage_user_mapping_oid,
						   ObjectIdGetDatum(useId),
						   ObjectIdGetDatum(srv->serverid));

	if (!OidIsValid(umId))
	{
		if (!stmt->missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("storage user mapping for \"%s\" does not exist for storage server \"%s\"",
			 				StorageMappingUserName(useId), stmt->servername)));

		/* IF EXISTS specified, just note it */
		ereport(NOTICE,
				(errmsg("storage user mapping for \"%s\" does not exist for storage server \"%s\", skipping",
						StorageMappingUserName(useId), stmt->servername)));
		return InvalidOid;
	}

	storage_user_mapping_ddl_aclcheck(useId, srv->serverid, srv->servername);

	gp_storage_user_mapping_rel = table_open(StorageUserMappingRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
			 	Anum_gp_storage_user_mapping_oid,
			 	BTEqualStrategyNumber, F_OIDEQ,
			 	ObjectIdGetDatum(umId));
	sscan = systable_beginscan(gp_storage_user_mapping_rel, InvalidOid,
							   false, NULL, 1, &scankey);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
	{
		if (!stmt->missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role \"%s\" does not exist", role->rolename)));
		}
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			ereport(NOTICE,
					(errmsg("role \"%s\" does not exist, skipping",
							role->rolename)));
		}
	}

	CatalogTupleDelete(gp_storage_user_mapping_rel, &tuple->t_self);

	systable_endscan(sscan);
	table_close(gp_storage_user_mapping_rel, RowExclusiveLock);

	/* DROP hook for the role being removed */
	InvokeObjectDropHook(StorageUserMappingRelationId, umId, 0);

	/*
	 * Delete shared dependency references related to this role object.
	 */
	deleteSharedDependencyRecordsFor(StorageUserMappingRelationId, umId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}
	return umId;
}
