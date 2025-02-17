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
 * pg_profile.c
 *	  routines to support manipulation of the pg_profile relation
 *
 * IDENTIFICATION
 *		src/backend/command/pg_profile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_profile.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "catalog/objectaccess.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

PG_FUNCTION_INFO_V1(get_role_status);

char *
ProfileGetNameByOid(Oid profileOid, bool noerr)
{
	char		*prfnamestr;
	Relation	rel;
	ScanKeyData	skey;
	SysScanDesc	scan;
	HeapTuple	tup;
	Form_pg_profile profile;

	rel = table_open(ProfileRelationId, AccessShareLock);

	/*
	 * Search pg_profile.
	 */
	ScanKeyInit(&skey,
		    Anum_pg_profile_oid,
		    BTEqualStrategyNumber, F_OIDEQ,
		    ObjectIdGetDatum(profileOid));
	scan = systable_beginscan(rel, ProfileOidIndexId, true,
				  			NULL, 1, &skey);
	tup = systable_getnext(scan);
	if (!HeapTupleIsValid(tup))
	{
		if (!noerr)
			elog(ERROR, "profile %u could not be found", profileOid);

		prfnamestr = NULL;
	}
	else
	{
		profile = (Form_pg_profile) GETSTRUCT(tup);

		prfnamestr = pstrdup(profile->prfname.data);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return prfnamestr;
}

/*
 * get_profile_oid - Given a profile name, look up the profile's OID.
 *
 * If missing_ok is false, throw an error if profile name not found.  If
 * true, just return InvalidOid.
 */
Oid get_profile_oid(const char* prfname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(PROFILENAME, Anum_pg_profile_oid,
			      CStringGetDatum(prfname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
			 	 errmsg("profile \"%s\" does not exist", prfname)));
	return oid;
}

/*
 * rename_profile -
 *	 change the name of a profile
 */
ObjectAddress
rename_profile(char *oldname, char *newname)
{
	HeapTuple	oldtuple,
				newtuple;
	TupleDesc	dsc;
	Relation	rel;
	Datum		repl_val[Natts_pg_profile];
	bool		repl_null[Natts_pg_profile];
	bool		repl_repl[Natts_pg_profile];
	Oid		profileid;
	ObjectAddress	address;
	Form_pg_profile	profileform;

	/* Only when enable_password_profile is true, can RENAME PROFILE. */
	if (!enable_password_profile)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),
				 errmsg("can't RENAME PROFILE for enable_password_profile is not open")));

	/* Only super user can RENMAE PROFILE. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to rename profile, must be superuser")));

	/* pg_default profile can't be renamed */
	if (strcmp(oldname, "pg_default") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("can't RENAME \"pg_default\" profile")));

	rel = table_open(ProfileRelationId, RowExclusiveLock);
	dsc = RelationGetDescr(rel);

	oldtuple = SearchSysCache1(PROFILENAME, CStringGetDatum(oldname));
	if (!HeapTupleIsValid(oldtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("profile \"%s\" does not exist", oldname)));

	profileform = (Form_pg_profile) GETSTRUCT(oldtuple);
	profileid = profileform->oid;

	/* make sure the new name doesn't exist */
	if (SearchSysCacheExists1(PROFILENAME, CStringGetDatum(newname)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("profile \"%s\" already exists", newname)));

	/* Build up updated catalog tuple */
	MemSet(repl_repl, false, sizeof(repl_repl));

	repl_repl[Anum_pg_profile_prfname - 1] = true;
	repl_val[Anum_pg_profile_prfname - 1] = DirectFunctionCall1(namein,CStringGetDatum(newname));
	repl_null[Anum_pg_profile_prfname - 1] = false;

	newtuple = heap_modify_tuple(oldtuple, dsc, repl_val, repl_null, repl_repl);
	CatalogTupleUpdate(rel, &oldtuple->t_self, newtuple);

	InvokeObjectPostAlterHook(ProfileRelationId, profileid, 0);

	ObjectAddressSet(address, ProfileRelationId, profileid);

	ReleaseSysCache(oldtuple);

	/*
	 * Close pg_profile, keep lock until transaction commit.
	 */
	table_close(rel, NoLock);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(ProfileRelationId,
				   			profileid,
				   			GetUserId(),
				   			"ALTER", "RENAME"
				);

	return address;
}

/*
 * CREATE PROFILE
 */
Oid
CreateProfile(ParseState *pstate, CreateProfileStmt *stmt)
{
	Relation	pg_profile_rel;
	TupleDesc	pg_profile_dsc;
	HeapTuple	tuple;
	Datum		new_record[Natts_pg_profile];
	bool		new_record_nulls[Natts_pg_profile];
	Oid		profileid;
	ListCell	*option;
	int		failed_login_attempts = -1;
	int		password_lock_time = -1;
	int		password_life_time = -1;
	int 		password_grace_time = -1;
	int		password_reuse_time = -1;
	int		password_reuse_max = -1;
	int		password_allow_hashed = -1;
	DefElem		*dfailedloginattempts = NULL;
	DefElem		*dpasswordlocktime = NULL;
	DefElem		*dpasswordreusemax = NULL;

	/* Only when enable_password_profile is true, can CREATE PROFILE. */
	if (!enable_password_profile)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),
				 errmsg("can't CREATE PROFILE for enable_password_profile is not open")));

	/* Only super user can CREATE PROFILE. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 	 errmsg("permission denied to create profile, must be superuser")));

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem	*defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "failed_login_attempts") == 0)
		{
			if (dfailedloginattempts)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dfailedloginattempts = defel;
		}
		else if (strcmp(defel->defname, "password_lock_time") == 0)
		{
			if (dpasswordlocktime)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dpasswordlocktime = defel;
		}
		else if (strcmp(defel->defname, "password_reuse_max") == 0)
		{
			if (dpasswordreusemax)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dpasswordreusemax = defel;
		}
	}

	if (dfailedloginattempts && dfailedloginattempts->arg)
	{
		failed_login_attempts = intVal(dfailedloginattempts->arg);
		if (failed_login_attempts == 0 ||
			failed_login_attempts < PROFILE_UNLIMITED ||
			failed_login_attempts > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid failed login attempts: %d", failed_login_attempts)));
	}

	if (dpasswordlocktime && dpasswordlocktime->arg)
	{
		password_lock_time = intVal(dpasswordlocktime->arg);
		if (password_lock_time < PROFILE_UNLIMITED ||
			password_lock_time > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid password lock time: %d", password_lock_time)));
	}

	if (dpasswordreusemax && dpasswordreusemax->arg)
	{
		password_reuse_max = intVal(dpasswordreusemax->arg);
		if (password_reuse_max < PROFILE_UNLIMITED ||
			password_reuse_max > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid password reuse max: %d", password_reuse_max)));
	}

	/*
	 * Check the pg_profile relation to be certain the profile doesn't already
	 * exist.
	 */
	pg_profile_rel = table_open(ProfileRelationId, RowExclusiveLock);
	pg_profile_dsc = RelationGetDescr(pg_profile_rel);

	if (OidIsValid(get_profile_oid(stmt->profile_name, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("profile \"%s\" already exists",
	    						stmt->profile_name)));

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_profile_prfname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->profile_name));

	new_record[Anum_pg_profile_prffailedloginattempts - 1] =
		Int32GetDatum(failed_login_attempts);
	new_record[Anum_pg_profile_prfpasswordlocktime - 1] =
		Int32GetDatum(password_lock_time);
	new_record[Anum_pg_profile_prfpasswordlifetime - 1] =
		Int32GetDatum(password_life_time);
	new_record[Anum_pg_profile_prfpasswordgracetime - 1] =
		Int32GetDatum(password_grace_time);
	new_record[Anum_pg_profile_prfpasswordreusetime - 1] =
		Int32GetDatum(password_reuse_time);
	new_record[Anum_pg_profile_prfpasswordreusemax - 1] =
		Int32GetDatum(password_reuse_max);
	new_record[Anum_pg_profile_prfpasswordallowhashed - 1] =
		Int32GetDatum(password_allow_hashed);
	new_record_nulls[Anum_pg_profile_prfpasswordverifyfuncdb -1] =
		true;
	new_record_nulls[Anum_pg_profile_prfpasswordverifyfunc - 1] =
		true;

	/*
	 * GetNewOidForProfile() / GetNewOrPreassignedOid() will return the
	 * pre-assigned OID, if any, and error out if there was no pre-assigned
	 * values in binary upgrade mode.
	 */
	profileid = GetNewOidForProfile(pg_profile_rel, ProfileOidIndexId,
								Anum_pg_profile_oid,
								stmt->profile_name);

	new_record[Anum_pg_profile_oid - 1] =
		ObjectIdGetDatum(profileid);

	tuple = heap_form_tuple(pg_profile_dsc, new_record, new_record_nulls);

	/*
	 * Insert new record in the pg_profile table
	 */
	CatalogTupleInsert(pg_profile_rel, tuple);

	InvokeObjectPostCreateHook(ProfileRelationId, profileid, 0);

	heap_freetuple(tuple);

	/*
	 * Close pg_profile, but keep lock till commit
	 */
	table_close(pg_profile_rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		Assert(stmt->type == T_CreateProfileStmt);
		CdbDispatchUtilityStatement((Node *) stmt,
					    				DF_CANCEL_ON_ERROR|
					    				DF_WITH_SNAPSHOT|
					    				DF_NEED_TWO_PHASE,
					    				GetAssignedOidsForDispatch(),
					    				NULL);

		/* MPP-6929: metadata tracking */
		MetaTrackAddObject(ProfileRelationId,
				   			profileid,
				   			GetUserId(),
				   			"CREATE", "PROFILE");
	}

	return profileid;
}

/*
 * ALTER PROFILE
 */
Oid
AlterProfile(AlterProfileStmt *stmt)
{
	Datum		new_record[Natts_pg_profile];
	bool		new_record_nulls[Natts_pg_profile];
	bool		new_record_repl[Natts_pg_profile];
	Relation	pg_profile_rel;
	TupleDesc	pg_profile_dsc;
	HeapTuple	tuple,
				new_tuple;
	Form_pg_profile	profileform;
	Oid		profileid;
	char		*profile_name;
	ListCell	*option;
	int		failed_login_attempts = -1;
	int		password_lock_time = -1;
	int		password_life_time = -1;
	int 		password_grace_time = -1;
	int		password_reuse_time = -1;
	int 		password_reuse_max = -1;
	int		password_allow_hashed = -1;
	Oid		password_verify_funcdb = 0;
	Oid		password_verify_func = 0;
	DefElem		*dfailedloginattempts = NULL;
	DefElem		*dpasswordlocktime = NULL;
	DefElem		*dpasswordreusemax = NULL;
	int		numopts = 0;

	numopts = list_length(stmt->options);

	/* Only when enable_password_profile is true, can ALTER PROFILE. */
	if (!enable_password_profile)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),
				 errmsg("can't ALTER PROFILE for enable_password_profile is not open")));

	/* Only super user can ALTER PROFILE */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 	 errmsg("permission denied to alter profile, must be superuser")));

	profile_name = stmt->profile_name;
	Assert(profile_name);

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem	*defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "failed_login_attempts") == 0)
		{
			if (dfailedloginattempts)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					 	 errmsg("conflicting or redundant options")));
			dfailedloginattempts = defel;
		}
		else if (strcmp(defel->defname, "password_lock_time") == 0)
		{
			if (dpasswordlocktime)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					 	 errmsg("conflicting or redundant options")));
			dpasswordlocktime = defel;
		}
		else if (strcmp(defel->defname, "password_reuse_max") == 0)
		{
			if (dpasswordreusemax)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					 	 errmsg("conflicting or redundant options")));
			dpasswordreusemax = defel;
		}
	}

	if (dfailedloginattempts && dfailedloginattempts->arg)
	{
		failed_login_attempts = intVal(dfailedloginattempts->arg);
		if (failed_login_attempts == 0 ||
			failed_login_attempts < PROFILE_UNLIMITED ||
			failed_login_attempts > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("invalid failed login attempts: %d", failed_login_attempts)));
		else if (failed_login_attempts == PROFILE_DEFAULT &&
				strcmp(profile_name, "pg_default") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("can't set failed login attempts to -1(DEFAULT) of pg_default")));
	}

	if (dpasswordlocktime && dpasswordlocktime->arg)
	{
		password_lock_time = intVal(dpasswordlocktime->arg);
		if (password_lock_time < PROFILE_UNLIMITED ||
			password_lock_time > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("invalid password lock time: %d", password_lock_time)));
		else if (password_lock_time == PROFILE_DEFAULT &&
				strcmp(profile_name, "pg_default") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("can't set password lock time to -1(DEFAULT) of pg_default")));
	}

	if (dpasswordreusemax && dpasswordreusemax->arg)
	{
		password_reuse_max = intVal(dpasswordreusemax->arg);
		if (password_reuse_max < PROFILE_UNLIMITED ||
			password_reuse_max > PROFILE_MAX_VALID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("invalid password reuse max: %d", password_reuse_max)));
		else if (password_reuse_max == PROFILE_DEFAULT &&
				strcmp(profile_name, "pg_default") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("can't set password reuse max to -1(DEFAULT) of pg_default")));
	}

	/*
	 * Scan the pg_profile relation to be certain the profile exists.
	 */
	pg_profile_rel = table_open(ProfileRelationId, RowExclusiveLock);
	pg_profile_dsc = RelationGetDescr(pg_profile_rel);

	tuple = SearchSysCache1(PROFILENAME, CStringGetDatum(profile_name));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("profile \"%s\" does not exist", profile_name)));

	profileform = (Form_pg_profile) GETSTRUCT(tuple);
	profileid = profileform->oid;

	/*
	 * If all altered profile parameters are same with existing parameters, don't need
	 * to update tuple and return directly.
	 */
	if (failed_login_attempts == profileform->prffailedloginattempts &&
		password_lock_time == profileform->prfpasswordlocktime &&
		password_reuse_max == profileform->prfpasswordreusemax)
	{
		ReleaseSysCache(tuple);
		table_close(pg_profile_rel, NoLock);

		return profileid;
	}

	/*
	 * Build an updated tuple, perusing the information just obtained
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	if (failed_login_attempts != -1 &&
		failed_login_attempts != profileform->prffailedloginattempts)
	{
		new_record[Anum_pg_profile_prffailedloginattempts - 1] =
			Int32GetDatum(failed_login_attempts);
		new_record_repl[Anum_pg_profile_prffailedloginattempts - 1] =
			true;
	}

	if (password_lock_time != -1 &&
		password_lock_time != profileform->prfpasswordlocktime)
	{
		new_record[Anum_pg_profile_prfpasswordlocktime - 1] =
			Int32GetDatum(password_lock_time);
		new_record_repl[Anum_pg_profile_prfpasswordlocktime - 1] =
			true;
	}

	if (password_reuse_max != -1 &&
		password_reuse_max != profileform->prfpasswordreusemax)
	{
		new_record[Anum_pg_profile_prfpasswordreusemax - 1] =
			Int32GetDatum(password_reuse_max);
		new_record_repl[Anum_pg_profile_prfpasswordreusemax - 1] =
			true;
	}

	/*
	 * Now, we just support failed_login_attempts, password_lock_time and
	 * password_reuse_max, we need to set other fields' values to default.
	 */
	new_record[Anum_pg_profile_prfpasswordlifetime - 1] =
		Int32GetDatum(password_life_time);
	new_record[Anum_pg_profile_prfpasswordgracetime - 1] =
		Int32GetDatum(password_grace_time);
	new_record[Anum_pg_profile_prfpasswordreusetime - 1] =
		Int32GetDatum(password_reuse_time);
	new_record[Anum_pg_profile_prfpasswordallowhashed - 1] =
		Int32GetDatum(password_allow_hashed);
	new_record[Anum_pg_profile_prfpasswordverifyfuncdb -1] =
		Int32GetDatum(password_verify_funcdb);
	new_record[Anum_pg_profile_prfpasswordverifyfunc - 1] =
		Int32GetDatum(password_verify_func);

	new_tuple = heap_modify_tuple(tuple, pg_profile_dsc,
			       			new_record, new_record_nulls, new_record_repl);
	CatalogTupleUpdate(pg_profile_rel, &tuple->t_self, new_tuple);

	InvokeObjectPostAlterHook(ProfileRelationId, profileid, 0);

	ReleaseSysCache(tuple);
	heap_freetuple(new_tuple);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(ProfileRelationId,
				   profileid,
				   GetUserId(),
				   "ALTER", "PROFILE");

	/*
	 * Close pg_profile, but keep lock untill commit.
	 */
	table_close(pg_profile_rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
					    				DF_CANCEL_ON_ERROR|
					    				DF_WITH_SNAPSHOT|
					    				DF_NEED_TWO_PHASE,
					    				NIL,
					    				NULL);
	}

	return profileid;
}

/*
 * DROP PROFILE
 */
void
DropProfile(DropProfileStmt *stmt)
{
	Relation	pg_profile_rel;
	ListCell	*cell;

	/* Only when enable_password_profile is true, can ALTER PROFILE. */
	if (!enable_password_profile)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),
				 errmsg("can't DROP PROFILE for enable_password_profile is not open")));

	/* Only super user can DROP PROFILE */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 	 errmsg("permission denied to drop profile, must be superuser")));

	/*
	 * Scan the pg_profile relation to find the Oid of the Profile(s) to be
	 * deleted.
	 */
	pg_profile_rel = table_open(ProfileRelationId, RowExclusiveLock);

	foreach(cell, stmt->profiles)
	{
		HeapTuple	tuple;
		Form_pg_profile	profileform;
		char		*detail;
		char		*detail_log;
		Oid		profileid;

		char	*profile_name = strVal(lfirst(cell));
		if (strcmp(profile_name, "pg_default") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 errmsg("Disallow to drop default profile")));

		tuple = SearchSysCache1(PROFILENAME, PointerGetDatum(profile_name));
		if (!HeapTupleIsValid(tuple))
		{
			if (!stmt->missing_ok)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("profile \"%s\" does not exist", profile_name)));
			}
			if (Gp_role != GP_ROLE_EXECUTE)
			{
				ereport(NOTICE,
						(errmsg("profile \"%s\" does not exist", profile_name)));
			}

			continue;
		}

		profileform = (Form_pg_profile) GETSTRUCT(tuple);
		profileid = profileform->oid;

		/* DROP hook for the profile being removed */
		InvokeObjectDropHook(ProfileRelationId, profileid, 0);

		/*
		 * Lock the profile, so nobody can add dependencies to her while we drop
		 * her. We keep the lock until the end of transaction.
		 */
		LockSharedObject(ProfileRelationId, profileid, 0, AccessExclusiveLock);

		/* Check for pg_shdepend entries depending on this profile */
		if (checkSharedDependencies(ProfileRelationId, profileid,
			      							&detail, &detail_log))
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("profile \"%s\" cannot be dropped because some objects depend on it",
	     							profile_name),
					 errdetail_internal("%s", detail),
					 errdetail_log("%s", detail_log)));

		/*
		 * Remove the profile from the pg_profile table
		 */
		CatalogTupleDelete(pg_profile_rel, &tuple->t_self);

		ReleaseSysCache(tuple);

		/* metadata track */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackDropObject(ProfileRelationId,
					    			profileid);
	}

	/*
	 * Now we can clean up; but keep locks until commit.
	 */
	table_close(pg_profile_rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
					    				DF_CANCEL_ON_ERROR|
					    				DF_WITH_SNAPSHOT|
					    				DF_NEED_TWO_PHASE,
					    				NIL,
					    				NULL);
	}
}

/*
 * As pg_profile fields' values may be PROFILE_UNLIMITED(-2) or
 * PROFILE_DEFAULT(-1), transform them to normal values.
 */
int32
tranformProfileValueToNormal(int32 profile_val, int attoff)
{
	HeapTuple	profile_tuple;
	int32		normal_profile_val;
	bool		isnull;
	Datum		datum;
	Relation	rel;

	/*
	 * If current value is PROFILE_DEFAULT(-1), we need to search
	 * pg_profile catalog to get default profile's value.
	 */
	if (profile_val == PROFILE_DEFAULT)
	{
		rel = table_open(ProfileRelationId, AccessShareLock);

		profile_tuple = SearchSysCache1(PROFILEID, ObjectIdGetDatum(DefaultProfileOID));
		Assert(HeapTupleIsValid(profile_tuple));

		datum = SysCacheGetAttr(PROFILEID, profile_tuple, attoff, &isnull);
		Assert(!isnull);

		normal_profile_val = DatumGetInt32(datum);

		if (normal_profile_val == PROFILE_UNLIMITED)
			normal_profile_val = PROFILE_MAX_VALID;

		ReleaseSysCache(profile_tuple);
		table_close(rel, AccessShareLock);
	}
	/*
	 * Currently, the profile field's max valid value is PROFILE_MAX_VALID(9999).
	 */
	else if (profile_val == PROFILE_UNLIMITED)
		normal_profile_val = PROFILE_MAX_VALID;
	else
		normal_profile_val = profile_val;

	return normal_profile_val;
}

/*
 * Function get_role_status()
 */
Datum
get_role_status(PG_FUNCTION_ARGS)
{
	HeapTuple	tuple;
	char		*user_name;
	Datum		datum;
	bool		isnull;
	int16		account_status;
	char		*ret = NULL;

	user_name = PG_GETARG_CSTRING(0);

	/* Only when enable_password_profile is true, can ALTER PROFILE. */
	if (!enable_password_profile)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_CONFIGURED),
				 errmsg("can't call get_role_status for enable_password_profile is not open")));

	/* Only super users has the privilege */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to call get_role_status, must be superuser")));

	tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(user_name));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("user \"%s\" does not exist", user_name)));

	datum = SysCacheGetAttr(AUTHNAME, tuple, Anum_pg_authid_rolaccountstatus, &isnull);
	Assert(!isnull);

	account_status = DatumGetInt16(datum);

	ReleaseSysCache(tuple);

	switch(account_status)
	{
		case ROLE_ACCOUNT_STATUS_OPEN:
			ret = "OPEN";
			break;
		case ROLE_ACCOUNT_STATUS_LOCKED_TIMED:
			ret = "LOCKED(TIMED)";
			break;
		case ROLE_ACCOUNT_STATUS_LOCKED:
			ret = "LOCKED";
			break;
		case ROLE_ACCOUNT_STATUS_EXPIRED_GRACE:
			ret = "EXPIRED(GRACE)";
			break;
		case ROLE_ACCOUNT_STATUS_EXPIRED:
			ret = "EXPIRED";
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("user %s's account_status %d not recognized",
						user_name, account_status)));
	}

	PG_RETURN_CSTRING(ret);
}