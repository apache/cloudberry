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
 * taskcmds.c
 *      Cloudberry TASK SCHEDULE support code.
 *
 * IDENTIFICATION
 *		src/backend/commands/taskcmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_task.h"
#include "catalog/pg_task_run_history.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/taskcmds.h"
#include "task/job_metadata.h"
#include "utils/acl.h"
#include "utils/builtins.h"

/*
 * DefineTask
 *      Create a new cron task.
 */
ObjectAddress
DefineTask(ParseState *pstate, CreateTaskStmt * stmt)
{
	ObjectAddress address;
	char	   *dbname = NULL;
	char	   *username = NULL;
	ListCell   *option;
	DefElem    *d_dbname = NULL;
	DefElem    *d_username = NULL;
	Oid			jobid = InvalidOid;
	AclResult	aclresult;

	/* must have CREATE privilege on database */
	aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(MyDatabaseId));

	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "dbname") == 0)
		{
			if (d_dbname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_dbname = defel;
		}
		else if (strcmp(defel->defname, "username") == 0)
		{
			if (d_username)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_username = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/* use the specified or current user */
	if (d_username != NULL && d_username->arg)
	{
		username = defGetString(d_username);
		if (!OidIsValid(get_role_oid(username, false)))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role \"%s\" does not exist", username)));
	}
	else
		username = GetUserNameFromId(GetUserId(), false);

	/* use the specified or current database */
	if (d_dbname != NULL && d_dbname->arg)
		dbname = defGetString(d_dbname);
	else
		dbname = get_database_name(MyDatabaseId);

	if (!OidIsValid(get_database_oid(dbname, true)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", dbname)));

	/* check if the task already exists */
	if (stmt->if_not_exists)
	{
		if (OidIsValid(GetTaskJobId(stmt->taskname, username)))
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("task \"%s\" already exists, skipping",
							stmt->taskname)));
			return InvalidObjectAddress;
		}
	}

	if (!allowSystemTableMods && strncmp(stmt->taskname, DYNAMIC_TASK_PREFIX, 25) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("unacceptable task name \"%s\"", stmt->taskname),
				 errdetail("The prefix \"%s\" is reserved for system tasks.",
						   DYNAMIC_TASK_PREFIX)));
	}
	jobid = ScheduleCronJob(cstring_to_text(stmt->schedule), cstring_to_text(stmt->sql),
							cstring_to_text(dbname), cstring_to_text(username),
							true, cstring_to_text(stmt->taskname));

	/* Depend on owner. */
	recordDependencyOnOwner(TaskRelationId, jobid, get_role_oid(username, false));

	ObjectAddressSet(address, TaskRelationId, jobid);
	return address;
}

/*
 * AlterTask
 *      Alter an existing cron task.
 */
ObjectAddress
AlterTask(ParseState *pstate, AlterTaskStmt * stmt)
{
	ObjectAddress address = InvalidObjectAddress;
	char	   *current_user;
	Oid			jobid = InvalidOid;
	DefElem    *d_schedule = NULL;
	DefElem    *d_dbname = NULL;
	DefElem    *d_username = NULL;
	DefElem    *d_active = NULL;
	DefElem    *d_sql = NULL;
	ListCell   *option;
	char	   *schedule = NULL;
	char	   *dbname = NULL;
	char	   *username = NULL;
	bool		active;
	char	   *sql = NULL;

	current_user = GetUserNameFromId(GetUserId(), false);
	jobid = GetTaskJobId(stmt->taskname, current_user);
	if (!OidIsValid(jobid))
	{
		if (stmt->missing_ok)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("task \"%s\" does not exist, skipping",
							stmt->taskname)));
			return address;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("task \"%s\" does not exist", stmt->taskname)));
	}

	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "schedule") == 0)
		{
			if (d_schedule)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_schedule = defel;
		}
		else if (strcmp(defel->defname, "dbname") == 0)
		{
			if (d_dbname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_dbname = defel;
		}
		else if (strcmp(defel->defname, "username") == 0)
		{
			if (d_username)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_username = defel;
		}
		else if (strcmp(defel->defname, "active") == 0)
		{
			if (d_active)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_active = defel;
		}
		else if (strcmp(defel->defname, "sql") == 0)
		{
			if (d_sql)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			d_sql = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (d_schedule != NULL && d_schedule->arg)
		schedule = defGetString(d_schedule);

	if (d_dbname != NULL && d_dbname->arg)
	{
		dbname = defGetString(d_dbname);
		if (!OidIsValid(get_database_oid(dbname, true)))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database \"%s\" does not exist", dbname)));
	}

	if (d_username != NULL && d_username->arg)
	{
		username = defGetString(d_username);
		if (!OidIsValid(get_role_oid(username, true)))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("role \"%s\" does not exist", username)));
	}

	if (d_active != NULL)
	{
		active = intVal(d_active->arg);
		Assert(BoolIsValid(active));
	}

	if (d_sql != NULL && d_sql->arg)
	{
		if (strncmp(stmt->taskname, DYNAMIC_TASK_PREFIX, 25) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("can not alter REFRESH SQL of dynamic tables")));
		else
			sql = defGetString(d_sql);
	}

	AlterCronJob(jobid, schedule, sql, dbname, username, d_active != NULL ? &active : NULL);

	if (username)
	{
		/* Update owner dependency reference */
		changeDependencyOnOwner(TaskRelationId,
								jobid,
								get_role_oid(username, false));
	}

	ObjectAddressSet(address, TaskRelationId, jobid);
	return address;
}

/*
 * DropTask
 *      Drop an existing cron task.
 */
ObjectAddress
DropTask(ParseState *pstate, DropTaskStmt * stmt)
{
	ObjectAddress address = InvalidObjectAddress;
	char	   *username;
	Oid			jobid = InvalidOid;

	/* current username */
	username = GetUserNameFromId(GetUserId(), false);

	/* delete from pg_task */
	jobid = UnscheduleCronJob(stmt->taskname, username, InvalidOid, stmt->missing_ok);

	/* delete from pg_task_run_history according to the jobid */
	if (OidIsValid(jobid))
	{
		if (!allowSystemTableMods && strncmp(stmt->taskname, DYNAMIC_TASK_PREFIX, 25) == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_RESERVED_NAME),
					 errmsg("can not drop a internal task \"%s\" paried with dynamic table", stmt->taskname),
					 errdetail("please drop the dynamic table instead")));
		}
		RemoveTaskRunHistoryByJobId(jobid);
		ObjectAddressSet(address, TaskRelationId, jobid);
		/* Clean up dependencies */
		deleteSharedDependencyRecordsFor(TaskRelationId, jobid, 0);
	}

	return address;
}

/*
 * RemoveTaskById
 *      Remove an existing cron task by jobid.
 */
void
RemoveTaskById(Oid jobid)
{
	/* remove the cron task in pg_task */
	UnscheduleCronJob(NULL, NULL, jobid, false);
	/* delete from pg_task_run_history according to the jobid */
	if (OidIsValid(jobid))
	{
		RemoveTaskRunHistoryByJobId(jobid);
	}
}
