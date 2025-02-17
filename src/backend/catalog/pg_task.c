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
 * pg_task.c
 *	  save all tasks of pg cron scheduler.
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_task.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_task.h"
#include "postmaster/bgworker.h"
#include "task/pg_cron.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/*
 * TaskCreate
 *		Create an new task in pg_task.
 */
Oid
TaskCreate(const char *schedule, const char *command,
		   const char *nodename, int32 nodeport,
		   const char *database, const char *username,
		   bool active, const char *jobname)
{
	Relation 	pg_task;
	HeapTuple	tup;
	Oid		 	jobid;
	Datum	 	values[Natts_pg_task];
	bool	 	nulls[Natts_pg_task];
	pid_t		cron_pid;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	pg_task = table_open(TaskRelationId, RowExclusiveLock);

	jobid = GetNewOidWithIndex(pg_task, TaskJobIdIndexId, Anum_pg_task_jobid);
	values[Anum_pg_task_jobid - 1] = ObjectIdGetDatum(jobid);
	values[Anum_pg_task_command - 1] = CStringGetTextDatum(command);
	values[Anum_pg_task_schedule - 1] = CStringGetTextDatum(schedule);
	values[Anum_pg_task_nodename - 1] = CStringGetTextDatum(nodename);
	values[Anum_pg_task_nodeport - 1] = Int32GetDatum(nodeport);
	values[Anum_pg_task_database - 1] = CStringGetTextDatum(database);
	values[Anum_pg_task_username - 1] = CStringGetTextDatum(username);
	values[Anum_pg_task_active - 1] = BoolGetDatum(active);
	if (jobname)
		values[Anum_pg_task_jobname - 1] = CStringGetTextDatum(jobname);
	else
		nulls[Anum_pg_task_jobname - 1] = true;

	tup = heap_form_tuple(RelationGetDescr(pg_task), values, nulls);
	CatalogTupleInsert(pg_task, tup);
	heap_freetuple(tup);

	table_close(pg_task, RowExclusiveLock);

	/* Send SIGHUP to pg_cron launcher to reload the task */
	cron_pid = PgCronLauncherPID();
	if (cron_pid == InvalidPid)
		elog(ERROR, "could not find pid of pg_cron launcher process");
	if (kill(cron_pid, SIGHUP) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) cron_pid, SIGHUP);

	return jobid;
}

/*
 * TaskUpdate
 *		Update an existing task in pg_task.
 */
void
TaskUpdate(Oid jobid, const char *schedule,
		   const char *command, const char *database,
		   const char *username, bool *active)
{
	Relation 	pg_task;
	HeapTuple	tup;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	Datum	 	values[Natts_pg_task];
	bool	 	nulls[Natts_pg_task];
	bool		doreplace[Natts_pg_task];
	pid_t		cron_pid;

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(doreplace, false, sizeof(doreplace));

	pg_task = table_open(TaskRelationId, RowExclusiveLock);

	/* try to find the task */
	ScanKeyInit(&scanKey[0], Anum_pg_task_jobid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(jobid));

	scanDescriptor = systable_beginscan(pg_task, TaskJobIdIndexId,
										true, NULL, 1, scanKey);
	tup = systable_getnext(scanDescriptor);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "could not find valid entry for job");
	
	/* specify the fields that need to be updated */
	if (schedule)
	{
		values[Anum_pg_task_schedule - 1] = CStringGetTextDatum(schedule);
		doreplace[Anum_pg_task_schedule - 1] = true;
	}
	if (command)
	{
		values[Anum_pg_task_command - 1] = CStringGetTextDatum(command);
		doreplace[Anum_pg_task_command - 1] = true;
	}
	if (database)
	{
		values[Anum_pg_task_database - 1] = CStringGetTextDatum(database);
		doreplace[Anum_pg_task_database - 1] = true;
	}
	if (username)
	{
		values[Anum_pg_task_username - 1] = CStringGetTextDatum(username);
		doreplace[Anum_pg_task_username - 1] = true;
	}
	if (active)
	{
		values[Anum_pg_task_active - 1] = BoolGetDatum(*active);	
		doreplace[Anum_pg_task_active - 1] = true;
	}

	tup = heap_modify_tuple(tup, RelationGetDescr(pg_task), values, nulls, doreplace);
	CatalogTupleUpdate(pg_task, &tup->t_self, tup);
	heap_freetuple(tup);

	systable_endscan(scanDescriptor);
	table_close(pg_task, RowExclusiveLock);

	/* Send SIGHUP to pg_cron launcher to reload the task */
	cron_pid = PgCronLauncherPID();
	if (cron_pid == InvalidPid)
		elog(ERROR, "could not find pid of pg_cron launcher process");
	if (kill(cron_pid, SIGHUP) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) cron_pid, SIGHUP);
}

/*
 * GetTaskJobId
 *		Get the jobid of a task.
 */
Oid
GetTaskJobId(const char *jobname, const char *username)
{
	Relation 	pg_task;
	HeapTuple	tup;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[2];
	Oid			jobid = InvalidOid;
	Form_pg_task task;

	pg_task = table_open(TaskRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_task_jobname, BTEqualStrategyNumber,
				F_TEXTEQ, CStringGetTextDatum(jobname));
	ScanKeyInit(&scanKey[1], Anum_pg_task_username, BTEqualStrategyNumber,
				F_TEXTEQ, CStringGetTextDatum(username));

	scanDescriptor = systable_beginscan(pg_task, TaskJobNameUserNameIndexId,
										true, NULL, 2, scanKey);
	tup = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(tup))
	{
		task = (Form_pg_task) GETSTRUCT(tup);
		jobid = task->jobid;
	}

	systable_endscan(scanDescriptor);
	table_close(pg_task, AccessShareLock);

	return jobid;
}

/*
 * GetTaskNameById
 *		Get task name by job id.
 */
char *
GetTaskNameById(Oid jobid)
{
	Relation 	pg_task;
	HeapTuple	tup;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	char		*result = NULL;
	TupleDesc	tupleDesc = NULL;
	bool		isNull = false;

	pg_task = table_open(TaskRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_task_jobid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(jobid));

	scanDescriptor = systable_beginscan(pg_task, TaskJobIdIndexId,
										true, NULL, 1, scanKey);
	tup = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(tup))
	{
		tupleDesc = RelationGetDescr(pg_task);
		Datum jobname = heap_getattr(tup, Anum_pg_task_jobname, tupleDesc, &isNull);
		if (!isNull)
			result = TextDatumGetCString(jobname);
	}

	systable_endscan(scanDescriptor);
	table_close(pg_task, AccessShareLock);

	return result;
}
