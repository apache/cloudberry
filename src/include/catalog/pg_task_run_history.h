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
 * pg_task_run_history.h
 *	  save all tasks run history of cron task scheduler.
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_task_run_history.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TASK_RUN_HISTORY_H
#define PG_TASK_RUN_HISTORY_H

#include "catalog/genbki.h"
#include "catalog/pg_task_run_history_d.h"
#include "datatype/timestamp.h"

#define timestamptz Datum

/* ----------------
 *		pg_task_run_history definition.  cpp turns this into
 *		typedef struct FormData_pg_task_run_history
 * ----------------
 */
CATALOG(pg_task_run_history,9993,TaskRunHistoryRelationId) BKI_SHARED_RELATION
{
	Oid         runid;
	Oid         jobid;
	int32		job_pid BKI_DEFAULT(0);
	text		database;
	text		username;
	text		command;
	text		status;
    text        return_message;
    timestamptz start_time;
    timestamptz end_time;
} FormData_pg_task_run_history;

typedef FormData_pg_task_run_history *Form_pg_task_run_history;

DECLARE_INDEX(pg_task_run_history_jobid_index, 8633, TaskRunHistoryJobIdIndexId, on pg_task_run_history using btree(jobid oid_ops));
DECLARE_UNIQUE_INDEX_PKEY(pg_task_run_history_runid_index, 8110, TaskRunHistoryRunIdIndexId, on pg_task_run_history using btree(runid oid_ops));

extern void TaskRunHistoryCreate(Oid runid, int64 *jobid, const char *database, const char *username,
								 const char *command, const char *status);

extern void TaskRunHistoryUpdate(Oid runid, int32 *job_pid, const char *status,
								 const char *return_message,
								 TimestampTz *start_time, TimestampTz *end_time);

extern void MarkRunningTaskAsFailed(void);

extern void RemoveTaskRunHistoryByJobId(Oid jobid);

#undef timestamptz

#endif			/* PG_TASK_RUN_HISTORY_H */
