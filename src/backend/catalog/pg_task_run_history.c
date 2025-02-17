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
 * pg_task_run_history.c
 *	  save all tasks run histories of pg cron scheduler.
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_task_run_history.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_task_run_history.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/*
 * TaskRunHistoryCreate
 *		Create an new task run history in pg_task_run_history.
 */
void
TaskRunHistoryCreate(Oid runid, int64 *jobid, const char *database, const char *username,
					 const char *command, const char *status)
{
    Relation pg_task_run_history;
    HeapTuple tup;
    Datum values[Natts_pg_task_run_history];
    bool nulls[Natts_pg_task_run_history];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    values[Anum_pg_task_run_history_runid - 1] = ObjectIdGetDatum(runid);
    values[Anum_pg_task_run_history_jobid - 1] = ObjectIdGetDatum((Oid) *jobid);
    values[Anum_pg_task_run_history_database - 1] = CStringGetTextDatum(database);
    values[Anum_pg_task_run_history_username - 1] = CStringGetTextDatum(username);
    values[Anum_pg_task_run_history_command - 1] = CStringGetTextDatum(command);
    values[Anum_pg_task_run_history_status - 1] = CStringGetTextDatum(status);

    nulls[Anum_pg_task_run_history_return_message - 1] = true;
    nulls[Anum_pg_task_run_history_start_time - 1] = true;
    nulls[Anum_pg_task_run_history_end_time - 1] = true;

    tup = heap_form_tuple(RelationGetDescr(pg_task_run_history), values, nulls);
    CatalogTupleInsert(pg_task_run_history, tup);
    heap_freetuple(tup);

    table_close(pg_task_run_history, RowExclusiveLock);
}

/*
 * TaskRunHistoryUpdate
 *		Update an existing task run history in pg_task_run_history.
 */
void
TaskRunHistoryUpdate(Oid runid, int32 *job_pid, const char *status,
                     const char *return_message, TimestampTz *start_time, TimestampTz *end_time)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_pg_task_run_history];
    bool        nulls[Natts_pg_task_run_history];
    bool        doreplace[Natts_pg_task_run_history];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0],
                Anum_pg_task_run_history_runid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(runid));
    
    scanDescriptor = systable_beginscan(pg_task_run_history,
                                        TaskRunHistoryRunIdIndexId,
                                        true, NULL, 1, scanKey);
    
    tup = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(tup))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("task run history with runid %u does not exist", runid)));
    }

    if (job_pid != NULL)
    {
        values[Anum_pg_task_run_history_job_pid - 1] = Int32GetDatum(*job_pid);
        doreplace[Anum_pg_task_run_history_job_pid - 1] = true;
    }
    if (status != NULL)
    {
        values[Anum_pg_task_run_history_status - 1] = CStringGetTextDatum(status);
        doreplace[Anum_pg_task_run_history_status - 1] = true;
    }
    if (return_message != NULL)
    {
        values[Anum_pg_task_run_history_return_message - 1] = CStringGetTextDatum(return_message);
        doreplace[Anum_pg_task_run_history_return_message - 1] = true;
    }
    if (start_time != NULL)
    {
        values[Anum_pg_task_run_history_start_time - 1] = TimestampTzGetDatum(*start_time);
        doreplace[Anum_pg_task_run_history_start_time - 1] = true;
    }
    if (end_time != NULL)
    {
        values[Anum_pg_task_run_history_end_time - 1] = TimestampTzGetDatum(*end_time);
        doreplace[Anum_pg_task_run_history_end_time - 1] = true;
    }

    tup = heap_modify_tuple(tup, RelationGetDescr(pg_task_run_history), values, nulls, doreplace);
    CatalogTupleUpdate(pg_task_run_history, &tup->t_self, tup);
    heap_freetuple(tup);

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, RowExclusiveLock);
}

/*
 * MarkRunningTaskAsFailed
 *		Mark all the running tasks as failed.
 */
void
MarkRunningTaskAsFailed(void)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_pg_task_run_history];
    bool        nulls[Natts_pg_task_run_history];
    bool        doreplace[Natts_pg_task_run_history];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_pg_task_run_history_status, BTEqualStrategyNumber,
                F_TEXTEQ, CStringGetTextDatum("running"));

    scanDescriptor = systable_beginscan(pg_task_run_history, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        values[Anum_pg_task_run_history_status - 1] = CStringGetTextDatum("failed");
        doreplace[Anum_pg_task_run_history_status - 1] = true;
        values[Anum_pg_task_run_history_return_message - 1] = CStringGetTextDatum("server restarted");
        doreplace[Anum_pg_task_run_history_return_message - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(pg_task_run_history), values, nulls, doreplace);
        CatalogTupleUpdate(pg_task_run_history, &tup->t_self, tup);
        heap_freetuple(tup);
    }

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, RowExclusiveLock);
}

/*
 * RemoveTaskRunHistoryByJobId
 *        Remove all the task run history records for the given jobid.
 */
void
RemoveTaskRunHistoryByJobId(Oid jobid)
{
    Relation    pg_task_run_history;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];

    pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_pg_task_run_history_jobid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(jobid));

    scanDescriptor = systable_beginscan(pg_task_run_history, TaskRunHistoryJobIdIndexId,
                                        true, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(pg_task_run_history, &tup->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(pg_task_run_history, RowExclusiveLock);
}
