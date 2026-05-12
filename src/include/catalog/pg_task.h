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
 * pg_task.h
 *	  save all tasks of cron task scheduler.
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_task.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TASK_H
#define PG_TASK_H

#include "catalog/genbki.h"
#include "catalog/pg_task_d.h"

/* ----------------
 *		pg_task definition.  cpp turns this into
 *		typedef struct FormData_pg_task
 * ----------------
 */
CATALOG(pg_task,9637,TaskRelationId) BKI_SHARED_RELATION
{
	Oid			jobid;
	text		schedule;
	text		command;
	text		nodename;
	int32		nodeport;
	text		database;
	text		username;
	bool		active BKI_DEFAULT(t);
	text		jobname;
} FormData_pg_task;

typedef FormData_pg_task *Form_pg_task;

DECLARE_UNIQUE_INDEX(pg_task_jobname_username_index, 8915, TaskJobNameUserNameIndexId, on pg_task using btree(jobname text_ops, username text_ops));
DECLARE_UNIQUE_INDEX_PKEY(pg_task_jobid_index, 8916, TaskJobIdIndexId, on pg_task using btree(jobid oid_ops));

extern Oid TaskCreate(const char *schedule, const char *command,
					  const char *nodename, int32 nodeport,
					  const char *database, const char *username,
					  bool active, const char *jobname);

extern void TaskUpdate(Oid jobid, const char *schedule,
					   const char *command, const char *database,
					   const char *username, bool *active);

extern Oid GetTaskJobId(const char *jobname, const char *username);

extern char * GetTaskNameById(Oid jobid);

/* Reversed prefix for Dynamic Tables. */
#define DYNAMIC_TASK_PREFIX "gp_dynamic_table_refresh_"

/* Default Dynamic Table Schedule */
#define DYNAMIC_TABLE_DEFAULT_REFRESH_INTERVAL "*/5 * * * *"

#endif			/* PG_TASK_H */
