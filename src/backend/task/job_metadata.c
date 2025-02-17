/*-------------------------------------------------------------------------
 *
 * src/job_metadata.c
 *
 * Functions for reading and manipulating pg_cron metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_task.h"
#include "catalog/pg_task_run_history.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "storage/lock.h"
#include "task/job_metadata.h"
#include "task/pg_cron.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

static HTAB *CreateCronJobHash(void);

static void EnsureDeletePermission(Relation cronJobsTable, HeapTuple heapTuple);
static void InvalidateJobCacheCallback(Datum argument, Oid relationId);
static void InvalidateJobCache(void);
static CronJob *TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple);
static Oid GetRoleOidIfCanLogin(char *username);
static entry *ParseSchedule(char *scheduleText);
static bool TryParseInterval(char *scheduleText, uint32 *secondsInterval);

/* GUC settings */
bool task_enable_superuser_jobs = true;

/* global variables */
static MemoryContext CronJobContext = NULL;
static HTAB *CronJobHash = NULL;
static Oid CachedCronJobRelationId = InvalidOid;
bool CronJobCacheValid = false;

/*
 * InitializeJobMetadataCache initializes the data structures for caching
 * job metadata.
 */
void
InitializeJobMetadataCache(void)
{
	/* watch for invalidation events */
	CacheRegisterRelcacheCallback(InvalidateJobCacheCallback, (Datum) 0);

	CronJobContext = AllocSetContextCreate(CurrentMemoryContext,
											 "pg_cron job context",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	CronJobHash = CreateCronJobHash();
}

/*
 * ResetJobMetadataCache resets the job metadata cache to its initial
 * state.
 */
void
ResetJobMetadataCache(void)
{
	MemoryContextResetAndDeleteChildren(CronJobContext);

	CronJobHash = CreateCronJobHash();
}

/*
 * CreateCronJobHash creates the hash for caching job metadata.
 */
static HTAB *
CreateCronJobHash(void)
{
	HTAB *taskHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CronJob);
	info.hash = tag_hash;
	info.hcxt = CronJobContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create("pg_cron jobs", 32, &info, hashFlags);

	return taskHash;
}

/*
 * GetCronJob gets the cron job with the given id.
 */
CronJob *
GetCronJob(int64 jobId)
{
	CronJob *job = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	job = hash_search(CronJobHash, &hashKey, HASH_FIND, &isPresent);

	return job;
}

/*
 * ScheduleCronJob schedules a cron job with the given name.
 */
int64
ScheduleCronJob(text *scheduleText, text *commandText, text *databaseText,
				text *usernameText, bool active, text *jobnameText)
{
	entry *parsedSchedule = NULL;
	char *schedule;
	char *command;
	char *database_name = NULL;
	char *jobName = NULL;
	char *username = NULL;
	AclResult aclresult;
	Oid userIdcheckacl;

	int64 jobId = 0;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	userIdcheckacl = GetUserId();

	/* check schedule is valid */
	schedule = text_to_cstring(scheduleText);
	parsedSchedule = ParseSchedule(schedule);

	if (parsedSchedule == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid schedule: %s", schedule),
						errhint("Use cron format (e.g. 5 4 * * *), or interval "
								"format '[1-59] seconds'")));
	}

	free_entry(parsedSchedule);

	command = text_to_cstring(commandText);

	if (jobnameText != NULL)
	{
		jobName = text_to_cstring(jobnameText);
	}

	/* username has been provided */
	if (usernameText != NULL)
	{
		username = text_to_cstring(usernameText);
		userIdcheckacl = GetRoleOidIfCanLogin(username);
	}

	/* database has been provided */
	if (databaseText != NULL)
		database_name = text_to_cstring(databaseText);

	/* first do a crude check to see whether superuser jobs are allowed */
	if (!task_enable_superuser_jobs && superuser_arg(userIdcheckacl))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cannot schedule jobs as superuser"),
						errdetail("Scheduling jobs as superuser is disallowed when "
								  "cron.enable_superuser_jobs is set to off.")));
	}

	/* ensure the user that is used in the job can connect to the database */
	aclresult = pg_database_aclcheck(get_database_oid(database_name, false),
									 userIdcheckacl, ACL_CONNECT);
	if (aclresult != ACLCHECK_OK)
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("User %s does not have CONNECT privilege on %s",
								GetUserNameFromId(userIdcheckacl, false), database_name)));

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);

	/* insert task into pg_catalog.pg_task table */
	jobId = TaskCreate(schedule, command, task_host_addr, PostPortNumber,
					   database_name, username, active, jobName);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	InvalidateJobCache();

	return jobId;
}

/*
 * GetRoleOidIfCanLogin
 * Checks user exist and can log in
 */
static Oid
GetRoleOidIfCanLogin(char *username)
{
	HeapTuple   roletup;
	Form_pg_authid rform;
	Oid roleOid = InvalidOid;

	roletup = SearchSysCache1(AUTHNAME, PointerGetDatum(username));
	if (!HeapTupleIsValid(roletup))
		ereport(ERROR,
				(errmsg("role \"%s\" does not exist",
						username)));

	rform = (Form_pg_authid) GETSTRUCT(roletup);

	if (!rform->rolcanlogin)
		ereport(ERROR,
				(errmsg("role \"%s\" can not log in",
						username),
				 errdetail("Jobs may only be run by roles that have the LOGIN attribute.")));

	roleOid = rform->oid;

	ReleaseSysCache(roletup);
	return roleOid;
}

/*
 * NextRunId draws a new run ID from GetNewOidWithIndex.
 */
int64
NextRunId(void)
{
	Relation pg_task_run_history;
	int64 runId = 0;
	MemoryContext originalContext = CurrentMemoryContext;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	pg_task_run_history = table_open(TaskRunHistoryRelationId, RowExclusiveLock);
	runId = GetNewOidWithIndex(pg_task_run_history, TaskRunHistoryRunIdIndexId,
							   Anum_pg_task_run_history_runid);
	table_close(pg_task_run_history, RowExclusiveLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);

	return runId;
}

Oid
UnscheduleCronJob(const char *jobname, const char *username, Oid taskid, bool missing_ok)
{
	Relation		pg_task = NULL;
	SysScanDesc		scanDescriptor = NULL;
	HeapTuple		heapTuple = NULL;
	pid_t			cron_pid;
	Form_pg_task	task = NULL;
	Oid				jobid = InvalidOid;

	pg_task = table_open(TaskRelationId, RowExclusiveLock);

	if (OidIsValid(taskid)) {
		ScanKeyData scanKey[1];
		ScanKeyInit(&scanKey[0], Anum_pg_task_jobid,
					BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(taskid));

		scanDescriptor = systable_beginscan(pg_task, TaskJobIdIndexId, false,
											NULL, 1, scanKey);
	}
	else
	{
		ScanKeyData scanKey[2];
		ScanKeyInit(&scanKey[0], Anum_pg_task_jobname,
					BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(jobname));
		ScanKeyInit(&scanKey[1], Anum_pg_task_username,
					BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(username));

		scanDescriptor = systable_beginscan(pg_task, TaskJobNameUserNameIndexId, false,
											NULL, 2, scanKey);
	}

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		if (missing_ok)
		{
			systable_endscan(scanDescriptor);
			table_close(pg_task, RowExclusiveLock);
			ereport(NOTICE,
					(errmsg("task \"%s\" does not exist, skipping",
							jobname)));
			return InvalidOid;
		} else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("task \"%s\" does not exist", jobname)));
	}

	task = (Form_pg_task) GETSTRUCT(heapTuple);
	jobid = task->jobid;

	EnsureDeletePermission(pg_task, heapTuple);

	simple_heap_delete(pg_task, &heapTuple->t_self);

	systable_endscan(scanDescriptor);
	table_close(pg_task, RowExclusiveLock);

	CommandCounterIncrement();
	InvalidateJobCache();

	/* Send SIGHUP to pg_cron launcher to reload the task */
	cron_pid = PgCronLauncherPID();
	if (cron_pid == InvalidPid)
		elog(ERROR, "could not find pid of pg_cron launcher process");
	if (kill(cron_pid, SIGHUP) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) cron_pid, SIGHUP);

	return jobid;
}

/*
 * EnsureDeletePermission throws an error if the current user does
 * not have permission to delete the given cron.job tuple.
 */
static void
EnsureDeletePermission(Relation cronJobsTable, HeapTuple heapTuple)
{
	TupleDesc tupleDescriptor = RelationGetDescr(cronJobsTable);

	/* check if the current user owns the row */
	Oid userId = GetUserId();
	char *userName = GetUserNameFromId(userId, false);

	bool isNull = false;
	Datum ownerNameDatum = heap_getattr(heapTuple, Anum_pg_task_username,
										tupleDescriptor, &isNull);
	char *ownerName = TextDatumGetCString(ownerNameDatum);
	if (pg_strcasecmp(userName, ownerName) != 0)
	{
		/* otherwise, allow if the user has DELETE permission */
		AclResult aclResult = pg_class_aclcheck(TaskRelationId, GetUserId(),
												ACL_DELETE);
		if (aclResult != ACLCHECK_OK)
		{
			aclcheck_error(aclResult,
						   OBJECT_TABLE,
						   get_rel_name(TaskRelationId));
		}
	}
}

/*
 * Invalidate job cache ensures the job cache is reloaded on the next
 * iteration of pg_cron.
 */
static void
InvalidateJobCache(void)
{
	HeapTuple classTuple = NULL;

	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(TaskRelationId));
	if (HeapTupleIsValid(classTuple))
	{
		CacheInvalidateRelcacheByTuple(classTuple);
		ReleaseSysCache(classTuple);
	}
}

/*
 * InvalidateJobCacheCallback invalidates the job cache in response to
 * an invalidation event.
 */
static void
InvalidateJobCacheCallback(Datum argument, Oid relationId)
{
	if (relationId == CachedCronJobRelationId ||
		CachedCronJobRelationId == InvalidOid)
	{
		CronJobCacheValid = false;
		CachedCronJobRelationId = InvalidOid;
	}
}

/*
 * LoadCronJobList loads the current list of jobs from the
 * cron.job table and adds each job to the CronJobHash.
 */
List *
LoadCronJobList(void)
{
	List *jobList = NIL;

	Relation cronJobTable = NULL;

	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * If we are on a hot standby, the job table is treated as being empty.
	 */
	if (RecoveryInProgress())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);

		return NIL;
	}

	cronJobTable = table_open(TaskRelationId, AccessShareLock);

	scanDescriptor = systable_beginscan(cronJobTable,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(cronJobTable);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		MemoryContext oldContext = NULL;
		CronJob *job = NULL;
		Oid jobOwnerId = InvalidOid;

		oldContext = MemoryContextSwitchTo(CronJobContext);

		job = TupleToCronJob(tupleDescriptor, heapTuple);

		jobOwnerId = get_role_oid(job->userName, false);
		if (!task_enable_superuser_jobs && superuser_arg(jobOwnerId))
		{
			/*
			 * Someone inserted a superuser into the metadata. Skip over the
			 * job when cron.enable_superuser_jobs is disabled. The memory
			 * will be cleaned up when CronJobContext is reset.
			 */
			ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							  errmsg("skipping job " INT64_FORMAT " since superuser jobs "
									 "are currently disallowed",
									 job->jobId)));
		}
		else
		{
			jobList = lappend(jobList, job);
		}

		MemoryContextSwitchTo(oldContext);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(cronJobTable, AccessShareLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);

	return jobList;
}

/*
 * TupleToCronJob takes a heap tuple and converts it into a CronJob
 * struct.
 */
static CronJob *
TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	CronJob *job = NULL;
	int64 jobKey = 0;
	bool isNull = false;
	bool isPresent = false;
	entry *parsedSchedule = NULL;

	Datum jobId = heap_getattr(heapTuple, Anum_pg_task_jobid,
							   tupleDescriptor, &isNull);
	Datum schedule = heap_getattr(heapTuple, Anum_pg_task_schedule,
								  tupleDescriptor, &isNull);
	Datum command = heap_getattr(heapTuple, Anum_pg_task_command,
								 tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_pg_task_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_pg_task_nodeport,
								  tupleDescriptor, &isNull);
	Datum database = heap_getattr(heapTuple, Anum_pg_task_database,
								  tupleDescriptor, &isNull);
	Datum userName = heap_getattr(heapTuple, Anum_pg_task_username,
								  tupleDescriptor, &isNull);

	jobKey = DatumGetInt64(jobId);
	job = hash_search(CronJobHash, &jobKey, HASH_ENTER, &isPresent);

	job->jobId = DatumGetInt64(jobId);
	job->scheduleText = TextDatumGetCString(schedule);
	job->command = TextDatumGetCString(command);
	job->nodeName = TextDatumGetCString(nodeName);
	job->nodePort = DatumGetInt32(nodePort);
	job->userName = TextDatumGetCString(userName);
	job->database = TextDatumGetCString(database);

	if (HeapTupleHeaderGetNatts(heapTuple->t_data) >= Anum_pg_task_active)
	{
		Datum active = heap_getattr(heapTuple, Anum_pg_task_active,
								tupleDescriptor, &isNull);
		Assert(!isNull);
		job->active = DatumGetBool(active);
	}
	else
	{
		job->active = true;
	}

	if (tupleDescriptor->natts >= Anum_pg_task_jobname)
	{
		bool isJobNameNull = false;
		Datum jobName = heap_getattr(heapTuple, Anum_pg_task_jobname,
									 tupleDescriptor, &isJobNameNull);
		if (!isJobNameNull)
		{
			job->jobName = TextDatumGetCString(jobName);
		}
		else
		{
			job->jobName = NULL;
		}
	}

	parsedSchedule = ParseSchedule(job->scheduleText);
	if (parsedSchedule != NULL)
	{
		/* copy the schedule and free the allocated memory immediately */

		job->schedule = *parsedSchedule;
		free_entry(parsedSchedule);
	}
	else
	{
		ereport(LOG, (errmsg("invalid pg_cron schedule for job " INT64_FORMAT ": %s",
							 job->jobId, job->scheduleText)));

		/* a zeroed out schedule never runs */
		memset(&job->schedule, 0, sizeof(entry));
	}

	return job;
}

void
InsertJobRunDetail(int64 runId, int64 *jobId, char *database, char *username, char *command, char *status)
{
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (RecoveryInProgress())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	TaskRunHistoryCreate(runId, jobId, database, username, command, status);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
}

void
UpdateJobRunDetail(int64 runId, int32 *job_pid, char *status, char *return_message,
				   TimestampTz *start_time,TimestampTz *end_time)
{
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (RecoveryInProgress())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	TaskRunHistoryUpdate(runId, job_pid, status, return_message, start_time, end_time);

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
}

void
AlterCronJob(int64 jobId, char *schedule, char *command,
			 char *database_name, char *username, bool *active)
{
	AclResult aclresult;
	Oid userIdcheckacl;
	Oid savedUserId;
	int savedSecurityContext;
	entry *parsedSchedule = NULL;

	userIdcheckacl = GetUserId();

	savedUserId = InvalidOid;
	savedSecurityContext = 0;

	if (RecoveryInProgress())
	{
		return;
	}

	/* username has been provided */
	if (username != NULL)
	{
		if (!superuser())
			elog(ERROR, "must be superuser to alter username");

		userIdcheckacl = GetRoleOidIfCanLogin(username);
	}

	if (!task_enable_superuser_jobs && superuser_arg(userIdcheckacl))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cannot schedule jobs as superuser"),
						errdetail("Scheduling jobs as superuser is disallowed when "
								  "task_enable_superuser_jobs is set to off.")));
	}

	/* database has been provided */
	if (database_name != NULL)
	{
		/* ensure the user that is used in the job can connect to the database */
		aclresult = pg_database_aclcheck(get_database_oid(database_name, false),
										 userIdcheckacl, ACL_CONNECT);

		if (aclresult != ACLCHECK_OK)
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("User %s does not have CONNECT privilege on %s",
									GetUserNameFromId(userIdcheckacl, false), database_name)));
	}

	/* ensure schedule is valid */
	if (schedule != NULL)
	{
		parsedSchedule = ParseSchedule(schedule);

		if (parsedSchedule == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid schedule: %s", schedule),
					errhint("Use cron format (e.g. 5 4 * * *), or interval "
							"format '[1-59] seconds'")));
		}

		free_entry(parsedSchedule);
	}

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);

	TaskUpdate(jobId, schedule, command, database_name, username, active);
	
	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	InvalidateJobCache();
}

void
MarkPendingRunsAsFailed(void)
{
	MemoryContext originalContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (RecoveryInProgress())
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		MemoryContextSwitchTo(originalContext);
		return;
	}

	MarkRunningTaskAsFailed();

	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);
}

char *
GetCronStatus(CronStatus cronstatus)
{
	char *statusDesc = "unknown status";

	switch (cronstatus)
	{
	case CRON_STATUS_STARTING:
		statusDesc = "starting";
		break;
	case CRON_STATUS_RUNNING:
		statusDesc = "running";
		break;
	case CRON_STATUS_SENDING:
		statusDesc = "sending";
		break;
	case CRON_STATUS_CONNECTING:
		statusDesc = "connecting";
		break;
	case CRON_STATUS_SUCCEEDED:
		statusDesc = "succeeded";
		break;
	case CRON_STATUS_FAILED:
		statusDesc = "failed";
		break;
	default:
		break;
	}
	return statusDesc;
}

/*
 * ParseSchedule attempts to parse a cron schedule or an interval in seconds.
 * The returned pointer is allocated using malloc and should be freed by the
 * caller.
 */
static entry *
ParseSchedule(char *scheduleText)
{
	uint32 secondsInterval = 0;

	/*
	 * First try to parse as a cron schedule.
	 */
	entry *schedule = parse_cron_entry(scheduleText);
	if (schedule != NULL)
	{
		/* valid cron schedule */
		return schedule;
	}

	/*
	 * Parse as interval on seconds.
	 */
	if (TryParseInterval(scheduleText, &secondsInterval))
	{
		entry *schedule = calloc(sizeof(entry), sizeof(char));
		schedule->secondsInterval = secondsInterval;
		return schedule;
	}

	elog(LOG, "failed to parse schedule: %s", scheduleText);
	return NULL;
}


/*
 * TryParseInterval returns whether scheduleText is of the form
 * <positive number> second[s].
 */
static bool
TryParseInterval(char *scheduleText, uint32 *secondsInterval)
{
	char lastChar = '\0';
	char plural = '\0';
	char extra = '\0';
	char *lowercaseSchedule = asc_tolower(scheduleText, strlen(scheduleText));

	int numParts = sscanf(lowercaseSchedule, " %u secon%c%c %c", secondsInterval,
						  &lastChar, &plural, &extra);
	if (lastChar != 'd')
	{
		/* value did not have a "second" suffix */
		return false;
	}

	if (numParts == 2)
	{
		/* <number> second (allow "2 second") */
		return 0 < *secondsInterval && *secondsInterval < 60;
	}
	else if (numParts == 3 && plural == 's')
	{
		/* <number> seconds (allow "1 seconds") */
		return 0 < *secondsInterval && *secondsInterval < 60;
	}

	return false;
}
