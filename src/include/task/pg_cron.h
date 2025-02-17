/*-------------------------------------------------------------------------
 *
 * pg_cron.h
 *	  definition of pg_cron data types
 *
 * Copyright (c) 2010-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_CRON_H
#define PG_CRON_H

/* GUC settings */
extern char *task_host_addr;
extern int  max_running_tasks;
extern bool task_enable_superuser_jobs;
extern bool task_log_run;
extern bool task_log_statement;
extern bool task_use_background_worker;
extern char *task_timezone;

/* Shared memory area for pg cron process */
typedef struct PgCronData
{
	pid_t cron_pid;		/* pid of pg cron process */
} PgCronData;

extern void PgCronLauncherMain(Datum arg);
extern bool PgCronStartRule(Datum main_arg);
extern void CronBackgroundWorker(Datum arg);
extern pid_t PgCronLauncherPID(void);
extern Size PgCronLauncherShmemSize(void);
extern void PgCronLauncherShmemInit(void);
extern void assign_task_timezone(const char *newval, void *extra);
extern const char *show_task_timezone(void);

#endif      /* PG_CRON_H */
