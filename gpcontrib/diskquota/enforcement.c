/* -------------------------------------------------------------------------
 *
 * enforcment.c
 *
 * This code registers enforcement hooks to cancel the query which exceeds
 * the quota limit.
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		gpcontrib/gp_diskquota/enforcement.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_async.h"
#include "executor/executor.h"
#include "storage/bufmgr.h"
#include "utils/resowner.h"
#include "diskquota.h"

#define CHECKED_OID_LIST_NUM 64

static bool quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
static bool quota_check_DispatcherCheckPerms(void);

static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static DispatcherCheckPerms_hook_type prev_DispatcherCheckPerms_hook;
static void diskquota_free_callback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);

/* result relation need to be checked in dispatcher */
static Oid	checked_reloid_list[CHECKED_OID_LIST_NUM];
static int	checked_reloid_list_count = 0;

/*
 * Initialize enforcement hooks.
 */
void
init_disk_quota_enforcement(void)
{
	/* enforcement hook before query is loading data */
	prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
	ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;

	/* enforcement hook during query is loading data */
	prev_DispatcherCheckPerms_hook = DispatcherCheckPerms_hook;
	DispatcherCheckPerms_hook = quota_check_DispatcherCheckPerms;

	/* setup and reset the result relaiton checked list */
	memset(checked_reloid_list, 0, sizeof(Oid) * CHECKED_OID_LIST_NUM);
	RegisterResourceReleaseCallback(diskquota_free_callback, NULL);
}

/*
 * Reset checked reloid list
 * This may be called multiple times at different resource relase
 * phase, but it's safe to reset the checked_reloid_list.
 */
static void
diskquota_free_callback(ResourceReleasePhase phase,
						bool isCommit,
						bool isTopLevel,
						void *arg)
{

	checked_reloid_list_count = 0;
	return;
}

/*
 * Enforcement hook function before query is loading data. Throws an error if
 * you try to INSERT, UPDATE or COPY into a table, and the quota has been exceeded.
 */
static bool
quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation)
{
	ListCell   *l;

	foreach(l, rangeTable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		/* see ExecCheckRTEPerms() */
		if (rte->rtekind != RTE_RELATION)
			continue;

		/*
		 * Only check quota on inserts. UPDATEs may well increase space usage
		 * too, but we ignore that for now.
		 */
		if ((rte->requiredPerms & ACL_INSERT) == 0 && (rte->requiredPerms & ACL_UPDATE) == 0)
			continue;

		/*
		 * Given table oid, check whether the quota limit of table's schema or
		 * table's owner are reached. This function will ereport(ERROR) when
		 * quota limit exceeded.
		 */
		quota_check_common(rte->relid);
		checked_reloid_list[checked_reloid_list_count++] = rte->relid;

	}
	return true;
}

/*
 * Enformcent hook function when query is loading data. Throws an error if
 * the quota has been exceeded.
 */
static bool
quota_check_DispatcherCheckPerms(void)
{
	int			i;

	/* Perform the check as the relation's owner and namespace */
	for (i = 0; i < checked_reloid_list_count; i++)
	{
		Oid			relid = checked_reloid_list[i];

		/*
		 * Given table oid, check whether the quota limit of table's schema or
		 * table's owner are reached. This function will ereport(ERROR) when
		 * quota limit exceeded.
		 */
		quota_check_common(relid);
	}
	return true;
}
