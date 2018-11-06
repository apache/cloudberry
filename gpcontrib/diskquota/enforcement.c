/* -------------------------------------------------------------------------
 *
 * enforcment.c
 *
 * This code registers enforcement hooks to cancle the query which exceeds 
 * the quota limit.
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/diskquota/enforcement.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "storage/bufmgr.h"

#include "diskquota.h"

static bool quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
static bool quota_check_ReadBufferExtendCheckPerms(Oid reloid, BlockNumber blockNum);

static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static BufferExtendCheckPerms_hook_type prev_BufferExtendCheckPerms_hook;

/*
 * Initialize enforcement hooks.
 */
void
init_disk_quota_enforcement(void)
{
	/* enforcement hook before query is loading data */
	prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
	ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;

	/* enforcement hook during query is loading data*/
	prev_BufferExtendCheckPerms_hook = BufferExtendCheckPerms_hook;
	BufferExtendCheckPerms_hook = quota_check_ReadBufferExtendCheckPerms;
}

/*
 * Enformcent hook function before query is loading data. Throws an error if 
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
		 * Only check quota on inserts. UPDATEs may well increase
		 * space usage too, but we ignore that for now.
		 */
		if ((rte->requiredPerms & ACL_INSERT) == 0 && (rte->requiredPerms & ACL_UPDATE) == 0)
			continue;

		/* Perform the check as the relation's owner and namespace */
		quota_check_common(rte->relid);

	}

	return true;
}

/*
 * Enformcent hook function when query is loading data. Throws an error if 
 * you try to extend a buffer page, and the quota has been exceeded.
 */
static bool
quota_check_ReadBufferExtendCheckPerms(Oid reloid, BlockNumber blockNum)
{
	bool isExtend;

	isExtend = (blockNum == P_NEW);
	/* if not buffer extend, we could skip quota limit check*/
	if (!isExtend)
	{
		return true;
	}

	/* Perform the check as the relation's owner and namespace */
	quota_check_common(reloid);
	return true;
}

