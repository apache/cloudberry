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
 *		diskquota/enforcement.c
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

static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;

/*
 * Initialize enforcement hooks.
 */
void
init_disk_quota_enforcement(void)
{
	/* enforcement hook before query is loading data */
	prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
	ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;
}

/*
 * Enforcement hook function before query is loading data. Throws an error if
 * you try to INSERT, UPDATE or COPY into a table, and the quota has been exceeded.
 */
static bool
quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation)
{
	ListCell	*l;

	foreach(l, rangeTable)
	{
		List	   	*indexIds;
		ListCell	*oid;
		Relation	relation;
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
		quota_check_common(rte->relid, NULL /*relfilenode*/);
		/* Check the indexes of the this relation */
		relation = try_relation_open(rte->relid, AccessShareLock, false);
		if (!relation)
			continue;

		indexIds = RelationGetIndexList(relation);
		PG_TRY();
		{
			if (indexIds != NIL )
			{
				foreach(oid, indexIds)
				{
					quota_check_common(lfirst_oid(oid), NULL /*relfilenode*/);
				}
			}
		}
		PG_CATCH();
		{
			relation_close(relation, AccessShareLock);
			list_free(indexIds);
			PG_RE_THROW();
		}
		PG_END_TRY();
		relation_close(relation, AccessShareLock);
		list_free(indexIds);
	}
	return true;
}
