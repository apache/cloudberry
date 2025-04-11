/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes_redo_test.c
 *	  code to test functionality from storage_pending_deletes_redo.c
 *
 * Copyright (c) 2025 Greengage Community
 *
 *	  src/backend/catalog/test/storage_pending_deletes_redo_test.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#include "access/clog.h"
#include "access/transam.h"
#include "catalog/storage_pending_deletes_redo.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define TEST_TABLESPACE_OID1	11111
#define TEST_TABLESPACE_OID2	11112

#define TEST_DB_OID1			11121
#define TEST_DB_OID2			11122

#define TEST_REL_OID1			11211
#define TEST_REL_OID2			11212

#define TEST_XID 10

#define TEST_XLOG_REC_PTR 100

void
__wrap_DropRelationFiles(RelFileNodePendingDelete *delrels,
								int ndelrels,
								bool isRedo);

XidStatus
__wrap_TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);

PendingRelXactDeleteArray *
__wrap_PdlXLogShmemDump(void);

XLogRecPtr
__wrap_XLogInsert(RmgrId rmid, uint8 info, XLogRecData *rdata);

void
			__wrap_XLogFlush(XLogRecPtr record);

/* id of test, which is currently being executed */
static int	test_number = 0;

/*
 * counter to accumulate how many times DropRelationFiles() was called during
 * test
 */
static int	DropRelationFiles_call_count = 0;

/* counter to accumulate how many times XLogInsert() was called during test */
static int	XLogInsert_call_count = 0;

/*
 * counter to accumulate how many times PdlXLogShmemDump() was called during
 * test
 */
static int	PdlXLogShmemDump_call_count = 0;

/*
 * array of relnodes expected by test, in case there are more than 1-2 nodes
 * involved
 */
#define TEST_EXPECTED_NOTES_COUNT 20
static RelFileNode test_expected_relnodes[TEST_EXPECTED_NOTES_COUNT];

/*
 * List with transaction IDs, that will report complete status from
 * TransactionIdGetStatus().
 */
static List *ls_transactions_comlpete = NIL;

static void
setup(int test)
{
	static VariableCacheData test_cache = {0};

	ShmemVariableCache = &test_cache;

	DropRelationFiles_call_count = 0;
	XLogInsert_call_count = 0;
	PdlXLogShmemDump_call_count = 0;

	test_number = test;
}

void
__wrap_DropRelationFiles(RelFileNodePendingDelete *delrels,
								int ndelrels,
								bool isRedo)
{
	DropRelationFiles_call_count++;
	switch (test_number)
	{
		case 1:
		case 8:
		case 9:
		case 13:
		case 18:
		case 19:
			{
				assert_int_equal(ndelrels, 1);
				assert_true(isRedo);
				RelFileNodePendingDelete *pd = &(delrels[0]);

				assert_false(pd->isTempRelation);
				assert_int_equal(pd->node.spcNode, TEST_TABLESPACE_OID1);
				assert_int_equal(pd->node.dbNode, TEST_DB_OID1);
				assert_int_equal(pd->node.relNode, TEST_REL_OID1);
				break;
			}
		case 3:
			{
				static RelFileNode test_3_expected_results[] =
				{
					[0] =
					{
						.spcNode = TEST_TABLESPACE_OID1,
						.dbNode = TEST_DB_OID1,
						.relNode = TEST_REL_OID1,
					},
					[1] =
					{
						.spcNode = TEST_TABLESPACE_OID2,
						.dbNode = TEST_DB_OID2,
						.relNode = TEST_REL_OID2,
					}
				};

				assert_int_equal(ndelrels, 1);
				assert_true(isRedo);
				RelFileNodePendingDelete *pd = &(delrels[0]);

				/*
				 * We can't guarantee that the order of relnodes dropping will
				 * be the same as the order of adding the pending delete
				 * nodes. So we just need to ensure that we got all the
				 * expected relnodes (and only them). We check it by excluding
				 * values from the array of expected relnodes by replacing
				 * them with InvalidOid. And we will check that all values are
				 * excluded as the last step.
				 */
				for (int i = 0; i < ARRAY_SIZE(test_3_expected_results); i++)
				{
					if (RelFileNodeEquals(test_3_expected_results[i], pd->node))
					{
						test_3_expected_results[i].spcNode = InvalidOid;
						test_3_expected_results[i].dbNode = InvalidOid;
						test_3_expected_results[i].relNode = InvalidOid;
					}
				}

				if (DropRelationFiles_call_count == 2)
				{
					for (int i = 0; i < ARRAY_SIZE(test_3_expected_results); i++)
					{
						assert_int_equal(test_3_expected_results[i].spcNode,
										 InvalidOid);
						assert_int_equal(test_3_expected_results[i].dbNode,
										 InvalidOid);
						assert_int_equal(test_3_expected_results[i].relNode,
										 InvalidOid);
					}
				}

				break;
			}
		case 4:
			{
				assert_int_equal(ndelrels, 2);
				assert_true(isRedo);

				RelFileNodePendingDelete *pd;

				pd = &(delrels[0]);
				assert_false(pd->isTempRelation);
				assert_int_equal(pd->node.spcNode, TEST_TABLESPACE_OID1);
				assert_int_equal(pd->node.dbNode, TEST_DB_OID1);
				assert_int_equal(pd->node.relNode, TEST_REL_OID1);

				pd = &(delrels[1]);
				assert_false(pd->isTempRelation);
				assert_int_equal(pd->node.spcNode, TEST_TABLESPACE_OID2);
				assert_int_equal(pd->node.dbNode, TEST_DB_OID2);
				assert_int_equal(pd->node.relNode, TEST_REL_OID2);

				break;
			}
		case 5:
		case 11:
		case 12:
		case 14:
			{
				assert_int_equal(ndelrels, 1);
				assert_true(isRedo);
				RelFileNodePendingDelete *pd = &(delrels[0]);

				assert_false(pd->isTempRelation);

				/*
				 * We can't guarantee that the order of relnodes dropping will
				 * be the same as the order of adding the pending delete
				 * nodes. So we just need to ensure that we got all the
				 * expected relnodes (and only them). We check it by excluding
				 * values from the array of expected relnodes by replacing
				 * them with InvalidOid. And we will check that all values are
				 * excluded in the end of the test.
				 */
				for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
				{
					assert_true(pd->node.relNode != InvalidOid);
					if (RelFileNodeEquals(test_expected_relnodes[i], pd->node))
					{
						test_expected_relnodes[i].relNode = InvalidOid;
						return;
					}
				}

				/*
				 * If we are here, then we didn't find the relnode in the
				 * expected data, and it is a problem, so fail.
				 */
				assert_true(false);
				break;
			}
		default:
			{
				/* we shouldn't even get here */
				assert_true(false);
				break;
			}
	}
}

XidStatus
__wrap_TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	ListCell   *cell;

	foreach(cell, ls_transactions_comlpete)
	{
		TransactionId xid_complete = (TransactionId) lfirst_int(cell);

		if (xid == xid_complete)
			return TRANSACTION_STATUS_COMMITTED;
	}
	return TRANSACTION_STATUS_IN_PROGRESS;
}

PendingRelXactDeleteArray *
__wrap_PdlXLogShmemDump(void)
{
	PdlXLogShmemDump_call_count++;
	if (test_number == 16)
		return NULL;

	/* return something valid */
	int			node_count = 1;

	char	   *buffer = palloc(PdlDumpSize(node_count));

	PendingRelXactDeleteArray *pending_deletes =
		(PendingRelXactDeleteArray *) buffer;

	pending_deletes->count = node_count;

	PendingRelXactDelete *pd = &(pending_deletes->array[0]);

	pd->xid = TEST_XID;
	pd->relnode.isTempRelation = false;
	pd->relnode.node.spcNode = TEST_TABLESPACE_OID1;
	pd->relnode.node.dbNode = TEST_DB_OID1;
	pd->relnode.node.relNode = TEST_REL_OID1;

	return pending_deletes;
}

XLogRecPtr
__wrap_XLogInsert(RmgrId rmid, uint8 info, XLogRecData *rdata)
{
	assert_int_equal(test_number, 17);	/* currently we should get here only
										 * in test_17 */
	XLogInsert_call_count++;

	assert_int_equal(rmid, RM_XLOG_ID);
	assert_int_equal(info, XLOG_PENDING_DELETE);
	assert_int_equal(rdata->buffer, InvalidBuffer);
	assert_false(rdata->buffer_std);
	assert_true(rdata->next == NULL);
	assert_true(rdata->len == (sizeof(Size) + sizeof(PendingRelXactDelete)));

	PendingRelXactDeleteArray *pending_deletes =
		(PendingRelXactDeleteArray *) rdata->data;

	assert_int_equal(pending_deletes->count, 1);

	PendingRelXactDelete *pd = &(pending_deletes->array[0]);

	assert_int_equal(pd->xid, TEST_XID);
	assert_false(pd->relnode.isTempRelation);
	assert_int_equal(pd->relnode.node.spcNode, TEST_TABLESPACE_OID1);
	assert_int_equal(pd->relnode.node.dbNode, TEST_DB_OID1);
	assert_int_equal(pd->relnode.node.relNode, TEST_REL_OID1);

	return TEST_XLOG_REC_PTR;
}

void
__wrap_XLogFlush(XLogRecPtr record)
{
	assert_int_equal(test_number, 17);	/* currently we should get here only
										 * in test_17 */
	assert_int_equal(record, TEST_XLOG_REC_PTR);
}

/*
 * Tests
 */

/*
 * Scenario:
 * add single pending delete node
 * and then drop files.
 */
static void
test_1(void **state)
{
	setup(1);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);
}

/*
 * Scenario:
 * add single pending delete node
 * and datfrozenxid is above the node's xid
 * and then drop files.
 */
static void
test_2(void **state)
{
	setup(2);
	ShmemVariableCache->oldestXid = (TransactionId) 2;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);
}

/*
 * Scenario:
 * add 2 pending delete nodes with different xids and different relnodes
 * and then drop files.
 */
static void
test_3(void **state)
{
	setup(3);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	pd.xid = (TransactionId) 2;
	pd.relnode.node.spcNode = TEST_TABLESPACE_OID2;
	pd.relnode.node.dbNode = TEST_DB_OID2;
	pd.relnode.node.relNode = TEST_REL_OID2;

	PdlRedoAdd(&pd);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 2);
}

/*
 * Scenario:
 * add 2 pending delete nodes with same xid and different relnodes
 * and then drop files.
 */
static void
test_4(void **state)
{
	setup(4);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	pd.relnode.node.spcNode = TEST_TABLESPACE_OID2;
	pd.relnode.node.dbNode = TEST_DB_OID2;
	pd.relnode.node.relNode = TEST_REL_OID2;

	PdlRedoAdd(&pd);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);
}

/*
 * Scenario:
 * add many pending delete nodes with different xids and different relnodes
 * and some xids precede datfrozenxid
 * and some transactions are not in progress
 * and then drop files.
 */
static void
test_5(void **state)
{
	setup(5);
	ShmemVariableCache->oldestXid = (TransactionId) 5;

	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		PendingRelXactDelete pd =
		{
			.xid = (TransactionId) i,
			.relnode.isTempRelation = false,
			.relnode.node.spcNode = TEST_TABLESPACE_OID1,
			.relnode.node.dbNode = TEST_DB_OID1,
			.relnode.node.relNode = TEST_REL_OID1 + i
		};

		PdlRedoAdd(&pd);

		/* and fill data which is expected... */
		if (TransactionIdPrecedes(pd.xid, ShmemVariableCache->oldestXid))
			test_expected_relnodes[i].relNode = (Oid) -1;
		else
			test_expected_relnodes[i] = pd.relnode.node;
	}

	/* mark some transactions as complete, let's say XIDs: 10, 12, 15 */
	TransactionId complete_xids[] = {10, 12, 15};
	int			complete_xids_count = ARRAY_SIZE(complete_xids);

	for (int i = 0; i < complete_xids_count; i++)
	{
		ls_transactions_comlpete = lappend_int(ls_transactions_comlpete,
											   complete_xids[i]);
	}

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count,
					TEST_EXPECTED_NOTES_COUNT - ShmemVariableCache->oldestXid -
					complete_xids_count);

	/* Check that data for complete xids is not touched by PdlRedoDropFiles */
	for (int i = 0; i < complete_xids_count; i++)
	{
		ls_transactions_comlpete = lappend_int(ls_transactions_comlpete,
											   complete_xids[i]);
		assert_int_equal(test_expected_relnodes[complete_xids[i]].relNode,
						 TEST_REL_OID1 + complete_xids[i]);
		/* Replace it with InvalidOid to simplify further check */
		test_expected_relnodes[complete_xids[i]].relNode = InvalidOid;
	}

	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		/*
		 * Check that data for xids preceding datfrozenxid is not touched by
		 * PdlRedoDropFiles, while all other is replaced with InvalidOid.
		 */
		if (TransactionIdPrecedes((TransactionId) i, ShmemVariableCache->oldestXid))
			assert_int_equal(test_expected_relnodes[i].relNode, (Oid) -1);
		else
			assert_int_equal(test_expected_relnodes[i].relNode, InvalidOid);
	}

	list_free(ls_transactions_comlpete);
	ls_transactions_comlpete = NIL;
}

/*
 * Scenario:
 * add single pending delete node
 * and transaction status of the node is not in progress
 * and then drop files.
 */
static void
test_6(void **state)
{
	setup(6);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	ls_transactions_comlpete = lappend_int(ls_transactions_comlpete, pd.xid);

	PdlRedoAdd(&pd);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);

	list_free(ls_transactions_comlpete);
	ls_transactions_comlpete = NIL;
}

/*
 * Scenario:
 * add single pending delete node
 * and remove pending deletes for that node's xid
 * and then drop files.
 */
static void
test_7(void **state)
{
	setup(7);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	PdlRedoRemoveTree(pd.xid, NULL, 0);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);
}

/*
 * Scenario:
 * add single pending delete node
 * and remove pending deletes for different xid
 * and then drop files.
 */
static void
test_8(void **state)
{
	setup(8);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	PdlRedoRemoveTree(pd.xid + 1, NULL, 0);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);
}

/*
 * Scenario:
 * add single pending delete node
 * and remove pending deletes for invalid xid
 * and then drop files.
 */
static void
test_9(void **state)
{
	setup(9);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	PdlRedoRemoveTree(InvalidTransactionId, NULL, 0);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);
}

/*
 * Scenario:
 * add several pending delete nodes with the same xid
 * and remove pending deletes for that xid
 * and then drop files.
 */
static void
test_10(void **state)
{
	setup(10);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	PdlRedoAdd(&pd);

	pd.relnode.node.relNode = TEST_REL_OID2;

	PdlRedoAdd(&pd);

	pd.relnode.node.dbNode = TEST_DB_OID2;
	pd.relnode.node.relNode = TEST_REL_OID1;

	PdlRedoAdd(&pd);

	PdlRedoRemoveTree(pd.xid, NULL, 0);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);
}

/*
 * Scenario:
 * add several pending delete nodes with the different xids
 * and remove pending deletes for one of the xids
 * and then drop files.
 */
static void
test_11(void **state)
{
	setup(11);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd = {0};

	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		PendingRelXactDelete pd =
		{
			/* add oldest xid here just to ensure that all nodes will be added */
			.xid = ShmemVariableCache->oldestXid + (TransactionId) i,
			.relnode.isTempRelation = false,
			.relnode.node.spcNode = TEST_TABLESPACE_OID1,
			.relnode.node.dbNode = TEST_DB_OID1,
			.relnode.node.relNode = TEST_REL_OID1 + i
		};

		PdlRedoAdd(&pd);

		/* and fill data which is expected... */
		test_expected_relnodes[i] = pd.relnode.node;
	}

	PdlRedoAdd(&pd);

	TransactionId xid_to_remove = 5;

	PdlRedoRemoveTree(xid_to_remove, NULL, 0);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, TEST_EXPECTED_NOTES_COUNT - 1);

	/*
	 * Check that data for removed xids is not touched by PdlRedoDropFiles and
	 * replace it with InvalidOid to simplify further check.
	 */
	int			idx = xid_to_remove - ShmemVariableCache->oldestXid;

	assert_int_equal(test_expected_relnodes[idx].relNode, TEST_REL_OID1 + idx);
	test_expected_relnodes[idx].relNode = InvalidOid;

	/*
	 * Check that all other are replaced with InvalidOid.
	 */
	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		assert_int_equal(test_expected_relnodes[i].relNode, InvalidOid);
	}
}

/*
 * Scenario:
 * add several pending delete nodes with the different xids
 * and remove pending deletes for one of the xids + some sub_xids
 * and then drop files.
 */
static void
test_12(void **state)
{
	setup(12);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		PendingRelXactDelete pd =
		{
			/* add oldest xid here just to ensure that all nodes will be added */
			.xid = ShmemVariableCache->oldestXid + (TransactionId) i,
			.relnode.isTempRelation = false,
			.relnode.node.spcNode = TEST_TABLESPACE_OID1,
			.relnode.node.dbNode = TEST_DB_OID1,
			.relnode.node.relNode = TEST_REL_OID1 + i
		};

		PdlRedoAdd(&pd);

		/* and fill data which is expected... */
		test_expected_relnodes[i] = pd.relnode.node;
	}

	TransactionId xid_to_remove = 5;
	TransactionId sub_xids_to_remove[] = {10, 11, 12, 15};
	int			nsubxacts = ARRAY_SIZE(sub_xids_to_remove);

	PdlRedoRemoveTree(xid_to_remove, sub_xids_to_remove, nsubxacts);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, TEST_EXPECTED_NOTES_COUNT - 5);

	/*
	 * Check that data for removed xids is not touched by PdlRedoDropFiles and
	 * replace it with InvalidOid to simplify further check...
	 */
	int			idx = xid_to_remove - ShmemVariableCache->oldestXid;

	assert_int_equal(test_expected_relnodes[idx].relNode, TEST_REL_OID1 + idx);
	test_expected_relnodes[idx].relNode = InvalidOid;
	/* ...including all subtransactions. */
	for (int j = 0; j < nsubxacts; j++)
	{
		idx = sub_xids_to_remove[j] - ShmemVariableCache->oldestXid;
		assert_int_equal(test_expected_relnodes[idx].relNode, TEST_REL_OID1 + idx);
		test_expected_relnodes[idx].relNode = InvalidOid;
	}

	/*
	 * Check that now all expected nodes are replaced with InvalidOid.
	 */
	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		assert_int_equal(test_expected_relnodes[i].relNode, InvalidOid);
	}
}

static XLogRecord *
test_create_xlog_record(int pending_deletes_count)
{
	Size buffer_size = SizeOfXLogRecord + sizeof(Size) + 
		sizeof(PendingRelXactDelete) * pending_deletes_count;

	return (XLogRecord *)palloc0(buffer_size);
}

/*
 * Scenario:
 * process PENDING_DELETE wal record with 1 pending delete node
 * and then drop files.
 */
static void
test_13(void **state)
{
	setup(13);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	int			pending_deletes_count = 1;

	XLogRecord *record = test_create_xlog_record(pending_deletes_count);

	PendingRelXactDeleteArray *pending_deletes =
		(PendingRelXactDeleteArray *) ((char *) record + SizeOfXLogRecord);

	pending_deletes->count = pending_deletes_count;

	PendingRelXactDelete *pd = &(pending_deletes->array[0]);

	pd->xid = (TransactionId) 1;
	pd->relnode.isTempRelation = false;
	pd->relnode.node.spcNode = TEST_TABLESPACE_OID1;
	pd->relnode.node.dbNode = TEST_DB_OID1;
	pd->relnode.node.relNode = TEST_REL_OID1;

	PdlRedoXLogRecord(record);

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);

	pfree(record);
}


/*
 * Scenario:
 * process PENDING_DELETE wal record with several pending delete nodes
 * and datfrozenxid is above the some node's xid
 * and some transactions are not in progress
 * and then drop files.
 */
static void
test_14(void **state)
{
	setup(14);
	ShmemVariableCache->oldestXid = (TransactionId) 2;

	int			pending_deletes_count = 5;

	XLogRecord *record = test_create_xlog_record(pending_deletes_count);

	PendingRelXactDeleteArray *pending_deletes =
		(PendingRelXactDeleteArray *) ((char *) record + SizeOfXLogRecord);

	pending_deletes->count = pending_deletes_count;

	memset(test_expected_relnodes, 0, sizeof(test_expected_relnodes));

	for (int i = 0; i < pending_deletes_count; i++)
	{
		PendingRelXactDelete *pd = &(pending_deletes->array[i]);

		pd->xid = (TransactionId) (i + 1);
		pd->relnode.isTempRelation = false;
		pd->relnode.node.spcNode = TEST_TABLESPACE_OID1;
		pd->relnode.node.dbNode = TEST_DB_OID1;
		pd->relnode.node.relNode = TEST_REL_OID1 + i;

		test_expected_relnodes[i] = pd->relnode.node;
	}

	/* mark some transaction as complete, let's say XID: 3 */
	ls_transactions_comlpete = lappend_int(ls_transactions_comlpete,
										   (TransactionId) 3);

	PdlRedoXLogRecord(record);

	PdlRedoDropFiles();

	/*
	 * The xids that should have been skipped due to datfrozenxid or
	 * transaction status. Their enties in the expected nodes should be
	 * untouched. Check it and replace it with InvalidOid to simplify further
	 * check...
	 */
	TransactionId skipped_xids[] = {1, 3};

	for (int i = 0; i < ARRAY_SIZE(skipped_xids); i++)
	{
		int			idx = skipped_xids[i] - 1;

		assert_int_equal(test_expected_relnodes[idx].relNode,
						 TEST_REL_OID1 + idx);
		test_expected_relnodes[idx].relNode = InvalidOid;
	}

	assert_int_equal(DropRelationFiles_call_count, 3);

	/*
	 * Check that now all expected nodes are replaced with InvalidOid.
	 */
	for (int i = 0; i < TEST_EXPECTED_NOTES_COUNT; i++)
	{
		assert_int_equal(test_expected_relnodes[i].relNode, InvalidOid);
	}

	pfree(record);

	list_free(ls_transactions_comlpete);
	ls_transactions_comlpete = NIL;
}


/*
 * Scenario:
 * check PdlXlogInsert() if PdlXLogShmemDump returned NULL.
 */
static void
test_16(void **state)
{
	setup(16);

	PdlXLogInsert();

	assert_int_equal(PdlXLogShmemDump_call_count, 1);
	assert_int_equal(XLogInsert_call_count, 0);
}

/*
 * Scenario:
 * check PdlXlogInsert() if PdlXLogShmemDump provided valid nodes.
 */
static void
test_17(void **state)
{
	setup(17);

	PdlXLogInsert();

	assert_int_equal(PdlXLogShmemDump_call_count, 1);
	assert_int_equal(XLogInsert_call_count, 1);
}

/*
 * Scenario:
 * guc is disabled
 */
static void
test_18(void **state)
{
	setup(18);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	gp_track_pending_delete = false;
	PdlRedoAdd(&pd);
	gp_track_pending_delete = true;

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);

	PdlRedoAdd(&pd);

	gp_track_pending_delete = false;
	PdlRedoDropFiles();
	gp_track_pending_delete = true;

	assert_int_equal(DropRelationFiles_call_count, 0);

	gp_track_pending_delete = false;
	PdlRedoRemoveTree(pd.xid, NULL, 0);
	gp_track_pending_delete = true;
	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);

	gp_track_pending_delete = false;
	PdlXLogInsert();
	gp_track_pending_delete = true;

	assert_int_equal(PdlXLogShmemDump_call_count, 0);
	assert_int_equal(XLogInsert_call_count, 0);
}

/*
 * Scenario:
 * IsBootstrapProcessingMode is true
 */
static void
test_19(void **state)
{
	setup(19);
	ShmemVariableCache->oldestXid = (TransactionId) 1;

	PendingRelXactDelete pd =
	{
		.xid = (TransactionId) 1,
		.relnode.isTempRelation = false,
		.relnode.node.spcNode = TEST_TABLESPACE_OID1,
		.relnode.node.dbNode = TEST_DB_OID1,
		.relnode.node.relNode = TEST_REL_OID1
	};

	Mode = BootstrapProcessing;
	PdlRedoAdd(&pd);
	Mode = NormalProcessing;

	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 0);

	PdlRedoAdd(&pd);

	Mode = BootstrapProcessing;
	PdlRedoDropFiles();
	Mode = NormalProcessing;

	assert_int_equal(DropRelationFiles_call_count, 0);

	Mode = BootstrapProcessing;
	PdlRedoRemoveTree(pd.xid, NULL, 0);
	Mode = NormalProcessing;
	PdlRedoDropFiles();

	assert_int_equal(DropRelationFiles_call_count, 1);

	Mode = BootstrapProcessing;
	PdlXLogInsert();
	Mode = NormalProcessing;

	assert_int_equal(PdlXLogShmemDump_call_count, 0);
	assert_int_equal(XLogInsert_call_count, 0);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_1),
		unit_test(test_2),
		unit_test(test_3),
		unit_test(test_4),
		unit_test(test_5),
		unit_test(test_6),
		unit_test(test_7),
		unit_test(test_8),
		unit_test(test_9),
		unit_test(test_10),
		unit_test(test_11),
		unit_test(test_12),
		unit_test(test_13),
		unit_test(test_14),
		unit_test(test_16),
		unit_test(test_17),
		unit_test(test_18),
		unit_test(test_19)
	};

	MemoryContextInit();

	return run_tests(tests);
}
