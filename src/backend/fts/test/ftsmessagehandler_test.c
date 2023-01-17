/*-------------------------------------------------------------------------
*
* ftsmessagehandler_test.c
*
* Portions Copyright (c) 2023, HashData Technology Limited.
*
*
*--------------------------------------------------------------------------
*/

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"

/* Actual function body */
#include "../ftsmessagehandler.c"

static XLogRecPtr fakeRedoRecPtr = 8948;

static void
expectSendFtsResponse(const QueryCompletion *qc, const FtsResponse *expectedResponse)
{
	expect_value(BeginCommand, commandTag, qc->commandTag);
	expect_value(BeginCommand, dest, DestRemote);
	will_be_called(BeginCommand);

	/* schema message */
	expect_any(pq_beginmessage, buf);
	expect_value(pq_beginmessage, msgtype, 'T');
	will_be_called(pq_beginmessage);

	expect_any(pq_endmessage, buf);
	will_be_called(pq_endmessage);

	/* data message */
	expect_any(pq_beginmessage, buf);
	expect_value(pq_beginmessage, msgtype, 'D');
	will_be_called(pq_beginmessage);

	expect_any(pq_endmessage, buf);
	will_be_called(pq_endmessage);

	expect_any_count(pq_sendint, buf, -1);
	expect_any_count(pq_sendint, b, -1);

	/* verify the schema */
	expect_value(pq_sendint, i, Natts_fts_message_response);
	expect_any_count(pq_sendint, i, Natts_fts_message_response * 6 /* calls_per_column_for_schema */);

	/* verify the data */
	expect_value(pq_sendint, i, Natts_fts_message_response);
	expect_value(pq_sendint, i, 1);
  	expect_value(pq_sendint, i, expectedResponse->IsMirrorUp);
	expect_value(pq_sendint, i, 1);
	expect_value(pq_sendint, i, expectedResponse->IsInSync);
	expect_value(pq_sendint, i, 1);
	expect_value(pq_sendint, i, expectedResponse->IsSyncRepEnabled);
	expect_value(pq_sendint, i, 1);
	expect_value(pq_sendint, i, expectedResponse->IsRoleMirror);
	expect_value(pq_sendint, i, 1);
	expect_value(pq_sendint, i, expectedResponse->RequestRetry);

	will_be_called_count(pq_sendint, -1);

	expect_any_count(pq_sendstring, buf, -1);
	expect_any_count(pq_sendstring, str, -1);
	will_be_called_count(pq_sendstring, Natts_fts_message_response);

	// Be careful for the padding bits
	expect_memory(EndCommand, qc, qc, sizeof(*qc));
	expect_value(EndCommand, dest, DestRemote);
	expect_value(EndCommand, force_undecorated_output, false);
	will_be_called(EndCommand);

	will_be_called(socket_flush);
}

static void
test_HandleFtsWalRepProbePrimary(void **state)
{
	QueryCompletion qc = {0};
	qc.commandTag = CMDTAG_FTS_MSG_PROBE;

	FtsResponse mockresponse;
	mockresponse.IsMirrorUp = true;
	mockresponse.IsInSync = true;
	mockresponse.IsSyncRepEnabled = false;
	mockresponse.IsRoleMirror = false;
	mockresponse.RequestRetry = false;

	expect_any(GetMirrorStatus, response);
	will_assign_memory(GetMirrorStatus, response, &mockresponse, sizeof(FtsResponse));
	will_be_called(GetMirrorStatus);

	will_be_called(SetSyncStandbysDefined);

	/* SyncRep should be enabled as soon as we found mirror is up. */
	mockresponse.IsSyncRepEnabled = true;
	expectSendFtsResponse(&qc, &mockresponse);

	HandleFtsWalRepProbe();
}

static void
test_HandleFtsWalRepSyncRepOff(void **state)
{
	QueryCompletion qc = {0};
	qc.commandTag = CMDTAG_FTS_MSG_SYNCREP_OFF;

	FtsResponse mockresponse;
	mockresponse.IsMirrorUp = false;
	mockresponse.IsInSync = false;
	mockresponse.RequestRetry = false;
	/* unblock primary if FTS requests it */
	mockresponse.IsSyncRepEnabled = false;
	mockresponse.RequestRetry = false;

	expect_any(GetMirrorStatus, response);
	will_assign_memory(GetMirrorStatus, response, &mockresponse, sizeof(FtsResponse));
	will_be_called(GetMirrorStatus);

	will_be_called(UnsetSyncStandbysDefined);

	/* since this function doesn't have any logic, the test just verified the message type */
	expectSendFtsResponse(&qc, &mockresponse);
	
	HandleFtsWalRepSyncRepOff();
}

static void
test_HandleFtsWalRepProbeMirror(void **state)
{
	QueryCompletion qc = {0};
	qc.commandTag = CMDTAG_FTS_MSG_PROBE;

	FtsResponse mockresponse;
	mockresponse.IsMirrorUp       = false;
	mockresponse.IsInSync         = false;
	mockresponse.IsSyncRepEnabled = false;
	mockresponse.IsRoleMirror     = false;
	mockresponse.RequestRetry     = false;

	/* expect the IsRoleMirror changed to reflect the global variable */
	am_mirror = true;
	mockresponse.IsRoleMirror = true;
	expectSendFtsResponse(&qc, &mockresponse);

	HandleFtsWalRepProbe();
}

static void
set_replication_slot(void *ptr)
{
	ReplicationSlotCtlData *repCtl = (ReplicationSlotCtlData *) ptr;
	MyReplicationSlot = &repCtl->replication_slots[0];
	/*
	 * any number except 1 to avoid calling ReplicationSlotReserveWal and
	 * other friends.
	 */
	MyReplicationSlot->data.restart_lsn = 8948;
}

static void
test_HandleFtsWalRepPromoteMirror(void **state)
{
	QueryCompletion qc = {0};
	qc.commandTag = CMDTAG_FTS_MSG_PROMOTE;

	ReplicationSlotCtlData repCtl;
	ReplicationSlotCtl = &repCtl;
	max_replication_slots = 1;
	am_mirror = true;

	will_return(GetCurrentDBState, DB_IN_ARCHIVE_RECOVERY);
	will_return(GetRedoRecPtr, &fakeRedoRecPtr);
	will_be_called(UnsetSyncStandbysDefined);
	will_be_called(SignalPromote);

	FtsResponse mockresponse;
	mockresponse.IsMirrorUp       = false;
	mockresponse.IsInSync         = false;
	mockresponse.IsSyncRepEnabled = false;
	mockresponse.IsRoleMirror     = am_mirror;
	mockresponse.RequestRetry     = false;

	expect_value(LWLockAcquire, lock, ReplicationSlotControlLock);
	expect_value(LWLockAcquire, mode, LW_SHARED);
	will_return(LWLockAcquire, true);

	expect_value(LWLockRelease, lock, ReplicationSlotControlLock);
	will_be_called(LWLockRelease);

	expect_value(ReplicationSlotCreate, name, INTERNAL_WAL_REPLICATION_SLOT_NAME);
	expect_value(ReplicationSlotCreate, db_specific, false);
	expect_value(ReplicationSlotCreate, persistency, RS_PERSISTENT);
	expect_value(ReplicationSlotCreate, two_phase, false);
	will_be_called_with_sideeffect(ReplicationSlotCreate,
								   &set_replication_slot, ReplicationSlotCtl);

	/* expect SignalPromote() */
	expectSendFtsResponse(&qc, &mockresponse);

	HandleFtsWalRepPromote();
}

static void
test_HandleFtsWalRepPromotePrimary(void **state)
{
	QueryCompletion qc = {0};
	qc.commandTag = CMDTAG_FTS_MSG_PROMOTE;

	am_mirror = false;

	will_return(GetCurrentDBState, DB_IN_PRODUCTION);
	will_return(GetRedoRecPtr, &fakeRedoRecPtr);

	FtsResponse mockresponse;
	mockresponse.IsMirrorUp       = false;
	mockresponse.IsInSync         = false;
	mockresponse.IsSyncRepEnabled = false;
	mockresponse.IsRoleMirror     = false;
	mockresponse.RequestRetry     = false;

	/* expect no SignalPromote() */
	expectSendFtsResponse(&qc, &mockresponse);

	HandleFtsWalRepPromote();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_HandleFtsWalRepProbePrimary),
		unit_test(test_HandleFtsWalRepSyncRepOff),
		unit_test(test_HandleFtsWalRepProbeMirror),
		unit_test(test_HandleFtsWalRepPromoteMirror),
		unit_test(test_HandleFtsWalRepPromotePrimary)
	};

	MemoryContextInit();
	return run_tests(tests);
}
