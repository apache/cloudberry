#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Fetch definition of PG_exception_stack */
#include "postgres.h"
#include "storage/ipc.h"

#undef ereport
#define ereport(elevel, ...) \
	do { \
		ereport_mock(elevel, TEXTDOMAIN, __VA_ARGS__); \
	} while(0)

static int expected_elevel;

#define ereport_mock(elevel, domain, ...) \
	do { \
		assert_int_equal(elevel, expected_elevel); \
		if (elevel >= ERROR) \
			siglongjmp(*PG_exception_stack, 1); \
		else \
			errstart(elevel, domain); \
	} while(0)

#include "../varsup.c"

static void expect_ereport(int log_level)
{
	expected_elevel = log_level;
	if (log_level < ERROR)
	{
		expect_value(errstart, elevel, log_level);
		expect_value(errstart, domain, TEXTDOMAIN);
		will_be_called(errstart);
	}
}

static void
init_ProcGlobal(void)
{
	/* we only need one proc as there are assertions added from PG14*/
	MyProc = (PGPROC *) malloc(sizeof(PROC_HDR));
	MyProc->pgxactoff = 0;
	MyProc->subxidStatus.count = 0;
	MyProc->subxidStatus.overflowed = false;

	ProcGlobal = (PROC_HDR *) malloc(sizeof(PROC_HDR));
	ProcGlobal->subxidStates = (XidCacheStatus *) malloc(sizeof(XidCacheStatus));
	ProcGlobal->subxidStates[MyProc->pgxactoff].count = 0;
	ProcGlobal->subxidStates[MyProc->pgxactoff].overflowed = false;
	ProcGlobal->xids = (TransactionId *) malloc(sizeof(TransactionId));
}

static void
test_GetNewTransactionId_xid_stop_limit(void **state)
{
	VariableCacheData data;

	/*
	 * make sure the nextXid is larger than xidVacLimit to check xidStopLimit,
	 * and it's larger than xidStopLimit to trigger the ereport(ERROR).
	 */
	ShmemVariableCache = &data;
	ShmemVariableCache->nextXid = FullTransactionIdFromU64(30);
	ShmemVariableCache->xidVacLimit = 10;
	ShmemVariableCache->xidStopLimit = 20;
	IsUnderPostmaster = true;

	will_return(RecoveryInProgress, false);

	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	will_return(LWLockAcquire, true);

	expect_any(LWLockRelease, lock);
	will_be_called(LWLockRelease);

	expect_any(get_database_name, dbid);
	will_return(get_database_name, "foo");
	
	expect_ereport(ERROR);
	init_ProcGlobal();

	PG_TRY();
	{
		GetNewTransactionId(false);
		assert_false("No ereport(ERROR, ...) was called");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

static void
test_GetNewTransactionId_xid_warn_limit(void **state)
{
	const int xid = 25;
	VariableCacheData data;

	/*
	 * make sure nextXid is between xidWarnLimit and xidStopLimit to trigger
	 * the ereport(WARNING).
	 */
	ShmemVariableCache = &data;
	ShmemVariableCache->nextXid = FullTransactionIdFromU64(xid);
	ShmemVariableCache->xidVacLimit = 10;
	ShmemVariableCache->xidWarnLimit = 20;
	ShmemVariableCache->xidStopLimit = 30;
	IsUnderPostmaster = true;

	will_return(RecoveryInProgress, false);

	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	will_return(LWLockAcquire, true);

	expect_any(LWLockRelease, lock);
	will_be_called(LWLockRelease);

	expect_any(get_database_name, dbid);
	will_return(get_database_name, "foo");
	
	expect_ereport(WARNING);

	/*
	 * verify rest of function logic, including assign MyProc->xid
	 */
	expect_any(LWLockAcquire, lock);
	expect_any(LWLockAcquire, mode);
	will_return(LWLockAcquire, true);

	expect_any(ExtendCLOG, newestXact);
	will_be_called(ExtendCLOG);
	expect_any(ExtendCommitTs, newestXact);
	will_be_called(ExtendCommitTs);
	expect_any(ExtendSUBTRANS, newestXact);
	will_be_called(ExtendSUBTRANS);
	expect_any(DistributedLog_Extend, newestXact);
	will_be_called(DistributedLog_Extend);

	expect_any(LWLockRelease, lock);
	will_be_called(LWLockRelease);
	/*
	* GPDB: this must come after all the expect_*() calls, else it will fatal
	*/
	init_ProcGlobal();
	
	PG_TRY();
	{
		GetNewTransactionId(false);
		assert_int_equal(MyProc->xid, xid);
	}
	PG_CATCH();
	{
		assert_false("No ereport(WARNING, ...) was called");
	}
	PG_END_TRY();
}

static void
should_acquire_and_release_oid_gen_lock()
{
	expect_value(LWLockAcquire, lock, OidGenLock);
	expect_value(LWLockAcquire, mode, LW_EXCLUSIVE);
	will_be_called(LWLockAcquire);

	expect_value(LWLockRelease, lock, OidGenLock);
	will_be_called(LWLockRelease);
}

static void
should_generate_xlog_for_next_oid(Oid expected_oid_in_xlog_rec)
{
	expect_value(XLogPutNextOid, nextOid, expected_oid_in_xlog_rec);
	will_be_called(XLogPutNextOid);
}

/*
 * QD wrapped around before QE did
 * QD nextOid: FirstNormalObjectId, QE nextOid: PG_UINT32_MAX
 * QE should set its nextOid to FirstNormalObjectId and create an xlog
 */
static void
test_AdvanceObjectId_QD_wrapped_before_QE(void **state)
{
	VariableCacheData data = {.nextOid = PG_UINT32_MAX, .oidCount = 1};
	ShmemVariableCache = &data;

	should_generate_xlog_for_next_oid(FirstNormalObjectId + VAR_OID_PREFETCH);

	should_acquire_and_release_oid_gen_lock();

	/* Run the test */
	AdvanceObjectId(FirstNormalObjectId);

	/* Check if shared memory Oid values have been changed correctly */
	assert_int_equal(ShmemVariableCache->nextOid, FirstNormalObjectId);
	assert_int_equal(ShmemVariableCache->oidCount, VAR_OID_PREFETCH);
}

/*
 * QE wrapped around but QD did not
 * QD nextOid: PG_UINT32_MAX, QE nextOid: FirstNormalObjectId
 * QE should do nothing
 */
static void
test_AdvanceObjectId_QE_wrapped_before_QD(void **state)
{
	VariableCacheData data = {.nextOid = FirstNormalObjectId, .oidCount = VAR_OID_PREFETCH};
	ShmemVariableCache = &data;

	should_acquire_and_release_oid_gen_lock();

	/* Run the test */
	AdvanceObjectId(PG_UINT32_MAX);

	/* Check if shared memory Oid values have been changed correctly */
	assert_int_equal(ShmemVariableCache->nextOid, FirstNormalObjectId);
	assert_int_equal(ShmemVariableCache->oidCount, VAR_OID_PREFETCH);
}

/*
 * Regular normal operation flow
 * QD nextOid: FirstNormalObjectId + 2, QE nextOid: FirstNormalObjectId
 * QE should set its nextOid to FirstNormalObjectId + 2 and not create an xlog
 */
static void
test_AdvanceObjectId_normal_flow(void **state)
{
	VariableCacheData data = {.nextOid = FirstNormalObjectId, .oidCount = VAR_OID_PREFETCH};
	ShmemVariableCache = &data;

	should_acquire_and_release_oid_gen_lock();

	/* Run the test */
	AdvanceObjectId(FirstNormalObjectId + 2);

	/* Check if shared memory Oid values have been changed correctly */
	assert_int_equal(ShmemVariableCache->nextOid, FirstNormalObjectId + 2);
	assert_int_equal(ShmemVariableCache->oidCount, VAR_OID_PREFETCH -2);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_GetNewTransactionId_xid_stop_limit),
		unit_test(test_GetNewTransactionId_xid_warn_limit),
		unit_test(test_AdvanceObjectId_QD_wrapped_before_QE),
		unit_test(test_AdvanceObjectId_QE_wrapped_before_QD),
		unit_test(test_AdvanceObjectId_normal_flow)
	};

	return run_tests(tests);
}
