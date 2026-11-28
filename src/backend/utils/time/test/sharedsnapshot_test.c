#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/faultinjector.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "storage/fd.h"
#include "storage/dsm.h"

#include "../sharedsnapshot.c"

static char *testSharedSnapshotAddress;

extern dsm_segment *__wrap_dsm_attach(dsm_handle handle);
extern void *__wrap_dsm_segment_address(dsm_segment *segment);
extern void __wrap_dsm_detach(dsm_segment *segment);
extern bool __wrap_LWLockAcquire(LWLock *lock, LWLockMode mode);
extern void __wrap_LWLockRelease(LWLock *lock);

dsm_segment *
__wrap_dsm_attach(dsm_handle handle)
{
	(void) handle;
	return (dsm_segment *) (uintptr_t) 1;
}

void *
__wrap_dsm_segment_address(dsm_segment *segment)
{
	(void) segment;
	return testSharedSnapshotAddress;
}

void
__wrap_dsm_detach(dsm_segment *segment)
{
	(void) segment;
}

bool
__wrap_LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	(void) lock;
	(void) mode;
	return true;
}

void
__wrap_LWLockRelease(LWLock *lock)
{
	(void) lock;
}

static void
test_boundaries_of_CreateSharedSnapshotArray(void **state)
{
	/*
	 * MaxBackends is used to calculate NUM_SHARED_SNAPSHOT_SLOTS. Make
	 * it non-zero so that we actually allocate some local snapshot slots.
	 */
	MaxBackends = 2;

	SharedSnapshotStruct 	*fakeSharedSnapshotArray = NULL;
	LWLockPadded 			*fakeLockBase = NULL;
	bool found = false;

	expect_string(RequestNamedLWLockTranche, tranche_name, "SharedSnapshotLocks");
	expect_value(RequestNamedLWLockTranche, num_lwlocks, NUM_SHARED_SNAPSHOT_SLOTS);
	will_be_called(RequestNamedLWLockTranche);
	Size sharedSnapshotShmemSize = SharedSnapshotShmemSize();
	fakeSharedSnapshotArray = malloc(sharedSnapshotShmemSize);

	will_return(ShmemInitStruct, fakeSharedSnapshotArray);
	will_assign_value(ShmemInitStruct, foundPtr, found);
	expect_any_count(ShmemInitStruct, name, 1);
	expect_any_count(ShmemInitStruct, size, 1);
	expect_any_count(ShmemInitStruct, foundPtr, 1);

	expect_string(RequestNamedLWLockTranche, tranche_name, "SharedSnapshotLocks");
	expect_value(RequestNamedLWLockTranche, num_lwlocks, NUM_SHARED_SNAPSHOT_SLOTS);
	will_be_called(RequestNamedLWLockTranche);
	fakeLockBase = malloc(NUM_SHARED_SNAPSHOT_SLOTS * sizeof(LWLockPadded));
	will_return(GetNamedLWLockTranche, fakeLockBase);
	expect_any(GetNamedLWLockTranche, tranche_name);

	CreateSharedSnapshotArray();

	for (int i=0; i<sharedSnapshotArray->maxSlots; i++)
	{
		SharedSnapshotSlot *s = &sharedSnapshotArray->slots[i];

		/*
		 * Assert that each slot in SharedSnapshotStruct has an associated
		 * dynamically allocated LWLock.
		 */
		assert_true(s->slotLock == &fakeLockBase[i].lock);
		/*
		 * Assert that every slot xip array falls inside the boundaries of the
		 * allocated shared snapshot.
		 */
		assert_true((uint8_t *)s > (uint8_t *)fakeSharedSnapshotArray);
		assert_true((uint8_t *)s < (((uint8_t *)fakeSharedSnapshotArray) +
									sharedSnapshotShmemSize));
	}
}

static void
test_read_shared_snapshot_preserves_recovery_flag(void **state)
{
	SnapshotData original;
	SnapshotData reader;
	SharedSnapshotSlot slot;
	TransactionId subxip[] = {43};
	Size		snapshotSize;
	char	   *serialized;
	TransactionId readerXip[1];
	TransactionId readerSubxip[1];

	MemSet(&original, 0, sizeof(original));
	original.snapshot_type = SNAPSHOT_MVCC;
	original.xmin = 40;
	original.xmax = 50;
	original.subxip = subxip;
	original.subxcnt = lengthof(subxip);
	original.suboverflowed = true;
	original.takenDuringRecovery = true;
	snapshotSize = EstimateSnapshotSpace(&original);
	serialized = malloc(snapshotSize);
	assert_true(serialized != NULL);
	SerializeSnapshot(&original, serialized);

	MemSet(&reader, 0, sizeof(reader));
	reader.xip = readerXip;
	reader.subxip = readerSubxip;
	MemSet(&slot, 0, sizeof(slot));
	slot.snapshot_handle = 1;
	SharedLocalSnapshotSlot = &slot;
	testSharedSnapshotAddress = serialized;
	TransactionXmin = original.xmin;

	ReadSharedLocalSnapshot(&reader);

	assert_true(reader.takenDuringRecovery);
	assert_true(reader.suboverflowed);
	assert_int_equal(reader.subxcnt, 1);
	assert_int_equal(reader.subxip[0], subxip[0]);

	SharedLocalSnapshotSlot = NULL;
	testSharedSnapshotAddress = NULL;
	free(serialized);
}

static void
test_cursor_reader_preserves_recovery_flag(void **state)
{
	SnapshotData original;
	SnapshotData reader;
	SharedSnapshotSlot slot;
	LWLock		lock;
	TransactionId subxip[] = {43};
	TransactionId readerXip[64];
	TransactionId readerSubxip[1];
	Size		snapshotSize;
	char	   *serialized;
	const uint32 segmateSync = 123;
	const DistributedTransactionId distributedXid = 456;
	const FullTransactionId localXid = FullTransactionIdFromU64(987654321);

	MemSet(&original, 0, sizeof(original));
	original.snapshot_type = SNAPSHOT_MVCC;
	original.xmin = 40;
	original.xmax = 50;
	original.subxip = subxip;
	original.subxcnt = lengthof(subxip);
	original.suboverflowed = true;
	original.takenDuringRecovery = true;
	snapshotSize = EstimateSnapshotSpace(&original);
	serialized = malloc(snapshotSize);
	assert_true(serialized != NULL);
	SerializeSnapshot(&original, serialized);

	MemSet(&reader, 0, sizeof(reader));
	reader.xip = readerXip;
	reader.subxip = readerSubxip;
	MemSet(&slot, 0, sizeof(slot));
	slot.slotLock = &lock;
	slot.cur_dump_id = 0;
	slot.dump[0].segmateSync = segmateSync;
	slot.dump[0].distributedXid = distributedXid;
	slot.dump[0].handle = 1;
	slot.dump[0].localXid = localXid;
	SharedLocalSnapshotSlot = &slot;
	testSharedSnapshotAddress = serialized;
	QEDtxContextInfo.segmateSync = segmateSync;
	QEDtxContextInfo.distributedXid = distributedXid;
	Gp_role = GP_ROLE_EXECUTE;
	Gp_is_writer = false;

	readSharedLocalSnapshot_forCursor(&reader, DTX_CONTEXT_QE_READER);

	assert_true(reader.takenDuringRecovery);
	assert_true(reader.suboverflowed);
	assert_int_equal(reader.subxcnt, 1);
	assert_int_equal(reader.subxip[0], subxip[0]);

	SharedLocalSnapshotSlot = NULL;
	testSharedSnapshotAddress = NULL;
	free(serialized);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_boundaries_of_CreateSharedSnapshotArray),
		unit_test(test_read_shared_snapshot_preserves_recovery_flag),
		unit_test(test_cursor_reader_preserves_recovery_flag)
	};
	MemoryContextInit();
	/* RestoreSnapshot() and hash_create() allocate in this context. */
	TopTransactionContext =
		AllocSetContextCreate(TopMemoryContext,
							  "mock TopTransactionContext",
							  ALLOCSET_DEFAULT_SIZES);
	InitFileAccess();
	return run_tests(tests);
}
