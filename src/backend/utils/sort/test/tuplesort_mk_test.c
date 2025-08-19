#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>

#include "cmockery.h"

#include "postgres.h"
#include "utils/elog.h"
#include "utils/tuplesort.h"
#include "utils/tuplesort_mk.h"

#undef USE_ASSERT_CHECKING
#include "../tuplesort_mk.c"


struct LogicalTapeSet
{
	BufFile    *pfile;			/* underlying file for whole tape set */
	long		nFileBlocks;	/* # of blocks used in underlying file */
	bool		forgetFreeSpace;		/* if we need to keep track of free
										 * space */
	bool		blocksSorted;	/* is freeBlocks[] currently in order? */
	long	   *freeBlocks;		/* resizable array */
	long		nFreeBlocks;	/* # of currently free blocks */
	long		freeBlocksLen;	/* current allocated length of freeBlocks[] */

	int			nTapes;			/* # of logical tapes in set */
};


static void
test_tuplesort_mk_readtup_heap_fail_len(void **test_state)
{
	gp_debug_linger = 0;

	elog(LOG, "Running test: readtup_heap_fail_len");

	struct LogicalTapeSet lts = {.nFileBlocks = 42 * 1024};

	Tuplesortstate_mk state = {
		.sortcontext = CurrentMemoryContext,
		.status = TSS_SORTEDONTAPE,
		.tapeset = &lts,
		.nKeys = 42,
		.randomAccess = false,
		.memAllowed = 64LL * 1024 * 1024 * 1024,
		.maxTapes = 42,
		.tapeRange = 468
	};

	const char *expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	char	   *message = NULL;
	bool		error_thrown = false;

	/* Check zero length */
	PG_TRY();
	{
		readtup_heap(&state, 0 /* pos */ , NULL /* stup */ , 
			NULL /* tape */ , 0		/* len, too small and no flag */ );
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);

	expected_message = 
		"invalid tuple len 2147483652. Sort method: external sort, "
		"space type: Disk, space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	message = NULL;
	error_thrown = false;
	state.sortcontext = CurrentMemoryContext;

	/* Check uint32 length */
	PG_TRY();
	{
		readtup_heap(&state, 0 /* pos */ , NULL /* stup */ , NULL /* tape */ , 
			sizeof(uint32) | MEMTUP_LEAD_BIT /* len, too small but with flag */);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);
}

static void
test_tuplesort_mk_writetup_heap_fail_len(void **test_state)
{
	gp_debug_linger = 0;

	elog(LOG, "Running test: writetup_heap_fail_len");

	struct LogicalTapeSet lts = {.nFileBlocks = 42 * 1024};

	Tuplesortstate_mk state = {
		.sortcontext = CurrentMemoryContext,
		.status = TSS_SORTEDONTAPE,
		.tapeset = &lts,
		.nKeys = 42,
		.randomAccess = false,
		.memAllowed = 64LL * 1024 * 1024 * 1024,
		.maxTapes = 42,
		.tapeRange = 468
	};

	const char *expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	char	   *message = NULL;
	bool		error_thrown = false;

	MemTupleData mtup = {
		.PRIVATE_mt_len = MEMTUP_LEAD_BIT | 0
	};

	MKEntry		entry = {
		.ptr = &mtup
	};

	/* Check zero length */
	PG_TRY();
	{
		writetup_heap(&state, 0, &entry);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);

	mtup.PRIVATE_mt_len = MEMTUP_LEAD_BIT | sizeof(uint32);
	expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	message = NULL;
	error_thrown = false;
	state.sortcontext = CurrentMemoryContext;

	/* Check uint32 length */
	PG_TRY();
	{
		writetup_heap(&state, 0, &entry);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {
		unit_test(test_tuplesort_mk_readtup_heap_fail_len),
		unit_test(test_tuplesort_mk_writetup_heap_fail_len)
	};

	MemoryContextInit();

	return run_tests(tests);
}
