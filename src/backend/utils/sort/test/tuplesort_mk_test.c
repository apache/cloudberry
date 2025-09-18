#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>

#include "cmockery.h"

#include "postgres.h"
#include "utils/elog.h"
#include "utils/tuplesort.h"
#include "utils/tuplesort_mk.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"

#include <sys/stat.h>

// #define MK_TEST_DEBUG_PRINT_TUPLES 1

#define get_compare_function_for_ordering_op mock_get_compare_function_for_ordering_op
#define ScanKeyEntryInitialize mock_ScanKeyEntryInitialize

/*
 * Modify the following to check more possible cases
 */
#define MIN_ATTRS 14
#define MAX_ATTRS 15
#define MIN_KEYS 4
#define MAX_KEYS 5

#define NTEST_TUPLES 10000

#define MAX_TEST_STRING_LEN 1024

static void run_sort_test_fixed(int nattrs, int nkeys);
static void run_sort_test_varlena(int nattrs, int nkeys);
static text *make_test_string(int idx);
static int random_sequence(int key);
static Datum test_textcmp(PG_FUNCTION_ARGS);

static void
mock_ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument);

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

bool
mock_get_compare_function_for_ordering_op(Oid opno, Oid *cmpfunc, bool *reverse)
{
	*cmpfunc = InvalidOid;
	*reverse = false;
	return true;
}

static Datum
test_textcmp(PG_FUNCTION_ARGS)
{
	text*		a = PG_GETARG_TEXT_PP(0);
	text*		b = PG_GETARG_TEXT_PP(1);

	char *pa = text_to_cstring(a);
	char *pb = text_to_cstring(b);

	int ret = strcmp(pa, pb);

	PG_FREE_IF_COPY(pa, 0);
	PG_FREE_IF_COPY(pb, 1);

	PG_RETURN_INT32(ret);
}

static PGFunction test_compare_fn = btint4cmp;

static void
mock_ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument)
{
	entry->sk_flags = flags;
	entry->sk_attno = attributeNumber;
	entry->sk_strategy = strategy;
	entry->sk_subtype = subtype;
	entry->sk_collation = collation;
	entry->sk_argument = argument;

	FmgrInfo *finfo = &entry->sk_func;

	finfo->fn_oid = InvalidOid;
	finfo->fn_extra = NULL;
	finfo->fn_mcxt = CurrentMemoryContext;
	finfo->fn_expr = NULL;		/* caller may set this later */

	finfo->fn_nargs = 2; // Binary comparison in this test
	finfo->fn_strict = false;
	finfo->fn_addr = test_compare_fn; //TODO use valid compare function
	finfo->fn_retset = false;
}

/*
 * Check that readtup_heap reports an error if len is zero
 */
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

/*
 * Check that writetup_heap reports an error if len is zero
*/
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

static void run_sort_test_fixed(int nattrs, int nkeys)
{
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "test_shm_mq worker");

    MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                               "mk_test",
                                               ALLOCSET_DEFAULT_SIZES);


	MemoryContext old_cxt = MemoryContextSwitchTo(mcxt);

	FormData_pg_attribute *attributes = (FormData_pg_attribute *)palloc0(sizeof(FormData_pg_attribute) * nattrs);
	Form_pg_attribute *attrs = (Form_pg_attribute *)palloc0(sizeof(Form_pg_attribute) * nattrs);
	for (int i = 0; i < nattrs; i++) 
	{
		attributes[i].attrelid = InvalidOid;
		sprintf(attributes[i].attname.data, "att%d", i);
		attributes[i].atttypid = InvalidOid;
		attributes[i].attstattarget = -1;
		attributes[i].attndims = 0;
		attributes[i].atttypmod = -1;
		attributes[i].attcollation = InvalidOid;
		attributes[i].attlen = 4;
		attributes[i].attbyval = true;
		attributes[i].attalign = 'i';
		attributes[i].attcacheoff = -1;

		attrs[i] = &attributes[i];
	}
	
	TupleDesc tupdesc = (TupleDesc)palloc0(sizeof(struct tupleDesc));
	tupdesc->attrs = attrs;
	tupdesc->natts = nattrs;
	tupdesc->constr = NULL;
	tupdesc->tdhasoid = false;
	tupdesc->tdrefcount = -1;
	tupdesc->tdtypeid = -1;
	tupdesc->tdtypmod = -1;

    ScanState ss;

    AttrNumber *attNums = (AttrNumber *)palloc0(sizeof(AttrNumber) * nkeys);
	Oid *sortOperators = (Oid *)palloc0(sizeof(Oid) * nkeys);
	Oid *sortCollations = (Oid *)palloc0(sizeof(Oid) * nkeys);
	bool *nullsFirstFlags = (bool *)palloc0(sizeof(bool) * nkeys);
	for (int i = 0; i < nkeys; i++)
	{
		attNums[i] = i + 1;
		sortOperators[i] = Int4LessOperator;
		sortCollations[i] = InvalidOid;
		nullsFirstFlags[i] = false;
	}

	test_compare_fn = btint4cmp;

	Tuplesortstate_mk *sortstate = tuplesort_begin_heap_file_readerwriter_mk(
		&ss,
		"test_sort_mk",
		true,
		tupdesc,
		nkeys,
		attNums,
		sortOperators,
		sortCollations,
		nullsFirstFlags,
		work_mem,
		true
	);

	Datum		*values = (Datum *)palloc0(sizeof(Datum) * nattrs);
	bool		*isnull = (bool *)palloc0(sizeof(bool) * nattrs);

	for (int i = 0; i < nattrs; i++)
	{
		values[i] = Int32GetDatum(0);
		isnull[i] = false;
	}

	TupleTableSlot *slot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot, tupdesc);

	slot->tts_tupleDescriptor = tupdesc;
	slot->PRIVATE_tts_heaptuple = NULL;
	slot->PRIVATE_tts_isnull = isnull;
	slot->PRIVATE_tts_values = values;

    /* Insert tuples in reverse order */
    for (int i = NTEST_TUPLES; i > 0; i--)
    {
		for (int j = 0; j < nattrs; j++)
		{
			values[j] = Int32GetDatum(NTEST_TUPLES - i);
		}

		slot->PRIVATE_tts_heaptuple = heap_form_tuple(tupdesc, values, isnull);

		tuplesort_puttupleslot_mk(sortstate, slot);
    }

    tuplesort_performsort_mk(sortstate);

    /* Read back tuples */
    for (int i = 0; ; i++)
    {
        Datum *values = (Datum *)palloc0(sizeof(Datum) * nattrs);
        bool *isnull = (bool *)palloc0(sizeof(bool) * nattrs);

		tuplesort_gettupleslot_mk(sortstate,
								  true,
								  slot);

		if (TupIsNull(slot))
			break;

		HeapTuple tuple = ExecFetchSlotHeapTuple(slot);
		heap_deform_tuple(tuple, tupdesc, values, isnull);

		pfree(values);
		pfree(isnull);
    }

    tuplesort_end_mk(sortstate);
	MemoryContextSwitchTo(old_cxt);
    MemoryContextDelete(mcxt);
}

static const char *test_str_templates[] = {
	"hello world ",
	"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat ",
};

// From https://www.researchgate.net/publication/2683298_A_Collection_of_Selected_Pseudorandom_Number_Generators_With_Linear_Structures

static int random_seed = 1013;

static int random_sequence(int key)
{
    return (3141592621u*key + 2718281829u) % 1000000007u;
}

static text *
make_test_string(int idx)
{
	char buffer[MAX_TEST_STRING_LEN];
	const char *template = test_str_templates[idx % sizeof(test_str_templates)/sizeof(test_str_templates[0])];

	random_seed = random_sequence(random_seed);

	size_t total_len = random_seed % MAX_TEST_STRING_LEN;
	sprintf(buffer, "%d. ", idx);
	int len = strlen(buffer);
	while (len < total_len) 
	{
		strncat(buffer, template, total_len - len);
		len = strlen(buffer);
	}

	text *ret_text = cstring_to_text(buffer);
	return ret_text;
}

static void run_sort_test_varlena(int nattrs, int nkeys)
{
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "test_shm_mq worker");

    MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                               "mk_test",
                                               ALLOCSET_DEFAULT_SIZES);


	MemoryContext old_cxt = MemoryContextSwitchTo(mcxt);

	FormData_pg_attribute *attributes = (FormData_pg_attribute *)palloc0(sizeof(FormData_pg_attribute) * nattrs);
	Form_pg_attribute *attrs = (Form_pg_attribute *)palloc0(sizeof(Form_pg_attribute) * nattrs);
	for (int i = 0; i < nattrs; i++) 
	{
		attributes[i].attrelid = InvalidOid;
		sprintf(attributes[i].attname.data, "att%d", i);
		attributes[i].atttypid = InvalidOid;
		attributes[i].attstattarget = -1;
		attributes[i].attndims = 0;
		attributes[i].atttypmod = -1;
		attributes[i].attcollation = InvalidOid;
		attributes[i].attlen = -1;    // varlena
		attributes[i].attbyval = false; // for varlena
		attributes[i].attstorage = 'e'; // allow TOAST
		attributes[i].attalign = 'c';  // Char alignment
		attributes[i].attcacheoff = -1;

		attrs[i] = &attributes[i];
	}
	
	TupleDesc tupdesc = (TupleDesc)palloc0(sizeof(struct tupleDesc));
	tupdesc->attrs = attrs;
	tupdesc->natts = nattrs;
	tupdesc->constr = NULL;
	tupdesc->tdhasoid = false;
	tupdesc->tdrefcount = -1;
	tupdesc->tdtypeid = -1;
	tupdesc->tdtypmod = -1;

    ScanState ss;

    AttrNumber *attNums = (AttrNumber *)palloc0(sizeof(AttrNumber) * nkeys);
	Oid *sortOperators = (Oid *)palloc0(sizeof(Oid) * nkeys);
	Oid *sortCollations = (Oid *)palloc0(sizeof(Oid) * nkeys);
	bool *nullsFirstFlags = (bool *)palloc0(sizeof(bool) * nkeys);
	for (int i = 0; i < nkeys; i++)
	{
		attNums[i] = i + 1;
		sortOperators[i] = TextEqualOperator;
		sortCollations[i] = InvalidOid;
		nullsFirstFlags[i] = false;
	}

	test_compare_fn = test_textcmp;

	Tuplesortstate_mk *sortstate = tuplesort_begin_heap_file_readerwriter_mk(
		&ss,
		"test_sort_mk",
		true,
		tupdesc,
		nkeys,
		attNums,
		sortOperators,
		sortCollations,
		nullsFirstFlags,
		work_mem,
		true
	);

	Datum		*values = (Datum *)palloc0(sizeof(Datum) * nattrs);
	bool		*isnull = (bool *)palloc0(sizeof(bool) * nattrs);

	for (int i = 0; i < nattrs; i++)
	{
		values[i] = Int32GetDatum(0);
		isnull[i] = false;
	}

	TupleTableSlot *slot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot, tupdesc);

	slot->tts_tupleDescriptor = tupdesc;
	slot->PRIVATE_tts_heaptuple = NULL;
	slot->PRIVATE_tts_isnull = isnull;
	slot->PRIVATE_tts_values = values;

    /* Insert tuples in reverse order */
    for (int i = NTEST_TUPLES; i > 0; i--)
    {
		for (int j = 0; j < nattrs; j++)
		{			
			text *attr_text = make_test_string(i+j);
			values[j] = PointerGetDatum(attr_text);
			isnull[j] = (i+j) % 3 == 0;
		}

		slot->PRIVATE_tts_heaptuple = heap_form_tuple(tupdesc, values, isnull);

		tuplesort_puttupleslot_mk(sortstate, slot);
    }

    tuplesort_performsort_mk(sortstate);

    /* Read back */

	for (int i = 0; ; i++)
    {
		tuplesort_gettupleslot_mk(sortstate,
								  true,
								  slot);

		if (TupIsNull(slot))
			break;

		HeapTuple tuple = ExecFetchSlotHeapTuple(slot);
		heap_deform_tuple(tuple, tupdesc, values, isnull);

// Redefine the following to get debug print		
#ifdef MK_TEST_DEBUG_PRINT_TUPLES
		text *t0 = (text *)DatumGetPointer(values[0]);
		text *t1 = (text *)DatumGetPointer(values[1]);
		
		printf("(%s,%s)\n", text_to_cstring(t0), text_to_cstring(t1)); 
		printf("\n");
#endif		
    }

    tuplesort_end_mk(sortstate);
	MemoryContextSwitchTo(old_cxt);
    MemoryContextDelete(mcxt);	
}

/*
 * Test: basic ascending sort of fixed length tuples of ints. 
 * This test uses different numbers of attributes and keys, so different
 * positions of tuple in the block is checked.
 */
static void
test_basic_int_sort(void **stateptr)
{

	mkdir("base", S_IRWXU);

	for (int nattrs = MIN_ATTRS; nattrs <= MAX_ATTRS; nattrs++)
		for (int nkeys = MIN_KEYS; nkeys <= Min(nattrs, MAX_KEYS); nkeys++)
		{
			run_sort_test_fixed(nattrs, nkeys);
		}
}


/*
 * Test: basic ascending sort of variable length tuples of varlena. 
 * This test uses different numbers of attributes and keys, so different
 * positions of tuple in the block is checked.
 */
static void
test_sort_varlena(void **stateptr)
{

	for (int nattrs = MIN_ATTRS; nattrs <= MAX_ATTRS; nattrs++)
		for (int nkeys = MIN_KEYS; nkeys <= Min(nattrs, MAX_KEYS); nkeys++)
		{
			run_sort_test_varlena(nattrs, nkeys);
		}
}


const UnitTest tests[] = {
	unit_test(test_tuplesort_mk_readtup_heap_fail_len),
	unit_test(test_tuplesort_mk_writetup_heap_fail_len),
	unit_test(test_basic_int_sort),
	unit_test(test_sort_varlena)
};

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	MemoryContextInit();

	DataDir = ".";

	PGPROC proc;
	MyProc = &proc;

	struct stat st = {0};
	if (stat("base", &st) == -1) {
		mkdir("base", 0700);
	}

	if (stat("base/pgsql_tmp", &st) == -1) {
		mkdir("base/pgsql_tmp", 0700);
	}

	// Workfile manager uses locks
	PGShmemHeader *shim = NULL;
	InitShmemAccess(PGSharedMemoryCreate(3000000, 6000, &shim));
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
	WorkFileShmemInit();
	InitFileAccess();

	return run_tests(tests);
}
