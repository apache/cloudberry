#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"

#include "../cdbappendonlyxlog.c"

#include "access/transam.h"
#include "catalog/pg_tablespace.h"

static int
check_relfilelocator_function(const LargestIntegralType arg1, const LargestIntegralType arg2)
{
	const RelFileLocator *value = (const RelFileLocator *) arg1;
	const RelFileLocator *check_value = (const RelFileLocator *) arg2;
	return RelFileLocatorEquals(*value, *check_value);
}

/*
 * Test that XLogAOSegmentFile will be called when we cannot find the AO
 * segment file.
 */
static void
ao_invalid_segment_file_test(uint8 xl_info)
{
	RelFileLocator relfilelocator;
	XLogReaderState *mockrecord;
	xl_ao_target xlaotarget;
	xl_ao_insert xlaoinsert;
	xl_ao_truncate xlaotruncate;

	/* create mock transaction log */
	relfilelocator.spcOid = DEFAULTTABLESPACE_OID;
	relfilelocator.dbOid = 12087 /* postgres database */;
	relfilelocator.relNumber = FirstNormalObjectId;

	xlaotarget.node = relfilelocator;
	xlaotarget.segment_filenum = 2;
	xlaotarget.offset = 12345;

	mockrecord = XLogReaderAllocate(DEFAULT_XLOG_SEG_SIZE, NULL, XL_ROUTINE(), NULL);
	mockrecord->record = palloc0(sizeof(DecodedXLogRecord));

	if (xl_info == XLOG_APPENDONLY_INSERT)
	{
		xlaoinsert.target = xlaotarget;
		XLogRecGetData(mockrecord) = (char *) &xlaoinsert;
	}
	else if (xl_info == XLOG_APPENDONLY_TRUNCATE)
	{
		xlaotruncate.target = xlaotarget;
		XLogRecGetData(mockrecord) = (char *) &xlaotruncate;
	}

	/* mock to not find AO segment file */
	expect_any(PathNameOpenFile, fileName);
	expect_any(PathNameOpenFile, fileFlags);
	will_return(PathNameOpenFile, -1);

	/* XLogAOSegmentFile should be called with our mock relfilelocator and segment file number */
	expect_check(XLogAOSegmentFile, &rnode, check_relfilelocator_function, &relfilelocator);
	expect_value(XLogAOSegmentFile, segmentFileNum, xlaotarget.segment_filenum);
	will_be_called(XLogAOSegmentFile);

	/* run test */
	if (xl_info == XLOG_APPENDONLY_INSERT)
		ao_insert_replay(mockrecord);
	else if (xl_info == XLOG_APPENDONLY_TRUNCATE)
		ao_truncate_replay(mockrecord);
}

/*
 * Test that ao_insert_replay will call XLogAOSegmentFile when we cannot find
 * the AO segment file.
 */
static void
test_ao_insert_replay_invalid_segment_file(void **state)
{
	ao_invalid_segment_file_test(XLOG_APPENDONLY_INSERT);
}

/*
 * Test that ao_truncate_replay will call XLogAOSegmentFile when we cannot find
 * the AO segment file.
 */
static void
test_ao_truncate_replay_invalid_segment_file(void **state)
{
	ao_invalid_segment_file_test(XLOG_APPENDONLY_TRUNCATE);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_ao_insert_replay_invalid_segment_file),
		unit_test(test_ao_truncate_replay_invalid_segment_file)
	};

	MemoryContextInit();

	return run_tests(tests);
}
