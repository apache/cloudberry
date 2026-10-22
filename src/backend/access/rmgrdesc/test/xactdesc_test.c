#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#include "utils/memutils.h"
#include "access/xlog.h"
#include "pgtime.h"

/* Actual function body */
#include "../xactdesc.c"

static void
test_xactdescprepareCommit(void **state)
{
	StringInfo buf = makeStringInfo();
	const char *gid = "4242424242-0000000042";
	Size		gidlen = strlen(gid) + 1;
	Size		datalen = MAXALIGN(sizeof(xl_xact_prepare)) + MAXALIGN(gidlen);

	XLogReaderState *record = palloc0(sizeof(XLogReaderState));
	record->record = palloc0(sizeof(DecodedXLogRecord));
	XLogRecGetData(record) = palloc0(datalen);

	XLogRecGetInfo(record) = XLOG_XACT_PREPARE;
	xl_xact_prepare *xlrec = (xl_xact_prepare *) XLogRecGetData(record);

	xlrec->prepared_at = 617826371830030;
	xlrec->gidlen = gidlen;
	memcpy((char *) xlrec + MAXALIGN(sizeof(xl_xact_prepare)), gid, gidlen);

	xact_desc(buf, record);

	assert_string_equal("gid 4242424242-0000000042: 2019-07-30 18:26:11.83003+00", buf->data);
}

static void
test_xactdescprepareAbort(void **state)
{
	StringInfo buf = makeStringInfo();
	const char *gid = "1111111111-0000000001";
	Size		gidlen = strlen(gid) + 1;
	Size		datalen = MAXALIGN(sizeof(xl_xact_prepare)) + MAXALIGN(gidlen);

	XLogReaderState *record = palloc0(sizeof(XLogReaderState));
	record->record = palloc0(sizeof(DecodedXLogRecord));
	XLogRecGetData(record) = palloc0(datalen);

	XLogRecGetInfo(record) = XLOG_XACT_PREPARE;
	xl_xact_prepare *xlrec = (xl_xact_prepare *) XLogRecGetData(record);

	xlrec->prepared_at = 617826371830030;
	xlrec->gidlen = gidlen;
	memcpy((char *) xlrec + MAXALIGN(sizeof(xl_xact_prepare)), gid, gidlen);

	xact_desc(buf, record);

	assert_string_equal("gid 1111111111-0000000001: 2019-07-30 18:26:11.83003+00", buf->data);
}

static void
test_xactdescprepareNone(void **state)
{
	StringInfo buf = makeStringInfo();
	const char *gid = "9999999999-0000000099";
	Size		gidlen = strlen(gid) + 1;
	Size		datalen = MAXALIGN(sizeof(xl_xact_prepare)) + MAXALIGN(gidlen);

	XLogReaderState *record = palloc0(sizeof(XLogReaderState));
	record->record = palloc0(sizeof(DecodedXLogRecord));
	XLogRecGetData(record) = palloc0(datalen);

	XLogRecGetInfo(record) = XLOG_XACT_PREPARE;
	xl_xact_prepare *xlrec = (xl_xact_prepare *) XLogRecGetData(record);

	xlrec->prepared_at = 0;
	xlrec->gidlen = gidlen;
	memcpy((char *) xlrec + MAXALIGN(sizeof(xl_xact_prepare)), gid, gidlen);

	xact_desc(buf, record);

	assert_string_equal("gid 9999999999-0000000099: 2000-01-01 00:00:00+00", buf->data);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	MemoryContextInit();
	pg_timezone_initialize();

	const UnitTest tests[] = {
		unit_test(test_xactdescprepareCommit),
		unit_test(test_xactdescprepareAbort),
		unit_test(test_xactdescprepareNone)
	};

	return run_tests(tests);
}
