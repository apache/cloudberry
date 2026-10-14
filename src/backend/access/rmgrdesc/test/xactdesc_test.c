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

/*
 * Lay the record out exactly as StartPrepare() does, with the GPDB-specific
 * database arrays between the abort rels and the stats, and check that the
 * parser walks past them correctly.
 */
static void
test_xactdescprepareDbsAndStats(void **state)
{
	StringInfo	buf = makeStringInfo();
	const char *gid = "4242424242-0000000042";
	Size		gidlen = strlen(gid) + 1;
	DbDirNode	commitdb = {.tablespace = 1663,.database = 16384};
	DbDirNode	abortdb = {.tablespace = 1663,.database = 16385};
	xl_xact_stats_item commitstat = {.kind = 1,.dboid = 16384,.objoid = 0};
	Size		datalen = MAXALIGN(sizeof(xl_xact_prepare)) + MAXALIGN(gidlen) +
		2 * MAXALIGN(sizeof(DbDirNode)) + MAXALIGN(sizeof(xl_xact_stats_item));
	char	   *ptr;

	XLogReaderState *record = palloc0(sizeof(XLogReaderState));

	record->record = palloc0(sizeof(DecodedXLogRecord));
	XLogRecGetData(record) = palloc0(datalen);

	XLogRecGetInfo(record) = XLOG_XACT_PREPARE;
	xl_xact_prepare *xlrec = (xl_xact_prepare *) XLogRecGetData(record);

	xlrec->prepared_at = 617826371830030;
	xlrec->ncommitdbs = 1;
	xlrec->nabortdbs = 1;
	xlrec->ncommitstats = 1;
	xlrec->gidlen = gidlen;

	ptr = (char *) xlrec + MAXALIGN(sizeof(xl_xact_prepare));
	memcpy(ptr, gid, gidlen);
	ptr += MAXALIGN(gidlen);
	memcpy(ptr, &commitdb, sizeof(DbDirNode));
	ptr += MAXALIGN(sizeof(DbDirNode));
	memcpy(ptr, &abortdb, sizeof(DbDirNode));
	ptr += MAXALIGN(sizeof(DbDirNode));
	memcpy(ptr, &commitstat, sizeof(xl_xact_stats_item));

	xact_desc(buf, record);

	assert_string_equal("gid 4242424242-0000000042: 2019-07-30 18:26:11.83003+00"
						"; dbs(commit): base/16384; dbs(abort): base/16385"
						"; commit dropped stats: 1/16384/0", buf->data);
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
		unit_test(test_xactdescprepareNone),
		unit_test(test_xactdescprepareDbsAndStats)
	};

	return run_tests(tests);
}
