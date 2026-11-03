#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"
#include "storage/smgr.h"

#include "../aocsam.c"

/*
 * aocs_begin_headerscan()
 *
 * Verify that we are setting correct storage attributes (no compression) for
 * scanning an existing column in ALTER TABLE ADD COLUMN case.
 */
static void
test__aocs_begin_headerscan(void **state)
{
	AOCSHeaderScanDesc desc;
	RelationData reldata;
	SMgrRelationData smgrdata;
	FormData_pg_class pgclass;

	memset(&reldata, 0, sizeof(RelationData));

	reldata.rd_rel = &pgclass;
	reldata.rd_id = 12345;
	StdRdOptions opt;

	opt.blocksize = 8192 * 5;
	StdRdOptions *opts[1];

	smgrdata.smgr_ao = smgrAOGetDefault();
	reldata.rd_smgr = &smgrdata;
	reldata.rd_backend = InvalidBackendId;

	opts[0] = &opt;

	strncpy(&pgclass.relname.data[0], "mock_relation", 13);
	expect_value(RelationGetAttributeOptions, rel, &reldata);
	will_return(RelationGetAttributeOptions, &opts);

	expect_value(GetAppendOnlyEntryAttributes, relid, 12345);
	expect_any(GetAppendOnlyEntryAttributes, blocksize);
	expect_any(GetAppendOnlyEntryAttributes, compresslevel);
	expect_any(GetAppendOnlyEntryAttributes, checksum);
	expect_any(GetAppendOnlyEntryAttributes, compresstype);
	will_be_called(GetAppendOnlyEntryAttributes);

	/*
	 * We used to mock AppendOnlyStorageRead_Init() here, however as the mocked
	 * one does not initialize desc->ao_read.storageAttributes at all, it makes
	 * the following assertion flaky.
	 *
	 * On the other hand aocs_begin_headerscan() itself does not do many useful
	 * things, the actual job is done inside AppendOnlyStorageRead_Init(), so
	 * to make the test more useful we removed the mocking and test against the
	 * real AppendOnlyStorageRead_Init() now.
	 */
	desc = aocs_begin_headerscan(&reldata, 0);
	assert_false(desc->ao_read.storageAttributes.compress);
	assert_int_equal(desc->colno, 0);
}


static void
test__aocs_writecol_init(void **state)
{
	AOCSWriteColumnDesc desc;
	NewColumnValue *newval1 = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
	NewColumnValue *newval2 = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
	RelationData reldata;
	SMgrRelationData smgrdata;
	int			nattr = 5;
	StdRdOptions **opts =
	(StdRdOptions **) malloc(sizeof(StdRdOptions *) * nattr);
	wal_level = WAL_LEVEL_REPLICA;
	List *newvals = NIL;

	newval1->attnum = 4;
	newval1->op = AOCSADDCOLUMN;
	newval2->attnum = 5;
	newval2->op = AOCSADDCOLUMN;
	newvals = lappend(newvals, newval1);
	newvals = lappend(newvals, newval2);

	/* 3 existing columns */
	opts[0] = opts[1] = opts[2] = (StdRdOptions *) NULL;

	/* 2 newly added columns */
	opts[3] = (StdRdOptions *) malloc(sizeof(StdRdOptions));
	strcpy(opts[3]->compresstype, "rle_type");
	opts[3]->compresslevel = 2;
	opts[3]->blocksize = 8192;
	opts[4] = (StdRdOptions *) malloc(sizeof(StdRdOptions));
	strcpy(opts[4]->compresstype, "none");
	opts[4]->compresslevel = 0;
	opts[4]->blocksize = 8192 * 2;

	/* One call to RelationGetAttributeOptions() */
	expect_any(RelationGetAttributeOptions, rel);
	will_return(RelationGetAttributeOptions, opts);

	/* Two calls to create_datumstreamwrite() */
	expect_string(create_datumstreamwrite, compName, "rle_type");
	expect_string(create_datumstreamwrite, compName, "none");
	expect_value(create_datumstreamwrite, compLevel, 2);
	expect_value(create_datumstreamwrite, compLevel, 0);
	expect_any_count(create_datumstreamwrite, checksum, 2);
	expect_value(create_datumstreamwrite, maxsz, 8192);
	expect_value(create_datumstreamwrite, maxsz, 8192 * 2);
	expect_value(create_datumstreamwrite, needsWAL, true);
	expect_value(create_datumstreamwrite, needsWAL, true);
	expect_any(create_datumstreamwrite, rnode);
	expect_any(create_datumstreamwrite, rnode);
	expect_any(create_datumstreamwrite, smgrAO);
	expect_any(create_datumstreamwrite, smgrAO);
	expect_any_count(create_datumstreamwrite, attr, 2);
	expect_any_count(create_datumstreamwrite, relname, 2);
	expect_any(create_datumstreamwrite, reloid);
	expect_any(create_datumstreamwrite, reloid);
	expect_any_count(create_datumstreamwrite, title, 2);
	will_return_count(create_datumstreamwrite, NULL, 2);

	FormData_pg_class rel;
	rel.relpersistence = RELPERSISTENCE_PERMANENT;
	reldata.rd_id = 12345;
	reldata.rd_rel = &rel;
	reldata.rd_att = (TupleDesc) malloc(sizeof(TupleDescData) +
										(sizeof(Form_pg_attribute *) * nattr));
	memset(reldata.rd_att->attrs, 0, sizeof(Form_pg_attribute *) * nattr);
	reldata.rd_att->natts = nattr;

	smgrdata.smgr_ao = smgrAOGetDefault();
	reldata.rd_smgr = &smgrdata;

	expect_value(GetAppendOnlyEntryAttributes, relid, 12345);
	expect_any(GetAppendOnlyEntryAttributes, blocksize);
	expect_any(GetAppendOnlyEntryAttributes, compresslevel);
	expect_any(GetAppendOnlyEntryAttributes, checksum);
	expect_any(GetAppendOnlyEntryAttributes, compresstype);
	will_be_called(GetAppendOnlyEntryAttributes);

	/* 3 existing columns, 2 new columns */
	desc = aocs_writecol_init(&reldata, newvals, AOCSADDCOLUMN);
	assert_int_equal(desc->num_cols_to_write, 2);
	assert_int_equal(desc->cur_segno, -1);
}

/*
 * Ensure that the column having the smallest on-disk segfile is
 * chosen for headerscan during ALTER TABLE ADD COLUMN operation.
 */
static void
test__get_anchor_col(void **state)
{
	RelationData reldata;
	AOCSFileSegInfo *segInfos[4];
	int numcols = 3;
	int col;
	bool *lastrownums_exist;
	HeapTupleData dummy_tuple;
	AttrNumber proj_atts[2];

	/* Empty segment, should be skipped over */
	segInfos[0] = (AOCSFileSegInfo *)
			malloc(sizeof(AOCSFileSegInfo) + sizeof(AOCSVPInfoEntry)*numcols);
	segInfos[0]->segno = 3;
	segInfos[0]->state = AOSEG_STATE_DEFAULT;
	segInfos[0]->total_tupcount = 0;
	segInfos[0]->vpinfo.nEntry = 3; /* number of columns */
	segInfos[0]->vpinfo.entry[0].eof = 200;
	segInfos[0]->vpinfo.entry[0].eof_uncompressed = 200;
	segInfos[0]->vpinfo.entry[1].eof = 100;
	segInfos[0]->vpinfo.entry[1].eof_uncompressed = 165;
	segInfos[0]->vpinfo.entry[2].eof = 50;
	segInfos[0]->vpinfo.entry[2].eof_uncompressed = 85;

	/* Valid segment, col=1 is the smallest */
	segInfos[1] = (AOCSFileSegInfo *)
			malloc(sizeof(AOCSFileSegInfo) + sizeof(AOCSVPInfoEntry)*numcols);
	segInfos[1]->segno = 2;
	segInfos[1]->total_tupcount = 51;
	segInfos[1]->state = AOSEG_STATE_DEFAULT;
	segInfos[1]->vpinfo.nEntry = 3; /* number of columns */
	segInfos[1]->vpinfo.entry[0].eof = 120;
	segInfos[1]->vpinfo.entry[0].eof_uncompressed = 200;
	segInfos[1]->vpinfo.entry[1].eof = 100;
	segInfos[1]->vpinfo.entry[1].eof_uncompressed = 100;
	segInfos[1]->vpinfo.entry[2].eof = 320;
	segInfos[1]->vpinfo.entry[2].eof_uncompressed = 400;

	/* AWATING_DROP segment, should be skipped over */
	segInfos[2] = (AOCSFileSegInfo *)
			malloc(sizeof(AOCSFileSegInfo) + sizeof(AOCSVPInfoEntry)*numcols);
	segInfos[2]->segno = 3;
	segInfos[2]->state = AOSEG_STATE_AWAITING_DROP;
	segInfos[2]->total_tupcount = 15;
	segInfos[2]->vpinfo.nEntry = 3; /* number of columns */
	segInfos[2]->vpinfo.entry[0].eof = 141;
	segInfos[2]->vpinfo.entry[0].eof_uncompressed = 200;
	segInfos[2]->vpinfo.entry[1].eof = 51;
	segInfos[2]->vpinfo.entry[1].eof_uncompressed = 65;
	segInfos[2]->vpinfo.entry[2].eof = 20;
	segInfos[2]->vpinfo.entry[2].eof_uncompressed = 80;

	/* Valid segment, col=0 is the smallest */
	segInfos[3] = (AOCSFileSegInfo *)
			malloc(sizeof(AOCSFileSegInfo) + sizeof(AOCSVPInfoEntry)*numcols);
	segInfos[3]->segno = 1;
	segInfos[3]->state = AOSEG_STATE_USECURRENT;
	segInfos[3]->total_tupcount = 135;
	segInfos[3]->vpinfo.nEntry = 3; /* number of columns */
	segInfos[3]->vpinfo.entry[0].eof = 60;
	segInfos[3]->vpinfo.entry[0].eof_uncompressed = 80;
	segInfos[3]->vpinfo.entry[1].eof = 500;
	segInfos[3]->vpinfo.entry[1].eof_uncompressed = 650;
	segInfos[3]->vpinfo.entry[2].eof = 100;
	segInfos[3]->vpinfo.entry[2].eof_uncompressed = 120;

	/* col=0 has lastrownums, meaning it has missing values, won't be picked in any case */
	lastrownums_exist = (bool*) palloc(numcols * sizeof(bool));
	lastrownums_exist[0] = true;
	lastrownums_exist[1] = false;
	lastrownums_exist[2] = false;
	expect_any_count(ExistValidLastrownums, relid, 2);
	expect_any_count(ExistValidLastrownums, natts, 2);
	will_return_count(ExistValidLastrownums, lastrownums_exist, 2);

	/* purpose of dummy tuple: make the column appear non-dropped*/
	expect_any_count(SearchSysCacheAttNum, relid, -1);
	expect_any_count(SearchSysCacheAttNum, attnum, -1);
	will_return_count(SearchSysCacheAttNum, &dummy_tuple, -1);
	expect_any_count(ReleaseSysCache, tuple, -1);
	will_be_called_count(ReleaseSysCache, -1);

	/* Without projection, col=1 (vpe index 1) has the smallest eof */
	col = get_anchor_col(segInfos, 4, numcols, &reldata, NULL, 0);
	assert_int_equal(col, 1);

	/* With projection (excluding col=1), col=2 is going to be picked */
	proj_atts[0] = 0;
	proj_atts[1] = 2;
	col = get_anchor_col(segInfos, 4, numcols, &reldata, proj_atts, 2);
	assert_int_equal(col, 2);
}

/*
 * Ensure that the column consulted by unique constraint checks is picked
 * deterministically: the first non-dropped complete column, falling back to
 * a dropped complete column, while never picking an incomplete column.
 */
static void
test__aocs_unique_check_col(void **state)
{
	RelationData reldata;
	FormData_pg_class pgclass;
	int numcols = 3;
	bool *lastrownums_exist;

	reldata.rd_rel = &pgclass;
	reldata.rd_id = 12345;
	reldata.rd_rel->relnatts = numcols;
	reldata.rd_att = (TupleDesc) palloc0(sizeof(TupleDescData) +
										 sizeof(FormData_pg_attribute) * numcols);
	reldata.rd_att->natts = numcols;

	/* all columns complete and non-dropped: the first column is picked */
	lastrownums_exist = (bool *) palloc0(numcols * sizeof(bool));
	expect_any(ExistValidLastrownums, relid);
	expect_any(ExistValidLastrownums, natts);
	will_return(ExistValidLastrownums, lastrownums_exist);
	assert_int_equal(aocs_unique_check_col(&reldata), 0);

	/* first column dropped: the first non-dropped complete column is picked */
	TupleDescAttr(reldata.rd_att, 0)->attisdropped = true;
	lastrownums_exist = (bool *) palloc0(numcols * sizeof(bool));
	expect_any(ExistValidLastrownums, relid);
	expect_any(ExistValidLastrownums, natts);
	will_return(ExistValidLastrownums, lastrownums_exist);
	assert_int_equal(aocs_unique_check_col(&reldata), 1);

	/* incomplete columns are skipped over */
	TupleDescAttr(reldata.rd_att, 0)->attisdropped = false;
	lastrownums_exist = (bool *) palloc0(numcols * sizeof(bool));
	lastrownums_exist[0] = true;
	lastrownums_exist[1] = true;
	expect_any(ExistValidLastrownums, relid);
	expect_any(ExistValidLastrownums, natts);
	will_return(ExistValidLastrownums, lastrownums_exist);
	assert_int_equal(aocs_unique_check_col(&reldata), 2);

	/*
	 * If the only complete column is a dropped one, fall back to it (block
	 * directory entries are written even for dropped columns).
	 */
	TupleDescAttr(reldata.rd_att, 0)->attisdropped = true;
	lastrownums_exist = (bool *) palloc0(numcols * sizeof(bool));
	lastrownums_exist[1] = true;
	lastrownums_exist[2] = true;
	expect_any(ExistValidLastrownums, relid);
	expect_any(ExistValidLastrownums, natts);
	will_return(ExistValidLastrownums, lastrownums_exist);
	assert_int_equal(aocs_unique_check_col(&reldata), 0);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {
		unit_test(test__aocs_begin_headerscan),
		unit_test(test__aocs_writecol_init),
		unit_test(test__get_anchor_col),
		unit_test(test__aocs_unique_check_col)
	};

	MemoryContextInit();

	return run_tests(tests);
}

