/*-------------------------------------------------------------------------
 *
 * copyfrom.c
 *		COPY <table> FROM file/program/client
 *
 * This file contains routines needed to efficiently load tuples into a
 * table.  That includes looking up the correct partition, firing triggers,
 * calling the table AM function to insert the data, and updating indexes.
 * Reading data from the input file or client and parsing it into Datums
 * is handled in copyfromparse.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfrom.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_directory_table.h"
#include "catalog/storage_directory_table.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "common/base64.h"
#include "common/cryptohash.h"
#include "common/md5.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "storage/ufile.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/*
 * When doing a COPY FROM through the dispatcher, the QD reads the input from
 * the input file (or stdin or program), and forwards the data to the QE nodes,
 * where they will actually be inserted.
 *
 * Ideally, the QD would just pass through each line to the QE as is, and let
 * the QEs to do all the processing. Because the more processing the QD has
 * to do, the more likely it is to become a bottleneck.
 *
 * However, the QD needs to figure out which QE to send each row to. For that,
 * it needs to at least parse the distribution key. The distribution key might
 * also be a DEFAULTed column, in which case the DEFAULT value needs to be
 * evaluated in the QD. In that case, the QD must send the computed value
 * to the QE - we cannot assume that the QE can re-evaluate the expression and
 * arrive at the same value, at least not if the DEFAULT expression is volatile.
 *
 * Therefore, we need a flexible format between the QD and QE, where the QD
 * processes just enough of each input line to figure out where to send it.
 * It must send the values it had to parse and evaluate to the QE, as well
 * as the rest of the original input line, so that the QE can parse the rest
 * of it.
 *
 * The 'copy_from_dispatch_*' structs are used in the QD->QE stream. For each
 * input line, the QD constructs a 'copy_from_dispatch_row' struct, and sends
 * it to the QE. Before any rows, a QDtoQESignature is sent first, followed by
 * a 'copy_from_dispatch_header'. When QD encounters a recoverable error that
 * needs to be logged in the error log (LOG ERRORS SEGMENT REJECT LIMIT), it
 * sends the erroneous raw to a QE, in a 'copy_from_dispatch_error' struct.
 *
 *
 * COPY TO is simpler: The QEs form the output rows in the final form, and the QD
 * just collects and forwards them to the client. The QD doesn't need to parse
 * the rows at all.
 */
extern CopyStmt *glob_copystmt;

extern bool Test_copy_qd_qe_split;

/* Header contains information that applies to all the rows that follow. */
typedef struct
{
	/*
	 * First field that should be processed in the QE. Any fields before
	 * this will be included as Datums in the rows that follow.
	 */
	int16		first_qe_processed_field;
} copy_from_dispatch_header;

typedef struct
{
	/*
	 * Information about this input line.
	 *
	 * 'relid' is the target relation's OID. Normally, the same as
	 * cstate->relid, but for a partitioned relation, it indicates the target
	 * partition. Note: this must be the first field, because InvalidOid means
	 * that this is actually a 'copy_from_dispatch_error' struct.
	 *
	 * 'lineno' is the input line number, for error reporting.
	 */
	int64		lineno;
	Oid			relid;

	uint32		line_len;			/* size of the included input line */
	uint32		residual_off;		/* offset in the line, where QE should
									 * process remaining fields */
	bool		delim_seen_at_end;  /* conveys to QE if QD saw a delim at end
									 * of its processing */
	uint16		fld_count;			/* # of fields that were processed in the
									 * QD. */

	/* The input line follows. */

	/*
	 * For each field that was parsed in the QD already, the following data follows:
	 *
	 * int16	fieldnum;
	 * <data>
	 *
	 * NULL values are not included, any attributes that are not included in
	 * the message are implicitly NULL.
	 *
	 * For pass-by-value datatypes, the <data> is the raw Datum. For
	 * simplicity, it is always sent as a full-width 8-byte Datum, regardless
	 * of the datatype's length.
	 *
	 * For other fixed width datatypes, <data> is the datatype's value.
	 *
	 * For variable-length datatypes, <data> begins with a 4-byte length field,
	 * followed by the data. Cstrings (typlen = -2) are also sent in this
	 * format.
	 */
} copy_from_dispatch_row;

/* Size of the struct, without padding at the end. */
#define SizeOfCopyFromDispatchRow (offsetof(copy_from_dispatch_row, fld_count) + sizeof(uint16))

typedef struct
{
	int64		error_marker;	/* constant -1, to mark that this is an error
								 * frame rather than 'copy_from_dispatch_row' */
	int64		lineno;
	uint32		errmsg_len;
	uint32		line_len;

	/* 'errmsg' follows */
	/* 'line' follows */
} copy_from_dispatch_error;

/* Size of the struct, without padding at the end. */
/*
* GPDB_114_MERGE_FIXME: last file of copy_from_dispatch_error has changed.
* Check if line_buf_converted need anymore?
*/
#define SizeOfCopyFromDispatchError (offsetof(copy_from_dispatch_error, line_len) + sizeof(uint32))

/* Low-level communications functions */
static void
SendCopyFromForwardedTuple(CopyFromState cstate,
						   CdbCopy *cdbCopy,
						   bool toAll,
						   int target_seg,
						   Relation rel,
						   int64 lineno,
						   char *line,
						   int line_len,
						   Datum *values,
						   bool *nulls,
						   bool is_directory_table);
static void SendCopyFromForwardedHeader(CopyFromState cstate, CdbCopy *cdbCopy);
static void SendCopyFromForwardedError(CopyFromState cstate, CdbCopy *cdbCopy, char *errmsg);

static bool NextCopyFromDispatch(CopyFromState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls);
static bool NextCopyFromExecute(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls, bool is_directory_table);
static bool NextCopyFromRawFieldsX(CopyFromState cstate, char ***fields, int *nfields,
								   int stop_processing_at_field);
static bool NextCopyFromX(CopyFromState cstate, ExprContext *econtext,
						  Datum *values, bool *nulls);
static void HandleQDErrorFrame(CopyFromState cstate, char *p);

static void CopyInitDataParser(CopyFromState cstate);

static GpDistributionData *InitDistributionData(CopyFromState cstate, EState *estate);
static void FreeDistributionData(GpDistributionData *distData);
static void InitCopyFromDispatchSplit(CopyFromState cstate, GpDistributionData *distData, EState *estate);
static unsigned int GetTargetSeg(GpDistributionData *distData, TupleTableSlot *slot);

static uint64 CopyFromDirectoryTable(CopyFromState cstate);
static CopyFromState BeginCopyFromDirectoryTable(ParseState *pstate, Relation rel,
												 const char *filename, bool is_program, List *options);

/*
 * No more than this many tuples per CopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * CopyMultiInsertBuffer items stored in CopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
#define MAX_BUFFERED_TUPLES		1000

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
#define MAX_BUFFERED_BYTES		65535

/* Trim the list of buffers back down to this number after flushing */
#define MAX_PARTITION_BUFFERS	32

/* The buffer size of directory table files */
#define DIR_FILE_BUFF_SIZE 8192

/* Stores multi-insert data related to a single relation in CopyFrom. */
typedef struct CopyMultiInsertBuffer
{
	TupleTableSlot *slots[MAX_BUFFERED_TUPLES]; /* Array to store tuples */
	ResultRelInfo *resultRelInfo;	/* ResultRelInfo for 'relid' */
	BulkInsertState bistate;	/* BulkInsertState for this rel */
	int			nused;			/* number of 'slots' containing tuples */
	uint64		linenos[MAX_BUFFERED_TUPLES];	/* Line # of tuple in copy
												 * stream */
} CopyMultiInsertBuffer;

/*
 * Stores one or many CopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 */
typedef struct CopyMultiInsertInfo
{
	List	   *multiInsertBuffers; /* List of tracked CopyMultiInsertBuffers */
	int			bufferedTuples; /* number of tuples buffered over all buffers */
	int			bufferedBytes;	/* number of bytes from all buffered tuples */
	CopyFromState cstate;		/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;

static const char QDtoQESignature[] = "PGCOPY-QD-TO-QE\n\377\r\n";

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyFromState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyFromState cstate = (CopyFromState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->opts.binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("COPY %s, line %s",
					   cstate->cur_relname, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %s, column %s: \"%s\"",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %s, column %s: null input",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, print it.
			 */
			if (cstate->line_buf_valid)
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %s: \"%s\"",
						   cstate->cur_relname, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * Returns a pstrdup'd copy of the input.
 */
char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			slen = strlen(str);
	int			len;
	char	   *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *) palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/*
 * Allocate memory and initialize a new CopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
static CopyMultiInsertBuffer *
CopyMultiInsertBufferInit(ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = (CopyMultiInsertBuffer *) palloc(sizeof(CopyMultiInsertBuffer));
	memset(buffer->slots, 0, sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	buffer->resultRelInfo = rri;
	buffer->bistate = GetBulkInsertState();
	buffer->nused = 0;

	return buffer;
}

/*
 * Make a new buffer for this ResultRelInfo.
 */
static inline void
CopyMultiInsertInfoSetupBuffer(CopyMultiInsertInfo *miinfo,
							   ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = CopyMultiInsertBufferInit(rri);

	/* Setup back-link so we can easily find this buffer again */
	rri->ri_CopyMultiInsertBuffer = buffer;
	/* Record that we're tracking this buffer */
	miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
}

/*
 * Initialize an already allocated CopyMultiInsertInfo.
 *
 * If rri is a non-partitioned table then a CopyMultiInsertBuffer is set up
 * for that table.
 */
static void
CopyMultiInsertInfoInit(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						CopyFromState cstate, EState *estate, CommandId mycid,
						int ti_options)
{
	miinfo->multiInsertBuffers = NIL;
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
	miinfo->cstate = cstate;
	miinfo->estate = estate;
	miinfo->mycid = mycid;
	miinfo->ti_options = ti_options;

	/*
	 * Only setup the buffer when not dealing with a partitioned table.
	 * Buffers for partitioned tables will just be setup when we need to send
	 * tuples their way for the first time.
	 */
	if (rri->ri_RelationDesc->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		CopyMultiInsertInfoSetupBuffer(miinfo, rri);
}

/*
 * Returns true if the buffers are full
 */
static inline bool
CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo)
{
	if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
		miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
		return true;
	return false;
}

/*
 * Returns true if we have no buffered tuples
 */
static inline bool
CopyMultiInsertInfoIsEmpty(CopyMultiInsertInfo *miinfo)
{
	return miinfo->bufferedTuples == 0;
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
static inline void
CopyMultiInsertBufferFlush(CopyMultiInsertInfo *miinfo,
						   CopyMultiInsertBuffer *buffer)
{
	MemoryContext oldcontext;
	int			i;
	uint64		save_cur_lineno;
	CopyFromState cstate = miinfo->cstate;
	EState	   *estate = miinfo->estate;
	CommandId	mycid = miinfo->mycid;
	int			ti_options = miinfo->ti_options;
	bool		line_buf_valid = cstate->line_buf_valid;
	int			nused = buffer->nused;
	ResultRelInfo *resultRelInfo = buffer->resultRelInfo;
	TupleTableSlot **slots = buffer->slots;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fails.
	 */
	cstate->line_buf_valid = false;
	save_cur_lineno = cstate->cur_lineno;

	/*
	 * table_multi_insert may leak memory, so switch to short-lived memory
	 * context before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	table_multi_insert(resultRelInfo->ri_RelationDesc,
					   slots,
					   nused,
					   mycid,
					   ti_options,
					   buffer->bistate);
	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < nused; i++)
	{
		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (resultRelInfo->ri_NumIndices > 0)
		{
			List	   *recheckIndexes;

			cstate->cur_lineno = buffer->linenos[i];
			recheckIndexes =
				ExecInsertIndexTuples(resultRelInfo,
									  buffer->slots[i], estate, false, false,
									  NULL, NIL);
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], recheckIndexes,
								 cstate->transition_capture);
			list_free(recheckIndexes);
		}

		/*
		 * There's no indexes, but see if we need to run AFTER ROW INSERT
		 * triggers anyway.
		 */
		else if (resultRelInfo->ri_TrigDesc != NULL &&
				 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
				  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		{
			cstate->cur_lineno = buffer->linenos[i];
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], NIL, cstate->transition_capture);
		}

		ExecClearTuple(slots[i]);
	}

	/* Mark that all slots are free */
	buffer->nused = 0;

	/* reset cur_lineno and line_buf_valid to what they were */
	cstate->line_buf_valid = line_buf_valid;
	cstate->cur_lineno = save_cur_lineno;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
static inline void
CopyMultiInsertBufferCleanup(CopyMultiInsertInfo *miinfo,
							 CopyMultiInsertBuffer *buffer)
{
	int			i;

	/* Ensure buffer was flushed */
	Assert(buffer->nused == 0);

	/* Remove back-link to ourself */
	buffer->resultRelInfo->ri_CopyMultiInsertBuffer = NULL;

	FreeBulkInsertState(buffer->bistate);

	/* Since we only create slots on demand, just drop the non-null ones. */
	for (i = 0; i < MAX_BUFFERED_TUPLES && buffer->slots[i] != NULL; i++)
		ExecDropSingleTupleTableSlot(buffer->slots[i]);

	if (RelationIsNonblockRelation(buffer->resultRelInfo->ri_RelationDesc))
	{
		/*
		 * CBDB: do not call table_finish_bulk_insert for AO/AOCO or PAX tables.
		 * https://github.com/apache/cloudberry/issues/547
		 * Do not clean up context or resource here, table_finish_bulk_insert
		 * routine will be called more than once during COPY FROM if
		 * the partition buffer is flushed but COPY is not finished.
		 */
		pfree(buffer);
		return;
	}

	table_finish_bulk_insert(buffer->resultRelInfo->ri_RelationDesc,
							 miinfo->ti_options);

	pfree(buffer);
}

/*
 * Write out all stored tuples in all buffers out to the tables.
 *
 * Once flushed we also trim the tracked buffers list down to size by removing
 * the buffers created earliest first.
 *
 * Callers should pass 'curr_rri' as the ResultRelInfo that's currently being
 * used.  When cleaning up old buffers we'll never remove the one for
 * 'curr_rri'.
 */
static inline void
CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, ResultRelInfo *curr_rri)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
	{
		CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) lfirst(lc);

		CopyMultiInsertBufferFlush(miinfo, buffer);
	}

	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;

	/*
	 * Trim the list of tracked buffers down if it exceeds the limit.  Here we
	 * remove buffers starting with the ones we created first.  It seems less
	 * likely that these older ones will be needed than the ones that were
	 * just created.
	 */
	while (list_length(miinfo->multiInsertBuffers) > MAX_PARTITION_BUFFERS)
	{
		CopyMultiInsertBuffer *buffer;

		buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);

		/*
		 * We never want to remove the buffer that's currently being used, so
		 * if we happen to find that then move it to the end of the list.
		 */
		if (buffer->resultRelInfo == curr_rri)
		{
			miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
			miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
			buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);
		}

		CopyMultiInsertBufferCleanup(miinfo, buffer);
		miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
	}
}

/*
 * Cleanup allocated buffers and free memory
 */
static inline void
CopyMultiInsertInfoCleanup(CopyMultiInsertInfo *miinfo)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
		CopyMultiInsertBufferCleanup(miinfo, lfirst(lc));

	list_free(miinfo->multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
 */
static inline TupleTableSlot *
CopyMultiInsertInfoNextFreeSlot(CopyMultiInsertInfo *miinfo,
								ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;
	int			nused = buffer->nused;

	Assert(buffer != NULL);
	Assert(nused < MAX_BUFFERED_TUPLES);

	if (buffer->slots[nused] == NULL)
		buffer->slots[nused] = table_slot_create(rri->ri_RelationDesc, NULL);
	return buffer->slots[nused];
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * CopyMultiInsertInfoNextFreeSlot as being consumed.
 */
static inline void
CopyMultiInsertInfoStore(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						 TupleTableSlot *slot, int tuplen, uint64 lineno)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;

	Assert(buffer != NULL);
	Assert(slot == buffer->slots[buffer->nused]);

	/* Store the line number so we can properly report any errors later */
	buffer->linenos[buffer->nused] = lineno;

	/* Record this slot as being used */
	buffer->nused++;

	/* Update how many tuples are stored and their size */
	miinfo->bufferedTuples++;
	miinfo->bufferedBytes += tuplen;
}

static CopyStmt *
convertToCopyTextStmt(CopyStmt *stmt)
{
	CopyStmt *copiedStmt = copyObject(stmt);

	copiedStmt->options = NIL;

	return copiedStmt;
}

static char *
trimFilePath(char *filePath, char c)
{
	char *end;

	while (*filePath == c)
		filePath++;

	end = filePath + strlen(filePath) - 1;
	while (end > filePath && *end == c)
		end--;

	*(end + 1) = '\0';

	return filePath;
}

static void
formDirTableSlot(CopyFromState cstate,
				 Oid spcId,
				 char *relativePath,
				 int64 fileSize,
				 char *md5sum,
				 char *tags,
				 Datum *values,
				 bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs;
	ListCell   *cur;
	char	   *field[5];
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	List	   *attnumlist = cstate->attnumlist;
	pg_time_t	stampTime = (pg_time_t) time(NULL);
	char		lastModified[128];

	pg_strftime(lastModified, sizeof(lastModified),
				"%Y-%m-%d %H:%M:%S",
				pg_localtime(&stampTime, log_timezone));

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;

	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, false, num_phys_attrs * sizeof(bool));

	field[0] = relativePath; /* relative_path */
	field[1] = psprintf(INT64_FORMAT, fileSize); /* size */
	field[2] = lastModified; /* last_modified */
	field[3] = md5sum; /* md5sum */
	if (md5sum == NULL)
		nulls[3] = true;
	field[4] = tags; /* tags */
	if (tags == NULL)
		nulls[4] = true;

	/* Loop to read the user attributes on the line. */
	foreach(cur, attnumlist)
	{
		int    attnum = lfirst_int(cur);
		int    m = attnum - 1;
		char *value;
		Form_pg_attribute att = TupleDescAttr(tupDesc, m);

		value = field[m];

		values[m] = InputFunctionCall(&in_functions[m],
									  value,
									  typioparams[m],
									  att->atttypmod);
	}
}

/*
 * Copy From file to directory table.
 */
static uint64
CopyFromDirectoryTable(CopyFromState cstate)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	EState	   *estate = CreateExecutorState();
	TupleTableSlot *myslot = NULL;
	ExprContext *econtext;
	int			bytesRead;
	char		hexMd5Sum[256];
	char		buffer[DIR_FILE_BUFF_SIZE];
	int64		processed = 0;
	int64		fileSize = 0;
	UFile	   *file;
	CdbCopy	   *cdbCopy = NULL;
	char	   *dirTablePath;
	char	   *orgiFileName;
	char	   *relaFileDirPointer;
	char	   relaFileDir[MAXPGPATH] = {0};
	char	   *orgiFileDir = NULL;
	char	   *relaFileName;
	TupleDesc	tupdesc;
	unsigned int targetSeg;
	DirectoryTable     *dirTable;
	pg_cryptohash_ctx  *hashCtx;
	uint8 md5Sum[MD5_DIGEST_LENGTH];
	char		errorMessage[256];
	GpDistributionData *distData = NULL; /* distribution data used to compute target seg */

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = target_resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);

	ExecOpenIndices(resultRelInfo, false);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 */
	cstate->transition_capture = MakeTransitionCaptureState(cstate->rel->trigdesc,
															RelationGetRelid(cstate->rel),
															CMD_INSERT);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	/* Assemble directory table file location. */
	relaFileName = trimFilePath(glob_copystmt->dirfilename, '/');
	relaFileDirPointer = strrchr(relaFileName, '/');
	if (relaFileDirPointer)
	{
		memcpy(relaFileDir, relaFileName, relaFileDirPointer - relaFileName);
	}

	dirTable = GetDirectoryTable(RelationGetRelid(cstate->rel));
	dirTablePath = dirTable->location;
	orgiFileName = psprintf("%s/%s", dirTablePath, relaFileName);
	if (relaFileDir[0] != '\0')
		orgiFileDir = psprintf("%s/%s", dirTablePath, relaFileDir);

	/*
	 * build tupledesc and slot for copy from
	 */
	tupdesc = CreateTemplateTupleDesc(5);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "relative_path",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "size",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "last_modified",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "md5",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "tag",
					   TEXTOID, -1 ,0);

	myslot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);

	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		/*
		 * Initialize information about distribution keys, needed to compute target
		 * segment for each row.
		 */
		distData = InitDistributionData(cstate, estate);

		/* Determine which fields we need to parse in the QD. */
		InitCopyFromDispatchSplit(cstate, distData, estate);
	}

	if (cstate->dispatch_mode == COPY_DISPATCH ||
		cstate->dispatch_mode == COPY_EXECUTOR)
	{
		/*
		 * Now split the attnumlist into the parts that are parsed in the QD, and
		 * in QE.
		 */
		ListCell   *lc;
		int			i = 0;
		List	   *qd_attnumlist = NIL;
		List	   *qe_attnumlist = NIL;
		int			first_qe_processed_field;

		first_qe_processed_field = cstate->first_qe_processed_field;

		foreach(lc, cstate->attnumlist)
		{
			int			attnum = lfirst_int(lc);

			if (i < first_qe_processed_field)
				qd_attnumlist = lappend_int(qd_attnumlist, attnum);
			else
				qe_attnumlist = lappend_int(qe_attnumlist, attnum);
			i++;
		}
		cstate->qd_attnumlist = qd_attnumlist;
		cstate->qe_attnumlist = qe_attnumlist;
	}

	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		/*
		 * We are the QD node, and we are receiving rows from client, or
		 * reading them from a file. We are not writing any data locally,
		 * instead, we determine the correct target segment for row,
		 * and forward each to the correct segment.
		 */

		/*
		 * pre-allocate buffer for constructing a message.
		 */
		cstate->dispatch_msgbuf = makeStringInfo();
		enlargeStringInfo(cstate->dispatch_msgbuf, SizeOfCopyFromDispatchRow);

		formDirTableSlot(cstate, 
						 dirTable->spcId,
						 relaFileName,
						 0,
						 NULL,
						 cstate->opts.tags,
						 myslot->tts_values,
						 myslot->tts_isnull);
		ExecStoreVirtualTuple(myslot);

		targetSeg = GetTargetSeg(distData, myslot);

		/*
		 * prepare to COPY data into segDBs:
		 */
		cdbCopy = makeCdbCopyFrom(cstate);

		/*
		 * Dispatch the COPY command.
		 */
		elog(DEBUG5, "COPY command sent to segdbs");

		cdbCopyStart(cdbCopy, convertToCopyTextStmt(glob_copystmt), cstate->file_encoding);

		/*
		 * Header processing.
		 */
		SendCopyFromForwardedHeader(cstate, cdbCopy);

		/* in the QD, forward the row to the correct segment(s). */
		SendCopyFromForwardedTuple(cstate, cdbCopy, false,
								   targetSeg,
								   resultRelInfo->ri_RelationDesc,
								   cstate->cur_lineno,
								   cstate->line_buf.data,
								   cstate->line_buf.len,
								   myslot->tts_values,
								   myslot->tts_isnull,
								   true);

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			bytesRead = CopyReadBinaryData(cstate, buffer, DIR_FILE_BUFF_SIZE);

			if (bytesRead > 0)
			{
				cdbCopySendData(cdbCopy, targetSeg, buffer, bytesRead);
			}

			if (bytesRead != DIR_FILE_BUFF_SIZE)
			{
				Assert(cstate->raw_reached_eof == true);
				break;
			}
		}

		{
			int64		total_completed_from_qes;
			int64		total_rejected_from_qes;

			cdbCopyEnd(cdbCopy,
					   &total_completed_from_qes,
					   &total_rejected_from_qes);

			processed = total_completed_from_qes;
		}
	}
	else if (cstate->dispatch_mode == COPY_EXECUTOR)
	{
#define DIRECTORY_TABLE_COLUMNS	5
		List	   *recheckIndexes = NIL;
		CommandId	mycid = GetCurrentCommandId(true);
		MemoryContext oldcontext = CurrentMemoryContext;
		bool		has_tuple = false;
		bool		update_indexes;

		econtext = GetPerTupleExprContext(estate);

		if (NextCopyFromExecute(cstate, econtext, myslot->tts_values, myslot->tts_isnull, true))
		{
			has_tuple = true;

			/*
			 * Reset the per-tuple exprcontext. We do this after every tuple, to
			 * clean-up after expression evaluations etc.
			 */
			ResetPerTupleExprContext(estate);

			/*
			 * Switch to per-tuple context before calling NextCopyFrom, which does
			 * evaluate default expressions etc. and requires per-tuple context.
			 */
			MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

			/*
			 * NextCopyFromExecute set up estate->es_result_relation_info,
			 * and stored the tuple in the correct slot.
			 */
			resultRelInfo = estate->es_result_relations[0];

			ExecStoreVirtualTuple(myslot);

			/*
			 * Constraints and where clause might reference the tableoid column,
			 * so (re-)initialize tts_tableOid before evaluating them.
			 */
			myslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);

			/* Triggers and stuff need to be invoked in query context. */
			MemoryContextSwitchTo(oldcontext);

			/* OK, store the tuple and create index entries for it */
			table_tuple_insert(resultRelInfo->ri_RelationDesc,
							   myslot, mycid, 0, NULL);

			recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
												   myslot,
												   estate,
												   false,
												   false,
												   NULL,
												   NIL);

			/* AFTER ROW INSERT Triggers */
			ExecARInsertTriggers(estate, resultRelInfo, myslot,
								 recheckIndexes, cstate->transition_capture);

			list_free(recheckIndexes);

			CommandCounterIncrement();
		}

		if (has_tuple)
		{
			if (UFileExists(dirTable->spcId, orgiFileName))
			{
				UFileUnlink(dirTable->spcId, orgiFileName);
			}

			if (orgiFileDir)
				UFileEnsurePath(dirTable->spcId, orgiFileDir);

			file = UFileOpen(dirTable->spcId,
							 orgiFileName,
							 O_CREAT | O_WRONLY,
							 errorMessage,
							 sizeof(errorMessage));

			if (file == NULL)
				ereport(ERROR,
							(errcode(ERRCODE_IO_ERROR),
							 errmsg("failed to open file \"%s\": %s", orgiFileName, errorMessage)));

			/* Delete uploaded file when the transaction fails */
			UFileAddPendingDelete(cstate->rel, dirTable->spcId, orgiFileName, false);

			/* FIXME:
			 *
			 * Even if we use FileExist function, writing to the same file in two
			 * concurrent sessions can still cause the file content to be corrupted.
			 *
			 * 1. Some DFS don't support renaming files, which means we can't use
			 * the common technique of generating a random filename for upload and
			 * then renaming it to the final name once it's complete.
			 *
			 * 2. Seems like unique index could be a good solution to fix the issue,
			 * But unique indexes can cause concurrent sessions to wait for each
			 * other(see comments in _bt_doinsert), and uploads can take a long time,
			 * so transactions waiting for each other for a long time without releasing
			 * resources is also not ideal(I know that we can set lock timeout).
			 *
			 * Another reason for not using unique index is that the file is uploading
			 * in the copy context, based on the current copy protocol, we are not able
			 * to insert a record first, then, once the file is uploaded, we could update
			 * the record.
			 *
			 * 3. We should probably create a file status table in catalog service
			 * to keep track of files currrently being uploaded.
			 */
			hashCtx = pg_cryptohash_create(PG_MD5);
			if (hashCtx == NULL)
				ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("failed to create md5hash context: out of memory")));
			pg_cryptohash_init(hashCtx);

			for (;;)
			{
				CHECK_FOR_INTERRUPTS();

				bytesRead = CopyGetData(cstate, buffer, DIR_FILE_BUFF_SIZE, DIR_FILE_BUFF_SIZE);

				if (bytesRead > 0)
				{
					if (UFileWrite(file, buffer, bytesRead) == -1)
						ereport(ERROR,
									(errcode(ERRCODE_IO_ERROR),
									 errmsg("failed to write file \"%s\": %s", orgiFileName, UFileGetLastError(file))));

					fileSize += bytesRead;
					pg_cryptohash_update(hashCtx, (const uint8 *) buffer, bytesRead);
				}

				if (bytesRead != DIR_FILE_BUFF_SIZE)
				{
					break;
				}
			}

			if (UFileSync(file) != 0)
				ereport(ERROR,
							(errcode(ERRCODE_IO_ERROR),
							 errmsg("unable to sync the file \"%s\": %s", glob_copystmt->dirfilename, UFileGetLastError(file))));

			if (UFileClose(file) < 0)
				ereport(ERROR,
							(errcode(ERRCODE_IO_ERROR),
							 errmsg("failed to close the file \"%s\": %s", glob_copystmt->dirfilename, UFileGetLastError(file))));

			pfree(file);

			pg_cryptohash_final(hashCtx, md5Sum, sizeof(md5Sum));
			pg_cryptohash_free(hashCtx);
			bytesToHex(md5Sum, hexMd5Sum);

			myslot->tts_values[1] = Int64GetDatum(fileSize);
			myslot->tts_values[3] = CStringGetTextDatum((char *) hexMd5Sum);
			myslot->tts_isnull[3] = false;

			simple_table_tuple_update(resultRelInfo->ri_RelationDesc, &myslot->tts_tid, myslot,
									  estate->es_snapshot, &update_indexes);

			ExecClearTuple(myslot);

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger
			 * or FDW; this is the same definition used by nodeModifyTable.c
			 * for counting tuples inserted by an INSERT command.  Update
			 * progress of the COPY command as well.
			 *
			 * MPP: incrementing this counter here only matters for utility
			 * mode. in dispatch mode only the dispatcher COPY collects row
			 * count, so this counter is meaningless.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
			
		}
	}
	else
	{
		List	   *recheckIndexes = NIL;
		CommandId	mycid = GetCurrentCommandId(true);
		bool		update_indexes;

		formDirTableSlot(cstate,
						 dirTable->spcId,
						 relaFileName,
						 0,
						 NULL,
						 cstate->opts.tags,
						 myslot->tts_values,
						 myslot->tts_isnull);
		ExecStoreVirtualTuple(myslot);

		/* OK, store the tuple and create index entries for it */
		table_tuple_insert(resultRelInfo->ri_RelationDesc,
						   myslot, mycid, 0, NULL);

		recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
											   myslot,
											   estate,
											   false,
											   false,
											   NULL,
											   NIL);

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, myslot,
							 recheckIndexes, cstate->transition_capture);

		list_free(recheckIndexes);

		CommandCounterIncrement();

		if (UFileExists(dirTable->spcId, orgiFileName))
		{
			UFileUnlink(dirTable->spcId, orgiFileName);
		}

		if (orgiFileDir)
			UFileEnsurePath(dirTable->spcId, orgiFileDir);

		file = UFileOpen(dirTable->spcId,
						 orgiFileName,
						 O_CREAT | O_WRONLY,
						 errorMessage,
						 sizeof(errorMessage));

		if (file == NULL)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("failed to open file \"%s\": %s", orgiFileName, errorMessage)));

		/* Delete uploaded file when the transaction fails */
		UFileAddPendingDelete(cstate->rel, dirTable->spcId, orgiFileName, false);

		hashCtx = pg_cryptohash_create(PG_MD5);
		if (hashCtx == NULL)
			ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("failed to create md5hash context: out of memory")));
		pg_cryptohash_init(hashCtx);

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			bytesRead = CopyReadBinaryData(cstate, buffer, DIR_FILE_BUFF_SIZE);

			if (bytesRead > 0)
			{
				if (UFileWrite(file, buffer, bytesRead) == -1)
					ereport(ERROR,
								(errcode(ERRCODE_IO_ERROR),
								 errmsg("failed to write file \"%s\": %s", orgiFileName, UFileGetLastError(file))));

				fileSize += bytesRead;
				pg_cryptohash_update(hashCtx, (const uint8 *) buffer, bytesRead);
			}

			if (bytesRead != DIR_FILE_BUFF_SIZE)
			{
				break;
			}
		}

		if (UFileSync(file) != 0)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("unable to sync file \"%s\": %s", glob_copystmt->dirfilename, UFileGetLastError(file))));

		if (UFileClose(file) < 0)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("failed to close the file \"%s\": %s", glob_copystmt->dirfilename, UFileGetLastError(file))));

		pfree(file);

		pg_cryptohash_final(hashCtx, md5Sum, sizeof(md5Sum));
		pg_cryptohash_free(hashCtx);
		bytesToHex(md5Sum, hexMd5Sum);

		myslot->tts_values[1] = Int64GetDatum(fileSize);
		myslot->tts_values[3] = CStringGetTextDatum((char *) hexMd5Sum);
		myslot->tts_isnull[3] = false;

		simple_table_tuple_update(resultRelInfo->ri_RelationDesc, &myslot->tts_tid, myslot,
								  estate->es_snapshot, &update_indexes);

		ExecClearTuple(myslot);

		/*
		 * We count only tuples not suppressed by a BEFORE INSERT trigger
		 * or FDW; this is the same definition used by nodeModifyTable.c
		 * for counting tuples inserted by an INSERT command.  Update
		 * progress of the COPY command as well.
		 *
		 * MPP: incrementing this counter here only matters for utility
		 * mode. in dispatch mode only the dispatcher COPY collects row
		 * count, so this counter is meaningless.
		 */
		pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
									 ++processed);
	}

	cstate->filename = NULL;

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	/*
	 * In QE, send the number of rejected rows to the client (QD COPY) if
	 * SREH is on, always send the number of completed rows.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
		SendNumRows((cstate->errMode != ALL_OR_NOTHING) ? cstate->cdbsreh->rejectcount : 0, processed);
	}

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeDistributionData(distData);
	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuple from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
static CopyFromState
BeginCopyFromDirectoryTable(ParseState *pstate,
							Relation rel,
							const char *filename,
							bool is_program,
							List *options)
{
	CopyFromState cstate;
	bool		pipe;
	MemoryContext oldcontext;
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE,
		PROGRESS_COPY_BYTES_TOTAL
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_FROM,
		0,
		0
	};

	if (!glob_copystmt->dirfilename)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Copy from directory table file name can't be null.")));

	/* Allocate workspace and zero all fields */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Process the target relation */
	cstate->rel = rel;

	/* Check whether copy directory table options allowed */
	ProcessCopyDirectoryTableOptions(pstate, &cstate->opts, true, options, rel->rd_id);

	if (Gp_role == GP_ROLE_DISPATCH && !cstate->opts.binary)
		ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Only support copy binary from directory table.")));

	cstate->copy_src = COPY_FILE;	/* default */

	/*
	 * Determine the mode
	 */
	if (Gp_role == GP_ROLE_DISPATCH &&
		cstate->rel && cstate->rel->rd_cdbpolicy &&
		cstate->rel->rd_cdbpolicy->ptype != POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DISPATCH;
	/*
	 * Handle case where fdw executes on coordinator while it's acting as a segment
	 * This occurs when fdw is under a redistribute on the coordinator
	 */
	else if (Gp_role == GP_ROLE_EXECUTE &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype == POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_EXECUTE)
		cstate->dispatch_mode = COPY_EXECUTOR;
	else
		cstate->dispatch_mode = COPY_DIRECT;

	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	if (filename)
		cstate->filename = pstrdup(filename);
	cstate->file_encoding = GetDatabaseEncoding();

	/*
	 * Allocate buffers for the input pipeline.
	 */
	cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	cstate->raw_buf[RAW_BUF_SIZE] = '\0';
	cstate->raw_reached_eof = false;

	initStringInfo(&cstate->line_buf);

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	/*
	 * build tupledesc and slot for copy from
	 */
	tupDesc = CreateTemplateTupleDesc(5);
	TupleDescInitEntry(tupDesc, (AttrNumber) 1, "relative_path",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupDesc, (AttrNumber) 2, "size",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupDesc, (AttrNumber) 3, "last_modified",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupDesc, (AttrNumber) 4, "md5",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupDesc, (AttrNumber) 5, "tag",
					   TEXTOID, -1 ,0);

	num_phys_attrs = tupDesc->natts;

	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, NIL);

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* Fetch the input function and typioparam info */
		getTypeInputInfo(att->atttypid,
						 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);
	}

	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	cstate->bytes_processed = 0;

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->is_program = is_program;

	pipe = (filename == NULL || cstate->dispatch_mode == COPY_EXECUTOR);

	if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		Assert(!is_program || cstate->dispatch_mode == COPY_EXECUTOR);	/* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate->filename, false);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[0], PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int		save_error = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 (save_error == ENOENT || save_error == EACCES) ?
						 errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			// Increase buffer size to improve performance  (cmcdevitt)
			/* GPDB_14_MERGE_FIXME: Ret value process. */
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
							    cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));

			progress_vals[2] = st.st_size;
		}
	}

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	if (cstate->dispatch_mode == COPY_EXECUTOR && cstate->copy_src != COPY_CALLBACK)
	{
		/* Read special header from QD */
		char		readSig[sizeof(QDtoQESignature)];
		copy_from_dispatch_header header_frame;

		if (CopyGetData(cstate, &readSig, sizeof(QDtoQESignature), sizeof(QDtoQESignature)) != sizeof(QDtoQESignature) ||
			memcmp(readSig, QDtoQESignature, sizeof(QDtoQESignature)) != 0)
			ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("QD->QE COPY communication signature not recognized")));

		if (CopyGetData(cstate, &header_frame, sizeof(header_frame), sizeof(header_frame)) != sizeof(header_frame))
			ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid QD->QD COPY communication header")));

		cstate->first_qe_processed_field = header_frame.first_qe_processed_field;
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Copy FROM file to relation.
 */
uint64
CopyFrom(CopyFromState cstate)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	ResultRelInfo *prevResultRelInfo = NULL;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	ModifyTableState *mtstate;
	ExprContext *econtext;
	TupleTableSlot *singleslot = NULL;
	MemoryContext oldcontext = CurrentMemoryContext;

	PartitionTupleRouting *proute = NULL;
	ErrorContextCallback errcallback;
	CommandId	mycid = GetCurrentCommandId(true);
	int			ti_options = 0; /* start with default options for insert */
	BulkInsertState bistate = NULL;
	CopyInsertMethod insertMethod;
	CopyMultiInsertInfo multiInsertInfo = {0};	/* pacify compiler */
	int64		processed = 0;
	int64		excluded = 0;
	bool		has_before_insert_row_trig;
	bool		has_instead_insert_row_trig;
	bool		leafpart_use_multi_insert = false;

	CdbCopy	   *cdbCopy = NULL;
	bool		is_check_distkey;
	GpDistributionData *distData = NULL; /* distribution data used to compute target seg */

	Assert(cstate->rel);
	Assert(list_length(cstate->range_table) == 1);

	/*
	 * The target must be a plain, foreign, or partitioned relation, or have
	 * an INSTEAD OF INSERT row trigger.  (Currently, such triggers are only
	 * allowed on views, so we only hint about them in the view case.)
	 */
	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE &&
		cstate->rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel)),
					 errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	if (cstate->rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
		return CopyFromDirectoryTable(cstate);

	/*
	 * If the target file is new-in-transaction, we assume that checking FSM
	 * for free space is a waste of time.  This could possibly be wrong, but
	 * it's unlikely.
	 */
	if (RELKIND_HAS_STORAGE(cstate->rel->rd_rel->relkind) &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_firstRelfilenodeSubid != InvalidSubTransactionId))
		ti_options |= TABLE_INSERT_SKIP_FSM;

	/*
	 * Optimize if new relfilenode was created in this subxact or one of its
	 * committed children and we won't see those rows later as part of an
	 * earlier scan or command. The subxact test ensures that if this subxact
	 * aborts then the frozen rows won't be visible after xact cleanup.  Note
	 * that the stronger test of exactly which subtransaction created it is
	 * crucial for correctness of this optimization. The test for an earlier
	 * scan or command tolerates false negatives. FREEZE causes other sessions
	 * to see rows they would not see under MVCC, and a false negative merely
	 * spreads that anomaly to the current session.
	 */
	if (cstate->opts.freeze)
	{
		/*
		 * We currently disallow COPY FREEZE on partitioned tables.  The
		 * reason for this is that we've simply not yet opened the partitions
		 * to determine if the optimization can be applied to them.  We could
		 * go and open them all here, but doing so may be quite a costly
		 * overhead for small copies.  In any case, we may just end up routing
		 * tuples to a small number of partitions.  It seems better just to
		 * raise an ERROR for partitioned tables.
		 */
		if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot perform COPY FREEZE on a partitioned table")));
		}

		/*
		 * Tolerate one registration for the benefit of FirstXactSnapshot.
		 * Scan-bearing queries generally create at least two registrations,
		 * though relying on that is fragile, as is ignoring ActiveSnapshot.
		 * Clear CatalogSnapshot to avoid counting its registration.  We'll
		 * still detect ongoing catalog scans, each of which separately
		 * registers the snapshot it uses.
		 */
		InvalidateCatalogSnapshot();
		if (!ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot perform COPY FREEZE because of prior transaction activity")));

		if (cstate->rel->rd_createSubid != GetCurrentSubTransactionId() &&
			cstate->rel->rd_newRelfilenodeSubid != GetCurrentSubTransactionId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")));

		ti_options |= TABLE_INSERT_FROZEN;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = target_resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);

	/* Verify the named relation is a valid target for INSERT */
	if (!(cstate->rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE))
		CheckValidResultRel(resultRelInfo, CMD_INSERT, NULL);

	ExecOpenIndices(resultRelInfo, false);

	/*
	 * Set up a ModifyTableState so we can let FDW(s) init themselves for
	 * foreign-table result relation(s).
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->mt_nrels = 1;
	mtstate->resultRelInfo = resultRelInfo;
	mtstate->rootResultRelInfo = resultRelInfo;

	if (resultRelInfo->ri_FdwRoutine != NULL &&
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert != NULL)
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert(mtstate,
														 resultRelInfo);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 *
	 * Because partition tuple routing would like to know about whether
	 * transition capture is active, we also set it in mtstate, which is
	 * passed to ExecFindPartition() below.
	 */
	cstate->transition_capture = mtstate->mt_transition_capture =
		MakeTransitionCaptureState(cstate->rel->trigdesc,
								   RelationGetRelid(cstate->rel),
								   CMD_INSERT);

	/*
	 * If the named relation is a partitioned table, initialize state for
	 * CopyFrom tuple routing.
	 */
	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		proute = ExecSetupPartitionTupleRouting(estate, cstate->rel);

	if (cstate->whereClause)
		cstate->qualexpr = ExecInitQual(castNode(List, cstate->whereClause),
										&mtstate->ps);

	/*
	 * It's generally more efficient to prepare a bunch of tuples for
	 * insertion, and insert them in one table_multi_insert() call, than call
	 * table_tuple_insert() separately for every tuple. However, there are a
	 * number of reasons why we might not be able to do this.  These are
	 * explained below.
	 */
	if (resultRelInfo->ri_TrigDesc != NULL &&
		(resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_insert_instead_row))
	{
		/*
		 * Can't support multi-inserts when there are any BEFORE/INSTEAD OF
		 * triggers on the table. Such triggers might query the table we're
		 * inserting into and act differently if the tuples that have already
		 * been processed and prepared for insertion are not there.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (proute != NULL && resultRelInfo->ri_TrigDesc != NULL &&
			 resultRelInfo->ri_TrigDesc->trig_insert_new_table)
	{
		/*
		 * For partitioned tables we can't support multi-inserts when there
		 * are any statement level insert triggers. It might be possible to
		 * allow partitioned tables with such triggers in the future, but for
		 * now, CopyMultiInsertInfoFlush expects that any before row insert
		 * and statement level insert triggers are on the same relation.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (resultRelInfo->ri_FdwRoutine != NULL ||
			 cstate->volatile_defexprs)
	{
		/*
		 * Can't support multi-inserts to foreign tables or if there are any
		 * volatile default expressions in the table.  Similarly to the
		 * trigger case above, such expressions may query the table we're
		 * inserting into.
		 *
		 * Note: It does not matter if any partitions have any volatile
		 * default expressions as we use the defaults from the target of the
		 * COPY command.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (contain_volatile_functions(cstate->whereClause))
	{
		/*
		 * Can't support multi-inserts if there are any volatile function
		 * expressions in WHERE clause.  Similarly to the trigger case above,
		 * such expressions may query the table we're inserting into.
		 */
		insertMethod = CIM_SINGLE;
	}
	else
	{
		/*
		 * For partitioned tables, we may still be able to perform bulk
		 * inserts.  However, the possibility of this depends on which types
		 * of triggers exist on the partition.  We must disable bulk inserts
		 * if the partition is a foreign table or it has any before row insert
		 * or insert instead triggers (same as we checked above for the parent
		 * table).  Since the partition's resultRelInfos are initialized only
		 * when we actually need to insert the first tuple into them, we must
		 * have the intermediate insert method of CIM_MULTI_CONDITIONAL to
		 * flag that we must later determine if we can use bulk-inserts for
		 * the partition being inserted into.
		 */
		if (proute)
			insertMethod = CIM_MULTI_CONDITIONAL;
		else
			insertMethod = CIM_MULTI;

		CopyMultiInsertInfoInit(&multiInsertInfo, resultRelInfo, cstate,
								estate, mycid, ti_options);
	}

	/*
	 * If not using batch mode (which allocates slots as needed) set up a
	 * tuple slot too. When inserting into a partitioned table, we also need
	 * one, even if we might batch insert, to read the tuple in the root
	 * partition's form.
	 */
	if (insertMethod == CIM_SINGLE || insertMethod == CIM_MULTI_CONDITIONAL)
	{
		singleslot = table_slot_create(resultRelInfo->ri_RelationDesc,
									   &estate->es_tupleTable);
		bistate = GetBulkInsertState();
	}

	has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

	has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * Do we need to check the distribution keys? Normally, the QD computes the
	 * target segment and sends the data to the correct segment. We don't need to
	 * verify that in the QE anymore. But in ON SEGMENT, we're reading data
	 * directly from a file, and there's no guarantee on what it contains, so we
	 * need to do the checking in the QE.
	 */
	is_check_distkey = (cstate->opts.on_segment && Gp_role == GP_ROLE_EXECUTE && gp_enable_segment_copy_checking) ? true : false;

	/*
	 * Initialize information about distribution keys, needed to compute target
	 * segment for each row.
	 */
	if (cstate->dispatch_mode == COPY_DISPATCH || is_check_distkey)
	{
		distData = InitDistributionData(cstate, estate);

		/*
		 * If this table is distributed randomly, there is nothing to check,
		 * after all.
		 */
		if (distData->policy == NULL || distData->policy->nattrs == 0)
			is_check_distkey = false;
	}

	/* Determine which fields we need to parse in the QD. */
	if (cstate->dispatch_mode == COPY_DISPATCH)
		InitCopyFromDispatchSplit(cstate, distData, estate);

	if (cstate->dispatch_mode == COPY_DISPATCH ||
		cstate->dispatch_mode == COPY_EXECUTOR)
	{
		/*
		 * Now split the attnumlist into the parts that are parsed in the QD, and
		 * in QE.
		 */
		ListCell   *lc;
		int			i = 0;
		List	   *qd_attnumlist = NIL;
		List	   *qe_attnumlist = NIL;
		int			first_qe_processed_field;

		first_qe_processed_field = cstate->first_qe_processed_field;

		foreach(lc, cstate->attnumlist)
		{
			int			attnum = lfirst_int(lc);

			if (i < first_qe_processed_field)
				qd_attnumlist = lappend_int(qd_attnumlist, attnum);
			else
				qe_attnumlist = lappend_int(qe_attnumlist, attnum);
			i++;
		}
		cstate->qd_attnumlist = qd_attnumlist;
		cstate->qe_attnumlist = qe_attnumlist;
	}

	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		/*
		 * We are the QD node, and we are receiving rows from client, or
		 * reading them from a file. We are not writing any data locally,
		 * instead, we determine the correct target segment for row,
		 * and forward each to the correct segment.
		 */

		/*
		 * pre-allocate buffer for constructing a message.
		 */
		cstate->dispatch_msgbuf = makeStringInfo();
		enlargeStringInfo(cstate->dispatch_msgbuf, SizeOfCopyFromDispatchRow);

		/*
		 * prepare to COPY data into segDBs:
		 * - set table partitioning information
		 * - set append only table relevant info for dispatch.
		 * - get the distribution policy for this table.
		 * - build a COPY command to dispatch to segdbs.
		 * - dispatch the modified COPY command to all segment databases.
		 * - prepare cdbhash for hashing on row values.
		 */
		cdbCopy = makeCdbCopyFrom(cstate);

		/*
		 * Dispatch the COPY command.
		 *
		 * From this point in the code we need to be extra careful about error
		 * handling. ereport() must not be called until the COPY command sessions
		 * are closed on the executors. Calling ereport() will leave the executors
		 * hanging in COPY state.
		 *
		 * For errors detected by the dispatcher, we save the error message in
		 * cdbcopy_err StringInfo, move on to closing all COPY sessions on the
		 * executors and only then raise an error. We need to make sure to TRY/CATCH
		 * all other errors that may be raised from elsewhere in the backend. All
		 * error during COPY on the executors will be detected only when we end the
		 * COPY session there, so we are fine there.
		 */
		elog(DEBUG5, "COPY command sent to segdbs");

		cdbCopyStart(cdbCopy, glob_copystmt, cstate->file_encoding);

		/*
		 * Skip header processing if dummy file get from master for COPY FROM ON
		 * SEGMENT
		 */
		if (!cstate->opts.on_segment)
		{
			SendCopyFromForwardedHeader(cstate, cdbCopy);
		}
	}

	CopyInitDataParser(cstate);

	table_dml_init(resultRelInfo->ri_RelationDesc, CMD_INSERT);

	for (;;)
	{
		TupleTableSlot *myslot;
		bool		skip_tuple;
		unsigned int target_seg = 0;	/* result segment of cdbhash */

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset the per-tuple exprcontext. We do this after every tuple, to
		 * clean-up after expression evaluations etc.
		 */
		ResetPerTupleExprContext(estate);

		/* select slot to (initially) load row into */
		if (insertMethod == CIM_SINGLE || proute)
		{
			myslot = singleslot;
			Assert(myslot != NULL);
		}
		else
		{
			Assert(resultRelInfo == target_resultRelInfo);
			Assert(insertMethod == CIM_MULTI);

			myslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
													 resultRelInfo);
		}

		/*
		 * Switch to per-tuple context before calling NextCopyFrom, which does
		 * evaluate default expressions etc. and requires per-tuple context.
		 */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

		if (cstate->dispatch_mode == COPY_EXECUTOR)
		{
			if (!NextCopyFromExecute(cstate, econtext, myslot->tts_values, myslot->tts_isnull, false))
				break;

			/*
			 * NextCopyFromExecute set up estate->es_result_relation_info,
			 * and stored the tuple in the correct slot.
			 */
			resultRelInfo = estate->es_result_relations[0];
		}
		else
		{
			/* Directly store the values/nulls array in the slot */
			if (cstate->dispatch_mode == COPY_DISPATCH)
			{
				if (!NextCopyFromDispatch(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
					break;
			}
			else
			{
				if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
					break;
			}
		}

		ExecStoreVirtualTuple(myslot);

		/*
		 * Constraints and where clause might reference the tableoid column,
		 * so (re-)initialize tts_tableOid before evaluating them.
		 */
		myslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		if (cstate->whereClause)
		{
			econtext->ecxt_scantuple = myslot;
			/* Skip items that don't match COPY's WHERE clause */
			if (!ExecQual(cstate->qualexpr, econtext))
			{
				/*
				 * Report that this tuple was filtered out by the WHERE
				 * clause.
				 */
				pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED,
											 ++excluded);
				continue;
			}
		}

		if (cstate->dispatch_mode != COPY_DISPATCH && is_check_distkey)
		{
			/*
			 * In COPY FROM ON SEGMENT, check the distribution key in the
			 * QE.
			 * Note: For partitioned tables, the order of the root table's columns can be
			 * inconsistent with the order of the partition's columns and Greenplum/PostgreSQL
			 * allows such behavior. When they have different orders, we need to re-order the
			 * TupleTableSlot (myslot) to make it match the partition's columns (see execute_attr_map_slot()
			 * for details). We must perform this check before the re-ordering of TupleTableslot,
			 * or the value of target_seg will be incorrect.
			 */
			if (distData->policy->nattrs != 0)
			{
				target_seg = GetTargetSeg(distData, myslot);
				if (GpIdentity.segindex != target_seg)
				{
					PG_TRY();
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
								 errmsg("value of distribution key doesn't belong to segment with ID %d, it belongs to segment with ID %d",
										GpIdentity.segindex, target_seg)));
					}
					PG_CATCH();
					{
						HandleCopyError(cstate);
					}
					PG_END_TRY();
				}
			}
		}
		/* Determine the partition to insert the tuple into */
		if (proute && cstate->dispatch_mode != COPY_DISPATCH)
		{
			TupleConversionMap *map;
			bool		got_error = false;

			/*
			 * Attempt to find a partition suitable for this tuple.
			 * ExecFindPartition() will raise an error if none can be found or
			 * if the found partition is not suitable for INSERTs.
			 */
			PG_TRY();
			{
				resultRelInfo = ExecFindPartition(mtstate, target_resultRelInfo,
												  proute, myslot, estate);
			}
			PG_CATCH();
			{
				/* after all the prep work let cdbsreh do the real work */
				HandleCopyError(cstate);
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (got_error)
				continue;

			if (prevResultRelInfo != resultRelInfo)
			{
				/* Determine which triggers exist on this partition */
				has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

				has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

				/*
				 * Disable multi-inserts when the partition has BEFORE/INSTEAD
				 * OF triggers, or if the partition is a foreign partition.
				 */
				leafpart_use_multi_insert = insertMethod == CIM_MULTI_CONDITIONAL &&
					!has_before_insert_row_trig &&
					!has_instead_insert_row_trig &&
					resultRelInfo->ri_FdwRoutine == NULL;

				/* Set the multi-insert buffer to use for this partition. */
				if (leafpart_use_multi_insert)
				{
					if (resultRelInfo->ri_CopyMultiInsertBuffer == NULL)
						CopyMultiInsertInfoSetupBuffer(&multiInsertInfo,
													   resultRelInfo);
				}
				else if (insertMethod == CIM_MULTI_CONDITIONAL &&
						 !CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
				{
					/*
					 * Flush pending inserts if this partition can't use
					 * batching, so rows are visible to triggers etc.
					 */
					CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}

				if (bistate != NULL)
					ReleaseBulkInsertStatePin(bistate);
				prevResultRelInfo = resultRelInfo;
			}

			/*
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to root rowtype. But if there are no
			 * BEFORE triggers on the partition that could change the tuple,
			 * we can just remember the original unconverted tuple to avoid a
			 * needless round trip conversion.
			 */
			if (cstate->transition_capture != NULL)
				cstate->transition_capture->tcs_original_insert_tuple =
					!has_before_insert_row_trig ? myslot : NULL;

			/*
			 * We might need to convert from the root rowtype to the partition
			 * rowtype.
			 */
			map = resultRelInfo->ri_RootToPartitionMap;
			if (insertMethod == CIM_SINGLE || !leafpart_use_multi_insert)
			{
				/* non batch insert */
				if (map != NULL)
				{
					TupleTableSlot *new_slot;

					new_slot = resultRelInfo->ri_PartitionTupleSlot;
					myslot = execute_attr_map_slot(map->attrMap, myslot, new_slot);
				}
			}
			else
			{
				/*
				 * Prepare to queue up tuple for later batch insert into
				 * current partition.
				 */
				TupleTableSlot *batchslot;

				/* no other path available for partitioned table */
				Assert(insertMethod == CIM_MULTI_CONDITIONAL);

				batchslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
															resultRelInfo);

				if (map != NULL)
					myslot = execute_attr_map_slot(map->attrMap, myslot,
												   batchslot);
				else
				{
					/*
					 * This looks more expensive than it is (Believe me, I
					 * optimized it away. Twice.). The input is in virtual
					 * form, and we'll materialize the slot below - for most
					 * slot types the copy performs the work materialization
					 * would later require anyway.
					 */
					ExecCopySlot(batchslot, myslot);
					myslot = batchslot;
				}
			}

			/* ensure that triggers etc see the right relation  */
			myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		}

		skip_tuple = false;

		/*
		 * Compute which segment this row belongs to.
		 */
		if (cstate->dispatch_mode == COPY_DISPATCH)
		{
			/* In QD, compute the target segment to send this row to. */
			target_seg = GetTargetSeg(distData, myslot);
			bool send_to_all = distData &&
							   GpPolicyIsReplicated(distData->policy);

			/* in the QD, forward the row to the correct segment(s). */
			SendCopyFromForwardedTuple(cstate, cdbCopy, send_to_all,
									   send_to_all ? 0 : target_seg,
									   resultRelInfo->ri_RelationDesc,
									   cstate->cur_lineno,
									   cstate->line_buf.data,
									   cstate->line_buf.len,
									   myslot->tts_values,
									   myslot->tts_isnull,
									   false);
			skip_tuple = true;
			processed++;
		}

		/* BEFORE ROW INSERT Triggers */
		if (has_before_insert_row_trig)
		{
			if (!skip_tuple && !ExecBRInsertTriggers(estate, resultRelInfo, myslot))
				skip_tuple = true;	/* "do nothing" */
		}

		if (!skip_tuple)
		{
			/*
			 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
			 * tuple.  Otherwise, proceed with inserting the tuple into the
			 * table or foreign table.
			 */
			if (has_instead_insert_row_trig)
			{
				ExecIRInsertTriggers(estate, resultRelInfo, myslot);
			}
			else
			{
				/* Compute stored generated columns */
				if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
					resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
					ExecComputeStoredGenerated(resultRelInfo, estate, myslot,
											   CMD_INSERT);

				/*
				 * If the target is a plain table, check the constraints of
				 * the tuple.
				 */
				if (resultRelInfo->ri_FdwRoutine == NULL &&
					resultRelInfo->ri_RelationDesc->rd_att->constr)
					ExecConstraints(resultRelInfo, myslot, estate);

				/*
				 * Also check the tuple against the partition constraint, if
				 * there is one; except that if we got here via tuple-routing,
				 * we don't need to if there's no BR trigger defined on the
				 * partition.
				 */
				if (resultRelInfo->ri_RelationDesc->rd_rel->relispartition &&
					(proute == NULL || has_before_insert_row_trig))
					ExecPartitionCheck(resultRelInfo, myslot, estate, true);

				/* Store the slot in the multi-insert buffer, when enabled. */
				if (insertMethod == CIM_MULTI || leafpart_use_multi_insert)
				{
					/*
					 * The slot previously might point into the per-tuple
					 * context. For batching it needs to be longer lived.
					 */
					ExecMaterializeSlot(myslot);

					/* Add this tuple to the tuple buffer */
					CopyMultiInsertInfoStore(&multiInsertInfo,
											 resultRelInfo, myslot,
											 cstate->line_buf.len,
											 cstate->cur_lineno);

					/*
					 * If enough inserts have queued up, then flush all
					 * buffers out to their tables.
					 */
					if (CopyMultiInsertInfoIsFull(&multiInsertInfo))
						CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}
				else
				{
					List	   *recheckIndexes = NIL;

					/* OK, store the tuple */
					if (resultRelInfo->ri_FdwRoutine != NULL)
					{
						myslot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
																				 resultRelInfo,
																				 myslot,
																				 NULL);

						if (myslot == NULL) /* "do nothing" */
							continue;	/* next tuple please */

						/*
						 * AFTER ROW Triggers might reference the tableoid
						 * column, so (re-)initialize tts_tableOid before
						 * evaluating them.
						 */
						myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
					}
					else
					{
						/* OK, store the tuple and create index entries for it */
						table_tuple_insert(resultRelInfo->ri_RelationDesc,
										   myslot, mycid, ti_options, bistate);

						if (resultRelInfo->ri_NumIndices > 0)
							recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
																   myslot,
																   estate,
																   false,
																   false,
																   NULL,
																   NIL);
					}

					/* AFTER ROW INSERT Triggers */
					ExecARInsertTriggers(estate, resultRelInfo, myslot,
										 recheckIndexes, cstate->transition_capture);

					list_free(recheckIndexes);
				}
			}

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger
			 * or FDW; this is the same definition used by nodeModifyTable.c
			 * for counting tuples inserted by an INSERT command.  Update
			 * progress of the COPY command as well.
			 *
			 * MPP: incrementing this counter here only matters for utility
			 * mode. in dispatch mode only the dispatcher COPY collects row
			 * count, so this counter is meaningless.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
			if (cstate->cdbsreh)
				cstate->cdbsreh->processed++;
		}
	}
	/*
	 * After processed data from QD, which is empty and just for workflow, now
	 * to process the data on segment, only one shot if cstate->opts.on_segment &&
	 * Gp_role == GP_ROLE_DISPATCH
	 */
	// wrong commens here
	if (cstate->opts.on_segment && Gp_role == GP_ROLE_EXECUTE)
	{
		CopyInitDataParser(cstate);
	}
	elog(DEBUG1, "Segment %u, Copied %lu rows.", GpIdentity.segindex, processed);

	/* Flush any remaining buffered tuples */
	if (insertMethod != CIM_SINGLE)
	{
		if (!CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
			CopyMultiInsertInfoFlush(&multiInsertInfo, NULL);
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	if (bistate != NULL)
		FreeBulkInsertState(bistate);

	table_dml_fini(resultRelInfo->ri_RelationDesc, CMD_INSERT);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Done reading input data and sending it off to the segment
	 * databases Now we would like to end the copy command on
	 * all segment databases across the cluster.
	 */
	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		int64		total_completed_from_qes;
		int64		total_rejected_from_qes;

		cdbCopyEnd(cdbCopy,
				   &total_completed_from_qes,
				   &total_rejected_from_qes);

		/*
		 * Reset returned processed to total_completed_from_qes.
		 *
		 * processed above excludes only rejected rows on QD, it
		 * should also exclude rejected rows on QEs.
		 *
		 * NOTE:
		 *  total_completed_from_qes + total_rejected_from_qes <= # of
		 *  input file rows
		 *
		 * total_rejected_from_qes includes only rows rejected by
		 * SREH; however, total_completed_from_qes excludes both
		 * SREH-rejected rows and TRIGGER-rejected rows.
		 */
		processed = total_completed_from_qes;

		if (cstate->cdbsreh)
		{
			/* emit a NOTICE with number of rejected rows */
			uint64		total_rejected = 0;
			uint64		total_rejected_from_qd = cstate->cdbsreh->rejectcount;

			/*
			 * If error log has been requested, then we send the row to the segment
			 * so that it can be written in the error log file. The segment process
			 * counts it again as a rejected row. So we ignore the reject count
			 * from the master and only consider the reject count from segments.
			 */
			if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
				total_rejected_from_qd = 0;

			total_rejected = total_rejected_from_qd + total_rejected_from_qes;

			ReportSrehResults(cstate->cdbsreh, total_rejected);
		}
	}
	/* In dispatcher, we have the report for rejected count. We should have it in singlenode too. */
	else if (IS_SINGLENODE() && cstate->cdbsreh)
	{
		ReportSrehResults(cstate->cdbsreh, cstate->cdbsreh->rejectcount);
	}

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, target_resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	/*
	 * In QE, send the number of rejected rows to the client (QD COPY) if
	 * SREH is on, always send the number of completed rows.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
		SendNumRows((cstate->errMode != ALL_OR_NOTHING) ? cstate->cdbsreh->rejectcount : 0, processed);
	}

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Allow the FDW to shut down */
	if (target_resultRelInfo->ri_FdwRoutine != NULL &&
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert != NULL)
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert(estate,
															  target_resultRelInfo);

	/* Tear down the multi-insert buffer data */
	if (insertMethod != CIM_SINGLE)
		CopyMultiInsertInfoCleanup(&multiInsertInfo);

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(mtstate, proute);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeDistributionData(distData);

	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'whereClause': WHERE clause from the COPY FROM command
 * 'filename': Name of server-local file to read, NULL for STDIN
 * 'is_program': true if 'filename' is program to execute
 * 'data_source_cb': callback that provides the input data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
CopyFromState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  Node *whereClause,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra,
			  List *attnamelist,
			  List *options)
{
	CopyFromState cstate;
	bool		pipe;
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
	MemoryContext oldcontext;
	bool		volatile_defexprs;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE,
		PROGRESS_COPY_BYTES_TOTAL
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_FROM,
		0,
		0
	};

	if (rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
		return BeginCopyFromDirectoryTable(pstate, rel, filename, is_program, options);

	/* Allocate workspace and zero all fields */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Process the target relation */
	cstate->rel = rel;
	cstate->escape_off = false;

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, &cstate->opts, true /* is_from */ , options, rel->rd_id);

	if (cstate->opts.escape != NULL && pg_strcasecmp(cstate->opts.escape, "off") == 0)
	{
		cstate->escape_off = true;
	}

	if (cstate->opts.delim_off && !rel_is_external_table(rel->rd_id))
	{
		/*
		 * We don't support delimiter 'off' for COPY because the QD COPY
		 * sometimes internally adds columns to the data that it sends to
		 * the QE COPY modules, and it uses the delimiter for it. There
		 * are ways to work around this but for now it's not important and
		 * we simply don't support it.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("using no delimiter is only supported for external tables")));
	}

	tupDesc = RelationGetDescr(cstate->rel);

	/* process commmon options or initialization */

	/* Generate or convert list of attributes to process */
	cstate->attnamelist = attnamelist;
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->opts.force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->opts.force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->opts.convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.convert_select);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->opts.file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();
	else
		cstate->file_encoding = cstate->opts.file_encoding;

	/*
	 * Look up encoding conversion function.
	 */
	if (cstate->file_encoding == GetDatabaseEncoding() ||
		cstate->file_encoding == PG_SQL_ASCII ||
		GetDatabaseEncoding() == PG_SQL_ASCII)
	{
		cstate->need_transcoding = false;
	}
	else
	{
		cstate->need_transcoding = true;
		cstate->conversion_proc = FindDefaultConversionProc(cstate->file_encoding, GetDatabaseEncoding());
	}

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 *
	 * In COPY_EXECUTE mode, the dispatcher has already done the conversion.
	 */
	if (cstate->dispatch_mode == COPY_DISPATCH) 
		cstate->need_transcoding = false;
	
	cstate->copy_src = COPY_FILE;	/* default */

	cstate->whereClause = whereClause;

	/*
	 * Determine the mode
	 */
	if (cstate->opts.on_segment || data_source_cb)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_DISPATCH &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype != POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DISPATCH;
	/*
	 * Handle case where fdw executes on coordinator while it's acting as a segment
	 * This occurs when fdw is under a redistribute on the coordinator
	 */
	else if (Gp_role == GP_ROLE_EXECUTE &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype == POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_EXECUTE)
		cstate->dispatch_mode = COPY_EXECUTOR;
	else
		cstate->dispatch_mode = COPY_DIRECT;

	MemoryContextSwitchTo(oldcontext);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	// cstate->eol_type = EOL_UNKNOWN; /* GPDB: don't overwrite value set in ProcessCopyOptions */
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/*
	 * Allocate buffers for the input pipeline.
	 *
	 * attribute_buf and raw_buf are used in both text and binary modes, but
	 * input_buf and line_buf only in text mode.
	 */
	cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	cstate->raw_buf[RAW_BUF_SIZE] = '\0';
	cstate->raw_reached_eof = false;

	if (!cstate->opts.binary)
	{
		/*
		 * If encoding conversion is needed, we need another buffer to hold
		 * the converted input data.  Otherwise, we can just point input_buf
		 * to the same buffer as raw_buf.
		 */
		if (cstate->need_transcoding)
		{
			cstate->input_buf = (char *) palloc(INPUT_BUF_SIZE + 1);
			cstate->input_buf_index = cstate->input_buf_len = 0;
		}
		else
			cstate->input_buf = cstate->raw_buf;
		cstate->input_reached_eof = false;

		initStringInfo(&cstate->line_buf);
	}

	initStringInfo(&cstate->attribute_buf);
	initStringInfo(&cstate->line_buf);

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	num_defaults = 0;
	volatile_defexprs = false;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		if (cstate->opts.binary)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum) && !att->attgenerated)
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr	   *defexpr = (Expr *) build_column_default(cstate->rel,
																attnum);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);

				/* Initialize executable expression in copycontext */
				defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;

				/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
				if (!volatile_defexprs)
					volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);
			}
		}
	}


	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	cstate->bytes_processed = 0;

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
	cstate->is_program = is_program;

	pipe = (filename == NULL || cstate->dispatch_mode == COPY_EXECUTOR);

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* open nothing */

		if (filename == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("STDIN is not supported by 'COPY ON SEGMENT'")));
	}
	else if (data_source_cb)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK;
		cstate->copy_src = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
		cstate->data_source_cb_extra = data_source_cb_extra;
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		Assert(!is_program || cstate->dispatch_mode == COPY_EXECUTOR);	/* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->opts.on_segment)
			MangleCopyFileName(&cstate->filename, cstate->cdbsreh);

		if (cstate->is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate->filename, false);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[0], PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			// Increase buffer size to improve performance  (cmcdevitt)
			/* GPDB_14_MERGE_FIXME: Ret value process. */
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));

			progress_vals[2] = st.st_size;
		}
	}

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* nothing to do */
	}
	else if (cstate->dispatch_mode == COPY_EXECUTOR && cstate->copy_src != COPY_CALLBACK)
	{
		/* Read special header from QD */
		char		readSig[sizeof(QDtoQESignature)];
		copy_from_dispatch_header header_frame;

		if (CopyGetData(cstate, &readSig, sizeof(QDtoQESignature), sizeof(QDtoQESignature)) != sizeof(QDtoQESignature) ||
			memcmp(readSig, QDtoQESignature, sizeof(QDtoQESignature)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("QD->QE COPY communication signature not recognized")));

		if (CopyGetData(cstate, &header_frame, sizeof(header_frame), sizeof(header_frame)) != sizeof(header_frame))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					errmsg("invalid QD->QD COPY communication header")));

		cstate->first_qe_processed_field = header_frame.first_qe_processed_field;
	}
	else if (cstate->opts.binary)
	{
		if (cstate->rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE)
		{
			/* Read and verify binary header */
			ReceiveCopyBinaryHeader(cstate);
		}
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->opts.binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

	cstate->find_eol_with_rawreading = false;
	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool
NextCopyFromRawFields(CopyFromState cstate, char ***fields, int *nfields)
{
	return NextCopyFromRawFieldsX(cstate, fields, nfields, -1);
}

static bool
NextCopyFromRawFieldsX(CopyFromState cstate, char ***fields, int *nfields,
					   int stop_processing_at_field)
{
	int			fldct;
	bool		done;

	/* only available for text or csv input */
	Assert(!cstate->opts.binary);

	/* on input just throw the header line away */
	if (cstate->cur_lineno == 0 && cstate->opts.header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
			return false;		/* done */
	}

	cstate->cur_lineno++;

	/* Actually read the line into memory here */
	done = CopyReadLine(cstate);

	/*
	 * EOF at start of line means we're done.  If we see EOF after some
	 * characters, we act as though it was newline followed by EOF, ie,
	 * process the line and then exit loop on next iteration.
	 */
	if (done && cstate->line_buf.len == 0)
		return false;

	/* Parse the line into de-escaped field values */
	if (cstate->opts.csv_mode)
		fldct = CopyReadAttributesCSV(cstate, stop_processing_at_field);
	else
		fldct = CopyReadAttributesText(cstate, stop_processing_at_field);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}

bool
NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
			   Datum *values, bool *nulls)
{
	if (!cstate->cdbsreh)
		return NextCopyFromX(cstate, econtext, values, nulls);
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		for (;;)
		{
			bool		got_error = false;
			bool		result = false;

			PG_TRY();
			{
				result = NextCopyFromX(cstate, econtext, values, nulls);
			}
			PG_CATCH();
			{
				HandleCopyError(cstate); /* cdbsreh->processed is updated inside here */
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (result)
				cstate->cdbsreh->processed++;

			if (!got_error)
				return result;
		}
	}
}

/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 *
 * changing me? take a look at FILEAM_HANDLE_ERROR in fileam.c as well.
 */
void
HandleCopyError(CopyFromState cstate)
{
	if (cstate->errMode == ALL_OR_NOTHING)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	/* SREH must only handle data errors. all other errors must not be caught */
	if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	else
	{
		/* SREH - release error state and handle error */
		MemoryContext oldcontext;
		ErrorData	*edata;
		char	   *errormsg;
		CdbSreh	   *cdbsreh = cstate->cdbsreh;

		cdbsreh->processed++;

		oldcontext = MemoryContextSwitchTo(cstate->cdbsreh->badrowcontext);

		/* save a copy of the error info */
		edata = CopyErrorData();

		FlushErrorState();

		/*
		 * set the error message. Use original msg and add column name if available.
		 * We do this even if we're not logging the errors, because
		 * ErrorIfRejectLimit() below will use this information in the error message,
		 * if the error count is reached.
		 */
		cdbsreh->rawdata->cursor = 0;
		cdbsreh->rawdata->data = cstate->line_buf.data;
		cdbsreh->rawdata->len = cstate->line_buf.len;

		cdbsreh->is_server_enc = true;
		cdbsreh->linenumber = cstate->cur_lineno;
		if (cstate->cur_attname)
		{
			errormsg =  psprintf("%s, column %s",
								 edata->message, cstate->cur_attname);
		}
		else
		{
			errormsg = edata->message;
		}
		cstate->cdbsreh->errmsg = errormsg;

		if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
		{
			if (Gp_role == GP_ROLE_DISPATCH && !cstate->opts.on_segment)
			{
				cstate->cdbsreh->rejectcount++;

				SendCopyFromForwardedError(cstate, cstate->cdbCopy, errormsg);
			}
			else
			{
				/* after all the prep work let cdbsreh do the real work */
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					cstate->cdbsreh->rejectcount++;
				}
				else
				{
					HandleSingleRowError(cstate->cdbsreh);
				}
			}
		}
		else
			cstate->cdbsreh->rejectcount++;

		ErrorIfRejectLimitReached(cstate->cdbsreh);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(cstate->cdbsreh->badrowcontext);
	}
}


/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 */
bool
NextCopyFromX(CopyFromState cstate, ExprContext *econtext,
			  Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	List	   *attnumlist;
	int			stop_processing_at_field;

	/*
	 * Figure out what fields we're going to process in this process.
	 *
	 * In the QD, set 'stop_processing_at_field' so that we only those
	 * fields that are needed in the QD.
	 */
	switch (cstate->dispatch_mode)
	{
		case COPY_DIRECT:
			stop_processing_at_field = -1;
			attnumlist = cstate->attnumlist;
			break;

		case COPY_DISPATCH:
			stop_processing_at_field = cstate->first_qe_processed_field;
			attnumlist = cstate->qd_attnumlist;
			break;

		case COPY_EXECUTOR:
			stop_processing_at_field = -1;
			attnumlist = cstate->qe_attnumlist;
			break;

		default:
			elog(ERROR, "unexpected COPY dispatch mode %d", cstate->dispatch_mode);
	}

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(attnumlist);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->opts.binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

		/* read raw fields in the next line */
		if (cstate->dispatch_mode != COPY_EXECUTOR)
		{
			if (!NextCopyFromRawFieldsX(cstate, &field_strings, &fldct,
										stop_processing_at_field))
				return false;
		}
		else
		{
			/*
			 * We have received the raw line from the QD, and we just
			 * need to split it into raw fields.
			 */
			if (cstate->stopped_processing_at_delim &&
				cstate->line_buf.cursor <= cstate->line_buf.len)
			{
				if (cstate->opts.csv_mode)
					fldct = CopyReadAttributesCSV(cstate, -1);
				else
					fldct = CopyReadAttributesText(cstate, -1);
			}
			else
				fldct = 0;
			field_strings = cstate->raw_fields;
		}
		/* 
		 * Check for overflowing fields.
		 * GPDB: Change below condition compared to upstream to 
		 * greater than or equal to 0 as in QE, 
		 * attr_count may be equal to 0, 
		 * when all fields are processed in the QD.
		 */
		if (fldct > attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		/*
		 * A completely empty line is not allowed with FILL MISSING FIELDS. Without
		 * FILL MISSING FIELDS, it's almost surely an error, but not always:
		 * a table with a single text column, for example, needs to accept empty
		 * lines.
		 */
		if (cstate->line_buf.len == 0 &&
			cstate->opts.fill_missing &&
			list_length(cstate->attnumlist) > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for column \"%s\", found empty data line",
							NameStr(TupleDescAttr(tupDesc, 1)->attname))));
		}

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			if (fieldno >= fldct)
			{
				/*
				 * Some attributes are missing. In FILL MISSING FIELDS mode,
				 * treat them as NULLs.
				 */
				if (!cstate->opts.fill_missing)
					ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(att->attname))));
				fieldno++;
				string = NULL;
			}
			else
				string = field_strings[fieldno++];

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->opts.csv_mode)
			{
				if (string == NULL &&
					cstate->opts.force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->opts.null_print;
				}
				else if (string != NULL && cstate->opts.force_null_flags[m]
						 && strcmp(string, cstate->opts.null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  att->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == attr_count);
	}
	else if (attr_count)
	{
		/* binary */
		int16		fld_count;
		ListCell   *cur;

		cstate->cur_lineno++;

		if (!CopyGetInt16(cstate, &fld_count))
		{
			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (fld_count == -1)
		{
			/*
			 * Received EOF marker.  In a V3-protocol copy, wait for the
			 * protocol-level EOF, and complain if it doesn't come
			 * immediately.  This ensures that we correctly handle CopyFail,
			 * if client chooses to send that now.
			 *
			 * Note that we MUST NOT try to read more data in an old-protocol
			 * copy, since there is no protocol-level EOF marker then.  We
			 * could go either way for copy from file, but choose to throw
			 * error if there's data after the EOF marker, for consistency
			 * with the new-protocol case.
			 */
			char		dummy;

			if (cstate->copy_src != COPY_FRONTEND &&
				CopyGetData(cstate, &dummy, 1, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}

		if (fld_count != attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("row field count is %d, expected %d",
							(int) fld_count, attr_count)));

		i = 0;
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			cstate->cur_attname = NameStr(att->attname);
			i++;
			values[m] = CopyReadBinaryAttribute(cstate,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 *
	 * GPDB: The defaults are always computed in the QD, and are included
	 * in the QD->QE stream as pre-computed Datums. Funny indentation, to
	 * keep the indentation of the code inside the same as in upstream.
	 * (We could improve this, and compute immutable defaults that don't
	 * affect which segment the row belongs to, in the QE.)
	 */
  if (cstate->dispatch_mode != COPY_EXECUTOR)
  {
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}
  }

	return true;
}


/*
 * Like NextCopyFrom(), but used in the QD, when we want to parse the
 * input line only partially. We only want to parse enough fields needed
 * to determine which target segment to forward the row to.
 *
 * (The logic is actually within NextCopyFrom(). This is a separate
 * function just for symmetry with NextCopyFromExecute()).
 */
static bool
NextCopyFromDispatch(CopyFromState cstate, ExprContext *econtext,
					 Datum *values, bool *nulls)
{
	return NextCopyFrom(cstate, econtext, values, nulls);
}

/*
 * Like NextCopyFrom(), but used in the QE, when we're reading pre-processed
 * rows from the QD.
 */
static bool
NextCopyFromExecute(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls, bool is_directory_table)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count;
	FormData_pg_attribute *attr;
	int			i;
	copy_from_dispatch_row frame;
	int			r;
	bool		got_error;

	if (!is_directory_table)
	{
		tupDesc = RelationGetDescr(cstate->rel);
	}
	else
	{
		tupDesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupDesc, (AttrNumber) 1, "relative_path",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 2, "size",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 3, "last_modified",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 4, "md5",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 5, "tag",
						   TEXTOID, -1 ,0);
	}
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/*
	 * The code below reads the 'copy_from_dispatch_row' struct, and only
	 * then checks if it was actually a 'copy_from_dispatch_error' struct.
	 * That only works when 'copy_from_dispatch_error' is larger than
	 *'copy_from_dispatch_row'.
	 */
	StaticAssertStmt(SizeOfCopyFromDispatchError >=  SizeOfCopyFromDispatchRow,
					 "copy_from_dispatch_error must be larger than copy_from_dispatch_row");

	/*
	 * If we encounter an error while parsing the row (or we receive a row from
	 * the QD that was already marked as an erroneous row), we loop back here
	 * until we get a good row.
	 */
retry:
	got_error = false;

	r = CopyGetData(cstate, (char *) &frame, SizeOfCopyFromDispatchRow, SizeOfCopyFromDispatchRow);
	if (r == 0)
		return false;
	if (r != SizeOfCopyFromDispatchRow)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (frame.lineno == -1)
	{
		HandleQDErrorFrame(cstate, (char *) &frame);
		goto retry;
	}

	/* Prepare for parsing the input line */
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	/* check for overflowing fields */
	if (frame.fld_count < 0 || frame.fld_count > num_phys_attrs)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("extra data after last expected column")));

	/*
	 * Read the input line into 'line_buf'.
	 */
	resetStringInfo(&cstate->line_buf);
	enlargeStringInfo(&cstate->line_buf, frame.line_len);
	if (CopyGetData(cstate, cstate->line_buf.data, frame.line_len, frame.line_len) != frame.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	cstate->line_buf.data[frame.line_len] = '\0';
	cstate->line_buf.len = frame.line_len;
	cstate->line_buf.cursor = frame.residual_off;
	cstate->line_buf_valid = true;
	cstate->cur_lineno = frame.lineno;
	cstate->stopped_processing_at_delim = frame.delim_seen_at_end;

	/*
	 * Parse any fields from the input line that were not processed in the
	 * QD already.
	 */
	if (!cstate->cdbsreh)
	{
		if (!NextCopyFromX(cstate, econtext, values, nulls))
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));
		}
	}
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;
		bool		result;

		PG_TRY();
		{
			result = NextCopyFromX(cstate, econtext, values, nulls);

			if (!result)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		PG_CATCH();
		{
			HandleCopyError(cstate);
			got_error = true;
			MemoryContextSwitchTo(oldcontext);
		}
		PG_END_TRY();
	}

	/*
	 * Read any attributes that were processed in the QD already. The attribute
	 * numbers in the message are already in terms of the target partition, so
	 * we do this after remapping and switching to the partition slot.
	 */
	for (i = 0; i < frame.fld_count; i++)
	{
		int16		attnum;
		int			m;
		int32		len;
		Datum		value;

		if (CopyGetData(cstate, &attnum, sizeof(attnum), sizeof(attnum)) != sizeof(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));

		if (attnum < 1 || attnum > num_phys_attrs)
			elog(ERROR, "invalid attnum received from QD: %d (num physical attributes: %d)",
				 attnum, num_phys_attrs);
		m = attnum - 1;

		cstate->cur_attname = NameStr(attr[m].attname);

		if (attr[attnum - 1].attbyval)
		{
			if (CopyGetData(cstate, &value, sizeof(Datum), sizeof(Datum)) != sizeof(Datum))
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		else
		{
			char	   *p;

			if (attr[attnum - 1].attlen > 0)
			{
				len = attr[attnum - 1].attlen;

				p = palloc(len);
				if (CopyGetData(cstate, p, len, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -1)
			{
				/* For simplicity, varlen's are always transmitted in "long" format */
				if (CopyGetData(cstate, &len, sizeof(len), sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				if (len < VARHDRSZ)
					elog(ERROR, "invalid varlen length received from QD: %d", len);
				p = palloc(len);
				SET_VARSIZE(p, len);
				if (CopyGetData(cstate, p + VARHDRSZ, len - VARHDRSZ, len - VARHDRSZ) != len - VARHDRSZ)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -2)
			{
				/*
				 * Like the varlen case above, cstrings are sent with a length
				 * prefix and no terminator, so we have to NULL-terminate in
				 * memory after reading them in.
				 */
				if (CopyGetData(cstate, &len, sizeof(len), sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p = palloc(len + 1);
				if (CopyGetData(cstate, p, len, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p[len] = '\0';
			}
			else
			{
				elog(ERROR, "attribute %d has invalid length %d",
					 attnum, attr[attnum - 1].attlen);
			}
			value = PointerGetDatum(p);
		}

		cstate->cur_attname = NULL;

		values[m] = value;
		nulls[m] = false;
	}

	if (got_error)
		goto retry;

	/*
	 * Here we should compute defaults for any columns for which we didn't
	 * get a default from the QD. But at the moment, all defaults are evaluated
	 * in the QD.
	 */
	return true;
}

/*
 * Parse and handle an "error frame" from QD.
 *
 * The caller has already read part of the frame; 'p' points to that part,
 * of length 'len'.
 */
static void
HandleQDErrorFrame(CopyFromState cstate, char *p)
{
	CdbSreh *cdbsreh = cstate->cdbsreh;
	MemoryContext oldcontext;
	copy_from_dispatch_error errframe;
	char	   *errormsg;
	char	   *line;
	int			r;

	Assert(Gp_role == GP_ROLE_EXECUTE);

	oldcontext = MemoryContextSwitchTo(cdbsreh->badrowcontext);

	/*
	 * Copy the part of the struct that the caller had already read, and
	 * read the rest.
	 */
	memcpy(&errframe, p, SizeOfCopyFromDispatchRow);

	size_t len = SizeOfCopyFromDispatchRow;
	r = CopyGetData(cstate, ((char *) &errframe) + len,SizeOfCopyFromDispatchError - len, SizeOfCopyFromDispatchError - len);
	if (r != SizeOfCopyFromDispatchError - len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	errormsg = palloc(errframe.errmsg_len + 1);
	line = palloc(errframe.line_len + 1);

	r = CopyGetData(cstate, errormsg, errframe.errmsg_len, errframe.errmsg_len);
	if (r != errframe.errmsg_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	errormsg[errframe.errmsg_len] = '\0';

	r = CopyGetData(cstate, line, errframe.line_len, errframe.line_len);
	if (r != errframe.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	line[errframe.line_len] = '\0';

	cdbsreh->linenumber = errframe.lineno;
	cdbsreh->rawdata->cursor = 0;
	cdbsreh->rawdata->data = line;
	cdbsreh->rawdata->len = strlen(line);
	cdbsreh->errmsg = errormsg;

	HandleSingleRowError(cdbsreh);

	MemoryContextSwitchTo(oldcontext);

}

/*
 * Inlined versions of appendBinaryStringInfo and enlargeStringInfo, for
 * speed.
 *
 * NOTE: These versions don't NULL-terminate the string. We don't need
 * it here.
 */
#define APPEND_MSGBUF_NOCHECK(buf, ptr, datalen) \
	do { \
		memcpy((buf)->data + (buf)->len, ptr, (datalen)); \
		(buf)->len += (datalen); \
	} while(0)

#define APPEND_MSGBUF(buf, ptr, datalen) \
	do { \
		if ((buf)->len + (datalen) >= (buf)->maxlen) \
			enlargeStringInfo((buf), (datalen)); \
		memcpy((buf)->data + (buf)->len, ptr, (datalen)); \
		(buf)->len += (datalen); \
	} while(0)

#define ENLARGE_MSGBUF(buf, needed) \
	do { \
		if ((buf)->len + (needed) >= (buf)->maxlen) \
			enlargeStringInfo((buf), (needed)); \
	} while(0)

/*
 * This is the sending counterpart of NextCopyFromExecute. Used in the QD,
 * to send a row to a QE.
 */
static void
SendCopyFromForwardedTuple(CopyFromState cstate,
						   CdbCopy *cdbCopy,
						   bool toAll,
						   int target_seg,
						   Relation rel,
						   int64 lineno,
						   char *line,
						   int line_len,
						   Datum *values,
						   bool *nulls,
						   bool is_directory_table)
{
	TupleDesc	tupDesc;
	FormData_pg_attribute *attr;
	copy_from_dispatch_row *frame;
	StringInfo	msgbuf;
	int			num_sent_fields;
	AttrNumber	num_phys_attrs;
	int			i;

	if (!OidIsValid(RelationGetRelid(rel)))
		elog(ERROR, "invalid target table OID in COPY");

	if (!is_directory_table)
	{
		tupDesc = RelationGetDescr(rel);
	}
	else
	{
		tupDesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupDesc, (AttrNumber) 1, "relative_path",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 2, "size",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 3, "last_modified",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 4, "md5",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupDesc, (AttrNumber) 5, "tag",
						   TEXTOID, -1 ,0);
	}
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;

	/*
	 * Reset the message buffer, and reserve enough space for the header,
	 * the OID if any, and the residual line.
	 */
	msgbuf = cstate->dispatch_msgbuf;
	ENLARGE_MSGBUF(msgbuf, SizeOfCopyFromDispatchRow + sizeof(Oid) + cstate->line_buf.len);

	/* the header goes to the beginning of the struct, but it will be filled in later. */
	msgbuf->len = SizeOfCopyFromDispatchRow;

	/*
	 * Next, any residual text that we didn't process in the QD.
	 */
	APPEND_MSGBUF_NOCHECK(msgbuf, cstate->line_buf.data, cstate->line_buf.len);

	/*
	 * Append attributes to the buffer.
	 */
	num_sent_fields = 0;
	for (i = 0; i < num_phys_attrs; i++)
	{
		int16		attnum = i + 1;

		/* NULLs are simply left out of the message. */
		if (nulls[i])
			continue;

		/*
		 * Make sure we have room for the attribute number. While we're at it,
		 * also reserve room for the Datum, if it's a by-value datatype, or for
		 * the length field, if it's a varlena. Allocating both in one call
		 * saves one size-check.
		 */
		ENLARGE_MSGBUF(msgbuf, sizeof(int16) + sizeof(Datum));

		/* attribute number comes first */
		APPEND_MSGBUF_NOCHECK(msgbuf, &attnum, sizeof(int16));

		if (attr[i].attbyval)
		{
			/* we already reserved space for this above, so we can just memcpy */
			APPEND_MSGBUF_NOCHECK(msgbuf, &values[i], sizeof(Datum));
		}
		else
		{
			if (attr[i].attlen > 0)
			{
				APPEND_MSGBUF(msgbuf, DatumGetPointer(values[i]), attr[i].attlen);
			}
			else if (attr[i].attlen == -1)
			{
				int32		len;
				char	   *ptr;

				/* For simplicity, varlen's are always transmitted in "long" format */
				Assert(!VARATT_IS_SHORT(values[i]));
				len = VARSIZE(values[i]);
				ptr = VARDATA(values[i]);

				/* we already reserved space for this int */
				APPEND_MSGBUF_NOCHECK(msgbuf, &len, sizeof(int32));
				APPEND_MSGBUF(msgbuf, ptr, len - VARHDRSZ);
			}
			else if (attr[i].attlen == -2)
			{
				/*
				 * These attrs are NULL-terminated in memory, but we send
				 * them length-prefixed (like the varlen case above) so that
				 * the receiver can preallocate a data buffer.
				 */
				int32		len;
				size_t		slen;
				char	   *ptr;

				ptr = DatumGetPointer(values[i]);
				slen = strlen(ptr);

				if (slen > PG_INT32_MAX)
				{
					elog(ERROR, "attribute %d is too long (%lld bytes)",
						 attnum, (long long) slen);
				}

				len = (int32) slen;

				APPEND_MSGBUF_NOCHECK(msgbuf, &len, sizeof(int32));
				APPEND_MSGBUF(msgbuf, ptr, len);
			}
			else
			{
				elog(ERROR, "attribute %d has invalid length %d",
					 attnum, attr[i].attlen);
			}
		}

		num_sent_fields++;
	}

	/*
	 * Fill in the header. We reserved room for this at the beginning of the
	 * buffer.
	 */
	frame = (copy_from_dispatch_row *) msgbuf->data;
	frame->lineno = lineno;
	frame->relid = RelationGetRelid(rel);
	frame->line_len = cstate->line_buf.len;
	frame->residual_off = cstate->line_buf.cursor;
	frame->fld_count = num_sent_fields;
	frame->delim_seen_at_end = cstate->stopped_processing_at_delim;

	if (toAll)
		cdbCopySendDataToAll(cdbCopy, msgbuf->data, msgbuf->len);
	else
		cdbCopySendData(cdbCopy, target_seg, msgbuf->data, msgbuf->len);
}

static void
SendCopyFromForwardedHeader(CopyFromState cstate, CdbCopy *cdbCopy)
{
	copy_from_dispatch_header header_frame;

	cdbCopySendDataToAll(cdbCopy, QDtoQESignature, sizeof(QDtoQESignature));

	memset(&header_frame, 0, sizeof(header_frame));
	header_frame.first_qe_processed_field = cstate->first_qe_processed_field;

	cdbCopySendDataToAll(cdbCopy, (char *) &header_frame, sizeof(header_frame));
}

static void
SendCopyFromForwardedError(CopyFromState cstate, CdbCopy *cdbCopy, char *errormsg)
{
	copy_from_dispatch_error *errframe;
	StringInfo	msgbuf;
	int			target_seg;
	int			errormsg_len = strlen(errormsg);

	msgbuf = cstate->dispatch_msgbuf;
	resetStringInfo(msgbuf);
	enlargeStringInfo(msgbuf, SizeOfCopyFromDispatchError);
	/* allocate space for the header (we'll fill it in last). */
	msgbuf->len = SizeOfCopyFromDispatchError;

	appendBinaryStringInfo(msgbuf, errormsg, errormsg_len);
	appendBinaryStringInfo(msgbuf, cstate->line_buf.data, cstate->line_buf.len);

	errframe = (copy_from_dispatch_error *) msgbuf->data;

	errframe->error_marker = -1;
	errframe->lineno = cstate->cur_lineno;
	errframe->line_len = cstate->line_buf.len;
	errframe->errmsg_len = errormsg_len;

	/* send the bad data row to a random QE (via roundrobin) */
	if (cstate->lastsegid == cdbCopy->total_segs)
		cstate->lastsegid = 0; /* start over from first segid */

	target_seg = (cstate->lastsegid++ % cdbCopy->total_segs);

	cdbCopySendData(cdbCopy, target_seg, msgbuf->data, msgbuf->len);
}

static void
EndCopyFromDirectoryTable(CopyFromState cstate)
{
	/* No COPY FROM related resources except memory. */
	if (cstate->is_program)
	{
		close_program_pipes(cstate->program_pipes, true);
	}
	else
	{
		if (cstate->copy_file && FreeFile(cstate->copy_file))
		{
			ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not close file \"%s\": %m",
							    cstate->filename)));
		}
	}

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
EndCopyFrom(CopyFromState cstate)
{
	if (cstate->rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
		return EndCopyFromDirectoryTable(cstate);

	/* No COPY FROM related resources except memory. */
	if (cstate->is_program)
	{
		close_program_pipes(cstate->program_pipes, true);
	}
	else
	{
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Initialize data loader parsing state
 */
/*
* GPDB_14_MERGE_FIXME:
* Do we need this function ?
* Check comments in this function.
*/
static void CopyInitDataParser(CopyFromState cstate)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	// cstate->eol_type = EOL_UNKNOWN; /* never set this, see comments in BeginCopyFrom */
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->opts.null_print_len = strlen(cstate->opts.null_print);

	if (!cstate->raw_buf)
	{
		/* Set up data buffer to hold a chunk of data */
		cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
		cstate->raw_buf_index = cstate->raw_buf_len = 0;
		MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
		cstate->raw_buf[RAW_BUF_SIZE] = '\0';
		cstate->raw_reached_eof = false;

		if (!cstate->opts.binary)
		{
			if (cstate->need_transcoding)
			{
				cstate->input_buf = (char *) palloc(INPUT_BUF_SIZE + 1);
				cstate->input_buf_index = cstate->input_buf_len = 0;
			}
			else
				cstate->input_buf = cstate->raw_buf;
			cstate->input_reached_eof = false;

			initStringInfo(&cstate->line_buf);
		}
	}

	// to keep the same with upstream
	initStringInfo(&cstate->attribute_buf);

	MemoryContextSwitchTo(oldcontext);
}

static GpDistributionData *
InitDistributionData(CopyFromState cstate, EState *estate)
{
	GpDistributionData *distData;
	GpPolicy   *policy;
	CdbHash	   *cdbHash;

	/*
	 * A non-partitioned table, or all the partitions have identical
	 * distribution policies.
	 */
	policy = GpPolicyCopy(cstate->rel->rd_cdbpolicy);
	cdbHash = makeCdbHashForRelation(cstate->rel);

	distData = palloc(sizeof(GpDistributionData));
	distData->policy = policy;
	distData->cdbHash = cdbHash;

	return distData;
}

static void
FreeDistributionData(GpDistributionData *distData)
{
	if (distData)
	{
		if (distData->policy)
			pfree(distData->policy);
		if (distData->cdbHash)
			pfree(distData->cdbHash);
		pfree(distData);
	}
}

/*
 * Compute which fields need to be processed in the QD, and which ones can
 * be delayed to the QE.
 */
static void
InitCopyFromDispatchSplit(CopyFromState cstate, GpDistributionData *distData,
						  EState *estate)
{
	int			first_qe_processed_field = 0;
	Bitmapset  *needed_cols = NULL;
	ListCell   *lc;

	if (cstate->opts.binary)
	{
		foreach(lc, cstate->attnumlist)
		{
			AttrNumber attnum = lfirst_int(lc);
			needed_cols = bms_add_member(needed_cols, attnum);
			first_qe_processed_field++;
		}
	}
	else
	{
		int			fieldno;
		/*
		 * We need all the columns that form the distribution key.
		 */
		if (distData->policy)
		{
			for (int i = 0; i < distData->policy->nattrs; i++)
				needed_cols = bms_add_member(needed_cols, distData->policy->attrs[i]);
		}

		/* Get the max fieldno that contains one of the needed attributes. */
		fieldno = 0;
		foreach(lc, cstate->attnumlist)
		{
			AttrNumber attnum = lfirst_int(lc);

			if (bms_is_member(attnum, needed_cols))
				first_qe_processed_field = fieldno + 1;
			fieldno++;
		}
	}

	cstate->first_qe_processed_field = first_qe_processed_field;

	if (Test_copy_qd_qe_split)
	{
		if (first_qe_processed_field == list_length(cstate->attnumlist))
			elog(INFO, "all fields will be processed in the QD");
		else
			elog(INFO, "first field processed in the QE: %d", first_qe_processed_field);
	}
}

static unsigned int
GetTargetSeg(GpDistributionData *distData, TupleTableSlot *slot)
{
	unsigned int target_seg;
	CdbHash	   *cdbHash = distData->cdbHash;
	GpPolicy   *policy = distData->policy; /* the partitioning policy for this table */
	AttrNumber	p_nattrs;	/* num of attributes in the distribution policy */

	/*
	 * These might be NULL, if we're called with a "main" GpDistributionData,
	 * for a partitioned table with heterogenous partitions. The caller
	 * should've used GetDistributionPolicyForPartition() to get the right
	 * distdata object for the partition.
	 */
	if (!policy)
		elog(ERROR, "missing distribution policy.");
	if (!cdbHash)
		elog(ERROR, "missing cdbhash");

	/*
	 * At this point in the code, baseValues[x] is final for this
	 * data row -- either the input data, a null or a default
	 * value is in there, and constraints applied.
	 *
	 * Perform a cdbhash on this data row. Perform a hash operation
	 * on each attribute.
	 */
	p_nattrs = policy->nattrs;
	if (p_nattrs > 0)
	{
		cdbhashinit(cdbHash);

		for (int i = 0; i < p_nattrs; i++)
		{
			/* current attno from the policy */
			AttrNumber	h_attnum = policy->attrs[i];
			Datum		d;
			bool		isnull;

			d = slot_getattr(slot, h_attnum, &isnull);

			cdbhash(cdbHash, i + 1, d, isnull);
		}

		target_seg = cdbhashreduce(cdbHash); /* hash result segment */
	}
	else if (gp_random_insert_segments > 0 &&
			 gp_random_insert_segments < policy->numsegments)
	{
		/* Select limited random segments for data insertion. */
		target_seg = cdbhashrandomseg(gp_random_insert_segments);
	}
	else
	{
		/*
		 * Randomly distributed. Pick a segment at random.
		 */
		target_seg = cdbhashrandomseg(policy->numsegments);
	}

	return target_seg;
}
