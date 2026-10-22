/*-------------------------------------------------------------------------
 *
 * copyto.c
 *		COPY <table> TO file/program/client
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyto.c
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
#include "commands/copy.h"
#include "commands/copyto_internal.h"
#include "commands/progress.h"
#include "common/cryptohash.h"
#include "common/md5.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/execute_pipe.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/metrics_utils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"

#define EXEC_DATA_P 0

extern CopyStmt *glob_copystmt;

/* NOTE: there's a copy of this in copyfromparse.c */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* non-export function prototypes */
static void EndCopy(CopyToState cstate);
static void CopyAttributeOutText(CopyToState cstate, char *string);
static void CopyAttributeOutCSV(CopyToState cstate, char *string,
								bool use_quote, bool single_attr);
static uint64 CopyToDispatch(CopyToState cstate);
static void CopyToDispatchFlush(CopyToState cstate);
static uint64 CopyTo(CopyToState cstate);
static void CopySendChar(CopyToState cstate, char c);
static uint64 CopyToDispatchDirectoryTable(CopyToState cstate);
static uint64 CopyToDirectoryTable(CopyToState cstate);

/* Low-level communications functions */
static void SendCopyBegin(CopyToState cstate);
static void SendCopyEnd(CopyToState cstate);
static void CopySendData(CopyToState cstate, const void *databuf, int datasize);
static void CopySendString(CopyToState cstate, const char *str);
static void CopySendInt32(CopyToState cstate, int32 val);
static void CopySendInt16(CopyToState cstate, int16 val);
static CopyIntoClause* MakeCopyIntoClause(CopyStmt *stmt);

/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyToState cstate)
{
	StringInfoData buf;
	int			natts = list_length(cstate->attnumlist);
	int16		format = (cstate->opts.binary ? 1 : 0);
	int			i;

	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, format);	/* overall format */
	pq_sendint16(&buf, natts);
	for (i = 0; i < natts; i++)
		pq_sendint16(&buf, format); /* per-column formats */
	pq_endmessage(&buf);
	cstate->copy_dest = COPY_FRONTEND;
}

static void
SendCopyEnd(CopyToState cstate)
{
	/* Shouldn't have any unsent data */
	Assert(cstate->fe_msgbuf->len == 0);
	/* Send Copy Done message */
	pq_putemptymessage('c');
}

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyToState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
CopySendString(CopyToState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

void CopySendEndOfRow(CopyToState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			if (!cstate->opts.binary)
			{
				/* Default line termination depends on platform */
#ifndef WIN32
				CopySendChar(cstate, '\n');
#else
				CopySendString(cstate, "\r\n");
#endif
			}

			if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1,
					   cstate->copy_file) != 1 ||
				ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					if (errno == EPIPE)
					{
						/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
						if (cstate->copy_file)
						{
							fclose(cstate->copy_file);
							cstate->copy_file = NULL;
						}
						close_program_pipes(cstate->program_pipes, true);

						/*
						 * If ClosePipeToProgram() didn't throw an error, the
						 * program terminated normally, but closed the pipe
						 * first. Restore errno, and throw an error.
						 */
						errno = EPIPE;
					}
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY program: %m")));
				}
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY file: %m")));
			}
			break;
		case COPY_FRONTEND:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->opts.binary)
				CopySendChar(cstate, '\n');

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_CALLBACK:
			if (cstate->data_dest_cb)
				cstate->data_dest_cb(fe_msgbuf->data, fe_msgbuf->len);
			else
			{
				/* we don't actually do the write here, we let the caller do it */
#ifndef WIN32
				CopySendChar(cstate, '\n');
#else
				CopySendString(cstate, "\r\n");
#endif
				return; /* don't want to reset msgbuf quite yet */
			}
			break;
	}

	/* Update the progress */
	cstate->bytes_processed += fe_msgbuf->len;
	pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);

	resetStringInfo(fe_msgbuf);
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static inline void
CopySendInt32(CopyToState cstate, int32 val)
{
	uint32		buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static inline void
CopySendInt16(CopyToState cstate, int16 val)
{
	uint16		buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * Setup CopyToState to read tuples from a table or a query for COPY TO.
 *
 * 'rel': Relation to be copied
 * 'raw_query': Query whose results are to be copied
 * 'queryRelId': OID of base relation to convert to a query (for RLS)
 * 'filename': Name of server-local file to write, NULL for STDOUT
 * 'is_program': true if 'filename' is program to execute
 * 'data_dest_cb': Callback that processes the output data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyToState, to be passed to DoCopyTo() and related functions.
 */
CopyToState
BeginCopyTo(ParseState *pstate,
			Relation rel,
			RawStmt *raw_query,
			Oid queryRelId,
			const char *filename,
			bool is_program,
			copy_data_dest_cb data_dest_cb,
			List *attnamelist,
			List *options)
{
	CopyToState cstate;
	bool		pipe;
	MemoryContext oldcontext;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_TO,
		0
	};

	if (rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from materialized view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from foreign table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from sequence \"%s\"",
							RelationGetRelationName(rel))));
		else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from partitioned table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from non-table relation \"%s\"",
							RelationGetRelationName(rel))));
	}


	cstate = BeginCopy(pstate, rel, raw_query, queryRelId, attnamelist,
					   options, NULL);
	pipe = (filename == NULL || (Gp_role == GP_ROLE_EXECUTE && !cstate->opts.on_segment));
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Determine the mode */
	if (Gp_role == GP_ROLE_DISPATCH && !cstate->opts.on_segment &&
		cstate->rel && cstate->rel->rd_cdbpolicy)
	{
		cstate->dispatch_mode = COPY_DISPATCH;
	}
	else
		cstate->dispatch_mode = COPY_DIRECT;

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* in ON SEGMENT mode, we don't open anything on the dispatcher. */

		if (filename == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("STDOUT is not supported by 'COPY ON SEGMENT'")));
	}
	else if (data_dest_cb)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK;
		cstate->copy_dest = COPY_CALLBACK;
		cstate->data_dest_cb = data_dest_cb;
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;

		/* the grammar does not allow this on QD*/
		/* on QE, this could happen */
		Assert(!is_program || Gp_role == GP_ROLE_EXECUTE);
		if (whereToSendOutput != DestRemote)
			cstate->copy_file = stdout;
	}
	else
	{
		cstate->filename = pstrdup(filename);
		cstate->is_program = is_program;


		if (cstate->opts.on_segment)
			MangleCopyFileName(&cstate->filename, NULL);

		if (is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate->filename, true);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[EXEC_DATA_P], PG_BINARY_W);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			mode_t		oumask; /* Pre-existing umask value */
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in
			 * the foot by overwriting a database file ...
			 */
			if (!is_absolute_path(cstate->filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask(S_IWGRP | S_IWOTH);
			PG_TRY();
			{
				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			}
			PG_FINALLY();
			{
				umask(oumask);
			}
			PG_END_TRY();
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY TO instructs the PostgreSQL server process to write a file. "
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
		}
	}
	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	pgstat_progress_update_multi_param(2, progress_cols, progress_vals);

	cstate->bytes_processed = 0;

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Clean up storage and release resources for COPY TO.
 */
void
EndCopyTo(CopyToState cstate, uint64 *processed)
{
	if (cstate->queryDesc != NULL)
	{
		/* Close down the query and free resources. */
		ExecutorFinish(cstate->queryDesc);
		ExecutorEnd(cstate->queryDesc);
		if (cstate->queryDesc->es_processed > 0)
			*processed = cstate->queryDesc->es_processed;
		FreeQueryDesc(cstate->queryDesc);
		PopActiveSnapshot();
	}

	/* Clean up storage */
	EndCopy(cstate);
}

/*
 * Copy from relation or query TO file.
 *
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
uint64
DoCopyTo(CopyToState cstate)
{
	bool		pipe = (cstate->filename == NULL && cstate->data_dest_cb == NULL);
	bool		fe_copy = (pipe && whereToSendOutput == DestRemote);
	uint64		processed;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet("DoCopyToFail", DDLNotSpecified, "", "");
#endif

	PG_TRY();
	{
		if (fe_copy)
			SendCopyBegin(cstate);

		/*
		 * We want to dispatch COPY TO commands only in the case that
		 * we are the dispatcher and we are copying from a user relation
		 * (a relation where data is distributed in the segment databases).
		 * Otherwize, if we are not the dispatcher *or* if we are
		 * doing COPY (SELECT) we just go straight to work, without
		 * dispatching COPY commands to executors.
		 */
		if (Gp_role == GP_ROLE_DISPATCH && cstate->rel && cstate->rel->rd_cdbpolicy)
			processed = CopyToDispatch(cstate);
		else
			processed = CopyTo(cstate);

		if (fe_copy)
			SendCopyEnd(cstate);
		else if (Gp_role == GP_ROLE_EXECUTE && cstate->opts.on_segment)
		{
			/*
			 * For COPY ON SEGMENT command, switch back to front end
			 * before sending copy end which is "\."
			 */
			cstate->copy_dest = COPY_FRONTEND;
			SendCopyEnd(cstate);
		}
	}
	PG_CATCH();
	{
		/*
		 * Make sure we turn off old-style COPY OUT mode upon error. It is
		 * okay to do this in all cases, since it does nothing if the mode is
		 * not on.
		 */
		if (Gp_role == GP_ROLE_EXECUTE && cstate->opts.on_segment)
			cstate->copy_dest = COPY_FRONTEND;

		PG_RE_THROW();
	}
	PG_END_TRY();

	return processed;
}

/*
 * Emit one row during DoCopyTo().
 */
void
CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->opts.binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = slot->tts_values[attnum - 1];
		bool		isnull = slot->tts_isnull[attnum - 1];

		if (!cstate->opts.binary)
		{
			if (need_delim)
				CopySendChar(cstate, cstate->opts.delim[0]);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->opts.binary)
				CopySendString(cstate, cstate->opts.null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->opts.binary)
			{
				char		quotec = cstate->opts.quote ? cstate->opts.quote[0] : '\0';

				/* int2out or int4out ? */
				if (out_functions[attnum -1].fn_oid == 39 ||  /* int2out or int4out */
					out_functions[attnum -1].fn_oid == 43 )
				{
					char tmp[33];
					/*
					 * The standard postgres way is to call the output function, but that involves one or more pallocs,
					 * and a call to sprintf, followed by a conversion to client charset.
					 * Do a fast conversion to string instead.
					 */

					if (out_functions[attnum -1].fn_oid ==  39)
						pg_itoa(DatumGetInt16(value),tmp);
					else
						pg_ltoa(DatumGetInt32(value),tmp);

					/*
					 * Integers don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->opts.force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
					CopySendData(cstate, tmp, strlen(tmp));
					if (cstate->opts.force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
				}
				else if (out_functions[attnum -1].fn_oid == F_NUMERIC_OUT)   /* numeric_out */
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					/*
					 * Numerics don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->opts.force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
					CopySendData(cstate, string, strlen(string));
					if (cstate->opts.force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
				}
				else
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					if (cstate->opts.csv_mode)
						CopyAttributeOutCSV(cstate, string,
											cstate->opts.force_quote_flags[attnum - 1],
											list_length(cstate->attnumlist) == 1);
					else
						CopyAttributeOutText(cstate, string);
				}
			}
			else
			{
				bytea	   *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if (cstate->copy_dest != COPY_CALLBACK)
	{
		CopySendEndOfRow(cstate);
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(CopyToState cstate, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->opts.delim[0];
	char		escapec = cstate->opts.escape[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string,
							   strlen(string),
							   cstate->file_encoding);
	else
		ptr = string;

	if (cstate->escape_off)
	{
		CopySendData(cstate, ptr, strlen(ptr));
		return;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == '\\' || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyToState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->opts.delim[0];
	char		quotec;
	char		escapec = cstate->opts.escape[0];

	/*
	 * MPP-8075. We may get called with cstate->opts.quote == NULL.
	 */
	if (cstate->opts.quote == NULL)
	{
		quotec = '"';
	}
	else
	{
		quotec = cstate->opts.quote[0];
	}

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->opts.null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_any(string,
							   strlen(string),
							   cstate->file_encoding);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char	   *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	if (Gp_role != GP_ROLE_EXECUTE)
		return;
	DR_copy    *myState = (DR_copy *) self;
	myState->cstate = BeginCopyToOnSegment(myState->queryDesc);
}

/*
 * copy_dest_receive --- receive one tuple
 */
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyToState cstate = myState->cstate;

	/* Send the data */
	CopyOneRowTo(cstate, slot);

	/* Increment the number of processed tuples, and report the progress */
	pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
								 ++myState->processed);

	return true;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self)
{
	if (Gp_role != GP_ROLE_EXECUTE)
		return;
	DR_copy    *myState = (DR_copy *) self;
	EndCopyToOnSegment(myState->cstate);
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver *
CreateCopyDestReceiver(void)
{
	DR_copy    *self = (DR_copy *) palloc(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->cstate = NULL;		/* will be set later */
	self->queryDesc = NULL;		/* need to be set later */
	self->processed = 0;

	return (DestReceiver *) self;
}

/*
 * AXG: This one is equivalent to CopySendEndOfRow() besides that
 * it doesn't send end of row - it just flushed the data. We need
 * this method for the dispatcher COPY TO since it already has data
 * with newlines (from the executors).
 */
static void
CopyToDispatchFlush(CopyToState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:

			(void) fwrite(fe_msgbuf->data, fe_msgbuf->len,
						  1, cstate->copy_file);
			if (ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					if (errno == EPIPE)
					{
						/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
						if (cstate->copy_file)
						{
							fclose(cstate->copy_file);
							cstate->copy_file = NULL;
						}
						close_program_pipes(cstate->program_pipes, true);

						/*
						 * If close_program_pipes() didn't throw an error,
						 * the program terminated normally, but closed the
						 * pipe first. Restore errno, and throw an error.
						 */
						errno = EPIPE;
					}
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY program: %m")));
				}
				else
					ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
			}
			break;
		case COPY_FRONTEND:

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_CALLBACK:
			elog(ERROR, "unexpected destination COPY_CALLBACK to flush data");
			break;
	}

	resetStringInfo(fe_msgbuf);
}

CopyIntoClause*
MakeCopyIntoClause(CopyStmt *stmt)
{
	CopyIntoClause *copyIntoClause;
	copyIntoClause = makeNode(CopyIntoClause);

	copyIntoClause->is_program = stmt->is_program;
	copyIntoClause->filename = stmt->filename;
	copyIntoClause->options = stmt->options;
	copyIntoClause->attlist = stmt->attlist;

	return copyIntoClause;
}

/*
 * Common setup routines used by BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
CopyToState
BeginCopy(ParseState *pstate,
		  Relation rel,
		  RawStmt *raw_query,
		  Oid queryRelId,
		  List *attnamelist,
		  List *options,
		  TupleDesc tupDesc)
{
	CopyToState	cstate;
	int			num_phys_attrs;
	MemoryContext oldcontext;

	/* Allocate workspace and zero all fields */
	cstate = (CopyToStateData *) palloc0(sizeof(CopyToStateData));

	glob_cstate = cstate;

	cstate->escape_off = false;

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Cloudberry needs this to detect custom protocol */
	if (rel)
		cstate->rel = rel;

	ProcessCopyOptions(pstate, &cstate->opts, false /* is_from */, options, rel ? rel->rd_id : InvalidOid);

	/* Extract options from the statement node tree */
	if (cstate->opts.escape != NULL && pg_strcasecmp(cstate->opts.escape, "off") == 0)
	{
		cstate->escape_off = true;
	}

	/* Process the source/target relation or query */
	if (rel)
	{
		Assert(!raw_query);

		tupDesc = RelationGetDescr(cstate->rel);
	}
	else if(raw_query)
	{
		List	   *rewritten;
		Query	   *query;
		PlannedStmt *plan;
		DestReceiver *dest;

		cstate->rel = NULL;

		/*
		 * Run parse analysis and rewrite.  Note this also acquires sufficient
		 * locks on the source table(s).
		 *
		 * Because the parser and planner tend to scribble on their input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that the COPY is in a portal or plpgsql
		 * function and is executed repeatedly.  (See also the same hack in
		 * DECLARE CURSOR and PREPARE.)  XXX FIXME someday.
		 */
		rewritten = pg_analyze_and_rewrite_fixedparams(copyObject(raw_query),
													   pstate->p_sourcetext, NULL, 0,
													   NULL);

		/* check that we got back something we can work with */
		if (rewritten == NIL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
		}
		else if (list_length(rewritten) > 1)
		{
			ListCell   *lc;

			/* examine queries to determine which error message to issue */
			foreach(lc, rewritten)
			{
				Query	   *q = lfirst_node(Query, lc);

				if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional DO INSTEAD rules are not supported for COPY")));
				if (q->querySource == QSRC_NON_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DO ALSO rules are not supported for COPY")));
			}

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
		}

		query = linitial_node(Query, rewritten);


		if (cstate->opts.on_segment && IsA(query, Query))
		{
			query->parentStmtType = PARENTSTMTTYPE_COPY;
		}
		/* Query mustn't use INTO, either */
		if (query->utilityStmt != NULL &&
			IsA(query->utilityStmt, CreateTableAsStmt))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT INTO) is not supported")));

		/* The only other utility command we could see is NOTIFY */
		if (query->utilityStmt != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY query must not be a utility command")));

		/*
		 * Similarly the grammar doesn't enforce the presence of a RETURNING
		 * clause, but this is required here.
		 */
		if (query->commandType != CMD_SELECT &&
			query->returningList == NIL)
		{
			Assert(query->commandType == CMD_INSERT ||
				   query->commandType == CMD_UPDATE ||
				   query->commandType == CMD_DELETE);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY query must have a RETURNING clause")));
		}

		/* plan the query */
		int			cursorOptions = CURSOR_OPT_PARALLEL_OK;

		/* GPDB: Pass the IGNORE EXTERNAL PARTITION option to the planner. */
		if (cstate->opts.skip_foreign_partitions)
			cursorOptions |= CURSOR_OPT_SKIP_FOREIGN_PARTITIONS;

		plan = pg_plan_query(query, pstate->p_sourcetext, cursorOptions, NULL);

		/*
		 * With row level security and a user using "COPY relation TO", we
		 * have to convert the "COPY relation TO" to a query-based COPY (eg:
		 * "COPY (SELECT * FROM ONLY relation) TO"), to allow the rewriter to
		 * add in any RLS clauses.
		 *
		 * When this happens, we are passed in the relid of the originally
		 * found relation (which we have locked).  As the planner will look up
		 * the relation again, we double-check here to make sure it found the
		 * same one that we have locked.
		 */
		if (queryRelId != InvalidOid)
		{
			/*
			 * Note that with RLS involved there may be multiple relations,
			 * and while the one we need is almost certainly first, we don't
			 * make any guarantees of that in the planner, so check the whole
			 * list and make sure we find the original relation.
			 */
			if (!list_member_oid(plan->relationOids, queryRelId))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("relation referenced by COPY statement has changed")));
		}

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create dest receiver for COPY OUT */
		dest = CreateDestReceiver(DestCopyOut);
		((DR_copy *) dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, NULL, NULL,
											GP_INSTRUMENT_OPTS);
		if (cstate->opts.on_segment)
			cstate->queryDesc->plannedstmt->copyIntoClause =
					MakeCopyIntoClause(glob_copystmt);

		/* GPDB hook for collecting query info */
		if (query_info_collect_hook)
			(*query_info_collect_hook)(METRICS_QUERY_SUBMIT, cstate->queryDesc);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		ExecutorStart(cstate->queryDesc, 0);

		tupDesc = cstate->queryDesc->tupDesc;
	}

	cstate->attnamelist = attnamelist;
	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->opts.force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_quote_all)
	{
		int			i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->opts.force_quote_flags[i] = true;
	}
	else if (cstate->opts.force_quote)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_quote_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();
	else
		cstate->file_encoding = cstate->opts.file_encoding;

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 *
	 * In COPY_EXECUTE mode, the dispatcher has already done the conversion.
	 */
	if (cstate->dispatch_mode != COPY_DISPATCH)
	{
		cstate->need_transcoding =
			((cstate->file_encoding != GetDatabaseEncoding() ||
			  pg_database_encoding_max_length() > 1));
	}
	else
	{
		cstate->need_transcoding = false;
	}
	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);

	cstate->copy_dest = COPY_FILE;	/* default */

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

CopyToState
BeginCopyToOnSegment(QueryDesc *queryDesc)
{
	CopyToState	cstate;
	MemoryContext oldcontext;
	ListCell   *cur;

	TupleDesc	tupDesc;
	int			num_phys_attrs;
	char	   *filename;
	CopyIntoClause *copyIntoClause;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_TO,
		0
	};

	Assert(Gp_role == GP_ROLE_EXECUTE);

	copyIntoClause = queryDesc->plannedstmt->copyIntoClause;
	tupDesc = queryDesc->tupDesc;
	cstate = BeginCopy(NULL, NULL, NULL, InvalidOid, copyIntoClause->attlist,
					   copyIntoClause->options, tupDesc);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	cstate->opts.null_print_client = cstate->opts.null_print;		/* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->filename = pstrdup(copyIntoClause->filename);
	cstate->is_program = copyIntoClause->is_program;

	if (cstate->opts.on_segment)
		MangleCopyFileName(&cstate->filename, NULL);
	filename = cstate->filename;

	if (cstate->is_program)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
		cstate->program_pipes = open_program_pipes(cstate->filename, true);
		cstate->copy_file = fdopen(cstate->program_pipes->pipes[EXEC_DATA_P], PG_BINARY_W);

		if (cstate->copy_file == NULL)
			ereport(ERROR,
					(errmsg("could not execute command \"%s\": %m",
							cstate->filename)));
	}
	else
	{
		mode_t oumask; /* Pre-existing umask value */
		struct stat st;

		progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
		/*
		 * Prevent write to relative path ... too easy to shoot oneself in
		 * the foot by overwriting a database file ...
		 */
		if (!is_absolute_path(filename))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
							errmsg("relative path not allowed for COPY to file")));

		oumask = umask(S_IWGRP | S_IWOTH);
		cstate->copy_file = AllocateFile(filename, PG_BINARY_W);
		umask(oumask);
		if (cstate->copy_file == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("could not open file \"%s\" for writing: %m", filename)));

		// Increase buffer size to improve performance  (cmcdevitt)
		/* GPDB_14_MERGE_FIXME: Ret value process. */
		setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

		fstat(fileno(cstate->copy_file), &st);
		if (S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is a directory", filename)));
	}

	num_phys_attrs = tupDesc->natts;
	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->opts.binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	if (cstate->opts.binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/* if a header has been requested send the line */
		if (cstate->opts.header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;
				Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

				if (hdr_delim)
					CopySendChar(cstate, cstate->opts.delim[0]);
				hdr_delim = true;

				colname = NameStr(attr->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			CopySendEndOfRow(cstate);
		}
	}

	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	pgstat_progress_update_multi_param(2, progress_cols, progress_vals);
	cstate->bytes_processed = 0;

	MemoryContextSwitchTo(oldcontext);
	return cstate;
}

/*
 * Set up CopyToState for writing to a foreign or external table.
 */
CopyToState
BeginCopyToForeignTable(Relation forrel, List *options)
{
	CopyToState	cstate;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_TO,
		PROGRESS_COPY_TYPE_CALLBACK
	};

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

	cstate = BeginCopy(NULL, forrel,
					   NULL, /* raw_query */
					   InvalidOid,
					   NIL, options,
					   RelationGetDescr(forrel));
	cstate->dispatch_mode = COPY_DIRECT;

	/*
	 * We use COPY_CALLBACK to mean that the each line should be
	 * left in fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
	cstate->opts.null_print_client = cstate->opts.null_print;		/* default */
	if (cstate->need_transcoding)
		cstate->opts.null_print_client = pg_server_to_any(cstate->opts.null_print,
														  cstate->opts.null_print_len,
														  cstate->file_encoding);
	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	pgstat_progress_update_multi_param(2, progress_cols, progress_vals);
	cstate->bytes_processed = 0;

	return cstate;
}

void EndCopyToOnSegment(CopyToState cstate)
{
	Assert(Gp_role == GP_ROLE_EXECUTE);

	if (cstate->opts.binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);

		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	MemoryContextDelete(cstate->rowcontext);

	EndCopy(cstate);
}

/*
 * Copy FROM relation TO file, in the dispatcher. Starts a COPY TO command on
 * each of the executors and gathers all the results and writes it out.
 */
static uint64
CopyToDispatch(CopyToState cstate)
{
	CopyStmt   *stmt = glob_copystmt;
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	int			attr_count;
	CdbCopy    *cdbCopy;
	uint64		processed = 0;

	if (cstate->rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
		return CopyToDispatchDirectoryTable(cstate);

	tupDesc = cstate->rel->rd_att;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	cdbCopy = makeCdbCopyTo(cstate);

	/* XXX: lock all partitions */

	/*
	 * Start a COPY command in every db of every segment in Apache Cloudberry.
	 *
	 * From this point in the code we need to be extra careful
	 * about error handling. ereport() must not be called until
	 * the COPY command sessions are closed on the executors.
	 * Calling ereport() will leave the executors hanging in
	 * COPY state.
	 */
	elog(DEBUG5, "COPY command sent to segdbs");

	PG_TRY();
	{
		bool		done;

		cdbCopyStart(cdbCopy, stmt, cstate->file_encoding);

		if (cstate->opts.binary)
		{
			/* Generate header for a binary copy */
			int32		tmp;

			/* Signature */
			CopySendData(cstate, (char *) BinarySignature, 11);
			/* Flags field */
			tmp = 0;
			CopySendInt32(cstate, tmp);
			/* No header extension */
			tmp = 0;
			CopySendInt32(cstate, tmp);
		}

		/* if a header has been requested send the line */
		if (cstate->opts.header_line)
		{
			ListCell   *cur;
			bool		hdr_delim = false;

			/*
			 * For non-binary copy, we need to convert null_print to client
			 * encoding, because it will be sent directly with CopySendString.
			 *
			 * MPP: in here we only care about this if we need to print the
			 * header. We rely on the segdb server copy out to do the conversion
			 * before sending the data rows out. We don't need to repeat it here
			 */
			if (cstate->need_transcoding)
				cstate->opts.null_print = (char *)
					pg_server_to_any(cstate->opts.null_print,
									 strlen(cstate->opts.null_print),
									 cstate->file_encoding);

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;
				Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

				if (hdr_delim)
					CopySendChar(cstate, cstate->opts.delim[0]);
				hdr_delim = true;

				colname = NameStr(attr->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			/* add a newline and flush the data */
			CopySendEndOfRow(cstate);
		}

		/*
		 * This is the main work-loop. In here we keep collecting data from the
		 * COPY commands on the segdbs, until no more data is available. We
		 * keep writing data out a chunk at a time.
		 */
		do
		{
			bool		copy_cancel = (QueryCancelPending ? true : false);

			/* get a chunk of data rows from the QE's */
			done = cdbCopyGetData(cdbCopy, copy_cancel, &processed);

			/* send the chunk of data rows to destination (file or stdout) */
			if (cdbCopy->copy_out_buf.len > 0) /* conditional is important! */
			{
				/*
				 * in the dispatcher we receive chunks of whole rows with row endings.
				 * We don't want to use CopySendEndOfRow() b/c it adds row endings and
				 * also b/c it's intended for a single row at a time. Therefore we need
				 * to fill in the out buffer and just flush it instead.
				 */
				CopySendData(cstate, (void *) cdbCopy->copy_out_buf.data, cdbCopy->copy_out_buf.len);
				CopyToDispatchFlush(cstate);
			}
		} while(!done);

		cdbCopyEnd(cdbCopy, NULL, NULL);

		/* now it's safe to destroy the whole dispatcher state */
		CdbDispatchCopyEnd(cdbCopy);
	}
	/* catch error from CopyStart, CopySendEndOfRow or CopyToDispatchFlush */
	PG_CATCH();
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

		cdbCopyAbort(cdbCopy);

		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (cstate->opts.binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);
		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	/* we can throw the error now if QueryCancelPending was set previously */
	CHECK_FOR_INTERRUPTS();

	pfree(cdbCopy);

	return processed;
}

/*
 * Copy FROM directory table TO file, in the dispatcher. Starts a COPY TO command
 * on each of the executors and gathers all the results and writes it out.
 */
static uint64
CopyToDispatchDirectoryTable(CopyToState cstate)
{
	CopyStmt	*stmt = glob_copystmt;
	TupleDesc	tupDesc;
	CdbCopy		*cdbCopy;
	uint64		processed = 0;

	tupDesc = cstate->rel->rd_att;

	cstate->fe_msgbuf = makeStringInfo();
	cdbCopy = makeCdbCopyTo(cstate);

	/*
	 * Start a COPY command in every db of every segment in Apache Cloudberry.
	 *
	 * From this point in the code we need to be extra careful
	 * about error handling. ereport() must not be called until
	 * the COPY command sessions are closed on the executors.
	 * Calling ereport() will leave the executors hanging in
	 * COPY state.
	 */
	elog(DEBUG5, "COPY command sent to segdbs");
	
	PG_TRY();
	{
		bool		done;

		cdbCopyStart(cdbCopy, stmt, cstate->file_encoding);

		if (!cstate->opts.binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Only support copy binary to directory table.")));

		/*
		 * This is the main work-loop. In here we keep collecting data from the
		 * COPY commands on the segdbs, until no more data is available. We
		 * keep writing data out a chunk at a time.
		 */
		do
		{
			bool	copy_cancel = (QueryCancelPending ? true : false);

			/* get a chunk of data rows from the QE's */
			done = cdbCopyGetData(cdbCopy, copy_cancel, &processed);

			/* send the chunk of data rows to destination (file or stdout) */
			if (cdbCopy->copy_out_buf.len > 0) /* conditional is important! */
			{
				/*
				 * in the dispatcher we receive chunks of file and flush it.
				 */
				CopySendData(cstate, (void *) cdbCopy->copy_out_buf.data, cdbCopy->copy_out_buf.len);
				CopyToDispatchFlush(cstate);
			}
		} while (!done);

		cdbCopyEnd(cdbCopy, NULL, NULL);

		/* now it's safe to destroy the whole dispatcher state */
		CdbDispatchCopyEnd(cdbCopy);
	}
	/* catch error */
	PG_CATCH();
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

		cdbCopyAbort(cdbCopy);

		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* we can throw the error now if QueryCancelPending was set previously */
	CHECK_FOR_INTERRUPTS();

	pfree(cdbCopy);

	if (processed)
		return 1;
	else
		return 0;
}

/*
 * Copy from relation or query TO file.
 *
 * Returns the number of rows processed.
 */
static uint64
CopyTo(CopyToState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	ListCell   *cur;
	uint64		processed = 0;

	if (cstate->rel && cstate->rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
		return CopyToDirectoryTable(cstate);

	if (cstate->rel)
		tupDesc = RelationGetDescr(cstate->rel);
	else
		tupDesc = cstate->queryDesc->tupDesc;
	num_phys_attrs = tupDesc->natts;
	cstate->opts.null_print_client = cstate->opts.null_print; /* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->opts.binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);
	if (!cstate->opts.binary)
	{
		/*
		 * For non-binary copy, we need to convert null_print to file
		 * encoding, because it will be sent directly with CopySendString.
		 */
		if (cstate->need_transcoding)
			cstate->opts.null_print_client = pg_server_to_any(cstate->opts.null_print,
															  cstate->opts.null_print_len,
															  cstate->file_encoding);
	}

	if (Gp_role == GP_ROLE_EXECUTE && !cstate->opts.on_segment)
	{
		/* header should not be printed in execute mode. */
	}
	else if (cstate->opts.binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/* if a header has been requested send the line */
		if (cstate->opts.header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->opts.delim[0]);
				hdr_delim = true;

				colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);

				if (cstate->opts.csv_mode)
					CopyAttributeOutCSV(cstate, colname, false,
										list_length(cstate->attnumlist) == 1);
				else
					CopyAttributeOutText(cstate, colname);
			}

			CopySendEndOfRow(cstate);
		}
	}

	if (cstate->rel)
	{
		TupleTableSlot *slot;
		TableScanDesc scandesc;

		scandesc = table_beginscan(cstate->rel, GetActiveSnapshot(), 0, NULL);
		slot = table_slot_create(cstate->rel, NULL);

		processed = 0;
		while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
		{
			CHECK_FOR_INTERRUPTS();

			/* Deconstruct the tuple ... */
			slot_getallattrs(slot);

			/* Format and send the data */
			CopyOneRowTo(cstate, slot);

			/*
			 * Increment the number of processed tuples, and report the
			 * progress.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
		}

		ExecDropSingleTupleTableSlot(slot);
		table_endscan(scandesc);
	}
	else
	{
		Assert(Gp_role != GP_ROLE_EXECUTE);

		/* run the plan --- the dest receiver will send tuples */
		ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0, true);
		processed = ((DR_copy *) cstate->queryDesc->dest)->processed;
	}

	if (Gp_role == GP_ROLE_EXECUTE && !cstate->opts.on_segment)
	{
		/*
		 * Trailer should not be printed in execute mode. The dispatcher will
		 * write it once.
		 */
	}
	else if (cstate->opts.binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);

		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	if (Gp_role == GP_ROLE_EXECUTE && cstate->opts.on_segment)
		SendNumRows(0, processed);

	MemoryContextDelete(cstate->rowcontext);

	return processed;
}

/*
 * Copy directory table To QD.
 */
static uint64
CopyToDirectoryTable(CopyToState cstate)
{
	uint64		processed = 0;
	TupleTableSlot *slot;
	TableScanDesc scandesc;
	ScanKeyData skey;
	Datum		datum;
	bool		isnull;
	char	   *relative_path;
	char	   *scopedFileUrl;
	DirectoryTable	*dirTable;
	UFile	   *file;
	int64_t		file_size;
	pg_cryptohash_ctx	*hashCtx;
	char		hexMd5Sum[256];
	uint8 md5Sum[MD5_DIGEST_LENGTH];
	char	   *md5;

	Assert(cstate->rel);

	dirTable = GetDirectoryTable(RelationGetRelid(cstate->rel));

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);

	ScanKeyInit(&skey,
				(AttrNumber) 1,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(cstate->dirfilename));
	scandesc = table_beginscan(cstate->rel, GetActiveSnapshot(), 1, &skey);
	slot = table_slot_create(cstate->rel, NULL);

	processed = 0;
	if (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
	{
		char 	buffer[4096];
		char	errorMessage[256];
		int		bytesRead;

		/* Deconstruct the tuple ... */
		slot_getallattrs(slot);

		datum = slot_getattr(slot, 1, &isnull);
		Assert(isnull == false);
		relative_path = TextDatumGetCString(datum);
		scopedFileUrl = psprintf("%s/%s", dirTable->location, relative_path);

		hashCtx = pg_cryptohash_create(PG_MD5);
		if (hashCtx == NULL)
			ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("failed to create md5hash context: out of memory")));
		pg_cryptohash_init(hashCtx);

		file = UFileOpen(dirTable->spcId,
						 scopedFileUrl,
						 O_RDONLY,
						 errorMessage,
						 sizeof(errorMessage));
		if (file == NULL)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("failed to open file \"%s\": %s",
								scopedFileUrl, errorMessage)));

		file_size = UFileSize(file);

		while (true)
		{
			bytesRead = UFileRead(file, buffer, sizeof(buffer));
			if (bytesRead == -1)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to read file \"%s\": %s", scopedFileUrl, UFileGetLastError(file))));

			if (bytesRead == 0)
				break;

			CopySendData(cstate, buffer, bytesRead);
			pg_cryptohash_update(hashCtx, (const uint8 *) buffer, bytesRead);
			CopySendEndOfRow(cstate);
		}

		pg_cryptohash_final(hashCtx, md5Sum, sizeof(md5Sum));
		pg_cryptohash_free(hashCtx);
		bytesToHex(md5Sum, hexMd5Sum);

		/* Get md5 from schema table */
		datum = slot_getattr(slot, 4, &isnull);
		Assert(isnull == false);
		md5 = TextDatumGetCString(datum);

		if (strcmp(md5, hexMd5Sum) != 0)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("Copy directory table to file failed, as file content is not consistent.")));
 
		if (UFileClose(file) < 0)
			ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("failed to close the file \"%s\": %s",
								scopedFileUrl, UFileGetLastError(file))));

		pfree(file);

		/*
		 * Increment the number of processed tuples, and report the
		 * progress.
		 */
		pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
									 ++processed);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scandesc);

	MemoryContextDelete(cstate->rowcontext);

	return processed;
}

void
CopyOneCustomRowTo(CopyToState cstate, bytea *value)
{
	appendBinaryStringInfo(cstate->fe_msgbuf,
						   VARDATA_ANY((void *) value),
						   VARSIZE_ANY_EXHDR((void *) value));
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
EndCopy(CopyToState cstate)
{
	if (cstate->is_program)
	{
		if (cstate->copy_file)
		{
			fclose(cstate->copy_file);
			cstate->copy_file = NULL;
		}

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

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

void
CopySendChar(CopyToState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

CopyToState
BeginCopyToDirectoryTable(ParseState *pstate,
						  const char *filename,
						  const char *dirfilename,
						  Relation rel,
						  bool	is_program,
						  List *options)
{
	CopyToState	cstate;
	bool		pipe;
	MemoryContext oldcontext;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_TO,
		0
	};

	if (!glob_copystmt->dirfilename)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("COPY to directory table must specify the relative_path name.")));

	/* Allocate workspace and zero all fields */
	cstate = (CopyToStateData *) palloc0(sizeof(CopyToStateData));

	glob_cstate = cstate;
	
	/*
	 * We allocate everying used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
											 	"COPY",
											 	ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Process the target relation */
	Assert(rel);
	cstate->rel = rel;

	/* Check whether copy directory table options allowed */
	ProcessCopyDirectoryTableOptions(pstate, &cstate->opts, true, options, rel->rd_id);

	if (Gp_role == GP_ROLE_DISPATCH && !cstate->opts.binary)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Only support copy binary directory table to.")));

	cstate->file_encoding = GetDatabaseEncoding();
	cstate->need_transcoding = false;

	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
	
	cstate->copy_dest = COPY_FILE;	/* default */

	pipe = (filename == NULL || Gp_role == GP_ROLE_EXECUTE);

	/* Determine the mode */
	if (Gp_role == GP_ROLE_DISPATCH && cstate->opts.on_segment &&
		cstate->rel && cstate->rel->rd_cdbpolicy)
	{
		cstate->dispatch_mode = COPY_DISPATCH;
	}
	else
		cstate->dispatch_mode = COPY_DIRECT;

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* in On SEGMENT mode, we don't open anything on the dispatcher. */

		if (filename == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("STDOUT is not supported by 'COPY ON SEGMENT'")));
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		cstate->dirfilename = pstrdup(dirfilename);
		
		/*
		 * the grammar does not allow this on QD.
		 * on QE, this could happen
		 */
		Assert(!is_program || Gp_role == GP_ROLE_EXECUTE);
		if (whereToSendOutput != DestRemote)
			cstate->copy_file = stdout;
	}
	else
	{
		cstate->filename = pstrdup(filename);
		cstate->dirfilename = pstrdup(dirfilename);
		cstate->is_program = is_program;

		if (is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate->filename, true);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[EXEC_DATA_P], PG_BINARY_W);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			mode_t		oumask;	/* Pre-existing umask value */
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in
			 * the foot by overwriting a database file ...
			 */
			if (!is_absolute_path(cstate->filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask(S_IWGRP | S_IWOTH);
			PG_TRY();
			{
				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			}
			PG_FINALLY();
			{
				umask(oumask);
			}
			PG_END_TRY();
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int		save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY TO instructs the PostgreSQL server process to write a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			// Increase buffer size to improve performance  (cmcdevitt)
			/* GPDB_14_MERGE_FIXME: Ret value process. */
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216);	// 384 Kbytes

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));
		}
	}
	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
							      cstate->rel ? RelationGetRelid(cstate->rel) :InvalidOid);
	pgstat_progress_update_multi_param(2, progress_cols, progress_vals);

	cstate->bytes_processed = 0;

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)
