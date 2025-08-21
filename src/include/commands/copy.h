/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "commands/trigger.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "executor/executor.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbsreh.h"

/*
 * The error handling mode for this data load.
 */
typedef enum CopyErrMode
{
	ALL_OR_NOTHING,	/* Either all rows or no rows get loaded (the default) */
	SREH_IGNORE,	/* Sreh - ignore errors (REJECT, but don't log errors) */
	SREH_LOG		/* Sreh - log errors */
} CopyErrMode;

typedef struct ProgramPipes
{
	char *shexec;
	int pipes[2];
	int pid;
} ProgramPipes;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

/*
 *
 * COPY FROM modes (from file/client to table)
 *
 * 1. "normal", direct, mode. This means ON SEGMENT running on a segment, or
 *    utility mode, or non-distributed table in QD.
 * 2. Dispatcher mode. We are reading from file/client, and forwarding all data to QEs,
 *    or vice versa.
 * 3. Executor mode. We are receiving pre-processed data from QD, and inserting to table.
 *
 * COPY TO modes (table/query to file/client)
 *
 * 1. Direct. This can mean ON SEGMENT running on segment, or utility mode, or
 *    non-distributed table in QD. Or COPY TO running on segment.
 * 2. Dispatcher mode. We are receiving pre-formatted data from segments, and forwarding
 *    it all to to the client.
 * 3. Executor mode. Not used.
 */

typedef enum
{
	COPY_DIRECT,
	COPY_DISPATCH,
	COPY_EXECUTOR
} CopyDispatchMode;

/*
 * Represents whether a header line should be present, and whether it must
 * match the actual names (which implies "true").
 */
typedef enum CopyHeaderChoice
{
	COPY_HEADER_FALSE = 0,
	COPY_HEADER_TRUE,
	COPY_HEADER_MATCH,
} CopyHeaderChoice;

/*
 * A struct to hold COPY options, in a parsed form. All of these are related
 * to formatting, except for 'freeze', which doesn't really belong here, but
 * it's expedient to parse it along with all the other options.
 */
typedef struct CopyFormatOptions
{
	/* parameters from the COPY command */
	int			file_encoding;	/* file or remote side's character encoding,
								 * -1 if not specified */
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	CopyHeaderChoice header_line;	/* header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *default_print;	/* DEFAULT marker string */
	int			default_print_len;	/* length of same */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
	bool	   *convert_select_flags;   /* per-column CSV/TEXT CS flags */
	bool		fill_missing;	/* missing attrs at end of line are NULL */


	/* Apache Cloudberry specific variables */
	bool		skip_foreign_partitions;  /* skip foreign/external partitions */

	bool		on_segment; /* QE save data files locally */
	bool		delim_off;	/* delimiter is set to OFF? */
	EolType		eol_type;		/* EOL type of input */
	char	   *eol_str;		/* optional NEWLINE from command. before eol_type is defined */
	char	   *tags;			/* directory table */
	SingleRowErrorDesc *sreh;
	/* end Apache Cloudberry specific variables */
} CopyFormatOptions;

/* These are private in commands/copy[from|to].c */
typedef struct CopyFromStateData *CopyFromState;
typedef struct CopyToStateData *CopyToState;

/* DestReceiver for COPY (query) TO */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyToState	cstate;			/* CopyStateData for the command */
	QueryDesc	*queryDesc;		/* QueryDesc for the copy*/
	uint64		processed;		/* # of tuples processed */
} DR_copy;

typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread, void *extra);
typedef void (*copy_data_dest_cb) (void *data, int len);

extern void DoCopy(ParseState *pstate, const CopyStmt *stmt,
				   int stmt_location, int stmt_len,
				   uint64 *processed);

extern void ProcessCopyDirectoryTableOptions(ParseState *pstate, CopyFormatOptions *ops_out, bool is_from, List *options, Oid rel_oid);
extern void ProcessCopyOptions(ParseState *pstate, CopyFormatOptions *opts_out, bool is_from, List *options, Oid rel_oid);
extern CopyFromState BeginCopyFrom(ParseState *pstate, Relation rel, Node *whereClause,
								   const char *filename,
								   bool is_program, copy_data_source_cb data_source_cb,
								   void *data_source_cb_extra,
								   List *attnamelist, List *options);
extern CopyToState BeginCopyToForeignTable(Relation forrel, List *options);
extern CopyToState BeginCopyToOnSegment(QueryDesc *queryDesc);
extern void EndCopyToOnSegment(CopyToState cstate);
extern CopyToState BeginCopy(ParseState *pstate, Relation rel, RawStmt *raw_query, Oid queryRelId,
							 List *attnamelist, List *options, TupleDesc tupDesc);
extern void CopyOneCustomRowTo(CopyToState cstate, bytea *value);
extern void EndCopyFrom(CopyFromState cstate);
extern bool NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
extern bool NextCopyFromRawFields(CopyFromState cstate,
								  char ***fields, int *nfields);
extern bool NextCopyFromRawFieldsX(CopyFromState cstate, char ***fields, int *nfields,
								   int stop_processing_at_field);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyFromState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);

extern void CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot);
extern void CopySendEndOfRow(CopyToState cstate);
extern void truncateEol(StringInfo buf, EolType	eol_type);
extern void truncateEolStr(char *str, EolType eol_type);
extern CopyToState BeginCopyToDirectoryTable(ParseState *pstate, const char *filename, const char *dirfilename,
											 Relation rel, bool is_program, List *options);

/*
 * This is used to hold information about the target's distribution policy,
 * during COPY FROM.
 */
typedef struct GpDistributionData
{
	GpPolicy   *policy;		/* partitioning policy for this table */
	CdbHash	   *cdbHash;	/* corresponding CdbHash object */
} GpDistributionData;

/*
 * internal prototypes
 */
extern CopyToState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *raw_query,
							   Oid queryRelId, const char *filename, bool is_program,
							   copy_data_dest_cb data_dest_cb, List *attnamelist, List *options);
extern void EndCopyTo(CopyToState cstate, uint64 *processed);
extern uint64 DoCopyTo(CopyToState cstate);
extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
							List *attnamelist);
extern ProgramPipes* open_program_pipes(char *command, bool forwrite);
extern void close_program_pipes(ProgramPipes *program_pipes, bool ifThrow);
extern void MangleCopyFileName(char **filenameptr, CdbSreh *cdbsreh);

extern volatile CopyToState glob_cstate;

#endif							/* COPY_H */
